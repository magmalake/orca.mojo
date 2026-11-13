//! Running a worker in a test, through the same C ABI morca calls.
//!
//! ```ignore
//! let out = morca_worker::testing::Run::new("2024-01").param("rows", "10").go::<Numbers>()?;
//! assert_eq!(out.rows(), 10);
//! ```
//!
//! A worker that writes Parquet gets an output directory of its own, and
//! [`Ran::files`] lists what it reported with `add_file`, in order.

use std::ffi::{c_char, c_void, CString};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, RecordBatchOptions, StructArray};
use arrow_schema::{Schema, SchemaRef};

use crate::{MorcaRunContext, Worker};

/// One run of a worker, built up and then made with [`Run::go`].
pub struct Run {
    partition: String,
    params: serde_json::Map<String, serde_json::Value>,
    /// Set to stop the worker; shared with the test.
    pub stop: Arc<AtomicI32>,
    size: u64,
}

/// What a run gave: its batches, the files it reported, and what it
/// logged, progress and metrics as lines (`progress 3/10`, `metric
/// name=value`). Dropping it removes the output directory.
#[derive(Debug)]
pub struct Ran {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub logs: Vec<String>,
    /// The Parquet files the worker reported, which exist.
    pub files: Vec<PathBuf>,
    /// The directory the worker was given for its files.
    pub output_dir: PathBuf,
}

impl Drop for Ran {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.output_dir);
    }
}

impl Ran {
    pub fn rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

#[derive(Default)]
struct Host {
    lines: Mutex<Vec<String>>,
    files: Mutex<Vec<PathBuf>>,
    /// The files reported that don't exist, which morca would refuse.
    missing: Mutex<Vec<PathBuf>>,
}

unsafe fn text(p: *const u8, n: usize) -> String {
    String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(p, n) }).into_owned()
}

unsafe extern "C" fn log(host: *mut c_void, msg: *const u8, len: usize) {
    let h = unsafe { &*(host as *const Host) };
    h.lines.lock().unwrap().push(unsafe { text(msg, len) });
}

unsafe extern "C" fn progress(host: *mut c_void, done: i64, total: i64) {
    let h = unsafe { &*(host as *const Host) };
    h.lines
        .lock()
        .unwrap()
        .push(format!("progress {done}/{total}"));
}

unsafe extern "C" fn metric(host: *mut c_void, name: *const u8, len: usize, value: f64) {
    let h = unsafe { &*(host as *const Host) };
    h.lines
        .lock()
        .unwrap()
        .push(format!("metric {}={value}", unsafe { text(name, len) }));
}

unsafe extern "C" fn add_file(host: *mut c_void, path: *const u8, len: usize) -> i32 {
    let h = unsafe { &*(host as *const Host) };
    let p = PathBuf::from(unsafe { text(path, len) });
    if p.is_file() {
        h.files.lock().unwrap().push(p);
        0
    } else {
        h.missing.lock().unwrap().push(p);
        1
    }
}

unsafe fn last_error(s: &mut FFI_ArrowArrayStream) -> String {
    let p = match s.get_last_error() {
        Some(f) => unsafe { f(s) },
        None => std::ptr::null(),
    };
    if p.is_null() {
        return "(no message)".into();
    }
    unsafe { std::ffi::CStr::from_ptr(p) }
        .to_string_lossy()
        .into_owned()
}

/// The stream's schema and batches, and the error that ended it, if one did.
unsafe fn pull(s: &mut FFI_ArrowArrayStream) -> (SchemaRef, Vec<RecordBatch>, Option<String>) {
    let mut cs = FFI_ArrowSchema::empty();
    let empty = Arc::new(Schema::empty());
    if unsafe { (s.get_schema().unwrap())(s, &mut cs) } != 0 {
        return (empty, Vec::new(), Some(unsafe { last_error(s) }));
    }
    let schema = match Schema::try_from(&cs) {
        Ok(sc) => Arc::new(sc),
        Err(e) => return (empty, Vec::new(), Some(e.to_string())),
    };
    let mut batches = Vec::new();
    loop {
        let mut a = FFI_ArrowArray::empty();
        if unsafe { (s.get_next().unwrap())(s, &mut a) } != 0 {
            return (schema, batches, Some(unsafe { last_error(s) }));
        }
        if a.is_released() {
            return (schema, batches, None);
        }
        let got = unsafe { arrow_array::ffi::from_ffi(a, &cs) }.and_then(|d| {
            let rows = d.len();
            let st = StructArray::from(d);
            RecordBatch::try_new_with_options(
                schema.clone(),
                st.columns().to_vec(),
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
        });
        match got {
            Ok(b) => batches.push(b),
            Err(e) => return (schema, batches, Some(e.to_string())),
        }
    }
}

static DIRS: AtomicUsize = AtomicUsize::new(0);

impl Run {
    pub fn new(partition: &str) -> Self {
        Run {
            partition: partition.to_owned(),
            params: serde_json::Map::new(),
            stop: Arc::new(AtomicI32::new(0)),
            size: std::mem::size_of::<MorcaRunContext>() as u64,
        }
    }

    /// Passes the context of a morca from before files from workers
    /// ([`crate::CONTEXT_SIZE_V1`] bytes), to test a worker under one.
    pub fn without_files(mut self) -> Self {
        self.size = crate::CONTEXT_SIZE_V1;
        self
    }

    pub fn param(mut self, name: &str, value: impl Into<serde_json::Value>) -> Self {
        self.params.insert(name.to_owned(), value.into());
        self
    }

    /// Runs `W` and pulls its whole stream. The error is what morca would
    /// fail the attempt with.
    pub fn go<W: Worker>(self) -> Result<Ran, String> {
        let host = Box::new(Host::default());
        let cs = |s: &str| CString::new(s).unwrap();
        let (pipeline, step, partition) = (cs("test"), cs("test"), cs(&self.partition));
        let params = cs(&serde_json::Value::Object(self.params.clone()).to_string());
        let (secrets, code) = (cs("{}"), cs(""));
        let dir = std::env::temp_dir().join(format!(
            "morca-worker-test-{}-{}",
            std::process::id(),
            DIRS.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        let output_dir = cs(dir.to_str().ok_or("the temporary directory isn't UTF-8")?);
        let ctx = MorcaRunContext {
            size: self.size,
            pipeline: pipeline.as_ptr(),
            step: step.as_ptr(),
            partition: partition.as_ptr(),
            params: params.as_ptr(),
            secrets: secrets.as_ptr(),
            code: code.as_ptr(),
            run: 0,
            attempt: 1,
            step_version: 0,
            stop: Arc::as_ptr(&self.stop),
            host: &*host as *const Host as *mut c_void,
            log: Some(log),
            progress: Some(progress),
            metric: Some(metric),
            output_dir: output_dir.as_ptr(),
            add_file: Some(add_file),
        };
        let mut out = FFI_ArrowArrayStream::empty();
        let mut error: *mut c_char = std::ptr::null_mut();
        let rc =
            unsafe { crate::__private::run::<W>(&ctx, std::ptr::null_mut(), &mut out, &mut error) };
        let lines = |h: &Host| h.lines.lock().unwrap().clone();
        let refused = |h: &Host| {
            h.missing.lock().unwrap().first().map(|p| {
                format!("the worker reported the file {}, which doesn't exist", p.display())
            })
        };
        if rc != 0 {
            let _ = std::fs::remove_dir_all(&dir);
            let msg = unsafe { std::ffi::CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned();
            unsafe { crate::__private::free(error) };
            return Err(refused(&host).unwrap_or(msg));
        }
        // Pulled through the C functions, as morca pulls them, so an error
        // is what `get_last_error` gives morca.
        let (schema, batches, failed) = unsafe { pull(&mut out) };
        drop(out);
        drop((pipeline, step, partition, params, secrets, code, output_dir));
        let ran = Ran {
            schema,
            batches,
            logs: lines(&host),
            files: host.files.lock().unwrap().clone(),
            output_dir: dir,
        };
        if let Some(e) = refused(&host).or(failed) {
            return Err(e);
        }
        Ok(ran)
    }
}
