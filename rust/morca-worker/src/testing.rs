//! Running a worker in a test, through the same C ABI morca calls.
//!
//! ```ignore
//! let out = morca_worker::testing::Run::new("2024-01").param("rows", "10").go::<Numbers>()?;
//! assert_eq!(out.rows(), 10);
//! ```

use std::ffi::{c_char, c_void, CString};
use std::sync::atomic::AtomicI32;
use std::sync::{Arc, Mutex};

use arrow_array::ffi_stream::{FFI_ArrowArrayStream, RecordBatchStreamReader};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;

use crate::{MorcaRunContext, Worker};

/// One run of a worker, built up and then made with [`Run::go`].
pub struct Run {
    partition: String,
    params: serde_json::Map<String, serde_json::Value>,
    /// Set to stop the worker; shared with the test.
    pub stop: Arc<AtomicI32>,
}

/// What a run gave: its batches and what it logged, progress and metrics
/// as lines (`progress 3/10`, `metric name=value`).
#[derive(Debug)]
pub struct Ran {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub logs: Vec<String>,
}

impl Ran {
    pub fn rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

#[derive(Default)]
struct Host {
    lines: Mutex<Vec<String>>,
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

impl Run {
    pub fn new(partition: &str) -> Self {
        Run {
            partition: partition.to_owned(),
            params: serde_json::Map::new(),
            stop: Arc::new(AtomicI32::new(0)),
        }
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
        let ctx = MorcaRunContext {
            size: std::mem::size_of::<MorcaRunContext>() as u64,
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
        };
        let mut out = FFI_ArrowArrayStream::empty();
        let mut error: *mut c_char = std::ptr::null_mut();
        let rc =
            unsafe { crate::__private::run::<W>(&ctx, std::ptr::null_mut(), &mut out, &mut error) };
        let lines = |h: &Host| h.lines.lock().unwrap().clone();
        if rc != 0 {
            let msg = unsafe { std::ffi::CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned();
            unsafe { crate::__private::free(error) };
            return Err(msg);
        }
        let reader =
            unsafe { RecordBatchStreamReader::from_raw(&mut out) }.map_err(|e| e.to_string())?;
        let schema = reader.schema();
        let mut batches = Vec::new();
        let mut failed = None;
        let mut reader = reader;
        for b in &mut reader {
            match b {
                Ok(b) => batches.push(b),
                Err(e) => {
                    failed = Some(e.to_string());
                    break;
                }
            }
        }
        drop(reader);
        drop((pipeline, step, partition, params, secrets, code));
        if let Some(e) = failed {
            return Err(e);
        }
        Ok(Ran {
            schema,
            batches,
            logs: lines(&host),
        })
    }
}
