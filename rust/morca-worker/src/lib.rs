//! Write a morca pipeline step in Rust.
//!
//! A worker is a `cdylib` that implements [`Worker`] and names it in
//! [`export_worker!`]. morca's `morca.rust_step` loads the library when the
//! step runs, calls the worker once, and writes the record batches it
//! streams back into the step's Iceberg table: Parquet as the batches
//! arrive, then one commit. docs/rust-workers.md in the morca repository is
//! the full contract; this crate is its Rust side.
//!
//! ```ignore
//! use std::sync::Arc;
//! use morca_worker::arrow_array::{Int64Array, RecordBatch};
//! use morca_worker::{export_worker, schema, Output, Result, RunContext, Worker};
//!
//! const SCHEMA: &str = include_str!("../schemas/numbers.json");
//!
//! struct Numbers;
//!
//! impl Worker for Numbers {
//!     const NAME: &'static str = "numbers";
//!     const VERSION: &'static str = env!("CARGO_PKG_VERSION");
//!
//!     fn run(ctx: &RunContext, _input: Option<morca_worker::Input>) -> Result<Output> {
//!         let schema = Arc::new(schema::to_arrow(SCHEMA)?);
//!         ctx.log(format!("making numbers for {}", ctx.partition()));
//!         let ids = Int64Array::from(vec![1, 2, 3]);
//!         let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids)])?;
//!         Ok(Output::from_batches(schema, vec![batch]))
//!     }
//! }
//!
//! export_worker!(Numbers);
//! ```
//!
//! What the crate takes care of, so a worker is safe Rust only:
//!
//! - **Panics** never cross into morca: `run`, and each batch the stream
//!   pulls, are wrapped in `catch_unwind`, and a panic fails the attempt
//!   with its message. Don't build workers with `panic = "abort"`: a panic
//!   would then end the whole process.
//! - **Errors** come back as a message in a buffer the worker owns, freed by
//!   `morca_worker_free`; there is no global "last error".
//! - **Callbacks** into morca (log, progress, metrics) happen only on the
//!   thread morca called the worker on. [`RunContext`] is `Send + Sync`:
//!   calls from other threads are queued and delivered on that thread the
//!   next time it logs or morca pulls a batch.
//! - **The stop flag**: [`RunContext::stopping`] reads an atomic morca sets
//!   when the run should end early (its code is outdated: a newer version
//!   leads). Poll it between units of work, from any thread.
//!
//! The cdylib exports only the four `morca_worker_*` functions
//! [`export_worker!`] defines, and Rust's standard library is linked into
//! it statically, as it always is for a `cdylib`.

use std::any::Any;
use std::ffi::{c_char, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, ThreadId};

pub use arrow_array;
pub use arrow_schema;
pub use serde_json;

use arrow_array::ffi_stream::RecordBatchStreamReader;
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};

pub mod schema;
pub mod testing;

/// The version of the C ABI this crate implements. morca refuses a library
/// whose `morca_worker_abi_version()` is another.
pub const ABI_VERSION: u32 = 1;

/// Any error a worker returns; its `Display` is the attempt's error.
pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The step's input stream, when morca passes one (none in ABI version 1).
pub type Input = RecordBatchStreamReader;

/// `log(host, utf8, len)`: a line for the run's log.
pub type LogFn = unsafe extern "C" fn(host: *mut c_void, msg: *const u8, len: usize);
/// `progress(host, done, total)`: how far the worker is; `total` 0 if unknown.
pub type ProgressFn = unsafe extern "C" fn(host: *mut c_void, done: i64, total: i64);
/// `metric(host, name, name_len, value)`: a named number, such as a peak.
pub type MetricFn =
    unsafe extern "C" fn(host: *mut c_void, name: *const u8, name_len: usize, value: f64);

/// What morca passes to `morca_worker_run`, field for field
/// (docs/rust-workers.md has the C declaration).
///
/// `size` comes first and is `sizeof` the struct as morca built it, so a
/// later morca can add fields at the end without a new ABI version: a
/// worker reads only the fields it knows, and refuses a context smaller
/// than this one. Strings are NUL-terminated UTF-8. The context and what it
/// points to stay valid until morca releases the output stream.
#[repr(C)]
pub struct MorcaRunContext {
    pub size: u64,
    pub pipeline: *const c_char,
    pub step: *const c_char,
    pub partition: *const c_char,
    /// The run's parameters for the worker: a JSON object.
    pub params: *const c_char,
    /// The secrets the pipeline declares, by name: a JSON object.
    pub secrets: *const c_char,
    /// The pipeline's registry hash, in hex.
    pub code: *const c_char,
    pub run: i64,
    pub attempt: i64,
    /// The step's `version=`.
    pub step_version: i64,
    /// An `int32` morca sets to non-zero to ask the worker to stop.
    pub stop: *const AtomicI32,
    /// Passed back to the callbacks.
    pub host: *mut c_void,
    pub log: Option<LogFn>,
    pub progress: Option<ProgressFn>,
    pub metric: Option<MetricFn>,
}

/// A worker: one step's work, run once per attempt.
pub trait Worker {
    /// The worker's name, in `morca_worker_describe`.
    const NAME: &'static str;
    /// Its version, in `morca_worker_describe`; usually the crate's.
    const VERSION: &'static str;

    /// Does the step's work and returns its output: a stream of batches
    /// morca pulls, one at a time, and writes as they come.
    ///
    /// Work can happen here, in the stream, or both: a reader that computes
    /// each batch when it's pulled keeps one batch in memory at a time. The
    /// batches' schema is checked against the step's table before anything
    /// is written (see [`schema`]). A worker that sees
    /// [`RunContext::stopping`] ends its stream early; morca commits what
    /// came, and the step decides what a stopped run means.
    fn run(ctx: &RunContext, input: Option<Input>) -> Result<Output>;
}

/// What [`Worker::run`] returns: the batches, as a reader morca pulls from.
pub struct Output {
    reader: Box<dyn RecordBatchReader + Send>,
}

impl Output {
    /// Any `RecordBatchReader`, e.g. one that makes each batch when pulled.
    pub fn from_reader(reader: impl RecordBatchReader + Send + 'static) -> Self {
        Output {
            reader: Box::new(reader),
        }
    }

    /// Batches made already.
    pub fn from_batches(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self::from_iter(schema, batches.into_iter().map(Ok))
    }

    /// Batches made one at a time as morca pulls them; an `Err` fails the
    /// attempt with its message.
    pub fn from_iter<I>(schema: SchemaRef, batches: I) -> Self
    where
        I: IntoIterator<Item = Result<RecordBatch>>,
        I::IntoIter: Send + 'static,
    {
        let it = batches
            .into_iter()
            .map(|r| r.map_err(ArrowError::ExternalError));
        Self::from_reader(RecordBatchIterator::new(it, schema))
    }
}

enum Msg {
    Log(String),
    Progress(i64, i64),
    Metric(String, f64),
}

struct Shared {
    raw: *const MorcaRunContext,
    thread: ThreadId,
    /// Set once the output stream is released (or `run` failed): from then
    /// on nothing calls into morca or reads the stop flag.
    closed: AtomicBool,
    tx: Sender<Msg>,
    rx: Mutex<Receiver<Msg>>,
    pipeline: String,
    step: String,
    partition: String,
    params: serde_json::Map<String, serde_json::Value>,
    secrets: serde_json::Map<String, serde_json::Value>,
    code: String,
    run: i64,
    attempt: i64,
    step_version: i64,
}

// The raw pointers are morca's, valid until `closed`; every call into morca
// happens on `thread`, and the stop flag is an atomic.
unsafe impl Send for Shared {}
unsafe impl Sync for Shared {}

/// The run, step and partition the worker runs for, and its way back into
/// morca: logs, progress, metrics and the stop flag.
///
/// Cheap to clone and `Send + Sync`: hand a clone to the threads the worker
/// starts. Their logs, progress and metrics are queued and delivered on the
/// thread morca called the worker on. Join those threads before the output
/// stream ends: once morca releases the stream, the context is closed, and
/// what's sent after that is dropped.
#[derive(Clone)]
pub struct RunContext {
    shared: Arc<Shared>,
}

unsafe fn c_str(p: *const c_char, what: &str) -> Result<String, String> {
    if p.is_null() {
        return Ok(String::new());
    }
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .map(str::to_owned)
        .map_err(|_| format!("morca-worker: the context's {what} isn't UTF-8"))
}

fn json_object(s: &str, what: &str) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    if s.trim().is_empty() {
        return Ok(serde_json::Map::new());
    }
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(serde_json::Value::Object(m)) => Ok(m),
        Ok(_) => Err(format!(
            "morca-worker: the context's {what} isn't a JSON object"
        )),
        Err(e) => Err(format!(
            "morca-worker: the context's {what} isn't JSON: {e}"
        )),
    }
}

impl RunContext {
    /// Reads morca's context; the calling thread becomes the run thread.
    ///
    /// # Safety
    /// `raw` must point to a valid `MorcaRunContext` that stays valid, with
    /// what it points to, until [`RunContext::close`].
    pub unsafe fn from_raw(raw: *const MorcaRunContext) -> Result<Self, String> {
        if raw.is_null() {
            return Err("morca-worker: morca passed no context".into());
        }
        let c = unsafe { &*raw };
        let want = std::mem::size_of::<MorcaRunContext>() as u64;
        if c.size < want {
            return Err(format!(
                "morca-worker: morca's context is {} bytes, smaller than the {want} this worker reads",
                c.size
            ));
        }
        let (tx, rx) = channel();
        let shared = unsafe {
            Shared {
                raw,
                thread: thread::current().id(),
                closed: AtomicBool::new(false),
                tx,
                rx: Mutex::new(rx),
                pipeline: c_str(c.pipeline, "pipeline")?,
                step: c_str(c.step, "step")?,
                partition: c_str(c.partition, "partition")?,
                params: json_object(&c_str(c.params, "params")?, "params")?,
                secrets: json_object(&c_str(c.secrets, "secrets")?, "secrets")?,
                code: c_str(c.code, "code")?,
                run: c.run,
                attempt: c.attempt,
                step_version: c.step_version,
            }
        };
        Ok(RunContext {
            shared: Arc::new(shared),
        })
    }

    pub fn pipeline(&self) -> &str {
        &self.shared.pipeline
    }

    pub fn step(&self) -> &str {
        &self.shared.step
    }

    /// The partition key, e.g. `2024-01`; empty for a gather step.
    pub fn partition(&self) -> &str {
        &self.shared.partition
    }

    /// The parameters the proxy step passed, a JSON object.
    pub fn params(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.shared.params
    }

    /// A string parameter, or `None` if it's missing or not a string.
    pub fn param(&self, name: &str) -> Option<&str> {
        self.shared.params.get(name).and_then(|v| v.as_str())
    }

    /// A secret the pipeline declares (`morca register --secret`).
    pub fn secret(&self, name: &str) -> Option<&str> {
        self.shared.secrets.get(name).and_then(|v| v.as_str())
    }

    /// The pipeline's registry hash, in hex.
    pub fn code(&self) -> &str {
        &self.shared.code
    }

    /// The run's number; 0 for a run in memory.
    pub fn run(&self) -> i64 {
        self.shared.run
    }

    /// The attempt's number, from 1.
    pub fn attempt(&self) -> i64 {
        self.shared.attempt
    }

    /// The step's `version=`.
    pub fn step_version(&self) -> i64 {
        self.shared.step_version
    }

    /// Whether morca asks the worker to stop: end the stream early, between
    /// units of work. Cheap (an atomic load); callable from any thread. True
    /// once the context is closed.
    pub fn stopping(&self) -> bool {
        if self.shared.closed.load(Ordering::Acquire) {
            return true;
        }
        let p = unsafe { (*self.shared.raw).stop };
        !p.is_null() && unsafe { (*p).load(Ordering::Acquire) } != 0
    }

    fn on_run_thread(&self) -> bool {
        thread::current().id() == self.shared.thread
    }

    /// A line for the run's log.
    pub fn log(&self, msg: impl Into<String>) {
        self.send(Msg::Log(msg.into()));
    }

    /// How far the worker is: `done` of `total` (0 if unknown).
    pub fn progress(&self, done: u64, total: u64) {
        self.send(Msg::Progress(done as i64, total as i64));
    }

    /// A named number for the run, such as the worker's peak memory.
    pub fn metric(&self, name: impl Into<String>, value: f64) {
        self.send(Msg::Metric(name.into(), value));
    }

    fn send(&self, m: Msg) {
        if self.shared.closed.load(Ordering::Acquire) {
            return;
        }
        if self.on_run_thread() {
            self.drain();
            self.deliver(m);
        } else {
            let _ = self.shared.tx.send(m);
        }
    }

    /// Delivers what other threads queued; only on the run thread.
    pub fn drain(&self) {
        if !self.on_run_thread() || self.shared.closed.load(Ordering::Acquire) {
            return;
        }
        let queued: Vec<Msg> = match self.shared.rx.lock() {
            Ok(rx) => rx.try_iter().collect(),
            Err(_) => return,
        };
        for m in queued {
            self.deliver(m);
        }
    }

    fn deliver(&self, m: Msg) {
        let c = unsafe { &*self.shared.raw };
        unsafe {
            match m {
                Msg::Log(s) => {
                    if let Some(f) = c.log {
                        f(c.host, s.as_ptr(), s.len());
                    }
                }
                Msg::Progress(d, t) => {
                    if let Some(f) = c.progress {
                        f(c.host, d, t);
                    }
                }
                Msg::Metric(n, v) => {
                    if let Some(f) = c.metric {
                        f(c.host, n.as_ptr(), n.len(), v);
                    }
                }
            }
        }
    }

    /// Delivers what's queued and closes the context: nothing calls into
    /// morca after this.
    pub fn close(&self) {
        self.drain();
        self.shared.closed.store(true, Ordering::Release);
    }
}

fn panic_message(p: &(dyn Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "(no message)".to_owned()
    }
}

/// The worker's reader as morca pulls it: each batch under `catch_unwind`,
/// the queued callbacks delivered around it, and the context closed when
/// the stream is released.
struct Guarded {
    inner: Option<Box<dyn RecordBatchReader + Send>>,
    schema: SchemaRef,
    ctx: RunContext,
    done: bool,
}

impl Iterator for Guarded {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        self.ctx.drain();
        let inner = self.inner.as_mut()?;
        let got = catch_unwind(AssertUnwindSafe(|| inner.next()));
        self.ctx.drain();
        match got {
            Ok(Some(Err(e))) => {
                self.done = true;
                Some(Err(e))
            }
            Ok(None) => {
                self.done = true;
                None
            }
            Ok(some) => some,
            Err(p) => {
                self.done = true;
                Some(Err(ArrowError::ExternalError(
                    format!("the worker panicked: {}", panic_message(p.as_ref())).into(),
                )))
            }
        }
    }
}

impl RecordBatchReader for Guarded {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for Guarded {
    fn drop(&mut self) {
        // The worker's reader first, so what its threads log as they're
        // joined is still delivered; a panic there is dropped with it.
        let inner = self.inner.take();
        let _ = catch_unwind(AssertUnwindSafe(move || drop(inner)));
        self.ctx.close();
    }
}

fn set_error(error: *mut *mut c_char, msg: String) {
    if error.is_null() {
        return;
    }
    let clean = msg.replace('\0', " ");
    let c = CString::new(clean).unwrap_or_else(|_| CString::new("(error)").unwrap());
    unsafe { *error = c.into_raw() };
}

/// What [`export_worker!`] expands to calls these; not for direct use.
#[doc(hidden)]
pub mod __private {
    use super::*;

    pub use arrow_array::ffi_stream::FFI_ArrowArrayStream;

    pub fn describe_json<W: Worker>() -> CString {
        let v = serde_json::json!({
            "name": W::NAME,
            "version": W::VERSION,
            "abi": ABI_VERSION,
            "morca_worker": env!("CARGO_PKG_VERSION"),
        });
        CString::new(v.to_string()).unwrap_or_default()
    }

    /// `morca_worker_run`: 0 when `out` holds the stream; else non-zero,
    /// with a message in `*error` for `morca_worker_free`.
    ///
    /// # Safety
    /// The pointers are morca's, as docs/rust-workers.md describes.
    pub unsafe fn run<W: Worker>(
        ctx: *const MorcaRunContext,
        input: *mut FFI_ArrowArrayStream,
        out: *mut FFI_ArrowArrayStream,
        error: *mut *mut c_char,
    ) -> i32 {
        let opened: std::cell::RefCell<Option<RunContext>> = std::cell::RefCell::new(None);
        let got = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
            if out.is_null() {
                return Err("morca-worker: morca passed no output stream".into());
            }
            let rc = unsafe { RunContext::from_raw(ctx)? };
            *opened.borrow_mut() = Some(rc.clone());
            let input = if input.is_null() {
                None
            } else {
                Some(
                    unsafe { RecordBatchStreamReader::from_raw(input) }
                        .map_err(|e| format!("morca-worker: can't read the input stream: {e}"))?,
                )
            };
            let output = W::run(&rc, input).map_err(|e| e.to_string())?;
            let schema = output.reader.schema();
            let guarded = Guarded {
                inner: Some(output.reader),
                schema,
                ctx: rc.clone(),
                done: false,
            };
            rc.drain();
            unsafe { std::ptr::write(out, FFI_ArrowArrayStream::new(Box::new(guarded))) };
            Ok(())
        }));
        let failed = |msg: String, code: i32| {
            if let Some(rc) = opened.borrow().as_ref() {
                rc.close();
            }
            set_error(error, msg);
            code
        };
        match got {
            Ok(Ok(())) => 0,
            Ok(Err(msg)) => failed(msg, 1),
            Err(p) => failed(
                format!("the worker panicked: {}", panic_message(p.as_ref())),
                2,
            ),
        }
    }

    /// `morca_worker_free`: frees a message `run` wrote.
    ///
    /// # Safety
    /// `p` is null or a pointer `run` wrote into `*error`, freed once.
    pub unsafe fn free(p: *mut c_char) {
        if !p.is_null() {
            drop(unsafe { CString::from_raw(p) });
        }
    }
}

/// Exports a [`Worker`] as the library's C ABI: `morca_worker_abi_version`,
/// `morca_worker_describe`, `morca_worker_run` and `morca_worker_free`,
/// and nothing else. One per `cdylib`.
#[macro_export]
macro_rules! export_worker {
    ($worker:ty) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn morca_worker_abi_version() -> u32 {
            $crate::ABI_VERSION
        }

        /// Static JSON (name, version, abi), owned by the library: never freed.
        #[unsafe(no_mangle)]
        pub extern "C" fn morca_worker_describe() -> *const ::std::ffi::c_char {
            static DESCRIBE: ::std::sync::OnceLock<::std::ffi::CString> =
                ::std::sync::OnceLock::new();
            DESCRIBE
                .get_or_init(|| $crate::__private::describe_json::<$worker>())
                .as_ptr()
        }

        /// # Safety
        /// Called by morca with the pointers docs/rust-workers.md describes.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn morca_worker_run(
            ctx: *const $crate::MorcaRunContext,
            input: *mut $crate::__private::FFI_ArrowArrayStream,
            out: *mut $crate::__private::FFI_ArrowArrayStream,
            error: *mut *mut ::std::ffi::c_char,
        ) -> i32 {
            unsafe { $crate::__private::run::<$worker>(ctx, input, out, error) }
        }

        /// # Safety
        /// `p` is a message `morca_worker_run` wrote, freed once.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn morca_worker_free(p: *mut ::std::ffi::c_char) {
            unsafe { $crate::__private::free(p) }
        }
    };
}
