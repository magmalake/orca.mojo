//! The ABI as morca drives it, through `morca_worker::testing`.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use morca_worker::arrow_array::{Int64Array, RecordBatch};
use morca_worker::arrow_schema::{DataType, Field, Schema};
use morca_worker::testing::Run;
use morca_worker::{Input, Output, Result, RunContext, Worker};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
}

fn batch(from: i64, n: i64) -> RecordBatch {
    let a = Int64Array::from((from..from + n).collect::<Vec<_>>());
    RecordBatch::try_new(schema(), vec![Arc::new(a)]).unwrap()
}

/// `batches` batches of 10 rows, made as they're pulled; logs from a thread.
struct Counting;

impl Worker for Counting {
    const NAME: &'static str = "counting";
    const VERSION: &'static str = "1";

    fn run(ctx: &RunContext, _input: Option<Input>) -> Result<Output> {
        let n: i64 = ctx.param("batches").unwrap_or("3").parse()?;
        let c = ctx.clone();
        std::thread::spawn(move || c.log("from a thread"))
            .join()
            .unwrap();
        ctx.log(format!("partition {}", ctx.partition()));
        let c = ctx.clone();
        let it = (0..n).map_while(move |i| {
            if c.stopping() {
                c.log("stopping");
                return None;
            }
            c.progress(i as u64 + 1, n as u64);
            Some(Ok(batch(i * 10, 10)))
        });
        Ok(Output::from_iter(schema(), it))
    }
}

struct PanicsInRun;

impl Worker for PanicsInRun {
    const NAME: &'static str = "panics";
    const VERSION: &'static str = "1";

    fn run(_ctx: &RunContext, _input: Option<Input>) -> Result<Output> {
        panic!("boom in run");
    }
}

struct PanicsInStream;

impl Worker for PanicsInStream {
    const NAME: &'static str = "panics-later";
    const VERSION: &'static str = "1";

    fn run(_ctx: &RunContext, _input: Option<Input>) -> Result<Output> {
        let it = (0..3).map(|i| {
            if i == 1 {
                panic!("boom in batch {i}");
            }
            Ok(batch(0, 1))
        });
        Ok(Output::from_iter(schema(), it))
    }
}

struct Fails;

impl Worker for Fails {
    const NAME: &'static str = "fails";
    const VERSION: &'static str = "1";

    fn run(_ctx: &RunContext, _input: Option<Input>) -> Result<Output> {
        Err("no data for this key".into())
    }
}

#[test]
fn streams_batches_and_delivers_logs() {
    let ran = Run::new("2024-01")
        .param("batches", "4")
        .go::<Counting>()
        .unwrap();
    assert_eq!(ran.rows(), 40);
    assert_eq!(ran.logs[0], "from a thread");
    assert_eq!(ran.logs[1], "partition 2024-01");
    assert!(
        ran.logs.contains(&"progress 4/4".to_owned()),
        "{:?}",
        ran.logs
    );
}

#[test]
fn a_panic_is_an_error() {
    let e = Run::new("x").go::<PanicsInRun>().unwrap_err();
    assert!(e.contains("the worker panicked: boom in run"), "{e}");
    let e = Run::new("x").go::<PanicsInStream>().unwrap_err();
    assert!(e.contains("the worker panicked: boom in batch 1"), "{e}");
}

#[test]
fn an_error_is_its_message() {
    assert_eq!(
        Run::new("x").go::<Fails>().unwrap_err(),
        "no data for this key"
    );
}

#[test]
fn stops_when_asked() {
    let run = Run::new("x").param("batches", "5");
    run.stop.store(1, Ordering::Release);
    let ran = run.go::<Counting>().unwrap();
    assert_eq!(ran.rows(), 0);
    assert!(ran.logs.contains(&"stopping".to_owned()));
}

/// Writes `files` files into the output directory: reported with `add_file`
/// in `run` (`how` = "run"), or one per pull with `Output::from_files`.
struct Files;

impl Worker for Files {
    const NAME: &'static str = "files";
    const VERSION: &'static str = "1";

    fn run(ctx: &RunContext, _input: Option<Input>) -> Result<Output> {
        let n: usize = ctx.param("files").unwrap_or("2").parse()?;
        let dir = ctx.output_dir()?.to_owned();
        match ctx.param("how").unwrap_or("run") {
            "run" => {
                for i in 0..n {
                    let name = format!("f{i}.parquet");
                    std::fs::write(dir.join(&name), b"PAR1")?;
                    ctx.add_file(&name)?;
                }
                Ok(Output::files())
            }
            "missing" => {
                ctx.add_file("nope.parquet")?;
                Ok(Output::files())
            }
            "thread" => {
                let c = ctx.clone();
                std::thread::spawn(move || {
                    std::fs::write(dir.join("t.parquet"), b"PAR1").unwrap();
                    c.add_file("t.parquet").unwrap();
                })
                .join()
                .unwrap();
                Ok(Output::files())
            }
            _ => {
                let c = ctx.clone();
                let it = (0..n).map(move |i| {
                    let p = dir.join(format!("p{i}.parquet"));
                    std::fs::write(&p, b"PAR1")?;
                    c.log(format!("wrote {i}"));
                    Ok(p)
                });
                Ok(Output::from_files(ctx, it))
            }
        }
    }
}

#[test]
fn reports_files_written_in_run() {
    let ran = Run::new("x").param("files", "3").go::<Files>().unwrap();
    assert_eq!(ran.rows(), 0);
    assert_eq!(ran.schema.fields().len(), 0);
    let names: Vec<_> = ran
        .files
        .iter()
        .map(|p| p.file_name().unwrap().to_str().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["f0.parquet", "f1.parquet", "f2.parquet"]);
    assert!(ran.files.iter().all(|p| p.starts_with(&ran.output_dir)));
}

#[test]
fn reports_files_one_per_pull() {
    let ran = Run::new("x")
        .param("files", "2")
        .param("how", "pull")
        .go::<Files>()
        .unwrap();
    assert_eq!(ran.files.len(), 2);
    // Two empty batches, one per file, each after its file was reported.
    assert_eq!(ran.batches.len(), 2);
    assert_eq!(ran.rows(), 0);
    assert_eq!(ran.logs, ["wrote 0", "wrote 1"]);
}

#[test]
fn a_file_from_another_thread_is_delivered() {
    let ran = Run::new("x").param("how", "thread").go::<Files>().unwrap();
    assert_eq!(ran.files.len(), 1);
}

#[test]
fn a_missing_file_is_refused() {
    let e = Run::new("x").param("how", "missing").go::<Files>().unwrap_err();
    assert!(e.contains("nope.parquet, which doesn't exist"), "{e}");
}

/// Fails in its stream with a plain message.
struct FailsInStream;

impl Worker for FailsInStream {
    const NAME: &'static str = "fails-later";
    const VERSION: &'static str = "1";

    fn run(_ctx: &RunContext, _input: Option<Input>) -> Result<Output> {
        let it = (0..2).map(|i| {
            if i == 1 {
                return Err("the source went away".into());
            }
            Ok(batch(0, 1))
        });
        Ok(Output::from_iter(schema(), it))
    }
}

#[test]
fn a_stream_error_is_the_workers_message() {
    // What morca's get_last_error gets: no "External error: " in front.
    let e = Run::new("x").go::<FailsInStream>().unwrap_err();
    assert_eq!(e, "the source went away");
}

#[test]
fn an_older_morca_has_no_output_dir() {
    // A context of ABI version 1 as first shipped: the stream still works...
    let ran = Run::new("x").without_files().go::<Counting>().unwrap();
    assert_eq!(ran.rows(), 30);
    // ...and files fail, saying why.
    let e = Run::new("x").without_files().go::<Files>().unwrap_err();
    assert!(e.contains("takes no Parquet files from workers (its context is 120 bytes"), "{e}");
}

#[test]
fn the_context_is_136_bytes() {
    assert_eq!(std::mem::size_of::<morca_worker::MorcaRunContext>() as u64, morca_worker::CONTEXT_SIZE_FILES);
}
