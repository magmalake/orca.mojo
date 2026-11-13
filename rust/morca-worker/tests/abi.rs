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
