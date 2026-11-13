//! A morca step in Rust: `rows` numbers for the partition, `id`, its square
//! and the partition key, made `batch_rows` at a time as morca pulls them.
//! numbers.mojo is its proxy pipeline, and schemas/numbers.json the table's
//! schema, which both read.
//!
//! Parameters (a JSON object, from the proxy step):
//!
//! - `rows` (default 1000) and `batch_rows` (default 100);
//! - `sleep_ms`: a pause before each batch, logging `batch N`, so a test
//!   can stop the worker mid-stream;
//! - `panic`: `"run"` or `"batch"` panics there, as a test of the boundary;
//! - `mismatch`: `"type"`, `"extra"` or `"missing"` streams a schema that
//!   doesn't fit the table's, which morca refuses before writing.

use std::sync::Arc;
use std::time::Duration;

use morca_worker::arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use morca_worker::arrow_schema::{DataType, Field, Schema};
use morca_worker::{export_worker, schema, Input, Output, Result, RunContext, Worker};

/// The table's schema, the file numbers.mojo embeds too.
pub const SCHEMA: &str = include_str!("../schemas/numbers.json");

pub struct Numbers;

fn int_param(ctx: &RunContext, name: &str, default: i64) -> i64 {
    ctx.params()
        .get(name)
        .and_then(|v| v.as_i64())
        .unwrap_or(default)
}

/// The schema the worker streams: the table's, or a wrong one if asked.
fn output_schema(mismatch: &str) -> Result<Arc<Schema>> {
    let good = schema::to_arrow(SCHEMA)?;
    let mut fields: Vec<Field> = good.fields().iter().map(|f| f.as_ref().clone()).collect();
    match mismatch {
        "" => {}
        "type" => fields[2] = Field::new("square", DataType::Utf8, true),
        "extra" => fields.push(Field::new("cube", DataType::Float64, true)),
        "missing" => {
            fields.remove(1);
        }
        other => return Err(format!("unknown mismatch {other:?}").into()),
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn batch(schema: &Arc<Schema>, partition: &str, from: i64, n: i64) -> Result<RecordBatch> {
    let ids: Vec<i64> = (from..from + n).collect();
    let mut cols: Vec<ArrayRef> = Vec::new();
    for f in schema.fields() {
        let col: ArrayRef = match (f.name().as_str(), f.data_type()) {
            ("id", _) => Arc::new(Int64Array::from(ids.clone())),
            ("partition", _) => Arc::new(StringArray::from(vec![partition; ids.len()])),
            ("square", DataType::Utf8) => Arc::new(StringArray::from(
                ids.iter().map(|i| (i * i).to_string()).collect::<Vec<_>>(),
            )),
            (_, _) => Arc::new(Float64Array::from(
                ids.iter().map(|&i| (i * i) as f64).collect::<Vec<_>>(),
            )),
        };
        cols.push(col);
    }
    Ok(RecordBatch::try_new(schema.clone(), cols)?)
}

impl Worker for Numbers {
    const NAME: &'static str = "numbers";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    fn run(ctx: &RunContext, _input: Option<Input>) -> Result<Output> {
        let rows = int_param(ctx, "rows", 1000);
        let batch_rows = int_param(ctx, "batch_rows", 100).max(1);
        let sleep_ms = int_param(ctx, "sleep_ms", 0);
        let panic_at = ctx.param("panic").unwrap_or("").to_owned();
        let schema = output_schema(ctx.param("mismatch").unwrap_or(""))?;
        if panic_at == "run" {
            panic!("asked to panic in run");
        }
        ctx.log(format!(
            "numbers worker {}: {rows} rows for {}, attempt {}",
            Self::VERSION,
            ctx.partition(),
            ctx.attempt()
        ));
        let ctx = ctx.clone();
        let partition = ctx.partition().to_owned();
        let out_schema = schema.clone();
        let mut from = 0;
        let batches = std::iter::from_fn(move || {
            if from >= rows {
                return None;
            }
            if ctx.stopping() {
                ctx.log(format!("stopping after {from} rows, as morca asks"));
                return None;
            }
            if sleep_ms > 0 {
                ctx.log(format!("batch {}", from / batch_rows));
                std::thread::sleep(Duration::from_millis(sleep_ms as u64));
            }
            if panic_at == "batch" && from > 0 {
                panic!("asked to panic in batch {}", from / batch_rows);
            }
            let n = batch_rows.min(rows - from);
            let b = batch(&out_schema, &partition, from, n);
            from += n;
            ctx.progress(from as u64, rows as u64);
            Some(b)
        });
        Ok(Output::from_iter(schema, batches))
    }
}

export_worker!(Numbers);

#[cfg(test)]
mod tests {
    use super::*;
    use morca_worker::testing::Run;

    /// The schema the worker streams fits the table's, so drift fails here.
    #[test]
    fn schema_fits_the_table() {
        schema::assert_fits(&output_schema("").unwrap(), SCHEMA);
        for bad in ["type", "extra", "missing"] {
            assert!(schema::check(&output_schema(bad).unwrap(), SCHEMA).is_err());
        }
    }

    #[test]
    fn makes_its_rows() {
        let ran = Run::new("2024-01")
            .param("rows", 250)
            .param("batch_rows", 100)
            .go::<Numbers>()
            .unwrap();
        assert_eq!(ran.rows(), 250);
        assert_eq!(ran.batches.len(), 3);
        schema::assert_fits(&ran.schema, SCHEMA);
    }
}
