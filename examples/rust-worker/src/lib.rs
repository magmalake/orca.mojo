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
//! - `panic`: `"run"` or `"batch"` panics there, as a test of the boundary,
//!   and `fail` returns an error from the second batch;
//! - `mismatch`: `"type"`, `"extra"` or `"missing"` streams a schema that
//!   doesn't fit the table's, which morca refuses before writing;
//! - `mode`: `"stream"` (the default) streams the rows; `"files"` writes
//!   them as Parquet files of `file_rows` rows (default 250) itself, one
//!   file each time morca asks for the next (`Output::from_files`); `"mix"`
//!   writes the first half to a file in `run` (`ctx.add_file`) and streams
//!   the rest;
//! - `file_mismatch`, with `"files"`: `"type"` writes `square` as a string,
//!   `"partition"` writes a file holding two partitions, and `"missing"`
//!   reports a file that isn't there, which morca all refuse.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use morca_worker::arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use morca_worker::arrow_schema::{DataType, Field, Schema};
use morca_worker::{export_worker, schema, Input, Output, Result, RunContext, Worker};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

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
        "type" => {
            // The table's field id kept: only the type is wrong.
            let id = fields[2].metadata().clone();
            fields[2] = Field::new("square", DataType::Utf8, true).with_metadata(id)
        }
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

/// Writes `batch` as one Parquet file in the output directory.
fn write_file(ctx: &RunContext, name: &str, batch: &RecordBatch) -> Result<PathBuf> {
    let path = ctx.output_dir()?.join(name);
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut w = ArrowWriter::try_new(File::create(&path)?, batch.schema(), Some(props))?;
    w.write(batch)?;
    w.close()?;
    Ok(path)
}

/// The files mode: `rows` rows in files of `file_rows`, each written when
/// morca asks for the next.
fn files(ctx: &RunContext, rows: i64, file_rows: i64, mismatch: &str) -> Result<Output> {
    let schema = output_schema(if mismatch == "type" { "type" } else { "" })?;
    let partition = ctx.partition().to_owned();
    let (c, mismatch) = (ctx.clone(), mismatch.to_owned());
    let mut from = 0;
    let it = std::iter::from_fn(move || {
        if from >= rows || c.stopping() {
            return None;
        }
        let n = file_rows.min(rows - from);
        let i = from / file_rows;
        from += n;
        Some((|| {
            if mismatch == "missing" {
                return Ok(c.output_dir()?.join("not-written.parquet"));
            }
            let mut b = batch(&schema, &partition, from - n, n)?;
            if mismatch == "partition" {
                // The second half of the file in another partition.
                let mut keys = vec![partition.as_str(); n as usize];
                let other = format!("{partition}-other");
                for k in keys.iter_mut().skip(n as usize / 2) {
                    *k = &other;
                }
                let mut cols = b.columns().to_vec();
                cols[1] = Arc::new(StringArray::from(keys));
                b = RecordBatch::try_new(schema.clone(), cols)?;
            }
            let path = write_file(&c, &format!("numbers-{i:05}.parquet"), &b)?;
            c.log(format!("wrote file {i}"));
            Ok(path)
        })())
    });
    Ok(Output::from_files(ctx, it))
}

impl Worker for Numbers {
    const NAME: &'static str = "numbers";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    fn run(ctx: &RunContext, _input: Option<Input>) -> Result<Output> {
        let rows = int_param(ctx, "rows", 1000);
        let batch_rows = int_param(ctx, "batch_rows", 100).max(1);
        let sleep_ms = int_param(ctx, "sleep_ms", 0);
        let panic_at = ctx.param("panic").unwrap_or("").to_owned();
        let fail = ctx.params().get("fail").is_some();
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
        let mut from = 0;
        match ctx.param("mode").unwrap_or("stream") {
            "stream" => {}
            "files" => {
                let file_rows = int_param(ctx, "file_rows", 250).max(1);
                return files(ctx, rows, file_rows, ctx.param("file_mismatch").unwrap_or(""));
            }
            "mix" => {
                // Half in a file of the worker's own, reported now; the
                // rest streamed. Both land in the step's one commit.
                let half = rows / 2;
                let b = batch(&schema, ctx.partition(), 0, half)?;
                ctx.add_file(write_file(ctx, "first-half.parquet", &b)?)?;
                from = half;
            }
            other => return Err(format!("unknown mode {other:?}").into()),
        }
        let ctx = ctx.clone();
        let partition = ctx.partition().to_owned();
        let out_schema = schema.clone();
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
            if fail && from > 0 {
                return Some(Err("asked to fail in the stream".into()));
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

    /// The files mode writes Parquet files whose schema fits the table's,
    /// field ids included.
    #[test]
    fn writes_its_files() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let ran = Run::new("2024-01")
            .param("rows", 600)
            .param("mode", "files")
            .go::<Numbers>()
            .unwrap();
        assert_eq!(ran.files.len(), 3);
        let mut rows = 0;
        for f in &ran.files {
            let r = ParquetRecordBatchReaderBuilder::try_new(File::open(f).unwrap()).unwrap();
            schema::assert_fits(r.schema(), SCHEMA);
            let ids: Vec<_> = r
                .parquet_schema()
                .root_schema()
                .get_fields()
                .iter()
                .map(|f| f.get_basic_info().id())
                .collect();
            assert_eq!(ids, [1, 2, 3]);
            rows += r.metadata().file_metadata().num_rows();
        }
        assert_eq!(rows, 600);
    }
}
