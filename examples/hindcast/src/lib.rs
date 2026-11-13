//! Hindcast's Rust worker: one GFS forecast cycle's 2 m temperature, every
//! lead, into `hindcast.t2m`. hindcast.mojo is its proxy pipeline, and
//! schemas/t2m.json the table's schema, which both read.
//!
//! For each lead it reads the file's `.idx`, fetches the one
//! `TMP:2 m above ground` message with a Range request, decodes it with the
//! `grib` crate and streams it as one batch of 65,160 rows (cycle,
//! lead_hours, valid_time, lat, lon, t2m_k). A batch is made when morca
//! pulls it, so the worker holds one lead at a time.
//!
//! Parameters (a JSON object, from the proxy step):
//!
//! - `cycle`: the 00z run's date, `YYYY-MM-DD` (default the partition);
//! - `leads`: the lead times in hours (default 0, 24, ..., 168);
//! - `source`: a base URL or a directory laid out as NOAA's bucket (default
//!   <https://noaa-gfs-bdp-pds.s3.amazonaws.com>); the tests use
//!   fixtures/.
//!
//! A file that isn't there fails with an error starting
//! "not published yet", which the proxy turns into a `defer`.

pub mod gfs;

use std::sync::Arc;

use morca_worker::arrow_array::{
    ArrayRef, Date32Array, Float32Array, Int32Array, RecordBatch, TimestampMicrosecondArray,
};
use morca_worker::arrow_schema::Schema;
use morca_worker::{export_worker, schema, Input, Output, Result, RunContext, Worker};

/// The table's schema, the file hindcast.mojo embeds too.
pub const SCHEMA: &str = include_str!("../schemas/t2m.json");

/// The field Hindcast scores.
pub const VAR: &str = "TMP";
pub const LEVEL: &str = "2 m above ground";

/// 0 to 7 days, a day apart.
pub const LEADS: [u32; 8] = [0, 24, 48, 72, 96, 120, 144, 168];

pub struct Hindcast;

/// One lead of `cycle` as a batch.
pub fn lead_batch(
    schema: &Arc<Schema>,
    source: &gfs::Source,
    cycle: &str,
    lead: u32,
) -> Result<(RecordBatch, String)> {
    let compact: String = cycle.chars().filter(|c| *c != '-').collect();
    let day = gfs::days(cycle)?;
    let path = gfs::file_path(&compact, lead);
    let idx = source.text(&format!("{path}.idx"))?;
    let span = gfs::find(&idx, VAR, LEVEL, lead)
        .map_err(|e| format!("{}.idx: {e}", source.describe(&path)))?;
    let bytes = source.range(&path, &span)?;
    let n_bytes = bytes.len();
    let field = gfs::decode(bytes).map_err(|e| format!("{}: {e}", source.describe(&path)))?;
    let n = field.values.len();
    let valid_us = (day as i64 * 86_400 + lead as i64 * 3_600) * 1_000_000;
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Date32Array::from(vec![day; n])),
        Arc::new(Int32Array::from(vec![lead as i32; n])),
        Arc::new(TimestampMicrosecondArray::from(vec![valid_us; n]).with_timezone("UTC")),
        Arc::new(Float32Array::from(field.lat)),
        Arc::new(Float32Array::from(field.lon)),
        Arc::new(Float32Array::from(field.values)),
    ];
    let b = RecordBatch::try_new(schema.clone(), cols)?;
    Ok((
        b,
        format!("f{lead:03}: {n_bytes} bytes at {}, {n} points", span.start),
    ))
}

fn leads(ctx: &RunContext) -> Result<Vec<u32>> {
    match ctx.params().get("leads") {
        None => Ok(LEADS.to_vec()),
        Some(v) => v
            .as_array()
            .ok_or("leads: not a list")?
            .iter()
            .map(|x| {
                x.as_u64()
                    .filter(|h| *h <= 384)
                    .map(|h| h as u32)
                    .ok_or_else(|| format!("leads: {x} isn't hours from 0 to 384").into())
            })
            .collect(),
    }
}

impl Worker for Hindcast {
    const NAME: &'static str = "hindcast";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    fn run(ctx: &RunContext, _input: Option<Input>) -> Result<Output> {
        let schema = Arc::new(schema::to_arrow(SCHEMA)?);
        let cycle = ctx.param("cycle").unwrap_or(ctx.partition()).to_owned();
        gfs::days(&cycle)?;
        let leads = leads(ctx)?;
        let source = gfs::Source::new(ctx.param("source").unwrap_or(gfs::NOAA));
        ctx.log(format!(
            "hindcast worker {}: GFS {cycle} 00z, {VAR}:{LEVEL}, {} leads from {}",
            Self::VERSION,
            leads.len(),
            source.describe("")
        ));
        let (ctx, s) = (ctx.clone(), schema.clone());
        let total = leads.len() as u64;
        let mut todo = leads.into_iter().enumerate();
        let batches = std::iter::from_fn(move || {
            let (i, lead) = todo.next()?;
            if ctx.stopping() {
                ctx.log(format!("stopping before f{lead:03}, as morca asks"));
                return None;
            }
            let got = lead_batch(&s, &source, &cycle, lead).map(|(b, what)| {
                ctx.log(what);
                b
            });
            ctx.progress(i as u64 + 1, total);
            Some(got)
        });
        Ok(Output::from_iter(schema, batches))
    }
}

export_worker!(Hindcast);

#[cfg(test)]
mod tests {
    use super::*;
    use morca_worker::arrow_array::cast::AsArray;
    use morca_worker::arrow_array::types::Float32Type;
    use morca_worker::testing::Run;

    fn fixtures() -> String {
        concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures").to_string()
    }

    #[test]
    fn schema_fits_the_table() {
        schema::assert_fits(&schema::to_arrow(SCHEMA).unwrap(), SCHEMA);
    }

    /// The fixtures decode to the whole grid, with temperatures on Earth,
    /// and values ecCodes gives (`grib_get_data`, once, by hand).
    #[test]
    fn decodes_the_fixtures() {
        let ran = Run::new("2026-11-12")
            .param("leads", morca_worker::serde_json::json!([0, 24]))
            .param("source", fixtures())
            .go::<Hindcast>()
            .unwrap();
        assert_eq!(ran.rows(), 2 * 65160);
        schema::assert_fits(&ran.schema, SCHEMA);
        let b = &ran.batches[0];
        let t = b.column(5).as_primitive::<Float32Type>();
        let (lo, hi) = t
            .values()
            .iter()
            .fold((f32::MAX, f32::MIN), |(a, z), &v| (a.min(v), z.max(v)));
        assert!(lo > 190.0 && hi < 330.0, "t2m from {lo} to {hi}");
        // The first point is the North Pole, the last the South Pole, and
        // point 32400 is (0°N, 0°E).
        let lat = b.column(3).as_primitive::<Float32Type>();
        let lon = b.column(4).as_primitive::<Float32Type>();
        assert_eq!((lat.value(0), lon.value(0)), (90.0, 0.0));
        assert_eq!((lat.value(65159), lon.value(65159)), (-90.0, -1.0));
        assert_eq!((lat.value(32400), lon.value(32400)), (0.0, 0.0));
        assert_eq!((lat.value(32580), lon.value(32580)), (0.0, -180.0));
        for (i, want) in [(0, 258.596992), (32400, 298.966992), (65159, 234.356992)] {
            assert!(
                (t.value(i) - want).abs() < 1e-3,
                "point {i}: {} not {want}",
                t.value(i)
            );
        }
        let t24 = ran.batches[1].column(5).as_primitive::<Float32Type>();
        assert!(
            (t24.value(32400) - 299.141563).abs() < 1e-3,
            "{}",
            t24.value(32400)
        );
        assert!(ran
            .logs
            .iter()
            .any(|l| l.contains("f024: 74088 bytes at 0, 65160 points")));
    }

    #[test]
    fn a_missing_file_is_not_published() {
        let e = Run::new("2026-11-14")
            .param("source", fixtures())
            .go::<Hindcast>()
            .unwrap_err();
        assert!(e.contains(gfs::NOT_PUBLISHED), "{e}");
    }

    /// The live bucket: `cargo test -- --ignored` (not in CI, which is
    /// offline). The Range request gets what the fixture holds.
    #[test]
    #[ignore]
    fn live_matches_the_fixture() {
        let live = Run::new("2026-11-12")
            .param("leads", morca_worker::serde_json::json!([24]))
            .go::<Hindcast>()
            .unwrap();
        let fixed = Run::new("2026-11-12")
            .param("leads", morca_worker::serde_json::json!([24]))
            .param("source", fixtures())
            .go::<Hindcast>()
            .unwrap();
        assert_eq!(live.batches, fixed.batches);
        assert!(
            live.logs
                .iter()
                .any(|l| l.contains("f024: 74088 bytes at 34478918")),
            "{:?}",
            live.logs
        );
    }
}
