//! GFS files: where a cycle's files are, finding a field's message in a
//! file's `.idx`, fetching just that message, and decoding it.
//!
//! A GFS 1° file (`pgrb2.1p00`) is about 45 MB of GRIB2 messages, and its
//! `.idx` lists them one per line, `n:offset:d=YYYYMMDDHH:VAR:level:time:`.
//! A message ends where the next line's starts, so one field costs one
//! Range request of about 74 KB.

use std::io::Read;
use std::time::Duration;

use grib::LatLons;
use morca_worker::Result;

/// NOAA's open GFS bucket (NODD), read without credentials.
pub const NOAA: &str = "https://noaa-gfs-bdp-pds.s3.amazonaws.com";

/// The text an error starts with when a cycle's file isn't there (yet): the
/// proxy step defers on it rather than failing.
pub const NOT_PUBLISHED: &str = "not published yet";

/// A file's path under the source: `gfs.20261112/00/atmos/gfs.t00z.pgrb2.1p00.f024`.
pub fn file_path(cycle: &str, lead: u32) -> String {
    format!("gfs.{cycle}/00/atmos/gfs.t00z.pgrb2.1p00.f{lead:03}")
}

/// One message's place in its file: bytes `start..=end`, or to the end of
/// the file when it's the last.
#[derive(Debug, PartialEq)]
pub struct Span {
    pub start: u64,
    pub end: Option<u64>,
    /// The line, for errors and logs.
    pub line: String,
}

/// What an `.idx` line's time field is for `lead`: `anl` for the analysis,
/// `24 hour fcst` for f024.
fn time_field(lead: u32) -> String {
    if lead == 0 {
        "anl".into()
    } else {
        format!("{lead} hour fcst")
    }
}

/// Finds `var` at `level` (`TMP`, `2 m above ground`) for `lead` in an
/// `.idx`. Fields are matched whole: `APTMP:2 m above ground` is another
/// variable.
pub fn find(idx: &str, var: &str, level: &str, lead: u32) -> Result<Span> {
    let lines: Vec<&str> = idx.lines().filter(|l| !l.trim().is_empty()).collect();
    let want = time_field(lead);
    for (i, line) in lines.iter().enumerate() {
        let f: Vec<&str> = line.split(':').collect();
        if f.len() < 6 || f[3] != var || f[4] != level {
            continue;
        }
        if f[5] != want {
            return Err(format!("{var}:{level} is for '{}', not '{want}': {line}", f[5]).into());
        }
        let start: u64 = f[1]
            .parse()
            .map_err(|_| format!("bad offset in {line:?}"))?;
        let end = match lines.get(i + 1) {
            Some(next) => {
                let n: u64 = next
                    .split(':')
                    .nth(1)
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| format!("bad offset in {next:?}"))?;
                if n <= start {
                    return Err(format!("offsets out of order at {line:?}").into());
                }
                Some(n - 1)
            }
            None => None,
        };
        return Ok(Span {
            start,
            end,
            line: line.to_string(),
        });
    }
    Err(format!("no {var}:{level} in the index").into())
}

/// Where the files come from: NOAA's bucket or another base URL, or a local
/// directory laid out the same way (the test fixtures).
pub enum Source {
    Http { base: String, agent: ureq::Agent },
    Dir(std::path::PathBuf),
}

impl Source {
    pub fn new(s: &str) -> Source {
        if s.starts_with("http://") || s.starts_with("https://") {
            let agent = ureq::Agent::config_builder()
                .timeout_global(Some(Duration::from_secs(120)))
                .http_status_as_error(false)
                .build()
                .into();
            Source::Http {
                base: s.trim_end_matches('/').to_string(),
                agent,
            }
        } else {
            Source::Dir(s.strip_prefix("file://").unwrap_or(s).into())
        }
    }

    pub fn describe(&self, path: &str) -> String {
        match self {
            Source::Http { base, .. } => format!("{base}/{path}"),
            Source::Dir(d) => d.join(path).display().to_string(),
        }
    }

    /// The whole of a (small) file: an `.idx`.
    pub fn text(&self, path: &str) -> Result<String> {
        let bytes = self.get(path, None)?;
        String::from_utf8(bytes).map_err(|_| format!("{} isn't text", self.describe(path)).into())
    }

    /// `span`'s bytes of a file.
    pub fn range(&self, path: &str, span: &Span) -> Result<Vec<u8>> {
        self.get(path, Some((span.start, span.end)))
    }

    fn get(&self, path: &str, range: Option<(u64, Option<u64>)>) -> Result<Vec<u8>> {
        let what = self.describe(path);
        match self {
            Source::Http { agent, .. } => {
                let mut req = agent.get(&what);
                if let Some((s, e)) = range {
                    let e = e.map(|e| e.to_string()).unwrap_or_default();
                    req = req.header("Range", format!("bytes={s}-{e}"));
                }
                let mut resp = req.call().map_err(|e| format!("{what}: {e}"))?;
                let status = resp.status().as_u16();
                // S3 says 403 for a missing key in a bucket that can't be
                // listed, and 404 in one that can.
                if status == 404 || status == 403 {
                    return Err(format!("{NOT_PUBLISHED}: {what} ({status})").into());
                }
                if range.is_some() && status != 206 {
                    return Err(
                        format!("{what}: a Range request got HTTP {status}, not 206").into(),
                    );
                }
                if range.is_none() && status != 200 {
                    return Err(format!("{what}: HTTP {status}").into());
                }
                let mut out = Vec::new();
                resp.body_mut()
                    .as_reader()
                    .read_to_end(&mut out)
                    .map_err(|e| format!("{what}: {e}"))?;
                if let Some((s, Some(e))) = range {
                    if out.len() as u64 != e - s + 1 {
                        return Err(
                            format!("{what}: got {} bytes, not {}", out.len(), e - s + 1).into(),
                        );
                    }
                }
                Ok(out)
            }
            Source::Dir(_) => {
                let mut f = match std::fs::File::open(&what) {
                    Ok(f) => f,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Err(format!("{NOT_PUBLISHED}: {what}").into())
                    }
                    Err(e) => return Err(format!("{what}: {e}").into()),
                };
                let mut all = Vec::new();
                f.read_to_end(&mut all)
                    .map_err(|e| format!("{what}: {e}"))?;
                Ok(match range {
                    None => all,
                    Some((s, e)) => {
                        let s = s as usize;
                        let e = e.map(|e| e as usize + 1).unwrap_or(all.len());
                        if s > e || e > all.len() {
                            return Err(format!("{what}: bytes {s}..{e} past its end").into());
                        }
                        all[s..e].to_vec()
                    }
                })
            }
        }
    }
}

/// A decoded field on the GFS 1° grid: 360 × 181 points, a value each.
pub struct Field {
    pub lat: Vec<f32>,
    /// From -180 to 180 (GRIB has 0 to 359).
    pub lon: Vec<f32>,
    pub values: Vec<f32>,
}

/// The GFS 1° grid's shape, (ni, nj).
pub const GRID: (usize, usize) = (360, 181);

/// Decodes the one GRIB2 message in `bytes`.
pub fn decode(bytes: Vec<u8>) -> Result<Field> {
    let grib2 = grib::from_bytes(bytes).map_err(|e| format!("not GRIB2: {e}"))?;
    let (_, sub) = grib2.iter().next().ok_or("no message in the GRIB2 bytes")?;
    let shape = sub.grid_shape().map_err(|e| format!("grid: {e}"))?;
    if shape != GRID {
        return Err(format!("a {shape:?} grid, not GFS 1°'s {GRID:?}").into());
    }
    let (mut lat, mut lon) = (Vec::with_capacity(65160), Vec::with_capacity(65160));
    for (la, lo) in sub.latlons().map_err(|e| format!("latlons: {e}"))? {
        lat.push(la as f32);
        lon.push(if lo >= 180.0 { lo - 360.0 } else { lo } as f32);
    }
    let decoder = grib::Grib2SubmessageDecoder::from(sub).map_err(|e| format!("decoder: {e}"))?;
    let values: Vec<f32> = decoder
        .dispatch()
        .map_err(|e| format!("decode: {e}"))?
        .collect();
    if values.len() != lat.len() {
        return Err(format!("{} values for {} points", values.len(), lat.len()).into());
    }
    Ok(Field { lat, lon, values })
}

/// Days from 1970-01-01 of `YYYY-MM-DD` (Iceberg's `date`), or of
/// `YYYYMMDD`.
pub fn days(date: &str) -> Result<i32> {
    let d: String = date.chars().filter(|c| *c != '-').collect();
    let bad = || format!("not a date: {date:?}");
    if d.len() != 8 {
        return Err(bad().into());
    }
    let y: i64 = d[0..4].parse().map_err(|_| bad())?;
    let m: i64 = d[4..6].parse().map_err(|_| bad())?;
    let dd: i64 = d[6..8].parse().map_err(|_| bad())?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&dd) {
        return Err(bad().into());
    }
    // Howard Hinnant's days_from_civil.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + dd - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Ok((era * 146097 + doe - 719468) as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    const IDX: &str = "\
579:34100000:d=2026111200:DPT:2 m above ground:anl:
580:34246228:d=2026111200:TMP:2 m above ground:anl:
581:34320096:d=2026111200:SPFH:2 m above ground:anl:
584:34556889:d=2026111200:APTMP:2 m above ground:anl:
585:34607957:d=2026111200:UGRD:10 m above ground:anl:
";

    #[test]
    fn finds_the_message_and_ends_it_at_the_next() {
        let s = find(IDX, "TMP", "2 m above ground", 0).unwrap();
        assert_eq!((s.start, s.end), (34246228, Some(34320095)));
        // APTMP isn't TMP; the last line runs to the end of the file.
        let s = find(IDX, "UGRD", "10 m above ground", 0).unwrap();
        assert_eq!((s.start, s.end), (34607957, None));
        let only_ap = IDX.replace(":TMP:", ":XXX:");
        assert!(find(&only_ap, "TMP", "2 m above ground", 0).is_err());
    }

    #[test]
    fn refuses_another_lead() {
        let e = find(IDX, "TMP", "2 m above ground", 24)
            .unwrap_err()
            .to_string();
        assert!(e.contains("not '24 hour fcst'"), "{e}");
    }

    #[test]
    fn dates() {
        assert_eq!(days("1970-01-01").unwrap(), 0);
        assert_eq!(days("2026-11-12").unwrap(), 20769);
        assert_eq!(days("20000301").unwrap(), 11017);
        assert!(days("2026-13-01").is_err());
    }
}
