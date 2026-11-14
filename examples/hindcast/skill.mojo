"""Forecast skill on the GFS 1° grid: Hindcast's scoring, apart from the
pipeline so it reads on its own.

A forecast of cycle C at lead L is valid at V = C + L. Its *analysis*, the
truth it's scored against, is the f000 of cycle V. For each (C, L):

- **bias**, **MAE** and **RMSE** of forecast minus analysis over the whole
  grid, each point weighted by cos(latitude), the area it stands for on a
  1° grid (a row near a pole covers far less of the Earth than one at the
  equator);
- a **city scorecard**: the forecast, the analysis and the error at the
  grid point nearest each of `CITIES`.

Values are in kelvin, so an error is one in °C too.
"""

from std.math import cos, floor, pi, sqrt

from parquet import RecordBatch

from morca.civil import Civil
from morca.tables import (
    BatchBuilder,
    PartitionField,
    PartitionSpec,
    Schema,
    TableScan,
    parse_transform,
)

comptime NI = 360
"""Longitudes, 0°E to 359°E."""
comptime NJ = 181
"""Latitudes, 90°N to 90°S."""
comptime POINTS = NI * NJ

comptime SKILL_SCHEMA = """{"type": "struct", "schema-id": 0, "fields": [
  {"id": 1, "name": "valid_date", "required": true, "type": "date"},
  {"id": 2, "name": "cycle", "required": true, "type": "date"},
  {"id": 3, "name": "lead_hours", "required": true, "type": "int"},
  {"id": 4, "name": "points", "required": true, "type": "long"},
  {"id": 5, "name": "bias_k", "required": true, "type": "double"},
  {"id": 6, "name": "mae_k", "required": true, "type": "double"},
  {"id": 7, "name": "rmse_k", "required": true, "type": "double"}]}"""
"""`hindcast.skill`: one row per forecast (cycle, lead), grid-wide."""

comptime CITIES_SCHEMA = """{"type": "struct", "schema-id": 0, "fields": [
  {"id": 1, "name": "valid_date", "required": true, "type": "date"},
  {"id": 2, "name": "cycle", "required": true, "type": "date"},
  {"id": 3, "name": "lead_hours", "required": true, "type": "int"},
  {"id": 4, "name": "city", "required": true, "type": "string"},
  {"id": 5, "name": "lat", "required": true, "type": "float"},
  {"id": 6, "name": "lon", "required": true, "type": "float"},
  {"id": 7, "name": "forecast_k", "required": true, "type": "double"},
  {"id": 8, "name": "analysis_k", "required": true, "type": "double"},
  {"id": 9, "name": "error_k", "required": true, "type": "double"}]}"""
"""`hindcast.cities`: one row per forecast (cycle, lead) and city, at the
grid point nearest the city (its `lat` and `lon`)."""


def date_text(day: Int64) -> String:
    """`YYYY-MM-DD` of a day number (days from 1970-01-01)."""
    var c = Civil.of(day * 86400)
    var out = String(c.year, "-")
    if c.month < 10:
        out += "0"
    out += String(c.month, "-")
    if c.day < 10:
        out += "0"
    return out + String(c.day)


def by_valid_date() raises -> PartitionSpec:
    """The scores' partitions: a day's analysis scores the forecasts valid
    that day, and the step replaces the day's partition whole."""
    return PartitionSpec(
        0,
        [
            PartitionField.single(
                1, 1000, "valid_date", parse_transform("identity")
            )
        ],
    )


@fieldwise_init
struct City(Copyable, Movable):
    var name: String
    var lat: Float64
    var lon: Float64


def cities() -> List[City]:
    """The scorecard's cities, by where they are, not their grid point."""
    return [
        City("New York", 40.71, -74.01),
        City("London", 51.51, -0.13),
        City("Tokyo", 35.68, 139.69),
        City("Sydney", -33.87, 151.21),
        City("Sao Paulo", -23.55, -46.63),
        City("Cairo", 30.04, 31.24),
        City("Mumbai", 19.08, 72.88),
        City("Moscow", 55.76, 37.62),
        City("Lagos", 6.52, 3.38),
        City("Anchorage", 61.22, -149.90),
    ]


def point_of(lat: Float64, lon: Float64) -> Int:
    """The index of the grid point nearest (lat, lon): rows from 90°N, each
    from 0°E eastwards, as GRIB lays the GFS grid out."""
    var j = Int(floor(90.0 - lat + 0.5))
    var i = Int(floor(lon + 0.5)) % NI
    if i < 0:
        i += NI
    return min(max(j, 0), NJ - 1) * NI + i


def lat_of(p: Int) -> Float64:
    return 90.0 - Float64(p // NI)


def lon_of(p: Int) -> Float64:
    """From -180 to 179, as the table has it."""
    var lon = p % NI
    return Float64(lon - NI if lon >= 180 else lon)


struct Field(Movable):
    """One (cycle, lead) of `hindcast.t2m`: a value per grid point, or
    `Float64.MAX` where the table has none."""

    var t: List[Float64]
    var n: Int
    """The points that have a value."""

    def __init__(out self):
        self.t = List[Float64](length=POINTS, fill=Float64.MAX)
        self.n = 0

    def has(self, p: Int) -> Bool:
        return self.t[p] != Float64.MAX


def _col(b: RecordBatch, name: String) raises -> Int:
    for i in range(b.num_columns()):
        if b.name(i) == name:
            return i
    raise Error("hindcast: no column ", name)


def read_field(s: TableScan, cycle_day: Int64, lead: Int) raises -> Field:
    """`(cycle, lead)` from a scan of `hindcast.t2m`. The table is
    partitioned by both, so the filter reads that one file; rows are
    checked again in case a file holds more."""
    var c = String('"', date_text(cycle_day), '"')
    var q = s.filter(
        String(
            '["and", ["=", "cycle", ', c, '], ["=", "lead_hours", ', lead, "]]"
        )
    ).select(["cycle", "lead_hours", "lat", "lon", "t2m_k"])
    var f = Field()
    for b in q.to_batches():
        var cy = b.column_i64(_col(b, "cycle"))[0].copy()
        var ld = b.column_i64(_col(b, "lead_hours"))[0].copy()
        var la = b.column_f64(_col(b, "lat"))[0].copy()
        var lo = b.column_f64(_col(b, "lon"))[0].copy()
        var t = b.column_f64(_col(b, "t2m_k"))[0].copy()
        for r in range(b.num_rows):
            if cy[r] != cycle_day or ld[r] != Int64(lead):
                continue
            var p = point_of(la[r], lo[r])
            if lat_of(p) != la[r] or lon_of(p) != lo[r]:
                raise Error(
                    "hindcast: (", la[r], ", ", lo[r], ") isn't on the 1° grid"
                )
            if t[r] != t[r]:  # NaN: no value
                continue
            if not f.has(p):
                f.n += 1
            f.t[p] = t[r]
    return f^


@fieldwise_init
struct Skill(Copyable, Movable, Writable):
    var points: Int
    var bias: Float64
    var mae: Float64
    var rmse: Float64


def score(forecast: Field, analysis: Field) raises -> Skill:
    """Bias, MAE and RMSE of `forecast - analysis` over the points both
    have, each weighted by cos(latitude)."""
    var sw = 0.0
    var se = 0.0
    var sa = 0.0
    var s2 = 0.0
    var n = 0
    for p in range(POINTS):
        if not forecast.has(p) or not analysis.has(p):
            continue
        var w = cos(lat_of(p) * pi / 180.0)
        var e = forecast.t[p] - analysis.t[p]
        sw += w
        se += w * e
        sa += w * abs(e)
        s2 += w * e * e
        n += 1
    if n == 0 or sw <= 0.0:
        raise Error("hindcast: the forecast and the analysis share no points")
    return Skill(n, se / sw, sa / sw, sqrt(s2 / sw))


struct Scores(Movable):
    """The rows the scoring step writes, gathered before one commit each."""

    var skill: BatchBuilder
    var city: BatchBuilder
    var sk: List[Int]
    """`skill`'s columns, in the schema's order."""
    var ci: List[Int]
    """`city`'s columns, in the schema's order."""
    var rows: Int

    def __init__(out self) raises:
        self.skill = BatchBuilder(Schema.parse(SKILL_SCHEMA))
        self.city = BatchBuilder(Schema.parse(CITIES_SCHEMA))
        self.sk = self.skill.roots.copy()
        self.ci = self.city.roots.copy()
        self.rows = 0

    def add(
        mut self,
        valid_day: Int64,
        cycle_day: Int64,
        lead: Int,
        forecast: Field,
        analysis: Field,
    ) raises -> Skill:
        var s = score(forecast, analysis)
        self.skill.add_int(self.sk[0], valid_day)
        self.skill.add_int(self.sk[1], cycle_day)
        self.skill.add_int(self.sk[2], Int64(lead))
        self.skill.add_int(self.sk[3], Int64(s.points))
        self.skill.add_float(self.sk[4], s.bias)
        self.skill.add_float(self.sk[5], s.mae)
        self.skill.add_float(self.sk[6], s.rmse)
        self.rows += 1
        for c in cities():
            var p = point_of(c.lat, c.lon)
            if not forecast.has(p) or not analysis.has(p):
                continue
            self.city.add_int(self.ci[0], valid_day)
            self.city.add_int(self.ci[1], cycle_day)
            self.city.add_int(self.ci[2], Int64(lead))
            self.city.add_str(self.ci[3], c.name)
            self.city.add_float(self.ci[4], lat_of(p))
            self.city.add_float(self.ci[5], lon_of(p))
            self.city.add_float(self.ci[6], forecast.t[p])
            self.city.add_float(self.ci[7], analysis.t[p])
            self.city.add_float(self.ci[8], forecast.t[p] - analysis.t[p])
        return s^

    def skill_batches(mut self) raises -> List[RecordBatch]:
        return [self.skill.batch()]

    def city_batches(mut self) raises -> List[RecordBatch]:
        return [self.city.batch()]
