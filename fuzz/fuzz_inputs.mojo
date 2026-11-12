"""Fuzz: a step's input decoded from arbitrary bytes, as resume reads it
back from Postgres (`steps.input`, `steps.output`).

Malformed bytes must be rejected, never read out of bounds. Bytes that are
accepted must be exactly one canonical encoding: the step below returns its
input, and its encoded output has to equal the bytes it was given, so no
input decodes with bytes left over or two encodings decode alike.

    pixi run -e fuzz fuzz-inputs
"""

from harness import check, only_findings, run_harness, save_last

from dista import Seq, encode
from morca import Ctx, Mapped, Month, Step, StepLike


@fieldwise_init
struct RawFile(Copyable, Movable):
    var month: Month
    var path: String
    var bytes: Int64


@fieldwise_init
struct Loaded(Copyable, Movable):
    var month: Month
    var snapshot_id: Int64
    var files: Seq[String]


def same_month(mut ctx: Ctx, x: Month) raises -> Month:
    return x.copy()


def same_raw(mut ctx: Ctx, x: RawFile) raises -> RawFile:
    return x.copy()


def same_loaded(mut ctx: Ctx, x: Loaded) raises -> Loaded:
    return x.copy()


def same_seq(mut ctx: Ctx, x: Seq[Loaded]) raises -> Seq[Loaded]:
    return x.copy()


def same_text(mut ctx: Ctx, x: String) raises -> String:
    return x


def _round_trip[T: StepLike](body: Span[UInt8, _]) raises:
    var ctx = Ctx("fuzz", 0, "s", "")
    var out = List[UInt8]()
    try:
        T.run(ctx, body, out)
    except e:
        only_findings(e)
        return
    check(len(out) == len(body), "accepted bytes aren't canonical (length)")
    for i in range(len(out)):
        check(out[i] == body[i], "accepted bytes aren't canonical")


def target(data: List[UInt8]) raises:
    save_last("inputs", data)
    if len(data) == 0:
        return
    var body = Span(data)[1:]
    var kind = data[0] % 5
    if kind == 0:
        _round_trip[Step["m", Month, Month, same_month, Mapped]](body)
    elif kind == 1:
        _round_trip[Step["r", RawFile, RawFile, same_raw, Mapped]](body)
    elif kind == 2:
        _round_trip[Step["l", Loaded, Loaded, same_loaded, Mapped]](body)
    elif kind == 3:
        _round_trip[Step["q", Seq[Loaded], Seq[Loaded], same_seq, Mapped]](body)
    else:
        _round_trip[Step["t", String, String, same_text, Mapped]](body)


def _seed(kind: UInt8, var body: List[UInt8]) -> List[UInt8]:
    var out: List[UInt8] = [kind]
    out.extend(body^)
    return out^


def main() raises:
    var seeds = List[List[UInt8]]()
    var b = List[UInt8]()
    encode(Month(2024, 2), b)
    seeds.append(_seed(0, b^))
    b = List[UInt8]()
    encode(RawFile(Month(2024, 2), "s3://tlc/2024-02.parquet", 49_000_000), b)
    seeds.append(_seed(1, b^))
    var loaded = Loaded(
        Month(2024, 2), 123456789, Seq[String]([String("a"), String("bc")])
    )
    b = List[UInt8]()
    encode(loaded, b)
    seeds.append(_seed(2, b^))
    b = List[UInt8]()
    encode(Seq[Loaded]([loaded.copy(), loaded.copy()]), b)
    seeds.append(_seed(3, b^))
    b = List[UInt8]()
    encode(String("héllo"), b)
    seeds.append(_seed(4, b^))
    run_harness[target]("inputs", seeds, max_len=2048)
