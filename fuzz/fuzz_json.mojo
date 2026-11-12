"""Fuzz: `quote`, which writes every string in `describe`'s JSON.

Its output must be one JSON string literal: quotes at both ends, no raw
control character, only JSON's escapes, and unescaped it gives back the input.

    pixi run -e fuzz fuzz-json
"""

from harness import check, run_harness, save_last

from morca.json import quote


def _hex(b: UInt8) raises -> Int:
    if b >= UInt8(ord("0")) and b <= UInt8(ord("9")):
        return Int(b) - ord("0")
    if b >= UInt8(ord("a")) and b <= UInt8(ord("f")):
        return Int(b) - ord("a") + 10
    raise Error("assertion failed: bad hex digit in \\u escape")


def unquote(q: String) raises -> List[UInt8]:
    """JSON's string unescaping, for what `quote` can write."""
    var b = q.as_bytes()
    check(len(b) >= 2, "shorter than two quotes")
    check(
        b[0] == UInt8(ord('"')) and b[len(b) - 1] == UInt8(ord('"')),
        "not quoted",
    )
    var out = List[UInt8]()
    var i = 1
    while i < len(b) - 1:
        var c = b[i]
        check(c >= UInt8(0x20), "a raw control character")
        check(c != UInt8(ord('"')), "an unescaped quote inside")
        if c != UInt8(ord("\\")):
            out.append(c)
            i += 1
            continue
        check(i + 1 < len(b) - 1, "a trailing backslash")
        var e = b[i + 1]
        if e == UInt8(ord('"')) or e == UInt8(ord("\\")):
            out.append(e)
            i += 2
        elif e == UInt8(ord("n")):
            out.append(UInt8(ord("\n")))
            i += 2
        elif e == UInt8(ord("t")):
            out.append(UInt8(ord("\t")))
            i += 2
        elif e == UInt8(ord("u")):
            check(i + 5 < len(b) - 1, "a short \\u escape")
            check(
                b[i + 2] == UInt8(ord("0")) and b[i + 3] == UInt8(ord("0")),
                "\\u above 0xff",
            )
            out.append(UInt8(_hex(b[i + 4]) * 16 + _hex(b[i + 5])))
            i += 6
        else:
            check(False, "an escape JSON doesn't have")
    return out^


def target(data: List[UInt8]) raises:
    save_last("json", data)
    var s = String(from_utf8_lossy=Span(data))
    var back = unquote(quote(s))
    var want = s.as_bytes()
    check(len(back) == len(want), "doesn't unescape to its input (length)")
    for i in range(len(want)):
        check(back[i] == want[i], "doesn't unescape to its input")


def main() raises:
    var seeds = List[List[UInt8]]()
    seeds.append([UInt8(ord("a")), 0x22, 0x5C, 0x0A, 0x09, 0x01, 0x1F, 0x7F])
    seeds.append([0xC3, 0xA9, 0xE2, 0x82, 0xAC, 0xF0, 0x9F, 0x90, 0x8B])
    run_harness[target]("json", seeds, max_len=512)
