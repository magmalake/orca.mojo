"""Checks sampler files (orca.sample): every line is a sample with sane
values. Prints how many lines each file has.

    python3 tests/metrics_ok.py [--min N] [--rss] <file.jsonl>...

--rss: some line has the run's processes resident.
"""

import json
import sys


def check(path, least, rss):
    with open(path) as f:
        lines = [l for l in f.read().splitlines() if l.strip()]
    if len(lines) < least:
        sys.exit(f"{path}: {len(lines)} lines, fewer than {least}")
    last = 0.0
    for n, line in enumerate(lines, 1):
        where = f"{path}:{n}"
        s = json.loads(line)
        assert s["v"] == 1, where
        assert s["t"] > 1.7e9 and s["t"] >= last - 1, where
        last = s["t"]
        assert s["cores"] >= 1, where
        for k in ("user", "system", "iowait", "steal", "busiest"):
            assert 0 <= s["cpu"][k] <= 100.05, (where, k, s["cpu"][k])
        assert s["cpu"]["user"] + s["cpu"]["system"] <= 100.2, where
        m = s["mem"]
        assert m["total"] > 1 << 26, where
        assert 0 < m["used"] <= m["total"], where
        assert 0 <= m["available"] <= m["total"], where
        assert 0 <= m["swap_used"] <= max(m["swap_total"], 0) + 1, where
        assert 0 <= m["rss"] <= m["total"], where
        assert len(s["disk"]) >= 1, where
        for d in s["disk"]:
            assert d["total"] > 0 and 0 <= d["used"] <= d["total"], where
            assert 0 <= d["free"] <= d["total"], where
        if s["io"] is not None:
            assert s["io"]["read"] >= 0 and s["io"]["write"] >= 0, where
        assert s["net"]["in"] >= 0 and s["net"]["out"] >= 0, where
        assert len(line) < 700, (where, len(line))
    if rss:
        assert max(json.loads(l)["mem"]["rss"] for l in lines) > 0, f"{path}: no rss"
    print(path, len(lines))


def main():
    args = sys.argv[1:]
    least = 1
    rss = False
    while args[:1] in (["--min"], ["--rss"]):
        if args[0] == "--rss":
            rss = True
            args = args[1:]
        else:
            least = int(args[1])
            args = args[2:]
    if not args:
        sys.exit("no files")
    for p in args:
        check(p, least, rss)


main()
