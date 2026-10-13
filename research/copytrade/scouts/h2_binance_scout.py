"""H2 feasibility scout: list data.binance.vision objects (keys, sizes, dates).

Listing only -- no market data is read and no returns are computed.
Usage:
  python -I h2_binance_scout.py prefixes <prefix>
  python -I h2_binance_scout.py summary <prefix> [cutoff YYYY-MM-DD]
     prints first/last date, file count, total bytes of *.zip under prefix
     (dates >= cutoff are counted separately and never fetched).
"""
import re
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

BASE = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def list_objects(prefix, delimiter=None):
    marker = ""
    while True:
        q = {"prefix": prefix, "marker": marker}
        if delimiter:
            q["delimiter"] = delimiter
        with urllib.request.urlopen(BASE + "?" + urllib.parse.urlencode(q), timeout=60) as r:
            root = ET.fromstring(r.read())
        last = None
        for c in root.findall(NS + "Contents"):
            key = c.find(NS + "Key").text
            last = key
            yield ("obj", key, int(c.find(NS + "Size").text))
        for p in root.findall(NS + "CommonPrefixes"):
            last = p.find(NS + "Prefix").text
            yield ("pre", last, 0)
        if root.find(NS + "IsTruncated").text != "true" or last is None:
            return
        marker = root.findtext(NS + "NextMarker") or last


DATE = re.compile(r"(\d{4}-\d{2}(?:-\d{2})?)\.zip$")


def summary(prefix, cutoff="2026-07-01", start=None, end=None):
    rows = [(DATE.search(k).group(1), s) for t, k, s in list_objects(prefix)
            if t == "obj" and k.endswith(".zip") and DATE.search(k)]
    rows.sort()
    if not rows:
        return {"prefix": prefix, "n": 0}
    pre = [r for r in rows if r[0] < cutoff]
    post = [r for r in rows if r[0] >= cutoff]
    out = {
        "prefix": prefix, "first": rows[0][0], "last": rows[-1][0], "n": len(rows),
        "bytes_all": sum(s for _, s in rows),
        "n_post_cutoff": len(post), "bytes_post_cutoff": sum(s for _, s in post),
    }
    if start and end:
        win = [r for r in rows if start[: len(r[0])] <= r[0] <= end[: len(r[0])]]
        out.update(n_window=len(win), bytes_window=sum(s for _, s in win))
    return out


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "prefixes":
        for t, k, s in list_objects(sys.argv[2], "/"):
            print(t, k, s)
    elif cmd == "summary":
        args = sys.argv[3:]
        print(summary(sys.argv[2], *args))


# ---- sample inspection (schema, row counts, timing only; no prices/returns) ----
import collections
import csv
import io
import zipfile


def _rows(zpath):
    with zipfile.ZipFile(zpath) as z:
        name = z.namelist()[0]
        with z.open(name) as fh:
            rd = csv.reader(io.TextIOWrapper(fh, "utf-8"))
            header = next(rd)
            yield header
            yield from rd


def _gaps(ts):
    d = sorted(set(ts))
    g = sorted(b - a for a, b in zip(d, d[1:]))
    q = lambda p: g[min(len(g) - 1, int(p * len(g)))] if g else None
    return {"distinct_ms": len(d), "gap_min_ms": q(0), "gap_p01": q(0.01), "gap_p10": q(0.10),
            "gap_med": q(0.5), "n_gaps_lt_1000ms": sum(1 for x in g if x < 1000)}


def inspect(zpath, kind):
    it = _rows(zpath)
    header = next(it)
    out = {"file": zpath.rsplit("/", 1)[-1], "header": header}
    if kind == "liq":
        rows = list(it)
        ts = [int(r[0]) for r in rows]
        per_sec = collections.Counter(t // 1000 for t in ts)
        dup = sum(c - 1 for c in collections.Counter(tuple(r) for r in rows).values() if c > 1)
        out.update(rows=len(rows), exact_duplicate_rows=dup,
                   max_rows_per_second=max(per_sec.values()),
                   seconds_with_gt1_distinct_ms=sum(1 for s, n in collections.Counter(
                       t // 1000 for t in set(ts)).items() if n > 1),
                   sides=dict(collections.Counter(r[1] for r in rows)),
                   types=dict(collections.Counter((r[2], r[3], r[7]) for r in rows)),
                   rows_partial_fill=sum(1 for r in rows if r[8] != r[9]), **_gaps(ts))
    elif kind in ("agg", "trades"):
        tcol = header.index("transact_time" if kind == "agg" else "time")
        n = 0
        per_sec = collections.Counter()
        first = last = None
        for r in it:
            t = int(r[tcol]); n += 1
            per_sec[t // 1000] += 1
            first = t if first is None else first
            last = t
        out.update(rows=n, first_ms=first, last_ms=last, seconds_with_rows=len(per_sec),
                   max_rows_per_second=max(per_sec.values()))
        if kind == "agg":
            pass
    else:  # bookDepth / metrics / klines: cadence only
        rows = list(it)
        tcol = 0
        stamps = sorted(set(r[tcol] for r in rows))
        out.update(rows=len(rows), distinct_stamps=len(stamps), first=stamps[0], last=stamps[-1])
        if kind == "depth":
            out["levels"] = sorted(set(r[1] for r in rows), key=float)
    return out
