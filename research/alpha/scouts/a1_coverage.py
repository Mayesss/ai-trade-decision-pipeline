"""A1 scout: coverage counts ONLY (number of points, first/last UTC date,
file size, median spacing in days, refusal if any point is at/after
2026-07-01). Never reads, prints or derives a value column.
Usage: python -I a1_coverage.py <pull_dir> [<pull_dir> ...]"""
import datetime
import json
import os
import sys

CUTOFF = 1782864000


def ts_list(obj):
    """Yield (series_name, [epoch_seconds]) from the known pull shapes."""
    if isinstance(obj, dict) and "pnl" in obj:
        yield "pnl", [int(r["timestamp"]) for r in obj["pnl"]]
    elif isinstance(obj, list):
        yield "rows", [int(r["snapshotTimestamp"]) for r in obj]
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, list) and v and isinstance(v[0], list):
                yield k, [int(p[0]) // 1000 for p in v]


def day(t):
    return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%d")


for d in sys.argv[1:]:
    for n in sorted(os.listdir(d)):
        if not n.endswith(".json"):
            continue
        p = os.path.join(d, n)
        obj = json.load(open(p))
        for name, ts in ts_list(obj):
            if not ts:
                print(f"{os.path.basename(d)}/{n} [{name}] 0 points")
                continue
            if max(ts) >= CUTOFF:
                raise SystemExit(f"REFUSE: {p} holds a point at/after 2026-07-01")
            ts.sort()
            gaps = sorted(b - a for a, b in zip(ts, ts[1:]))
            med = gaps[len(gaps) // 2] / 86400 if gaps else 0
            print(f"{os.path.basename(d)}/{n} [{name}] n={len(ts)} first={day(ts[0])} "
                  f"last={day(ts[-1])} median_spacing_d={med:.2f} bytes={os.path.getsize(p)}")
