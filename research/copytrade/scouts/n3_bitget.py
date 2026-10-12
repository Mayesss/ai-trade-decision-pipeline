"""N3 scout: Bitget USDT-M perp listing dates (counts only).

BEHAVIOUR ONLY.
  cache <bg-first-day dir>       : the pre-existing ct/bitget.py first_candle_day cache (one JSON value per
                                   file: ms of the first daily candle, or null). Values >= 2026-07-01 are dropped
                                   at load and only COUNTED, never printed. Prints counts by year.
  contracts <contracts.json>     : today's /api/v2/mix/market/contracts snapshot. launchTime >= 2026-07-01 is
                                   dropped at load (count only). Prints field names and launch counts by year.
"""
import collections
import datetime as dt
import json
import os
import sys

CUT = int(dt.datetime(2026, 7, 1, tzinfo=dt.UTC).timestamp() * 1000)


def year(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).year


def cmd_cache(d):
    files = [f for f in os.listdir(d) if f.endswith(".json")]
    vals, nulls, post, other = [], 0, 0, 0
    for f in files:
        v = json.load(open(os.path.join(d, f)))
        if v is None:
            nulls += 1
        elif isinstance(v, (int, float)):
            if v >= CUT:
                post += 1
            else:
                vals.append(int(v))
        else:
            other += 1
    print(f"files {len(files)}; null {nulls}; non-numeric {other}; dropped >= 2026-07-01: {post}; kept {len(vals)}")
    print("kept by year:", dict(sorted(collections.Counter(year(v) for v in vals).items())))
    if vals:
        print("kept range years:", year(min(vals)), "-", year(max(vals)))


def cmd_contracts(p):
    d = json.load(open(p))
    rows = d.get("data", [])
    print("code", d.get("code"), "rows", len(rows))
    if rows:
        print("fields:", sorted(rows[0].keys()))
    lt = []
    post = 0
    status = collections.Counter()
    for r in rows:
        t = r.get("launchTime") or r.get("openTime")
        if t in (None, "", "0", "-1"):
            status["no launchTime"] += 1
            continue
        t = int(t)
        if t >= CUT:
            post += 1
            continue
        lt.append(t)
        status[r.get("symbolStatus")] += 1
    print(f"launchTime < cutoff: {len(lt)}; dropped >= cutoff: {post}; status of kept {dict(status)}")
    print("kept launches by year:", dict(sorted(collections.Counter(year(v) for v in lt).items())))


if __name__ == "__main__":
    {"cache": cmd_cache, "contracts": cmd_contracts}[sys.argv[1]](sys.argv[2])
