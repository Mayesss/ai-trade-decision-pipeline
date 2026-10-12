"""N7/N4 scout: schema inspection of two free order-book samples. No outcome.

Prints field names, message-type counts, row counts, timestamp spacing and
book depth (number of levels) only. No price, return or fill is computed.

Run: research/copytrade/.venv/bin/python -I research/copytrade/scouts/n7_schema.py <scout_n7 dir>
"""
import csv
import io
import json
import sys
import zipfile
from collections import Counter
from pathlib import Path


def binance_bookticker(path: Path) -> None:
    with zipfile.ZipFile(path) as z:
        name = z.namelist()[0]
        with z.open(name) as fh:
            rd = csv.reader(io.TextIOWrapper(fh))
            header = next(rd)
            n = 0
            first_t = last_t = None
            ti = header.index("transaction_time") if "transaction_time" in header else None
            for row in rd:
                n += 1
                if ti is not None:
                    t = int(row[ti])
                    first_t = t if first_t is None else first_t
                    last_t = t
    span_s = (last_t - first_t) / 1000 if first_t else None
    print(json.dumps({"file": path.name, "columns": header, "rows": n,
                      "span_s": span_s,
                      "mean_update_interval_ms": round(1000 * span_s / n, 1) if n else None}))


def bybit_ob500(path: Path) -> None:
    types = Counter()
    keys = set()
    data_keys = set()
    levels_snapshot = []
    n = 0
    first_ts = last_ts = None
    with zipfile.ZipFile(path) as z:
        name = z.namelist()[0]
        with z.open(name) as fh:
            for line in io.TextIOWrapper(fh):
                n += 1
                m = json.loads(line)
                keys.update(m.keys())
                types[m.get("type")] += 1
                d = m.get("data", {})
                data_keys.update(d.keys())
                ts = m.get("ts")
                if ts is not None:
                    first_ts = ts if first_ts is None else first_ts
                    last_ts = ts
                if m.get("type") == "snapshot" and len(levels_snapshot) < 3:
                    levels_snapshot.append((len(d.get("b", [])), len(d.get("a", []))))
    span_s = (last_ts - first_ts) / 1000 if first_ts else None
    print(json.dumps({"file": path.name, "message_keys": sorted(keys),
                      "data_keys": sorted(data_keys), "types": dict(types),
                      "messages": n, "span_s": span_s,
                      "mean_msg_interval_ms": round(1000 * span_s / n, 1) if n else None,
                      "snapshot_levels_bid_ask": levels_snapshot}))


def main() -> None:
    root = Path(sys.argv[1])
    for p in sorted((root / "binance_bookticker").glob("*.zip")):
        binance_bookticker(p)
    for p in sorted((root / "bybit_ob500").glob("*.zip")):
        bybit_ob500(p)


if __name__ == "__main__":
    main()
