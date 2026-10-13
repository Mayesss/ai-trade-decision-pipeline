"""L4 scout: schema of downloaded SAMPLE zips (header, row count, cadence, empties).

Prints no market value: only column names, counts, timestamp cadence and how
many cells per column are empty. Usage:
  ../.venv/bin/python -I l4_sample_schema.py <zip> [<zip> ...]
"""
import collections
import csv
import datetime as dt
import io
import sys
import zipfile


def ts_to_iso(v):
    try:
        x = int(float(v))
        if x > 10**14:  # microseconds
            x //= 1000
        return dt.datetime.fromtimestamp(x / 1000, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return v  # already a datetime string


def inspect(path):
    with zipfile.ZipFile(path) as z:
        info = z.infolist()[0]
        with z.open(info) as fh:
            rows = list(csv.reader(io.TextIOWrapper(fh, "utf-8")))
    has_header = not rows[0][0].strip().lstrip("-").replace(".", "").isdigit()
    header = rows[0] if has_header else [f"c{i}" for i in range(len(rows[0]))]
    body = rows[1:] if has_header else rows
    empties = collections.Counter()
    for r in body:
        for h, v in zip(header, r):
            if v.strip() == "":
                empties[h] += 1
    stamps = sorted(set(r[0] for r in body))
    try:
        nums = sorted(int(float(s)) for s in stamps)
        gaps = collections.Counter(b - a for a, b in zip(nums, nums[1:]))
    except ValueError:
        parsed = sorted(dt.datetime.fromisoformat(s) for s in stamps)
        gaps = collections.Counter(int((b - a).total_seconds() * 1000) for a, b in zip(parsed, parsed[1:]))
    print(f"{path.rsplit('/', 2)[-2]}/{path.rsplit('/', 1)[-1]}  entry={info.filename} "
          f"entry_mtime={dt.datetime(*info.date_time).date()}")
    print(f"  header_row={has_header} columns={header}")
    print(f"  rows={len(body)} distinct_t0={len(stamps)} first={ts_to_iso(stamps[0])} last={ts_to_iso(stamps[-1])}")
    print(f"  step_ms_counts={dict(gaps.most_common(4))} empty_cells={dict(empties) or 0}")


if __name__ == "__main__":
    for p in sys.argv[1:]:
        inspect(p)
