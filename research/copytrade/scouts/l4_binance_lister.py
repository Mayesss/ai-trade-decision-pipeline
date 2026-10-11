"""L4 feasibility scout: list every symbol's files for a data.binance.vision dataset.

LISTING ONLY. Writes (symbol, date, bytes, key) for every *.zip under
data/futures/um/<freq>/<dataset>/<SYMBOL>/[<interval>/]. No object is fetched.
Keys dated on or after the holdout cutoff (2026-07-01) are listed (key + size)
but never downloaded by anything in this scout.

Usage (from research/copytrade/scouts):
  ../.venv/bin/python -I l4_binance_lister.py <freq> <dataset> [interval] [--out DIR]
    e.g.  daily metrics            monthly fundingRate
          monthly klines 1h        monthly premiumIndexKlines 1d
Output: <out>/<freq>_<dataset>[_<interval>].csv  (default out: ../data/scout_l4/listings)

The S3 listing logic is copied from h2_binance_scout.py (public bucket, not
requester-pays) and extended with a thread pool over symbols.
"""
import concurrent.futures as cf
import csv
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

BASE = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
DATE = re.compile(r"-(\d{4}-\d{2}(?:-\d{2})?)\.zip$")


def _get(url, tries=5):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return r.read()
        except Exception:  # noqa: BLE001 - retry transient network errors
            if i == tries - 1:
                raise
            time.sleep(1.5 * (i + 1))


def list_objects(prefix, delimiter=None):
    marker = ""
    while True:
        q = {"prefix": prefix, "marker": marker}
        if delimiter:
            q["delimiter"] = delimiter
        root = ET.fromstring(_get(BASE + "?" + urllib.parse.urlencode(q)))
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


def symbols(freq, dataset):
    base = f"data/futures/um/{freq}/{dataset}/"
    return [k[len(base):].rstrip("/") for t, k, _ in list_objects(base, "/") if t == "pre"]


def list_symbol(freq, dataset, sym, interval=None):
    pre = f"data/futures/um/{freq}/{dataset}/{sym}/" + (f"{interval}/" if interval else "")
    out = []
    for t, k, s in list_objects(pre):
        if t != "obj" or not k.endswith(".zip"):
            continue
        m = DATE.search(k)
        if m:
            out.append((sym, m.group(1), s, k))
    return out


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "scout_l4", "listings")
    if "--out" in sys.argv:
        out_dir = sys.argv[sys.argv.index("--out") + 1]
        args = [a for a in args if a != out_dir]
    freq, dataset = args[0], args[1]
    interval = args[2] if len(args) > 2 else None
    os.makedirs(out_dir, exist_ok=True)
    syms = symbols(freq, dataset)
    name = f"{freq}_{dataset}" + (f"_{interval}" if interval else "") + ".csv"
    rows, errs = [], []
    with cf.ThreadPoolExecutor(24) as ex:
        futs = {ex.submit(list_symbol, freq, dataset, s, interval): s for s in syms}
        for f in cf.as_completed(futs):
            try:
                rows.extend(f.result())
            except Exception as e:  # noqa: BLE001
                errs.append((futs[f], repr(e)))
    rows.sort()
    with open(os.path.join(out_dir, name), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["symbol", "date", "bytes", "key"])
        w.writerows(rows)
    print(f"{name}: {len(syms)} symbol prefixes, {len(rows)} zips, "
          f"{sum(r[2] for r in rows)} bytes, errors={len(errs)}", errs[:5])


if __name__ == "__main__":
    main()
