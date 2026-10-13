"""N3 scout: per-event S3 prefix listings and first-hour/listing-day pulls for Binance listings.

BEHAVIOUR ONLY. Subcommands:

  list  <binance_events.csv> <out_dir>
      Prefix listings (keys, sizes, dates; no contents) per listing event for:
        futures daily klines 1m and daily bookDepth in the listing month(s) (day0..day13),
        spot monthly klines 1d for candidate spot pairs of the same base, and spot daily 1d keys in the
        perp listing month and the month before.
      Every key dated >= 2026-07-01 is dropped in memory. Writes <out_dir>/prefix_keys.csv.

  fetch <binance_events.csv> <prefix_keys.csv> <raw_dir>
      Downloads, per non-censored listing event: futures daily 1m klines of day0 (and day1 when the
      first trade is after 23:00), daily 1d kline of day0, daily metrics of day0, and the monthly
      fundingRate file of the listing month. Verifies each zip against its .CHECKSUM, writes
      <raw_dir>/SHA256SUMS. raw_dir must be new/empty.

  read  <binance_events.csv> <raw_dir> <out_csv>
      Parses ONLY: 1m -> row count, first open time, number of 1m rows and quote volume inside the first
      60 minutes; 1d -> day0 quote volume (a level); metrics -> row count, first stamp, OI value at the
      last stamp inside the first hour; fundingRate -> the FIRST row's calc_time and interval_hours (a
      contract spec at listing). No price, return or funding-rate value is parsed.
"""
import concurrent.futures as cf
import csv
import datetime as dt
import hashlib
import io
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile

BASE = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
DL = "https://data.binance.vision/"
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
CUTOFF = "2026-07-01"
DATE = re.compile(r"-(\d{4}-\d{2}(?:-\d{2})?)\.zip$")
QUOTES = ["USDT", "BUSD", "FDUSD", "USDC", "BTC", "BNB"]


def _get(url, tries=5):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return r.read()
        except Exception:  # noqa: BLE001
            if i == tries - 1:
                raise
            time.sleep(1.5 * (i + 1))


def list_keys(prefix):
    out, marker = [], ""
    while True:
        root = ET.fromstring(_get(BASE + "?" + urllib.parse.urlencode({"prefix": prefix, "marker": marker})))
        last = None
        for c in root.findall(NS + "Contents"):
            k = c.find(NS + "Key").text
            last = k
            m = DATE.search(k)
            if k.endswith(".zip") and m:
                ds = m.group(1)
                if (ds if len(ds) == 10 else ds + "-01") < CUTOFF:      # drop post-cutoff keys in memory
                    out.append((k, ds, int(c.find(NS + "Size").text)))
        if root.find(NS + "IsTruncated").text != "true" or last is None:
            return out
        marker = last


def events(path, censored_ok=False):
    return [r for r in csv.DictReader(open(path)) if censored_ok or r["censored"] == "0"]


def bases(sym):
    b = sym[:-4]
    out = [b]
    for p in ("1000000", "10000", "1000", "1M"):
        if b.startswith(p) and len(b) > len(p):
            out.append(b[len(p):])
    return out


def months_for(day, n=14):
    d0 = dt.date.fromisoformat(day)
    return sorted({(d0 + dt.timedelta(days=i)).isoformat()[:7] for i in range(n)})


def cmd_list(ev_path, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    jobs = []
    for r in events(ev_path):
        s, day = r["symbol"], r["date"]
        for m in months_for(day):
            jobs.append(("fut_1m", s, day, f"data/futures/um/daily/klines/{s}/1m/{s}-1m-{m}"))
            jobs.append(("fut_bookDepth", s, day, f"data/futures/um/daily/bookDepth/{s}/{s}-bookDepth-{m}"))
        if r["event"] == "listing":
            for b in bases(s):
                for q in QUOTES:
                    jobs.append(("spot_m1d", s, day, f"data/spot/monthly/klines/{b}{q}/1d/"))
    rows = []

    def run(j):
        return [(j[0], j[1], j[2], *k) for k in list_keys(j[3])]
    with cf.ThreadPoolExecutor(24) as ex:
        for res in ex.map(run, jobs):
            rows.extend(res)
    # spot daily keys for the perp listing month and the month before, for pairs that exist
    spot_pairs = {(r[1], r[2], r[3].split("/")[4]) for r in rows if r[0] == "spot_m1d"}
    jobs2 = []
    for s, day, pair in spot_pairs:
        d0 = dt.date.fromisoformat(day)
        prev = (d0.replace(day=1) - dt.timedelta(days=1)).isoformat()[:7]
        for m in (prev, day[:7]):
            jobs2.append(("spot_d1d", s, day, f"data/spot/daily/klines/{pair}/1d/{pair}-1d-{m}"))
    with cf.ThreadPoolExecutor(24) as ex:
        for res in ex.map(run, jobs2):
            rows.extend(res)
    with open(os.path.join(out_dir, "prefix_keys.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["kind", "symbol", "listing_day", "key", "date", "bytes"])
        w.writerows(sorted(rows))
    print(f"jobs {len(jobs)} + {len(jobs2)}; keys kept {len(rows)}")


def fetch_one(key, raw_dir):
    dest = os.path.join(raw_dir, key)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    body = _get(DL + key)
    chk = _get(DL + key + ".CHECKSUM").decode().split()[0]
    h = hashlib.sha256(body).hexdigest()
    if h != chk:
        return key, None
    with open(dest, "wb") as f:
        f.write(body)
    return key, h


def cmd_fetch(ev_path, keys_path, raw_dir):
    assert not os.path.exists(raw_dir) or not os.listdir(raw_dir), "raw_dir must be new"
    os.makedirs(raw_dir, exist_ok=True)
    have = {r["key"] for r in csv.DictReader(open(keys_path))}
    want = []
    for r in events(ev_path):
        if r["event"] != "listing":
            continue
        s, day = r["symbol"], r["date"]
        d1 = (dt.date.fromisoformat(day) + dt.timedelta(days=1)).isoformat()
        for k in (f"data/futures/um/daily/klines/{s}/1m/{s}-1m-{day}.zip",
                  f"data/futures/um/daily/klines/{s}/1m/{s}-1m-{d1}.zip"):
            if k in have and k.rsplit("-1m-", 1)[1][:10] < CUTOFF:
                want.append(k)
        want.append(f"data/futures/um/daily/klines/{s}/1d/{s}-1d-{day}.zip")
        if r["metrics_d0"] == "1":
            want.append(f"data/futures/um/daily/metrics/{s}/{s}-metrics-{day}.zip")
        if r["funding_months_ok"] == "1":
            want.append(f"data/futures/um/monthly/fundingRate/{s}/{s}-fundingRate-{day[:7]}.zip")
    want = sorted(set(want))
    for k in want:
        ds = DATE.search(k).group(1)
        assert (ds if len(ds) == 10 else ds + "-01") < CUTOFF, k
    ok, bad = [], []
    with cf.ThreadPoolExecutor(16) as ex:
        for k, h in ex.map(lambda k: fetch_one(k, raw_dir), want):
            (ok if h else bad).append((k, h))
    with open(os.path.join(raw_dir, "SHA256SUMS"), "w") as f:
        for k, h in sorted(ok):
            f.write(f"{h}  {k}\n")
    print(f"wanted {len(want)}; verified {len(ok)}; checksum/fetch failures {len(bad)}",
          f"bytes {sum(os.path.getsize(os.path.join(raw_dir, k)) for k, _ in ok)}")


def rows_of(path):
    with zipfile.ZipFile(path) as z:
        (n,) = z.namelist()
        text = z.read(n).decode()
    rows = [r for r in csv.reader(io.StringIO(text)) if r]
    if rows and not rows[0][0][:1].isdigit():
        rows = rows[1:]
    return rows


def ts_ms(s):
    if s.isdigit():
        v = int(s)
        return v // 1000 if v > 10**14 else v          # 2025+ files may be in microseconds
    return int(dt.datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.UTC).timestamp() * 1000)


def cmd_read(ev_path, raw_dir, out_csv):
    out = []
    for r in events(ev_path):
        if r["event"] != "listing":
            continue
        s, day = r["symbol"], r["date"]
        d1 = (dt.date.fromisoformat(day) + dt.timedelta(days=1)).isoformat()
        rec = {"symbol": s, "date": day}
        m1 = []
        for dd in (day, d1):
            p = os.path.join(raw_dir, f"data/futures/um/daily/klines/{s}/1m/{s}-1m-{dd}.zip")
            if os.path.exists(p):
                m1 += [(ts_ms(x[0]), float(x[7])) for x in rows_of(p)]
        p0 = os.path.join(raw_dir, f"data/futures/um/daily/klines/{s}/1m/{s}-1m-{day}.zip")
        rec["m1_rows_day0"] = len(rows_of(p0)) if os.path.exists(p0) else ""
        if m1:
            t0 = min(t for t, _ in m1)
            first_hr = [(t, q) for t, q in m1 if t < t0 + 3_600_000]
            rec.update(first_trade=dt.datetime.fromtimestamp(t0 / 1000, dt.UTC).isoformat()[:16],
                       m1_rows_first_hour=len(first_hr), qv_first_hour=round(sum(q for _, q in first_hr)))
        else:
            t0 = None
        p = os.path.join(raw_dir, f"data/futures/um/daily/klines/{s}/1d/{s}-1d-{day}.zip")
        if os.path.exists(p):
            rec["qv_day0"] = round(float(rows_of(p)[0][7]))
        p = os.path.join(raw_dir, f"data/futures/um/daily/metrics/{s}/{s}-metrics-{day}.zip")
        if os.path.exists(p):
            mr = sorted({(ts_ms(x[0]), x[3]) for x in rows_of(p)})
            rec["metrics_rows_day0"] = len(mr)
            rec["metrics_first"] = dt.datetime.fromtimestamp(mr[0][0] / 1000, dt.UTC).isoformat()[:16]
            if t0 is not None:
                inside = [v for t, v in mr if t < t0 + 3_600_000]
                rec["oiv_end_first_hour"] = round(float(inside[-1])) if inside else ""
        p = os.path.join(raw_dir, f"data/futures/um/monthly/fundingRate/{s}/{s}-fundingRate-{day[:7]}.zip")
        if os.path.exists(p):
            fr = rows_of(p)
            first = min(fr, key=lambda x: ts_ms(x[0]))
            rec["first_funding"] = dt.datetime.fromtimestamp(ts_ms(first[0]) / 1000, dt.UTC).isoformat()[:16]
            rec["interval_h_at_listing"] = first[1]
        out.append(rec)
    keys = sorted({k for x in out for k in x}, key=lambda k: list(out[0]).index(k) if k in out[0] else 99)
    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(out)
    print(f"events read {len(out)}")


def cmd_coverage(ev_path, keys_path):
    """Counts only: per listing year, events whose daily 1m klines / bookDepth exist for day0 and all of day0..13."""
    have = {}
    for r in csv.DictReader(open(keys_path)):
        if r["kind"] in ("fut_1m", "fut_bookDepth"):
            have.setdefault((r["kind"], r["symbol"]), set()).add(r["date"])
    import collections
    tab = collections.defaultdict(collections.Counter)
    for r in events(ev_path):
        y = r["date"][:4] + ("" if r["event"] == "listing" else " relist")
        d0 = dt.date.fromisoformat(r["date"])
        days = [(d0 + dt.timedelta(days=i)).isoformat() for i in range(14)]
        t = tab[y]
        t["n"] += 1
        for kind in ("fut_1m", "fut_bookDepth"):
            got = have.get((kind, r["symbol"]), set())
            t[kind + " day0"] += r["date"] in got
            t[kind + " 14/14"] += all(x in got for x in days)
    for y in sorted(tab):
        print(y, dict(tab[y]))


def cmd_summary(csv_path):
    """Counts and LEVEL quantiles per listing year (capacity inputs at listing). Never crossed with any outcome."""
    import collections
    rows = list(csv.DictReader(open(csv_path)))
    by = collections.defaultdict(list)
    for r in rows:
        by[r["date"][:4]].append(r)

    def q(xs):
        xs = sorted(xs)
        if not xs:
            return "-"
        n = len(xs)
        return "/".join(f"{xs[min(n - 1, int(n * p))] / 1e6:.2f}" for p in (0.1, 0.5, 0.9)) + f" (n={n})"
    for y in sorted(by):
        R = by[y]
        fh = collections.Counter("60" if r.get("m1_rows_first_hour") == "60" else "<60" if r.get("m1_rows_first_hour") else "none" for r in R)
        mg = collections.Counter()
        for r in R:
            if r.get("metrics_first") and r.get("first_trade"):
                g = (dt.datetime.fromisoformat(r["metrics_first"]) - dt.datetime.fromisoformat(r["first_trade"])).total_seconds() / 60
                mg["metrics starts <= first trade+5m" if g <= 5 else "+5..60m" if g <= 60 else ">60m"] += 1
        fg, iv = collections.Counter(), collections.Counter()
        for r in R:
            if r.get("first_funding") and r.get("first_trade"):
                g = (dt.datetime.fromisoformat(r["first_funding"]) - dt.datetime.fromisoformat(r["first_trade"])).total_seconds() / 3600
                fg["<0 (pre-trade row)" if g < 0 else "<=4h" if g <= 4 else "4-8h" if g <= 8 else ">8h"] += 1
            if r.get("interval_h_at_listing") is not None:
                iv[r["interval_h_at_listing"] or "blank"] += 1
        tod = collections.Counter(r["first_trade"][11:13] for r in R if r.get("first_trade"))
        print(f"{y}: n={len(R)} | 1m rows in first hour {dict(fh)} | first-trade UTC hour top3 {tod.most_common(3)}")
        print(f"   quote vol first hour $M p10/p50/p90 {q([float(r['qv_first_hour']) for r in R if r.get('qv_first_hour')])}"
              f" | day0 $M {q([float(r['qv_day0']) for r in R if r.get('qv_day0')])}"
              f" | OI value end of first hour $M {q([float(r['oiv_end_first_hour']) for r in R if r.get('oiv_end_first_hour')])}")
        print(f"   metrics start vs first trade {dict(mg)} | first funding row vs first trade {dict(fg)} | interval_h at first settlement {dict(iv)}")


if __name__ == "__main__":
    c = sys.argv[1]
    {"list": cmd_list, "fetch": cmd_fetch, "read": cmd_read, "coverage": cmd_coverage, "summary": cmd_summary}[c](*sys.argv[2:])
