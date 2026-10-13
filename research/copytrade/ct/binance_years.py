"""Registration 009 (L4) — loader for the Binance USDⓈ-M discovery pull (data/binance_years).

Turns the verified zips (r009_pull.py) into four small panels under data/binance_years/panel/:

  bars_1h.parquet     symbol, ts (open time, ms), open, high, low, close, qv (quote volume), cnt
                      (trades), tbq (taker-buy quote volume)            — monthly klines 1h + 2024-01-01 daily
  funding.parquet     symbol, ts (calc_time, ms), rate, interval_h     — monthly fundingRate + REST 2024-01-01
  metrics_1h.parquet  symbol, ts (hour start, ms), n (5-min rows), oiv_sum, oi_sum (sums over the hour's
                      rows, so any window's mean is exact), top_acct, top_pos, global_ratio (last row of the hour),
                      taker_sum (sum of the 5-min taker buy/sell ratio)  — daily metrics, exact
                      duplicate rows dropped (the archive writes some days twice)
  life.json           per symbol: alive segments [first full metrics day, last full metrics day] from the
                      scout's listing (a file >= 4 KB is a full day; a gap > 7 days starts a new segment,
                      i.e. a relisting), and the first 1d-kline day (the listing date)

Only zips recorded as verified ('ok' in fetch_state.jsonl, sha256 equal to the .CHECKSUM) are read.
Nothing dated on or after 2024-01-08 can be in the pull; every row is asserted below it anyway.
Run reading the downloads with `python -I`.
"""
import csv
import datetime as dt
import io
import json
import os
import re
import zipfile

import duckdb

HOUR, DAY = 3_600_000, 86_400_000
HARD_END = int(dt.datetime(2024, 1, 8, tzinfo=dt.UTC).timestamp() * 1000)   # discovery: nothing at or after
HARD_END_ACTIVE = [HARD_END]                  # build() sets the bound of the pull it reads
FULL = 4000
SEGMENT_GAP_DAYS = 7


def _day_ms(s):
    return int(dt.datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=dt.UTC).timestamp() * 1000)


def _rows(path):
    """CSV rows of the single member of a zip; a header row (non-numeric first cell) is skipped."""
    with zipfile.ZipFile(path) as z:
        names = z.namelist()
        assert len(names) == 1, (path, names)
        text = z.read(names[0]).decode()
    rows = list(csv.reader(io.StringIO(text)))
    if rows and rows[0] and not rows[0][0][:1].isdigit():
        rows = rows[1:]
    return [r for r in rows if r]


def verified(out_dir):
    """{key: dataset-ish} of zips whose fetch was verified against the .CHECKSUM."""
    ok = {}
    with open(os.path.join(out_dir, 'fetch_state.jsonl')) as f:
        for line in f:
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if r.get('status') == 'ok':
                ok[r['key']] = r['sha256']
    return ok


def build_bars(raw, keys, tmp_csv):
    n = 0
    with open(tmp_csv, 'w', newline='') as fh:
        w = csv.writer(fh)
        for k in keys:
            sym = k.split('/')[5]
            for r in _rows(os.path.join(raw, k)):
                ts = int(r[0])
                assert ts < HARD_END_ACTIVE[0], (k, ts)
                w.writerow((sym, ts, r[1], r[2], r[3], r[4], r[7], r[8], r[10]))
                n += 1
    return n


def build_funding(raw, keys, rest_dir, tmp_csv):
    n = 0
    with open(tmp_csv, 'w', newline='') as fh:
        w = csv.writer(fh)
        for k in keys:
            sym = k.split('/')[5]
            for r in _rows(os.path.join(raw, k)):
                ts = int(r[0])
                assert ts < HARD_END_ACTIVE[0], (k, ts)
                w.writerow((sym, ts, r[2], r[1]))
                n += 1
        if os.path.isdir(rest_dir):
            for fn in sorted(os.listdir(rest_dir)):
                sym = fn.split('-')[0]
                for x in json.load(open(os.path.join(rest_dir, fn))):
                    ts = int(x['fundingTime'])
                    assert ts < HARD_END_ACTIVE[0] and x['symbol'] == sym
                    w.writerow((sym, ts, x['fundingRate'], ''))
                    n += 1
    return n


def metrics_hourly(path, day_ms):
    """One daily metrics file -> {hour_ts: [n, oiv_sum, oi_sum, top_acct, top_pos, global_ratio, taker_sum]}.
    Exact duplicate rows are dropped; rows are processed in time order so 'last' is the hour's last."""
    seen, parsed = set(), []
    for r in _rows(path):
        t = tuple(r)
        if t in seen:
            continue
        seen.add(t)
        s = r[0]
        if s[:4].isdigit() and len(s) >= 19 and s[4] == '-':
            ts = _day_ms(s[:10]) + int(s[11:13]) * HOUR + int(s[14:16]) * 60_000 + int(s[17:19]) * 1000
        else:
            ts = int(s)
        parsed.append((ts, r))
    parsed.sort(key=lambda x: x[0])
    out = {}
    for ts, r in parsed:
        assert ts < HARD_END
        h = ts - ts % HOUR
        a = out.setdefault(h, [0, 0.0, 0.0, None, None, None, 0.0])
        a[0] += 1
        a[1] += float(r[3])
        a[2] += float(r[2])
        a[3], a[4], a[5] = (float(x) if x not in ('', None) else None for x in (r[4], r[5], r[6]))
        a[6] += float(r[7]) if r[7] not in ('', None) else 0.0
    return out


def build_metrics(raw, keys, tmp_csv):
    n = 0
    with open(tmp_csv, 'w', newline='') as fh:
        w = csv.writer(fh)
        for i, k in enumerate(keys):
            sym = k.split('/')[5]
            day = k.rsplit('-metrics-', 1)[1][:10]
            for h, a in sorted(metrics_hourly(os.path.join(raw, k), _day_ms(day)).items()):
                w.writerow((sym, h, *('' if x is None else x for x in a)))
                n += 1
            if (i + 1) % 20000 == 0:
                print(f'    metrics {i + 1}/{len(keys)}', flush=True)
    return n


def life_from_listing(listing_dir, cutoff='2026-07-01'):
    """{symbol: {'segments': [[start_day, end_day], ...], 'first_kline_day': day}} from the scout's
    listing CSVs (keys, sizes, dates; no contents)."""
    full = {}
    with open(os.path.join(listing_dir, 'daily_metrics.csv')) as f:
        for r in csv.DictReader(f):
            if int(r['bytes']) >= FULL and r['date'] < cutoff:
                full.setdefault(r['symbol'], []).append(r['date'])
    first_k = {}
    with open(os.path.join(listing_dir, 'daily_klines_1d.csv')) as f:
        for r in csv.DictReader(f):
            if r['date'] < cutoff and (r['symbol'] not in first_k or r['date'] < first_k[r['symbol']]):
                first_k[r['symbol']] = r['date']
    return {s: {'segments': segments(sorted(ds)), 'first_kline_day': first_k.get(s)} for s, ds in full.items()}


def segments(days, gap=SEGMENT_GAP_DAYS):
    """Sorted 'YYYY-MM-DD' full-file days -> [[start, end]]; a hole longer than `gap` days splits."""
    out = []
    for d in days:
        if out and (_day_ms(d) - _day_ms(out[-1][1])) // DAY <= gap:
            out[-1][1] = d
        else:
            out.append([d, d])
    return out


def build(out_dir, listing_dir, hard_end=HARD_END):
    HARD_END_ACTIVE[0] = hard_end
    raw = os.path.join(out_dir, 'raw')
    panel = os.path.join(out_dir, 'panel')
    os.makedirs(panel, exist_ok=True)
    ok = sorted(verified(out_dir))
    kl = [k for k in ok if '/klines/' in k and '/1h/' in k]
    fu = [k for k in ok if '/fundingRate/' in k]
    me = [k for k in ok if '/metrics/' in k]
    con = duckdb.connect()
    tmp = os.path.join(panel, '_tmp.csv')
    counts = {}
    counts['bars'] = build_bars(raw, kl, tmp)
    con.execute(f"""COPY (SELECT DISTINCT * FROM read_csv('{tmp}', header=false, columns={{'symbol':'VARCHAR','ts':'BIGINT',
        'open':'DOUBLE','high':'DOUBLE','low':'DOUBLE','close':'DOUBLE','qv':'DOUBLE','cnt':'BIGINT','tbq':'DOUBLE'}})
        ORDER BY symbol, ts) TO '{panel}/bars_1h.parquet' (FORMAT parquet)""")
    counts['funding'] = build_funding(raw, fu, os.path.join(out_dir, 'rest_funding'), tmp)
    con.execute(f"""COPY (SELECT symbol, ts, max(rate) rate, max(interval_h) interval_h FROM read_csv('{tmp}', header=false,
        columns={{'symbol':'VARCHAR','ts':'BIGINT','rate':'DOUBLE','interval_h':'INTEGER'}}) GROUP BY 1, 2 ORDER BY 1, 2)
        TO '{panel}/funding.parquet' (FORMAT parquet)""")
    if me:                                       # the holdout-A pull has no metrics (F1 needs none)
        counts['metrics_hours'] = build_metrics(raw, me, tmp)
        con.execute(f"""COPY (SELECT * FROM read_csv('{tmp}', header=false, columns={{'symbol':'VARCHAR','ts':'BIGINT','n':'INTEGER',
            'oiv_sum':'DOUBLE','oi_sum':'DOUBLE','top_acct':'DOUBLE','top_pos':'DOUBLE','global_ratio':'DOUBLE','taker_sum':'DOUBLE'}})
            ORDER BY symbol, ts) TO '{panel}/metrics_1h.parquet' (FORMAT parquet)""")
    os.remove(tmp)
    life = life_from_listing(listing_dir)
    pulled = {k.split('/')[5] for k in me} | {k.split('/')[5] for k in kl}
    with open(os.path.join(panel, 'life.json'), 'w') as f:
        json.dump({s: life[s] for s in sorted(pulled) if s in life}, f, indent=0, sort_keys=True)
    for name in ('bars_1h', 'funding', 'metrics_1h'):
        if not os.path.exists(f'{panel}/{name}.parquet'):
            continue
        mx = con.execute(f"SELECT max(ts) FROM '{panel}/{name}.parquet'").fetchone()[0]
        assert mx < hard_end, (name, mx)
    return counts


def connect(panel_dir, max_ts=None):
    """DuckDB connection with views bars and funding over the panel parquet files. max_ts: rows
    stamped after it are invisible (Revision 3: discovery reads nothing after its last exit,
    2024-01-01 01:00, although the panel holds that whole day). No metrics view: F1 reads none."""
    pd = str(panel_dir).replace('\\', '/')
    if not re.search(r'/data/binance_years(_holdoutA)?/panel/?$', pd):      # whitelist (Revision 4); carry-guard
        raise PermissionError(f'009 never reads data/carry/ or any panel outside data/binance_years*: {pd}')  # carry-guard
    con = duckdb.connect()
    cut = f' WHERE ts <= {int(max_ts)}' if max_ts is not None else ''
    con.execute(f"CREATE VIEW bars AS SELECT * FROM '{panel_dir}/bars_1h.parquet'{cut}")
    con.execute(f"CREATE VIEW funding AS SELECT * FROM '{panel_dir}/funding.parquet'{cut}")
    return con
