"""H3 data pull: Binance USDⓈ-M aggTrades + bookDepth daily files from data.binance.vision.

Download and verification only. Reads only event TIMES, coin names and liquidation notional
(for the day filter / priority) from the Hyperliquid derived files. No price outcome, return,
markout, gap or half-life is computed anywhere: checks are row counts, date coverage, sizes
and timestamp ranges.

    .venv/bin/python -I h3_fetch.py plan    # build day set, list S3 sizes, print totals
    .venv/bin/python -I h3_fetch.py fetch   # download, verify SHA-256, convert, pin manifest

Hard rule: no file dated 2026-07-01 or later (holdout). `guard()` asserts it before every
file request.
"""
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import os
import pathlib
import random
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

HERE = pathlib.Path(__file__).resolve().parent
DATA = HERE / 'data'
HL = DATA / 'derived' / 'hyperliquid'
OUT = DATA / 'binance'
UM = OUT / 'um'
MANIFEST = OUT / 'manifest.json'
PLAN = OUT / 'plan.json'

LIST_BASE = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
DL_BASE = 'https://data.binance.vision/'
NS = '{http://s3.amazonaws.com/doc/2006-03-01/}'

W0, W1 = dt.date(2025, 7, 28), dt.date(2026, 6, 30)
HOLDOUT = dt.date(2026, 7, 1)
COINS = {'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'SOL': 'SOLUSDT', 'XRP': 'XRPUSDT',
         'DOGE': 'DOGEUSDT', 'HYPE': 'HYPEUSDT'}
DATASETS = ('aggTrades', 'bookDepth')
LIQ_DAY_MIN = 100_000.0
LATE_S = 22 * 3600                # event ends within 2 h of 24:00 UTC -> also take next day
AGG_CAP = 24 * 10**9
TOTAL_STOP = 26 * 10**9
WORKERS = 4

AGG_COLS = ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id',
            'transact_time', 'is_buyer_maker']
AGG_TYPES = {'agg_trade_id': pa.int64(), 'price': pa.float64(), 'quantity': pa.float64(),
             'first_trade_id': pa.int64(), 'last_trade_id': pa.int64(),
             'transact_time': pa.int64(), 'is_buyer_maker': pa.bool_()}
DEPTH_COLS = ['timestamp', 'percentage', 'depth', 'notional']
DEPTH_TYPES = {'timestamp': pa.timestamp('s'), 'percentage': pa.float64(),
               'depth': pa.float64(), 'notional': pa.float64()}
DATE_RE = re.compile(r'(\d{4}-\d{2}-\d{2})\.zip(?:\.CHECKSUM)?$')


def guard(date):
    """Assert a file date is inside the discovery window (never the holdout)."""
    if isinstance(date, str):
        date = dt.date.fromisoformat(date)
    assert date < HOLDOUT, f'HOLDOUT VIOLATION: {date} >= {HOLDOUT}'
    assert W0 <= date <= W1, f'outside window: {date}'
    return date


def guard_url(url):
    m = DATE_RE.search(url)
    assert m, f'no date in url {url}'
    guard(m.group(1))


def utc_day(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).date()


# ---------------------------------------------------------------- day set (times only)

def day_set():
    """{coin: {date: liq_notional_that_day}} from event times; notional only for priority."""
    casc = json.load(open(HL / 'cascades.json'))['primary']
    rows = duckdb.sql(f"""
        SELECT coin, CAST(epoch_ms(t) AS DATE) AS d, SUM(notional) AS n, MAX(t) AS tmax
        FROM read_parquet('{HL / 'liquidations.parquet'}')
        WHERE coin IN {tuple(COINS)} GROUP BY 1, 2""").fetchall()
    liq_n = {(c, d): n for c, d, n, _ in rows}
    out = {}
    for coin in COINS:
        days, why = set(), {}
        for e in casc:
            if e['coin'] != coin:
                continue
            a, b = utc_day(e['t_start']), utc_day(e['t_end'])
            days |= {a, b}
            if (e['t_end'] // 1000) % 86400 >= LATE_S:
                days.add(b + dt.timedelta(days=1))
        for c, d, n, tmax in rows:
            if c == coin and n >= LIQ_DAY_MIN:
                days.add(d)
                if (tmax // 1000) % 86400 >= LATE_S:   # a liquidation of that day ends late
                    days.add(d + dt.timedelta(days=1))
        out[coin] = {d: liq_n.get((coin, d), 0.0) for d in sorted(days) if W0 <= d <= W1}
    return out


# ---------------------------------------------------------------- S3 listing

def list_window(prefix):
    """Keys/sizes of daily zips under prefix, paging from the window start, stopping at cutoff."""
    sym, ds = prefix.rstrip('/').rsplit('/', 1)[-1], prefix.rstrip('/').rsplit('/', 2)[-2]
    marker = f'{prefix}{sym}-{ds}-{(W0 - dt.timedelta(days=1)).isoformat()}.zip.CHECKSUM'
    res = {}
    while True:
        q = urllib.parse.urlencode({'prefix': prefix, 'marker': marker})
        root = ET.fromstring(http_get(f'{LIST_BASE}?{q}'))
        last, done = None, False
        for c in root.findall(NS + 'Contents'):
            key = c.find(NS + 'Key').text
            last = key
            m = DATE_RE.search(key)
            if not m:
                continue
            d = dt.date.fromisoformat(m.group(1))
            if d >= HOLDOUT:
                done = True
                break
            if key.endswith('.zip') and d >= W0:
                res[d] = (key, int(c.find(NS + 'Size').text))
        if done or root.find(NS + 'IsTruncated').text != 'true' or last is None:
            return res
        marker = last


# ---------------------------------------------------------------- http

def http_get(url, dest=None, tries=6):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'h3-research-fetch/1.0'})
            with urllib.request.urlopen(req, timeout=120) as r:
                if dest is None:
                    return r.read()
                h = hashlib.sha256()
                n = 0
                with open(dest, 'wb') as fh:
                    while chunk := r.read(1 << 20):
                        fh.write(chunk)
                        h.update(chunk)
                        n += len(chunk)
                return h.hexdigest(), n
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise
            err = e
        except Exception as e:  # noqa: BLE001 -- retry transient network errors
            err = e
        time.sleep(min(60, 2 ** i) + random.random())
    raise RuntimeError(f'failed after {tries}: {url}: {err}')


# ---------------------------------------------------------------- plan

def plan():
    days = day_set()
    files, missing = [], {}
    for coin, sym in COINS.items():
        for ds in DATASETS:
            listing = list_window(f'data/futures/um/daily/{ds}/{sym}/')
            miss = []
            for d, n in days[coin].items():
                if d in listing:
                    key, size = listing[d]
                    files.append({'dataset': ds, 'symbol': sym, 'date': d.isoformat(),
                                  'key': key, 'size': size, 'liq_notional': n})
                else:
                    miss.append(d.isoformat())
            missing[f'{ds}/{sym}'] = miss
    agg = [f for f in files if f['dataset'] == 'aggTrades']
    agg_bytes = sum(f['size'] for f in agg)
    dropped = []
    if agg_bytes > AGG_CAP:   # prioritise by that coin-day's HL liquidation notional
        keep, tot = [], 0
        for f in sorted(agg, key=lambda f: -f['liq_notional']):
            if tot + f['size'] <= AGG_CAP:
                keep.append(f)
                tot += f['size']
            else:
                dropped.append(f)
        drop_ids = {(f['symbol'], f['date']) for f in dropped}
        files = [f for f in files if not (f['dataset'] == 'aggTrades'
                                          and (f['symbol'], f['date']) in drop_ids)]
    for f in files:
        guard(f['date'])
    summary = {}
    for f in files:
        s = summary.setdefault(f"{f['dataset']}/{f['symbol']}", {'days': 0, 'bytes': 0})
        s['days'] += 1
        s['bytes'] += f['size']
    total = sum(f['size'] for f in files)
    p = {'built_at': dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'),
         'window': [W0.isoformat(), W1.isoformat()], 'hl_day_set_sizes':
         {c: len(v) for c, v in days.items()}, 'summary': summary, 'total_bytes': total,
         'missing_on_binance': missing, 'dropped_for_cap': [(f['symbol'], f['date']) for f in dropped],
         'files': files}
    OUT.mkdir(parents=True, exist_ok=True)
    PLAN.write_text(json.dumps(p, indent=1))
    for k, v in summary.items():
        print(f"{k:22s} {v['days']:4d} days {v['bytes'] / 1e9:7.3f} GB")
    print('HL day-set sizes:', p['hl_day_set_sizes'])
    print('missing on Binance:', {k: v for k, v in missing.items() if v})
    print(f'dropped for 24 GB aggTrades cap: {len(dropped)}')
    print(f'TOTAL {len(files)} files {total / 1e9:.3f} GB')
    if total > TOTAL_STOP:
        print('OVER 26 GB -- STOP')
    return p


# ---------------------------------------------------------------- convert

def csv_rows(zpath):
    """Independent data-row count: newline count of the decompressed entry minus header."""
    with zipfile.ZipFile(zpath) as z:
        names = z.namelist()
        assert len(names) == 1, names
        n, last, first = 0, b'\n', None
        with z.open(names[0]) as fh:
            while chunk := fh.read(1 << 24):
                if first is None:
                    first = chunk[:64]
                n += chunk.count(b'\n')
                last = chunk[-1:]
        if last != b'\n':
            n += 1
        header = first is not None and not first[:1].isdigit()
        return n - (1 if header else 0), header


def to_parquet(zpath, dest, cols, types, header, ts_col):
    with zipfile.ZipFile(zpath) as z, z.open(z.namelist()[0]) as fh:
        ro = pcsv.ReadOptions(column_names=cols, skip_rows=1 if header else 0,
                              block_size=1 << 25)
        co = pcsv.ConvertOptions(column_types=types, strings_can_be_null=False,
                                 timestamp_parsers=['%Y-%m-%d %H:%M:%S'])
        rd = pcsv.open_csv(fh, read_options=ro, convert_options=co)
        tmp = dest.with_suffix('.parquet.tmp')
        w = None
        lo = hi = None
        try:
            for b in rd:
                if w is None:
                    w = pq.ParquetWriter(tmp, b.schema, compression='zstd')
                w.write_batch(b)
                col = b.column(ts_col)
                if len(col):
                    mn, mx = pc.min(col).as_py(), pc.max(col).as_py()
                    lo = mn if lo is None or mn < lo else lo
                    hi = mx if hi is None or mx > hi else hi
        finally:
            if w is not None:
                w.close()
        os.replace(tmp, dest)
    return pq.ParquetFile(dest).metadata.num_rows, lo, hi


def ts_ms(v):
    return int(v.replace(tzinfo=dt.timezone.utc).timestamp() * 1000) if isinstance(v, dt.datetime) else int(v)  # naive = UTC


# ---------------------------------------------------------------- fetch

lock = threading.Lock()


def load_manifest():
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text())
    return {'source': DL_BASE, 'note': 'SHA-256 pinned at download; Binance regenerates files.',
            'files': {}}


def save_manifest(m):
    tmp = MANIFEST.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(m, indent=1, sort_keys=True))
    os.replace(tmp, MANIFEST)


def one(f, man):
    ds, sym, date = f['dataset'], f['symbol'], f['date']
    guard(date)
    rid = f'{ds}/{sym}/{date}'
    with lock:
        if man['files'].get(rid, {}).get('status') == 'ok':
            return rid, 'skip'
    zdir = UM / ds / sym
    zdir.mkdir(parents=True, exist_ok=True)
    name = f['key'].rsplit('/', 1)[-1]
    zpath = zdir / name
    url = DL_BASE + f['key']
    guard_url(url + '.CHECKSUM')
    cs = http_get(url + '.CHECKSUM').decode().split()
    assert len(cs) == 2 and cs[1] == name, cs
    guard_url(url)
    sha, nbytes = http_get(url, zpath)
    rec = {'key': f['key'], 'listing_size': f['size'], 'bytes': nbytes, 'sha256': sha,
           'checksum_file': cs[0],
           'downloaded_at': dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}
    if sha != cs[0]:
        rec['status'] = 'checksum_fail'
        zpath.unlink(missing_ok=True)
        return rid, rec
    nrows, header = csv_rows(zpath)
    pdir = UM / f'{ds}_parquet' / sym
    pdir.mkdir(parents=True, exist_ok=True)
    ppath = pdir / f'date={date}.parquet'
    if ds == 'aggTrades':
        prow, lo, hi = to_parquet(zpath, ppath, AGG_COLS, AGG_TYPES, header, 'transact_time')
    else:
        prow, lo, hi = to_parquet(zpath, ppath, DEPTH_COLS, DEPTH_TYPES, header, 'timestamp')
    d0 = dt.datetime.combine(dt.date.fromisoformat(date), dt.time(), dt.timezone.utc)
    day_lo, day_hi = int(d0.timestamp() * 1000), int(d0.timestamp() * 1000) + 86_400_000
    lo_ms, hi_ms = (ts_ms(lo), ts_ms(hi)) if lo is not None else (None, None)
    rec.update(csv_header=header, rows_csv=nrows, rows_parquet=prow, ts_min_ms=lo_ms,
               ts_max_ms=hi_ms, ts_in_day=lo_ms is not None and day_lo <= lo_ms and hi_ms < day_hi,
               parquet=str(ppath.relative_to(OUT)))
    if prow != nrows:
        rec['status'] = 'rowcount_mismatch'
        return rid, rec
    if ds == 'aggTrades':
        zpath.unlink()
        rec['zip_deleted'] = True
    else:
        rec['zip_deleted'] = False
    rec['status'] = 'ok'
    return rid, rec


def fetch():
    p = json.loads(PLAN.read_text())
    assert p['total_bytes'] <= TOTAL_STOP, 'plan over 26 GB'
    files = p['files']
    for f in files:
        guard(f['date'])
    man = load_manifest()
    t0, done = time.time(), 0
    with cf.ThreadPoolExecutor(WORKERS) as ex:
        futs = {ex.submit(one, f, man): f for f in files}
        for fu in cf.as_completed(futs):
            f = futs[fu]
            rid = f"{f['dataset']}/{f['symbol']}/{f['date']}"
            try:
                rid, rec = fu.result()
            except Exception as e:  # noqa: BLE001
                rec = {'status': 'error', 'error': repr(e)[:300]}
            done += 1
            if rec == 'skip':
                continue
            with lock:
                man['files'][rid] = rec
                if done % 10 == 0 or rec.get('status') != 'ok':
                    save_manifest(man)
            if rec.get('status') != 'ok' or done % 50 == 0:
                print(f'[{done}/{len(files)} {time.time() - t0:.0f}s] {rid} {rec.get("status")}',
                      flush=True)
    save_manifest(man)
    bad = {k: v['status'] for k, v in man['files'].items() if v.get('status') != 'ok'}
    print('done', len(man['files']), 'records; not ok:', bad)


def report():
    man = load_manifest()
    agg = {}
    for rid, r in man['files'].items():
        ds, sym, _ = rid.split('/')
        s = agg.setdefault(f'{ds}/{sym}', {'ok': 0, 'bad': 0, 'bytes': 0, 'rows': 0,
                                           'ts_out_of_day': 0})
        if r.get('status') == 'ok':
            s['ok'] += 1
            s['bytes'] += r['bytes']
            s['rows'] += r['rows_parquet']
            s['ts_out_of_day'] += 0 if r['ts_in_day'] else 1
        else:
            s['bad'] += 1
    for k, v in sorted(agg.items()):
        print(k, v)
    print('total bytes', sum(v['bytes'] for v in agg.values()))


if __name__ == '__main__':
    {'plan': plan, 'fetch': fetch, 'report': report}[sys.argv[1]]()
