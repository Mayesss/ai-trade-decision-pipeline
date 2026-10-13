"""L2 carry scout: bounded, cached fetchers for funding and klines (behaviour only).

Every request carries an end bound before CUT (2026-07-01T00:00:00Z) and every
row dated >= CUT is dropped at load, before anything is saved or printed. The
number of dropped rows is logged (count only) so an exposure can be ledgered.

Downloads land in research/copytrade/data/scout_l2/<source>/ (gitignored).
Run with the venv python and -I:

  .venv/bin/python -I scouts/l2_fetch.py universe     # S3 symbol list + HL probes
  .venv/bin/python -I scouts/l2_fetch.py hl           # HL windows + core full span
  .venv/bin/python -I scouts/l2_fetch.py binance      # Binance monthly fundingRate
  .venv/bin/python -I scouts/l2_fetch.py bybit        # Bybit v5 funding history
  .venv/bin/python -I scouts/l2_fetch.py okx          # OKX monthly funding files
  .venv/bin/python -I scouts/l2_fetch.py klines       # Binance spot + um 1h klines, core
"""
import csv
import io
import json
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]          # research/copytrade
sys.path.insert(0, str(ROOT))
from ct.net import RatePacer, WeightBudget, get_json, post_json  # noqa: E402

CUT = 1782864000000                                  # 2026-07-01T00:00:00Z
OUT = ROOT / 'data' / 'scout_l2'
HOUR = 3_600_000
HL_START = 1682899200000                             # 2023-05-01, before HL's first funding print
CORE = ['BTC', 'ETH', 'SOL']
WIN_H = 500                                          # one HL page of hourly rows
EXPOSURE = {'dropped_rows': 0}
_lock = threading.Lock()


def ms(y, m, d=1):
    return int(datetime(y, m, d, tzinfo=timezone.utc).timestamp() * 1000)


def windows():
    """Fixed calendar sample: 500 h from the 1st of Jan/Apr/Jul/Oct, 2023-07 .. 2026-04."""
    out = []
    for y in (2023, 2024, 2025, 2026):
        for m in (1, 4, 7, 10):
            t0 = ms(y, m)
            if ms(2023, 7) <= t0 and t0 + WIN_H * HOUR <= CUT:
                out.append(t0)
    return out


def months(y0=2023, m0=1):
    out, y, m = [], y0, m0
    while (y, m) < (2026, 7):
        out.append(f'{y:04d}-{m:02d}')
        y, m = (y + (m == 12), m % 12 + 1)
    return out


def drop_holdout(rows, tkey):
    keep = [r for r in rows if tkey(r) < CUT]
    if len(keep) != len(rows):
        with _lock:
            EXPOSURE['dropped_rows'] += len(rows) - len(keep)
    return keep


def _save(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(obj))
    tmp.replace(path)


def _memo(path, fetch):
    if path.exists():
        return json.loads(path.read_text())
    v = fetch()
    _save(path, v)
    return v


# ---------------------------------------------------------------- Hyperliquid
_hl_budget = WeightBudget(1100)


def _hl_info(body):
    """HL answers HTTP 500 for a coin it does not know; two 500s in a row mean 'unknown'."""
    n500 = 0
    for attempt in range(8):
        _hl_budget.spend(20)
        req = urllib.request.Request('https://api.hyperliquid.xyz/info', data=json.dumps(body).encode(),
                                     headers={'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                res = json.load(r)
            break
        except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
            if not isinstance(e, urllib.error.HTTPError):
                time.sleep(min(30, 2 ** attempt))
                continue
            if e.code == 500:
                n500 += 1
                if n500 >= 2:
                    return []
                time.sleep(2)
                continue
            if e.code == 429:
                time.sleep(30)
            res = post_json('https://api.hyperliquid.xyz/info', body)
            break
    if isinstance(res, list):
        _hl_budget.spend(len(res) // 20)
    return res


def hl_funding(coin, t0, t1, max_pages=None):
    """[time, rate, premium] for coin in [t0, t1), t1 clipped below CUT."""
    t1 = min(t1, CUT) - 1
    path = OUT / 'hl' / f'{coin}_{t0}_{t1}_{max_pages or 0}.json'

    def fetch():
        out, t, pages = [], t0, 0
        while t <= t1:
            page = _hl_info({'type': 'fundingHistory', 'coin': coin, 'startTime': t, 'endTime': t1})
            pages += 1
            if not isinstance(page, list) or not page:
                break
            out += [[r['time'], float(r['fundingRate']), float(r['premium'])] for r in page]
            if len(page) < 500 or (max_pages and pages >= max_pages) or page[-1]['time'] >= t1 - HOUR:
                break
            t = page[-1]['time'] + 1
        return drop_holdout(out, lambda r: r[0])
    return drop_holdout(_memo(path, fetch), lambda r: r[0])


# ---------------------------------------------------------------- Binance S3
BV = 'https://data.binance.vision/data'


def _zip_rows(url, retries=6):
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                raw = r.read()
            break
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None, 0
            if e.code not in (429, 500, 502, 503, 504) or attempt == retries:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt == retries:
                raise
        time.sleep(min(30, 2 ** attempt))
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        text = z.read(z.namelist()[0]).decode()
    return list(csv.reader(io.StringIO(text))), len(raw)


def bn_funding(sym, ym):
    """[calc_time, interval_h, rate] for a month < 2026-07, or None if no file."""
    assert ym < '2026-07'
    path = OUT / 'binance' / 'funding' / f'{sym}_{ym}.json'

    def fetch():
        rows, size = _zip_rows(f'{BV}/futures/um/monthly/fundingRate/{sym}/{sym}-fundingRate-{ym}.zip')
        if rows is None:
            return None
        if rows and rows[0][0] == 'calc_time':
            rows = rows[1:]
        return {'bytes': size, 'rows': drop_holdout(
            [[int(r[0]), float(r[1]) if r[1] else None, float(r[2])] for r in rows], lambda r: r[0])}
    v = _memo(path, fetch)
    if v:
        v['rows'] = drop_holdout(v['rows'], lambda r: r[0])
    return v


def bn_klines(market, sym, ym):
    """[open_time_ms, close] 1h klines, market in {'spot', 'um'}."""
    assert ym < '2026-07'
    path = OUT / 'binance' / f'klines_{market}' / f'{sym}_{ym}.json'
    base = f'{BV}/spot/monthly/klines' if market == 'spot' else f'{BV}/futures/um/monthly/klines'

    def fetch():
        rows, size = _zip_rows(f'{base}/{sym}/1h/{sym}-1h-{ym}.zip')
        if rows is None:
            return None
        out = []
        for r in rows:
            if not r or not r[0].isdigit():
                continue
            t = int(r[0])
            t = t // 1000 if t > 10 ** 14 else t       # spot files switched to microseconds in 2025
            out.append([t, float(r[4])])
        return {'bytes': size, 'rows': drop_holdout(out, lambda r: r[0])}
    v = _memo(path, fetch)
    if v:
        v['rows'] = drop_holdout(v['rows'], lambda r: r[0])
    return v


def bn_symbols():
    """USDT-M perp symbols with a fundingRate directory (names only, from the S3 listing)."""
    path = OUT / 'binance' / 'um_fundingRate_symbols.json'

    def fetch():
        sys.path.insert(0, str(ROOT / 'scouts'))
        from h2_binance_scout import list_objects
        return sorted(k.rstrip('/').rsplit('/', 1)[-1]
                      for t, k, _ in list_objects('data/futures/um/monthly/fundingRate/', '/') if t == 'pre')
    return _memo(path, fetch)


# ---------------------------------------------------------------- Bybit
_by_pace = RatePacer(4)


def by_funding(sym, t0, t1):
    """[time, rate] for a linear perp in [t0, t1), or None if the symbol is unknown."""
    t1 = min(t1, CUT) - 1
    path = OUT / 'bybit' / f'{sym}_{t0}_{t1}.json'

    def fetch():
        out, end = [], t1
        while end >= t0:
            _by_pace.wait()
            q = urllib.parse.urlencode({'category': 'linear', 'symbol': sym, 'startTime': t0,
                                        'endTime': end, 'limit': 200})
            res = get_json(f'https://api.bybit.com/v5/market/funding/history?{q}')
            if res.get('retCode') == 10001:          # "Symbol Invalid": not listed
                return None
            if res.get('retCode') != 0:
                raise RuntimeError(f"bybit {sym}: {res.get('retCode')} {res.get('retMsg')}")
            page = res['result']['list']
            out += [[int(r['fundingRateTimestamp']), float(r['fundingRate'])] for r in page]
            if len(page) < 200:
                break
            end = min(int(r['fundingRateTimestamp']) for r in page) - 1
        return sorted(drop_holdout(out, lambda r: r[0]))
    v = _memo(path, fetch)
    return None if v is None else drop_holdout(v, lambda r: r[0])


# ---------------------------------------------------------------- OKX
def okx_funding(fam, ym):
    """[time, rate] from OKX's monthly file (month in UTC+8), or None if absent."""
    assert ym < '2026-07'
    path = OUT / 'okx' / f'{fam}_{ym}.json'
    y, m = ym.split('-')

    def fetch():
        url = (f'https://static.okx.com/cdn/okex/traderecords/swaprates/monthly/{y}{m}/'
               f'{fam}-SWAP-fundingrates-{ym}.zip')
        try:
            rows, size = _zip_rows(url)
        except (zipfile.BadZipFile, urllib.error.HTTPError):
            return None
        if rows is None:
            return None
        rows = [r for r in rows[1:] if len(r) >= 3 and r[1] not in ('', 'None') and r[2].isdigit()]
        return {'bytes': size, 'rows': drop_holdout([[int(r[2]), float(r[1])] for r in rows], lambda r: r[0])}
    v = _memo(path, fetch)
    if v:
        v['rows'] = drop_holdout(v['rows'], lambda r: r[0])
    return v


# ---------------------------------------------------------------- universe
def hl_name_from_bn(sym):
    if not sym.endswith('USDT'):
        return None
    base = sym[:-4]
    if base.startswith('1000000') or base.startswith('1M'):
        return None
    if base.startswith('1000'):
        return 'k' + base[4:]
    return base


def base_of(hl):
    return hl[1:] if hl.startswith('k') and hl[1:].isupper() else hl


def universe():
    bn = bn_symbols()
    arch = [r[0] for r in json.loads((OUT / 'hl_archive_coins.json').read_text())]
    cands = sorted(set(arch) | {h for h in map(hl_name_from_bn, bn) if h})
    first = {}
    for i, c in enumerate(cands):
        rows = hl_funding(c, HL_START, CUT, max_pages=1)
        first[c] = rows[0][0] if rows else None
        if i % 50 == 0:
            print('probe', i, len(cands), flush=True)
    _save(OUT / 'hl_first.json', first)
    print('candidates', len(cands), 'on HL', sum(v is not None for v in first.values()),
          'dropped holdout rows', EXPOSURE['dropped_rows'])


def hl_coins():
    return {c: t for c, t in json.loads((OUT / 'hl_first.json').read_text()).items() if t is not None}


def hl():
    coins = hl_coins()
    for c in CORE:
        hl_funding(c, HL_START, CUT)
        print('core', c, flush=True)
    for i, (c, t) in enumerate(sorted(coins.items())):
        for w in windows():
            if t < w + WIN_H * HOUR:
                hl_funding(c, w, w + WIN_H * HOUR)
        if i % 25 == 0:
            print('hl windows', i, len(coins), flush=True)
    print('dropped holdout rows', EXPOSURE['dropped_rows'])


def _ym(t):
    return datetime.fromtimestamp(t / 1000, timezone.utc).strftime('%Y-%m')


def binance():
    coins, bn = hl_coins(), set(bn_symbols())
    jobs = []
    for c in coins:
        b = base_of(c)
        for sym in (f'1000{b}USDT', f'{b}USDT') if c.startswith('k') else (f'{c}USDT',):
            if sym in bn:
                ms_ = months() if c in CORE else sorted({_ym(w) for w in windows()})
                jobs += [(sym, m) for m in ms_]
                break
    with ThreadPoolExecutor(6) as ex:
        list(ex.map(lambda j: bn_funding(*j), jobs))
    print('binance files', len(jobs), 'dropped holdout rows', EXPOSURE['dropped_rows'])


def bybit_sym_candidates(c):
    b = base_of(c)
    return [f'1000{b}USDT', f'{b}1000USDT', f'{b}USDT'] if c.startswith('k') else [f'{c}USDT']


def bybit():
    coins = hl_coins()
    for i, c in enumerate(sorted(coins)):
        for sym in bybit_sym_candidates(c):
            if c in CORE:
                v = by_funding(sym, ms(2023, 1), CUT)
            else:
                v = [by_funding(sym, w, w + WIN_H * HOUR) for w in windows()]
                v = None if all(x is None for x in v) else v
            if v is not None:
                break
        if i % 25 == 0:
            print('bybit', i, len(coins), flush=True)
    print('dropped holdout rows', EXPOSURE['dropped_rows'])


def okx():
    coins = hl_coins()
    jobs = []
    for c in coins:
        ms_ = months() if c in CORE else sorted({_ym(w) for w in windows()})
        jobs += [(f'{base_of(c)}-USDT', m) for m in ms_]
    with ThreadPoolExecutor(4) as ex:
        list(ex.map(lambda j: okx_funding(*j), jobs))
    print('okx files', len(jobs), 'dropped holdout rows', EXPOSURE['dropped_rows'])


def klines():
    jobs = [(mk, f'{c}USDT', m) for c in CORE for mk in ('spot', 'um') for m in months()]
    with ThreadPoolExecutor(6) as ex:
        list(ex.map(lambda j: bn_klines(*j), jobs))
    print('kline files', len(jobs), 'dropped holdout rows', EXPOSURE['dropped_rows'])


if __name__ == '__main__':
    {'universe': universe, 'hl': hl, 'binance': binance, 'bybit': bybit, 'okx': okx,
     'klines': klines}[sys.argv[1]]()
