"""Registration 010 (L2, cross-venue funding carry) — bounded, cached fetchers. Behaviour data only.

Every request carries an end bound before CUT (2026-07-01T00:00Z). Rows dated >= CUT are dropped at
load, before anything is saved or printed; the dropped count is logged (count only). Price and premium
series are fetched for the DISCOVERY span only (ending PRICE_END = 2025-01-01): the holdout's prices
are not on disk. Funding is fetched to CUT, because counting qualifying entries in the holdout is
behaviour (the registration's §3); Binance 8h klines are fetched to CUT for their quote volume (the
liquidity floor), and the loader exposes only volume after PRICE_END.

Downloads land in data/carry/<source>/ (gitignored). Run with -I:

  .venv/bin/python -I r010_fetch.py binance-list   # S3 names of months per symbol (names only)
  .venv/bin/python -I r010_fetch.py binance        # fundingRate, klines 8h, premiumIndexKlines 8h
  .venv/bin/python -I r010_fetch.py hl disc|hold   # Hyperliquid fundingHistory (rate, premium)
  .venv/bin/python -I r010_fetch.py bybit          # funding, kline 240 + premium 240 (disc), kline D
  .venv/bin/python -I r010_fetch.py okx            # monthly funding files, 4H candles + index (disc), 1D
  .venv/bin/python -I r010_fetch.py errsample      # Binance 1h premium for the core gap-error sample
"""
import json
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scouts'))
import socket  # noqa: E402

from ct.net import RatePacer, WeightBudget, get_json  # noqa: E402
from l2_fetch import _zip_rows, base_of, hl_name_from_bn  # noqa: E402

socket.setdefaulttimeout(60)       # no socket read may block longer than this (a machine sleep hung one)

CUT = 1_782_864_000_000            # 2026-07-01 00:00 UTC: nothing at or after is kept
LIMIT = CUT                        # raised ONLY by holdout_gate() after REGISTERED + a discovery pass
HOLDOUT_FETCH_START = 1_730_419_200_000   # 2024-11-01: holdout pull starts here (windows, seasoning, volume)
HOLDOUT_END = {'A': 1_790_812_800_000, 'B': 1_798_761_600_000}   # 2026-10-01 / 2027-01-01, exclusive
PRICE_END = 1_735_689_600_000      # 2025-01-01 00:00 UTC: price/premium fetched only before this
START = 1_684_972_800_000          # 2023-05-25 00:00 UTC (two weeks before HL's hourly start)
HOUR = 3_600_000
DAY = 86_400_000
OUT = ROOT / 'data' / 'carry'
INPUTS = OUT / 'inputs'            # snapshots of the scout's two NAME lists (copied once, 2026-10-11)
ALLOWED = ROOT / 'data' / 'carry'  # 010 reads and writes only here (whitelist, 000g amendment)
assert OUT.resolve() == ALLOWED.resolve() and INPUTS.resolve().is_relative_to(ALLOWED.resolve())
BV = 'https://data.binance.vision/data'
EXPOSURE = {'dropped_rows': 0}
_lock = threading.Lock()


def ym_list(y0=2023, m0=5, y1=2026, m1=6):
    out, y, m = [], y0, m0
    while (y, m) <= (y1, m1):
        out.append(f'{y:04d}-{m:02d}')
        y, m = (y + (m == 12), m % 12 + 1)
    return out


def ym_start(ym):
    y, m = map(int, ym.split('-'))
    return int(datetime(y, m, 1, tzinfo=timezone.utc).timestamp() * 1000)


def drop(rows, tkey, end=None):
    end = LIMIT if end is None else min(end, LIMIT)
    keep = [r for r in rows if tkey(r) < end]
    if len(keep) != len(rows) and end == LIMIT:
        with _lock:
            EXPOSURE['dropped_rows'] += len(rows) - len(keep)
    return keep


def _save(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(obj))
    tmp.replace(path)


def _memo(path, fetch):
    """Cache a response on disk. In a holdout or extension fetch (LIMIT past 2026-07-01) an EMPTY
    answer (a 404 or an unknown symbol) is never cached: a month fetched too early must be retried,
    not lost (Revision 2, N2)."""
    if path.exists():
        return json.loads(path.read_text())
    v = fetch()
    if (v is None or v == []) and LIMIT > CUT:
        return v
    _save(path, v)
    return v


# ------------------------------------------------------------------ universe (names only)
def binance_symbols():
    """Binance USDT-M symbols (names from the scout's S3 listing, delisted included)."""
    return [s for s in json.loads((INPUTS / 'um_fundingRate_symbols.json').read_text())
            if s.endswith('USDT')]


def binance_months():
    """{symbol: [YYYY-MM with a fundingRate file, 2023-05 .. 2026-06]} from S3 key names only."""
    from h2_binance_scout import list_objects
    path = OUT / 'binance' / 'months.json'

    def fetch():
        out = {}
        syms = binance_symbols()

        def one(s):
            for attempt in range(8):
                try:
                    keys = [k for t, k, _ in list_objects(f'data/futures/um/monthly/fundingRate/{s}/')
                            if k.endswith('.zip')]
                    break
                except (OSError, urllib.error.URLError):
                    time.sleep(min(30, 2 ** attempt))
            else:
                raise RuntimeError(f'listing failed for {s}')
            return s, sorted(k[-11:-4] for k in keys if '2023-05' <= k[-11:-4] <= '2026-06')
        with ThreadPoolExecutor(4) as ex:
            for s, ms in ex.map(one, syms):
                if ms:
                    out[s] = ms
        return out
    return _memo(path, fetch)


def hl_coin_for(sym):
    """HL coin name for a Binance symbol, if HL ever listed it (scout probe hl_first.json)."""
    first = {c for c, t in json.loads((INPUTS / 'hl_first.json').read_text()).items() if t}
    base = sym[:-4]
    for c in (hl_name_from_bn(sym), base, 'k' + base[4:] if base.startswith('1000') else None, 'k' + base):
        if c and c in first:
            return c
    return None


def okx_family_candidates(sym):
    base = sym[:-4]
    if base.startswith('1000'):
        return [f'{base[4:]}-USDT', f'{base}-USDT']
    return [f'{base}-USDT']


def bybit_candidates(sym):
    base = sym[:-4]
    if base.startswith('1000'):
        return [sym, f'{base[4:]}1000USDT', f'{base[4:]}USDT']
    return [sym, f'1000{base}USDT']


# ------------------------------------------------------------------ Binance (data.binance.vision)
def bn_file(kind, sym, ym, interval=None):
    """kind: fundingRate | klines | premiumIndexKlines. Rows: funding [t, interval_h, rate];
    klines [open_t, open, high, low, close, quote_volume]; premium [open_t, close]."""
    assert ym_start(ym) < LIMIT
    sub = f'{kind}/{sym}' + (f'/{interval}' if interval else '')
    fname = f'{sym}-{interval}-{ym}.zip' if interval else f'{sym}-{kind}-{ym}.zip'
    path = OUT / 'binance' / kind / (f'{sym}_{interval}_{ym}.json' if interval else f'{sym}_{ym}.json')

    def fetch():
        rows, size = _zip_rows(f'{BV}/futures/um/monthly/{urllib.parse.quote(sub)}/{urllib.parse.quote(fname)}')
        if rows is None:
            return None
        rows = [r for r in rows if r and r[0][:1].isdigit()]
        if kind == 'fundingRate':
            out = [[int(r[0]), float(r[1]) if r[1] else None, float(r[2])] for r in rows]
        elif kind == 'klines':
            out = [[int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[7])] for r in rows]
        else:
            out = [[int(r[0]), float(r[4])] for r in rows]
        return {'bytes': size, 'rows': drop(out, lambda r: r[0])}
    v = _memo(path, fetch)
    if v:
        v['rows'] = drop(v['rows'], lambda r: r[0])
    return v


def binance():
    months = binance_months()
    jobs = []
    for s, ms in months.items():
        for m in ms:
            jobs.append(('fundingRate', s, m, None))
            jobs.append(('klines', s, m, '8h'))
            if ym_start(m) < PRICE_END:
                jobs.append(('premiumIndexKlines', s, m, '8h'))
    print('binance jobs', len(jobs), flush=True)
    done = [0]

    def run(j):
        bn_file(*j)
        with _lock:
            done[0] += 1
            if done[0] % 2000 == 0:
                print('  binance', done[0], flush=True)
    with ThreadPoolExecutor(16) as ex:
        list(ex.map(run, jobs))
    print('binance done; dropped holdout rows', EXPOSURE['dropped_rows'], flush=True)


# ------------------------------------------------------------------ Hyperliquid
SPANS = {'disc': (START, PRICE_END), 'hold': (PRICE_END, CUT), 'h2': (HOLDOUT_FETCH_START, None)}


_hl_budget = WeightBudget(1100)


def _hl_info(body, retries=10):
    """POST to HL /info with a 30 s per-request timeout and bounded retries (backoff up to 60 s).
    HTTP 500 twice in a row = unknown coin -> []. Raises after `retries` failures, so a stalled
    connection can never hang the fetch: rerunning resumes from the per-coin cache."""
    n500 = 0
    for attempt in range(retries + 1):
        _hl_budget.spend(20)
        req = urllib.request.Request('https://api.hyperliquid.xyz/info', data=json.dumps(body).encode(),
                                     headers={'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                res = json.load(r)
            if isinstance(res, list):
                _hl_budget.spend(len(res) // 20)
            return res
        except urllib.error.HTTPError as e:
            if e.code == 500:
                n500 += 1
                if n500 >= 2:
                    return []
            elif e.code != 429 and e.code < 500:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
            pass
        time.sleep(min(60, 2 ** attempt))
    raise RuntimeError(f'HL info failed after {retries} retries: {body.get("coin")}')


def hl_funding(coin, span):
    """[time, rate, premium] for coin over the span (end clipped below CUT)."""
    t0, t1 = SPANS[span]
    t1 = min(t1 or LIMIT, LIMIT) - 1
    path = OUT / 'hl' / (f'{coin}_{span}.json' if span != 'h2' else f'{coin}_h2_{t1 + 1}.json')

    def fetch():
        out, t = [], t0
        while t <= t1:
            assert t1 < LIMIT
            page = _hl_info({'type': 'fundingHistory', 'coin': coin, 'startTime': t, 'endTime': t1})
            if not isinstance(page, list) or not page:
                break
            out += [[r['time'], float(r['fundingRate']), float(r['premium'])] for r in page]
            if len(page) < 500 or page[-1]['time'] >= t1 - HOUR:
                break
            t = page[-1]['time'] + 1
        return drop(out, lambda r: r[0])
    return drop(_memo(path, fetch), lambda r: r[0])


def hl_candles(coin, interval='8h', t0=START, t1=None):
    """HL candleSnapshot (last-trade OHLCV) for bars starting in [t0, t1), t1 <= CUT, always with an
    explicit endTime before 2026-07-01. HL serves the latest 5,000 bars per interval; 8h bars from
    2023-05 fit (3,320). Rows [t_open, o, h, l, c, v_coin]."""
    t1 = t1 or CUT
    assert t1 <= LIMIT
    path = OUT / 'hl' / 'candles' / f'{coin}_{interval}_{t0}_{t1}.json'

    def fetch():
        out, t = [], t0
        while t < t1:
            page = _hl_info({'type': 'candleSnapshot',
                             'req': {'coin': coin, 'interval': interval, 'startTime': t, 'endTime': t1 - 1}})
            if not isinstance(page, list) or not page:
                break
            out += [[r['t'], float(r['o']), float(r['h']), float(r['l']), float(r['c']), float(r['v'])] for r in page]
            if len(page) < 5000:
                break
            t = page[-1]['t'] + 1
        return sorted(drop([r for r in out if r[0] >= t0], lambda r: r[0], end=t1))
    v = _memo(path, fetch)
    return drop(v, lambda r: r[0], end=t1)


def hlcandles():
    coins = sorted({hl_coin_for(s) for s in binance_months()} - {None})
    print('hl candles coins', len(coins), flush=True)
    empty = 0
    for i, c in enumerate(coins):
        empty += not hl_candles(c)
        if i % 25 == 0:
            print('  hl candles', i, len(coins), flush=True)
    print('hl candles done; coins without candles', empty, '; dropped holdout rows', EXPOSURE['dropped_rows'], flush=True)


def hl_liquid_in(coins, t0, t1, floor=10_000_000.0, days=30):
    """HL coins whose own trailing 30-day mean daily quote volume (8h candles, volume x close) reaches
    `floor` on some day in [t0, t1): only these can form an eligible HL pair there, so only these
    need funding (a cost saving; the eligibility rule itself is unchanged). Levels only."""
    out = []
    for c in coins:
        rows = hl_candles(c)
        vol = {}
        for o, op, hi, lo, cl, v in rows:
            vol[o - o % DAY] = vol.get(o - o % DAY, 0.0) + v * cl
        d = t0 - t0 % DAY
        while d < t1:
            if sum(vol.get(d - i * DAY, 0.0) for i in range(1, days + 1)) / days >= floor:
                out.append(c)
                break
            d += DAY
    return out


def hl(span):
    first = {c: t for c, t in json.loads((INPUTS / 'hl_first.json').read_text()).items() if t}
    coins = sorted({hl_coin_for(s) for s in binance_months()} - {None})
    t0, t1 = SPANS[span]
    coins = [c for c in coins if first[c] < (t1 or LIMIT)]
    if '--liquid' in sys.argv:
        coins = hl_liquid_in(coins, 1_736_899_200_000, t1 or LIMIT)
    print('hl', span, 'coins', len(coins), flush=True)
    for i, c in enumerate(coins):
        hl_funding(c, span)
        if i % 20 == 0:
            print('  hl', span, i, len(coins), flush=True)
    print('hl', span, 'done; dropped holdout rows', EXPOSURE['dropped_rows'], flush=True)


# ------------------------------------------------------------------ Bybit (v5, bounded)
_by_pace = RatePacer(8)


def _by_get(path, q):
    _by_pace.wait()
    return get_json(f'https://api.bybit.com/v5/market/{path}?{urllib.parse.urlencode(q)}')


def by_funding(sym, t0=START, t1=None):
    """[time, rate] in [t0, t1) (default [START, CUT)), or None if Bybit does not know the symbol."""
    t1 = t1 or CUT
    assert t1 <= LIMIT
    path = OUT / 'bybit' / 'funding' / (f'{sym}.json' if (t0, t1) == (START, CUT) else f'{sym}_{t0}_{t1}.json')

    def fetch():
        out, end = [], t1 - 1
        while end >= t0:
            res = _by_get('funding/history', {'category': 'linear', 'symbol': sym, 'startTime': t0,
                                              'endTime': end, 'limit': 200})
            if res.get('retCode') == 10001:
                return None
            if res.get('retCode') != 0:
                raise RuntimeError(f"bybit {sym}: {res.get('retCode')} {res.get('retMsg')}")
            page = res['result']['list']
            out += [[int(r['fundingRateTimestamp']), float(r['fundingRate'])] for r in page]
            if len(page) < 200:
                break
            end = min(int(r['fundingRateTimestamp']) for r in page) - 1
        return sorted(drop(out, lambda r: r[0]))
    v = _memo(path, fetch)
    return None if v is None else drop(v, lambda r: r[0])


def by_kline(sym, kind, interval, t0, t1):
    """kind: kline -> [start, open, high, low, close, turnover]; premium -> [start, close].
    Bars with start in [t0, t1), t1 <= LIMIT."""
    assert t1 <= LIMIT
    ep = 'kline' if kind == 'kline' else 'premium-index-price-kline'
    path = OUT / 'bybit' / kind / f'{sym}_{interval}_{t0}_{t1}.json'

    def fetch():
        out, end = [], t1 - 1
        while end >= t0:
            res = _by_get(ep, {'category': 'linear', 'symbol': sym, 'interval': interval, 'start': t0,
                               'end': end, 'limit': 1000})
            if res.get('retCode') != 0:
                return None
            page = res['result']['list']
            if kind == 'kline':
                out += [[int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[6])] for r in page]
            else:
                out += [[int(r[0]), float(r[4])] for r in page]
            if len(page) < 1000:
                break
            end = min(int(r[0]) for r in page) - 1
        return sorted(drop([r for r in out if r[0] >= t0], lambda r: r[0], end=t1))
    v = _memo(path, fetch)
    return None if v is None else drop(v, lambda r: r[0], end=t1)


def bybit():
    syms = sorted(binance_months())
    found = {}

    def one(s):
        for c in bybit_candidates(s):
            v = by_funding(c)
            if v:
                return s, c
        return s, None
    with ThreadPoolExecutor(8) as ex:
        for i, (s, c) in enumerate(ex.map(one, syms)):
            if c:
                found[s] = c
            if i % 100 == 0:
                print('  bybit funding', i, len(syms), len(found), flush=True)
    _save(OUT / 'bybit' / 'symbols.json', found)

    def px(item):
        s, c = item
        by_kline(c, 'kline', 'D', START, CUT)
        by_kline(c, 'kline', '240', START, PRICE_END)
        by_kline(c, 'premium', '240', START, PRICE_END)
    with ThreadPoolExecutor(8) as ex:
        for i, _ in enumerate(ex.map(px, sorted(found.items()))):
            if i % 100 == 0:
                print('  bybit klines', i, len(found), flush=True)
    print('bybit done; symbols', len(found), 'dropped holdout rows', EXPOSURE['dropped_rows'], flush=True)


# ------------------------------------------------------------------ OKX
def okx_funding(fam, ym):
    """[time, rate] from OKX's monthly file (month in UTC+8), or None if absent."""
    assert ym <= '2026-06'
    path = OUT / 'okx' / 'funding' / f'{fam}_{ym}.json'
    y, m = ym.split('-')

    def fetch():
        import zipfile
        url = (f'https://static.okx.com/cdn/okex/traderecords/swaprates/monthly/{y}{m}/'
               f'{urllib.parse.quote(fam)}-SWAP-fundingrates-{ym}.zip')
        try:
            rows, size = _zip_rows(url)
        except (zipfile.BadZipFile, urllib.error.HTTPError):
            return None
        if rows is None:
            return None
        rows = [r for r in rows[1:] if len(r) >= 3 and r[1] not in ('', 'None') and r[2].isdigit()]
        return {'bytes': size, 'rows': drop([[int(r[2]), float(r[1])] for r in rows], lambda r: r[0])}
    v = _memo(path, fetch)
    if v:
        v['rows'] = drop(v['rows'], lambda r: r[0])
    return v


_okx_c = RatePacer(8)       # history-candles: 20 req / 2 s
_okx_i = RatePacer(4)       # history-index-candles: 10 req / 2 s


def _okx_get(url, retries=8):
    """OKX's API refuses urllib's default User-Agent (403); a browser UA is accepted."""
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if isinstance(e, urllib.error.HTTPError) and e.code not in (429, 500, 502, 503, 504):
                raise
            if attempt == retries:
                raise
            time.sleep(min(30, 2 ** attempt))


def okx_candles(inst, kind, bar, t0, t1):
    """kind: last (history-candles) -> [ts, open, high, low, close, vol_quote]; index -> [ts, close].
    Bars with ts in [t0, t1), paged backwards from t1 (after = t1: rows strictly older)."""
    assert t1 <= LIMIT
    ep, pace = ('history-candles', _okx_c) if kind == 'last' else ('history-index-candles', _okx_i)
    path = OUT / 'okx' / kind / f'{inst}_{bar}_{t0}_{t1}.json'

    def fetch():
        out, after = [], t1
        while after > t0:
            pace.wait()
            res = _okx_get(f'https://www.okx.com/api/v5/market/{ep}?' + urllib.parse.urlencode(
                {'instId': inst, 'bar': bar, 'after': after, 'limit': 100}))
            if res.get('code') != '0':
                if res.get('code') in ('51001', '51000'):
                    return None
                time.sleep(2)
                continue
            page = res['data']
            if not page:
                break
            if kind == 'last':
                out += [[int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[7])] for r in page]
            else:
                out += [[int(r[0]), float(r[4])] for r in page]
            after = min(int(r[0]) for r in page)
            if len(page) < 100:
                break
        return sorted(drop([r for r in out if r[0] >= t0], lambda r: r[0], end=t1))
    v = _memo(path, fetch)
    return None if v is None else drop(v, lambda r: r[0], end=t1)


def okx():
    syms = sorted(binance_months())
    jobs = [(s, fam, m) for s in syms for fam in okx_family_candidates(s) for m in ym_list()]
    print('okx funding jobs', len(jobs), flush=True)
    with ThreadPoolExecutor(16) as ex:
        res = []
        for i, r in enumerate(ex.map(lambda j: (j, okx_funding(j[1], j[2])), jobs)):
            res.append(r)
            if i % 3000 == 0:
                print('  okx funding', i, flush=True)
    fams = {}
    for (s, fam, m), v in res:
        if v and v['rows']:
            fams.setdefault(s, {}).setdefault(fam, []).append(m)
    found = {s: max(d, key=lambda f: len(d[f])) for s, d in fams.items()}
    _save(OUT / 'okx' / 'symbols.json', found)
    print('okx funding symbols', len(found), flush=True)
    hl_syms = {s for s in found if hl_coin_for(s)}

    def px(item):
        s, fam = item
        inst = f'{fam}-SWAP'
        okx_candles(inst, 'last', '1D', START, CUT)
        okx_candles(inst, 'last', '4H', START, PRICE_END)
        if s in hl_syms:
            okx_candles(fam, 'index', '4H', START, PRICE_END)
    with ThreadPoolExecutor(4) as ex:
        for i, _ in enumerate(ex.map(px, sorted(found.items()))):
            if i % 50 == 0:
                print('  okx candles', i, len(found), flush=True)
    print('okx done; dropped holdout rows', EXPOSURE['dropped_rows'], flush=True)


# ------------------------------------------------------------------ gap-error sample
ERR_MONTHS = [f'2025-{m:02d}' for m in range(8, 13)] + [f'2026-{m:02d}' for m in range(1, 7)]


def errsample():
    """Binance 1h premium index for BTC/ETH/SOL in the months of the HL-archive sample (levels only;
    used to measure the HL price-gap model error against HL's own trade prints)."""
    for s in ('BTCUSDT', 'ETHUSDT', 'SOLUSDT'):
        for m in ERR_MONTHS:
            bn_file('premiumIndexKlines', s, m, '1h')
    print('errsample done; dropped holdout rows', EXPOSURE['dropped_rows'], flush=True)


# ------------------------------------------------------------------ the decisive holdout (gated)
REG = ROOT / 'registrations' / '010-cross-venue-funding-carry.md'


def holdout_gate(kind='holdout', stage='fetch'):
    """The ONLY way past 2026-07-01 (Revision 2, N2): r010_run's guard in mode <kind>_<stage> —
    REGISTERED and committed, the owner's two anchored lines, exactly one committed 010 discovery row
    whose decision recomputes to read_holdout from its numbers, the owner's approval line quoting
    that row's sha256 committed after it, now >= end + 7 days, and no started read. Raises LIMIT to
    the end (exclusive) and returns (end, tag). Anything else exits."""
    global LIMIT
    import r010_run as run
    fails, commit = run.guard_failures(ROOT, str(REG.relative_to(ROOT)), mode=f'{kind}_{stage}')
    if fails:
        sys.exit('\n'.join(fails))
    end, date, ext, _ = run.owner_choice(REG.read_text())
    if kind == 'extension':
        end, date = run.carry.EXTENSION_END, '2027-06-30'
    LIMIT = end
    return end, f'{kind}_{date}', commit


def started_row(kind, commit):
    """Append <kind>_fetch_started once, before the first request (N2, m-a)."""
    import r010_run as run
    if not any(r.get('mode') == f'{kind}_fetch_started' for _, r in run.result_rows(ROOT, 'results.jsonl', REG.name)):
        run.append_row({'registration': REG.name, 'mode': f'{kind}_fetch_started', 'registration_commit': commit,
                        'run_at': datetime.now(timezone.utc).isoformat()})


def months_upto(tag, end):
    """Point-in-time Binance universe for the holdout (m-b): every USDT-M symbol in the S3 listing
    (names only, listed at fetch time) with fundingRate month files in [2024-11, the end month]."""
    from h2_binance_scout import list_objects
    path = OUT / 'binance' / f'months_{tag}.json'
    last = datetime.fromtimestamp((end - 1) / 1000, timezone.utc).strftime('%Y-%m')

    def fetch():
        syms = sorted(k.rstrip('/').rsplit('/', 1)[-1]
                      for t, k, _ in list_objects('data/futures/um/monthly/fundingRate/', '/') if t == 'pre')
        out = {}
        for s_ in (x for x in syms if x.endswith('USDT')):
            keys = [k for t, k, _ in list_objects(f'data/futures/um/monthly/fundingRate/{s_}/') if k.endswith('.zip')]
            ms = sorted(k[-11:-4] for k in keys if '2024-11' <= k[-11:-4] <= last)
            if ms:
                out[s_] = ms
        return out
    return _memo(path, fetch)


def holdout(kind='holdout'):
    """Pre-specified pull (Revision 1, M4; Revision 2, N2 and m-b): HL, Binance, Bybit, 2024-11-01 ->
    the end, for a universe rebuilt POINT IN TIME from holdout-period listings: Binance symbols with a
    funding file in the window, their Bybit symbol (probed with a bounded funding request) and HL
    coin (probed with a bounded fundingHistory request). Every request is bounded by the end; no empty
    answer is cached."""
    end, tag, commit = holdout_gate(kind, 'fetch')
    started_row(kind, commit)
    months = months_upto(tag, end)
    print(kind, tag, 'symbols', len(months), flush=True)
    with ThreadPoolExecutor(16) as ex:
        list(ex.map(lambda j: bn_file(*j), [(k, s_, m, iv) for s_, ms in months.items() for m in ms
                                            for k, iv in (('fundingRate', None), ('klines', '8h'))]))
    by, hlmap = {}, {}
    for s_ in sorted(months):
        for c in bybit_candidates(s_):
            if by_funding(c, HOLDOUT_FETCH_START, end):
                by[s_] = c
                by_kline(c, 'kline', '240', HOLDOUT_FETCH_START, end)
                by_kline(c, 'kline', 'D', HOLDOUT_FETCH_START, end)
                break
        base = s_[:-4]
        for c in dict.fromkeys(x for x in (hl_name_from_bn(s_), base, 'k' + base[4:] if base.startswith('1000') else None) if x):
            rows = hl_funding(c, 'h2')
            if rows:
                hlmap[s_] = c
                hl_candles(c, '8h', HOLDOUT_FETCH_START, end)
                break
    _save(OUT / 'bybit' / f'symbols_{tag}.json', by)
    _save(OUT / 'hl' / f'coins_{tag}.json', hlmap)
    print(kind, 'fetch done; rows dropped at the end bound', EXPOSURE['dropped_rows'], flush=True)


if __name__ == '__main__':
    cmd = sys.argv[1]
    if cmd == 'binance-list':
        print('symbols with months', len(binance_months()))
    elif cmd == 'binance':
        binance()
    elif cmd == 'hl':
        hl(sys.argv[2])
    elif cmd == 'hlcandles':
        hlcandles()
    elif cmd == 'holdout':
        holdout('holdout')
    elif cmd == 'extension':
        holdout('extension')
    elif cmd == 'bybit':
        bybit()
    elif cmd == 'okx':
        okx()
    elif cmd == 'errsample':
        errsample()
