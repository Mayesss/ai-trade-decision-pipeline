"""Registration 014 — the design-sized Deribit pull (free, keyless, end-bounded; well under 5 GB).

    .venv/bin/python -I r014_fetch.py pull data/r014/pull_<YYYYMMDD>            # a NEW, empty directory
    .venv/bin/python -I r014_fetch.py pull data/r014/pull_<YYYYMMDD> --resume   # finish an interrupted pull

What it fetches (every request carries an explicit end bound < 2026-07-01; any row stamped on or after
2026-07-01 is a hard error, never dropped silently):
  dvol/{BTC,ETH}.json      DVOL 1h candles, www.deribit.com get_volatility_index_data, 2021-03-01 ->
                           2026-06-30 23:00, fixed 30-day FORWARD windows; a `continuation` is an error
  perp/{BTC,ETH}.json      {BTC,ETH}-PERPETUAL 1h candles, get_tradingview_chart_data, same span; a
                           candle is kept only if it CLOSES inside its window
  funding/{BTC,ETH}.json   perpetual hourly funding, get_funding_rate_history, same span
  trades/{COIN}_{YYYY-MM-DD}.json   every option trade of the coin in [Friday 04:00, Friday 08:00) UTC,
                           history.deribit.com get_last_trades_by_currency_and_time, paged FORWARD by
                           timestamp (inclusive restart, de-duplicated by trade_id), for every Friday
                           2021-04-02 -> 2026-06-26: the entry marks ([E - 4 h, E)) and the settlement
                           window ([X - 30 min, X)) of every position in both calendars
NOT fetched: the full option tape; get_instruments (no date bound: it returns today's state) and
get_delivery_prices (no end bound, newest first): both would read past the cutoff.

Hygiene: errors raise a typed FetchError after retries and are NEVER written; a unit (one file) is
written atomically only when complete; nothing null or empty is cached as data (an empty answer for a
window that must have data is an error); tail coverage is checked (each series' last stamp); every
file is listed in SHA256SUMS and pull_report.json (requests, bytes, rows). The network is reached only
through `HTTP` (tests replace it).
"""
import datetime as dt
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from ct import shortvol as sv  # noqa: E402

WWW = 'https://www.deribit.com/api/v2/public/'
HIST = 'https://history.deribit.com/api/v2/public/'
CUTOFF_MS = sv.CUTOFF_MS
LAST_MS = CUTOFF_MS - 1
START_MS = sv.ms(2021, 3, 1)
FIRST_FRIDAY = sv.ms(2021, 4, 2, 8)
LAST_FRIDAY = sv.ms(2026, 6, 26, 8)
WINDOW_H = sv.MARK_WINDOW_H                     # [F 04:00, F 08:00)
STEP = 30 * sv.DAY
END_KEYS = ('end_timestamp',)
UA = {'User-Agent': 'Mozilla/5.0 (r014 research pull)'}
KEEP = ('timestamp', 'trade_id', 'trade_seq', 'instrument_name', 'price', 'mark_price', 'iv', 'index_price',
        'direction', 'amount')
DATA_ROOT = HERE / 'data' / 'r014'
STATS = {'requests': 0, 'bytes': 0}


class FetchError(RuntimeError):
    pass


def _urlopen_json(url):
    """The only network primitive. Returns (parsed JSON, n bytes); raises urllib errors."""
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=60) as r:
        body = r.read()
    return json.loads(body), len(body)


HTTP = _urlopen_json


def get_json(base, method, params, tries=6, pause=0.12):
    """One end-bounded GET. The end bound is asserted BEFORE any request; errors are retried then
    raised (never returned, so never cached)."""
    ends = [k for k in END_KEYS if k in params]
    if not ends:
        raise FetchError(f'{method}: no end bound in {params}')
    for k in ends:
        if not int(params[k]) < CUTOFF_MS:
            raise FetchError(f'{method}: {k}={params[k]} is not before the 2026-07-01 cutoff')
    url = base + method + '?' + urllib.parse.urlencode(params)
    last = None
    for i in range(tries):
        try:
            out, nb = HTTP(url)
            STATS['requests'] += 1
            STATS['bytes'] += nb
            if not isinstance(out, dict) or 'result' not in out:
                raise FetchError(f'{method}: no result in answer ({str(out)[:200]})')
            if out.get('error'):
                raise FetchError(f'{method}: api error {out["error"]}')
            if pause:
                time.sleep(pause)
            return out['result']
        except urllib.error.HTTPError as e:
            last = f'http {e.code}'
            if e.code not in (429, 500, 502, 503, 504):
                raise FetchError(f'{method}: {last}') from e
        except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as e:
            last = repr(e)
        except FetchError as e:
            last = str(e)
            if 'api error' in last:
                raise
        time.sleep(min(30, 2 ** i))
    raise FetchError(f'{method}: gave up after {tries} tries: {last}')


def assert_pre_cutoff(stamps, tag):
    bad = [t for t in stamps if int(t) >= CUTOFF_MS]
    if bad:
        raise FetchError(f'{tag}: {len(bad)} rows on/after 2026-07-01 — refusing (a holdout-calendar read)')


def windows(a=START_MS, b=LAST_MS, step=STEP):
    t = a
    while t <= b:
        yield t, min(t + step - 1, b)
        t += step


# ------------------------------------------------------------------ units
def fetch_dvol(coin):
    rows = []
    for a, b in windows():
        r = get_json(WWW, 'get_volatility_index_data', {'currency': coin, 'start_timestamp': a, 'end_timestamp': b,
                                                         'resolution': '3600'})
        if r.get('continuation') not in (None, 'none'):
            raise FetchError(f'dvol {coin} {sv.iso(a)}: continuation set — the window was truncated')
        assert_pre_cutoff([x[0] for x in r['data']], f'dvol {coin}')
        rows.extend(x for x in r['data'] if a <= x[0] <= b)
    rows = sorted({x[0]: x for x in rows}.values())
    if not rows:
        raise FetchError(f'dvol {coin}: no rows')
    if rows[-1][0] != sv.ms(2026, 6, 30, 23):
        raise FetchError(f'dvol {coin}: tail coverage — last row {sv.iso(rows[-1][0])}, expected 2026-06-30 23:00')
    return rows


def fetch_perp(coin):
    inst = f'{coin}-PERPETUAL'
    rows = []
    for a, b in windows():
        r = get_json(WWW, 'get_tradingview_chart_data', {'instrument_name': inst, 'start_timestamp': a,
                                                          'end_timestamp': b, 'resolution': '60'})
        if r.get('status') != 'ok':
            raise FetchError(f'perp {inst} {sv.iso(a)}: status {r.get("status")} (a missing window is an error)')
        assert_pre_cutoff(r['ticks'], f'perp {inst}')
        for i, tk in enumerate(r['ticks']):
            if a <= tk and tk + sv.HOUR <= b + 1:   # the candle closes inside the window
                rows.append([tk, r['open'][i], r['high'][i], r['low'][i], r['close'][i], r['volume'][i]])
    rows = sorted({x[0]: x for x in rows}.values())
    if rows[-1][0] != sv.ms(2026, 6, 30, 23):
        raise FetchError(f'perp {inst}: tail coverage — last candle {sv.iso(rows[-1][0])}, expected 2026-06-30 23:00')
    return rows


def fetch_funding(coin):
    inst = f'{coin}-PERPETUAL'
    rows = []
    for a, b in windows():
        r = get_json(WWW, 'get_funding_rate_history', {'instrument_name': inst, 'start_timestamp': a,
                                                        'end_timestamp': b})
        if not r:
            raise FetchError(f'funding {inst} {sv.iso(a)}: empty window (never cached as data)')
        assert_pre_cutoff([x['timestamp'] for x in r], f'funding {inst}')
        rows.extend([x['timestamp'], x['interest_1h']] for x in r if a <= x['timestamp'] <= b)
    rows = sorted({x[0]: x for x in rows}.values())
    if rows[-1][0] < sv.ms(2026, 6, 30, 22):
        raise FetchError(f'funding {inst}: tail coverage — last row {sv.iso(rows[-1][0])}')
    return rows


def fetch_window(coin, start, end):
    """Every option trade of `coin` in [start, end], paged FORWARD by timestamp (inclusive restart,
    de-duplicated by trade_id). An answer with has_more and no new trade on one millisecond is an error."""
    if not end < CUTOFF_MS:
        raise FetchError('trade window past the cutoff')
    seen, out, t = set(), [], start
    while True:
        r = get_json(HIST, 'get_last_trades_by_currency_and_time',
                     {'currency': coin, 'kind': 'option', 'start_timestamp': t, 'end_timestamp': end,
                      'count': 1000, 'sorting': 'asc', 'include_old': 'true'}, pause=0.05)
        tr = r.get('trades')
        if tr is None:
            raise FetchError(f'trades {coin} {sv.iso(t)}: no trades key')
        assert_pre_cutoff([x['timestamp'] for x in tr], f'trades {coin}')
        new = [x for x in tr if x['trade_id'] not in seen]
        for x in new:
            seen.add(x['trade_id'])
            if not start <= x['timestamp'] <= end:
                raise FetchError(f'trades {coin}: a row outside the requested window')
            out.append({k: x.get(k) for k in KEEP})
        if not r.get('has_more') or not tr:
            break
        nxt = tr[-1]['timestamp']
        if not new and nxt == t:
            raise FetchError(f'trades {coin}: >1000 trades on one millisecond at {t}; cannot page by time')
        t = nxt
    return sorted(out, key=lambda x: (x['timestamp'], x['trade_id']))


def fridays():
    out, t = [], FIRST_FRIDAY
    while t <= LAST_FRIDAY:
        out.append(t)
        t += sv.WEEK
    return out


# ------------------------------------------------------------------ files
def write_atomic(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(obj, separators=(',', ':')))
    os.replace(tmp, path)


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def write_sums(d):
    d = Path(d)
    lines = [f'{sha256(f)}  {f.relative_to(d)}' for f in sorted(d.rglob('*'))
             if f.is_file() and f.name not in ('SHA256SUMS',) and not f.name.endswith('.tmp')]
    (d / 'SHA256SUMS').write_text('\n'.join(lines) + '\n')
    return len(lines)


def new_pull_dir(path, resume=False):
    p = Path(path).resolve()
    if not p.is_relative_to(DATA_ROOT.resolve()):
        raise FetchError(f'014 pulls only under {DATA_ROOT}')
    if p.exists() and any(p.iterdir()) and not resume:
        raise FetchError(f'{p} exists and is not empty: each pull goes in a NEW directory (or --resume it)')
    if resume and (p / 'SHA256SUMS').exists():
        raise FetchError(f'{p} is complete (SHA256SUMS written): a completed pull is never resumed')
    p.mkdir(parents=True, exist_ok=True)
    return p


def pull(outdir, resume=False):
    d = new_pull_dir(outdir, resume)
    t0 = time.time()
    report = {'started': dt.datetime.now(dt.UTC).isoformat(), 'units': {}}
    for coin in sv.COINS:
        for kind, fn in (('dvol', fetch_dvol), ('perp', fetch_perp), ('funding', fetch_funding)):
            f = d / kind / f'{coin}.json'
            if f.exists():
                continue
            rows = fn(coin)
            write_atomic(f, rows)
            print(f'{kind} {coin}: {len(rows)} rows {sv.iso(rows[0][0])} .. {sv.iso(rows[-1][0])}', flush=True)
    fr = fridays()
    for i, F in enumerate(fr):
        for coin in sv.COINS:
            f = d / 'trades' / f'{coin}_{dt.datetime.fromtimestamp(F / 1000, dt.UTC):%Y-%m-%d}.json'
            if f.exists():
                continue
            rows = fetch_window(coin, F - WINDOW_H * sv.HOUR, F - 1)
            write_atomic(f, rows)
        if i % 20 == 0:
            print(f'trades: {i + 1}/{len(fr)} Fridays, {STATS["requests"]} requests, '
                  f'{STATS["bytes"] / 1e6:.0f} MB, {time.time() - t0:.0f}s', flush=True)
    report.update({'finished': dt.datetime.now(dt.UTC).isoformat(), 'requests_this_session': STATS['requests'],
                   'bytes_this_session': STATS['bytes'], 'fridays': len(fr),
                   'files': sum(1 for x in d.rglob('*.json') if x.name != 'pull_report.json')})
    write_atomic(d / 'pull_report.json', report)
    n = write_sums(d)
    print(f'complete: {n} files in SHA256SUMS; {STATS["requests"]} requests, {STATS["bytes"] / 1e6:.1f} MB this session')
    return d


if __name__ == '__main__':
    if len(sys.argv) >= 3 and sys.argv[1] == 'pull':
        pull(sys.argv[2], resume='--resume' in sys.argv)
    else:
        sys.exit(__doc__)
