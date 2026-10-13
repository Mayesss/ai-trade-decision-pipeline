"""Registration 012 — bounded, cached fetchers. Writes only under data/carry012/; reads 010's name lists
and registered panel under data/carry/ (whitelist). 010's modules are imported READ-ONLY; every network
call goes through the four `_*_raw` hooks below (tests replace them with fixtures).

    .venv/bin/python -I r012_fetch.py stage1      # Bybit 240-minute klines 2024-11-01 -> 2026-07-01 for the
                                                  # Stage-1 coins (the only Stage-1 price series 010 did not keep)
    .venv/bin/python -I r012_fetch.py stage2      # GATED: refuses unless r012_run's Stage-2 gate passes
    .venv/bin/python -I r012_fetch.py extension   # GATED: the pre-registered extension, if due

Rules (Revision 2):
- Every request carries an end bound. Before the gate LIMIT = 2026-07-01, and every primitive asserts
  its end <= LIMIT before any request. Only `stage2_gate` raises LIMIT, to the end.
- **No error is ever cached, and no null.** A non-answer (HTTP error, Bybit retCode other than 0, an
  HL 500) is retried; if it persists it raises a typed `FetchError`. "Unknown symbol" exists only where
  the venue says so explicitly (Bybit retCode 10001) or where the name is absent from HL's `meta`
  universe — never from an error.
- An EMPTY answer is cached before 2026-07-01 (that history is complete) and never after.
- Bybit klines are paged FORWARD in windows of 1,000 bars: the endpoint answers only the last `limit`
  bars before `end`, so backward paging returned nothing for a coin delisted earlier in the window.
- A Stage-2 pull is resumable until it completes. On completion it runs the build's coverage checks on
  the raw files and writes one `<kind>_fetched` row (owner lines, a manifest hash). A second completed
  fetch is refused by the gate.
"""
import hashlib
import json
import sys
import threading
import time
import urllib.error
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scouts'))
import r010_fetch as F  # noqa: E402   (read-only: _by_get, bybit_candidates)
from l2_fetch import _zip_rows, hl_name_from_bn  # noqa: E402

CUT = 1_782_864_000_000                 # 2026-07-01 00:00 UTC
LIMIT = CUT                             # raised ONLY by stage2_gate()
FETCH_START = 1_730_419_200_000         # 2024-11-01: Stage-1 panel start (30-day volume, seasoning, windows)
STAGE2_FETCH_START = 1_777_593_600_000  # 2026-05-01: Stage-2 pull start (61 days of look-back before 2026-07-01)
HOUR, DAY = 3_600_000, 86_400_000
IV_MS = {'240': 4 * HOUR, 'D': DAY}
OUT = ROOT / 'data' / 'carry012'
SRC010 = ROOT / 'data' / 'carry'
BV = 'https://data.binance.vision/data'
EXPOSURE = {'dropped_rows': 0}
FAILURES = []                           # typed failures of this process: [(kind, key, detail)]
RETRIES = 6
SLEEP = time.sleep
_lock = threading.Lock()


class FetchError(Exception):
    """A typed non-answer after retries: never cached, never read as 'unknown'."""

    def __init__(self, kind, key, detail=''):
        super().__init__(f'{kind} {key} {detail}')
        self.kind, self.key, self.detail = kind, key, detail


# ------------------------------------------------------------------ network hooks (tests replace these)
def _by_raw(path, q):
    return F._by_get(path, q)


def _hl_raw(body):
    """One POST to HL /info; raises urllib errors as they come (no '500 means unknown' rule)."""
    req = urllib.request.Request('https://api.hyperliquid.xyz/info', data=json.dumps(body).encode(),
                                 headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def _zip_raw(url):
    return _zip_rows(url)


def _list_raw(prefix, delimiter=None):
    from h2_binance_scout import list_objects
    return list(list_objects(prefix, delimiter))


# ------------------------------------------------------------------ helpers
def _check_paths():
    import r012_run as run
    run.assert_allowed(OUT)
    run.assert_allowed(SRC010)


def drop(rows, tkey, end=None):
    end = LIMIT if end is None else min(end, LIMIT)
    keep = [r for r in rows if tkey(r) < end]
    if len(keep) != len(rows):
        with _lock:
            EXPOSURE['dropped_rows'] += len(rows) - len(keep)
    return keep


def _save(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(obj))
    tmp.replace(path)


def _empty(v):
    return v == [] or (isinstance(v, dict) and 'rows' in v and not v['rows'])


def _memo(path, fetch):
    """Cache a ROW answer on disk. `None` is never cached (and never returned by a primitive). An
    empty answer is cached before 2026-07-01 only. Errors raise and are never cached."""
    if path.exists():
        v = json.loads(path.read_text())
        if v is not None:
            return v
    v = fetch()
    if v is None:
        raise FetchError('null_answer', str(path.name))
    if _empty(v) and LIMIT > CUT:
        return v
    _save(path, v)
    return v


def _retry(kind, key, call, ok):
    """call() until ok(answer); exceptions and non-ok answers are retried RETRIES times, then a typed
    FetchError is raised."""
    last = ''
    for attempt in range(RETRIES + 1):
        try:
            v = call()
            if ok(v):
                return v
            last = f'bad answer: {str(v)[:120]}'
        except FetchError:
            raise
        except Exception as e:                      # noqa: BLE001  (typed below, never swallowed)
            last = f'{type(e).__name__}: {e}'
        if attempt < RETRIES:
            SLEEP(min(30, 2 ** attempt))
    raise FetchError(kind, key, last)


# ------------------------------------------------------------------ primitives (end-bounded)
def _by(path, q, key):
    """Bybit v5 GET; retCode 0 -> the answer; 10001 (invalid symbol) -> None (explicit unknown);
    anything else is retried, then a FetchError."""
    v = _retry('bybit', key, lambda: _by_raw(path, q),
               lambda r: isinstance(r, dict) and r.get('retCode') in (0, 10001))
    return None if v.get('retCode') == 10001 else v


def by_kline(base, sym, interval, t0, t1):
    """Bybit linear klines [start, open, high, low, close, turnover] with start in [t0, t1), t1 <= LIMIT.
    Paged FORWARD in windows of 1,000 bars (the endpoint answers only the last `limit` bars before
    `end`). An unknown symbol is an empty answer."""
    assert t1 <= LIMIT
    path = base / 'bybit' / 'kline' / f'{sym}_{interval}_{t0}_{t1}.json'
    span = 1000 * IV_MS[interval]

    def fetch():
        out = []
        a = t0
        while a < t1:
            b = min(a + span, t1)
            res = _by('kline', {'category': 'linear', 'symbol': sym, 'interval': interval, 'start': a,
                                'end': b - 1, 'limit': 1000}, f'{sym} kline {interval} {a}')
            if res is None:
                return []
            out += [[int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[6])]
                    for r in res['result']['list']]
            a = b
        return sorted({r[0]: r for r in drop([r for r in out if r[0] >= t0], lambda r: r[0], end=t1)}.values())
    return drop(_memo(path, fetch), lambda r: r[0], end=t1)


def by_funding(base, sym, t0, t1):
    """[time, rate] in [t0, t1); an unknown symbol (retCode 10001) is an empty answer."""
    assert t1 <= LIMIT
    path = base / 'bybit' / 'funding' / f'{sym}_{t0}_{t1}.json'

    def fetch():
        out, end = [], t1 - 1
        while end >= t0:
            res = _by('funding/history', {'category': 'linear', 'symbol': sym, 'startTime': t0,
                                          'endTime': end, 'limit': 200}, f'{sym} funding {end}')
            if res is None:
                return []
            page = res['result']['list']
            out += [[int(r['fundingRateTimestamp']), float(r['fundingRate'])] for r in page]
            if len(page) < 200:
                break
            end = min(int(r['fundingRateTimestamp']) for r in page) - 1
        return sorted(drop(out, lambda r: r[0], end=t1))
    return drop(_memo(path, fetch), lambda r: r[0], end=t1)


def _hl(body, key):
    """HL /info with retries on ANY error (a 500 is an error, never 'unknown coin')."""
    return _retry('hl', key, lambda: _hl_raw(body), lambda r: isinstance(r, (list, dict)))


def hl_meta(base):
    """HL's perp universe at fetch time (names, and isDelisted where HL marks it): the only source of
    'HL does not list this coin'. Saved every attempt (not a row answer)."""
    m = _hl({'type': 'meta'}, 'meta')
    if not isinstance(m, dict) or not isinstance(m.get('universe'), list):
        raise FetchError('hl', 'meta', 'no universe in the answer')
    names = {u['name']: bool(u.get('isDelisted')) for u in m['universe'] if isinstance(u, dict) and 'name' in u}
    _save(base / 'hl' / 'meta.json', names)
    return names


def hl_funding(base, coin, t0, t1):
    """HL fundingHistory [time, rate, premium] in [t0, t1)."""
    assert t1 <= LIMIT
    path = base / 'hl' / f'{coin}_funding_{t0}_{t1}.json'

    def fetch():
        out, t = [], t0
        while t < t1:
            page = _hl({'type': 'fundingHistory', 'coin': coin, 'startTime': t, 'endTime': t1 - 1}, f'{coin} funding {t}')
            if not isinstance(page, list) or not page:
                break
            out += [[r['time'], float(r['fundingRate']), float(r['premium'])] for r in page]
            if len(page) < 500 or page[-1]['time'] >= t1 - 1 - HOUR:
                break
            t = page[-1]['time'] + 1
        return drop(out, lambda r: r[0], end=t1)
    return drop(_memo(path, fetch), lambda r: r[0], end=t1)


def hl_candles(base, coin, t0, t1, interval='8h'):
    """HL candleSnapshot [t_open, o, h, l, c, v_coin] for bars starting in [t0, t1)."""
    assert t1 <= LIMIT
    path = base / 'hl' / 'candles' / f'{coin}_{interval}_{t0}_{t1}.json'

    def fetch():
        out, t = [], t0
        while t < t1:
            page = _hl({'type': 'candleSnapshot', 'req': {'coin': coin, 'interval': interval, 'startTime': t,
                                                           'endTime': t1 - 1}}, f'{coin} candles {t}')
            if not isinstance(page, list) or not page:
                break
            out += [[r['t'], float(r['o']), float(r['h']), float(r['l']), float(r['c']), float(r['v'])] for r in page]
            if len(page) < 5000:
                break
            t = page[-1]['t'] + 1
        return sorted(drop([r for r in out if r[0] >= t0], lambda r: r[0], end=t1))
    return drop(_memo(path, fetch), lambda r: r[0], end=t1)


def bn_file(base, kind, sym, ym, interval=None):
    """Binance monthly file (data.binance.vision) that the listing says EXISTS: fundingRate
    [t, interval_h, rate] or klines [open_t, open, high, low, close, quote_volume]. A 404 or an error
    is a FetchError (the listing named the file), never an empty month."""
    y, m = map(int, ym.split('-'))
    assert int(datetime(y, m, 1, tzinfo=timezone.utc).timestamp() * 1000) < LIMIT
    sub = f'{kind}/{sym}' + (f'/{interval}' if interval else '')
    fname = f'{sym}-{interval}-{ym}.zip' if interval else f'{sym}-{kind}-{ym}.zip'
    path = base / 'binance' / kind / (f'{sym}_{interval}_{ym}.json' if interval else f'{sym}_{ym}.json')

    def fetch():
        url = f'{BV}/futures/um/monthly/{urllib.parse.quote(sub)}/{urllib.parse.quote(fname)}'
        rows, size = _retry('binance', f'{sym} {kind} {ym}', lambda: _zip_raw(url), lambda r: r[0] is not None)
        rows = [r for r in rows if r and r[0][:1].isdigit()]
        if kind == 'fundingRate':
            out = [[int(r[0]), float(r[1]) if r[1] else None, float(r[2])] for r in rows]
        else:
            out = [[int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[7])] for r in rows]
        return {'bytes': size, 'rows': drop(out, lambda r: r[0])}
    v = _memo(path, fetch)
    v['rows'] = drop(v['rows'], lambda r: r[0])
    return v


# ------------------------------------------------------------------ Stage 1 (before registration allowed: < 2026-07-01)
def stage1_symbols():
    """Bybit symbols of the Stage-1 coins: every coin of 010's registered universe with an HL symbol."""
    uni = json.loads((SRC010 / 'panel' / 'universe.json').read_text())
    return sorted(u['BY'] for u in uni.values() if 'HL' in u and 'BY' in u)


def stage1():
    _check_paths()
    syms = stage1_symbols()
    base = OUT / 'stage1'
    print('stage1 bybit 240-minute klines (forward windows), symbols', len(syms), flush=True)
    res, fails = {}, []

    def one(s):
        try:
            return s, by_kline(base, s, '240', FETCH_START, CUT)
        except FetchError as e:
            return s, e
    with ThreadPoolExecutor(8) as ex:
        for s, v in ex.map(one, syms):
            (fails.append((s, str(v))) if isinstance(v, FetchError) else res.__setitem__(s, v))
    if fails:
        sys.exit(f'stage1: {len(fails)} typed fetch failures (nothing cached for them; re-run): {fails[:5]}')
    print('stage1 done; symbols with rows', sum(1 for v in res.values() if v), 'of', len(syms),
          '; rows dropped at the 2026-07-01 bound', EXPOSURE['dropped_rows'], flush=True)


# ------------------------------------------------------------------ Stage 2 / extension (gated)
REG = ROOT / 'registrations' / '012-hl-cex-funding-carry.md'


def stage2_gate(kind='stage2', stage='fetch'):
    """The ONLY way past 2026-07-01: r012_run's guard in mode <kind>_<stage>. Raises LIMIT to the end
    (exclusive) and returns (end, tag, commit, owner). Anything else exits."""
    global LIMIT
    import r012_run as run
    fails, commit = run.guard_failures(ROOT, str(REG.relative_to(ROOT)), mode=f'{kind}_{stage}')
    if fails:
        sys.exit('\n'.join(fails))
    end, date, ext, _ = run.owner_choice(REG.read_text())
    owner = {'end_date': date, 'extension': ext}
    if kind == 'extension':
        end, date = run.c12.EXTENSION_END, '2027-06-30'
    LIMIT = end
    return end, f'{kind}_{date}', commit, owner


def started_row(kind, commit, owner):
    import r012_run as run
    if not any(r.get('mode') == f'{kind}_fetch_started' for _, r in run.result_rows(ROOT, 'results.jsonl', REG.name)):
        run.append_row({'registration': REG.name, 'mode': f'{kind}_fetch_started', 'registration_commit': commit,
                        'owner': owner, 'run_at': datetime.now(timezone.utc).isoformat()})


def ym_range(t0, t1):
    a = datetime.fromtimestamp(t0 / 1000, timezone.utc)
    b = datetime.fromtimestamp((t1 - 1) / 1000, timezone.utc)
    out, y, m = [], a.year, a.month
    while (y, m) <= (b.year, b.month):
        out.append(f'{y:04d}-{m:02d}')
        y, m = y + (m == 12), m % 12 + 1
    return out


def months_in(base, t0, t1):
    """Point-in-time Binance universe, LISTED AGAIN on every attempt and saved with `_save` (Revision 2,
    B1): every USDT-M symbol in the S3 listing (names only) with fundingRate month files in [the month of
    t0, the month of t1 - 1], and, per symbol, the 8h kline months listed."""
    lo, hi = ym_range(t0, t1)[0], ym_range(t0, t1)[-1]
    pre = _retry('binance', 'listing', lambda: _list_raw('data/futures/um/monthly/fundingRate/', '/'), lambda r: isinstance(r, list))
    syms = sorted(k.rstrip('/').rsplit('/', 1)[-1] for t, k, _ in pre if t == 'pre')
    out = {}
    for s_ in (x for x in syms if x.endswith('USDT')):
        keys = _retry('binance', f'listing {s_}', lambda s_=s_: _list_raw(f'data/futures/um/monthly/fundingRate/{s_}/'),
                      lambda r: isinstance(r, list))
        ms = sorted(k[-11:-4] for t, k, _ in keys if k.endswith('.zip') and lo <= k[-11:-4] <= hi)
        if ms:
            out[s_] = ms
    _save(base / 'binance' / 'months.json', out)
    return out


def kline_months_in(base, syms, t0, t1):
    """The 8h kline month files LISTED for each symbol in the window (a second, independent listing: a
    symbol can keep funding files after its kline files stop). Saved every attempt."""
    lo, hi = ym_range(t0, t1)[0], ym_range(t0, t1)[-1]
    out = {}
    for s_ in sorted(syms):
        keys = _retry('binance', f'kline listing {s_}', lambda s_=s_: _list_raw(f'data/futures/um/monthly/klines/{s_}/8h/'),
                      lambda r: isinstance(r, list))
        out[s_] = sorted(k[-11:-4] for t, k, _ in keys if k.endswith('.zip') and lo <= k[-11:-4] <= hi)
    _save(base / 'binance' / 'kline_months.json', out)
    return out


def manifest(base):
    files = sorted(p for p in Path(base).rglob('*.json') if p.is_file())
    h = hashlib.sha256()
    for p in files:
        h.update(str(p.relative_to(base)).encode())
        h.update(hashlib.sha256(p.read_bytes()).digest())
    return {'files': len(files), 'sha256': h.hexdigest()}


def stage2(kind='stage2'):
    """Pre-specified pull, window 2026-05-01 -> the end:
    - the Binance listing (point in time) and HL's `meta` universe;
    - for every Binance symbol whose HL candidate name is in `meta`: HL funding and 8h candles;
    - for those: Binance funding and 8h klines for every listed month; the Bybit symbol (probed with
      bounded funding requests; retCode 10001 = unknown) with 240-minute and daily klines.
    Any typed failure stops the attempt with nothing cached for it (re-run to resume). On success the
    build's coverage checks run on the raw files; only then is `<kind>_fetched` written."""
    _check_paths()
    end, tag, commit, owner = stage2_gate(kind, 'fetch')
    started_row(kind, commit, owner)
    base = OUT / tag
    t0 = STAGE2_FETCH_START
    report = {'hl_not_in_meta': [], 'hl_delisted_in_meta': [], 'bybit_unknown': [], 'binance_symbols': 0}
    try:
        months = months_in(base, t0, end)
        report['binance_symbols'] = len(months)
        meta = hl_meta(base)
        hlmap, by = {}, {}
        for s_ in sorted(months):
            b_ = s_[:-4]
            cands = list(dict.fromkeys(x for x in (hl_name_from_bn(s_), b_, 'k' + b_[4:] if b_.startswith('1000') else None) if x))
            hit = [c for c in cands if c in meta]
            if not hit:
                report['hl_not_in_meta'].append(s_)
                continue
            c = hit[0]
            if meta[c]:
                report['hl_delisted_in_meta'].append(c)
            if hl_funding(base, c, t0, end):
                hlmap[s_] = c
                hl_candles(base, c, t0, end)
        kmonths = kline_months_in(base, hlmap, t0, end)
        for s_ in sorted(hlmap):
            for m in months[s_]:
                bn_file(base, 'fundingRate', s_, m)
            for m in kmonths[s_]:
                bn_file(base, 'klines', s_, m, '8h')
            for c in F.bybit_candidates(s_):
                if by_funding(base, c, t0, end):
                    by[s_] = c
                    by_kline(base, c, '240', t0, end)
                    by_kline(base, c, 'D', t0, end)
                    break
            else:
                report['bybit_unknown'].append(s_)
    except FetchError as e:
        FAILURES.append((e.kind, e.key, e.detail))
        _save(base / 'failures.json', FAILURES)
        sys.exit(f'{kind} fetch stopped on a typed failure (nothing cached for it; re-run to resume): {e}')
    _save(base / 'bybit' / 'symbols.json', by)
    _save(base / 'hl' / 'coins.json', hlmap)
    import r012_build as B
    a = B.assemble_stage2(base, end)
    problems, cov = a['problems'], a['coverage']
    if problems:
        _save(base / 'coverage_failures.json', problems)
        sys.exit(f'{kind} fetch complete but coverage fails ({len(problems)}); no {kind}_fetched row, re-run when '
                 f'the missing files exist: {problems[:5]}')
    import r012_run as run
    run.append_row({'registration': REG.name, 'mode': f'{kind}_fetched', 'registration_commit': commit, 'owner': owner,
                    'manifest': manifest(base), 'report': {k: (len(v) if isinstance(v, list) else v) for k, v in report.items()},
                    'coverage': cov, 'run_at': datetime.now(timezone.utc).isoformat()})
    print(kind, 'fetch done; rows dropped at the end bound', EXPOSURE['dropped_rows'], '; report',
          {k: (len(v) if isinstance(v, list) else v) for k, v in report.items()}, flush=True)


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else ''
    if cmd == 'stage1':
        stage1()
    elif cmd in ('stage2', 'extension'):
        stage2(cmd)
    else:
        sys.exit(__doc__)
