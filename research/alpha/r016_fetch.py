"""Registration 016 — the guarded forward fetch (Revision 1). Runs only through r016_run's guard in mode 'fetch':
REGISTERED (reg0-bound), committed, unmodified, the real repository's main worktree on a named branch, python -I -S
with the pinned interpreter, code and static data matching the table, today >= 2028-01-08 (UTC), results.jsonl clean,
no fetched / read_started / read row anywhere.

    python3 -I -S research/alpha/r016_fetch.py      # resumable until its `fetched` row

The real fetch takes NO argument. `_fetch_in()` (tests only) refuses unless the tree, the store and the ledger all
lie outside the real repository (review B M1: mock hooks and a `now` never touch the real repository in any role).

Witness (review B m1): after the guard, before any row is committed or any file written, the real fetch lists BTCUSDT's monthly
1d keys and requires the 2027-12 key, and requires the HTTP `Date` of that answer (UTC) >= 2028-01-08: an early run
leaves nothing behind. Attempts committed before 2028-01-08 never count toward the persistence chain.

What it pulls (free, keyless, public; nothing else):
1. Binance USDⓈ-M monthly 1d klines (data.binance.vision), every USDT symbol, months 2026-07 -> 2027-12. Keys of
   later months are COUNTED, never fetched; months <= 2026-06 are the C1 scout's pinned pull (never re-fetched).
2. Binance USDⓈ-M monthly fundingRate, every USDT symbol, months 2025-03 -> 2027-12 (the deployable variant).
3. Binance fapi exchangeInfo (contract types; the listing cross-check).
4. Hyperliquid `meta` and, per HL coin, its first funding time and the last month start at which it still funded.
Every zip is verified against Binance's `.CHECKSUM` (also on resume: a stored file is re-checked) and parsed before an
atomic write. Never cached: an error, a null, an empty answer or a checksum mismatch. S3 ListObjects pages FORWARD
(`marker` exclusive); HL `fundingHistory` takes inclusive start and end, so windows are [a, b - 1 ms].
Coverage (keyed refusals; review B m5): BTCUSDT every forward day (never waivable); the 2026-06 / 2026-07 seam and the
2027-11 / 2027-12 seam for every symbol trading at the end of the earlier month; contiguous forward months; a funding
file for every (symbol, month) with a traded kline row in 2025-03 -> 2027-12; every PERPETUAL/COIN USDT contract of
the 2028 exchangeInfo onboarded before 2027-12 and alive after 2026-07-01 has forward klines. The same key on 3
committed attempts, each >= 24 h after the previous one and all on or after 2028-01-08, becomes 'unavailable
(persistent)', recorded and counted (<= 10 names, else stop for a decision); BTCUSDT keys never do.
"""
import datetime as dt
import email.utils
import json
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.pycache_prefix = tempfile.mkdtemp(prefix='r016f-pyc-')  # BEFORE importing the guard (review B M2)
sys.dont_write_bytecode = True
sys.path.append(str(HERE))                                  # after the stdlib
import r016_run as run016  # noqa: E402
from ct16 import data as D  # noqa: E402
from ct16 import rule as R  # noqa: E402

_HERE0 = HERE
LIST_URL = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
DL_URL = 'https://data.binance.vision/'
FAPI_EXCHANGE_INFO = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
HL_INFO = 'https://api.hyperliquid.xyz/info'
NS = '{http://s3.amazonaws.com/doc/2006-03-01/}'
KLINES_BASE = 'data/futures/um/monthly/klines/'
FUNDING_BASE = 'data/futures/um/monthly/fundingRate/'
SYM_RE = re.compile(r'^[A-Z0-9]+USDT$')
MONTH_RE = re.compile(r'-(\d{4})-(\d{2})\.zip$')
HOOK_NAMES = ('witness', 's3_list', 'get', 'exchange_info', 'hl_meta', 'hl_funding')
PERSIST_ATTEMPTS, PERSIST_GAP_S = 3, 24 * 3600
PERSISTABLE = ('klines_seam:', 'klines_gap:', 'klines_error:', 'funding_error:', 'funding_missing:', 'hl_error:',
               'listing_missing:')
MAX_UNAVAILABLE = 10
DAY_MS = 86_400_000
HL_START = dt.datetime(2023, 5, 1, tzinfo=dt.UTC)
_REAL = {'ok': False}


class FetchError(RuntimeError):
    """A typed failure: nothing is written; the fetch resumes on re-run."""


def ms(d):
    return int(dt.datetime.combine(d, dt.time(), dt.UTC).timestamp() * 1000)


def months(a, b):
    y, m = map(int, a.split('-'))
    out = []
    while f'{y:04d}-{m:02d}' <= b:
        out.append(f'{y:04d}-{m:02d}')
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def month_of_key(key):
    m = MONTH_RE.search(key)
    return f'{m.group(1)}-{m.group(2)}' if m else None


# ------------------------------------------------------------------ real hooks
def _require_real():
    if not _REAL['ok']:
        raise SystemExit("refusing: a real network hook was reached outside the real repository's guarded fetch")


def _http(url, data=None, tries=4, headers=False):
    _require_real()
    for i in range(tries):
        try:
            req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'} if data else {})
            with urllib.request.urlopen(req, timeout=60) as r:
                body = r.read()
                return (body, r.headers.get('Date')) if headers else body
        except urllib.error.HTTPError as e:
            if e.code in (403, 404) or i == tries - 1:     # never retry around an access refusal
                raise FetchError(f'{url[:120]}: HTTP {e.code}') from e
        except Exception as e:  # noqa: BLE001 - typed, never cached
            if i == tries - 1:
                raise FetchError(f'{url[:120]}: {e!r}'[:200]) from e
        time.sleep(2.0 * (i + 1))
    raise FetchError(url)


def _witness():
    """(keys of BTCUSDT's monthly 1d listing, the answer's HTTP Date as an aware UTC datetime)."""
    body, date = _http(LIST_URL + '?' + urllib.parse.urlencode({'prefix': f'{KLINES_BASE}BTCUSDT/1d/'}), headers=True)
    keys = [c.find(NS + 'Key').text for c in ET.fromstring(body).findall(NS + 'Contents')]
    return keys, email.utils.parsedate_to_datetime(date) if date else None


def _s3_list(prefix, delimiter=None):
    out, marker = [], ''
    while True:
        q = {'prefix': prefix, 'marker': marker}
        if delimiter:
            q['delimiter'] = delimiter
        root = ET.fromstring(_http(LIST_URL + '?' + urllib.parse.urlencode(q)))
        last = None
        for c in root.findall(NS + 'Contents'):
            last = c.find(NS + 'Key').text
            out.append(('obj', last))
        for p in root.findall(NS + 'CommonPrefixes'):
            last = p.find(NS + 'Prefix').text
            out.append(('pre', last))
        if root.findtext(NS + 'IsTruncated') != 'true' or last is None:
            return out
        marker = root.findtext(NS + 'NextMarker') or last


def _get(key):
    return _http(DL_URL + key)


def _exchange_info():
    v = json.loads(_http(FAPI_EXCHANGE_INFO))
    if not isinstance(v, dict) or not v.get('symbols'):
        raise FetchError('exchangeInfo: no symbols')
    return v


def _hl_meta():
    v = json.loads(_http(HL_INFO, json.dumps({'type': 'meta'}).encode()))
    uni = v.get('universe') if isinstance(v, dict) else None
    if not isinstance(uni, list) or not uni:
        raise FetchError('hl meta: empty universe')
    return uni


def _hl_funding(coin, start, end):
    time.sleep(1.5)                                    # well inside HL's published 1200 weight / minute
    v = json.loads(_http(HL_INFO, json.dumps({'type': 'fundingHistory', 'coin': coin, 'startTime': start,
                                              'endTime': end}).encode()))
    if not isinstance(v, list):
        raise FetchError(f'hl funding {coin}: not a list')
    return v


REAL_HOOKS = {'witness': _witness, 's3_list': _s3_list, 'get': _get, 'exchange_info': _exchange_info,
              'hl_meta': _hl_meta, 'hl_funding': _hl_funding}


# ------------------------------------------------------------------ helpers
def _write(path, blob):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_bytes(blob)
    tmp.replace(path)


def witness_failures(hooks):
    """Review B m1: the archive must already hold BTCUSDT 2027-12 and the server's own clock must be past the gate."""
    keys, date = hooks['witness']()
    out = []
    if not any(month_of_key(k) == run016.FWD_MONTHS[1] for k in keys):
        out.append(f'(t) witness: the archive does not list BTCUSDT {run016.FWD_MONTHS[1]} yet')
    if date is None or date.astimezone(dt.UTC).date() < R.READ_NOT_BEFORE:
        out.append(f'(t) witness: the server date {date} is before {R.READ_NOT_BEFORE}')
    return out


def symbols(hooks, base):
    keys = [k for kind, k in hooks['s3_list'](base, '/') if kind == 'pre']
    return sorted({k[len(base):].strip('/') for k in keys if SYM_RE.match(k[len(base):].strip('/'))})


def plan(hooks, base, sub, first, last):
    """{symbol: [(month, key)]} of the wanted months; plus the count of keys of LATER months (never fetched)."""
    want, later = {}, 0
    for sym in symbols(hooks, base):
        for kind, key in hooks['s3_list'](f'{base}{sym}/{sub}'):
            if kind != 'obj' or not key.endswith('.zip'):
                continue
            mo = month_of_key(key)
            if mo is None:
                continue
            if mo > last:
                later += 1
                continue
            if mo >= first:
                want.setdefault(sym, []).append((mo, key))
    return {s: sorted(v) for s, v in want.items()}, later


def _checksum(hooks, key):
    chk = hooks['get'](key + '.CHECKSUM')
    return (chk.decode() if isinstance(chk, bytes) else str(chk)).split()[0].strip().lower() if chk else ''


def fetch_verified(hooks, key, path):
    """Download key and key.CHECKSUM; write only a verified, parseable, non-empty zip. Returns its sha256."""
    blob = hooks['get'](key)
    if not blob:
        raise FetchError(f'{key}: empty answer')
    got = D.sha256_bytes(blob)
    if got != _checksum(hooks, key):
        raise FetchError(f'{key}: checksum mismatch')
    D.zip_text(blob)
    _write(path, blob)
    return got


def pull(hooks, store, base, sub, first, last, kind):
    """Fetch every wanted file; a file already on disk is re-checked against .CHECKSUM (review B m5d) and re-fetched if
    it differs. Returns (want, {rel: sha}, {key: error}, later-count)."""
    want, later = plan(hooks, base, sub, first, last)
    files, errors = {}, {}
    tag = 'klines' if kind == 'klines' else 'funding'
    for sym, items in sorted(want.items()):
        for mo, key in items:
            rel = f"{kind}/{sym}/{sub}{key.rsplit('/', 1)[1]}"
            path = store / rel
            try:
                if path.exists():
                    if run016.sha256(path) == _checksum(hooks, key):
                        files[rel] = run016.sha256(path)
                        continue
                    path.unlink()
                files[rel] = fetch_verified(hooks, key, path)
            except FetchError as e:
                errors[f'{tag}_error:{sym}:{mo}'] = str(e)[:200]
    return want, files, errors, later


def hl_presence(hooks, coins, end_ms):
    """{coin: {'first': ms | None, 'last': month-start ms at which it still funded | None (alive at the end)}}."""
    out, errors = {}, {}
    starts = [ms(dt.date.fromisoformat(m + '-01')) for m in months('2025-03', '2027-12')]
    for c in coins:
        try:
            page = hooks['hl_funding'](c, int(HL_START.timestamp() * 1000), end_ms - 1)
            first = min((int(r['time']) for r in page), default=None)
            if first is None:
                out[c] = {'first': None, 'last': None}
                continue
            if hooks['hl_funding'](c, end_ms - 2 * DAY_MS, end_ms - 1):
                out[c] = {'first': first, 'last': None}
                continue
            alive = [s for s in starts if s >= first]
            lo, hi, last = 0, len(alive) - 1, None
            while lo <= hi:                             # the last month start with funding rows in [s, s + 2 d)
                mid = (lo + hi) // 2
                if hooks['hl_funding'](c, alive[mid], alive[mid] + 2 * DAY_MS - 1):
                    last, lo = alive[mid], mid + 1
                else:
                    hi = mid - 1
            out[c] = {'first': first, 'last': last if last is not None else first}
        except FetchError as e:
            errors[f'hl_error:{c}'] = str(e)[:200]
    return out, errors


def traded_months(blob, lo, hi, tail_from=None):
    """(months with a traded row, traded in the window from day `tail_from` on) of one klines zip (counts only)."""
    rows = D.parse_klines(D.zip_text(blob), not_before=lo, before=hi)
    mos = {R.month_of(d) for d, r in rows.items() if r[2] > 0}
    tail = tail_from is not None and any(r[2] > 0 for d, r in rows.items() if d >= tail_from)
    return mos, tail


def coverage(root, store, files, a_sums, ex=None):
    """Keyed refusals from the files on disk and the pinned span-(a) pull (names, months, days, trade counts; no price)."""
    refuse = {}
    by_sym, traded = {}, set()
    span = months(*run016.FWD_MONTHS)
    end_nov = R.day(dt.date(2027, 11, 30))
    lo_f, hi_f = R.day(R.SPAN_C[0]), R.day(R.END) + 1
    for rel in files:
        fm = D.file_month(rel)
        if fm and fm[0] == 'klines':
            by_sym.setdefault(fm[1], set()).add(fm[2])
            mos, tail = traded_months((store / rel).read_bytes(), lo_f, hi_f, end_nov - 6 if fm[2] == '2027-11' else None)
            traded |= {(fm[1], m) for m in mos}
            if fm[2] == '2027-11' and tail and not any(D.file_month(r) == ('klines', fm[1], '2027-12') for r in files):
                refuse[f'klines_seam:{fm[1]}:2027-12'] = 'traded at the end of 2027-11 but no 2027-12 file'
    for sym, have in sorted(by_sym.items()):
        gap = [m for m in span if min(have) <= m <= max(have) and m not in have]
        if gap:
            refuse[f'klines_gap:{sym}'] = f'interior months missing {gap[:3]}'
    for rel, h in sorted(a_sums.items()):
        fm = D.file_month(rel)
        if not fm or fm[0] != 'klines' or fm[2] < run016.FUNDING_MONTHS[0]:
            continue
        blob = (root / run016.KLINES_A_REL / rel).read_bytes()
        R.refuse_unless(D.sha256_bytes(blob) == h, f'{rel} does not match the pinned SHA256SUMS')
        cut = R.day(R.SPAN_A[1]) - 6
        mos, tail = traded_months(blob, R.day(R.START) - 19, R.day(R.SPAN_C[0]), cut if fm[2] == run016.A_MONTH_LAST else None)
        traded |= {(fm[1], m) for m in mos}
        if fm[2] == run016.A_MONTH_LAST and tail and span[0] not in by_sym.get(fm[1], ()):
            refuse[f'klines_seam:{fm[1]}'] = 'traded at the end of 2026-06 but no 2026-07 file was fetched'
    fund = {(D.file_month(r)[1], D.file_month(r)[2]) for r in files if D.file_month(r) and D.file_month(r)[0] == 'funding'}
    for sym, mo in sorted(traded):
        if run016.FUNDING_MONTHS[0] <= mo <= run016.FUNDING_MONTHS[1] and (sym, mo) not in fund:
            refuse[f'funding_missing:{sym}:{mo}'] = 'a traded kline month without a funding file'
    for x in (ex or {}).get('symbols', []):
        s, ob, dl = x.get('symbol', ''), int(x.get('onboardDate') or 0), int(x.get('deliveryDate') or 0)
        if (SYM_RE.match(s) and x.get('contractType') == 'PERPETUAL' and x.get('underlyingType') == 'COIN'
                and 0 < ob < ms(dt.date(2027, 12, 1)) and (x.get('status') == 'TRADING' or dl >= ms(R.SPAN_C[0]))
                and s not in by_sym):
            refuse[f'listing_missing:{s}'] = 'a live COIN perp of the exchangeInfo has no forward klines in the listing'
    btc = sorted(by_sym.get(R.BTC, ()))
    if btc != span:
        refuse['btc_months'] = f'BTCUSDT forward months {len(btc)} of {len(span)}'
    else:
        days = set()
        for m in span:
            days |= set(D.parse_klines(D.zip_text((store / f'klines/{R.BTC}/1d/{R.BTC}-1d-{m}.zip').read_bytes())))
        want = set(range(lo_f, hi_f))
        if not want <= days:
            refuse['btc_tail'] = f'BTCUSDT misses {len(want - days)} forward days'
    return refuse


def attempt_history(root, results_rel=run016.RESULTS_REL):
    """{attempt: {'refusals': set, 'time': committer time of the earliest commit holding its end row}}."""
    att = {}
    for line, r in run016.result_rows(root, results_rel)[0]:
        if r.get('mode') != 'fetch_attempt':
            continue
        a = att.setdefault(int(r.get('attempt', 0)), {'refusals': set(), 'time': None})
        a['refusals'] |= set(r.get('refusals') or [])
        c = run016.first_commit_containing(root, results_rel, line)
        t = run016._git(root, 'show', '-s', '--format=%ct', c).stdout.strip() if c else ''
        if t and (a['time'] is None or int(t) < a['time']):
            a['time'] = int(t)
    return att


def apply_persistence(refusals, att, now_s, not_before_s=None):
    """Split keyed refusals into (blocking, persistent). Only attempts committed on or after 2028-01-08 count."""
    nb = not_before_s if not_before_s is not None else ms(R.READ_NOT_BEFORE) / 1000
    blocking, persistent = {}, {}
    for key, msg in refusals.items():
        if not key.startswith(PERSISTABLE) or f':{R.BTC}' in key:
            blocking[key] = msg
            continue
        times = sorted(a['time'] for a in att.values() if key in a['refusals'] and a['time'] is not None
                       and a['time'] >= nb)
        chain, last = 1, now_s
        for t in reversed(times):
            if last - t >= PERSIST_GAP_S:
                chain += 1
                last = t
        (persistent if chain >= PERSIST_ATTEMPTS else blocking)[key] = msg
    return blocking, persistent


# ------------------------------------------------------------------ the fetch
def _do_fetch(root, store, hooks, ctx, code, commit_row, now):
    """The fetch proper, after every check. commit_row(row, msg) appends, commits and verifies a row."""
    if not ctx['resume']:
        commit_row({'registration': run016.REG_NAME, 'mode': 'fetch_started', 'registration_commit': ctx['commit'],
                    'reg0': ctx['reg0'], 'code_hashes': run016.hashes(root, code), 'env': run016.env_fingerprint(),
                    'run_at': dt.datetime.now(dt.UTC).isoformat()}, '016: fetch_started (automatic, r016_fetch.py)')
    att = attempt_history(root)
    n_att = max(att, default=0) + 1
    end_row = {'registration': run016.REG_NAME, 'mode': 'fetch_attempt', 'attempt': n_att, 'refusals': [],
               'unavailable': {}}
    store.mkdir(parents=True, exist_ok=True)
    try:
        _, kfiles, kerr, k_later = pull(hooks, store, KLINES_BASE, '1d/', *run016.FWD_MONTHS, 'klines')
        _, ffiles, ferr, f_later = pull(hooks, store, FUNDING_BASE, '', *run016.FUNDING_MONTHS, 'fundingRate')
        ex = hooks['exchange_info']()
        _write(store / 'fapi_exchangeInfo.json', json.dumps(ex, sort_keys=True).encode())
        meta = hooks['hl_meta']()
        coins = sorted({u['name'] for u in meta if isinstance(u, dict) and u.get('name')})
        hl, herr = hl_presence(hooks, coins, ms(R.END + dt.timedelta(days=1)))
        sums = root / run016.KLINES_A_REL / 'SHA256SUMS'
        a_sums = D.parse_sums(sums.read_text()) if sums.exists() else {}
        refusals = {**kerr, **ferr, **coverage(root, store, {**kfiles, **ffiles}, a_sums, ex), **herr}
        end_row['refusals'] = sorted(refusals)
        blocking, persistent = apply_persistence(refusals, att, now.timestamp())
        names = {k.split(':')[1] for k in persistent}
        if len(names) > MAX_UNAVAILABLE:
            blocking['unavailable_ceiling'] = f'{len(names)} names would become unavailable: STOP for a decision'
        end_row['unavailable'] = {'persistent_after_3_attempts': sorted(persistent)}
        _write(store / 'hl_presence.json', json.dumps(hl, sort_keys=True).encode())
        report = {'klines_files': len(kfiles), 'funding_files': len(ffiles), 'later_keys_counted_only':
                  k_later + f_later, 'hl_coins': len(hl), 'unavailable': end_row['unavailable']}
        _write(store / 'report.json', json.dumps(report, sort_keys=True, indent=1).encode())
        if blocking:
            raise FetchError('coverage refused (no fetched row; re-run later):\n  '
                             + '\n  '.join(f'{k}: {v}' for k, v in list(blocking.items())[:20]))
    finally:
        _REAL['ok'] = False
        end_row['run_at'] = dt.datetime.now(dt.UTC).isoformat()
        commit_row(end_row, f'016: fetch attempt {n_att} end (automatic)')
    files = {**kfiles, **ffiles}
    for name in ('fapi_exchangeInfo.json', 'hl_presence.json', 'report.json'):
        files[name] = run016.sha256(store / name)
    digest = run016.manifest_digest(files)
    _write(store / 'manifest.json', json.dumps({'digest': digest, 'files': files}, sort_keys=True).encode())
    row = {'registration': run016.REG_NAME, 'mode': 'fetched', 'registration_commit': ctx['commit'], 'reg0': ctx['reg0'],
           'code_hashes': run016.hashes(root, code), 'env': run016.env_fingerprint(), 'manifest_digest': digest,
           'manifest_files': len(files), 'attempts': n_att, 'report': report,
           'run_at': dt.datetime.now(dt.UTC).isoformat()}
    commit_row(row, '016: fetched (automatic, r016_fetch.py)')
    print(f'fetched: {len(files)} files in the manifest after {n_att} attempt(s)')
    return row


def fetch():
    """The real fetch: no argument; the real repository, the real store, the real hooks, the wall clock."""
    if run016.HERE != _HERE0 or run016.REPO.resolve() != run016.bound_repo(
            run016.head_text(_HERE0, run016.REG_REL)).resolve():
        raise SystemExit('(w) the fetcher runs only from the real repository')
    f = run016.real_tree_failures()
    if f:
        raise SystemExit('\n'.join(f))
    ctx = run016.guard('fetch')                         # the clock gate first: no request at all before 2028-01-08
    wf = witness_failures(_real_witness_hooks())        # then the archive's and the server's own witness
    if wf:
        raise SystemExit('\n'.join(wf))
    _REAL['ok'] = True
    store = (_HERE0 / run016.STORE_REL).resolve()
    return _do_fetch(_HERE0, store, dict(REAL_HOOKS), ctx, run016.GUARDED_CODE, run016.record,
                     dt.datetime.now(dt.UTC))


def _real_witness_hooks():
    _REAL['ok'] = True
    try:
        keys, date = _witness()
    finally:
        _REAL['ok'] = False
    return {'witness': lambda: (keys, date)}


def _fetch_in(root, store, hooks, now, **guard_kw):
    """TESTS ONLY: a throwaway tree, its own store and ledger, mock hooks and a `now`. Refused when any of them lies
    in the real repository (review B M1), before any check that could write."""
    root, store = Path(root).resolve(), Path(store).resolve()
    if run016.in_real(root) or run016.in_real(store) or run016.in_real(root / run016.RESULTS_REL):
        raise SystemExit('(M1) mock hooks and a `now` never touch the real repository (tree, store or ledger)')
    if set(hooks) != set(HOOK_NAMES):
        raise SystemExit(f'(B1) mock hooks must supply exactly {list(HOOK_NAMES)}; no fallback to a real hook')
    wf = witness_failures(hooks)
    if wf:
        raise SystemExit('\n'.join(wf))
    store_rel = str(store.relative_to(root)) if store.is_relative_to(root) else run016.STORE_REL
    failures, ctx = run016.guard_failures(root, 'fetch', now=now, store_rel=store_rel, **guard_kw)
    if failures:
        raise SystemExit('\n'.join(failures))
    results = root / run016.RESULTS_REL

    def commit_row(row, msg):
        line = run016.append_row(row, results)
        run016.commit_results(msg, results)
        if line not in run016.head_lines(root):
            raise SystemExit('STOP: the row is not in HEAD:./results.jsonl after the commit')
        return line
    return _do_fetch(root, store, dict(hooks), ctx, guard_kw.get('code', run016.GUARDED_CODE), commit_row, now)


if __name__ == '__main__':
    try:
        fetch()
    except FetchError as err:
        sys.exit(f'fetch stopped: {err}')
