"""Registration 016 — the guarded forward fetch. Runs only through r016_run's gate (mode 'fetch'): REGISTERED,
committed, unmodified, code and static data matching the hash table, today >= 2028-01-08 (UTC), results.jsonl clean,
no fetched / read_started / read row anywhere (history and reflog included).

    research/copytrade/.venv/bin/python -I r016_fetch.py      # resumable until its `fetched` row

What it pulls (free, keyless, public; nothing else):
1. Binance USDⓈ-M monthly 1d klines (data.binance.vision) for every USDT symbol, months 2026-07 -> 2027-12. Keys of
   later months are COUNTED, never fetched. Months <= 2026-06 are the C1 scout's pinned pull (not re-fetched).
2. Binance USDⓈ-M monthly fundingRate for every USDT symbol, months 2025-03 -> 2027-12 (the deployable variant).
3. Binance fapi exchangeInfo (today's contract types: TradFi and index perps are excluded from the universe).
4. Hyperliquid `meta` and, per HL coin, its first funding time and (if it stopped) the last month start at which it
   still funded (the reported HL-tradable sub-book).
Every zip is verified against Binance's `.CHECKSUM` before it is written (atomic write). Never cached: an error, a
null, an empty answer or a checksum mismatch. Listings page FORWARD (S3 ListObjects `marker` is exclusive). HL
`fundingHistory` takes inclusive startTime and endTime (verified for 008, registration 008 §16); windows here are
[a, b - 1 ms]. Coverage refusals are keyed; the same key on 3 committed attempts each >= 24 h after the previous one
becomes 'unavailable (persistent)', recorded and counted, and stops blocking. BTCUSDT and the tail check are never
eligible for that end state.

Network. With no `hooks` argument the REAL hooks are used, only for the real repository, the real data tree and the
default guard arguments. A `hooks` argument must supply all the hooks (no fallback) and is never used on the real tree.
"""
import datetime as dt
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import r016_run as run016  # noqa: E402
from ct16 import data as D  # noqa: E402
from ct16 import rule as R  # noqa: E402

LIST_URL = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
DL_URL = 'https://data.binance.vision/'
FAPI_EXCHANGE_INFO = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
HL_INFO = 'https://api.hyperliquid.xyz/info'
NS = '{http://s3.amazonaws.com/doc/2006-03-01/}'
KLINES_BASE = 'data/futures/um/monthly/klines/'
FUNDING_BASE = 'data/futures/um/monthly/fundingRate/'
SYM_RE = re.compile(r'^[A-Z0-9]+USDT$')
MONTH_RE = re.compile(r'-(\d{4})-(\d{2})\.zip$')
HOOK_NAMES = ('s3_list', 'get', 'exchange_info', 'hl_meta', 'hl_funding')
PERSIST_ATTEMPTS, PERSIST_GAP_S = 3, 24 * 3600
PERSISTABLE = ('klines_seam:', 'klines_gap:', 'klines_error:', 'funding_error:', 'hl_error:')
MAX_UNAVAILABLE = 10
DAY_MS = 86_400_000
HL_START = dt.datetime(2023, 5, 1, tzinfo=dt.UTC)
_REAL = {'ok': False}


class FetchError(RuntimeError):
    """A typed failure: nothing is written; the fetch resumes on re-run."""


def ms(d):
    return int(dt.datetime.combine(d, dt.time(), dt.UTC).timestamp() * 1000)


def months(a, b):
    """['YYYY-MM', ...] from a to b inclusive."""
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


def _http(url, data=None, tries=4):
    _require_real()
    for i in range(tries):
        try:
            req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'} if data else {})
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code in (403, 404) or i == tries - 1:     # never retry around an access refusal
                raise FetchError(f'{url[:120]}: HTTP {e.code}') from e
        except Exception as e:  # noqa: BLE001 - typed, never cached
            if i == tries - 1:
                raise FetchError(f'{url[:120]}: {e!r}'[:200]) from e
        time.sleep(2.0 * (i + 1))
    raise FetchError(url)


def _s3_list(prefix, delimiter=None):
    """[(kind, key)] for every object ('obj') and common prefix ('pre') under prefix, paging FORWARD with the
    exclusive `marker` (ListObjects v1)."""
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


REAL_HOOKS = {'s3_list': _s3_list, 'get': _get, 'exchange_info': _exchange_info, 'hl_meta': _hl_meta,
              'hl_funding': _hl_funding}


# ------------------------------------------------------------------ helpers
def _write(path, blob):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_bytes(blob)
    tmp.replace(path)


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


def fetch_verified(hooks, key, path):
    """Download key and key.CHECKSUM; write only a verified, non-empty zip. Returns its sha256."""
    blob = hooks['get'](key)
    if not blob:
        raise FetchError(f'{key}: empty answer')
    chk = hooks['get'](key + '.CHECKSUM')
    want = (chk.decode() if isinstance(chk, bytes) else str(chk)).split()[0].strip().lower() if chk else ''
    got = D.sha256_bytes(blob)
    if got != want:
        raise FetchError(f'{key}: checksum mismatch')
    D.zip_text(blob)                                   # a malformed zip is refused, never written
    _write(path, blob)
    return got


def pull(hooks, store, base, sub, first, last, kind):
    """Fetch every wanted file not already on disk; returns ({rel: sha}, {key: error}, later-count)."""
    want, later = plan(hooks, base, sub, first, last)
    files, errors = {}, {}
    for sym, items in sorted(want.items()):
        for mo, key in items:
            rel = f"{kind}/{sym}/{sub}{key.rsplit('/', 1)[1]}" if sub else f"{kind}/{sym}/{key.rsplit('/', 1)[1]}"
            path = store / rel
            if path.exists():
                files[rel] = run016.sha256(path)
                continue
            try:
                files[rel] = fetch_verified(hooks, key, path)
            except FetchError as e:
                errors[f"{'klines' if kind == 'klines' else 'funding'}_error:{sym}:{mo}"] = str(e)[:200]
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
            tail = hooks['hl_funding'](c, end_ms - 2 * DAY_MS, end_ms - 1)
            if tail:
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


def traded_at_cut(root, rel, want_sha):
    """True if the pinned 2026-06 file of a symbol has a traded row (count > 0) in its last 7 days (names, days and
    trade counts only; no price is used). A file that does not match its pinned hash refuses."""
    blob = (root / run016.KLINES_A_REL / rel).read_bytes()
    R.refuse_unless(D.sha256_bytes(blob) == want_sha, f'{rel} does not match the pinned SHA256SUMS')
    rows = D.parse_klines(D.zip_text(blob), not_before=R.day(R.SPAN_A[1]) - 6, before=R.day(R.SPAN_C[0]))
    return any(r[2] > 0 for r in rows.values())


def coverage(root, store, files, a_sums):
    """Keyed refusals from the files on disk and the pinned span-(a) listing (names, months, days, trade counts; no
    price). A symbol that traded in the last 7 days of 2026-06 must continue in 2026-07 (the seam); a symbol's forward
    months are contiguous; BTCUSDT has every forward day."""
    refuse = {}
    by_sym = {}
    for rel in files:
        fm = D.file_month(rel)
        if fm and fm[0] == 'klines':
            by_sym.setdefault(fm[1], set()).add(fm[2])
    span = months(*run016.FWD_MONTHS)
    for sym, have in sorted(by_sym.items()):
        lo, hi = min(have), max(have)
        gap = [m for m in span if lo <= m <= hi and m not in have]
        if gap:
            refuse[f'klines_gap:{sym}'] = f'interior months missing {gap[:3]}'
    for rel, h in sorted(a_sums.items()):
        fm = D.file_month(rel)
        if not fm or fm[0] != 'klines' or fm[2] != run016.A_MONTH_LAST or not traded_at_cut(root, rel, h):
            continue
        sym = fm[1]
        if span[0] not in by_sym.get(sym, ()):
            refuse[f'klines_seam:{sym}'] = 'traded at the end of 2026-06 but no 2026-07 file was fetched'
    btc = sorted(m for m in by_sym.get(R.BTC, ()))
    if btc != span:
        refuse['btc_months'] = f'BTCUSDT forward months {len(btc)} of {len(span)}'
    else:
        days = set()
        for m in span:
            rel = f'klines/{R.BTC}/1d/{R.BTC}-1d-{m}.zip'
            days |= set(D.parse_klines(D.zip_text((store / rel).read_bytes())))
        want = set(range(R.day(R.SPAN_C[0]), R.day(R.END) + 1))
        if not want <= days:
            refuse['btc_tail'] = f'BTCUSDT misses {len(want - days)} forward days (last {max(days) if days else None})'
    return refuse


def attempt_history(root, results_rel=run016.RESULTS_REL):
    """{attempt: {'refusals': set, 'time': committer time of the earliest commit holding its end row}}."""
    att = {}
    for line, r in run016.result_rows(root, results_rel):
        if r.get('mode') != 'fetch_attempt':
            continue
        a = att.setdefault(int(r.get('attempt', 0)), {'refusals': set(), 'time': None})
        a['refusals'] |= set(r.get('refusals') or [])
        c = run016.first_commit_containing(root, results_rel, line)
        t = run016._git(root, 'show', '-s', '--format=%ct', c).stdout.strip() if c else ''
        if t and (a['time'] is None or int(t) < a['time']):
            a['time'] = int(t)
    return att


def apply_persistence(refusals, att, now_s):
    blocking, persistent = {}, {}
    for key, msg in refusals.items():
        if not key.startswith(PERSISTABLE) or key.startswith(f'klines_seam:{R.BTC}') or f':{R.BTC}' in key:
            blocking[key] = msg
            continue
        times = sorted(a['time'] for a in att.values() if key in a['refusals'] and a['time'] is not None)
        chain, last = 1, now_s
        for t in reversed(times):
            if last - t >= PERSIST_GAP_S:
                chain += 1
                last = t
        (persistent if chain >= PERSIST_ATTEMPTS else blocking)[key] = msg
    return blocking, persistent


# ------------------------------------------------------------------ the fetch
def fetch(root=None, store=None, now=None, hooks=None, **guard_kw):
    root = Path(root or run016.HERE).resolve()
    store = Path(store).resolve() if store else root / run016.STORE_REL
    real_store = store == (run016.HERE / run016.STORE_REL).resolve()
    over = set(guard_kw) | ({'now'} if now is not None else set())
    pre = []
    if real_store and (root != run016.HERE or over):
        pre.append('(B1) the real data tree is written only by the guard of the real repository with default arguments')
    if hooks is None:
        if root != run016.HERE or not real_store or over:
            pre.append('(B1) the real network hooks run only for the real repository, the real data tree and the '
                       'default guard arguments')
    else:
        if real_store:
            pre.append('(B1) mock network hooks are never used on the real data tree')
        if set(hooks) != set(HOOK_NAMES):
            pre.append(f'(B1) mock hooks must supply exactly {list(HOOK_NAMES)}; no fallback to a real hook')
    if pre:
        raise SystemExit('\n'.join(pre))
    kw = dict(guard_kw)
    if hooks is None:
        kw.update(toplevel=run016.REPO, anchor=run016.ANCHOR, env_check=True)
    failures, ctx = run016.guard_failures(root, 'fetch', now=now, store_rel=str(store.relative_to(root))
                                          if store.is_relative_to(root) else run016.STORE_REL, **kw)
    if failures:
        raise SystemExit('\n'.join(failures))
    _REAL['ok'] = hooks is None
    hooks = dict(REAL_HOOKS) if hooks is None else dict(hooks)
    code = guard_kw.get('code', run016.GUARDED_CODE)
    results = root / run016.RESULTS_REL

    def commit(row, msg):
        line = run016.append_row(row, results)
        r = run016._git(root, 'commit', '--only', '-m', msg, '--', run016.RESULTS_REL)
        if r.returncode or line not in run016.head_lines(root):
            raise SystemExit(f'STOP: could not commit {run016.RESULTS_REL} ({r.stderr.strip()[:160]}). Commit it BY HAND, '
                             'as it is; every later fetch or read refuses (k) until it is committed.')
        return line
    if not ctx['resume']:
        commit({'registration': run016.REG_NAME, 'mode': 'fetch_started', 'registration_commit': ctx['commit'],
                'code_hashes': run016.hashes(root, code), 'env': run016.env_fingerprint(),
                'run_at': dt.datetime.now(dt.UTC).isoformat()}, '016: fetch_started (automatic, r016_fetch.py)')
    att = attempt_history(root)
    n_att = max(att, default=0) + 1
    now = now or dt.datetime.now(dt.UTC)
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
        a_sums = D.parse_sums((root / run016.KLINES_A_REL / 'SHA256SUMS').read_text()) \
            if (root / run016.KLINES_A_REL / 'SHA256SUMS').exists() else {}
        refusals = {**kerr, **ferr, **coverage(root, store, kfiles, a_sums), **herr}
        end_row['refusals'] = sorted(refusals)
        blocking, persistent = apply_persistence(refusals, att, now.timestamp())
        names = {k.split(':')[1] for k in persistent}
        if len(names) > MAX_UNAVAILABLE:
            blocking['unavailable_ceiling'] = f'{len(names)} names would become unavailable: STOP for a decision'
        end_row['unavailable'] = {'persistent_after_3_attempts': sorted(persistent)}
        _write(store / 'hl_presence.json', json.dumps(hl, sort_keys=True).encode())
        report = {'klines_files': len(kfiles), 'funding_files': len(ffiles), 'later_keys_counted_only':
                  k_later + f_later, 'hl_coins': len(hl),
                  'unavailable': end_row['unavailable']}
        _write(store / 'report.json', json.dumps(report, sort_keys=True, indent=1).encode())
        if blocking:
            raise FetchError('coverage refused (no fetched row; re-run later):\n  '
                             + '\n  '.join(f'{k}: {v}' for k, v in list(blocking.items())[:20]))
    finally:
        _REAL['ok'] = False
        end_row['run_at'] = dt.datetime.now(dt.UTC).isoformat()
        commit(end_row, f'016: fetch attempt {n_att} end (automatic)')
    files = {**kfiles, **ffiles}
    for name in ('fapi_exchangeInfo.json', 'hl_presence.json', 'report.json'):
        files[name] = run016.sha256(store / name)
    digest = run016.manifest_digest(files)
    _write(store / 'manifest.json', json.dumps({'digest': digest, 'files': files}, sort_keys=True).encode())
    row = {'registration': run016.REG_NAME, 'mode': 'fetched', 'registration_commit': ctx['commit'],
           'code_hashes': run016.hashes(root, code), 'env': run016.env_fingerprint(), 'manifest_digest': digest,
           'manifest_files': len(files), 'attempts': n_att, 'report': report,
           'run_at': dt.datetime.now(dt.UTC).isoformat()}
    commit(row, '016: fetched (automatic, r016_fetch.py)')
    print(f'fetched: {len(files)} files in the manifest after {n_att} attempt(s)')
    return row


if __name__ == '__main__':
    try:
        fetch()
    except FetchError as err:
        sys.exit(f'fetch stopped: {err}')
