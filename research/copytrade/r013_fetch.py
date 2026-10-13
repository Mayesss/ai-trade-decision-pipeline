"""Registration 013 (N3 i, new perp listings) — the owner-approved pull from data.binance.vision (free, keyless).

    .venv/bin/python -I r013_fetch.py list              # S3 key listings (names, dates, sizes only) -> data/r013/listing/
    .venv/bin/python -I r013_fetch.py plan              # events (ct/listings013 rules) + per-source file plan; prints bytes
    .venv/bin/python -I r013_fetch.py fetch <source>    # download + verify against .CHECKSUM; resumable; SHA256SUMS when complete
    .venv/bin/python -I r013_fetch.py verify <source>   # re-hash every file against SHA256SUMS and its .CHECKSUM

Sources, each in its OWN new directory under data/r013/:
  klines1m   futures/um daily klines 1m, days d0 .. d0+9 of every non-censored first-segment listing whose
             listing day falls in one of the three calendars (reported / discovery / confirmation)
  funding    futures/um monthly fundingRate for the months those days touch
  klines1h   futures/um monthly klines 1h, 2024-01 .. 2026-06, every universe symbol (the market index)
Listed, never downloaded: daily indexPriceKlines 1m keys of the listing month (the pre-market flag: no index
file on d0) and spot daily 1d keys of the month of d0-1 (the spot-first flag).

HARD RULES (refusals raised before any request, never `assert`; tested): no key whose last calendar day is on or after 2026-07-01 is
ever requested (listing responses that name later keys are dropped in memory, never saved or counted); the plan
must stay under 5 GB; a listed key that cannot be fetched, or whose SHA-256 differs from its .CHECKSUM, is an
error — retried, never cached, never recorded as 'absent'; listings page FORWARD (S3 marker) and refuse a
truncated final page; SHA256SUMS is written only when every planned key of the source is verified.
"""
import concurrent.futures as cf
import csv
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from ct import listings013 as L  # noqa: E402

DATA = HERE / 'data' / 'r013'
LISTING = DATA / 'listing'
INPUTS = DATA / 'inputs'
RAW = {'klines1m': DATA / 'raw_klines1m', 'funding': DATA / 'raw_funding', 'klines1h': DATA / 'raw_klines1h'}
PLAN = DATA / 'plan.csv'
XI_SOURCE = HERE / 'data' / 'scout_l4' / 'exchangeinfo' / 'fapi_exchangeInfo.json'   # 2026-10-11 snapshot (L4)
S3 = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
DL = 'https://data.binance.vision/'
NS = '{http://s3.amazonaws.com/doc/2006-03-01/}'
DATE = re.compile(r'-(\d{4}-\d{2})(-\d{2})?\.zip$')
MAX_BYTES = 5_000_000_000
MARKET_MONTHS = ('2024-01', '2026-06')         # the market index: discovery and confirmation spans only
QUOTES = ('USDT', 'BUSD', 'FDUSD', 'USDC', 'BTC', 'BNB')
RATE, THREADS, TRIES = 40.0, 24, 6


class FetchError(Exception):
    pass


# ------------------------------------------------------------------ dates
def key_last_day(key):
    """Last calendar day a data file can contain: its day, or its month's last day."""
    m = DATE.search(key)
    if not m:
        raise ValueError(f'no date in key {key}')
    y, mo = map(int, m.group(1).split('-'))
    if m.group(2):
        return dt.date(y, mo, int(m.group(2)[1:])).isoformat()
    return (dt.date(y + (mo == 12), mo % 12 + 1, 1) - dt.timedelta(days=1)).isoformat()


def assert_before_cutoff(key):
    """A refusal (L.Refused), never an `assert`: it holds under `python -O` (review B m4)."""
    L.refuse_unless(key_last_day(key) < L.CUTOFF_DAY, f'refused: {key} reaches on or after {L.CUTOFF_DAY}')


# ------------------------------------------------------------------ HTTP (hooks replaced in tests)
class Pacer:
    def __init__(self, rate):
        self.gap, self.lock, self.next = 1.0 / rate, threading.Lock(), time.monotonic()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            t = max(now, self.next)
            self.next = t + self.gap
        if t > now:
            time.sleep(t - now)


PACER = Pacer(RATE)


def _raw_get(url):
    """One GET -> (status, body). Network errors raise."""
    req = urllib.request.Request(url, headers={'User-Agent': 'r013-fetch/1'})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, b''


HTTP = {'get': _raw_get, 'sleep': time.sleep}


def get(url, tries=TRIES):
    """200 -> body. Anything else (404 included) is retried, then raises FetchError. Never returns None."""
    last = None
    for i in range(tries):
        PACER.wait()
        try:
            st, body = HTTP['get'](url)
            if st == 200:
                return body
            last = f'HTTP {st}'
        except Exception as e:  # noqa: BLE001 - transient network errors are retried
            last = repr(e)[:120]
        HTTP['sleep'](min(20, 1.5 * (i + 1)))
    raise FetchError(f'{url}: {last} after {tries} tries')


def list_prefix(prefix, delimiter=None):
    """Forward-paged S3 v1 listing -> ([(key, size)], [common prefixes]). The final page must say
    IsTruncated=false; any error raises FetchError (nothing is returned partially)."""
    keys, prefs, marker = [], [], ''
    while True:
        q = {'prefix': prefix, 'marker': marker}
        if delimiter:
            q['delimiter'] = delimiter
        body = get(S3 + '?' + urllib.parse.urlencode(q))
        try:
            root = ET.fromstring(body)
        except ET.ParseError as e:
            raise FetchError(f'unparseable listing for {prefix}: {e}') from e
        trunc = root.find(NS + 'IsTruncated')
        if trunc is None:
            raise FetchError(f'listing for {prefix} has no IsTruncated element')
        page_last = None
        for c in root.findall(NS + 'Contents'):
            k = c.find(NS + 'Key').text
            keys.append((k, int(c.find(NS + 'Size').text)))
            page_last = k
        for p in root.findall(NS + 'CommonPrefixes'):
            pp = p.find(NS + 'Prefix').text
            prefs.append(pp)
            page_last = max(page_last or '', pp)
        if trunc.text != 'true':
            return keys, prefs
        if page_last is None:
            raise FetchError(f'truncated listing for {prefix} with an empty page')
        nm = root.find(NS + 'NextMarker')
        marker = nm.text if nm is not None and nm.text else page_last


def zips_before_cutoff(keys):
    """[(key, size)] -> [(key, day-or-month, size)] of .zip data files whose last day is before the cutoff.
    Later keys are dropped here, in memory, uncounted."""
    out = []
    for k, sz in keys:
        if not k.endswith('.zip') or not DATE.search(k):
            continue
        if key_last_day(k) < L.CUTOFF_DAY:
            m = DATE.search(k)
            out.append((k, m.group(1) + (m.group(2) or ''), sz))
    return out


def _atomic_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.part')
    tmp.write_text(json.dumps(obj, sort_keys=True))
    os.replace(tmp, path)


def _pmap(f, items):
    with cf.ThreadPoolExecutor(THREADS) as ex:
        return list(ex.map(f, items))


# ------------------------------------------------------------------ list
def cmd_list():
    """Symbols under futures/um/daily/klines/, then each universe symbol's daily 1d keys (dates only)."""
    _, prefs = list_prefix('data/futures/um/daily/klines/', delimiter='/')
    syms = sorted({p.rstrip('/').rsplit('/', 1)[-1] for p in prefs})
    syms = [s for s in syms if L.UNIV.match(s)]

    def one(s):
        return s, sorted({d for _, d, _ in zips_before_cutoff(list_prefix(f'data/futures/um/daily/klines/{s}/1d/')[0])})
    days = dict(_pmap(one, syms))
    days = {s: v for s, v in days.items() if v}
    _atomic_json(LISTING / 'um_daily_1d_days.json', days)
    INPUTS.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(XI_SOURCE, INPUTS / 'fapi_exchangeInfo.json')
    print(json.dumps({'symbol_dirs_matching_universe_regex': len(syms), 'with_1d_files_before_cutoff': len(days)}))


def load_events():
    days = json.loads((LISTING / 'um_daily_1d_days.json').read_text())
    xi = json.loads((INPUTS / 'fapi_exchangeInfo.json').read_text())
    return L.events_from_listing(days, L.excluded_symbols(xi)), days, xi


def bases(sym):
    b = sym[:-4]
    out = [b]
    for p in ('1000000', '10000', '1000', '1M'):
        if b.startswith(p) and len(b) > len(p):
            out.append(b[len(p):])
    return out


def months_of(days):
    return sorted({d[:7] for d in days})


def cmd_plan():
    events, days, _ = load_events()
    want = [e for e in events if e['kind'] == 'listing' and not e['censored']
            and L.calendar_of_day(e['d0']) in L.CALENDARS]

    def one(e):
        s, d0 = e['symbol'], e['d0']
        need = L.pull_days(d0)
        prev = (dt.date.fromisoformat(d0) - dt.timedelta(days=1)).isoformat()
        m1 = []
        for m in months_of([prev] + need):
            m1 += zips_before_cutoff(list_prefix(f'data/futures/um/daily/klines/{s}/1m/{s}-1m-{m}')[0])
        idx = zips_before_cutoff(list_prefix(f'data/futures/um/daily/indexPriceKlines/{s}/1m/{s}-1m-{d0[:7]}')[0])
        fr = zips_before_cutoff(list_prefix(f'data/futures/um/monthly/fundingRate/{s}/{s}-fundingRate-')[0])
        spot = []
        for b in bases(s):
            for q in QUOTES:
                spot += zips_before_cutoff(list_prefix(f'data/spot/daily/klines/{b}{q}/1d/{b}{q}-1d-{prev[:7]}')[0])
        return {'id': e['id'], 'symbol': s, 'd0': d0, 'calendar': L.calendar_of_day(d0),
                'm1': [(k, d, z) for k, d, z in m1 if d in need], 'm1_days_before_d0': sorted(d for _, d, _ in m1 if d < d0),
                'index_on_d0': any(d == d0 for _, d, _ in idx),
                'funding': [(k, d, z) for k, d, z in fr if d in months_of(need)],
                'spot_on_prev_day': any(d == prev for _, d, _ in spot)}
    per = _pmap(one, want)
    xi_syms = L.excluded_symbols(json.loads((INPUTS / 'fapi_exchangeInfo.json').read_text()))
    univ = sorted(s for s in days if L.UNIV.match(s) and s not in xi_syms)

    def h1(s):
        return s, [(k, d, z) for k, d, z in zips_before_cutoff(list_prefix(f'data/futures/um/monthly/klines/{s}/1h/')[0])
                   if MARKET_MONTHS[0] <= d <= MARKET_MONTHS[1]]
    mkt = dict(_pmap(h1, univ))
    rows = []
    for p in per:
        rows += [('klines1m', p['symbol'], k, z) for k, _, z in p['m1']]
        rows += [('funding', p['symbol'], k, z) for k, _, z in p['funding']]
    for s, ks in mkt.items():
        rows += [('klines1h', s, k, z) for k, _, z in ks]
    rows = sorted(set(rows))
    for r in rows:
        assert_before_cutoff(r[2])
    total = sum(r[3] for r in rows)
    L.refuse_unless(total < MAX_BYTES, f'plan {total} bytes exceeds the 5 GB line')
    tmp = PLAN.with_suffix('.part')
    with open(tmp, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['source', 'symbol', 'key', 'bytes'])
        w.writerows(rows)
    os.replace(tmp, PLAN)
    _atomic_json(LISTING / 'event_keys.json', per)
    by = {}
    for r in rows:
        n, b = by.get(r[0], (0, 0))
        by[r[0]] = (n + 1, b + r[3])
    print(json.dumps({'events_planned': len(per), 'market_symbols': sum(1 for v in mkt.values() if v),
                      'files_bytes_by_source': by, 'total_bytes': total}, indent=1))


# ------------------------------------------------------------------ fetch / verify
def sha256_bytes(b):
    return hashlib.sha256(b).hexdigest()


def sha256_file(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def fetch_one(key, raw):
    """Download one listed key and verify it against its .CHECKSUM. Returns the record, or raises
    FetchError. Nothing is written unless the SHA-256 matches."""
    assert_before_cutoff(key)
    want = get(DL + key + '.CHECKSUM').decode().split()[0].lower()
    got = None
    for _ in range(3):
        body = get(DL + key)
        got = sha256_bytes(body)
        if got == want:
            dest = raw / key
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix('.part')
            tmp.write_bytes(body)
            os.replace(tmp, dest)
            (raw / (key + '.CHECKSUM')).write_text(f'{want}  {Path(key).name}\n')
            return {'key': key, 'sha256': got, 'bytes': len(body)}
    raise FetchError(f'{key}: checksum mismatch (want {want}, got {got})')


def planned(source):
    with open(PLAN) as f:
        return [r['key'] for r in csv.DictReader(f) if r['source'] == source]


def verified(raw):
    out = {}
    p = raw / '_verified.jsonl'
    if p.exists():
        for line in p.read_text().splitlines():
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            out[r['key']] = r
    return out


def cmd_fetch(source):
    raw = RAW[source]
    keys = planned(source)
    for k in keys:
        assert_before_cutoff(k)
    raw.mkdir(parents=True, exist_ok=True)
    done = verified(raw)
    todo = [k for k in keys if k not in done or not (raw / k).exists()]
    print(f'{source}: {len(keys)} planned, {len(keys) - len(todo)} verified earlier, {len(todo)} to fetch', flush=True)
    errors, lock, n, t0 = [], threading.Lock(), [0], time.time()
    with open(raw / '_verified.jsonl', 'a') as out:
        def run(k):
            try:
                rec = fetch_one(k, raw)
            except FetchError as e:
                with lock:
                    errors.append(str(e))
                return
            with lock:
                out.write(json.dumps(rec) + '\n')
                n[0] += 1
                if n[0] % 500 == 0:
                    out.flush()
                    print(f'  {n[0]}/{len(todo)} {time.time() - t0:.0f}s', flush=True)
        _pmap(run, todo)
    if errors:
        print(f'{len(errors)} keys failed (not recorded; re-run to resume):', *errors[:10], sep='\n  ')
        sys.exit(1)
    done = verified(raw)
    missing = [k for k in keys if k not in done or not (raw / k).exists()]
    if missing:
        sys.exit(f'{len(missing)} planned keys are not verified; no SHA256SUMS written')
    lines = sorted(f'{done[k]["sha256"]}  {k}' for k in keys)
    (raw / 'SHA256SUMS').write_text('\n'.join(lines) + '\n')
    print(json.dumps({'source': source, 'files': len(keys), 'bytes': sum(done[k]['bytes'] for k in keys),
                      'SHA256SUMS': sha256_file(raw / 'SHA256SUMS')}))


def cmd_verify(source):
    raw = RAW[source]
    ok = bad = 0
    bad_keys = []
    for line in (raw / 'SHA256SUMS').read_text().splitlines():
        h, k = line.split('  ', 1)
        ck = (raw / (k + '.CHECKSUM')).read_text().split()[0].lower()
        if (raw / k).exists() and sha256_file(raw / k) == h == ck:
            ok += 1
        else:
            bad += 1
            bad_keys.append(k)
    print(json.dumps({'source': source, 'ok': ok, 'mismatch_or_missing': bad, 'examples': bad_keys[:5]}))
    return bad == 0


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else ''
    if cmd == 'list':
        cmd_list()
    elif cmd == 'plan':
        cmd_plan()
    elif cmd == 'fetch' and len(sys.argv) == 3 and sys.argv[2] in RAW:
        cmd_fetch(sys.argv[2])
    elif cmd == 'verify' and len(sys.argv) == 3 and sys.argv[2] in RAW:
        sys.exit(0 if cmd_verify(sys.argv[2]) else 1)
    else:
        sys.exit(__doc__)
