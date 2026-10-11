"""Registration 009 (L4) — the owner-approved DISCOVERY pull from data.binance.vision.

    .venv/bin/python -I r009_pull.py plan       # build the file plan from the L4 scout's listings; prints bytes
    .venv/bin/python -I r009_pull.py fetch      # download + verify against .CHECKSUM (resumable, paced)
    .venv/bin/python -I r009_pull.py rest       # 2024-01-01 00:00 funding settlements (REST, end-dated)
    .venv/bin/python -I r009_pull.py manifest   # MANIFEST.tsv: path, bytes, sha256, checksum verdict

What is pulled (approved 2026-10-11: discovery only, ~1.54 GB):
  * daily `metrics` 2021-11-01 → 2023-12-31, monthly `fundingRate` and `klines` 1h 2021-11 → 2023-12,
    for every crypto USDT perpetual with at least one full (>= 4 KB) metrics day in that span
    (the scout's universe rule; index composites and the USDC stablecoin pair are not pulled);
  * daily `klines` 1h for 2024-01-01 only, for symbols still alive on 2023-12-25: the last discovery
    week's exit is the 01:00 open of 2024-01-01 (no monthly 2024-01 file is ever fetched — it would
    carry the whole of January);
  * the 2024-01-01 00:00 funding settlement from Binance REST `/fapi/v1/fundingRate`, end-dated
    2024-01-01 00:59:59 (the monthly 2024-01 funding file would carry all of January).

HARD LIMITS (asserted): nothing dated on or after 2024-01-08 is requested; the plan must stay under
2.0 GB. Files land in data/binance_years/raw/<key> (gitignored); this script lives outside it.
Every zip is verified against its `.CHECKSUM` before it counts as fetched; failures are retried and
then recorded, never silently kept.
"""
import concurrent.futures as cf
import csv
import datetime as dt
import hashlib
import http.client
import json
import os
import re
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
LIST = os.path.join(HERE, 'data', 'scout_l4', 'listings')
XI = os.path.join(HERE, 'data', 'scout_l4', 'exchangeinfo', 'fapi_exchangeInfo.json')
OUT = os.path.join(HERE, 'data', 'binance_years')
RAW = os.path.join(OUT, 'raw')
PLAN = os.path.join(OUT, 'plan.csv')
STATE = os.path.join(OUT, 'fetch_state.jsonl')
MANIFEST = os.path.join(OUT, 'MANIFEST.tsv')
HOST = 'data.binance.vision'

FIRST_DAY, LAST_DAY = '2021-11-01', '2023-12-31'
EXIT_DAY = '2024-01-01'                       # the last discovery exit (01:00 open) and funding (00:00)
NEVER_ON_OR_AFTER = dt.date(2024, 1, 8)       # the approval's hard line
MAX_BYTES = 2_000_000_000
FULL = 4000                                   # the scout's full-metrics-file rule (bytes)
EXCLUDE = {'BLUEBIRDUSDT', 'FOOTBALLUSDT', 'BTCDOMUSDT', 'DEFIUSDT', 'USDCUSDT'}  # index composites, stablecoin
RATE = 50.0                                   # requests per second, all threads together
THREADS = 40
DATE = re.compile(r'-(\d{4}-\d{2})(-\d{2})?\.zip$')


def key_last_date(key):
    """The last calendar day a file can contain: the day itself, or the month's last day."""
    m = DATE.search(key)
    y, mo = map(int, m.group(1).split('-'))
    if m.group(2):
        return dt.date(y, mo, int(m.group(2)[1:]))
    nxt = dt.date(y + (mo == 12), mo % 12 + 1, 1)
    return nxt - dt.timedelta(days=1)


def universe_names():
    xi = json.load(open(XI))['symbols']
    bad = {s['symbol'] for s in xi if s.get('contractType') == 'TRADIFI_PERPETUAL' or s.get('underlyingType') == 'INDEX'}
    return bad | EXCLUDE


def read_listing(name):
    with open(os.path.join(LIST, name + '.csv')) as f:
        return [(r['symbol'], r['date'], int(r['bytes']), r['key']) for r in csv.DictReader(f)]


def plan():
    bad = universe_names()
    ok = lambda s: re.fullmatch(r'[A-Z0-9]+USDT', s) and s not in bad
    metrics = [r for r in read_listing('daily_metrics') if ok(r[0]) and FIRST_DAY <= r[1] <= LAST_DAY]
    syms = sorted({r[0] for r in metrics if r[2] >= FULL})
    sset = set(syms)
    rows = [('metrics', *r) for r in metrics if r[0] in sset]
    for ds, name in (('fundingRate', 'monthly_fundingRate'), ('klines_1h', 'monthly_klines_1h')):
        rows += [(ds, *r) for r in read_listing(name) if r[0] in sset and FIRST_DAY[:7] <= r[1] <= LAST_DAY[:7]]
    # alive on 2023-12-25: a full metrics file on or after it (the whole listing, to 2026-06, is used only here)
    last_full = {}
    for s, d, b, _ in read_listing('daily_metrics'):
        if s in sset and b >= FULL and d < '2026-07-01':
            last_full[s] = max(last_full.get(s, ''), d)
    for s in syms:
        if last_full.get(s, '') >= '2023-12-25':
            rows.append(('klines_1h_daily', s, EXIT_DAY, -1, f'data/futures/um/daily/klines/{s}/1h/{s}-1h-{EXIT_DAY}.zip'))
    for r in rows:
        assert key_last_date(r[4]) < NEVER_ON_OR_AFTER, r
    known = sum(r[3] for r in rows if r[3] > 0)
    assert known < MAX_BYTES, f'plan {known} bytes exceeds the 2.0 GB stop line'
    os.makedirs(OUT, exist_ok=True)
    with open(PLAN, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['dataset', 'symbol', 'date', 'bytes', 'key'])
        w.writerows(rows)
    by = {}
    for r in rows:
        n, b = by.get(r[0], (0, 0))
        by[r[0]] = (n + 1, b + max(r[3], 0))
    print(json.dumps({'symbols': len(syms), 'files': len(rows), 'bytes_listed': known, 'by_dataset': by}, indent=1))


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


_local = threading.local()


def http_get(path, pacer, tries=6):
    for i in range(tries):
        pacer.wait()
        try:
            c = getattr(_local, 'conn', None)
            if c is None:
                c = _local.conn = http.client.HTTPSConnection(HOST, timeout=60)
            c.request('GET', '/' + path, headers={'User-Agent': 'r009-pull/1', 'Connection': 'keep-alive'})
            r = c.getresponse()
            body = r.read()
            if r.status == 404:
                return 404, b''
            if r.status == 200:
                return 200, body
            raise OSError(f'HTTP {r.status}')
        except Exception:  # noqa: BLE001 - reconnect and retry transient errors
            try:
                _local.conn.close()
            except Exception:  # noqa: BLE001
                pass
            _local.conn = None
            time.sleep(min(30, 2 ** i))
    return None, b''


def fetch_one(key, pacer):
    dest = os.path.join(RAW, key)
    st, ck = http_get(key + '.CHECKSUM', pacer)
    if st == 404:
        return {'key': key, 'status': 'absent'}
    if st != 200:
        return {'key': key, 'status': 'error_checksum'}
    want = ck.decode().split()[0].lower()
    for _ in range(3):
        st, body = http_get(key, pacer)
        if st != 200:
            return {'key': key, 'status': 'absent' if st == 404 else 'error_zip'}
        got = hashlib.sha256(body).hexdigest()
        if got == want:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest + '.CHECKSUM', 'wb') as f:
                f.write(ck)
            tmp = dest + '.part'
            with open(tmp, 'wb') as f:
                f.write(body)
            os.replace(tmp, dest)
            return {'key': key, 'status': 'ok', 'bytes': len(body), 'sha256': got}
    return {'key': key, 'status': 'checksum_mismatch', 'want': want, 'got': got}


def done_keys():
    out = {}
    if os.path.exists(STATE):
        with open(STATE) as f:
            for line in f:
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                out[r['key']] = r
    return out


def fetch():
    with open(PLAN) as f:
        keys = [r['key'] for r in csv.DictReader(f)]
    for k in keys:
        assert key_last_date(k) < NEVER_ON_OR_AFTER, k
    done = done_keys()
    todo = [k for k in keys if done.get(k, {}).get('status') not in ('ok', 'absent')
            or (done[k]['status'] == 'ok' and not os.path.exists(os.path.join(RAW, k)))]
    print(f'{len(keys)} planned, {len(keys) - len(todo)} already verified, {len(todo)} to fetch', flush=True)
    pacer, lock, n = Pacer(RATE), threading.Lock(), 0
    t0 = time.time()
    with open(STATE, 'a') as out, cf.ThreadPoolExecutor(THREADS) as ex:
        for res in ex.map(lambda k: fetch_one(k, pacer), todo):
            res['at'] = dt.datetime.now(dt.UTC).isoformat(timespec='seconds')
            with lock:
                out.write(json.dumps(res) + '\n')
                n += 1
                if n % 500 == 0:
                    out.flush()
                    print(f'  {n}/{len(todo)}  {time.time() - t0:.0f}s', flush=True)
    done = done_keys()
    tally = {}
    for k in keys:
        s = done.get(k, {}).get('status', 'missing')
        tally[s] = tally.get(s, 0) + 1
    print(json.dumps(tally))


def rest():
    """The 2024-01-01 00:00 settlement for symbols alive then, from REST, end-dated 00:59:59."""
    start = int(dt.datetime(2024, 1, 1, tzinfo=dt.UTC).timestamp() * 1000) - 60_000
    end = int(dt.datetime(2024, 1, 1, 0, 59, 59, tzinfo=dt.UTC).timestamp() * 1000)
    assert end < int(dt.datetime(2024, 1, 8, tzinfo=dt.UTC).timestamp() * 1000)
    with open(PLAN) as f:
        syms = sorted({r['symbol'] for r in csv.DictReader(f) if r['dataset'] == 'klines_1h_daily'})
    d = os.path.join(OUT, 'rest_funding')
    os.makedirs(d, exist_ok=True)
    got = {}
    for s in syms:
        path = os.path.join(d, f'{s}-2024-01-01.json')
        if not os.path.exists(path):
            url = f'https://fapi.binance.com/fapi/v1/fundingRate?symbol={s}&startTime={start}&endTime={end}&limit=10'
            for i in range(4):
                try:
                    with urllib.request.urlopen(url, timeout=30) as r:
                        body = r.read()
                    break
                except Exception:  # noqa: BLE001
                    time.sleep(2 * (i + 1))
                    body = None
            if body is None:
                continue
            rows = json.loads(body)
            assert all(int(x['fundingTime']) <= end for x in rows)
            with open(path, 'wb') as f:
                f.write(body)
            time.sleep(0.25)
        got[s] = len(json.load(open(path)))
    print(json.dumps({'symbols': len(syms), 'with_rows': sum(1 for v in got.values() if v), 'empty': [s for s, v in got.items() if not v]}))


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def manifest():
    done = done_keys()
    with open(PLAN) as f:
        keys = [r['key'] for r in csv.DictReader(f)]
    lines, nbytes, bad = [], 0, []
    for k in keys:
        r = done.get(k, {})
        p = os.path.join(RAW, k)
        if r.get('status') == 'ok' and os.path.exists(p):
            h = sha256(p)
            ck = open(p + '.CHECKSUM').read().split()[0].lower()
            verdict = 'ok' if h == ck else 'MISMATCH'
            if verdict != 'ok':
                bad.append(k)
            size = os.path.getsize(p)
            nbytes += size
            lines.append(f'raw/{k}\t{size}\t{h}\t{verdict}')
        else:
            lines.append(f'raw/{k}\t-\t-\t{r.get("status", "missing")}')
    rd = os.path.join(OUT, 'rest_funding')
    for fn in sorted(os.listdir(rd)) if os.path.isdir(rd) else []:
        p = os.path.join(rd, fn)
        lines.append(f'rest_funding/{fn}\t{os.path.getsize(p)}\t{sha256(p)}\tno-checksum(REST)')
    with open(MANIFEST, 'w') as f:
        f.write('path\tbytes\tsha256\tchecksum\n' + '\n'.join(lines) + '\n')
    tally = {}
    for ln in lines:
        v = ln.rsplit('\t', 1)[1]
        tally[v] = tally.get(v, 0) + 1
    print(json.dumps({'files': len(lines), 'zip_bytes': nbytes, 'verdicts': tally, 'mismatch': bad[:10],
                      'manifest_sha256': sha256(MANIFEST)}))


if __name__ == '__main__':
    {'plan': plan, 'fetch': fetch, 'rest': rest, 'manifest': manifest}[sys.argv[1]]()
