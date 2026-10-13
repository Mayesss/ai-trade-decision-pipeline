"""Registration 009 — the HOLDOUT-A pull (built before registration, Revision 3; never run before
the holdout guard passes).

    .venv/bin/python -I r009_pull_holdout.py plan       # listing only: the file plan and its bytes
    .venv/bin/python -I r009_pull_holdout.py fetch      # REFUSES unless r009_run's holdout-A guard passes
    .venv/bin/python -I r009_pull_holdout.py rest       # 2025-07 settlements by end-dated REST (same guard)
    .venv/bin/python -I r009_pull_holdout.py manifest
    .venv/bin/python -I r009_pull_holdout.py build      # panel for r009_run.py --holdout-A

What it pulls, into data/binance_years_holdoutA/ (its own directory and manifest; never data/carry/):
  * monthly `klines` 1h and `fundingRate` 2023-12 -> 2025-06 (December 2023 is the 30-day warm-up of
    the first holdout signal, 2024-01-08), for every crypto USDT perpetual with klines in that span;
  * daily `klines` 1h 2025-07-01 -> 2025-07-28 (the last exit is the 01:00 open of 2025-07-28, the
    first hour of holdout B's features-only week; accepted in Revision 1);
  * the settlements of 2025-07-01 00:00 -> 2025-07-28 01:00 by Binance REST, end-dated (the monthly
    2025-07 file would carry the rest of July, which is holdout B).
No metrics: F1 reads none, and the life table comes from the listing. Nothing dated after
2025-07-28 is requested (asserted per key).
"""
import csv
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import r009_pull as P  # noqa: E402

OUT = os.path.join(HERE, 'data', 'binance_years_holdoutA')
assert 'data/carry' not in OUT.replace('\\', '/')            # carry-guard (000g)
LAST_DAY = dt.date(2025, 7, 28)
NEVER_AFTER = dt.date(2025, 7, 29)
REST_START = int(dt.datetime(2025, 7, 1, tzinfo=dt.UTC).timestamp() * 1000) - 60_000
REST_END = int(dt.datetime(2025, 7, 28, 0, 59, 59, tzinfo=dt.UTC).timestamp() * 1000)


def point_pull_here():
    """Re-point r009_pull's module paths at the holdout directory and its date line."""
    P.OUT, P.RAW = OUT, os.path.join(OUT, 'raw')
    P.PLAN, P.STATE = os.path.join(OUT, 'plan.csv'), os.path.join(OUT, 'fetch_state.jsonl')
    P.MANIFEST = os.path.join(OUT, 'MANIFEST.tsv')
    P.NEVER_ON_OR_AFTER = NEVER_AFTER


def holdout_guard():
    import r009_run
    failures, _ = r009_run.guard_failures(r009_run.HERE, str(r009_run.REG.relative_to(r009_run.HERE)), mode='holdout_A')
    if failures:
        sys.exit('holdout-A pull refused:\n' + '\n'.join(failures))


def plan():
    bad = P.universe_names()
    ok = lambda s: re.fullmatch(r'[A-Z0-9]+USDT', s) and s not in bad
    kl = [r for r in P.read_listing('monthly_klines_1h') if ok(r[0]) and '2023-12' <= r[1] <= '2025-06']
    syms = sorted({r[0] for r in kl})
    rows = [('klines_1h', *r) for r in kl]
    rows += [('fundingRate', *r) for r in P.read_listing('monthly_fundingRate') if r[0] in set(syms) and '2023-12' <= r[1] <= '2025-06']
    last_full = {}
    for s, d, b, _ in P.read_listing('daily_metrics'):
        if b >= P.FULL and d < '2026-07-01':
            last_full[s] = max(last_full.get(s, ''), d)
    alive_july = [s for s in syms if last_full.get(s, '') >= '2025-07-01']
    day = dt.date(2025, 7, 1)
    while day <= LAST_DAY:
        for s in alive_july:
            rows.append(('klines_1h_daily', s, day.isoformat(), -1, f'data/futures/um/daily/klines/{s}/1h/{s}-1h-{day.isoformat()}.zip'))
        day += dt.timedelta(days=1)
    for r in rows:
        assert P.key_last_date(r[4]) < NEVER_AFTER, r
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, 'plan.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['dataset', 'symbol', 'date', 'bytes', 'key'])
        w.writerows(rows)
    known = sum(r[3] for r in rows if r[3] > 0)
    assert known < P.MAX_BYTES
    print(json.dumps({'symbols': len(syms), 'files': len(rows), 'bytes_listed': known, 'alive_july_2025': len(alive_july)}))


def rest():
    holdout_guard()
    with open(os.path.join(OUT, 'plan.csv')) as f:
        syms = sorted({r['symbol'] for r in csv.DictReader(f) if r['dataset'] == 'klines_1h_daily'})
    d = os.path.join(OUT, 'rest_funding')
    os.makedirs(d, exist_ok=True)
    for s in syms:
        path = os.path.join(d, f'{s}-2025-07.json')
        if os.path.exists(path):
            continue
        url = (f'https://fapi.binance.com/fapi/v1/fundingRate?symbol={s}&startTime={REST_START}'
               f'&endTime={REST_END}&limit=1000')
        with urllib.request.urlopen(url, timeout=30) as r:
            body = r.read()
        assert all(int(x['fundingTime']) <= REST_END for x in json.loads(body))
        with open(path, 'wb') as f:
            f.write(body)
        time.sleep(0.25)


def main():
    point_pull_here()
    cmd = sys.argv[1]
    if cmd == 'plan':
        plan()
    elif cmd == 'fetch':
        holdout_guard()
        P.fetch()
    elif cmd == 'rest':
        rest()
    elif cmd == 'manifest':
        P.manifest()
    elif cmd == 'build':
        holdout_guard()
        from ct import binance_years as by
        print(by.build(OUT, os.path.join(HERE, 'data', 'scout_l4', 'listings'),
                       hard_end=int(dt.datetime(2025, 7, 29, tzinfo=dt.UTC).timestamp() * 1000)))


if __name__ == '__main__':
    main()
