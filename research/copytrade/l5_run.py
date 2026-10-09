"""Registration 003 — the run. REFUSES to start unless the registration is committed with
status REGISTERED and unmodified since. One line appended to results.jsonl.

    .venv/bin/python l5_run.py
"""
import datetime as dt
import json
import random
import re
import statistics
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq

from ct import eventstudy as es, passive, tape
from ct.leaders import con
from ct.net import DATA

HERE = Path(__file__).resolve().parent
REG = HERE / 'registrations/003-passive-liquidity-into-cascades.md'
RESULTS = HERE / 'results.jsonl'
DEX = 'hyperliquid'
MINUTE, HOUR, DAY = 60_000, 3_600_000, 86_400_000
TRIALS, Z_CRIT = 3, 2.13
HORIZONS = {'5m': 5 * MINUTE, '15m': 15 * MINUTE, '60m': HOUR, '240m': 4 * HOUR}
PRIMARY = '15m'
EXIT_SLIP = 0.0005                      # D4; 0 and 10 bp reported
D_PRIMARY = 0.02                        # D2; 1% and 3% reported
GROSS_MIN = None                        # D3, read from the registration (guard)
DENSE_MIN = None                        # D4, read from the registration (guard)
MAJORS = {'BTC', 'ETH', 'SOL'}


def guard():
    text = REG.read_text()
    m = re.search(r'^\*\*Status: (\w+)', text, re.M)
    if not m or m.group(1) != 'REGISTERED':
        sys.exit(f'{REG.name} is not REGISTERED — refusing to compute any forward return.')
    if subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--', str(REG)], cwd=HERE).returncode:
        sys.exit(f'{REG.name} differs from the committed version — commit (as an amendment) first.')
    g = re.search(r'`GROSS_MIN = \$([\d,]+)`', text)
    d = re.search(r'`DENSE_MIN = ([\d.]+)`', text)
    global GROSS_MIN, DENSE_MIN
    if not g or not d:
        sys.exit('GROSS_MIN / DENSE_MIN not found in the registration')
    GROSS_MIN, DENSE_MIN = float(g.group(1).replace(',', '')), float(d.group(1))
    return subprocess.run(['git', 'log', '-1', '--format=%H', '--', str(REG)], cwd=HERE,
                          capture_output=True, text=True).stdout.strip()


def summarise_trial(rows, placebo_seeds=200):
    """rows: [{'ret', 'cluster', 'entry', 'major', ...}] with ret at PRIMARY (None dropped).
    The four criteria of §4 / §5, the direction placebo, horizons and splits."""
    rows = [r for r in rows if r['ret'].get(PRIMARY) is not None]

    def block(rs, h=PRIMARY):
        v = [r['ret'][h] for r in rs]
        cl = [r['cluster'] for r in rs]
        m, se, t, n, g = es.cluster_t(v, cl)
        vv = [x for x in v if x is not None]
        return {'mean': m, 'se': se, 't': t, 'n': n, 'clusters': g, 'median': statistics.median(vv) if vv else None,
                'hit': (sum(1 for x in vv if x > 0) / len(vv)) if vv else None,
                'winsor20_mean': statistics.fmean(max(min(x, 0.2), -0.2) for x in vv) if vv else None}
    main = block(rows)
    srt = sorted(rows, key=lambda r: r['entry'])
    half = len(srt) // 2
    halves = [block(srt[:half])['mean'], block(srt[half:])['mean']] if half else [None, None]
    maj, rest = block([r for r in rows if r['major']]), block([r for r in rows if not r['major']])
    crit = {'mean_t': main['t'] is not None and main['t'] >= Z_CRIT and main['mean'] > 0,
            'mean_median_same_sign': main['median'] is not None and (main['mean'] > 0) == (main['median'] > 0),
            'both_halves_positive': all(h is not None and h > 0 for h in halves),
            'majors_and_rest_positive': (maj['mean'] or 0) > 0 and (rest['mean'] or 0) > 0}
    base = [r['ret'][PRIMARY] for r in rows]
    cl = [r['cluster'] for r in rows]
    hits = 0
    for s in range(placebo_seeds):
        rng = random.Random(s)
        _, _, t, _, _ = es.cluster_t([x * rng.choice((-1, 1)) for x in base], cl)
        hits += t is not None and abs(t) >= Z_CRIT
    biggest = max(set(cl), key=cl.count) if cl else None
    ex = [r for r in rows if r['cluster'] != biggest]
    return {'primary': main, 'criteria': crit, 'pass': all(crit.values()), 'halves': halves, 'majors': maj, 'rest': rest,
            'placebo_share': hits / placebo_seeds, 'without_largest_cluster': block(ex) if ex else None,
            'by_horizon': {h: block(rows, h) for h in HORIZONS},
            'by_side': {s: block([r for r in rows if r['side'] == s]) for s in ('L', 'S')}}


def t1_cascade_fills(events):
    """Per event: the cascade's liquidation fills (notional, price) from the extracted table."""
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE ev(k INTEGER, coin VARCHAR, t0 BIGINT, t1 BIGINT)')
    c.executemany('INSERT INTO ev VALUES (?, ?, ?, ?)', [(i, e['coin'], e['t_start'], e['t_end']) for i, e in enumerate(events)])
    rows = c.execute(f"""SELECT ev.k, l.notional, l.price FROM read_parquet('{DATA / f'derived/{DEX}/liquidations.parquet'}') l
                         JOIN ev ON l.coin = ev.coin AND l.t >= ev.t0 AND l.t <= ev.t1""").fetchall()
    out = {}
    for k, n, px in rows:
        out.setdefault(k, []).append((n, px))
    return out


def run_t1(events):
    fills = t1_cascade_fills(events)
    rows = []
    queries = []
    for i, e in enumerate(events):
        if i not in fills:
            continue
        fp = passive.cascade_fill_price(fills[i], e['side'])
        rows.append({'i': i, 'coin': e['coin'], 'side': e['side'], 'fill': fp, 'entry': e['t_end'],
                     'cluster': e['t_end'] // HOUR, 'major': e['coin'] in MAJORS, 'ret': {}, 'ret_slip': {}})
        for h, ms in HORIZONS.items():
            queries.append((f'{i}:{h}', e['coin'], e['t_end'] + ms))
    # price lookups batched per day
    by_day = {}
    for q in queries:
        by_day.setdefault(q[2] // DAY, []).append(q)
    prices = {}
    for n_day, (day, qs) in enumerate(sorted(by_day.items()), 1):
        prices.update(tape.prices_at(DEX, qs))
        if n_day % 50 == 0:
            print(f'    T1 price lookups: day {n_day}/{len(by_day)}')
    for r in rows:
        for h in HORIZONS:
            px = prices.get(f"{r['i']}:{h}")
            r['ret'][h] = passive.passive_return(r['side'], r['fill'], px, EXIT_SLIP)
            r['ret_slip'][h] = {s: passive.passive_return(r['side'], r['fill'], px, s) for s in (0.0, 0.0005, 0.001)}
    return rows


def run_t2(levels):
    """Resting orders at fixed distances, one per (snapshot, coin, side, d), alive until the next
    snapshot (at most 24 h). Returns order rows with fill time, outcome per horizon."""
    snaps = sorted({r['snapshot_ms'] for r in levels})
    nxt = {s: (snaps[i + 1] if i + 1 < len(snaps) else s + DAY) for i, s in enumerate(snaps)}
    by_snap = {}
    for r in levels:
        by_snap.setdefault(r['snapshot_ms'], []).append(r)
    orders = []
    for n_s, (s, lv) in enumerate(sorted(by_snap.items()), 1):
        end = min(nxt[s], s + DAY)
        keys = [(f"{s}:{r['coin']}:{r['side']}:{r['d']}", r['coin'], r['side'], r['level']) for r in lv]
        touched = tape.first_touch(DEX, keys, s, end)
        q = []
        for r in lv:
            k = f"{s}:{r['coin']}:{r['side']}:{r['d']}"
            tf = touched.get(k)
            o = {**r, 'key': k, 'window_end': end, 'fill_t': tf, 'filled': tf is not None, 'ret': {}, 'ret_slip': {},
                 'cluster': (tf // HOUR) if tf else None, 'entry': tf or s, 'major': r['coin'] in MAJORS}
            orders.append(o)
            if tf:
                for h, ms in HORIZONS.items():
                    q.append((f'{k}:{h}', r['coin'], tf + ms))
        prices = tape.prices_at(DEX, q) if q else {}
        for o in orders[-len(lv):]:
            if o['filled']:
                for h in HORIZONS:
                    px = prices.get(f"{o['key']}:{h}")
                    o['ret'][h] = passive.passive_return(o['side'], o['level'], px, EXIT_SLIP)
                    o['ret_slip'][h] = {sl: passive.passive_return(o['side'], o['level'], px, sl) for sl in (0.0, 0.0005, 0.001)}
        if n_s % 25 == 0:
            print(f'    T2 snapshots {n_s}/{len(by_snap)}, orders {len(orders):,}, filled {sum(1 for o in orders if o["filled"]):,}')
    return orders


def ev_block(orders):
    filled = [o for o in orders if o['filled'] and o['ret'].get(PRIMARY) is not None]
    return {'orders': len(orders), 'filled': len(filled), 'fill_rate': len(filled) / len(orders) if orders else None,
            'ev_per_order': (sum(o['ret'][PRIMARY] for o in filled) / len(orders)) if orders else None}


def main():
    commit = guard()
    grid = json.loads((DATA / f'derived/{DEX}/cascades.json').read_text())
    levels = pq.read_table(DATA / f'derived/{DEX}/map_levels.parquet').to_pylist()
    levels = [r for r in levels if r['gross'] >= GROSS_MIN]
    out = {'registration': REG.name, 'registration_commit': commit, 'run_at': dt.datetime.now(dt.UTC).isoformat(),
           'params': {'primary': PRIMARY, 'exit_slip': EXIT_SLIP, 'd_primary': D_PRIMARY, 'gross_min': GROSS_MIN,
                      'maker': passive.MAKER, 'taker': passive.TAKER, 'trials': TRIALS, 'z_crit': Z_CRIT}}
    print('T1 — passive fill at cascade prints')
    t1_rows = run_t1(grid['primary'])
    out['T1'] = summarise_trial(t1_rows)
    out['T1']['by_exit_slip'] = {str(s): es.cluster_t([r['ret_slip'][PRIMARY][s] for r in t1_rows if r['ret_slip'].get(PRIMARY)],
                                                       [r['cluster'] for r in t1_rows if r['ret_slip'].get(PRIMARY)])[0] for s in (0.0, 0.0005, 0.001)}
    print('T2 — resting orders at map levels')
    orders = run_t2(levels)
    all2 = [o for o in orders if o['d'] == D_PRIMARY]
    prim = [o for o in all2 if o['density'] >= DENSE_MIN]          # the strategy: placed only where the map shows flow
    sparse = [o for o in all2 if o['density'] < 0.0005]
    filled = [o for o in prim if o['filled']]
    out['T2'] = summarise_trial(filled)
    out['T2']['ev'] = ev_block(prim)
    out['T2']['ev_sparse_control'] = ev_block(sparse)
    out['T2']['ev_by_distance'] = {str(d): ev_block([o for o in orders if o['d'] == d and o['density'] >= DENSE_MIN]) for d in (0.01, 0.02, 0.03)}
    out['T2']['ev_by_side'] = {s: ev_block([o for o in prim if o['side'] == s]) for s in ('L', 'S')}
    out['T2']['ev_majors'] = ev_block([o for o in prim if o['major']])
    out['T2']['conditional_sparse'] = summarise_trial([o for o in sparse if o['filled']], placebo_seeds=20)['primary']
    # T3 — map value: density terciles within snapshot; conditional return and EV per tercile
    filled_all = [o for o in all2 if o['filled'] and o['ret'].get(PRIMARY) is not None]
    split = es.tercile_split(filled_all, key=lambda o: o['density'], value=lambda o: o['ret'][PRIMARY],
                             cluster=lambda o: o['cluster'], within=lambda o: o['snapshot_ms']) if filled_all else None
    terc = {}
    by_snap = {}
    for o in all2:
        by_snap.setdefault(o['snapshot_ms'], []).append(o)
    lab = {}
    for s, os_ in by_snap.items():
        os_ = sorted(os_, key=lambda o: o['density'])
        k = len(os_) // 3
        for i, o in enumerate(os_):
            lab[o['key']] = 0 if i < k else 1 if i < 2 * k else 2
    for q in range(3):
        terc[q] = ev_block([o for o in all2 if lab.get(o['key']) == q])
    out['T3'] = {'split': split, 'ev_by_density_tercile': terc,
                 'pass': bool(split and split['z'] is not None and split['z'] >= Z_CRIT
                              and all(m is not None for m in split['means']) and split['means'][2] > split['means'][1] > split['means'][0]
                              and out['T2']['pass'])}
    with open(RESULTS, 'a') as f:
        f.write(json.dumps(out, default=str) + '\n')
    print(json.dumps({k: {kk: vv for kk, vv in out[k].items() if kk in ('primary', 'criteria', 'pass', 'ev', 'split')}
                      for k in ('T1', 'T2', 'T3')}, indent=1, default=str)[:3000])


if __name__ == '__main__':
    main()
