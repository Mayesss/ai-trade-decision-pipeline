"""Registration 004 — the run. REFUSES to start unless the registration is committed with
status REGISTERED and unmodified since. One line appended to results.jsonl.

    .venv/bin/python l6_run.py            # T1 provider, T2 cross-venue capture
    .venv/bin/python l6_run.py --triggers # print trigger count only (no outcome)
"""
import bisect
import datetime as dt
import json
import re
import statistics
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq

from ct import bitget as bg, eventstudy as es, liq, passive, provider, tape, trigger
from ct.leaders import con
from ct.net import DATA
from l5_run import summarise_trial, MAJORS, HORIZONS  # noqa: F401  (same four criteria, same placebo)

HERE = Path(__file__).resolve().parent
REG = HERE / 'registrations/004-forced-flow-liquidity-provider.md'
RESULTS = HERE / 'results.jsonl'
DEX = 'hyperliquid'
SECOND, MINUTE, HOUR, DAY = 1_000, 60_000, 3_600_000, 86_400_000
TRIALS, Z_CRIT = 2, 1.96
# D1-D9 (registration §9)
TRIG_FLOOR, TRIG_FRAC, TRIG_WINDOW, TRIG_COOLDOWN = 100_000.0, 0.5, 60_000, 15 * MINUTE
GROSS_MIN = 5_000_000.0
RUNGS, SIZES = (0.005, 0.010, 0.015), (1, 2, 3)
LATENCY, LIFE_AFTER_FLOW, LIFE_CAP = 2 * SECOND, 60 * SECOND, 300 * SECOND
FORCED_WINDOW = 1 * SECOND
EXIT_AFTER_FLOW, EXIT_CAP, SCRATCH_DELAY = 10 * MINUTE, 240 * MINUTE, 2 * SECOND
EXIT_SLIP = 0.0005
STRESS_WINDOW, STRESS_THRESHOLD = 5 * MINUTE, 77_000_000.0   # D8: p99.9 of market-wide 5-min liquidation notional
CAPTURE_HOLD = 15 * MINUTE
LIQ = DATA / f'derived/{DEX}/liquidations.parquet'


def guard():
    text = REG.read_text()
    m = re.search(r'^\*\*Status: (\w+)', text, re.M)
    if not m or m.group(1) != 'REGISTERED':
        sys.exit(f'{REG.name} is not REGISTERED — refusing to compute any forward return.')
    if subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--', str(REG)], cwd=HERE).returncode:
        sys.exit(f'{REG.name} differs from the committed version — commit (as an amendment) first.')
    return subprocess.run(['git', 'log', '-1', '--format=%H', '--', str(REG)], cwd=HERE,
                          capture_output=True, text=True).stdout.strip()


def build_triggers():
    """Triggers with their ladders, flow end and stress state. Behaviour only (no forward price)."""
    c = con()
    daily = {}
    for coin, day, n in c.execute(f"SELECT coin, (t // {DAY}) * {DAY}, SUM(notional) FROM read_parquet('{LIQ}') GROUP BY 1, 2 ORDER BY 1, 2").fetchall():
        daily.setdefault(coin, []).append((day, n))
    all_days = sorted((d, n) for v in daily.values() for d, n in v)
    fallback = liq.trailing_median_fn(all_days)
    table = {k: r for k, r in json.loads((DATA / 'p0/2026-10-07/symbols.json').read_text()).items() if r['status'] == 'ok'}
    oi = {}
    for r in pq.read_table(DATA / f'derived/{DEX}/oi_by_coin_snapshot.parquet').to_pylist():
        oi.setdefault(r['coin'], []).append((r['snapshot_ms'], r['gross_notional']))
    # market-wide liquidation notional per second-level bucket for the stress switch
    mkt = c.execute(f"SELECT (t // 1000) * 1000 AS s, SUM(notional) FROM read_parquet('{LIQ}') GROUP BY 1 ORDER BY 1").fetchall()
    mkt_t = [s for s, _ in mkt]
    mkt_c = [0.0]
    for _, n in mkt:
        mkt_c.append(mkt_c[-1] + n)

    def market_rate(t):
        hi = bisect.bisect_right(mkt_t, t)
        lo = bisect.bisect_right(mkt_t, t - STRESS_WINDOW)
        return mkt_c[hi] - mkt_c[lo]
    out, skipped = [], {'stress': 0, 'gross': 0, 'no_rung': 0}
    for coin in sorted(daily):
        if coin not in table:
            continue
        fills = c.execute(f"SELECT t, direction, notional, price FROM read_parquet('{LIQ}') WHERE coin = ? ORDER BY t", [coin]).fetchall()
        own = liq.trailing_median_fn(daily[coin])
        thr = lambda t, own=own: (lambda m: max(TRIG_FLOOR, TRIG_FRAC * m) if m else None)(own(t) or fallback(t))
        runs = liq.runs(fills, TRIG_WINDOW)
        run_starts = [r['t_start'] for r in runs]
        g = sorted(oi.get(coin, []))
        times = [t for t, _, _, _ in fills]
        for tr in trigger.triggers(fills, thr, TRIG_WINDOW, TRIG_COOLDOWN):
            i = bisect.bisect_left([x for x, _ in g], tr['t']) - 1
            if i < 0 or g[i][1] < GROSS_MIN:
                skipped['gross'] += 1
                continue
            if provider.stress(market_rate(tr['t']), STRESS_THRESHOLD):
                skipped['stress'] += 1
                continue
            j = bisect.bisect_right(run_starts, tr['t']) - 1
            run = runs[j]
            k0 = bisect.bisect_left(times, tr['t'] - TRIG_WINDOW)
            ref = next(fills[k][3] for k in range(k0, len(fills)) if liq.side_of(fills[k][1]) is not None)
            rungs = provider.ladder(tr['side'], ref, tr['price'], RUNGS, SIZES)
            if not rungs:
                skipped['no_rung'] += 1
                continue
            t_post = tr['t'] + LATENCY
            t_ttl = min(run['t_end'] + LIFE_AFTER_FLOW, tr['t'] + LIFE_CAP)
            if t_ttl <= t_post:
                t_ttl = t_post + LIFE_AFTER_FLOW
            out.append({'coin': coin, 'symbol': table[coin]['symbol'], 'side': tr['side'], 't_trig': tr['t'], 'ref': ref,
                        'flow_end': run['t_end'], 't_post': t_post, 't_ttl': t_ttl, 'major': coin in MAJORS,
                        'rungs': [{'level': lv, 'size': w, 'rung': r} for lv, w, r in rungs]})
    return out, skipped


def forced_flags(crossings):
    """crossings: [(key, coin, t)] -> {key: True if a liquidation print in the coin within FORCED_WINDOW}."""
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE cx(key VARCHAR, coin VARCHAR, t BIGINT)')
    c.executemany('INSERT INTO cx VALUES (?, ?, ?)', [(k, co, int(t)) for k, co, t in crossings])
    rows = c.execute(f"""SELECT cx.key, COUNT(l.t) FROM cx LEFT JOIN read_parquet('{LIQ}') l
                         ON l.coin = cx.coin AND l.t BETWEEN cx.t - {FORCED_WINDOW} AND cx.t + {FORCED_WINDOW} GROUP BY cx.key""").fetchall()
    return {k: n > 0 for k, n in rows}


def main():
    if '--triggers' in sys.argv:
        trigs, skipped = build_triggers()
        print(f'triggers {len(trigs):,} ({sum(len(t["rungs"]) for t in trigs):,} rungs); skipped {skipped}')
        return
    commit = guard()
    trigs, skipped = build_triggers()
    print(f'triggers {len(trigs):,} ({sum(len(t["rungs"]) for t in trigs):,} rungs); skipped {skipped}')
    out = {'registration': REG.name, 'registration_commit': commit, 'run_at': dt.datetime.now(dt.UTC).isoformat(),
           'params': {'rungs': RUNGS, 'sizes': SIZES, 'latency_ms': LATENCY, 'life_after_flow_ms': LIFE_AFTER_FLOW,
                      'life_cap_ms': LIFE_CAP, 'forced_window_ms': FORCED_WINDOW, 'exit_after_flow_ms': EXIT_AFTER_FLOW,
                      'exit_cap_ms': EXIT_CAP, 'stress_threshold': STRESS_THRESHOLD, 'gross_min': GROSS_MIN,
                      'trials': TRIALS, 'z_crit': Z_CRIT, 'cumulative_trials_001_004': 13},
           'triggers': len(trigs), 'skipped': skipped}
    # fills per rung, batched by day
    by_day = {}
    for ti, t in enumerate(trigs):
        for ri, r in enumerate(t['rungs']):
            r['key'] = f'{ti}:{ri}'
            by_day.setdefault(t['t_post'] // DAY, []).append((r['key'], t['coin'], t['side'], r['level'], t['t_post'], t['t_ttl'], ti, ri))
    rung_rows = []
    for n_day, (day, os_) in enumerate(sorted(by_day.items()), 1):
        det = tape.first_touch_details(DEX, [o[:6] for o in os_])
        filled = [(o, det[o[0]]) for o in os_ if o[0] in det]
        flags = forced_flags([(o[0], o[1], d[0]) for o, d in filled]) if filled else {}
        # reference touch after the fill (opposite side: a print back at or beyond the reference)
        ref_q = [(o[0], o[1], 'S' if o[2] == 'L' else 'L', trigs[o[6]]['ref'], d[0], d[0] + EXIT_CAP) for o, d in filled]
        touched = tape.first_touch_windows(DEX, ref_q) if ref_q else {}
        price_q = []
        for o, d in filled:
            t_fill = d[0]
            tr = trigs[o[6]]
            forced = flags.get(o[0], False)
            x_t = provider.exit_time(t_fill, tr['flow_end'], touched.get(o[0]), EXIT_AFTER_FLOW, EXIT_CAP) if forced else t_fill + SCRATCH_DELAY
            price_q += [(f'{o[0]}:exit', o[1], x_t), (f'{o[0]}:15', o[1], t_fill + 15 * MINUTE), (f'{o[0]}:60', o[1], t_fill + HOUR),
                        (f'{o[0]}:cap', o[1], t_fill + CAPTURE_HOLD)]
            rung_rows.append({'key': o[0], 'coin': o[1], 'symbol': tr['symbol'], 'side': o[2], 'level': o[3], 'size': trigs[o[6]]['rungs'][o[7]]['size'],
                              'rung': trigs[o[6]]['rungs'][o[7]]['rung'], 'fill_t': t_fill, 'forced': forced, 'exit_t': x_t,
                              'cluster': t_fill // HOUR, 'entry': t_fill, 'major': tr['major'], 'ti': o[6], 'ret': {}})
        prices = tape.prices_at(DEX, price_q) if price_q else {}
        for r in [x for x in rung_rows if x['fill_t'] // DAY == day or x['exit_t'] // DAY >= day]:
            if f"{r['key']}:exit" in prices and 'held' not in r:
                px = prices.get(f"{r['key']}:exit")
                held, ret = provider.fill_outcome(r['side'], r['level'], r['forced'], px, px, EXIT_SLIP)
                r['held'], r['ret']['15m'] = held, ret                 # primary outcome stored under the trial's key
                r['ret_fixed15'] = passive.passive_return(r['side'], r['level'], prices.get(f"{r['key']}:15"), EXIT_SLIP)
                r['ret_fixed60'] = passive.passive_return(r['side'], r['level'], prices.get(f"{r['key']}:60"), EXIT_SLIP)
                r['hl_cap'] = prices.get(f"{r['key']}:cap")
        if n_day % 50 == 0:
            print(f'  T1 days {n_day}/{len(by_day)}, rungs filled {len(rung_rows):,}')
    # T1 — size-weighted: replicate rows by size for the unweighted statistics
    weighted = [dict(r) for r in rung_rows if r['ret'].get('15m') is not None for _ in range(r['size'])]
    majors = [r for r in weighted if r['major']]
    out['T1'] = summarise_trial(majors)
    out['T1']['primary_universe'] = 'majors'
    out['T1']['all_coins'] = summarise_trial(weighted, placebo_seeds=50)
    out['T1']['alts'] = summarise_trial([r for r in weighted if not r['major']], placebo_seeds=50)
    out['T1']['fills'] = {'rungs_posted': sum(len(t['rungs']) for t in trigs), 'rungs_filled': len(rung_rows),
                          'forced_share': sum(1 for r in rung_rows if r['forced']) / len(rung_rows) if rung_rows else None,
                          'by_rung': {str(k): {'filled': sum(1 for r in rung_rows if r['rung'] == k),
                                               'mean': statistics.fmean([r['ret']['15m'] for r in rung_rows if r['rung'] == k and r['ret'].get('15m') is not None] or [0])}
                                      for k in RUNGS}}
    no_check = [dict(r, ret={'15m': r['ret_fixed15']}) for r in rung_rows if r.get('ret_fixed15') is not None for _ in range(r['size'])]
    out['T1']['reported'] = {'no_counterparty_check_fixed_15m': summarise_trial([r for r in no_check if r['major']], placebo_seeds=20)['primary'],
                             'fixed_60m_majors': summarise_trial([dict(r, ret={'15m': r['ret_fixed60']}) for r in rung_rows if r['major'] and r.get('ret_fixed60') is not None for _ in range(r['size'])], placebo_seeds=20)['primary'],
                             'held_share': sum(1 for r in rung_rows if r.get('held')) / len(rung_rows) if rung_rows else None,
                             'ev_per_posted_size_majors': (sum(r['size'] * r['ret']['15m'] for r in rung_rows if r['major'] and r['ret'].get('15m') is not None)
                                                           / sum(sum(x['size'] for x in t['rungs']) for t in trigs if t['major'])) if any(t['major'] for t in trigs) else None,
                             'ev_per_posted_size_all': (sum(r['size'] * r['ret']['15m'] for r in rung_rows if r['ret'].get('15m') is not None)
                                                        / sum(sum(x['size'] for x in t['rungs']) for t in trigs)) if trigs else None}
    # T2 — cross-venue capture on forced fills: hedge on Bitget at the next minute open, unwind both at +15 min
    cap_rows = []
    for r in rung_rows:
        if not r['forced'] or r.get('hl_cap') is None:
            continue
        m = r['fill_t'] - r['fill_t'] % MINUTE + MINUTE
        bg_in, bg_out = bg.candle_at(r['symbol'], m), bg.candle_at(r['symbol'], m + CAPTURE_HOLD)
        tot, hl_leg, bg_leg = provider.hedged_capture(r['side'], r['level'], r['hl_cap'], bg_in, bg_out, EXIT_SLIP)
        if tot is not None:
            cap_rows += [dict(r, ret={'15m': tot}, hl_leg=hl_leg, bg_leg=bg_leg) for _ in range(r['size'])]
    out['T2'] = summarise_trial([r for r in cap_rows if r['major']])
    out['T2']['primary_universe'] = 'majors'
    out['T2']['all_coins'] = summarise_trial(cap_rows, placebo_seeds=50) if cap_rows else None
    out['T2']['legs_majors'] = {'hl': statistics.fmean(r['hl_leg'] for r in cap_rows if r['major']) if any(r['major'] for r in cap_rows) else None,
                                'bg': statistics.fmean(r['bg_leg'] for r in cap_rows if r['major']) if any(r['major'] for r in cap_rows) else None}
    out['T2']['legs_all'] = {'hl': statistics.fmean(r['hl_leg'] for r in cap_rows) if cap_rows else None,
                             'bg': statistics.fmean(r['bg_leg'] for r in cap_rows) if cap_rows else None}
    with open(RESULTS, 'a') as f:
        f.write(json.dumps(out, default=str) + '\n')
    print(json.dumps({k: {kk: vv for kk, vv in out[k].items() if kk in ('primary', 'criteria', 'pass', 'fills', 'legs_majors')}
                      for k in ('T1', 'T2')}, indent=1, default=str)[:3000])


if __name__ == '__main__':
    main()
