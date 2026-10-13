"""Probe 1 + 4: size of H1 under the reviewer's own regime worlds (constant slopes vs year / half-year cells), the
Monte Carlo error of CAL_H1, and how much real trend alpha the cell models absorb. Synthetic only."""
import json
import multiprocessing as mp
import sys
import time

import common as C

CAL, FLOOR = 1.731, 1.699
GJR = (0.05, 0.88, 0.10)
A = ('2025-03-20', '2026-06-30')
CELLS = [('2025-03-20', '2025-06-30', 0.9), ('2025-07-01', '2025-12-31', -0.7), ('2026-01-01', '2026-06-30', 0.3),
         ('2026-07-01', '2026-12-31', -0.5), ('2027-01-01', '2027-06-30', 0.8), ('2027-07-01', '2027-12-31', -0.3)]
ALL = ('2023-09-01', '2027-12-31')

NULLS = [
    {'name': 'N1 flat, GJR leverage, t3', 'garch': GJR, 'df': 3},
    {'name': 'N2 bull +80%/yr, GJR leverage, t3', 'garch': GJR, 'df': 3, 'drift': [(*ALL, 0.8)]},
    {'name': 'N3 bull +80%, GJR t3, -35% crash in (c)', 'garch': GJR, 'df': 3, 'drift': [(*ALL, 0.8)],
     'jumps': {'2026-11-16': -0.35}},
    {'name': 'N4 alt-minus-BTC drift (BTC +30%, alts -50%/yr extra)', 'btc_extra': [(*ALL, 0.3)],
     'alt_extra': [(*ALL, -0.5)]},
    {'name': 'N5 beta rises in uptrends (x1.4/x0.6), bull +60%', 'beta_trend': 0.4, 'drift': [(*ALL, 0.6)]},
    {'name': 'N6 vol timing pays (+100% low vol / -60% high), GJR t3', 'garch': GJR, 'df': 3, 'voldrift': (1.0, -0.6)},
    {'name': 'N7 2022-style bear in (c) with +12% short squeezes, GJR', 'garch': GJR,
     'drift': [('2026-07-01', '2027-06-30', -0.9)], 'squeeze': ('2026-07-01', '2027-06-30', 1 / 25, 0.12)},
    {'name': 'N8 2025-26 path in (a) + alt bleed in (a), GJR t3, (c) flat', 'garch': GJR, 'df': 3,
     'drift': [('2025-04-01', '2025-10-05', 1.2), ('2025-10-11', '2026-02-28', -1.0), ('2026-03-01', '2026-06-30', -0.2)],
     'jumps': {'2025-10-10': -0.20}, 'alt_extra': [(*A, -0.5)]},
    {'name': 'N9 universe enters on rallies (volume ~ 30d return), flat', 'vol_link': 3.0},
    {'name': 'N10 universe enters on rallies, bull +60%, GJR t3', 'vol_link': 3.0, 'garch': GJR, 'df': 3,
     'drift': [(*ALL, 0.6)]},
    {'name': 'N11 binding world re-run (cells aligned, t3, GJR)', 'drift': CELLS, 'df': 3, 'garch': GJR},
]
TRENDISH = [   # deterministic or random regimes: a trend book legitimately harvests these (reported, never size)
    {'name': 'R1 momentum crash x2 (quiet +150% rally, -35% day, V rebound), GJR', 'garch': GJR,
     'drift': [('2025-06-01', '2025-10-09', 1.5), ('2025-10-11', '2025-12-10', 1.0),
               ('2026-11-01', '2027-02-28', 1.5), ('2027-03-02', '2027-04-30', 1.0)],
     'jumps': {'2025-10-10': -0.35, '2027-03-01': -0.35}},
    {'name': 'R2 year-aligned bull/bear +80/-80/+80', 'drift': [('2025-01-01', '2025-12-31', 0.8),
                                                             ('2026-01-01', '2026-12-31', -0.8),
                                                             ('2027-01-01', '2027-12-31', 0.8)]},
    {'name': 'R3 Markov regimes +-80%/yr, mean duration 120 d', 'regime': (0.8, 120)},
    {'name': 'T1 AR trend h60 kappa 0.116 (drafter SR~1)', 'kappa': 0.116, 'half_life': 60},
    {'name': 'T2 AR trend h120 kappa 0.110 (drafter SR~1)', 'kappa': 0.110, 'half_life': 120},
    {'name': 'T3 AR trend h20 kappa 0.153 (drafter SR~1)', 'kappa': 0.153, 'half_life': 20},
]


def summ(name, recs):
    n = len(recs)
    o = {'world': name, 'n': n}
    for k in ('t', 't_year', 't_half'):
        v = [r[k] for r in recs]
        o[k] = {'mean': C.mean(v), 'q95': C.q(v, 0.95), 'p_ge_cal': sum(x >= CAL for x in v) / n,
                'p_ge_floor': sum(x >= FLOOR for x in v) / n}
    o['h1_pass'] = sum(r['t'] >= CAL and r['alpha_c_pos'] for r in recs) / n
    o['mean_sr'] = C.mean([r['sr'] for r in recs])
    o['mean_alpha'] = C.mean([r['alpha'] for r in recs])
    o['mean_T'] = C.mean([r['T'] for r in recs])
    o['h2_reject'] = sum(r['T'] < 0.117 for r in recs) / n
    return o


def main():
    draws = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    out = {'nulls': [], 'trendish': []}
    keep = {}
    t0 = time.time()
    with mp.Pool(10) as pool:
        for i, w in enumerate(NULLS):
            n = 3000 if w['name'].startswith('N11') else draws
            recs = C.run_world(w, n, 1_000_000 + 10_000 * i, pool)
            keep[w['name']] = recs
            s = summ(w['name'], recs)
            out['nulls'].append(s)
            print(json.dumps(s), f'{time.time() - t0:.0f}s', flush=True)
        for i, w in enumerate(TRENDISH):
            recs = C.run_world(w, 600, 2_000_000 + 10_000 * i, pool)
            keep[w['name']] = recs
            s = summ(w['name'], recs)
            out['trendish'].append(s)
            print(json.dumps(s), f'{time.time() - t0:.0f}s', flush=True)
    json.dump(out, open('p1_out.json', 'w'), indent=1)
    json.dump(keep, open('p1_records.json', 'w'))


if __name__ == '__main__':
    main()
