"""Probe 6: the gross held INTO a crash day when the crash follows a quiet rally (a long-only trend book is most
invested exactly then), against the unconditional gross distribution the draft's tail table uses. Synthetic only."""
import json
import multiprocessing as mp

import common as C

GJR = (0.05, 0.88, 0.10)
CRASH = ['2025-10-10', '2027-03-01']
W = [
    {'name': 'quiet +150% rally then -35% day (x2)', 'garch': GJR, 'vol_mult': 0.8,
     'drift': [('2025-06-01', '2025-10-09', 1.5), ('2026-11-01', '2027-02-28', 1.5)],
     'jumps': {'2025-10-10': -0.35, '2027-03-01': -0.35}},
    {'name': 'modest +60% rally then -35% day (x2)', 'garch': GJR,
     'drift': [('2025-06-01', '2025-10-09', 0.6), ('2026-11-01', '2027-02-28', 0.6)],
     'jumps': {'2025-10-10': -0.35, '2027-03-01': -0.35}},
    {'name': 'trend world h60 SR~1 with -35% days', 'kappa': 0.116, 'half_life': 60,
     'jumps': {'2025-10-10': -0.35, '2027-03-01': -0.35}},
]


def main():
    out = []
    with mp.Pool(10) as pool:
        for i, w in enumerate(W):
            recs = C.run_world(w, 300, 6_000_000 + 10_000 * i, pool, cells=False, extra=CRASH)
            g = [x for r in recs for x in r['gross_on']]
            rr = [x for r in recs for x in r['ret_on']]
            row = {'world': w['name'], 'gross_into_crash_p50': C.q(g, .5), 'p90': C.q(g, .9), 'max': max(g),
                   'uncond_gross_mean': C.mean([r['gross_mean'] for r in recs]),
                   'uncond_gross_max_p50': C.q([r['gross_max'] for r in recs], .5),
                   'crash_day_book_ret_p50': C.q(rr, .5), 'p10': C.q(rr, .1), 'min': min(rr),
                   'worst7_p50': C.q([r['worst7'] for r in recs], .5), 'worst30_p05': C.q([r['worst30'] for r in recs], .05),
                   'mdd_p05': C.q([r['mdd'] for r in recs], .05), 'book_vol_mean': C.mean([r['vol'] for r in recs])}
            out.append(row)
            print(json.dumps(row), flush=True)
    json.dump(out, open('p6_out.json', 'w'), indent=1)


if __name__ == '__main__':
    main()
