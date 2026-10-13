"""Revision 1 recheck (synthetic only): the registered timing statistic (ct16.stats.timing / h1 / ladder at 1555bf8)
on the reviewer's own worlds (common.make_world), at the registered end 2027-12-31 and at two later end dates.

Per draw: t_A pooled; t_A,c with (c)'s own wbar (registered); t_A,c as the (c) days of the POOLED series (pooled
wbar; a variant); the registered ladder outcome. Writes p7_records.json (raw, gitignored) and prints summaries."""
import datetime as dt
import json
import multiprocessing as mp
import sys
import time

import common as C
from ct16 import rule as R
from ct16 import stats as S

GJR = (0.05, 0.88, 0.10)
ENDS = {'2027-12-31': None, '2028-12-31': None, '2029-06-30': None}


def all_(d):
    return [('2023-09-01', '2029-12-31', d)]


SIZE = [
    {'name': 'N1 flat GJR t3', 'garch': GJR, 'df': 3},
    {'name': 'N2 bull +80 GJR t3', 'garch': GJR, 'df': 3, 'drift': all_(0.8)},
    {'name': 'B7 bull +80 GJR t3 crash (c)', 'garch': GJR, 'df': 3, 'drift': all_(0.8), 'jumps': {'2026-11-16': -0.35}},
    {'name': 'B4 bull +120 GJR t3', 'garch': GJR, 'df': 3, 'drift': all_(1.2)},
    {'name': 'N6 vol timing asym GJR t3', 'garch': GJR, 'df': 3, 'voldrift': (1.0, -0.6)},
    {'name': 'VT vol timing sym (drafter binding)', 'voldrift': (1.0, -1.0)},
    {'name': 'N11 cells aligned t3 GJR', 'df': 3, 'garch': GJR,
     'drift': [('2025-03-20', '2025-06-30', 0.9), ('2025-07-01', '2025-12-31', -0.7), ('2026-01-01', '2026-06-30', 0.3),
               ('2026-07-01', '2026-12-31', -0.5), ('2027-01-01', '2027-06-30', 0.8), ('2027-07-01', '2027-12-31', -0.3)]},
    {'name': 'N8 2025-26 path (a) alt bleed GJR t3', 'garch': GJR, 'df': 3,
     'drift': [('2025-04-01', '2025-10-05', 1.2), ('2025-10-11', '2026-02-28', -1.0), ('2026-03-01', '2026-06-30', -0.2)],
     'jumps': {'2025-10-10': -0.20}, 'alt_extra': [('2025-03-20', '2026-06-30', -0.5)]},
    {'name': 'S1 (a) trend SR~1.5, (c) dead', 'half_life': 60, 'kappa_windows': [('2023-09-01', '2026-06-30', 0.161)]},
    {'name': 'S2 (a) trend SR~2.5, (c) dead', 'half_life': 60, 'kappa_windows': [('2023-09-01', '2026-06-30', 0.25)]},
]
POWER = [
    {'name': 'SR1 h20', 'kappa': 0.153, 'half_life': 20},
    {'name': 'SR1 h60', 'kappa': 0.116, 'half_life': 60},
    {'name': 'SR1 h120', 'kappa': 0.110, 'half_life': 120},
    {'name': 'SR0.75 h60', 'kappa': 0.096, 'half_life': 60},
    {'name': 'SR0 h60 (dead)', 'kappa': 0.045, 'half_life': 60},
    {'name': 'kappa0 (dead, no trend)', 'half_life': 60},
    {'name': 'pub h20', 'kappa': 0.090, 'half_life': 20, 'garch': GJR, 'df': 3},
    {'name': 'pub h60', 'kappa': 0.065, 'half_life': 60, 'garch': GJR, 'df': 3},
    {'name': 'pub h120', 'kappa': 0.061, 'half_life': 120, 'garch': GJR, 'df': 3},
    {'name': 'VT vol timing sym (drafter binding)', 'voldrift': (1.0, -1.0)},
    {'name': 'N6 vol timing asym GJR t3', 'garch': GJR, 'df': 3, 'voldrift': (1.0, -0.6)},
]


def draw(args):
    spec, seed, end = args
    R.END = dt.date.fromisoformat(end)                 # the generator runs to R.END; compute end passed explicitly
    syms = C.make_world(spec, seed)
    a, b = R.day(R.START), R.day(R.END)
    bk = R.run_book(syms, a, b, R.universe_fn_for(syms))
    days, contrib, cost = bk['days'], bk['contrib'], bk['cost']
    h = S.h1(days, contrib, cost)
    A, _ = S.timing_series(contrib, cost)
    fi = S.forward_idx(days)
    tcp = S.mean_t([days[i] for i in fi], [A[i] for i in fi])['t']
    out = {'t': h['pooled']['t'], 't_c': h['forward']['t'], 't_c_pooledwbar': tcp, 'outcome': S.ladder(h),
           'sr': S.sharpe(days, bk['ret'])['sr'], 'G': h['pooled']['G']}
    return out


def main():
    which = sys.argv[1]
    n = int(sys.argv[2])
    ends = sys.argv[3].split(',')
    worlds = SIZE if which == 'size' else POWER
    path = f'p7_{which}_records.json'
    try:
        keep = json.load(open(path))
    except Exception:  # noqa: BLE001
        keep = {}
    t0 = time.time()
    with mp.Pool(10) as pool:
        for end in ends:
            for i, w in enumerate(worlds):
                key = f"{w['name']} | {end}"
                if key in keep and len(keep[key]) >= n:
                    continue
                recs = pool.map(draw, [(w, 11_000_000 + 10_000 * i + k, end) for k in range(n)], chunksize=6)
                keep[key] = recs
                al = sum(r['outcome'] == 'alive' for r in recs) / n
                de = sum(r['outcome'] == 'decayed' for r in recs) / n
                print(f"{key:48s} G {recs[0]['G']} t {C.mean([r['t'] for r in recs]):+.2f} q95 {C.q([r['t'] for r in recs], .95):.2f} "
                      f"q05 {C.q([r['t'] for r in recs], .05):+.2f} P(t>=1.75) {sum(r['t'] >= 1.75 for r in recs) / n:.3f} "
                      f"alive {al:.3f} decayed {de:.3f} | alive(pooled-wbar t_c) "
                      f"{sum(r['t'] >= 1.75 and r['t_c_pooledwbar'] >= 1.282 for r in recs) / n:.3f} {time.time() - t0:.0f}s",
                      flush=True)
                json.dump(keep, open(path, 'w'))


if __name__ == '__main__':
    main()
