"""Probe 1b: what drives the bull + leverage false passes. Drift x GARCH asymmetry x tails, constant slopes.
Synthetic only. Run after p1/p23/p6."""
import json, multiprocessing as mp, sys
import common as C
GJR = (0.05, 0.88, 0.10); SYM = (0.08, 0.90, 0.0); ALL = ('2023-09-01', '2027-12-31')
W = [
    {'name': 'B1 bull +40, GJR t3', 'garch': GJR, 'df': 3, 'drift': [(*ALL, 0.4)]},
    {'name': 'B2 bull +80, symmetric GARCH t3', 'garch': SYM, 'df': 3, 'drift': [(*ALL, 0.8)]},
    {'name': 'B3 bull +80, GJR t4', 'garch': GJR, 'df': 4, 'drift': [(*ALL, 0.8)]},
    {'name': 'B4 bull +120, GJR t3', 'garch': GJR, 'df': 3, 'drift': [(*ALL, 1.2)]},
    {'name': 'B5 bull +80 GJR t3, crash -35% in (a) 2025-10-10', 'garch': GJR, 'df': 3, 'drift': [(*ALL, 0.8)],
     'jumps': {'2025-10-10': -0.35}},
    {'name': 'B6 bear -60, GJR t3', 'garch': GJR, 'df': 3, 'drift': [(*ALL, -0.6)]},
    {'name': 'B7 bull +80 GJR t3 + crash in (c), rerun seed', 'garch': GJR, 'df': 3, 'drift': [(*ALL, 0.8)],
     'jumps': {'2026-11-16': -0.35}},
]
def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    out = []
    with mp.Pool(10) as pool:
        for i, w in enumerate(W):
            recs = C.run_world(w, n, 7_000_000 + 10_000 * i, pool)
            row = {'world': w['name'], 'n': n}
            for k in ('t', 't_year', 't_half'):
                v = [r[k] for r in recs]
                row[k] = {'mean': C.mean(v), 'q95': C.q(v, .95), 'p_ge_1.731': sum(x >= 1.731 for x in v) / n}
            row['h1_pass'] = sum(r['t'] >= 1.731 and r['alpha_c_pos'] for r in recs) / n
            row['h1_pass_year_1.714'] = sum(r['t_year'] >= 1.714 for r in recs) / n
            row['mean_beta_btc'] = C.mean([r['beta_btc'] for r in recs]); row['mean_alpha'] = C.mean([r['alpha'] for r in recs])
            out.append(row); print(json.dumps(row), flush=True)
    json.dump(out, open('p1b_out.json', 'w'), indent=1)
if __name__ == '__main__':
    main()
