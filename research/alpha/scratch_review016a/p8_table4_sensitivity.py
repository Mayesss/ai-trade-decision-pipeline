"""Sensitivity of the 'as published' calibration (Revision 1 §6) to the leverage-effect assumption: the registered
Table-4 decade draw (ct16.sim.table4_draw, synthetic) under GJR t3 (registered) vs symmetric GARCH t4. h 60."""
import json, multiprocessing as mp, sys
sys.dont_write_bytecode = True
import common  # noqa: F401  (sets sys.path for ct16)
from ct16 import sim
GJR = (0.05, 0.88, 0.10)
VAR = {'GJR t3 (registered)': {'df': 3, 'garch': GJR}, 'symmetric GARCH t4': {'df': 4, 'garch': (0.08, 0.90, 0.0)}}
def job(a):
    spec, seed = a
    return sim.table4_draw(spec, seed)
if __name__ == '__main__':
    out = {}
    with mp.Pool(10) as pool:
        for vn, v in VAR.items():
            for k in (0.0, 0.04, 0.065, 0.09, 0.12):
                spec = {'start': '2014-01-01', 'end': '2025-03-19', 'drift': 'const', 'annual': 0.75, 'equal_drift': True,
                        'kappa': k, 'half_life': 60, **v}
                recs = pool.map(job, [(spec, 12_000_000 + i) for i in range(40)])
                m = sum(r['ratio'] for r in recs) / len(recs)
                out[f'{vn} k{k}'] = {'mean_ratio': m, 'beta': sum(r['beta'] for r in recs) / 40, 'vol': sum(r['vol'] for r in recs) / 40}
                print(vn, k, round(m, 3), 'beta', round(out[f'{vn} k{k}']['beta'], 3), 'vol', round(out[f'{vn} k{k}']['vol'], 3), flush=True)
    json.dump(out, open('p8_out.json', 'w'), indent=1)
