"""Review A of 015, probe 5: can a cheap extra condition stop a decayed effect from 'existing'?

Joint pass = registered gate (t >= 1.91, mean > 0, mean w/o largest > 0) AND the post-2017-09 months (T+2 and T+1,
105) have t_post >= k, for k in {-inf, 0, 0.5, 1.0, 1.28}; the same for the vol-scaled gate at 1.85. Same worlds and
seeds as p3 (re-simulated).

    python3 -I research/alpha/scratch_review015a/p5_decay_gate.py [sims]
"""
import json
import multiprocessing as mp
import sys

import common as K
import p3_worlds as P3

KEEP = ['N iid normal', 'N garch t4', 'N garch t4 + 2020 aligned', 'N garch t4 + 2020&2025 aligned (stress)',
        'A full iid', 'A half iid', 'A full garch t4', 'A half garch t4', 'A half garch t4, effect prop. to vol',
        'D decayed: T+3 era only, full, iid', 'D died at publication (2020-01), full before, iid',
        'D linear decay full(2014-01) -> 0 (2026-05), iid', 'D died at publication, x1.5 before, garch t4',
        'U unshifted full iid', 'U TOM only (T..T+3 +23 bp abnormal) iid']
KS = [-99, 0.0, 0.5, 1.0, 1.28]

if __name__ == '__main__':
    sims = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    idx = [i for i, (nm, _) in enumerate(P3.WORLDS) if nm in KEEP]
    with mp.Pool(min(len(idx), mp.cpu_count())) as pool:
        res = dict(pool.map(P3.sim_world, [(i, sims) for i in idx]))
    out = {}
    for name in KEEP:
        recs = res[name]
        N = len(recs)
        row = {}
        for k in KS:
            row[f'reg191_post_t>={k}'] = sum(1 for r in recs if r['gate'] >= 1.91 and r['post_t'] >= k) / N
            row[f'z185_post_t>={k}'] = sum(1 for r in recs if r['gate_z'] >= 1.85 and r['post_t'] >= k) / N
        out[name] = row
        print(f'{name:52s} ' + ' '.join(f'{k}:{row[f"reg191_post_t>={k}"]:.3f}/{row[f"z185_post_t>={k}"]:.3f}'
                                         for k in KS), flush=True)
    (K.HERE / 'p5_out.json').write_text(json.dumps(out, indent=1))
