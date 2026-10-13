"""Review A of 015, probe 3: an independent size / power simulation (own generator and estimator; dates only).

For each world: the registered rule (gate t >= 1.91 / 2.13 / t_148), the registered rule plus a post-2017 sign
condition (mean y over the T+2 and T+1 months > 0), a vol-scaled variant (y / trailing 20-day RMS ending T-9), and a
trimmed variant (two largest |y| months dropped); q95 of each gate; the decayed and TOM-only alternatives.

    python3 -I research/alpha/scratch_review015a/p3_worlds.py [sims]   -> scratch_review015a/p3_out.json
"""
import json
import multiprocessing as mp
import sys
import time

import common as K

WORLDS = [
    # name, null?, spec
    ('N iid normal', {}),
    ('N garch t4', {'garch': (0.12, 0.87), 'kind': 't4'}),
    ('N gjr t4 (asym, a.03 g.15 b.88)', {'gjr': (0.03, 0.15, 0.88), 'kind': 't4'}),
    ('N garch t4 + daily AR -0.08', {'garch': (0.12, 0.87), 'kind': 't4', 'ar': -0.08}),
    ('N garch t4 + 2020 aligned', {'garch': (0.12, 0.87), 'kind': 't4', 'shocks': [('2020', 'aligned')]}),
    ('N garch t4 + 2020&2025 aligned (stress)', {'garch': (0.12, 0.87), 'kind': 't4',
                                                 'shocks': [('2020', 'aligned'), ('2025', 'aligned')]}),
    ('N garch t4 + 2020&2025 anywhere', {'garch': (0.12, 0.87), 'kind': 't4',
                                         'shocks': [('2020', 'anywhere'), ('2025', 'anywhere')]}),
    ('N gjr t4 + 2020 aligned', {'gjr': (0.03, 0.15, 0.88), 'kind': 't4', 'shocks': [('2020', 'aligned')]}),
    ('A full iid', {'effect': 1.0}),
    ('A half iid', {'effect': 0.5}),
    ('A full garch t4', {'garch': (0.12, 0.87), 'kind': 't4', 'effect': 1.0}),
    ('A half garch t4', {'garch': (0.12, 0.87), 'kind': 't4', 'effect': 0.5}),
    ('A full garch t4, effect prop. to vol', {'garch': (0.12, 0.87), 'kind': 't4', 'effect': 1.0, 'prop': True}),
    ('A half garch t4, effect prop. to vol', {'garch': (0.12, 0.87), 'kind': 't4', 'effect': 0.5, 'prop': True}),
    ('A full garch t4 + 2020&2025 anywhere', {'garch': (0.12, 0.87), 'kind': 't4', 'effect': 1.0,
                                              'shocks': [('2020', 'anywhere'), ('2025', 'anywhere')]}),
    ('D decayed: T+3 era only, full, iid', {'effect': 1.0, 'mode': 'decayed_t3'}),
    ('D died at publication (2020-01), full before, iid', {'effect': 1.0, 'mode': 'died_pub'}),
    ('D linear decay full(2014-01) -> 0 (2026-05), iid', {'effect': 1.0, 'mode': 'linear_decay'}),
    ('D died at publication, x1.5 before, garch t4', {'garch': (0.12, 0.87), 'kind': 't4', 'effect': 1.5,
                                                      'mode': 'died_pub'}),
    ('U unshifted full iid', {'effect': 1.0, 'mode': 'unshifted'}),
    ('U TOM only (T..T+3 +23 bp abnormal) iid', {'effect': 1.0, 'mode': 'tom_only'}),
    ('U TOM only x2 (+46 bp) iid', {'effect': 2.0, 'mode': 'tom_only'}),
]


def sim_world(args):
    wi, sims = args
    name, w = WORLDS[wi]
    dates, rows = K.load()
    n = len(dates)
    rng = K.rng_for(91000 + wi)
    recs = []
    for _ in range(sims):
        x, sig = K.gen(n, rng, w.get('kind', 'normal'), w.get('garch'), w.get('gjr'), w.get('ar', 0.0))
        for nm, how in w.get('shocks', ()):
            K.place(x, rows, K.SHOCKS[nm], how, rng, n)
        if w.get('effect'):
            K.inject(x, sig, rows, w['effect'], w.get('mode', 'shifted'), w.get('prop', False))
        st = K.stats(x, rows)
        recs.append(st)
    return name, recs


def summarise(name, recs, crits):
    N = len(recs)
    f = lambda pred: sum(1 for r in recs if pred(r)) / N
    t148 = K.tq(148)
    out = {'sims': N,
           'q95_gate': K.quantile([r['gate'] for r in recs], 0.95),
           'q95_gate_z': K.quantile([r['gate_z'] for r in recs], 0.95),
           'q95_gate_trim2': K.quantile([r['gate_trim2'] for r in recs], 0.95),
           'mean_t': sum(r['t'] for r in recs) / N,
           'pass_t148': f(lambda r: r['gate'] >= t148),
           'pass_191': f(lambda r: r['gate'] >= 1.91),
           'pass_213': f(lambda r: r['gate'] >= 2.13),
           'pass_191_post': f(lambda r: r['gate'] >= 1.91 and r['post_mean'] > 0),
           'pass_213_post': f(lambda r: r['gate'] >= 2.13 and r['post_mean'] > 0),
           'pass_z_at': {str(c): f(lambda r, c=c: r['gate_z'] >= c) for c in crits['z']},
           'pass_z_post_at': {str(c): f(lambda r, c=c: r['gate_z'] >= c and r['post_mean'] > 0) for c in crits['z']},
           'pass_trim_at': {str(c): f(lambda r, c=c: r['gate_trim2'] >= c) for c in crits['trim']},
           'P_post_mean_pos': f(lambda r: r['post_mean'] > 0),
           'P_t1_mean_pos': f(lambda r: r['t1_mean'] > 0),
           'mean_v_bp': 1e4 * sum(r['v_mean'] for r in recs) / N,
           'mean_sd_v_pct': 100 * sum(r['v_sd'] for r in recs) / N,
           'median_maxabs_y_pct': 100 * K.quantile([r['maxabs'] for r in recs], 0.5)}
    return out


if __name__ == '__main__':
    sims = int(sys.argv[1]) if len(sys.argv) > 1 else 4000
    t0 = time.time()
    with mp.Pool(min(len(WORLDS), mp.cpu_count())) as pool:
        res = dict(pool.map(sim_world, [(i, sims) for i in range(len(WORLDS))]))
    crits = {'z': [1.70, 1.75, 1.80, 1.85, 1.91], 'trim': [1.70, 1.75, 1.80, 1.91]}
    out = {name: summarise(name, recs, crits) for name, recs in res.items()}
    (K.HERE / 'p3_out.json').write_text(json.dumps(out, indent=1))
    for name, o in out.items():
        print(f"{name:52s} q95 {o['q95_gate']:.3f} z {o['q95_gate_z']:.3f} trim {o['q95_gate_trim2']:.3f} | "
              f"t148 {o['pass_t148']:.3f} 1.91 {o['pass_191']:.3f} 2.13 {o['pass_213']:.3f} "
              f"1.91+post {o['pass_191_post']:.3f} | z1.75 {o['pass_z_at']['1.75']:.3f} z1.80 {o['pass_z_at']['1.8']:.3f} "
              f"z1.85 {o['pass_z_at']['1.85']:.3f} | trim1.80 {o['pass_trim_at']['1.8']:.3f} | meanT {o['mean_t']:.2f} "
              f"Ppost>0 {o['P_post_mean_pos']:.3f}", flush=True)
    print(f'{time.time() - t0:.0f}s')
