"""Review A, Revision 1 recheck, probe 7: placement scan for the REGISTERED z rule at 1.92 (daily simulation, since z
needs the vol window). For the 2020 and 2008 shapes: 120 evenly spaced placements x 1,000 iid-normal draws; plus the
two worst placements re-run at 5,000 draws (to remove the selection bias). Synthetic only.

    python3 research/alpha/scratch_review015a/p7_zplacement_rev1.py
"""
import json
import multiprocessing as mp

import common as K

dates, rows = K.load()
n = len(dates)
LO, HI = rows[0]['pre'][0], rows[-1]['R'][-1]


def run(args):
    name, start, sims, seed = args
    rng = K.rng_for(seed)
    hit = hit_raw = 0
    for _ in range(sims):
        x, _s = K.gen(n, rng)
        K.place(x, rows, K.SHOCKS[name], ('at', start), rng, n)
        st = K.stats(x, rows)
        hit += st['gate_z'] >= 1.92
        hit_raw += st['gate'] >= 1.91
    return name, start, hit / sims, hit_raw / sims


if __name__ == '__main__':
    jobs = []
    for name in ('2020', '2008'):
        L = len(K.SHOCKS[name])
        span = HI - L - LO
        for k in range(120):
            jobs.append((name, LO + k * span // 119, 1000, 1000 * k + (0 if name == '2020' else 7)))
    with mp.Pool(mp.cpu_count()) as pool:
        res = pool.map(run, jobs)
    out = {}
    for name in ('2020', '2008'):
        r = [x for x in res if x[0] == name]
        avg = sum(x[2] for x in r) / len(r)
        avg_raw = sum(x[3] for x in r) / len(r)
        top = sorted(r, key=lambda x: -x[2])[:2]
        with mp.Pool(2) as pool:
            re = pool.map(run, [(name, t[1], 5000, 99 + i) for i, t in enumerate(top)])
        out[name] = {'placements': len(r), 'avg_z_192': avg, 'avg_raw_191': avg_raw,
                     'share_gt_0.05_z': sum(1 for x in r if x[2] > 0.05) / len(r),
                     'top2_at_1000': [(dates[t[1]].isoformat(), t[2]) for t in top],
                     'top2_rerun_5000': [(dates[t[1]].isoformat(), t[2], t[3]) for t in re]}
        print(name, json.dumps(out[name]), flush=True)
    (K.HERE / 'p7_out_rev1.json').write_text(json.dumps(out, indent=1))
