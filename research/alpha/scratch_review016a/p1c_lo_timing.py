"""Probe 1c: a holdings-based timing statistic (Lo 2008, 'active component') as a fix for the constant-slope
size failure. A_t = sum_i (w_i,t-1 - wbar_i) r_i,t with wbar_i the coin's mean book weight over its member-days;
t = mean(A) / CR1(month) SE. Under any constant-drift world it has mean ~0 whatever the vol dynamics; it keeps
timing gains from drift that changes over time. Synthetic only."""
import json
import multiprocessing as mp

import common as C
from ct16 import rule as R

GJR = (0.05, 0.88, 0.10)
ALL = ('2023-09-01', '2027-12-31')
W = [
    {'name': 'N1 flat GJR t3', 'garch': GJR, 'df': 3},
    {'name': 'N2 bull +80 GJR t3', 'garch': GJR, 'df': 3, 'drift': [(*ALL, 0.8)]},
    {'name': 'B4 bull +120 GJR t3', 'garch': GJR, 'df': 3, 'drift': [(*ALL, 1.2)]},
    {'name': 'N6 vol timing pays (asym), GJR t3', 'garch': GJR, 'df': 3, 'voldrift': (1.0, -0.6)},
    {'name': 'T1 AR trend h60 SR~1', 'kappa': 0.116, 'half_life': 60},
    {'name': 'T2 AR trend h120 SR~1', 'kappa': 0.110, 'half_life': 120},
    {'name': 'T3 AR trend h20 SR~1', 'kappa': 0.153, 'half_life': 20},
]


def draw(args):
    spec, seed = args
    syms = C.make_world(spec, seed)
    a, b = R.day(R.START), R.day(R.END)
    ufn = R.universe_fn_for(syms)
    bk = R.run_book(syms, a, b, ufn, keep_paths=True)
    f1, f3 = R.factor_series(syms, a, b, ufn)
    days, y, paths = bk['days'], bk['ret'], bk['paths']
    # weights held into day t: notional after trading at t-1 over book value at t-1
    V = [1.0]
    for r in y:
        V.append(V[-1] * (1 + r))
    w_prev = {}
    wsum, wcnt = {}, {}
    contrib = []
    for i, d in enumerate(days):
        items = []
        for s, wv in w_prev.items():
            row = syms[s].rows.get(d)
            prev = syms[s].last_traded_close(d)
            if row and prev and row[2] > 0:
                items.append((s, wv, row[0] / prev - 1))
        contrib.append(items)
        w_prev = {s: eq * H / V[i + 1] for s, (eq, H) in paths[i].items()}
        for s, wv in w_prev.items():
            wsum[s] = wsum.get(s, 0.0) + wv
            wcnt[s] = wcnt.get(s, 0) + 1
    wbar = {s: wsum[s] / wcnt[s] for s in wsum}
    A = [sum((wv - wbar.get(s, 0.0)) * r for s, wv, r in items) for items in contrib]
    gross_ret = [sum(wv * r for s, wv, r in items) for items in contrib]
    cl = [R.cluster_of(d) for d in days]
    # mean of A with CR1 by month (intercept-only regression)
    n = len(A)
    m = sum(A) / n
    g = {}
    for x, c in zip(A, cl):
        g[c] = g.get(c, 0.0) + (x - m)
    G = len(g)
    var = sum(v * v for v in g.values()) / n / n * G / (G - 1)
    tA = m / var ** 0.5
    _, t, _, _ = C.ols_alpha(y, [f1, bk['ew_alt'], f3], cl)
    err = max(abs(a - b) for a, b in zip(gross_ret, y))
    return {'tA': tA, 't': t, 'A_annual': m * 365, 'recon_err': err}


def main():
    out = []
    with mp.Pool(10) as pool:
        for i, w in enumerate(W):
            recs = pool.map(draw, [(w, 9_000_000 + 10_000 * i + k) for k in range(400)], chunksize=8)
            row = {'world': w['name'], 'n': len(recs)}
            for k in ('t', 'tA'):
                v = [r[k] for r in recs]
                row[k] = {'mean': C.mean(v), 'q95': C.q(v, .95), 'p_ge_1.731': sum(x >= 1.731 for x in v) / len(v),
                          'p_ge_1.699': sum(x >= 1.699 for x in v) / len(v)}
            row['A_annual_mean'] = C.mean([r['A_annual'] for r in recs])
            out.append(row)
            print(json.dumps(row), flush=True)
    json.dump(out, open('p1c_out.json', 'w'), indent=1)


if __name__ == '__main__':
    main()
