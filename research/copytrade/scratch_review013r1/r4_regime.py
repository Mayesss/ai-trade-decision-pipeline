"""Review r1 of 013: beta that varies with the market's state (§9: "not modelled"), the 008-r1 B1 analogue.
Confirmation-calendar worlds only (the discovery stage is irrelevant to the size of the confirm given a carry; we
report the confirmation-alone rate of T1 at the registered critical value).

World: listings whose first trade falls in a stylised crash period (2025-10-06 .. 2026-02-28) carry beta b_in, others
b_out; the alt market drifts down d %/week in the crash period only. The edge is zero (mu 0).
Remedy probed: the same alpha test with the beta allowed to differ by calendar quarter of the first trade
(intercept common; CR1 by listing week; df G - 2 as registered; CAL 1.90 kept).
Usage: python -I r4_regime.py SIMS
"""
import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r1_own_sim as S  # noqa: E402


def solve(A, b):
    n = len(A)
    M = [row[:] + [b[i]] for i, row in enumerate(A)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(M[r][c]))
        M[c], M[p] = M[p], M[c]
        if abs(M[c][c]) < 1e-300:
            return None
        for r in range(n):
            if r != c:
                f = M[r][c] / M[c][c]
                M[r] = [x - f * y for x, y in zip(M[r], M[c])]
    return [M[i][n] / M[i][i] for i in range(n)]


def inv(A):
    n = len(A)
    cols = [solve(A, [1.0 if i == j else 0.0 for i in range(n)]) for j in range(n)]
    if any(c is None for c in cols):
        return None
    return [[cols[j][i] for j in range(n)] for i in range(n)]


def ols_k(y, X, cl):
    n, k = len(y), len(X[0])
    XtX = [[sum(X[i][a] * X[i][b] for i in range(n)) for b in range(k)] for a in range(k)]
    Xi = inv(XtX)
    if Xi is None:
        return None
    Xty = [sum(X[i][a] * y[i] for i in range(n)) for a in range(k)]
    bt = [sum(Xi[a][b] * Xty[b] for b in range(k)) for a in range(k)]
    g = {}
    for i in range(n):
        e = y[i] - sum(X[i][a] * bt[a] for a in range(k))
        u = g.setdefault(cl[i], [0.0] * k)
        for a in range(k):
            u[a] += X[i][a] * e
    G = len(g)
    meat = [[sum(u[a] * u[b] for u in g.values()) for b in range(k)] for a in range(k)]
    v00 = sum(Xi[0][a] * meat[a][b] * Xi[b][0] for a in range(k) for b in range(k))
    v00 *= G / (G - 1) * (n - 1) / (n - k)
    return {'a': bt[0], 't': bt[0] / math.sqrt(v00) if v00 > 0 else None, 'G': G}


def quarter(t):
    import datetime as dt
    d = dt.datetime.fromtimestamp(t / 1000, dt.UTC)
    return f'{d.year}Q{(d.month - 1) // 3 + 1}'


def run(sims, b_in, b_out, d, mu=0.0, k=1.5):
    C = S.sched('confirmation')
    r = random.Random(7)
    hit = hit_q = 0
    for _ in range(sims):
        ev = S.events(C, mu, S.bear_beta(b_in, b_out), k, S.bear_drift(d), r)
        s = S.stats(ev, S.SPAN['confirmation'])
        hit += S.conf_pass(s)
        qs = sorted({quarter(e['en'] - S.DAY) for e in ev})
        X = [[1.0] + [e['m'] if quarter(e['en'] - S.DAY) == q else 0.0 for q in qs] for e in ev]
        o = ols_k([e['net'] for e in ev], X, [e['w'] for e in ev])
        hit_q += bool(o and o['t'] is not None and o['a'] > 0 and o['t'] >= max(S.t_crit(o['G'] - 2, 0.95), 1.90)
                      and (s['wo'] or 0) > 0)
    return hit / sims, hit_q / sims


if __name__ == '__main__':
    sims = int(sys.argv[1])
    for (b_in, b_out, d, mu, k) in ((2.5, 1.0, 0.03, 0.0, 1.5), (2.0, 1.25, 0.02, 0.0, 1.5), (2.0, 1.25, 0.03, 0.0, 1.0),
                                    (1.5, 1.5, 0.03, 0.0, 1.5), (1.5, 1.5, 0.0, 0.04, 1.5), (1.5, 1.5, 0.0, 0.03, 1.5)):
        a, q = run(sims, b_in, b_out, d, mu, k)
        print(f'beta in/out {b_in}/{b_out}, crash drift {d:.2f}/wk, mu {mu}, k {k}: confirm (registered) {a:.3f}; '
              f'quarter-beta alpha {q:.3f}', flush=True)
