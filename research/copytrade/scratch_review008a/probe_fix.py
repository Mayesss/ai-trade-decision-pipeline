"""Review 008-A probe 2 (synthetic only): (i) size of C1 with many more simulations; (ii) does a
window-specific (or month-specific) BTC beta with ONE common intercept restore size under the
cohort-beta null, and what power does it cost?

    cd research/copytrade && .venv/bin/python -I scratch_review008a/probe_fix.py [sims]
"""
import datetime as dt
import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ct import crowd008 as c8  # noqa: E402

SIMS = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
KEYS = c8.planned_keys(dt.date(2026, 9, 28))
N = len(KEYS)
WK = [c8.week_of(k) for k in KEYS]
MONTH = [c8.month_of(k) for k in KEYS]
WIN = [0 if k < c8.ms(dt.date(2026, 8, 3)) else 1 for k in KEYS]
SIG = c8.SIGMA_DAY
FULL = c8.DISC_GROSS_INV
CRIT = c8.t_quantile(12, 0.95)


def inv(a):
    n = len(a)
    m = [row[:] + [1.0 if i == j else 0.0 for j in range(n)] for i, row in enumerate(a)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(m[r][c]))
        m[c], m[p] = m[p], m[c]
        pv = m[c][c]
        m[c] = [v / pv for v in m[c]]
        for r in range(n):
            if r != c:
                f = m[r][c]
                m[r] = [vr - f * vc for vr, vc in zip(m[r], m[c])]
    return [row[n:] for row in m]


def ols_cr1(y, X, cl):
    """y = X b; CR1 cluster-robust t of b[0] (the intercept column first)."""
    n, k = len(y), len(X[0])
    xtx = [[sum(X[i][a] * X[i][b] for i in range(n)) for b in range(k)] for a in range(k)]
    xi = inv(xtx)
    xty = [sum(X[i][a] * y[i] for i in range(n)) for a in range(k)]
    b = [sum(xi[a][c] * xty[c] for c in range(k)) for a in range(k)]
    g = {}
    for i in range(n):
        e = y[i] - sum(X[i][a] * b[a] for a in range(k))
        u = g.setdefault(cl[i], [0.0] * k)
        for a in range(k):
            u[a] += X[i][a] * e
    G = len(g)
    r = xi[0]
    v = sum(sum(r[a] * u[a] for a in range(k)) ** 2 for u in g.values()) * G / (G - 1) * (n - 1) / (n - k)
    return b, b[0] / math.sqrt(v)


def design(m, kind):
    if kind == 'one beta':
        return [[1.0, x] for x in m]
    if kind == 'beta by window':
        return [[1.0, x if w == 0 else 0.0, x if w == 1 else 0.0] for x, w in zip(m, WIN)]
    if kind == 'beta by month':
        mos = sorted(set(MONTH))
        return [[1.0] + [x if mo == M else 0.0 for M in mos] for x, mo in zip(m, MONTH)]


def drift_path(july):
    per = {'2026-07': july, '2026-08': math.log(1.24) / 31, '2026-09': math.log(1.08) / 30}
    return [per[mo] for mo in MONTH]


def rate(gen, kinds, sims=SIMS, seed=3):
    rng = random.Random(seed)
    hits = {k: 0 for k in kinds}
    for _ in range(sims):
        y, m = gen(rng)
        for kd in kinds:
            b, t = ols_cr1(y, design(m, kd), WK)
            hits[kd] += b[0] > 0 and t >= CRIT
    return {k: v / sims for k, v in hits.items()}


KINDS = ('one beta', 'beta by window', 'beta by month')

if __name__ == '__main__':
    big = rate(lambda r: ([r.gauss(0, SIG) for _ in range(N)], [r.gauss(0, 0.025) for _ in range(N)]),
               ('one beta',), sims=4 * SIMS, seed=11)
    print(f'C1 size, iid Gaussian null, {4 * SIMS} sims: {big}  (binomial se {math.sqrt(0.05 * 0.95 / (4 * SIMS)):.4f})')
    for name, mu, bA, bB, july in (('null, beta A 0 / B 0.3, July 0', 0, 0.0, 0.3, 0.0),
                                   ('null, beta A 0 / B 0.5, July -0.3 %/d', 0, 0.0, 0.5, -0.003),
                                   ('null, beta Jul 0 / Aug 0.4 / Sep 0', 0, None, None, 0.0),
                                   ('null, constant beta 0.3, July 0', 0, 0.3, 0.3, 0.0),
                                   ('full effect, constant beta 0.3, July 0', FULL, 0.3, 0.3, 0.0),
                                   ('half effect, constant beta 0.3, July 0', FULL / 2, 0.3, 0.3, 0.0)):
        dp = drift_path(july)

        def gen(r, dp=dp, mu=mu, bA=bA, bB=bB):
            m = [r.gauss(d, 0.025) for d in dp]
            if bA is None:
                beta = [0.4 if mo == '2026-08' else 0.0 for mo in MONTH]
            else:
                beta = [bA if w == 0 else bB for w in WIN]
            return [mu + b * x + r.gauss(0, SIG) for b, x in zip(beta, m)], m
        print(f'{name:<45}', {k: round(v, 3) for k, v in rate(gen, KINDS).items()})
