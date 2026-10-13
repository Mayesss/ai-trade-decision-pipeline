"""Review 008-R1 probe (synthetic only): an independent OLS + CR1 implementation of the Revision-1 confirm
rule (C1-C3 on BTC x cell + EW alt + 24 h reversal), reproduction of one false-confirm cell (N10) and one power
cell, then nulls the revision did not try, and candidate mitigations for month-scale persistence.

    cd research/copytrade && .venv/bin/python -I scratch_review008r1/probe_stats.py [sims] [which...]

No data file is read; every number is from random draws.
"""
import datetime as dt
import math
import random
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ct import crowd008 as c8  # noqa: E402

SIMS = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
WHICH = set(sys.argv[2:])
END = dt.date(2026, 9, 28)
DAYS = c8.planned_days(END)
KEYS = [k for k, _ in DAYS]
WIN = [w for _, w in DAYS]
N = len(KEYS)
WK = [c8.week_of(k) for k in KEYS]
MON = [c8.month_of(k) for k in KEYS]
CELL = [f'{w}:{m}' for w, m in zip(WIN, MON)]          # no merge needed at 2026-09-28 (30/29/24)
CELLS = sorted(set(CELL))
G = len(set(WK))
CRIT = c8.t_quantile(G - 1, 0.95)
SIG = c8.SIGMA_DAY
FULL = c8.DISC_GROSS_INV
AUG, SEP = 1.24 ** (1 / 31) - 1, 1.08 ** (1 / 30) - 1


def drift_m(july):
    return {'2026-07': july, '2026-08': AUG, '2026-09': SEP}


# ------------------------------------------------------------------ independent OLS + CR1
def solve_inv(A):
    n = len(A)
    M = [row[:] + [float(i == j) for j in range(n)] for i, row in enumerate(A)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(M[r][c]))
        M[c], M[p] = M[p], M[c]
        pv = M[c][c]
        M[c] = [v / pv for v in M[c]]
        for r in range(n):
            if r != c:
                f = M[r][c]
                if f:
                    M[r] = [a - f * b for a, b in zip(M[r], M[c])]
    return [row[n:] for row in M]


def fit(y, X, cl, nw_lag=0):
    """X rows include the constant. Returns alpha, t (CR1), resid, and (optionally) a week-NW t:
    Bartlett-weighted cross-products of the intercept's weekly influence sums up to nw_lag weeks."""
    n, k = len(y), len(X[0])
    XtX = [[sum(X[i][a] * X[i][b] for i in range(n)) for b in range(k)] for a in range(k)]
    inv = solve_inv(XtX)
    Xty = [sum(X[i][a] * y[i] for i in range(n)) for a in range(k)]
    beta = [sum(inv[a][b] * Xty[b] for b in range(k)) for a in range(k)]
    e = [y[i] - sum(beta[a] * X[i][a] for a in range(k)) for i in range(n)]
    # intercept influence per obs: (inv row 0) . x_i * e_i
    psi = [sum(inv[0][a] * X[i][a] for a in range(k)) * e[i] for i in range(n)]
    order = list(dict.fromkeys(cl))
    sums = {g: 0.0 for g in order}
    for p, g in zip(psi, cl):
        sums[g] += p
    s = [sums[g] for g in order]
    Gn = len(s)
    adj = Gn / (Gn - 1) * (n - 1) / (n - k)
    v = sum(x * x for x in s)
    t = beta[0] / math.sqrt(adj * v)
    out = {'alpha': beta[0], 't': t, 'resid': e, 'coef': beta}
    if nw_lag:
        vv = v
        for l in range(1, nw_lag + 1):
            w = 1 - l / (nw_lag + 1)
            vv += 2 * w * sum(s[i] * s[i - l] for i in range(l, Gn))
        out['t_nw'] = beta[0] / math.sqrt(adj * vv) if vv > 0 else float('nan')
    return out


def design(fac, cells=CELL):
    rows = []
    for i in range(N):
        r = [1.0] + [fac['btc'][i] if cells[i] == c else 0.0 for c in sorted(set(cells))]
        r += [fac['alt'][i], fac['rev'][i]]
        rows.append(r)
    return rows


def rule(y, fac, nw_lag=0, extra=()):
    f = fit(y, design(fac), WK, nw_lag)
    a = [f['alpha'] + e for e in f['resid']]
    h = N // 2
    c1 = f['alpha'] > 0 and f['t'] >= CRIT
    c2 = statistics.median(a) > 0
    c3 = statistics.fmean(a[:h]) > 0 and statistics.fmean(a[h:]) > 0
    res = {'reg': c1 and c2 and c3}
    if 'win' in extra:
        res['reg+win'] = res['reg'] and all(statistics.fmean([a[i] for i in range(N) if WIN[i] == w]) > 0 for w in (0, 1))
    if 'month' in extra:
        res['reg+month'] = res['reg'] and all(statistics.fmean([a[i] for i in range(N) if MON[i] == m]) > 0
                                              for m in ('2026-07', '2026-08', '2026-09'))
    if nw_lag:
        res[f'nw{nw_lag}'] = f['alpha'] > 0 and f['t_nw'] >= CRIT and c2 and c3
    return res, f


# ------------------------------------------------------------------ generator (independent of c8.simulate)
def ar1(rng, rho, n, gen):
    out, p, sc = [], 0.0, math.sqrt(1 - rho * rho)
    for _ in range(n):
        p = rho * p + sc * gen()
        out.append(p)
    return out


def draw(rng, mu=0.0, beta=lambda i: 0.3, btc_drift=drift_m(0.0), alt_load=lambda i: 0.0, u_drift=lambda i: 0.0,
         rev_load=lambda i: 0.0, rev_drift=lambda i: 0.0, slow=0.0, slow_rho=0.9, btc_t=None, hetero=False):
    def btc_shock():
        if btc_t:
            z = rng.gauss(0, 1) / math.sqrt(sum(rng.gauss(0, 1) ** 2 for _ in range(btc_t)) / btc_t)
            return z * math.sqrt((btc_t - 2) / btc_t)
        return rng.gauss(0, 1)
    btc = [btc_drift[MON[i]] + 0.025 * btc_shock() for i in range(N)]
    u = [u_drift(i) + rng.gauss(0, 0.015) for i in range(N)]
    rev = [rev_drift(i) + rng.gauss(0, 0.008) for i in range(N)]
    fast = [rng.gauss(0, 1) for _ in range(N)]
    sl = ar1(rng, slow_rho, N, lambda: rng.gauss(0, 1)) if slow else [0.0] * N
    y = []
    for i in range(N):
        sc = 1.0
        if hetero:
            z = (btc[i] - btc_drift[MON[i]]) / 0.025
            sc = math.sqrt((0.5 + 0.5 * z * z) / 1.0)
        e = SIG * (math.sqrt(1 - slow) * fast[i] * sc + math.sqrt(slow) * sl[i])
        y.append(mu + beta(i) * btc[i] + alt_load(i) * u[i] + rev_load(i) * rev[i] + e)
    fac = {'btc': btc, 'alt': [1.1 * b + x for b, x in zip(btc, u)], 'rev': rev}
    return y, fac


def rate(name, sims=SIMS, seed=1, nw_lag=0, extra=(), **kw):
    rng = random.Random(seed)
    hits = {}
    for _ in range(sims):
        y, fac = draw(rng, **kw)
        r, _ = rule(y, fac, nw_lag, extra)
        for k, v in r.items():
            hits[k] = hits.get(k, 0) + v
    out = {k: round(v / sims, 3) for k, v in hits.items()}
    print(f'{name:<78} ' + '  '.join(f'{k} {v:.3f}' for k, v in out.items()), flush=True)
    return out


def agreement():
    rng = random.Random(7)
    worst = 0.0
    cells = c8.beta_cells(KEYS, dict(DAYS))
    for _ in range(40):
        y, fac = draw(rng, mu=0.0005)
        cf = {k: {'btc': fac['btc'][i], 'alt': fac['alt'][i], 'rev': fac['rev'][i]} for i, k in enumerate(KEYS)}
        ok, num = c8.confirm(y, KEYS, cf, cells)
        r, f = rule(y, fac)
        worst = max(worst, abs(num['t_alpha'] - f['t']), abs(num['alpha'] - f['alpha']))
        assert ok == r['reg'], (ok, r)
    print(f'agreement with crowd008.confirm over 40 draws: max |diff| {worst:.2e}; decisions identical')


W0 = lambda i: WIN[i] == 0                       # noqa: E731
JUL = lambda i: MON[i] == '2026-07'              # noqa: E731

if __name__ == '__main__':
    print(f'2026-09-28: n {N}, G {G}, cells {CELLS}, crit {CRIT:.4f}, sims {SIMS}')
    if not WHICH or 'agree' in WHICH:
        agreement()
    if not WHICH or 'repro' in WHICH:
        rate('base, effect 0 (iid)', seed=11)
        rate('base, effect full (7.46 bp/day), iid  [draft: 0.295]', seed=12, mu=FULL)
        rate('base, effect half, iid                 [draft: 0.139]', seed=13, mu=FULL / 2)
        rate('N10 combined N1+N4+N6                   [draft: 0.047]', seed=14, beta=lambda i: 0.0 if W0(i) else 0.5,
             btc_drift=drift_m(-0.003), alt_load=lambda i: 0.3, u_drift=lambda i: 0.002, rev_load=lambda i: 0.5,
             rev_drift=lambda i: 0.001)
        rate('N8 persistence 5 %, rho 0.9              [draft: 0.081]', seed=15, slow=0.05, slow_rho=0.9)
    if not WHICH or 'new' in WHICH:
        # A-B1's logic applied to the factors that kept ONE loading
        rate('N11 alt loading by cohort A 0 / B 0.5; U drift Jul 0, Aug-Sep +0.2 %/day', seed=21,
             alt_load=lambda i: 0.0 if W0(i) else 0.5, u_drift=lambda i: 0.0 if JUL(i) else 0.002)
        rate('N11b alt loading A 0.5 / B 0; U drift Jul -0.3 %/day, Aug-Sep 0', seed=22,
             alt_load=lambda i: 0.5 if W0(i) else 0.0, u_drift=lambda i: -0.003 if JUL(i) else 0.0)
        rate('N11c alt loading A -0.2 / B 0.3; U drift Jul -0.2, Aug-Sep +0.2 %/day', seed=23,
             alt_load=lambda i: -0.2 if W0(i) else 0.3, u_drift=lambda i: -0.002 if JUL(i) else 0.002)
        rate('N12 rev loading by cohort A 0 / B 1.0; REV drift Jul 0, Aug-Sep +0.10 %/day', seed=24,
             rev_load=lambda i: 0.0 if W0(i) else 1.0, rev_drift=lambda i: 0.0 if JUL(i) else 0.001)
        rate('N12b rev loading A 1.0 / B 0; REV drift Jul -0.10, Aug-Sep +0.10 %/day', seed=25,
             rev_load=lambda i: 1.0 if W0(i) else 0.0, rev_drift=lambda i: -0.001 if JUL(i) else 0.001)
        rate('N13 heavy tails: BTC t3, residual sd ~ |BTC| (constant beta 0.3)', seed=26, btc_t=3, hetero=True)
    if not WHICH or 'mitig' in WHICH:
        ex = ('win', 'month')
        rate('M base effect 0', seed=31, nw_lag=2, extra=ex)
        rate('M N8 persistence 5 %, rho 0.9', seed=32, nw_lag=2, extra=ex, slow=0.05, slow_rho=0.9)
        rate('M N9 persistence 3 %, rho 0.97', seed=33, nw_lag=2, extra=ex, slow=0.03, slow_rho=0.97)
        rate('M N8b persistence 10 %, rho 0.9', seed=34, nw_lag=2, extra=ex, slow=0.10, slow_rho=0.9)
        rate('M power full, iid', seed=35, nw_lag=2, extra=ex, mu=FULL)
        rate('M power half, iid', seed=36, nw_lag=2, extra=ex, mu=FULL / 2)
        rate('M power full under N8', seed=37, nw_lag=2, extra=ex, mu=FULL, slow=0.05, slow_rho=0.9)


# ------------------------------------------------------------------ candidate fix: per-window / per-cell alt and rev loadings
def design_v(fac, variant):
    rows = []
    groups = {'cell': CELL, 'win': [str(w) for w in WIN]}
    for i in range(N):
        r = [1.0] + [fac['btc'][i] if CELL[i] == c else 0.0 for c in CELLS]
        for name in ('alt', 'rev'):
            g = groups[variant[name]] if variant.get(name) else None
            if g is None:
                r.append(fac[name][i])
            else:
                r += [fac[name][i] if g[i] == c else 0.0 for c in sorted(set(g))]
        rows.append(r)
    return rows


def rate_v(name, variants, sims=SIMS, seed=1, **kw):
    rng = random.Random(seed)
    hits = {v: 0 for v in variants}
    h = N // 2
    for _ in range(sims):
        y, fac = draw(rng, **kw)
        for v, spec in variants.items():
            f = fit(y, design_v(fac, spec), WK)
            a = [f['alpha'] + e for e in f['resid']]
            hits[v] += (f['alpha'] > 0 and f['t'] >= CRIT and statistics.median(a) > 0
                        and statistics.fmean(a[:h]) > 0 and statistics.fmean(a[h:]) > 0)
    print(f'{name:<70} ' + '  '.join(f'{k} {v / sims:.3f}' for k, v in hits.items()), flush=True)


VARIANTS = {'registered(k6)': {}, 'alt+rev by window(k8)': {'alt': 'win', 'rev': 'win'},
            'alt+rev by cell(k10)': {'alt': 'cell', 'rev': 'cell'}}

if 'fix' in WHICH:
    rate_v('F base effect 0', VARIANTS, seed=41)
    rate_v('F N10 combined', VARIANTS, seed=42, beta=lambda i: 0.0 if W0(i) else 0.5, btc_drift=drift_m(-0.003),
           alt_load=lambda i: 0.3, u_drift=lambda i: 0.002, rev_load=lambda i: 0.5, rev_drift=lambda i: 0.001)
    rate_v('F N11 alt A 0 / B 0.5, U drift Aug-Sep +0.2', VARIANTS, seed=43,
           alt_load=lambda i: 0.0 if W0(i) else 0.5, u_drift=lambda i: 0.0 if JUL(i) else 0.002)
    rate_v('F N11c alt A -0.2 / B 0.3, U drift Jul -0.2 / Aug-Sep +0.2', VARIANTS, seed=44,
           alt_load=lambda i: -0.2 if W0(i) else 0.3, u_drift=lambda i: -0.002 if JUL(i) else 0.002)
    rate_v('F N12 rev A 0 / B 1.0, REV drift Aug-Sep +0.10', VARIANTS, seed=45,
           rev_load=lambda i: 0.0 if W0(i) else 1.0, rev_drift=lambda i: 0.0 if JUL(i) else 0.001)
    rate_v('F N8 persistence 5 %, rho 0.9', VARIANTS, seed=46, slow=0.05, slow_rho=0.9)
    rate_v('F N13 heavy tails + hetero', VARIANTS, seed=47, btc_t=3, hetero=True)
    rate_v('F power full iid', VARIANTS, seed=48, mu=FULL)
    rate_v('F power half iid', VARIANTS, seed=49, mu=FULL / 2)
