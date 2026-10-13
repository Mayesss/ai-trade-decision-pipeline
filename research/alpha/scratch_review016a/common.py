"""Reviewer A of 016: an independent synthetic generator and estimator. Reads no real data at all.

The book itself is the registered object (`ct16.rule.compute`); the world generator, the OLS / CR1 estimator, the
calendar splits and every derived statistic here are this reviewer's own code. Nothing touches real prices.
"""
import datetime as dt
import math
import os
import random
import sys

sys.dont_write_bytecode = True
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))          # research/alpha, for ct16
from ct16 import rule as R                        # noqa: E402  (the registered book)
from ct16 import stats as S                       # noqa: E402  (used only for the cell variants and a cross-check)

SIM_START = dt.date(2023, 9, 1)
NC = 24
SBAR = 0.55 / math.sqrt(365)
A_END = R.day(R.SPAN_A[1])
C_START = R.day(R.SPAN_C[0])


def D(s):
    return R.day(dt.date.fromisoformat(s))


def tunit(rng, df):
    if df is None:
        return rng.gauss(0.0, 1.0)
    z = rng.gauss(0.0, 1.0)
    chi = rng.gammavariate(df / 2.0, 2.0)
    return z / math.sqrt(chi / df) / math.sqrt(df / (df - 2.0))


def windows(spec_list):
    return [(D(a), D(b), v / 365.0) for a, b, v in (spec_list or [])]


def wval(ws, o, default=0.0):
    for a, b, v in ws:
        if a <= o <= b:
            return v
    return default


def make_world(spec, seed):
    rng = random.Random(seed)
    df = spec.get('df', 4)
    ga, gb, gg = spec.get('garch', (0.08, 0.90, 0.0))
    sbar = SBAR * spec.get('vol_mult', 1.0)
    omega = sbar * sbar * max(1e-6, 1.0 - ga - gb - gg / 2.0)
    d0, d1 = R.day(SIM_START), R.day(R.END)
    mdrift = windows(spec.get('drift'))
    btc_x = windows(spec.get('btc_extra'))
    alt_x = windows(spec.get('alt_extra'))
    voldrift = spec.get('voldrift')                     # (low, high) annual, by conditional vol
    jumps = {D(k): v for k, v in spec.get('jumps', {}).items()}
    squeeze = spec.get('squeeze')                       # (start, end, prob per day, size)
    sq = (D(squeeze[0]), D(squeeze[1]), squeeze[2], squeeze[3]) if squeeze else None
    beta_trend = spec.get('beta_trend', 0.0)            # alt beta x (1 + g sign(trailing 60d market))
    coin_sd = spec.get('coin_drift_sd', 0.0) / 365.0
    # trend: AR(1) common + idio, kappa by window
    half = spec.get('half_life', 60)
    kap_w = windows([(a, b, k * 365.0) for a, b, k in spec.get('kappa_windows', [])])
    kap0 = spec.get('kappa', 0.0)
    rho = spec.get('rho', 0.7)
    phi = 0.5 ** (1.0 / half)
    inn = math.sqrt(1 - phi * phi)
    # Markov regime drift (trend by definition)
    reg = spec.get('regime')                            # (annual drift magnitude, mean duration days)
    vol_link = spec.get('vol_link', 0.0)                # volume responds to trailing 30d return
    names = [R.BTC] + [f'C{i:02d}USDT' for i in range(1, NC)]
    betas = [1.0] + [rng.uniform(1.0, 1.5) for _ in range(1, NC)]
    idio = [0.30 / math.sqrt(365)] + [rng.uniform(0.35, 0.80) / math.sqrt(365) for _ in range(1, NC)]
    cdrift = [0.0] + [rng.gauss(0.0, coin_sd) for _ in range(1, NC)]
    lvl = [math.log(5e9)] + [rng.uniform(math.log(8e7), math.log(2e9)) for _ in range(1, NC)]
    tot = [math.sqrt((betas[i] * SBAR) ** 2 + idio[i] ** 2) for i in range(NC)]
    u = rng.gauss(0, 1)
    v = [rng.gauss(0, 1) for _ in range(NC)]
    state = 1 if rng.random() < 0.5 else -1
    sig2 = sbar * sbar
    prev = 0.0
    boost = 0
    mhist = []
    lp = [[0.0] * 31 for _ in range(NC)]                # trailing log prices for vol_link
    price = [rng.uniform(0.5, 50.0) * (2000 if i == 0 else 1) for i in range(NC)]
    occ = [0.0] * NC
    rows = {n: {} for n in names}
    lpx = [math.log(p) for p in price]
    for o in range(d0, d1 + 1):
        sig2 = omega + (ga + (gg if prev < 0 else 0.0)) * prev * prev + gb * sig2
        sig = math.sqrt(sig2) * (2.0 if boost > 0 else 1.0)
        boost = max(0, boost - 1)
        z = tunit(rng, df)
        dm = wval(mdrift, o)
        if voldrift:
            dm += (voldrift[0] if sig < sbar else voldrift[1]) / 365.0
        if reg:
            if rng.random() < 1.0 / reg[1]:
                state = -state
            dm += state * reg[0] / 365.0
        m = dm + sig * z
        prev = sig * z
        if o in jumps:
            m += math.log(1 + jumps[o])
            boost = 20
        if sq and sq[0] <= o <= sq[1] and rng.random() < sq[2]:
            m += math.log(1 + sq[3])
        mhist.append(m)
        tr60 = sum(mhist[-60:])
        kap = wval(kap_w, o, kap0 * 365.0 / 365.0) if kap_w else kap0
        if kap:
            u = phi * u + inn * rng.gauss(0, 1)
        for i, n in enumerate(names):
            b = betas[i]
            if i and beta_trend:
                b *= 1.0 + beta_trend * (1 if tr60 > 0 else -1)
            mu = cdrift[i] + (wval(btc_x, o) if i == 0 else wval(alt_x, o))
            if kap:
                v[i] = phi * v[i] + inn * rng.gauss(0, 1)
                mu += kap * tot[i] * (rho * u + math.sqrt(1 - rho * rho) * v[i])
            r = mu + b * m + idio[i] * (sig / sbar) * tunit(rng, df)
            r = max(-0.95, r)
            price[i] *= 1.0 + r
            lpx[i] = math.log(price[i])
            lp[i].append(lpx[i])
            lp[i].pop(0)
            occ[i] = 0.98 * occ[i] + rng.gauss(0, 0.08)
            qv = math.exp(lvl[i] + occ[i] + vol_link * (lp[i][-1] - lp[i][0]) + rng.gauss(0, 0.3))
            rows[n][o] = (price[i], qv, 1000)
    return {n: R.Sym(n, rows[n]) for n in names}


# ------------------------------------------------------------------ own estimator
def solve(A, b):
    n = len(A)
    M = [list(A[i]) + [b[i]] for i in range(n)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(M[r][c]))
        M[c], M[p] = M[p], M[c]
        pv = M[c][c]
        M[c] = [x / pv for x in M[c]]
        for r in range(n):
            if r != c and M[r][c]:
                f = M[r][c]
                M[r] = [x - f * y for x, y in zip(M[r], M[c])]
    return [M[i][n] for i in range(n)]


def ols_alpha(y, F, clusters):
    """alpha, its CR1 t (clusters), annualised alpha. F: list of factor columns (constant slopes)."""
    n = len(y)
    X = [[1.0] + [f[i] for f in F] for i in range(n)]
    p = len(X[0])
    XtX = [[sum(X[i][a] * X[i][b] for i in range(n)) for b in range(p)] for a in range(p)]
    Xty = [sum(X[i][a] * y[i] for i in range(n)) for a in range(p)]
    inv = [solve(XtX, [1.0 if i == j else 0.0 for i in range(p)]) for j in range(p)]
    inv = [[inv[j][i] for j in range(p)] for i in range(p)]
    coef = [sum(inv[a][b] * Xty[b] for b in range(p)) for a in range(p)]
    e = [y[i] - sum(coef[a] * X[i][a] for a in range(p)) for i in range(n)]
    sc = {}
    for i in range(n):
        u = sc.setdefault(clusters[i], [0.0] * p)
        for a in range(p):
            u[a] += X[i][a] * e[i]
    G = len(sc)
    # only the intercept's variance: row 0 of inv
    r0 = inv[0]
    var = sum((sum(r0[a] * u[a] for a in range(p))) ** 2 for u in sc.values())
    var *= (G / (G - 1)) * ((n - 1) / (n - p))
    se = math.sqrt(var) if var > 0 else float('nan')
    return coef[0], coef[0] / se, coef[0] * 365, coef[1:]


def monthly_T(days, y):
    acc, order = {}, []
    for d, r in zip(days, y):
        m = R.cluster_of(d)
        if m not in acc:
            acc[m] = 1.0
            order.append(m)
        acc[m] *= 1 + r
    mo = [acc[m] - 1 for m in order]
    G = len(mo)
    mu = sum(mo) / G
    sd = math.sqrt(sum((x - mu) ** 2 for x in mo) / (G - 1))
    return math.sqrt(G) * mu / sd, mu / sd * math.sqrt(12), sd * math.sqrt(12), mu * 12


def tails(y):
    worst1 = min(y)
    eq = [1.0]
    for r in y:
        eq.append(eq[-1] * (1 + r))
    w7 = min(eq[i + 7] / eq[i] - 1 for i in range(len(eq) - 7))
    w30 = min(eq[i + 30] / eq[i] - 1 for i in range(len(eq) - 30))
    pk, mdd = eq[0], 0.0
    for x in eq:
        pk = max(pk, x)
        mdd = min(mdd, x / pk - 1)
    return worst1, w7, w30, mdd


def run_draw(spec, seed, cells=True, extra_days=()):
    syms = make_world(spec, seed)
    out = R.compute(syms)
    bk = out['book']
    days, y = bk['days'], bk['ret']
    F = [out['F1'], out['F2'], out['F3']]
    cl = [R.cluster_of(d) for d in days]
    a, t, aa, sl = ols_alpha(y, F, cl)
    ia = [i for i, d in enumerate(days) if d <= A_END]
    ic = [i for i, d in enumerate(days) if d >= C_START]

    def sub(idx):
        return [y[i] for i in idx], [[f[i] for i in idx] for f in F], [cl[i] for i in idx]
    ya, Fa, ca = sub(ia)
    yc, Fc, cc = sub(ic)
    _, t_a, aa_a, _ = ols_alpha(ya, Fa, ca)
    ac, t_c, aa_c, _ = ols_alpha(yc, Fc, cc)
    T, sr, vol, mean = monthly_T(days, y)
    _, t_btc, aa_btc, sl_btc = ols_alpha(y, [F[0]], cl)
    rec = {'t': t, 'alpha': aa, 'beta_btc': sl[0], 't_a': t_a, 't_c': t_c, 'alpha_c_pos': ac > 0, 'alpha_a': aa_a,
           'alpha_c': aa_c, 'T': T, 'sr': sr, 'vol': vol, 'mean': mean, 't_btc': t_btc, 'alpha_btc': aa_btc,
           'b_btconly': sl_btc[0], 'gross_mean': sum(bk['gross']) / len(bk['gross']),
           'gross_max': max(bk['gross']), 'turnover': bk['turnover_per_year']}
    rec['T_a'] = monthly_T([days[i] for i in ia], ya)[0]
    rec['T_c'] = monthly_T([days[i] for i in ic], yc)[0]
    w1, w7, w30, mdd = tails(y)
    rec.update({'worst1': w1, 'worst7': w7, 'worst30': w30, 'mdd': mdd})
    if extra_days:
        idx = {d: i for i, d in enumerate(days)}
        rec['gross_on'] = [bk['gross'][idx[R.day(dt.date.fromisoformat(x))]] for x in extra_days]
        rec['ret_on'] = [y[idx[R.day(dt.date.fromisoformat(x))]] for x in extra_days]
    if cells:
        rec['t_year'] = S.alpha_fit(days, y, F, per_cell=True, cells=R.YEAR_CELLS)['t']
        rec['t_half'] = S.alpha_fit(days, y, F, per_cell=True)['t']
    return rec


def _w(args):
    spec, seed, cells, extra = args
    return run_draw(spec, seed, cells, extra)


def run_world(spec, draws, seed0, pool, cells=True, extra=()):
    jobs = [(spec, seed0 + i, cells, tuple(extra)) for i in range(draws)]
    return pool.map(_w, jobs, chunksize=max(1, draws // 50))


def q(v, p):
    v = sorted(x for x in v if x == x)
    i = p * (len(v) - 1)
    lo = int(i)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (i - lo)


def mean(v):
    v = [x for x in v if x == x]
    return sum(v) / len(v)
