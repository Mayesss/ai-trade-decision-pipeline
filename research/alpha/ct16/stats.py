"""Registration 016 — the estimators, the two hypotheses, the decision and its recheck. Pure functions.

H1 (existence): the intercept alpha of the daily net book return on BTC, the EW alt basket and the vol-managed EW
basket (one slope per factor over the span, D14; calendar-cell slopes are reported, never trailing-trend cells), CR1
standard errors clustered by calendar month; one-sided. Passes iff t >= max(t_{G-1-k}(0.95), CAL_H1) AND the same
model fitted on the forward span alone has alpha > 0.
H2 (decay): the annualised Sharpe ratio of the monthly net book returns against the paper's 1.57; one-sided
(lower). Rejects iff T = sqrt(G) * mean / sd < min(the noncentral-t 5% quantile, CAL_H2).
"""
import math

from ct16 import rule as R

Q = 0.05
SR_PAPER = 1.57                    # Table 4, p. 19: top-20 long-only, net of 10 bp, 2015-01-01 -> 2025-03-19
CAL_H1 = 1.731                     # §5: the largest 95th percentile of the alpha t over the 11 calibration worlds
                                   # (1.7308, 'null fat t3 leverage cells'; the floor t_29(0.95) is 1.699)
CAL_H2 = 0.117                     # §6: the smallest 5th percentile of T over the three SR-1.57 worlds (0.1177, h120;
                                   # the noncentral-t value is 0.954); the smaller of the two is used
HLP_CALM = 0.04                    # the economic bar: HLP's calm return, %/yr (programme benchmark)
K_PROPOSED = 2.8                   # the post-pass size: 25% book vol over the paper's ~9% (scout §4.4)


# ------------------------------------------------------------------ distributions
def _betacf(a, b, x):
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c, d = 1.0, 1.0 - qab * x / qap
    d = 1.0 / (d if abs(d) > 1e-300 else 1e-300)
    h = d
    for m in range(1, 400):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1.0 + aa / c
        c = c if abs(c) > 1e-300 else 1e-300
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1.0 + aa / c
        c = c if abs(c) > 1e-300 else 1e-300
        de = d * c
        h *= de
        if abs(de - 1.0) < 3e-15:
            break
    return h


def ibeta(a, b, x):
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    bt = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1 - x))
    if x < (a + 1) / (a + b + 2):
        return bt * _betacf(a, b, x) / a
    return 1.0 - bt * _betacf(b, a, 1 - x) / b


def t_cdf(t, df):
    x = df / (df + t * t)
    p = 0.5 * ibeta(df / 2.0, 0.5, x)
    return 1.0 - p if t > 0 else p


def t_crit(df, q=Q):
    """The upper q critical value of Student t with df degrees of freedom (bisection)."""
    R.refuse_unless(df >= 1, f't_crit needs df >= 1, got {df}')
    lo, hi = 0.0, 60.0
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        if 1.0 - t_cdf(mid, df) > q:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def norm_cdf(z):
    return 0.5 * math.erfc(-z / math.sqrt(2.0))


def nct_cdf(x, df, delta, n=4000):
    """P(T <= x) for T = (Z + delta) / sqrt(V / df), V ~ chi2(df): E_V[Phi(x sqrt(V / df) - delta)], by Simpson's
    rule over V on [0, df + 40 sqrt(2 df)] against the chi2 density (log-space)."""
    hi = df + 40.0 * math.sqrt(2.0 * df)
    h = hi / n
    k = df / 2.0
    lnorm = -k * math.log(2.0) - math.lgamma(k)
    tot = 0.0
    for i in range(n + 1):
        v = i * h
        if v <= 0:
            dens = 0.0 if k > 1 else (0.5 if k == 1 else float('inf'))
        else:
            dens = math.exp(lnorm + (k - 1) * math.log(v) - v / 2.0)
        w = 1 if i in (0, n) else (4 if i % 2 else 2)
        tot += w * dens * norm_cdf(x * math.sqrt(v / df) - delta)
    return tot * h / 3.0


def nct_quantile(p, df, delta):
    lo, hi = delta - 40.0, delta + 40.0
    for _ in range(100):
        mid = 0.5 * (lo + hi)
        if nct_cdf(mid, df, delta) < p:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


# ------------------------------------------------------------------ linear algebra (no numpy)
def solve(A, b):
    """Gauss-Jordan with partial pivoting; A is a list of rows (copied). Refuses a singular system."""
    n = len(A)
    M = [list(A[i]) + [b[i]] for i in range(n)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(M[r][c]))
        R.refuse_unless(abs(M[p][c]) > 1e-14, 'singular design matrix')
        M[c], M[p] = M[p], M[c]
        piv = M[c][c]
        for j in range(c, n + 1):
            M[c][j] /= piv
        for r in range(n):
            if r != c and M[r][c] != 0.0:
                f = M[r][c]
                for j in range(c, n + 1):
                    M[r][j] -= f * M[c][j]
    return [M[i][n] for i in range(n)]


def inverse(A):
    n = len(A)
    cols = [solve(A, [1.0 if i == j else 0.0 for i in range(n)]) for j in range(n)]
    return [[cols[j][i] for j in range(n)] for i in range(n)]


def ols_cr1(y, X, clusters):
    """OLS of y on the columns of X (rows = observations; column 0 is the intercept) with CR1 cluster-robust
    variance: G/(G-1) (n-1)/(n-p) (X'X)^-1 [sum_g u_g u_g'] (X'X)^-1, u_g = sum_{i in g} x_i e_i.
    Returns {'coef', 'se', 't', 'G', 'n', 'p', 'resid'}."""
    n, p = len(y), len(X[0])
    R.refuse_unless(n == len(X) == len(clusters) and n > p, 'ols_cr1: shape')
    XtX = [[0.0] * p for _ in range(p)]
    Xty = [0.0] * p
    for xi, yi in zip(X, y):
        for a in range(p):
            xa = xi[a]
            if xa == 0.0:
                continue
            Xty[a] += xa * yi
            row = XtX[a]
            for b_ in range(p):
                row[b_] += xa * xi[b_]
    inv = inverse(XtX)
    coef = [sum(inv[a][b_] * Xty[b_] for b_ in range(p)) for a in range(p)]
    resid = [yi - sum(c * x for c, x in zip(coef, xi)) for xi, yi in zip(X, y)]
    score = {}
    for xi, e, g in zip(X, resid, clusters):
        u = score.setdefault(g, [0.0] * p)
        for a in range(p):
            u[a] += xi[a] * e
    G = len(score)
    meat = [[0.0] * p for _ in range(p)]
    for u in score.values():
        for a in range(p):
            if u[a] == 0.0:
                continue
            for b_ in range(p):
                meat[a][b_] += u[a] * u[b_]
    adj = (G / (G - 1)) * ((n - 1) / (n - p)) if G > 1 else float('nan')
    tmp = [[sum(inv[a][c] * meat[c][b_] for c in range(p)) for b_ in range(p)] for a in range(p)]
    V = [[adj * sum(tmp[a][c] * inv[c][b_] for c in range(p)) for b_ in range(p)] for a in range(p)]
    se = [math.sqrt(V[a][a]) if V[a][a] > 0 else float('nan') for a in range(p)]
    t = [c / s if s and s == s else float('nan') for c, s in zip(coef, se)]
    return {'coef': coef, 'se': se, 't': t, 'G': G, 'n': n, 'p': p, 'resid': resid}


# ------------------------------------------------------------------ the market model (H1)
MIN_CELL_DAYS = 60


def cell_groups(days, cells=R.CELLS, min_days=MIN_CELL_DAYS):
    """Calendar cells present among `days`; a cell with fewer than min_days days joins the previous one (the first
    joins the next). Returns {day: group index}."""
    raw = [R.cell_of(d, cells) for d in days]
    R.refuse_unless(all(c is not None for c in raw), 'a day lies outside every calendar cell')
    present = sorted(set(raw))
    size = {c: raw.count(c) for c in present}
    merged = {}
    groups = []
    for c in present:
        if groups and (size[c] < min_days or sum(size[x] for x in groups[-1]) < min_days):
            groups[-1].append(c)
        else:
            groups.append([c])
    if len(groups) > 1 and sum(size[x] for x in groups[-1]) < min_days:
        last = groups.pop()
        groups[-1].extend(last)
    for gi, g in enumerate(groups):
        for c in g:
            merged[c] = gi
    return {d: merged[c] for d, c in zip(days, raw)}


def design(days, factors, cells=R.CELLS, per_cell=True):
    """Rows [1, f1*1[c=0], f2*1[c=0], f3*1[c=0], f1*1[c=1], ...]; per_cell False -> constant slopes."""
    nf = len(factors)
    grp = cell_groups(days, cells) if per_cell else {d: 0 for d in days}
    ng = max(grp.values()) + 1
    X = []
    for i, d in enumerate(days):
        row = [1.0] + [0.0] * (nf * ng)
        g = grp[d]
        for j, f in enumerate(factors):
            row[1 + g * nf + j] = f[i]
        X.append(row)
    return X, ng


PRIMARY_PER_CELL = False          # D14: one slope per factor over the span (half-year and year cells reported)


def alpha_fit(days, y, factors, per_cell=None, cells=None):
    """The H1 model (D14: per_cell None -> the primary, constant slopes; cells None -> the half-year CELLS). Returns alpha (daily), its CR1 t by calendar month, G, k (slopes), the annualised alpha, the
    residual SD, and the slopes."""
    per_cell = PRIMARY_PER_CELL if per_cell is None else per_cell
    cells = R.CELLS if cells is None else cells
    X, ng = design(days, factors, cells, per_cell)
    fit = ols_cr1(y, X, [R.cluster_of(d) for d in days])
    k = len(X[0]) - 1
    rs = fit['resid']
    m = sum(rs) / len(rs)
    sd = math.sqrt(sum((e - m) ** 2 for e in rs) / (len(rs) - 1))
    return {'alpha': fit['coef'][0], 't': fit['t'][0], 'se': fit['se'][0], 'G': fit['G'], 'k': k, 'n': fit['n'],
            'cells': ng, 'alpha_annual': fit['coef'][0] * R.ANN_DAYS, 'resid_sd_annual': sd * math.sqrt(R.ANN_DAYS),
            'slopes': fit['coef'][1:]}


def h1_crit(G, k, cal=None):
    """max(t_{G-1-k}(0.95), CAL_H1); the floor never falls below t_{G-1-k}."""
    floor = t_crit(max(G - 1 - k, 1))
    c = CAL_H1 if cal is None else cal
    return max(floor, c or 0.0), floor


def h1(days, y, factors, cal=None):
    """H1 on the pooled span, plus the forward-span alpha (its sign is a pass condition, D18)."""
    pooled = alpha_fit(days, y, factors)
    crit, floor = h1_crit(pooled['G'], pooled['k'], cal)
    fwd_idx = [i for i, d in enumerate(days) if d >= R.day(R.SPAN_C[0])]
    fwd = alpha_fit([days[i] for i in fwd_idx], [y[i] for i in fwd_idx], [[f[i] for i in fwd_idx] for f in factors]) \
        if len(fwd_idx) > 60 else None
    t_ok = pooled['t'] == pooled['t'] and pooled['alpha'] > 0 and pooled['t'] >= crit
    f_ok = fwd is not None and fwd['alpha'] > 0
    return {'pooled': pooled, 'crit': crit, 'floor': floor, 'forward': fwd, 'pass_t': t_ok, 'pass_forward_sign': f_ok,
            'pass': bool(t_ok and f_ok)}


# ------------------------------------------------------------------ monthly returns and H2
def monthly(days, y):
    """Compounded monthly returns keyed by cluster month (the March 2025 stub joins April)."""
    acc = {}
    order = []
    for d, r in zip(days, y):
        m = R.cluster_of(d)
        if m not in acc:
            acc[m] = 1.0
            order.append(m)
        acc[m] *= 1.0 + r
    return [(m, acc[m] - 1.0) for m in order]


def sharpe(days, y):
    mo = [r for _, r in monthly(days, y)]
    G = len(mo)
    m = sum(mo) / G
    sd = math.sqrt(sum((x - m) ** 2 for x in mo) / (G - 1)) if G > 1 else float('nan')
    sr = m / sd * math.sqrt(12) if sd and sd == sd else float('nan')
    return {'G': G, 'mean_month': m, 'sd_month': sd, 'sr': sr, 'T': math.sqrt(G) * m / sd if sd else float('nan')}


def h2_crit(G, cal=None, sr0=SR_PAPER):
    delta = sr0 * math.sqrt(G / 12.0)
    q = nct_quantile(Q, G - 1, delta)
    c = CAL_H2 if cal is None else cal
    return (min(q, c) if c is not None else q), q


def h2(days, y, cal=None):
    s = sharpe(days, y)
    crit, q = h2_crit(s['G'], cal)
    return {**s, 'crit': crit, 'nct_q05': q, 'reject': bool(s['T'] == s['T'] and s['T'] < crit)}


# ------------------------------------------------------------------ the decision (§5)
OUTCOMES = {
    (True, False): 'alive_as_published',
    (True, True): 'alive_decayed',
    (False, True): 'decayed_not_shown',
    (False, False): 'undecided',
}


def decide(h1_pass, h2_reject):
    return OUTCOMES[(bool(h1_pass), bool(h2_reject))]


def shrink_factor(t):
    """Positive-part James-Stein factor towards zero: max(0, 1 - 1/t^2)."""
    if t is None or t != t or t <= 1:
        return 0.0
    return 1.0 - 1.0 / (t * t)


def economic(dep_raw_annual, dep_adj_annual, t_alpha, k=K_PROPOSED, bar=HLP_CALM):
    """Applied only after H1 passes; never a pass condition of H1 or H2. The deployable book's annualised net return
    (6 bp, Binance funding) at the proposed size k, taken as min(raw, market-adjusted), shrunk by the H1 alpha's
    t, against HLP's calm 4%/yr."""
    base = min(dep_raw_annual, dep_adj_annual)
    s = shrink_factor(t_alpha)
    val = k * base * s
    return {'k': k, 'raw_annual': dep_raw_annual, 'adj_annual': dep_adj_annual, 'min_annual': base, 'shrink': s,
            'shrunk_at_k': val, 'bar': bar, 'clears': bool(val >= bar)}


ROUTE = {
    ('alive_as_published', True): 'paper_then_scale',
    ('alive_as_published', False): 'exists_below_bar',
    ('alive_decayed', True): 'paper_reduced_size',
    ('alive_decayed', False): 'exists_below_bar',
    ('decayed_not_shown', None): 'close',
    ('undecided', None): 'not_demonstrated',
}


def route(outcome, econ):
    if outcome in ('decayed_not_shown', 'undecided'):
        return ROUTE[(outcome, None)]
    return ROUTE[(outcome, bool(econ and econ['clears']))]


def decision_from_series(days, y, factors, dep_days, dep_y, cal_h1=None, cal_h2=None):
    """Everything the decision uses, from the stored daily series (the recheck path)."""
    a = h1(days, y, factors, cal_h1)
    b = h2(days, y, cal_h2)
    out = decide(a['pass'], b['reject'])
    econ = None
    if a['pass']:
        dep = alpha_fit(dep_days, dep_y, factors)
        raw = sum(dep_y) / len(dep_y) * R.ANN_DAYS
        adj = dep['alpha_annual']
        econ = economic(raw, adj, a['pooled']['t'])
    return {'h1': a, 'h2': b, 'outcome': out, 'economic': econ, 'route': route(out, econ)}


def recheck(row):
    """([failures], decision) recomputed from the row's stored daily series; the row's own decision must match."""
    fails = []
    try:
        s = row['series']
        days = [R.day(R.dt.date.fromisoformat(x)) for x in s['days']]
        dec = decision_from_series(days, s['net'], [s['F1'], s['F2'], s['F3']], days, s['deploy'])
    except Exception as e:  # noqa: BLE001 - a malformed row unlocks nothing
        return [f'recheck: {e!r}'[:200]], None
    if dec['outcome'] != row.get('outcome') or dec['route'] != row.get('route'):
        fails.append(f"recheck: recomputed {dec['outcome']}/{dec['route']} != stored {row.get('outcome')}/{row.get('route')}")
    for key, val in (('h1_t', dec['h1']['pooled']['t']), ('h2_T', dec['h2']['T'])):
        st = row.get(key)
        if st is None or abs(st - val) > 1e-9:
            fails.append(f'recheck: {key} {st} != {val}')
    return fails, dec
