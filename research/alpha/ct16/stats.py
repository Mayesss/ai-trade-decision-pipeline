"""Registration 016 (Revision 1) — the estimators, the decision and its recheck. Pure functions.

The primary statistic (D14, review A B1) is Lo's (2008) holdings-based timing component of the net book return:
    A_t = sum_i (w_i,t-1 - wbar_i) r_i,t - cost_t
w_i,t-1 the book weight of coin i held into day t (notional over book value), wbar_i its mean over the coin's
member-days in the span evaluated, cost_t the day's fees (and, in the deployable variant, funding) over book value.
Under ANY constant drift the timing component has mean ~0 whatever the vol dynamics (leverage effect included);
drift that changes over time — what a trend rule harvests — gives it a positive mean. t_A = mean / CR1 SE, clustered
by calendar month (G = 33). Ladder (review A M1): `alive` iff t_A >= CAL_H1 and t_A on span (c) alone (its own
wbar) >= 1.282 (review A B2); else `decayed` iff t_A < C2 (calibrated on Table-4 worlds); else `not_demonstrated`.
The factor alpha t, the raw Sharpe and the calendar-cell alphas are reported blocks.
"""
import math

from ct16 import rule as R

Q = 0.05
SR_PAPER = 1.57                    # Table 4, p. 19: top-20 long-only, net of 10 bp, 2015-01-01 -> 2025-03-19
HLP_CALM = 0.04                    # the economic bar: HLP's calm return, %/yr (programme benchmark)


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


PRIMARY_PER_CELL = False          # the reported factor alpha: one slope per factor (calendar cells also reported)


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


def raw_sharpe_vs_paper(days, y, sr0=SR_PAPER):
    """Reported only (review A M1): T = sqrt(G) mean / sd of monthly returns and the noncentral-t 5% quantile at the
    paper's 1.57. It carries the forward market drift and decides nothing."""
    s = sharpe(days, y)
    return {**s, 'nct_q05_at_paper': nct_quantile(Q, s['G'] - 1, sr0 * math.sqrt(s['G'] / 12.0))}


# ------------------------------------------------------------------ the timing statistic (D14) and the ladder
T_FWD = 1.282                      # review A B2: span (c) alone must be one-sided significant at 10%


def timing_series(contrib, cost, idx=None):
    """(A_t for t in idx, wbar): wbar_i is coin i's mean held weight over its member-days INSIDE idx."""
    idx = list(range(len(contrib))) if idx is None else list(idx)
    wsum, wcnt = {}, {}
    for i in idx:
        for s_, w, _r in contrib[i]:
            wsum[s_] = wsum.get(s_, 0.0) + w
            wcnt[s_] = wcnt.get(s_, 0) + 1
    wbar = {k: wsum[k] / wcnt[k] for k in wsum}
    return [sum((w - wbar[s_]) * r for s_, w, r in contrib[i]) - cost[i] for i in idx], wbar


def mean_t(days, x):
    """Mean of x with a CR1 standard error clustered by calendar month (intercept-only OLS)."""
    n = len(x)
    R.refuse_unless(n > 1 and n == len(days), 'mean_t: shape')
    m = sum(x) / n
    g = {}
    for d, v in zip(days, x):
        c = R.cluster_of(d)
        g[c] = g.get(c, 0.0) + (v - m)
    G = len(g)
    R.refuse_unless(G > 1, 'mean_t: one cluster')
    var = sum(u * u for u in g.values()) / (n * n) * G / (G - 1)      # CR1 with one parameter: (n-1)/(n-p) = 1
    se = math.sqrt(var)
    sd = math.sqrt(sum((v - m) ** 2 for v in x) / (n - 1))
    return {'mean': m, 'se': se, 't': m / se if se > 0 else float('nan'), 'G': G, 'n': n,
            'annual': m * R.ANN_DAYS, 'sd_annual': sd * math.sqrt(R.ANN_DAYS),
            'sr_annual': m / sd * math.sqrt(R.ANN_DAYS) if sd > 0 else float('nan')}


def timing(days, contrib, cost, idx=None):
    idx = list(range(len(days))) if idx is None else list(idx)
    A, wbar = timing_series(contrib, cost, idx)
    return {**mean_t([days[i] for i in idx], A), 'coins': len(wbar)}


def forward_idx(days):
    return [i for i, d in enumerate(days) if d >= R.day(R.SPAN_C[0])]


CAL_H1 = 1.75                      # §5: max over the 19 calibration worlds of q95 + 2 MC SE, rounded up
                                   # ('null vol-timing pays': q95 1.691, SE 0.028 -> 1.746), 3,000 draws per world
C2 = -1.26                         # §6: min over the 9 Table-4 worlds of q05 - 2 MC SE, rounded down
                                   # ('as published h120 drift +0%': q05 -1.174, SE 0.040 -> -1.254)


def h1_crit(G, cal=None):
    """max(t_{G-1}(0.95), CAL_H1); the floor never falls below Student t with G - 1 df."""
    floor = t_crit(max(G - 1, 1))
    c = CAL_H1 if cal is None else cal
    return max(floor, c or 0.0), floor


def h1(days, contrib, cost, cal=None):
    pooled = timing(days, contrib, cost)
    crit, floor = h1_crit(pooled['G'], cal)
    fi = forward_idx(days)
    fwd = timing(days, contrib, cost, fi) if len(fi) > 60 else None
    t_ok = pooled['t'] == pooled['t'] and pooled['t'] >= crit
    f_ok = fwd is not None and fwd['t'] == fwd['t'] and fwd['t'] >= T_FWD
    return {'pooled': pooled, 'crit': crit, 'floor': floor, 'forward': fwd, 't_forward_min': T_FWD,
            'pass_t': bool(t_ok), 'pass_forward': bool(f_ok), 'pass': bool(t_ok and f_ok)}


def ladder(h1res, c2=None):
    """alive (H1 passes) > decayed (t_A < C2) > not_demonstrated."""
    c2 = C2 if c2 is None else c2
    if h1res['pass']:
        return 'alive'
    t = h1res['pooled']['t']
    if c2 is not None and t == t and t < c2:
        return 'decayed'
    return 'not_demonstrated'


def shrink_factor(t):
    """Positive-part James-Stein factor towards zero: max(0, 1 - 1/t^2)."""
    if t is None or t != t or t <= 1:
        return 0.0
    return 1.0 - 1.0 / (t * t)


def economic(dep, t_h1, years, bar=HLP_CALM):
    """Applied only to `alive`; routes only (review A M3). The deployable variant's timing leg (6 bp, Binance funding),
    its annualised Sharpe shrunk by the H1 t, sized at the Bayes-Kelly vol SR_s / (1 + 1/years) (the variance of a
    Sharpe estimate over `years`), gives the return SR_s^2 / (1 + 1/years); it must reach 4%/yr. A sanity floor."""
    sr = dep['sr_annual']
    s = shrink_factor(t_h1)
    sr_s = max(0.0, sr * s) if sr == sr else 0.0
    vol = sr_s / (1.0 + 1.0 / years)
    ret = sr_s * vol
    return {'sr_timing_leg': sr, 'shrink': s, 'sr_shrunk': sr_s, 'bayes_kelly_vol': vol, 'return_at_bk': ret,
            'bar': bar, 'clears': bool(ret >= bar)}


ROUTES = {'alive': ('paper_then_scale', 'exists_below_bar'), 'not_demonstrated': 'not_demonstrated',
          'decayed': 'close'}


def route(outcome, econ):
    if outcome != 'alive':
        return ROUTES[outcome]
    return ROUTES['alive'][0 if econ and econ['clears'] else 1]


def decision_from_series(days, contrib, cost, dep_contrib, dep_cost, cal_h1=None, c2=None):
    """Everything the decision uses, from the stored per-day records (the recheck path)."""
    a = h1(days, contrib, cost, cal_h1)
    out = ladder(a, c2)
    econ = None
    if out == 'alive':
        econ = economic(timing(days, dep_contrib, dep_cost), a['pooled']['t'], len(days) / R.ANN_DAYS)
    return {'h1': a, 'outcome': out, 'economic': econ, 'route': route(out, econ),
            'c2': C2 if c2 is None else c2}


def recheck(row):
    """([failures], decision) recomputed from the row's stored per-day records; the stored decision must match."""
    fails = []
    try:
        s = row['series']
        days = [R.day(R.dt.date.fromisoformat(x)) for x in s['days']]
        tup = lambda c: [[tuple(x) for x in day] for day in c]
        dec = decision_from_series(days, tup(s['contrib']), s['cost'], tup(s['dep_contrib']), s['dep_cost'])
    except Exception as e:  # noqa: BLE001 - a malformed row unlocks nothing
        return [f'recheck: {e!r}'[:200]], None
    if dec['outcome'] != row.get('outcome') or dec['route'] != row.get('route'):
        fails.append(f"recheck: recomputed {dec['outcome']}/{dec['route']} != stored {row.get('outcome')}/{row.get('route')}")
    for key, val in (('h1_t', dec['h1']['pooled']['t']), ('h1_t_forward', (dec['h1']['forward'] or {}).get('t'))):
        st = row.get(key)
        if st is None or val is None or abs(st - val) > 1e-9:
            fails.append(f'recheck: {key} {st} != {val}')
    return fails, dec
