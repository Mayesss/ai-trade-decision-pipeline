"""Registration 016 — synthetic worlds for size, calibration and power. Reads no data at all.

A world is a 24-coin daily panel (BTC plus 23 alts) from SIM_START to the registered END, on the real calendar, so
the registered path runs end to end on it: universe selection, the nine-lookback book, the three factors, H1 with
half-year cells and month clusters, H2. Log returns:

    r_i,t = mu_i,t + beta_i(t) m_t + s_i (sigma_t / sigma_bar) eta_i,t
    m_t   = d_t + sigma_t z_t,  sigma_t^2 = omega + a (sigma z)_{t-1}^2 [+ g 1[z<0] (sigma z)^2] + b sigma_{t-1}^2

z, eta: Student t (df 4 unless stated) scaled to unit variance; crash days add a fixed market log return and double
sigma for 20 days. d_t: the world's market drift schedule (trend-free in the calibration set: constant, aligned
with the half-year cells, or a function of the conditional vol). mu_i,t: the trend component of the power worlds,
kappa x the coin's daily vol x (rho u_t + sqrt(1 - rho^2) v_i,t), u and v AR(1) with half-life h days.
Quote volumes rotate the bottom of the top 20 (all far above $2 M); every row trades.
"""
import datetime as dt
import math
import random

from ct16 import rule as R
from ct16 import stats as S

SIM_START = dt.date(2023, 9, 1)
N_COINS = 24
SIGMA_BAR = 0.55 / math.sqrt(365)            # market daily vol (annual 55%)


def _t_unit(rng, df):
    if df is None:
        return rng.gauss(0.0, 1.0)
    z = rng.gauss(0.0, 1.0)
    chi = rng.gammavariate(df / 2.0, 2.0)
    return z / math.sqrt(chi / df) / math.sqrt(df / (df - 2.0))


def _annual(x):
    return x / 365.0


def drift_fn(spec):
    """d(day, sigma_t) -> market log drift per day."""
    kind = spec.get('drift', 'flat')
    if kind == 'flat':
        return lambda o, sig: 0.0
    if kind == 'const':
        v = _annual(spec['annual'])
        return lambda o, sig: v
    if kind == 'cells':
        seq = spec['seq']

        def f(o, sig):
            c = R.cell_of(o)
            return 0.0 if c is None else _annual(seq[c])
        return f
    if kind == 'windows':                     # [(start, end, annual)] on real dates; 0 elsewhere
        w = [(R.day(dt.date.fromisoformat(a)), R.day(dt.date.fromisoformat(b)), _annual(v)) for a, b, v in spec['windows']]

        def f(o, sig):
            for a, b, v in w:
                if a <= o <= b:
                    return v
            return 0.0
        return f
    if kind == 'voldrift':                    # vol timing pays: drift + when the conditional vol is low
        lo, hi = _annual(spec['low']), _annual(spec['high'])
        return lambda o, sig: lo if sig < SIGMA_BAR else hi
    raise R.Refused(f'unknown drift {kind}')


def make_world(spec, seed):
    """{symbol: rule.Sym} for one draw of a world spec."""
    rng = random.Random(seed)
    df = spec.get('df', 4)
    a_, b_, g_ = spec.get('garch', (0.08, 0.90, 0.0))
    vol_mult = spec.get('vol_mult', 1.0)
    sbar = SIGMA_BAR * vol_mult
    omega = sbar * sbar * max(1e-6, 1.0 - a_ - b_ - g_ / 2.0)
    d0, d1 = R.day(SIM_START), R.day(R.END)
    drift = drift_fn(spec)
    crash = {R.day(dt.date.fromisoformat(x)): v for x, v in spec.get('crashes', {}).items()}
    ar = spec.get('ar', 0.0)                  # market return autocorrelation (mean reversion < 0)
    names = [R.BTC] + [f'C{i:02d}USDT' for i in range(1, N_COINS)]
    betas = [1.0] + [rng.uniform(1.0, 1.5) for _ in range(1, N_COINS)]
    idio = [0.30 / math.sqrt(365)] + [rng.uniform(0.35, 0.80) / math.sqrt(365) for _ in range(1, N_COINS)]
    lvl = [math.log(5e9)] + [rng.uniform(math.log(8e7), math.log(2e9)) for _ in range(1, N_COINS)]
    beta_switch = spec.get('beta_cells')      # alt beta multiplier per half-year cell
    kappa, half, rho = spec.get('kappa', 0.0), spec.get('half_life', 60), spec.get('rho', 0.7)
    phi = 0.5 ** (1.0 / half) if half else 0.0
    innov = math.sqrt(1.0 - phi * phi)
    u = rng.gauss(0.0, 1.0)
    v = [rng.gauss(0.0, 1.0) for _ in range(N_COINS)]
    tot_sd = [math.sqrt((betas[i] * SIGMA_BAR) ** 2 + idio[i] ** 2) for i in range(N_COINS)]
    sig2 = sbar * sbar
    prev_shock = 0.0
    prev_m = 0.0
    boost = 0
    price = [rng.uniform(0.5, 50.0) * (2000 if i == 0 else 1) for i in range(N_COINS)]
    occ = [0.0] * N_COINS
    rows = {n: {} for n in names}
    for o in range(d0, d1 + 1):
        sig2 = omega + (a_ + (g_ if prev_shock < 0 else 0.0)) * prev_shock * prev_shock + b_ * sig2
        sig = math.sqrt(sig2) * (2.0 if boost > 0 else 1.0)
        boost = max(0, boost - 1)
        z = _t_unit(rng, df)
        m = drift(o, sig) + sig * z + ar * prev_m
        prev_shock = sig * z
        if o in crash:
            m += math.log(1.0 + crash[o])
            boost = 20
        prev_m = m
        if kappa:
            u = phi * u + innov * rng.gauss(0.0, 1.0)
        cell = R.cell_of(o)
        for i, n in enumerate(names):
            b = betas[i]
            if beta_switch and i and cell is not None:
                b *= beta_switch[cell]
            mu = 0.0
            if kappa:
                v[i] = phi * v[i] + innov * rng.gauss(0.0, 1.0)
                mu = kappa * tot_sd[i] * (rho * u + math.sqrt(1.0 - rho * rho) * v[i])
            r = mu + b * m + idio[i] * (sig / sbar) * _t_unit(rng, df)
            price[i] *= max(0.05, 1.0 + r)               # simple returns: E[r | past] = mu + beta d exactly
            occ[i] = 0.98 * occ[i] + rng.gauss(0.0, 0.08)
            qv = math.exp(lvl[i] + occ[i] + rng.gauss(0.0, 0.3))
            rows[n][o] = (price[i], qv, 1000)
    return {n: R.Sym(n, rows[n]) for n in names}


def run_draw(spec, seed):
    """One draw through the registered path. Returns the statistics the size and power tables need, for the primary
    market model (constant slopes, D14) and the two calendar-cell variants (reported)."""
    syms = make_world(spec, seed)
    out = R.compute(syms)
    bk = out['book']
    days = bk['days']
    y = bk['ret']
    fac = [out['F1'], out['F2'], out['F3']]
    b = S.h2(days, y, cal=None)
    sc = [i for i, d in enumerate(days) if d >= R.day(R.SPAN_C[0])]
    sa = [i for i, d in enumerate(days) if d <= R.day(R.SPAN_A[1])]
    sub = lambda idx: ([days[i] for i in idx], [y[i] for i in idx], [[f[i] for i in idx] for f in fac])
    rec = {'T': b['T'], 'sr': b['sr'], 'vol': b['sd_month'] * math.sqrt(12), 'G': b['G'],
           'gross_mean': sum(bk['gross']) / len(bk['gross']), 'turnover': bk['turnover_per_year']}
    for key, kw in (('const', {'per_cell': False}), ('half', {'per_cell': True}),
                    ('year', {'per_cell': True, 'cells': R.YEAR_CELLS})):
        full = S.alpha_fit(days, y, fac, **kw)
        fwd = S.alpha_fit(*sub(sc), **kw)
        rec[f't_{key}'] = full['t']
        rec[f'k_{key}'] = full['k']
        rec[f'fwd_{key}_pos'] = fwd['alpha'] > 0
        rec[f'alpha_{key}'] = full['alpha_annual']
    rec['t_btc_only'] = S.alpha_fit(days, y, [out['F1']], per_cell=False)['t']
    for key, idx in (('a', sa), ('c', sc)):
        dd, yy, ff = sub(idx)
        rec[f'{key}_t'] = S.alpha_fit(dd, yy, ff)['t']
        rec[f'{key}_sr'] = S.sharpe(dd, yy)['sr']
    rec['t'] = rec['t_half' if S.PRIMARY_PER_CELL else 't_const']
    return rec


def _worker(args):
    spec, seed = args
    return run_draw(spec, seed)


def run_world(spec, draws, seed0, pool=None):
    jobs = [(spec, seed0 + i) for i in range(draws)]
    if pool is None:
        return [_worker(j) for j in jobs]
    return pool.map(_worker, jobs, chunksize=max(1, draws // 40))


# ------------------------------------------------------------------ the worlds
CELL_SEQ = (0.9, -0.7, 0.3, -0.5, 0.8, -0.3)          # annual market log drifts per half-year cell (aligned)
CALIBRATION = [                                       # trend-free: a beta-only book would pass several of these raw
    {'name': 'null flat', 'drift': 'flat'},
    {'name': 'null bull +60%', 'drift': 'const', 'annual': 0.6},
    {'name': 'null bear -60%', 'drift': 'const', 'annual': -0.6},
    {'name': 'null cells aligned', 'drift': 'cells', 'seq': CELL_SEQ},
    {'name': 'null crash x2', 'drift': 'flat', 'crashes': {'2025-10-10': -0.35, '2027-03-15': -0.35}},
    {'name': 'null chop vol x1.4', 'drift': 'flat', 'vol_mult': 1.4},
    {'name': 'null mean-reverting chop', 'drift': 'flat', 'ar': -0.10},
    {'name': 'null vol-timing pays', 'drift': 'voldrift', 'low': 1.0, 'high': -1.0},
    {'name': 'null alt-beta cells', 'drift': 'cells', 'seq': CELL_SEQ, 'beta_cells': (1.5, 0.7, 1.4, 0.8, 1.5, 0.7)},
    {'name': 'null fat t3 leverage cells', 'drift': 'cells', 'seq': CELL_SEQ, 'df': 3, 'garch': (0.05, 0.88, 0.10)},
    {'name': 'null bull + crash', 'drift': 'const', 'annual': 0.6, 'crashes': {'2025-10-10': -0.30}},
]
HINDSIGHT = [                                         # the designer-known shape of span (a); span (c) trend-free
    {'name': 'hindsight (a) rally->crash->decline, (c) flat', 'drift': 'windows',
     'windows': [('2025-04-01', '2025-10-05', 1.2), ('2025-10-11', '2026-02-28', -1.0)],
     'crashes': {'2025-10-10': -0.20}},
]
MISALIGNED = [                                        # regimes switching mid-cell: trend by definition (reported)
    {'name': 'regimes misaligned (quarterly)', 'drift': 'windows',
     'windows': [('2025-04-01', '2025-06-30', 1.0), ('2025-10-01', '2025-12-31', -1.0), ('2026-04-01', '2026-06-30', 1.0),
                 ('2026-10-01', '2026-12-31', -1.0), ('2027-04-01', '2027-06-30', 1.0), ('2027-10-01', '2027-12-31', -1.0)]},
]


def trend_world(kappa, half_life, extra=None):
    w = {'name': f'trend h{half_life} k{kappa:g}', 'drift': 'flat', 'kappa': kappa, 'half_life': half_life}
    w.update(extra or {})
    return w


# ------------------------------------------------------------------ helpers
def quantile(v, q):
    v = sorted(x for x in v if x == x)
    if not v:
        return None
    pos = q * (len(v) - 1)
    lo = int(math.floor(pos))
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


# ------------------------------------------------------------------ the size / calibration / power run
KEYS = ('t_const', 't_year', 't_half', 't_btc_only', 'T', 'sr', 'vol', 'a_t', 'c_t', 'a_sr', 'c_sr', 'alpha_const',
        'gross_mean', 'turnover')


def summarise(name, recs):
    out = {'name': name, 'draws': len(recs)}
    for k in KEYS:
        v = [r[k] for r in recs if r.get(k) is not None and r[k] == r[k]]
        out[f'mean_{k}'] = sum(v) / len(v) if v else None
    for k in ('t_const', 't_year', 't_half'):
        out[f'q95_{k}'] = quantile([r[k] for r in recs], 0.95)
    out['q05_T'] = quantile([r['T'] for r in recs], 0.05)
    return out


def pass_rates(recs, crit, fwd_key='fwd_alpha_pos', t_key='t'):
    n = len(recs)
    t_only = sum(1 for r in recs if r[t_key] == r[t_key] and r[t_key] >= crit) / n
    full = sum(1 for r in recs if r[t_key] == r[t_key] and r[t_key] >= crit and r[fwd_key]) / n
    return t_only, full


def reject_rate(recs, crit):
    return sum(1 for r in recs if r['T'] == r['T'] and r['T'] < crit) / len(recs)


def run_set(worlds, draws, pool, seed0, log=None, keep=None):
    res = []
    for wi, spec in enumerate(worlds):
        recs = run_world(spec, draws, seed0 + 100_000 * wi, pool)
        if keep is not None:
            keep[spec['name']] = recs
        row = summarise(spec['name'], recs)
        res.append(row)
        if log:
            log(row)
    return res


# ------------------------------------------------------------------ the registered tables
VARIANTS = {'constant': ('t_const', 'fwd_const_pos', 3), 'year': ('t_year', 'fwd_year_pos', 9),
            'half_year': ('t_half', 'fwd_half_pos', 18)}
G_SPAN = 33
TARGETS = (0.0, 0.5, 0.75, 1.0, 1.5, 1.57)
HALF_LIVES = (20, 60, 120)
# kappa for each target Sharpe, by linear interpolation in a synthetic pilot (200 draws per kappa in {0.05, 0.1, 0.15,
# 0.2, 0.3}; the mean per-draw 33-month Sharpe of the book; registration §6). The achieved mean Sharpe is reported.
KAPPA = {20: {0.0: 0.050, 0.5: 0.101, 0.75: 0.127, 1.0: 0.153, 1.5: 0.200, 1.57: 0.212},
         60: {0.0: 0.045, 0.5: 0.079, 0.75: 0.096, 1.0: 0.116, 1.5: 0.161, 1.57: 0.170},
         120: {0.0: 0.044, 0.5: 0.076, 0.75: 0.092, 1.0: 0.110, 1.5: 0.149, 1.57: 0.156}}


def target_worlds():
    return [trend_world(KAPPA[h][s], h, {'name': f'trend h{h} SR {s:g}', 'target_sr': s})
            for h in HALF_LIVES for s in TARGETS if h in KAPPA and s in KAPPA[h]]


def critical_values(keep, worlds=None):
    """Per variant: the floor t_{G-1-k}(0.95), the largest 95th percentile of the alpha t over the calibration
    worlds, and the registered critical value max(floor, that percentile)."""
    names = [w['name'] for w in (worlds or CALIBRATION)]
    out = {}
    for v, (tk, _fk, k) in VARIANTS.items():
        floor = S.t_crit(G_SPAN - 1 - k)
        q = max(quantile([r[tk] for r in keep[n]], 0.95) for n in names if n in keep)
        out[v] = {'floor': floor, 'max_q95': q, 'crit': max(floor, q), 'k': k}
    return out


def h2_critical(keep, names):
    q = min(quantile([r['T'] for r in keep[n]], 0.05) for n in names if n in keep)
    nct = S.nct_quantile(S.Q, G_SPAN - 1, S.SR_PAPER * math.sqrt(G_SPAN / 12.0))
    return {'nct_q05': nct, 'min_q05_sr157_worlds': q, 'crit': min(nct, q)}


def tables(keep):
    """Every rate the registration quotes, from the per-draw records."""
    crit = critical_values(keep)
    pub = [w['name'] for w in target_worlds() if w.get('target_sr') == 1.57]
    h2c = h2_critical(keep, pub) if pub else None
    rows = []
    for name, recs in keep.items():
        row = {'world': name, 'draws': len(recs), 'mean_sr': sum(r['sr'] for r in recs) / len(recs)}
        for v, (tk, fk, _k) in VARIANTS.items():
            t_only, full = pass_rates(recs, crit[v]['crit'], fk, tk)
            row[v] = {'mean_t': sum(r[tk] for r in recs) / len(recs), 'h1_t_only': t_only, 'h1': full}
        if h2c:
            row['h2_reject'] = reject_rate(recs, h2c['crit'])
        rows.append(row)
    return {'critical_values': crit, 'h2': h2c, 'worlds': rows}
