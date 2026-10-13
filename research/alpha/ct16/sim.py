"""Registration 016 — synthetic worlds for size, calibration and power. Reads no data at all.

A world is a 24-coin daily panel (BTC plus 23 alts) from SIM_START to the registered END, on the real calendar, so
the registered path runs end to end on it: universe selection, the nine-lookback book, the timing statistic t_A
(pooled and on span (c) alone), the reported factor alpha and raw Sharpe. Simple returns:

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
    d0 = R.day(dt.date.fromisoformat(spec['start'])) if 'start' in spec else R.day(SIM_START)
    d1 = R.day(dt.date.fromisoformat(spec['end'])) if 'end' in spec else R.day(R.END)
    k_until = R.day(dt.date.fromisoformat(spec['kappa_until'])) if 'kappa_until' in spec else None
    drift = drift_fn(spec)
    crash = {R.day(dt.date.fromisoformat(x)): v for x, v in spec.get('crashes', {}).items()}
    ar = spec.get('ar', 0.0)                  # market return autocorrelation (mean reversion < 0)
    equal_drift = spec.get('equal_drift', False)
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
        dr = drift(o, sig)
        m = dr + sig * z + ar * prev_m
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
                if k_until is not None and o > k_until:
                    mu = 0.0
            if equal_drift and i:              # every coin's expected drift equals the market's (no alt excess drift)
                mu += (1.0 - b) * dr
            r = mu + b * m + idio[i] * (sig / sbar) * _t_unit(rng, df)
            price[i] *= max(0.05, 1.0 + r)               # simple returns: E[r | past] = mu + beta d exactly
            occ[i] = 0.98 * occ[i] + rng.gauss(0.0, 0.08)
            qv = math.exp(lvl[i] + occ[i] + rng.gauss(0.0, 0.3))
            rows[n][o] = (price[i], qv, 1000)
    return {n: R.Sym(n, rows[n]) for n in names}


def run_draw(spec, seed):
    """One draw through the registered path: t_A pooled and on (c) alone (the decision statistics), the raw Sharpe;
    with spec['with_alpha'] also the reported constant-slope factor alpha t (BTC, EW alt, vol-managed EW)."""
    syms = make_world(spec, seed)
    a, b = R.day(R.START), R.day(R.END)
    ufn = R.universe_fn_for(syms)
    bk = R.run_book(syms, a, b, ufn)
    days, y = bk['days'], bk['ret']
    pooled = S.timing(days, bk['contrib'], bk['cost'])
    fwd = S.timing(days, bk['contrib'], bk['cost'], S.forward_idx(days))
    span_a = S.timing(days, bk['contrib'], bk['cost'], [i for i, d in enumerate(days) if d <= R.day(R.SPAN_A[1])])
    sh = S.sharpe(days, y)
    rec = {'tA': pooled['t'], 'tA_c': fwd['t'], 'tA_a': span_a['t'], 'A_annual': pooled['annual'],
           'A_sr': pooled['sr_annual'], 'T': sh['T'], 'sr': sh['sr'], 'vol': sh['sd_month'] * math.sqrt(12),
           'gross_mean': sum(bk['gross']) / len(bk['gross']), 'turnover': bk['turnover_per_year']}
    if spec.get('with_alpha'):
        f1, f3 = R.factor_series(syms, a, b, ufn)
        rec['t_alpha'] = S.alpha_fit(days, y, [f1, bk['ew_alt'], f3], per_cell=False)['t']
    return rec


def table4_draw(spec, seed):
    """The paper's in-sample object on a synthetic decade (Table 4 calibration, review A M1): the book from 2015-01-01
    to 2025-03-19, its constant-beta alpha against BTC alone (the paper's alpha), its beta and its volatility."""
    syms = make_world(spec, seed)
    a, b = R.day(R.dt.date(2015, 1, 1)), R.day(R.PAPER_END)
    ufn = R.universe_fn_for(syms)
    bk = R.run_book(syms, a, b, ufn)
    f1, _ = R.factor_series(syms, a, b, ufn, with_vm=False)
    fit = S.alpha_fit(bk['days'], bk['ret'], [f1], per_cell=False)
    vol = S.mean_t(bk['days'], bk['ret'])['sd_annual']
    return {'alpha': fit['alpha_annual'], 'beta': fit['slopes'][0], 'vol': vol, 'ratio': fit['alpha_annual'] / vol}


def _worker(args):
    fn, spec, seed = args
    return (table4_draw if fn == 'table4' else run_draw)(spec, seed)


def run_world(spec, draws, seed0, pool=None, fn='draw'):
    jobs = [(fn, spec, seed0 + i) for i in range(draws)]
    if pool is None:
        return [_worker(j) for j in jobs]
    return pool.map(_worker, jobs, chunksize=max(1, draws // 80))


# ------------------------------------------------------------------ the worlds
GJR = (0.05, 0.88, 0.10)                              # GARCH with a leverage term (review A B1)
CELL_SEQ = (0.9, -0.7, 0.3, -0.5, 0.8, -0.3)          # annual drifts per half-year cell (aligned)
CALIBRATION = [                                       # trend-free (constant or regime drift): H1's size, 3,000 draws each
    {'name': 'null flat', 'drift': 'flat'},
    {'name': 'null bull +60%', 'drift': 'const', 'annual': 0.6},
    {'name': 'null bear -60%', 'drift': 'const', 'annual': -0.6},
    {'name': 'null cells aligned', 'drift': 'cells', 'seq': CELL_SEQ},
    {'name': 'null crash x2', 'drift': 'flat', 'crashes': {'2025-10-10': -0.35, '2027-03-15': -0.35}},
    {'name': 'null chop vol x1.4', 'drift': 'flat', 'vol_mult': 1.4},
    {'name': 'null mean-reverting chop', 'drift': 'flat', 'ar': -0.10},
    {'name': 'null vol-timing pays', 'drift': 'voldrift', 'low': 1.0, 'high': -1.0},
    {'name': 'null alt-beta cells', 'drift': 'cells', 'seq': CELL_SEQ, 'beta_cells': (1.5, 0.7, 1.4, 0.8, 1.5, 0.7)},
    {'name': 'null fat t3 leverage cells', 'drift': 'cells', 'seq': CELL_SEQ, 'df': 3, 'garch': GJR},
    {'name': 'null bull + crash', 'drift': 'const', 'annual': 0.6, 'crashes': {'2025-10-10': -0.30}},
    # review A B1: leverage effect with a sustained bull, and asymmetric vol timing
    {'name': 'lev bull +40% GJR t3', 'drift': 'const', 'annual': 0.4, 'df': 3, 'garch': GJR},
    {'name': 'lev bull +80% GJR t3', 'drift': 'const', 'annual': 0.8, 'df': 3, 'garch': GJR},
    {'name': 'lev bull +80% GJR t4', 'drift': 'const', 'annual': 0.8, 'df': 4, 'garch': GJR},
    {'name': 'lev bull +80% GJR t3, -35% day in (a)', 'drift': 'const', 'annual': 0.8, 'df': 3, 'garch': GJR,
     'crashes': {'2025-10-10': -0.35}},
    {'name': 'lev bull +80% GJR t3, -35% day in (c)', 'drift': 'const', 'annual': 0.8, 'df': 3, 'garch': GJR,
     'crashes': {'2027-03-15': -0.35}},
    {'name': 'lev bull +120% GJR t3', 'drift': 'const', 'annual': 1.2, 'df': 3, 'garch': GJR},
    {'name': 'vol timing asym (+100/-60) GJR t3', 'drift': 'voldrift', 'low': 1.0, 'high': -0.6, 'df': 3, 'garch': GJR},
    {'name': 'lev bear -60% GJR t3', 'drift': 'const', 'annual': -0.6, 'df': 3, 'garch': GJR},
]
HINDSIGHT = [                                         # the designer-known shape of span (a); span (c) trend-free
    {'name': 'hindsight (a) rally->crash->decline, (c) flat', 'drift': 'windows',
     'windows': [('2025-04-01', '2025-10-05', 1.2), ('2025-10-11', '2026-02-28', -1.0)],
     'crashes': {'2025-10-10': -0.20}},
    # review A B2: trend in the warm-up and (a), none in (c) — the choice to test now made because (a) looked good
    {'name': 'S1 trend (a) SR~1.5, (c) dead', 'kappa': 0.161, 'half_life': 60, 'kappa_until': '2026-06-30'},
    {'name': 'S2 trend (a) SR~2.5, (c) dead', 'kappa': 0.26, 'half_life': 60, 'kappa_until': '2026-06-30'},
    {'name': 'regimes misaligned (quarterly)', 'drift': 'windows',
     'windows': [('2025-04-01', '2025-06-30', 1.0), ('2025-10-01', '2025-12-31', -1.0), ('2026-04-01', '2026-06-30', 1.0),
                 ('2026-10-01', '2026-12-31', -1.0), ('2027-04-01', '2027-06-30', 1.0), ('2027-10-01', '2027-12-31', -1.0)]},
]
TARGETS = (0.0, 0.5, 0.75, 1.0, 1.5)
HALF_LIVES = (20, 60, 120)
# kappa for each target Sharpe (mean per-draw 33-month Sharpe of the book), from the Revision-0 pilot (200 draws per
# kappa in {0.05, 0.1, 0.15, 0.2, 0.3}); the achieved mean per-draw and pooled Sharpes are reported (review A m1).
KAPPA = {20: {0.0: 0.050, 0.5: 0.101, 0.75: 0.127, 1.0: 0.153, 1.5: 0.200},
         60: {0.0: 0.045, 0.5: 0.079, 0.75: 0.096, 1.0: 0.116, 1.5: 0.161},
         120: {0.0: 0.044, 0.5: 0.076, 0.75: 0.092, 1.0: 0.110, 1.5: 0.149}}
# Table 4 (review A M1): kappa at which a synthetic decade 2015-01 -> 2025-03-19 (BTC-like drift +75%/yr shared by
# every coin, GJR t3)
# reproduces the paper's alpha / vol = 10.8% / 9% = 1.2 against BTC; filled from the Table-4 pilot (§6).
KAPPA_PUB = {20: 0.090, 60: 0.065, 120: 0.061}     # pilot: 40 decades per kappa in {0, .03, .06, .09, .12}
TABLE4_WORLD = {'start': '2014-01-01', 'end': '2025-03-19', 'drift': 'const', 'annual': 0.75, 'df': 3, 'garch': GJR,
                'equal_drift': True}
FWD_DRIFTS = (0.0, 0.4, -0.4)


def trend_world(kappa, half_life, extra=None):
    w = {'name': f'trend h{half_life} k{kappa:g}', 'drift': 'flat', 'kappa': kappa, 'half_life': half_life,
         'with_alpha': True}
    w.update(extra or {})
    return w


def target_worlds():
    return [trend_world(KAPPA[h][s], h, {'name': f'trend h{h} SR {s:g}', 'target_sr': s})
            for h in HALF_LIVES for s in TARGETS]


def drift_power_worlds():
    """H2 power at a dead and a half-strength book under a forward market drift of +-40%/yr (review A M1)."""
    return [trend_world(KAPPA[60][s], 60, {'name': f'trend h60 SR {s:g} drift {d:+.0%}', 'drift': 'const',
                                             'annual': d, 'target_sr': s})
            for s in (0.0, 0.5) for d in (0.4, -0.4)]


def table4_worlds():
    """The as-published worlds for C2: kappa_pub(h) x forward drift 0 / +-40% (GJR t3) on the test calendar."""
    return [{'name': f'as published h{h} drift {d:+.0%}', 'kappa': KAPPA_PUB[h], 'half_life': h, 'drift': 'const',
             'annual': d, 'df': 3, 'garch': GJR} for h in HALF_LIVES if h in KAPPA_PUB for d in FWD_DRIFTS]


# ------------------------------------------------------------------ quantiles with Monte Carlo standard errors
def quantile(v, q):
    v = sorted(x for x in v if x == x)
    if not v:
        return None
    pos = q * (len(v) - 1)
    lo = int(math.floor(pos))
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def quantile_se(v, q):
    """Order-statistic standard error of the q-quantile: half the width of the 95% binomial interval / 1.96."""
    n = len([x for x in v if x == x])
    d = 1.96 * math.sqrt(q * (1 - q) / n)
    return (quantile(v, min(1.0, q + d)) - quantile(v, max(0.0, q - d))) / (2 * 1.96)


def summary_row(name, recs):
    """The per-world summary written to the log (a deterministic function of the records)."""
    row = {'name': name, 'draws': len(recs)}
    for k in recs[0]:
        v = [r[k] for r in recs if isinstance(r[k], (int, float)) and r[k] == r[k]]
        row[f'mean_{k}'] = sum(v) / len(v) if v else None
    if 'tA' in recs[0]:
        row['q95_tA'] = quantile([r['tA'] for r in recs], 0.95)
        row['q95_tA_se'] = quantile_se([r['tA'] for r in recs], 0.95)
        row['q05_tA'] = quantile([r['tA'] for r in recs], 0.05)
        row['q05_tA_se'] = quantile_se([r['tA'] for r in recs], 0.05)
    return row


def _world_file(cache_dir, spec, draws, seed0, fn):
    import hashlib
    import json
    key = json.dumps({'spec': spec, 'draws': draws, 'seed0': seed0, 'fn': fn}, sort_keys=True)
    return cache_dir / f"{hashlib.sha256(key.encode()).hexdigest()[:20]}.json", key


def run_set(worlds, draws, pool, seed0, log=None, keep=None, fn='draw', cache_dir=None):
    """Run each world (draw seeds seed0 + 100,000 x its index + i). With cache_dir, a world whose file holds the same
    (spec, draws, seed0, fn) is NOT re-run: its records are read back (resumable); a new world's file is written
    atomically as soon as it finishes. The records depend only on (spec, seed), never on the pool or on the order of
    execution, so a resumed run returns exactly the records of an uninterrupted one."""
    import json
    res = []
    for wi, spec in enumerate(worlds):
        s0 = seed0 + 100_000 * wi
        recs = None
        if cache_dir is not None:
            path, key = _world_file(cache_dir, spec, draws, s0, fn)
            if path.exists():
                blob = json.loads(path.read_text())
                if blob.get('key') == key:
                    recs = blob['records']
        if recs is None:
            recs = run_world(spec, draws, s0, pool, fn)
            if cache_dir is not None:
                cache_dir.mkdir(parents=True, exist_ok=True)
                tmp = path.with_suffix('.tmp')
                tmp.write_text(json.dumps({'key': key, 'records': recs}))
                tmp.replace(path)
        if keep is not None:
            keep[spec['name']] = recs
        row = summary_row(spec['name'], recs)
        res.append(row)
        if log:
            log(row)
    return res


def kappa_for_ratio(rows, target=1.2):
    """Linear interpolation of kappa at which the mean alpha / vol ratio of the Table-4 decade equals target."""
    pts = sorted((r['kappa'], r['mean_ratio']) for r in rows)
    for (k0, r0), (k1, r1) in zip(pts, pts[1:]):
        if (r0 - target) * (r1 - target) <= 0 and r1 != r0:
            return k0 + (k1 - k0) * (target - r0) / (r1 - r0)
    return None


# ------------------------------------------------------------------ the registered tables
def pass_rates(recs, crit, t_fwd=None):
    t_fwd = S.T_FWD if t_fwd is None else t_fwd
    n = len(recs)
    t_only = sum(1 for r in recs if r['tA'] == r['tA'] and r['tA'] >= crit) / n
    full = sum(1 for r in recs if r['tA'] == r['tA'] and r['tA'] >= crit and r['tA_c'] == r['tA_c']
               and r['tA_c'] >= t_fwd) / n
    return t_only, full


def tables(keep, cal_names, pub_names, cal_h1, c2):
    """Every rate the registration quotes, from the per-draw records, at the registered critical values."""
    rows = []
    for name, recs in keep.items():
        if 'tA' not in recs[0]:
            continue
        t_only, full = pass_rates(recs, cal_h1)
        n = len(recs)
        srs = [r['sr'] for r in recs]
        pooled_sr = (sum(r['sr'] * r['vol'] for r in recs) / n) / math.sqrt(sum(r['vol'] ** 2 for r in recs) / n)
        row = {'world': name, 'draws': n, 'mean_per_draw_sr': sum(srs) / n, 'pooled_sr': pooled_sr,
               'mean_tA': sum(r['tA'] for r in recs) / n, 'q95_tA': quantile([r['tA'] for r in recs], 0.95),
               'q95_tA_se': quantile_se([r['tA'] for r in recs], 0.95),
               'q05_tA': quantile([r['tA'] for r in recs], 0.05), 'h1_t_only': t_only, 'h1': full,
               'decayed': sum(1 for r in recs if not (r['tA'] >= cal_h1 and r['tA_c'] >= S.T_FWD) and r['tA'] < c2) / n}
        if 't_alpha' in recs[0]:
            row['mean_t_alpha'] = sum(r['t_alpha'] for r in recs) / n
        rows.append(row)
    cal = [r for r in rows if r['world'] in cal_names]
    pub = [r for r in rows if r['world'] in pub_names]
    return {'cal_h1': cal_h1, 'c2': c2,
            'max_q95_plus_2se': max(r['q95_tA'] + 2 * r['q95_tA_se'] for r in cal) if cal else None,
            'min_q05_minus_2se': min(r['q05_tA'] - 2 * quantile_se(keep[r['world']] and [x['tA'] for x in keep[r['world']]], 0.05)
                                     for r in pub) if pub else None,
            'worlds': rows}


def power_run(pool, log, draws_cal=3000, draws_pub=3000, draws_power=600, draws_hind=1000, cache_dir=None):
    """The registered size, calibration and power run (reads no data). Returns {name: [records]} in a fixed order."""
    keep = {}
    run_set(CALIBRATION, draws_cal, pool, 7, log, keep, cache_dir=cache_dir)
    run_set(table4_worlds(), draws_pub, pool, 11, log, keep, cache_dir=cache_dir)
    run_set(target_worlds(), draws_power, pool, 9, log, keep, cache_dir=cache_dir)
    run_set(drift_power_worlds(), draws_power, pool, 13, log, keep, cache_dir=cache_dir)
    run_set(HINDSIGHT, draws_hind, pool, 8, log, keep, cache_dir=cache_dir)
    return keep
