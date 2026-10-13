"""Registration 014 — SYNTHETIC power, construction choice and sizing. No real data is read here.

A synthetic BTC/ETH world calibrated to the N2 scout's printed UNCONDITIONAL dispersion facts
(weekly SD 9.0 % / 11.3 %, kurtosis 6.1 / 7.4, |r| SD/mean ~1.0, 1-SD strangle payoff SD/mean
2.7 / 2.8, put 4.9, top-3 weeks 16-20 % of strangle payoff): a daily log-vol AR(1) regime, hourly
Student-t innovations, common jumps, BTC-ETH correlation 0.8. The market prices options at its own
vol forecast x a multiplier m (plus pricing noise); m is calibrated per construction so that the
GROSS mean P&L per unit premium equals the target VRP v (0, 0.05, 0.10, 0.15). Costs are the run's.

Constructions compared (registration §3): U weekly ATM straddle unhedged; H24 / H8 delta-hedged
every 24 h / 8 h on the perp; D1 seven 1-day ATM straddles a week, unhedged. All held to expiry.
Power: discovery-sized samples (143 weeks) and holdout-sized samples (129 weeks) drawn as blocks of
13 consecutive synthetic weeks; the decision functions are `ct.shortvol`'s own.
"""
import math
import random
import statistics

from . import shortvol as sv

H = sv.HOUR
DAY = sv.DAY
DISC_WEEKS = 143                     # Fridays 2021-04-02 -> 2023-12-22
HOLD_WEEKS = 129                     # Fridays 2024-01-05 -> 2026-06-19
BLOCK = 13

# 0-1 d, 30-45 delta p75 half-spreads (scout cost table) for the daily construction
HALF_SPREAD_1D = {'BTC': 0.0515, 'ETH': 0.0525}

# 'base' fits the scout's facts except the put tail (synthetic put payoff SD/mean ~3.5-3.8 against 4.9);
# 'fat' has rarer, larger, down-skewed jumps (put SD/mean ~4.0-4.2, kurtosis 7.2 / 6.5) - a sensitivity.
PARAMS = {
    'phi': 0.99, 'sd_x': 0.44, 'vol_mean': {'BTC': 0.68, 'ETH': 0.85}, 'df': 4.0,
    'p_jump': 0.0002, 'jump_sd': 0.12, 'eth_jump_scale': 1.4, 'rho': 0.8, 'eth_idio_sd': 0.15,
    'jump_down': 0.85, 'iv_noise': 0.08, 'dvol_m': 1.1,
}
FAT = dict(PARAMS, vol_mean={'BTC': 0.66, 'ETH': 0.825}, p_jump=0.00008, jump_sd=0.20, jump_down=0.9)


def _t_std(rng, df):
    """Standardised Student-t draw (unit variance)."""
    z = rng.gauss(0, 1)
    c = sum(rng.gauss(0, 1) ** 2 for _ in range(int(df)))
    return z / math.sqrt(c / df) / math.sqrt(df / (df - 2))


def _vol_fc(x, mu, phi, s2, k_days, jvar_day):
    """Forecast annualised vol over the next k days from today's log-vol x (lognormal AR(1))."""
    tot = 0.0
    for j in range(k_days):
        m = mu + phi ** j * (x - mu)
        v = s2 * (1 - phi ** (2 * j))
        tot += math.exp(2 * m + 2 * v) / 365.0 + jvar_day
    return math.sqrt(tot / (k_days / 365.0))


def gen_bank(n_weeks, seed=0, p=PARAMS):
    """Synthetic weeks. Each: {coin: {'S': 169 hourly prices from entry, 'dvol': 169 hourly DVOL,
    'fc7': forecast vol for the 7-day straddle, 'fc1': [7 one-day forecasts], 'stress': bool|None}}."""
    rng = random.Random(seed)
    phi, sd_x = p['phi'], p['sd_x']
    s2 = sd_x ** 2
    se = sd_x * math.sqrt(1 - phi ** 2)
    jvar = {'BTC': 24 * p['p_jump'] * p['jump_sd'] ** 2,
            'ETH': 24 * p['p_jump'] * (p['jump_sd'] * p['eth_jump_scale']) ** 2}
    mu = {}
    for c in sv.COINS:                   # E[sigma^2] + jump variance -> the target mean annual variance
        target = p['vol_mean'][c] ** 2 - jvar[c] * 365
        mu[c] = (math.log(target) - 2 * s2) / 2
    burn = 60
    days = n_weeks * 7 + burn + 10
    x = {'BTC': mu['BTC'], 'ETH': mu['ETH']}
    e_idio = 0.0
    S = {'BTC': [1.0], 'ETH': [1.0]}
    dv = {'BTC': [], 'ETH': []}
    fc7, fc1 = {'BTC': [], 'ETH': []}, {'BTC': [], 'ETH': []}
    for d in range(days):
        z = rng.gauss(0, 1)
        x['BTC'] = mu['BTC'] + phi * (x['BTC'] - mu['BTC']) + se * z
        e_idio = 0.98 * e_idio + p['eth_idio_sd'] * math.sqrt(1 - 0.98 ** 2) * rng.gauss(0, 1)
        x['ETH'] = mu['ETH'] + (x['BTC'] - mu['BTC']) + e_idio
        for c in sv.COINS:
            mu_c = mu[c]
            fc7[c].append(_vol_fc(x[c], mu_c, phi, s2, 7, jvar[c]))
            fc1[c].append(_vol_fc(x[c], mu_c, phi, s2, 1, jvar[c]))
            dv[c].append(100 * p['dvol_m'] * _vol_fc(x[c], mu_c, phi, s2, 30, jvar[c]))
        sig = {c: math.exp(x[c]) / math.sqrt(8760) for c in sv.COINS}
        for _ in range(24):
            zb = _t_std(rng, p['df'])
            ze = p['rho'] * zb + math.sqrt(1 - p['rho'] ** 2) * _t_std(rng, p['df'])
            jb = je = 0.0
            if rng.random() < p['p_jump']:
                jb = abs(rng.gauss(0, p['jump_sd'])) * (-1 if rng.random() < p['jump_down'] else 1)
                je = jb * p['eth_jump_scale']
            S['BTC'].append(S['BTC'][-1] * math.exp(sig['BTC'] * zb + jb - 0.5 * sig['BTC'] ** 2))
            S['ETH'].append(S['ETH'][-1] * math.exp(sig['ETH'] * ze + je - 0.5 * sig['ETH'] ** 2))
    weeks = []
    for w in range(n_weeks):
        d0 = burn + 8 + 7 * w
        wk = {}
        for c in sv.COINS:
            hs = S[c][d0 * 24: d0 * 24 + 169]
            s0 = hs[0]
            hist = dv[c][d0 - 8: d0]
            flag = hist[-1] / min(hist[-7:]) >= sv.STRESS_RATIO
            wk[c] = {'S': [v / s0 for v in hs], 'dvol': [dv[c][d0 + i // 24] for i in range(169)],
                     'fc7': fc7[c][d0], 'fc1': fc1[c][d0: d0 + 7], 'stress': flag}
        weeks.append(wk)
    return weeks


# ------------------------------------------------------------------ calibration diagnostics (synthetic)
def dispersion_facts(bank, coin, block=307):
    """The scout's statistics on synthetic weeks: weekly SD, kurtosis, |r| SD/mean, strangle/put payoff
    SD/mean and top-1/3/5 shares (medians over blocks of `block` weeks)."""
    r = [w[coin]['S'][-1] - 1 for w in bank]
    out = {'sd': [], 'kurt': [], 'absratio': [], 'strangle_sdm': [], 'put_sdm': [], 'top1': [], 'top3': [], 'top5': [],
           'put_top3': []}
    for i in range(0, len(r) - block + 1, block):
        x = r[i:i + block]
        sd = statistics.pstdev(x)
        a = [abs(v) for v in x]
        otm = sorted((max(v - sd, 0) for v in a), reverse=True)
        put = sorted((max(-v - sd, 0) for v in x), reverse=True)
        out['sd'].append(sd)
        out['kurt'].append(statistics.fmean((v / sd) ** 4 for v in x))
        out['absratio'].append(statistics.pstdev(a) / statistics.fmean(a))
        if sum(otm) > 0:
            out['strangle_sdm'].append(statistics.pstdev(otm) / statistics.fmean(otm))
            out['top1'].append(otm[0] / sum(otm))
            out['top3'].append(sum(otm[:3]) / sum(otm))
            out['top5'].append(sum(otm[:5]) / sum(otm))
        if sum(put) > 0:
            out['put_sdm'].append(statistics.pstdev(put) / statistics.fmean(put))
            out['put_top3'].append(sum(put[:3]) / sum(put))
    res = {k: round(statistics.median(v), 3) for k, v in out.items() if v}
    d7 = [bank[i + 1][coin]['dvol'][0] - bank[i][coin]['dvol'][0] for i in range(len(bank) - 1)]
    res['dvol_7d_change_sd'] = round(statistics.pstdev(d7), 2)
    res['dvol_median'] = round(statistics.median(w[coin]['dvol'][0] for w in bank), 1)
    return res


# ------------------------------------------------------------------ constructions on one synthetic week
def _mark(coin, S0, iv, tenor_d=7):
    T = tenor_d / 365.0
    return {'coin': coin, 'entry': 0, 'expiry': tenor_d * DAY, 'K': S0, 'S': S0, 'iv': iv,
            'mark_call': sv.b76(S0, S0, T, iv, True), 'mark_put': sv.b76(S0, S0, T, iv, False)}


def week_outcome(wk, coin, construction, m, noise, cost_mult=1.0):
    """(net, gross, cost, premium) per unit notional for one coin-week. `noise`: the IV pricing noise
    draws [7 or 1 values] for this coin-week (fixed across m so calibration is smooth)."""
    c = wk[coin]
    if construction == 'D1':
        net = gross = cost = prem = 0.0
        for d in range(7):
            s0, s1 = c['S'][24 * d], c['S'][24 * (d + 1)]
            iv = c['fc1'][d] * m * noise[d]
            T = 1 / 365.0
            pc, pp = sv.b76(s0, s0, T, iv, True), sv.b76(s0, s0, T, iv, False)
            hs = HALF_SPREAD_1D[coin] * cost_mult
            sold = (pc + pp) * (1 - hs)
            fees = sv.leg_fee(s0, pc * (1 - hs)) + sv.leg_fee(s0, pp * (1 - hs))
            pay = abs(s1 - s0)
            g = (pc + pp) - pay
            k = hs * (pc + pp) + fees
            gross += g
            cost += k
            prem += pc + pp
        return gross - cost, gross, cost, prem
    iv = c['fc7'] * m * noise[0]
    mark = _mark(coin, 1.0, iv)
    path = [(i * H, c['S'][i], c['dvol'][i]) for i in range(169)]
    he = {'U': None, 'H24': 24, 'H8': 8}[construction]
    o = sv.position_pnl(mark, path, c['S'][-1], hedge_every_h=he, half_spread=sv.HALF_SPREAD[coin] * cost_mult,
                        perp_cost=(sv.PERP_TAKER + sv.PERP_SLIP) * cost_mult)
    cost = o['spread'] + o['fees'] + o['delivery'] + o['hedge_fees']
    return o['net'], o['net'] + cost, cost, o['premium_mark']


def noises(bank, seed=1, sd=PARAMS['iv_noise']):
    rng = random.Random(seed)
    return [{c: [math.exp(rng.gauss(0, sd) - sd * sd / 2) for _ in range(7)] for c in sv.COINS} for _ in bank]


def book(bank, nz, construction, m, cost_mult=1.0):
    """Weekly book rows (equal notional BTC/ETH): r (net), gross, cost, premium, stressed."""
    out = []
    for wk, z in zip(bank, nz):
        o = {c: week_outcome(wk, c, construction, m, z[c], cost_mult) for c in sv.COINS}
        out.append({'r': statistics.fmean(o[c][0] for c in sv.COINS), 'gross': statistics.fmean(o[c][1] for c in sv.COINS),
                    'cost': statistics.fmean(o[c][2] for c in sv.COINS), 'prem': statistics.fmean(o[c][3] for c in sv.COINS),
                    'stressed': any(wk[c]['stress'] for c in sv.COINS), 'entry': len(out)})
    return out


def gross_ratio(rows):
    return sum(r['gross'] for r in rows) / sum(r['prem'] for r in rows)


def net_ratio(rows):
    return sum(r['r'] for r in rows) / sum(r['prem'] for r in rows)


def calibrate_m(bank, nz, construction, targets, grid=None, key=gross_ratio):
    """m per target (linear interpolation of key(book) over an m grid)."""
    grid = grid or [0.80 + 0.025 * i for i in range(21)]
    vals = [(m, key(book(bank, nz, construction, m))) for m in grid]
    out = {}
    for tgt in targets:
        for (m0, v0), (m1, v1) in zip(vals, vals[1:]):
            if (v0 - tgt) * (v1 - tgt) <= 0 and v1 != v0:
                out[tgt] = m0 + (tgt - v0) * (m1 - m0) / (v1 - v0)
                break
    return out, vals


# ------------------------------------------------------------------ power
def draw(rows, weeks, rng, block=BLOCK):
    out = []
    while len(out) < weeks:
        i = rng.randrange(0, len(rows) - block)
        out.extend(rows[i:i + block])
    return out[:weeks]


def pass_rates(rows, sims=2000, seed=7, floors=None, n=sv.SIZE_N):
    """P(discovery pass) for T1 and T2, P(any), P(holdout existence) (k = 1), joint, on block draws.
    floors: (disc, {1: hold1, 2: hold2}) or None for the registered floors."""
    fd, fh = floors if floors else (None, {1: None, 2: None})
    rng = random.Random(seed)
    d1 = d2 = dany = h1 = h2 = j1 = j2 = jany = 0
    for _ in range(sims):
        disc = draw(rows, DISC_WEEKS, rng)
        hold = draw(rows, HOLD_WEEKS, rng)
        s1 = [w['r'] for w in disc]
        s2 = [0.0 if w['stressed'] else w['r'] for w in disc]
        res, (dec, passed) = sv.evaluate_discovery({'T1': s1, 'T2': s2}, floor=fd, n=n)
        p1, p2 = res['T1']['pass'], res['T2']['pass']
        k = max(1, len(passed))
        e1 = sv.holdout_existence([w['r'] for w in hold], k, floor=fh[k])[0]
        e2 = sv.holdout_existence([0.0 if w['stressed'] else w['r'] for w in hold], k, floor=fh[k])[0]
        d1 += p1
        d2 += p2
        dany += bool(passed)
        h1 += e1
        h2 += e2
        j1 += p1 and e1
        j2 += p2 and e2
        jany += (p1 and e1) or (p2 and e2)
    n = float(sims)
    return {'disc_T1': d1 / n, 'disc_T2': d2 / n, 'disc_any': dany / n, 'hold_T1_alone': h1 / n,
            'hold_T2_alone': h2 / n, 'joint_T1': j1 / n, 'joint_T2': j2 / n, 'joint_any': jany / n}


def kelly(rows, p_atom, atom, grid=None):
    """Tail-aware Kelly: argmax_f mean log(1 + f r) over the synthetic net weekly book returns mixed with
    an adversarial atom (probability p_atom per week, return `atom`)."""
    grid = grid or [0.05 * i for i in range(1, 60)]
    best = (0.0, -1e9)
    for f in grid:
        if 1 + f * atom <= 0 or any(1 + f * r['r'] <= 0 for r in rows):
            break
        g = (1 - p_atom) * statistics.fmean(math.log(1 + f * r['r']) for r in rows) + p_atom * math.log(1 + f * atom)
        if g > best[1]:
            best = (f, g)
    return best[0]


def gaussian_kelly(rows):
    rs = [r['r'] for r in rows]
    return statistics.fmean(rs) / statistics.pvariance(rs)


# ------------------------------------------------------------------ the §3 table
CONSTRUCTIONS = ('U', 'H24', 'H8', 'D1')
VRPS = (0.0, 0.05, 0.10, 0.15)
M_GRID = tuple(0.85 + 0.05 * i for i in range(11))


def _m_for(series, tgt):
    for (m0, v0), (m1, v1) in zip(series, series[1:]):
        if (v0 - tgt) * (v1 - tgt) <= 0 and v1 != v0:
            return m0 + (tgt - v0) * (m1 - m0) / (v1 - v0)
    return None


def _q(v, p):
    v = sorted(x for x in v if x is not None)
    return v[min(len(v) - 1, int(p * len(v)))]


def calibrate_floors(con, n_weeks=4000, seed=101, params=FAT, draws=4000):
    """Synthetic null quantiles of the skew-corrected t (`shortvol.skew_t`) (net mean exactly 0 over the bank) for one construction:
    discovery q 0.975 at G 143; holdout q 0.95 and 0.975 at G 129."""
    bank = gen_bank(n_weeks, seed, params)
    nz = noises(bank, seed + 1)
    m0 = _m_for([(m, net_ratio(book(bank, nz, con, m))) for m in (0.95, 1.0, 1.05, 1.10)], 0.0)
    b = book(bank, nz, con, m0)
    rng = random.Random(seed + 2)
    td = [sv.summary([w['r'] for w in draw(b, DISC_WEEKS, rng)])['t'] for _ in range(draws)]
    th = [sv.summary([w['r'] for w in draw(b, HOLD_WEEKS, rng)])['t'] for _ in range(draws)]
    return {'m_null': m0, 'disc_q975': _q(td, 0.975), 'hold_q95': _q(th, 0.95), 'hold_q975': _q(th, 0.975)}


def registered_floors(con, seeds=(101, 202), n_weeks=20000, draws=6000):
    """The registered procedure: the max over two FAT-world calibrations, rounded UP to 0.01."""
    runs = [calibrate_floors(con, n_weeks=n_weeks, seed=s, draws=draws) for s in seeds]
    up = lambda x: math.ceil(x * 100 - 1e-9) / 100
    return {'disc_q975': up(max(r['disc_q975'] for r in runs)), 'hold_q95': up(max(r['hold_q95'] for r in runs)),
            'hold_q975': up(max(r['hold_q975'] for r in runs)), 'runs': runs}


def construction_table(n_weeks=3000, seeds=(11, 31), sims=1000, constructions=CONSTRUCTIONS, floors=None):
    """Per construction: its own calibrated floors (FAT null), then on the BASE and FAT banks, per VRP:
    gross/net/cost per premium, weekly SD per premium, worst synthetic week, pass rates (discovery T1/T2,
    holdout alone, joint); and the false-pass rates where the NET mean is exactly zero."""
    out = {'n_weeks': n_weeks, 'sims': sims, 'banks': {}, 'rows': {}}
    banks = {}
    for name, params, sd in (('base', PARAMS, seeds[0]), ('fat', FAT, seeds[1])):
        bank = gen_bank(n_weeks, sd, params)
        banks[name] = (bank, noises(bank, sd + 1))
        out['banks'][name] = {'seed': sd, 'stress_share': sum(any(w[c]['stress'] for c in sv.COINS) for w in bank) / len(bank),
                              'facts': {c: dispersion_facts(bank, c) for c in sv.COINS}}
    for con in constructions:
        fl = (floors or {}).get(con) or registered_floors(con)
        fset = (fl['disc_q975'], {1: fl['hold_q95'], 2: fl['hold_q975']})
        row = {'floors': fl}
        for name, (bank, nz) in banks.items():
            vals = [(m, book(bank, nz, con, m)) for m in M_GRID]
            g = [(m, gross_ratio(b)) for m, b in vals]
            nr = [(m, net_ratio(b)) for m, b in vals]
            cells = {}
            for v in VRPS:
                m = _m_for(g, v)
                if m is None:
                    cells[str(v)] = None
                    continue
                b = book(bank, nz, con, m)
                prem = statistics.fmean(r['prem'] for r in b)
                cells[str(v)] = {'m': round(m, 4), 'gross_per_prem': round(gross_ratio(b), 4),
                                 'net_per_prem': round(net_ratio(b), 4),
                                 'cost_per_prem': round(sum(r['cost'] for r in b) / sum(r['prem'] for r in b), 4),
                                 'sd_week_per_prem': round(statistics.pstdev(r['r'] for r in b) / prem, 3),
                                 'mean_week_notional': statistics.fmean(r['r'] for r in b),
                                 'sd_week_notional': statistics.pstdev(r['r'] for r in b),
                                 'worst_week_notional': min(r['r'] for r in b),
                                 'prem_notional': prem, **pass_rates(b, sims, floors=fset, n=None)}
            m0 = _m_for(nr, 0.0)
            null = None
            if m0 is not None:
                b0 = book(bank, nz, con, m0)
                null = {'m': round(m0, 4), 'gross_per_prem': round(gross_ratio(b0), 4),
                        **pass_rates(b0, sims, seed=99, floors=fset, n=None),
                        'student_only': pass_rates(b0, sims, seed=99, floors=(0.0, {1: 0.0, 2: 0.0}), n=None)}
            row[name] = {'cells': cells, 'net_zero': null}
        out['rows'][con] = row
    return out


def sizing(n_weeks=3000, seed=11, params=PARAMS, con='H24', vrp=0.10):
    """Tail-aware Kelly on the primary's synthetic net weekly book at the prior VRP, with the adversarial
    out-of-sample atom (2 events in HIST_WEEKS at the mean adversarial book loss)."""
    bank = gen_bank(n_weeks, seed, params)
    nz = noises(bank, seed + 1)
    g = [(m, gross_ratio(book(bank, nz, con, m))) for m in M_GRID]
    b = book(bank, nz, con, _m_for(g, vrp))
    atom = statistics.fmean(sv.ADVERSARIAL_WEEK[e][2] for e in sv.TAIL_OUT_OF_SAMPLE)
    p_atom = len(sv.TAIL_OUT_OF_SAMPLE) / sv.HIST_WEEKS
    fk = kelly(b, p_atom, atom)
    worst = min(v[2] for v in sv.ADVERSARIAL_WEEK.values())
    return {'construction': con, 'vrp': vrp, 'gaussian_kelly': gaussian_kelly(b), 'tail_kelly': fk,
            'quarter_tail_kelly': fk / 4, 'cap_week_25pct': 0.25 / -worst, 'cap_month_35pct': 0.35 / -sv.WORST_MONTH_BOOK,
            'mean_week_notional': statistics.fmean(r['r'] for r in b), 'sd_week_notional': statistics.pstdev(r['r'] for r in b),
            'atom': atom, 'p_atom': p_atom}


def primary_table(con='H24', n_weeks=6000, seeds=(41, 61), sims=2000, sizes=(0.10, 0.25, 0.40, 0.45)):
    """The registered primary at the registered floors, every criterion ON, at each size n (criterion 5
    depends on n): P(discovery pass) T1/T2/any, holdout alone, joint; false-pass at net zero; plus
    a 2x spread-and-hedge-cost sensitivity at VRP 0.10 and 0.15 (n = SIZE_N)."""
    out = {'construction': con, 'floors': {'disc': sv.DISC_T_FLOOR, 'hold': sv.HOLD_T_FLOOR}, 'rows': {}}
    for name, params, sd in (('base', PARAMS, seeds[0]), ('fat', FAT, seeds[1])):
        bank = gen_bank(n_weeks, sd, params)
        nz = noises(bank, sd + 1)
        g = [(m, gross_ratio(book(bank, nz, con, m))) for m in M_GRID]
        nr = [(m, net_ratio(book(bank, nz, con, m))) for m in M_GRID]
        res = {'stress_share': sum(any(w[c]['stress'] for c in sv.COINS) for w in bank) / len(bank)}
        for v in VRPS:
            b = book(bank, nz, con, _m_for(g, v))
            res[str(v)] = {'net_per_prem': round(net_ratio(b), 4), 'mean_week_notional': statistics.fmean(r['r'] for r in b),
                           'annual_at_n': {str(n): statistics.fmean(r['r'] for r in b) * 52 * n for n in sizes},
                           'by_n': {str(n): pass_rates(b, sims, n=n) for n in sizes},
                           'stats_only': pass_rates(b, sims, n=None)}
        for v in (0.10, 0.15):
            b2 = book(bank, nz, con, _m_for(g, v), cost_mult=2.0)
            res[f'{v}_2x_spread_and_hedge_costs'] = {'net_per_prem': round(net_ratio(b2), 4),
                                                       'n_registered': pass_rates(b2, sims, n=sv.SIZE_N)}
        b0 = book(bank, nz, con, _m_for(nr, 0.0))
        res['net_zero'] = {'by_n': {str(n): pass_rates(b0, sims, seed=99, n=n) for n in sizes},
                           'stats_only': pass_rates(b0, sims, seed=99, n=None)}
        out['rows'][name] = res
    return out


# ------------------------------------------------------------------ realistic tail (000m): many atoms
def kelly_atoms(rows, atoms, p_each, grid=None):
    """argmax_f of (1 - k p) E_syn log(1 + f r) + p sum_i log(1 + f a_i): the synthetic weekly book mixed
    with k out-of-calendar stress weeks (`atoms`, realised book returns per notional), each at p_each."""
    grid = grid or [0.01 * i for i in range(1, 300)]
    k = len(atoms)
    best = (0.0, -1e9)
    for f in grid:
        if any(1 + f * a <= 0 for a in atoms) or any(1 + f * r['r'] <= 0 for r in rows):
            break
        g = (1 - k * p_each) * statistics.fmean(math.log(1 + f * r['r']) for r in rows) + \
            p_each * sum(math.log(1 + f * a) for a in atoms)
        if g > best[1]:
            best = (f, g)
    return best[0]


def sizing_realistic(atoms, n_weeks=3000, seeds=(11, 31), con='H24', vrps=(0.05, 0.10, 0.15)):
    """Tail-aware Kelly with the REALISTIC atoms (base and fat worlds, each VRP), plus the synthetic mean
    weekly book return used for the expected %/yr."""
    out = {}
    for name, params, sd in (('base', PARAMS, seeds[0]), ('fat', FAT, seeds[1])):
        bank = gen_bank(n_weeks, sd, params)
        nz = noises(bank, sd + 1)
        g = [(m, gross_ratio(book(bank, nz, con, m))) for m in M_GRID]
        for v in vrps:
            b = book(bank, nz, con, _m_for(g, v))
            fk = kelly_atoms(b, atoms, 1 / sv.HIST_WEEKS)
            out[f'{name}_{v}'] = {'tail_kelly': fk, 'quarter': fk / 4,
                                  'mean_week_notional': statistics.fmean(r['r'] for r in b)}
    return out
