"""Review r1 of 013: an INDEPENDENT re-implementation of the registered decision path (own OLS/CR1, own caps,
own criteria) on synthetic events at the real first-trade times (schedule.json: pre-entry facts only).

Worlds reproduced: one size cell (beta 2, conf drift 2%/wk, edge gone in confirmation) and one power cell
(mu 4%, k 1.5). New worlds: squeeze atoms with a tail-adjusted alpha; regime-dependent beta; combined
squeeze + bear drift through the deploy bar's raw-vs-alpha economics.

Usage: python -I r1_own_sim.py WORLD SIMS
"""
import datetime as dt
import json
import math
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
MIN, HOUR, DAY = 60_000, 3_600_000, 86_400_000
YEAR = 365.25 * DAY


def ms(d):
    return int(dt.datetime.fromisoformat(d).replace(tzinfo=dt.UTC).timestamp() * 1000)


SPAN = {'reported': (ms('2023-12-23') - ms('2020-01-02')) / YEAR, 'discovery': (ms('2025-07-19') - ms('2024-01-01')) / YEAR,
        'confirmation': (ms('2026-06-22') - ms('2025-07-28')) / YEAR}


def sched(cal):
    s = json.loads((HERE / 'data/r013/schedule.json').read_text())[cal]
    return sorted(e['first_trade'] for e in s if e['first_trade'] and not e['premarket'] and not e['continuation'])


def week(t):
    y, w, _ = dt.datetime.fromtimestamp(t / 1000, dt.UTC).isocalendar()
    return f'{y}-{w:02d}'


def month(t):
    return dt.datetime.fromtimestamp(t / 1000, dt.UTC).strftime('%Y-%m')


# ---------------------------------------------------------------- own t quantile (bisection on own incomplete beta via series)
def t_cdf(t, df):
    # numeric integration of the t density (Simpson), adequate to 1e-7 for df >= 3
    c = math.exp(math.lgamma((df + 1) / 2) - math.lgamma(df / 2)) / math.sqrt(df * math.pi)
    f = lambda x: c * (1 + x * x / df) ** (-(df + 1) / 2)
    n = 4000
    h = abs(t) / n
    s = f(0) + f(abs(t)) + sum((4 if i % 2 else 2) * f(i * h) for i in range(1, n))
    half = s * h / 3
    return 0.5 + half if t >= 0 else 0.5 - half


_TC = {}


def t_crit(df, q):
    if (df, q) not in _TC:
        lo, hi = 0.0, 20.0
        for _ in range(60):
            mid = (lo + hi) / 2
            lo, hi = (mid, hi) if t_cdf(mid, df) < q else (lo, mid)
        _TC[(df, q)] = (lo + hi) / 2
    return _TC[(df, q)]


# ---------------------------------------------------------------- own OLS with CR1 (2x2 sandwich written out)
def ols(y, x, cl):
    n = len(y)
    if n < 3:
        return None
    mx, my = sum(x) / n, sum(y) / n
    sxx = sum((a - mx) ** 2 for a in x)
    if sxx <= 0:
        return None
    b = sum((a - mx) * (c - my) for a, c in zip(x, y)) / sxx
    a = my - b * mx
    # influence of the intercept: alpha = sum w_i y_i, w_i = 1/n - mx (x_i - mx)/sxx ; CR1 on cluster sums of w_i e_i
    g = {}
    for yi, xi, c in zip(y, x, cl):
        e = yi - a - b * xi
        w = 1 / n - mx * (xi - mx) / sxx
        g[c] = g.get(c, 0.0) + w * e
    G = len(g)
    if G < 2:
        return None
    v = G / (G - 1) * (n - 1) / (n - 2) * sum(u * u for u in g.values())
    return {'a': a, 'b': b, 't': a / math.sqrt(v) if v > 0 else None, 'G': G, 'n': n, 'se': math.sqrt(v)}


def take(ev):
    out, ts = [], []
    for e in sorted(ev, key=lambda e: e['en']):
        t = e['en']
        if sum(1 for x in ts if t - x < 15 * DAY) < 7 and sum(1 for x in ts if t - x < 39 * DAY) < 19:
            out.append(e)
            ts.append(t)
    return out


# ---------------------------------------------------------------- synthetic world
SIG_E, SIG_M = 0.083, math.sqrt(0.133 ** 2 - 0.083 ** 2)


def tdraw(r, nu=4.5):
    z = r.gauss(0, 1)
    chi = sum(r.gauss(0, 1) ** 2 for _ in range(int(nu))) + (nu - int(nu)) * r.gauss(0, 1) ** 2
    return z / math.sqrt(chi / nu) / math.sqrt(nu / (nu - 2))


def market(t0, t1, drift_fn, r, msd=1.0):
    nd = int((t1 - t0) // DAY) + 3
    lv = [0.0]
    for i in range(nd):
        lv.append(lv[-1] + r.gauss(-drift_fn(t0 + i * DAY) / 7, msd * SIG_M / math.sqrt(7)))

    def at(t):
        u = (t - t0) / DAY
        i = max(0, min(int(u), nd - 1))
        return lv[i] + (u - i) * (lv[i + 1] - lv[i])
    return at


def events(sch, mu, beta_fn, k, drift_fn, r, liq_p=0.0, msd=1.0):
    at = market(sch[0] - DAY, sch[-1] + 10 * DAY, drift_fn, r, msd)
    shift = (mu + liq_p) / (1 - liq_p) if liq_p else mu
    out = []
    for ft in sch:
        en, ex = ft + DAY, ft + 8 * DAY
        m = math.exp(at(ex) - at(en)) - 1
        if liq_p and r.random() < liq_p:
            net, liq = -1.0, True
        else:
            net, liq = max(-1.0, min(1.0, shift - beta_fn(ft, m) * m + k * SIG_E * tdraw(r))), False
        out.append({'net': net, 'm': m, 'w': week(ft), 'en': en, 'ex': ex, 't2': r.random() < 0.7, 'liq': liq})
    return out


def stats(ev, span):
    y, x, cl = [e['net'] for e in ev], [e['m'] for e in ev], [e['w'] for e in ev]
    o = ols(y, x, cl)
    if o is None:
        return None
    adj = [e['net'] - o['b'] * e['m'] for e in ev]
    ws = {}
    for a, c in zip(adj, cl):
        ws[c] = ws.get(c, 0.0) + a
    pos = [(v, c) for c, v in ws.items() if v > 0]
    drop = max(pos)[1] if pos else None
    keep = [e for e in ev if e['w'] != drop]
    o2 = ols([e['net'] for e in keep], [e['m'] for e in keep], [e['w'] for e in keep])
    weeks = sorted(set(cl))
    first = set(weeks[:(len(weeks) + 1) // 2])
    h1 = [a for a, c in zip(adj, cl) if c in first]
    h2 = [a for a, c in zip(adj, cl) if c not in first]
    sa = sorted(adj)
    med = (sa[len(sa) // 2] + sa[(len(sa) - 1) // 2]) / 2
    tk = take(ev)
    return {'o': o, 'wo': o2['a'] if o2 else None, 'med': med, 'h1': sum(h1) / len(h1) if h1 else None,
            'h2': sum(h2) / len(h2) if h2 else None, 'raw': sum(y) / len(y), 'book': sum(0.025 * e['net'] for e in tk) / span,
            'n_taken': len(tk), 'pi': sum(e['liq'] for e in ev) / len(ev),
            'nonliq_mean': (lambda v: sum(v) / len(v) if v else 0.0)([e['net'] for e in ev if not e['liq']])}


def disc_pass(s):
    o = s['o']
    return (o['n'] >= 20 and o['G'] >= 10 and o['a'] > 0 and o['t'] is not None and o['t'] >= max(t_crit(o['G'] - 2, 0.975), 0.0)
            and s['med'] > 0 and s['h1'] > 0 and s['h2'] > 0 and (s['wo'] or 0) > 0 and s['raw'] >= 0.01 and s['book'] >= 0.03)


def conf_pass(s, cal=1.90, alpha_override=None):
    o = s['o']
    a = o['a'] if alpha_override is None else alpha_override
    t = a / o['se'] if o['se'] > 0 else None
    return (o['n'] >= 10 and o['G'] >= 5 and a > 0 and t is not None and t >= max(t_crit(o['G'] - 2, 0.95), cal)
            and (s['wo'] or 0) > 0)


def run(world, sims):
    D, C, R = sched('discovery'), sched('confirmation'), sched('reported')
    r = random.Random(12345)
    tal = {}
    def inc(k, v=1):
        tal[k] = tal.get(k, 0) + v
    for _ in range(sims):
        d = events(D, world['mu_d'], world['beta_d'], world['k'], world['drift_d'], r, world.get('liq_p', 0.0), world.get('msd', 1.0))
        c = events(C, world['mu_c'], world['beta_c'], world['k'], world['drift_c'], r, world.get('liq_p', 0.0), world.get('msd', 1.0))
        rp = events(R, 0.0, lambda ft, m: 1.5, world['k'], lambda t: 0.0, r, world.get('liq_p', 0.0)) if world.get('liq_p') else []
        p = {}
        for tr in ('T1', 'T2'):
            dd = d if tr == 'T1' else [e for e in d if e['t2']]
            p[tr] = disc_pass(stats(dd, SPAN['discovery']))
        carried = 'T1' if p['T1'] else ('T2' if p['T2'] else None)
        sc1 = stats(c, SPAN['confirmation'])
        ok1 = conf_pass(sc1)
        inc('conf_alone_T1', ok1)
        # tail-adjusted alpha (proposed fix): alpha - max(0, pi_pool - pi_conf) * (1 + nonliq mean), same SE
        if world.get('liq_p'):
            pool = d + c + rp
            pi_pool = sum(e['liq'] for e in pool) / len(pool)
            a_t = sc1['o']['a'] - max(0.0, pi_pool - sc1['pi']) * (1 + sc1['nonliq_mean'])
            inc('conf_alone_T1_tailadj', conf_pass(sc1, alpha_override=a_t))
            inc('conf_alone_T1_tailadj_t169', conf_pass(sc1, cal=0.0, alpha_override=a_t))
            inc('conf_no_liq_in_sample', sc1['pi'] == 0)
        if carried:
            inc('disc')
            cc = c if carried == 'T1' else [e for e in c if e['t2']]
            sc = stats(cc, SPAN['confirmation'])
            ok = conf_pass(sc)
            inc('conf_given', ok)
            # deploy-bar economics: raw mean vs alpha (what the book would earn with a flat market)
            if ok:
                inc('confirmed_raw_ge_100bp', sc['raw'] >= 0.01)
                inc('confirmed_alpha_ge_100bp', sc['o']['a'] >= 0.01)
                ev_yr = sc['n_taken'] / SPAN['confirmation']
                inc('confirmed_rawbook_minus_floor_ge_3', sc['book'] - 0.046 >= 0.03)
                inc('confirmed_alphabook_minus_floor_ge_3', ev_yr * 0.025 * sc['o']['a'] - 0.046 >= 0.03)
    out = {k: v / sims for k, v in tal.items() if k not in ('conf_given',) and not k.startswith('confirmed_')}
    out['conf_given'] = tal.get('conf_given', 0) / tal['disc'] if tal.get('disc') else None
    for k, v in tal.items():
        if k.startswith('confirmed_'):
            out[k + '_share_of_confirms'] = v / tal['conf_given'] if tal.get('conf_given') else None
    out['joint'] = tal.get('conf_given', 0) / sims
    return out


CONST = lambda b: (lambda ft, m: b)
DRIFT = lambda d: (lambda t: d)
C0, C1 = ms('2025-07-28'), ms('2026-06-22')
BEAR = (ms('2025-10-06'), ms('2026-02-28'))          # a stylised crash period inside the confirmation calendar


def bear_drift(d):
    return lambda t: d if BEAR[0] <= t < BEAR[1] else 0.0


def bear_beta(b_in, b_out):
    return lambda ft, m: b_in if BEAR[0] <= ft < BEAR[1] else b_out


WORLDS = {
    'size_decayed_b2_cdrift2': dict(mu_d=0.04, mu_c=0.0, beta_d=CONST(2.0), beta_c=CONST(2.0), k=1.5, drift_d=DRIFT(0.0), drift_c=DRIFT(0.02)),
    'null_b15_k1_drift1': dict(mu_d=0.0, mu_c=0.0, beta_d=CONST(1.5), beta_c=CONST(1.5), k=1.0, drift_d=DRIFT(0.01), drift_c=DRIFT(0.01)),
    'power_mu4_k15': dict(mu_d=0.04, mu_c=0.04, beta_d=CONST(1.5), beta_c=CONST(1.5), k=1.5, drift_d=DRIFT(0.0), drift_c=DRIFT(0.0)),
    'squeeze_p2_b15_k15': dict(mu_d=0.0, mu_c=0.0, beta_d=CONST(1.5), beta_c=CONST(1.5), k=1.5, drift_d=DRIFT(0.0), drift_c=DRIFT(0.0), liq_p=0.02),
    'squeeze_p2_power_mu4_k15': dict(mu_d=0.04, mu_c=0.04, beta_d=CONST(1.5), beta_c=CONST(1.5), k=1.5, drift_d=DRIFT(0.0), drift_c=DRIFT(0.0), liq_p=0.02),
    'squeeze_p1_b15_k1': dict(mu_d=0.0, mu_c=0.0, beta_d=CONST(1.5), beta_c=CONST(1.5), k=1.0, drift_d=DRIFT(0.0), drift_c=DRIFT(0.0), liq_p=0.01),
    # regime beta: listings in the crash period carry beta 2.5, others 1.0; drift 3%/wk only in the crash period
    'regime_beta_decayed': dict(mu_d=0.04, mu_c=0.0, beta_d=CONST(1.5), beta_c=bear_beta(2.5, 1.0), k=1.5, drift_d=DRIFT(0.0), drift_c=bear_drift(0.03)),
    'regime_beta_decayed_k1': dict(mu_d=0.04, mu_c=0.0, beta_d=CONST(1.5), beta_c=bear_beta(2.5, 1.0), k=1.0, drift_d=DRIFT(0.0), drift_c=bear_drift(0.03)),
    # downside beta: short earns 2.0x the market's fall, 1.0x its rise (convex), no drift
    'downside_beta_null': dict(mu_d=0.0, mu_c=0.0, beta_d=(lambda ft, m: 2.0 if m < 0 else 1.0), beta_c=(lambda ft, m: 2.0 if m < 0 else 1.0), k=1.5, drift_d=DRIFT(0.0), drift_c=DRIFT(0.0)),
    # decayed edge in a bear confirmation: what the deploy bar's economics are made of after a (false) confirm
    'decayed_b2_cdrift2_alpha3': dict(mu_d=0.04, mu_c=0.03, beta_d=CONST(2.0), beta_c=CONST(2.0), k=1.5, drift_d=DRIFT(0.0), drift_c=DRIFT(0.02)),
    'alpha3_flat': dict(mu_d=0.04, mu_c=0.03, beta_d=CONST(2.0), beta_c=CONST(2.0), k=1.5, drift_d=DRIFT(0.0), drift_c=DRIFT(0.0)),
}

if __name__ == '__main__':
    name, sims = sys.argv[1], int(sys.argv[2])
    print(name, sims, json.dumps({k: (round(v, 4) if isinstance(v, float) else v) for k, v in run(WORLDS[name], sims).items()}), flush=True)
