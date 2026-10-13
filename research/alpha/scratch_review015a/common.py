"""Review A of 015: shared helpers for the synthetic probes (written independently of r015_core; dates only from
calendar.json; no return value of any real file is read)."""
import collections
import datetime as dt
import json
import math
import random
from pathlib import Path

HERE = Path(__file__).resolve().parent
ALPHA = HERE.parent
SD = 0.0112                      # the one ledgered real number (000s)
EFF_P, EFF_R = -0.0037, 0.0048   # Etula abnormal, T+3 windows
T2, T1 = dt.date(2017, 9, 5), dt.date(2024, 5, 28)
PUB = dt.date(2020, 1, 1)        # RFS publication (working paper from 2015)

# rounded public daily S&P moves, used only as shapes (same numbers as r015_power, which cites them)
SHOCK_2020 = [-0.034, -0.044, -0.004, 0.046, -0.028, 0.042, -0.034, -0.076, 0.049, -0.049, -0.095, 0.093, -0.120,
              0.060, -0.052, 0.047, -0.043, -0.029, 0.094, 0.012, 0.062, -0.016, 0.034, -0.016]
SHOCK_2025 = [-0.048, -0.060, -0.002, -0.016, 0.095, -0.035, 0.018, -0.002, 0.004, -0.022]
SHOCK_2008 = [-0.004, -0.040, -0.014, -0.039, -0.057, -0.011, -0.076, -0.012, 0.116, -0.005, -0.090, 0.043, -0.006,
              0.048, -0.031, -0.061, 0.013, -0.035, -0.032, 0.108, -0.011, 0.026, 0.015]
SHOCKS = {'2020': SHOCK_2020, '2025': SHOCK_2025, '2008': SHOCK_2008}


def lag(d):
    return 3 if d < T2 else 2 if d < T1 else 1


def load():
    j = json.loads((ALPHA / 'data/r015/calendar.json').read_text())
    dates = [dt.date.fromisoformat(d) for d in j['dates']]
    by_m = collections.OrderedDict()
    for i, d in enumerate(dates):
        by_m.setdefault((d.year, d.month), []).append(i)
    rows = []
    for (y, m), idx in by_m.items():
        if (y, m) > (2026, 5) or (y, m) < (2014, 1):
            continue
        iT = idx[-1]
        s = [s for s in (3, 2, 1) if lag(dates[iT - s - 1]) == s][0]
        rows.append({'month': (y, m), 'T': dates[iT], 'iT': iT, 's': s, 'fri': dates[iT].weekday() == 4,
                     'P': list(range(iT - 8, iT - s)), 'R': list(range(iT - s, iT + 4)),
                     'P3': list(range(iT - 8, iT - 3)), 'R3': list(range(iT - 3, iT + 4)),
                     'TOM': list(range(iT, iT + 4)),
                     'out': [i for i in idx[3:] if i < iT - 8],
                     'pre': list(range(max(0, iT - 28), iT - 8))})       # 20 trading days ending T-9 (known at the open)
    return dates, rows


def tq(df, q=0.95):
    """Student-t quantile by bisection on a simple series CDF (df > 30: Cornish-Fisher is enough here)."""
    z = 1.6448536269514722 if q == 0.95 else None
    g1 = (z ** 3 + z) / 4
    g2 = (5 * z ** 5 + 16 * z ** 3 + 3 * z) / 96
    return z + g1 / df + g2 / df ** 2


def mean_t(v):
    n = len(v)
    m = sum(v) / n
    var = sum((a - m) ** 2 for a in v) / (n - 1)
    return m, math.sqrt(var), (m / math.sqrt(var / n) if var > 0 else 0.0)


def quantile(v, q):
    s = sorted(v)
    k = (len(s) - 1) * q
    lo = int(math.floor(k))
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def innov(rng, kind):
    if kind == 'normal':
        return rng.gauss(0.0, 1.0)
    nu = {'t3': 3, 't4': 4}[kind]
    z = rng.gauss(0.0, 1.0) / math.sqrt(rng.gammavariate(nu / 2.0, 2.0) / nu)
    return z / math.sqrt(nu / (nu - 2.0))


def gen(n, rng, kind='normal', garch=None, gjr=None, ar=0.0):
    """Daily zero-mean returns with unconditional SD = SD. Returns (x, sigma_t)."""
    x, sig = [0.0] * n, [SD] * n
    if garch:
        a, b = garch
        om = SD * SD * (1 - a - b)
        h, e = SD * SD, 0.0
        for i in range(n):
            h = om + a * e * e + b * h
            e = math.sqrt(h) * innov(rng, kind)
            x[i], sig[i] = e, math.sqrt(h)
    elif gjr:
        a, g, b = gjr                  # h = om + (a + g*1[e<0]) e^2 + b h
        om = SD * SD * (1 - a - g / 2 - b)
        h, e = SD * SD, 0.0
        for i in range(n):
            h = om + (a + (g if e < 0 else 0.0)) * e * e + b * h
            e = math.sqrt(h) * innov(rng, kind)
            x[i], sig[i] = e, math.sqrt(h)
    else:
        for i in range(n):
            x[i] = SD * innov(rng, kind)
    if ar:
        k = math.sqrt(1 - ar * ar)
        prev = 0.0
        for i in range(n):
            prev = ar * prev + k * x[i]
            x[i] = prev
    return x, sig


def place(x, rows, shape, how, rng, n):
    L = len(shape)
    if how == 'anywhere':
        start = rng.randrange(0, n - L)
    elif how == 'aligned':        # the draft's rule: worst two-day leg ends on a random month's last P day
        pairs = [shape[i] + shape[i + 1] for i in range(L - 1)]
        j = pairs.index(min(pairs)) + 1
        r = rng.choice(rows)
        start = min(max(0, r['P'][-1] - j), n - L)
    elif isinstance(how, tuple) and how[0] == 'at':
        start = how[1]
    else:
        raise ValueError(how)
    x[start:start + L] = shape
    return start


def stats(x, rows, crit=1.91, pre_floor=0.004):
    """Every statistic the probes compare, on one synthetic history."""
    acc = {}
    for r in rows:
        a = acc.setdefault(r['s'], [0.0, 0])
        for i in r['out']:
            a[0] += x[i]
            a[1] += 1
    mu = {s: t / n for s, (t, n) in acc.items()}
    y, v, z, lo = [], [], [], []
    for r in rows:
        m = mu[r['s']]
        sr = sum(x[i] for i in r['R'])
        sp = sum(x[i] for i in r['P'])
        vv = sr - sp
        yy = vv - (len(r['R']) - len(r['P'])) * m
        y.append(yy)
        v.append(vv)
        pre = r['pre']
        sh = math.sqrt(sum(x[i] * x[i] for i in pre) / len(pre))
        z.append(yy / (max(sh, pre_floor) * math.sqrt(12)))
        lo.append(sr - len(r['R']) * m)
    out = {}
    m, sd, t = mean_t(y)
    wo = sorted(y)[:-1]
    out['t'] = t
    out['mean'] = m
    out['ok_wo'] = sum(wo) / len(wo) > 0
    out['gate'] = t if (m > 0 and out['ok_wo']) else -9.0
    post = [a for a, r in zip(y, rows) if r['s'] <= 2]
    out['post_mean'] = sum(post) / len(post)
    out['post_t'] = mean_t(post)[2]
    t1 = [a for a, r in zip(y, rows) if r['s'] == 1]
    out['t1_mean'] = sum(t1) / len(t1)
    mz, _, tz = mean_t(z)
    wz = sorted(z)[:-1]
    out['gate_z'] = tz if (mz > 0 and sum(wz) / len(wz) > 0) else -9.0
    # trimmed: drop the two largest |y| months
    keep = sorted(range(len(y)), key=lambda k: abs(y[k]))[:-2]
    mt, _, tt = mean_t([y[k] for k in keep])
    out['gate_trim2'] = tt if (mt > 0 and m > 0) else -9.0
    out['v_mean'] = sum(v) / len(v)
    out['v_sd'] = mean_t(v)[1]
    out['lo_t'] = mean_t(lo)[2]
    out['maxabs'] = max(abs(a) for a in y)
    return out


def inject(x, sig, rows, k, mode='shifted', prop_vol=False):
    for r in rows:
        if mode == 'decayed_t3' and r['s'] != 3:
            continue
        if mode == 'died_pub' and r['T'] >= PUB:
            continue
        kk = k
        if mode == 'linear_decay':
            frac = rows.index(r) / (len(rows) - 1)
            kk = k * (1 - frac)
        if mode == 'tom_only':
            for i in r['TOM']:
                x[i] += k * 0.0023 / 4 * ((sig[i] / SD) if prop_vol else 1.0)
            continue
        pk, rk = ('P3', 'R3') if mode == 'unshifted' else ('P', 'R')
        ap, ar = kk * EFF_P / len(r[pk]), kk * EFF_R / len(r[rk])
        for i in r[pk]:
            x[i] += ap * ((sig[i] / SD) if prop_vol else 1.0)
        for i in r[rk]:
            x[i] += ar * ((sig[i] / SD) if prop_vol else 1.0)


def rng_for(seed):
    return random.Random(seed)
