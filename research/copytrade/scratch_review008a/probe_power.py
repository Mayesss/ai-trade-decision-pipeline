"""Review 008-A probe 1 (synthetic only): an independent re-implementation of the §5 confirm rule and a
reproduction of the §3 power / null cells, then nulls the draft did not try.

    cd research/copytrade && .venv/bin/python -I scratch_review008a/probe_power.py [sims]

No file is read except the code; every number is from random draws.
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
KEYS = c8.planned_keys(dt.date(2026, 9, 28))
N = len(KEYS)
WK = [c8.week_of(k) for k in KEYS]
MONTH = [c8.month_of(k) for k in KEYS]
WIN = [0 if k < c8.ms(dt.date(2026, 8, 3)) else 1 for k in KEYS]
SIG = c8.SIGMA_DAY
FULL = c8.DISC_GROSS_INV
CRIT = c8.t_quantile(12, 0.95)


def my_ols_cr1(y, x, cl):
    """Independent OLS + CR1 (written from the textbook sandwich, not from crowd008)."""
    n = len(y)
    mx, my = sum(x) / n, sum(y) / n
    sxx = sum((a - mx) ** 2 for a in x)
    b = sum((a - mx) * (c - my) for a, c in zip(x, y)) / sxx
    a = my - b * mx
    # (X'X)^-1 for X = [1, x]
    s0, s1, s2 = n, sum(x), sum(v * v for v in x)
    det = s0 * s2 - s1 * s1
    i00, i01, i11 = s2 / det, -s1 / det, s0 / det
    g = {}
    for yi, xi, c in zip(y, x, cl):
        e = yi - a - b * xi
        u = g.setdefault(c, [0.0, 0.0])
        u[0] += e
        u[1] += e * xi
    G = len(g)
    # alpha row of (X'X)^-1 : (i00, i01); var(alpha) = r' M r
    va = sum((i00 * u0 + i01 * u1) ** 2 for u0, u1 in g.values())
    va *= G / (G - 1) * (n - 1) / (n - 2)
    return a, b, a / math.sqrt(va), G


def my_confirm(y, m):
    a, b, t, G = my_ols_cr1(y, m, WK)
    adj = [yi - b * mi for yi, mi in zip(y, m)]
    h = len(adj) // 2
    return (a > 0 and t >= CRIT and statistics.median(adj) > 0
            and statistics.fmean(adj[:h]) > 0 and statistics.fmean(adj[h:]) > 0), t


def run(name, gen, sims=SIMS, seed=1):
    rng = random.Random(seed)
    hit = hit_c1 = raw = 0
    for _ in range(sims):
        y, m = gen(rng)
        ok, num = c8.confirm(y, m, KEYS)
        ok2, t2 = my_confirm(y, m)
        assert ok == ok2 and abs(num['t_alpha'] - t2) < 1e-9, 'independent implementation disagrees'
        hit += ok
        hit_c1 += num['criteria']['C1']
        mean, _, t, _, g = c8.cluster_mean_t(y, WK)
        h = N // 2
        raw += (t is not None and t >= CRIT and statistics.median(y) > 0
                and statistics.fmean(y[:h]) > 0 and statistics.fmean(y[h:]) > 0)
    print(f'{name:<78} confirm {hit / sims:.3f}  C1-only {hit_c1 / sims:.3f}  raw-rule {raw / sims:.3f}')
    return hit / sims


def gauss_noise(rng, sd=SIG):
    return [rng.gauss(0, sd) for _ in range(N)]


def t_noise(rng, df=3, sd=SIG):
    s = math.sqrt(df / (df - 2))
    return [rng.gauss(0, 1) / math.sqrt(sum(rng.gauss(0, 1) ** 2 for _ in range(df)) / df) / s * sd for _ in range(N)]


def btc_iid(rng, drift=0.0, sd=0.025):
    return [rng.gauss(drift, sd) for _ in range(N)]


def btc_iid_list(rng, drifts, sd=0.025):
    return [rng.gauss(d, sd) for d in drifts]


def btc_garch_t(rng, drifts, sd=0.025, df=4, a=0.10, b=0.85):
    """GARCH(1,1)-like vol clustering with t4 shocks; drift per key given as a list."""
    s = math.sqrt(df / (df - 2))
    var, out = sd * sd, []
    w = sd * sd * (1 - a - b)
    for d in drifts:
        z = rng.gauss(0, 1) / math.sqrt(sum(rng.gauss(0, 1) ** 2 for _ in range(df)) / df) / s
        e = math.sqrt(var) * z
        out.append(d + e)
        var = w + a * e * e + b * var
    return out


# BTC drift scenarios per key. 001 §2 states Aug +24 %, Sep +8 %; July is NOT known to this reviewer and is varied.
def drift_path(july):
    per = {'2026-07': july, '2026-08': math.log(1.24) / 31, '2026-09': math.log(1.08) / 30}
    return [per[mo] for mo in MONTH]


if __name__ == '__main__':
    print(f'{N} days, {len(set(WK))} clusters, crit {CRIT:.4f}, sigma {SIG:.5f}, full {FULL * 1e4:.2f} bp\n')
    print('-- reproduction of §3 (iid Gaussian, beta 0)')
    run('null', lambda r: (gauss_noise(r), btc_iid(r)))
    run('half effect', lambda r: ([FULL / 2 + e for e in gauss_noise(r)], btc_iid(r)))
    run('full effect', lambda r: ([FULL + e for e in gauss_noise(r)], btc_iid(r)))
    run('null, beta +0.4, BTC +0.25 %/day', lambda r: (lambda m: ([0.4 * x + e for x, e in zip(m, gauss_noise(r))], m))(btc_iid(r, 0.0025)))

    print('\n-- realistic BTC: GARCH + t4 shocks, Aug/Sep drift from 001 §2, July varied; constant beta')
    for july in (-0.003, 0.0, 0.003):
        dp = drift_path(july)
        run(f'null, beta +0.3 constant, July drift {july * 100:+.1f} %/day',
            lambda r, dp=dp: (lambda m: ([0.3 * x + e for x, e in zip(m, t_noise(r))], m))(btc_garch_t(r, dp)))

    print('\n-- residual vol scales with BTC vol (alts move more on big BTC days), t3 residual, beta +0.3')
    def het(r, dp=drift_path(0.0)):
        m = btc_garch_t(r, dp)
        e = t_noise(r)
        y = [0.3 * x + ei * (0.5 + 0.5 * abs(x - d) / 0.02) for x, ei, d in zip(m, e, dp)]
        return y, m
    run('null, heteroskedastic in |BTC|', het)

    print('\n-- the beta differs between the two cohorts (window A: S 2026-06-08; window B: S 2026-08-03)')
    for bA, bB in ((0.0, 0.3), (0.0, 0.5), (0.3, 0.0), (-0.2, 0.3)):
        for july in (-0.003, 0.0):
            dp = drift_path(july)
            run(f'null, beta A {bA:+.1f} / B {bB:+.1f}, July drift {july * 100:+.1f} %/day',
                lambda r, dp=dp, bA=bA, bB=bB: (lambda m: ([(bA if w == 0 else bB) * x + e
                                                           for x, e, w in zip(m, gauss_noise(r), WIN)], m))(btc_iid_list(r, dp)))
    print('\n-- the beta drifts with the cohort leverage, which chases BTC (beta_t = 0.2 + 3 x trailing 7-day BTC return)')
    for july in (-0.003, 0.0):
        dp = drift_path(july)
        def chase(r, dp=dp):
            m = btc_iid_list(r, dp)
            y = []
            for i, x in enumerate(m):
                tr = sum(m[max(0, i - 7):i])
                y.append((0.2 + 3 * tr) * x + r.gauss(0, SIG))
            return y, m
        run(f'null, beta chases trailing BTC, July drift {july * 100:+.1f} %/day', chase)
