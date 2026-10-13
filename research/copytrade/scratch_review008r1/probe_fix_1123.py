"""2026-11-23 design (136 days, 21 weeks, 5 cells): size and power of the registered rule vs alt+rev loadings per cell (k 16)."""
import datetime as dt, math, random, statistics, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sims = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
sys.argv = [sys.argv[0], '0']
import probe_stats as P  # noqa: E402
c8 = P.c8
days = c8.planned_days(dt.date(2026, 11, 23))
keys = [k for k, _ in days]
cellmap = c8.beta_cells(keys, dict(days))
CELL = [cellmap[k] for k in keys]
CS = sorted(set(CELL))
WK = [c8.week_of(k) for k in keys]
MON = [c8.month_of(k) for k in keys]
WIN = [w for _, w in days]
N = len(keys)
G = len(set(WK))
CRIT = c8.t_quantile(G - 1, 0.95)
DR = {'2026-07': 0.0, '2026-08': P.AUG, '2026-09': P.SEP, '2026-10': 0.0, '2026-11': 0.0}

def one(rng, mu, alt_load, u_drift, per_cell):
    btc = [DR[MON[i]] + rng.gauss(0, 0.025) for i in range(N)]
    u = [u_drift(i) + rng.gauss(0, 0.015) for i in range(N)]
    rev = [rng.gauss(0, 0.008) for _ in range(N)]
    y = [mu + 0.3 * btc[i] + alt_load(i) * u[i] + P.SIG * rng.gauss(0, 1) for i in range(N)]
    alt = [1.1 * b + x for b, x in zip(btc, u)]
    res = {}
    for name, pc in (('registered', False), ('per-cell', True)):
        X = []
        for i in range(N):
            r = [1.0] + [btc[i] if CELL[i] == c else 0.0 for c in CS]
            if pc:
                r += [alt[i] if CELL[i] == c else 0.0 for c in CS] + [rev[i] if CELL[i] == c else 0.0 for c in CS]
            else:
                r += [alt[i], rev[i]]
            X.append(r)
        f = P.fit(y, X, WK)
        a = [f['alpha'] + e for e in f['resid']]
        h = N // 2
        res[name] = (f['alpha'] > 0 and f['t'] >= CRIT and statistics.median(a) > 0
                     and statistics.fmean(a[:h]) > 0 and statistics.fmean(a[h:]) > 0)
    return res

def rate(label, seed, mu=0.0, alt_load=lambda i: 0.0, u_drift=lambda i: 0.0):
    rng = random.Random(seed)
    tot = {'registered': 0, 'per-cell': 0}
    for _ in range(sims):
        for k, v in one(rng, mu, alt_load, u_drift, True).items():
            tot[k] += v
    print(f'{label:<60} ' + '  '.join(f'{k} {v / sims:.3f}' for k, v in tot.items()), flush=True)

print(f'2026-11-23: n {N}, G {G}, cells {CS}, crit {CRIT:.3f}')
rate('base effect 0', 81)
rate('power full', 82, mu=P.FULL)
rate('power half', 83, mu=P.FULL / 2)
rate('alt loading A 0 / B,C 0.5; U drift from Aug +0.2 %/day', 84, alt_load=lambda i: 0.0 if WIN[i] == 0 else 0.5,
     u_drift=lambda i: 0.0 if MON[i] == '2026-07' else 0.002)
