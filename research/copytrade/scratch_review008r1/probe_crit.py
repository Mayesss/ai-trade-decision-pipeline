"""What critical value would hold 0.05 under month-scale persistence (N8 / N9), and what it costs in power."""
import random, statistics, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.argv = [sys.argv[0], '0']
import probe_stats as P  # noqa: E402

def tvals(sims, seed, **kw):
    rng = random.Random(seed)
    out = []
    h = P.N // 2
    for _ in range(sims):
        y, fac = P.draw(rng, **kw)
        f = P.fit(y, P.design(fac), P.WK)
        a = [f['alpha'] + e for e in f['resid']]
        ok23 = statistics.median(a) > 0 and statistics.fmean(a[:h]) > 0 and statistics.fmean(a[h:]) > 0
        out.append(f['t'] if (f['alpha'] > 0 and ok23) else -9)
    return out

S = 8000
n8 = tvals(S, 61, slow=0.05, slow_rho=0.9)
n9 = tvals(S, 62, slow=0.03, slow_rho=0.97)
full = tvals(S, 63, mu=P.FULL)
half = tvals(S, 64, mu=P.FULL / 2)
base = tvals(S, 65)
for c in (P.CRIT, 2.0, 2.1, 2.18, 2.25):
    r = lambda xs: sum(t >= c for t in xs) / S
    print(f'crit {c:.3f}: base {r(base):.3f}  N8 {r(n8):.3f}  N9 {r(n9):.3f}  power full {r(full):.3f}  half {r(half):.3f}')
