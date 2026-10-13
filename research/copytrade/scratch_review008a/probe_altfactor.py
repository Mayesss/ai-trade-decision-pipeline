"""Review 008-A probe 5 (synthetic only): an alt-market factor the BTC regression omits. The book has
no positioning alpha but a constant loading g on an alt-minus-BTC factor that trends (alt season).
Compares the registered C1 (BTC only) with C1 that also regresses on an equal-weight alt index."""
import datetime as dt
import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ct import crowd008 as c8  # noqa: E402
from scratch_review008a.probe_fix import ols_cr1, WK, MONTH, KEYS, N, SIG, CRIT  # noqa: E402

def run(g, alt_drift, sims=3000, seed=13):
    rng = random.Random(seed)
    h1 = h2 = 0
    for _ in range(sims):
        m = [rng.gauss(0.004, 0.025) for _ in range(N)]                   # BTC, rally drift
        a_perp = [rng.gauss(alt_drift.get(mo, 0.0), 0.015) for mo in MONTH]   # alt-minus-BTC factor
        alt = [1.2 * x + z for x, z in zip(m, a_perp)]                    # EW alt index the run could compute
        y = [0.2 * x + g * z + rng.gauss(0, SIG) for x, z in zip(m, a_perp)]
        ok, _ = c8.confirm(y, m, KEYS)
        h1 += ok
        b, t = ols_cr1(y, [[1.0, x, a] for x, a in zip(m, alt)], WK)
        h2 += b[0] > 0 and t >= CRIT
    return h1 / sims, h2 / sims

for g, ad, name in ((0.3, {'2026-08': 0.003}, 'loading 0.3, alt season +0.3 %/day in Aug only'),
                    (0.3, {'2026-07': 0.002, '2026-08': 0.002, '2026-09': 0.002}, 'loading 0.3, alts +0.2 %/day throughout'),
                    (0.0, {'2026-08': 0.003}, 'no loading (control)')):
    r1, r2 = run(g, ad)
    print(f'null, {name:<52} registered C1-C3 {r1:.3f}   C1 with BTC + EW-alt regressors {r2:.3f}')
