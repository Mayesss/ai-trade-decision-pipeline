"""Review 008-A probe 3 (synthetic only): size of the registered rule when the book's residual has a slow,
month-scale persistent component that week clusters cannot absorb, and the economic bar's conditional
pass rate given a confirm.

    cd research/copytrade && .venv/bin/python -I scratch_review008a/probe_persist.py [sims]
"""
import datetime as dt
import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ct import crowd008 as c8  # noqa: E402

SIMS = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
KEYS = c8.planned_keys(dt.date(2026, 9, 28))
N = len(KEYS)
SIG = c8.SIGMA_DAY
FULL = c8.DISC_GROSS_INV


def slow_noise(rng, share, rho):
    """Daily noise of total SD SIG: (1 - share) of the variance iid, `share` in an AR(1) with coefficient rho."""
    sf, ss = SIG * math.sqrt(1 - share), SIG * math.sqrt(share)
    s, out = rng.gauss(0, ss), []
    for _ in range(N):
        s = rho * s + math.sqrt(1 - rho * rho) * rng.gauss(0, ss)
        out.append(s + rng.gauss(0, sf))
    return out


def rate(mu, share, rho, sims=SIMS, seed=5):
    rng = random.Random(seed)
    hit = bar = 0
    for _ in range(sims):
        m = [rng.gauss(0, 0.025) for _ in range(N)]
        y = [mu + e for e in slow_noise(rng, share, rho)]
        ok, _ = c8.confirm(y, m, KEYS)
        hit += ok
        if ok:
            bar += c8.economic([v - c8.COST_DAY for v in y], m, KEYS)['meets_bar']
    return hit / sims, (bar / hit if hit else float('nan'))


if __name__ == '__main__':
    for share, rho in ((0.0, 0.0), (0.02, 0.9), (0.05, 0.9), (0.03, 0.97)):
        lrv = (1 - share) + share * (1 + rho) / (1 - rho)
        r0, _ = rate(0.0, share, rho)
        print(f'null, slow share {share:.2f}, rho {rho:.2f} (long-run var x{lrv:.2f}): confirm {r0:.3f}')
    for mu, name in ((FULL, 'full'), (FULL / 2, 'half')):
        r, b = rate(mu, 0.0, 0.0)
        print(f'{name} effect, iid: confirm {r:.3f}; P(economic bar | confirm) {b:.3f}')
