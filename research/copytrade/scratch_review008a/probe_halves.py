"""Review 008-A probe 4 (synthetic only): what C2 (median) and C3 (halves) cost when the effect is not
uniform in time — e.g. present only under window B's cohort, or only in window A's."""
import datetime as dt
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ct import crowd008 as c8  # noqa: E402

KEYS = c8.planned_keys(dt.date(2026, 9, 28))
N = len(KEYS)
WIN = [0 if k < c8.ms(dt.date(2026, 8, 3)) else 1 for k in KEYS]
nA, nB = WIN.count(0), WIN.count(1)
FULL, SIG = c8.DISC_GROSS_INV, c8.SIGMA_DAY
for name, eff in (('uniform full', [FULL] * N),
                  ('window B only (same 83-day mean)', [0 if w == 0 else FULL * N / nB for w in WIN]),
                  ('window A only (same 83-day mean)', [FULL * N / nA if w == 0 else 0 for w in WIN])):
    rng = random.Random(7)
    hit = c1 = 0
    for _ in range(4000):
        m = [rng.gauss(0, 0.025) for _ in range(N)]
        y = [e + rng.gauss(0, SIG) for e in eff]
        ok, num = c8.confirm(y, m, KEYS)
        hit += ok
        c1 += num['criteria']['C1']
    print(f'{name:<36} C1 only {c1 / 4000:.3f}   C1+C2+C3 {hit / 4000:.3f}')
