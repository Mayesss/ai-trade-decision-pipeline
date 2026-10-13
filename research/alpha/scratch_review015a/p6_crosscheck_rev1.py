"""Review A, Revision 1 recheck, probe 6: my estimator (common.stats) against the revised r015_core on synthetic draws
(z, its t, the existence gate, the T+1 bar's ingredients). Dates only from calendar.json; no real return is read.

    python3 -I research/alpha/scratch_review015a/p6_crosscheck_rev1.py
"""
import math
import sys

import common as K

sys.path.append(str(K.ALPHA))
import r015_core as C  # noqa: E402

dates, rows = K.load()
cal = C.build_calendar(dates)
assert [c['iT'] for c in cal] == [r['iT'] for r in rows]
assert all(c['vol'] == list(range(r['iT'] - 28, r['iT'] - 8)) for c, r in zip(cal, rows))
rng = K.rng_for(5)
worst = 0.0
for k in range(200):
    x, sig = K.gen(len(dates), rng, 't4', (0.12, 0.87))
    if k % 2:
        K.inject(x, sig, rows, 0.5)
    mine = K.stats(x, rows)
    z, y, s, mu = C.z_series(x, cal)
    ex = C.existence(z)
    worst = max(worst, abs(ex['t'] - mine['gate_z']) if mine['gate_z'] > -9 else 0.0)
    assert (mine['gate_z'] >= 1.92) == ex['exists'], (k, mine['gate_z'], ex)
    econ = C.economic(y, C.v_series(x, cal), cal, mu)
    my_net = 0.5 * mine['mean'] - 2 * mu[1] - 1.6e-4
    assert abs(econ['net_shrunk_month'] - my_net) < 1e-12, (econ, my_net)
print(f'200 draws: existence decisions identical; max |t(z) difference| {worst:.2e}; T+1 bar net identical; crit {C.crit(149)}')
