"""Probe 5 (SYNTHETIC only): the regime-VRP null of probe 4 at two strengths; the false-pass rate at
the registered discovery floor 2.59 and the 0.975 quantile of the skew-corrected t on contiguous
143-week windows (what the floor would have to be to hold 2.5 % under this null)."""
import statistics, sys
sys.path.insert(0, '.'); sys.path.insert(0, 'scratch_review014a')
from ct import shortvol as sv
import p4_own_sim as P
W = P.world(143000, 2024)
mA = 1.0515                     # probe 4's Null-A multiplier (same world, same seed)
for off in (0.04, 0.08):
    mc = mA + off
    ms = P.solve(lambda m: statistics.fmean(r for r, _ in P.series(W, mc, m)), 0.3, 1.4)
    B = [r for r, _ in P.series(W, mc, ms)]
    ts = sorted(sv.summary(B[i:i + 143])['t'] for i in range(0, len(B) - 142, 143))
    hold = sorted(sv.summary(B[i:i + 129])['t'] for i in range(0, len(B) - 128, 129))
    print('offset', off, 'm_calm', round(mc, 3), 'm_stress', round(ms, 3), 'rates', P.rates(B),
          'disc q975 of skew t', round(ts[int(0.975 * len(ts))], 2), 'hold q95', round(hold[int(0.95 * len(hold))], 2),
          'hold q975', round(hold[int(0.975 * len(hold))], 2), flush=True)
