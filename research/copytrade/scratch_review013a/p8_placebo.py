"""SYNTHETIC. Can the registered placebo (listing-week sign flips, listings013.placebo) reveal the overlap
mis-calibration? True sampling SD of the CR1 t under the overlap null is ~1.16 (p2); what does the placebo report?"""
import random, statistics, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from p2_overlap import S, tdraw, market_increments, L
r = random.Random(5); ft = S['confirmation']; wk = [L.iso_week(t) for t in ft]
ent = [t + L.ENTRY_DELAY for t in ft]; ext = [t + L.HOLD for t in ent]
sds, shares = [], []
for _ in range(30):
    M = market_increments(ent + ext, 0.104, r)
    rows = [{'id': str(i), 'week': wk[i], 'net': -(M[b] - M[a]) + 0.083 * tdraw(r), 'fees': 0.0, 'slip': 0.0}
            for i, (a, b) in enumerate(zip(ent, ext))]
    p = L.placebo(rows, L.t_crit(38, L.Q_CONFIRM))
    sds.append(p['gross_t_sd']); shares.append(p['abs_t_share_gross'])
print('placebo gross_t_sd over 30 synthetic overlap samples: mean', round(statistics.mean(sds), 3),
      '| |t|>=crit share mean', round(statistics.mean(shares), 3), '(nominal two-sided 0.10)')
