"""SYNTHETIC. Hansen-Hodrick-style fix: week-cluster scores plus the FULL lag-1 cross product of consecutive
ISO weeks (weight 1, the MA(1) overlap of 7-day holds)."""
import random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from p2_overlap import S, tdraw, market_increments, L
from p2b_remedies import dk_t

def run(cal, q, sm, se, sims=4000, seed=1, mu=0.0):
    ft = S[cal]; r = random.Random(seed); wk = [L.iso_week(t) for t in ft]
    ent = [t + L.ENTRY_DELAY for t in ft]; ext = [t + L.HOLD for t in ent]
    h = {'cr1_week': 0, 'hh_lag1_w1': 0, 'neg_var': 0}
    for _ in range(sims):
        M = market_increments(ent + ext, sm, r)
        net = [mu - (M[b] - M[a]) + se * tdraw(r) for a, b in zip(ent, ext)]
        m, t, G = L.clustered_t(net, wk); h['cr1_week'] += m > 0 and t >= L.t_crit(G - 1, q)
        try:
            m, t, G = dk_t(net, wk, lag_w=1.0); h['hh_lag1_w1'] += m > 0 and t >= L.t_crit(G - 1, q)
        except ValueError:
            h['neg_var'] += 1
    return {k: v / sims for k, v in h.items()}

for cal, q in (('discovery', L.Q_DISCOVERY), ('confirmation', L.Q_CONFIRM)):
    print(cal, q, 'null', run(cal, q, 0.104, 0.083), flush=True)
    print(cal, q, 'mu=4% (power)', run(cal, q, 0.104, 0.083, mu=0.04, sims=2000), flush=True)
