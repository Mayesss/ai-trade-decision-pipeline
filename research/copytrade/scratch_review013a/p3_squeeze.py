"""SYNTHETIC. Size of the one-sided week-clustered t when the null mean-zero event distribution has a squeeze atom:
-1 (whole margin) with probability p, else a + sigma*t4.5 with a = p/(1-p) (mean exactly 0). Optional common market
(continuous, overlap present). Also: an atom clustered in 'squeeze weeks' (all events of a week liquidate w.p. pw)."""
import random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from p2_overlap import S, tdraw, market_increments, L

def run(cal, q, p, se, sm=0.0, week_atom=False, sims=4000, seed=11):
    ft = S[cal]; r = random.Random(seed); wk = [L.iso_week(t) for t in ft]
    ent = [t + L.ENTRY_DELAY for t in ft]; ext = [t + L.HOLD for t in ent]
    a = p / (1 - p); hits = 0; zero_liq_hits = 0; zero_liq = 0
    for _ in range(sims):
        M = market_increments(ent + ext, sm, r) if sm else None
        wl = {w: r.random() < p for w in set(wk)} if week_atom else None
        net, nl = [], 0
        for i in range(len(ft)):
            liq = wl[wk[i]] if week_atom else (r.random() < p)
            if liq:
                net.append(-1.0); nl += 1
            else:
                c = -(M[ext[i]] - M[ent[i]]) if sm else 0.0
                net.append(a + c + se * tdraw(r))
        m, t, G = L.clustered_t(net, wk)
        h = m > 0 and t is not None and t >= L.t_crit(G - 1, q)
        hits += h
        if nl == 0:
            zero_liq += 1; zero_liq_hits += h
    return round(hits / sims, 4), round(zero_liq / sims, 3)

if __name__ == '__main__':
    for cal, q in (('discovery', L.Q_DISCOVERY), ('confirmation', L.Q_CONFIRM)):
        for se in (0.083, 0.166):
            for p in (0.0, 0.01, 0.02, 0.04):
                print(cal, q, f'sigma_e {se} p_liq {p}: size, P(no liquidation)', run(cal, q, p, se),
                      '| +market 10.4%:', run(cal, q, p, se, sm=0.104), flush=True)
        print(cal, q, 'week-clustered squeeze, p_week 0.03, sigma_e 0.166', run(cal, q, 0.03, 0.166, week_atom=True))
