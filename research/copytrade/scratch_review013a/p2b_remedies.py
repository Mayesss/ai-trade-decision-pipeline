"""SYNTHETIC. Candidate fixes for the overlap: (i) 2-week block clusters, (ii) week-cluster scores with a
Newey-West lag-1 (Bartlett 1/2, consecutive ISO weeks only) — i.e. Driscoll-Kraay on listing weeks."""
import datetime as dt, math, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from p2_overlap import S, tdraw, market_increments, L

def monday(w):
    y, n = w.split('-W'); return dt.date.fromisocalendar(int(y), int(n), 1)

def dk_t(v, wk, lag_w=0.5):
    n = len(v); m = sum(v) / n; g = {}
    for x, w in zip(v, wk): g[w] = g.get(w, 0.0) + x - m
    ws = sorted(g, key=monday); G = len(ws)
    s = sum(x * x for x in g.values())
    for a, b in zip(ws, ws[1:]):
        if (monday(b) - monday(a)).days == 7: s += 2 * lag_w * g[a] * g[b]
    var = G / (G - 1) * s / n / n
    return m, m / math.sqrt(var), G

def run(cal, q, sm, se, sims=4000, seed=1):
    ft = S[cal]; r = random.Random(seed)
    wk = [L.iso_week(t) for t in ft]
    allw = sorted(set(wk), key=monday); base = monday(allw[0])
    blk = [str(((monday(w) - base).days // 7) // 2) for w in wk]
    ent = [t + L.ENTRY_DELAY for t in ft]; ext = [t + L.HOLD for t in ent]
    h = {'cr1_week': 0, 'cr1_2week_block': 0, 'dk_lag1': 0}
    for _ in range(sims):
        M = market_increments(ent + ext, sm, r)
        net = [-(M[b] - M[a]) + se * tdraw(r) for a, b in zip(ent, ext)]
        m, t, G = L.clustered_t(net, wk); h['cr1_week'] += m > 0 and t >= L.t_crit(G - 1, q)
        m, t, G = L.clustered_t(net, blk); h['cr1_2week_block'] += m > 0 and t >= L.t_crit(G - 1, q)
        m, t, G = dk_t(net, wk); h['dk_lag1'] += m > 0 and t >= L.t_crit(G - 1, q)
    return {k: v / sims for k, v in h.items()}

for cal, q in (('discovery', L.Q_DISCOVERY), ('confirmation', L.Q_CONFIRM)):
    print(cal, q, run(cal, q, 0.104, 0.083), flush=True)
