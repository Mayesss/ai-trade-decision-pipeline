"""SYNTHETIC. Beta worlds on the real event times (continuous Brownian market, exact entry/exit sampling).
net_i = mu + beta*u_i + e_i, u_i = -(market 7-day log move over the event's own hold) + drift (per week), e ~ t4.5.
Market-adjusted as registered: adj_i = net_i - u_i (beta 1). Alternative: pooled OLS alpha with estimated beta,
week-clustered (CR1) intercept t.  Rates: raw t >= crit (the confirmation test), adjusted t >= crit (criterion 5),
OLS-alpha t >= crit."""
import math, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from p2_overlap import S, tdraw, market_increments, L

def ols_alpha_t(y, x, cl):
    n = len(y); mx = sum(x) / n; my = sum(y) / n
    sxx = sum((a - mx) ** 2 for a in x); b = sum((a - mx) * (c - my) for a, c in zip(x, y)) / sxx
    a0 = my - b * mx
    res = [c - a0 - b * a for a, c in zip(x, y)]
    # sandwich for (alpha, beta): bread = (X'X)^-1
    S00, S01, S11 = n, sum(x), sum(a * a for a in x)
    det = S00 * S11 - S01 * S01
    i00, i01, i11 = S11 / det, -S01 / det, S00 / det
    g = {}
    for a, e, c in zip(x, res, cl):
        s = g.setdefault(c, [0.0, 0.0]); s[0] += e; s[1] += e * a
    G = len(g)
    m00 = sum(s[0] ** 2 for s in g.values()); m01 = sum(s[0] * s[1] for s in g.values()); m11 = sum(s[1] ** 2 for s in g.values())
    # V = B M B, element (0,0)
    v00 = i00 * (i00 * m00 + i01 * m01) + i01 * (i00 * m01 + i01 * m11)
    v00 *= G / (G - 1)
    return a0, a0 / math.sqrt(v00), G, b

def run(cal, q, beta, drift, mu=0.0, sm=0.104, se=0.083, sims=3000, seed=7):
    ft = S[cal]; r = random.Random(seed); wk = [L.iso_week(t) for t in ft]
    ent = [t + L.ENTRY_DELAY for t in ft]; ext = [t + L.HOLD for t in ent]
    h = {'raw': 0, 'adj_beta1': 0, 'ols_alpha': 0, 'adj_mean_pos': 0}
    for _ in range(sims):
        M = market_increments(ent + ext, sm, r)
        u = [-(M[b] - M[a]) + drift for a, b in zip(ent, ext)]
        net = [mu + beta * x + se * tdraw(r) for x in u]
        m, t, G = L.clustered_t(net, wk); h['raw'] += m > 0 and t >= L.t_crit(G - 1, q)
        adj = [y - x for y, x in zip(net, u)]
        m, t, G = L.clustered_t(adj, wk); h['adj_beta1'] += m > 0 and t >= L.t_crit(G - 1, q); h['adj_mean_pos'] += m > 0
        a, t, G, b = ols_alpha_t(net, u, wk); h['ols_alpha'] += a > 0 and t >= L.t_crit(G - 2, q)
    return {k: round(v / sims, 3) for k, v in h.items()}

if __name__ == '__main__':
    for cal, q in (('confirmation', L.Q_CONFIRM), ('discovery', L.Q_DISCOVERY)):
        for beta in (1.0, 1.5, 2.0):
            for drift in (0.0, 0.01, 0.02):
                print(cal, q, f'beta {beta} drift/wk {drift:.2f} mu 0', run(cal, q, beta, drift), flush=True)
    print('power check, confirmation, beta 1.5 drift 0.02 mu 0.04', run('confirmation', L.Q_CONFIRM, 1.5, 0.02, mu=0.04))
    print('power check, confirmation, beta 1.0 drift 0 mu 0.04', run('confirmation', L.Q_CONFIRM, 1.0, 0.0, mu=0.04))
