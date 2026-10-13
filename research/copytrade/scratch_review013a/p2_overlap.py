"""SYNTHETIC. Size of the listing-week CR1 t when 7-day holds overlap across adjacent listing weeks.
Event times = the real first-trade times (coverage only); every return is simulated.
Market: Brownian, sampled exactly at each event's entry and exit; net_i = mu - beta*dM_i + e_i, e ~ t4.5 (unit var).
A: one continuous market path (real overlap).  B: an independent path per listing week (no cross-week overlap,
the assumption of 013's §3 simulator).  Reports one-sided rejection rates at the registered critical values."""
import json, math, random, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))
from ct import listings013 as L
S = json.loads((Path(__file__).parent / 'structure.json').read_text())

def tdraw(r, nu=4.5):
    z = r.gauss(0, 1)
    chi = sum(r.gauss(0, 1) ** 2 for _ in range(int(nu))) + (nu - int(nu)) * r.gauss(0, 1) ** 2
    return z / math.sqrt(chi / nu) / math.sqrt(nu / (nu - 2))

def market_increments(times, sig7, r):
    """{t: M(t)} Brownian with 7-day SD sig7, at the given times."""
    ts = sorted(set(times)); M = {}; m = 0.0; prev = ts[0]
    for t in ts:
        m += r.gauss(0, sig7 * math.sqrt((t - prev) / L.WEEK)); M[t] = m; prev = t
    return M

def run(cal, q, sig_m, sig_e, beta=1.0, mu=0.0, mode='A', sims=4000, seed=1):
    ft = S[cal]; r = random.Random(seed)
    wk = [L.iso_week(t) for t in ft]
    ent = [t + L.ENTRY_DELAY for t in ft]; ext = [t + L.HOLD for t in ent]
    hits = 0; tvals = []
    for _ in range(sims):
        if mode == 'A':
            M = market_increments(ent + ext, sig_m, r)
            dm = [M[b] - M[a] for a, b in zip(ent, ext)]
        else:
            dm = [0.0] * len(ft); byw = {}
            for i, w in enumerate(wk): byw.setdefault(w, []).append(i)
            for w, idx in byw.items():
                M = market_increments([ent[i] for i in idx] + [ext[i] for i in idx], sig_m, r)
                for i in idx: dm[i] = M[ext[i]] - M[ent[i]]
        net = [mu - beta * d + sig_e * tdraw(r) for d in dm]
        m, t, G = L.clustered_t(net, wk)
        crit = L.t_crit(G - 1, q)
        hits += t is not None and m > 0 and t >= crit
        tvals.append(t)
    sd = (sum(x * x for x in tvals) / len(tvals) - (sum(tvals) / len(tvals)) ** 2) ** 0.5
    return hits / sims, sd

if __name__ == '__main__':
    for cal, q in (('discovery', L.Q_DISCOVERY), ('confirmation', L.Q_CONFIRM)):
        for lab, sm, se in (('k1 (10.4/8.3)', 0.104, 0.083), ('k2 (20.8/16.6)', 0.208, 0.166), ('mkt-heavy (15/8.3)', 0.15, 0.083)):
            a = run(cal, q, sm, se, mode='A'); b = run(cal, q, sm, se, mode='B')
            print(f'{cal:12s} q={q} {lab:20s} overlap A: size {a[0]:.4f} sd(t) {a[1]:.3f} | no-overlap B: size {b[0]:.4f} sd(t) {b[1]:.3f}', flush=True)
