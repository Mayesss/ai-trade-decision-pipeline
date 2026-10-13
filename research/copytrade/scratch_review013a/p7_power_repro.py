"""SYNTHETIC. Independent re-implementation of 013 §3's noise model and the eight discovery criteria plus the
confirmation test (own t-quantile, own clustered t, own criteria) to reproduce the mu = 4 %, k = 1 cell
(draft: T1 discovery 0.87, T2 0.76, any 0.90, joint 0.63) and the mu = 0, k = 1 null (0.003 / 0.005 / joint 0)."""
import json, math, random
from pathlib import Path
import datetime as dt
S = json.loads((Path(__file__).parent / 'structure.json').read_text())

def isoweek(t):
    y, w, _ = dt.datetime.fromtimestamp(t / 1000, dt.UTC).isocalendar(); return (y, w)

def weeks(cal):
    c = {}
    for t in S[cal]: c[isoweek(t)] = c.get(isoweek(t), 0) + 1
    return [c[w] for w in sorted(c)]

def tq(q, df):          # Cornish-Fisher / Hill expansion of the Student-t quantile
    # normal quantile by bisection on erf
    lo, hi = 0.0, 10.0
    for _ in range(100):
        mid = (lo + hi) / 2
        if 0.5 * (1 + math.erf(mid / math.sqrt(2))) < q: lo = mid
        else: hi = mid
    z = (lo + hi) / 2
    g1 = (z ** 3 + z) / 4; g2 = (5 * z ** 5 + 16 * z ** 3 + 3 * z) / 96
    g3 = (3 * z ** 7 + 19 * z ** 5 + 17 * z ** 3 - 15 * z) / 384
    return z + g1 / df + g2 / df ** 2 + g3 / df ** 3

def tstat(v, cl):
    n = len(v); m = sum(v) / n; g = {}
    for x, c in zip(v, cl): g[c] = g.get(c, 0) + x - m
    G = len(g); var = G / (G - 1) * sum(x * x for x in g.values()) / n / n
    return m, m / math.sqrt(var), G

def tdraw(r, nu=4.5):
    z = r.gauss(0, 1); chi = sum(r.gauss(0, 1) ** 2 for _ in range(4)) + 0.5 * r.gauss(0, 1) ** 2
    return z / math.sqrt(chi / nu) / math.sqrt(nu / (nu - 2))

SE, SC = 0.083, math.sqrt(0.133 ** 2 - 0.083 ** 2) * math.sqrt(2 / 3)

def calendar(wc, mu, k, r):
    T1, T2 = [], []
    for i, n in enumerate(wc):
        u = r.gauss(0, k * SC)
        for _ in range(n):
            x = max(-1, min(1, mu + u + k * SE * tdraw(r)))
            e = (x, i, x - u)
            T1.append(e)
            if r.random() < 0.5: T2.append(e)
    return {'T1': T1, 'T2': T2}

def disc_pass(ev, yrs, q=0.975):
    v = [e[0] for e in ev]; cl = [e[1] for e in ev]
    if len(v) < 20 or len(set(cl)) < 10: return False
    m, t, G = tstat(v, cl)
    if not (m > 0 and t >= tq(q, G - 1)): return False
    if sorted(v)[len(v) // 2] <= 0 if len(v) % 2 else (sorted(v)[len(v) // 2 - 1] + sorted(v)[len(v) // 2]) / 2 <= 0: return False
    ws = sorted(set(cl)); first = set(ws[:(len(ws) + 1) // 2])
    a = [x for x, c in zip(v, cl) if c in first]; b = [x for x, c in zip(v, cl) if c not in first]
    if not (sum(a) / len(a) > 0 and sum(b) / len(b) > 0): return False
    s = {}
    for x, c in zip(v, cl): s[c] = s.get(c, 0) + x
    pos = [(x, c) for c, x in s.items() if x > 0]
    if pos:
        d = max(pos)[1]; rest = [x for x, c in zip(v, cl) if c != d]
        if not sum(rest) / len(rest) > 0: return False
    am, at, aG = tstat([e[2] for e in ev], cl)
    if not (am > 0 and at >= tq(q, aG - 1)): return False
    if m < 0.01: return False
    taken, cnt = 0.0, {}
    for x, c in zip(v, cl):
        if cnt.get(c, 0) < 4: cnt[c] = cnt.get(c, 0) + 1; taken += x
    return 0.03 * taken / yrs >= 0.03

def confirm(ev):
    v = [e[0] for e in ev]; cl = [e[1] for e in ev]
    if len(v) < 10 or len(set(cl)) < 5: return False
    m, t, G = tstat(v, cl)
    return m > 0 and t >= tq(0.95, G - 1)

if __name__ == '__main__':
    dw, cw = weeks('discovery'), weeks('confirmation')
    dy, cy = 1.5470, 0.9008
    print('week caps only: discovery events kept', sum(min(n, 4) for n in dw), '(both caps: 196)')
    print('t quantiles (own):', round(tq(0.975, 65), 4), round(tq(0.95, 38), 4))
    for mu, sims in ((0.04, 3000), (0.0, 3000)):
        r = random.Random(2024 + int(mu * 100)); h = {'T1': 0, 'T2': 0, 'any': 0, 'joint': 0}
        for _ in range(sims):
            d = calendar(dw, mu, 1.0, r)
            p1, p2 = disc_pass(d['T1'], dy), disc_pass(d['T2'], dy)
            h['T1'] += p1; h['T2'] += p2
            if p1 or p2:
                h['any'] += 1
                c = calendar(cw, mu, 1.0, r)['T1' if p1 else 'T2']
                h['joint'] += confirm(c)
        print(f'mu {mu} k 1:', {k: round(v / sims, 3) for k, v in h.items()}, flush=True)
