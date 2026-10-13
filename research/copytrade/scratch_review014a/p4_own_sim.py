"""Probe 4 (SYNTHETIC only; an independent world, not ct.shortvol_sim's). Daily-hedged 7-day ATM
straddle, BTC+ETH book, weekly. Regime-switching vol (calm/stress Markov by week), Student-t(5), truncated at 8 SD,
daily returns, down-jumps more frequent in stress. Uses ct.shortvol's b76, call_delta, leg_fee, the
registered half-spreads and perp costs, summary() and discovery_criteria() (registered floor 2.59).

Null A: one IV multiplier m for all weeks, net mean exactly 0 (the draft's kind of null).
Null B: VRP regime-dependent (positive in calm, negative in stress: the draft's own T2 mechanism),
        unconditional net mean exactly 0.
Power:  Null-A world with net = 0.05 of premium (the draft's prior VRP 0.10 cell).
Rates on CONTIGUOUS 143-week windows (all dependence kept) and, for comparison, on 13-week block draws."""
import math, random, statistics, sys
sys.path.insert(0, '.')
from ct import shortvol as sv

def tdraw(rng, df=5):
    z = rng.gauss(0, 1); c = sum(rng.gauss(0, 1) ** 2 for _ in range(df))
    return max(-8.0, min(8.0, z / math.sqrt(c / df) / math.sqrt(df / (df - 2))))

def world(n_weeks, seed, p_cs=1/40, p_sc=1/6, vol=(0.55, 1.10), jump=(0.002, 0.012)):
    rng = random.Random(seed)
    st, out = 0, []
    for w in range(n_weeks):
        if st == 0 and rng.random() < p_cs: st = 1
        elif st == 1 and rng.random() < p_sc: st = 0
        sig = vol[st]
        zb = [tdraw(rng) for _ in range(7)]
        ze = [0.8 * a + 0.6 * tdraw(rng) for a in zb]
        jb = [(-abs(rng.gauss(0.10, 0.06)) if rng.random() < jump[st] else 0.0) for _ in range(7)]
        paths = {}
        for c, z, sc in (('BTC', zb, 1.0), ('ETH', ze, 1.25)):
            s = sig * sc / math.sqrt(365)
            S = [1.0]
            for d in range(7):
                S.append(S[-1] * math.exp(s * z[d] + jb[d] * sc - 0.5 * s * s))
            paths[c] = (S, sig * sc)
        out.append((st, paths))
    return out

def week_net(paths, mult):
    T = 7 / 365.0
    rs = []
    for c, (S, sigma) in paths.items():
        iv = sigma * mult * 1.0
        pc, pp = sv.b76(1, 1, T, iv, True), sv.b76(1, 1, T, iv, False)
        hs = sv.HALF_SPREAD[c]
        prem = pc + pp
        net = prem * (1 - hs) - sv.leg_fee(1, pc * (1 - hs)) - sv.leg_fee(1, pp * (1 - hs))
        h, cost = 0.0, 0.0
        for d in range(7):
            tgt = 2 * sv.call_delta(S[d], 1, (7 - d) / 365.0, iv) - 1
            cost += abs(tgt - h) * S[d] * (sv.PERP_TAKER + sv.PERP_SLIP); h = tgt
            net += h * (S[d + 1] - S[d])
        cost += abs(h) * S[7] * (sv.PERP_TAKER + sv.PERP_SLIP)
        pay = abs(S[7] - 1)
        net -= cost + pay + min(sv.DELIV_FEE * S[7], sv.DELIV_CAP * pay)
        rs.append((net, prem))
    return statistics.fmean(r for r, _ in rs), statistics.fmean(p for _, p in rs)

def series(W, m_calm, m_stress):
    return [week_net(p, m_calm if st == 0 else m_stress) for st, p in W]

def solve(f, lo, hi, tgt=0.0):
    for _ in range(40):
        mid = (lo + hi) / 2
        if f(mid) < tgt: lo = mid
        else: hi = mid
    return (lo + hi) / 2

def rates(rs, G=143, block=None, draws=3000, seed=5, floor=None):
    rng = random.Random(seed)
    hit_c1 = hit_all = hit_student = 0
    if block is None:
        wins = [rs[i:i + G] for i in range(0, len(rs) - G + 1, G)]
    else:
        wins = []
        for _ in range(draws):
            o = []
            while len(o) < G:
                i = rng.randrange(0, len(rs) - block); o.extend(rs[i:i + block])
            wins.append(o[:G])
    for x in wins:
        s = sv.summary(x)
        c, tc = sv.discovery_criteria(s, floor=floor)
        hit_c1 += c['1_mean_pos_t_ge_tcrit']; hit_all += all(c.values())
        tp = s['t_plain_reported']
        hit_student += bool(s['mean'] > 0 and tp >= sv.t_crit(G - 1, 0.975))
    n = len(wins)
    return {'windows': n, 'c1': round(hit_c1 / n, 4), 'all5': round(hit_all / n, 4), 'plain_t_student': round(hit_student / n, 4)}

if __name__ == '__main__':
    N = int(sys.argv[1]) if len(sys.argv) > 1 else 143 * 1500
    W = world(N, 2024)
    print('stress share', sum(st for st, _ in W) / N)
    btc = [p['BTC'][0][-1] - 1 for _, p in W]
    m4 = statistics.fmean(x ** 4 for x in btc) / statistics.pvariance(btc) ** 2
    print('BTC weekly sd', round(statistics.pstdev(btc), 4), 'kurt', round(m4, 2))
    # Null A
    mA = solve(lambda m: statistics.fmean(r for r, _ in series(W, m, m)), 0.8, 1.4)
    A = [r for r, _ in series(W, mA, mA)]
    print('Null A m', round(mA, 4), 'mean', statistics.fmean(A), 'sd', statistics.pstdev(A), 'worst', min(A))
    print(' contiguous', rates(A)); print(' block13', rates(A, block=13))
    # Null B: calm VRP positive (m_calm = mA + 0.08), stress solved for zero unconditional net
    mc = mA + 0.08
    ms = solve(lambda m: statistics.fmean(r for r, _ in series(W, mc, m)), 0.3, 1.4)
    B = [r for r, _ in series(W, mc, ms)]
    print('Null B m_calm', round(mc, 4), 'm_stress', round(ms, 4), 'mean', statistics.fmean(B))
    print(' contiguous', rates(B)); print(' block13', rates(B, block=13))
    # Power: net = 0.05 of premium, constant m
    def netprem(m):
        s = series(W, m, m); return sum(r for r, _ in s) / sum(p for _, p in s)
    mP = solve(netprem, 0.8, 1.6, 0.05)
    P = [r for r, _ in series(W, mP, mP)]
    print('Power m', round(mP, 4), 'net/prem 0.05 mean/wk', statistics.fmean(P))
    print(' contiguous', rates(P)); print(' block13', rates(P, block=13))
