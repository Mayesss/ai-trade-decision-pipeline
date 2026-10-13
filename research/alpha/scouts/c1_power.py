"""C1 scout: power arithmetic for a crypto trend existence test. No data is read.

Model: monthly strategy (or alpha) returns, i.i.d., annual Sharpe S. Over G monthly
clusters the test statistic is a (noncentral) t with df = G - 1 - k (k = extra
regression parameters, e.g. beta per regime cell) and noncentrality d = S * sqrt(G/12).
One-sided existence test at level a. Power by simulation (stdlib random), critical
values by bisection on the Student-t CDF (regularized incomplete beta).

Usage: python -I c1_power.py
"""
import math
import random


def _betacf(a, b, x):
    m_max, eps = 300, 3e-14
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c, d = 1.0, 1.0 - qab * x / qap
    d = 1.0 / (d if abs(d) > 1e-300 else 1e-300)
    h = d
    for m in range(1, m_max + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1.0 + aa / c
        c = c if abs(c) > 1e-300 else 1e-300
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1.0 + aa / c
        c = c if abs(c) > 1e-300 else 1e-300
        de = d * c
        h *= de
        if abs(de - 1.0) < eps:
            break
    return h


def _ibeta(a, b, x):
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    lbt = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1 - x)
    bt = math.exp(lbt)
    if x < (a + 1) / (a + b + 2):
        return bt * _betacf(a, b, x) / a
    return 1.0 - bt * _betacf(b, a, 1 - x) / b


def t_cdf(t, df):
    x = df / (df + t * t)
    p = 0.5 * _ibeta(df / 2, 0.5, x)
    return 1 - p if t > 0 else p


def t_crit(alpha, df):
    lo, hi = 0.0, 50.0
    for _ in range(200):
        mid = (lo + hi) / 2
        if 1 - t_cdf(mid, df) > alpha:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def power(S, G, k=0, alpha=0.05, sims=40000, seed=7):
    df = G - 1 - k
    c = t_crit(alpha, df)
    d = S * math.sqrt(G / 12)
    rng = random.Random(seed)
    hit = 0
    for _ in range(sims):
        z = rng.gauss(0, 1) + d
        chi = rng.gammavariate(df / 2, 2)  # chi-square(df)
        if z / math.sqrt(chi / df) > c:
            hit += 1
    return hit / sims, c, d


def years_for_power(S, target=0.8, alpha=0.05):
    za = 1.6448536269514722
    zb = 0.8416212335729143
    return ((za + zb) / S) ** 2


def main():
    spans = [
        ("(a) 2025-03-20 -> 2026-06-30 (out-of-paper, pre-cut)", 15),
        ("(c) 2026-07-01 -> 2027-06-30 (forward, unread)", 12),
        ("(a)+(c) 2025-03-20 -> 2027-06-30", 27),
        ("(c) 2026-07-01 -> 2027-12-31", 18),
        ("(a)+(c) 2025-03-20 -> 2027-12-31", 33),
        ("(a)+(c) 2025-03-20 -> 2028-06-30", 39),
        ("(a)+(c) 2025-03-20 -> 2029-06-30", 51),
        ("(b) in-paper 2020-01 -> 2025-03 (replication only)", 63),
    ]
    print("one-sided alpha=0.05; t_{G-1-k}; power by simulation (40k)")
    print("span | G | k | crit | power S=0.5 | S=0.75 | S=1.0 | S=1.5 | E[t] at S=1.0")
    for name, G in spans:
        for k in (0, 2):
            row = [name, str(G), str(k)]
            crit = None
            ps = []
            for S in (0.5, 0.75, 1.0, 1.5):
                p, crit, d = power(S, G, k)
                ps.append(f"{p:.2f}")
            row.append(f"{crit:.3f}")
            row += ps
            row.append(f"{1.0 * math.sqrt(G / 12):.2f}")
            print(" | ".join(row))
    print()
    print("years of out-of-sample data for 80% power (normal approx, one-sided 5%):")
    for S in (0.5, 0.75, 1.0, 1.25, 1.5):
        y = years_for_power(S)
        print(f"  S={S}: {y:.1f} years -> from 2025-03-20 ends ~{2025.22 + y:.1f}; from 2026-07-01 ends ~{2026.5 + y:.1f}")
    print()
    print("decay test: reject S = 1.57 (paper) when the truth is S, one-sided 5%:")
    for S in (0.0, 0.5, 0.78):
        for G in (15, 27, 39):
            d = (1.57 - S) * math.sqrt(G / 12)
            pw = 1 - 0.5 * (1 + math.erf((1.6448536 - d) / math.sqrt(2)))
            print(f"  truth S={S} G={G}: power {pw:.2f}")
    print()
    print("breadth: portfolio SR = s*sqrt(N/(1+(N-1)rho)); effective N = N/(1+(N-1)rho)")
    for rho in (0.1, 0.2, 0.3, 0.5):
        print("  rho", rho, "Neff(N=20)", round(20 / (1 + 19 * rho), 1), "Neff(N=100)", round(100 / (1 + 99 * rho), 1),
              "cap 1/rho", round(1 / rho, 1))


if __name__ == "__main__":
    main()
