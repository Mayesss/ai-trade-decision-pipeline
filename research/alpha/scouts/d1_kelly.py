"""D1 scout: SYNTHETIC Kelly fraction and tail of a short-new-listings book (no data read at all).

Per event (fractions of notional): net = mu - beta*m_w + body, body = shift + k*SIGMA_E*t4.5 clipped at -cap, or an atom
-atom w.p. p; m_w is the 7-day alt-market return shared by every exit of the same week (SD 10.4 %); squeeze WEEKS: with
probability q per week every exit of that week is an atom (013's 'squeeze weeks' world). Exits per week ~ Poisson(lam),
capped at 7 (013's 16-day cap). Model parameters: the body/atom split calibrated to 013's cited discovery se
(d1_power.py): k 2.5, p 0.04 for T2 frozen; the stop variant shrinks the atom to -0.60 (stop at +50 %, gap and costs)
and makes it 2.5x as frequent, the body clipped at -0.60. Assumed, not measured.

Reported per rule and mu: full Kelly c* (max mean weekly log growth over a grid), and at c = 2.5 % / 3.5 %: the annual
mean, the median-year and 1-in-10-year worst month, the 1-in-10-year worst week.

Usage: python -I d1_kelly.py <out_txt>
"""
import math
import random
import sys

SIGMA_E, SIGMA_M, NU = 0.083, math.sqrt(0.133 ** 2 - 0.083 ** 2), 4.5


def tdraw(rnd):
    z = rnd.gauss(0, 1)
    chi = sum(rnd.gauss(0, 1) ** 2 for _ in range(4)) + 0.5 * rnd.gauss(0, 1) ** 2
    return z / math.sqrt(chi / NU) / math.sqrt(NU / (NU - 2))


def poisson(rnd, lam):
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rnd.random()
        if p < L:
            return k
        k += 1


def weeks(rnd, n_weeks, mu, k, p, atom, cap, lam, beta=1.5, q=0.0):
    shift = (mu + p * atom) / (1 - p)
    out = []
    for _ in range(n_weeks):
        m = rnd.gauss(0, SIGMA_M)
        n = min(7, poisson(rnd, lam))
        sq = rnd.random() < q
        nets = []
        for _ in range(n):
            if sq or rnd.random() < p:
                nets.append(-atom)
            else:
                nets.append(max(-cap, shift - beta * m + k * SIGMA_E * tdraw(rnd)))
        out.append(nets)
    return out


def main():
    rnd = random.Random(11)
    lines = []
    lam = 55 / 52.18                     # T2-sized book: about 55 entries a year (013: 96 T2 entries in 1.55 discovery yr)
    rules = {'T2 frozen (atom -1, p 4 %)': dict(k=2.5, p=0.04, atom=1.0, cap=1.0),
             'T2 + stop 50 % (atom -0.60, p 10 %)': dict(k=2.5, p=0.10, atom=0.60, cap=0.60)}
    for q in (0.0, 0.03):
        for name, r in rules.items():
            for mu in (0.02, 0.034, 0.05):
                W = weeks(rnd, 52 * 400, mu, r['k'], r['p'], r['atom'], r['cap'], lam, q=q)
                best = None
                for c in [x / 1000 for x in range(5, 401, 5)]:
                    g = 0.0
                    ok = True
                    for nets in W:
                        v = 1 + c * sum(nets)
                        if v <= 0:
                            ok = False
                            break
                        g += math.log(v)
                    if ok and (best is None or g > best[1]):
                        best = (c, g)
                cells = [f'{name:36s} q {q:.2f} mu {mu:.3f} | full Kelly c* {best[0]:.3f}']
                for c in (0.025, 0.035):
                    yrs = [W[i:i + 52] for i in range(0, len(W) - 51, 52)]
                    ann = sum(c * sum(n) for n in W) / (len(W) / 52.18)
                    worst_m = sorted(min(c * sum(sum(n) for n in y[j:j + 4]) for j in range(0, 52, 4)) for y in yrs)
                    worst_w = sorted(min(c * sum(n) for n in y) for y in yrs)
                    cells.append(f'c {c:.3f}: {ann * 100:+.1f} %/yr, worst month median-yr {worst_m[len(yrs) // 2] * 100:+.1f} %'
                                 f' / 1-in-10-yr {worst_m[len(yrs) // 10] * 100:+.1f} %, worst week 1-in-10-yr '
                                 f'{worst_w[len(yrs) // 10] * 100:+.1f} %')
                lines.append(' | '.join(cells))
                print(lines[-1], flush=True)
    open(sys.argv[1], 'w').write('\n'.join(lines) + '\n')


if __name__ == '__main__':
    main()
