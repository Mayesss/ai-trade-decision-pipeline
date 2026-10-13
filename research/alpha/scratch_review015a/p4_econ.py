"""Review A of 015, probe 4: closed-form power, the economic bar's implied hurdle, Kelly / size, expected return by
venue, the forward step's power, and the tail of every placement of the documented shapes (synthetic arithmetic only).

    python3 -I research/alpha/scratch_review015a/p4_econ.py
"""
import math

import common as K

Phi = lambda z: 0.5 * (1 + math.erf(z / math.sqrt(2)))
SD, G = K.SD, 149
sig_y = SD * math.sqrt(12)
se = sig_y / math.sqrt(G)
print(f'sigma_y {100 * sig_y:.2f} %/month, SE(ybar) {1e4 * se:.1f} bp')
for eff in (85, 63.75, 42.5, 23, 0):
    d = eff / 1e4 / se
    print(f'  effect {eff:6.2f} bp: noncentral t {d:.2f}; power(1.655) {1 - Phi(1.655 - d):.3f} '
          f'power(1.91) {1 - Phi(1.91 - d):.3f} power(2.13) {1 - Phi(2.13 - d):.3f}')

# post-2017 (T+2 and T+1, 105 months) sign condition: P(mean_post > 0)
se_post = sig_y / math.sqrt(105)
for eff in (85, 42.5):
    print(f'  P(post-2017 mean > 0 | {eff} bp everywhere) = {Phi(eff / 1e4 / se_post):.3f}')

# contrast power (T+2: d = -2 x_{T-3}; T+1: d = -2 (x_{T-3} + x_{T-2})), the draft's injected model
e2, e1 = 2 * 37 / 6, 2 * 2 * 37 / 7
var_d = (80 * (2 * SD) ** 2 + 25 * 8 * SD ** 2) / 105 ** 2
d_mean = (80 * e2 + 25 * e1) / 105 / 1e4
print(f'contrast: E[d] {1e4 * d_mean:.1f} bp, SE {1e4 * math.sqrt(var_d):.1f} bp, noncentral t {d_mean / math.sqrt(var_d):.2f}')

# economic bar: shrunk net, half Kelly capped at 1.2, annual >= 4 %
cost = 1.6e-4
def bar_annual(vbar, sd=sig_y, cap=1.2):
    net = 0.5 * vbar - cost
    if net <= 0:
        return net, 0.0, 0.0
    f = net / sd ** 2
    L = min(f / 2, cap)
    return net, L, 12 * L * net
lo, hi = 0.0, 0.02
for _ in range(60):
    mid = (lo + hi) / 2
    if bar_annual(mid)[2] >= 0.04:
        hi = mid
    else:
        lo = mid
print(f'bar: needs vbar >= {1e4 * hi:.1f} bp/month = t {hi / se:.2f} on 149 months (SE {1e4 * se:.1f} bp)')
for eff in (85, 42.5):
    net, L, ann = bar_annual(eff / 1e4)
    print(f'  at vbar {eff} bp: shrunk net {1e4 * net:.1f} bp, size {L:.2f}x, annual {100 * ann:.1f} %')

# era tilt of the tradable: v = y + (2s-4) mu ; in the T+1 era v = y - 2 mu
for mu_yr in (0.05, 0.075, 0.10):
    mu = mu_yr / 252
    vbar_sample = (44 * 2 - 25 * 2) / 149 * mu
    print(f'equity excess drift {100 * mu_yr:.1f} %/yr: v-y over the sample {1e4 * vbar_sample:+.1f} bp, in the T+1 '
          f'era {-2e4 * mu:+.1f} bp/month ({-24 * mu * 100:+.2f} %/yr at 1x)')

# expected return by venue (total, including what collateral earns)
rf = 0.038
for eff in (85, 42.5, 0):
    mes = 12 * (eff / 1e4 - cost) + rf
    # xyz: 4.4 bp RT costs, funding ~0 averaged over eras but in the T+1 era a net-short month gains ~1.6 bp;
    # USDC collateral earns nothing (B1 2.2)
    xyz = 12 * (eff / 1e4 - 4.4e-4 + 1.6e-4)
    print(f'effect {eff:5.1f} bp at 1x: MES total {100 * mes:5.1f} %/yr (excess {100 * (mes - rf):5.1f}); '
          f'xyz total {100 * xyz:5.1f} %/yr -> xyz behind MES by {100 * (mes - xyz):.1f} %/yr')

# Kelly with fat tails: a crude check, Gaussian vs a month mixture (1-in-60 months a -20 % draw)
for eff in (85, 42.5):
    m = eff / 1e4 - cost
    fg = m / sig_y ** 2
    best, bestf = -1, 0
    for k in range(1, 800):
        f = k / 100
        g = 0.0
        # mixture: 59/60 normal months (3-point Gauss-Hermite) + 1/60 a -20 % month
        for w, z in ((1 / 6, -math.sqrt(3)), (2 / 3, 0.0), (1 / 6, math.sqrt(3))):
            r = m + sig_y * z
            g += (59 / 60) * w * math.log(max(1e-9, 1 + f * r))
        g += (1 / 60) * math.log(max(1e-9, 1 + f * (-0.20 + m)))
        if g > best:
            best, bestf = g, f
    print(f'Kelly at {eff} bp: Gaussian {fg:.2f}x; with a -20 % month 1-in-60 {bestf:.2f}x; '
          f'1.0x = {1 / fg:.2f} / {1 / bestf:.2f} of Kelly')

# forward step: sign check P(ybar_fwd > 0) and P(t >= 1) at 19 and 31 months
for mths in (19, 31):
    s_f = sig_y / math.sqrt(mths)
    print(f'forward {mths} months: ' + ', '.join(
        f'{eff} bp P(>0) {Phi(eff / 1e4 / s_f):.2f} P(t>=1) {1 - Phi(1 - eff / 1e4 / s_f):.2f}' for eff in (85, 42.5, 0)))

# tail: every placement of each shape on the real calendar; worst month P&L of the 1x L/S (flat outside windows)
dates, rows = K.load()
n = len(dates)
role = {}
for k, r in enumerate(rows):
    for i in r['P']:
        role[i] = (-1, k)
    for i in r['R']:
        role[i] = (+1, k)
for name, shape in K.SHOCKS.items():
    worst_m, worst_w = 0.0, 0.0
    for st in range(0, n - len(shape)):
        pm = {}
        daily = []
        for j, val in enumerate(shape):
            rl = role.get(st + j)
            pos = rl[0] if rl else 0
            daily.append(pos * val)
            if rl:
                pm[rl[1]] = pm.get(rl[1], 0.0) + pos * val
        worst_m = min([worst_m] + list(pm.values()))
        for a in range(len(daily) - 4):
            worst_w = min(worst_w, sum(daily[a:a + 5]))
    print(f'tail {name}: worst month at 1x {100 * worst_m:.1f} %, worst 5 trading days {100 * worst_w:.1f} %')
