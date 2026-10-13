"""Probe 6 (SYNTHETIC paths only): the registered position_pnl (H24, p75 spreads, fees, no funding)
under one stylised crash of size `move` spread over hours [a, a+12), then flat; DVOL 60 -> 150.
Day 1 (a = 2: between the entry hedge and the next) against day 6 (a = 122, after five hedges)."""
import sys
sys.path.insert(0, '.')
from ct import shortvol as sv
T = 7 / 365.0
def mark(iv):
    return {'coin': 'BTC', 'entry': 0, 'expiry': 7 * sv.DAY, 'K': 1.0, 'S': 1.0, 'iv': iv,
            'mark_call': sv.b76(1, 1, T, iv, True), 'mark_put': sv.b76(1, 1, T, iv, False)}
def path(move, a, dv0=60, dv1=150):
    out, S, D = [], 1.0, dv0
    for i in range(169):
        if a <= i < a + 12:
            S = (1 + move) ** ((i - a + 1) / 12); D = dv0 + (dv1 - dv0) * (i - a + 1) / 12
        elif i >= a + 12:
            S = 1 + move; D = dv1
        out.append((i * sv.HOUR, S, D))
    return out
for iv in (0.50, 0.80):
    for move in (-0.25, -0.40, -0.50, +0.40):
        row = []
        for a, lab in ((2, 'day1'), (50, 'day3'), (122, 'day6')):
            p = path(move, a)
            o = sv.position_pnl(mark(iv), p, p[-1][1], liq_n=sv.SIZE_N)
            row.append(f"{lab} {o['ret_notional'] * 100:6.1f}%" + (' LIQ' if o['liquidated'] else ''))
        print(f'entry IV {iv:.2f} move {move:+.2f}:', ' | '.join(row), f'(equity at n 0.40, day1: {0.4 * sv.position_pnl(mark(iv), path(move, 2), 1 + move)["ret_notional"] * 100:.1f}%)')
