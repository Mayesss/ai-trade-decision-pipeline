"""Revision 2 recheck (synthetic only): the REVISED crowd008.confirm (per-cell BTC, alt and reversal loadings)
under the r1 nulls N11-N15, base size and power; agreement with this probe's independent k-10 implementation.

    cd research/copytrade && .venv/bin/python -I scratch_review008r1/probe_r2_stats.py [sims]
"""
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sims = int(sys.argv[1]) if len(sys.argv) > 1 else 4000
sys.argv = [sys.argv[0], '0']
import probe_stats as P  # noqa: E402

c8 = P.c8
CELLS = c8.beta_cells(P.KEYS, dict(P.DAYS))


def as_fac(fac):
    return {k: {'btc': fac['btc'][i], 'alt': fac['alt'][i], 'rev': fac['rev'][i]} for i, k in enumerate(P.KEYS)}


def agreement():
    rng = random.Random(9)
    worst, ks = 0.0, set()
    spec = {'alt': 'cell', 'rev': 'cell'}
    for _ in range(40):
        y, fac = P.draw(rng, mu=0.0005)
        ok, num = c8.confirm(y, P.KEYS, as_fac(fac), CELLS)
        f = P.fit(y, P.design_v(fac, spec), P.WK)
        worst = max(worst, abs(num['t_alpha'] - f['t']), abs(num['alpha'] - f['alpha']))
        ks.add(num['k'])
    print(f'revised confirm vs independent per-cell (k 10) fit over 40 draws: max |diff| {worst:.2e}; k = {sorted(ks)}')


def rate(name, seed, **kw):
    rng = random.Random(seed)
    hits = 0
    for _ in range(sims):
        y, fac = P.draw(rng, **kw)
        hits += c8.confirm(y, P.KEYS, as_fac(fac), CELLS)[0]
    print(f'{name:<72} revised confirm {hits / sims:.3f}', flush=True)


W0, JUL, AUGM = P.W0, P.JUL, (lambda i: P.MON[i] == '2026-08')
agreement()
rate('base, effect 0', 101)
rate('power full (7.46 bp/day)', 102, mu=P.FULL)
rate('power half', 103, mu=P.FULL / 2)
rate('N10 combined', 104, beta=lambda i: 0.0 if W0(i) else 0.5, btc_drift=P.drift_m(-0.003), alt_load=lambda i: 0.3,
     u_drift=lambda i: 0.002, rev_load=lambda i: 0.5, rev_drift=lambda i: 0.001)
rate('N11 alt A 0 / B 0.5; U drift Aug-Sep +0.2 %/day            [r1: 0.082]', 105,
     alt_load=lambda i: 0.0 if W0(i) else 0.5, u_drift=lambda i: 0.0 if JUL(i) else 0.002)
rate('N11c alt A -0.2 / B 0.3; U drift Jul -0.2 / Aug-Sep +0.2   [r1: 0.140]', 106,
     alt_load=lambda i: -0.2 if W0(i) else 0.3, u_drift=lambda i: -0.002 if JUL(i) else 0.002)
rate('N12 rev A 0 / B 1.0; REV drift Aug-Sep +0.10               [r1: 0.084]', 107,
     rev_load=lambda i: 0.0 if W0(i) else 1.0, rev_drift=lambda i: 0.0 if JUL(i) else 0.001)
rate('N14 alt Jul 0 / Aug 0.4 / Sep 0; U drift +0.3 in Aug       [r1: 0.095]', 108,
     alt_load=lambda i: 0.4 if AUGM(i) else 0.0, u_drift=lambda i: 0.003 if AUGM(i) else 0.0)
rate('N15 rev Jul 0 / Aug 1.0 / Sep 0; REV drift +0.15 in Aug    [r1: 0.110]', 109,
     rev_load=lambda i: 1.0 if AUGM(i) else 0.0, rev_drift=lambda i: 0.0015 if AUGM(i) else 0.0)
rate('N13 BTC t3, residual SD ~ |BTC|', 110, btc_t=3, hetero=True)
rate('N8 persistence 5 %, rho 0.9', 111, slow=0.05, slow_rho=0.9)
