"""Registration 008 (Revision 2) — power and false-confirm rates by SIMULATION on synthetic data only.

Calibration (001 §12 / results.jsonl line 1, published numbers; no data read):
- book daily SD 0.553 %/day = NW se (0.1475 %/day / 3.416) x sqrt(164 days);
- inverted discovery magnitude before trading costs +7.46 bp/day (001's net mean flipped, minus the book's
  own 7.29 bp/day cost); half 3.73 bp/day.
BTC monthly drifts for the nulls use only 001 §2's public figures (Aug +24 %, Sep +8 %) and a July that is
varied (0 or -0.3 %/day); October and November are set to 0. Not on the outcome path; not hashed.

    .venv/bin/python -I r008_power.py [--sims 4000]
"""
import argparse
import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ct import crowd008 as c  # noqa: E402

DESIGNS = {'E1 2026-09-28 (recommended)': dt.date(2026, 9, 28), 'E2 2026-11-23': dt.date(2026, 11, 23)}
FULL = c.DISC_GROSS_INV
AUG, SEP = 1.24 ** (1 / 31) - 1, 1.08 ** (1 / 30) - 1          # 001 §2: Aug +24 %, Sep +8 %


def month(k):
    return c.month_of(k)


def drift(july):
    return lambda k, w: {'2026-07': july, '2026-08': AUG, '2026-09': SEP}.get(month(k), 0.0)


BASE = dict(beta=0.3, btc_drift=drift(0.0))
NOISE = {'gaussian iid': dict(rho=0.0, tails=False), 'AR(1) 0.15 + t4 tails': dict(rho=0.15, tails=True)}
EFFECTS = {'0': 0.0, 'half (3.7 bp/day)': FULL / 2, 'full (7.5 bp/day)': FULL}
NULLS = {
    'N1 cohort betas: A 0 / B(,C) 0.5; July -0.3 %/day': dict(beta=lambda k, w: 0.0 if w == 0 else 0.5, btc_drift=drift(-0.003)),
    'N2 cohort betas: A -0.2 / B(,C) 0.3; July -0.3 %/day': dict(beta=lambda k, w: -0.2 if w == 0 else 0.3, btc_drift=drift(-0.003)),
    'N3 within-window beta change: Jul 0 / Aug 0.4 / Sep 0': dict(beta=lambda k, w: 0.4 if month(k) == '2026-08' else 0.0,
                                                                 btc_drift=drift(0.0)),
    'N4 alt-minus-BTC trend: loading 0.3, +0.2 %/day throughout': dict(**BASE, alt_load=0.3, alt_drift=0.002),
    'N5 alt-minus-BTC trend: loading 0.3, +0.3 %/day in August only': dict(
        **BASE, alt_load=0.3, alt_drift=lambda k, w: 0.003 if month(k) == '2026-08' else 0.0),
    'N6 reversal exposure: loading 0.5, factor +0.10 %/day': dict(**BASE, rev_load=0.5, rev_drift=0.001),
    'N7 reversal exposure: loading 1.0, factor +0.10 %/day': dict(**BASE, rev_load=1.0, rev_drift=0.001),
    'N8 month-long persistence: 5 % of variance, rho 0.9': dict(**BASE, slow_share=0.05, slow_rho=0.9),
    'N9 month-long persistence: 3 % of variance, rho 0.97': dict(**BASE, slow_share=0.03, slow_rho=0.97),
    'N10 combined N1 + N4 + N6': dict(beta=lambda k, w: 0.0 if w == 0 else 0.5, btc_drift=drift(-0.003),
                                      alt_load=0.3, alt_drift=0.002, rev_load=0.5, rev_drift=0.001),
    # review r1 (008-review-r1.md, B1): cohort- and month-specific alt / reversal loadings
    'N11 alt loading A 0 / B(,C) 0.5; alt drift Jul 0, later +0.2 %/day': dict(
        **BASE, alt_load=lambda k, w: 0.0 if w == 0 else 0.5, alt_drift=lambda k, w: 0.0 if month(k) == '2026-07' else 0.002),
    'N11c alt loading A -0.2 / B(,C) 0.3; alt drift Jul -0.2, later +0.2 %/day': dict(
        **BASE, alt_load=lambda k, w: -0.2 if w == 0 else 0.3, alt_drift=lambda k, w: -0.002 if month(k) == '2026-07' else 0.002),
    'N12 reversal loading A 0 / B(,C) 1.0; drift Jul 0, later +0.10 %/day': dict(
        **BASE, rev_load=lambda k, w: 0.0 if w == 0 else 1.0, rev_drift=lambda k, w: 0.0 if month(k) == '2026-07' else 0.001),
    'N13 heavy tails: BTC t3, residual SD rising with |BTC|': dict(**BASE, btc_t=3, hetero=True),
    'N14 alt loading Jul 0 / Aug 0.4 / Sep 0; alt drift +0.3 %/day in Aug': dict(
        **BASE, alt_load=lambda k, w: 0.4 if month(k) == '2026-08' else 0.0,
        alt_drift=lambda k, w: 0.003 if month(k) == '2026-08' else 0.0),
    'N15 reversal loading Jul 0 / Aug 1.0 / Sep 0; drift +0.15 %/day in Aug': dict(
        **BASE, rev_load=lambda k, w: 1.0 if month(k) == '2026-08' else 0.0,
        rev_drift=lambda k, w: 0.0015 if month(k) == '2026-08' else 0.0),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--sims', type=int, default=4000)
    args = ap.parse_args()
    out = []
    for dname, end in DESIGNS.items():
        days = c.planned_days(end)
        G = len({c.week_of(k) for k, _ in days})
        print(f'\n== {dname}: {len(days)} book days, {G} week clusters, crit t_(G-1)(0.95) = {c.t_quantile(G - 1, 0.95):.3f}')
        for nname, nk in NOISE.items():
            for ename, mu in EFFECTS.items():
                r = c.simulate(days, mu, sims=args.sims, seed=8, **BASE, **nk)
                out.append({'design': dname, 'case': f'{nname}, effect {ename}', **r})
                print(f'  {nname:<24} effect {ename:<20} confirm {r["confirm"]:.3f}  rev1 {r["rev1_rule_pooled_alt_rev"]:.3f}  '
                      f'rev0 {r["draft_rule_btc_only"]:.3f}  raw {r["raw_rule"]:.3f}  confirm&bar {r["confirm_and_bar"]:.3f}',
                      flush=True)
        for name, kw in NULLS.items():
            r = c.simulate(days, 0.0, sims=args.sims, seed=9, **kw)
            out.append({'design': dname, 'case': 'null ' + name, **r})
            print(f'  null {name:<72} confirm {r["confirm"]:.3f}  rev1 {r["rev1_rule_pooled_alt_rev"]:.3f}  '
                  f'rev0 {r["draft_rule_btc_only"]:.3f}  raw {r["raw_rule"]:.3f}', flush=True)
        for name in ('N1 cohort betas: A 0 / B(,C) 0.5; July -0.3 %/day', 'N10 combined N1 + N4 + N6',
                     'N11c alt loading A -0.2 / B(,C) 0.3; alt drift Jul -0.2, later +0.2 %/day'):
            r = c.simulate(days, FULL, sims=args.sims, seed=10, **NULLS[name])
            out.append({'design': dname, 'case': 'power full under ' + name, **r})
            print(f'  power (full) under {name:<50} confirm {r["confirm"]:.3f}', flush=True)
    print(json.dumps({'sigma_day': c.SIGMA_DAY, 'full_effect': FULL, 'cost_day': c.COST_DAY, 'aug': AUG, 'sep': SEP}))


if __name__ == '__main__':
    main()
