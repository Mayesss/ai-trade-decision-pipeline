"""l3_run.summarise on SYNTHETIC event rows: pass flags, placebo, reading rule, T2 split.

    .venv/bin/python -m tests.test_l3_assembly
"""
import random

import l3_run as run


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def rows(rng, n, effect, density_effect=0.0):
    out = []
    for i in range(n):
        d = rng.random()
        base = effect + density_effect * d + rng.gauss(0, 0.01)
        r = {'coin': 'BTC' if i % 5 == 0 else f'C{i % 7}', 'symbol': 'X', 'side': rng.choice('LS'), 'entry': i * 600_000,
             'cluster': i // 3, 'day': i // 50, 'month': f'2026-{1 + (i // 300) % 6:02d}', 'major': i % 5 == 0,
             'fallback_median': False, 'hl_gap_bp': rng.gauss(0, 5), 'rel': rng.uniform(1, 50), 'beta': 1.0,
             'ret': {h: base + rng.gauss(0, 0.002) for h in run.HORIZONS},
             'ret_slip': {h: {s: base - s * 0.001 for s in run.SLIPS} for h in run.HORIZONS},
             'btc': {h: rng.gauss(0, 0.005) for h in run.HORIZONS}, 'month_label': rng.choice(['UP', 'DOWN', 'FLAT']),
             'density': d, 'snap_age_h': rng.uniform(1, 24), 'size_ok': True}
        out.append(r)
    return out


def main():
    print('l3 assembly (synthetic rows)')
    rng = random.Random(1)
    null = run.summarise(rows(rng, 1500, 0.0), placebo_seeds=50)
    check('null: T1 fails, placebo share small', null['T1']['pass'] is False and null['T1']['placebo_share'] <= 0.12,
          f"t {null['T1']['primary']['t']:.2f} placebo {null['T1']['placebo_share']:.2f}")
    pos = run.summarise(rows(rng, 1500, 0.002, 0.003), placebo_seeds=50)
    check('planted reversal: T1 passes all four', pos['T1']['pass'] is True, str(pos['T1']['criteria']))
    check('planted density effect: T2 passes with ordered terciles', pos['T2']['pass'] is True,
          f"z {pos['T2']['split']['z']:.1f} means {[round(m, 4) for m in pos['T2']['split']['means']]}")
    cont = run.summarise(rows(rng, 1500, -0.003), placebo_seeds=20)
    check('continuation: fails and is labelled as such', cont['T1']['pass'] is False and 'continuation' in (cont['T1']['reading'] or ''))
    check('reported blocks present', set(pos['T1']['reported']['by_horizon']) == set(run.HORIZONS) and len(pos['T1']['reported']['by_size_quintile']) == 5)
    # a density effect on top of a NEGATIVE base: the split is positive but T1 fails, so T2 may not pass
    neg = run.summarise(rows(rng, 1500, -0.006, 0.006), placebo_seeds=10)
    check('T2 cannot pass without T1', neg['T1']['pass'] is False and neg['T2']['split']['z'] > 1.96 and neg['T2']['pass'] is False,
          f"T1 mean {neg['T1']['primary']['mean']:.4f}, split z {neg['T2']['split']['z']:.1f}")
    print('l3 assembly checks passed')


if __name__ == '__main__':
    main()
