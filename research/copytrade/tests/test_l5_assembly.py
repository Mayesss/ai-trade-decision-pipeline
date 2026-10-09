"""l5_run.summarise_trial on SYNTHETIC rows: pass flags, placebo, largest-cluster exclusion.

    .venv/bin/python -m tests.test_l5_assembly
"""
import random

import l5_run as run


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def rows(rng, n, effect, crash=False):
    out = []
    for i in range(n):
        base = effect + rng.gauss(0, 0.008)
        if crash and i < 60:
            base += 0.5                        # one hour of absurd returns, all in cluster 0
        out.append({'coin': 'BTC' if i % 6 == 0 else f'C{i % 9}', 'side': rng.choice('LS'), 'entry': i * 700_000,
                    'cluster': 0 if (crash and i < 60) else i // 2, 'major': i % 6 == 0,
                    'ret': {h: base + rng.gauss(0, 0.001) for h in run.HORIZONS}})
    return out


def main():
    print('l5 assembly (synthetic rows)')
    rng = random.Random(4)
    null = run.summarise_trial(rows(rng, 1500, 0.0), placebo_seeds=50)
    check('null fails, placebo calibrated', null['pass'] is False and null['placebo_share'] <= 0.12,
          f"t {null['primary']['t']:.2f} placebo {null['placebo_share']:.2f}")
    pos = run.summarise_trial(rows(rng, 1500, 0.002), placebo_seeds=50)
    check('planted +0.2% passes all four', pos['pass'] is True, str(pos['criteria']))
    cr = run.summarise_trial(rows(rng, 1500, 0.0, crash=True), placebo_seeds=20)
    check('one crash hour does not pass (clustered t) and the without-largest-cluster block is ~0',
          cr['pass'] is False and abs(cr['without_largest_cluster']['mean']) < 0.002,
          f"mean {cr['primary']['mean']:.4f} t {cr['primary']['t']:.2f} | excl {cr['without_largest_cluster']['mean']:.4f}")
    check('horizons and sides reported', set(pos['by_horizon']) == set(run.HORIZONS) and set(pos['by_side']) == {'L', 'S'})
    print('l5 assembly checks passed')


if __name__ == '__main__':
    main()
