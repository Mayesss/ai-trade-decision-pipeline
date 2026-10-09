"""p5_run.assemble on SYNTHETIC accumulators — exercises every pass flag and reading rule
without a registration, a score or an outcome. T3 is skipped here (it needs venue prices);
its pieces are covered by test_prepare / test_crowd_sets.

    .venv/bin/python -m tests.test_runner_assembly
"""
import random

import p5_run as run


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def fake_copy(acc_t, cfg, rng, effect, decay):
    """Fill a copy-trial accumulator with 4 windows of synthetic wallets.

    effect: slope of alpha on score; decay: 24 h-lag mean as a share of the 1 h mean (top quintile).
    """
    prim = f'{cfg["poll"]}:{cfg["primary"]}'
    mech = f'{cfg["poll"]}:{cfg["mechanism"]}'
    for w in range(4):
        rows = []
        for i in range(400):
            s = rng.gauss(0, 1)
            a = effect * s + rng.gauss(0, 1)
            rows.append((f'w{i}', s, a, rng.random()))
        acc_t['ics'].append(run.stats.tercile_ic([r[1:] for r in rows]))
        acc_t['ics_active'].append(run.stats.tercile_ic([r[1:] for r in rows]))
        acc_t['ics_top_half'].append(run.stats.top_half_ic([r[1:] for r in rows]))
        acc_t['ics_plain'].append(run.stats.tercile_ic([r[1:] for r in rows]))
        acc_t['ics_static'].append(run.stats.tercile_ic([r[1:] for r in rows]))
        acc_t['windows'].append(rows)
        acc_t['decile_means'].append(run.stats.decile_means([r[1:] for r in rows]))
        acc_t['zero_share'].append(run.stats.zero_share_by_quintile([r[1:] for r in rows]))
        top = sorted(rows, key=lambda r: r[1])[-80:]
        base = [0.002 * effect + rng.gauss(0, 0.004) for _ in top]
        acc_t['top'].setdefault(prim, []).extend(base)
        acc_t['top'].setdefault(mech, []).extend(b * decay + rng.gauss(0, 0.0005) for b in base)
        for k in (prim, mech):
            acc_t['top_alpha'].setdefault(k, []).extend(acc_t['top'][k][-80:])
            acc_t['top_unhedged'].setdefault(k, []).extend(acc_t['top'][k][-80:])
        acc_t['top_holdings'].extend(rng.gauss(0, 0.004) for _ in top)
        acc_t['top_beta_days'].extend(90 for _ in top)
        acc_t['n'].append(len(rows))


def main():
    print('runner assembly (synthetic accumulators)')
    rng = random.Random(9)
    acc = run.new_acc()
    for w in range(4):
        rows = [(f'w{i}', rng.gauss(0, 1), rng.gauss(0, 1), rng.random()) for i in range(900)]
        acc['T1']['ics'].append(run.stats.tercile_ic([r[1:] for r in rows]))
        acc['T1']['ics_zero'].append(run.stats.tercile_ic([r[1:] for r in rows]))
        acc['T1']['ics_top_half'].append(run.stats.top_half_ic([r[1:] for r in rows]))
        acc['T1']['decile_means'].append(run.stats.decile_means([r[1:] for r in rows]))
        acc['T1']['windows'].append(rows)
        acc['T1']['by_bucket'].setdefault('slow', []).append(run.stats.tercile_ic([r[1:] for r in rows[:300]]))
        acc['T1']['n'].append(len(rows))
    fake_copy(acc['T2'], run.T2, rng, effect=1.0, decay=0.9)     # real, delay-robust edge
    fake_copy(acc['T2b'], run.T2B, rng, effect=0.0, decay=1.0)   # nothing
    for w in range(4):
        for i in range(100):
            c = rng.random()
            acc['T4']['rows'].append((w, f'w{i}', c, (0.002 if c < 0.5 else -0.001) + rng.gauss(0, 0.006)))
            acc['T4']['rows_repeat'].append((w, f'w{i}', rng.random(), rng.gauss(0, 0.006)))
        acc['T4']['n'].append(100)
    out = run.assemble(acc)
    check('T1 null: no pass', out['T1']['pass'] is False, f"Z {out['T1']['Z']:.2f}")
    check('T2 planted edge: all criteria pass', out['T2']['pass'] is True, str(out['T2']['criteria']))
    check('T2 trial count and z_crit carried', out['T2']['trials'] == 5 and out['T2']['z_crit'] == 2.33)
    check('T2 paired difference below half the primary mean',
          out['T2']['top_quintile']['paired_diff'] < 0.5 * out['T2']['top_quintile']['primary_mean'])
    check('T2b null: no pass, no reading', out['T2b']['pass'] is False and out['T2b']['reading'] is None,
          f"Z {out['T2b']['Z']:.2f}")
    check('T4 planted crowding penalty passes', out['T4']['pass'] is True,
          f"z {out['T4']['shadower_excess']['z']:.1f}")
    check('T4 repeat-share variant reported', out['T4']['reported']['repeat_share'] is not None)
    # loser-persistence reading: IC passes, top quintile does not earn
    acc2 = run.new_acc()
    fake_copy(acc2['T2'], run.T2, rng, effect=1.0, decay=0.9)
    prim = f'{run.T2["poll"]}:{run.T2["primary"]}'
    acc2['T2']['top'][prim] = [x - 0.01 for x in acc2['T2']['top'][prim]]
    fake_copy(acc2['T2b'], run.T2B, rng, effect=0.0, decay=1.0)
    out2 = run.assemble(acc2)
    check('reading rule: IC pass with a flat top quintile is a NULL',
          out2['T2']['pass'] is False and out2['T2']['reading'].startswith('NULL'), out2['T2']['reading'])
    print('runner assembly checks passed')


if __name__ == '__main__':
    main()
