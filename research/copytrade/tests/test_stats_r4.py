"""Registration 001 revision 4 statistics — SYNTHETIC data only.

    .venv/bin/python -m tests.test_stats_r4
"""
import random
import statistics

from ct import stats

Z_CRIT = 2.33


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def main():
    print('revision 4 statistics')
    hits = 0
    for seed in range(200):
        rng = random.Random(seed)
        ws = [stats.tercile_ic([(rng.gauss(0, 1), rng.gauss(0, 1), rng.random()) for _ in range(600)])
              for _ in range(4)]
        hits += stats.combine(ws) >= Z_CRIT
    check('null false positives at z 2.33 <= 3% of 200 seeds', hits <= 6, f'{hits}/200 (expected ~2)')
    rng = random.Random(1)
    rows = [(s, 0.1 * s + rng.gauss(0, 1), rng.random()) for s in (rng.gauss(0, 1) for _ in range(2000))]
    ic, n = stats.top_half_ic(rows)
    check('top-half IC on the upper half only', n <= 1000 and ic is not None and ic > 0, f'ic {ic:.3f} n {n}')
    dm = stats.decile_means(rows)
    check('decile means increase with a planted slope', len(dm) == 10 and dm[9][2] > dm[0][2],
          f'd1 {dm[0][2]:.3f} d10 {dm[9][2]:.3f}')
    rows0 = [(s, 0.0 if s < -0.5 else rng.gauss(0, 1), rng.random()) for s in (rng.gauss(0, 1) for _ in range(2000))]
    zs = stats.zero_share_by_quintile(rows0)
    check('zero share by quintile: bottom quintile all zero', zs[0][1] > 0.9 and zs[4][1] < 0.05,
          f'{[round(v, 2) for _, v in zs]}')
    a = [rng.gauss(0.002, 0.01) for _ in range(300)]
    b = [x - 0.001 + rng.gauss(0, 0.002) for x in a]
    m, se, n = stats.paired_diff(a, b)
    check('paired diff recovers the planted 0.001 with a small SE', abs(m - 0.001) < 0.0004 and se < 0.0003,
          f'diff {m:.5f} se {se:.5f}')
    # cluster bootstrap: independent wallets -> sd ~ 1; a repeated wallet effect across 4 windows -> sd > 1
    rng = random.Random(2)
    indep = [[(f'w{i}_{k}', rng.gauss(0, 1), rng.gauss(0, 1), rng.random()) for i in range(500)] for k in range(4)]
    r1 = stats.cluster_bootstrap(indep, n_boot=150, seed=3)
    check('bootstrap sd ~ 1 for independent wallets', r1['sd'] is not None and 0.7 <= r1['sd'] <= 1.35,
          f"sd {r1['sd']:.2f}, overlap {r1['overlap']:.2f}")
    effect = {f'w{i}': rng.gauss(0, 1) for i in range(500)}
    dep = [[(w, effect[w] + rng.gauss(0, 0.3), effect[w] + rng.gauss(0, 0.3), rng.random()) for w in effect]
           for _ in range(4)]
    r2 = stats.cluster_bootstrap(dep, n_boot=150, seed=3)
    check('repeated wallet effect widens the bootstrap (sd > 1, overlap 1)',
          r2['sd'] is not None and r2['sd'] > 1.3 and r2['overlap'] == 1.0,
          f"sd {r2['sd']:.2f}, z {r2['z']:.1f} -> clustered {r2['z_clustered']:.1f}")
    # split test: a planted crowding penalty
    rng = random.Random(4)
    rows = []
    for w in range(4):
        for i in range(120):
            c = rng.random()
            rows.append((w, f'w{i}', c, (0.002 if c < 0.5 else -0.001) + rng.gauss(0, 0.01)))
    r3 = stats.split_test(rows, n_boot=300)
    check('split test finds a planted crowding penalty', r3['z'] is not None and r3['z'] >= Z_CRIT,
          f"diff {r3['diff']:.4f} se {r3['se']:.4f} z {r3['z']:.1f}")
    rows_null = [(w, f'w{i}', rng.random(), rng.gauss(0, 0.01)) for w in range(4) for i in range(120)]
    r4 = stats.split_test(rows_null, n_boot=300)
    check('split test null stays under z 2.33', abs(r4['z']) < Z_CRIT, f"z {r4['z']:.2f}")
    print('revision 4 statistics checks passed')


if __name__ == '__main__':
    main()
