"""Registration 002 — event-study statistics on SYNTHETIC data.

    .venv/bin/python -m tests.test_eventstudy
"""
import random
import statistics

from ct import eventstudy as es


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def main():
    print('event study')
    c_in = [0, 100.0, 101.0, 99.0, 100.5]
    c_out = [1, 101.0, 101.5, 100.5, 101.0]
    r = es.event_return('L', c_in, c_out, 0.0006, 0.0)
    check('long after longs liquidated: +1% gross - 12 bp', abs(r - (0.01 - 0.0012)) < 1e-12, f'{r:.5f}')
    r2 = es.event_return('S', c_in, c_out, 0.0006, 0.0)
    check('short after shorts liquidated: -1% gross - 12 bp', abs(r2 - (-0.01 - 0.0012)) < 1e-12)
    r3 = es.event_return('L', c_in, c_out, 0.0, 0.1)
    check('slippage 10% of range hurts both legs', r3 < 0.01 and abs(r3 - ((101 - 0.1) / (100 + 0.2) - 1)) < 1e-12)
    # clustered t: independent -> ~N(0,1); 50 clusters of perfectly correlated values -> same t as 50 obs
    rng = random.Random(0)
    fp = 0
    for s in range(300):
        rng = random.Random(s)
        vals = [rng.gauss(0, 1) for _ in range(600)]
        _, _, t, _, _ = es.cluster_t(vals, list(range(600)))
        fp += abs(t) >= 1.96
    check('clustered t, independent null: ~5% false positives', 0.02 <= fp / 300 <= 0.09, f'{fp}/300')
    fp2 = 0
    for s in range(300):
        rng = random.Random(1000 + s)
        base = [rng.gauss(0, 1) for _ in range(50)]
        vals = [b for b in base for _ in range(12)]          # 12 copies of each: one cluster each
        _, _, t, _, g = es.cluster_t(vals, [i for i in range(50) for _ in range(12)])
        fp2 += abs(t) >= 1.96
    check('clustered t with 50 clusters of duplicates: still ~5%', 0.02 <= fp2 / 300 <= 0.10, f'{fp2}/300, clusters {g}')
    fp3 = 0
    for s in range(300):
        rng = random.Random(2000 + s)
        base = [rng.gauss(0, 1) for _ in range(50)]
        vals = [b for b in base for _ in range(12)]
        _, _, t, _, _ = es.cluster_t(vals, list(range(600)))   # wrong: treated as independent
        fp3 += abs(t) >= 1.96
    check('...and badly inflated if treated as independent', fp3 / 300 > 0.3, f'{fp3}/300')
    # placebo and planted reversal
    events = [{'cluster': i // 3, 'coin': 'X'} for i in range(900)]
    share = es.placebo(events, lambda rng, e: rng.gauss(0, 0.01), n_seeds=100)
    check('placebo on noise: |t| >= 1.96 in <= 10% of seeds', share <= 0.10, f'{share:.2f}')
    rng = random.Random(5)
    planted = [0.001 + rng.gauss(0, 0.01) for _ in events]
    _, _, t, _, _ = es.cluster_t(planted, [e['cluster'] for e in events])
    check('planted 0.1% reversal found (t >= 1.96)', t >= 1.96, f't = {t:.1f}')
    # tercile split with a planted density effect
    ev = []
    for i in range(1200):
        d = rng.random()
        ev.append({'density': d, 'ret': 0.004 * d + rng.gauss(0, 0.01), 'cluster': i // 2, 'month': i // 100})
    r = es.tercile_split(ev, key=lambda e: e['density'], value=lambda e: e['ret'], cluster=lambda e: e['cluster'],
                         within=lambda e: e['month'])
    check('tercile split finds a planted density effect, ordered', r['z'] >= 1.96 and r['means'][2] > r['means'][1] > r['means'][0],
          f"diff {r['diff']:.4f} z {r['z']:.1f} means {[round(m, 4) for m in r['means']]}")
    ev0 = [{'density': rng.random(), 'ret': rng.gauss(0, 0.01), 'cluster': i // 2, 'month': i // 100} for i in range(1200)]
    r0 = es.tercile_split(ev0, key=lambda e: e['density'], value=lambda e: e['ret'], cluster=lambda e: e['cluster'],
                          within=lambda e: e['month'])
    check('tercile split null stays under 1.96', abs(r0['z']) < 1.96, f"z {r0['z']:.2f}")
    print('event-study checks passed')


if __name__ == '__main__':
    main()
