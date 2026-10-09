"""Registration 002 — cascade detection and map density on SYNTHETIC data.

    .venv/bin/python -m tests.test_liq
"""
import random

from ct import liq

MINUTE, DAY = 60_000, 86_400_000


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def main():
    print('cascades (synthetic liquidation fills)')
    rng = random.Random(2)
    t0 = 1_767_225_600_000
    fills = []
    # background: small liquidations every few minutes, both sides
    for i in range(2000):
        t = t0 + i * 3 * MINUTE + rng.randrange(MINUTE)
        fills.append((t, rng.choice(['Close Long', 'Close Short']), rng.uniform(100, 2000), 100.0))
    # planted cascade 1: 40 long liquidations within 50 s, $2M, at day 3
    c1 = t0 + 3 * DAY + 5 * 3600_000
    for i in range(40):
        fills.append((c1 + i * 1200, 'Close Long', 50_000.0, 100.0 - i * 0.05))
    # planted cascade 2: shorts, with a backstop row, at day 5
    c2 = t0 + 5 * DAY + 12 * 3600_000
    for i in range(10):
        fills.append((c2 + i * 2000, 'Close Short', 60_000.0, 100.0 + i * 0.1))
    fills.append((c2 + 25_000, 'Liquidated Cross Short', 400_000.0, 101.5))
    fills.sort()
    daily = [(t0 + d * DAY, 300_000.0) for d in range(7)]        # a typical day: $300k
    med = liq.trailing_median_fn(daily, days=30)
    check('trailing median needs 5 days', med(t0 + 3 * DAY) is None and med(t0 + 5 * DAY) == 300_000.0)
    rs = liq.runs([f for f in fills], 60_000)
    check('runs are one-directional bursts', all(r['purity'] >= 0.99 for r in rs if r['n'] >= 10))
    cas = liq.cascades(fills, 60_000, 0.8, 1.0, 250_000, lambda t: 300_000.0)
    check('two planted cascades found, no others', len(cas) == 2, f'{len(cas)} cascades')
    a, b = cas
    # a background fill within 60 s of the planted run legitimately joins it: allow one or two
    check('cascade 1: longs, ~$2M, >= 40 fills, start/end within a gap of the plant',
          a['side'] == 'L' and 2e6 <= a['notional'] < 2e6 + 5000 and 40 <= a['n'] <= 42
          and abs(a['t_start'] - c1) <= 60_000 and abs(a['t_end'] - (c1 + 39 * 1200)) <= 60_000,
          f"n {a['n']} notional {a['notional']:,.0f} start {a['t_start'] - c1} ms")
    check('cascade 2: shorts incl. backstop row, rel ~ 1M/300k',
          b['side'] == 'S' and 1e6 <= b['notional'] < 1e6 + 5000 and abs(b['rel'] - b['notional'] / 3e5) < 1e-9,
          f"notional {b['notional']:,.0f}")
    check('a higher K removes the smaller one', len(liq.cascades(fills, 60_000, 0.8, 5.0, 250_000, lambda t: 300_000.0)) == 1)
    check('random background alone gives no cascade',
          liq.cascades([f for f in fills if f[2] < 5000], 60_000, 0.8, 1.0, 250_000, lambda t: 300_000.0) == [])
    print('map density (synthetic snapshot)')
    pos = [(1.0, 1000.0, 99.5)] * 10 + [(1.0, 1000.0, 95.0)] * 10 + [(-1.0, 1000.0, 101.0)] * 5 + [(1.0, 1000.0, None)] * 3
    d, gross = liq.density(pos, 'L', 100.0, 0.01)
    check('density: longs with liq price within 1% below = 10k of 28k', abs(d - 10_000 / 28_000) < 1e-9 and gross == 28_000, f'{d:.3f}')
    d2, _ = liq.density(pos, 'S', 100.0, 0.02)
    check('density: shorts within 2% above = 5k of 28k', abs(d2 - 5_000 / 28_000) < 1e-9)
    d3, _ = liq.density(pos, 'L', 100.0, 0.001)
    check('density: nothing inside a 0.1% band', d3 == 0.0)
    print('liq checks passed')


if __name__ == '__main__':
    main()
