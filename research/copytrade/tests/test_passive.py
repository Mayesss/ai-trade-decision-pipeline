"""Registration 003 — passive-fill outcomes on SYNTHETIC numbers.

    .venv/bin/python -m tests.test_passive
"""
from ct import passive


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def main():
    print('passive liquidity outcomes')
    r = passive.passive_return('L', 100.0, 101.0, exit_slip=0.0)
    check('bid filled at 100, exit 101: +1% - 6 bp', abs(r - (0.01 - 0.0006)) < 1e-12, f'{r:.5f}')
    r2 = passive.passive_return('S', 100.0, 99.0, exit_slip=0.0)
    check('ask filled at 100, exit 99: +1% - 6 bp', abs(r2 - (0.01 - 0.0006)) < 1e-12)
    r3 = passive.passive_return('L', 100.0, 101.0, exit_slip=0.001)
    check('exit slippage reduces the long by 10 bp of exit', abs(r3 - (101 * 0.999 / 100 - 1 - 0.0006)) < 1e-12)
    fills = [(100.0, 10.0), (300.0, 9.8), (200.0, 9.5), (400.0, 9.0)]   # longs liquidated, price falling
    fp = passive.cascade_fill_price(fills, 'L')
    check('notional-weighted median fill price for a bid (half the notional at or below)', fp == 9.5, f'{fp}')
    fp2 = passive.cascade_fill_price([(100.0, 10.0), (100.0, 10.5), (100.0, 11.0), (700.0, 12.0)], 'S')
    check('for an ask: half the notional at or above', fp2 == 12.0, f'{fp2}')
    o = [passive.order_outcome('L', 100.0, 123, 100.5, True), passive.order_outcome('L', 100.0, None, None, False),
         passive.order_outcome('L', 100.0, 5, 99.0, True)]
    s = passive.ev_summary(o)
    check('EV summary: 2 of 3 filled, EV averages over all orders',
          s['orders'] == 3 and abs(s['fill_rate'] - 2 / 3) < 1e-12 and abs(s['ev_per_order'] - (o[0][1] + o[2][1]) / 3) < 1e-12, str(s))
    print('passive checks passed')


if __name__ == '__main__':
    main()
