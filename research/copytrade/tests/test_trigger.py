"""Registration 003 revision 2 — causal trigger on SYNTHETIC liquidation fills.

    .venv/bin/python -m tests.test_trigger
"""
from ct import trigger


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def main():
    print('trigger')
    t0 = 1_767_225_600_000
    fills = [(t0 + i * 1000, 'Close Long', 30_000.0, 100.0 - i * 0.05) for i in range(10)]      # $300k in 10 s
    fills += [(t0 + 20 * 60_000 + i * 1000, 'Close Short', 40_000.0, 100.0 + i * 0.1) for i in range(5)]  # $200k, shorts
    fills += [(t0 + 2 * 3_600_000, 'Close Long', 50_000.0, 99.0)]                                  # lone print
    trs = trigger.triggers(fills, lambda t: 100_000.0)
    check('crosses $100k at the 4th print, once per cooldown', len(trs) == 2 and trs[0]['t'] == t0 + 3000,
          f"{[(x['t'] - t0, x['side']) for x in trs]}")
    check('direction follows the window majority', trs[0]['side'] == 'L' and trs[1]['side'] == 'S')
    check('no trigger without a threshold', trigger.triggers(fills, lambda t: None) == [])
    o = trigger.order_for(trs[0], 0.003, 2000)
    check('bid 0.3% below the crossing print, live 2 s later',
          abs(o['level'] - trs[0]['price'] * 0.997) < 1e-9 and o['t_post'] == trs[0]['t'] + 2000)
    o2 = trigger.order_for(trs[1], 0.003, 2000)
    check('ask above for shorts liquidated', o2['level'] > trs[1]['price'])
    print('trigger checks passed')


if __name__ == '__main__':
    main()
