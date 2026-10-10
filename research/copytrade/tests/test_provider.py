"""Registration 004 — provider mechanics on SYNTHETIC numbers.

    .venv/bin/python -m tests.test_provider
"""
from ct import provider


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def main():
    print('provider mechanics')
    l = provider.ladder('L', 100.0, 99.4, (0.005, 0.01, 0.015), (1, 2, 3))
    check('ladder skips the rung already passed (0.5% when the print is 0.6% down)',
          [r for _, _, r in l] == [0.01, 0.015] and abs(l[0][0] - 99.0) < 1e-9, str(l))
    ls = provider.ladder('S', 100.0, 100.2, (0.005, 0.01), (1, 2))
    check('ask ladder above the print', [round(x, 2) for x, _, _ in ls] == [100.5, 101.0])
    check('exit: flow end + k wins when earliest', provider.exit_time(1000, 5000, None, 600_000, 3_600_000) == 605_000)
    check('exit: reference touch wins when earlier', provider.exit_time(1000, 5000, 300_000, 600_000, 3_600_000) == 300_000)
    check('exit: cap binds', provider.exit_time(1000, 5000, None, 10_000_000, 3_600_000) == 3_601_000)
    check('exit never before the fill', provider.exit_time(10_000, 1000, None, 1000, 3_600_000) == 10_001)
    held, r = provider.fill_outcome('L', 100.0, True, 100.5, 99.9, exit_slip=0.0)
    check('forced fill held to exit: +0.5% - 6 bp', held and abs(r - (0.005 - 0.0006)) < 1e-12)
    held2, r2 = provider.fill_outcome('L', 100.0, False, 100.5, 99.9, exit_slip=0.0)
    check('unforced fill scratched at the next print: -0.1% - 6 bp', not held2 and abs(r2 - (-0.001 - 0.0006)) < 1e-12)
    ev, cond = provider.ladder_return([(1, 0.01), (2, None), (3, 0.002)])
    check('ladder return: EV over posted size, conditional over filled', abs(ev - (0.01 + 0.006) / 6) < 1e-12 and abs(cond - (0.01 + 0.006) / 4) < 1e-12)
    check('stress switch', provider.stress(20e6, 15e6) and not provider.stress(1e6, 15e6) and not provider.stress(None, 15e6))
    tot, hl, bg = provider.hedged_capture('L', 99.4, 99.9, [0, 100.0, 100.0, 100.0, 100.0], [0, 100.0, 100.0, 100.0, 100.0],
                                          hl_exit_slip=0.0, bg_slip_frac=0.0)
    check('capture: HL leg +0.503%-6bp, Bitget leg flat -12 bp', abs(hl - (99.9 / 99.4 - 1 - 0.0006)) < 1e-12 and abs(bg + 0.0012) < 1e-12
          and abs(tot - (hl + bg)) < 1e-12, f'{tot:.5f}')
    tot2, hl2, bg2 = provider.hedged_capture('L', 99.4, 99.4, [0, 100.0, 100, 100, 100], [0, 99.4, 99.4, 99.4, 99.4], 0.0, 0.0)
    check('capture when Bitget catches down to HL: Bitget leg earns the gap', bg2 > 0 and abs(bg2 - (-(99.4 / 100 - 1) - 0.0012)) < 1e-12)
    print('provider checks passed')


if __name__ == '__main__':
    main()
