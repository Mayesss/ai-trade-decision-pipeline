"""Registration 010 — cross-venue funding carry mechanics on SYNTHETIC numbers.

    .venv/bin/python -m tests.test_carry
"""
import json
import math
import random
import statistics
import subprocess
import tempfile
from pathlib import Path

from ct import carry

H, D, WK = carry.HOUR, carry.DAY, carry.WEEK
T0 = carry.DISC_START + 30 * D                     # 2023-07-08 00:00 UTC, on the 8-hour grid
YEAR = carry.YEAR


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def close(a, b, tol=1e-12):
    return a is not None and b is not None and abs(a - b) <= tol


def series(rate_fn, interval_h, t0, t1, jitter=0):
    """Settlements every interval_h hours on the UTC grid in [t0, t1); rate_fn(T) -> rate.
    jitter: ms added to each timestamp (venues stamp a few ms after the hour)."""
    out, t = [], t0 - t0 % (interval_h * H)
    while t < t1:
        if t >= t0:
            out.append((t + jitter, rate_fn(t)))
        t += interval_h * H
    return carry.Funding(out)


def flat_gap(coin, a, b, t):
    return 0.0


def no_path(coin, a, b):
    return []


def main():
    print('arithmetic (no data)')
    c = carry.round_trip(('HL', 'BN'), 'BTC')
    check('round trip HL+BN core = 2 x (4.5 + 5.0) bp + 4 x 0.5 bp = 21 bp', close(c, 0.0021))
    check('round trip BY+OK other = 2 x (5.5 + 5.0) + 4 x 2 = 29 bp', close(carry.round_trip(('BY', 'OK'), 'PEPE'), 0.0029))
    h = carry.hold_ms(c)
    check('H = C / m rounded up to 8 h: 21 bp / 6 %/yr = 306.6 h -> 312 h', h == 312 * H, str(h / H))
    th = carry.threshold(c, h)
    check('threshold = C / H + m = 5.90 + 6.00 %/yr', close(th, 0.0021 * 8760 / 312 + 0.06), f'{th:.4%}')
    terms = {p: carry.pair_terms(p, coin)[2] for p in carry.PAIRS for coin in ('BTC', 'PEPE')}
    check('every pair\'s threshold is within [11.8, 12.0] %/yr (twice the margin, minus the rounding)',
          all(0.118 <= v <= 0.12 + 1e-12 for v in terms.values()), str(sorted(round(v, 4) for v in terms.values())))
    check('a spread held at the threshold for H nets exactly 3 %/yr on gross capital',
          close((th * h / YEAR - c) / 2 / (h / YEAR), carry.BENCH))
    check('1x short liquidation distance (1 - mm)/(1 + mm): HL core 95.1 %, BN core 99.0 %',
          close(carry.liq_distance('HL', 'BTC'), 0.975 / 1.025) and close(carry.liq_distance('BN', 'BTC'), 0.995 / 1.005))

    print('funding accrual: hourly vs 8-hourly, entry and exit mid-interval')
    hl = series(lambda T: 0.00001, 1, T0 - 10 * D, T0 + 30 * D, jitter=57)
    bn = series(lambda T: 0.0001, 8, T0 - 10 * D, T0 + 30 * D, jitter=8)
    t_in, t_out = T0 + 3 * H, T0 + 13 * H             # 03:00 -> 13:00: HL 04..13 (10 prints), BN 08:00 (1)
    check('hourly leg: 10 prints in (03:00, 13:00]', close(hl.accrue(t_in, t_out), 10 * 0.00001))
    check('8-hourly leg: only the 08:00 settlement, the full rate although held 5 of its 8 hours',
          close(bn.accrue(t_in, t_out), 0.0001))
    check('entry exactly at a settlement does not collect it; exit exactly at one does',
          close(bn.accrue(T0 + 8 * H, T0 + 16 * H), 0.0001) and close(hl.accrue(T0, T0 + H), 0.00001))
    check('entering and exiting between 8-h stamps collects nothing on the 8-h leg', close(bn.accrue(T0 + H, T0 + 7 * H), 0.0))
    four = series(lambda T: 0.00005, 4, T0 - 10 * D, T0 + 30 * D)
    check('a 4-hourly leg over (03:00, 13:00] collects 04:00, 08:00, 12:00',
          close(four.accrue(t_in, t_out), 3 * 0.00005))
    check('ms jitter in venue stamps is rounded to the hour', hl.t[0] % H == 0 and bn.t[0] % H == 0)
    s = carry.spread(hl, bn, T0)
    check('24-h spread annualised: HL 24 x 1e-5, BN 3 x 1e-4 -> (2.4e-4 - 3e-4) x 365',
          close(s, (24e-5 - 3e-4) * 365), f'{s:.4%}')

    print('no look-ahead: the settlement AT t is never read')
    poison = series(lambda T: 0.5 if T >= T0 else 0.0001, 8, T0 - 10 * D, T0 + 30 * D)
    check('window(t) excludes the print at t (a 50 % poison at t and after changes nothing)',
          close(poison.window(T0), bn.window(T0)))
    check('... and includes the print at t - 8 h', close(poison.window(T0 + 8 * H), 0.0001 * 2 + 0.5))

    class Leaky(carry.Funding):                      # a window that reads the print at t
        def window(self, t, w=carry.W):
            return self.cum[self._idx(t, True)] - self.cum[self._idx(t - w, True)]
    leaky = Leaky([(t, r) for t, r in zip(poison.t, poison.r)])
    check('the same check catches a window that includes t (the leak is detected)',
          not close(leaky.window(T0), bn.window(T0)))
    fund = {('HL', 'X'): series(lambda T: 0.00002, 1, T0 - 10 * D, T0 + 60 * D),
            ('BN', 'X'): series(lambda T: 0.0, 8, T0 - 10 * D, T0 + 60 * D)}
    grid = [T0 + k * carry.GRID for k in range(60 * 3)]
    pos0, _ = carry.plan(['X'], fund, lambda *a: True, grid, T0, T0 + 60 * D)
    t_star = T0 + 20 * D
    fund_p = dict(fund)
    fund_p[('BN', 'X')] = series(lambda T: 0.9 if T >= t_star else 0.0, 8, T0 - 10 * D, T0 + 60 * D)
    pos1, _ = carry.plan(['X'], fund_p, lambda *a: True, grid, T0, T0 + 60 * D)
    early0 = [(p['t_in'], p['short']) for p in pos0 if p['t_in'] <= t_star]
    early1 = [(p['t_in'], p['short']) for p in pos1 if p['t_in'] <= t_star]
    check('plan: poisoning every print at or after t* leaves every decision at or before t* unchanged',
          early0 == early1 and len(early0) > 0, f'{len(early0)} positions')
    late1 = [p for p in pos1 if p['t_in'] > t_star]
    check('... and the poison is visible after t* (short BN once its 0.9 prints enter the window)',
          any(p['short'] == 'BN' for p in late1))

    print('the rule: threshold, direction, one position per coin, edge, pair choice')
    # HL pays 1.5e-5/h = 13.1 %/yr; BN 0 -> spread 13.1 %/yr > 11.9 %: short HL, long BN
    fund = {('HL', 'BTC'): series(lambda T: 1.5e-5, 1, T0 - 10 * D, T0 + 80 * D),
            ('BN', 'BTC'): series(lambda T: 0.0, 8, T0 - 10 * D, T0 + 80 * D),
            ('BY', 'BTC'): series(lambda T: 0.0, 8, T0 - 10 * D, T0 + 80 * D)}
    grid = [T0 + k * carry.GRID for k in range(80 * 3)]
    pos, cnt = carry.plan(['BTC'], fund, lambda *a: True, grid, T0, T0 + 80 * D)
    p0 = pos[0]
    check('spread 13.1 %/yr clears ~11.9 %: entry at the first decision, short HL, long BN',
          p0['t_in'] == T0 and p0['short'] == 'HL' and p0['long'] == 'BN', str((p0['short'], p0['long'])))
    check('same spread on BN and BY: the cheaper round trip (HL-BN 21 bp vs HL-BY 22 bp) wins', p0['pair'] == ('HL', 'BN'))
    check('fixed hold: exit at t_in + H; the next entry is at that exit time, never before',
          all(b['t_in'] == a['t_out'] for a, b in zip(pos, pos[1:])) and p0['t_out'] - p0['t_in'] == 312 * H)
    check('edge: no position whose exit would reach the end; counts hold "opened" only (000f)',
          all(p['t_out'] < T0 + 80 * D for p in pos) and set(cnt) == {'opened'}, str(cnt))
    fund_lo = {**fund, ('HL', 'BTC'): series(lambda T: 1.3e-5, 1, T0 - 10 * D, T0 + 80 * D)}
    pos_lo, _ = carry.plan(['BTC'], fund_lo, lambda *a: True, grid, T0, T0 + 80 * D)
    check('spread 11.4 %/yr (below the threshold) opens nothing', pos_lo == [])
    fund_neg = {**fund, ('HL', 'BTC'): series(lambda T: -1.5e-5, 1, T0 - 10 * D, T0 + 80 * D)}
    pn, _ = carry.plan(['BTC'], fund_neg, lambda *a: True, grid, T0, T0 + 80 * D)
    check('negative spread: short the CEX (higher funding), long HL', pn[0]['short'] == 'BN' and pn[0]['dir'] == -1)
    young = {**fund, ('BN', 'BTC'): series(lambda T: 0.0, 8, T0 - 3 * D, T0 + 80 * D)}
    del young[('BY', 'BTC')]
    py, _ = carry.plan(['BTC'], young, lambda *a: True, grid, T0, T0 + 80 * D)
    check('seasoning: a leg listed 3 days before t is not eligible until 7 days', py[0]['t_in'] == T0 + 4 * D)
    gone = {**fund, ('BN', 'BTC'): series(lambda T: 0.0, 8, T0 - 10 * D, T0 - D)}
    del gone[('BY', 'BTC')]
    pg, _ = carry.plan(['BTC'], gone, lambda *a: True, grid, T0, T0 + 80 * D)
    check('a leg with no print in the last 8 h (delisted) is not eligible', pg == [])
    pe, _ = carry.plan(['BTC'], fund, lambda coin, pair, t: pair != ('HL', 'BN'), grid, T0, T0 + 80 * D)
    check('the caller\'s eligibility (liquidity, name check, entry gap) removes a pair', pe[0]['pair'] == ('HL', 'BY'))

    print('outcome: funding, price gap, fees')
    rate_hl, rate_bn = 2e-5, 0.0
    fund = {('HL', 'BTC'): series(lambda T: rate_hl, 1, T0 - 10 * D, T0 + 80 * D),
            ('BN', 'BTC'): series(lambda T: rate_bn, 8, T0 - 10 * D, T0 + 80 * D)}
    pos, _ = carry.plan(['BTC'], fund, lambda *a: True, grid, T0, T0 + 80 * D)
    p = pos[0]
    o = carry.outcome(p, fund, flat_gap, no_path)
    check('funding: short HL collects 312 hourly prints of 2e-5', close(o['funding'], 312 * 2e-5))
    check('fees: the round trip is charged once, 21 bp of N', close(o['fees'], 0.0021))
    check('net on gross capital = (funding - fees) / 2', close(o['ret'], (312 * 2e-5 - 0.0021) / 2))
    check('hold years = 312 h / 8760 h', close(o['hold_years'], 312 / 8760))

    def gap_move(g_in, g_out):
        def gap(coin, a, b, t):              # ln P_a - ln P_b; HL short leg: positive = HL rich
            v = g_in if t <= p['t_in'] else g_out
            return v if (a, b) == ('HL', 'BN') else -v
        return gap
    o2 = carry.outcome(p, fund, gap_move(0.002, 0.0005), no_path)
    check('price gap: short HL entered 20 bp rich, exit 5 bp rich -> +15 bp of N', close(o2['gap_pnl'], 0.0015))
    o3 = carry.outcome(p, fund, gap_move(0.0005, 0.003), no_path)
    check('price gap: the gap widens against the short leg by 25 bp -> -25 bp', close(o3['gap_pnl'], -0.0025))
    o4 = carry.outcome(p, fund, lambda *a: None, no_path)
    check('no entry gap observation -> no outcome (the plan requires one through eligibility)', o4 is None)

    def gap_stale(coin, a, b, t):
        if t > p['t_out'] - 5 * H:
            return None
        return 0.001 if (a, b) == ('HL', 'BN') else -0.001
    o5 = carry.outcome(p, fund, gap_stale, no_path)
    check('exit gap: the latest observation at or before the exit within 24 h (lag recorded)',
          o5['exit_lag_ms'] == 5 * H and close(o5['gap_pnl'], 0.0))
    o5b = carry.outcome(p, fund, gap_move(0.002, 0.0005), no_path,
                        gap_ok=lambda c, a, b, t: t <= p['t_out'] - 12 * H)
    check('exit gap with a failed name check: the latest observation where both legs still pass, within 24 h',
          o5b['exit_lag_ms'] == 12 * H and close(o5b['gap_pnl'], 0.0015))
    o5c = carry.outcome(p, fund, gap_move(0.002, 0.0005), no_path,
                        gap_ok=lambda c, a, b, t: t <= p['t_out'] - 3 * D)
    check('N4: a failed name check at exit (no valid observation within 24 h) uses the RAW exit gap — the '
          'divergence stays in the primary', o5c is not None and o5c['exit_gap'] == 'raw' and close(o5c['gap_pnl'], 0.0015))

    def gap_gone(coin, a, b, t):
        if t > p['t_out'] - 30 * H:
            return None
        return 0.001 if (a, b) == ('HL', 'BN') else -0.001
    check('... no price at all within 24 h of the exit -> no outcome (the runner fills the worst return of the pair)',
          carry.outcome(p, fund, gap_gone, no_path) is None)

    def fill_after(coin, a, b, t):               # the first trade after the stamp: 30 bp richer at entry
        v = 0.003 if t == p['t_in'] else 0.0005
        return v if (a, b) == ('HL', 'BN') else -v
    of = carry.outcome(p, fund, gap_move(0.002, 0.0005), no_path, fill=fill_after)
    check('N6: entry and exit fill at the post-settlement price (fill), not the pre-settlement close',
          close(of['gap_in'], 0.003) and close(of['gap_pnl'], 0.0025))
    holed = {**fund, ('BN', 'BTC'): carry.Funding([(t, r) for t, r in zip(fund[('BN', 'BTC')].t, fund[('BN', 'BTC')].r)
                                                    if t != p['t_in'] + 2 * D])}
    oh = carry.outcome(p, holed, flat_gap, no_path)
    hs, nh = holed[('BN', 'BTC')].holes(p['t_in'], p['t_out'])
    excluded = sum(1 for T in range(p['t_in'] + H, p['t_out'] + H, H) if any(a < T <= b for a, b in hs))
    check('m-d: a missing BN settlement is a hole: counted, and both legs leave the accrual inside it',
          oh['funding_holes'] == 1 and nh == 1 and excluded == 8
          and close(oh['funding'], (312 - 8) * 2e-5), f"holes {oh['funding_holes']}, excluded HL prints {excluded}")
    # price in units: short P_A, long P_B, equal size q; P&L = q (A_in - A_out) + q (B_out - B_in)
    A_in, B_in, A_out, B_out = 101.0, 100.0, 103.0, 102.5
    exact = ((A_in - A_out) + (B_out - B_in)) / 100.5
    approx = (math.log(A_in) - math.log(B_in)) - (math.log(A_out) - math.log(B_out))
    check('log-gap change approximates the two legs\' price P&L; the error is second order '
          '(~ price move x gap P&L: 1 bp on 50 bp at a 2 % move)',
          abs(exact - approx) <= 0.03 * abs(exact), f'{exact:.6f} vs {approx:.6f}')

    def path_from(prices):
        def path(coin, a, b):
            return [(t, hi, lo, cl) for t, hi, lo, cl in prices if a <= t <= b]
        return path
    rising = [(p['t_in'] + k * 8 * H, 100.0 + k, 100.0 + k, 100.0 + k) for k in range(40)]
    ow = carry.outcome(p, fund, flat_gap, path_from(rising))
    want = sum(2e-5 * (100.0 + (T - p['t_in']) // (8 * H)) / 100.0 for T in range(p['t_in'] + H, p['t_out'] + H, H))
    check('funding is paid on size x price: each settlement scaled by the latest path close / entry close',
          close(ow['funding'], want, 1e-12) and ow['funding'] > 312 * 2e-5, f"{ow['funding']:.6f} vs {312 * 2e-5:.6f}")

    print('delisting, top-ups, liquidation')
    fund_d = {**fund, ('BN', 'BTC'): series(lambda T: 0.0, 8, T0 - 10 * D, T0 + 5 * D)}
    od = carry.outcome(p, fund_d, flat_gap, no_path)
    check('a leg that stops settling closes the position at its last settlement',
          od['delisted'] and od['t_x'] == T0 + 5 * D - 8 * H and close(od['funding'], (5 * 24 - 8) * 2e-5))
    flat = [(p['t_in'] + k * 8 * H, 100.0, 100.0, 100.0) for k in range(40)]
    o6 = carry.outcome(p, fund, flat_gap, path_from(flat))
    check('a flat path: no top-up, no liquidation', o6['topups'] == 0 and not o6['liquidated'] and o6['max_up'] == 0)
    spike = list(flat)
    spike[5] = (spike[5][0], 160.0, 100.0, 150.0)       # +60 % high: short leg top-up at the bar close
    o7 = carry.outcome(p, fund, flat_gap, path_from(spike))
    check('+60 % on the short leg forces one rebalance: an extra round trip + 10 bp on the moved 60 %',
          o7['topups'] == 1 and close(o7['topup_cost'], 0.0021 + 0.001 * 0.6))
    crash = flat[:5] + [(flat[5][0], 100.0, 45.0, 50.0)] + [(t, 50.0, 50.0, 50.0) for t, *_ in flat[6:]]
    # -55 % low, then stays at 50: the long leg needs margin moved, once
    o8 = carry.outcome(p, fund, flat_gap, path_from(crash))
    check('-55 % on the long leg also forces a rebalance (its loss is the short leg\'s gain on another venue)',
          o8['topups'] == 1 and not o8['liquidated'])
    boom = list(flat)
    boom[5] = (boom[5][0], 200.0, 100.0, 190.0)          # +100 % high, closes +90 %: HL short liquidated
    o9 = carry.outcome(p, fund, flat_gap, path_from(boom))
    check('B3: +100 % within one 8-h bar liquidates the 1x short: it loses its whole margin (-N), the long leg '
          'gains to the bar close (+90 %), the gap term is dropped: price P&L = -1 + 0.9 = -10 % of N',
          o9['liquidated'] and o9['t_x'] == boom[5][0] and close(o9['gap_pnl'], -0.1, 1e-12))
    check('... net = (funding to the liquidation bar - 10 % - full fees) / 2',
          close(o9['ret'], (o9['funding'] - 0.1 - 0.0021) / 2, 1e-12) and o9['funding'] > 0, f"{o9['ret']:.5f}")
    wick = list(flat)
    wick[5] = (wick[5][0], 200.0, 100.0, 110.0)          # a wick: +100 % high, closes +10 %
    o10 = carry.outcome(p, fund, flat_gap, path_from(wick))
    check('B3: a liquidating wick that closes +10 % costs -90 % of N (the long leg gains only 10 %)',
          o10['liquidated'] and close(o10['gap_pnl'], -0.9, 1e-12))
    up_then = flat[:5] + [(flat[5][0], 160.0, 100.0, 150.0)] + [(t, 150.0, 150.0, 150.0) for t, *_ in flat[6:]]
    ou = carry.outcome(p, fund, flat_gap, path_from(up_then))
    pre = sum(1 for T in range(p['t_in'] + H, flat[5][0] + H, H))
    post = sum(1 for T in range(flat[5][0] + H, p['t_out'] + H, H))
    want_u = 2e-5 * (pre - 1 + 1.5 + post)   # the stamp AT the rebalance is paid on the old size at 150 (1.5);
    #                                          after it the legs are N again: weight 150/150
    check('m3: the funding weight resets after a rebalance (P(T) / P(rebalance close), not / P(entry))',
          ou['topups'] == 1 and close(ou['funding'], want_u, 1e-12), f"{ou['funding']:.6f} vs {want_u:.6f}")

    print('planted persistent spread earns; noise loses its costs')
    rng = random.Random(1)
    coins = [f'C{k}' for k in range(30)]
    end = T0 + 540 * D
    grid = [T0 + k * carry.GRID for k in range(540 * 3)]
    fund = {}
    for k, cn in enumerate(coins):
        lvl = 4e-5 if k % 2 == 0 else -4e-5               # 35 %/yr, persistent, both signs
        fund[('HL', cn)] = series(lambda T, lvl=lvl: lvl + rng.gauss(0, 2e-6), 1, T0 - 10 * D, end + 30 * D)
        fund[('BN', cn)] = series(lambda T: 0.0, 8, T0 - 10 * D, end + 30 * D)
    pos, _ = carry.plan(coins, fund, lambda *a: True, grid, T0, end)
    rows = [carry.outcome(q, fund, flat_gap, no_path) for q in pos]
    sm = carry.summarise(rows, placebo_seeds=50)
    check('persistent 35 %/yr spread: all six criteria pass', sm['passed'], json.dumps(sm['criteria']))
    check('... annualised net on gross capital ~ (35 % - 21 bp / 13 d) / 2 ~ 14.5 %/yr',
          13 < 100 * sm['annualised'] < 16, f"{sm['annualised']:.2%}")
    fund_n = {}
    for cn in coins:                                       # iid noise, 8-hour blocks, sd 40 %/yr
        blocks = {}

        def r(T, blocks=blocks):
            b = T // (8 * H)
            if b not in blocks:
                blocks[b] = rng.gauss(0, 0.40) / 8760
            return blocks[b]
        fund_n[('HL', cn)] = series(r, 1, T0 - 10 * D, end + 30 * D)
        fund_n[('BN', cn)] = series(lambda T: 0.0, 8, T0 - 10 * D, end + 30 * D)
    pos_n, _ = carry.plan(coins, fund_n, lambda *a: True, grid, T0, end)
    rows_n = [carry.outcome(q, fund_n, flat_gap, no_path) for q in pos_n]
    sn = carry.summarise(rows_n, placebo_seeds=50)
    mean_f = statistics.fmean(r_['funding'] for r_ in rows_n)
    check('iid noise: the 24-h signal predicts nothing; funding mean ~ 0 per position',
          abs(mean_f) < 0.0004, f'{mean_f:.5f} on N over {len(rows_n)} positions')
    check('... and the trade loses about its round trip: mean net ~ -C/2 on gross capital, no pass',
          not sm is sn and not sn['passed'] and -0.0016 < sn['primary_week']['mean'] < -0.0005,
          f"{sn['primary_week']['mean']:.5f}")

    print('placebo calibration and criteria')
    pl = carry.placebo(rows_n, seeds=400, flip_key='month')
    check('placebo (direction reversed per MONTH, month-clustered t, critical t_{G-1}(0.975)) on noise: '
          'the |t| share on the flipped gross is near its nominal 5 % (<= 9 %)', pl['gross_abs_share'] <= 0.09, str(pl))
    check('... and with costs kept the pass share is ~0 by construction (reported as such, m6)',
          pl['pass_share_costs_kept'] <= 0.05, str(pl))
    plw = carry.placebo(rows_n, seeds=400, flip_key='week')
    check('the week-flip version (|t| at 1.96) is also near nominal (<= 9 %)', plw['gross_abs_share'] <= 0.09, str(plw))
    pl2 = carry.placebo(rows, seeds=200)
    check('placebo on the planted spread: the reversed trade does not pass (pass share <= 5 %)',
          pl2['pass_share_costs_kept'] <= 0.05, str(pl2))
    syn = []
    for k in range(200):
        t = T0 + k * 3 * D
        syn.append({'coin': 'X', 't_in': t, 'week': carry.week_of(t), 'month': carry.month_of(t),
                    'ret': 0.001 + random.Random(k).gauss(0, 0.0005), 'hold_years': 13 / 365,
                    'gross': 0.002, 'costs': 0.001})
    crit, det = carry.criteria(syn)
    check('a clean positive series passes criteria 1-4 but not 5 (0.1 % per 13 days = 2.8 %/yr < 3 %)',
          all(v for k, v in crit.items() if not k.startswith('5')) and not crit['5_annualised_ge_benchmark'],
          f"{det['annualised']:.4f}")
    big = [dict(r_, ret=r_['ret'] + (0.5 if r_['month'] == syn[100]['month'] else 0.0)) for r_ in syn]
    neg = [dict(r_, ret=-0.0005) for r_ in syn]
    neg_big = [dict(r_, ret=r_['ret'] + (2.0 if r_['month'] == syn[100]['month'] else 0.0)) for r_ in neg]
    cb, db = carry.criteria(neg_big)
    check('criterion 4: one huge positive month carries the mean; removing it fails the trial',
          db['primary_week']['mean'] > 0 and not cb['4_without_largest_positive_month_positive'])
    neg_loss = [dict(r_, ret=r_['ret'] - (2.0 if r_['month'] == syn[100]['month'] else 0.0)) for r_ in syn]
    cl, dl = carry.criteria(neg_loss)
    check('criterion 4 never removes a loss month (largest POSITIVE month is removed)',
          dl['largest_positive_month'] != syn[100]['month'])
    corr = []
    for k in range(200):                     # month-level common shocks: week t looks fine, month t does not
        t = T0 + k * 3 * D
        mshock = random.Random(carry.month_of(t)).gauss(0, 0.01)
        corr.append(dict(syn[k], ret=0.0012 + mshock + random.Random(10 + k).gauss(0, 0.0002)))
    cc, dc = carry.criteria(corr)
    check('criterion 1b: month-level dependence that week clusters miss lowers the month t',
          dc['primary_month']['t'] < dc['primary_week']['t'], f"week t {dc['primary_week']['t']:.2f}, month t {dc['primary_month']['t']:.2f}")
    half = [dict(r_, ret=(0.002 if i < 100 else -0.0005)) for i, r_ in enumerate(syn)]
    ch, _ = carry.criteria(half)
    check('criterion 3: a second half below zero fails', not ch['3_both_halves_positive'])
    check('criterion 1b uses t_{G-1}(0.975): 19 months -> 2.101', abs(carry.t_crit(18, 0.975) - 2.101) < 1e-9)
    check('decide: a pass on either trial -> read_holdout; otherwise close',
          carry.decide({'passed': False}, {'passed': True}) == 'read_holdout'
          and carry.decide({'passed': False}, {'passed': False}) == 'close')
    small = [dict(r_, ret=0.0004 + random.Random(50 + i).gauss(0, 0.0005)) for i, r_ in enumerate(syn)]
    ok, det_h = carry.holdout_confirm(small)
    check('N5: holdout CONFIRM is an existence test — a clean 1.1 %/yr series confirms although it is below the '
          '3 %/yr bar, which is only reported; month t at t_{G-1}(0.95)',
          ok and det_h['annualised_reported'] < carry.BENCH and det_h['z'] == carry.t_crit(det_h['primary_month']['clusters'] - 1, 0.95),
          f"{det_h['criteria']} ann {det_h['annualised_reported']:.4f}")
    okb, det_b = carry.holdout_confirm(big)
    check('holdout_confirm: one huge month carries the mean -> the month-clustered t fails',
          not okb and not det_b['criteria']['month_t'], str(det_b['criteria']))
    ok2, _ = carry.holdout_confirm(neg)
    check('holdout_confirm on a negative series: no', not ok2)
    weak = [dict(r_, ret=0.0001 + random.Random(70 + i).gauss(0, 0.0009) + random.Random(r_['month']).gauss(0, 0.0003))
            for i, r_ in enumerate(syn)]
    okw, detw = carry.holdout_confirm(weak, carry.Q_CONFIRM_TWO_LOOKS)
    tw, zw = detw['primary_month']['t'], detw['z']
    check('N5 extension: due only when the first look ends with 0 < month t < t_{G-1}(0.975) (each look at 0.975)',
          carry.extension_due(detw) == (0 < tw < zw) and abs(zw - carry.t_crit(detw['primary_month']['clusters'] - 1, 0.975)) < 1e-12
          and not carry.extension_due({'primary_month': {'t': 3.5}, 'z': 3.18})
          and not carry.extension_due({'primary_month': {'t': -0.3}, 'z': 3.18})
          and carry.extension_due({'primary_month': {'t': 2.5}, 'z': 3.18}), f'month t {tw:.2f}, z {zw:.2f}')
    rng_n = random.Random(99)                   # Revision 2a: null false-confirm with 4 month clusters
    four = sorted({r_['month'] for r_ in syn})[:4]
    base4 = [r_ for r_ in syn if r_['month'] in four]
    hits = 0
    for k in range(2000):
        shocks = {m: rng_n.gauss(0, 0.001) for m in four}
        rows4 = [dict(r_, ret=shocks[r_['month']] + rng_n.gauss(0, 0.002)) for r_ in base4]
        hits += carry.holdout_confirm(rows4)[0]
    check('Revision 2a: under the null with 4 month clusters (month-level shocks), the holdout false-confirm '
          'rate at t_3(0.95) = 2.353 is about 5 % or less', hits / 2000 <= 0.06, f'{hits / 2000:.3f} of 2,000')

    print('recheck of a 010 discovery row from its numbers (N2)')
    row = {'T1': carry.summarise(rows, placebo_seeds=0), 'T2': carry.summarise(rows[:200], placebo_seeds=0)}
    row['decision'] = carry.decide(row['T1'], row['T2'])
    row = json.loads(json.dumps(row, default=str))
    dec, fails = carry.recheck_discovery(row)
    check('a genuine row recomputes to its stored decision with no failures', dec == row['decision'] and not fails,
          f'{dec} {fails}')
    forged = json.loads(json.dumps(row))
    forged['T1']['annualised'] = 0.01
    dec2, fails2 = carry.recheck_discovery(forged)
    check('a row whose numbers no longer support its labels is caught', fails2 != [], str(fails2))
    forged2 = json.loads(json.dumps(row))
    forged2['decision'] = 'read_holdout' if row['decision'] == 'close' else 'close'
    check('a forged decision label is caught', carry.recheck_discovery(forged2)[1] != [])

    print('power simulation')
    plan_pos = [dict(q, hold_years=q['hold_ms'] / YEAR) for q in pos]
    sig_s, sig_g = {'core': 0.15, 'other': 0.15}, {'core': 0.001, 'other': 0.001}
    p_big = carry.simulate_power(plan_pos, sig_s, sig_g, 0.3, 0.20, sims=100)
    p_none = carry.simulate_power(plan_pos, sig_s, sig_g, 0.3, 0.0, sims=200, seed=2)
    check('a 20 %/yr planted edge passes in most simulations', p_big['p_pass'] > 0.8, str(p_big))
    check('no edge passes rarely (criterion 5 alone needs 3 %/yr)', p_none['p_pass'] < 0.03, str(p_none))
    p_iid = carry.simulate_power(plan_pos, sig_s, sig_g, 0.3, 0.05, sims=100, persist=carry.persist_iid)
    p_per = carry.simulate_power(plan_pos, sig_s, sig_g, 0.3, 0.05, sims=100)
    p_reg = carry.simulate_power(plan_pos, sig_s, sig_g, 0.3, 0.05, sims=100, sigma_month=0.30)
    check('a monthly regime shock (30 %/yr) lowers power at the same edge (M1)', p_reg['p_pass'] < p_per['p_pass'],
          f"{p_reg['p_pass']} vs {p_per['p_pass']}")
    h_big = carry.simulate_holdout(plan_pos, 120 * D, sig_s, sig_g, 0.3, 0.20, sims=100)
    h_none = carry.simulate_holdout(plan_pos, 120 * D, sig_s, sig_g, 0.3, 0.0, sims=100, seed=3)
    check('holdout simulation: contiguous 120-day windows of the pattern; a 20 %/yr edge confirms; at 0 the '
          'false-confirm rate is about 5 % (t_{G-1}(0.95))',
          h_big['p_confirm'] > 0.8 and h_none['p_confirm'] < 0.1 and h_big['mean_positions'] > 50,
          f"{h_big} / {h_none['p_confirm']}")
    j_big = carry.simulate_joint(plan_pos, plan_pos, 120 * D, sig_s, sig_g, 0.3, 0.20, sims=60)
    j_none = carry.simulate_joint(plan_pos, plan_pos, 120 * D, sig_s, sig_g, 0.3, 0.0, sims=60, seed=4)
    j_ext = carry.simulate_joint(plan_pos, plan_pos, 120 * D, sig_s, sig_g, 0.3, 0.20, sims=60, extension_span_ms=240 * D)
    check('joint power (discovery x confirm): a 20 %/yr edge goes through both; none essentially never; the '
          'extension path runs', j_big['p_joint'] > 0.8 and j_none['p_joint'] < 0.05 and j_ext['p_joint'] > 0.8,
          f"{j_big['p_joint']} / {j_none['p_joint']} / {j_ext['p_joint']}")
    check('the independent-window noise bound is less noisy than the persistent bound',
          p_iid['annualised_sd'] <= p_per['annualised_sd'], f"{p_iid['annualised_sd']:.4f} vs {p_per['annualised_sd']:.4f}")
    check('week index: Monday 00:00 UTC starts a week',
          carry.week_of(1_704_067_200_000) == carry.week_of(1_704_067_200_000 + 6 * D + 23 * H)
          and carry.week_of(1_704_067_200_000 + 7 * D) == carry.week_of(1_704_067_200_000) + 1)

    panel_checks()
    guard_checks()
    print('all checks passed')


def panel_checks():
    """r010_run.Panel on a synthetic panel: per-venue liquidity floor, tiered slippage and name check
    read only days before t; last-trade gap; a DELISTED Bybit coin passes its name check while alive
    (Revision 1, B2)."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import r010_run
    print('panel loaders (synthetic parquet)')
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        t0 = T0
        days = [t0 + k * D for k in range(-40, 30)]
        dead = t0 + 10 * D                               # DEAD stops trading on Bybit here

        def write(name, cols, rows):
            pq.write_table(pa.table([pa.array([r[i] for r in rows]) for i in range(len(cols))], names=cols),
                           root / f'{name}.parquet')
        (root / 'universe.json').write_text(json.dumps({'BTC': {'BN': 'BTCUSDT', 'BY': 'BTCUSDT', 'HL': 'BTC'},
                                                        '1000PEPE': {'BN': '1000PEPEUSDT', 'HL': 'kPEPE'},
                                                        'DEAD': {'BN': 'DEADUSDT', 'BY': 'DEADUSDT'}}))
        fund = [(v, c, t0 + k * 8 * H, 0.0001) for v, c in (('BN', 'BTC'), ('BY', 'BTC'), ('HL', 'BTC'),
                                                          ('BN', '1000PEPE'), ('HL', '1000PEPE'), ('BN', 'DEAD'))
                for k in range(-60, 80)]
        fund += [('BY', 'DEAD', t0 + k * 8 * H, 0.002) for k in range(-60, 80) if t0 + k * 8 * H < dead]
        write('funding', ['venue', 'coin', 't', 'rate'], fund)
        write('prices', ['venue', 'coin', 't', 'lp', 'lpo'],
              [('BN', 'BTC', t0, math.log(100.0), math.log(100.5)), ('BY', 'BTC', t0, math.log(100.3), math.log(101.0)),
               ('HL', 'BTC', t0, math.log(100.1), None)]
              + [(v, 'DEAD', t0 + k * 8 * H, math.log(5.0), math.log(5.0)) for v in ('BN', 'BY') for k in range(-60, 80)
                 if v == 'BN' or t0 + k * 8 * H < dead])
        write('path', ['coin', 't', 'high', 'low', 'close'], [('BTC', t0, 100.0, 100.0, 100.0)])
        vol = []
        for dd in days:
            vol.append(('BN', 'BTC', dd, 2e9 if dd < t0 else 0.0))       # tier 1 bp before t0, 0 after
            vol.append(('BY', 'BTC', dd, 3e8 if dd < t0 else 0.0))       # tier 2 bp
            vol.append(('HL', 'BTC', dd, 6e7 if dd < t0 else 0.0))       # tier 4 bp
            vol.append(('BN', '1000PEPE', dd, 2e7))
            vol.append(('HL', '1000PEPE', dd, 5e6))                       # HL leg below the floor
            vol.append(('BN', 'DEAD', dd, 2e7))
            if dd < dead:
                vol.append(('BY', 'DEAD', dd, 2e7))
        write('volume', ['venue', 'coin', 'day', 'qvol'], vol)
        lv = []
        for dd in days:
            lv.append(('BN', '1000PEPE', dd, 0.01))
            lv.append(('HL', '1000PEPE', dd, 0.01 if dd < t0 else 0.03))   # HL kPEPE: same scale, then 3x off
            lv.append(('BN', 'BTC', dd, 100.0))
            lv.append(('BY', 'BTC', dd, 100.2))
            lv.append(('HL', 'BTC', dd, 100.1))
            lv.append(('BN', 'DEAD', dd, 5.0))
            if dd < dead:
                lv.append(('BY', 'DEAD', dd, 5.0))
        write('levels', ['venue', 'coin', 't', 'close'], lv)
        old = r010_run.PANEL, r010_run.ALLOWED
        r010_run.PANEL = r010_run.ALLOWED = root          # the synthetic panel stands in for data/carry/
        try:
            pnl = r010_run.Panel(carry.DISC_END)
        finally:
            r010_run.PANEL, r010_run.ALLOWED = old
        check('liquidity floor per leg: the 30-day MEAN before t\'s day (the $0 days from t0 on are not read at t0)',
              pnl.liq_ok('BTC', 'HL', t0 + 16 * H) and not pnl.liq_ok('BTC', 'HL', t0 + 28 * D))
        check('M3: tiered slippage per leg from its own venue volume: BN 1 bp, BY 2 bp, HL 4 bp',
              (pnl.slip('BTC', 'BN', t0), pnl.slip('BTC', 'BY', t0), pnl.slip('BTC', 'HL', t0)) == (0.0001, 0.0002, 0.0004))
        check('an HL leg below $10 M of its own volume is not eligible', not pnl.eligible(False)('1000PEPE', ('HL', 'BN'), t0))
        check('M2: HL name check from HL closes (same scale passes; 3x off fails once in the 7-day window; '
              'closes at and after t are not read)',
              pnl.name_ok('HL', '1000PEPE', t0) and not pnl.name_ok('HL', '1000PEPE', t0 + 8 * D))
        check('gap = ln last_a - ln last_b on every pair (BY 30 bp rich of BN; HL 10 bp)',
              close(pnl.gap('BTC', 'BY', 'BN', t0), math.log(100.3 / 100.0))
              and close(pnl.gap('BTC', 'HL', 'BN', t0), math.log(100.1 / 100.0)))
        check('no observation -> None (eligibility then refuses the entry)', pnl.gap('BTC', 'HL', 'BN', t0 + 8 * H) is None)
        check('N6: the fill is the open of the bar starting at t (after the settlement), on both venues',
              close(pnl.fill('BTC', 'BY', 'BN', t0), math.log(101.0 / 100.5)) and pnl.fill_fallbacks == 0)
        check('... where an open is missing the close at t is used and counted',
              close(pnl.fill('BTC', 'HL', 'BN', t0), math.log(100.1 / 100.0)) and pnl.fill_fallbacks == 1)
        el = pnl.eligible(True)
        check('B2: a Bybit coin that is later delisted passes its name check while it trades (point in time)',
              pnl.name_ok('BY', 'DEAD', dead - D) and el('DEAD', ('BN', 'BY'), dead - D))
        check('... and fails after its last close (no closes in the 7-day window)', not pnl.name_ok('BY', 'DEAD', dead + 8 * D))
        grid_ = r010_run.grid(t0 - 20 * D, dead + 20 * D)
        pos_d, _ = carry.plan(['DEAD'], pnl.funding, el, grid_, t0 - 20 * D, dead + 40 * D, slip=pnl.slip)
        check('... so the rule trades it before delisting (BY funding 2e-3/8h >> BN): short BY, long BN',
              len(pos_d) >= 1 and pos_d[0]['short'] == 'BY' and pos_d[0]['t_in'] < dead, str(len(pos_d)))
        check('Panel refuses to read prices past discovery', _raises(lambda: r010_run.Panel(carry.EXPOSED_END)))
        hp = root / 'panel_holdout_2026-12-31'
        hp.mkdir()
        for f in root.glob('*.*'):
            (hp / f.name).write_bytes(f.read_bytes())
        r010_run.ALLOWED = root
        try:
            check('N7: counting / describing code never opens a holdout panel (allow_holdout is False by default)',
                  _raises(lambda: r010_run.Panel(carry.DISC_END, panel_dir=hp))
                  and not _raises(lambda: r010_run.Panel(carry.DISC_END, panel_dir=hp, allow_holdout=True)))
        finally:
            r010_run.PANEL, r010_run.ALLOWED = old
        data = Path(r010_run.__file__).parent / 'data'
        for bad in ('binance_years', 'binance_years_holdoutA', 'scout_l2', 'binance', 'cache'):
            check(f'000g whitelist: Panel refuses data/{bad}/', _raises(lambda b=bad: r010_run.Panel(carry.DISC_END, panel_dir=data / b)))
        check('000g whitelist: data/carry/ itself is accepted (the registered panel loads)',
              not _raises(lambda: r010_run.Panel(carry.DISC_END, panel_dir=data / 'carry' / 'panel')))
        import r010_fetch
        check('000g whitelist: the fetcher reads and writes only under data/carry/ (its name lists are snapshots in '
              'data/carry/inputs/)', r010_fetch.OUT.resolve() == r010_fetch.ALLOWED.resolve()
              and r010_fetch.INPUTS.resolve().is_relative_to(r010_fetch.ALLOWED.resolve()))
        src = {f: (Path(r010_run.__file__).parent / f).read_text() for f in
               ('ct/carry.py', 'r010_run.py', 'r010_fetch.py', 'r010_build.py', 'r010_describe.py')}
        check('000g whitelist: no 010 source names scout_l2, binance_years or data/binance outside the FORBIDDEN '
              'example constant', all(t.count('scout_l2') == 0 and t.count("'data' / 'binance'") == 0
                                      and t.count('binance_years') == (1 if f == 'r010_run.py' else 0)
                                      for f, t in src.items()),
              str({f: (t.count('scout_l2'), t.count('binance_years')) for f, t in src.items()}))


def _raises(f):
    try:
        f()
    except AssertionError:
        return True
    return False


def _row009(decision, passing=False, mode='discovery', commit='', sha=None):
    """A 009 row whose numbers support (or, with passing, contradict) the label."""
    from ct import factors as fx
    if passing:
        ev = {'ic_mean': 0.03, 'ic_t': 4.0, 'net_mean': 0.004, 'net_t': 4.0, 'net_median': 0.003, 'halves': [0.003, 0.002],
              'without_largest_positive_month': 0.003,
              'criteria': {'1_ic_mean_pos_nw_t': True, '2_book_net_pos_nw_t': True, '3_mean_median_same_sign': True,
                           '4_both_halves_positive': True, '5_without_largest_positive_month_positive': True},
              'pass': True, 'pull_conditions': {'all': False}}
    else:
        ev = {'ic_mean': -0.01, 'ic_t': -1.0, 'net_mean': -0.001, 'net_t': -1.0, 'net_median': -0.001,
              'halves': [-1.0, -1.0], 'without_largest_positive_month': -0.001,
              'criteria': {'1_ic_mean_pos_nw_t': False, '2_book_net_pos_nw_t': False, '3_mean_median_same_sign': True,
                           '4_both_halves_positive': False, '5_without_largest_positive_month_positive': False},
              'pass': False, 'pull_conditions': {'all': False}}
    row = {'registration': '009-binance-weekly-factors.md', 'mode': mode, 'registration_commit': commit}
    if mode == 'discovery':
        row.update(results={f: ev for f in fx.FAMILY}, decision=decision)
    else:
        row['discovery_row_sha256'] = sha
    return row


def _row010(code_hashes, commit, good=True):
    """A 010 discovery row (from synthetic positions) whose numbers support read_holdout."""
    rows = []
    for k in range(240):
        t = T0 + k * 3 * D
        rows.append({'coin': 'X', 't_in': t, 'week': carry.week_of(t), 'month': carry.month_of(t),
                     'ret': (0.002 if good else -0.001) + random.Random(k).gauss(0, 0.0003), 'hold_years': 13 / 365,
                     'gross': 0.003, 'costs': 0.001})
    row = {'registration': '010-x.md', 'mode': 'discovery', 'registration_commit': commit, 'code_hashes': code_hashes,
           'T1': carry.summarise(rows, placebo_seeds=0), 'T2': carry.summarise(rows, placebo_seeds=0)}
    row['decision'] = carry.decide(row['T1'], row['T2'])
    return json.loads(json.dumps(row, default=str))


def guard_checks():
    """r010_run's guard in a throwaway git repo: (a)-(f), the status, (h) the 000g ordering with the
    reviewer's bypass probes inverted (N3), the owner's lines (N1, incl. the real registration), and
    the holdout gate (g1)-(g4), (h'), (t), (i) (N2)."""
    import r010_run
    code, data = ('ct/carry.py', 'r010_run.py'), ('data/c.json',)
    reg_rel = 'registrations/010-x.md'
    print('guard (throwaway git repo)')
    real = (Path(r010_run.__file__).parent / 'registrations' / '010-cross-venue-funding-carry.md').read_text()
    end_r, date_r, ext_r, f_r = r010_run.owner_choice(real)
    check('N1, the REAL registration: exactly one "Holdout end (owner):" line and one "Holdout extension (owner):" '
          'line, set by the owner at REGISTERED (2026-12-31, YES) and parsed as such (no instruction or quote matches)',
          len(r010_run.END_PREFIX.findall(real)) == 1 and len(r010_run.EXT_PREFIX.findall(real)) == 1
          and end_r == 1_798_761_600_000 and ext_r is True and f_r == [], str(f_r))
    base = 'x\n\nHoldout end (owner): {e}\n\nHoldout extension (owner): {x}\n'
    ok_ = r010_run.owner_choice(base.format(e='2026-12-31', x='NO'))
    check('N1: a chosen offered date and NO -> accepted, end exclusive 2027-01-01', ok_[0] == 1_798_761_600_000 and ok_[2] is False
          and ok_[3] == [])
    check('N1: a date outside the offer refuses', r010_run.owner_choice(base.format(e='2026-10-31', x='NO'))[3] != [])
    check('N1: two end lines refuse (exactly one required)',
          r010_run.owner_choice(base.format(e='2026-09-30', x='NO') + 'Holdout end (owner): 2026-12-31\n')[3] != [])
    check('N1: a quoted or indented instruction does not count as the line',
          r010_run.owner_choice('the owner writes `Holdout end (owner): 2026-09-30` here\n  Holdout end (owner): 2026-09-30\n'
                                + 'Holdout extension (owner): NO\n')[0] is None)
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root,
                                        capture_output=True, text=True, check=True)
        head = lambda: git('rev-parse', 'HEAD').stdout.strip()
        git('init', '-q')
        for f, body in (('ct/carry.py', 'x = 1\n'), ('r010_run.py', 'y = 2\n'), ('data/c.json', '{}\n'),
                        ('results.jsonl', json.dumps({'registration': '005-y.md'}) + '\n')):
            (root / f).parent.mkdir(parents=True, exist_ok=True)
            (root / f).write_text(body)
        (root / '.gitignore').write_text('data/\n')

        def write_reg(status='REGISTERED', extra=''):
            hashes = {f: r010_run.sha256(root / f) for f in (*code, *data)}
            rows = '\n'.join(f'| `{f}` | `{h}` |' for f, h in hashes.items())
            (root / reg_rel).parent.mkdir(exist_ok=True)
            (root / reg_rel).write_text(f'# 010\n\n**Status: {status} 2026-10-11**\n\n| file | sha256 |\n|---|---|\n{rows}\n{extra}')
        fails = lambda mode='discovery', now=None: r010_run.guard_failures(root, reg_rel, code, data, mode=mode, now_ms=now)
        add = lambda row: open(root / 'results.jsonl', 'a').write(json.dumps(row) + '\n')
        write_reg()
        git('add', '.gitignore', 'ct', 'r010_run.py', 'results.jsonl')
        git('commit', '-q', '-m', 'code')
        f0, _ = fails()
        check('guard (a): an untracked registration is refused',
              any(x.startswith('(a)') for x in f0) and any(x.startswith('(c)') for x in f0), str(f0))
        git('add', reg_rel)
        f1, _ = fails()
        check('guard (c): staged but never committed -> refused', any(x.startswith('(c)') for x in f1), str(f1))
        git('commit', '-q', '-m', 'register')
        f2, c2 = fails()
        check('guard: committed, clean, hashes match -> only the 000g ordering (h) refuses',
              [x for x in f2 if not x.startswith('(h)')] == [] and any(x.startswith('(h)') for x in f2) and len(c2) == 40, str(f2))
        h_ = lambda: [x for x in fails()[0] if x.startswith('(h)')]
        reg_c = head()

        # --- (h) the 000g ordering: the reviewer's bypass probes must refuse (N3)
        add(_row009(['close', []], commit=reg_c))
        check('N3 probe 1: an UNCOMMITTED 009 close row unlocks nothing', h_() != [] and r010_run.ordering_state(root) is None)
        git('checkout', '--', 'results.jsonl')
        git('checkout', '-q', '-b', 'side')
        add(_row009(['close', []], commit=reg_c))
        add(_row009(None, mode='holdout_A', commit=reg_c, sha='0' * 64))
        git('commit', '-q', '-am', 'rows on a side branch')
        git('checkout', '-q', 'master') if git('branch', '--list', 'master').stdout.strip() else git('checkout', '-q', 'main')
        check('N3 probe 2: 009 rows committed only on another branch unlock nothing', h_() != [])
        add(_row009(['close', []], commit='f' * 40))
        git('commit', '-q', '-am', 'row with a foreign commit')
        check('N3 probe 3: a committed 009 row whose registration commit is not an ancestor of HEAD unlocks nothing', h_() != [])
        git('reset', '-q', '--hard', 'HEAD~1')
        add(_row009(['close_conditions_unmet', ['F1']], commit=reg_c))
        git('commit', '-q', '-am', 'mislabelled')
        check('N3 probe 4: a label the stored numbers do not support (close_conditions_unmet on failing numbers) '
              'unlocks nothing', h_() != [])
        git('reset', '-q', '--hard', 'HEAD~1')
        add(_row009(['pull_holdout_A', ['F1']], passing=True, commit=reg_c))
        git('commit', '-q', '-am', '009 passes, pull conditions unmet')
        check('N3: a 009 row whose numbers pass but stores pull_holdout_A (recomputed: close_conditions_unmet) unlocks nothing',
              h_() != [])
        git('reset', '-q', '--hard', 'HEAD~1')
        add(_row009(['close_conditions_unmet', ['F1']], passing=True, commit=reg_c))
        git('commit', '-q', '-am', '009 close_conditions_unmet')
        check('N3: a committed, verified close_conditions_unmet unlocks', r010_run.ordering_state(root) == 'close_conditions_unmet')
        git('reset', '-q', '--hard', 'HEAD~1')
        add(_row009(['pull_holdout_A', ['F1']], passing=True, commit=reg_c))
        git('commit', '-q', '-am', '009 pass, pending')
        check('N3: a 009 pass with holdout A pending is still refused', h_() != [])
        dline = [l for l in (root / 'results.jsonl').read_text().splitlines() if '009-binance' in l][0]
        add(_row009(None, mode='holdout_A_declined', commit=reg_c, sha='a' * 64))
        git('commit', '-q', '-am', 'declined, wrong sha')
        check('N3 probe 5: a holdout_A_declined row whose sha does not match the discovery row unlocks nothing', h_() != [])
        git('reset', '-q', '--hard', 'HEAD~1')
        add(_row009(None, mode='holdout_A_declined', commit=reg_c, sha=r010_run.line_sha(dline)))
        git('commit', '-q', '-am', 'declined')
        check('N3: a committed holdout_A_declined matching the discovery row unlocks', r010_run.ordering_state(root) == 'holdout_A_declined')
        git('reset', '-q', '--hard', 'HEAD~1')
        add(_row009(None, mode='holdout_A', commit=reg_c, sha=r010_run.line_sha(dline)))
        git('commit', '-q', '-am', 'holdout A read')
        check('N3: a committed holdout_A row matching the discovery row unlocks', r010_run.ordering_state(root) == 'holdout_A')
        git('reset', '-q', '--hard', 'HEAD~2')
        add(_row009(['close', []], commit=reg_c))
        git('commit', '-q', '-am', '009 closed')
        check('N3: a committed, verified 009 close unlocks', r010_run.ordering_state(root) == 'close' and h_() == [])
        (root / 'results.jsonl').write_text(json.dumps({'registration': '005-y.md'}) + '\n')
        git('commit', '-q', '-am', '009 row deleted')
        check('N3 probe 6: a 009 row deleted from HEAD (still in history) unlocks nothing', h_() != [])
        git('reset', '-q', '--hard', 'HEAD~1')
        check('... restored: unlocked', h_() == [])

        # --- discovery (f) and the status
        f3, _ = fails()
        check('guard: committed, clean, hashes match, 009 closed -> allowed', f3 == [], str(f3))
        (root / reg_rel).write_text((root / reg_rel).read_text() + 'edit\n')
        check('guard (b): registration modified after commit -> refused', any(x.startswith('(b)') for x in fails()[0]))
        git('checkout', '--', reg_rel)
        (root / 'ct/carry.py').write_text('x = 3\n')
        f4, _ = fails()
        check('guard (d)+(e): guarded code edited -> not clean and hash mismatch',
              any(x.startswith('(d)') for x in f4) and any(x.startswith('(e) ct/carry.py') for x in f4), str(f4))
        git('checkout', '--', 'ct/carry.py')
        (root / 'data/c.json').write_text('{"changed": 1}\n')
        check('guard (e): the (untracked) data file changed -> hash mismatch only',
              fails()[0] == ['(e) data/c.json does not match its registered SHA-256'], str(fails()[0]))
        (root / 'data/c.json').write_text('{}\n')
        add({'registration': '010-x.md', 'mode': 'discovery_started'})
        check('guard (f): any 010 row (even a started row) blocks a second discovery run',
              any(x.startswith('(f)') for x in fails()[0]))
        git('commit', '-q', '-am', 'a started row')
        (root / 'results.jsonl').write_text('\n'.join(l for l in (root / 'results.jsonl').read_text().splitlines()
                                                       if '010-x' not in l) + '\n')
        git('commit', '-q', '-am', 'started row deleted')
        check('guard (f): a deleted 010 row is still found in git history -> refused', any(x.startswith('(f)') for x in fails()[0]))
        git('reset', '-q', '--hard', 'HEAD~2')

        # --- the holdout gate (N2), with the 009 ordering satisfied
        late = carry.HOLDOUT_START + 400 * D
        g_ = lambda mode, now=late: [x for x in fails(mode, now)[0]]
        write_reg(extra='\nHoldout end (owner): UNSET\n\nHoldout extension (owner): UNSET\n')
        git('commit', '-q', '-am', 'owner lines unset')
        gh = g_('holdout_fetch')
        check('N2 (g1): no 010 discovery row -> no holdout fetch; N1: UNSET refuses',
              any(x.startswith('(g1)') for x in gh) and sum(x.startswith('(n1)') for x in gh) == 2, str(gh))
        write_reg(extra='\nHoldout end (owner): 2026-12-31\n\nHoldout extension (owner): NO\n')
        git('commit', '-q', '-am', 'owner chose')
        reg_c2 = head()
        code_h = {f: r010_run.sha256(root / f) for f in code}
        add(_row010(code_h, reg_c2))
        row_line = (root / 'results.jsonl').read_text().splitlines()[-1]
        gh = g_('holdout_fetch')
        check('N2 (g2): an uncommitted discovery row -> refused', any(x.startswith('(g2)') for x in gh), str(gh))
        git('commit', '-q', '-am', 'discovery row')
        gh = g_('holdout_fetch')
        check("N2 (h'): no approval line -> refused; g2-g4 satisfied",
              any(x.startswith("(h')") for x in gh) and not any(x[:3] in ('(g2', '(g3', '(g4') for x in gh), str(gh))
        sha = r010_run.line_sha(row_line)
        write_reg(extra=f'\nHoldout end (owner): 2026-12-31\n\nHoldout extension (owner): NO\n\nHoldout fetch: APPROVED 2027-01-08 row {sha}\n')
        git('commit', '-q', '-am', 'approved')
        check("N2: approval committed after the row -> the fetch gate is open (late enough)", g_('holdout_fetch') == [],
              str(g_('holdout_fetch')))
        check('N2 (t): before end + 7 days -> refused', any(x.startswith('(t)') for x in g_('holdout_fetch', 1_798_761_600_000 + 6 * D)))
        check('N2 (i): building before a fetch started -> refused', any('fetch first' in x for x in g_('holdout_build')))
        add({'registration': '010-x.md', 'mode': 'holdout_fetch_started'})
        check('N2 (i): the run before a build -> refused', any('build first' in x for x in g_('holdout')))
        add({'registration': '010-x.md', 'mode': 'holdout_built', 'panel_sha256': {}})
        check('N2: fetch started and built -> the run gate is open', g_('holdout') == [], str(g_('holdout')))
        add({'registration': '010-x.md', 'mode': 'holdout_started'})
        check('N2 (i): a started holdout is never read again', any(x.startswith('(i)') for x in g_('holdout')))
        git('checkout', '--', 'results.jsonl')
        (root / 'ct/carry.py').write_text('x = 4\n')
        git('commit', '-q', '-am', 'code changed after the run')
        check('N2 (g3): code changed since the discovery run -> refused', any(x.startswith('(g3)') for x in g_('holdout_fetch')))
        git('reset', '-q', '--hard', 'HEAD~1')
        forged = json.loads(row_line)
        forged['T1']['annualised'] = 0.0
        lines = (root / 'results.jsonl').read_text().replace(row_line, json.dumps(forged))
        (root / 'results.jsonl').write_text(lines)
        git('commit', '-q', '-am', 'forged numbers')
        check('N2 (g4): a row whose numbers no longer support its labels -> refused',
              any(x.startswith('(g4)') for x in g_('holdout_fetch')) or any(x.startswith('(g1)') for x in g_('holdout_fetch')))
        git('reset', '-q', '--hard', 'HEAD~1')
        git('reset', '-q', '--hard', 'HEAD~2')          # back to: owner chose (no row, no approval)
        add(_row010(code_h, reg_c2))
        write_reg(extra=f'\nHoldout end (owner): 2026-12-31\n\nHoldout extension (owner): NO\n\nHoldout fetch: APPROVED 2027-01-08 row {sha}\n')
        git('commit', '-q', '-am', 'row and approval in one commit')
        check("N2 (h'): an approval committed together with the row is refused",
              any(x.startswith("(h')") for x in g_('holdout_fetch')), str(g_('holdout_fetch')))
        write_reg(status='DRAFT')
        git('commit', '-q', '-am', 'draft')
        check('guard: status DRAFT -> refused', any('not REGISTERED' in x for x in fails()[0]))


if __name__ == '__main__':
    main()
