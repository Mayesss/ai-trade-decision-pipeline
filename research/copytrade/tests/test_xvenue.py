"""Registration 006 — cross-venue gap mechanics on SYNTHETIC numbers.

    .venv/bin/python -m tests.test_xvenue
"""
import json
import random
import subprocess
import tempfile
from pathlib import Path

from ct import drift, xvenue as xv

S, M, H, D = xv.SECOND, xv.MINUTE, xv.HOUR, xv.DAY
T0 = 1_760_000_000_000 - 1_760_000_000_000 % D           # a day start well inside discovery
L = xv.L_PRIMARY


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def flat(t0, t1, px, step=100, taker=None):
    """A print every `step` ms at a constant price: a buy- and a sell-aggressor print at each time
    unless a taker side is given."""
    out = []
    for t in range(t0, t1 + 1, step):
        out += [(t, px, taker)] if taker else [(t, px, 'buy'), (t, px, 'sell')]
    return out


def pos_for(coin='BTC', side='L', t_sig=T0 + 10 * H, i=0):
    return {'i': i, 'coin': coin, 'side': side, 't_start': t_sig - 5 * S, 't_sig': t_sig, 'g_sig': 0.01,
            'p_hl_sig': 99.0, 'p_bn_sig': 100.0, 'k_sig': 0, 'major': coin in xv.MAJORS, 'entry': t_sig + L,
            'hour': t_sig // H, 'day': t_sig // D}


def synth_rows(rng, n, effect, sigma=0.002, crash=False, alt_effect=None, side_effect=None):
    """Pooled rows over ~n hours on many days, alternating coins (3 majors, 3 alts) and sides."""
    out = []
    for k in range(n):
        coin = xv.COINS[k % 6]
        t = T0 + k * 5 * H + (k % 6) * M
        side = 'L' if k % 2 == 0 else 'S'
        mu = effect
        if alt_effect is not None and coin not in xv.MAJORS:
            mu = alt_effect
        if side_effect is not None and side == 'S':
            mu = side_effect
        r = mu + rng.gauss(0, sigma)
        if crash and k < 3:
            r += 0.5
        cost = xv.round_trip_cost(coin)
        out.append({'i': k, 'coin': coin, 'side': side, 'entry': t, 'hour': (t if not crash or k >= 3 else T0) // H,
                    'day': t // D, 'major': coin in xv.MAJORS, 'ret': {xv.PRIMARY: r}, 'gross': {xv.PRIMARY: r + cost}})
    return out


def main():
    print('cross-venue gap mechanics')

    # --- costs exact
    c = xv.net_return('L', 100.0, 100.0, 100.0, 100.0, 0.0, 0.0)
    check('flat, no slippage: -19 bp (HL 2 x 4.5 + Binance 2 x 5)', abs(c + 0.0019) < 1e-12, f'{c:.6f}')
    sh, sb = 0.0003, 0.0002
    c = xv.net_return('L', 100.0, 101.0, 200.0, 201.0, sh, sb)
    hl = (101.0 * (1 - sh)) / (100.0 * (1 + sh)) - 1 - 2 * xv.TAKER_HL
    bnl = -((201.0 * (1 + sb)) / (200.0 * (1 - sb)) - 1) - 2 * xv.TAKER_BN
    check('longs liquidated: long HL / short Binance, adverse slippage on all four fills', abs(c - (hl + bnl)) < 1e-12, f'{c:.6f}')
    c = xv.net_return('S', 100.0, 99.0, 200.0, 199.0, sh, sb)
    hl = -((99.0 * (1 + sh)) / (100.0 * (1 - sh)) - 1) - 2 * xv.TAKER_HL
    bnl = (199.0 * (1 - sb)) / (200.0 * (1 + sb)) - 1 - 2 * xv.TAKER_BN
    check('shorts liquidated: short HL / long Binance', abs(c - (hl + bnl)) < 1e-12, f'{c:.6f}')
    check('one missing price -> None', xv.net_return('L', 100.0, None, 1.0, 1.0, 0, 0) is None)
    rt = xv.round_trip_cost('BTC', 0.0002, 0.0002)
    check('round trip with 2 bp per fill ~ 19 + 8 = 27 bp', abs(rt - 0.0027) < 2e-6, f'{rt * 1e4:.3f} bp')
    check('G_MIN = round trip + 10 bp', abs(xv.g_min('BTC', slip_hl=0.0002, slip_bn=0.0002) - (rt + 0.001)) < 1e-12)

    # --- the gap's sign
    check('longs liquidated, HL 99 below Binance 100: gap +1.01%', abs(xv.signed_gap('L', 100.0, 99.0) - 1 / 99) < 1e-12)
    check('shorts liquidated, HL 101 above Binance 100: gap +0.99%', abs(xv.signed_gap('S', 100.0, 101.0) - 1 / 101) < 1e-12)
    check('longs liquidated but HL above Binance: gap negative', xv.signed_gap('L', 100.0, 101.0) < 0)

    # --- trigger: first print reaching G_MIN, Binance at or before the print only
    t = T0 + 10 * H
    bn_ser = xv.Series([(t - 10 * S, 100.0, 'buy'), (t + 1, 1000.0, 'buy')])
    scan = xv.scan_from_series([(t - 2 * S, 99.9), (t, 99.0), (t + 500, 98.0)], bn_ser)
    trig = xv.first_trigger('L', scan, 0.005)
    check('trigger at the first print whose gap reaches G_MIN', trig and trig['t'] == t and trig['k'] == 1, str(trig))
    check('a Binance trade 1 ms after the print is not read at the print', trig['p_bn'] == 100.0)
    check('no print reaches G_MIN -> no trigger', xv.first_trigger('L', scan[:2], 0.05) is None)

    # --- clock offset: Binance transact_time T sits at T + offset on HL's clock
    bn_off = xv.Series([(t - 10 * S, 100.0), (t - 300, 105.0)], offset_ms=500)     # 105 at T = t - 300 -> HL clock t + 200
    check('offset +500: a trade at T = t - 300 is not yet visible at HL time t', bn_off.last_at_or_before(t)[1] == 100.0)
    check('... and visible at HL time t + 200', bn_off.last_at_or_before(t + 200)[1] == 105.0)
    bn_neg = xv.Series([(t - 10 * S, 100.0), (t + 300, 105.0)], offset_ms=-500)
    check('offset -500: a trade at T = t + 300 is visible at HL time t', bn_neg.last_at_or_before(t)[1] == 105.0)
    hl_f = xv.Series([(t + L + 10, 99.0, 'buy')])
    bn_f = xv.Series([(t + L - 400, 100.0, 'sell'), (t + L + 50, 100.0, 'sell')], offset_ms=300)
    r = xv.simulate(pos_for(t_sig=t), hl_f, xv.Series([(t + L - 400, 100.0, 'sell'), (t + L + 50, 100.0, 'sell')], 300))
    check('entry fill on Binance uses the offset clock (T = t+L-400 -> t+L-100: too early; next one taken)',
          'skipped' not in r and r['fill_bn'][0] == t + L + 350, str(r.get('fill_bn')))
    check('... an exit with no print at all in its window is counted missing (ret None), never dropped silently',
          r['exit_missing'] >= 1 and r['ret'][xv.PRIMARY] is None)

    # --- a planted gap that never closes loses exactly the costs (exit at the cap)
    p0 = pos_for(t_sig=t)
    hl_s = xv.Series(flat(t - 60 * S, t + 400 * S, 99.0))
    bn_s = xv.Series(flat(t - 60 * S, t + 400 * S, 100.0))
    r = xv.simulate(p0, hl_s, bn_s)
    cost = xv.round_trip_cost('BTC')
    check('gap never closes: exit at the cap (t_open + 60 s)', r['exit_reason'] == 'cap' and r['exit_decision'] == r['t_open'] + xv.CAP
          and r['t_open'] == t + L)
    check('... and the trade loses exactly the round-trip cost', abs(r['ret'][xv.PRIMARY] + cost) < 1e-12,
          f'{r["ret"][xv.PRIMARY]:.8f} vs {-cost:.8f}')
    check('... every fixed exit too', all(abs(r['ret'][h] + cost) < 1e-12 for h in xv.FIXED))
    check('... gross (no fee, no slippage) is exactly zero', abs(r['gross'][xv.PRIMARY]) < 1e-15)

    # --- planted convergence: HL jumps back to Binance at t + 3 s
    tc = t + 3 * S
    hl_c = xv.Series(flat(t - 60 * S, tc - 100, 99.0) + flat(tc, t + 400 * S, 100.0))
    r = xv.simulate(p0, hl_c, bn_s)
    exp = xv.net_return('L', 99.0, 100.0, 100.0, 100.0, xv.SLIP_HL['BTC'], xv.SLIP_BN['BTC'])
    check('convergence at t+3 s: exit decided at the first print with gap <= 0', r['exit_reason'] == 'gap' and r['exit_decision'] == tc)
    check('... exit fills at the first prints at or after decision + L', abs(r['ret'][xv.PRIMARY] - exp) < 1e-12 and exp > 0,
          f'{r["ret"][xv.PRIMARY] * 1e4:.2f} bp')
    check('... 5 s fixed exit also captures it; it is the same arithmetic', abs(r['ret']['5s'] - exp) < 1e-12)
    # gap already closed during the latency -> exit decided at entry time, filled L later
    hl_q = xv.Series(flat(t - 60 * S, t + 200, 99.0) + flat(t + 300, t + 400 * S, 100.0))
    r = xv.simulate(p0, hl_q, bn_s)
    check('gap closed during the latency: exit decided at t + L, entry already at the closed price',
          r['exit_decision'] == t + L and r['fill_hl'][1] == 100.0 and abs(r['ret'][xv.PRIMARY] + cost) < 1e-12)

    # --- latency respected: poison every price in (t, t + L) on both venues
    poison_hl = xv.Series(flat(t - 60 * S, t, 99.0) + flat(t + 1, t + L - 1, 1.0, step=7) + flat(t + L, t + 400 * S, 99.5))
    poison_bn = xv.Series(flat(t - 60 * S, t, 100.0) + flat(t + 1, t + L - 1, 1e6, step=7) + flat(t + L, t + 400 * S, 100.0))
    clean_hl = xv.Series(flat(t - 60 * S, t, 99.0) + flat(t + L, t + 400 * S, 99.5))
    clean_bn = xv.Series(flat(t - 60 * S, t, 100.0) + flat(t + L, t + 400 * S, 100.0))
    a, b = xv.simulate(p0, poison_hl, poison_bn), xv.simulate(p0, clean_hl, clean_bn)
    check('latency: prices inside (t, t + L) change nothing', a['ret'] == b['ret'] and a['fill_hl'] == b['fill_hl'],
          f'{a["ret"][xv.PRIMARY]} vs {b["ret"][xv.PRIMARY]}')
    check('latency: both entry fills at or after t + L', a['fill_hl'][0] >= t + L and a['fill_bn'][0] >= t + L)
    for lat in xv.LATENCIES:
        rr = xv.simulate(p0, clean_hl, clean_bn, latency=lat)
        check(f'latency {lat} ms: fills at or after t + {lat}', rr['fill_hl'][0] >= t + lat and rr['fill_bn'][0] >= t + lat)
    nf = xv.simulate(p0, xv.Series(flat(t - 60 * S, t, 99.0) + flat(t + L + xv.FILL_WINDOW + 1, t + 400 * S, 99.0)), clean_bn)
    check('no HL print within the fill window -> position never opened, returned as skipped (counted)',
          nf.get('skipped') == 'no_entry_fill' and nf.get('missing') == 'hl')

    # --- the fill rule (D8): never better than the extreme print between t0 and our-side print
    hx = xv.Series([(t + L, 100.5, 'sell'), (t + L + 30, 100.9, 'sell'), (t + L + 50, 100.2, 'buy')])
    f = xv.fill(hx, t + L, xv.FILL_WINDOW, 'buy')
    check('BUY: first buy-aggressor print 100.2 at +50 ms, but 100.9 printed in between -> fills at 100.9, delay 50',
          f == (t + L + 50, 100.9, 50), str(f))
    bx = xv.Series([(t + L, 99.5, 'buy'), (t + L + 10, 99.1, 'buy'), (t + L + 40, 99.8, 'sell')])
    f = xv.fill(bx, t + L, xv.FILL_WINDOW, 'sell')
    check('SELL: mirrors with the lowest print in between', f == (t + L + 40, 99.1, 40), str(f))
    check('no qualifying print within the window -> None', xv.fill(hx, t + L, 40, 'buy') is None)
    check('worst print of an exit window: highest for a buy, lowest for a sell',
          xv.worst_fill(hx, t + L, 100, 'buy')[1] == 100.9 and xv.worst_fill(bx, t + L, 100, 'sell')[1] == 99.1)
    # exit window with prints of the wrong side only -> worst print, counted
    hl_w = xv.Series(flat(t - 60 * S, t + L + 2 * S, 99.0) + flat(t + L + 2 * S + 100, t + 400 * S, 99.0, taker='buy'))
    rw = xv.simulate(p0, hl_w, clean_bn)
    check('exit with only buy prints after the entry (we must sell HL): worst print of the window, counted',
          rw['exit_fallback'] >= 1 and rw['ret'][xv.PRIMARY] is not None)
    # exit scan starts at the later entry fill
    bn_late = xv.Series(flat(t - 60 * S, t + L - 1, 100.0) + [(t + L + 800, 100.0, 'sell')] + flat(t + L + 900, t + 400 * S, 100.0))
    hl_early = xv.Series(flat(t - 60 * S, t + L + 500, 99.0) + flat(t + L + 600, t + 400 * S, 100.0))
    rl = xv.simulate(p0, hl_early, bn_late)
    check('the exit scan starts at max(both entry fills): gap closed at +600 ms is seen only from t_open = +800',
          rl['t_open'] == t + L + 800 and rl['exit_decision'] >= rl['t_open'], f"{rl['t_open'] - t - L} {rl['exit_decision'] - t - L}")

    # --- executable-quote signal (D4): HL ask (buy-aggressor) vs Binance bid (seller-taker), within 1 s
    hq = xv.Series([(t - 500, 99.0, 'sell'), (t - 400, 99.6, 'buy'), (t, 98.5, 'sell')])
    bq = xv.Series([(t - 300, 100.0, 'sell'), (t - 100, 100.2, 'buy')])
    sc = xv.exec_scan('L', [t, t], hq, bq)
    check('exec scan: one point per distinct time; HL ask = last buy print 99.6, Binance bid = last sell 100.0',
          sc == [(t, 99.6, 100.0)], str(sc))
    check('... the exec gap is (100.0 - 99.6)/99.6, not the sweep gap (100.2 - 98.5)/98.5',
          abs(xv.signed_gap('L', sc[0][2], sc[0][1]) - 0.4 / 99.6) < 1e-12)
    stale = xv.exec_scan('L', [t + 2 * S], hq, bq)
    check('a quote older than 1 s is no quote -> no signal at that point',
          stale[0][1] is None and xv.first_trigger('L', stale, 0.0) is None)
    sh_ = xv.exec_scan('S', [t], hq, bq)
    check('shorts liquidated: HL bid (sell print) vs Binance ask (buyer-taker)', sh_ == [(t, 98.5, 100.2)], str(sh_))

    # --- side-aware fills: buy HL at an ask print, sell Binance at a bid print
    hl_sa = xv.Series([(t - S, 99.0, 'sell'), (t + L, 98.9, 'sell'), (t + L + 20, 99.1, 'buy')] + flat(t + 2 * S, t + 400 * S, 99.1, taker='sell'))
    bn_sa = xv.Series([(t - S, 100.0, 'buy'), (t + L, 100.01, 'buy'), (t + L + 5, 99.99, 'sell')] + flat(t + 2 * S, t + 400 * S, 100.0, taker='buy'))
    r = xv.simulate(p0, hl_sa, bn_sa)
    check('side-aware is the primary (D8): long HL fills at max(first BUY print, prints in between), short Binance at min(...)',
          r['fill_hl'] == (t + L + 20, 99.1) and r['fill_bn'] == (t + L + 5, 99.99) and r['delay_hl'] == 20 and r['delay_bn'] == 5,
          f'{r["fill_hl"]} {r["fill_bn"]}')
    r = xv.simulate(p0, hl_sa, bn_sa, side_aware=False)
    check('side-blind (reported): the first print of either side', r['fill_hl'] == (t + L, 98.9) and r['fill_bn'] == (t + L, 100.01))

    # --- plan: causality poison, warm-up, stress window, overlap, holdout edge
    ev = {'coin': 'BTC', 'side': 'L', 't_start': t - 5 * S, 't_end': t + 30 * S, 'fallback_median': False,
          'n': 50, 'notional': 5e6, 'purity': 0.95, 'end_price': 90.0, 'start_price': 100.0, 'rel': 3.0}
    scan = [(t - 5 * S, 99.95, 100.0), (t, 99.0, 100.0), (t + 10 * S, 98.0, 98.5)]
    rate = drift.StressRate([])
    pa, _ = xv.plan([ev], {0: scan}, rate, {})
    poisoned = dict(ev, t_end=t + 999 * S, n=1, notional=1.0, purity=0.0, end_price=1e9, start_price=-1.0, rel=0.0)
    pb, _ = xv.plan([poisoned], {0: scan}, rate, {})
    check('poisoned non-entry event fields (t_end, n, notional, purity, prices, rel) change nothing', pa == pb)
    scan_after = scan[:2] + [(t + 10 * S, 1e-9, 1e9)]
    pc, _ = xv.plan([ev], {0: scan_after}, rate, {})
    check('poisoned prints AFTER the trigger change nothing (trigger, entry, gap)', pa == pc and pa[0]['t_sig'] == t, str(pa))
    check('the position enters at trigger + L', pa[0]['entry'] == t + L)
    pw, sk = xv.plan([dict(ev, fallback_median=True)], {0: scan}, rate, {})
    check('warm-up (fallback median) event dropped', not pw and sk['warmup_fallback'] == [0])
    pw, sk = xv.plan([dict(ev, fallback_median=True)], {0: scan}, rate, {}, drop_fallback=False)
    check('... kept in the inclusive set', len(pw) == 1)
    pw, sk = xv.plan([ev], {0: scan}, rate, {}, bn_ok={0: False})
    check('no Binance file for the event -> skipped', not pw and sk['no_binance'] == [0])
    big = xv.STRESS_THRESHOLD
    check('stress: a bucket [t - 1 s, t) counts', xv.stressed(ev, t, drift.StressRate([(t - S, big)])))
    check('stress: the bucket starting at t does not', not xv.stressed(ev, t, drift.StressRate([(t, big)])))
    check('stress: flow before the cascade within 5 min counts', xv.stressed(ev, t, drift.StressRate([(ev['t_start'] - 2 * M, big)])))
    check('stress: own coin excluded (D5)', not xv.stressed(ev, t, drift.StressRate([(t - 3 * S, big)]),
                                                             own=drift.StressRate([(t - 3 * S, big)])))

    class Spy(drift.StressRate):
        seen = []

        def rate(self, x):
            self.seen.append(x)
            return super().rate(x)
    spy = Spy([(t - 3 * M, 1.0), (t + S, 1.0), (t + H, 1.0)])
    xv.plan([ev], {0: scan}, spy, {})
    check('stress switch evaluated only up to trigger - 1 s', spy.seen and max(spy.seen) <= t - S, f'{max(spy.seen) - t} ms')
    trig_first = [(t - 5 * S, 99.0, 100.0)]
    spy.seen.clear()
    xv.plan([ev], {0: trig_first}, spy, {})
    check('trigger at the first print: no evaluation at or after it', max(spy.seen) <= t - 5 * S - S)
    pw, sk = xv.plan([ev], {0: scan}, drift.StressRate([(t - 2 * S, big)]), {})
    check('a stressed trigger holds nothing', not pw and sk['stress'] == [0])
    ev2 = dict(ev, t_start=t + 61 * S)
    scan2 = [(t + 62 * S, 98.0, 100.0)]
    pw, sk = xv.plan([ev, ev2], {0: scan, 1: scan2}, rate, {})
    check('one position per coin: a trigger while the earlier one can still be open is skipped', len(pw) == 1 and sk['overlap'] == [1])
    ev3 = dict(ev, t_start=t + 10 * M)
    pw, sk = xv.plan([ev, ev3], {0: scan, 1: [(t + 10 * M, 98.0, 100.0)]}, rate, {})
    check('... one 10 min later is traded', len(pw) == 2)
    late = dict(ev, t_start=xv.HOLDOUT_START - 2 * M)
    pw, sk = xv.plan([late], {0: [(xv.HOLDOUT_START - 2 * M, 98.0, 100.0)]}, rate, {})
    check('events whose 300-s exit could reach the holdout are dropped', not pw and sk['holdout_edge'] == [0])
    pw, sk = xv.plan([ev], {0: [(t, 99.9, 100.0)]}, rate, {})
    check('a cascade that never reaches G_MIN holds nothing', not pw and sk['no_trigger'] == [0])

    # --- statistics: the seven criteria
    rng = random.Random(1)
    rows = synth_rows(rng, 300, 0.002)
    sm = xv.summarise(rows, placebo_seeds=100)
    check('planted +20 bp per trade (SD 20 bp), 300 trades: passes all eight', sm['pass'], str(sm['criteria']))
    check('placebo (hour flip, costs kept): pass share 0', sm['placebo_hour_flip']['pass_share'] == 0.0, str(sm['placebo_hour_flip']))
    check('decision on a pass with estimate >= 10 bp and a positive 3-s block: ask for the holdout', xv.decide(sm, 0.001) == 'ask_holdout')
    check('... a pass whose 3-s block is <= 0: close (latency)', xv.decide(sm, -0.0001) == 'close_latency')
    small = xv.summarise(synth_rows(random.Random(21), 300, 0.0005, sigma=0.0005), placebo_seeds=0)
    check('a pass with an estimate below 10 bp: close (below the economic floor)',
          small['pass'] and xv.decide(small, 0.001) == 'close_below_floor', f'{small["primary_hour"]["mean"] * 1e4:.2f} bp')
    ok, _ = xv.holdout_confirm(synth_rows(random.Random(22), 60, 0.003))
    bad, _ = xv.holdout_confirm(synth_rows(random.Random(23), 60, -0.001))
    check('holdout confirm: a planted +30 bp confirms, a planted -10 bp does not', ok and not bad)
    null = xv.summarise(synth_rows(random.Random(2), 300, 0.0), placebo_seeds=0)
    check('null: fails', not null['pass'] and xv.decide(null, None) == 'close')
    fails = sum(xv.summarise(synth_rows(random.Random(100 + k), 200, 0.0), placebo_seeds=0)['pass'] for k in range(100))
    check('null: passes in at most ~2% of 100 draws', fails <= 3, f'{fails}/100')
    crash = xv.summarise(synth_rows(random.Random(3), 300, 0.0, crash=True), placebo_seeds=0)
    check('crash hour (+50% on three trades in one hour, rest null): does not pass', not crash['pass'],
          f't {crash["primary_hour"]["t"]:.2f}; without largest positive hour {crash["without_largest_positive_hour"]["mean"]:.5f}')
    mixed = xv.summarise(synth_rows(random.Random(4), 300, 0.004, alt_effect=-0.001), placebo_seeds=0)
    check('majors +40 bp, alts -10 bp: criterion 6 fails', not mixed['criteria']['6_majors_and_alts_positive'] and not mixed['pass'])
    one_side = xv.summarise(synth_rows(random.Random(5), 300, 0.004, side_effect=-0.001), placebo_seeds=0)
    check('one side negative: criterion 5 fails', not one_side['criteria']['5_both_sides_positive'])
    oneday = synth_rows(random.Random(24), 300, -0.0005, sigma=0.0005)
    for k in range(12):
        tt = T0 + 400 * D + k * H
        oneday.append(dict(oneday[k], i=1000 + k, entry=tt, hour=tt // H, day=tt // D, ret={xv.PRIMARY: 0.05},
                           gross={xv.PRIMARY: 0.05 + 0.0027}))
    od = xv.summarise(oneday, placebo_seeds=0)
    check('one good DAY over 12 hours carries the mean: criterion 7 (hour) holds, criterion 8 (day) fails',
          od['criteria']['7_without_largest_positive_hour_positive'] and not od['criteria']['8_without_largest_positive_day_positive']
          and not od['pass'], str(od['criteria']))
    loss = synth_rows(random.Random(6), 300, 0.002)
    for r in loss[:3]:
        r['ret'][xv.PRIMARY] = -0.5
        r['hour'] = T0 // H
    lc = drift.largest_cluster(loss, 'hour', xv.PRIMARY, how='positive')
    check('a planted loss hour is never the one removed', lc != T0 // H)

    # --- reported measurements
    two = lambda pts: [(tt, px, k) for tt, px in pts for k in ('buy', 'sell')]
    hl_p = xv.Series(two([(t - S, 99.0), (t + 500, 99.5), (t + 2 * S, 100.0)]))
    bn_p = xv.Series(two([(t - S, 100.0), (t, 100.0), (t + 1500, 99.8)]))
    prof = xv.gap_profile('L', t, 99.0, 100.0, hl_p, bn_p, offsets=(0, 1000, 2000))
    g0, g2 = prof[0][0], prof[2000][0]
    check('gap profile (executable quotes): +0 is the signal gap', abs(g0 - 1 / 99) < 1e-12)
    check('... HL reverting up and Binance falling both count as closing',
          prof[2000][1] > 0 and prof[2000][2] > 0 and abs((g0 - g2) - (prof[2000][1] + prof[2000][2])) < 1e-3, str(prof))
    pe = xv.gap_profile('L', t, 99.0, 100.0, xv.Series([(t - S, 99.0, 'buy'), (t + 500, 99.2, 'sell'), (t + 600, 99.6, 'buy')]),
                        xv.Series([(t - S, 100.0, 'sell'), (t + 400, 100.1, 'buy')]), offsets=(0, 1000), mode='exec')
    check('gap profile, exec mode: HL ask = last buy print (99.6), Binance bid = last seller-taker within 1 s (none -> None)',
          pe[1000] == (None, None, None), str(pe))
    pw_ = xv.gap_profile('L', t, 99.0, 100.0, xv.Series([(t - S, 99.0, 'buy'), (t + 500, 99.2, 'sell'), (t + 600, 99.6, 'buy')]),
                         xv.Series([(t - S, 100.0, 'sell'), (t + 400, 100.1, 'buy')]), offsets=(0, 1000), mode='sweep')
    check('gap profile, sweep mode: last prints of either side (HL 99.6, Binance 100.1)',
          abs(pw_[1000][0] - (100.1 - 99.6) / 99.6) < 1e-12, str(pw_))
    wp = xv.weighted_profile([(1.0, {0: (0.01, 0.0, 0.0)}), (3.0, {0: (0.03, 0.0, 0.0)})])
    check('notional-weighted profile', abs(wp[0][0] - 0.025) < 1e-12)
    check('minute gap signed like the trigger gap', abs(xv.minute_gap('S', 101.0, 100.0) - 1 / 101) < 1e-12)

    # --- power arithmetic
    ps = [{'coin': 'BTC', 'side': 'L', 'entry': T0 + k * H, 'hour': (T0 + k * H) // H, 'day': (T0 + k * H) // D,
           'major': True} for k in range(100)]
    se = xv.null_se(ps, {'BTC': 0.001}, {})
    check('null SE of non-overlapping 60-s holds = sigma / sqrt(n)', abs(se - 0.0001) < 1e-12, f'{se}')
    pos6 = [dict(r, ret=None) for r in synth_rows(random.Random(7), 240, 0.0)]
    sig = {c: 0.002 for c in xv.COINS}
    hi = xv.simulate_power(pos6, sig, {}, 0.002, sims=100, seed=1)
    lo = xv.simulate_power(pos6, sig, {}, 0.0, sims=200, seed=2)
    check('simulation: a planted mean of 20 bp (SE ~1.3 bp) passes nearly always', hi['p_pass'] > 0.9, str(hi))
    check('simulation: no effect passes rarely', lo['p_pass'] < 0.03, str(lo))
    e2e = xv.simulate_power(pos6, sig, {}, 0.002, sims=50, seed=4, holdout_n=40, hold_sims=50)
    check('end-to-end: pass x gate x holdout confirm', abs(e2e['end_to_end'] - e2e['p_pass_and_gate'] * e2e['p_confirm']) < 1e-12
          and e2e['p_confirm'] > 0.5, str(e2e))

    # --- the causal trigger (004 D1) as the event set: scan from the trigger print to the run's end
    import l8_run
    t9 = T0 + 20 * H
    fills = [(t9 - 50 * S, 'Close Long', 40_000.0, 100.0), (t9 - 20 * S, 'Close Long', 40_000.0, 99.5),
             (t9, 'Close Long', 40_000.0, 99.0), (t9 + 30 * S, 'Close Long', 10_000.0, 98.0),
             (t9 + 80 * S, 'Close Long', 10_000.0, 97.0), (t9 + 150 * S, 'Close Short', 10_000.0, 97.5),
             (t9 + 5 * M, 'Close Long', 200_000.0, 96.0), (t9 + 20 * M, 'Close Long', 200_000.0, 95.0)]
    own = lambda t: 150_000.0
    evs = l8_run.causal_events('BTC', fills, own, lambda t: None)
    check('causal trigger: fires at the print where trailing-60-s notional reaches max($100 k, 0.5 x median)',
          len(evs) == 2 and evs[0]['t_trig'] == t9 and evs[0]['side'] == 'L', str([(e['t_trig'] - t9) for e in evs]))
    check('... t_start = first print in the trailing window; scan from the trigger print to the run end (first 60-s gap)',
          evs[0]['t_start'] == t9 - 50 * S and evs[0]['scan_from'] == t9 and evs[0]['scan_to'] == t9 + 80 * S)
    check('... 15-min cooldown: the +5 min crossing is skipped, +20 min fires', evs[1]['t_trig'] == t9 + 20 * M)
    check('... own history -> not a fallback event', not evs[0]['fallback_median'])
    evf = l8_run.causal_events('BTC', fills, lambda t: None, lambda t: 150_000.0)
    check('... no own history -> threshold from the cross-coin fallback, flagged (dropped in the primary)',
          len(evf) == 2 and all(e['fallback_median'] for e in evf))
    check('... no history at all -> no trigger', l8_run.causal_events('BTC', fills, lambda t: None, lambda t: None) == [])

    assembly_checks()
    guard_checks()
    print('xvenue checks passed')


def assembly_checks():
    """l8_run.report / profile_table on synthetic positions run through xv.simulate: every key the
    results row carries is built and serialisable (the 004 crash was in this step)."""
    import l8_run
    rng = random.Random(9)
    names = [f'L{lat}' for lat in xv.LATENCIES] + ['side_blind', 'slip0', 'slip4']
    runs = {n: {'rows': [], 'skipped': []} for n in names}

    def add(name, r):
        runs[name]['skipped' if 'skipped' in r else 'rows'].append(r)
    trig = []
    for k in range(60):
        coin = xv.COINS[k % 6]
        side = 'L' if k % 3 else 'S'
        ts = T0 + k * 3 * H + 7 * M
        pos = dict(pos_for(coin, side, ts, k))
        s_ = xv.direction(side)
        hl_px = 100.0 * (1 - s_ * 0.005)
        close_at = ts + rng.choice((2, 5, 20, 90)) * S
        hl = xv.Series([(t, hl_px if t < close_at else 100.0, 'buy' if t % 200 else 'sell') for t in range(ts - 60 * S, ts + 400 * S, 100)])
        bns = xv.Series([(t, 100.0, 'sell' if t % 300 else 'buy') for t in range(ts - 60 * S, ts + 400 * S, 50)])
        for lat in xv.LATENCIES:
            add(f'L{lat}', xv.simulate(pos, hl, bns, latency=lat))
        add('side_blind', xv.simulate(pos, hl, bns, side_aware=False))
        add('slip0', xv.simulate(pos, hl, bns, slip_hl=0.0, slip_bn=0.0))
        add('slip4', xv.simulate(pos, hl, bns, slip_hl=0.0004, slip_bn=0.0004))
        trig.append({'coin': coin, 'major': coin in xv.MAJORS, 'side': side,
                     'profile': xv.gap_profile(side, ts, hl_px, 100.0, hl, bns)})
    T1 = l8_run.report(runs, placebo_seeds=20)
    keys = {'primary_hour', 'primary_day', 'criteria', 'pass', 'decision', 'l3_mean', 'majors', 'alts', 'by_coin',
            'fixed_exits', 'variants_trade', 'entry_skipped', 'exit_fallback', 'exit_missing', 'fill_delay_ms_q',
            'exit_fill_delay_ms_q', 'legs', 'exit_reason', 'hold_ms_q', 'entry_gap_q', 'signal_gap_q',
            'placebo_hour_flip', 'placebo_day_flip', 'without_largest_positive_day'}
    check('report: every key built', keys <= set(T1), str(keys - set(T1)))
    check('report: serialisable', bool(json.dumps(T1, default=str)))
    check('report: exits split between gap and cap', T1['exit_reason']['gap'] > 0 and T1['exit_reason']['cap'] > 0, str(T1['exit_reason']))
    check('report: empty universe does not crash',
          bool(json.dumps(l8_run.report({'L1000': {'rows': [], 'skipped': []}}, placebo_seeds=5), default=str)))
    tab = l8_run.profile_table(trig)
    check('profile table: gap shrinks to <= 0 by +60 s in the synthetic set',
          tab['all'][60_000]['gap_median'] <= 0 < tab['all'][0]['gap_median'], str(tab['all'][60_000]))


def guard_checks():
    """l8_run's guard (005's guard_failures with 006's file lists): refuses today on the real repo;
    in a throwaway repo, the 006 file list and a 006 result row."""
    import l7_run
    import l8_run
    real, _ = l7_run.guard_failures(l8_run.HERE, str(l8_run.REG.relative_to(l8_run.HERE)), l8_run.GUARDED_CODE, l8_run.GUARDED_DATA)
    check('real repo: the 006 guard refuses today (status DRAFT)', any('not REGISTERED' in x for x in real), str(real[:3]))
    check('guarded code list covers the new modules and the 005 code they import',
          {'ct/xvenue.py', 'ct/bn.py', 'l8_run.py', 'ct/drift.py', 'ct/tape.py', 'l7_run.py'} <= set(l8_run.GUARDED_CODE)
          and 'data/binance/manifest.json' in l8_run.GUARDED_DATA)
    code, data = ('ct/xvenue.py', 'l8_run.py'), ('data/m.json',)
    reg_rel = 'registrations/006-x.md'
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root,
                                        capture_output=True, text=True, check=True)
        git('init', '-q')
        for f, body in (('ct/xvenue.py', 'x = 1\n'), ('l8_run.py', 'y = 2\n'), ('data/m.json', '{}\n'),
                        ('results.jsonl', json.dumps({'registration': '005-post-cascade-drift.md'}) + '\n')):
            (root / f).parent.mkdir(parents=True, exist_ok=True)
            (root / f).write_text(body)
        (root / '.gitignore').write_text('data/\n')
        hashes = {f: l7_run.sha256(root / f) for f in (*code, *data)}
        rows = '\n'.join(f'| `{f}` | `{h}` |' for f, h in hashes.items())
        (root / reg_rel).parent.mkdir(exist_ok=True)
        (root / reg_rel).write_text(f'# 006\n\n**Status: REGISTERED 2026-10-10**\n\n| file | sha256 |\n|---|---|\n{rows}\n')
        git('add', '.')
        git('commit', '-q', '-m', 'register')
        fails = lambda: l7_run.guard_failures(root, reg_rel, code, data)
        f0, c0 = fails()
        check('throwaway repo: committed, clean, hashes match, a 005 row only -> allowed', f0 == [] and len(c0) == 40, str(f0))
        (root / 'ct/xvenue.py').write_text('x = 2\n')
        f1, _ = fails()
        check('xvenue edited -> refused (d) and (e)', any(x.startswith('(d)') for x in f1) and any(x.startswith('(e) ct/xvenue.py') for x in f1))
        git('checkout', '--', 'ct/xvenue.py')
        (root / 'data/m.json').write_text('{"x": 1}\n')
        f2, _ = fails()
        check('Binance manifest changed -> refused (e)', f2 == ['(e) data/m.json does not match its registered SHA-256'], str(f2))
        (root / 'data/m.json').write_text('{}\n')
        with open(root / 'results.jsonl', 'a') as fh:
            fh.write(json.dumps({'registration': '006-x.md'}) + '\n')
        f3, _ = fails()
        check('a 006 result row exists -> refused (f)', any(x.startswith('(f)') for x in f3))


if __name__ == '__main__':
    main()
