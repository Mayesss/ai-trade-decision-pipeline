"""Registration 005 — post-cascade drift mechanics on SYNTHETIC numbers.

    .venv/bin/python -m tests.test_drift
"""
import json
import math
import random
import statistics
import subprocess
import tempfile
from pathlib import Path

from ct import drift

H, M, S, D = drift.HOUR, drift.MINUTE, drift.SECOND, drift.DAY
T0 = 1_760_000_000_000 - 1_760_000_000_000 % D          # a day start well inside discovery
MAJORS = {'BTC', 'ETH', 'SOL'}


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def synth_rows(rng, n, effect, sigma=0.01, crash=False):
    """Major positions spread over ~n/3 hours across many days, alternating sides, one per coin per
    hour block (the overlap rule's shape)."""
    out = []
    coins = ('BTC', 'ETH', 'SOL')
    for k in range(n):
        entry = T0 + (k // 3) * 7 * H + (k % 3) * 5 * M + 62 * S
        side = 'L' if (k // 3) % 2 == 0 else 'S'
        base = effect + rng.gauss(0, sigma)
        if crash and k < 3:
            base += 0.6                                     # one absurd hour, all three coins
        ret = {h: base + rng.gauss(0, 0.001) for h in drift.HORIZONS}
        out.append({'i': k, 'coin': coins[k % 3], 'side': side, 'entry': entry, 'hour': entry // H,
                    'day': entry // D, 'major': True, 'ret': ret,
                    'ret_slip': {h: {str(s): ret[h] + drift.SLIP * 2 - s * 2 for s in drift.SLIPS} for h in drift.HORIZONS},
                    'gross': {h: ret[h] + drift.round_trip_cost() for h in drift.HORIZONS}})
    return out


def main():
    print('post-cascade drift mechanics')

    # --- costs exact
    r = drift.net_return('L', 100.0, 100.0, slip=0.0)
    check('flat, no slippage: -9 bp (2 x 4.5 bp taker)', abs(r + 0.0009) < 1e-12, f'{r:.6f}')
    r = drift.net_return('L', 100.0, 101.0, slip=0.0005)
    exp = (101.0 * 0.9995) / (100.0 * 1.0005) - 1 - 0.0009
    check('buy 100 sell 101, 5 bp adverse both sides', abs(r - exp) < 1e-12, f'{r:.6f}')
    r = drift.net_return('S', 100.0, 99.0, slip=0.0005)
    exp = -((99.0 * 1.0005) / (100.0 * 0.9995) - 1) - 0.0009
    check('sell 100 buy back 99 (shorts liquidated -> sell), adverse both sides', abs(r - exp) < 1e-12, f'{r:.6f}')
    rt = {str(s): round(drift.round_trip_cost(s) * 1e4, 3) for s in drift.SLIPS}
    check('round trip 9 / 19 / 29 bp at 0 / 5 / 10 bp slippage',
          abs(drift.round_trip_cost(0.0) - 0.0009) < 1e-12 and abs(drift.round_trip_cost(0.0005) - 0.0019) < 2e-6
          and abs(drift.round_trip_cost(0.001) - 0.0029) < 4e-6, str(rt))
    check('direction: longs liquidated -> buy', drift.direction('L') == 1.0 and drift.direction('S') == -1.0)
    check('missing price -> None', drift.net_return('L', 100.0, None) is None)

    # --- entry at flow end, not before
    ev = {'coin': 'BTC', 'side': 'L', 't_start': T0 + 10 * H, 't_end': T0 + 10 * H + 90 * S}
    check('entry time = t_end + 60 s + 2 s', drift.entry_time(ev) == ev['t_end'] + 62 * S)
    rate = drift.StressRate([])
    te = drift.entry_time(ev)
    pos, sk = drift.plan([ev], rate, {0: (te - 1, 100.0)}, MAJORS)
    check('a print before the entry time is not an entry', not pos and sk['no_entry_print'] == [0])
    pos, sk = drift.plan([ev], rate, {0: (te + drift.ENTRY_WINDOW + 1, 100.0)}, MAJORS)
    check('a print after the entry window is not an entry', not pos and sk['no_entry_print'] == [0])
    pos, _ = drift.plan([ev], rate, {0: (te + 300, 100.0)}, MAJORS)
    check('entry at the first print at or after the entry time',
          len(pos) == 1 and pos[0]['entry'] == te + 300 and pos[0]['entry'] > ev['t_end'] + drift.QUIET
          and pos[0]['exit_t']['120m'] == te + 300 + 120 * M and pos[0]['hour'] == (te + 300) // H)

    # --- causality: only ENTRY_FIELDS are read; fields known after the entry change nothing
    ev_poison = dict(ev, end_price=float('nan'), notional=-1, n=None, rel='after', exit_px=1e9)
    p1, _ = drift.plan([ev], rate, {0: (te + 300, 100.0)}, MAJORS)
    p2, _ = drift.plan([ev_poison], rate, {0: (te + 300, 100.0)}, MAJORS)
    check('poisoned non-entry fields change nothing', p1 == p2)

    class Spy(drift.StressRate):
        seen = []

        def rate(self, t):
            self.seen.append(t)
            return super().rate(t)
    spy = Spy([(ev['t_start'] - 10 * M, 1.0), (te + 5 * S, 1.0), (te + H, 1.0)])
    drift.plan([ev], spy, {0: (te + 300, 100.0)}, MAJORS)
    check('stress switch evaluates only up to t_e - 1 s', spy.seen and max(spy.seen) <= te - S, f'max {max(spy.seen) - te} ms')
    check('entry time is a whole second here (so the bucket boundaries below are exact)', te % S == 0)
    check('stress: a bucket for the last full second before the entry [t_e - 1 s, t_e) counts',
          drift.stressed(ev, drift.StressRate([(te - S, drift.STRESS_THRESHOLD)])))
    check('stress: the bucket starting at the entry [t_e, t_e + 1 s) does not count',
          not drift.stressed(ev, drift.StressRate([(te, drift.STRESS_THRESHOLD)])))
    try:
        drift.exit_queries([dict(p1[0], exit_t={'120m': drift.HOLDOUT_START})])
        check('exit query at the holdout refused', False)
    except AssertionError:
        check('exit query at the holdout refused', True)
    late = {'coin': 'BTC', 'side': 'L', 't_start': drift.HOLDOUT_START - 3 * H, 't_end': drift.HOLDOUT_START - 3 * H + S}
    pos, sk = drift.plan([late], rate, {0: (drift.entry_time(late) + 1, 100.0)}, MAJORS)
    check('events whose 240-min exit would reach the holdout are dropped', not pos and sk['holdout_edge'] == [0])

    # --- stress switch (004 D8) over [first print, entry]
    big = drift.STRESS_THRESHOLD
    in_window = drift.StressRate([(ev['t_start'] - 2 * M, big)])
    before = drift.StressRate([(ev['t_start'] - 6 * M, big)])
    after = drift.StressRate([(te + S, big)])
    mid = drift.StressRate([(ev['t_end'] + 30 * S, big * 0.6), (ev['t_end'] + 40 * S, big * 0.5)])
    check('stress: $77 M in the trailing 5 min at the first print -> skip', drift.stressed(ev, in_window))
    check('stress: flow older than 5 min at the first print -> no skip', not drift.stressed(ev, before))
    check('stress: flow after the entry -> no skip', not drift.stressed(ev, after))
    check('stress: two buckets summing past $77 M between flow end and entry -> skip', drift.stressed(ev, mid))
    pos, sk = drift.plan([ev], in_window, {0: (te + 1, 100.0)}, MAJORS)
    check('a stressed event holds nothing', not pos and sk['stress'] == [0])

    # --- warm-up: events qualified on the cross-coin fallback median are dropped (primary)
    fb = dict(ev, fallback_median=True)
    pos, sk = drift.plan([fb], rate, {0: (te + 1, 100.0)}, MAJORS)
    check('warm-up (fallback median) event dropped in the primary', not pos and sk['warmup_fallback'] == [0])
    pos, sk = drift.plan([fb], rate, {0: (te + 1, 100.0)}, MAJORS, drop_fallback=False)
    check('... and kept in the inclusive 002 set', len(pos) == 1)

    # --- stress switch excluding the cascade coin's own liquidations (primary, D5)
    own_b = [(ev['t_start'] + S, big)]                        # the cascade's own $77 M
    other_b = [(ev['t_start'] + S, big)]                      # another coin's $77 M, same second
    allr = drift.StressRate(own_b)
    ownr = {'BTC': drift.StressRate(own_b)}
    check('own-coin flow alone: stressed inclusive, not stressed excluding own',
          drift.stressed(ev, allr) and not drift.stressed(ev, allr, own=ownr['BTC']))
    allr2 = drift.StressRate([(ev['t_start'] + S, 2 * big)])  # own + other in the same bucket
    check('another coin\'s $77 M stresses either way', drift.stressed(ev, allr2, own=ownr['BTC']))
    pos, sk = drift.plan([ev], allr, {0: (te + 1, 100.0)}, MAJORS, own_rates=ownr)
    check('plan with own_rates: own-driven event is traded', len(pos) == 1 and not sk['stress'])
    pos, sk = drift.plan([ev], allr, {0: (te + 1, 100.0)}, MAJORS, own_rates={})
    check('plan with own_rates but no own series for the coin: market series as is', not pos and sk['stress'] == [0])
    pos, sk = drift.plan([ev], allr, {0: (te + 1, 100.0)}, MAJORS)
    check('plan inclusive (own_rates None): own-driven event skipped', not pos and sk['stress'] == [0])

    # --- one position per coin
    evs = [
        {'coin': 'ETH', 'side': 'L', 't_start': T0, 't_end': T0 + 30 * S},                  # 0: taken
        {'coin': 'ETH', 'side': 'S', 't_start': T0 + 60 * M, 't_end': T0 + 61 * M},          # 1: inside 0's hold -> skip
        {'coin': 'SOL', 'side': 'L', 't_start': T0 + 60 * M, 't_end': T0 + 61 * M},          # 2: other coin -> taken
        {'coin': 'ETH', 'side': 'L', 't_start': T0 + 121 * M, 't_end': T0 + 122 * M},        # 3: after 0's exit -> taken
        {'coin': 'ETH', 'side': 'L', 't_start': T0 + 200 * M, 't_end': T0 + 201 * M},        # 4: inside 3's hold -> skip
    ]
    stressed_first = drift.StressRate([(T0 - M, big)])                                       # 0 stressed: holds nothing
    prints = {i: (drift.entry_time(e) + 100, 100.0) for i, e in enumerate(evs)}
    pos, sk = drift.plan(evs, drift.StressRate([]), prints, MAJORS)
    check('overlap: skips entries inside an open position in the same coin, not in another coin',
          [p['i'] for p in sorted(pos, key=lambda p: p['i'])] == [0, 2, 3] and sk['overlap'] == [1, 4], str(sk))
    pos, sk = drift.plan(evs, stressed_first, prints, MAJORS)
    check('overlap: a stressed (skipped) event does not block the next one',
          sk['stress'] == [0] and 1 in [p['i'] for p in pos] and sk['overlap'] == [3], str(sk))
    exact = [{'coin': 'BTC', 'side': 'L', 't_start': T0, 't_end': T0},
             {'coin': 'BTC', 'side': 'L', 't_start': T0 + 120 * M, 't_end': T0 + 120 * M}]
    pr = {0: (drift.entry_time(exact[0]), 1.0), 1: (drift.entry_time(exact[1]), 1.0)}
    pos, sk = drift.plan(exact, drift.StressRate([]), pr, MAJORS)
    check('overlap boundary: an entry exactly at the previous exit is allowed', len(pos) == 2
          and drift.entry_time(exact[1]) == pr[0][0] + drift.HOLD)

    # --- outcomes wiring
    o = drift.outcomes(p1, {f"{p1[0]['i']}:{h}": 101.0 for h in drift.HORIZONS})
    check('outcomes: return per horizon and slippage',
          abs(o[0]['ret']['120m'] - drift.net_return('L', 100.0, 101.0)) < 1e-12
          and abs(o[0]['ret_slip']['120m']['0.0'] - drift.net_return('L', 100.0, 101.0, 0.0)) < 1e-12)

    # --- the statistics on planted rows
    rng = random.Random(5)
    null = drift.summarise(synth_rows(rng, 450, 0.0), placebo_seeds=100)
    ph, pd = null['placebo_hour_flip'], null['placebo_day_flip']
    check('null fails; gross-flip placebo (costs kept) calibrated: pass share <= 5%, |t| share on gross <= 8%',
          null['pass'] is False and ph['pass_share'] <= 0.05 and ph['gross_abs_share'] <= 0.08 and pd['pass_share'] is not None,
          f"t {null['primary_hour']['t']:.2f} hour-flip {ph} day-flip {pd}")
    # jointness: two positions per hour with gross +g and -g; a joint flip keeps every hour's sum at 0
    jt = []
    for k in range(120):
        e = T0 + k * 3 * H
        for sgn, coin in ((1, 'BTC'), (-1, 'ETH')):
            g = sgn * (0.01 + 0.001 * (k % 7))
            jt.append({'i': len(jt), 'coin': coin, 'side': 'L', 'entry': e, 'hour': e // H, 'day': e // D,
                       'major': True, 'ret': {drift.PRIMARY: g}, 'gross': {drift.PRIMARY: g}})
    pj = drift.placebo(jt, 'hour', 50)
    check('placebo flips each hour cluster jointly (hour sums stay 0 -> never passes, never |t| >= z)',
          pj['pass_share'] == 0 and pj['gross_abs_share'] == 0, str(pj))
    pc = drift.placebo([dict(r, gross={drift.PRIMARY: r['ret'][drift.PRIMARY] + 0.01}) for r in jt], 'hour', 50)
    check('placebo keeps costs: a 1% cost per position never passes', pc['pass_share'] == 0)
    check('null decides close', drift.decide(null) == 'close')
    pos_ = drift.summarise(synth_rows(rng, 450, 0.003), placebo_seeds=100)
    check('planted +0.3% passes all six', pos_['pass'] is True,
          f"t_hour {pos_['primary_hour']['t']:.2f} t_day {pos_['primary_day']['t']:.2f} {pos_['criteria']}")
    check('planted +0.3% decides pull_holdout (>= 25 bp)', drift.decide(pos_) == 'pull_holdout')
    small = drift.summarise(synth_rows(rng, 450, 0.0015, sigma=0.004), placebo_seeds=20)
    check('planted +0.15% passes but sits below the bar', small['pass'] and drift.decide(small) == 'record_below_bar',
          f"mean {small['primary_hour']['mean']:.4f}")
    cr = drift.summarise(synth_rows(rng, 450, 0.0, crash=True), placebo_seeds=20)
    check('one crash hour does not pass and the without-largest-positive-hour block is ~0',
          cr['pass'] is False and abs(cr['without_largest_positive_hour']['mean']) < 0.002
          and cr['largest_positive_hour'] == synth_rows(rng, 3, 0.0)[0]['hour'],
          f"mean {cr['primary_hour']['mean']:.4f} t {cr['primary_hour']['t']:.2f} | excl {cr['without_largest_positive_hour']['mean']:.4f}")
    loss = synth_rows(rng, 450, 0.003)
    for r in loss[:3]:
        r['ret'] = {h: -0.5 for h in drift.HORIZONS}            # one huge LOSS hour
    lo = drift.summarise(loss, placebo_seeds=5)
    check('criterion 6 removes the largest POSITIVE hour, never a loss hour',
          lo['largest_positive_hour'] != loss[0]['hour'] and drift.largest_cluster(loss, how='abs') == loss[0]['hour']
          and lo['without_largest_positive_hour']['mean'] < lo['without_largest_abs_hour']['mean'])
    only_neg = [dict(r, ret={h: -0.01 for h in drift.HORIZONS}) for r in synth_rows(rng, 30, 0.0)]
    check('no positive hour -> nothing removed, criterion 6 fails on a negative mean',
          drift.largest_cluster(only_neg) is None and drift.summarise(only_neg, placebo_seeds=0)['criteria']['6_without_largest_positive_hour_positive'] is False)
    check('by-count and by-absolute blocks reported', pos_['without_largest_abs_hour'] and pos_['without_largest_count_hour'])
    one_side = synth_rows(rng, 450, 0.003)
    for r in one_side:
        if r['side'] == 'S':
            r['ret'] = {h: v - 0.006 for h, v in r['ret'].items()}
    os_ = drift.summarise(one_side, placebo_seeds=10)
    check('a side-specific drift (one side negative) fails criterion 5', os_['criteria']['5_both_sides_positive'] is False)
    # day clustering bites where hours do not: identical shocks within each day across hours
    dd = []
    for k in range(300):
        day = k // 6
        dshock = random.Random(day).gauss(0, 0.03)
        entry = T0 + day * D + (k % 6) * 3 * H
        ret = {h: 0.002 + dshock + rng.gauss(0, 0.002) for h in drift.HORIZONS}
        dd.append({'i': k, 'coin': ('BTC', 'ETH', 'SOL')[k % 3], 'side': 'LS'[k % 2], 'entry': entry,
                   'hour': entry // H, 'day': entry // D, 'major': True, 'ret': ret})
    ds = drift.summarise(dd, placebo_seeds=10)
    check('day-correlated noise: hour t inflated, day t smaller', abs(ds['primary_day']['t']) < abs(ds['primary_hour']['t']),
          f"t_hour {ds['primary_hour']['t']:.2f} t_day {ds['primary_day']['t']:.2f}")
    check('horizons and sides reported', set(pos_['by_horizon']) == set(drift.HORIZONS) and set(pos_['by_side']) == {'L', 'S'})

    # --- holdout read (D14)
    hrng = random.Random(9)
    ok, det = drift.holdout_confirm(synth_rows(hrng, 70, 0.006, sigma=0.008))
    check('holdout: planted +0.6% over 70 positions confirms', ok, str(det['criteria']))
    neg_med = synth_rows(hrng, 70, -0.002, sigma=0.0005)
    for r in neg_med[:20]:
        r['ret'] = {h: 0.05 for h in drift.HORIZONS}
    ok2, det2 = drift.holdout_confirm(neg_med)
    check('holdout: positive mean with a non-positive median does not confirm', not ok2 and not det2['criteria']['median_pos'])
    ok3, _ = drift.holdout_confirm(synth_rows(hrng, 70, -0.003, sigma=0.008))
    check('holdout: negative drift does not confirm', not ok3)
    check('power at mu = z x se is 50%', abs(drift.power_at(0.00164, 0.001, 0.001, 1.64) - 0.5) < 1e-12)

    # --- hedge beta and the hedged return (reported variant)
    brng = random.Random(3)
    btc, eth = {}, {}
    pb = pe = 100.0
    for k in range(40 * 24):
        x = brng.gauss(0, 0.005)
        pb *= math.exp(x)
        pe *= math.exp(2 * x)
        btc[T0 + k * H], eth[T0 + k * H] = pb, pe
    t_e = T0 + 35 * D
    b = drift.beta(eth, btc, t_e)
    poisoned = {k: (v * (3 if (k // H) % 2 else 0.5) if k + H > t_e else v) for k, v in eth.items()}
    check('beta: 2x BTC returns -> 2; candles not closed before the entry are ignored',
          abs(b - 2) < 1e-9 and drift.beta(poisoned, btc, t_e) == b, f'{b}')
    check('beta: too few hours -> None', drift.beta(eth, btc, T0 + 5 * D) is None)
    hr = drift.hedged_return('L', 100.0, 102.0, 50.0, 50.5, 2.0, slip=0.0)
    check('hedged: long coin +2%, short 2 x BTC +1%: (+2% - 9 bp) + 2 x (-1% - 9 bp)',
          abs(hr - ((0.02 - 0.0009) + 2 * (-0.01 - 0.0009))) < 1e-12, f'{hr:.6f}')

    # --- simulation machinery (joint power)
    L = drift.cholesky({0: {0: 4.0}, 1: {0: 2.0, 1: 5.0}})
    check('cholesky 2x2', abs(L[0][0] - 2) < 1e-12 and abs(L[1][0] - 1) < 1e-12 and abs(L[1][1] - 2) < 1e-12)
    pp = [{'coin': ('BTC', 'ETH')[k % 2], 'side': 'LS'[k % 3 == 0], 'entry': T0 + k * 47 * M,
           'hour': (T0 + k * 47 * M) // H, 'day': (T0 + k * 47 * M) // D} for k in range(200)]
    order, low = drift.null_cov(pp, {'BTC': 0.01, 'ETH': 0.01}, {('BTC', 'ETH'): 0.8})
    a = order.index(1)
    b = order.index(0)
    check('null_cov: overlapping opposite-side positions in correlated coins covary negatively',
          low[a][b] < 0 and abs(low[a][b] - (-0.8 * 1e-4 * (120 - 47) / 120)) < 1e-12, f'{low[a][b]}')
    rngc = random.Random(2)
    Lp = drift.cholesky(low)
    draws = [drift.mvn_draw(Lp, rngc) for _ in range(3000)]
    v0 = statistics.fmean(d[0] ** 2 for d in draws)
    c01 = statistics.fmean(d[a] * d[b] for d in draws)
    check('mvn draws reproduce variance and covariance', abs(v0 - 1e-4) < 1.2e-5 and abs(c01 - low[a][b]) < 1.2e-5,
          f'{v0:.2e} {c01:.2e}')
    big_ = drift.simulate_power(pp, {'BTC': 0.004, 'ETH': 0.004}, {('BTC', 'ETH'): 0.8}, 0.004, sims=60, seed=1,
                                holdout_blocks=[('A', 60)], hold_sims=60)
    nul_ = drift.simulate_power(pp, {'BTC': 0.01, 'ETH': 0.01}, {('BTC', 'ETH'): 0.8}, 0.0, sims=150, seed=2,
                                holdout_blocks=[('A', 60)], hold_sims=150)
    check('simulate_power: a large planted mean passes and confirms; no effect rarely passes or confirms',
          big_['p_pass'] > 0.9 and big_['p_pass_and_bar'] > 0.9 and big_['holdout']['A']['p_confirm'] > 0.9
          and nul_['p_pass'] < 0.05 and nul_['holdout']['A']['p_confirm'] < 0.12, f'{big_} {nul_}')
    end, days = drift.holdout_end_for(126, 0.635)
    check('holdout end: 126 positions at 0.635/day -> 199 days -> 2027-01-15',
          days == 199 and end == drift.HOLDOUT_START + 198 * D, f'{days}')

    # --- power arithmetic
    ps = [{'coin': 'BTC', 'entry': T0 + k * 10 * H, 'hour': (T0 + k * 10 * H) // H, 'day': (T0 + k * 10 * H) // D}
          for k in range(100)]
    se = drift.null_se(ps, {'BTC': 0.01}, {})
    check('null SE of independent holds = sigma / sqrt(n)', abs(se - 0.001) < 1e-12, f'{se}')
    pair = [{'coin': 'BTC', 'entry': T0, 'hour': T0 // H, 'day': T0 // D},
            {'coin': 'ETH', 'entry': T0 + 60 * M, 'hour': (T0 + 60 * M) // H, 'day': T0 // D}]
    se_all = drift.null_se(pair, {'BTC': 0.01, 'ETH': 0.01}, {('BTC', 'ETH'): 0.8})
    se_hour = drift.null_se(pair, {'BTC': 0.01, 'ETH': 0.01}, {('BTC', 'ETH'): 0.8}, key='hour')
    exp_all = (2 * 1e-4 + 2 * 0.8 * 1e-4 * 0.5) ** 0.5 / 2
    check('null SE with half-overlapping correlated holds; the hour estimator misses the cross-hour term',
          abs(se_all - exp_all) < 1e-12 and abs(se_hour - (2e-4) ** 0.5 / 2) < 1e-12, f'{se_all:.6f} {se_hour:.6f}')
    m = drift.mde(0.001, 0.0012, 2.33)
    check('MDE: 2.33 x se at 50%, + 0.84 x true se at 80%', abs(m['p50'] - 0.00233) < 1e-12 and abs(m['p80'] - (0.00233 + 0.8416 * 0.0012)) < 1e-12)
    guard_checks()
    print('drift checks passed')


def guard_checks():
    """l7_run.guard_failures in a throwaway git repo: (a)-(f) and the status line."""
    import l7_run
    code, data = ('ct/drift.py', 'l7_run.py'), ('data/c.json',)
    reg_rel = 'registrations/005-x.md'
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root,
                                        capture_output=True, text=True, check=True)
        git('init', '-q')
        for f, body in (('ct/drift.py', 'x = 1\n'), ('l7_run.py', 'y = 2\n'), ('data/c.json', '{}\n'),
                        ('results.jsonl', json.dumps({'registration': '004-y.md'}) + '\n')):
            (root / f).parent.mkdir(parents=True, exist_ok=True)
            (root / f).write_text(body)
        (root / '.gitignore').write_text('data/\n')

        def write_reg(status='REGISTERED', hashes=None):
            hashes = hashes or {f: l7_run.sha256(root / f) for f in (*code, *data)}
            rows = '\n'.join(f'| `{f}` | `{h}` |' for f, h in hashes.items())
            (root / reg_rel).parent.mkdir(exist_ok=True)
            (root / reg_rel).write_text(f'# 005\n\n**Status: {status} 2026-10-10**\n\n| file | sha256 |\n|---|---|\n{rows}\n')
        fails = lambda: l7_run.guard_failures(root, reg_rel, code, data)
        write_reg()
        git('add', '.gitignore', 'ct', 'l7_run.py', 'results.jsonl')
        git('commit', '-q', '-m', 'code')
        f0, c0 = fails()
        check('guard (a): an untracked registration is refused', any(x.startswith('(a)') for x in f0) and any(x.startswith('(c)') for x in f0), str(f0))
        git('add', reg_rel)
        f1, _ = fails()
        check('guard (c): staged but never committed -> no commit -> refused', any(x.startswith('(c)') for x in f1), str(f1))
        git('commit', '-q', '-m', 'register')
        f2, c2 = fails()
        check('guard: committed, clean, hashes match, no result row -> allowed', f2 == [] and len(c2) == 40, str(f2))
        (root / reg_rel).write_text((root / reg_rel).read_text() + 'edit\n')
        f3, _ = fails()
        check('guard (b): registration modified after commit -> refused', any(x.startswith('(b)') for x in f3), str(f3))
        git('checkout', '--', reg_rel)
        (root / 'ct/drift.py').write_text('x = 3\n')
        f4, _ = fails()
        check('guard (d)+(e): guarded code edited -> not clean and hash mismatch',
              any(x.startswith('(d)') for x in f4) and any(x.startswith('(e) ct/drift.py') for x in f4), str(f4))
        git('checkout', '--', 'ct/drift.py')
        (root / 'data/c.json').write_text('{"changed": 1}\n')
        f5, _ = fails()
        check('guard (e): the (untracked) event file changed -> hash mismatch only', f5 == ['(e) data/c.json does not match its registered SHA-256'], str(f5))
        (root / 'data/c.json').write_text('{}\n')
        with open(root / 'results.jsonl', 'a') as fh:
            fh.write(json.dumps({'registration': '005-x.md'}) + '\n')
        f6, _ = fails()
        check('guard (f): a result row for this registration already exists -> refused', any(x.startswith('(f)') for x in f6), str(f6))
        git('checkout', '--', 'results.jsonl')
        write_reg(status='DRAFT')
        git('commit', '-q', '-am', 'draft')
        f7, _ = fails()
        check('guard: status DRAFT -> refused', any('not REGISTERED' in x for x in f7), str(f7))


if __name__ == '__main__':
    main()
