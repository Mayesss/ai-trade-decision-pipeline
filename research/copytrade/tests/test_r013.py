"""Registration 013 (N3 i, new perp listings), Revision 1 — mechanics, statistics, fetch, build, read gate and guard on
SYNTHETIC data only. No real listing window is read (the real-file checks read the registration, hashes, schemas).

    .venv/bin/python -m tests.test_r013
"""
import csv
import io
import json
import math
import os
import random
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

from ct import listings013 as L

HERE = Path(__file__).resolve().parent.parent
MIN, HOUR, DAY = L.MIN, L.HOUR, L.DAY
N_CHECKS = [0]
REF = (L.Refused,)


def check(name, ok, detail=''):
    N_CHECKS[0] += 1
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def close(a, b, tol=1e-12):
    return a is not None and b is not None and abs(a - b) <= tol


def _raises(f, exc=(AssertionError,)):
    try:
        f()
    except exc:
        return True
    return False


# ------------------------------------------------------------------ synthetic bars (t, o, h, l, c, qv, n)
FT = L.ms('2024-03-05') + 12 * HOUR
T_IN = FT + L.ENTRY_DELAY
T_X = T_IN + L.HOLD
QV = 1e6                                            # per minute -> $60 M an hour -> entry 5 bp, exit 8 bp


def bars(price=lambda t: 100.0, high=None, start=FT, end=FT + 10 * DAY, skip=lambda t: False, dead_from=None, qv=QV):
    out, t = [], start
    while t < end:
        if not skip(t):
            p = price(t)
            h = high(t) if high else p
            dead = dead_from is not None and t >= dead_from
            out.append((t, p, max(p, h), p, p, 0.0 if dead else qv, 0 if dead else 5))
        t += MIN
    return out


def ev():
    return {'id': 'X@2024-03-05', 'first_trade': FT}


def every(hours, rate, start=FT, end=FT + 10 * DAY, offset_ms=3):
    out, t = [], start - start % (hours * HOUR) + hours * HOUR
    while t < end:
        out.append((t + offset_ms, hours, rate(t) if callable(rate) else rate))
        t += hours * HOUR
    return out


END = L.DATA_END['discovery']


def main():
    stats_checks()
    trade_checks()
    calendar_checks()
    book_checks()
    decision_checks()
    placebo_and_sim_checks()
    fetch_checks()
    build_checks()
    panel_checks()
    run_e2e_checks()
    guard_checks()
    bypass_checks()
    real_file_checks()
    print(f'all checks passed ({N_CHECKS[0]})')


# ------------------------------------------------------------------ statistics
def stats_checks():
    print('statistics')
    from ct import carry                     # read-only: 010's tabulated quantiles, an independent reference
    ok95 = all(abs(L.t_crit(df, 0.95) - carry._T95[df - 1]) < 2e-3 for df in range(1, 31))
    ok975 = all(abs(L.t_crit(df, 0.975) - carry._T975[df - 1]) < 2e-3 for df in range(1, 31))
    check('t quantiles match the printed table for df 1..30 (0.95 and 0.975)', ok95 and ok975)
    m, t, G = L.clustered_t([1.0, 3.0, 2.0, 4.0], ['a', 'a', 'b', 'b'])
    check('clustered t: CR1 with G/(G-1), hand-computed', close(m, 2.5) and close(t, 2.5 / math.sqrt(2 * 2 / 16)) and G == 2)
    x = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
    y = [0.5 - 2.0 * v + e for v, e in zip(x, (0.1, -0.1, 0.05, -0.05, 0.02, -0.02))]
    o = L.ols_alpha(y, x, ['a', 'a', 'b', 'b', 'c', 'c'])
    n, sx, sy = 6, sum(x), sum(y)
    b = (n * sum(a * c for a, c in zip(x, y)) - sx * sy) / (n * sum(v * v for v in x) - sx * sx)
    check('OLS alpha and beta equal the closed form', close(o['beta'], b, 1e-12) and close(o['alpha'], (sy - b * sx) / n, 1e-12))
    # CR1 by brute force: (X'X)^-1 (sum_g X_g'e_g e_g'X_g) (X'X)^-1 * G/(G-1) * (n-1)/(n-2)
    a_, e = o['alpha'], [yi - o['alpha'] - o['beta'] * xi for xi, yi in zip(x, y)]
    sxx = sum(v * v for v in x)
    det = n * sxx - sx * sx
    inv = [[sxx / det, -sx / det], [-sx / det, n / det]]
    meat = [[0.0, 0.0], [0.0, 0.0]]
    for g in ('a', 'b', 'c'):
        idx = [i for i, c in enumerate(['a', 'a', 'b', 'b', 'c', 'c']) if c == g]
        u = [sum(e[i] for i in idx), sum(e[i] * x[i] for i in idx)]
        for i in range(2):
            for j in range(2):
                meat[i][j] += u[i] * u[j]
    v00 = sum(inv[0][i] * meat[i][j] * inv[j][0] for i in range(2) for j in range(2)) * 3 / 2 * 5 / 4
    check('OLS alpha CR1 t equals the brute-force sandwich', close(o['t'], a_ / math.sqrt(v00), 1e-9))
    o2 = L.ols_cols(y, [x], ['a', 'a', 'b', 'b', 'c', 'c'])
    check('the general OLS (ols_cols) equals the one-regressor fit (alpha and its CR1 t)', close(o2['alpha'], o['alpha'], 1e-12)
          and close(o2['t'], o['t'], 1e-9))
    check('beta groups: calendar quarters; a quarter with < 10 events joins the previous; a short first group joins the next',
          L.beta_groups(['2024-Q1'] * 3 + ['2024-Q2'] * 12 + ['2024-Q3'] * 5 + ['2024-Q4'] * 11)
          == {'2024-Q1': 0, '2024-Q2': 0, '2024-Q3': 0, '2024-Q4': 1})
    rnd_ = random.Random(9)
    reg = []
    for i in range(240):
        qq = ('2025-Q3', '2025-Q4', '2026-Q1', '2026-Q2')[i // 60]
        m_ = (-0.06 if qq == '2025-Q4' else 0.0) + rnd_.gauss(0, 0.05)
        reg.append({'net': -(2.5 if qq == '2025-Q4' else 1.0) * m_ + rnd_.gauss(0, 0.02), 'mkt': m_, 'quarter': qq, 'week': f'w{i // 3}'})
    single = L.ols_alpha([r['net'] for r in reg], [r['mkt'] for r in reg], [r['week'] for r in reg])
    fitq, bq = L.alpha_fit(reg)
    check('a beta regime (2.5 in the crash quarter, 1 elsewhere; no edge): the pooled single-beta alpha is biased up; '
          'the quarter-beta alpha is not (review r1 B1)', single['alpha'] > 0.01 and abs(fitq['alpha']) < 0.006 and abs(bq['2025-Q4'] + 2.5) < 0.2,
          f"single {single['alpha']:.4f} quarter {fitq['alpha']:.4f}")
    h1, h2 = L.halves([1.0, 2.0, 3.0, 4.0, 5.0], ['w1', 'w2', 'w2', 'w3', 'w4'])
    check('halves by listing week order', close(h1, 2.0) and close(h2, 4.5))
    check('without the largest positive week', close(L.without_largest_positive_week([1.0, 2.0, -3.0, 5.0], ['a', 'b', 'b', 'c']), 0.0))
    check('trimmed mean drops 5 % each tail (a token-switch jump cannot move the index)',
          close(L.trimmed_mean([0.0] * 38 + [50.0, -0.5]), 0.0))
    check('alpha critical value = max(t_{G-2}(q), the registered calibrated value)',
          close(L.alpha_crit(40, L.Q_CONFIRM), max(L.t_crit(38, 0.95), L.CAL_ALPHA_CONFIRM))
          and close(L.alpha_crit(65, L.Q_DISCOVERY), max(L.t_crit(63, 0.975), L.CAL_ALPHA_DISCOVERY)) and L.alpha_crit(2, 0.95) is None)


# ------------------------------------------------------------------ the trade
def trade_checks():
    print('the trade (simulate)')
    f = L.FEE_DECISION
    s_in, s_out = L.slip_tiers(60e6)
    r = L.simulate(ev(), bars(), [], END)
    check('flat price: net = -(2 fees + entry slip + exit slip one tier worse); exit at entry + 7 d',
          r['status'] == 'exit' and close(r['net'], -(2 * f + s_in + s_out)) and r['entry_t'] == T_IN and r['exit_t'] == T_X
          and (s_in, s_out) == (0.0005, 0.0008))
    check('slippage tiers by the average hourly quote volume of the first 24 h; exit one tier worse; worst exit 50 bp',
          L.slip_tiers(1e6) == (0.0030, 0.0050) and L.slip_tiers(5e6) == (0.0015, 0.0030))
    r = L.simulate(ev(), bars(price=lambda t: 100.0 if t < T_IN + DAY else 80.0), [], END)
    check('price 100 -> 80: net = 0.2 - fees - slippage (exit legs scaled by 0.8)',
          close(r['net'], 0.2 - f - f * 0.8 - s_in - s_out * 0.8, 1e-12))
    fund = every(4, 0.001)
    r = L.simulate(ev(), bars(), fund, END)
    check('funding +0.1 %/4 h: 42 settlements in (entry, exit]', r['n_settlements'] == 42 and close(r['funding'], 0.042, 1e-12))
    check('a settlement in the entry minute is not counted; one in the exit minute is',
          close(L.simulate(ev(), bars(), [(T_IN + 2, 4, 0.5)], END)['funding'], 0.0)
          and close(L.simulate(ev(), bars(), [(T_X + 2, 4, 0.01)], END)['funding'], 0.01))
    half = lambda t: 100.0 if t < T_IN + DAY else 50.0
    check('funding paid at the settlement price', close(L.simulate(ev(), bars(price=half), [(T_IN + 2 * DAY + 5, 4, 0.01)], END)['funding'], 0.005, 1e-12))
    neg = L.simulate(ev(), bars(), [(T_IN + DAY + 1, 4, -0.004), (T_IN + 2 * DAY + 1, 4, 0.004)], END, pay_mult=2.0, receive_mult=0.5)
    check('stress: pay x2, receive x0.5', close(neg['funding'], -0.008 + 0.002, 1e-12))
    check('stress: capped at 2 % per settlement',
          close(L.simulate(ev(), bars(), [(T_IN + DAY + 1, 1, -0.015)], END, pay_mult=2.0, receive_mult=0.5)['funding'], -0.02, 1e-12))
    thr = (2 - f - s_in) / (1 + L.MMR)
    spike = lambda x: (lambda t: 100.0 * x if T_IN + 3 * DAY <= t < T_IN + 3 * DAY + MIN else 100.0)
    r1, r2 = L.simulate(ev(), bars(high=spike(thr + 0.01)), [], END), L.simulate(ev(), bars(high=spike(thr - 0.01)), [], END)
    check(f'liquidation just above {thr:.3f}x (net -1), not just below', r1['status'] == 'liquidated' and r1['net'] == -1.0 and r2['status'] == 'exit')
    check('liquidated components add up to the net', close(r1['price'] + r1['funding'] - r1['fees'] - r1['slip'], -1.0))
    check('funding paid out of the isolated margin lowers the liquidation price',
          L.simulate(ev(), bars(high=spike(thr - 0.15)), [(T_IN + DAY + 1, 4, -0.2)], END)['status'] == 'liquidated')
    check('no traded bar within 60 min of first trade + 24 h -> no_entry',
          L.simulate(ev(), bars(skip=lambda t: T_IN <= t < T_IN + 60 * MIN), [], END)['status'] == 'no_entry')
    zero_at_entry = [b if not (T_IN <= b[0] < T_IN + 30 * MIN) else (*b[:5], 0.0, 0) for b in bars()]
    check('entry at the first TRADED bar (zero-trade bars are not fills)', L.simulate(ev(), zero_at_entry, [], END)['entry_t'] == T_IN + 30 * MIN)
    dead = bars(price=lambda t: 100.0 if t < T_IN + 2 * DAY else 60.0, dead_from=T_IN + 5 * DAY)
    r6 = L.simulate(ev(), dead, [], END)
    check('a listing whose archive keeps flat ZERO-TRADE rows after its last trade (delisting) exits at its last TRADED close: '
          'exit_last_trade, not exit (review B M1)', r6['status'] == 'exit_last_trade' and close(r6['p_x'], 60.0) and r6['exit_t'] == T_IN + 5 * DAY)
    r7 = L.simulate(ev(), bars(skip=lambda t: T_X <= t < T_X + 3 * HOUR), [], END)
    check('a halt at the exit target: exit at the first traded bar within 6 h', r7['status'] == 'exit' and r7['exit_t'] == T_X + 3 * HOUR)
    r7b = L.simulate(ev(), bars(skip=lambda t: T_X - DAY <= t < T_X + 12 * HOUR), [], END)
    check('a data hole across the exit window with trading 12 h after the target: exit_late at the first traded bar (A m9)',
          r7b['status'] == 'exit_late' and r7b['exit_t'] == T_X + 12 * HOUR)
    r7c = L.simulate(ev(), bars(skip=lambda t: T_X - DAY <= t < T_X + 30 * HOUR), [], END)
    check('no trade within 24 h after the target: exit_last_trade at the last traded close BEFORE the target (late exits '
          'capped, so the tail bounds stay exact; review r1 m4)', r7c['status'] == 'exit_last_trade' and r7c['exit_t'] <= T_X - DAY)
    check("the calendar's data end bounds every read", L.simulate(ev(), bars(), [], T_X - HOUR)['status'] == 'exit_last_trade')
    mk = L.simulate(ev(), bars(price=lambda t: 100.0 if t < T_IN + DAY else 90.0), [], END)['marks']
    check('daily marks at each UTC midnight while open (entry costs paid, price mark to the last traded close)',
          len(mk) == 7 and close(mk[-1][1], -f - s_in + 0.10, 1e-12) and all(m % DAY == 0 for m, _ in mk))
    fp = [(FT - HOUR, 8, 0.5), (FT + 4 * HOUR + 3, 4, -0.001), (FT + 8 * HOUR + 3, 4, 0.003), (T_IN + 3, 4, 0.002), (T_IN + 4 * HOUR, 4, 9.0)]
    check("T2's feature: rates settled in (first trade, entry]", close(L.funding_before_entry(FT, T_IN, fp), 0.004, 1e-12)
          and L.funding_before_entry(FT, T_IN, []) is None)


# ------------------------------------------------------------------ calendars, events, continuations, segments
def calendar_checks():
    print('calendars, embargo, events, continuations, later segments')
    check('calendar boundaries and embargoes', L.calendar_of_day('2024-01-01') == 'discovery' and L.calendar_of_day('2025-07-19') == 'embargo'
          and L.calendar_of_day('2025-07-28') == 'confirmation' and L.calendar_of_day('2026-06-22') == 'embargo'
          and L.calendar_of_day('2023-12-23') == 'embargo' and L.calendar_of_day('2026-07-01') == 'outside')
    ok = True
    for cal, (a, b) in L.CALENDARS.items():
        last = L.day_of(b - DAY)
        ok &= last and (L.ms(last) + DAY - MIN + L.ENTRY_DELAY + L.HOLD + L.EXIT_WINDOW < L.DATA_END[cal])
        ok &= L.ms(L.pull_days(last)[-1]) + DAY <= L.DATA_END[cal]
    check("every byte an event needs lies before its calendar's DATA_END", ok)
    days = {'AAAUSDT': ['2019-12-31', '2020-01-02'], 'BBBUSDT': ['2024-01-05', '2024-03-01'], 'XAUUSDT': ['2025-12-20'], 'DDDUSDC': ['2024-01-01']}
    e = {x['id']: x for x in L.events_from_listing(days, {'XAUUSDT'})}
    check('events: the first file of each universe symbol; censored at the archive start; excluded and non-USDT out',
          e['AAAUSDT@2019-12-31']['censored'] == 1 and 'BBBUSDT@2024-01-05' in e and 'BBBUSDT@2024-03-01' not in e
          and 'XAUUSDT@2025-12-20' not in e and not any(k.startswith('DDD') for k in e))
    hs = [L.ms('2024-06-01') + i * HOUR for i in range(100)] + [L.ms('2024-07-01') + i * HOUR for i in range(10)]
    check('later trading segments: a run after >= 7 days without trades is found; hours before not_before are ignored',
          L.later_trading_segments(hs, L.ms('2024-05-01')) == [L.ms('2024-07-01')]
          and L.later_trading_segments(hs, L.ms('2024-06-20')) == [])
    check('continuations: ONE rule (p: the token or its predecessor had a Binance perp before d0) plus (d) gold; '
          'the withdrawn rule-(c) names (LUMIA, SYRUP, CFG, ASTER, BEAMX) and USTC are back in the events',
          all(v[0] in 'pd' and (v[1] is None) == (v[0] == 'd') for v in L.CONTINUATIONS.values())
          and not any(k.split('USDT@')[0] in ('LUMIA', 'SYRUP', 'CFG', 'ASTER', 'BEAMX', 'USTC') for k in L.CONTINUATIONS)
          and sum(1 for k in L.CONTINUATIONS if L.calendar_of_day(k.split('@')[1]) == 'discovery') == 14
          and sum(1 for k in L.CONTINUATIONS if L.calendar_of_day(k.split('@')[1]) == 'confirmation') == 5)
    check('continuation_failures: a predecessor whose first file is not before d0 is caught (point in time)',
          L.continuation_failures({'MATICUSDT': ['2024-09-20']}) != [] and
          not [f for f in L.continuation_failures({'MATICUSDT': ['2020-10-22']}) if f.startswith('POLUSDT')])
    check('pre-market flag only from 2025', L.is_premarket('2025-04-05', False) and not L.is_premarket('2021-03-01', False))
    check('months_between spans month boundaries', L.months_between(L.ms('2024-01-31'), L.ms('2024-02-08')) == ['2024-01', '2024-02'])


# ------------------------------------------------------------------ the book
def _row(i, t, net, marks=None, hold=L.HOLD):
    r = {'id': f'e{i:04d}', 'entry_t': t, 'exit_t': t + hold, 'net': net, 'week': L.iso_week(t), 'liquidated': net == -1.0}
    if marks is not None:
        r['marks'] = marks
    return r


def book_checks():
    print('the book (rolling caps, exact tail bounds, MTM, Kelly, deploy)')
    t0 = L.ms('2024-03-04')
    rows = [_row(i, t0 + i * HOUR, 0.10) for i in range(10)]
    b = L.book(rows, 1.0)
    check('rolling week cap: 10 entries within hours -> 7 taken', b['events_taken'] == L.WEEK_MAX)
    rnd = random.Random(7)
    dense = [_row(i, t0 + i * 2 * HOUR, -1.0, hold=rnd.choice((HOUR, 3 * DAY, L.HOLD, L.HOLD + L.LATE_WINDOW))) for i in range(1200)]
    b = L.book(dense, 1.0)
    check(f'structural bounds hold on an adversarial dense schedule incl. late exits at target + 24 h (every exit -1): week >= {L.STRUCTURAL_WEEK:.3f}, '
          f'worst exit month >= {L.STRUCTURAL_MONTH:.3f}', b['worst_week'] >= L.STRUCTURAL_WEEK - 1e-12 and b['worst_month'] >= L.STRUCTURAL_MONTH - 1e-12,
          f"{b['worst_week']:.4f} {b['worst_month']:.4f}")
    check('the pre-registered plausible month is inside the -30 % line', L.PLAUSIBLE_MONTH > -0.30 and close(L.PLAUSIBLE_MONTH, -0.296875))
    mk = [(t0 + DAY * d, -0.4) for d in (1, 2, 3)]
    b = L.book([_row(0, t0 + HOUR, 0.2, marks=mk)], 1.0)
    check('daily mark-to-market drawdown sees an open loss the realised series cannot (-40 % mark, +20 % exit)',
          close(b['max_drawdown_daily_mtm'], -0.4 * L.C_EVENT) and b['max_drawdown_realised'] == 0.0)
    rows = [_row(i, t0 + i * 2 * DAY, 0.05 + rnd.gauss(0, 0.2)) for i in range(150)]
    b = L.book(rows, 300 * DAY / L.YEAR)
    check('Kelly finite; quarter Kelly = Kelly / 4', b['kelly_fraction_per_event'] is not None and close(b['quarter_kelly_per_event'], b['kelly_fraction_per_event'] / 4))
    check('squeeze months: exit months at or below half the plausible month', L.squeeze_months({'2024-01': -0.15, '2024-02': -0.14}) == ['2024-01'])
    conf = [_row(i, L.ms('2025-08-01') + i * 2 * DAY, 0.06) for i in range(100)]
    pool_liq = [_row(i, L.ms('2024-01-01') + i * 2 * DAY, -1.0 if i % 10 == 0 else 0.06) for i in range(200)]
    pooled = {'discovery': (pool_liq, pool_liq, 1.1), 'confirmation': (conf, conf, 0.55)}
    d = L.deploy(conf, conf, pooled, 0.55, {})['raw']
    check('deploy: a liquidation-free confirmation is charged the pooled liquidation rate (peso problem)',
          d['pi_conf'] == 0 and d['pi_pool'] > 0 and d['mu_tail'] < d['mu_conf'] and d['tail_charge_per_year'] > 0)
    check('deploy: one definition; the economic bar is evaluated at c*, after the charges',
          d['c_star'] <= L.C_EVENT and close(d['annual_charged'], d['annual_at_c_star'] - d['tail_charge_per_year']))
    bear = [dict(_row(i, L.ms('2025-08-01') + i * 2 * DAY, 0.05), mkt=-0.05, quarter='2025-Q3') for i in range(100)]
    db = L.deploy(bear, bear, {'confirmation': (bear, bear, 0.55)}, 0.55, {'2025-Q3': -1.0})
    check('deploy on min(raw, adjusted) (review r1 M1): a bear calendar whose raw +5 % is all beta (adjusted 0) does not deploy',
          db['raw']['bars']['mean_tail_ge_100bp'] and db['adjusted']['mu_tail'] < 0.01 and db['deploy'] is False
          and db['c_star'] == min(db['raw']['c_star'], db['adjusted']['c_star']))
    worse = [_row(i, L.ms('2024-01-01') + i * DAY, -1.0) for i in range(60)]
    d2 = L.deploy(conf, conf, {'discovery': (worse, worse, 1.0), 'confirmation': (conf, conf, 0.55)}, 0.55, {})['raw']
    check('deploy: a pooled worst month beyond -30 % scales c* down to the -30 % size',
          close(d2['c_star'], L.C_EVENT * -0.30 / d2['worst_month_pooled_at_c0']) and d2['c_star'] < L.C_EVENT)


# ------------------------------------------------------------------ criteria, decisions, recheck from records
def good_stats(**over):
    s = {'n': 200, 'G': 60, 'mean': 0.04, 't': 3.0, 'median_raw': 0.03, 'alpha': 0.03, 'alpha_beta': -1.2, 'alpha_t': 3.5,
         'alpha_G': 58, 'alpha_n': 190, 'adj_median': 0.02, 'adj_half_1': 0.02, 'adj_half_2': 0.04,
         'alpha_without_largest_positive_week': 0.025, 'mean_econ': 0.03, 'annual_econ': 0.2, 'span_years': 1.55,
         'q': L.Q_DISCOVERY}
    s.update(over)
    return s


def syn_records(n_weeks=60, mu=0.05, seed=3, t0=None):
    rnd = random.Random(seed)
    t0 = t0 or L.CALENDARS['discovery'][0] + 12 * HOUR
    out = []
    for w in range(n_weeks):
        m = rnd.gauss(0, 0.05)
        for j in range(4):
            ft = t0 + w * 7 * DAY + j * DAY
            mk = m + rnd.gauss(0, 0.01)
            out.append({'id': f'{w}-{j}', 'week': L.iso_week(ft), 'entry_t': ft + DAY, 'exit_t': ft + 8 * DAY, 'status': 'exit',
                        'net': mu - 1.3 * mk + rnd.gauss(0, 0.08), 'net_stress': 0.0, 'primary': True, 'premarket': False,
                        'continuation': None, 't2': j % 3 != 0, 'mkt': mk, 'liquidated': False, 'slip_tier': 0.0005})
    return out


def disc_row_from_records(recs, span=1.55):
    tr = L.records_by_trial(recs)
    trials = {}
    for tn in L.TRIALS:
        s = L.trial_stats(tr[tn], span, L.Q_DISCOVERY)
        c, ok, cr = L.discovery_criteria(s)
        trials[tn] = {**s, 'crit': cr, 'criteria': c, 'pass': ok}
    return json.loads(json.dumps({'registration': '013-x.md', 'mode': 'discovery', 'trials': trials, 'span_years': span,
                                  'events': recs, 'decision': L.decide_discovery({k: v['pass'] for k, v in trials.items()})}))


def decision_checks():
    print('criteria, decision, recheck from per-event records')
    c, ok, cr = L.discovery_criteria(good_stats())
    check('a clean pass: all seven criteria; crit = max(t_56(0.975), CAL_D)', ok and close(cr, L.alpha_crit(58, L.Q_DISCOVERY)))
    fails = {'alpha t below crit': good_stats(alpha_t=cr - 1e-6), 'alpha <= 0': good_stats(alpha=-0.002),
             'adjusted median negative': good_stats(adj_median=-0.001), 'adjusted second half negative': good_stats(adj_half_2=-0.01),
             'alpha without the best week <= 0': good_stats(alpha_without_largest_positive_week=-0.001),
             'min(raw, adjusted) mean below 100 bp': good_stats(mean_econ=0.009),
             'min(raw, adjusted) book below 3 %/yr': good_stats(annual_econ=0.029),
             'too few weeks': good_stats(alpha_G=9), 'too few events': good_stats(alpha_n=19)}
    check('each criterion alone fails the trial', all(not L.discovery_criteria(s)[1] for s in fails.values()),
          str([k for k, s in fails.items() if L.discovery_criteria(s)[1]]))
    check('the raw t is NOT a criterion (raw net feeds the economic bars only; orchestrator decision 1)',
          L.discovery_criteria(good_stats(t=-5.0))[1])
    check('decision: T1 precedence', L.decide_discovery({'T1': True, 'T2': True}) == ['read_confirmation', 'T1']
          and L.decide_discovery({'T1': False, 'T2': True}) == ['read_confirmation', 'T2'] and L.decide_discovery({}) == ['close', None])
    row = disc_row_from_records(syn_records())
    dec, f = L.recheck_discovery(row)
    check('recheck of a consistent row (statistics recomputed from its per-event records): no failures, a carry',
          not f and dec[0] == 'read_confirmation', str(f))
    forged = json.loads(json.dumps(row))
    forged['events'][5]['net'] += 0.5
    check('a tampered per-event record is caught (stored aggregates no longer match the records; review B m8)',
          any('per-event records' in x for x in L.recheck_discovery(forged)[1]))
    forged = json.loads(json.dumps(row))
    forged['trials']['T1']['alpha_t'] = 0.5
    check('a forged alpha t under pass labels is caught', L.recheck_discovery(forged)[1] != [])
    check('recheck of an empty row fails', L.recheck_discovery({})[1] != [])
    s = {'alpha_n': 120, 'alpha_G': 40, 'alpha_tail': 0.02, 'alpha_tail_t': 2.5, 'alpha_tail_without_largest_positive_week': 0.01}
    ok, cr = L.confirm(s)
    check('confirmation = existence test on the TAIL-ADJUSTED alpha: > 0, t >= max(t_{G-2}(0.95), CAL_C), > 0 without the best week',
          ok and close(cr, L.alpha_crit(40, L.Q_CONFIRM)))
    check('confirmation fails at t just below crit, alpha_tail <= 0, < 5 weeks, or <= 0 without the best week',
          not L.confirm(dict(s, alpha_tail_t=cr - 1e-6))[0] and not L.confirm(dict(s, alpha_tail=-0.01))[0]
          and not L.confirm(dict(s, alpha_G=4))[0] and not L.confirm(dict(s, alpha_tail_without_largest_positive_week=-0.001))[0])
    mk = lambda n, k: [{'liquidated': i < k} for i in range(n)]
    pr = L.pooled_liquidation_rate({'reported': mk(100, 0), 'discovery': mk(100, 4), 'confirmation': mk(50, 0)})
    pr2 = L.pooled_liquidation_rate({'reported': mk(100, 10), 'discovery': mk(100, 1), 'confirmation': mk(50, 0)})
    check('pooled liquidation rate = max(all calendars, discovery + confirmation): a calm 2020-23 block cannot dilute it '
          '(review r2-m1)', close(pr, 4 / 150) and close(pr2, 11 / 250))
    recs = syn_records()
    st0 = L.trial_stats(recs, 1.55, L.Q_CONFIRM, pool=0.0)
    st1 = L.trial_stats(recs, 1.55, L.Q_CONFIRM, pool=0.02)
    check('tail-adjusted alpha (review r1 M2): no charge when the pooled liquidation rate <= the sample\'s; otherwise alpha - '
          '(pool - pi) * (1 + mean non-liquidated net), same SE', close(st0['alpha_tail'], st0['alpha'])
          and close(st1['alpha_tail'], st1['alpha'] - 0.02 * (1 + sum(r['net'] for r in recs) / len(recs)), 1e-12)
          and close(st1['alpha_tail_t'] / st1['alpha_tail'], st1['alpha_t'] / st1['alpha'], 1e-9))
    check('decision after CONFIRM only: deploy -> paper; not deploy -> below bar; no CONFIRM -> not confirmed',
          L.decide_confirmation(True, {'deploy': True}) == 'confirm_paper' and L.decide_confirmation(True, {'deploy': False}) == 'confirm_below_bar'
          and L.decide_confirmation(False, {'deploy': True}) == 'not_confirmed')


# ------------------------------------------------------------------ placebo and simulation
def placebo_and_sim_checks():
    print('placebo and simulation (synthetic)')
    rows = [dict(r, fees=0.0012, slip=0.0013) for r in syn_records(mu=0.0)]
    p = L.placebo(rows, L.t_crit(59, 0.975))
    check('placebo: calendar-month sign flips, costs kept', p['seeds'] == 200 and 0 <= p['pass_share_net_t'] <= 0.2, json.dumps(p))
    pa = L.placebo_alpha(syn_records(mu=0.0), {}, L.t_crit(58, 0.975))
    check('alpha placebo: month flips of the adjusted series (review r1 m9)', pa['seeds'] == 200 and pa['share_t_ge_crit'] <= 0.2, json.dumps(pa))
    rnd = random.Random(1)
    sched = [L.CALENDARS['discovery'][0] + int(rnd.random() * 500 * DAY) for _ in range(200)]
    reg_ev = L.sim_events(sched, 0.0, 1.0, 1.0, 0.0, random.Random(2), regime=(L.ms('2024-06-01'), L.ms('2024-09-01'), 2.5, 0.03))
    check('regime worlds: events with a first trade in the window carry beta_in; every event has its quarter',
          all('quarter' in e for e in reg_ev) and len(reg_ev) == 200)
    ev_ = L.sim_events(sched, 0.0, 1.5, 1.0, 0.02, rnd)
    check('simulated events sit at the given first-trade times with overlapping 7-day holds and a continuous market',
          len(ev_) == 200 and all(e['exit_t'] - e['entry_t'] == L.HOLD for e in ev_) and len({round(e['mkt'], 9) for e in ev_}) > 150)
    big = L.simulate_worlds(sched, sched[:120], 1.5, 0.9, [{'name': 'big', 'mu_d': 0.12, 'mu_c': 0.12, 'beta': 1.0, 'k': 1.0,
                                                          'drift_d': 0.0, 'drift_c': 0.0}], sims=30)[0]
    r = L.world_rates(big['records'], L.CAL_ALPHA_DISCOVERY, L.CAL_ALPHA_CONFIRM)
    check('a large true edge passes discovery and confirmation (rates from the stored draws)', r['disc'] >= 0.9 and r['joint'] >= 0.8, json.dumps(r))
    r0 = L.world_rates(big['records'], 1e9, 1e9)
    check('world_rates re-evaluates the same draws at any critical value (an infinite one passes nothing)', r0['disc'] == 0 and r0['joint'] == 0)


# ------------------------------------------------------------------ fetch (fixture HTTP)
def fetch_checks():
    print('fetch (fixture HTTP: forward paging, errors never cached, cutoff refused)')
    import r013_fetch as F
    import hashlib
    saved = dict(F.HTTP)
    F.HTTP['sleep'] = lambda s: None
    F.PACER.gap = 0.0
    try:
        keys = [f'data/x/K-1m-2024-{m:02d}-{d:02d}.zip' for m in range(1, 13) for d in range(1, 29)]
        seen = []

        def s3(url):
            import urllib.parse as up
            q = dict(up.parse_qsl(up.urlparse(url).query))
            seen.append(q.get('marker', ''))
            rest = [k for k in keys if k > q.get('marker', '')]
            body = ''.join(f'<Contents><Key>{k}</Key><Size>10</Size></Contents>' for k in rest[:100])
            return 200, (f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>'
                         f'{"true" if len(rest) > 100 else "false"}</IsTruncated>{body}</ListBucketResult>').encode()
        F.HTTP['get'] = s3
        got, _ = F.list_prefix('data/x/')
        check('listing pages FORWARD by marker, every key once, in order', [k for k, _ in got] == keys and seen == sorted(seen) and len(seen) == 4)
        F.HTTP['get'] = lambda url: (200, b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>true</IsTruncated></ListBucketResult>')
        check('a truncated empty page raises FetchError', _raises(lambda: F.list_prefix('p/'), F.FetchError))
        calls = []
        F.HTTP['get'] = lambda url: (calls.append(url), (404, b''))[1]
        check('a 404 is retried, then raises (never "absent")', _raises(lambda: F.get('u'), F.FetchError) and len(calls) == F.TRIES)
        check('cutoff: daily 2026-07-01 and monthly 2026-07 refused (a refusal, not an assert)',
              _raises(lambda: F.assert_before_cutoff('a/X-1m-2026-07-01.zip'), REF) and _raises(lambda: F.assert_before_cutoff('a/X-fundingRate-2026-07.zip'), REF))
        check('cutoff: 2026-06-30 daily and 2026-06 monthly pass', not _raises(lambda: F.assert_before_cutoff('a/X-1m-2026-06-30.zip'), REF))
        z = F.zips_before_cutoff([('a/X-1m-2026-06-30.zip', 1), ('a/X-1m-2026-07-01.zip', 1), ('a/X-1m-2026-06-30.zip.CHECKSUM', 1)])
        check('listing results: later keys and CHECKSUM files dropped in memory', [k for k, _, _ in z] == ['a/X-1m-2026-06-30.zip'])
        with tempfile.TemporaryDirectory() as d:
            raw = Path(d) / 'raw'
            good = b'PK-good'
            F.HTTP['get'] = lambda url: (200, (f'{hashlib.sha256(b"other").hexdigest()}  x\n'.encode() if url.endswith('.CHECKSUM') else good))
            check('a persistent checksum mismatch raises and writes nothing',
                  _raises(lambda: F.fetch_one('k/B-1m-2024-01-01.zip', raw), F.FetchError) and not (raw / 'k/B-1m-2024-01-01.zip').exists())
            F.HTTP['get'] = lambda url: (200, (f'{hashlib.sha256(good).hexdigest()}  x\n'.encode() if url.endswith('.CHECKSUM') else good))
            check('a verified file is written', F.fetch_one('k/A-1m-2024-01-01.zip', raw)['sha256'] == hashlib.sha256(good).hexdigest())
            check('fetch_one refuses a post-cutoff key before any request', _raises(lambda: F.fetch_one('k/A-1m-2026-07-02.zip', raw), REF))
    finally:
        F.HTTP.clear()
        F.HTTP.update(saved)
        F.PACER.gap = 1.0 / F.RATE


# ------------------------------------------------------------------ build (synthetic zips)
def _zip(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.StringIO()
    w = csv.writer(buf)
    if header:
        w.writerow(header)
    w.writerows(rows)
    with zipfile.ZipFile(path, 'w') as z:
        z.writestr(path.name.replace('.zip', '.csv'), buf.getvalue())


def synthetic_raw(d, n_mkt=25):
    d = Path(d)
    m1, fr, h1, plan = {}, {}, {}, []
    for sym, d0, us in (('AAAUSDT', '2024-03-05', False), ('BBBUSDT', '2026-06-15', True)):
        keys = []
        for day in L.pull_days(d0):
            k = f'data/futures/um/daily/klines/{sym}/1m/{sym}-1m-{day}.zip'
            rows = []
            for i in range(1440):
                t = L.ms(day) + i * MIN
                n = 0 if (day == d0 and i < 600) else 5
                ts = t * 1000 if us else t
                rows.append([ts, 1, 1.1, 0.9, 1, 10, ts + 59999, 1e6, n, 5, 5, 0])
            _zip(d / k, ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count',
                         'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'] if us else None, rows)
            m1[k] = d / k
            keys.append((k, day, 1))
        fks = []
        for mo in sorted({x[:7] for x in L.pull_days(d0)}):
            k = f'data/futures/um/monthly/fundingRate/{sym}/{sym}-fundingRate-{mo}.zip'
            _zip(d / k, ['calc_time', 'funding_interval_hours', 'last_funding_rate'], [[L.ms(mo + '-01') + j * 4 * HOUR + 7, 4, 0.0001] for j in range(6 * 31)])
            fr[k] = d / k
            fks.append((k, mo, 1))
        plan.append({'id': f'{sym}@{d0}', 'symbol': sym, 'd0': d0, 'calendar': L.calendar_of_day(d0), 'm1': keys,
                     'm1_days_before_d0': [], 'index_on_d0': not us, 'funding': fks, 'spot_on_prev_day': not us})
    days = {'AAAUSDT': ['2024-03-05'], 'BBBUSDT': ['2026-06-15']}
    for i in range(n_mkt):
        sym = f'M{i:02d}USDT'
        days[sym] = ['2021-01-01']
        for mo in ('2024-03', '2026-06'):
            k = f'data/futures/um/monthly/klines/{sym}/1h/{sym}-1h-{mo}.zip'
            base = L.ms(mo + '-01')
            _zip(d / k, None, [[base + j * HOUR, 2, 2, 2, 2 * (1.10 if base + j * HOUR >= L.ms(mo + '-12') else 1.0), 1, 0, 1,
                                0 if (i == 0 and j > 100) else 1, 0, 0, 0] for j in range(24 * 30)])
            h1[k] = d / k
    return plan, days, m1, fr, h1


def build_checks():
    print('build (synthetic archive zips; pre-entry coverage only)')
    import r013_build as B
    with tempfile.TemporaryDirectory() as d:
        plan, days, m1, fr, h1 = synthetic_raw(d)
        out = B.assemble(plan, days, set(), m1, fr, h1)
        later = out.pop('_later_segments')
        a = out['discovery']['events'][0]
        check('first trade = the first 1m bar with trades', a['first_trade'] == L.ms('2024-03-05') + 600 * MIN)
        check('per-event coverage is PRE-ENTRY only (rows in [d0, first trade + 24 h)); no hold-window count stored (000n)',
              set(a['cov_pre_entry']) == {'daily_files_d0_d1', 'rows_1m', 'rows_zero_trades', 'first_trade_on_d0', 'funding_rows'}
              and a['cov_pre_entry']['rows_1m'] == 600 + 24 * 60 and 'cov' not in a)
        check('calendar counts hold no window-level total', not any(k in out['discovery']['counts'] for k in ('rows_1m', 'funding_rows', 'rows_zero_trades')))
        b = out['confirmation']['events']
        check('confirmation listing: microsecond stamps read as ms; nothing on or after 2026-07-01',
              len(b) == 1 and out['confirmation']['bars'].max_of('t') < L.CUTOFF and b[0]['premarket'] is True)
        check('market_1h keeps close and trade count (dead rows are recognisable)',
              set(out['discovery']['market'].cols) == {'symbol', 't', 'o', 'c', 'n'} and 0 in out['discovery']['market'].cols['n'])
        check('later segments come from TRADE COUNTS, not file existence: trading again after a > 7-day absence of trades '
              'starts one (here every symbol resumes on 2026-06-01 after the synthetic 27-month gap); dead zero-trade rows do not',
              len(later) == 25 and all(x['segment_start'] == '2026-06-01' for x in later))
        emb = [dict(plan[1], id='BBBUSDT@2026-06-23', d0='2026-06-23', calendar=L.calendar_of_day('2026-06-23'))]
        check('an embargoed listing day is never assembled', all(not v['events'] for k, v in B.assemble(emb, days, set(), m1, fr, h1).items() if not k.startswith('_')))
        check("a plan row whose calendar disagrees with the event's first trade is refused (a refusal, not an assert)",
              _raises(lambda: B.assemble([dict(plan[0], calendar='confirmation')], days, set(), m1, fr, h1), REF))
        import r013_run as R
        root = Path(d) / 'r013'
        B.write_panels(out, root)
        pnl = R.Panel('discovery', 'discovery', data_root=root)
        recs, cnt = R.evaluate(pnl)
        tr = R.primary(recs)
        check('END TO END (zips -> build writer -> Panel -> evaluate): one T1 event; robust market return +10 % from 24 seasoned '
              'traded symbols (the dead-row symbol is absent)', len(tr['T1']) == 1 and close(tr['T1'][0]['mkt'], 0.10, 1e-9)
              and tr['T1'][0]['t2'] is True, json.dumps(cnt))
        s1 = L.trial_stats(tr['T1'], 1.0, L.Q_DISCOVERY)
        check('end to end: trial statistics compute (1 event -> fails criterion 0)', not L.discovery_criteria(s1)[1])
        c = B.Columns(B.BAR_SPEC)
        c.add('x', 2, 1, 1, 1, 1, 1, 1)
        c.add('x', 1, 1, 1, 1, 1, 1, 1)
        check('the panel writer refuses rows out of order', _raises(lambda: c.write(Path(d) / 'o.parquet', ('id', 't')), REF))


# ------------------------------------------------------------------ panel gate
def write_panel(root, cal, events, bar_rows, fund_rows, mkt_rows, symbols):
    import pyarrow as pa
    import pyarrow.parquet as pq
    p = Path(root) / f'panel_{cal}'
    p.mkdir(parents=True, exist_ok=True)
    (p / 'events.json').write_text(json.dumps(events))
    (p / 'symbols.json').write_text(json.dumps(symbols))
    cols = lambda rows, names: pa.table({n: [r[i] for r in rows] for i, n in enumerate(names)})
    pq.write_table(cols(bar_rows, ('id', 't', 'o', 'h', 'l', 'c', 'qv', 'n')), p / 'bars_1m.parquet')
    pq.write_table(cols(fund_rows, ('id', 'calc_time', 'interval_h', 'rate')), p / 'funding.parquet')
    if mkt_rows is not None:
        pq.write_table(cols(mkt_rows, ('symbol', 't', 'o', 'c', 'n')), p / 'market_1h.parquet')
    (p / 'coverage.json').write_text('{}')


def panel_checks():
    print('the read gate (review B M4)')
    import r013_run as R
    check('the real panels refuse without the gate (no guard has passed in this process)',
          R._GATE is None and _raises(lambda: R.Panel('discovery', 'discovery'), REF))
    saved = R.guard
    try:
        R.guard = lambda mode: 'c0ffee'                 # a stubbed guard sets no gate
        R.guard('discovery')
        check('a stubbed guard + the real DATA still refuses', _raises(lambda: R.Panel('discovery', 'discovery'), REF))
        check('pointing data_root at the real data directory refuses too', _raises(lambda: R.Panel('discovery', 'discovery', data_root=R.DATA_REAL), REF))
        check('... and a parent of it', _raises(lambda: R.Panel('discovery', 'discovery', data_root=R.DATA_REAL.parent), REF))
    finally:
        R.guard = saved
    R._GATE = (str(R.HERE.resolve()), 'discovery', 'x')
    try:
        check('a gate for discovery does not open confirmation mode', _raises(lambda: R.Panel('confirmation', 'confirmation'), REF))
    finally:
        R._GATE = None
    with tempfile.TemporaryDirectory() as d:
        write_panel(d, 'confirmation', [], [], [], [], {})
        write_panel(d, 'discovery', [], [], [], [], {})
        check('discovery mode may not open the confirmation panel (synthetic root)', _raises(lambda: R.Panel('confirmation', 'discovery', data_root=d), REF))
        check('confirmation mode may open discovery (pooled tail charge; already read) on a synthetic root',
              not _raises(lambda: R.Panel('discovery', 'confirmation', data_root=d), REF))
    src = (HERE / 'r013_run.py').read_text()
    check('r013_run.py names no data directory but data/r013 (static scan)',
          not any(x in src for x in ('binance_years', "'carry'", 'carry012', 'scout_', 'hyperliquid')))
    check("every parquet read in r013_run.py sits under the panel's own directory",
          all('{self.dir}/' in l for l in src.splitlines() if "parquet'" in l and 'FROM' in l))
    check('no `assert` remains in the decision path (r013_run, r013_build, r013_fetch, ct/listings013)',
          not any(l.strip().startswith('assert ') for f in ('r013_run.py', 'r013_build.py', 'r013_fetch.py', 'ct/listings013.py')
                  for l in (HERE / f).read_text().splitlines()))


# ------------------------------------------------------------------ the full runs (synthetic panels)
def _syn_panel(root, cal, n_weeks, seed, mu=0.08):
    rnd = random.Random(seed)
    a = L.CALENDARS[cal][0]
    evs, br, fu, mk = [], [], [], []
    syms = {f'S{i:02d}USDT': '2020-01-01' for i in range(22)}
    mpath = [1.0]
    for h in range(n_weeks * 7 * 24 + 12 * 24):
        mpath.append(mpath[-1] * math.exp(rnd.gauss(0, 0.005)))
    t0 = a - a % HOUR
    for w in range(n_weeks):
        for j in range(3):
            ft = a + w * 7 * DAY + j * DAY + 11 * HOUR
            sym = f'N{w:02d}{j}USDT'
            eid = f'{sym}@{L.day_of(ft)}'
            evs.append({'id': eid, 'symbol': sym, 'd0': L.day_of(ft), 'first_trade': ft, 'premarket': j == 2 and w % 5 == 0,
                        'spot_first': j == 0, 'continuation': 'a test' if (w == 3 and j == 1) else None, 'funding_months_missing': []})
            shock = rnd.gauss(-mu, 0.08)
            for m in range(0, 9 * 24 * 60, 5):
                t = ft + m * MIN
                if t >= L.DATA_END[cal]:
                    break
                hm = mpath[min(len(mpath) - 1, int((t - t0) // HOUR))]
                px = hm * (1 + shock * min(1.0, m / (8 * 1440)))
                br.append((eid, t, px, px * 1.001, px * 0.999, px, 2e5 if m < 60 * 24 else 1e5, 3))
            for k in range(1, 9 * 6):
                fu.append((eid, ft - ft % (4 * HOUR) + k * 4 * HOUR + 5, 4, rnd.choice((-1, 1)) * 0.0002))
    for s_ in syms:
        for h in range(len(mpath)):
            t = t0 + h * HOUR
            if t < L.DATA_END[cal]:
                mk.append((s_, t, mpath[h], mpath[h], 1))
    br.sort(key=lambda r: (r[0], r[1]))
    fu.sort(key=lambda r: (r[0], r[1]))
    write_panel(root, cal, evs, br, fu, mk if cal != 'reported' else None, syms)


def run_e2e_checks():
    print('the full runs end to end (synthetic panels, guard stubbed, a temp git repo for results.jsonl, the DEFAULT commit hook)')
    import r013_run as R
    saved = (R.guard, R.DATA, R.RESULTS, R.evaluate)
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        _syn_panel(root, 'discovery', 30, 1)
        _syn_panel(root, 'reported', 3, 2)
        _syn_panel(root, 'confirmation', 14, 3)
        git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root, capture_output=True, text=True)
        git('init', '-q', '-b', 'main')
        git('config', 'user.name', 't')
        git('config', 'user.email', 't@t')
        (root / 'results.jsonl').write_text('')
        git('add', 'results.jsonl')
        git('commit', '-q', '-m', 'empty results')
        try:
            R.guard = lambda mode: 'c0ffee'
            R.DATA, R.RESULTS = root, root / 'results.jsonl'
            check('real_tree_failures is empty on a fully synthetic tree', R.real_tree_failures() == [])
            R.run_discovery()
            lines = R.RESULTS.read_text().splitlines()
            rows = [json.loads(l) for l in lines]
            head = git('show', 'HEAD:results.jsonl').stdout.splitlines()
            check('discovery: started row then one result row, EACH IN HEAD (committed alone by the default hook, verified)',
                  [r['mode'] for r in rows] == ['discovery_started', 'discovery'] and all(l in head for l in lines)
                  and rows[0]['env']['duckdb']['files'] > 0)
            dec, f = L.recheck_discovery(rows[1])
            check('discovery row: the decision recomputed from its per-event records equals the stored one', not f and dec == rows[1]['decision'], str(f))
            check('discovery row: quarter betas, min(raw, adjusted) economics, split-beta alpha and both placebos are recorded',
                  rows[1]['trials']['T1']['betas'] and 'annual_econ' in rows[1]['trials']['T1'] and 'alpha_split_reported' in rows[1]['trials']['T1']
                  and rows[1]['reported']['placebo_alpha_month_flips'])
            if dec[0] != 'read_confirmation':
                check('the synthetic discovery was planted to pass', False, str(rows[1]['trials']['T1']['criteria']))
            R.run_confirmation()
            lines = R.RESULTS.read_text().splitlines()
            rows = [json.loads(l) for l in lines]
            head = git('show', 'HEAD:results.jsonl').stdout.splitlines()
            check('confirmation: started (bound to the discovery row) and result rows in HEAD; tail-adjusted alpha; deploy on raw AND adjusted',
                  [r['mode'] for r in rows][2:] == ['confirmation_started', 'confirmation'] and all(l in head for l in lines)
                  and rows[2]['discovery_row_sha256'] == R.line_sha(lines[1]) and 'alpha_tail' in rows[3]['stats']
                  and {'raw', 'adjusted'} <= set(rows[3]['deploy']) and rows[3]['decision'] in ('confirm_paper', 'confirm_below_bar', 'not_confirmed'))
            R.RESULTS.write_text('')
            git('commit', '-q', '-am', 'reset for the crash probe')

            def boom(*a, **k):
                raise RuntimeError('disk full')
            R.evaluate = boom
            check('a crash after the started row writes and commits a discovery_failed row (a terminal state)',
                  _raises(R.run_discovery, (RuntimeError,)) and [json.loads(l)['mode'] for l in R.RESULTS.read_text().splitlines()]
                  == ['discovery_started', 'discovery_failed'] and R.RESULTS.read_text().splitlines()[-1] in
                  git('show', 'HEAD:results.jsonl').stdout.splitlines())
            R.evaluate = saved[3]
            saved_hook = R.HOOKS['commit']
            R.HOOKS['commit'] = lambda msg: None
            try:
                check('a MOCKED commit hook that does not commit: the row is not in HEAD, so the run refuses (review r1 M4)',
                      _raises(lambda: R.record({'registration': '013-x.md', 'mode': 'probe'}, 'probe'), REF))
                R.DATA = R.DATA_REAL
                check('a mocked hook, or a stubbed guard, on the real data tree is refused (r008 real_tree_failures, ported)',
                      any('non-default commit hook' in x for x in R.real_tree_failures())
                      and any('only together' in x for x in R.real_tree_failures()) and any('real guard' in x for x in R.real_tree_failures()))
            finally:
                R.HOOKS['commit'] = saved_hook
            R.DATA, R.RESULTS = root, R.REAL_RESULTS
            check('the real results.jsonl with a synthetic data tree is refused too', any('only together' in x for x in R.real_tree_failures()))
        finally:
            R.guard, R.DATA, R.RESULTS, R.evaluate = saved


# ------------------------------------------------------------------ guard (throwaway git repos)
def _row009(commit):
    from ct import factors as fx
    ev_ = {'ic_mean': -0.01, 'ic_t': -1.0, 'net_mean': -0.001, 'net_t': -1.0, 'net_median': -0.001, 'halves': [-1.0, -1.0],
           'without_largest_positive_month': -0.001,
           'criteria': {'1_ic_mean_pos_nw_t': False, '2_book_net_pos_nw_t': False, '3_mean_median_same_sign': True,
                        '4_both_halves_positive': False, '5_without_largest_positive_month_positive': False},
           'pass': False, 'pull_conditions': {'all': False}}
    return {'registration': '009-binance-weekly-factors.md', 'mode': 'discovery', 'registration_commit': commit,
            'results': {f: ev_ for f in fx.FAMILY}, 'decision': ['close', []]}


CODE, DATAF, REG_REL = ('ct/listings013.py', 'r013_run.py'), ('data/r013/panel_discovery/events.json',), 'registrations/013-x.md'


def _repo(d):
    root = Path(d)
    git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root, capture_output=True, text=True, check=True)
    git('init', '-q', '-b', 'main')
    for f, body in (('ct/listings013.py', 'x = 1\n'), ('r013_run.py', 'y = 2\n'), (DATAF[0], '[]\n'), ('results.jsonl', '')):
        (root / f).parent.mkdir(parents=True, exist_ok=True)
        (root / f).write_text(body)
    (root / '.gitignore').write_text('data/\n')
    return root, git


def _write_reg(R, root, status='REGISTERED', extra=''):
    rows = '\n'.join(f'| `{f}` | `{R.sha256(root / f)}` |' for f in (*CODE, *DATAF))
    (root / REG_REL).parent.mkdir(exist_ok=True)
    (root / REG_REL).write_text(f'# 013\n\n**Status: {status} 2026-10-13**\n\n| file | sha256 |\n|---|---|\n{rows}\n' + extra)


def _add(root, row):
    open(root / 'results.jsonl', 'a').write(json.dumps(row, sort_keys=True) + '\n')


def guard_checks():
    import r013_run as R
    print('guard (throwaway git repos): status, (a)-(f), (h), (k), the confirmation gate')
    with tempfile.TemporaryDirectory() as d:
        root, git = _repo(d)
        fails = lambda mode='discovery': R.guard_failures(root, REG_REL, CODE, DATAF, mode=mode)[0]
        _write_reg(R, root, status='DRAFT')
        git('add', '.gitignore', 'ct', 'r013_run.py', 'results.jsonl')
        git('commit', '-q', '-m', 'code')
        f0 = fails()
        check('DRAFT, untracked, no 009 terminal state: each refuses', any('not REGISTERED' in x for x in f0)
              and any(x.startswith('(a)') for x in f0) and any(x.startswith('(h)') for x in f0))
        _write_reg(R, root)
        git('add', REG_REL)
        git('commit', '-q', '-m', 'register')
        rc = git('rev-parse', 'HEAD').stdout.strip()
        _add(root, _row009(rc))
        f1 = fails()
        check('(h)+(k): an UNCOMMITTED 009 close unlocks nothing, and results.jsonl is dirty',
              any(x.startswith('(h)') for x in f1) and any(x.startswith('(k)') for x in f1))
        git('commit', '-q', '-am', '009 closed')
        check('a committed 009 close recomputed by its own recheck unlocks; discovery may run', fails() == [], str(fails()))
        (root / REG_REL).write_text((root / REG_REL).read_text() + 'edit\n')
        check('(b) registration modified after commit', any(x.startswith('(b)') for x in fails()))
        git('checkout', '--', REG_REL)
        (root / 'ct/listings013.py').write_text('x = 3\n')
        f4 = fails()
        check('(d)+(e) guarded code edited', any(x.startswith('(d)') for x in f4) and any(x.startswith('(e) ct/listings013.py') for x in f4))
        git('checkout', '--', 'ct/listings013.py')
        _add(root, {'registration': '013-other-name.md', 'mode': 'discovery_started'})
        check('(f) keyed on the 013- prefix: a row under another 013 file name blocks (review B m10)', any(x.startswith('(f)') for x in fails()))
        git('checkout', '--', 'results.jsonl')

        # ---- the confirmation gate
        cf = lambda: fails('confirmation')
        check('(g1) confirmation without a discovery row', any(x.startswith('(g1)') for x in cf()))
        rc = git('rev-parse', 'HEAD').stdout.strip()
        row = disc_row_from_records(syn_records())
        row.update(registration='013-x.md', registration_commit=rc, code_hashes={f: R.sha256(root / f) for f in CODE},
                   data_hashes={f: R.sha256(root / f) for f in DATAF})
        _add(root, {'registration': '013-x.md', 'mode': 'discovery_started', 'registration_commit': rc})
        _add(root, row)
        f1 = cf()
        check('(g2)+(s)+(k): uncommitted rows are refused', any(x.startswith('(g2)') for x in f1) and any(x.startswith('(s)') for x in f1)
              and any(x.startswith('(k)') for x in f1))
        git('commit', '-q', '-am', 'discovery rows')
        check("(h') no approval line", any(x.startswith("(h')") for x in cf()))
        dline = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"mode": "discovery"' in l and '013-x' in l][0]
        (root / REG_REL).write_text((root / REG_REL).read_text() + f'\nConfirmation: APPROVED 2026-10-20 row {R.line_sha(dline)}\n')
        git('commit', '-q', '-am', 'approval')
        check('everything in order: the confirmation gate opens', cf() == [], str(cf()))
        (root / REG_REL).write_text((root / REG_REL).read_text() + 'new prose\n')
        git('commit', '-q', '-am', 'prose')
        check('(r) prose added after the discovery run', any(x.startswith('(r)') for x in cf()))
        git('reset', '-q', '--hard', 'HEAD~1')
        _add(root, {'registration': '013-x.md', 'mode': 'confirmation_failed'})
        git('commit', '-q', '-am', 'failed confirmation')
        check('(i) a confirmation_failed row is terminal: no second read', any(x.startswith('(i)') for x in cf()))
    for label, recs, later, want in (('a genuine pass row, approval later: open', syn_records(), True, 'open'),
                                     ('(g4) ALONE: tampered per-event records under the stored aggregates, clean history',
                                      'tamper', True, 'g4'),
                                     ("(h') approval in the same commit as the row", syn_records(), False, "h'")):
        f6 = _fresh_confirmation(R, recs, later)
        if want == 'open':
            check(label, f6 == [], str(f6))
        elif want == 'g4':
            check(label, f6 and all(x.startswith('(g4)') for x in f6), str(f6))
        else:
            check(label, any(x.startswith("(h')") for x in f6), str(f6))


def _fresh_confirmation(R, recs, approval_later):
    with tempfile.TemporaryDirectory() as d:
        root, git = _repo(d)
        _write_reg(R, root)
        git('add', '.')
        git('commit', '-q', '-m', 'all')
        rc = git('rev-parse', 'HEAD').stdout.strip()
        _add(root, _row009(rc))
        if recs == 'tamper':
            row = disc_row_from_records(syn_records())
            for e in row['events']:
                e['net'] -= 0.2
        else:
            row = disc_row_from_records(recs)
        row.update(registration='013-x.md', registration_commit=rc, code_hashes={f: R.sha256(root / f) for f in CODE},
                   data_hashes={f: R.sha256(root / f) for f in DATAF})
        _add(root, {'registration': '013-x.md', 'mode': 'discovery_started', 'registration_commit': rc})
        line = json.dumps(row, sort_keys=True)
        open(root / 'results.jsonl', 'a').write(line + '\n')
        if approval_later:
            git('commit', '-q', '-am', 'rows')
        (root / REG_REL).write_text((root / REG_REL).read_text() + f'\nConfirmation: APPROVED 2026-10-20 row {R.line_sha(line)}\n')
        git('commit', '-q', '-am', 'approval')
        return R.guard_failures(root, REG_REL, CODE, DATAF, mode='confirmation')[0]


# ------------------------------------------------------------------ the reviewers' bypasses
def bypass_checks():
    import r013_run as R
    print("bypasses the reviewers used (review B M3, m4, m5, m6)")

    def base():
        d = tempfile.mkdtemp()
        root, git = _repo(d)
        _write_reg(R, root)
        git('add', '.')
        git('commit', '-q', '-m', 'all')
        _add(root, _row009(git('rev-parse', 'HEAD').stdout.strip()))
        git('commit', '-q', '-am', '009')
        return root, git
    f = lambda root: R.guard_failures(root, REG_REL, CODE, DATAF, mode='discovery')[0]
    root, git = base()
    _add(root, {'registration': '013-x.md', 'mode': 'discovery'})
    check('(1) a row written and not committed: (k) and (f) refuse', any(x.startswith('(k)') for x in f(root)) and any(x.startswith('(f)') for x in f(root)))
    git('commit', '-q', '-am', 'row')
    git('reset', '-q', '--hard', 'HEAD~1')
    check('(2) row committed then `reset --hard`: the reflog still blocks (f)', any(x.startswith('(f)') for x in f(root)))
    root, git = base()
    _add(root, {'registration': '013-x.md', 'mode': 'discovery'})
    git('commit', '-q', '-am', 'row')
    (root / 'results.jsonl').write_text('\n'.join(l for l in (root / 'results.jsonl').read_text().splitlines() if '013-x' not in l) + '\n')
    git('commit', '-q', '--amend', '--allow-empty', '-am', 'row removed by amend')
    check('(3) a row removed with `commit --amend`: blocked', any(x.startswith('(f)') for x in f(root)))
    root, git = base()
    _add(root, {'registration': '013-x.md', 'mode': 'discovery'})
    git('stash', '-q')
    check('(4) a row stashed (a merge commit): blocked by -m', any(x.startswith('(f)') for x in f(root)))
    root, git = base()
    _add(root, {'registration': '013-x.md', 'mode': 'discovery'})
    git('commit', '-q', '-am', 'row')
    sub = root / 'nested'
    sub.mkdir()
    for x in ('ct', 'registrations'):
        (sub / x).mkdir()
    for fl in ('ct/listings013.py', 'r013_run.py', REG_REL):
        (sub / fl).write_text((root / fl).read_text())
    (sub / 'data/r013/panel_discovery').mkdir(parents=True)
    (sub / DATAF[0]).write_text('[]\n')
    (sub / 'results.jsonl').write_text((root / 'results.jsonl').read_text().replace('013-x', 'zzz'))
    subprocess.run(['git', 'init', '-q', '-b', 'main'], cwd=sub, capture_output=True)
    fl = R.guard_failures(sub, REG_REL, CODE, DATAF, mode='discovery', toplevel=root)[0]
    check('(5) a nested `git init` hiding the outer history: (w) refuses (top level differs)', any(x.startswith('(w)') for x in fl), str(fl[:3]))
    other, _g = base()
    os.environ['GIT_DIR'] = str(other / '.git')
    try:
        fr = f(root)
    finally:
        del os.environ['GIT_DIR']
    check('(6) GIT_DIR pointing at another repository is stripped: the real history still blocks (f)', any(x.startswith('(f)') for x in fr))
    check('(7) an anchor commit that is not an ancestor refuses (w)',
          any(x.startswith('(w)') for x in R.guard_failures(root, REG_REL, CODE, DATAF, anchor=R.ANCHOR)[0]))
    (root / 'duckdb.py').write_text('raise SystemExit("shadow")\n')
    check('(8) an untracked duckdb.py next to the runner is refused (v)', any('shadow' in x for x in R.environment_failures(root)))
    # ---- review r1 m2: textconv / -diff attributes and an external diff driver cannot blind the scans
    root, git = base()
    _add(root, {'registration': '013-x.md', 'mode': 'discovery'})
    git('commit', '-q', '-am', 'row')
    (root / 'results.jsonl').write_text('\n'.join(l for l in (root / 'results.jsonl').read_text().splitlines() if '013-x' not in l) + '\n')
    git('commit', '-q', '-am', 'row erased')
    (root / '.git' / 'info').mkdir(exist_ok=True)
    (root / '.git' / 'info' / 'attributes').write_text('results.jsonl -diff\n*.jsonl diff=blank\n')
    git('config', 'diff.blank.textconv', 'true')
    check('(11) a `-diff` attribute and a textconv driver on results.jsonl do not hide an erased row (--text --no-textconv)',
          any(x.startswith('(f)') for x in f(root)))
    root, git = base()
    rc = git('rev-parse', 'HEAD').stdout.strip()
    (root / REG_REL).write_text((root / REG_REL).read_text() + 'prose added later\n')
    git('commit', '-q', '-am', 'prose')
    git('config', 'diff.external', '/usr/bin/true')
    changed = [l for l in R._git(root, 'diff', '-U0', '--text', '--no-textconv', '--no-ext-diff', '--no-color', rc, 'HEAD', '--',
                                 REG_REL).stdout.splitlines() if l.startswith('+') and not l.startswith('+++')]
    check('(12) diff.external cannot blank the (r) diff (--no-ext-diff)', changed == ['+prose added later'])
    # ---- review r1 m3: r008's shadow_modules (packages, sourceless .pyc, ignored files, extension modules)
    root, git = base()
    (root / 'email').mkdir()
    (root / 'email' / '__init__.py').write_text('raise SystemExit("shadow")\n')
    (root / 'ipaddress.pyc').write_bytes(b'\x00')
    (root / 'quopri.py').write_text('x = 1\n')
    (root / '.git' / 'info' / 'exclude').write_text('quopri.py\n')
    import sysconfig
    (root / 'ct' / ('listings013' + sysconfig.get_config_var('EXT_SUFFIX'))).write_bytes(b'\x00')
    sh = R.shadow_modules(root)
    check('(13) shadow_modules catches a top-level package, a sourceless .pyc, an ignored .py and an extension module in ct/',
          {'email/__init__.py', 'ipaddress.pyc', 'quopri.py', 'ct/listings013' + sysconfig.get_config_var('EXT_SUFFIX')} <= set(sh), str(sh))
    # ---- review r1 m3: a forged __pycache__ .pyc (header matching the source's mtime and size) is never loaded
    with tempfile.TemporaryDirectory() as d:
        dd = Path(d)
        (dd / 'modx.py').write_text('X = 1\n')
        import importlib.util as iu, marshal, struct
        st = (dd / 'modx.py').stat()
        code = compile('X = 666\n', str(dd / 'modx.py'), 'exec')
        pyc = dd / '__pycache__' / f'modx.{sys.implementation.cache_tag}.pyc'
        pyc.parent.mkdir()
        pyc.write_bytes(iu.MAGIC_NUMBER + struct.pack('<III', 0, int(st.st_mtime) & 0xFFFFFFFF, st.st_size & 0xFFFFFFFF) + marshal.dumps(code))
        run = lambda pre: subprocess.run([sys.executable, '-I', '-c', pre + 'import sys; sys.path.insert(0, "."); import modx; print(modx.X)'],
                                         cwd=dd, capture_output=True, text=True).stdout.strip()
        check('(14) a forged __pycache__ .pyc IS loaded by plain Python (the attack is real) and is NOT once sys.pycache_prefix '
              'points to a fresh directory (what r013_run does before importing ct)',
              run('') == '666' and run('import sys, tempfile; sys.pycache_prefix = tempfile.mkdtemp(); ') == '1')
    p2 = subprocess.run([sys.executable, '-I', '-c', 'import sys; sys.path.insert(0, "."); import r013_run as R; print(R.loaded_from_source())'],
                        cwd=HERE, capture_output=True, text=True)
    check('(15) the runner, imported as a program does, loads ct from source under a fresh pycache prefix', p2.stdout.strip() == '[]', p2.stderr[-200:])
    p = subprocess.run([sys.executable, '-O', '-c', 'import sys; sys.path.insert(0, ".");'
                        'import r013_run as R; print(any("-O" in x for x in R.environment_failures(R.HERE)))'],
                       cwd=HERE, capture_output=True, text=True)
    check('(9) `python -O` is refused by (v)', p.stdout.strip() == 'True', p.stderr[-200:])
    check('(10) the pinned versions match this interpreter (python 3.14.8, duckdb 1.5.6, pyarrow 25.0.1)', R.versions() == R.PINS)
    fp = R.env_fingerprint()
    check('(16) the environment fingerprint hashes every duckdb and pyarrow file (parity with r008; review r1 m10)',
          fp['duckdb']['files'] > 10 and fp['pyarrow']['files'] > 10 and fp == R.env_fingerprint())


# ------------------------------------------------------------------ the real files
def real_file_checks():
    import r013_run as R
    print('the real registration (DRAFT) and its hash table')
    reg = HERE / 'registrations' / '013-new-perp-listings.md'
    text = reg.read_text()
    status = 'DRAFT' if '\n**Status: DRAFT**' in text else ('REGISTERED' if '\n**Status: REGISTERED' in text else None)
    check('the status line is DRAFT or REGISTERED (this check does not go stale at commit or registration; review r1 m1)', status is not None)
    check('no line starts with the approval phrase', not R.APPROVAL.findall(text) and not any(l.startswith('Confirmation:') for l in text.splitlines()))
    fails = R.guard_failures(HERE, 'registrations/013-new-perp-listings.md', mode='discovery', toplevel=R.REPO, anchor=R.ANCHOR)[0]
    check('while DRAFT the real guard refuses on status (commit state is not asserted)', status != 'DRAFT' or any('not REGISTERED' in x for x in fails))
    table = dict(R.HASH_ROW.findall(text))
    files = (*R.GUARDED_CODE, *R.GUARDED_DATA)
    missing = [f for f in files if f not in table]
    wrong = [f for f in files if f in table and (not (HERE / f).exists() or R.sha256(HERE / f) != table[f])]
    check('§10 lists every guarded code and data file, and every hash matches', not missing and not wrong, f'missing {missing} wrong {wrong}')
    check('before registration results.jsonl holds no 013 row (any name starting 013-)', status != 'DRAFT' or not R.result_rows(HERE, 'results.jsonl'))
    days = json.loads((R.DATA_REAL / 'listing' / 'um_daily_1d_days.json').read_text())
    check('every continuation is point in time against the 1d listing (names and dates only)', L.continuation_failures(days) == [])
    import duckdb
    con = duckdb.connect()
    want = {'bars_1m.parquet': ['id', 't', 'o', 'h', 'l', 'c', 'qv', 'n'], 'funding.parquet': ['id', 'calc_time', 'interval_h', 'rate'],
            'market_1h.parquet': ['symbol', 't', 'o', 'c', 'n']}
    ok = True
    for cal, fs in R.PANEL_FILES.items():
        for f_ in fs:
            if f_ in want:      # SCHEMA ONLY (column names), never a value
                ok &= [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{R.DATA_REAL / f'panel_{cal}' / f_}'").fetchall()] == want[f_]
    check('the real panels carry exactly the columns the runner queries (schema only)', ok)
    ev_keys = set()
    for cal in R.PANEL_FILES:
        for e in json.loads((R.DATA_REAL / f'panel_{cal}' / 'events.json').read_text()):
            ev_keys |= set(e) | set(e.get('cov_pre_entry', {}))
    check('the real events.json files hold pre-entry fields only (000n)', ev_keys == {'id', 'symbol', 'd0', 'calendar', 'first_trade', 'premarket',
          'spot_first', 'continuation', 'funding_months_missing_pre_entry', 'cov_pre_entry', 'daily_files_d0_d1', 'rows_1m', 'rows_zero_trades',
          'first_trade_on_d0', 'funding_rows'}, str(ev_keys))


if __name__ == '__main__':
    main()
