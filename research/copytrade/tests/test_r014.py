"""Registration 014 — short-vol book on Deribit: mechanics, fetch, build, read lock and guard, on
SYNTHETIC data only (no network, no real data opened).

    .venv/bin/python -m tests.test_r014
"""
import gzip
import json
import math
import random
import re
import subprocess
import tempfile
import urllib.error
from pathlib import Path

from ct import shortvol as sv

HERE = Path(__file__).resolve().parent.parent
H, D = sv.HOUR, sv.DAY
N_CHECKS = [0]


def check(name, ok, detail=''):
    N_CHECKS[0] += 1
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + str(detail) if detail and not ok else ""}')
    if not ok:
        raise SystemExit(1)


def close(a, b, tol=1e-9):
    return a is not None and b is not None and abs(a - b) <= tol


def raises(f, exc=(AssertionError,)):
    try:
        f()
    except exc:
        return True
    return False


def main():
    maths_checks()
    calendar_checks()
    entry_checks()
    settlement_checks()
    position_checks()
    stress_checks()
    stats_checks()
    sim_smoke_checks()
    fetch_checks()
    build_and_panel_checks()
    guard_checks()
    e2e_checks()
    tail_checks()
    real_file_checks()
    print(f'all checks passed ({N_CHECKS[0]})')


# ------------------------------------------------------------------ maths
def maths_checks():
    print('maths: Black-76, implied vol, delta, Student-t quantiles')
    F, K, T, s = 30000.0, 31000.0, 7 / 365, 0.6
    c, p = sv.b76(F, K, T, s, True), sv.b76(F, K, T, s, False)
    check('put-call parity C - P = F - K', close(c - p, F - K, 1e-6))
    check('implied vol round-trips (call and put)', close(sv.implied_vol(c, F, K, T, True), s, 1e-6)
          and close(sv.implied_vol(p, F, K, T, False), s, 1e-6))
    check('implied vol of an out-of-range price is None', sv.implied_vol(F * 2, F, K, T, True) is None)
    check('ATM call delta ~ 0.5 + small drift term', 0.5 < sv.call_delta(F, F, T, s) < 0.53)
    check('expiry payoff at T = 0', sv.b76(F, K, 0, s, True) == 0 and sv.b76(F, K, 0, s, False) == 1000)
    check('t quantiles: table at df <= 30', sv.t_crit(5, 0.975) == 2.571 and sv.t_crit(30, 0.95) == 1.697)
    check('t quantiles: expansion t_60(0.975)=2.000, t_120(0.975)=1.980, t_128(0.95)=1.657',
          close(sv.t_crit(60, 0.975), 2.000, 0.002) and close(sv.t_crit(120, 0.975), 1.980, 0.002)
          and close(sv.t_crit(128, 0.95), 1.657, 0.002))
    check('inverse normal 0.975 = 1.95996', close(sv._ninv(0.975), 1.959964, 1e-5))
    check('df < 1 -> no test possible', sv.t_crit(0, 0.95) == math.inf)


# ------------------------------------------------------------------ calendars
def calendar_checks():
    print('calendars: entries, expiries, the embargo week')
    d, h = sv.calendar_entries('discovery'), sv.calendar_entries('holdout')
    check('discovery: 2021-04-02 08:00 -> 2023-12-22 08:00, 143 Fridays',
          sv.iso(d[0]) == '2021-04-02 08:00' and sv.iso(d[-1]) == '2023-12-22 08:00' and len(d) == 143, (sv.iso(d[-1]), len(d)))
    check('holdout: 2024-01-05 08:00 -> 2026-06-19 08:00, 129 Fridays',
          sv.iso(h[0]) == '2024-01-05 08:00' and sv.iso(h[-1]) == '2026-06-19 08:00' and len(h) == 129, (sv.iso(h[-1]), len(h)))
    emb = sv.ms(2023, 12, 29, 8)
    check('the 2023-12-29 entry (expiry in 2024) belongs to neither calendar', emb not in d and emb not in h)
    check('every discovery expiry < 2024-01-01 and every holdout expiry < 2026-07-01',
          all(e + 7 * D < sv.DISC_END for e in d) and all(e + 7 * D < sv.HOLD_END for e in h))
    check('instrument names parse to an 08:00 expiry', sv.parse_name('BTC-9APR21-58000-C') == ('BTC', sv.ms(2021, 4, 9, 8), 58000.0, True)
          and sv.parse_name('ETH-PERPETUAL') is None and sv.parse_name('ETH-28JUN24-3d5-P')[2] == 3.5)


# ------------------------------------------------------------------ synthetic trades
def mk_trade(coin, t, expiry, K, call, S, iv, seq=0, direction='buy'):
    T = (expiry - t) / D / 365
    usd = sv.b76(S, K, T, iv, call)
    import datetime as dt
    d = dt.datetime.fromtimestamp(expiry / 1000, dt.UTC)
    name = f'{coin}-{d.day}{d.strftime("%b").upper()}{d.strftime("%y")}-{int(K)}-{"C" if call else "P"}'
    return {'timestamp': t, 'trade_id': f'{coin}{t}{seq}{K}{call}', 'trade_seq': seq, 'instrument_name': name,
            'price': usd / S, 'mark_price': usd / S, 'iv': iv * 100, 'index_price': S, 'direction': direction, 'amount': 1}


def entry_checks():
    print('entry: strike choice, window, index source, mark IV, coverage twin')
    E = sv.ms(2022, 6, 10, 8)
    X = E + 7 * D
    S = 30000.0
    tr = [mk_trade('BTC', E - 3 * H, X, 30000, True, 29800, 0.7, 1),
          mk_trade('BTC', E - 20 * 60_000, X, 30000, False, S, 0.8, 2),            # latest at K: IV 0.8
          mk_trade('BTC', E - 10 * 60_000, X, 31000, True, S, 0.75, 3),
          mk_trade('BTC', E - 5 * 60_000, X + 7 * D, 30000, True, S, 0.9, 4),      # another expiry: ignored
          mk_trade('BTC', E, X, 30000, True, 40000, 2.0, 5),                        # AT the entry: outside the window
          mk_trade('BTC', E - 5 * H, X, 30000, True, 20000, 2.0, 6)]                # before the window
    em = sv.straddle_entry('BTC', E, tr, perp_close_at_entry=30010.0)
    check('nearest traded strike of the 7-day expiry; index from the latest trade (<= 30 min)',
          em['K'] == 30000 and em['S'] == S and em['S_source'] == 'trade_index', em)
    check('mark IV from the latest trade at K (0.8), not a later or earlier one', close(em['iv'], 0.8, 1e-6), em['iv'])
    check('marks re-priced at the entry index with 7 days to expiry',
          close(em['mark_call'], sv.b76(S, 30000, 7 / 365, 0.8, True), 1e-6))
    stale = [dict(r, timestamp=r['timestamp'] - 40 * 60_000) for r in tr if r['timestamp'] < E]
    em2 = sv.straddle_entry('BTC', E, stale, perp_close_at_entry=30010.0)
    check('a last trade older than 30 min -> the perp close is the index', em2.get('S_source') == 'perp_close' and em2['S'] == 30010.0)
    check('no trade in the 7-day expiry -> missing', sv.straddle_entry('BTC', E, [tr[3]], 30000.0) == {'missing': 'no_trade_in_expiry'})
    far = [mk_trade('BTC', E - H, X, 33000, True, S, 0.7)]
    check('nearest strike beyond 2.5 % -> missing', sv.straddle_entry('BTC', E, far, 30000.0) == {'missing': 'no_strike_within_band'})
    check('no trade and no perp -> missing index', sv.straddle_entry('BTC', E, [], None) == {'missing': 'no_index'})
    agree = all((sv.entry_coverage('BTC', E, w, p)[0] == 'ok') == ('missing' not in sv.straddle_entry('BTC', E, w, p))
                for w, p in ((tr, 30010.0), (stale, 30010.0), ([tr[3]], 30000.0), (far, 30000.0), ([], None)))
    check('entry_coverage (build, no marks) agrees with straddle_entry on availability', agree)
    check('entry_coverage returns the chosen strike trade age, no price', sv.entry_coverage('BTC', E, tr, 30010.0)[2] == 20.0)


def settlement_checks():
    print('settlement: 07:30-08:00 per-minute TWAP, fallback, window')
    X = sv.ms(2022, 6, 17, 8)
    tr = [{'timestamp': X - 30 * 60_000 + i * 60_000 + 5, 'index_price': 100.0 + i, 'instrument_name': 'x'} for i in range(12)]
    tr += [{'timestamp': X - 30 * 60_000 + 7, 'index_price': 1000.0, 'instrument_name': 'x'},
           {'timestamp': X - 30 * 60_000 + 9, 'index_price': 1002.0, 'instrument_name': 'x'}]    # minute 0: median of 3
    tr += [{'timestamp': X, 'index_price': 9999.0, 'instrument_name': 'x'}]                    # at X: outside
    px, src, n = sv.settlement(tr, X, 50.0)
    exp = (1000.0 + sum(100.0 + i for i in range(1, 12))) / 12
    check('mean of per-minute medians over covered minutes; the trade at X is excluded', src == 'index_twap' and n == 12 and close(px, exp, 1e-9), (px, exp))
    px2, src2, n2 = sv.settlement(tr[:9], X, 50.0)
    check('fewer than 10 covered minutes -> perp close', src2 == 'perp_close' and px2 == 50.0 and n2 == 9)
    check('settlement_coverage counts minutes only', sv.settlement_coverage(tr, X) == 12)


# ------------------------------------------------------------------ the position
def mark(S=100.0, iv=0.6, coin='BTC', E=0):
    T = 7 / 365
    return {'coin': coin, 'entry': E, 'expiry': E + 7 * D, 'K': S, 'S': S, 'iv': iv, 'S_source': 'trade_index',
            'mark_call': sv.b76(S, S, T, iv, True), 'mark_put': sv.b76(S, S, T, iv, False)}


def flat_path(S=100.0, E=0, dv=60.0, f=None):
    return [(E + i * H, (f(i) if f else S), dv) for i in range(169)]


def position_checks():
    print('position: premium, costs, hedge, funding, liquidation, path checks')
    m = mark()
    prem = m['mark_call'] + m['mark_put']
    o = sv.position_pnl(m, flat_path(), 100.0, hedge_every_h=None)
    fees = 2 * min(0.0003 * 100, 0.125 * m['mark_call'] * (1 - 0.026))
    check('unhedged, flat path: net = premium - spread - fees (no payoff, no delivery)',
          close(o['net'], prem * (1 - 0.026) - fees, 1e-9) and o['payoff'] == 0 and o['delivery'] == 0)
    o2 = sv.position_pnl(m, flat_path(), 100.0, hedge_every_h=24)
    check('hedged, flat path: the same plus the (small) hedge fees', o2['net'] < o['net'] and close(o2['hedge_pnl'], 0, 1e-12))
    rally = flat_path(f=lambda i: 100 * (1 + 0.3 * i / 168))
    u = sv.position_pnl(m, rally, 130.0, hedge_every_h=None)
    hd = sv.position_pnl(m, rally, 130.0, hedge_every_h=24)
    check('a steady +30 % rally: unhedged loss = 30 - premium net; the daily hedge recovers most of it',
          close(u['payoff'], 30.0) and hd['net'] > u['net'] + 10, (u['net'], hd['net']))
    dlv = sv.position_pnl(m, rally, 130.0, hedge_every_h=None)['delivery']
    check('delivery fee 0.015 % of the settlement, capped at 12.5 % of the value', close(dlv, min(0.00015 * 130, 0.125 * 30)))
    fund = {i * H: 0.001 for i in range(1, 169)}
    f1 = sv.position_pnl(m, rally, 130.0, hedge_every_h=24, funding=fund)
    check('funding: a long hedge pays positive funding', f1['funding_pnl'] < 0 and close(f1['net'] - hd['net'], f1['funding_pnl'], 1e-9))
    crash = flat_path(f=lambda i: 100.0 if i < 30 else 50.0)
    big = sv.position_pnl(m, crash, 50.0, hedge_every_h=24, liq_n=3.0)
    small = sv.position_pnl(m, crash, 50.0, hedge_every_h=24, liq_n=sv.SIZE_N)
    check('a -50 % gap liquidates a 3x book at the first mark below maintenance, not the registered n',
          big['liquidated'] and big['liquidated']['t'] == 30 * H and not small['liquidated'], (big['liquidated'], small['liquidated']))
    check('the liquidated value is crystallised (no payoff, the loss includes the buy-back costs)',
          big['payoff'] is None and big['net'] < -40)
    check('a path with a gap, or one not starting at the entry, is refused',
          raises(lambda: sv.position_pnl(m, flat_path()[:50] + flat_path()[51:], 100.0))
          and raises(lambda: sv.position_pnl(m, flat_path(E=H), 100.0)))
    iv_up = [(t, S, 60.0 + (20.0 if i > 10 else 0)) for i, (t, S, _) in enumerate(crash)]
    b1 = sv.position_pnl(m, iv_up, 50.0, hedge_every_h=24, liq_n=1.0, beta_iv=1.0)
    b2 = sv.position_pnl(m, iv_up, 50.0, hedge_every_h=24, liq_n=1.0, beta_iv=2.0)
    check('beta_iv scales the vega mark (stress variant): liquidation no later at beta 2',
          (b2['liquidated'] or {}).get('t', 1e18) <= (b1['liquidated'] or {}).get('t', 1e18))


# ------------------------------------------------------------------ stress rule
def stress_checks():
    print('stress (T2): point in time, ratio, undefined, book rule')
    E = sv.ms(2022, 6, 10, 8)
    bars = [(E - 9 * D + i * H, 60.0) for i in range(9 * 24)]
    check('flat DVOL -> not stressed', sv.stress_flag(bars, E, 'BTC') is False)
    sp = [(t, 80.0 if t >= E - 30 * H else c) for t, c in bars]
    check('DVOL 80 vs a 7-day minimum of 60 (ratio 1.33 >= 1.25) -> stressed', sv.stress_flag(sp, E, 'BTC') is True)
    late = [(t, 200.0 if t > E - 2 * H else c) for t, c in bars] + [(E, 500.0), (E + H, 500.0)]
    check('a spike in bars closing after entry - 1 h (or after the entry) changes nothing', sv.stress_flag(late, E, 'BTC') is False)
    check('fewer than 120 bars in the look-back -> undefined', sv.stress_flag(bars[-100:], E, 'BTC') is None)
    check('book stands aside if ANY coin is stressed; undefined is not stressed',
          sv.book_stressed({'BTC': False, 'ETH': True}) and not sv.book_stressed({'BTC': None, 'ETH': False}))


# ------------------------------------------------------------------ statistics and decisions
def series(mu, sd, n, seed, skew=False):
    rng = random.Random(seed)
    out = []
    for _ in range(n):
        x = rng.gauss(mu, sd)
        if skew and rng.random() < 0.03:
            x -= 12 * sd
        out.append(x)
    return out


def stats_checks():
    print('statistics: skew-corrected t, criteria, decisions, recheck, holdout, economics, placebo')
    neg = series(0.004, 0.02, 143, 1, skew=True)
    s = sv.summary(neg)
    check('skew-corrected t < plain t on a negatively skewed series', s['t'] < s['t_plain_reported'], (s['t'], s['t_plain_reported']))
    ths = [sv.skew_t([x + k * 0.002 for x in neg]) for k in range(0, 40)]
    check('skew-corrected t is monotone in the mean, even far from the null (Johnson alone is not)', all(b >= a for a, b in zip(ths, ths[1:])))
    good = series(0.012, 0.02, 143, 2)
    res, dec = sv.evaluate_discovery({'T1': good, 'T2': [0.0 if i % 10 == 0 else x for i, x in enumerate(good)]})
    check('a strong synthetic book passes all five criteria (T1, T2) -> read_holdout',
          res['T1']['pass'] and res['T2']['pass'] and dec == ('read_holdout', ['T1', 'T2']), res['T1']['criteria'])
    check('the critical value is max(t_{G-1}(0.975), the registered floor 2.59)', res['T1']['t_crit'] == max(sv.t_crit(142, 0.975), sv.DISC_T_FLOOR))
    weak = series(0.0005, 0.02, 143, 3)
    r2, d2 = sv.evaluate_discovery({'T1': weak, 'T2': weak})
    check('a weak book -> close', d2 == ('close', []))
    small = [x * 0.05 for x in series(0.02, 0.02, 143, 4)]
    r3, _ = sv.evaluate_discovery({'T1': small, 'T2': small})
    check('criterion 5: a significant but tiny edge fails the 3 %/yr bar at n',
          r3['T1']['criteria']['1_mean_pos_t_ge_tcrit'] and not r3['T1']['criteria']['5_annualised_at_n_ge_hlp_bar'])
    stored = json.loads(json.dumps(res))
    dec_r, fails = sv.recheck_discovery(stored, weekly={'T1': [[i, x] for i, x in enumerate(good)],
                                                        'T2': [[i, 0.0 if i % 10 == 0 else x] for i, x in enumerate(good)]})
    check('recheck from the stored numbers reproduces the decision with no failure', dec_r == dec and not fails, fails)
    forged = json.loads(json.dumps(r2))
    forged['T1']['pass'] = True
    forged['T1']['criteria'] = {k: True for k in forged['T1']['criteria']}
    dec_f, ff = sv.recheck_discovery(forged)
    check('recheck: forged pass labels over failing numbers are caught and the decision stays close', ff and dec_f == ('close', []))
    forged2 = json.loads(json.dumps(res))
    forged2['T1']['summary']['n'] = 0.45
    check('recheck: a stored size n other than the registered one is caught', sv.recheck_discovery(forged2)[1])
    hold = series(0.006, 0.02, 129, 5)
    ok1, d1 = sv.holdout_existence(hold, 1)
    ok2, d2_ = sv.holdout_existence(hold, 2)
    check('holdout existence: q 0.95 for one trial, 0.975 for two; floors 2.16 / 2.52',
          d1['q'] == 0.95 and d2_['q'] == 0.975 and d1['t_crit'] == 2.16 and d2_['t_crit'] == 2.52)
    e = sv.economic_after(d1['summary'])
    check('economics after existence: realistic tail charge (21 stress weeks / 390) ~0.172 %/week; bar on (mean - charge) x 52 x n',
          close(e['tail_charge_per_week'], -sum(sv.REALISTIC_WEEK.values()) / 390, 1e-12) and len(sv.REALISTIC_WEEK) == 21
          and close(sv.tail_charge_adversarial(), (0.538 + 0.4915) / 390, 1e-12)
          and close(e['annualised_tail_charged'], (d1['summary']['mean'] - e['tail_charge_per_week']) * 52 * sv.SIZE_N, 1e-12))
    check('decide_holdout: no confirm -> not_demonstrated; confirm without economics -> exists_not_economic',
          sv.decide_holdout({'T1': False}, {}) == 'not_demonstrated'
          and sv.decide_holdout({'T1': True}, {'T1': {'bar_met': False, 'drawdown_within_preregistered_month': True}}) == 'exists_not_economic'
          and sv.decide_holdout({'T1': True}, {'T1': {'bar_met': True, 'drawdown_within_preregistered_month': True}}) == 'deploy_paper_registration')
    bk = [{'entry': i, 'r': x, 'gross': x + 0.001, 'cost': 0.001} for i, x in enumerate(good)]
    p1, p2 = sv.placebo(bk, seeds=50), sv.placebo(bk, seeds=50)
    check('placebo: deterministic by seed; sign flips of the gross with costs kept rarely pass', p1 == p2 and p1['share_c1_pass'] < 0.1, p1)
    rows = [{'entry': 1, 'coin': 'BTC', 'ret_notional': 0.01, 'gross_notional': 0.02, 'cost_notional': 0.01},
            {'entry': 1, 'coin': 'ETH', 'ret_notional': -0.03, 'gross_notional': -0.02, 'cost_notional': 0.01},
            {'entry': 2, 'coin': 'BTC', 'ret_notional': 0.02, 'gross_notional': 0.03, 'cost_notional': 0.01}]
    wb = sv.weekly_book(rows)
    check('weekly book: equal notional over the coins present', close(wb[0]['r'], -0.01) and close(wb[1]['r'], 0.02) and wb[1]['coins'] == ['BTC'])
    t2 = sv.stand_aside(wb, {1})
    check('T2: a stressed week is idle (zero), the others unchanged', t2[0]['r'] == 0.0 and t2[0]['idle'] and t2[1]['r'] == 0.02)
    st = sv.sizing_statement()
    check('the pre-registered tail at n = 0.40: realistic worst week -5.6 % and month -5.6 %; adversarial stress -21.5 % / -26.8 %',
          close(st['worst_week_equity'], 0.40 * -0.141, 1e-9) and close(st['worst_month_equity'], 0.40 * -0.141, 1e-9)
          and close(st['stress_adversarial_week_equity'], 0.40 * -0.538, 1e-9) and close(st['stress_adversarial_month_equity'], 0.40 * -0.671, 1e-9))


def sim_smoke_checks():
    print('synthetic world (smoke): bank, constructions, pass rates')
    from ct import shortvol_sim as sim
    bank = sim.gen_bank(60, seed=3)
    nz = sim.noises(bank, 4)
    check('bank weeks carry 169 hourly prices and DVOL per coin', all(len(w[c]['S']) == 169 and len(w[c]['dvol']) == 169 for w in bank[:5] for c in sv.COINS))
    rows = {con: sim.book(bank, nz, con, 1.05) for con in sim.CONSTRUCTIONS}
    check('every construction yields one book row per week with costs > 0', all(len(r) == 60 and all(x['cost'] > 0 for x in r) for r in rows.values()))
    check('hedging lowers the weekly SD versus unhedged (synthetic)', sim.statistics.pstdev(x['r'] for x in rows['H24'])
          < sim.statistics.pstdev(x['r'] for x in rows['U']))
    pr = sim.pass_rates(rows['H24'] * 5, sims=20)
    check('pass_rates returns rates in [0, 1]', all(0 <= v <= 1 for v in pr.values()))


# ------------------------------------------------------------------ fetch
def fetch_checks():
    print('fetch: end bounds before any request, typed errors never cached, forward paging, tail coverage, directories')
    import r014_fetch as F
    calls = []

    def boom(url):
        calls.append(url)
        raise urllib.error.HTTPError(url, 503, 'x', {}, None)
    old_http, old_sleep = F.HTTP, F.time.sleep
    F.HTTP, F.time.sleep = boom, (lambda s: None)
    try:
        check('a request without an end bound is refused before any call',
              raises(lambda: F.get_json(F.WWW, 'x', {'start_timestamp': 1}), (F.FetchError,)) and not calls)
        check('an end bound at/after 2026-07-01 is refused before any call',
              raises(lambda: F.get_json(F.WWW, 'x', {'end_timestamp': F.CUTOFF_MS}), (F.FetchError,)) and not calls)
        check('a persistent 503 raises a typed FetchError after retries',
              raises(lambda: F.get_json(F.WWW, 'x', {'end_timestamp': 5}), (F.FetchError,)) and len(calls) == 6)

        def api_err(url):
            return {'error': {'code': 1}}, 10
        F.HTTP = api_err
        check('an API error raises at once (not retried, not returned)', raises(lambda: F.get_json(F.WWW, 'x', {'end_timestamp': 5}), (F.FetchError,)))
        page = {'n': 0}

        def pages(url):
            page['n'] += 1
            import urllib.parse as up
            q = dict(up.parse_qsl(up.urlparse(url).query))
            t, end = int(q['start_timestamp']), int(q['end_timestamp'])
            data = [{'timestamp': t + i, 'trade_id': f'id{t + i}', 'trade_seq': 0, 'instrument_name': 'n', 'price': 1,
                     'mark_price': 1, 'iv': 1, 'index_price': 1, 'direction': 'buy', 'amount': 1} for i in range(0, 3) if t + i <= end]
            more = t + 2 < end
            return {'result': {'trades': data, 'has_more': more}}, 100
        F.HTTP = pages
        rows = F.fetch_window('BTC', 1000, 1012)
        check('forward paging with inclusive restart, de-duplicated by trade_id, sorted',
              [r['timestamp'] for r in rows] == list(range(1000, 1013)) and page['n'] > 1, [r['timestamp'] for r in rows])

        def same_ms(url):
            return {'result': {'trades': [{'timestamp': 5, 'trade_id': 'a'}], 'has_more': True}}, 10
        F.HTTP = same_ms
        check('>1000 trades on one millisecond is an error (cannot page)', raises(lambda: F.fetch_window('BTC', 5, 9), (F.FetchError,)))

        def late(url):
            return {'result': {'trades': [{'timestamp': F.CUTOFF_MS, 'trade_id': 'z'}], 'has_more': False}}, 10
        F.HTTP = late
        check('a row stamped on/after the cutoff is a hard error', raises(lambda: F.fetch_window('BTC', 1, 5), (F.FetchError,)))

        def cont(url):
            return {'result': {'data': [[F.START_MS, 1, 1, 1, 1]], 'continuation': 123}}, 10
        F.HTTP = cont
        check('DVOL: a continuation (truncated window) is an error', raises(lambda: F.fetch_dvol('BTC'), (F.FetchError,)))

        def short_tail(url):
            import urllib.parse as up
            q = dict(up.parse_qsl(up.urlparse(url).query))
            a = int(q['start_timestamp'])
            return {'result': {'data': [[a, 1, 1, 1, 1]] if a < sv.ms(2026, 1, 1) else [], 'continuation': None}}, 10
        F.HTTP = short_tail
        check('DVOL: tail coverage short of 2026-06-30 23:00 is an error', raises(lambda: F.fetch_dvol('BTC'), (F.FetchError,)))

        def empty(url):
            return {'result': []}, 10
        F.HTTP = empty
        check('funding: an empty window is an error, never cached as data', raises(lambda: F.fetch_funding('BTC'), (F.FetchError,)))
        with tempfile.TemporaryDirectory() as d:
            old_root = F.DATA_ROOT
            F.DATA_ROOT = Path(d)
            try:
                check('a pull directory outside data/r014 is refused', raises(lambda: F.new_pull_dir('/tmp/x'), (F.FetchError,)))
                p = Path(d) / 'pull_x'
                p.mkdir()
                (p / 'a.json').write_text('1')
                check('a non-empty pull directory is refused without --resume', raises(lambda: F.new_pull_dir(p), (F.FetchError,)))
                check('--resume continues an incomplete pull', F.new_pull_dir(p, resume=True) == p.resolve())
                F.write_sums(p)
                check('a completed pull (SHA256SUMS) is never resumed', raises(lambda: F.new_pull_dir(p, resume=True), (F.FetchError,)))
                F.HTTP = boom
                q = Path(d) / 'pull_y'
                check('a failing unit leaves no file behind (nothing erroneous is written)',
                      raises(lambda: F.pull(q), (F.FetchError,)) and not any(x.suffix == '.json' for x in q.rglob('*')))
            finally:
                F.DATA_ROOT = old_root
    finally:
        F.HTTP, F.time.sleep = old_http, old_sleep


# ------------------------------------------------------------------ build, panel, read lock, end to end
def synthetic_pull(root, entries, seed=0, mu_vol=0.6):
    """A tiny synthetic pull under root/pull_syn: hourly perp/DVOL/funding around `entries` and option
    trades (ATM strikes, index, marks) in each Friday window. Returns the pull dir."""
    import r014_fetch as F
    rng = random.Random(seed)
    p = root / 'pull_syn'
    (p / 'dvol').mkdir(parents=True)
    (p / 'perp').mkdir()
    (p / 'funding').mkdir()
    (p / 'trades').mkdir()
    t0 = sv.ms(2021, 3, 1)
    t1 = sv.ms(2026, 7, 1)
    fridays = F.fridays()
    keep_days = set()
    for E in entries:
        for k in range(-9, 9):
            keep_days.add((E // D) + k)
    for c in sv.COINS:
        S, perp, dvol, fund = 100.0 if c == 'ETH' else 1000.0, [], [], []
        t = t0
        while t < t1:
            if t // D in keep_days:
                S *= math.exp(rng.gauss(0, mu_vol / math.sqrt(8760)))
                perp.append([t, S, S, S, S, 1.0])
                dvol.append([t, 60.0, 60.0, 60.0, 60.0 + rng.random()])
                fund.append([t + H, 0.00001])
            t += H
        (p / 'perp' / f'{c}.json').write_text(json.dumps(perp))
        (p / 'dvol' / f'{c}.json').write_text(json.dumps(dvol))
        (p / 'funding' / f'{c}.json').write_text(json.dumps(fund))
        close_at = {r[0] + H: r[4] for r in perp}
        for Fr in fridays:
            rows = []
            if Fr in close_at:
                S0 = close_at[Fr]
                K = round(S0, 2 - int(math.floor(math.log10(S0))))      # three significant digits
                for i in range(40):
                    ts = Fr - 45 * 60_000 + i * 60_000
                    r = mk_trade(c, ts, Fr + 7 * D, K, i % 2 == 0, S0, 0.6, i)
                    rows.append({k: r[k] for k in F.KEEP})
            import datetime as dt
            (p / 'trades' / f'{c}_{dt.datetime.fromtimestamp(Fr / 1000, dt.UTC):%Y-%m-%d}.json').write_text(json.dumps(rows))
    (p / 'pull_report.json').write_text(json.dumps({'synthetic': True}))
    F.write_sums(p)
    return p


def build_and_panel_checks():
    print('build and read lock: verify, split, determinism, coverage keys, Panel whitelist, positions')
    import r014_build as B
    import r014_run as R
    disc = sv.calendar_entries('discovery')[:6]
    hold = sv.calendar_entries('holdout')[:4]
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / 'r014'
        root.mkdir()
        saved = (B.DATA_ROOT, dict(B.PANELS), R.DATA_ROOT, dict(R.PANELS))
        B.DATA_ROOT = root
        B.PANELS.update({'discovery': root / 'panel_discovery', 'holdout': root / 'panel_holdout'})
        R.DATA_ROOT = root
        R.PANELS.update({'discovery': root / 'panel_discovery', 'holdout': root / 'panel_holdout'})
        try:
            pull = synthetic_pull(root, disc + hold + [sv.ms(2023, 12, 29, 8)])
            f = pull / 'perp' / 'BTC.json'
            good = f.read_text()
            f.write_text(good.replace('1.0]', '2.0]', 1))
            check('build refuses a pull whose files differ from SHA256SUMS', raises(lambda: B.verify_pull(pull), (SystemExit,)))
            f.write_text(good)
            B.main(str(pull))
            cov = json.loads((root / 'logs' / 'coverage.json').read_text())
            text = json.dumps({k: v for k, v in cov.items() if k in ('discovery', 'holdout')})
            check('coverage holds counts only: no mark, iv, premium, payoff, pnl or stress key',
                  not re.search(r'"[^"]*(mark|iv|prem|payoff|pnl|stress|vol_)[^"]*":', text.replace('dvol_', 'DV_')), text[:300])
            dc = cov['discovery']['BTC']
            check('coverage: synthetic entries are available ("ok") and settle on the TWAP',
                  dc['entry_classes'].get('ok', 0) >= 5 and dc['settlements']['index_twap_ge_10_min'] >= 5, dc)
            h1 = {fn: B.sha256(root / 'panel_discovery' / fn) for fn in B.PANEL_FILES}
            check('a panel is built once (an existing panel directory is refused)', raises(lambda: B.main(str(pull)), (SystemExit,)))
            import shutil
            shutil.rmtree(root / 'panel_discovery')
            shutil.rmtree(root / 'panel_holdout')
            B.main(str(pull))
            check('a rebuild is byte-identical', h1 == {fn: B.sha256(root / 'panel_discovery' / fn) for fn in B.PANEL_FILES})
            man = json.loads((root / 'panel_discovery' / 'MANIFEST.json').read_text())
            check('discovery panel: every series row before 2024-01-01; holdout panel: every row before 2026-07-01',
                  man['series_bounds'][1] == sv.DISC_END and
                  json.loads((root / 'panel_holdout' / 'MANIFEST.json').read_text())['series_bounds'][1] == sv.HOLD_END)
            check('Panel: the holdout panel is refused without the gate flag', raises(lambda: R.Panel('holdout'), (PermissionError,)))
            check('Panel: a path outside the two panel directories is refused', raises(lambda: R.assert_allowed(HERE / 'data' / 'scout_n2'), (PermissionError,)))
            check('Panel: a discovery run never opens a holdout directory',
                  raises(lambda: R.Panel('discovery', panel_dir=root / 'panel_holdout'), (PermissionError,)))
            pd = R.Panel('discovery')
            rows, counts = R.positions(pd, 'discovery')
            check('positions: every synthetic week with data is computed; every other entry is counted missing by reason',
                  counts['positions'] >= 12 and counts['positions'] + sum(counts['missing'].values()) + counts['no_path'] == 2 * 143
                  and set(counts['missing']) <= {'no_index', 'no_settlement'}, counts)
            stressed, undefined = R.stressed_entries(pd, 'discovery')
            t1, t2 = R.series_of(rows, stressed)
            check('weekly T1 has one row per traded week; T2 the same weeks',
                  len(t1) == len({r['entry'] for r in rows}) == len(t2))
            mp = root / 'panel_discovery' / 'dvol.json'
            body = mp.read_text()
            mp.write_text(body + ' ')
            check('Panel: a file changed after its build (MANIFEST mismatch) is refused', raises(lambda: R.Panel('discovery'), (PermissionError,)))
            mp.write_text(body)
            ph = R.Panel('holdout', allow_holdout=True)
            check('holdout panel (gate flag): positions computed for the synthetic holdout weeks',
                  R.positions(ph, 'holdout')[1]['positions'] >= 8)
        finally:
            B.DATA_ROOT, R.DATA_ROOT = saved[0], saved[2]
            B.PANELS.clear(), B.PANELS.update(saved[1])
            R.PANELS.clear(), R.PANELS.update(saved[3])


# ------------------------------------------------------------------ guard (throwaway git repo)
def guard_checks():
    print('guard (throwaway git repo): status, (a)-(f), discovery_started first, one result row, holdout (g1)-(g4), (h\'), (i)')
    import r014_run as R
    code, data = ('ct/shortvol.py', 'r014_run.py'), ('data/r014/panel_discovery/x.json',)
    reg_rel = 'registrations/014-x.md'
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root,
                                        capture_output=True, text=True, check=True)
        git('init', '-q', '-b', 'main')
        for f, body in (('ct/shortvol.py', 'x = 1\n'), ('r014_run.py', 'y = 2\n'), ('data/r014/panel_discovery/x.json', '{}\n'),
                        ('results.jsonl', json.dumps({'registration': '005-y.md'}) + '\n')):
            (root / f).parent.mkdir(parents=True, exist_ok=True)
            (root / f).write_text(body)
        (root / '.gitignore').write_text('data/\n')

        def write_reg(status='REGISTERED', extra=''):
            rows = '\n'.join(f'| `{f}` | `{R.sha256(root / f)}` |' for f in (*code, *data))
            (root / reg_rel).parent.mkdir(exist_ok=True)
            (root / reg_rel).write_text(f'# 014\n\n**Status: {status} 2026-10-12**\n\n| file | sha256 |\n|---|---|\n{rows}\n' + extra)
        fails = lambda mode='discovery': R.guard_failures(root, reg_rel, code, data, mode=mode)[0]
        add = lambda row: open(root / 'results.jsonl', 'a').write(json.dumps(row, sort_keys=True) + '\n')
        lines = lambda: (root / 'results.jsonl').read_text().splitlines()
        write_reg(status='DRAFT')
        git('add', '.gitignore', 'ct', 'r014_run.py', 'results.jsonl', reg_rel)
        git('commit', '-q', '-m', 'draft')
        check('DRAFT -> refused', any('not REGISTERED' in x for x in fails()))
        write_reg()
        check('(b) registration modified after commit -> refused', any(x.startswith('(b)') for x in fails()))
        git('commit', '-q', '-am', 'register')
        reg_c = git('rev-parse', 'HEAD').stdout.strip()
        check('REGISTERED, committed, clean, hashes match, no row -> discovery may run', fails() == [], fails())
        (root / 'ct/shortvol.py').write_text('x = 3\n')
        f4 = fails()
        check('(d)+(e) guarded code edited -> refused', any(x.startswith('(d)') for x in f4) and any(x.startswith('(e) ct/shortvol.py') for x in f4))
        git('checkout', '--', 'ct/shortvol.py')
        (root / data[0]).write_text('{"changed": 1}\n')
        check('(e) a panel file changed -> refused', any(x.startswith('(e) data/r014') for x in fails()))
        (root / data[0]).write_text('{}\n')
        add({'registration': '014-x.md', 'mode': 'discovery_started', 'registration_commit': reg_c})
        check('(f) a started row alone blocks a second discovery run', any(x.startswith('(f)') for x in fails()))
        git('commit', '-q', '-am', 'started')
        (root / 'results.jsonl').write_text('\n'.join(l for l in lines() if '014-x' not in l) + '\n')
        git('commit', '-q', '-am', 'deleted')
        check('(f) a deleted 014 row is still found in history', any(x.startswith('(f)') for x in fails()))
        git('checkout', '-q', '-b', 'side')
        add({'registration': '014-x.md', 'mode': 'discovery'})
        git('commit', '-q', '-am', 'side row')
        git('checkout', '-q', 'main')
        check('(f) a 014 row on another branch blocks too', any(x.startswith('(f)') for x in fails()))
        git('branch', '-q', '-D', 'side')
        git('reset', '-q', '--hard', 'HEAD~2')
        subprocess.run(['git', 'reflog', 'expire', '--expire=now', '--all'], cwd=root, capture_output=True)
        subprocess.run(['git', 'gc', '-q', '--prune=now'], cwd=root, capture_output=True)
        check('... history purged (throwaway repo only): discovery may run', fails() == [], fails())

        # ---- a discovery row exists: the holdout gate
        code_h = {f: R.sha256(root / f) for f in code}
        data_h = {f: R.sha256(root / f) for f in data}
        good = series(0.012, 0.02, 143, 2)
        res, dec = sv.evaluate_discovery({'T1': good, 'T2': good})
        wk = {k: [[i, x] for i, x in enumerate(good)] for k in sv.FAMILY}
        row = json.loads(json.dumps({'registration': '014-x.md', 'mode': 'discovery', 'registration_commit': reg_c,
                                     'code_hashes': code_h, 'data_hashes': data_h, 'results': res, 'weekly': wk,
                                     'decision': [dec[0], list(dec[1])]}, default=str))
        add({'registration': '014-x.md', 'mode': 'discovery_started', 'registration_commit': reg_c})
        add(row)
        rline = lines()[-1]
        gh = fails('holdout')
        check('(g2) uncommitted rows -> refused', any(x.startswith('(g2)') for x in gh), gh)
        git('commit', '-q', '-am', 'discovery rows')
        gh = fails('holdout')
        check("(h') no approval -> refused; (g1)-(g4) satisfied", any(x.startswith("(h')") for x in gh)
              and not any(x[:4] in ('(g1)', '(g2)', '(g3)', '(g4)') for x in gh), gh)
        write_reg(extra=f'\nHoldout: APPROVED 2026-10-20 row {"b" * 64}\n')
        git('commit', '-q', '-am', 'wrong approval')
        check("(h') an approval quoting another row's sha -> refused", any(x.startswith("(h')") for x in fails('holdout')))
        write_reg(extra=f'\nHoldout: APPROVED 2026-10-20 row {R.line_sha(rline)}\n')
        git('commit', '-q', '-am', 'approval')
        check('approval committed after the row -> the holdout gate opens', fails('holdout') == [], fails('holdout'))
        write_reg(extra=f'\nHoldout: APPROVED 2026-10-20 row {R.line_sha(rline)}\nprose added later\n')
        git('commit', '-q', '-am', 'prose')
        check('(g3) the registration changed beyond approval lines -> refused', any('beyond added approval' in x for x in fails('holdout')))
        git('reset', '-q', '--hard', 'HEAD~1')
        (root / 'ct/shortvol.py').write_text('x = 9\n')
        git('commit', '-q', '-am', 'code change')
        check('(g3) guarded code changed since the discovery run -> refused', any(x.startswith('(g3)') for x in fails('holdout')))
        git('reset', '-q', '--hard', 'HEAD~1')
        add({'registration': '014-x.md', 'mode': 'holdout_started'})
        check('(i) a holdout_started row -> the holdout is read once', any(x.startswith('(i)') for x in fails('holdout')))
        git('checkout', '--', 'results.jsonl')
        # a forged pass: failing numbers under pass labels, with every earlier row purged from history
        git('reset', '-q', '--hard', reg_c)
        subprocess.run(['git', 'reflog', 'expire', '--expire=now', '--all'], cwd=root, capture_output=True)
        subprocess.run(['git', 'gc', '-q', '--prune=now'], cwd=root, capture_output=True)
        weak = series(0.0005, 0.02, 143, 3)
        rw, _ = sv.evaluate_discovery({'T1': weak, 'T2': weak})
        fake = json.loads(json.dumps(rw))
        for k in fake:
            fake[k]['pass'] = True
            fake[k]['criteria'] = {c: True for c in fake[k]['criteria']}
        add({'registration': '014-x.md', 'mode': 'discovery_started', 'registration_commit': reg_c})
        add(dict(row, results=fake, decision=['read_holdout', ['T1', 'T2']], weekly={k: [[i, x] for i, x in enumerate(weak)] for k in sv.FAMILY}))
        fline = lines()[-1]
        git('commit', '-q', '-am', 'forged row')
        write_reg(extra=f'\nHoldout: APPROVED 2026-10-20 row {R.line_sha(fline)}\n')
        git('commit', '-q', '-am', 'approval of the forged row')
        g4 = fails('holdout')
        check('(g4) alone: a forged pass over failing numbers is caught (the recomputed decision is close)',
              any(x.startswith('(g4)') and 'not read_holdout' in x for x in g4)
              and not any(x[:4] in ('(g1)', '(g2)', "(h')") for x in g4), g4)
        # forged NUMBERS: a passing summary stored over a failing weekly series
        git('reset', '-q', '--hard', reg_c)
        subprocess.run(['git', 'reflog', 'expire', '--expire=now', '--all'], cwd=root, capture_output=True)
        subprocess.run(['git', 'gc', '-q', '--prune=now'], cwd=root, capture_output=True)
        add({'registration': '014-x.md', 'mode': 'discovery_started', 'registration_commit': reg_c})
        add(dict(row, weekly={k: [[i, x] for i, x in enumerate(weak)] for k in sv.FAMILY}))
        fline = lines()[-1]
        git('commit', '-q', '-am', 'forged numbers')
        write_reg(extra=f'\nHoldout: APPROVED 2026-10-20 row {R.line_sha(fline)}\n')
        git('commit', '-q', '-am', 'approval of the forged numbers')
        g4b = fails('holdout')
        check('(g4) alone: a passing summary stored over a failing weekly series is caught (summary recomputed)',
              any(x.startswith('(g4)') and 'recomputed from the weekly series' in x for x in g4b)
              and not any(x[:4] in ('(g1)', '(g2)', "(h')") for x in g4b), g4b)


# ------------------------------------------------------------------ end to end (synthetic panels, throwaway repo)
def e2e_checks():
    print('end to end: build -> guarded discovery run (started row first, one result row) -> approval -> holdout run, once')
    import contextlib
    import io
    import r014_build as B
    import r014_run as R
    disc = sv.calendar_entries('discovery')[:8]
    hold = sv.calendar_entries('holdout')[:6]
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root,
                                        capture_output=True, text=True, check=True)
        git('init', '-q', '-b', 'main')
        dr = root / 'data' / 'r014'
        dr.mkdir(parents=True)
        saved = {m: (m.DATA_ROOT, dict(m.PANELS)) for m in (B, R)}
        saved_r = (R.HERE, R.REG, R.RESULTS, R.guard)
        for m in (B, R):
            m.DATA_ROOT = dr
            m.PANELS.update({'discovery': dr / 'panel_discovery', 'holdout': dr / 'panel_holdout'})
        try:
            pull = synthetic_pull(dr, disc + hold + [sv.ms(2023, 12, 29, 8)], mu_vol=0.01)
            with contextlib.redirect_stdout(io.StringIO()):
                B.main(str(pull))
            code = ('r014_run.py',)
            (root / 'r014_run.py').write_text((HERE / 'r014_run.py').read_text())
            data = tuple(f'data/r014/{p}/{f}' for p in ('panel_discovery', 'panel_holdout') for f in R.PANEL_FILES)
            reg_rel = 'registrations/014-short-vol-deribit.md'
            (root / 'registrations').mkdir()

            def write_reg(extra=''):
                rows = '\n'.join(f'| `{f}` | `{R.sha256(root / f)}` |' for f in (*code, *data))
                (root / reg_rel).write_text(f'# 014\n\n**Status: REGISTERED 2026-10-12**\n\n| file | sha256 |\n|---|---|\n{rows}\n' + extra)
            write_reg()
            (root / 'results.jsonl').write_text(json.dumps({'registration': '005-y.md'}) + '\n')
            (root / '.gitignore').write_text('data/\n')
            git('add', '.')
            git('commit', '-q', '-m', 'registered')
            R.HERE, R.REG, R.RESULTS = root, root / reg_rel, root / 'results.jsonl'

            def g(mode):
                f, c = R.guard_failures(root, reg_rel, code, data, mode=mode)
                if f:
                    raise SystemExit('\n'.join(f))
                return c
            R.guard = g
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                R.run_discovery()
            rows = [json.loads(l) for l in (root / 'results.jsonl').read_text().splitlines()]
            mine = [r for r in rows if r.get('registration') == '014-short-vol-deribit.md']
            check('discovery writes exactly two rows: discovery_started first, then one result row',
                  [r['mode'] for r in mine] == ['discovery_started', 'discovery'])
            dr_row = mine[1]
            check('the counts were printed before the decision', out.getvalue().index('counts before the decision') < out.getvalue().index('decision ['))
            rc = sv.recheck_discovery(dr_row['results'], weekly=dr_row['weekly'])
            check('the stored decision and summary are recomputable from the stored weekly series',
                  rc[0] == tuple([dr_row['decision'][0], dr_row['decision'][1]]) and not rc[1], rc[1])
            check('the synthetic calm world passes discovery (both trials) -> read_holdout', dr_row['decision'] == ['read_holdout', ['T1', 'T2']],
                  (dr_row['decision'], {k: (v['criteria'], v['summary'].get('t'), v['summary'].get('G')) for k, v in dr_row['results'].items()}, dr_row['counts']))
            check('a second discovery run is refused ((f))', raises(R.run_discovery, (SystemExit,)))
            check('the holdout run is refused before the rows are committed and approved', raises(R.run_holdout, (SystemExit,)))
            git('commit', '-q', '-am', 'discovery rows')
            dline = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"mode": "discovery"' in l][0]
            write_reg(f'\nHoldout: APPROVED 2026-10-20 row {R.line_sha(dline)}\n')
            git('commit', '-q', '-am', 'approval')
            with contextlib.redirect_stdout(io.StringIO()):
                R.run_holdout()
            mine = [json.loads(l) for l in (root / 'results.jsonl').read_text().splitlines() if '014-short-vol' in l]
            check('holdout writes holdout_started then one holdout row, for the recomputed trials with k = 2',
                  [r['mode'] for r in mine][-2:] == ['holdout_started', 'holdout'] and mine[-1]['k'] == 2
                  and mine[-1]['trials'] == ['T1', 'T2'])
            check('holdout row: existence per trial at q 0.975 and the decision', all(mine[-1]['existence'][t]['q'] == 0.975 for t in ('T1', 'T2'))
                  and mine[-1]['decision'] in ('not_demonstrated', 'exists_not_economic', 'deploy_paper_registration'))
            check('a second holdout run is refused ((i))', raises(R.run_holdout, (SystemExit,)))
        finally:
            for m, (dr0, pn) in saved.items():
                m.DATA_ROOT = dr0
                m.PANELS.clear()
                m.PANELS.update(pn)
            R.HERE, R.REG, R.RESULTS, R.guard = saved_r


# ------------------------------------------------------------------ 000m: the realistic tail script
def tail_checks():
    print('000m tail script: the 2021-03-24 bound before any request; selection on the underlying only')
    import r014_fetch as F
    import r014_tail as T
    calls = []
    old = F.HTTP
    F.HTTP = lambda url: calls.append(url) or ({'result': []}, 1)
    try:
        check('a request ending on/after 2021-03-24 is refused before any call',
              raises(lambda: T.tget(F.WWW, 'x', {'end_timestamp': T.TAIL_CUTOFF}), (F.FetchError,)) and not calls)
        check('a row on/after 2021-03-24 is refused', raises(lambda: T.below([T.TAIL_CUTOFF], 'x'), (F.FetchError,)))
    finally:
        F.HTTP = old
    E = T.entries()
    check('entries 2019-01-04 -> 2021-03-12, every hold ending before 2021-03-24',
          sv.iso(E[0]) == '2019-01-04 08:00' and sv.iso(E[-1]) == '2021-03-12 08:00' and all(e + 7 * D < T.TAIL_CUTOFF for e in E))
    rows = [[sv.ms(2020, 1, 3, 8) + i * H, 100, 100 * (1.3 if i == 50 else 1.0), 100 * (0.9 if i == 60 else 1.0), 100] for i in range(168)]
    x = T.excursion(rows, sv.ms(2020, 1, 3, 8))
    check('excursion = max(|close|, -low, high) from candles only', close(x['excursion'], 0.3, 1e-12) and close(x['low'], -0.1, 1e-12))
    src = (HERE / 'r014_tail.py').read_text()
    sel = src[src.index('def select('):src.index('# ------------------------------------------------------------------ options tape')]
    check('select() reads the underlying only (no option file, no premium, no outcome)',
          'options' not in sel and 'premium' not in sel and 'position_pnl' not in sel)


# ------------------------------------------------------------------ the real files (status-aware: never stale)
def real_file_checks():
    import r014_run as R
    print('the real registration and code (status-aware)')
    reg = HERE / 'registrations' / '014-short-vol-deribit.md'
    text = reg.read_text()
    m = re.search(r'^\*\*Status: (\w+)', text, re.M)
    check('the status line is DRAFT or REGISTERED', m and m.group(1) in ('DRAFT', 'REGISTERED'))
    if m.group(1) == 'DRAFT':
        fails = R.guard_failures(HERE, 'registrations/014-short-vol-deribit.md', mode='discovery')[0]
        check('while DRAFT the real guard refuses discovery', any('not REGISTERED' in x for x in fails))
        check('while DRAFT no line starts with an approval phrase', not R.APPROVAL.findall(text)
              and not any(l.startswith('Holdout:') for l in text.splitlines()))
    table = dict(R.HASH_ROW.findall(text))
    missing = [f for f in (*R.GUARDED_CODE, *R.GUARDED_DATA) if f not in table]
    wrong = [f for f in (*R.GUARDED_CODE, *R.GUARDED_DATA)
             if f in table and (not (HERE / f).exists() or R.sha256(HERE / f) != table[f])]
    check('§9 lists every guarded code and data file and every hash matches the disk', not missing and not wrong,
          f'missing {missing} wrong {wrong}')
    src = {f: (HERE / f).read_text() for f in ('r014_run.py', 'r014_build.py', 'r014_fetch.py', 'ct/shortvol.py', 'ct/shortvol_sim.py')}
    bad = {f: re.findall(r"(scout_n2|binance_years|carry012|data/carry|data/cache|derived/hyperliquid|scout_l)", s) for f, s in src.items()}
    check("014's code names no data directory but data/r014 (read whitelist, static)", not any(bad.values()), bad)
    check('the run never imports another registration\'s runner or ct module',
          not re.search(r'import (r0(0|1)\d_|ct\.(carry|factors|crowd|vaults|liq|tape))|from ct import (carry|factors|crowd|vaults)', src['r014_run.py']))


if __name__ == '__main__':
    main()
