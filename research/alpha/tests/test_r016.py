"""Registration 016 (crypto trend, verbatim), Revision 1 — mechanics, the timing statistic, parsers, the fetch, the read
gate and the guard, on SYNTHETIC data only. No real kline, funding or price is read: the real-file checks read the
registration, the hash table and file hashes, and call the real guard read-only. The network is disabled for the
whole suite (review B m4): any socket connection raises.

    python3 -I -S tests/test_r016.py                 (from research/alpha)
    python3 -I -S tests/test_r016.py --only gate     (one section; used by tests/test_r016_mutants.py)
"""
import ast
import datetime as dt
import io
import json
import math
import os
import random
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


def _no_network(*a, **k):
    raise OSError('the 016 suite never opens a network connection (review B m4)')


socket.socket.connect = _no_network
socket.create_connection = _no_network

HERE = Path(__file__).resolve().parent.parent
sys.path.append(str(HERE))
import r016_fetch as F  # noqa: E402
import r016_run as RUN  # noqa: E402
from ct16 import data as D  # noqa: E402
from ct16 import rule as R  # noqa: E402
from ct16 import sim  # noqa: E402
from ct16 import stats as S  # noqa: E402

N_CHECKS = [0]


def check(name, ok, detail=''):
    N_CHECKS[0] += 1
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def close(a, b, tol=1e-12):
    return a is not None and b is not None and abs(a - b) <= tol


def raises(f, exc=(R.Refused,)):
    try:
        f()
    except exc:
        return True
    return False


D0 = R.day(dt.date(2024, 1, 1))


def mk(closes, start=D0, qv=1e8, cnt=1000):
    return R.Sym('XUSDT', {start + i: (c, qv, cnt) for i, c in enumerate(closes)})


# ------------------------------------------------------------------ the signal (§4.1-§4.3)
def signal_checks():
    print('signal: Donchian entries, the midpoint trailing stop, sizing, the threshold')
    c = [100.0] * 4 + [101.0, 100.5, 102.0, 101.0, 99.0, 98.0, 97.0, 103.0]
    sp = R.signal_path(c)
    k5 = R.LOOKBACKS.index(5)
    pos5 = [x[0][k5] for x in sp]
    check('no position before n closes exist (t < n - 1)', pos5[:4] == [0, 0, 0, 0])
    check('a flat window enters (c == Up includes ties) and a new high keeps the position', pos5[4] == 1 and pos5[6] == 1)
    # stop after entry at t=4: Mid_4 = (101 + 100) / 2 = 100.5 ; t=5 close 100.5 <= 100.5 -> exit
    check('the initial stop is the midpoint at entry (footnote 3); a close AT the stop exits (<=)', pos5[5] == 0, str(pos5))
    check('re-entry at a new 5-day high', pos5[6] == 1)
    # t=6 entry: window 100,101,100.5,102 + 100 -> Up 102, Dn 100 -> Mid 101 ; t=7 close 101 <= 101 -> exit
    check('exit at the stop on the next close', pos5[7] == 0)
    check('change flags mark entries and exits only', [x[1][k5] for x in sp][3:8] == [False, True, True, True, True])
    # the stop never moves down: rising then falling midpoints
    c2 = [10, 11, 12, 13, 14, 15, 14.9, 14.8, 14.7, 14.6, 14.5, 14.4]
    st, prev, ok = None, None, True
    pos = [x[0][k5] for x in R.signal_path(c2)]
    check('a steady rise stays long until the close falls to the ratcheted stop', pos[4:6] == [1, 1] and 0 in pos[6:])
    c3 = [100, 100, 100, 100, 100, 80, 120, 119, 118]
    pl = [x[0][k5] for x in R.signal_path(c3, 'long_short')]
    check('long/short (Appendix): a close at the n-day low goes short, a new high flips long', pl[5] == -1 and pl[6] == 1, str(pl))
    pa = R.signal_path(c3, 'always')
    check("'always' holds every sub-model long (the vol-managed factor)", all(x[0] == (1,) * 9 for x in pa))
    # entry precedence: the stop above an n-day high (a spike left the window) keeps the long (paper's case order)
    c4 = [10, 10, 10, 10, 10, 100, 60, 58, 56, 57, 61, 50]
    p4 = [x[0][k5] for x in R.signal_path(c4)]
    check('entry precedence: a new 5-day high at or below a stop of 78 stays long (the paper\'s case order); 50 exits',
          p4[5:] == [1, 1, 1, 1, 1, 1, 0], str(p4))
    # sigma: sample SD (n-1) of the last 90 simple returns x sqrt(365), exact recompute every 512 steps
    rng = random.Random(1)
    cs = [100.0]
    for _ in range(1300):
        cs.append(cs[-1] * (1 + rng.gauss(0, 0.04)))
    sg = R.signal_path(cs)
    rets = [cs[i] / cs[i - 1] - 1 for i in range(1, len(cs))]
    ok = all(close(sg[t][2], statistics.stdev(rets[t - 90:t]) * math.sqrt(365), 1e-9) for t in (90, 91, 600, 1100, 1300))
    check('sigma90 = stdev(last 90 simple returns) x sqrt(365), also after 512-step recomputes', ok)
    check('sigma undefined before 90 returns', sg[89][2] is None and sg[90][2] is not None)
    check('targets: min(0.25 / sigma, 2) x pos; the 200% cap; no sigma -> flat',
          R.targets((1,) * 9, 0.5) == (0.5,) * 9 and R.targets((1,) * 9, 0.1) == (2.0,) * 9
          and R.targets((1,) * 9, None) == (0.0,) * 9 and R.targets((0,) * 9, 0.5) == (0.0,) * 9)
    h = R.rebalance((0.5, 0.5, 0.0), (0.65, 0.75, 0.3), (False, False, True))
    check('threshold (§5.1): |0.15| <= 0.20 holds, |0.25| > 0.20 trades, a signal change trades at once', h == (0.5, 0.75, 0.3))
    hr = R.rebalance((0.5, 0.5, 0.0), (0.55, 0.65, 0.0), (False, False, False), relative=True)
    check('the relative-threshold variant (reported): 10% holds, 30% trades', hr == (0.5, 0.65, 0.0))
    # no look-ahead: a later close never changes an earlier state
    base = R.signal_path(cs[:900])
    later = R.signal_path(cs[:900] + [x * 3 for x in cs[900:]])
    check('no look-ahead: states up to t are a function of closes up to t', base == later[:900])


# ------------------------------------------------------------------ the universe (§7.1-§7.2; D8, D9)
def universe_checks():
    print('universe: age, the 7-day traded rule (delisting cut), $2 M median, top 20, exclusions')
    d = R.day(dt.date(2025, 2, 28))
    syms = {}

    def add(name, first, qv, dead_from=None, days_to=d):
        rows = {}
        for x in range(first, days_to + 1):
            dead = dead_from is not None and x >= dead_from
            rows[x] = (10.0 + (x % 3), 0.0 if dead else qv, 0 if dead else 50)
        syms[name] = R.Sym(name, rows)
    for i in range(25):
        add(f'A{i:02d}USDT', d - 800, 1e7 * (i + 1))
    add('YOUNGUSDT', d - 300, 1e12)
    add('DEADUSDT', d - 800, 1e12, dead_from=d - 3)
    add('USDCUSDT', d - 800, 1e12)
    add('WBTCUSDT', d - 800, 1e12)
    add('XAUUSDT', d - 800, 1e12)
    add('TSLAXUSDT', d - 800, 1e12)
    add('THINUSDT', d - 800, 1.5e6)
    sel = R.select_universe(syms, d, non_coin=frozenset({'TSLAXUSDT'}))
    names = [n for n, _ in sel]
    check('top 20 by 30-day median quote volume, highest first', len(names) == 20 and names[0] == 'A24USDT'
          and names[-1] == 'A05USDT', str(names[:3]))
    check('listed < 365 days: out (§7.1)', 'YOUNGUSDT' not in names)
    check('dead rows in the last 7 days: out (the LUNA case; D9)', 'DEADUSDT' not in names)
    check('stablecoin, wrapped, gold and exchangeInfo non-COIN perps: out (§3; D8)',
          not ({'USDCUSDT', 'WBTCUSDT', 'XAUUSDT', 'TSLAXUSDT'} & set(names)))
    sel_all = R.select_universe(syms, d, b=100)
    check('median below $2 M: out', 'THINUSDT' not in [n for n, _ in sel_all])
    check('crypto_symbol: 1000PEPEUSDT in, USDEUSDT and BTCDOMUSDT out, a lowercase name out',
          R.crypto_symbol('1000PEPEUSDT') and not R.crypto_symbol('USDEUSDT') and not R.crypto_symbol('BTCDOMUSDT')
          and not R.crypto_symbol('abcUSDT'))
    s = R.Sym('Q', {x: (100.0 * (1.001 ** (x % 2)), 5e5, 10) for x in range(d - 60, d + 1)})
    check('§7.2 exit: 30-day median volume < $1 M', R.liquidity_exit(s, d))
    s2 = R.Sym('Q', {x: (100.0 * (1.001 ** (x % 2)), 5e7, 10) for x in range(d - 60, d + 1)})
    check('§7.2 exit: 30-day median |daily change| < 0.5%', R.liquidity_exit(s2, d))
    s3 = R.Sym('Q', {x: (100.0 * (1.02 ** (x % 2)), 5e7, 10) for x in range(d - 60, d + 1)})
    check('§7.2: a liquid, moving coin stays', not R.liquidity_exit(s3, d))


# ------------------------------------------------------------------ the book
def _world(seed=3, **kw):
    spec = {'name': 'x', 'drift': 'flat', **kw}
    return sim.make_world(spec, seed)


def book_checks():
    print('book: accounting, resets, the delisting cut, funding, costs, lag, no look-ahead')
    syms = _world()
    a, b = R.day(R.START), R.day(dt.date(2025, 8, 31))
    ufn = R.universe_fn_for(syms)
    b0 = R.run_book(syms, a, b, ufn, cost=0.0, keep_paths=True)
    # with no cost and no funding, the book's return is the equity-weighted sum of H x r over sleeves
    ok = True
    v = 1.0
    for i, d in enumerate(b0['days']):
        prev = b0['paths'][i - 1] if i else None
        if prev is None or R.is_month_end(d - 1):
            v *= 1 + b0['ret'][i]
            continue
        num = 0.0
        for sym, (eq, H) in prev.items():
            r = syms[sym].rows[d][0] / syms[sym].rows[d - 1][0] - 1
            num += eq * H * r
        ok &= close(b0['ret'][i], num / v, 1e-9)
        v *= 1 + b0['ret'][i]
    check('cost 0: R_d = sum over sleeves of equity x H x r / book value (every non-reset day)', ok)
    eqs = [sum(e for e, _ in p.values()) for p in b0['paths']]
    first_after = [i for i, d in enumerate(b0['days']) if R.is_month_end(d)]
    resets = [b0['paths'][i] for i in first_after[:-1]]
    check('§7.2 month-end reset: every sleeve holds exactly 1/B of book value after a reset',
          all(max(e for e, _ in p.values()) - min(e for e, _ in p.values()) < 1e-12 for p in resets) and len(resets) >= 4)
    b1 = R.run_book(syms, a, b, ufn, cost=0.001)
    diff = sum(x - y for x, y in zip(b0['ret'], b1['ret']))
    check('10 bp per unit of traded notional: the cost drag is positive and equals cost x turnover to first order',
          diff > 0 and abs(diff / (0.001 * b1['turnover_per_year'] * (b - a + 1) / 365) - 1) < 0.05,
          f"{diff:.6f} vs {0.001 * b1['turnover_per_year'] * (b - a + 1) / 365:.6f}")
    # funding: a positive rate costs a long; a negative rate pays it
    fund_pos = {s: {d: 0.001 for d in range(a, b + 1)} for s in syms}
    bp = R.run_book(syms, a, b, ufn, cost=0.0, funding=fund_pos)
    fund_neg = {s: {d: -0.001 for d in range(a, b + 1)} for s in syms}
    bn = R.run_book(syms, a, b, ufn, cost=0.0, funding=fund_neg)
    check('funding (D6): long pays a positive rate and receives a negative one',
          sum(bp['ret']) < sum(b0['ret']) < sum(bn['ret']))
    # no look-ahead: altering prices after day X leaves every return up to X unchanged
    x = R.day(dt.date(2025, 6, 10))
    syms2 = _world()
    for s in syms2.values():
        for d in list(s.rows):
            if d > x:
                c, q, n = s.rows[d]
                s.rows[d] = (c * (1.5 if d % 2 else 0.6), q, n)
        s.__init__(s.name, s.rows)
    b2 = R.run_book(syms2, a, b, R.universe_fn_for(syms2), cost=0.001)
    upto = [i for i, d in enumerate(b1['days']) if d <= x]
    check('no look-ahead in the book: returns up to day X do not depend on prices after X',
          all(b1['ret'][i] == b2['ret'][i] for i in upto))
    # the delisting cut: a member whose rows go dead settles at its last traded close and leaves until the month end
    syms3 = _world()
    m = ufn(R.day(dt.date(2025, 4, 30)))
    victim = [s for s in m if s != R.BTC][0]
    cut = R.day(dt.date(2025, 5, 10))
    s = syms3[victim]
    for d in list(s.rows):
        if d >= cut:
            s.rows[d] = (s.rows[cut - 1][0], 0.0, 0)
    s.__init__(s.name, s.rows)
    b3 = R.run_book(syms3, a, b, R.universe_fn_for(syms3), cost=0.001, keep_paths=True)
    i_cut = b3['days'].index(cut)
    check('delisting cut (D9): counted once, the sleeve is gone the same day, and the coin is not reselected',
          b3['counts']['delisting_cuts'] == 1 and victim in b3['paths'][i_cut - 1] and victim not in b3['paths'][i_cut]
          and victim not in b3['members'].get('2025-06', []))
    # a missing row is carried (no return, no decision), counted
    syms4 = _world()
    m5 = ufn(R.day(dt.date(2025, 4, 30)))
    gap = R.day(dt.date(2025, 5, 12))
    del syms4[m5[1]].rows[gap]
    syms4[m5[1]].__init__(m5[1], syms4[m5[1]].rows)
    b4 = R.run_book(syms4, a, b, R.universe_fn_for(syms4), cost=0.001)
    check('a missing row is carried and counted (D4)', b4['counts']['missing_rows'] >= 1)
    bl = R.run_book(syms, a, b, ufn, cost=0.001, lag=1)
    check('the one-close-lag variant runs and differs (reported execution check)', bl['ret'] != b1['ret'])
    check('the per-day records reconstruct the book: sum(w r) - cost = R_d, exactly (the timing statistic\'s input)',
          max(abs(sum(w * r for _, w, r in it) - c - y) for it, c, y in zip(b1['contrib'], b1['cost'], b1['ret'])) < 1e-12)
    f1, f3 = R.factor_series(syms, a, b, ufn)
    btc = syms[R.BTC]
    check('F1 is BTC close-to-close', close(f1[5], btc.rows[a + 5][0] / btc.rows[a + 4][0] - 1))
    vm = R.run_book(syms, a, b, ufn, cost=0.0, mode='always')
    check('F3 is the same book with Pos = 1, gross of costs', vm['ret'] == f3 and vm['gross'][10] > b0['gross'][10] * 0.99)


# ------------------------------------------------------------------ statistics (Revision 1: the timing statistic)
def _fake_book(days, seed=4, timing=0.0, drift=0.0, fwd_timing=None):
    """Per-day contrib for 3 coins: weight w_t = 0.1 + timing * sign(z_t) where z_t is next day's shock direction
    (a timing skill of strength `timing`), plus a constant drift."""
    rng = random.Random(seed)
    contrib, cost = [], []
    for d in days:
        tm = timing if (fwd_timing is None or d < R.day(R.SPAN_C[0])) else fwd_timing
        items = []
        for s in ('BTCUSDT', 'AUSDT', 'BUSDT'):
            z = rng.gauss(0, 0.03)
            w = 0.1 + 0.05 * rng.random() + tm * (1 if z > 0 else -1) * rng.random()
            items.append((s, w, z + drift))
        contrib.append(items)
        cost.append(0.00002)
    return contrib, cost


def stats_checks():
    print('statistics: t, the CR1 mean, the timing statistic A_t, H1 (pooled + span (c) alone), the ladder, the bar, '
          'the recheck; the reported factor alpha')
    check('t_crit: t_32(0.95) 1.694, t_14(0.95) 1.761', close(S.t_crit(32), 1.6939, 2e-4) and close(S.t_crit(14), 1.7613, 2e-4))
    check('noncentral t at delta 0 is Student t', close(S.nct_quantile(0.05, 20, 0.0), -S.t_crit(20), 1e-3))
    rng = random.Random(4)
    days = list(range(R.day(R.START), R.day(R.END) + 1))
    check('G = 33 month clusters (the March 2025 stub joins April)', len({R.cluster_of(d) for d in days}) == 33)
    x = [rng.gauss(0.001, 0.01) for _ in days]
    mt = S.mean_t(days, x)
    m = sum(x) / len(x)
    g = {}
    for d, v in zip(days, x):
        g[R.cluster_of(d)] = g.get(R.cluster_of(d), 0.0) + v - m
    se = math.sqrt(sum(u * u for u in g.values()) / len(x) ** 2 * 33 / 32)
    check('mean_t: the CR1 standard error by calendar month, by hand', close(mt['se'], se, 1e-15) and close(mt['t'], m / se))
    contrib, cost = _fake_book(days, timing=0.0, drift=0.0005)
    A, wbar = S.timing_series(contrib, cost)
    R_ = [sum(w * r for _, w, r in it) - c for it, c in zip(contrib, cost)]
    passive = [sum(wbar[s] * r for s, _, r in it) for it in contrib]
    check('A_t = book return - sum_i wbar_i r_i,t (Lo 2008: active = total - passive), exactly',
          max(abs(a - (rr - p)) for a, rr, p in zip(A, R_, passive)) < 1e-15)
    t0 = S.timing(days, contrib, cost)['t']
    check('a constant drift and no timing gives no timing t (|t| < 2), whatever the passive drift earns', abs(t0) < 2.0,
          f'{t0:.2f}')
    c1, k1 = _fake_book(days, timing=0.05)
    h = S.h1(days, c1, k1, cal=1.9)
    check('H1 passes a strong timing book: pooled t >= crit and span (c) alone t >= 1.282', h['pass'] and h['pass_forward'],
          f"{h['pooled']['t']:.2f} / {h['forward']['t']:.2f}")
    fi = S.forward_idx(days)
    _, wb_c = S.timing_series(c1, k1, fi)
    check('the span-(c) statistic uses its OWN wbar (computed over (c) member-days only)',
          close(h['forward']['t'], S.mean_t([days[i] for i in fi], S.timing_series(c1, k1, fi)[0])['t']))
    c2_, k2 = _fake_book(days, timing=0.05, fwd_timing=0.0)
    h2_ = S.h1(days, c2_, k2, cal=1.9)
    check('H1 refuses when the pooled t passes but (c) alone is below 1.282 (review A B2)',
          h2_['pass_t'] and not h2_['pass_forward'] and not h2_['pass'], f"{h2_['pooled']['t']:.2f} / {h2_['forward']['t']:.2f}")
    check('the floor is t_{G-1}(0.95) = t_32; a calibrated value never lowers it',
          S.h1_crit(33, cal=0.0) == (S.t_crit(32), S.t_crit(32)) and S.h1_crit(33, cal=2.5)[0] == 2.5)
    check('ladder: alive > decayed (t < C2) > not_demonstrated',
          S.ladder({'pass': True, 'pooled': {'t': 5.0}}, c2=1.0) == 'alive'
          and S.ladder({'pass': False, 'pooled': {'t': 0.5}}, c2=1.0) == 'decayed'
          and S.ladder({'pass': False, 'pooled': {'t': 1.5}}, c2=1.0) == 'not_demonstrated'
          and S.ladder({'pass': False, 'pooled': {'t': 2.5}}, c2=1.0) == 'not_demonstrated')
    e = S.economic({'sr_annual': 1.0}, 3.0, 2.78)
    srs = 1.0 * (1 - 1 / 9)
    check('economic bar (review A M3): return at the Bayes-Kelly vol SR_s/(1 + 1/years) of the shrunk Sharpe, vs 4%',
          close(e['return_at_bk'], srs * srs / (1 + 1 / 2.78)) and e['clears']
          and not S.economic({'sr_annual': 0.25}, 2.0, 2.78)['clears'])
    check('routes: decayed -> close; not_demonstrated -> not_demonstrated; alive -> paper or below bar',
          S.route('decayed', None) == 'close' and S.route('not_demonstrated', None) == 'not_demonstrated'
          and S.route('alive', {'clears': True}) == 'paper_then_scale' and S.route('alive', {'clears': False})
          == 'exists_below_bar')
    dec = S.decision_from_series(days, c1, k1, c1, k1, cal_h1=1.9, c2=0.0)
    pack = lambda c: [[[s, w, r] for s, w, r in day] for day in c]
    row = {'series': {'days': [R.date_of(d).isoformat() for d in days], 'contrib': pack(c1), 'cost': k1,
                      'dep_contrib': pack(c1), 'dep_cost': k1},
           'outcome': dec['outcome'], 'route': dec['route'], 'h1_t': dec['h1']['pooled']['t'],
           'h1_t_forward': dec['h1']['forward']['t']}
    saved = (S.CAL_H1, S.C2)
    S.CAL_H1, S.C2 = 1.9, 0.0
    try:
        check('recheck: a genuine row recomputes', S.recheck(json.loads(json.dumps(row)))[0] == [])
        forged = dict(row, outcome='decayed')
        check('recheck: a forged outcome is caught', S.recheck(forged)[0] != [])
        tampered = json.loads(json.dumps(row))
        tampered['series']['contrib'][100][0][1] += 0.5
        check('recheck: a tampered record under the stored statistics is caught', S.recheck(tampered)[0] != [])
    finally:
        S.CAL_H1, S.C2 = saved
    check('the registered constants: CAL_H1 and C2 are set and C2 < CAL_H1', S.CAL_H1 is not None and S.C2 is not None
          and S.C2 < S.CAL_H1, f'{S.CAL_H1} / {S.C2}')
    # the reported factor alpha (constant slopes) still recovers a known alpha
    f1 = [rng.gauss(0, 0.03) for _ in days]
    f2 = [0.8 * a + rng.gauss(0, 0.02) for a in f1]
    f3 = [0.3 * a + rng.gauss(0, 0.01) for a in f2]
    yc = [0.0004 + 0.2 * a + 0.05 * b - 0.2 * cc + rng.gauss(0, 0.004) for a, b, cc in zip(f1, f2, f3)]
    fc = S.alpha_fit(days, yc, [f1, f2, f3])
    check('the reported factor alpha: constant slopes, k = 3, G = 33, recovers the slope', fc['k'] == 3 and fc['G'] == 33
          and abs(fc['slopes'][0] - 0.2) < 0.03)
    fh = S.alpha_fit(days, yc, [f1, f2, f3], per_cell=True)
    check('half-year cells (reported): k = 18', fh['k'] == 18 and fh['cells'] == 6)


# ------------------------------------------------------------------ parsers
def _zip(name, text):
    b = io.BytesIO()
    with zipfile.ZipFile(b, 'w') as z:
        z.writestr(name, text)
    return b.getvalue()


def kline_csv(rows, header=True):
    out = ['open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,'
           'ignore'] if header else []
    for d, c, q, n in rows:
        t = int(dt.datetime.combine(R.date_of(d), dt.time(), dt.UTC).timestamp() * 1000)
        out.append(f'{t},{c},{c},{c},{c},1,{t + 86399999},{q},{n},0,0,0')
    return '\n'.join(out) + '\n'


def parser_checks():
    print('parsers: klines, funding days, SHA256SUMS, file names')
    d = R.day(dt.date(2026, 7, 1))
    k = D.parse_klines(kline_csv([(d, 10.0, 5e6, 7), (d + 1, 11.0, 6e6, 0)]))
    check('klines with a header; (close, quote volume, count) by UTC day', k == {d: (10.0, 5e6, 7), d + 1: (11.0, 6e6, 0)})
    k2 = D.parse_klines(kline_csv([(d, 10.0, 5e6, 7)], header=False))
    check('klines without a header (older files)', k2 == {d: (10.0, 5e6, 7)})
    t_us = int(dt.datetime(2026, 7, 2, tzinfo=dt.UTC).timestamp() * 1e6)
    check('microsecond open times are read as such', D.parse_klines(f'{t_us},1,1,1,2,1,0,3,4,0,0,0\n') == {d + 1: (2.0, 3.0, 4)})
    check('rows outside [not_before, before) are dropped', D.parse_klines(kline_csv([(d, 1, 1, 1), (d + 1, 1, 1, 1)]),
                                                                          before=d + 1) == {d: (1.0, 1.0, 1)})
    check('a malformed row, a duplicate day and a bad header refuse',
          raises(lambda: D.parse_klines('123,1,1\n')) and raises(lambda: D.parse_klines(kline_csv([(d, 1, 1, 1), (d, 1, 1, 1)])))
          and raises(lambda: D.parse_klines('time,x\n')))
    base = int(dt.datetime(2026, 7, 2, tzinfo=dt.UTC).timestamp() * 1000)
    check('funding day (D6): 00:00 of d+1 belongs to d; 08:00 and 16:00 of d to d; a few ms of offset are rounded',
          D.funding_day(base) == d and D.funding_day(base + 8 * 3600000) == d + 1 and D.funding_day(base + 5) == d
          and D.funding_day(base - 3) == d)
    ft = 'calc_time,funding_interval_hours,last_funding_rate\n' + '\n'.join(
        f'{base + h * 3600000},8,0.0001' for h in (0, 8, 16, 24)) + '\n'
    fd = D.parse_funding(ft)
    check('funding sums per holding day', close(fd[d], 0.0001) and close(fd[d + 1], 0.0003))
    check('a duplicate funding time refuses', raises(lambda: D.parse_funding(ft + f'{base},8,0.0001\n')))
    check('SHA256SUMS: parsed; a malformed line refuses', D.parse_sums('a' * 64 + '  klines/X/1d/X-1d-2026-06.zip\n')
          == {'klines/X/1d/X-1d-2026-06.zip': 'a' * 64} and raises(lambda: D.parse_sums('zz  x\n')))
    check('file names: klines and funding months', D.file_month('klines/BTCUSDT/1d/BTCUSDT-1d-2026-07.zip')
          == ('klines', 'BTCUSDT', '2026-07') and D.file_month('fundingRate/BTCUSDT/BTCUSDT-fundingRate-2025-03.zip')
          == ('funding', 'BTCUSDT', '2025-03') and D.file_month('klines/A/1d/B-1d-2026-07.zip') is None)
    check('a zip with two members refuses', raises(lambda: D.zip_text(_two_member_zip())))


def _two_member_zip():
    b = io.BytesIO()
    with zipfile.ZipFile(b, 'w') as z:
        z.writestr('a.csv', 'x')
        z.writestr('b.csv', 'y')
    return b.getvalue()




# ------------------------------------------------------------------ throwaway repositories (both layouts)
CODE = ('ct16/rule.py', 'r016_run.py')
DATAF = ('data/static.json',)
REG = 'registrations/016-x.md'
NOW = dt.datetime(2028, 1, 10, tzinfo=dt.UTC)
EARLY = dt.datetime(2028, 1, 7, 23, 0, tzinfo=dt.UTC)
GIT = '/usr/bin/git'
PY = sys.executable


def _git_fn(cwd, name='t'):
    def git(*a, check_=True):
        env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_')}
        env.update({'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': os.devnull})
        return subprocess.run([GIT, '-c', f'user.name={name}', '-c', f'user.email={name}@t', '-c',
                               'core.hooksPath=' + os.devnull, *a], cwd=cwd, capture_output=True, text=True,
                              check=check_, env=env)
    return git


def _repo(layout):
    """(runner dir, git top, git). layout 'top': the runner at the top level; 'sub': in research/alpha."""
    top = Path(tempfile.mkdtemp(prefix='r016-test-'))
    root = top if layout == 'top' else top / 'research' / 'alpha'
    root.mkdir(parents=True, exist_ok=True)
    git = _git_fn(top)
    git('init', '-q', '-b', 'main')
    for f, body in (('ct16/rule.py', 'x = 1\n'), ('r016_run.py', 'y = 2\n'), ('data/static.json', '{}\n'),
                    ('results.jsonl', '')):
        (root / f).parent.mkdir(parents=True, exist_ok=True)
        (root / f).write_text(body)
    (top / '.gitignore').write_text('data/\n' if layout == 'top' else 'research/alpha/data/\n')
    return root, top, git


def _write_reg(root, status='REGISTERED', extra=''):
    rows = '\n'.join(f'| `{f}` | `{RUN.sha256(root / f)}` |' for f in (*CODE, *DATAF))
    (root / REG).parent.mkdir(exist_ok=True)
    (root / REG).write_text(f'# 016\n\n**Status: {status} 2026-10-14**\n\n| file | sha256 |\n|---|---|\n{rows}\n' + extra)


def _add(root, row):
    with open(root / 'results.jsonl', 'a') as f:
        f.write(json.dumps(row, sort_keys=True) + '\n')


def _gf(root, mode='fetch', now=NOW, **kw):
    return RUN.guard_failures(root, mode, now=now, code=CODE, data=DATAF, reg_rel=REG, **kw)[0]


def _has(fs, prefix):
    return any(x.startswith(prefix) for x in fs)


def _registered(layout='sub'):
    root, top, git = _repo(layout)
    _write_reg(root)
    git('add', '.')
    git('commit', '-q', '-m', 'registered')
    return root, top, git


def _rel(root, top, p):
    return str((root / p).relative_to(top))


def guard_checks(layout):
    print(f'guard ({layout} layout, throwaway git repository)')
    root, top, git = _repo(layout)
    rel = lambda p: _rel(root, top, p)
    _write_reg(root, status='DRAFT')
    git('add', '.gitignore', rel('ct16'), rel('r016_run.py'), rel('results.jsonl'))
    git('commit', '-q', '-m', 'code')
    f0 = _gf(root)
    check('DRAFT and untracked registration: (s) and (a) refuse', _has(f0, '(s)') and _has(f0, '(a)'))
    _write_reg(root)
    git('add', rel(REG))
    git('commit', '-q', '-m', 'register')
    check('REGISTERED, committed, clean, after 2028-01-08: the fetch gate opens', _gf(root) == [], str(_gf(root)))
    check('(t) the date gate refuses before 2028-01-08 (UTC)', _has(_gf(root, now=EARLY), '(t) the forward span'))
    check('the real clock today is before the gate', _has(_gf(root, now=None), '(t)') or dt.date.today() >= R.READ_NOT_BEFORE)
    (root / REG).write_text((root / REG).read_text().replace('REGISTERED', 'DRAFT'))
    f_wt = _gf(root)
    check('(s) is read from HEAD, (b) sees the working-tree edit', not _has(f_wt, '(s)') and _has(f_wt, '(b)'))
    git('checkout', '--', rel(REG))
    (root / 'ct16/rule.py').write_text('x = 3\n')
    f4 = _gf(root)
    check('(d)+(e) guarded code edited', _has(f4, '(d) guarded code') and _has(f4, '(e) ct16/rule.py'))
    git('checkout', '--', rel('ct16/rule.py'))
    _add(root, {'registration': '016-x.md', 'mode': 'fetch_attempt'})
    check('(k) results.jsonl dirty', _has(_gf(root), '(k)'))
    git('checkout', '--', rel('results.jsonl'))
    (root / 'results.jsonl').write_text('{"registration": "015-y.md"}')
    git('commit', '-q', '-am', 'no newline')
    check('(k) results.jsonl without a final newline (a row would merge into another; review B m3)',
          _has(_gf(root), '(k) results.jsonl does not end with a newline'))
    check('append_row refuses to glue a row onto a line without a newline',
          raises(lambda: RUN.append_row({'registration': '016-x.md'}, root / 'results.jsonl')))
    git('reset', '-q', '--hard', 'HEAD~1')
    check('(w) the anchor must be an ancestor of HEAD', _has(_gf(root, anchor='0' * 40), '(w)'))
    rc = git('rev-parse', 'HEAD').stdout.strip()
    _add(root, {'registration': '016-x.md', 'mode': 'fetch_started', 'registration_commit': rc})
    git('commit', '-q', '-am', 'fetch_started')
    check('a committed fetch_started row carrying reg0: the fetch resumes', _gf(root) == [], str(_gf(root)))
    store = root / RUN.STORE_REL
    store.mkdir(parents=True)
    (store / 'a.zip').write_bytes(b'abc')
    files = {'a.zip': RUN.sha256(store / 'a.zip')}
    (store / 'manifest.json').write_text(json.dumps({'files': files, 'digest': RUN.manifest_digest(files)}))
    _add(root, {'registration': '016-x.md', 'mode': 'fetched', 'registration_commit': rc,
                'code_hashes': RUN.hashes(root, CODE), 'manifest_digest': RUN.manifest_digest(files)})
    git('commit', '-q', '-am', 'fetched')
    check('(f) the fetch runs once: a fetched row closes it', _has(_gf(root), '(f) the fetch has completed'))
    fr = _gf(root, mode='run')
    check("(h') the read needs the approval line", _has(fr, "(h')") and not _has(fr, '(g'), str(fr))
    fline = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"fetched"' in l][0]
    (root / REG).write_text((root / REG).read_text() + f'016 read: APPROVED 2028-01-11 row {RUN.line_sha(fline)}\n')
    git('commit', '-q', '-am', 'approval')
    check('everything in order: the read gate opens (HEAD:./ paths; reg0 accepts an approval line)',
          _gf(root, mode='run') == [], str(_gf(root, mode='run')))
    check('the read gate also refuses before the date', _has(_gf(root, mode='run', now=EARLY), '(t)'))
    (store / 'a.zip').write_bytes(b'abd')
    check('(g3) a fetched file changed after the fetch', _has(_gf(root, mode='run'), '(g3)'))
    (store / 'a.zip').write_bytes(b'abc')
    _add(root, {'registration': '016-x.md', 'mode': 'read_started'})
    git('commit', '-q', '-am', 'started by hand')
    check('(i) a read_started row without a restart line blocks', _has(_gf(root, mode='run'), '(i)'))
    sline = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"read_started"' in l][0]
    (root / REG).write_text((root / REG).read_text() + f'016 read restart: APPROVED 2028-01-12 started-row '
                            f'{RUN.line_sha(sline)}\n')
    git('commit', '-q', '-am', 'restart')
    check('a restart line reopens a started row that no runner commit wrote', _gf(root, mode='run') == [],
          str(_gf(root, mode='run')))
    _add(root, {'registration': '016-x.md', 'mode': 'read'})
    git('commit', '-q', '-am', 'read')
    git('reset', '-q', '--hard', 'HEAD~1')
    check('(i) a read row erased by reset --hard still blocks (reflog)', _has(_gf(root, mode='run'), '(i)'))


def prose_checks():
    print('(r) prose committed after REGISTERED, even if later reset, refuses forever (review B M4)')
    root, top, git = _registered()
    (root / REG).write_text((root / REG).read_text() + 'new prose\n')
    git('commit', '-q', '-am', 'prose')
    check('(r) prose committed after REGISTERED', _has(_gf(root), '(r)'))
    git('reset', '-q', '--hard', 'HEAD~1')
    check('(r) ... and still after `reset --hard` (the reflog keeps the second REGISTERED content)', _has(_gf(root), '(r)'))


def restart_checks():
    print('restart: refused when the runner itself committed a read row (the data were opened)')
    root, top, git = _registered()
    rc = git('rev-parse', 'HEAD').stdout.strip()
    _add(root, {'registration': '016-x.md', 'mode': 'read_started', 'registration_commit': rc})
    rg = _git_fn(top, name=RUN.RUNNER_NAME)
    rg('commit', '-q', '-am', 'started by the runner')
    sline = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"read_started"' in l][0]
    (root / REG).write_text((root / REG).read_text() + f'016 read restart: APPROVED 2028-01-12 started-row '
                            f'{RUN.line_sha(sline)}\n')
    git('commit', '-q', '-am', 'restart line')
    check('(i) a runner-committed read_started row is never restarted', _has(_gf(root, mode='run'), '(i) a read_started'))


def history_checks():
    print('reg0 (review B M4): the read is bound to the one REGISTERED content')
    root, top, git = _registered()
    (root / 'ct16/rule.py').write_text('x = 99\n')
    _write_reg(root)
    git('commit', '-q', '-am', 'a 2027 revision of the code and the table')
    f = _gf(root)
    check('a committed revision of code + hash table after REGISTERED refuses, naming the REGISTERED commit',
          any('(r) ct16/rule.py at HEAD differs from the REGISTERED commit' in x for x in f), str(f[:3]))
    root, top, git = _registered()
    git('checkout', '-q', '-b', 'side')
    (root / REG).write_text((root / REG).read_text().replace('# 016', '# 016 (revised)'))
    git('commit', '-q', '-am', 'a second REGISTERED content on a side branch')
    git('checkout', '-q', 'main')
    git('merge', '-q', '--no-ff', '-s', 'ours', 'side', '-m', 'merge keeping main')
    f = _gf(root)
    check('a second REGISTERED content anywhere (a --no-ff merge) refuses',
          any('different REGISTERED versions' in x for x in f), str(f[:3]))
    root, top, git = _registered()
    rel = _rel(root, top, 'ct16/rule.py')
    (root / 'ct16/rule.py').write_text('x = 7\n')
    git('update-index', '--skip-worktree', rel)
    f = _gf(root)
    check('(d) skip-worktree on guarded code refuses (review B M3)', any('index flag' in x for x in f) and _has(f, '(d) guarded'))
    git('update-index', '--no-skip-worktree', rel)
    git('checkout', '--', rel)
    git('update-index', '--assume-unchanged', _rel(root, top, REG))
    (root / REG).write_text((root / REG).read_text() + 'x\n')
    f = _gf(root)
    check('(b)+(d) assume-unchanged on the registration refuses', _has(f, '(b)') and any('index flag' in x for x in f))
    git('update-index', '--no-assume-unchanged', _rel(root, top, REG))
    git('checkout', '--', _rel(root, top, REG))
    (top / '.gitattributes').write_text('*.py filter=x\n')
    git('add', '.gitattributes')
    git('commit', '-q', '-m', 'attr')
    check('(d) a filter attribute on guarded code refuses', any('filter attribute' in x for x in _gf(root)))
    root, top, git = _registered()
    _add(root, {'registration': '016-x.md', 'mode': 'fetched'})
    git('commit', '-q', '-am', 'row')
    tree = git('write-tree').stdout.strip()
    git('checkout', '-q', '--orphan', 'orphan')
    git('rm', '-q', '-r', '--cached', '.', check_=False)
    (root / 'results.jsonl').write_text('{"registration": "016-x.md", "mode": "read"}\n')
    git('add', _rel(root, top, 'results.jsonl'))
    git('commit', '-q', '-m', 'orphan root with a read row')
    git('checkout', '-q', '-f', 'main')
    check('a read row in the ROOT commit of an orphan branch is seen (log --root, showRoot)',
          _has(_gf(root, mode='run'), '(i)'))
    root, top, git = _registered()
    _add(root, {'registration': '016-x.md', 'mode': 'read'})
    git('commit', '-q', '-am', 'row')
    row_c = git('rev-parse', 'HEAD').stdout.strip()
    (root / 'results.jsonl').write_text('')
    git('commit', '-q', '-am', 'erased')
    decoy = git('rev-parse', 'HEAD~2').stdout.strip()
    git('replace', row_c, decoy)
    check('git replace cannot hide a read row (--no-replace-objects)', _has(_gf(root, mode='run'), '(i)'))
    root, top, git = _registered()
    with open(root / 'results.jsonl', 'a') as fh:
        fh.write('{"registration": "016-x.md", "mode": "read"\n')
    git('commit', '-q', '-am', 'broken line')
    check('(f) an unparseable added line that mentions a 016 registration refuses',
          any('unparseable line mentions a 016' in x for x in _gf(root)))


def bypass_checks():
    print('bypasses used on earlier runners (erasure, attributes, GIT_DIR, nested repos, .pyc, -O)')
    for layout in ('top', 'sub'):
        root, top, git = _repo(layout)
        _write_reg(root)
        git('add', '.')
        git('commit', '-q', '-m', 'all')
        rc = git('rev-parse', 'HEAD').stdout.strip()
        store = root / RUN.STORE_REL
        store.mkdir(parents=True)
        (store / 'manifest.json').write_text(json.dumps({'files': {}, 'digest': RUN.manifest_digest({})}))
        _add(root, {'registration': '016-x.md', 'mode': 'fetch_started', 'registration_commit': rc})
        fr = {'registration': '016-x.md', 'mode': 'fetched', 'registration_commit': rc,
              'code_hashes': RUN.hashes(root, CODE), 'manifest_digest': RUN.manifest_digest({})}
        line = json.dumps(fr, sort_keys=True)
        with open(root / 'results.jsonl', 'a') as fh:
            fh.write(line + '\n')
        (root / REG).write_text((root / REG).read_text() + f'016 read: APPROVED 2028-01-11 row {RUN.line_sha(line)}\n')
        git('commit', '-q', '-am', 'rows and approval together')
        check(f"({layout}) (h') an approval committed with the fetched row is refused", _has(_gf(root, mode='run'), "(h')"))
    root, top, git = _registered()
    _add(root, {'registration': '016-x.md', 'mode': 'fetched'})
    git('commit', '-q', '-am', 'row')
    (root / 'results.jsonl').write_text('')
    git('commit', '-q', '--amend', '--allow-empty', '-am', 'row removed by amend')
    check('a row removed with commit --amend still blocks (reflog)', _has(_gf(root), '(f)'))
    root, top, git = _registered()
    _add(root, {'registration': '016-other.md', 'mode': 'fetched'})
    git('stash', '-q')
    check('a stashed row (a merge commit) blocks (-m), keyed on the 016- prefix', _has(_gf(root), '(f)'))
    root, top, git = _registered()
    _add(root, {'registration': '016-x.md', 'mode': 'read'})
    git('commit', '-q', '-am', 'row')
    (root / 'results.jsonl').write_text('')
    git('commit', '-q', '-am', 'erased')
    (top / '.git' / 'info').mkdir(exist_ok=True)
    (top / '.git' / 'info' / 'attributes').write_text('*.jsonl -diff\n*.jsonl diff=blank\n')
    git('config', 'diff.blank.textconv', 'true')
    check('a -diff attribute and a textconv driver do not hide an erased row', _has(_gf(root, mode='run'), '(i)'))
    root, top, git = _registered()
    _add(root, {'registration': '016-x.md', 'mode': 'fetched'})
    git('commit', '-q', '-am', 'row')
    other, otop, ogit = _registered()
    os.environ['GIT_DIR'] = str(otop / '.git')
    try:
        fr = _gf(root)
    finally:
        del os.environ['GIT_DIR']
    check('GIT_DIR pointing at another repository is stripped: the real history still blocks', _has(fr, '(f)'))
    with tempfile.TemporaryDirectory() as d:
        dd = Path(d)
        (dd / 'modx.py').write_text('X = 1\n')
        import importlib.util as iu
        import marshal
        import struct
        st = (dd / 'modx.py').stat()
        code = compile('X = 666\n', str(dd / 'modx.py'), 'exec')
        pyc = dd / '__pycache__' / f'modx.{sys.implementation.cache_tag}.pyc'
        pyc.parent.mkdir()
        pyc.write_bytes(iu.MAGIC_NUMBER + struct.pack('<III', 0, int(st.st_mtime) & 0xFFFFFFFF, st.st_size & 0xFFFFFFFF)
                        + marshal.dumps(code))
        run = lambda pre: subprocess.run([PY, '-I', '-S', '-c', pre + 'import sys; sys.path.insert(0, "."); import modx; '
                                          'print(modx.X)'], cwd=dd, capture_output=True, text=True).stdout.strip()
        check('a forged __pycache__ .pyc IS loaded by plain Python and is NOT under a fresh sys.pycache_prefix',
              run('') == '666' and run('import sys, tempfile; sys.pycache_prefix = tempfile.mkdtemp(); ') == '1')
    asserts = []
    for f in RUN.GUARDED_CODE:
        asserts += [f for n in ast.walk(ast.parse((HERE / f).read_text())) if isinstance(n, ast.Assert)]
    check('no assert statement anywhere in the guarded code (refusals survive -O)', not asserts, str(asserts))


# ------------------------------------------------------------------ the environment (review B M2)
def env_checks():
    print('environment: -I -S, interpreter and stdlib pins, module origins, stdlib-named files, the fetcher\'s imports')
    p = subprocess.run([PY, '-c', 'import sys; sys.path.append("."); import r016_run as R; '
                        'print([x[:40] for x in R.environment_failures(R.HERE)])'], cwd=HERE, capture_output=True,
                       text=True)
    check('without -I: (v) refuses (isolation)', '(v) run with python3 -I -S (isolated' in p.stdout, p.stderr[-200:])
    p = subprocess.run([PY, '-I', '-c', 'import sys; sys.path.append("."); import r016_run as R; '
                        'print([x[:40] for x in R.environment_failures(R.HERE)])'], cwd=HERE, capture_output=True,
                       text=True)
    check('-I without -S: (v) refuses (a site-packages .pth would run)', '(no site)' in p.stdout, p.stdout[-300:])
    p = subprocess.run([PY, '-I', '-S', 'r016_run.py', '--check'], cwd=HERE, capture_output=True, text=True)
    out = p.stdout
    check('the registered command (python3 -I -S r016_run.py) shows no (v) failure: the pins equal this interpreter',
          '(v)' not in out and p.returncode == 0, out[-400:] + p.stderr[-300:])
    p = subprocess.run([PY, '-I', '-S', '-O', '-c', 'import sys; sys.path.append("."); import r016_run as R; '
                        'print(any("-O" in x for x in R.environment_failures(R.HERE)))'], cwd=HERE,
                       capture_output=True, text=True)
    check('python -O is refused by (v)', p.stdout.strip() == 'True', p.stderr[-200:])
    p = subprocess.run([PY, '-I', '-S', '-c', 'import runpy, sys; runpy.run_path("r016_fetch.py", run_name="fetcher"); '
                        'print(sys.modules["r016_run"].module_origin_failures())'], cwd=HERE, capture_output=True,
                       text=True)
    check('the fetcher imports the guard from source under a fresh pycache prefix (review B M2)',
          p.stdout.strip() == '[]', p.stdout[-300:] + p.stderr[-300:])
    root, top, git = _registered()
    (root / 'csv.py').write_text('MARK = True\n')
    git('add', _rel(root, top, 'csv.py'))
    git('commit', '-q', '-m', 'a committed csv.py')
    check('(d) a COMMITTED stdlib-named module beside the runner is refused', any('csv.py (stdlib name)' in x for x in _gf(root)))
    for f in ('ct16/__init__.py', 'ct16/data.py', 'ct16/rule.py', 'ct16/stats.py', 'r016_run.py'):
        (root / f).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(HERE / f, root / f)
    p = subprocess.run([PY, '-I', '-S', '-c', 'import sys; sys.path.append("."); import r016_run; import csv; '
                        'print(hasattr(csv, "MARK"), csv.__file__)'], cwd=root, capture_output=True, text=True)
    check('with research/alpha APPENDED to sys.path, `import csv` resolves to the stdlib', p.stdout.startswith('False'),
          p.stdout + p.stderr[-200:])


# ------------------------------------------------------------------ the binding to the real repository (review B B1)
def make_sandbox():
    """A stand-in for the real repository: a fresh git repo (no clone of the real one, no real data) holding a copy of
    016's code with REAL_REPO and ANCHOR rewritten to the sandbox, a synthetic pull listing and exchangeInfo, and a
    REGISTERED registration whose table matches the sandbox's files. Returns (alpha dir, top, git)."""
    top = Path(tempfile.mkdtemp(prefix='r016-sandbox-')).resolve()
    alpha = top / 'research' / 'alpha'
    git = _git_fn(top)
    git('init', '-q', '-b', 'alpha-discovery')
    (top / 'README').write_text('anchor\n')
    git('add', 'README')
    git('commit', '-q', '-m', 'anchor')
    anchor = git('rev-parse', 'HEAD').stdout.strip()
    for f in (*RUN.GUARDED_CODE, 'tests/test_r016.py'):
        (alpha / f).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(HERE / f, alpha / f)
    src = (alpha / 'r016_run.py').read_text()
    src = src.replace(f"REAL_REPO = Path('{RUN.REAL_REPO}')", f"REAL_REPO = Path('{top}')")
    src = src.replace(f"ANCHOR = '{RUN.ANCHOR}'", f"ANCHOR = '{anchor}'")
    (alpha / 'r016_run.py').write_text(src)
    (alpha / 'results.jsonl').write_text('')
    (top / '.gitignore').write_text('research/alpha/data/\n__pycache__/\n')
    (alpha / RUN.KLINES_A_REL).mkdir(parents=True)
    (alpha / RUN.KLINES_A_REL / 'SHA256SUMS').write_text('a' * 64 + '  klines/XUSDT/1d/XUSDT-1d-2026-06.zip\n')
    (alpha / RUN.STATIC_EXCHANGE_REL).parent.mkdir(parents=True)
    (alpha / RUN.STATIC_EXCHANGE_REL).write_text('{"symbols": []}')
    rows = '\n'.join(f'| `{f}` | `{RUN.sha256(alpha / f)}` |' for f in (*RUN.GUARDED_CODE, *RUN.GUARDED_DATA))
    (alpha / RUN.REG_REL).parent.mkdir(parents=True, exist_ok=True)
    (alpha / RUN.REG_REL).write_text(f'# 016 sandbox\n\n**Status: REGISTERED 2026-10-14**\n\n| file | sha256 |\n'
                                     f'|---|---|\n{rows}\n')
    git('add', '.')
    git('commit', '-q', '-m', 'sandbox REGISTERED')
    return alpha, top, git


def _check(alpha, script='r016_run.py', args=('--check',)):
    p = subprocess.run([PY, '-I', '-S', script, *args], cwd=alpha, capture_output=True, text=True)
    return p.stdout + p.stderr


def binding_checks():
    print('binding (review B B1): only the main worktree of the pinned repository, on a named branch')
    alpha, top, git = make_sandbox()
    out = _check(alpha)
    lines = [l for l in out.splitlines() if l.startswith('(')]
    check('the sandbox standing in for the real repository refuses only on (t) and (g1) today',
          {l[:3] for l in lines} == {'(t)', '(g1'} and not any(l.startswith('(w)') for l in lines), out[-500:])
    clone = Path(tempfile.mkdtemp(prefix='r016-clone-')) / 'repo'
    subprocess.run([GIT, 'clone', '-q', '--local', str(top), str(clone)], capture_output=True, check=True)
    calpha = clone / 'research' / 'alpha'
    (calpha / RUN.KLINES_A_REL).mkdir(parents=True)
    shutil.copy(alpha / RUN.KLINES_A_REL / 'SHA256SUMS', calpha / RUN.KLINES_A_REL / 'SHA256SUMS')
    (calpha / RUN.STATIC_EXCHANGE_REL).parent.mkdir(parents=True)
    shutil.copy(alpha / RUN.STATIC_EXCHANGE_REL, calpha / RUN.STATIC_EXCHANGE_REL)
    out = _check(calpha)
    check('a clone with its data copied refuses with (w)', '(w)' in out, out[-300:])
    p = subprocess.run([PY, '-I', '-S', 'r016_fetch.py'], cwd=calpha, capture_output=True, text=True)
    check('the fetcher in a clone refuses: it runs only from the real repository',
          'the fetcher runs only from the real repository' in p.stdout + p.stderr, (p.stdout + p.stderr)[-300:])
    wt = Path(tempfile.mkdtemp(prefix='r016-wt-')) / 'wt'
    git('worktree', 'add', '-q', '--detach', str(wt))
    walpha = wt / 'research' / 'alpha'
    out = _check(walpha)
    check('a linked worktree refuses with (w)', '(w)' in out, out[-300:])
    git('worktree', 'remove', '--force', str(wt))
    git('checkout', '-q', '--detach')
    out = _check(alpha)
    check('a detached HEAD in the real repository refuses with (w)', '(w) HEAD is detached' in out, out[-300:])
    git('checkout', '-q', 'alpha-discovery')
    real_line = f'016 repo-path: APPROVED 2028-01-01 {clone}\n'
    check('a repo-path line is ignored while the pinned repository exists',
          RUN.bound_repo(real_line) == RUN.REAL_REPO)


# ------------------------------------------------------------------ the gate and the real tree
def gate_checks():
    print('read gate and real-tree binding')
    check('Panel refuses the real data without the gate (before any file is opened)', raises(lambda: RUN.Panel('run')))
    check('Panel refuses the real span-(a) pull even next to a synthetic store',
          raises(lambda: RUN.Panel('run', store=tempfile.mkdtemp())))
    RUN._GUARD_PASSED = (str(RUN.HERE), 'run', 'x')
    try:
        check('a guard token without the post-started gate does not open the read', raises(lambda: RUN.Panel('run')))
    finally:
        RUN._GUARD_PASSED = None
    copy = Path(tempfile.mkdtemp())
    (copy / 'SHA256SUMS').write_text('synthetic copy\n')
    saved = RUN.registered_real_hashes
    RUN.registered_real_hashes = lambda: {D.sha256_bytes(b'synthetic copy\n')}
    try:
        check('a byte copy of a registered pull listing outside the real tree is refused (review B m2)',
              raises(lambda: RUN.Panel('run', klines_a=copy, store=tempfile.mkdtemp())))
    finally:
        RUN.registered_real_hashes = saved
    check('the real results file and the real data tree only together',
          RUN.real_tree_failures([tempfile.mkdtemp()]) != [] and RUN.real_tree_failures() == [])
    saved_g = RUN.guard_failures
    RUN.guard_failures = lambda *a, **k: ([], {})
    try:
        check('a stubbed guard_failures is refused on the real tree (review B m2)', RUN.real_tree_failures() != [])
    finally:
        RUN.guard_failures = saved_g
    is_real = RUN.in_real(RUN.HERE)
    if is_real:
        real_res = RUN.REAL_RESULTS
        before = real_res.read_bytes() if real_res.exists() else None
        head0 = RUN._git(RUN.HERE, 'rev-parse', 'HEAD').stdout
        mh = _mock_hooks({})
        mh.pop('calls')
        for label, kw in (('root = the real directory', dict(root=RUN.HERE, store=tempfile.mkdtemp())),
                          ('store under the real store', dict(root=tempfile.mkdtemp(), store=RUN.HERE / RUN.STORE_REL / 'sub'))):
            try:
                F._fetch_in(hooks=mh, now=NOW, **kw)
                msg = ''
            except SystemExit as e:
                msg = str(e)
            check(f'mock hooks and `now` with {label} are refused before anything is written (review B M1)', '(M1)' in msg, msg[:200])
        check('the real results.jsonl and HEAD are unchanged', (real_res.read_bytes() if real_res.exists() else None) == before
              and RUN._git(RUN.HERE, 'rev-parse', 'HEAD').stdout == head0)
        nf = RUN.guard_failures(RUN.HERE, 'fetch', now=NOW)[0]
        check('`now` is refused on the real repository (it never opens the date gate there)', _has(nf, '(t) a `now` override'))
    else:                                   # a clone (the commit simulation): not the real repository at all
        rw = RUN.guard_failures(RUN.HERE, 'run', real_repo=RUN.REAL_REPO)[0]
        check('outside the real repository (a clone): the binding refuses with (w)', _has(rw, '(w)'), str(rw[:2]))
    check('fetch: a missing hook is refused (no fallback)',
          raises(lambda: F._fetch_in(tempfile.mkdtemp(), tempfile.mkdtemp(), {'get': None}, NOW), (SystemExit,)))
    check('fetch: the real hooks refuse outside the real guarded fetch', raises(lambda: F._get('x'), (SystemExit,)))
    rr = RUN.guard_failures(RUN.HERE, 'run', real_repo=RUN.REAL_REPO, anchor=RUN.ANCHOR)[0]
    check('the real guard (read-only call) refuses today on the date (t); (w) appears only outside the real repository',
          (_has(rr, '(t)') or dt.date.today() >= R.READ_NOT_BEFORE) and _has(rr, '(w)') == (not is_real), str(rr[:3]))
    root, top, git = _repo('sub')
    git('add', '.')
    git('commit', '-q', '-m', 'init')
    saved_r = RUN.RESULTS
    RUN.RESULTS = root / 'results.jsonl'
    saved_h = RUN.HOOKS['commit']
    try:
        RUN.record({'registration': '016-x.md', 'mode': 'probe'}, 'probe row')
        log = git('log', '-1', '--format=%an').stdout.strip()
        check('record() commits the row alone, as the runner identity, and finds it in HEAD:./results.jsonl',
              '"probe"' in git('show', 'HEAD:' + _rel(root, top, 'results.jsonl')).stdout and log == RUN.RUNNER_NAME)
        RUN.HOOKS['commit'] = lambda msg, results=None: None
        check('record() refuses when the hook did not commit', raises(lambda: RUN.record({'registration': '016-x.md',
                                                                                          'mode': 'p2'}, 'x')))
    finally:
        RUN.HOOKS['commit'] = saved_h
        RUN.RESULTS = saved_r


# ------------------------------------------------------------------ the panel's own cuts (review B m4)
def _mini_panel(extra_a=None, june_with_july_row=False):
    """A tiny synthetic pull (BTC 2026-06) and store (BTC 2026-07) for the panel's cut checks."""
    base = Path(tempfile.mkdtemp())
    a, f = base / 'a', base / 'f'
    jun = [(R.day(dt.date(2026, 6, 1)) + i, 100.0 + i, 5e7, 10) for i in range(30)]
    if june_with_july_row:
        jun.append((R.day(dt.date(2026, 7, 1)), 999.0, 5e7, 10))
    files = {'klines/BTCUSDT/1d/BTCUSDT-1d-2026-06.zip': _zip('x.csv', kline_csv(jun))}
    if extra_a:
        files.update(extra_a)
    sums = []
    for rel, blob in files.items():
        (a / rel).parent.mkdir(parents=True, exist_ok=True)
        (a / rel).write_bytes(blob)
        sums.append(f'{D.sha256_bytes(blob)}  {rel}')
    (a / 'SHA256SUMS').write_text('\n'.join(sums) + '\n')
    jul = [(R.day(dt.date(2026, 7, 1)) + i, 200.0 + i, 5e7, 10) for i in range(31)]
    mf = {'klines/BTCUSDT/1d/BTCUSDT-1d-2026-07.zip': _zip('y.csv', kline_csv(jul)),
          'fapi_exchangeInfo.json': b'{"symbols": []}', 'hl_presence.json': b'{}'}
    for rel, blob in mf.items():
        (f / rel).parent.mkdir(parents=True, exist_ok=True)
        (f / rel).write_bytes(blob)
    man = {rel: D.sha256_bytes(blob) for rel, blob in mf.items()}
    (f / 'manifest.json').write_text(json.dumps({'files': man, 'digest': RUN.manifest_digest(man)}))
    static = base / 'static.json'
    static.write_text('{"symbols": []}')
    return RUN.Panel('run', klines_a=a, store=f), static


def panel_checks():
    print('panel: the span-(a) month cap, the span-(a) row cut, the hashes of the bytes used')
    p, st = _mini_panel(extra_a={'klines/ETHUSDT/1d/ETHUSDT-1d-2026-07.zip': _zip('z.csv', kline_csv(
        [(R.day(dt.date(2026, 7, 1)), 5.0, 5e7, 10)]))})
    try:
        p.load(static_exchange=st)
        msg = ''
    except R.Refused as e:
        msg = str(e)
    check('a pull listing a 2026-07 file is refused (the month cap)', 'holds a month after 2026-06' in msg, msg[:120])
    p, st = _mini_panel(june_with_july_row=True)
    try:
        v = p.load(static_exchange=st)[0]['BTCUSDT'].rows[R.day(dt.date(2026, 7, 1))][0]
    except R.Refused as e:
        v = f'refused: {e}'
    check('a 2026-06 file holding a 2026-07 row: the row is cut, the forward value is used (the row cut)', v == 200.0, str(v))
    p, st = _mini_panel()
    p.table = {f'{RUN.KLINES_A_REL}/SHA256SUMS': '0' * 64, RUN.STATIC_EXCHANGE_REL: '0' * 64}
    check('the panel re-hashes the SHA256SUMS and exchangeInfo bytes it parses against the table (review B m8)',
          raises(lambda: p.load(static_exchange=st)))


# ------------------------------------------------------------------ the fetcher against mocks
def _mock_hooks(objects, page=3, hl=None, fail=(), empty=(), badsum=(), witness=None, ex=None):
    """S3 ListObjects v1 with `page` keys per page and an EXCLUSIVE marker; data.binance.vision GET; HL funding with
    INCLUSIVE start and end; the witness (keys, server date)."""
    calls = {'list': 0, 'get': []}
    keys = sorted(objects)

    def s3_list(prefix, delimiter=None):
        items = []
        for k in keys:
            if not k.startswith(prefix):
                continue
            rest = k[len(prefix):]
            if delimiter and delimiter in rest:
                p = prefix + rest.split(delimiter)[0] + delimiter
                if not items or items[-1] != ('pre', p):
                    items.append(('pre', p))
            else:
                items.append(('obj', k))
        out, marker = [], ''
        while True:
            calls['list'] += 1
            pagev = [x for x in items if x[1] > marker][:page]
            out += pagev
            if len(pagev) < page:
                return out
            marker = pagev[-1][1]

    def get(key):
        calls['get'].append(key)
        if key.endswith('.CHECKSUM'):
            k = key[:-9]
            h = D.sha256_bytes(objects[k])
            return (('0' * 64) if k in badsum else h).encode() + b'  ' + k.rsplit('/', 1)[1].encode()
        if key in fail:
            raise F.FetchError(f'mock failure {key}')
        if key in empty:
            return b''
        return objects[key]

    hl = hl or {'BTC': {'first': F.ms(dt.date(2023, 6, 1)), 'last': None}}

    def hl_funding(coin, start, end):
        h = hl[coin]
        out = []
        stop = h['last'] if h['last'] is not None else F.ms(dt.date(2028, 1, 1)) + 10 * 3600000
        t = max(h['first'], start - (start - h['first']) % 3600000)
        while t <= min(end, stop) and len(out) < 500:
            if t >= start:
                out.append({'coin': coin, 'time': t, 'fundingRate': '0.00001'})
            t += 3600000
        return out
    wit = witness or ([k for k in keys if k.startswith(F.KLINES_BASE + 'BTCUSDT/1d/')],
                      dt.datetime(2028, 1, 9, tzinfo=dt.UTC))
    return {'witness': lambda: wit, 's3_list': s3_list, 'get': get,
            'exchange_info': lambda: ex or {'symbols': [{'symbol': 'BTCUSDT', 'contractType': 'PERPETUAL',
                                                          'underlyingType': 'COIN'}]},
            'hl_meta': lambda: [{'name': c} for c in hl], 'hl_funding': hl_funding, 'calls': calls}


def _month_zip(sym, mo, dead=False):
    y, m = map(int, mo.split('-'))
    first = dt.date(y, m, 1)
    nxt = dt.date(y + (m == 12), m % 12 + 1, 1)
    rows = [(R.day(first) + i, 100.0 + i, 5e7, 0 if dead else 10) for i in range((nxt - first).days)]
    return _zip(f'{sym}-1d-{mo}.csv', kline_csv(rows))


def _fund_zip(sym, mo):
    y, m = map(int, mo.split('-'))
    t = int(dt.datetime(y, m, 1, 8, tzinfo=dt.UTC).timestamp() * 1000)
    return _zip(f'{sym}-fundingRate-{mo}.csv', f'calc_time,funding_interval_hours,last_funding_rate\n{t},8,0.0001\n')


def _objs(syms=('BTCUSDT', 'ETHUSDT'), k_months=('2026-05', '2028-02'), f_months=('2025-01', '2028-02')):
    objs = {}
    for sym in syms:
        for mo in F.months(*k_months):
            objs[f'{F.KLINES_BASE}{sym}/1d/{sym}-1d-{mo}.zip'] = _month_zip(sym, mo)
        for mo in F.months(*f_months):
            objs[f'{F.FUNDING_BASE}{sym}/{sym}-fundingRate-{mo}.zip'] = _fund_zip(sym, mo)
    return objs


def fetch_checks():
    print('fetch (mocks): witness, paging, months, checksums, resume, coverage rules, persistence, the HL probe')
    objs = _objs()
    hk = _mock_hooks(objs)
    want, later = F.plan(hk, F.KLINES_BASE, '1d/', *RUN.FWD_MONTHS)
    check('plan: exactly the 18 forward months per symbol; later months counted, never planned; <= 2026-06 not planned',
          [m for m, _ in want['BTCUSDT']] == F.months('2026-07', '2027-12') and later == 4)
    full = [k for kind, k in hk['s3_list'](F.KLINES_BASE + 'BTCUSDT/1d/') if kind == 'obj']
    check('S3 paging forward with an exclusive marker: every key once (page size 3)',
          full == sorted(k for k in objs if k.startswith(F.KLINES_BASE + 'BTCUSDT/1d/')) and len(full) == len(set(full)))
    early = _mock_hooks(objs, witness=([k for k in objs if '2026-0' in k and 'BTCUSDT/1d' in k],
                                       dt.datetime(2028, 1, 9, tzinfo=dt.UTC)))
    check('witness: the archive must list BTCUSDT 2027-12', F.witness_failures(early) != [])
    late_clock = _mock_hooks(objs, witness=([k for k in objs if 'BTCUSDT/1d' in k], dt.datetime(2027, 12, 31, tzinfo=dt.UTC)))
    check('witness: the server date must be >= 2028-01-08', F.witness_failures(late_clock) != [] and F.witness_failures(hk) == [])
    with tempfile.TemporaryDirectory() as d:
        store = Path(d)
        key = f'{F.KLINES_BASE}BTCUSDT/1d/BTCUSDT-1d-2026-07.zip'
        bad = _mock_hooks(objs, badsum={key})
        check('a checksum mismatch raises and writes nothing',
              raises(lambda: F.fetch_verified(bad, key, store / 'x.zip'), (F.FetchError,)) and not (store / 'x.zip').exists())
        em = _mock_hooks(objs, empty={key})
        check('an empty answer raises and writes nothing',
              raises(lambda: F.fetch_verified(em, key, store / 'x.zip'), (F.FetchError,)) and not (store / 'x.zip').exists())
        fl = _mock_hooks(objs, fail={key})
        _, files, errors, _l = F.pull(fl, store, F.KLINES_BASE, '1d/', *RUN.FWD_MONTHS, 'klines')
        check('a failed download is a keyed error and is never written',
              'klines_error:BTCUSDT:2026-07' in errors and not (store / 'klines/BTCUSDT/1d/BTCUSDT-1d-2026-07.zip').exists()
              and len(files) == 35)
        tam = store / 'klines/BTCUSDT/1d/BTCUSDT-1d-2026-08.zip'
        tam.write_bytes(b'tampered')
        _, files2, errors2, _l = F.pull(hk, store, F.KLINES_BASE, '1d/', *RUN.FWD_MONTHS, 'klines')
        check('resume: a stored file is re-checked against .CHECKSUM and re-fetched when it differs (review B m5d)',
              not errors2 and len(files2) == 36
              and D.sha256_bytes(tam.read_bytes()) == D.sha256_bytes(objs[f'{F.KLINES_BASE}BTCUSDT/1d/BTCUSDT-1d-2026-08.zip']))
        _, ff, ferr, _l = F.pull(hk, store, F.FUNDING_BASE, '', *RUN.FUNDING_MONTHS, 'fundingRate')
        allf = {**files2, **ff}
        root = Path(tempfile.mkdtemp())
        check('coverage: BTC every forward day, no gap, funding for every traded month', F.coverage(root, store, allf, {}) == {},
              str(F.coverage(root, store, allf, {}))[:300])
        no_eth = {k: v for k, v in allf.items() if not ('ETHUSDT' in k and '2027-03' in k and k.startswith('klines'))}
        check('coverage: an interior month missing is a keyed refusal', 'klines_gap:ETHUSDT' in F.coverage(root, store, no_eth, {}))
        no_dec = {k: v for k, v in allf.items() if not ('ETHUSDT-1d-2027-12' in k)}
        check('coverage: traded at the end of 2027-11 but no 2027-12 file (the second seam; review B m5b)',
              'klines_seam:ETHUSDT:2027-12' in F.coverage(root, store, no_dec, {}))
        no_btc = {k: v for k, v in allf.items() if 'BTCUSDT-1d-2027-12' not in k}
        check('coverage: BTC missing its last month refuses (never persistable)', 'btc_months' in F.coverage(root, store, no_btc, {}))
        no_fund = {k: v for k, v in allf.items() if not ('ETHUSDT-fundingRate-2027-05' in k)}
        check('coverage: a traded kline month without a funding file refuses (review B m5a)',
              'funding_missing:ETHUSDT:2027-05' in F.coverage(root, store, no_fund, {}))
        ex = {'symbols': [{'symbol': 'NEWUSDT', 'contractType': 'PERPETUAL', 'underlyingType': 'COIN', 'status': 'TRADING',
                           'onboardDate': F.ms(dt.date(2027, 2, 1))}]}
        check('coverage: a live COIN perp of the exchangeInfo with no forward klines refuses (review B m5c)',
              'listing_missing:NEWUSDT' in F.coverage(root, store, allf, {}, ex))
        a_dir = root / RUN.KLINES_A_REL
        rel = 'klines/SOLUSDT/1d/SOLUSDT-1d-2026-06.zip'
        blob = _month_zip('SOLUSDT', '2026-06')
        (a_dir / rel).parent.mkdir(parents=True)
        (a_dir / rel).write_bytes(blob)
        cov = F.coverage(root, store, allf, {rel: D.sha256_bytes(blob)})
        check('coverage: traded at the end of 2026-06 in the pinned pull but no 2026-07 file (the seam)',
              'klines_seam:SOLUSDT' in cov, str(cov)[:200])
    gap, t0 = F.PERSIST_GAP_S, 1_900_000_000
    att = {1: {'refusals': {'klines_gap:ETHUSDT', 'klines_error:BTCUSDT:2026-07'}, 'time': t0},
           2: {'refusals': {'klines_gap:ETHUSDT', 'klines_error:BTCUSDT:2026-07'}, 'time': t0 + gap + 10}}
    blk, per = F.apply_persistence({'klines_gap:ETHUSDT': 'x', 'klines_error:BTCUSDT:2026-07': 'y'}, att,
                                   t0 + 2 * gap + 20, not_before_s=0)
    check('persistence: 3 attempts >= 24 h apart make a key unavailable; a BTCUSDT key never',
          'klines_gap:ETHUSDT' in per and 'klines_error:BTCUSDT:2026-07' in blk)
    blk2, per2 = F.apply_persistence({'klines_gap:ETHUSDT': 'x'}, att, t0 + 2 * gap + 20, not_before_s=t0 + 1)
    check('persistence: attempts committed before 2028-01-08 never count (review B m1)', 'klines_gap:ETHUSDT' in blk2)
    end_ms = F.ms(dt.date(2028, 1, 1))
    hl = {'AAA': {'first': F.ms(dt.date(2023, 6, 1)), 'last': None},
          'BBB': {'first': F.ms(dt.date(2024, 2, 1)), 'last': F.ms(dt.date(2026, 9, 15))},
          'CCC': {'first': F.ms(dt.date(2026, 11, 1)), 'last': F.ms(dt.date(2026, 11, 1))}}
    pres, err = F.hl_presence(_mock_hooks({}, hl=hl), ['AAA', 'BBB', 'CCC'], end_ms)
    check('HL: alive at the end -> last None; a coin that stopped -> its last funding month start (bisection)',
          pres['AAA']['last'] is None and pres['BBB']['last'] == F.ms(dt.date(2026, 9, 1)) and not err)
    check('HL: a row exactly at a window end (s + 2 d) is outside [s, s + 2 d - 1 ms] (inclusive endTime handled)',
          pres['CCC']['last'] == F.ms(dt.date(2026, 11, 1)))


# ------------------------------------------------------------------ end to end on a synthetic tree (both layouts)
def _synthetic_tree(root, seed=11):
    syms = sim.make_world({'name': 'e2e', 'drift': 'flat', 'kappa': 0.25, 'half_life': 60}, seed)
    a_dir = root / RUN.KLINES_A_REL
    objs, sums = {}, []
    for name, s in syms.items():
        by_m = {}
        for d_, (c, q, n) in s.rows.items():
            by_m.setdefault(R.month_of(d_), []).append((d_, c, q, n))
        for mo, rows in sorted(by_m.items()):
            blob = _zip(f'{name}-1d-{mo}.csv', kline_csv(sorted(rows)))
            if mo <= RUN.A_MONTH_LAST:
                rel = f'klines/{name}/1d/{name}-1d-{mo}.zip'
                (a_dir / rel).parent.mkdir(parents=True, exist_ok=True)
                (a_dir / rel).write_bytes(blob)
                sums.append(f'{D.sha256_bytes(blob)}  {rel}')
            else:
                objs[f'{F.KLINES_BASE}{name}/1d/{name}-1d-{mo}.zip'] = blob
        for mo in F.months('2025-03', '2027-12'):
            objs[f'{F.FUNDING_BASE}{name}/{name}-fundingRate-{mo}.zip'] = _fund_zip(name, mo)
    (a_dir / 'SHA256SUMS').write_text('\n'.join(sorted(sums, key=lambda x: x.split('  ')[1])) + '\n')
    (root / RUN.STATIC_EXCHANGE_REL).parent.mkdir(parents=True, exist_ok=True)
    (root / RUN.STATIC_EXCHANGE_REL).write_text(json.dumps({'symbols': []}))
    return objs


def e2e_checks(layout):
    print(f'end to end ({layout} layout): guarded fetch from mocks -> approval -> gated panel -> compute -> recheck')
    root, top, git = _repo(layout)
    objs = _synthetic_tree(root)
    data = (f'{RUN.KLINES_A_REL}/SHA256SUMS', RUN.STATIC_EXCHANGE_REL)
    rows = '\n'.join(f'| `{f}` | `{RUN.sha256(root / f)}` |' for f in (*CODE, *data))
    (root / REG).parent.mkdir(exist_ok=True)
    (root / REG).write_text(f'# 016\n\n**Status: REGISTERED 2026-10-14**\n\n| file | sha256 |\n|---|---|\n{rows}\n')
    git('add', '.')
    git('commit', '-q', '-m', 'registered')
    hooks = _mock_hooks(objs, page=50)
    hooks.pop('calls')
    kw = dict(code=CODE, data=data, reg_rel=REG)
    store = root / RUN.STORE_REL
    check('fetch refuses before 2028-01-08', raises(lambda: F._fetch_in(root, store, hooks, EARLY, **kw), (SystemExit,)))
    F._fetch_in(root, store, hooks, NOW, **kw)
    head = git('show', 'HEAD:' + _rel(root, top, 'results.jsonl')).stdout
    check('the fetch commits fetch_started, an attempt row and the fetched row (verified in HEAD), as the runner',
          head.count('"fetch_started"') == 1 and '"fetch_attempt"' in head and '"fetched"' in head
          and git('log', '-1', '--format=%an').stdout.strip() == RUN.RUNNER_NAME)
    check('the fetch runs once', raises(lambda: F._fetch_in(root, store, hooks, NOW, **kw), (SystemExit,)))
    fr = RUN.guard_failures(root, 'run', now=NOW, **kw)[0]
    check("the read needs the approval line (h')", _has(fr, "(h')") and len(fr) == 1, str(fr))
    fline = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"fetched"' in l][0]
    (root / REG).write_text((root / REG).read_text() + f'\n016 read: APPROVED 2028-01-11 row {RUN.line_sha(fline)}\n')
    git('commit', '-q', '-am', 'approval')
    gfo = RUN.guard_failures(root, 'run', now=NOW, **kw)[0]
    check('after the approval the read gate opens', gfo == [], str(gfo))
    table = RUN.guard_failures(root, 'run', now=NOW, **kw)[1]['table']
    panel = RUN.Panel('run', klines_a=root / RUN.KLINES_A_REL, store=store, table=table)
    syms, funding, non_coin, hl, counts = panel.load(static_exchange=root / RUN.STATIC_EXCHANGE_REL)
    check('the panel joins the pinned pull (<= 2026-06) and the fetch (2026-07 -> 2027-12) without overlap',
          counts['symbols'] == 24 and min(syms[R.BTC].rows) == R.day(sim.SIM_START)
          and max(syms[R.BTC].rows) == R.day(R.END) and len(funding) == 24, str(counts))
    dec, rep, series, bk = RUN.compute(syms, funding, non_coin, hl, placebo_seeds=2)
    row = {'outcome': dec['outcome'], 'route': dec['route'], 'h1_t': dec['h1']['pooled']['t'],
           'h1_t_forward': dec['h1']['forward']['t'], 'series': series}
    fails, _ = S.recheck(json.loads(json.dumps(row)))
    check('compute runs end to end on the synthetic trend world; the decision recomputes from the stored records',
          fails == [] and dec['h1']['pooled']['G'] == 33, f"{dec['outcome']} tA {dec['h1']['pooled']['t']:.2f} {fails}")
    check('the reported blocks are present (strata, factor alphas, raw Sharpe, variants, tail, age, placebo)',
          {'strata', 'factor_alpha_constant', 'raw_sharpe_vs_paper', 'costs', 'long_short', 'tail_gross_x_k25',
           'age_excluded_by_month', 'placebo_shifted_signals'} <= set(rep))
    tamper = store / 'klines' / 'BTCUSDT' / '1d' / 'BTCUSDT-1d-2027-01.zip'
    blob = bytearray(tamper.read_bytes())
    blob[len(blob) // 2] ^= 0xFF
    tamper.write_bytes(bytes(blob))
    check('a forward file altered after the fetch: (g3) refuses and the panel refuses it',
          _has(RUN.guard_failures(root, 'run', now=NOW, **kw)[0], '(g3)')
          and raises(lambda: RUN.Panel('run', klines_a=root / RUN.KLINES_A_REL, store=store, table=table).load(
              static_exchange=root / RUN.STATIC_EXCHANGE_REL)))


# ------------------------------------------------------------------ the simulator
def sim_checks():
    print('simulator: determinism, the timing statistic in trend-free and leverage-bull worlds')
    a = sim.run_draw({'name': 'x', 'drift': 'flat'}, 5)
    check('a draw is a deterministic function of (spec, seed)', a == sim.run_draw({'name': 'x', 'drift': 'flat'}, 5))
    w = sim.make_world({'name': 'x', 'drift': 'flat', 'df': None, 'garch': (0, 0, 0)}, 2)
    btc = w[R.BTC]
    r = [btc.tclose[i] / btc.tclose[i - 1] - 1 for i in range(1, len(btc.tclose))]
    ac = sum(x * y for x, y in zip(r, r[1:])) / sum(x * x for x in r)
    check('trend-free world: BTC daily returns have ~zero mean and ~zero autocorrelation',
          abs(statistics.fmean(r)) < 0.003 and abs(ac) < 0.08, f'mean {statistics.fmean(r):.5f} ac {ac:.3f}')
    b = sim.run_draw({'name': 'x', 'drift': 'const', 'annual': 0.8, 'df': 3, 'garch': sim.GJR}, 3)
    check('a leverage-bull draw records tA, tA on (c), tA on (a), T', {'tA', 'tA_c', 'tA_a', 'T'} <= set(b))
    code = ('import sys, json; sys.path.append("."); from ct16 import sim; '
            'print(json.dumps(sim.run_draw({"name": "x", "drift": "const", "annual": 0.5}, 9)))')
    outs = {subprocess.run([PY, '-I', '-S', '-c', code], cwd=HERE, capture_output=True, text=True).stdout for _ in range(3)}
    check('a draw is bit-identical across processes (string hashing differs per process; no set order reaches a sum)',
          len(outs) == 1 and next(iter(outs)).startswith('{'), str(outs)[:200])
    cache = Path(tempfile.mkdtemp())
    w = [{'name': 'tiny flat', 'drift': 'flat'}]
    k1, k2, k3 = {}, {}, {}
    sim.run_set(w, 2, None, 5, keep=k1)
    sim.run_set(w, 2, None, 5, keep=k2, cache_dir=cache)
    files = sorted(cache.glob('*.json'))
    sim.run_set(w, 2, None, 5, keep=k3, cache_dir=cache)
    check('the power run is resumable: a cached world is read back, identical to an uninterrupted run',
          k1 == k2 == k3 and len(files) == 1 and sorted(cache.glob('*.json')) == files)
    sim.run_set(w, 2, None, 6, keep=k3, cache_dir=cache)
    check('a different seed is a different world file (never reused)', len(list(cache.glob('*.json'))) == 2)
    rq = random.Random(1)
    q = sim.quantile_se([rq.gauss(0, 1) for _ in range(3000)], 0.95)
    check('the Monte Carlo SE of a 95th percentile at 3,000 draws is ~0.04', 0.02 < q < 0.07, f'{q:.3f}')


def real_file_checks():
    print('the real registration and its hash table (does not go stale at commit, REGISTERED or approval)')
    reg = HERE / 'registrations' / RUN.REG_NAME
    check('the registration exists', reg.exists())
    text = reg.read_text()
    m = RUN.STATUS.search(text)
    check('the status line is DRAFT or REGISTERED', m is not None and m.group(1) in ('DRAFT', 'REGISTERED'))
    has_fetched = any(r.get('mode') == 'fetched' for _, r in RUN.result_rows(HERE)[0])
    check('no approval line exists before a fetched row exists (review B m9)', has_fetched or not RUN.APPROVAL.findall(text))
    tbl = RUN.HASH_ROW.findall(text)
    names = [f for f, _ in tbl]
    check('the hash table lists every guarded code and data file exactly once',
          sorted(names) == sorted((*RUN.GUARDED_CODE, *RUN.GUARDED_DATA)))
    tab = dict(tbl)
    stale = [f for f in RUN.GUARDED_CODE if RUN.sha256(HERE / f) != tab.get(f)]
    check('the code hashes in the table equal the files on disk', not stale, str(stale))
    present = [f for f in RUN.GUARDED_DATA if (HERE / f).exists()]
    check('the data hashes equal the files on disk (where the gitignored data exist on this machine)',
          all(RUN.sha256(HERE / f) == tab.get(f) for f in present))
    check('the calendars: (a) 2025-03-20 -> 2026-06-30, (c) 2026-07-01 -> 2027-12-31, gate 2028-01-08 = END + 8 days',
          R.SPAN_A == (dt.date(2025, 3, 20), dt.date(2026, 6, 30)) and R.SPAN_C == (dt.date(2026, 7, 1), dt.date(2027, 12, 31))
          and R.READ_NOT_BEFORE == dt.date(2028, 1, 8) == R.END + dt.timedelta(days=8))


SECTIONS = {
    'signal': lambda: signal_checks(), 'universe': lambda: universe_checks(), 'book': lambda: book_checks(),
    'stats': lambda: stats_checks(), 'parsers': lambda: parser_checks(),
    'guard': lambda: [guard_checks(l) for l in ('top', 'sub')], 'restart': lambda: (prose_checks(), restart_checks()),
    'history': lambda: history_checks(), 'bypass': lambda: bypass_checks(), 'env': lambda: env_checks(),
    'binding': lambda: binding_checks(), 'gate': lambda: gate_checks(), 'panel': lambda: panel_checks(),
    'fetch': lambda: fetch_checks(), 'e2e': lambda: [e2e_checks(l) for l in ('top', 'sub')], 'sim': lambda: sim_checks(),
    'real': lambda: real_file_checks(),
}


def main():
    only = sys.argv[sys.argv.index('--only') + 1].split(',') if '--only' in sys.argv else list(SECTIONS)
    for name in only:
        SECTIONS[name]()
    print(f'all checks passed ({N_CHECKS[0]})')


if __name__ == '__main__':
    main()
