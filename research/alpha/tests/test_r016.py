"""Registration 016 (crypto trend, verbatim) — mechanics, statistics, parsers, the fetch, the read gate and the guard,
on SYNTHETIC data only. No real kline, funding or price is read: the real-file checks read the registration, the
hash table and file hashes, and call the real guard read-only.

    research/copytrade/.venv/bin/python -I tests/test_r016.py          (from research/alpha)
"""
import ast
import datetime as dt
import io
import json
import math
import os
import random
import statistics
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))
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
    f1, f3 = R.factor_series(syms, a, b, ufn)
    btc = syms[R.BTC]
    check('F1 is BTC close-to-close', close(f1[5], btc.rows[a + 5][0] / btc.rows[a + 4][0] - 1))
    vm = R.run_book(syms, a, b, ufn, cost=0.0, mode='always')
    check('F3 is the same book with Pos = 1, gross of costs', vm['ret'] == f3 and vm['gross'][10] > b0['gross'][10] * 0.99)


# ------------------------------------------------------------------ statistics
def stats_checks():
    print('statistics: t and noncentral t, CR1 OLS, cells, H1, H2, the decision, the recheck')
    check('t_crit: t_32(0.95) 1.694, t_14(0.95) 1.761', close(S.t_crit(32), 1.6939, 2e-4) and close(S.t_crit(14), 1.7613, 2e-4))
    check('noncentral t at delta 0 is Student t', close(S.nct_quantile(0.05, 20, 0.0), -S.t_crit(20), 1e-3))
    rng = random.Random(4)
    hits = sum(1 for _ in range(20000) if (rng.gauss(0, 1) + 2.6) / math.sqrt(rng.gammavariate(16, 2) / 32) < 0.9545)
    check('nct 5% quantile (df 32, delta 1.57 sqrt(33/12)) = 0.95 matches Monte Carlo', abs(hits / 20000 - 0.05) < 0.006,
          f'{hits / 20000:.4f}')
    x = [rng.gauss(0, 1) for _ in range(600)]
    y = [0.2 + 1.5 * v + rng.gauss(0, 1) for v in x]
    cl = [i // 20 for i in range(600)]
    f = S.ols_cr1(y, [[1.0, v] for v in x], cl)
    mx, my = statistics.fmean(x), statistics.fmean(y)
    bh = sum((a - mx) * (b - my) for a, b in zip(x, y)) / sum((a - mx) ** 2 for a in x)
    check('OLS slope and intercept equal the closed form', close(f['coef'][1], bh, 1e-9) and close(f['coef'][0], my - bh * mx, 1e-9))
    e = [b - f['coef'][0] - f['coef'][1] * a for a, b in zip(x, y)]
    meat = {}
    for a, ee, g in zip(x, e, cl):
        u = meat.setdefault(g, [0.0, 0.0])
        u[0] += ee
        u[1] += a * ee
    n, G = 600, 30
    sxx = sum((a - mx) ** 2 for a in x)
    inv = [[sum(a * a for a in x) / (n * sxx), -mx / sxx], [-mx / sxx, 1 / sxx]]
    m00 = sum(u[0] ** 2 for u in meat.values())
    m01 = sum(u[0] * u[1] for u in meat.values())
    m11 = sum(u[1] ** 2 for u in meat.values())
    v00 = inv[0][0] ** 2 * m00 + 2 * inv[0][0] * inv[0][1] * m01 + inv[0][1] ** 2 * m11
    v00 *= G / (G - 1) * (n - 1) / (n - 2)
    check('CR1 standard error of the intercept equals the sandwich by hand', close(f['se'][0], math.sqrt(v00), 1e-10))
    days = list(range(R.day(R.START), R.day(R.END) + 1))
    grp = S.cell_groups(days)
    check('six half-year cells over the span; the 103-day stub stands (>= 60 days)', max(grp.values()) == 5
          and sum(1 for d in days if grp[d] == 0) == 103)
    check('G = 33 month clusters (the March 2025 stub joins April)', len({R.cluster_of(d) for d in days}) == 33)
    short = days[:40] + days[200:400]
    check('a cell under 60 days joins its neighbour', len(set(S.cell_groups(short).values())) <= 2)
    # alpha recovery with cell-specific slopes
    f1 = [rng.gauss(0, 0.03) for _ in days]
    f2 = [0.8 * a + rng.gauss(0, 0.02) for a in f1]
    f3 = [0.3 * a + rng.gauss(0, 0.01) for a in f2]
    cells = [grp[d] for d in days]
    yv = [0.0004 + (0.1 + 0.05 * c) * a + 0.05 * b - 0.2 * cc + rng.gauss(0, 0.004)
          for a, b, cc, c in zip(f1, f2, f3, cells)]
    fit = S.alpha_fit(days, yv, [f1, f2, f3], per_cell=True)
    check('alpha_fit with half-year cells recovers a known alpha and cell slopes (k = 18)', fit['k'] == 18 and abs(fit['alpha'] - 0.0004) < 0.0002
          and abs(fit['slopes'][0] - 0.1) < 0.05 and abs(fit['slopes'][15] - 0.35) < 0.05, f"{fit['alpha']:.6f}")
    check('the H1 floor is t_{G-1-k}(0.95): t_14 for k 18, t_29 for the primary k 3',
          close(S.h1_crit(33, 18, cal=0.0)[0], S.t_crit(14), 1e-12) and close(S.h1_crit(33, 3, cal=0.0)[0], S.t_crit(29), 1e-12))
    yc = [0.0004 + 0.2 * a + 0.05 * b - 0.2 * cc + rng.gauss(0, 0.004) for a, b, cc in zip(f1, f2, f3)]
    fc = S.alpha_fit(days, yc, [f1, f2, f3])
    check('the primary market model (D14): constant slopes on three factors, k = 3, G = 33',
          fc['k'] == 3 and fc['G'] == 33 and abs(fc['slopes'][0] - 0.2) < 0.03 and not S.PRIMARY_PER_CELL)
    check('a calibrated critical value never lowers the floor', S.h1_crit(33, 18, cal=1.0)[0] == S.t_crit(14)
          and S.h1_crit(33, 18, cal=2.5)[0] == 2.5)
    h = S.h1(days, yv, [f1, f2, f3], cal=0.0)
    check('H1 passes on a strong alpha with a positive forward alpha', h['pass'] and h['pass_forward_sign'])
    yneg = [v + (-0.0008 if d >= R.day(R.SPAN_C[0]) else 0.003) for v, d in zip(yv, days)]
    h2_ = S.h1(days, yneg, [f1, f2, f3], cal=0.0)
    check('H1 refuses when the pooled t passes but the forward alpha is <= 0 (D18)', h2_['pass_t'] and not h2_['pass'],
          f"t {h2_['pooled']['t']:.2f}")
    # H2
    yy = [0.0 if i % 2 else 0.001 for i in range(len(days))]
    yy = [v + rng.gauss(0, 0.01) for v in yy]
    hh = S.h2(days, yy)
    check('H2: T = sqrt(G) mean / sd of monthly returns; rejects iff T < crit',
          close(hh['T'], math.sqrt(33) * hh['mean_month'] / hh['sd_month']) and hh['reject'] == (hh['T'] < hh['crit']))
    check('H2 critical: min(noncentral-t q05 0.954, CAL_H2 0.117) = 0.117 (SR-hat below ~0.07 rejects 1.57)',
          abs(hh['nct_q05'] - 0.9545) < 0.01 and hh['crit'] == min(hh['nct_q05'], S.CAL_H2) == 0.117)
    check('H1 critical: max(t_29(0.95) 1.699, CAL_H1 1.731) = 1.731', S.h1_crit(33, 3) == (1.731, S.t_crit(29)))
    check('decide: the four outcomes', S.decide(True, False) == 'alive_as_published' and S.decide(True, True) == 'alive_decayed'
          and S.decide(False, True) == 'decayed_not_shown' and S.decide(False, False) == 'undecided')
    e1 = S.economic(0.05, 0.03, 3.0)
    check('economic bar: min(raw, adjusted) x k x (1 - 1/t^2) vs 4%/yr',
          close(e1['shrunk_at_k'], 2.8 * 0.03 * (1 - 1 / 9)) and e1['clears'])
    check('economic bar fails at a small shrunk value', not S.economic(0.01, 0.02, 1.9)['clears'])
    check('routes: no economic bar without an H1 pass; a fail never routes to paper',
          S.route('undecided', None) == 'not_demonstrated' and S.route('decayed_not_shown', None) == 'close'
          and S.route('alive_decayed', S.economic(0.1, 0.1, 4.0)) == 'paper_reduced_size')
    series = {'days': [R.date_of(d).isoformat() for d in days], 'net': yv, 'F1': f1, 'F2': f2, 'F3': f3, 'deploy': yv}
    dec = S.decision_from_series(days, yv, [f1, f2, f3], days, yv)
    row = {'series': series, 'outcome': dec['outcome'], 'route': dec['route'], 'h1_t': dec['h1']['pooled']['t'],
           'h2_T': dec['h2']['T']}
    fails, _ = S.recheck(row)
    check('recheck: a genuine row recomputes', fails == [], str(fails))
    forged = dict(row, outcome='alive_as_published' if row['outcome'] != 'alive_as_published' else 'undecided')
    check('recheck: a forged outcome is caught', S.recheck(forged)[0] != [])
    tampered = json.loads(json.dumps(row))
    tampered['series']['net'] = [v + 0.001 for v in yv]
    check('recheck: a tampered series under the stored statistics is caught', S.recheck(tampered)[0] != [])


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


def _git_fn(cwd):
    def git(*a, check_=True):
        env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_')}
        return subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=cwd, capture_output=True,
                              text=True, check=check_, env=env)
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


def guard_checks(layout):
    print(f'guard ({layout} layout, throwaway git repository)')
    root, top, git = _repo(layout)
    rel = lambda p: str((root / p).relative_to(top))
    _write_reg(root, status='DRAFT')
    git('add', '.gitignore', rel('ct16'), rel('r016_run.py'), rel('results.jsonl'))
    git('commit', '-q', '-m', 'code')
    f0 = _gf(root)
    check('DRAFT and untracked registration: (s) and (a) refuse', _has(f0, '(s)') and _has(f0, '(a)'))
    _write_reg(root)
    git('add', rel(REG))
    git('commit', '-q', '-m', 'register')
    check('REGISTERED, committed, clean, after 2028-01-08: the fetch gate opens', _gf(root) == [], str(_gf(root)))
    check('(t) the date gate refuses before 2028-01-08 (UTC)', _has(_gf(root, now=EARLY), '(t)'))
    check('the real clock today is before the gate', _has(_gf(root, now=None), '(t)') or dt.date.today() >= R.READ_NOT_BEFORE)
    (root / REG).write_text((root / REG).read_text() + 'edit\n')
    check('(b) the registration modified after its commit', _has(_gf(root), '(b)'))
    git('checkout', '--', rel(REG))
    (root / 'ct16/rule.py').write_text('x = 3\n')
    f4 = _gf(root)
    check('(d)+(e) guarded code edited', _has(f4, '(d)') and _has(f4, '(e) ct16/rule.py'))
    git('checkout', '--', rel('ct16/rule.py'))
    (root / REG).write_text((root / REG).read_text() + f"| `ct16/rule.py` | `{'0' * 64}` |\n")
    git('commit', '-q', '-am', 'dup')
    check('(e) a file listed twice in the hash table', _has(_gf(root), '(e) files listed more than once'))
    git('reset', '-q', '--hard', 'HEAD~1')
    _add(root, {'registration': '016-x.md', 'mode': 'fetch_attempt'})
    check('(k) results.jsonl dirty', _has(_gf(root), '(k)'))
    git('checkout', '--', rel('results.jsonl'))
    # shadow modules
    (root / 'statistics.py').write_text('raise SystemExit("shadow")\n')
    check('(d) an untracked statistics.py next to the runner shadows an import', _has(_gf(root), '(d) untracked'))
    (root / 'statistics.py').unlink()
    (root / 'ct16' / 'rule').mkdir()
    (root / 'ct16' / 'rule' / '__init__.py').write_text('x = 9\n')
    check('(d) a package ct16/rule/ beside ct16/rule.py is refused', _has(_gf(root), '(d) untracked'))
    (root / 'ct16' / 'rule' / '__init__.py').unlink()
    (root / 'ct16' / 'rule').rmdir()
    import sysconfig
    ext = 'stats' + sysconfig.get_config_var('EXT_SUFFIX')
    (root / 'ct16' / ext).write_bytes(b'\x00')
    (root / 'ipaddress.pyc').write_bytes(b'\x00')
    sh = RUN.shadow_modules(root)
    check('shadow_modules: an extension module in ct16/ and a sourceless .pyc at the top level',
          {f'ct16/{ext}', 'ipaddress.pyc'} <= set(sh), str(sh))
    (root / 'ct16' / ext).unlink()
    (root / 'ipaddress.pyc').unlink()
    # (w)
    check('(w) the anchor must be an ancestor of HEAD', _has(_gf(root, anchor='0' * 40), '(w)'))
    check('(w) the git top level must be the repository', _has(_gf(root, toplevel=root / 'nowhere'), '(w)')
          and not _has(_gf(root, toplevel=top), '(w)'))
    rc = git('rev-parse', 'HEAD').stdout.strip()
    # --- the fetch rows; (f)
    _add(root, {'registration': '016-x.md', 'mode': 'fetch_started', 'registration_commit': rc})
    git('commit', '-q', '-am', 'fetch_started')
    check('a committed fetch_started row: the fetch resumes', _gf(root) == [])
    store = root / RUN.STORE_REL
    store.mkdir(parents=True)
    (store / 'a.zip').write_bytes(b'abc')
    files = {'a.zip': RUN.sha256(store / 'a.zip')}
    (store / 'manifest.json').write_text(json.dumps({'files': files, 'digest': RUN.manifest_digest(files)}))
    fetched = {'registration': '016-x.md', 'mode': 'fetched', 'registration_commit': rc,
               'code_hashes': RUN.hashes(root, CODE), 'manifest_digest': RUN.manifest_digest(files)}
    _add(root, fetched)
    git('commit', '-q', '-am', 'fetched')
    check('(f) the fetch runs once: a fetched row closes it', _has(_gf(root), '(f) the fetch has completed'))
    fr = _gf(root, mode='run')
    check("(h') the read needs the approval line", _has(fr, "(h')") and not _has(fr, '(g'), str(fr))
    fline = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"fetched"' in l][0]
    appr = f'016 read: APPROVED 2028-01-11 row {RUN.line_sha(fline)}\n'
    (root / REG).write_text((root / REG).read_text() + '\n' + appr)
    git('commit', '-q', '-am', 'approval')
    check('everything in order: the read gate opens (HEAD:./ paths resolve in this layout)', _gf(root, mode='run') == [],
          str(_gf(root, mode='run')))
    check('the read gate also refuses before the date', _has(_gf(root, mode='run', now=EARLY), '(t)'))
    (store / 'a.zip').write_bytes(b'abd')
    check('(g3) a fetched file changed after the fetch', _has(_gf(root, mode='run'), '(g3)'))
    (store / 'a.zip').write_bytes(b'abc')
    (root / REG).write_text((root / REG).read_text() + 'new prose\n')
    git('commit', '-q', '-am', 'prose')
    check('(r) prose added to the registration after the fetch', _has(_gf(root, mode='run'), '(r)'))
    git('reset', '-q', '--hard', 'HEAD~1')
    (root / 'ct16/rule.py').write_text('x = 4\n')
    git('commit', '-q', '-am', 'code change')
    fcc = _gf(root, mode='run')
    check('(g2) code changed since the fetch', _has(fcc, '(g2)'), str(fcc))
    git('reset', '-q', '--hard', 'HEAD~1')
    _add(root, {'registration': '016-x.md', 'mode': 'read_started'})
    git('commit', '-q', '-am', 'started')
    check('(i) a read_started row without a restart line blocks', _has(_gf(root, mode='run'), '(i)'))
    sline = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"read_started"' in l][0]
    (root / REG).write_text((root / REG).read_text() + f'016 read restart: APPROVED 2028-01-12 started-row '
                            f'{RUN.line_sha(sline)}\n')
    git('commit', '-q', '-am', 'restart')
    check('a later committed restart line reopens a mechanical stop', _gf(root, mode='run') == [], str(_gf(root, mode='run')))
    _add(root, {'registration': '016-x.md', 'mode': 'read'})
    git('commit', '-q', '-am', 'read')
    git('reset', '-q', '--hard', 'HEAD~1')
    check('(i) a read row erased by reset --hard still blocks (reflog)', _has(_gf(root, mode='run'), '(i)'))
    return root, top, git


def _approval_same_commit(layout):
    root, top, git = _repo(layout)
    _write_reg(root)
    git('add', '.')
    git('commit', '-q', '-m', 'all')
    rc = git('rev-parse', 'HEAD').stdout.strip()
    store = root / RUN.STORE_REL
    store.mkdir(parents=True)
    (store / 'manifest.json').write_text(json.dumps({'files': {}, 'digest': RUN.manifest_digest({})}))
    _add(root, {'registration': '016-x.md', 'mode': 'fetch_started', 'registration_commit': rc})
    f = {'registration': '016-x.md', 'mode': 'fetched', 'registration_commit': rc, 'code_hashes': RUN.hashes(root, CODE),
         'manifest_digest': RUN.manifest_digest({})}
    line = json.dumps(f, sort_keys=True)
    with open(root / 'results.jsonl', 'a') as fh:
        fh.write(line + '\n')
    (root / REG).write_text((root / REG).read_text() + f'\n016 read: APPROVED 2028-01-11 row {RUN.line_sha(line)}\n')
    git('commit', '-q', '-am', 'rows and approval together')
    return _gf(root, mode='run')


def bypass_checks():
    print('bypasses the reviewers used on earlier runners (erasure, attributes, GIT_DIR, nested repos, .pyc, -O)')
    for layout in ('top', 'sub'):
        check(f"({layout}) (h') an approval committed with the fetched row is refused", _has(_approval_same_commit(layout), "(h')"))

    def base():
        root, top, git = _repo('sub')
        _write_reg(root)
        git('add', '.')
        git('commit', '-q', '-m', 'all')
        return root, top, git
    root, top, git = base()
    _add(root, {'registration': '016-x.md', 'mode': 'fetched'})
    git('commit', '-q', '-am', 'row')
    keep = [l for l in (root / 'results.jsonl').read_text().splitlines() if '016-x' not in l]
    (root / 'results.jsonl').write_text('\n'.join(keep) + ('\n' if keep else ''))
    git('commit', '-q', '--amend', '--allow-empty', '-am', 'row removed by amend')
    check('a row removed with commit --amend still blocks (reflog)', _has(_gf(root), '(f)'))
    root, top, git = base()
    _add(root, {'registration': '016-other.md', 'mode': 'fetched'})
    git('stash', '-q')
    check('a stashed row (a merge commit) blocks (-m), keyed on the 016- prefix', _has(_gf(root), '(f)'))
    root, top, git = base()
    _add(root, {'registration': '016-x.md', 'mode': 'read'})
    git('commit', '-q', '-am', 'row')
    (root / 'results.jsonl').write_text('')
    git('commit', '-q', '-am', 'erased')
    (top / '.git' / 'info').mkdir(exist_ok=True)
    (top / '.git' / 'info' / 'attributes').write_text('*.jsonl -diff\n*.jsonl diff=blank\n')
    git('config', 'diff.blank.textconv', 'true')
    check('a -diff attribute and a textconv driver do not hide an erased row (--text --no-textconv)',
          _has(_gf(root, mode='run'), '(i)'))
    root, top, git = base()
    _add(root, {'registration': '016-x.md', 'mode': 'fetched'})
    git('commit', '-q', '-am', 'row')
    other, otop, ogit = base()
    os.environ['GIT_DIR'] = str(otop / '.git')
    try:
        fr = _gf(root)
    finally:
        del os.environ['GIT_DIR']
    check('GIT_DIR pointing at another repository is stripped: the real history still blocks', _has(fr, '(f)'))
    sub = root / 'nested'
    sub.mkdir()
    subprocess.run(['git', 'init', '-q', '-b', 'main'], cwd=sub, capture_output=True)
    check('a nested git init: (w) refuses when the top level differs',
          _has(RUN.guard_failures(sub, 'fetch', now=NOW, code=(), data=(), reg_rel=REG, toplevel=top)[0], '(w)'))
    root, top, git = base()
    rc = git('rev-parse', 'HEAD').stdout.strip()
    (root / REG).write_text((root / REG).read_text() + 'prose added later\n')
    git('commit', '-q', '-am', 'prose')
    git('config', 'diff.external', '/usr/bin/true')
    changed = [l for l in RUN._git(root, 'diff', '-U0', '--text', '--no-textconv', '--no-ext-diff', '--no-color', rc, 'HEAD',
                                   '--', REG).stdout.splitlines() if l.startswith('+') and not l.startswith('+++')]
    check('diff.external cannot blank the (r) diff (--no-ext-diff)', changed == ['+prose added later'])
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
        run = lambda pre: subprocess.run([sys.executable, '-I', '-c', pre + 'import sys; sys.path.insert(0, "."); import modx; '
                                          'print(modx.X)'], cwd=dd, capture_output=True, text=True).stdout.strip()
        check('a forged __pycache__ .pyc IS loaded by plain Python and is NOT under a fresh sys.pycache_prefix',
              run('') == '666' and run('import sys, tempfile; sys.pycache_prefix = tempfile.mkdtemp(); ') == '1')
    p = subprocess.run([sys.executable, '-I', '-c', 'import sys; sys.path.insert(0, "."); import r016_run as R; '
                        'print(R.loaded_from_source())'], cwd=HERE, capture_output=True, text=True)
    check('the runner, imported as a program, loads ct16 from source under a fresh pycache prefix', p.stdout.strip() == '[]',
          p.stderr[-300:])
    p = subprocess.run([sys.executable, '-O', '-c', 'import sys; sys.path.insert(0, "."); import r016_run as R; '
                        'print(any("-O" in x for x in R.environment_failures(R.HERE)))'], cwd=HERE, capture_output=True,
                       text=True)
    check('python -O is refused by (v)', p.stdout.strip() == 'True', p.stderr[-200:])
    check('the Python pin matches this interpreter', RUN.versions() == RUN.PINS, str(RUN.versions()))
    asserts = []
    for f in RUN.GUARDED_CODE:
        tree = ast.parse((HERE / f).read_text())
        asserts += [f for n in ast.walk(tree) if isinstance(n, ast.Assert)]
    check('no assert statement anywhere in the guarded code (refusals survive -O)', not asserts, str(asserts))


# ------------------------------------------------------------------ the gate and the real tree
def gate_checks():
    print('read gate and real-tree binding')
    check('Panel refuses the real data without the gate (before any file is opened)', raises(lambda: RUN.Panel('run')))
    check('Panel refuses the real span-(a) pull even next to a synthetic store',
          raises(lambda: RUN.Panel('run', store=tempfile.mkdtemp())))
    RUN._GATE = (str(RUN.HERE), 'fetch', 'x')
    try:
        check('a gate for another mode does not open the read', raises(lambda: RUN.Panel('run')))
    finally:
        RUN._GATE = None
    check('the real results file and the real data tree only together',
          RUN.real_tree_failures([tempfile.mkdtemp()]) != [] and RUN.real_tree_failures() == [])
    saved = RUN.HOOKS['commit']
    RUN.HOOKS['commit'] = lambda msg: None
    try:
        check('a mocked commit hook is refused on the real tree', RUN.real_tree_failures() != [])
    finally:
        RUN.HOOKS['commit'] = saved
    saved_g = RUN.guard
    RUN.guard = lambda mode: {'commit': 'x'}
    try:
        check('a stubbed guard is refused on the real tree', RUN.real_tree_failures() != [])
    finally:
        RUN.guard = saved_g
    mh = _mock_hooks({})
    mh.pop('calls')
    check('fetch: mock hooks on the real store are refused', raises(lambda: F.fetch(hooks=mh), (SystemExit,)))
    check('fetch: a missing hook is refused (no fallback)', raises(lambda: F.fetch(root=tempfile.mkdtemp(),
                                                                                 hooks={'get': None}), (SystemExit,)))
    check('fetch: the real hooks refuse outside the real guarded fetch', raises(lambda: F._get('x'), (SystemExit,)))
    rr = RUN.guard_failures(RUN.HERE, 'run', toplevel=RUN.REPO, anchor=RUN.ANCHOR)[0]
    check('the real guard (read-only call) refuses today: the date gate (t)', _has(rr, '(t)') or dt.date.today() >= R.READ_NOT_BEFORE,
          str(rr[:3]))
    # record(): the default hook commits and the line is verified in HEAD; a no-op hook refuses
    root, top, git = _repo('sub')
    git('add', '.')
    git('commit', '-q', '-m', 'init')
    saved_r = RUN.RESULTS
    RUN.RESULTS = root / 'results.jsonl'
    try:
        RUN.record({'registration': '016-x.md', 'mode': 'probe'}, 'probe row')
        check('record() commits the row alone and finds it in HEAD:./results.jsonl (subdirectory layout)',
              '"probe"' in git('show', 'HEAD:' + str((root / 'results.jsonl').relative_to(top))).stdout)
        RUN.HOOKS['commit'] = lambda msg: None
        check('record() refuses when the hook did not commit', raises(lambda: RUN.record({'registration': '016-x.md',
                                                                                          'mode': 'p2'}, 'x')))
    finally:
        RUN.HOOKS['commit'] = saved
        RUN.RESULTS = saved_r


# ------------------------------------------------------------------ the fetcher against mocks
def _mock_hooks(objects, page=3, hl=None, fail=(), empty=(), badsum=()):
    """S3 ListObjects v1 with `page` keys per page and an EXCLUSIVE marker; data.binance.vision GET; HL funding with
    INCLUSIVE start and end."""
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
        t = h['first']
        out = []
        stop = h['last'] if h['last'] is not None else F.ms(dt.date(2028, 1, 1)) + 10 * 3600000
        t = max(t, start - (start - h['first']) % 3600000)
        while t <= min(end, stop) and len(out) < 500:
            if t >= start:
                out.append({'coin': coin, 'time': t, 'fundingRate': '0.00001'})
            t += 3600000
        return out
    return {'s3_list': s3_list, 'get': get, 'exchange_info': lambda: {'symbols': [{'symbol': 'BTCUSDT',
                                                                                    'contractType': 'PERPETUAL',
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
    return _zip(f'{sym}-fundingRate-{mo}.csv', 'calc_time,funding_interval_hours,last_funding_rate\n'
                f'{t},8,0.0001\n')


def fetch_checks():
    print('fetch (mocks): paging, months, checksums, never caching an error, coverage, persistence, the HL probe')
    objs = {}
    for sym in ('BTCUSDT', 'ETHUSDT'):
        for mo in F.months('2026-05', '2028-02'):
            objs[f'{F.KLINES_BASE}{sym}/1d/{sym}-1d-{mo}.zip'] = _month_zip(sym, mo)
        for mo in F.months('2025-01', '2028-02'):
            objs[f'{F.FUNDING_BASE}{sym}/{sym}-fundingRate-{mo}.zip'] = _fund_zip(sym, mo)
    hk = _mock_hooks(objs)
    want, later = F.plan(hk, F.KLINES_BASE, '1d/', *RUN.FWD_MONTHS)
    check('plan: exactly the 18 forward months per symbol; later months counted, never planned; <= 2026-06 not planned',
          [m for m, _ in want['BTCUSDT']] == F.months('2026-07', '2027-12') and later == 4)
    full = [k for kind, k in hk['s3_list'](F.KLINES_BASE + 'BTCUSDT/1d/') if kind == 'obj']
    check('S3 paging forward with an exclusive marker: every key once, none skipped (page size 3)',
          full == sorted(k for k in objs if k.startswith(F.KLINES_BASE + 'BTCUSDT/1d/')) and len(full) == len(set(full)))
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
        _, files2, errors2, _l = F.pull(hk, store, F.KLINES_BASE, '1d/', *RUN.FWD_MONTHS, 'klines')
        check('the re-run fetches only the missing file (resumable)', not errors2 and len(files2) == 36
              and sum(1 for k in hk['calls']['get'] if k.endswith('2026-07.zip') and 'BTCUSDT' in k) == 1)
        a_sums = {}
        cov = F.coverage(Path(d), store, files2, a_sums)
        check('coverage: BTC has every forward day; no gap', cov == {}, str(cov))
        files3 = {k: v for k, v in files2.items() if '2027-03' not in k or 'ETHUSDT' not in k}
        cov3 = F.coverage(Path(d), store, files3, a_sums)
        check('coverage: an interior month missing is a keyed refusal', 'klines_gap:ETHUSDT' in cov3)
        files4 = {k: v for k, v in files2.items() if 'BTCUSDT-1d-2027-12' not in k}
        check('coverage: BTC missing its last month refuses (never persistable)', 'btc_months' in F.coverage(Path(d), store, files4, a_sums))
    att = {1: {'refusals': {'klines_gap:ETHUSDT'}, 'time': 1_000_000}, 2: {'refusals': {'klines_gap:ETHUSDT'},
                                                                          'time': 1_000_000 + 90_000}}
    blk, per = F.apply_persistence({'klines_gap:ETHUSDT': 'x', 'btc_months': 'y'}, att, 1_000_000 + 180_000)
    check('persistence: the same key on 3 attempts >= 24 h apart becomes unavailable; BTC never does',
          'klines_gap:ETHUSDT' in per and 'btc_months' in blk)
    blk2, per2 = F.apply_persistence({'klines_gap:ETHUSDT': 'x'}, att, 1_000_000 + 100_000)
    check('persistence: attempts closer than 24 h do not chain', 'klines_gap:ETHUSDT' in blk2 and not per2)
    # HL presence: inclusive endTime; [s, s + 2 d - 1 ms] windows; bisection for the last funding month start
    end_ms = F.ms(dt.date(2028, 1, 1))
    hl = {'AAA': {'first': F.ms(dt.date(2023, 6, 1)), 'last': None},
          'BBB': {'first': F.ms(dt.date(2024, 2, 1)), 'last': F.ms(dt.date(2026, 9, 15))},
          'CCC': {'first': F.ms(dt.date(2026, 11, 1)), 'last': F.ms(dt.date(2026, 11, 1))}}
    mh = _mock_hooks({}, hl=hl)
    pres, err = F.hl_presence(mh, ['AAA', 'BBB', 'CCC'], end_ms)
    check('HL: alive at the end -> last None; first funding time found',
          pres['AAA'] == {'first': hl['AAA']['first'], 'last': None} and not err)
    check('HL: a coin that stopped -> the last month start with funding (bisection)',
          pres['BBB']['last'] == F.ms(dt.date(2026, 9, 1)), str(pres['BBB']))
    check('HL: a row exactly at a window end (s + 2 d) is outside [s, s + 2 d - 1 ms] (inclusive endTime handled)',
          pres['CCC']['last'] == F.ms(dt.date(2026, 11, 1)))
    ufn = RUN.hl_universe_fn(lambda d: ['AAAUSDT', 'BBBUSDT', '1000CCCUSDT'], {'AAA': pres['AAA'], 'BBB': pres['BBB'],
                                                                                'kCCC': pres['CCC']})
    check('HL sub-book: members listed at the month start, mapped (1000X -> kX)',
          ufn(R.day(dt.date(2026, 10, 31))) == ['AAAUSDT', '1000CCCUSDT'] and ufn(R.day(dt.date(2026, 8, 31)))
          == ['AAAUSDT', 'BBBUSDT'] and ufn(R.day(dt.date(2026, 11, 30))) == ['AAAUSDT'])


# ------------------------------------------------------------------ end to end on a synthetic tree (both layouts)
def _synthetic_tree(root, seed=11):
    """A scout-like pull (months <= 2026-06) and nothing forward: the fetch fills the rest from mocks."""
    syms = sim.make_world({'name': 'e2e', 'drift': 'flat', 'kappa': 0.25, 'half_life': 60}, seed)
    a_dir = root / RUN.KLINES_A_REL
    objs = {}
    sums = []
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
    check('fetch refuses before 2028-01-08', raises(lambda: F.fetch(root=root, now=EARLY, hooks=hooks, **kw), (SystemExit,)))
    row = F.fetch(root=root, now=NOW, hooks=hooks, **kw)
    head = git('show', 'HEAD:' + str((root / 'results.jsonl').relative_to(top))).stdout
    check('the fetch commits fetch_started, an attempt row and the fetched row (verified in HEAD)',
          head.count('"fetch_started"') == 1 and '"fetch_attempt"' in head and '"fetched"' in head)
    check('the fetch runs once', raises(lambda: F.fetch(root=root, now=NOW, hooks=hooks, **kw), (SystemExit,)))
    fr = RUN.guard_failures(root, 'run', now=NOW, **kw)[0]
    check("the read needs the approval line (h')", _has(fr, "(h')") and len(fr) == 1, str(fr))
    fline = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"fetched"' in l][0]
    (root / REG).write_text((root / REG).read_text() + f'\n016 read: APPROVED 2028-01-11 row {RUN.line_sha(fline)}\n')
    git('commit', '-q', '-am', 'approval')
    check('after the approval the read gate opens', RUN.guard_failures(root, 'run', now=NOW, **kw)[0] == [])
    panel = RUN.Panel('run', klines_a=root / RUN.KLINES_A_REL, store=root / RUN.STORE_REL)
    syms, funding, non_coin, hl, counts = panel.load(static_exchange=root / RUN.STATIC_EXCHANGE_REL)
    check('the panel joins the pinned pull (<= 2026-06) and the fetch (2026-07 -> 2027-12) without overlap',
          counts['symbols'] == 24 and min(syms[R.BTC].rows) == R.day(sim.SIM_START)
          and max(syms[R.BTC].rows) == R.day(R.END) and len(funding) == 24, str(counts))
    dec, rep, series, bk = RUN.compute(syms, funding, non_coin, hl, placebo_seeds=3)
    row = {'outcome': dec['outcome'], 'route': dec['route'], 'h1_t': dec['h1']['pooled']['t'], 'h2_T': dec['h2']['T'],
           'series': series}
    fails, _ = S.recheck(json.loads(json.dumps(row)))
    check('compute runs end to end on the synthetic trend world; the decision recomputes from the stored series',
          fails == [] and dec['h1']['pooled']['G'] == 33, f"{dec['outcome']} t {dec['h1']['pooled']['t']:.2f}")
    check('the reported blocks are present (strata, variants, tail, placebo)',
          {'strata', 'costs', 'long_short', 'tail_25pct_vol', 'placebo_shifted_signals'} <= set(rep))
    tamper = root / RUN.STORE_REL / 'klines' / 'BTCUSDT' / '1d' / 'BTCUSDT-1d-2027-01.zip'
    blob = bytearray(tamper.read_bytes())
    blob[len(blob) // 2] ^= 0xFF
    tamper.write_bytes(bytes(blob))
    check('a forward file altered after the fetch: (g3) refuses and the panel refuses it',
          _has(RUN.guard_failures(root, 'run', now=NOW, **kw)[0], '(g3)')
          and raises(lambda: RUN.Panel('run', klines_a=root / RUN.KLINES_A_REL, store=root / RUN.STORE_REL).load(
              static_exchange=root / RUN.STATIC_EXCHANGE_REL)))


# ------------------------------------------------------------------ the simulator
def sim_checks():
    print('simulator: determinism, a trend-free world is a martingale, a trend world trends')
    a = sim.run_draw({'name': 'x', 'drift': 'flat'}, 5)
    b = sim.run_draw({'name': 'x', 'drift': 'flat'}, 5)
    check('a draw is a deterministic function of (spec, seed)', a == b)
    w = sim.make_world({'name': 'x', 'drift': 'flat', 'df': None, 'garch': (0, 0, 0)}, 2)
    btc = w[R.BTC]
    r = [btc.tclose[i] / btc.tclose[i - 1] - 1 for i in range(1, len(btc.tclose))]
    ac = sum(x * y for x, y in zip(r, r[1:])) / sum(x * x for x in r)
    check('trend-free world: BTC daily returns have ~zero mean and ~zero autocorrelation',
          abs(statistics.fmean(r)) < 0.003 and abs(ac) < 0.08, f'mean {statistics.fmean(r):.5f} ac {ac:.3f}')
    check('every calibration world has a name and runs', all(sim.run_draw(s, 1)['G'] == 33 for s in sim.CALIBRATION[:3]))


def real_file_checks():
    print('the real registration and its hash table (does not go stale at commit or at REGISTERED)')
    reg = HERE / 'registrations' / RUN.REG_NAME
    check('the registration exists', reg.exists())
    text = reg.read_text()
    m = RUN.STATUS.search(text)
    check('the status line is DRAFT or REGISTERED', m is not None and m.group(1) in ('DRAFT', 'REGISTERED'))
    check('no approval or restart line exists yet', not RUN.APPROVAL.findall(text) and not RUN.RESTART.findall(text))
    tbl = RUN.HASH_ROW.findall(text)
    names = [f for f, _ in tbl]
    check('the hash table lists every guarded code and data file exactly once',
          sorted(names) == sorted((*RUN.GUARDED_CODE, *RUN.GUARDED_DATA)), str(sorted(set(names) ^ set((*RUN.GUARDED_CODE,
                                                                                                          *RUN.GUARDED_DATA)))))
    tab = dict(tbl)
    stale = [f for f in RUN.GUARDED_CODE if RUN.sha256(HERE / f) != tab.get(f)]
    check('the code hashes in the table equal the files on disk', not stale, str(stale))
    present = [f for f in RUN.GUARDED_DATA if (HERE / f).exists()]
    check('the data hashes equal the files on disk (where the gitignored data exist on this machine)',
          all(RUN.sha256(HERE / f) == tab.get(f) for f in present))
    check('the calendars: (a) 2025-03-20 -> 2026-06-30, (c) 2026-07-01 -> 2027-12-31, gate 2028-01-08',
          R.SPAN_A == (dt.date(2025, 3, 20), dt.date(2026, 6, 30)) and R.SPAN_C == (dt.date(2026, 7, 1), dt.date(2027, 12, 31))
          and R.READ_NOT_BEFORE == dt.date(2028, 1, 8) == R.END + dt.timedelta(days=8))


def main():
    signal_checks()
    universe_checks()
    book_checks()
    stats_checks()
    parser_checks()
    for layout in ('top', 'sub'):
        guard_checks(layout)
    bypass_checks()
    gate_checks()
    fetch_checks()
    for layout in ('top', 'sub'):
        e2e_checks(layout)
    sim_checks()
    real_file_checks()
    print(f'all checks passed ({N_CHECKS[0]})')


if __name__ == '__main__':
    main()
