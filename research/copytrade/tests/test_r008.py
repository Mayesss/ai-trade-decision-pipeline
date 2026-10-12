"""Registration 008 (L3, 001 T3 inverted, holdout-only) — mechanics on SYNTHETIC data only.

No real file under research/copytrade/data is opened, and nothing is fetched: every network hook is
replaced, and the end-to-end check runs in a throwaway git repo with a synthetic data tree.

    .venv/bin/python -m tests.test_r008
"""
import datetime as dt
import json
import math
import random
import statistics
import subprocess
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ct import crowd, crowd008 as c8, eventstudy, schedule, stats

HERE = Path(__file__).resolve().parent.parent
H, D = c8.HOUR, c8.DAY
N_CHECKS = [0]
END = c8.OFFERED_ENDS['2026-09-28']


def check(name, ok, detail=''):
    N_CHECKS[0] += 1
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + str(detail) if detail != "" else ""}')
    if not ok:
        raise SystemExit(1)


def close(a, b, tol=1e-12):
    return a is not None and b is not None and abs(a - b) <= tol


def ms(y, m, d, h=0):
    return int(dt.datetime(y, m, d, h, tzinfo=dt.UTC).timestamp() * 1000)


# ------------------------------------------------------------------ synthetic market
class Market:
    """Hourly closes for a few symbols (random walks), and hourly HL funding."""

    def __init__(self, syms, t0, t1, seed=1):
        rng = random.Random(seed)
        self.closes = {}
        for s in syms:
            p, rows = 100.0 * (1 + rng.random()), {}
            for h in range(t0 - t0 % H, t1, H):
                p *= math.exp(rng.gauss(0, 0.006))
                rows[h] = p
            self.closes[s] = rows
        self.funding = {s: [(h, rng.gauss(0.00001, 0.00002)) for h in range(t0 - t0 % H, t1, H)] for s in syms}

    def candle_1h(self, sym, hour_ms):
        c = self.closes.get(sym, {}).get(hour_ms)
        return [hour_ms, c, c, c, c] if c is not None else None


def write_snapshot(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    users, markets, sizes, notionals = zip(*rows)
    pq.write_table(pa.table({'user': list(users), 'market': list(markets), 'size': list(sizes),
                             'notional': list(notionals),
                             'account_value': [500.0 if u.endswith(('3', '7')) else 50_000.0 for u in users]}), path)


def snapshot_rows(wallets, coins, rng):
    rows = []
    for w in wallets:
        for c in coins:
            if rng.random() < 0.8:
                q = rng.choice((-1, 1)) * rng.uniform(1, 10)
                rows.append((w, c, q, abs(q) * rng.uniform(500, 1500)))
    return rows


# ------------------------------------------------------------------ 1. calendar
def calendar_checks():
    print('\n1. calendar')
    w = c8.grid_windows(END)
    check('end 2026-09-28: windows S = 2026-06-08 and 2026-08-03 (001 grid continued)',
          [(s.isoformat(), he.isoformat()) for s, _, he in w] == [('2026-06-08', '2026-08-03'), ('2026-08-03', '2026-09-28')])
    w2 = c8.grid_windows(c8.OFFERED_ENDS['2026-11-23'])
    check('end 2026-11-23 adds S = 2026-09-28', [s.isoformat() for s, _, _ in w2] == ['2026-06-08', '2026-08-03', '2026-09-28'])
    hd = c8.holdout_fill_days(END)
    check('holdout fill days 2026-07-01 .. 2026-08-02 (33)', len(hd) == 33 and hd[0] == dt.date(2026, 7, 1)
          and hd[-1] == dt.date(2026, 8, 2))
    hd2 = c8.holdout_fill_days(c8.OFFERED_ENDS['2026-11-23'])
    check('end 2026-11-23: 2026-07-01 .. 2026-09-27 (89)', len(hd2) == 89 and hd2[-1] == dt.date(2026, 9, 27))
    check('every 001 discovery window lies before the holdout (no overlap with 008 windows)',
          all(he <= dt.date(2026, 6, 8) for _, _, he in schedule.windows('hyperliquid')))
    files = [Path(f'date=x/1_{t}.parquet') for t in (ms(2026, 6, 30, 0) + 11_000, ms(2026, 7, 1) + 11_000,
                                                      ms(2026, 7, 30, 16), ms(2026, 7, 31) + 5_000,
                                                      ms(2026, 8, 3) + 5_000, ms(2026, 9, 24, 23), ms(2026, 9, 25) + 1)]
    sel = c8.signal_snapshots(files, END)
    got = [t for _, _, t in sel]
    check('signal filter: pre-holdout and the last 72 h of each hold are excluded',
          got == [ms(2026, 7, 1) + 11_000, ms(2026, 7, 30, 16), ms(2026, 8, 3) + 5_000, ms(2026, 9, 24, 23)], got)
    check('entry = first full hour >= snapshot + 1 h', c8.entry_of(ms(2026, 7, 1) + 11_000) == ms(2026, 7, 1, 2))
    keys = c8.planned_keys(END)
    check('planned book days 83 in 13 ISO weeks', len(keys) == 83 and len({c8.week_of(k) for k in keys}) == 13)


# ------------------------------------------------------------------ 2. statistics
def stats_checks():
    print('\n2. statistics')
    tab = {1: 6.314, 5: 2.015, 12: 1.782, 20: 1.725, 30: 1.697}
    check('t quantiles match the table (3 dp)', all(abs(c8.t_quantile(k, 0.95) - v) < 6e-4 for k, v in tab.items()),
          {k: round(c8.t_quantile(k, 0.95), 4) for k in tab})
    check('t_{G-1}: df < 1 -> inf', c8.t_quantile(0, 0.95) == math.inf)
    rng = random.Random(3)
    vals = [rng.gauss(0.001, 0.01) for _ in range(90)]
    cl = [f'w{i // 7}' for i in range(90)]
    a = c8.cluster_mean_t(vals, cl)
    b = eventstudy.cluster_t(vals, cl)
    check('cluster_mean_t == ct.eventstudy.cluster_t', all(close(x, y, 1e-15) for x, y in zip(a[:3], b[:3])) and a[3:] == b[3:])
    x = [rng.gauss(0, 0.03) for _ in range(400)]
    y = [0.002 + 0.5 * xi + rng.gauss(0, 0.004) for xi in x]
    fit = c8.ols_cluster(y, x, [f'w{i // 7}' for i in range(400)])
    check('OLS recovers a planted intercept and beta', abs(fit['alpha'] - 0.002) < 0.0006 and abs(fit['beta'] - 0.5) < 0.03,
          (round(fit['alpha'], 5), round(fit['beta'], 3)))
    check('OLS alpha == stats.alpha (001 T2 helper)', close(fit['alpha'], stats.alpha(y, x), 1e-12))


# ------------------------------------------------------------------ 3. equivalence with 001's code
def equivalence_checks():
    print("\n3. 008's data path equals 001's p5_run T3 path (synthetic), and the inversion is exact")
    import p5_run
    syms = [f'C{i}USDT' for i in range(12)] + ['BTCUSDT']
    coins = [f'C{i}' for i in range(12)]
    coin_to_sym = {c: f'{c}USDT' for c in coins}
    t0, t1 = ms(2026, 5, 20), ms(2026, 8, 10)
    mk = Market(syms, t0, t1, seed=5)
    closes = c8.Closes(mk.closes)
    bg_orig = p5_run.bg.candle_1h
    p5_run.bg.candle_1h = mk.candle_1h
    try:
        r_old, v_old = p5_run.b_returns()
        hs = [ms(2026, 7, 2, 2) + k * H for k in range(0, 24 * 20, 7)]
        check('Closes.ret / vol == p5_run.b_returns over bg.candle_1h',
              all(r_old(s, h) == closes.ret(s, h) and r_old(s, h, 72 * H) == closes.ret(s, h, 72 * H)
                  and v_old(s, h) == closes.vol(s, h) for s in syms for h in hs))
        fund = c8.Funding(mk.funding)
        naive = lambda s, h, span=D: sum(r for t, r in mk.funding.get(s, []) if h <= t < h + span)
        check('Funding.fund (bisect) == p5_run lambda', all(close(fund.fund(s, h), naive(s, h), 1e-15)
                                                            and close(fund.fund(s, h, 72 * H), naive(s, h, 72 * H), 1e-15)
                                                            for s in syms for h in hs))
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            rng = random.Random(9)
            wallets = [f'0x{i:040x}' for i in range(20)]
            top, bottom = wallets[:5], wallets[5:10]
            base = tmp / 'archive/by_dex/hyperliquid/snapshots/perp'
            for d in range(25):
                day = dt.date(2026, 7, 1) + dt.timedelta(days=d)
                t_s = c8.ms(day) + 7_000 + d * 1000
                write_snapshot(base / f'date={day.isoformat()}' / f'{1000 + d}_{t_s}.parquet', snapshot_rows(wallets, coins, rng))
            write_snapshot(base / 'date=2026-06-30' / f'999_{ms(2026, 6, 30) + 5000}.parquet', snapshot_rows(wallets, coins, rng))
            listed = {s: ms(2025, 1, 1) for s in syms}
            listed['C11USDT'] = ms(2026, 7, 10)               # a late listing: enters only after 31 days
            data_orig = p5_run.DATA
            p5_run.DATA = tmp
            try:
                acc = p5_run.new_acc()
                acc['T3']['fund'] = lambda s, h, span=D: sum(r for t, r in mk.funding.get(s, []) if h <= t < h + span)
                p5_run.crowd_trial(top, bottom, ms(2026, 6, 8), ms(2026, 8, 3), listed, coin_to_sym, mk.funding, acc)
                old = p5_run.finish_t3(acc['T3'], 'bottom')
            finally:
                p5_run.DATA = data_orig
            snaps = sorted(base.glob('date=*/*.parquet'))
            plan = c8.signal_snapshots(snaps, END)
            check('the pre-holdout snapshot is not in 008\'s plan', all(t >= c8.HOLDOUT_START_MS for _, _, t in plan) and len(plan) == 25)
            days = [(c8.entry_of(t), c8.signals_for(p, top, bottom, listed, coin_to_sym, c8.entry_of(t))) for _, p, t in plan]
            check("signals equal 001's (same entries, same values)", [(k, s) for k, s in days] == acc['T3']['bottom']['days'][1:]
                  or [(k, s) for k, s in days] == [x for x in acc['T3']['bottom']['days'] if x[0] >= c8.HOLDOUT_START_MS])
            net = c8.book(days, closes, fund, c8.B_COST)
            gross = c8.book(days, closes, fund, 0.0)
            # p5_run's T3 includes the 2026-06-30 snapshot (it has no holdout cut); compare on 001's own day set
            days_all = acc['T3']['bottom']['days']
            net_all = c8.book(days_all, closes, fund, c8.B_COST)
            vals = [v for _, v in net_all]
            half = len(vals) // 2
            check("008's book on 001's days == p5_run.finish_t3 (mean, median, halves)",
                  close(statistics.fmean(vals), old['portfolio_mean'], 1e-15) and close(statistics.median(vals), old['portfolio_median'], 1e-15)
                  and close(statistics.fmean(vals[:half]), old['halves'][0], 1e-15) and close(statistics.fmean(vals[half:]), old['halves'][1], 1e-15))
            ic_all = [x for _, x in c8.ic3(days_all, closes, fund) if x is not None]
            check("008's IC on 001's days == p5_run's ic3 mean", close(statistics.fmean(ic_all), old['ic_mean'], 1e-15))
            inv = [(k, c8.invert(s)) for k, s in days]
            g_inv = c8.book(inv, closes, fund, 0.0)
            n_inv = c8.book(inv, closes, fund, c8.B_COST)
            cost = [g - n for (_, g), (_, n) in zip(gross, net)]
            check('inverted gross == -(001 gross), day by day', all(close(a, -b, 1e-15) for (_, a), (_, b) in zip(g_inv, gross)))
            check('inverted net == -(001 net) - 2 x cost, day by day',
                  all(close(a, -b - 2 * c, 1e-15) for (_, a), (_, b), c in zip(n_inv, net, cost)))
            check('the cost is the same for both signs (turnover unchanged)',
                  all(close(gi - ni, c, 1e-15) for (_, gi), (_, ni), c in zip(g_inv, n_inv, cost)))
            longs, shorts = crowd.quintiles(days[3][1])
            il, is_ = crowd.quintiles(inv[3][1])
            check('inversion swaps the long and short quintiles', set(il) == set(shorts) and set(is_) == set(longs))
            check('C11USDT (listed 2026-07-10) never enters before 31 days', all('C11USDT' not in s for k, s in days if k < ms(2026, 8, 10)))
    finally:
        p5_run.bg.candle_1h = bg_orig
    print('   steady-state book cost per day:', round(statistics.fmean(cost[3:]) * 1e4, 3), 'bp (2 x 11 bp / 3 = 7.333)')
    check('book cost is 2 x B_COST / 3 per day in steady state', close(statistics.fmean(cost[3:]), 2 * c8.B_COST / 3, 1e-12))


def cohort_checks():
    print("\n4. cohort rule == p5_run.score_wallet + finish_copy_window's quintiles")
    import p5_run
    rng = random.Random(11)
    is_rising = lambda t: (t // (10 * D)) % 2 == 0

    def fills_for(mu, n):
        out, t = [], ms(2026, 3, 10)
        for i in range(n):
            coin = f'C{i % 3}'
            q, p0 = 10.0, 100.0
            p1 = p0 * (1 + rng.gauss(mu, 0.02))
            long = rng.random() < 0.5
            out.append({'coin': coin, 'time': t, 'side': 'B' if long else 'A', 'sz': q, 'px': p0, 'startPosition': 0.0,
                        'crossed': True})
            out.append({'coin': coin, 'time': t + 30 * H, 'side': 'A' if long else 'B', 'sz': q, 'px': p1 if long else 2 * p0 - p1,
                        'startPosition': q if long else -q, 'crossed': True})
            t += 2 * D + rng.randrange(0, 3) * H
        return out

    class Eq:
        def __init__(self, ok):
            self.ok = ok

        def at(self, a, t):
            return (50_000.0 if self.ok[a][0] else None, t - 1)

        def typical_leverage(self, a, s, e):
            return 1.2 if self.ok[a][1] else None

        def equity_fn(self, a):
            return lambda t: 50_000.0

    class Venue:
        def candle_hour(self, s, t):
            return None

        def beta(self, s, t):
            return None

    wallets = {f'0x{i:040x}': fills_for((i - 15) * 0.002, 20 if i % 9 else 5) for i in range(30)}
    ok = {a: (i % 7 != 0, i % 11 != 0) for i, a in enumerate(wallets)}

    class Reg:
        pass
    reg = Reg()
    reg.is_rising = is_rising
    table = {f'C{i}': {'status': 'ok', 'symbol': f'C{i}USDT', 'qty_mult': 1.0} for i in range(3)}
    old, new = [], []
    for a in sorted(wallets):
        w = p5_run.score_wallet(p5_run.T2, a, wallets[a], [], Eq(ok), table, {}, Venue(), reg, ms(2026, 6, 8),
                                ms(2026, 3, 9), 4)
        if w is not None:
            old.append(w)
        sc = c8.score_t3(wallets[a], Eq(ok).at(a, 0)[0], Eq(ok).typical_leverage(a, 0, 0), is_rising)
        if sc is not None:
            new.append((a, sc))
    check('same scored wallets and scores', [(w['address'], w['score']) for w in old] == new, f'{len(new)} scored')
    ranked = sorted(old, key=lambda w: w['score'])
    k = len(ranked) // 5
    top, bottom = c8.quintiles_by_score(new)
    check('same top / bottom quintiles', [w['address'] for w in ranked[-k:]] == top and [w['address'] for w in ranked[:k]] == bottom
          and k >= 2, f'k={k}')


# ------------------------------------------------------------------ 5. factors, cells, OLS (Revision 1)
def factor_checks():
    print('\n5. factors of the confirm regression, BTC-slope cells, multi-regressor OLS')
    days = c8.planned_days(END)
    keys = [k for k, _ in days]
    cells = c8.beta_cells(keys, dict(days))
    from collections import Counter
    cnt = Counter(cells.values())
    check('2026-09-28: cells (A, Jul) 30, (B, Aug) 29, (B, Sep) 24 — one BTC slope per month', sorted(cnt.values()) == [24, 29, 30], cnt)
    d2 = c8.planned_days(c8.OFFERED_ENDS['2026-11-23'])
    c2 = Counter(c8.beta_cells([k for k, _ in d2], dict(d2)).values())
    check("2026-11-23: window C's 3 September days merge into (B, Sep): 5 cells, none under 10 days",
          len(c2) == 5 and min(c2.values()) >= 10 and c2['1:2026-09'] == 27, c2)
    rng = random.Random(4)
    n = 300
    xs = [[rng.gauss(0, 1) for _ in range(n)] for _ in range(3)]
    y = [0.5 + 1.0 * a - 2.0 * b + 0.3 * c + rng.gauss(0, 0.5) for a, b, c in zip(*xs)]
    fit = c8.ols_multi(y, xs, [i // 7 for i in range(n)])
    check('ols_multi recovers a planted intercept and three slopes',
          all(abs(u - v) < 0.12 for u, v in zip(fit['coef'], (0.5, 1.0, -2.0, 0.3))), [round(x, 2) for x in fit['coef']])
    one = c8.ols_multi(y, [xs[0]], [i // 7 for i in range(n)])
    old = c8.ols_cluster(y, xs[0], [i // 7 for i in range(n)])
    check('ols_multi with one regressor == ols_cluster', close(one['alpha'], old['alpha'], 1e-12) and close(one['t_alpha'], old['t_alpha'], 1e-9))
    check('rank-deficient design -> None', c8.ols_multi(y, [xs[0], xs[0]], [i // 7 for i in range(n)]) is None)
    syms = ['A1USDT', 'A2USDT', 'A3USDT', 'A4USDT', 'A5USDT', 'BTCUSDT']
    mk = Market(syms, ms(2026, 6, 1), ms(2026, 7, 20), seed=2)
    cl = c8.Closes(mk.closes)
    k0 = ms(2026, 7, 5, 2)
    sig = {s: 1.0 for s in syms}
    f = c8.day_factors([(k0 - D, {'A1USDT': 1.0}), (k0, sig)], cl)[k0]
    r = {s: cl.ret(s, k0) for s in syms if s != 'BTCUSDT'}
    prev = {s: cl.ret(s, k0 - D) for s in r}
    lo = sorted(r, key=prev.get)[:2]
    hi = sorted(r, key=prev.get)[-2:]
    want = 0.5 * statistics.fmean(r[s] for s in lo) - 0.5 * statistics.fmean(r[s] for s in hi)
    check('alt = equal-weight universe return, BTCUSDT excluded; btc = BTCUSDT return',
          close(f['alt'], statistics.fmean(r.values())) and close(f['btc'], cl.ret('BTCUSDT', k0)) and f['universe'] == 5)
    check('rev = 0.5 x losers - 0.5 x winners of the PREVIOUS 24 h (point-in-time, odd middle name left out)',
          close(f['rev'], want), (round(f['rev'], 6), round(want, 6)))
    ff = c8.day_factors([(k0, {'A1USDT': 1.0, 'A2USDT': 1.0})], cl)[k0]
    check('rev is 0 with fewer than 4 names (flagged)', ff['rev'] == 0.0 and ff['rev_missing'])
    uni = c8.day_factors([(k0 - 2 * D, {'A1USDT': 1}), (k0 - D, {'A2USDT': 1}), (k0, {'A3USDT': 1}), (k0 + D, {'A4USDT': 1})], cl)
    check("the day's universe is the live tranches' coins (this and the two previous signal days)",
          uni[k0]['universe'] == 3 and uni[k0 + D]['universe'] == 3)


# ------------------------------------------------------------------ 6. decision rule
def synth_factors(keys, rng, drift=0.0):
    return {k: {'btc': rng.gauss(drift, 0.025), 'alt': rng.gauss(0, 0.03), 'rev': rng.gauss(0, 0.008)} for k in keys}


def decision_checks():
    print('\n6. existence test, economic bar (half-shrunk), recheck')
    days = c8.planned_days(END)
    keys = [k for k, _ in days]
    cells = c8.beta_cells(keys, dict(days))
    rng = random.Random(21)
    fac = synth_factors(keys, rng, 0.002)
    for k in keys:
        fac[k]['alt'] = 1.1 * fac[k]['btc'] + rng.gauss(0, 0.015)
    strong = [0.004 + 0.3 * fac[k]['btc'] + rng.gauss(0, 0.004) for k in keys]
    ok, num = c8.confirm(strong, keys, fac, cells)
    check('a planted strong edge confirms (k = 1 + 3 cells x (BTC, alt, rev) = 10)', ok and num['k'] == 10 and num['G'] == 13,
          round(num['t_alpha'], 2))
    econ = c8.economic(num['alpha'], c8.COST_DAY)
    row = {'confirm': num, 'economic': econ, 'decision': c8.decide(ok, econ)}
    dec, fails = c8.recheck(json.loads(json.dumps(row)))
    check('recheck reproduces the decision; 40 bp/day gross clears the half-shrunk bar',
          dec == row['decision'] == 'confirm_gross_and_shrunk_net' and not fails, (dec, fails))
    forged = json.loads(json.dumps({**row, 'confirm': {**num, 't_alpha': 0.5}}))
    dec2, fails2 = c8.recheck(forged)
    check('a forged t under a confirm label is caught, and the failure text names no label',
          dec2 == 'not_demonstrated' and fails2 and not any(l in ' '.join(fails2) for l in c8.LABELS[1:]), fails2[:1])
    pooled, permonth = [], []
    for seed in range(60):
        r = random.Random(100 + seed)
        for k in keys:
            fac[k]['btc'] = r.gauss({'2026-07': -0.003, '2026-08': 0.007}.get(c8.month_of(k), 0.0026), 0.025)
        by_win = [(0.0 if w == 0 else 0.5) * fac[k]['btc'] + r.gauss(0, 0.004) for k, w in days]
        permonth.append(c8.confirm(by_win, keys, fac, cells)[1]['alpha'])
        pooled.append(c8.ols_cluster(by_win, [fac[k]['btc'] for k in keys], [c8.week_of(k) for k in keys])['alpha'])
    check('cohort-specific beta with month drifts, no alpha (60 draws): one pooled slope biases alpha by > 5 bp/day; '
          'the per-month slopes are unbiased (< 2.5 bp, ~4 se of the 60-draw mean)', abs(statistics.fmean(pooled)) > 0.0005
          and abs(statistics.fmean(permonth)) < 0.00025,
          (round(statistics.fmean(pooled) * 1e4, 2), round(statistics.fmean(permonth) * 1e4, 3)))
    for k in keys:
        fac[k]['rev'] += 0.001
    rev_only = [1.0 * fac[k]['rev'] + rng.gauss(0, 0.003) for k in keys]
    ok4, num4 = c8.confirm(rev_only, keys, fac, cells)
    check('a book that is pure short-term reversal (drifting +10 bp/day) does not confirm', not ok4, round(num4['t_alpha'], 2))
    econ5 = c8.economic(0.0001, c8.COST_DAY)
    check('labels: a 1 bp/day gross confirm is confirm_gross_only (shrunk net negative)',
          c8.decide(True, econ5) == 'confirm_gross_only' and econ5['shrunk_net_annualised'] < 0
          and c8.decide(False, econ5) == 'not_demonstrated')
    bar_edge = 2 * (c8.COST_DAY + 0.03 / 365)
    check('the half-shrunk bar sits at 2 x (cost + 3 %/yr) = 16.2 bp/day gross',
          c8.economic(bar_edge + 1e-9, c8.COST_DAY)['meets_bar'] and not c8.economic(bar_edge - 1e-7, c8.COST_DAY)['meets_bar'],
          round(bar_edge * 1e4, 2))
    halves_bad = [(0.004 if i < 41 else -0.001) + rng.gauss(0, 0.0005) for i in range(len(keys))]
    ok6, num6 = c8.confirm(halves_bad, keys, synth_factors(keys, rng), cells)
    check('a negative second half blocks a confirm (C3)', not ok6 and num6['criteria']['C1'] and not num6['criteria']['C3'])


def survivorship_rule_checks():
    print('\n7b. the survivorship reading rule (reported; r1 m4)')
    base = {'cohort_gross_share_in_dropped': 0.02, 'crowd_gross_share_in_dropped': 0.03}
    one = {'w': {**base, 'dropped_coins_ranked': 1, 'dropped_would_rank': {'long': 3, 'short': 0, 'middle': 1}}}
    check('1 dropped coin ranked long, 0 short -> NOT flagged (minimum 5 coins)', not c8.survivorship_flag(one)['flag'])
    five = {'w': {**base, 'dropped_coins_ranked': 5, 'dropped_would_rank': {'long': 10, 'short': 4, 'middle': 0}}}
    check('5 coins ranked, long 10 >= 2 x short 4 -> flagged', c8.survivorship_flag(five)['flag'])
    share = {'w': {**base, 'cohort_gross_share_in_dropped': 0.12, 'dropped_coins_ranked': 0, 'dropped_would_rank': {}}}
    check('cohort notional share 12 % in dropped coins -> flagged', c8.survivorship_flag(share)['flag'])
    row = {'confirm': {'G': 0, 'crit': None, 'criteria': {'C1': False, 'C2': False, 'C3': False}},
           'economic': {'alpha_gross': None, 'mean_cost_per_day': None, 'meets_bar': False},
           'decision': 'not_demonstrated', 'reported': {'survivorship': five},
           'survivorship_reading': {'flag': False, 'why': []}}
    dec, fails = c8.recheck(row)
    check('recheck catches a stored survivorship reading that does not recompute', any('survivorship' in x for x in fails))


def calibration_checks():
    print('\n7. null calibration and power on synthetic series (registered rule)')
    days = c8.planned_days(END)
    r0 = c8.simulate(days, 0.0, sims=1500, seed=31)
    check('null false-confirm rate within 0.03 .. 0.07', 0.03 <= r0['confirm'] <= 0.07, r0['confirm'])
    aug, sep = 1.24 ** (1 / 31) - 1, 1.08 ** (1 / 30) - 1
    drift = lambda k, w: {'2026-07': -0.003, '2026-08': aug, '2026-09': sep}[c8.month_of(k)]
    rb = c8.simulate(days, 0.0, sims=1500, seed=32, beta=lambda k, w: 0.0 if w == 0 else 0.5, btc_drift=drift,
                     alt_load=0.3, alt_drift=0.002, rev_load=0.5, rev_drift=0.001)
    check('combined null (cohort betas + alt trend + reversal): registered rule <= 0.07, the draft rule is not',
          rb['confirm'] <= 0.07 and rb['draft_rule_btc_only'] > 0.3, (rb['confirm'], rb['draft_rule_btc_only']))
    r1 = c8.simulate(days, c8.DISC_GROSS_INV, sims=1500, seed=33)
    check('power at the full inverted discovery magnitude is ~0.3 (stated in §3)', 0.22 <= r1['confirm'] <= 0.37, r1['confirm'])
    check('simulation is deterministic for a seed', c8.simulate(days, c8.DISC_GROSS_INV, sims=300, seed=33) ==
          c8.simulate(days, c8.DISC_GROSS_INV, sims=300, seed=33))


# ------------------------------------------------------------------ git scaffolding
def git(root, *a):
    r = subprocess.run(['git', *a], cwd=root, capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f'git {a}: {r.stderr}')
    return r.stdout


def sha(p):
    import hashlib
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


REG_TEMPLATE = """# 008 test registration

**Status: {status}**

Some prose.

008 holdout end (owner): {end}

| file | sha256 |
|---|---|
{rows}
"""


def make_repo(tmp, status='REGISTERED', end='2026-09-28', pre_files=()):
    root = Path(tmp) / 'repo'
    (root / 'registrations').mkdir(parents=True)
    (root / 'code').mkdir()
    (root / 'code' / 'a.py').write_text('x = 1\n')
    (root / '.gitignore').write_text('data/\n')
    (root / 'results.jsonl').write_text('')
    pre = {'files': {f: sha(root / f) for f in pre_files}}
    (root / 'registrations' / '008-manifest.json').write_text(json.dumps(pre))
    git(root, 'init', '-q')
    git(root, 'config', 'user.email', 't@t')
    git(root, 'config', 'user.name', 't')
    write_reg(root, status, end)
    git(root, 'add', '-A')
    git(root, 'commit', '-qm', 'init')
    return root


def write_reg(root, status='REGISTERED', end='2026-09-28', extra=''):
    files = ('code/a.py', 'registrations/008-manifest.json')
    rows = '\n'.join(f'| `{f}` | `{sha(root / f)}` |' for f in files)
    (root / 'registrations' / '008-inverted-positioning-holdout.md').write_text(
        REG_TEMPLATE.format(status=status, end=end, rows=rows) + extra)


GK = dict(code=('code/a.py',), data=('registrations/008-manifest.json',))
NOW = ms(2026, 10, 12)


def refused(f, *a, **k):
    try:
        f(*a, **k)
    except SystemExit as e:
        return str(e) or 'exit'
    return None


def guard_checks():
    print('\n8. guard (throwaway git repo)')
    import r008_run as R
    with tempfile.TemporaryDirectory() as tmp:
        root = make_repo(tmp, status='DRAFT')
        f, _ = R.guard_failures(root, 'fetch', now_ms=NOW, **GK)
        check('DRAFT -> fetch refused', any(x.startswith('(s)') for x in f))
        f, _ = R.guard_failures(root, 'run', now_ms=NOW, **GK)
        check('DRAFT -> run refused', any(x.startswith('(s)') for x in f))
    with tempfile.TemporaryDirectory() as tmp:
        root = make_repo(tmp)
        f, ctx = R.guard_failures(root, 'fetch', now_ms=NOW, **GK)
        check('REGISTERED, committed, hashes match -> fetch gate open', f == [] and ctx['end'] == END, f)
        f, _ = R.guard_failures(root, 'fetch', now_ms=ms(2026, 10, 4), **GK)
        check('(t) before end + 7 days -> refused', any(x.startswith('(t)') for x in f))
        f, _ = R.guard_failures(root, 'run', now_ms=NOW, **GK)
        check('run without fetch rows -> (g1) refused', any(x.startswith('(g1)') for x in f))
        (root / 'code' / 'a.py').write_text('x = 2\n')
        f, _ = R.guard_failures(root, 'fetch', now_ms=NOW, **GK)
        check('changed code -> (d) and (e) refused', any(x.startswith('(d)') for x in f) and any(x.startswith('(e)') for x in f))
        git(root, 'checkout', '-q', '--', 'code/a.py')
        import shutil
        for path, label in ((root / 'duckdb.py', 'an untracked duckdb.py'),
                            (root / 'duckdb' / '__init__.py', 'a top-level package duckdb/'),
                            (root / 'ct' / 'stats' / '__init__.py', 'a ct/stats/ package (beats ct/stats.py)'),
                            (root / 'pyarrow.pyc', 'sourceless pyarrow.pyc'),
                            (root / 'json.py', 'a git-IGNORED json.py')):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('# shadow\n')
            if path.name == 'json.py':
                (root / '.git' / 'info' / 'exclude').write_text('json.py\n')
            f, _ = R.guard_failures(root, 'fetch', now_ms=NOW, **GK)
            check(f'B-m11 / r1 m1: {label} -> (d) refused', any('shadow an import' in x for x in f))
            path.unlink()
            if path.parent != root and not any(path.parent.iterdir()):
                shutil.rmtree(path.parent)
        if (root / 'ct').exists():
            shutil.rmtree(root / 'ct')
        (root / 'results.jsonl').write_text('{"x": 1}\n')
        f, _ = R.guard_failures(root, 'fetch', now_ms=NOW, **GK)
        check('B-m1: results.jsonl differing from HEAD -> (k) refused', any(x.startswith('(k)') for x in f))
        (root / 'results.jsonl').write_text('')
        reg = root / 'registrations' / '008-inverted-positioning-holdout.md'
        txt = reg.read_text()
        reg.write_text(txt + '\n008 holdout end (owner): 2026-11-23\n')
        f, _ = R.guard_failures(root, 'fetch', now_ms=NOW, **GK)
        check('two owner lines -> (n) and (b) refused', any(x.startswith('(n)') for x in f) and any(x.startswith('(b)') for x in f))
        reg.write_text(txt.replace('2026-09-28', '2026-12-31'))
        f, _ = R.guard_failures(root, 'fetch', now_ms=NOW, **GK)
        check('an end that is not offered -> (n) refused', any(x.startswith('(n)') for x in f))
        row = [l for l in txt.splitlines() if l.startswith('| `code/a.py`')][0]
        reg.write_text(txt + row.replace(sha(root / 'code' / 'a.py'), '0' * 64) + '\n')
        f, _ = R.guard_failures(root, 'fetch', now_ms=NOW, **GK)
        check('B-m12: a file listed twice in the hash table -> (e) refused', any('more than once' in x for x in f))
        reg.write_text(txt)
        (root / 'results.jsonl').write_text(json.dumps({'registration': R.REG_NAME, 'mode': 'holdout_started'}) + '\n')
        git(root, 'commit', '-qam', 'a started row')
        (root / 'results.jsonl').write_text('')
        git(root, 'commit', '-qam', 'row deleted')
        f, _ = R.guard_failures(root, 'fetch', now_ms=NOW, **GK)
        check('(f) a holdout_started row deleted but in history still blocks', any(x.startswith('(f)') for x in f))
    with tempfile.TemporaryDirectory() as tmp:
        root = make_repo(tmp)
        (root / 'results.jsonl').write_text(json.dumps({'registration': R.REG_NAME, 'mode': 'holdout'}) + '\n')
        git(root, 'commit', '-qam', 'a holdout row')
        git(root, 'reset', '-q', '--hard', 'HEAD~1')
        f, _ = R.guard_failures(root, 'fetch', now_ms=NOW, **GK)
        check('B-m1: a holdout row removed with reset --hard is still found through the reflog', any(x.startswith('(f)') for x in f))
    with tempfile.TemporaryDirectory() as tmp:
        root = make_repo(tmp)
        git(root, 'checkout', '-qb', 'side')
        (root / 'results.jsonl').write_text(json.dumps({'registration': R.REG_NAME, 'mode': 'holdout'}) + '\n')
        git(root, 'commit', '-qam', 'side row')
        git(root, 'checkout', '-q', '-')
        f, _ = R.guard_failures(root, 'fetch', now_ms=NOW, **GK)
        check('(f) a holdout row on another branch blocks', any(x.startswith('(f)') for x in f))
    with tempfile.TemporaryDirectory() as tmp:
        root = make_repo(tmp)
        (root / 'data').mkdir()
        (root / 'data' / 'd.bin').write_bytes(b'abc')
        pre = {'files': {'data/d.bin': sha(root / 'data' / 'd.bin')}}
        (root / 'registrations' / '008-manifest.json').write_text(json.dumps(pre))
        write_reg(root)
        git(root, 'add', '-A')
        git(root, 'commit', '-qm', 'manifest')
        f, _ = R.guard_failures(root, 'fetch', now_ms=NOW, **GK)
        check('pre-registration manifest matches -> open', f == [], f)
        (root / 'data' / 'd.bin').write_bytes(b'abd')
        f, _ = R.guard_failures(root, 'fetch', now_ms=NOW, **GK)
        check('a manifest file changed -> (e) refused', any('manifest files do not match' in x for x in f))


def bypass_checks():
    print('\n9. B-B1: the real data tree and the real network cannot be reached through a throwaway repo')
    import r008_fetch as F
    import r008_run as R
    real = HERE / 'data'
    with tempfile.TemporaryDirectory() as tmp:
        root = make_repo(tmp)
        msg = refused(R.run, root, real, now_ms=NOW, offline=False, **GK)
        check('run(root=<throwaway, gate open>, data_dir=<real tree>) refuses before any read', msg and '(B1)' in msg, (msg or '')[:80])
        msg = refused(R.run, R.HERE, real, code=())
        check('run(root=<real repo>, data_dir=<real tree>, an overridden guard argument) refuses', msg and '(B1)' in msg)
        msg = refused(F.fetch, root, root / 'data', now_ms=NOW, **GK)
        check('fetch with the real hooks on a throwaway repo refuses before any request', msg and '(B1)' in msg)
        partial = {k: (lambda *a, **kw: None) for k in F.HOOK_NAMES if k != 's3_plan'}
        msg = refused(F.fetch, root, root / 'data', now_ms=NOW, hooks=partial, **GK)
        check('a partial mock dict refuses (no fallback to a real requester-pays hook)', msg and 'mock hooks must supply' in msg)
        full = {k: (lambda *a, **kw: None) for k in F.HOOK_NAMES}
        msg = refused(F.fetch, root, real, now_ms=NOW, hooks=full, **GK)
        check('mock hooks writing into the real tree from a throwaway repo refuse', msg and '(B1)' in msg)
        saved = R._GATE['pair']
        R._GATE['pair'] = (root.resolve(), (root / 'data').resolve())
        msg = refused(R.compute, real, END, (), [])
        R._GATE['pair'] = saved
        check("compute() on the real tree refuses when the gate passed for another (root, data) pair", msg and 'read lock' in msg)
        stand = make_repo(Path(tmp) / 'stand')
        (stand / 'data').mkdir()
        sh, sd = R.HERE, R.REAL_DATA
        R.HERE, R.REAL_DATA = stand.resolve(), (stand / 'data').resolve()
        try:
            msg = refused(F.fetch, stand, stand / 'data', hooks=full)
        finally:
            R.HERE, R.REAL_DATA = sh, sd
        check('r1 M1: mock hooks on the real tree refuse even from the real repository (stand-in), before any row',
              msg and 'mock network hooks' in msg and (stand / 'results.jsonl').read_text() == '')
        F._REAL['ok'] = False
        msg = refused(F._s3_plan, 'k', 1)
        check('a real hook called directly (outside the guarded fetch) refuses', msg and 'real network hook' in msg)
        F._REAL['ok'] = True
        msg = refused(F._s3_get_columns, 'k', 1, root / 'data')
        F._REAL['ok'] = False
        check('the archive data-directory check is a refusal, not an assert (survives -O)', msg and 'research/copytrade/data' in msg)


# ------------------------------------------------------------------ 10. synthetic data tree for fetch and e2e
COINS = [f'C{i}' for i in range(12)]
SYMS = [f'{c}USDT' for c in COINS]
WALLETS = [f'0x{i:040x}' for i in range(100)]


def btc_close(h):
    return 100_000 * (1 + 0.2 * math.sin(2 * math.pi * h / (50 * D)))


def build_tree(data):
    """A complete synthetic data/ tree for the 2026-09-28 end, except the holdout fills (the fetch 'pulls'
    them through the mocked S3 hook) and 008's store. 'DX' is a coin the table marks delisted_on_hl."""
    rng = random.Random(77)
    (data / 'p0/2026-10-07').mkdir(parents=True)
    table = {c: {'status': 'ok', 'symbol': f'{c}USDT', 'qty_mult': 1.0} for c in COINS}
    table['BTC'] = {'status': 'ok', 'symbol': 'BTCUSDT', 'qty_mult': 1.0}
    table['DX'] = {'status': 'delisted_on_hl'}
    (data / 'p0/2026-10-07/symbols.json').write_text(json.dumps(table))
    (data / 'p0/2026-10-07/report.json').write_text(json.dumps({'wallets': [{'user': w} for w in WALLETS[::9]]}))
    (data / 'derived/regimes').mkdir(parents=True)
    (data / 'derived/hyperliquid').mkdir(parents=True)
    (data / 'derived/bitget_listing.json').write_text(json.dumps({s: ms(2025, 1, 1) for s in SYMS + ['BTCUSDT']}))
    (data / 'cache/hl-vaults').mkdir(parents=True)
    (data / 'cache/hl-vaults/v.json').write_text(json.dumps([{'summary': {'vaultAddress': '0xVAULT'}}]))
    hs = list(range(ms(2025, 12, 1), ms(2026, 10, 10), H))
    pq.write_table(pa.table({'ts': pa.array(hs, pa.int64()), 'open': [btc_close(h) for h in hs],
                             'high': [btc_close(h) for h in hs], 'low': [btc_close(h) for h in hs],
                             'close': [btc_close(h) for h in hs]}), data / 'derived/regimes/btc_1h.parquet')
    acc = {'snapshot_ms': [], 'date': [], 'user': [], 'account_value': [], 'n_positions': [], 'gross_notional': [],
           'net_notional': []}
    d = dt.date(2026, 3, 1)
    while d < dt.date(2026, 9, 29):
        for w in WALLETS:
            acc['snapshot_ms'].append(c8.ms(d) + 30_000)
            acc['date'].append(d.isoformat())
            acc['user'].append(w)
            acc['account_value'].append(50_000.0)
            acc['n_positions'].append(3)
            acc['gross_notional'].append(50_000.0)
            acc['net_notional'].append(0.0)
        d += dt.timedelta(days=1)
    pq.write_table(pa.table({**acc, 'snapshot_ms': pa.array(acc['snapshot_ms'], pa.int64())}),
                   data / 'derived/hyperliquid/snapshot_accounts.parquet')
    base = data / 'archive/by_dex/hyperliquid/snapshots/perp'
    for day in [dt.date(2026, 6, 30)] + [dt.date(2026, 7, 1) + dt.timedelta(days=i) for i in range(30)] + \
               [dt.date(2026, 8, 3) + dt.timedelta(days=i) for i in range(53)]:
        write_snapshot(base / f'date={day.isoformat()}' / f'{day.toordinal()}_{c8.ms(day) + 9_000}.parquet',
                       snapshot_rows(WALLETS, COINS + ['DX'], rng))
    fills = synthetic_fills(rng)
    for day, rows in fills.items():
        if day < '2026-07-01':
            write_fill_day(data, day, rows)
    return fills


def synthetic_fills(rng):
    """{day: [row]}: each wallet does ~2-day round trips, one coin at a time, all taker, with a wallet-specific
    mean return (so scores differ), 2026-03-05 .. 2026-08-02."""
    out, tid = {}, 1
    for i, w in enumerate(WALLETS):
        mu = (i - 50) * 0.0005
        t = ms(2026, 3, 5) + (i % 40) * 3 * H
        n = 0
        while t < ms(2026, 8, 1):
            coin = COINS[(i + n) % len(COINS)]
            long = rng.random() < 0.5
            q, p0 = 100.0, 100.0
            r = rng.gauss(mu, 0.02)
            p1 = p0 * (1 + r) if long else p0 * (1 - r)
            tc = t + 30 * H + rng.randrange(0, 20) * H
            for tt, side, sp, px, pnl, dirn in ((t, 'buy' if long else 'sell', 0.0, p0, 0.0, 'Open'),
                                                (tc, 'sell' if long else 'buy', q if long else -q, p1, (p1 - p0) * q * (1 if long else -1), 'Close')):
                day = dt.datetime.fromtimestamp(tt / 1000, dt.UTC).date().isoformat()
                out.setdefault(day, []).append((w, coin, tt, side, q, px, sp, pnl, tid, f'{dirn} {"Long" if long else "Short"}'))
                tid += 1
            t = tc + (10 + rng.randrange(0, 30)) * H
            n += 1
    return out


def write_fill_day(data, day, rows):
    p = data / 'pruned/by_dex/hyperliquid/fills/perp/all' / f'date={day}' / 'fills.parquet'
    p.parent.mkdir(parents=True, exist_ok=True)
    cols = list(zip(*sorted(rows, key=lambda r: r[2]))) or [()] * 10
    pq.write_table(pa.table({
        'address': pa.array(cols[0], pa.string()), 'coin': pa.array(cols[1], pa.string()),
        'timestamp': pa.array(cols[2], pa.timestamp('ms', tz='UTC')), 'side': pa.array(cols[3], pa.string()),
        'size': pa.array(cols[4], pa.float64()), 'price': pa.array(cols[5], pa.float64()),
        'start_position': pa.array(cols[6], pa.float64()), 'realized_pnl': pa.array(cols[7], pa.float64()),
        'trade_id': pa.array(cols[8], pa.uint64()), 'direction': pa.array(cols[9], pa.string()),
        'twap_id': pa.array([None] * len(cols[0]), pa.uint64()), 'is_liquidation': pa.array([False] * len(cols[0])),
        'crossed': pa.array([True] * len(cols[0])), 'dex': pa.array(['hyperliquid'] * len(cols[0]))}), p)


class Hooks:
    """Mocked network: S3 listing/plan/get, Bitget candles and contracts, HL funding and meta. Records calls;
    reports requester-pays bytes through r008_fetch.count as a real hook would (before it may raise)."""

    def __init__(self, data, fills, market, plan_size=100 << 20, fail=None, empty=None, missing_day=None, truncate=(),
                 not_listed=(), raise_after_transfer=None, hole=None):
        self.data, self.fills, self.mk = data, fills, market
        self.plan_size, self.fail, self.empty, self.missing_day = plan_size, fail or set(), empty or set(), missing_day
        self.truncate, self.not_listed, self.raise_after = set(truncate), set(not_listed), raise_after_transfer
        self.hole = hole or set()
        self.calls = {'s3_list': 0, 's3_get': [], 'bg': [], 'hl': []}

    def s3_list(self, prefix):
        self.calls['s3_list'] += 1
        d, out = dt.date(2025, 7, 28), []
        while d <= dt.date(2026, 10, 11):
            if d.isoformat() != self.missing_day:
                out.append((f'{prefix}date={d.isoformat()}/fills.parquet', 300 << 20))
            d += dt.timedelta(days=1)
        return out

    def s3_plan(self, key, size):
        import r008_fetch as F
        F.count(4096)
        return (self.plan_size, size, 4096)

    def s3_get_columns(self, key, size, data_dir):
        import r008_fetch as F
        day = key.split('date=')[1].split('/')[0]
        self.calls['s3_get'].append(day)
        F.count(self.plan_size)
        if day == self.raise_after:
            raise RuntimeError(f'synthetic: {day} transferred, then the row-count check failed')
        write_fill_day(Path(data_dir), day, self.fills.get(day, []))
        return self.plan_size

    def s3_get_object(self, key, size, data_dir):
        raise AssertionError('no snapshot pull is needed for the 2026-09-28 end')

    def bg_candles(self, sym, start, end):
        from r008_fetch import FetchError
        self.calls['bg'].append((sym, start))
        if (sym, start) in self.fail or sym in self.not_listed:
            raise FetchError(f'synthetic 500 {sym} {start}')
        if (sym, start) in self.empty:
            return []
        return [self.mk.candle_1h(sym, h) for h in range(start, end + 1, H)
                if self.mk.candle_1h(sym, h) and (sym, h) not in self.hole]

    def bg_contracts(self):
        return [s for s in SYMS + ['BTCUSDT'] if s not in self.not_listed]

    def hl_funding(self, coin, start, end):
        from r008_fetch import FetchError
        self.calls['hl'].append((coin, start))
        if ('hl', coin) in self.fail:
            raise FetchError(f'synthetic HL error {coin}')
        stop = c8.ms(END) - 3 * D if coin in self.truncate else end     # a silent short answer, no error
        rows = [{'time': t, 'fundingRate': str(r)} for t, r in self.mk.funding[f'{coin}USDT' if coin != 'BTC' else 'BTCUSDT']
                if start <= t < min(end, stop)]
        return rows[:500]

    def hl_meta(self):
        return [{'name': c} for c in COINS + ['BTC']]

    def as_dict(self):
        return {k: getattr(self, k) for k in ('s3_list', 's3_plan', 's3_get_columns', 's3_get_object', 'bg_candles',
                                               'bg_contracts', 'hl_funding', 'hl_meta')}


def rows_of(root):
    return [json.loads(l) for l in (root / 'results.jsonl').read_text().splitlines()]


def attempt(F, root, data, hk, **kw):
    try:
        F.fetch(root, data, now_ms=NOW, hooks=hk.as_dict(), **GK, **kw)
        return None
    except F.FetchError as e:
        return str(e)


def fetch_and_e2e_checks():
    print('\n10. fetch (mocked network) and the end-to-end read (throwaway repo, synthetic data tree)')
    import r008_fetch as F
    import r008_run as R
    mk = Market(SYMS + ['BTCUSDT'], F.PRICE_START, c8.ms(END) + 5 * D, seed=13)
    with tempfile.TemporaryDirectory() as tmp:
        root = make_repo(tmp, status='DRAFT')
        hk = Hooks(root / 'data', {}, mk)
        msg = refused(F.fetch, root, root / 'data', now_ms=NOW, hooks=hk.as_dict(), **GK)
        check('DRAFT: the fetch refuses before any request, and writes no row',
              msg and hk.calls['s3_list'] == 0 and not hk.calls['bg'] and (root / 'results.jsonl').read_text() == '')
        msg = refused(R.run, root, root / 'data', now_ms=NOW, offline=False, **GK)
        check('DRAFT: the run refuses and writes no row (no outcome)', msg and (root / 'results.jsonl').read_text() == '')
    with tempfile.TemporaryDirectory() as tmp:
        root = make_repo(tmp)
        data = root / 'data'
        fills = build_tree(data)
        hk = Hooks(data, fills, mk, missing_day='2026-07-15')
        e = attempt(F, root, data, hk)
        check('a fill day missing from the listing -> FetchError, no download; the started row is COMMITTED at once',
              e and 'not listed' in e and not hk.calls['s3_get'] and [r['mode'] for r in rows_of(root)] == ['holdout_fetch_started', 'holdout_fetch_attempt']
              and git(root, 'status', '--porcelain', '--', 'results.jsonl') == '')
        hk = Hooks(data, fills, mk, plan_size=600 << 20)
        e = attempt(F, root, data, hk)
        check('33 days x 600 MiB > 15 GiB -> refused before any download', e and '15 GiB cap' in e and not hk.calls['s3_get'])
        hk = Hooks(data, fills, mk, raise_after_transfer='2026-07-10')
        try:
            F.fetch(root, data, now_ms=NOW, hooks=hk.as_dict(), **GK)
            raised = False
        except RuntimeError:
            raised = True
        ends = [r for r in rows_of(root) if r['mode'] == 'holdout_fetch_attempt' and r['phase'] == 'end']
        check('B-M2 / r1 m2: a transfer that raises still counts: its COMMITTED end row holds footers + 10 days of bytes',
              raised and ends[-1]['bytes'] == 33 * 4096 + 10 * (100 << 20)
              and git(root, 'status', '--porcelain', '--', 'results.jsonl') == '', ends[-1]['bytes'])
        before = F.spent_bytes(F.attempt_history(root))
        lines = (root / 'results.jsonl').read_text().splitlines()
        edited = [l if '"phase": "end"' not in l else json.dumps({**json.loads(l), 'bytes': 0}, sort_keys=True) for l in lines]
        (root / 'results.jsonl').write_text('\n'.join(edited) + '\n')
        git(root, 'commit', '-qam', 'tamper: lower the bytes of the attempt rows')
        check('r1 m2: editing committed attempt rows cannot lower the cumulative bytes (the history keeps the originals)',
              F.spent_bytes(F.attempt_history(root)) == before, (before, F.spent_bytes(F.attempt_history(root))))
        (root / 'results.jsonl').write_text('\n'.join(lines) + '\n')
        git(root, 'commit', '-qam', 'restore')
        killed = R.append_row(root, {'registration': R.REG_NAME, 'mode': 'holdout_fetch_attempt', 'attempt': 99,
                                     'phase': 'start', 'reserved': 7})
        R.commit_results(root, 'a killed attempt: start row, no end row')
        check('r1 m2: an attempt killed before its end row counts its reservation',
              F.spent_bytes(F.attempt_history(root)) == before + 7)
        b_fail = F.blocks(F.PRICE_START, c8.ms(END))[3]
        hk = Hooks(data, fills, mk, fail={('C2USDT', b_fail)})
        e = attempt(F, root, data, hk)
        check('a Bitget error on a LIVE symbol: keyed refusal, the attempt continues, nothing cached for the block',
              e and 'closes_error:C2USDT' in e and not (data / 'r008/bitget_1h/C2USDT' / f'{b_fail}.json').exists()
              and any(s == 'C9USDT' for s, _ in hk.calls['bg']))
        check('the remaining 24 holdout fill days were pulled (resume skips the 9 complete days; 07-10 is re-asked)', len(hk.calls['s3_get']) == 24,
              len(hk.calls['s3_get']))
        check('P3 G1-G2 ran on the 33 pulled days and G3 on a sample (mechanics only)',
              json.loads((data / 'r008/p3_gates.json').read_text())['g1_g2_pass'])
        check('a sentinel README marks the holdout days in the shared partition root',
              (data / 'pruned/by_dex/hyperliquid/fills/perp/all/HOLDOUT_DAYS_008_README.txt').exists())
        hk = Hooks(data, fills, mk, empty={('C2USDT', b_fail)})
        e = attempt(F, root, data, hk)
        check('an empty interior block for a live symbol -> not cached, coverage refuses (re-fetch)',
              e and 'closes_interior:C2USDT' in e and not (data / 'r008/bitget_1h/C2USDT' / f'{b_fail}.json').exists())
        for f in (data / 'r008/funding').glob('*.json'):
            f.unlink()
        hk = Hooks(data, fills, mk, fail={('hl', 'C4')})
        e = attempt(F, root, data, hk)
        check('an HL funding error on a listed coin: keyed refusal, nothing cached', e and 'funding_error:C4' in e
              and not (data / 'r008/funding/C4.json').exists())
        pages = [s for c, s in hk.calls['hl'] if c == 'C0']
        check('funding is paged forward (start times strictly increase)', pages == sorted(set(pages)) and len(pages) >= 2, len(pages))
        cut = c8.ms(END) - 3 * D
        saved_c = {h: v for h, v in mk.closes['C7USDT'].items() if h >= cut}
        for h in saved_c:
            del mk.closes['C7USDT'][h]
        for f in (data / 'r008/bitget_1h/C7USDT').glob('*.json'):
            f.unlink()
        e = attempt(F, root, data, Hooks(data, fills, mk))
        check('tail: a live symbol whose closes stop before the end -> refused, its partial last block deleted',
              e and 'closes_tail:C7USDT' in e and not (data / 'r008/bitget_1h/C7USDT' / f'{cut - cut % F.HOUR_BLOCK}.json').exists())
        mk.closes['C7USDT'].update(saved_c)
        (data / 'r008/funding/C5.json').unlink(missing_ok=True)
        e = attempt(F, root, data, Hooks(data, fills, mk, truncate={'C5'}))
        check('a silent short funding answer -> refused and not kept', e and 'funding_tail:C5' in e
              and not (data / 'r008/funding/C5.json').exists())
        # B-B2: a symbol Bitget no longer lists, and a persistent partial block on a live one
        for f in (data / 'r008/bitget_1h/C8USDT').glob('*.json'):
            f.unlink()
        hole_h = F.blocks(F.PRICE_START, c8.ms(END))[5] + 7 * H
        for f in (data / 'r008/bitget_1h/C9USDT').glob('*.json'):
            f.unlink()
        import os
        key = f'closes_partial:C9USDT:{hole_h - hole_h % F.HOUR_BLOCK}'
        t0 = dt.datetime.now(dt.UTC) + dt.timedelta(hours=1)
        backdated = []
        for gap in (0, 25, 50):                   # claimed 24 h gaps, real commit times seconds apart
            hk = Hooks(data, fills, mk, not_listed={'C8USDT'}, hole={('C9USDT', hole_h)})
            backdated.append(attempt(F, root, data, hk, now=t0 + dt.timedelta(hours=gap)))
        check('r1 m2: a back-dated clock does not make a refusal persistent (the chain uses commit times)',
              all(m and key in m for m in backdated))
        for name in ('C0USDT', 'C1USDT', 'C3USDT', 'C4USDT', 'C5USDT', 'C6USDT', 'C7USDT', 'C10USDT', 'C11USDT'):
            for f in (data / 'r008/bitget_1h' / name).glob('*.json'):
                f.unlink()
        e = attempt(F, root, data, Hooks(data, fills, mk, not_listed={'C0USDT', 'C1USDT', 'C3USDT', 'C4USDT', 'C5USDT',
                                                                      'C6USDT', 'C7USDT', 'C8USDT', 'C10USDT', 'C11USDT'},
                                         hole={('C9USDT', hole_h)}))
        check('r1 m2: more than 8 names (~5 % of 168) becoming unavailable stops the fetch for the orchestrator',
              e and 'unavailable_ceiling' in e)
        msgs = []
        t1 = dt.datetime.now(dt.UTC) + dt.timedelta(days=3)
        for gap in (0, 25):                       # today's committed attempts are link 1; t1 link 2; t1 + 25 h link 3
            when = t1 + dt.timedelta(hours=gap)
            os.environ['GIT_COMMITTER_DATE'] = os.environ['GIT_AUTHOR_DATE'] = when.strftime('%Y-%m-%dT%H:%M:%S+0000')
            hk = Hooks(data, fills, mk, not_listed={'C8USDT'}, hole={('C9USDT', hole_h)})
            msgs.append(attempt(F, root, data, hk, now=when))
        os.environ.pop('GIT_COMMITTER_DATE')
        os.environ.pop('GIT_AUTHOR_DATE')
        check('B-B2: a partial block on a live symbol refuses while the committed chain is short (re-asked: not kept)',
              msgs[0] and key in msgs[0], [m[:60] if m else m for m in msgs])
        fr = [r for r in rows_of(root) if r['mode'] == 'holdout_fetched']
        check('... and once 3 attempts are COMMITTED >= 24 h apart it is recorded unavailable (persistent); the fetch completes',
              msgs[1] is None and len(fr) == 1 and key in fr[0]['unavailable']['persistent_after_3_attempts'])
        check('B-B2: a symbol Bitget no longer lists (errors) is recorded, never a stop',
              'C8USDT' in fr[0]['unavailable']['bitget_not_listed_now'])
        total = F.spent_bytes(F.attempt_history(root))
        check('B-M2: the fetched row reports every requester-pays byte of every attempt (footers, reservations included)',
              fr[0]['bytes_requester_pays'] == total and total == 34 * (100 << 20) + 90 * 4096 + 7, total)
        msg = refused(F.fetch, root, data, now_ms=NOW, hooks=Hooks(data, fills, mk).as_dict(), **GK)
        check('a second fetch after the fetched row is refused', msg and 'runs once' in msg)
        check('every fetch row was committed as it was written (results.jsonl clean)',
              git(root, 'status', '--porcelain', '--', 'results.jsonl') == '')
        # the run gate
        f, _ = R.guard_failures(root, 'run', now_ms=NOW, **GK)
        check("run with no approval -> (h') refused", any(x.startswith("(h')") for x in f))
        fetched_line = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"holdout_fetched"' in l][0]
        write_reg(root, extra=f'\n008 holdout read: APPROVED 2026-10-12 row {"0" * 64}\n')
        git(root, 'commit', '-qam', 'wrong approval')
        f, _ = R.guard_failures(root, 'run', now_ms=NOW, **GK)
        check("an approval quoting another row's sha -> (h') refused", any(x.startswith("(h')") for x in f))
        write_reg(root, extra=f'\n008 holdout read: APPROVED 2026-10-12 row {R.line_sha(fetched_line)}\nmore prose\n')
        git(root, 'commit', '-qam', 'approval plus prose')
        f, _ = R.guard_failures(root, 'run', now_ms=NOW, **GK)
        check('(r) prose added after the fetch -> refused', any(x.startswith('(r)') for x in f))
        write_reg(root, extra=f'\n008 holdout read: APPROVED 2026-10-12 row {R.line_sha(fetched_line)}\n')
        git(root, 'commit', '-qam', 'approval')
        f, _ = R.guard_failures(root, 'run', now_ms=NOW, **GK)
        check('approval committed after the fetched row -> run gate open', f == [], f)
        store_file = next((data / 'r008/funding').glob('*.json'))
        keep = store_file.read_bytes()
        store_file.write_bytes(keep + b' ')
        f, _ = R.guard_failures(root, 'run', now_ms=NOW, **GK)
        check('(g3) a fetched file changed after the fetch -> refused', any(x.startswith('(g3)') for x in f))
        store_file.write_bytes(keep)
        inj = data / 'archive/by_dex/hyperliquid/snapshots/perp/date=2026-07-31' / f'1_{ms(2026, 7, 30, 12)}.parquet'
        write_snapshot(inj, snapshot_rows(WALLETS, COINS, random.Random(5)))
        f, _ = R.guard_failures(root, 'run', now_ms=NOW, **GK)
        check('B-M1: an injected snapshot (unlisted directory, in-range timestamp) -> (g3) refused',
              any('pinned' in x for x in f))
        inj.unlink()
        inj.parent.rmdir()
        inj2 = data / 'archive/by_dex/hyperliquid/snapshots/perp/date=2026-08-10' / f'2_{ms(2026, 8, 10, 1)}.parquet'
        write_snapshot(inj2, snapshot_rows(WALLETS, COINS, random.Random(6)))
        f, _ = R.guard_failures(root, 'run', now_ms=NOW, **GK)
        check('B-M1: a second file in a needed date directory -> (g3) refused', any('exactly one required' in x for x in f))
        inj2.unlink()
        # a mechanical stop after holdout_started (simulated): blocked until a later committed restart line
        crashed = R.append_row(root, {'registration': R.REG_NAME, 'mode': 'holdout_started', 'registration_commit': 'x',
                                      'run_at': 'simulated crash'})
        R.commit_results(root, 'simulated crash row')
        f, _ = R.guard_failures(root, 'run', now_ms=NOW, **GK)
        check('a holdout_started row without a restart line -> (f) refused', any(x.startswith('(f)') for x in f))
        reg_extra = f'\n008 holdout read: APPROVED 2026-10-12 row {R.line_sha(fetched_line)}\n'
        write_reg(root, extra=reg_extra + f'008 holdout restart: APPROVED 2026-10-13 started-row {R.line_sha(crashed)}\n')
        git(root, 'commit', '-qam', 'restart line after the row')
        f, _ = R.guard_failures(root, 'run', now_ms=NOW, **GK)
        check('a restart line committed after the started row -> gate open; (r) accepts it', f == [], f)
        # the read
        row = R.run(root, data, now_ms=NOW, offline=True, **GK)
        modes = [r['mode'] for r in rows_of(root)]
        modes = [m for m in modes if m != 'holdout_fetch_attempt']
        check('the read commits holdout_started, then exactly one holdout row (restart counted)',
              modes == ['holdout_fetch_started', 'holdout_fetched', 'holdout_started', 'holdout_started', 'holdout']
              and row['restarts_after_mechanical_stop'] == 1 and git(root, 'status', '--porcelain', '--', 'results.jsonl') == '',
              modes)
        check('the read ran offline (lock active) and produced a recomputable decision',
              c8.recheck(rows_of(root)[-1]) == (row['decision'], []))
        cnt = row['counts']
        check('cohorts of 20 in both windows (100 scored wallets), no window excluded; 83 signal days, June excluded',
              [w['cohort'] for w in cnt['windows']] == [20, 20] and not any(w['excluded'] for w in cnt['windows'])
              and cnt['signal_days'] == 83, [(w['cohort'], w['median_signal_coins']) for w in cnt['windows']])
        cf = row['confirm']
        check('the confirm regression has 3 cells x (BTC, alt, reversal) loadings (k = 10)', cf['k'] == 10 and len(cf['cells']) == 3,
              cf['cells'])
        rep = row['reported']
        sv = rep['survivorship']
        check("survivorship (reported): DX (delisted_on_hl) counted by status, its ranking and the groups' notional share",
              all(v['dropped_coin_days_by_status'].get('delisted_on_hl', 0) > 0 and v['cohort_gross_share_in_dropped'] > 0
                  for v in sv.values()) and set(sv) == {'2026-06-08', '2026-08-03'})
        check('P0 (reported): count and gross-notional share per cohort, and the confirm without P0 wallets',
              all('cohort_gross_share' in v for k, v in rep['p0'].items() if k != 'confirm_without_p0_wallets')
              and rep['p0']['confirm_without_p0_wallets'] is not None)
        check('row carries env fingerprint and the economic bar on the half-shrunk alpha',
              row['env'] == R.env_fingerprint() and 'shrunk_net_annualised' in row['economic'])
        body = R.compute(data, END, [w for w in WALLETS[::9]], [root / p for p in json.loads((data / 'r008/manifest.json').read_text())['snapshots']])
        same = json.loads(json.dumps(body, sort_keys=True, default=str)) == \
            json.loads(json.dumps({k: row[k] for k in ('counts', 'confirm', 'economic', 'decision', 'reported', 'survivorship_reading')}, sort_keys=True, default=str))
        if not same:
            a = json.loads(json.dumps(body, sort_keys=True, default=str))
            b = json.loads(json.dumps({k: row[k] for k in ('counts', 'confirm', 'economic', 'decision', 'reported', 'survivorship_reading')}, sort_keys=True, default=str))
            def diff(x, y, path=''):
                if isinstance(x, dict) and isinstance(y, dict):
                    for k in set(x) | set(y):
                        diff(x.get(k), y.get(k), f'{path}/{k}')
                elif x != y:
                    print('   DIFF', path, str(x)[:80], str(y)[:80])
            diff(a, b)
        check('determinism: a second computation on the same files gives the same numbers', same)
        old = (c8.MIN_COHORT, )
        c8.MIN_COHORT = 1000
        body2 = R.compute(data, END, [], [root / p for p in json.loads((data / 'r008/manifest.json').read_text())['snapshots']])
        c8.MIN_COHORT = old[0]
        check('A-m6: windows under the cohort floor are excluded and the decision is not_demonstrated',
              all(w['excluded'] for w in body2['counts']['windows']) and body2['decision'] == 'not_demonstrated')
        msg = refused(R.run, root, data, now_ms=NOW, offline=True, **GK)
        check('a second read is refused (f)', msg and '(f)' in msg and [r['mode'] for r in rows_of(root)].count('holdout') == 1)
        git(root, 'reset', '-q', '--hard', 'HEAD~1')
        msg = refused(R.run, root, data, now_ms=NOW, offline=True, **GK)
        check('B-m1: the holdout row removed by reset --hard still blocks (reflog)', msg and '(f)' in msg)


def recheck_failure_check():
    print('\n11. a decision that does not recompute is stored, not printed')
    import r008_run as R
    import io
    import contextlib
    row = {'confirm': {'G': 13, 'crit': 1.0, 'criteria': {'C1': True, 'C2': True, 'C3': True}, 'alpha': 0.01,
                       't_alpha': 3.0, 'median_adj': 0.01, 'halves_adj': [0.01, 0.01]},
           'economic': {'alpha_gross': 0.01, 'mean_cost_per_day': c8.COST_DAY, 'meets_bar': True},
           'decision': 'confirm_gross_and_shrunk_net'}
    dec, fails = c8.recheck(row)
    check('a wrong stored critical value is caught by recheck', fails and dec == 'confirm_gross_and_shrunk_net')
    src = Path(R.__file__).read_text()
    i = src.index("if fails or dec != row['decision']:")
    block = src[i:src.index("append_row(root, row)\n    commit_results(root, '008: holdout row (automatic", i)]
    check('run(): on a recheck failure the row is appended with recheck_failed and the exit text prints no decision',
          "row['recheck_failed']" in block and 'print(' not in block and "row['decision']" not in block.split('raise SystemExit')[1])


# ------------------------------------------------------------------ 12. the real registration file (parse only)
def real_file_checks():
    print('\n12. the read lock and the real registration text (parse only; nothing that goes stale at REGISTERED)')
    import r008_run as R
    saved = R._GATE['pair']
    R._GATE['pair'] = None
    msg = refused(R.compute, HERE / 'data', END, (), [])
    R._GATE['pair'] = saved
    check('compute() refuses the real data tree outside run() (before opening any file)', msg and 'read lock' in msg)
    reg = HERE / R.REG_REL
    text = reg.read_text()
    end, fails = R.owner_end(text)
    check('exactly one anchored owner line with an offered end', end in c8.OFFERED_ENDS.values() and not fails, end)
    check('no line starts with an approval or restart phrase', not R.APPROVAL.search(text) and not R.RESTART.search(text))
    rows = R.HASH_ROW.findall(text)
    table = dict(rows)
    missing = [f for f in (*R.GUARDED_CODE, *R.GUARDED_DATA) if f not in table]
    check('every guarded file has exactly one row in the §11 hash table', not missing and len(rows) == len(table), missing)
    code_ok = [f for f in R.GUARDED_CODE if R.sha256(HERE / f) != table.get(f)]
    check('the code hashes in §11 equal the files on disk', not code_ok, code_ok)


if __name__ == '__main__':
    calendar_checks()
    stats_checks()
    equivalence_checks()
    cohort_checks()
    factor_checks()
    decision_checks()
    survivorship_rule_checks()
    calibration_checks()
    guard_checks()
    bypass_checks()
    fetch_and_e2e_checks()
    recheck_failure_check()
    real_file_checks()
    print(f'\nall checks passed ({N_CHECKS[0]})')
