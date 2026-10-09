"""Prepare-phase checks for registration 001 — SYNTHETIC data only.

No real wallet is scored and no real copy outcome is computed here: the
scores and statistics run on hand-built fills and random numbers, and the
hedge / lag mechanics run on a synthetic leader against real Bitget prices.

    .venv/bin/python -m tests.test_prepare
"""
import math
import random

from ct import crowd, replay, scores, stats, targets
from ct.net import DATA
from ct.venue import BitgetVenue

MINUTE, HOUR, DAY = 60_000, 3_600_000, 86_400_000


def fill(t, side, sz, px, sp, coin='BTC', crossed=True):
    return {'coin': coin, 'time': t, 'side': side, 'sz': sz, 'px': px, 'startPosition': sp, 'crossed': crossed}


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def test_round_trips():
    print('round trips / RoN / long-bias')
    trips = scores.round_trips([fill(1, 'B', 1, 100, 0), fill(2, 'A', 1, 110, 1, crossed=False)])
    fees = 100 * scores.HL_TAKER + 110 * scores.HL_MAKER
    check('simple long: one trip, cash +10', len(trips) == 1 and abs(trips[0]['cash'] - 10) < 1e-9)
    check('RoN = (cash - fees) / max notional', abs(scores.ron(trips[0]) - (10 - fees) / 110) < 1e-12)
    flip = [fill(1, 'B', 1, 100, 0), fill(2, 'A', 2, 90, 1), fill(3, 'B', 1, 80, -1)]
    t2 = scores.round_trips(flip)
    check('flip splits into two trips', len(t2) == 2 and t2[0]['long'] and not t2[1]['long'])
    check('flip cash: -10 then +10', abs(t2[0]['cash'] + 10) < 1e-9 and abs(t2[1]['cash'] - 10) < 1e-9)
    check('pre-existing position ignored', scores.round_trips([fill(1, 'A', 2, 100, 2)]) == [])
    check('long-bias 100 / (100 + 90)', abs(scores.long_bias(flip) - 100 / 190) < 1e-12)
    many = [{'open_t': i, 'cash': c, 'fees': 0.0, 'max_notional': 100.0}
            for i, c in enumerate([1, 2, 3, 4, -1, -2, -3, -4, 5, 6])]
    rising = lambda t: t < 5
    check('cross-regime score = min of the two t-stats',
          scores.cross_regime_score(many, rising) == min(scores.t_stat([.01, .02, .03, .04, -.01]),
                                                         scores.t_stat([-.02, -.03, -.04, .05, .06])))
    check('cross-regime needs >= 4 trips per regime', scores.cross_regime_score(many[:6], rising) is None)


def test_statistics():
    print('statistics')
    check('spearman perfect', abs(stats.spearman([1, 2, 3, 4], [10, 20, 30, 40]) - 1) < 1e-12)
    check('spearman ties average', stats.ranks([5, 1, 5, 3]) == [3.5, 1.0, 3.5, 2.0])
    hits_one, hits_two, z_power = 0, 0, []
    for seed in range(200):
        rng = random.Random(seed)
        windows = []
        for _ in range(4):
            rows = [(rng.gauss(0, 1), rng.gauss(0, 1), rng.random()) for _ in range(800)]
            windows.append(stats.tercile_ic(rows))
        z = stats.combine(windows)
        hits_one += z >= 2.13
        hits_two += abs(z) >= 2.13
        # power: a small planted effect (IC ~ 0.1)
        rows_p = []
        for _ in range(4):
            r = []
            for _ in range(800):
                s = rng.gauss(0, 1)
                r.append((s, 0.1 * s + rng.gauss(0, 1), rng.random()))
            rows_p.append(stats.tercile_ic(r))
        z_power.append(stats.combine(rows_p))
    check('null: one-sided false positives <= 5% of 200 seeds', hits_one <= 10, f'{hits_one}/200 (expected ~3)')
    check('null: two-sided |Z| >= 2.13 <= 5%', hits_two <= 10, f'{hits_two}/200 (expected ~7)')
    power = sum(z >= 2.13 for z in z_power) / len(z_power)
    check('power: planted IC 0.1, n=800 x 4 windows, detected >= 90%', power >= 0.9, f'{power:.0%}')
    rng = random.Random(1)
    ar, x = [], 0.0
    nw_hits = 0
    for seed in range(200):
        rng = random.Random(seed)
        ar, x = [], 0.0
        for _ in range(220):
            x = 0.5 * x + rng.gauss(0, 1)   # autocorrelated, mean zero
            ar.append(x)
        nw_hits += (stats.newey_west_t(ar, 5) or 0) >= 2.13
    check('Newey-West on AR(1) noise: false positives <= 7%', nw_hits <= 14, f'{nw_hits}/200')


def synthetic_leader(start, hold_h, size_btc=1.0):
    """A fake leader that is long `size_btc` BTC from start for hold_h hours."""
    meta = {'symbol': 'BTCUSDT', 'qty_mult': 1.0, 'min_usdt': 5.0, 'size_step': 0.0001, 'min_qty': 0.0001,
            'coin': 'BTC', 'n_fills': 1}
    return [{**meta, 't': start, 't_last': start, 'pos_before': 0.0, 'pos_after': size_btc,
             'notional': 0.0, 'signed_notional': 0.0, 'size': size_btc, 'vwap': 1.0},
            {**meta, 't': start + hold_h * HOUR, 't_last': start + hold_h * HOUR, 'pos_before': size_btc,
             'pos_after': 0.0, 'notional': 0.0, 'signed_notional': 0.0, 'size': size_btc, 'vwap': 1.0}]


def test_hedge_and_lag():
    print('hedge and lag mechanics (synthetic leader, real Bitget BTC prices)')
    start = 1_767_225_600_000           # 2026-01-01 00:00 UTC
    end = start + 10 * DAY
    venue = BitgetVenue('2026-10-07', funding_override={})
    equity = lambda t: 100_000.0         # the leader holds 1 BTC on $100k: ~0.9x leverage
    book = lambda lag=0: targets.Copy([targets.LeaderBook(synthetic_leader(start + HOUR, 7 * 24), equity, 1.0, lag)])
    p = replay.Params(60, 10_000.0, hedge_mode='holdings')
    r = replay.follow(book(), p, venue, end)
    unhedged, hedged = r['net_usd'], r['hedged']['net_usd']
    check('holdings hedge traded', r['hedged']['hedge_trades'] >= 2, f"{r['hedged']['hedge_trades']} hedge trades")
    check('holdings hedge: a pure-BTC book is mostly hedged away',
          abs(hedged) < 0.15 * abs(unhedged) + 30, f'unhedged ${unhedged:,.2f} -> hedged ${hedged:,.2f}')
    check('daily returns exposed, one per mark', len(r['daily']['returns']) == r['daily']['n'])
    lagged = replay.follow(book(lag=24 * HOUR), p, venue, end)
    first_open = lambda res: min(t['open_t'] for t in res['trips'])
    check('24 h lag opens the copy 24 h later',
          first_open(lagged) - first_open(r) == 24 * HOUR,
          f'{(first_open(lagged) - first_open(r)) / HOUR:.0f} h later')


def test_static_hedge_and_alpha():
    print('static hedge (revision 4): keeps timing, removes average exposure; alpha; average beta')
    start = 1_767_225_600_000           # 2026-01-01 00:00 UTC
    end = start + 10 * DAY
    venue = BitgetVenue('2026-10-07', funding_override={})
    equity = lambda t: 100_000.0
    px0 = venue.candle_minute('BTCUSDT', start + 2 * HOUR)[1]
    w = px0 / 100_000.0                  # the follower's exposure while the leader holds 1 BTC
    btc_end = venue.candle_minute('BTCUSDT', end - MINUTE)[1]
    r_win = btc_end / px0 - 1
    # (a) constant-beta book: long the whole window; static beta = w -> flat
    const = targets.Copy([targets.LeaderBook(synthetic_leader(start + HOUR, 9 * 24 + 20), equity, 1.0)])
    r = replay.follow(const, replay.Params(60, 10_000.0, hedge_mode='static', static_beta=w), venue, end)
    u, h = r['net_usd'], r['hedged']['net_usd']
    check('static hedge: a constant-beta book is neutralised',
          abs(h) < 0.15 * abs(u) + 40, f'unhedged ${u:,.2f} -> static-hedged ${h:,.2f} (BTC {r_win:+.1%})')
    check('static hedge trades once plus re-trues only', 1 <= r['hedged']['hedge_trades'] <= 4,
          f"{r['hedged']['hedge_trades']} trades")
    # (b) a timer: long the first 5 days only; average beta over the window = w / 2
    timer = lambda: targets.Copy([targets.LeaderBook(synthetic_leader(start + HOUR, 5 * 24), equity, 1.0)])
    rs = replay.follow(timer(), replay.Params(60, 10_000.0, hedge_mode='static', static_beta=w / 2), venue, end)
    rh = replay.follow(timer(), replay.Params(60, 10_000.0, hedge_mode='holdings'), venue, end)
    expected = rs['net_usd'] - 0.5 * w * 10_000.0 * r_win
    check('static hedge on a timer = unhedged minus average beta x window move (timing kept)',
          abs(rs['hedged']['net_usd'] - expected) < 0.15 * abs(rs['net_usd']) + 40,
          f"unhedged ${rs['net_usd']:,.2f}, static ${rs['hedged']['net_usd']:,.2f}, expected ${expected:,.2f}")
    check('holdings hedge on the same timer removes it',
          abs(rh['hedged']['net_usd']) < 0.15 * abs(rh['net_usd']) + 30,
          f"holdings-hedged ${rh['hedged']['net_usd']:,.2f}")
    # alpha (synthetic series)
    rng = random.Random(5)
    x = [rng.gauss(0, 0.03) for _ in range(400)]
    y = [0.001 + 0.8 * a + rng.gauss(0, 0.004) for a in x]
    a = stats.alpha(y, x)
    check('alpha recovers the planted intercept', abs(a - 0.001) < 0.0006, f'alpha = {a:.5f}')
    check('alpha pairs with benchmark returns',
          len(replay.benchmark_daily_returns(venue, [t for t, _ in rs['daily']['returns']], start))
          == len(rs['daily']['returns']))
    # average beta from a synthetic fill path: long 1 BTC on marks 1..5 of 10
    from ct.leaders import average_beta
    symtab = {'BTC': {'symbol': 'BTCUSDT', 'qty_mult': 1.0, 'status': 'ok'}}
    fills = [fill(start + HOUR, 'B', 1.0, px0, 0.0), fill(start + 5 * DAY + HOUR, 'A', 1.0, px0, 1.0)]
    b, days = average_beta(fills, equity, venue, symtab, start, end)
    check('average beta = (days held / days) x price / equity', days == 10 and abs(b - 0.5 * w) < 0.05 * w,
          f'beta {b:.3f} vs {0.5 * w:.3f} over {days} days')
    b0, _ = average_beta([fill(start + HOUR, 'A', 1.0, px0, 1.0)], equity, venue, symtab, start, end)
    check('position open at window start is read from startPosition', abs(b0 - 0.1 * w) < 0.05 * w,
          f'beta {b0:.3f} vs {0.1 * w:.3f}')


def test_crowd():
    print('idea B: long/short book (synthetic) and tilts (one real snapshot, arbitrary addresses)')
    rng = random.Random(7)
    coins = [f'C{i}' for i in range(60)]
    days = [(d, {c: rng.gauss(0, 1) for c in coins}) for d in range(300)]
    noise = {(c, d): rng.gauss(0, 0.03) for d in range(300) for c in coins}
    cost = 0.0007
    res = crowd.long_short(days, lambda c, d: noise[(c, d)], lambda c, d: 0.03, cost)
    m = crowd.summary(res[10:])['mean']
    check('pure noise loses its costs (~ -2 x cost / 3 per day)', abs(m - (-2 * cost / 3)) < 0.0012,
          f'mean {m * 1e4:.1f} bp/day vs {-2 * cost / 3 * 1e4:.1f} bp')
    # A persistent edge (what slow money would have): each coin carries a
    # lasting component alpha_c that shows in its signal AND its returns, so
    # every live tranche — not only today's — benefits.
    alpha = {c: rng.gauss(0, 1) for c in coins}
    days_p = [(d, {c: alpha[c] + rng.gauss(0, 1) for c in coins}) for d in range(300)]
    planted = {(c, d): 0.003 * alpha[c] + rng.gauss(0, 0.03) for d in range(300) for c in coins}
    res_p = crowd.long_short(days_p, lambda c, d: planted[(c, d)], lambda c, d: 0.03, cost)
    t = stats.newey_west_t([v for _, v in res_p[10:]], 3)
    check('planted signal is detected (NW t >= 2.13)', t is not None and t >= 2.13, f't = {t:.1f}')
    snap = sorted((DATA / 'archive/by_dex/hyperliquid/snapshots/perp/date=2026-02-16').glob('*.parquet'))[0]
    import duckdb
    users = [u for (u,) in duckdb.connect().execute(
        f"SELECT DISTINCT \"user\" FROM read_parquet('{snap}') USING SAMPLE 2000 ROWS").fetchall()]
    sig = crowd.tilts(snap, users[:500])
    check('tilts: finite signals for coins with >= 3 holders', sig and all(math.isfinite(v) for v in sig.values()),
          f'{len(sig)} coins')
    check('tilt difference bounded by 2', all(abs(v) <= 2 for v in sig.values()))


if __name__ == '__main__':
    test_round_trips()
    test_statistics()
    test_hedge_and_lag()
    test_static_hedge_and_alpha()
    test_crowd()
    print('all prepare-phase checks passed')
