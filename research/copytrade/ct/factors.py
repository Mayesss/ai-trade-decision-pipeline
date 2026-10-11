"""Registration 009 (L4) — a weekly cross-sectional funding-carry factor (F1) on Binance USDⓈ-M
perpetuals. Mechanics. (Revision 3: the family is F1 alone; every F2 variant was rejected on turnover
before any outcome and has no code path here.)

Three layers, each tested on synthetic data (tests/test_factors.py) before it touches real data:

  1. FEATURES (point in time, behaviour): `features(con, signals)` — one row per (signal time S,
     symbol alive at S) with everything a signal at S may use. Every input is stamped before S:
       * 1h bars: a bar opening at t is complete at t + 1 h, so only bars with t <= S - 1 h;
       * funding: settlements with calc_time < S (a settlement AT S is paid by the book held
         through S, so it belongs to the previous week's outcome, never to the signal at S).
     F1 needs funding and klines only; no metrics column is read (Revision 3). The life table
     (listing, delisting cut) comes from the listing; a metrics-archive hole while the coin kept
     trading is bridged (`bridge_trading_gaps`).
  2. OUTCOMES (forward; the guarded runner only): `outcomes(con, universe)` — entry at the open of
     the 1h bar starting S + 1 h, exit at the open of the bar starting S + 7 d + 1 h (the next
     rebalance's entry), funding settled in (entry, exit] signed by the position, each symbol cut at
     its delisting (last full metrics day) — a coin dying inside the week exits at the close of its
     last traded bar before the cut and holds cash after it.
  3. TESTS (pure Python): the buffered book (equal risk by trailing vol, costs on the traded amount
     with drift), the weekly Spearman IC, the five criteria with Newey–West t, the holdout-pull
     conditions (beta-adjusted net, 2x slippage), the shuffle placebo, the decision table, and the
     power simulation (planted IC on the empirical cross-sectional dispersion of contemporaneous
     returns plus a common market factor; no return is paired with a score — the trailing weeks
     are earlier signals' outcome windows, used only as unconditional distributions).

Sign convention: the SCORE is oriented so that high = long. F1 (funding carry): score =
-(sum of funding rates settled in [S - 7 d, S)), rounded to 1e-10 so equal rates tie exactly —
long low funding, short high.
"""
import datetime as dt
import math
import random
import statistics

from . import stats

HOUR, DAY = 3_600_000, 86_400_000
WEEK = 7 * DAY
ENTRY_LAG = HOUR                     # trade at the open of the 1h bar starting S + 1 h


def _ms(y, m, d):
    return int(dt.datetime(y, m, d, tzinfo=dt.UTC).timestamp() * 1000)


# --- the split (fixed by the orchestrator; registration §2) -------------------------------------
# A signal S (Monday 00:00 UTC) belongs to a split [a, b] when its feature week [S - 7 d, S) starts on
# or after a and its outcome week ends by b + 1 day + 1 h: S >= a + 7 d and S + 7 d <= b + 1 d.
# The first week of each split therefore carries features only: that is the 1-week embargo.
SPLITS = {
    'discovery': (_ms(2021, 12, 6), _ms(2023, 12, 31)),
    'holdout_A': (_ms(2024, 1, 1), _ms(2025, 7, 27)),
    'holdout_B': (_ms(2025, 7, 28), _ms(2026, 6, 30)),
}
HARD_END_DISCOVERY = _ms(2024, 1, 8)          # no data on or after this is on disk
LAST_EXIT_DISCOVERY = _ms(2024, 1, 1) + HOUR   # the true last read: S 2023-12-25 + 7 d + 1 h (Revision 3)
LAST_EXIT_HOLDOUT_A = _ms(2025, 7, 28) + HOUR  # S 2025-07-21 + 7 d + 1 h


def signal_weeks(split):
    a, b = SPLITS[split]
    s, out = a + WEEK, []
    while s + WEEK <= b + DAY:
        out.append(s)
        s += WEEK
    return out


# --- registered parameters (D-table; the runner reads them from here) ---------------------------
TAKER = 0.0005                        # Binance USDⓈ-M VIP0 taker, each side
LISTED_MIN = 30 * DAY
ADV_DAYS = 30                         # trailing quote-volume window (mean daily $ volume)
FLOOR_ADV = 10_000_000                # liquidity floor: trailing 30-day mean daily quote volume, USD
# slippage per side by liquidity tier (trailing 30-day mean daily quote volume, USD lower bound)
TIERS = ((1_000_000_000, 'T1', 0.0001), (200_000_000, 'T2', 0.0002),
         (50_000_000, 'T3', 0.0004), (FLOOR_ADV, 'T4', 0.0006))
VOL_DAYS, VOL_MIN = 30, 20            # equal risk: s.d. of the last 30 daily log returns (>= 20 needed)
QUANTILE = 5
BAND = 0.4                            # Revision 1: a held coin stays while inside the top/bottom 40%
NW_LAGS = 4
FAMILY = ('F1',)                      # Revision 3: F2 rejected on turnover (all four variants); F3 dropped
ALPHA = 0.05
Z_CRIT = statistics.NormalDist().inv_cdf(1 - ALPHA / len(FAMILY))
Z_HOLDOUT_ONE = 1.6449                # one-sided 5% when one factor goes to the holdout
FUND_DECIMALS = 10                    # F1 ties: fund7 rounded to 1e-10 before ranking (Revision 3)
EXCLUDE = frozenset({'BLUEBIRDUSDT', 'FOOTBALLUSDT', 'BTCDOMUSDT', 'DEFIUSDT', 'USDCUSDT'})


def z_crit(n_trials):
    return statistics.NormalDist().inv_cdf(1 - ALPHA / n_trials)


def tier(adv):
    for lo, name, slip in TIERS:
        if adv is not None and adv >= lo:
            return name, slip
    return None, None


def cost_rate(adv, slip_mult=1.0):
    _, slip = tier(adv)
    return None if slip is None else TAKER + slip_mult * slip


def with_slippage(universe, mult):
    """The universe with each coin's slippage scaled by mult (0x reported; 2x is a holdout-pull
    condition, D13)."""
    return {S: [dict(r, cost=cost_rate(r['adv30'], mult)) for r in rows] for S, rows in universe.items()}


def with_stress_tiers(universe):
    """Reported only (Revision 3, review M5): tiers from the minimum 30-day volume of the past 90 days."""
    return {S: [dict(r, cost=cost_rate(r['adv_min90'] if r.get('adv_min90') is not None else r['adv30'])
                     if cost_rate(r['adv_min90'] if r.get('adv_min90') is not None else r['adv30']) is not None
                     else TAKER + TIERS[-1][2])
                for r in rows] for S, rows in universe.items()}


# --- 1. features -------------------------------------------------------------------------------

def bridge_trading_gaps(con, life):
    """Revision 3 (review minor 7, ICP): a metrics-archive hole is not a delisting while the contract
    kept trading. With the panel's 1h bars, a DAY is active when it holds >= 1 traded bar (cnt > 0;
    dead contracts show cnt 0). For each hole between two segments, the earlier segment is extended
    forward over the hole's leading run of active days and the later one backward over its trailing
    run; a hole that is active throughout merges the two. The first segment is extended backward
    over a run of active days before it (a coin whose metrics start late). Days outside the bars on
    disk are inactive, so nothing is extended beyond the panel. Returns (new life dict,
    [(symbol, what, from_day, to_day)])."""
    d = lambda x: int(dt.datetime.strptime(x, '%Y-%m-%d').replace(tzinfo=dt.UTC).timestamp() * 1000)
    day = lambda ms: dt.datetime.fromtimestamp(ms / 1000, dt.UTC).strftime('%Y-%m-%d')
    out, changes = {}, []
    for sym, v in life.items():
        segs = [list(x) for x in v['segments']]
        if not segs:
            out[sym] = v
            continue
        active = {k for k, n in con.execute(f"SELECT ts // {DAY}, sum(CASE WHEN cnt > 0 THEN 1 ELSE 0 END) FROM bars "
                                            "WHERE symbol = ? GROUP BY 1", [sym]).fetchall() if n and n > 0}
        k0 = d(segs[0][0]) // DAY - 1
        while k0 in active:
            k0 -= 1
        if k0 + 1 < d(segs[0][0]) // DAY:
            changes.append((sym, 'start_back', day((k0 + 1) * DAY), segs[0][0]))
            segs[0][0] = day((k0 + 1) * DAY)
        res = [segs[0]]
        for a, b in segs[1:]:
            lo, hi = d(res[-1][1]) // DAY + 1, d(a) // DAY - 1          # the hole's days, inclusive
            f = lo
            while f <= hi and f in active:
                f += 1
            if f > hi:
                changes.append((sym, 'merged', day(lo * DAY), day(hi * DAY)))
                res[-1][1] = b
                continue
            if f > lo:
                changes.append((sym, 'end_forward', res[-1][1], day((f - 1) * DAY)))
                res[-1][1] = day((f - 1) * DAY)
            g = hi
            while g >= f and g in active:
                g -= 1
            if g < hi:
                changes.append((sym, 'start_back', day((g + 1) * DAY), a))
                a = day((g + 1) * DAY)
            res.append([a, b])
        out[sym] = dict(v, segments=res)
    return out, changes


def life_table(con, life):
    """life: {symbol: {'segments': [[start_day, end_day]], 'first_kline_day': day}} -> temp table
    life(symbol, seg_start, cut_end, listing_ts). A segment is alive on [start 00:00, end + 1 d 00:00);
    the listing time is the first 1d kline (first segment) or the segment start (a relisting)."""
    d = lambda s: int(dt.datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=dt.UTC).timestamp() * 1000)
    rows = []
    for sym, v in life.items():
        for i, (a, b) in enumerate(v['segments']):
            lst = d(v['first_kline_day']) if (i == 0 and v.get('first_kline_day')) else d(a)
            rows.append((sym, d(a), d(b) + DAY, min(lst, d(a)) if i == 0 else d(a)))
    con.execute('CREATE OR REPLACE TEMP TABLE life(symbol VARCHAR, seg_start BIGINT, cut_end BIGINT, listing_ts BIGINT)')
    if rows:
        con.executemany('INSERT INTO life VALUES (?, ?, ?, ?)', rows)


FEATURE_SQL = """
WITH cand AS (
    SELECT s.S, l.symbol, l.listing_ts, l.cut_end FROM sig s JOIN life l ON l.seg_start <= s.S AND s.S < l.cut_end),
liq AS (
    SELECT c.S, c.symbol, sum(b.qv) / {adv_days} AS adv30, count(*) AS n_bars30
    FROM cand c JOIN bars b ON b.symbol = c.symbol AND b.ts >= c.S - {ADV_MS} AND b.ts <= c.S - {HOUR}
    GROUP BY 1, 2),
liq90 AS (
    SELECT c.S, c.symbol,
           sum(b.qv) FILTER (WHERE b.ts >= c.S - {ADV2_MS} AND b.ts < c.S - {ADV_MS}) / {adv_days} AS adv_b2,
           count(*) FILTER (WHERE b.ts >= c.S - {ADV2_MS} AND b.ts < c.S - {ADV_MS}) AS n_b2,
           sum(b.qv) FILTER (WHERE b.ts < c.S - {ADV2_MS}) / {adv_days} AS adv_b3,
           count(*) FILTER (WHERE b.ts < c.S - {ADV2_MS}) AS n_b3
    FROM cand c JOIN bars b ON b.symbol = c.symbol AND b.ts >= c.S - {ADV3_MS} AND b.ts < c.S - {ADV_MS}
    GROUP BY 1, 2),
f1 AS (
    SELECT c.S, c.symbol, sum(f.rate) AS fund7, count(*) AS fund_n, min(f.ts) AS fund_first, max(f.ts) AS fund_last,
           max(coalesce(f.interval_h, 8)) AS fund_int
    FROM cand c JOIN funding f ON f.symbol = c.symbol AND f.ts >= c.S - {WEEK_MS} AND f.ts < c.S
    GROUP BY 1, 2),
dcl AS (
    SELECT c.S, c.symbol, b.ts, b.close FROM cand c JOIN bars b ON b.symbol = c.symbol
      AND b.ts >= c.S - {HOUR} - {VOL_MS} AND b.ts <= c.S - {HOUR} AND (c.S - {HOUR} - b.ts) % {DAY} = 0),
dr AS (
    SELECT S, symbol, ts, close, ln(close / lag(close) OVER (PARTITION BY S, symbol ORDER BY ts)) AS r FROM dcl),
vol AS (
    SELECT S, symbol, stddev_samp(r) AS sigma_d, count(r) AS n_r,
           max(close) FILTER (WHERE ts = S - {HOUR}) AS c_last,
           max(close) FILTER (WHERE ts = S - {HOUR} - {WEEK_MS}) AS c_week_ago
    FROM dr GROUP BY 1, 2),
mkt AS (
    SELECT dr.S, dr.ts, avg(dr.r) AS mr, count(*) AS n_mkt
    FROM dr JOIN liq USING (S, symbol) JOIN cand USING (S, symbol)
    WHERE dr.r IS NOT NULL AND liq.adv30 >= {FLOOR} AND cand.listing_ts <= dr.S - {LISTED_MS}
    GROUP BY 1, 2),
beta AS (
    SELECT dr.S, dr.symbol, regr_slope(dr.r, mkt.mr) AS beta, count(*) AS n_beta
    FROM dr JOIN mkt USING (S, ts) WHERE dr.r IS NOT NULL GROUP BY 1, 2),
hb AS (
    SELECT c.S, c.symbol, b.ts, ln(b.close) AS lc, (ln(b.high) + ln(b.low)) / 2 AS eta
    FROM cand c JOIN bars b ON b.symbol = c.symbol AND b.ts >= c.S - {ADV_MS} AND b.ts <= c.S - {HOUR}
      AND b.cnt > 0 AND b.low > 0),
hb2 AS (
    SELECT S, symbol, (lc - eta) * (lc - lead(eta) OVER (PARTITION BY S, symbol ORDER BY ts)) AS x FROM hb),
ar AS (
    SELECT S, symbol, 4 * avg(x) AS s2 FROM hb2 WHERE x IS NOT NULL GROUP BY 1, 2)
SELECT c.S, c.symbol, c.listing_ts, c.cut_end, liq.adv30, liq.n_bars30,
       liq90.adv_b2, liq90.n_b2, liq90.adv_b3, liq90.n_b3,
       f1.fund7, f1.fund_n, f1.fund_first, f1.fund_last, f1.fund_int,
       vol.sigma_d, vol.n_r, vol.c_last, vol.c_week_ago, beta.beta, beta.n_beta, ar.s2 AS ar_s2
FROM cand c
LEFT JOIN liq USING (S, symbol) LEFT JOIN liq90 USING (S, symbol) LEFT JOIN f1 USING (S, symbol)
LEFT JOIN vol USING (S, symbol) LEFT JOIN beta USING (S, symbol) LEFT JOIN ar USING (S, symbol)
ORDER BY c.S, c.symbol
"""


def feature_sql():
    return FEATURE_SQL.format(DAY=DAY, HOUR=HOUR, adv_days=ADV_DAYS, ADV_MS=ADV_DAYS * DAY, ADV2_MS=2 * ADV_DAYS * DAY,
                              ADV3_MS=3 * ADV_DAYS * DAY, VOL_MS=VOL_DAYS * DAY, WEEK_MS=WEEK, FLOOR=FLOOR_ADV,
                              LISTED_MS=LISTED_MIN)


def features(con, signals, sql=None):
    """Raw feature rows for every (S, symbol alive at S). Needs tables/views bars, funding, life.
    `sql` overrides the query (tests use it to show a leaky query is caught)."""
    con.execute('CREATE OR REPLACE TEMP TABLE sig(S BIGINT)')
    con.executemany('INSERT INTO sig VALUES (?)', [(int(s),) for s in signals])
    cur = con.execute(sql or feature_sql())
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def derive(row):
    """Score, validity, costs and behaviour fields from one raw feature row (pure)."""
    S = row['S']
    out = dict(row)
    # Revision 4 (m5): every settlement of the window must be present — at least 168 h / the longest
    # interval seen in it (21 at 8 h) — and the window must be covered at both ends; otherwise no score
    f1_ok = (row['fund_n'] or 0) >= 1 and row['fund_first'] is not None \
        and row['fund_first'] <= S - WEEK + 8 * HOUR and row['fund_last'] >= S - 8 * HOUR \
        and row['fund_n'] >= (7 * 24) // (row.get('fund_int') or 8)
    out['F1_missing_settlement'] = (row['fund_n'] or 0) >= 1 and not f1_ok
    out['F1'] = -round(row['fund7'], FUND_DECIMALS) if f1_ok else None
    out['sigma'] = row['sigma_d'] if (row['n_r'] or 0) >= VOL_MIN and (row['sigma_d'] or 0) > 0 else None
    out['beta'] = row.get('beta') if (row.get('n_beta') or 0) >= VOL_MIN else None
    out['listed_ok'] = S - row['listing_ts'] >= LISTED_MIN
    out['tier'], slip = tier(row['adv30'])
    out['cost'] = None if slip is None else TAKER + slip
    # stress-week tiers (reported only): the minimum of the 30-day mean daily volume over the three
    # 30-day blocks of the past 90 days, each block counted only if it holds >= 25 days of bars
    blocks = [row['adv30']] + [row.get(a) for a, n in (('adv_b2', 'n_b2'), ('adv_b3', 'n_b3'))
                               if (row.get(n) or 0) >= 25 * 24]
    out['adv_min90'] = min(x for x in blocks if x is not None) if any(x is not None for x in blocks) else None
    out['ar_half_spread'] = (math.sqrt(row['ar_s2']) / 2) if (row['ar_s2'] or 0) > 0 else 0.0
    out['ret7_trailing'] = (row['c_last'] / row['c_week_ago'] - 1) if row['c_last'] and row['c_week_ago'] else None
    # the universe needs no metrics (Revision 3): alive, listed, liquid, sigma; F1 needs funding
    out['in_universe'] = bool(row['symbol'] not in EXCLUDE and out['listed_ok'] and row['adv30'] is not None
                              and row['adv30'] >= FLOOR_ADV and out['sigma'] is not None)
    return out


def universe_by_week(rows):
    """{S: [derived rows in the universe]}."""
    out = {}
    for r in rows:
        d = derive(r) if 'in_universe' not in r else r
        if d['in_universe']:
            out.setdefault(d['S'], []).append(d)
    return out


def poison_after(con, S, scale=7.0):
    """Test helper: multiply every price/volume/funding stamped at or after S (bars opening at or
    after S, settlements at or after S) by `scale`, in place."""
    con.execute(f"UPDATE bars SET open = open * {scale}, high = high * {scale}, low = low * {scale}, "
                f"close = close * {scale}, qv = qv * {scale} WHERE ts >= {S}")
    con.execute(f"UPDATE funding SET rate = rate * {scale} + 0.001 WHERE ts >= {S}")


# --- 2. outcomes (guarded runner only on real data) --------------------------------------------

OUTCOME_SQL = """
WITH u AS (SELECT * FROM uni),
ent AS (
    SELECT u.S, u.symbol, u.cut_end, b.ts AS entry_ts, b.open AS p0
    FROM u JOIN bars b ON b.symbol = u.symbol AND b.ts = u.S + {LAG} AND b.cnt > 0 AND b.ts < u.cut_end AND b.open > 0),
nxt AS (
    SELECT e.S, e.symbol, b.ts AS exit_ts, b.open AS p1 FROM ent e JOIN bars b ON b.symbol = e.symbol
      AND b.ts = e.S + {WEEK} + {LAG} AND b.cnt > 0 AND b.ts < e.cut_end AND b.open > 0),
lastb AS (
    SELECT e.S, e.symbol, arg_max(b.ts, b.ts) + {HOUR} AS exit_ts, arg_max(b.close, b.ts) AS p1
    FROM ent e JOIN bars b ON b.symbol = e.symbol AND b.ts >= e.entry_ts AND b.ts < e.S + {WEEK} + {LAG}
      AND b.ts < e.cut_end AND b.cnt > 0 AND b.close > 0
    GROUP BY 1, 2),
ex AS (
    SELECT e.S, e.symbol, e.entry_ts, e.p0, e.cut_end,
           coalesce(n.exit_ts, l.exit_ts) AS exit_ts, coalesce(n.p1, l.p1) AS p1,
           (n.exit_ts IS NULL) AS fallback_exit, (e.S + {WEEK} + {LAG} >= e.cut_end) AS delisted
    FROM ent e LEFT JOIN nxt n USING (S, symbol) LEFT JOIN lastb l USING (S, symbol)),
fu AS (
    SELECT x.S, x.symbol, sum(f.rate * coalesce(b.open, x.p0) / x.p0) AS fund_paid_long, count(*) AS n_settle,
           max(coalesce(f.interval_h, 8)) AS fund_int
    FROM ex x JOIN funding f ON f.symbol = x.symbol AND f.ts > x.entry_ts AND f.ts <= x.exit_ts
    LEFT JOIN bars b ON b.symbol = x.symbol AND b.ts = f.ts - f.ts % {HOUR}
    GROUP BY 1, 2)
SELECT x.S, x.symbol, x.entry_ts, x.exit_ts, x.p0, x.p1, x.fallback_exit, x.delisted,
       fu.fund_paid_long, coalesce(fu.n_settle, 0) AS n_settle, coalesce(fu.fund_int, 8) AS fund_int
FROM ex x LEFT JOIN fu USING (S, symbol) ORDER BY x.S, x.symbol
"""


def outcomes(con, universe, hard_end=None):
    """{S: {symbol: {'R': price return, 'F': funding P&L of a long (= -funding paid), 'delisted': bool}}}
    for the universe rows (needs bars, funding, and the rows' cut_end). Every bar read is asserted to
    lie before hard_end (discovery: 2024-01-08)."""
    con.execute('CREATE OR REPLACE TEMP TABLE uni(S BIGINT, symbol VARCHAR, cut_end BIGINT)')
    con.executemany('INSERT INTO uni VALUES (?, ?, ?)',
                    [(r['S'], r['symbol'], r['cut_end']) for rows in universe.values() for r in rows])
    out = {}
    for S, sym, ent, ext, p0, p1, fb, dl, fpl, ns, fint in con.execute(
            OUTCOME_SQL.format(LAG=ENTRY_LAG, WEEK=WEEK, HOUR=HOUR)).fetchall():
        if hard_end is not None:
            assert ext <= hard_end, (S, sym, ext)
        if p1 is None:
            continue
        # Revision 4 (m5): a hold missing a settlement has no funding P&L (None, never 0) and is not
        # traded (cash, like a missing entry print); the runner counts these coin-weeks
        need = ((ext - ent) // HOUR) // (fint or 8)
        missing = ns < need
        out.setdefault(S, {})[sym] = {'R': p1 / p0 - 1, 'F': None if missing else -(fpl or 0.0), 'delisted': bool(dl),
                                      'fallback_exit': bool(fb), 'entry_ts': ent, 'exit_ts': ext, 'n_settle': ns,
                                      'funding_missing': missing}
    return out


def usable(outc):
    """The outcomes a book may trade: those with every settlement present (Revision 4, m5)."""
    return {S: {s: o for s, o in v.items() if o.get('F') is not None} for S, v in outc.items()}


# --- 3. the book, the IC, the criteria ---------------------------------------------------------

def _leg(use, k, top):
    """[(row, fraction)] filling k slots from the top (or bottom) of `use` (sorted ascending by
    score). Coins tied with the boundary score share the slots left at the boundary equally, so a
    tie (e.g. many coins at the default funding rate) is never resolved by name."""
    ordered = use[::-1] if top else use
    b = ordered[k - 1]['_s']
    strictly = [r for r in ordered if (r['_s'] > b if top else r['_s'] < b)]
    tied = [r for r in ordered if r['_s'] == b]
    f = (k - len(strictly)) / len(tied)
    return [(r, 1.0) for r in strictly] + [(r, f) for r in tied]


def quintile_weights(rows, key, q=QUANTILE):
    """Long the top 1/q by score, short the bottom 1/q (k = floor(n / q) slots a leg); weights
    proportional to fraction / sigma, each leg summing to 1 (long +1, short -1). Coins tied at a
    leg's boundary share its remaining slots (`_leg`). {} when a leg would be empty or the two legs
    would overlap (every coin tied)."""
    use = sorted(({'_s': r[key], 'symbol': r['symbol'], 'sigma': r['sigma']} for r in rows
                  if r.get(key) is not None and r.get('sigma')), key=lambda r: (r['_s'], r['symbol']))
    k = len(use) // q
    if k < 1:
        return {}
    out = {}
    for sign, leg in ((1.0, _leg(use, k, True)), (-1.0, _leg(use, k, False))):
        inv = [f / r['sigma'] for r, f in leg]
        tot = sum(inv)
        for (r, _), v in zip(leg, inv):
            if r['symbol'] in out:
                return {}
            out[r['symbol']] = sign * v / tot
    return out


def buffered_targets(rows, key, prev_long, prev_short, q=QUANTILE, band=BAND):
    """The PRIMARY rebalance rule (Revision 1), membership with hysteresis. Among the coins with a
    score and a sigma (n of them, k = floor(n / q), b = floor(band x n)):
      * a coin ENTERS the long leg when its score is >= the k-th highest score (the top quintile,
        coins tied with that score all included), the short leg when <= the k-th lowest;
      * a coin already HELD long stays while its score is >= the b-th highest score (inside the top
        40% band, ties included), a held short while <= the b-th lowest; it LEAVES only when it
        drops out of that band, or out of the universe (delisted, below the floor, no score/sigma);
      * a coin in both legs (only possible through ties around the median) is dropped from both.
    Weights proportional to 1/sigma within each leg, legs summing to +1 / -1. Membership never
    changes for a held coin inside its band; its SIZE moves when the leg's other members change.
    Returns (weights, long_set, short_set); ({}, set(), set()) when k < 1."""
    use = sorted((r for r in rows if r.get(key) is not None and r.get('sigma')), key=lambda r: (r[key], r['symbol']))
    n = len(use)
    k, b = n // q, int(band * n)
    if k < 1:
        return {}, set(), set()
    b = max(b, k)
    hi_enter, lo_enter = use[-k][key], use[k - 1][key]
    hi_keep, lo_keep = use[-b][key], use[b - 1][key]
    longs = {r['symbol'] for r in use if r[key] >= hi_enter or (r['symbol'] in prev_long and r[key] >= hi_keep)}
    shorts = {r['symbol'] for r in use if r[key] <= lo_enter or (r['symbol'] in prev_short and r[key] <= lo_keep)}
    both = longs & shorts
    longs, shorts = longs - both, shorts - both
    if not longs or not shorts:
        return {}, set(), set()
    sig = {r['symbol']: r['sigma'] for r in use}
    out = {}
    for leg, sign in ((longs, 1.0), (shorts, -1.0)):
        tot = sum(1 / sig[x] for x in leg)
        for x in leg:
            out[x] = sign * (1 / sig[x]) / tot
    return out, longs, shorts


def target_path(universe, key, buffer=True, scores=None, feasible=None):
    """[(S, target weights)] through the weeks in time order. feasible: optional {S: set of symbols
    that can be entered} (the runner's entry prints); a target coin outside it is held as cash and
    is not a member next week. Behaviour only when feasible is None."""
    pl, ps, out = set(), set(), []
    for S in sorted(universe):
        rows = universe[S]
        if scores is not None:
            rows = [dict(r, **{key: scores[S].get(r['symbol'])}) for r in rows]
        if buffer:
            w, _, _ = buffered_targets(rows, key, pl, ps)
        else:
            w = quintile_weights(rows, key)
        if feasible is not None:
            w = {s: x for s, x in w.items() if s in feasible.get(S, set())}
        pl = {s for s, x in w.items() if x > 0}
        ps = {s for s, x in w.items() if x < 0}
        out.append((S, w))
    return out


def run_book(universe, key, outc, q=QUANTILE, scores=None, buffer=True):
    """Weekly quintile book through the signal weeks in time order. universe: {S: rows};
    outc: {S: {symbol: {'R', 'F', 'delisted'}}}; scores: optional {S: {symbol: score}} overriding
    row[key] (the placebo). Costs: at each rebalance the traded amount |target - drifted holding|
    times (taker + the coin's tier slippage); a coin cut inside the week is closed at its exit and
    pays the same rate; the first week pays the full entry, the last week no liquidation.
    A target coin without an entry print is held as cash (its weight is not re-spread).
    buffer=True: the registered rule (`buffered_targets`, Revision 1); False: the plain quintile
    book (`quintile_weights`, reported). Weeks with no book and nothing held are skipped.
    Returns [{'S', 'gross', 'gross_price', 'gross_funding', 'cost', 'net', 'long', 'short', 'turnover',
    'n_long', 'n_short', 'beta_book'}]; beta_book = sum of target weight x the coin's ex-ante beta to
    the equal-weight universe (a coin without a beta counts 1.0)."""
    held, rate_of, beta_of, out = {}, {}, {}, []
    outc = usable(outc)                                 # a hold missing a settlement is cash (m5)
    feasible = {S: set(outc.get(S, {})) for S in universe}
    path = dict(target_path(universe, key, buffer, scores, feasible))
    for S in sorted(universe):
        for r in universe[S]:
            rate_of[r['symbol']] = r['cost']
            beta_of[r['symbol']] = r.get('beta') if r.get('beta') is not None else 1.0
        oc = outc.get(S, {})
        tgt = path[S]
        if not tgt and not held:
            continue                                    # no book this week (e.g. no scores yet)
        prev = held
        keys = set(tgt) | set(prev)
        traded = {s: abs(tgt.get(s, 0.0) - prev.get(s, 0.0)) for s in keys}
        turnover = sum(traded.values())
        pnl = {s: w * (oc[s]['R'] + oc[s]['F']) for s, w in tgt.items()}
        closing = {s: abs(w * (1 + oc[s]['R'])) * rate_of[s] for s, w in tgt.items() if oc[s]['delisted']}
        held = {s: w * (1 + oc[s]['R']) for s, w in tgt.items() if not oc[s]['delisted']}

        def is_long(s):
            w = tgt.get(s, 0.0)
            return w > 0 or (w == 0 and prev.get(s, 0.0) > 0)
        costs = {s: traded[s] * rate_of[s] + closing.get(s, 0.0) for s in keys}
        cost = sum(costs.values())
        lc = sum(v for s, v in costs.items() if is_long(s))
        gross = sum(pnl.values())
        out.append({'S': S, 'gross': gross, 'cost': cost, 'net': gross - cost,
                    'gross_price': sum(w * oc[s]['R'] for s, w in tgt.items()),
                    'gross_funding': sum(w * oc[s]['F'] for s, w in tgt.items()),
                    'beta_book': sum(w * beta_of[s] for s, w in tgt.items()),
                    'long': sum(v for s, v in pnl.items() if tgt[s] > 0) - lc,
                    'short': sum(v for s, v in pnl.items() if tgt[s] < 0) - (cost - lc),
                    'turnover': turnover, 'n_long': sum(1 for w in tgt.values() if w > 0),
                    'n_short': sum(1 for w in tgt.values() if w < 0)})
    return out


def weekly_ic(universe, key, outc, scores=None):
    """[(S, Spearman IC of score vs the long's total return R + F, n)] over the universe coins
    with an entry."""
    outc = usable(outc)
    out = []
    for S in sorted(universe):
        oc = outc.get(S, {})
        pairs = [((scores[S].get(r['symbol']) if scores is not None else r[key]), oc[r['symbol']])
                 for r in universe[S] if r['symbol'] in oc]
        pairs = [(s, o['R'] + o['F']) for s, o in pairs if s is not None]
        ic = stats.spearman([p[0] for p in pairs], [p[1] for p in pairs]) if len(pairs) >= 10 else None
        out.append((S, ic, len(pairs)))
    return out


def month_of(S):
    return dt.datetime.fromtimestamp(S / 1000, dt.UTC).strftime('%Y-%m')


def year_of(S):
    return dt.datetime.fromtimestamp(S / 1000, dt.UTC).year


def largest_positive_month(book):
    """The calendar month (of S) with the largest POSITIVE summed weekly net — the cluster most
    favourable to the hypothesis. None if no month sums above zero. Ties: the earliest."""
    sums = {}
    for b in book:
        sums[month_of(b['S'])] = sums.get(month_of(b['S']), 0.0) + b['net']
    pos = {m: v for m, v in sums.items() if v > 0}
    return min(pos, key=lambda m: (-pos[m], m)) if pos else None


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return statistics.fmean(xs) if xs else None


def evaluate(ic_rows, book, z=None):
    """The five criteria on one factor. ic_rows: weekly_ic output; book: run_book output."""
    z = Z_CRIT if z is None else z
    ics = [ic for _, ic, _ in ic_rows if ic is not None]
    nets = [b['net'] for b in book]
    ic_mean, ic_t = _mean(ics), stats.newey_west_t(ics, NW_LAGS)
    net_mean, net_t = _mean(nets), stats.newey_west_t(nets, NW_LAGS)
    med = statistics.median(nets) if nets else None
    half = len(book) // 2
    halves = [_mean([b['net'] for b in book[:half]]), _mean([b['net'] for b in book[half:]])]
    big = largest_positive_month(book)
    rest = [b['net'] for b in book if month_of(b['S']) != big]
    without = _mean(rest)
    crit = {
        '1_ic_mean_pos_nw_t': ic_mean is not None and ic_t is not None and ic_mean > 0 and ic_t >= z,
        '2_book_net_pos_nw_t': net_mean is not None and net_t is not None and net_mean > 0 and net_t >= z,
        '3_mean_median_same_sign': net_mean is not None and med is not None and (net_mean > 0) == (med > 0),
        '4_both_halves_positive': all(h is not None and h > 0 for h in halves),
        '5_without_largest_positive_month_positive': without is not None and without > 0,
    }
    return {'ic_mean': ic_mean, 'ic_t': ic_t, 'ic_sd': statistics.stdev(ics) if len(ics) > 1 else None,
            'weeks': len(book), 'ic_weeks': len(ics), 'net_mean': net_mean, 'net_t': net_t, 'net_median': med,
            'gross_mean': _mean([b['gross'] for b in book]), 'cost_mean': _mean([b['cost'] for b in book]),
            'halves': halves, 'ic_halves': [_mean(ics[:len(ics) // 2]), _mean(ics[len(ics) // 2:])],
            'largest_positive_month': big, 'without_largest_positive_month': without,
            'criteria': crit, 'pass': all(crit.values()), 'z': z}


def shuffled_scores(universe, key, rng):
    """Within each week, the factor's scores permuted across the week's SCORED coins (Revision 4: an
    unscored coin never receives a score, so the shuffled book has the real book's membership pool)."""
    out = {}
    for S in sorted(universe):
        scored = [r for r in universe[S] if r.get(key) is not None]
        syms = [r['symbol'] for r in scored]
        vals = [r[key] for r in scored]
        rng.shuffle(vals)
        out[S] = dict(zip(syms, vals))
    return out


def placebo(universe, key, outc, seeds=200, z=None):
    """Signal shuffled across coins within each week, costs kept (each shuffled book pays its own
    turnover). Returns the share of seeds passing all five criteria, criterion 1 alone and criterion
    2 alone, and the |t| tails: the share with |IC NW t| >= z and with |gross book NW t| >= z
    (two-sided, ~2 x alpha/n expected for a calibrated statistic)."""
    z = Z_CRIT if z is None else z
    n_pass = c1 = c2 = ic_tail = gross_tail = 0
    ts = []
    for s in range(seeds):
        sc = shuffled_scores(universe, key, random.Random(s))
        ic_rows = weekly_ic(universe, key, outc, sc)
        book = run_book(universe, key, outc, scores=sc)
        ev = evaluate(ic_rows, book, z)
        n_pass += ev['pass']
        c1 += ev['criteria']['1_ic_mean_pos_nw_t']
        c2 += ev['criteria']['2_book_net_pos_nw_t']
        ic_tail += ev['ic_t'] is not None and abs(ev['ic_t']) >= z
        gt = stats.newey_west_t([b['gross'] for b in book], NW_LAGS)
        gross_tail += gt is not None and abs(gt) >= z
        ts.append(ev['ic_t'])
    return {'seeds': seeds, 'pass_share': n_pass / seeds, 'crit1_share': c1 / seeds, 'crit2_share': c2 / seeds,
            'ic_abs_t_tail': ic_tail / seeds, 'gross_abs_t_tail': gross_tail / seeds,
            'ic_t_sd': statistics.pstdev([t for t in ts if t is not None]) if seeds > 1 else None, 'z': z}


def market_returns(universe, outc):
    """{S: equal-weight mean price return R over the universe coins with an entry} (runner only)."""
    out = {}
    for S, rows in universe.items():
        rs = [outc[S][r['symbol']]['R'] for r in rows if r['symbol'] in outc.get(S, {})]
        out[S] = statistics.fmean(rs) if rs else 0.0
    return out


def beta_adjusted(book, mkt):
    """Revision 3 (review M2): the residual book return net - beta_book x the equal-weight universe
    return over the hold, per week. mkt: {S: R_m}. Returns [(S, adjusted net)]."""
    return [(b['S'], b['net'] - b['beta_book'] * mkt.get(b['S'], 0.0)) for b in book]


def beta_adjusted_ok(adj, z=None):
    """Revision 3a: the beta-adjusted weekly net has mean > 0 AND Newey-West t >= z (one-sided 1.645)."""
    z = Z_CRIT if z is None else z
    m, t = _mean(adj), stats.newey_west_t(adj, NW_LAGS)
    return m is not None and t is not None and m > 0 and t >= z


def market_regression(book, mkt):
    """Revision 4 (review m1), reported: OLS of the weekly net on the equal-weight universe return over
    the hold; the intercept with a Newey-West t (the t of the mean of net - slope x m), and the slope
    (the realised book beta, against the ex-ante beta_book the adjustment uses)."""
    xs = [mkt.get(b['S'], 0.0) for b in book]
    ys = [b['net'] for b in book]
    if len(xs) < 3:
        return None
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx if sxx else 0.0
    resid = [y - slope * x for x, y in zip(xs, ys)]
    return {'intercept': statistics.fmean(resid), 'intercept_nw_t': stats.newey_west_t(resid, NW_LAGS), 'slope': slope,
            'beta_book_mean': _mean([b.get('beta_book') for b in book])}


def pull_conditions(book, mkt, book_2x):
    """Conditions of the holdout-A pull (D13), not of the discovery pass (Revision 3a): the
    beta-adjusted net has mean > 0 with Newey-West t >= 1.645, and the book at 2x slippage has a
    mean net > 0."""
    adj = [x for _, x in beta_adjusted(book, mkt)]
    n2 = [b['net'] for b in book_2x]
    out = {'beta_adjusted_mean': _mean(adj), 'beta_adjusted_t': stats.newey_west_t(adj, NW_LAGS), 'net_2x_mean': _mean(n2)}
    out['beta_adjusted_mean_pos'] = beta_adjusted_ok(adj)
    out['net_2x_mean_pos'] = out['net_2x_mean'] is not None and out['net_2x_mean'] > 0
    out['all'] = out['beta_adjusted_mean_pos'] and out['net_2x_mean_pos']
    return out


def decide(results):
    """Registration D-table: {factor: {'pass': ..., 'pull_conditions': {...}}} ->
    ('pull_holdout_A', [factors]) when a factor passes all five criteria AND meets the pull
    conditions (D13); ('close_conditions_unmet', [factors]) when it passes but a condition fails;
    otherwise ('close', [])."""
    passing = [f for f in FAMILY if results.get(f, {}).get('pass')]
    pull = [f for f in passing if results[f].get('pull_conditions', {}).get('all')]
    if pull:
        return ('pull_holdout_A', pull)
    return ('close_conditions_unmet', passing) if passing else ('close', [])


def holdout_confirm(ic_rows, book, n_taken, book_2x=None):
    """The holdout-A read (D14): CONFIRM if IC mean > 0 with NW t >= z, book net mean > 0 with NW t >= z,
    median weekly net > 0, mean >= 0 without the largest positive month, and (Revision 4, review M3)
    the book at 2x slippage has a mean net > 0; z one-sided 5% / number of factors taken."""
    z = z_crit(n_taken) if n_taken > 1 else Z_HOLDOUT_ONE
    ev = evaluate(ic_rows, book, z)
    n2 = _mean([b['net'] for b in book_2x]) if book_2x is not None else None
    ev['net_2x_mean'] = n2
    ok = (ev['criteria']['1_ic_mean_pos_nw_t'] and ev['criteria']['2_book_net_pos_nw_t']
          and ev['net_median'] is not None and ev['net_median'] > 0
          and ev['without_largest_positive_month'] is not None and ev['without_largest_positive_month'] >= 0
          and n2 is not None and n2 > 0)
    return ok, ev


def recheck_discovery(results, z=None):
    """Revision 4 (review M1): recompute, from the NUMBERS stored in a discovery row's results, every
    criterion, both pull conditions and the decision — never trusting a stored label. Returns
    (recomputed decision tuple, [failures])."""
    z = Z_CRIT if z is None else z
    fails, recomputed = [], {}
    for f in FAMILY:
        ev = results.get(f) or {}
        g = lambda k: ev.get(k)
        num = lambda x: isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)
        halves = g('halves') or [None, None]
        crit = {
            '1_ic_mean_pos_nw_t': num(g('ic_mean')) and num(g('ic_t')) and g('ic_mean') > 0 and g('ic_t') >= z,
            '2_book_net_pos_nw_t': num(g('net_mean')) and num(g('net_t')) and g('net_mean') > 0 and g('net_t') >= z,
            '3_mean_median_same_sign': num(g('net_mean')) and num(g('net_median')) and (g('net_mean') > 0) == (g('net_median') > 0),
            '4_both_halves_positive': len(halves) == 2 and all(num(h) and h > 0 for h in halves),
            '5_without_largest_positive_month_positive': num(g('without_largest_positive_month'))
                                                         and g('without_largest_positive_month') > 0,
        }
        pc = ev.get('pull_conditions') or {}
        pull = {'beta_adjusted': num(pc.get('beta_adjusted_mean')) and num(pc.get('beta_adjusted_t'))
                                 and pc['beta_adjusted_mean'] > 0 and pc['beta_adjusted_t'] >= z,
                'net_2x': num(pc.get('net_2x_mean')) and pc['net_2x_mean'] > 0}
        if (ev.get('criteria') or {}) != crit:
            fails.append(f'{f}: stored criteria differ from those recomputed from the stored statistics')
        if ev.get('pass') != all(crit.values()):
            fails.append(f'{f}: stored pass label differs from the recomputed pass')
        if pc.get('all') != all(pull.values()):
            fails.append(f'{f}: stored pull label differs from the recomputed pull conditions')
        recomputed[f] = {'pass': all(crit.values()), 'pull_conditions': {'all': all(pull.values())}}
    return decide(recomputed), fails


# --- behaviour summaries (no outcome) ----------------------------------------------------------

def turnover_path(universe, key, buffer=True, drift=True, slip_mult=1.0):
    """[(S, traded amount, cost)] of moving to each week's targets, from behaviour only. With
    drift=True (Revision 3, the registered cost estimate) the previous week's holdings are first
    drifted by each coin's trailing-week price return measured AT S (close at S - 7 d to close at S,
    `ret7_trailing`): exactly the re-sizing `run_book` pays, known before the rebalance. A held coin
    that left the universe drifts by 0 (no price read) and is sold. Weeks with no book and nothing
    held are skipped; the first row is the entry from cash. slip_mult scales the tier slippage."""
    prev, out, rate_of = {}, [], {}
    for S, w in target_path(universe, key, buffer):
        ret = {}
        for r in universe[S]:
            rate_of[r['symbol']] = TAKER + slip_mult * (r['cost'] - TAKER)     # the row's own tier (stress tiers respected)
            ret[r['symbol']] = r.get('ret7_trailing') or 0.0
        if not w and not prev:
            continue
        held = {s: x * (1 + ret.get(s, 0.0)) for s, x in prev.items()} if drift else prev
        keys = set(w) | set(held)
        traded = {s: abs(w.get(s, 0.0) - held.get(s, 0.0)) for s in keys}
        out.append((S, sum(traded.values()), sum(v * rate_of[s] for s, v in traded.items())))
        prev = w
    return out


def turnover_no_drift(universe, key, buffer=True):
    """Target-only turnover (Revisions 1-2's gate measure; it omits drift re-sizing)."""
    return turnover_path(universe, key, buffer, drift=False)


def rank_autocorr(universe, key):
    """[(S, Spearman of the score at S vs at S - 1 week over coins in both)]."""
    out, prev = [], None
    for S in sorted(universe):
        cur = {r['symbol']: r[key] for r in universe[S] if r.get(key) is not None}
        if prev:
            both = [s for s in cur if s in prev]
            if len(both) >= 10:
                out.append((S, stats.spearman([cur[s] for s in both], [prev[s] for s in both])))
        prev = cur
    return out


# --- power by simulation (behaviour inputs only) ----------------------------------------------

def _normal_scores(vals):
    rk = stats.ranks(vals)
    nd = statistics.NormalDist()
    n = len(vals)
    return [nd.inv_cdf((r - 0.5) / n) for r in rk], rk


def sim_inputs(universe, key, dispersion, buffer=True):
    """Per week, what the simulation holds fixed: the coins' score ranks (normal scores), the actual
    target weights, their ex-ante betas, the behaviour cost of moving to them WITH drift (Revision 3)
    at 1x and 2x slippage, and the week's empirical cross-sectional distribution of CONTEMPORANEOUS
    returns, demeaned (dispersion: {S: [returns]} — the trailing week's total return of a long,
    measured before S; it is the previous signal's outcome window, used unconditionally and never
    paired with a score). The demeaned week mean is the market move
    (`market_moves`)."""
    tov = {S: c for S, _, c in turnover_path(universe, key, buffer, drift=True)}
    tov2 = {S: c for S, _, c in turnover_path(universe, key, buffer, drift=True, slip_mult=2.0)}
    path = dict(target_path(universe, key, buffer))
    out = []
    for S in sorted(universe):
        rows = [r for r in universe[S] if r.get(key) is not None and r.get('sigma')]
        if len(rows) < 2 * QUANTILE or S not in tov:
            continue
        zs, rk = _normal_scores([r[key] for r in rows])
        w = path[S]
        m = statistics.fmean(dispersion[S])
        d = sorted(x - m for x in dispersion[S])
        mk = statistics.fmean(rk)
        out.append({'S': S, 'z': zs, 'rk': rk, 'rk_mean': mk, 'rk_ss': sum((a - mk) ** 2 for a in rk),
                    'w': [w.get(r['symbol'], 0.0) for r in rows], 'cost': tov[S], 'cost2x': tov2[S],
                    'beta': [r['beta'] if r.get('beta') is not None else 1.0 for r in rows], 'D': d})
    return out


def market_moves(dispersion):
    """The empirical weekly equal-weight market moves (each week's mean trailing return), from which
    the simulation draws a common factor."""
    return [statistics.fmean(v) for _, v in sorted(dispersion.items()) if v]


def _resize(d, n):
    """n quantiles of the sorted list d (so a week's dispersion can be mapped onto n coins)."""
    m = len(d)
    if m == n:
        return d
    return [d[min(m - 1, int((i + 0.5) * m / n))] for i in range(n)]


def _pearson(a, b):
    ma, mb = statistics.fmean(a), statistics.fmean(b)
    sab = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    saa = sum((x - ma) ** 2 for x in a)
    sbb = sum((y - mb) ** 2 for y in b)
    return sab / math.sqrt(saa * sbb) if saa and sbb else 0.0


def sim_once(inp, ic, sd_extra, rng, weeks=None, mkt=None):
    """One simulated history. Each week: the idiosyncratic part is the week's demeaned empirical
    return quantiles, assigned in the order of y = rho z + sqrt(1 - rho^2) e (rho = 2 sin(pi IC_w / 6),
    IC_w = ic + N(0, sd_extra)); a common market move m, drawn from the empirical weekly moves
    (`mkt`), is added as beta_i x m (Revision 3, review M2: a beta-loaded book is tested under the
    null). The IC is the Spearman (Pearson on tie-aware ranks) of the score with the coin's total
    return. Book rows carry the net, the net at 2x slippage, and the beta-adjusted net."""
    ic_rows, book = [], []
    for wk in (weeks if weeks is not None else inp):
        n = len(wk['z'])
        icw = ic + (rng.gauss(0, sd_extra) if sd_extra else 0.0)
        rho = max(-0.99, min(0.99, 2 * math.sin(math.pi * icw / 6)))
        s = math.sqrt(1 - rho * rho)
        y = [rho * zi + s * rng.gauss(0, 1) for zi in wk['z']]
        order = sorted(range(n), key=lambda i: y[i])
        d = _resize(wk['D'], n)
        m = rng.choice(mkt) if mkt else 0.0
        r = [0.0] * n
        for pos, i in enumerate(order):
            r[i] = d[pos] + wk['beta'][i] * m
        ic_rows.append((wk['S'], _pearson(wk['rk'], stats.ranks(r)), n))
        gross = sum(wi * ri for wi, ri in zip(wk['w'], r))
        bb = sum(wi * bi for wi, bi in zip(wk['w'], wk['beta']))
        net = gross - wk['cost']
        book.append({'S': wk['S'], 'gross': gross, 'cost': wk['cost'], 'net': net, 'beta_book': bb, 'm': m,
                     'adj': net - bb * m, 'net2x': gross - wk['cost2x']})
    return ic_rows, book


def simulate_power(inp, ic, sd_extra, sims=500, seed=0, z=None, holdout_weeks=None, holdout_n_taken=1, mkt=None):
    """Joint power over all five criteria at a planted weekly IC, with a common market factor drawn
    from `mkt`. Per history: P(discovery pass); P(pass AND the holdout-pull conditions: beta-adjusted
    net NW t >= z (Revision 3a), mean net at 2x slippage > 0); with holdout_weeks, P(holdout confirm) on a contiguous
    circular block of the same week structures, and end to end = P(pull and confirm) in the same
    history."""
    z = Z_CRIT if z is None else z
    rng = random.Random(seed)
    p = c1 = c2 = pull = conf = e2e = 0
    nets, ics = [], []
    for _ in range(sims):
        ic_rows, book = sim_once(inp, ic, sd_extra, rng, mkt=mkt)
        ev = evaluate(ic_rows, book, z)
        p += ev['pass']
        c1 += ev['criteria']['1_ic_mean_pos_nw_t']
        c2 += ev['criteria']['2_book_net_pos_nw_t']
        cond = ev['pass'] and beta_adjusted_ok([b['adj'] for b in book], z) and statistics.fmean(b['net2x'] for b in book) > 0
        pull += cond
        nets.append(ev['net_mean'])
        ics.append(ev['ic_mean'])
        if holdout_weeks:
            st = rng.randrange(len(inp))
            blk = [inp[(st + k) % len(inp)] for k in range(holdout_weeks)]
            base = SPLITS['holdout_A'][0] + WEEK
            blk = [dict(wk, S=base + k * WEEK) for k, wk in enumerate(blk)]
            hi, hb = sim_once(inp, ic, sd_extra, rng, weeks=blk, mkt=mkt)
            ok = holdout_confirm(hi, hb, holdout_n_taken, book_2x=[{'net': b['net2x']} for b in hb])[0]
            conf += ok
            e2e += cond and ok
    out = {'ic': ic, 'sd_extra': sd_extra, 'sims': sims, 'p_pass': p / sims, 'p_crit1': c1 / sims,
           'p_crit2': c2 / sims, 'p_pass_and_pull': pull / sims, 'mean_net': statistics.fmean(nets),
           'mean_ic': statistics.fmean(ics), 'sd_mean_net': statistics.stdev(nets) if sims > 1 else None,
           'mean_cost': statistics.fmean(wk['cost'] for wk in inp),
           'mean_cost2x': statistics.fmean(wk['cost2x'] for wk in inp)}
    if holdout_weeks:
        out['holdout_weeks'] = holdout_weeks
        out['p_holdout_confirm'] = conf / sims
        out['end_to_end'] = e2e / sims
    return out


def sampling_sd_ic(inp):
    """Mean over weeks of 1/sqrt(n - 1): the IC's s.d. from finite breadth alone."""
    return statistics.fmean(1 / math.sqrt(len(w['z']) - 1) for w in inp)


def extra_for_total(inp, total_sd):
    """The week-level IC noise to add so the IC's total s.d. is total_sd."""
    s = sampling_sd_ic(inp)
    return math.sqrt(max(0.0, total_sd ** 2 - s ** 2))
