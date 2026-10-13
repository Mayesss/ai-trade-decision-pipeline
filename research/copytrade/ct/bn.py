"""Binance USDⓈ-M lookups for registration 006 (H3): aggTrades and bookDepth day files under
data/binance/um/*_parquet/<SYMBOL>/date=YYYY-MM-DD.parquet (pulled by h3_fetch.py, SHA-256
pinned in data/binance/manifest.json). DuckDB over local parquet; no network.

Clock. Every function takes and returns times on HYPERLIQUID's clock: a Binance trade with
transact_time T is placed at T + offset_ms (registration 006 §3: the offset is estimated from
non-event periods and fixed before any outcome). A query 'at or before t' reads transact_time
<= t - offset_ms.

Ordering. aggTrade ids increase with transact_time (checked: 0 inversions on the largest day);
same-millisecond trades are ordered by agg_trade_id, so 'last at or before' and 'first at or
after' are deterministic.

Nothing here decides what is before or after an event: callers do. Every file read is
asserted to be dated before 2026-07-01 (the holdout).
"""
import datetime as dt

from .leaders import con
from .net import DATA

ROOT = DATA / 'binance' / 'um'
SYMBOL = {'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'SOL': 'SOLUSDT', 'XRP': 'XRPUSDT', 'DOGE': 'DOGEUSDT',
          'HYPE': 'HYPEUSDT'}
HOLDOUT_DAY = dt.date(2026, 7, 1)
DAY = 86_400_000
_verify = None                 # the guarded run installs a per-file SHA-256 check (l8_run, data manifest)


def set_verifier(fn):
    """fn(path) -> None, raising on a file whose SHA-256 is not the manifest's. None: no check."""
    global _verify
    _verify = fn


def _day(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).date()


def files(dataset, symbol, t0_ms, t1_ms):
    """Existing day files of `dataset` ('aggTrades' / 'bookDepth') covering [t0, t1] (Binance
    clock). Asserts no file at or after the holdout."""
    out = []
    d, d1 = _day(t0_ms), _day(t1_ms)
    while d <= d1:
        assert d < HOLDOUT_DAY, f'holdout file requested: {symbol} {d}'
        f = ROOT / f'{dataset}_parquet' / symbol / f'date={d.isoformat()}.parquet'
        if f.exists():
            if _verify is not None:
                _verify(f)
            out.append(str(f))
        d += dt.timedelta(days=1)
    return out


def has_days(symbol, t0_ms, t1_ms, dataset='aggTrades'):
    """True when every UTC day touched by [t0, t1] has a local file."""
    d, d1 = _day(t0_ms), _day(t1_ms)
    n = 0
    while d <= d1:
        n += 1
        d += dt.timedelta(days=1)
    return len(files(dataset, symbol, t0_ms, t1_ms)) == n


def last_at_or_before(symbol, queries, offset_ms=0, lookback_ms=60_000, taker=None):
    """queries: [(key, t)] on HL's clock -> {key: (t_hl_clock, price)} of the LAST aggTrade with
    transact_time + offset <= t, within lookback_ms (deterministic: (transact_time, id)). taker:
    None = any; 'buy' = buyer took (is_buyer_maker false: a print at the ASK); 'sell' = seller took
    (is_buyer_maker true: a print at the BID). A price at the instant, nothing after it."""
    if not queries:
        return {}
    lo = min(t for _, t in queries) - offset_ms - lookback_ms
    hi = max(t for _, t in queries) - offset_ms
    fs = files('aggTrades', symbol, lo, hi)
    if not fs:
        return {}
    c = con()
    cond = '' if taker is None else ('AND NOT is_buyer_maker' if taker == 'buy' else 'AND is_buyer_maker')
    c.execute('CREATE OR REPLACE TEMP TABLE bq(k VARCHAR, t BIGINT)')
    c.executemany('INSERT INTO bq VALUES (?, ?)', [(str(k), int(t) - int(offset_ms)) for k, t in queries])
    rows = c.execute(f"""
        WITH a AS (SELECT transact_time AS tt, arg_max(price, agg_trade_id) AS px
                   FROM read_parquet({fs!r}) WHERE transact_time >= {lo} AND transact_time <= {hi} {cond} GROUP BY 1)
        SELECT bq.k, a.tt, a.px FROM bq ASOF JOIN a ON bq.t >= a.tt
        WHERE a.tt > bq.t - {int(lookback_ms)}""").fetchall()
    return {k: (tt + offset_ms, px) for k, tt, px in rows}


def first_at_or_after(symbol, queries, offset_ms=0, window_ms=5_000, taker=None, times_only=False):
    """queries: [(key, t)] on HL's clock -> {key: (t_hl_clock, price)} of the FIRST aggTrade with
    transact_time + offset >= t, within window_ms. taker: None = any trade; 'buy' = buyer was
    the taker (is_buyer_maker false: a print at the ask); 'sell' = seller the taker (at the bid).
    times_only=True returns {key: (t, None)} — the timing of the next trade, never its price."""
    if not queries:
        return {}
    lo = min(t for _, t in queries) - offset_ms
    hi = max(t for _, t in queries) - offset_ms + window_ms
    fs = files('aggTrades', symbol, lo, hi)
    if not fs:
        return {}
    cond = '' if taker is None else ('AND NOT is_buyer_maker' if taker == 'buy' else 'AND is_buyer_maker')
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE fq(k VARCHAR, t BIGINT)')
    c.executemany('INSERT INTO fq VALUES (?, ?)', [(str(k), int(t) - int(offset_ms)) for k, t in queries])
    px = 'NULL' if times_only else 'arg_min(price, agg_trade_id)'
    rows = c.execute(f"""
        WITH a AS (SELECT transact_time AS tt, {px} AS px
                   FROM read_parquet({fs!r}) WHERE transact_time >= {lo} AND transact_time <= {hi} {cond} GROUP BY 1)
        SELECT fq.k, a.tt, a.px FROM fq ASOF JOIN a ON fq.t <= a.tt
        WHERE a.tt <= fq.t + {int(window_ms)}""").fetchall()
    return {k: (tt + offset_ms, px) for k, tt, px in rows}


def window_trades(symbol, t0, t1, offset_ms=0):
    """[(t_hl_clock, price, is_buyer_maker)] of every aggTrade with t0 <= transact_time + offset
    <= t1, in (transact_time, id) order. The caller bounds [t0, t1]."""
    fs = files('aggTrades', symbol, t0 - offset_ms, t1 - offset_ms)
    if not fs:
        return []
    rows = con().execute(f"""
        SELECT transact_time, price, is_buyer_maker FROM read_parquet({fs!r})
        WHERE transact_time >= {int(t0 - offset_ms)} AND transact_time <= {int(t1 - offset_ms)}
        ORDER BY transact_time, agg_trade_id""").fetchall()
    return [(t + offset_ms, p, m) for t, p, m in rows]


def bucket_last(symbol, t0, t1, bucket_ms, offset_ms=0):
    """{bucket index: last price in the bucket} for buckets floor((transact_time + offset) /
    bucket_ms) over [t0, t1) on HL's clock. For the non-event clock-alignment study."""
    fs = files('aggTrades', symbol, t0 - offset_ms, t1 - offset_ms)
    if not fs:
        return {}
    rows = con().execute(f"""
        SELECT (transact_time + {int(offset_ms)}) // {int(bucket_ms)} AS b, arg_max(price, agg_trade_id)
        FROM read_parquet({fs!r})
        WHERE transact_time >= {int(t0 - offset_ms)} AND transact_time < {int(t1 - offset_ms)} GROUP BY 1""").fetchall()
    return dict(rows)


def pre_window_stats(symbol, queries, window_ms=10_000, offset_ms=0):
    """Trade intensity and a spread proxy in the window BEFORE each query time: for [(key, t)],
    trades with t - window <= transact_time + offset < t. Returns {key: (n_trades, median
    inter-trade ms, median |relative price change| between consecutive trades whose taker side
    flipped (the bid-ask bounce, a full-spread proxy), n_flips)}. Nothing at or after t."""
    if not queries:
        return {}
    lo = min(t for _, t in queries) - offset_ms - window_ms
    hi = max(t for _, t in queries) - offset_ms
    fs = files('aggTrades', symbol, lo, hi)
    if not fs:
        return {}
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE pw(k VARCHAR, t BIGINT)')
    c.executemany('INSERT INTO pw VALUES (?, ?)', [(str(k), int(t) - int(offset_ms)) for k, t in queries])
    rows = c.execute(f"""
        WITH a AS (SELECT transact_time AS tt, agg_trade_id AS id, price, is_buyer_maker AS m
                   FROM read_parquet({fs!r}) WHERE transact_time >= {lo} AND transact_time < {hi}),
        j AS (SELECT pw.k, a.tt, a.id, a.price, a.m,
                     lag(a.tt) OVER w AS ptt, lag(a.price) OVER w AS pp, lag(a.m) OVER w AS pm
              FROM pw JOIN a ON a.tt >= pw.t - {int(window_ms)} AND a.tt < pw.t
              WINDOW w AS (PARTITION BY pw.k ORDER BY a.tt, a.id))
        SELECT k, COUNT(*), median(tt - ptt),
               median(CASE WHEN pm IS NOT NULL AND pm <> m THEN abs(price / pp - 1) END),
               COUNT(CASE WHEN pm IS NOT NULL AND pm <> m THEN 1 END)
        FROM j GROUP BY k""").fetchall()
    return {k: (n, dtm, bounce, nf) for k, n, dtm, bounce, nf in rows}


def depth_before(symbol, queries, pct=0.2, offset_ms=0):
    """{key: (snapshot t_hl_clock, notional at -pct%, notional at +pct%)} of the LAST bookDepth
    snapshot strictly before each query time (cumulative depth within pct% of mid)."""
    if not queries:
        return {}
    lo = min(t for _, t in queries) - offset_ms - 10 * 60_000
    hi = max(t for _, t in queries) - offset_ms
    fs = files('bookDepth', symbol, lo, hi)
    if not fs:
        return {}
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE dq(k VARCHAR, t BIGINT)')
    c.executemany('INSERT INTO dq VALUES (?, ?)', [(str(k), int(t) - int(offset_ms)) for k, t in queries])
    rows = c.execute(f"""
        WITH s AS (SELECT epoch_ms(timestamp) AS ts,
                          max(CASE WHEN percentage = {-pct} THEN notional END) AS bid,
                          max(CASE WHEN percentage = {pct} THEN notional END) AS ask
                   FROM read_parquet({fs!r}) WHERE epoch_ms(timestamp) >= {lo} AND epoch_ms(timestamp) < {hi}
                   GROUP BY 1)
        SELECT dq.k, s.ts, s.bid, s.ask FROM dq ASOF JOIN s ON dq.t > s.ts""").fetchall()
    return {k: (ts + offset_ms, b, a) for k, ts, b, a in rows}
