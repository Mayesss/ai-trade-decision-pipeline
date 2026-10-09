"""Hyperliquid's own trade tape from the archive fills (every fill, not only liquidations).
Point-in-time price lookups via DuckDB over the day files. No forward return here; the
callers decide what is 'before' and what is 'after'.
"""
import datetime as dt

from .archive_fills import day_file
from .leaders import con

DAY = 86_400_000


def _files(dex, t_from_ms, t_to_ms):
    d0 = dt.datetime.fromtimestamp(t_from_ms / 1000, dt.UTC).date()
    d1 = dt.datetime.fromtimestamp(t_to_ms / 1000, dt.UTC).date()
    out = []
    d = d0
    while d <= d1:
        f = day_file(dex, d.isoformat())
        if f.exists():
            out.append(str(f))
        d += dt.timedelta(days=1)
    return out


def last_price_before(dex, coins, t_ms, lookback_ms=6 * 3_600_000):
    """{coin: last traded price strictly before t_ms} over the previous `lookback_ms`."""
    files = _files(dex, t_ms - lookback_ms, t_ms)
    if not files:
        return {}
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE tape_coins(coin VARCHAR)')
    c.executemany('INSERT INTO tape_coins VALUES (?)', [(x,) for x in coins])
    rows = c.execute(f"""
        SELECT coin, arg_max(CAST(price AS DOUBLE), (epoch_ms(timestamp), filename, file_row_number))
        FROM read_parquet({files!r}, filename = true, file_row_number = true)
        WHERE coin IN (SELECT coin FROM tape_coins) AND epoch_ms(timestamp) < {t_ms}
          AND epoch_ms(timestamp) >= {t_ms - lookback_ms}
        GROUP BY coin""").fetchall()
    return dict(rows)


def first_touch(dex, levels, t_from_ms, t_to_ms):
    """levels: [(key, coin, side, level)]; side 'L' = a resting BID at `level` (filled when a trade
    prints at or below it), 'S' = a resting ASK (trade at or above). Returns {key: first fill time}
    within [t_from, t_to)."""
    files = _files(dex, t_from_ms, t_to_ms)
    if not files or not levels:
        return {}
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE lv(key VARCHAR, coin VARCHAR, side VARCHAR, level DOUBLE)')
    c.executemany('INSERT INTO lv VALUES (?, ?, ?, ?)', [(str(k), co, s, float(l)) for k, co, s, l in levels])
    rows = c.execute(f"""
        SELECT lv.key, MIN(epoch_ms(f.timestamp))
        FROM read_parquet({files!r}) f JOIN lv ON f.coin = lv.coin
        WHERE epoch_ms(f.timestamp) >= {t_from_ms} AND epoch_ms(f.timestamp) < {t_to_ms}
          AND ((lv.side = 'L' AND CAST(f.price AS DOUBLE) <= lv.level) OR (lv.side = 'S' AND CAST(f.price AS DOUBLE) >= lv.level))
        GROUP BY lv.key""").fetchall()
    return {k: t for k, t in rows}


def prices_at(dex, queries):
    """queries: [(key, coin, t_ms)] -> {key: last trade price at or before t_ms (within 2 h)}."""
    if not queries:
        return {}
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE pq(key VARCHAR, coin VARCHAR, t BIGINT)')
    c.executemany('INSERT INTO pq VALUES (?, ?, ?)', [(str(k), co, int(t)) for k, co, t in queries])
    t_min, t_max = min(t for _, _, t in queries), max(t for _, _, t in queries)
    files = _files(dex, t_min - 2 * 3_600_000, t_max)
    rows = c.execute(f"""
        SELECT pq.key, arg_max(CAST(f.price AS DOUBLE), epoch_ms(f.timestamp))
        FROM read_parquet({files!r}) f JOIN pq ON f.coin = pq.coin
        WHERE epoch_ms(f.timestamp) <= pq.t AND epoch_ms(f.timestamp) > pq.t - 7_200_000
        GROUP BY pq.key""").fetchall()
    return dict(rows)


def first_touch_windows(dex, orders):
    """orders: [(key, coin, side, level, t_from, t_to)] with per-order windows; returns {key: first
    fill time}. Files are scanned once over the batch's overall span, so callers batch by day."""
    if not orders:
        return {}
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE lw(key VARCHAR, coin VARCHAR, side VARCHAR, level DOUBLE, t0 BIGINT, t1 BIGINT)')
    c.executemany('INSERT INTO lw VALUES (?, ?, ?, ?, ?, ?)',
                  [(str(k), co, s, float(l), int(a), int(b)) for k, co, s, l, a, b in orders])
    files = _files(dex, min(o[4] for o in orders), max(o[5] for o in orders))
    if not files:
        return {}
    rows = c.execute(f"""
        SELECT lw.key, MIN(epoch_ms(f.timestamp))
        FROM read_parquet({files!r}) f JOIN lw ON f.coin = lw.coin
        WHERE epoch_ms(f.timestamp) > lw.t0 AND epoch_ms(f.timestamp) <= lw.t1
          AND ((lw.side = 'L' AND CAST(f.price AS DOUBLE) <= lw.level) OR (lw.side = 'S' AND CAST(f.price AS DOUBLE) >= lw.level))
        GROUP BY lw.key""").fetchall()
    return {k: t for k, t in rows}
