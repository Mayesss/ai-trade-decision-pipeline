"""Behaviour-only wallet features at every discovery selection date.

Plan §5.2: cleaning and copyability thresholds may be set from feature
distributions; no copy return may be computed before the P4 registration is
committed. So this script never reads `realized_pnl`, and nothing it outputs
is a return. Account value is read as-of the selection date only (a size
filter), never as growth.

Per (dex, selection date), over the lookback [S - 91d, S):
  n_fills, active_days, n_coins, notional, maker_share (by notional; crossed
  = taker), twap_share, n_liquidations, round trips completed, median hold
  (completed trips only — trips still open at S are counted separately, so
  long holders are not silently dropped), share of trips under 1 h,
  mapped_share (notional in coins that map to the execution venue),
and from the snapshot equity table: account value and gross leverage as-of
S (latest snapshot strictly before S), median leverage over the lookback.

Round trips need execution order: rows are ordered by (timestamp, file,
row within file) — file order is execution order within a millisecond (P3
gate G3). A flip ("Long > Short") closes one trip and opens the next.

    .venv/bin/python p4_features.py [--dex xyz] [--only 2026-01-12]
Writes data/derived/<dex>/features_<S>.parquet.
"""
import argparse
import datetime as dt
import json
import time

import duckdb

from ct import schedule
from ct.archive_fills import day_file
from ct.net import DATA

EPS = 1e-9


def lookback_files(dex, start, end):
    days = [start + dt.timedelta(days=i) for i in range((end - start).days)]
    return [str(day_file(dex, d.isoformat())) for d in days if day_file(dex, d.isoformat()).exists()]


def mapped_coins(dex):
    """Coins that map to the execution venue. Main dex: the P0 symbol table
    (verified by price, 2026-10-07 — current, not point-in-time; listing dates
    are applied at P5). xyz: Capital mapping does not exist yet (T1), so None."""
    if dex != 'hyperliquid':
        return None
    table = json.loads((DATA / 'p0/2026-10-07/symbols.json').read_text())
    return [c for c, r in table.items() if r['status'] == 'ok']


def features(con, dex, sel, lb_start):
    files = lookback_files(dex, lb_start, sel)
    sel_ms = schedule.ms(sel)
    lb_ms = schedule.ms(lb_start)
    mapped = mapped_coins(dex)
    con.execute('DROP TABLE IF EXISTS mapped')
    con.execute('CREATE TEMP TABLE mapped(coin VARCHAR)')
    if mapped:
        con.executemany('INSERT INTO mapped VALUES (?)', [(c,) for c in mapped])
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW f AS
        SELECT address, coin, timestamp, side,
               CAST(size AS DOUBLE) AS size, CAST(price AS DOUBLE) AS price,
               CAST(start_position AS DOUBLE) AS sp, crossed,
               twap_id IS NOT NULL AS is_twap, COALESCE(is_liquidation, false) AS is_liq,
               filename, file_row_number
        FROM read_parquet({files!r}, filename = true, file_row_number = true)
    """)
    # Intermediate results stay INSIDE DuckDB as temp tables. Registering
    # pyarrow tables back into the connection deadlocked (ArrowScan mutex wait
    # at 0% CPU, duckdb 1.5.6 / Python 3.14) — only the final result leaves.
    # A filtered SUM over zero rows is NULL, not 0: without COALESCE a wallet
    # with no maker fills had maker_share NULL and fell out of any maker filter
    # (caught by the Python cross-check, 2026-10-07).
    con.execute("""
        CREATE OR REPLACE TEMP TABLE activity AS
        SELECT address,
               COUNT(*) AS n_fills,
               COUNT(DISTINCT CAST(timestamp AS DATE)) AS active_days,
               COUNT(DISTINCT coin) AS n_coins,
               SUM(size * price) AS notional,
               COALESCE(SUM(size * price) FILTER (WHERE NOT crossed), 0) / SUM(size * price) AS maker_share,
               COALESCE(SUM(size * price) FILTER (WHERE is_twap), 0) / SUM(size * price) AS twap_share,
               COUNT(*) FILTER (WHERE is_liq) AS n_liquidations,
               COALESCE(SUM(size * price) FILTER (WHERE coin IN (SELECT coin FROM mapped)), 0) / SUM(size * price) AS mapped_share
        FROM f GROUP BY address
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE trips AS
        WITH s AS (
            SELECT address, coin, timestamp, sp,
                   sp + CASE WHEN side = 'buy' THEN size ELSE -size END AS ep,
                   filename, file_row_number
            FROM f
        ), e AS (
            SELECT *,
                   (sp * ep < 0) AS is_flip,
                   (abs(sp) < {EPS} AND abs(ep) > {EPS}) OR (sp * ep < 0) AS is_open,
                   (abs(sp) > {EPS} AND abs(ep) <= {EPS} * greatest(1, abs(sp))) OR (sp * ep < 0) AS is_close
            FROM s
        ), q AS (
            SELECT *, SUM(CAST(is_open AS INT)) OVER (
                       PARTITION BY address, coin ORDER BY timestamp, filename, file_row_number
                       ROWS UNBOUNDED PRECEDING) AS seq
            FROM e WHERE is_open OR is_close
        ), opens AS (
            SELECT address, coin, seq, timestamp AS t_open FROM q WHERE is_open
        ), closes AS (
            SELECT address, coin, seq - CAST(is_flip AS INT) AS seq, MIN(timestamp) AS t_close
            FROM q WHERE is_close GROUP BY 1, 2, 3
        )
        SELECT o.address,
               COUNT(c.t_close) AS round_trips,
               COUNT(*) - COUNT(c.t_close) AS open_at_selection,
               MEDIAN(epoch(c.t_close) - epoch(o.t_open)) / 3600.0 AS median_hold_h,
               AVG(CASE WHEN epoch(c.t_close) - epoch(o.t_open) < 3600 THEN 1.0 ELSE 0.0 END)
                   FILTER (WHERE c.t_close IS NOT NULL) AS share_trips_under_1h
        FROM opens o LEFT JOIN closes c USING (address, coin, seq)
        GROUP BY o.address
    """)
    snaps = str(DATA / f'derived/{dex}/snapshot_accounts.parquet')
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE equity AS
        WITH s AS (
            SELECT "user" AS address, snapshot_ms, account_value,
                   gross_notional / NULLIF(account_value, 0) AS lev
            FROM read_parquet('{snaps}')
            WHERE snapshot_ms < {sel_ms} AND snapshot_ms >= {lb_ms}
        )
        SELECT address,
               arg_max(account_value, snapshot_ms) AS account_value,
               arg_max(lev, snapshot_ms) AS leverage_at_selection,
               MEDIAN(lev) AS median_leverage,
               (MAX(snapshot_ms) - {sel_ms}) / 3600000.0 AS equity_age_h
        FROM s GROUP BY address
    """)
    out = con.execute("""
        SELECT a.*, t.round_trips, t.open_at_selection, t.median_hold_h, t.share_trips_under_1h,
               e.account_value, e.leverage_at_selection, e.median_leverage, e.equity_age_h,
               a.n_fills / a.active_days AS fills_per_active_day
        FROM activity a
        LEFT JOIN trips t USING (address)
        LEFT JOIN equity e USING (address)
    """).arrow()
    if hasattr(out, 'read_all'):  # duckdb >= 1.5 returns a RecordBatchReader
        out = out.read_all()
    return out, len(files)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dex', choices=list(schedule.ARCHIVE_START), action='append')
    ap.add_argument('--only')
    args = ap.parse_args()
    tmp = DATA / 'duckdb_tmp'
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '9GB'; SET threads = 8; SET temp_directory = '{tmp}'; "
                "SET preserve_insertion_order = false")
    import pyarrow.parquet as pq
    for dex in args.dex or list(schedule.ARCHIVE_START):
        for sel, lb_start, _ in schedule.windows(dex):
            if args.only and sel.isoformat() != args.only:
                continue
            t0 = time.time()
            table, n_files = features(con, dex, sel, lb_start)
            dest = DATA / f'derived/{dex}/features_{sel}.parquet'
            pq.write_table(table, dest, compression='zstd')
            print(f'{dex} {sel}: {n_files} lookback days, {table.num_rows:,} wallets, {time.time() - t0:.0f}s -> {dest.name}')


if __name__ == '__main__':
    main()
