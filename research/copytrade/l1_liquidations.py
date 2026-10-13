"""Liquidation workstream, step 1 — extract every liquidation fill in the discovery period
and a per-(snapshot, coin) open-interest proxy. Behaviour-only: events and their own fill
prices, no forward return anywhere.

    .venv/bin/python l1_liquidations.py
Writes data/derived/hyperliquid/liquidations.parquet and oi_by_coin_snapshot.parquet.
"""
import datetime as dt
import time

from ct import schedule
from ct.archive_fills import day_file
from ct.leaders import con
from ct.net import DATA

DEX = 'hyperliquid'
START, END = schedule.ARCHIVE_START[DEX], schedule.HOLDOUT_START


def main():
    c = con()
    out = DATA / f'derived/{DEX}'
    days = [START + dt.timedelta(days=i) for i in range((END - START).days)]
    files = [str(day_file(DEX, d.isoformat())) for d in days if day_file(DEX, d.isoformat()).exists()]
    t0 = time.time()
    c.execute(f"""
        COPY (
            SELECT address, coin, epoch_ms(timestamp) AS t, side, direction,
                   CAST(size AS DOUBLE) AS size, CAST(price AS DOUBLE) AS price,
                   CAST(size AS DOUBLE) * CAST(price AS DOUBLE) AS notional,
                   CAST(start_position AS DOUBLE) AS start_position, trade_id
            FROM read_parquet({files!r}, filename = true, file_row_number = true)
            WHERE is_liquidation
            ORDER BY timestamp, filename, file_row_number
        ) TO '{out / "liquidations.parquet"}' (FORMAT PARQUET)
    """)
    n = c.execute(f"SELECT COUNT(*), COUNT(DISTINCT coin), MIN(t), MAX(t) FROM read_parquet('{out / 'liquidations.parquet'}')").fetchone()
    print(f'liquidation fills: {n[0]:,} rows, {n[1]} coins, {len(files)} days, {time.time() - t0:.0f} s')
    snaps = sorted((DATA / f'archive/by_dex/{DEX}/snapshots/perp').glob('date=*/*.parquet'))
    snaps = [str(p) for p in snaps if int(p.stem.split('_')[1]) < schedule.ms(END)]
    t0 = time.time()
    c.execute(f"""
        COPY (
            SELECT CAST(regexp_extract(filename, '_(\\d+)\\.parquet$', 1) AS BIGINT) AS snapshot_ms, market AS coin,
                   SUM(abs(notional)) AS gross_notional,
                   SUM(abs(notional)) FILTER (WHERE size > 0) AS long_notional,
                   SUM(abs(notional)) FILTER (WHERE size < 0) AS short_notional,
                   COUNT(*) AS positions,
                   COUNT(*) FILTER (WHERE liquidation_price IS NOT NULL) AS with_liq_price
            FROM read_parquet({snaps!r}, filename = true)
            WHERE size != 0
            GROUP BY 1, 2
            ORDER BY 1, 2
        ) TO '{out / "oi_by_coin_snapshot.parquet"}' (FORMAT PARQUET)
    """)
    n = c.execute(f"SELECT COUNT(*), COUNT(DISTINCT snapshot_ms) FROM read_parquet('{out / 'oi_by_coin_snapshot.parquet'}')").fetchone()
    print(f'oi proxy: {n[0]:,} (snapshot, coin) rows over {n[1]} snapshots, {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
