"""L1 scout: per (address, day) activity of every listed vault and every vault leader
in the pruned discovery archive (dates < 2026-07-01). Behaviour only: fill counts,
notional traded, taker share, liquidation count. Never reads realized_pnl.

    .venv/bin/python -I scouts/l1_archive.py
Writes data/scout_l1/archive/daily.parquet (resumable per day under data/scout_l1/archive/days/).
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import duckdb  # noqa: E402

from l1_common import DATA, FILLS_DIR, OUT, fill_days, load_list  # noqa: E402


def main():
    vaults = load_list()
    wanted = sorted({v['address'] for v in vaults} | {v['leader'] for v in vaults})
    dest = OUT / 'archive' / 'days'
    dest.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='3GB'; SET threads=6; SET temp_directory='{DATA / 'duckdb_tmp'}'")
    con.execute('CREATE TABLE wanted(address VARCHAR)')
    con.executemany('INSERT INTO wanted VALUES (?)', [(a,) for a in wanted])
    days = fill_days()
    t0 = time.time()
    for i, d in enumerate(days):
        out = dest / f'{d}.parquet'
        if out.exists():
            continue
        src = FILLS_DIR / f'date={d}' / 'fills.parquet'
        con.execute(f"""
            COPY (
              SELECT address, DATE '{d}' AS day, COUNT(*) AS n_fills,
                     SUM(CAST(size AS DOUBLE) * CAST(price AS DOUBLE)) AS notional,
                     COUNT(*) FILTER (WHERE crossed) AS n_taker,
                     COUNT(*) FILTER (WHERE COALESCE(is_liquidation, false)) AS n_liq,
                     COUNT(DISTINCT coin) AS n_coins,
                     MIN(epoch_ms(timestamp)) AS first_ms, MAX(epoch_ms(timestamp)) AS last_ms
              FROM read_parquet('{src}')
              WHERE address IN (SELECT address FROM wanted)
              GROUP BY address
            ) TO '{out}.tmp' (FORMAT parquet)""")
        Path(f'{out}.tmp').replace(out)
        if i % 20 == 0:
            print(d, f'{i + 1}/{len(days)}', f'{time.time() - t0:.0f}s', flush=True)
    con.execute(f"COPY (SELECT * FROM read_parquet('{dest}/*.parquet')) TO '{OUT / 'archive' / 'daily.parquet'}' (FORMAT parquet)")
    print('done', con.execute(f"SELECT COUNT(*), COUNT(DISTINCT address) FROM read_parquet('{OUT / 'archive' / 'daily.parquet'}')").fetchall())


if __name__ == '__main__':
    main()
