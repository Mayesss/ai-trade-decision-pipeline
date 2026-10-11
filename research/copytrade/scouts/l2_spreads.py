"""L2 scout, question B: effective-spread proxy from local tapes (behaviour only).

Proxy: consecutive taker prints of opposite side within 1 s; the gap
(buy px - sell px) / mid, in bp, median per coin. Contemporaneous prices only,
no forward return.

- Hyperliquid: local pruned archive, fixed sample = the 15th of each month,
  2025-08 .. 2026-06 (all < 2026-07-01).
- Binance USDT-M: the local aggTrades parquet, which exists only on HL-cascade
  days (stress-biased; an upper bound for normal days).

  .venv/bin/python -I scouts/l2_spreads.py
"""
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
HL = ROOT / 'data' / 'pruned' / 'by_dex' / 'hyperliquid' / 'fills' / 'perp' / 'all'
BN = ROOT / 'data' / 'binance' / 'um' / 'aggTrades_parquet'
OUT = ROOT / 'data' / 'scout_l2' / 'spreads.json'
DAYS = [f'2025-{m:02d}-15' for m in range(8, 13)] + [f'2026-{m:02d}-15' for m in range(1, 7)]


def main():
    con = duckdb.connect()
    con.execute('SET enable_progress_bar=false')
    files = [str(HL / f'date={d}' / 'fills.parquet') for d in DAYS if (HL / f'date={d}').exists()]
    hl = con.execute(f"""
        WITH t AS (
          SELECT coin, epoch_ms(timestamp) ms, trade_id, side, price::DOUBLE px
          FROM read_parquet({files}) WHERE crossed AND timestamp < TIMESTAMP '2026-07-01'),
        p AS (
          SELECT coin, ms, side, px,
                 lag(ms) OVER w pms, lag(side) OVER w pside, lag(px) OVER w ppx
          FROM t WINDOW w AS (PARTITION BY coin ORDER BY ms, trade_id))
        SELECT coin, count(*) n,
               median(CASE WHEN side = 'B' THEN px - ppx ELSE ppx - px END / ((px + ppx) / 2) * 1e4) eff_bp
        FROM p WHERE pside IS NOT NULL AND pside <> side AND ms - pms <= 1000
        GROUP BY coin HAVING count(*) >= 200 ORDER BY coin""").fetchall()
    bn = []
    for d in sorted(BN.iterdir()):
        fs = sorted(str(f) for f in d.glob('date=*.parquet') if f.name < 'date=2026-07-01')
        if not fs:
            continue
        r = con.execute(f"""
            WITH t AS (SELECT transact_time ms, agg_trade_id id, NOT is_buyer_maker buy, price::DOUBLE px
                       FROM read_parquet({fs})),
            p AS (SELECT ms, buy, px, lag(ms) OVER w pms, lag(buy) OVER w pbuy, lag(px) OVER w ppx
                  FROM t WINDOW w AS (ORDER BY id))
            SELECT count(*), median(CASE WHEN buy THEN px - ppx ELSE ppx - px END / ((px + ppx) / 2) * 1e4)
            FROM p WHERE pbuy IS NOT NULL AND pbuy <> buy AND ms - pms <= 1000""").fetchone()
        bn.append((d.name, len(fs), r[0], r[1]))
    OUT.write_text(json.dumps({'hl': hl, 'binance_cascade_days': bn}))
    for row in hl:
        if row[0] in ('BTC', 'ETH', 'SOL', 'DOGE', 'XRP', 'HYPE'):
            print('HL', row)
    others = sorted(r[2] for r in hl if r[0] not in ('BTC', 'ETH', 'SOL'))
    q = lambda p: others[int(p * (len(others) - 1))]
    print('HL others: coins', len(others), 'p25/p50/p75 of per-coin median eff spread bp',
          round(q(.25), 2), round(q(.5), 2), round(q(.75), 2))
    for row in bn:
        print('BN', row)


if __name__ == '__main__':
    main()
