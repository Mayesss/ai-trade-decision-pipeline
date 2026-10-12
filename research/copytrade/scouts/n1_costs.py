"""N1 scout, item 3: cost and depth proxies by session, contemporaneous only (<= 1 s).

Sample: every 3rd calendar day 2025-10-13 -> 2026-06-30 (rotates weekdays;
fixed before looking at anything). A taker EVENT = the taker's fills in one
(coin, timestamp, address, side) - one aggressive order hitting the book.
  touched  = the event's first fill price in book order (best price touched:
             min for a buy, max for a sell)
  sweep_bp = |vwap - touched| / touched (impact of the event inside itself)
  spread proxy = (buy-touched - sell-touched) / mid, between an event and the
             most recent OPPOSITE-side taker event on the same coin no more than
             1 s earlier. Nothing is compared across more than 1 s.
Reports per class x session: median / p75 / p90 spread proxy, sweep cost,
event-size quantiles, TWAP share, liquidation share. No markouts, no P&L.

    .venv/bin/python -I scouts/n1_costs.py
"""
import csv
import datetime as dt
import sys

import duckdb

sys.path.insert(0, __import__('os').path.dirname(__file__))
from n1_common import ROOT, OUT, CUTOFF, setup, session_sql, register_classes  # noqa: E402

TOP = ['CL', 'XYZ100', 'SILVER', 'BRENTOIL', 'SP500', 'GOLD', 'MU', 'NVDA', 'TSLA', 'SKHX', 'EUR', 'JPY', 'AAPL', 'MSFT']


def sample_days():
    d, end = dt.date(2025, 10, 13), dt.date.fromisoformat(CUTOFF)
    while d < end:
        p = ROOT / f'data/pruned/by_dex/xyz/fills/perp/all/date={d.isoformat()}/fills.parquet'
        if p.exists():
            yield d.isoformat(), str(p)
        d += dt.timedelta(days=3)


def main():
    con = duckdb.connect()
    setup(con)
    ev_path = OUT / 'cost_events.parquet'
    if not ev_path.exists():
        con.execute("""CREATE OR REPLACE TEMP TABLE evt_t (coin VARCHAR, ts TIMESTAMPTZ, side VARCHAR, notional DOUBLE,
                       touched DOUBLE, vwap DOUBLE, n_fills BIGINT, is_twap BOOLEAN, is_liq BOOLEAN)""")
        for day, path in sample_days():
            con.execute(f"""
                INSERT INTO evt_t
                SELECT coin, timestamp, side,
                       sum(CAST(size AS DOUBLE) * CAST(price AS DOUBLE)),
                       CASE WHEN side = 'buy' THEN min(CAST(price AS DOUBLE)) ELSE max(CAST(price AS DOUBLE)) END,
                       sum(CAST(size AS DOUBLE) * CAST(price AS DOUBLE)) / sum(CAST(size AS DOUBLE)),
                       count(*), bool_or(twap_id IS NOT NULL), bool_or(coalesce(is_liquidation, false))
                FROM read_parquet('{path}')
                WHERE crossed AND timestamp < TIMESTAMPTZ '{CUTOFF} 00:00:00+00'
                GROUP BY coin, timestamp, address, side""")
        con.execute(f"COPY evt_t TO '{ev_path}' (FORMAT parquet)")
    con.execute(f"CREATE OR REPLACE TEMP VIEW evt AS SELECT * FROM read_parquet('{ev_path}') WHERE ts < TIMESTAMPTZ '{CUTOFF} 00:00:00+00'")
    coins = [r[0] for r in con.execute('SELECT DISTINCT coin FROM evt').fetchall()]
    register_classes(con, coins)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE e AS
        WITH x AS (
          SELECT evt.*, cc.sym, cc.cls, {session_sql('ts', 'cc.cls')} AS sess,
                 last_value(CASE WHEN side = 'buy' THEN touched END IGNORE NULLS) OVER w AS pb,
                 last_value(CASE WHEN side = 'buy' THEN ts END IGNORE NULLS) OVER w AS tb,
                 last_value(CASE WHEN side = 'sell' THEN touched END IGNORE NULLS) OVER w AS ps,
                 last_value(CASE WHEN side = 'sell' THEN ts END IGNORE NULLS) OVER w AS tsell
          FROM evt JOIN cc USING (coin)
          WINDOW w AS (PARTITION BY coin ORDER BY ts, side ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
        )
        SELECT *,
          CASE WHEN side = 'sell' AND tb IS NOT NULL AND ts - tb <= INTERVAL 1 SECOND THEN (pb - touched) / ((pb + touched) / 2) * 1e4
               WHEN side = 'buy' AND tsell IS NOT NULL AND ts - tsell <= INTERVAL 1 SECOND THEN (touched - ps) / ((touched + ps) / 2) * 1e4
          END AS spread_bp,
          abs(vwap - touched) / touched * 1e4 AS sweep_bp
        FROM x
    """)
    q = """
        SELECT {key}, sess, count(*) AS events, sum(notional) / 1e6 AS notional_m,
               count(spread_bp) AS n_spread,
               quantile_cont(spread_bp, 0.5) AS sp50, quantile_cont(spread_bp, 0.75) AS sp75, quantile_cont(spread_bp, 0.9) AS sp90,
               quantile_cont(sweep_bp, 0.5) AS sw50, quantile_cont(sweep_bp, 0.9) AS sw90, quantile_cont(sweep_bp, 0.99) AS sw99,
               sum(sweep_bp * notional) / sum(notional) AS sweep_notional_wtd,
               quantile_cont(notional, 0.5) AS sz50, quantile_cont(notional, 0.9) AS sz90, quantile_cont(notional, 0.99) AS sz99,
               sum(notional) FILTER (WHERE is_twap) / sum(notional) AS twap_share,
               sum(notional) FILTER (WHERE is_liq) / sum(notional) AS liq_share,
               sum(notional) FILTER (WHERE notional >= 100000) / sum(notional) AS share_ge_100k
        FROM e {where} GROUP BY ALL ORDER BY 1, 2
    """
    hdr = ['key', 'sess', 'events', 'notional_m', 'n_spread', 'spread_p50_bp', 'spread_p75_bp', 'spread_p90_bp',
           'sweep_p50_bp', 'sweep_p90_bp', 'sweep_p99_bp', 'sweep_notional_wtd_bp', 'size_p50', 'size_p90', 'size_p99',
           'twap_share', 'liq_share', 'share_events_ge_100k']
    for name, key, where in (('class', 'cls', ''), ('coin', 'sym', f"WHERE sym IN ({','.join(repr(t) for t in TOP)})")):
        rows = con.execute(q.format(key=key, where=where)).fetchall()
        with open(OUT / f'costs_by_{name}.csv', 'w', newline='') as fh:
            w = csv.writer(fh)
            w.writerow(hdr)
            for r in rows:
                w.writerow([r[0], r[1]] + [round(v, 4) if isinstance(v, float) else v for v in r[2:]])
        print('==', name)
        print(' '.join(hdr))
        for r in rows:
            print(' '.join(str(round(v, 2)) if isinstance(v, float) else str(v) for v in r))


if __name__ == '__main__':
    main()
