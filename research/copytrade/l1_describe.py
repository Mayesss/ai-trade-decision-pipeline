"""Liquidation workstream — distributions for registration 002 §3. Behaviour only: no forward
return, no Bitget price. Candidate run gaps G are compared by the runs they produce.

    .venv/bin/python l1_describe.py > data/derived/l1_describe.txt
"""
from ct.leaders import con
from ct.net import DATA

DEX = 'hyperliquid'
LIQ = DATA / f'derived/{DEX}/liquidations.parquet'
OI = DATA / f'derived/{DEX}/oi_by_coin_snapshot.parquet'
GAPS_S = [10, 30, 60, 120, 300]


def q(sql):
    return con().execute(sql).fetchall()


def main():
    c = con()
    print('== liquidation fills (discovery)')
    for r in q(f"SELECT COUNT(*), COUNT(DISTINCT coin), COUNT(DISTINCT address), ROUND(SUM(notional)/1e9,2) FROM read_parquet('{LIQ}')"):
        print(f'   fills {r[0]:,}  coins {r[1]}  wallets {r[2]:,}  notional ${r[3]}B')
    print('   by direction:', q(f"SELECT direction, COUNT(*), ROUND(SUM(notional)/1e9,2) FROM read_parquet('{LIQ}') GROUP BY 1"))
    print('\n== notional share by coin (top 12)')
    for r in q(f"SELECT coin, ROUND(SUM(notional)/1e6,1) m, COUNT(*) n FROM read_parquet('{LIQ}') GROUP BY coin ORDER BY m DESC LIMIT 12"):
        print(f'   {r[0]:<8} ${r[1]:>8}M  {r[2]:>9,} fills')
    print('\n== daily liquidation notional per coin ($): percentiles over coin-days with any liquidation')
    c.execute(f"""CREATE OR REPLACE TEMP TABLE daily AS
        SELECT coin, CAST(to_timestamp(t / 1000) AS DATE) AS day, SUM(notional) AS notional, COUNT(*) AS n
        FROM read_parquet('{LIQ}') GROUP BY 1, 2""")
    for r in q("SELECT quantile_cont(notional, [0.1, 0.25, 0.5, 0.75, 0.9, 0.99]) FROM daily"):
        print('   ', [f'{x:,.0f}' for x in r[0]])
    for coin in ('BTC', 'ETH', 'SOL', 'HYPE', 'XRP'):
        for r in q(f"SELECT quantile_cont(notional, [0.1, 0.5, 0.9]), COUNT(*) FROM daily WHERE coin = '{coin}'"):
            print(f'   {coin:<5} p10/p50/p90 daily $', [f'{x:,.0f}' for x in r[0]], f'({r[1]} days)')
    print('\n== inter-fill gaps within a coin (seconds): percentiles')
    c.execute(f"""CREATE OR REPLACE TEMP TABLE gaps AS
        SELECT coin, t, direction, notional,
               (t - LAG(t) OVER (PARTITION BY coin ORDER BY t)) / 1000.0 AS gap_s
        FROM read_parquet('{LIQ}')""")
    for r in q("SELECT quantile_cont(gap_s, [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]) FROM gaps WHERE gap_s IS NOT NULL"):
        print('   all coins', [f'{x:,.1f}' for x in r[0]])
    for r in q("SELECT quantile_cont(gap_s, [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]) FROM gaps WHERE coin = 'BTC' AND gap_s IS NOT NULL"):
        print('   BTC      ', [f'{x:,.1f}' for x in r[0]])
    print('\n== runs at candidate gaps G (a run = consecutive fills <= G apart, same coin)')
    print(f'   {"G s":>5} {"runs":>9} {"fills/run p50":>13} {"p90":>6} {"notional p50":>13} {"p90":>13} {"p99":>13} '
          f'{"purity p50":>10} {"share pure>=.8":>14} {"dur s p50":>10} {"p90":>6}')
    for g in GAPS_S:
        c.execute(f"""CREATE OR REPLACE TEMP TABLE runs_{g} AS
            WITH marked AS (
                SELECT *, CASE WHEN gap_s IS NULL OR gap_s > {g} THEN 1 ELSE 0 END AS new_run FROM gaps
            ), numbered AS (
                SELECT *, SUM(new_run) OVER (PARTITION BY coin ORDER BY t ROWS UNBOUNDED PRECEDING) AS run_id FROM marked
            )
            SELECT coin, run_id, MIN(t) AS t_start, MAX(t) AS t_end, COUNT(*) AS n, SUM(notional) AS notional,
                   SUM(notional) FILTER (WHERE direction = 'Close Long') AS long_liq,
                   GREATEST(SUM(notional) FILTER (WHERE direction = 'Close Long'),
                            SUM(notional) FILTER (WHERE direction = 'Close Short')) / SUM(notional) AS purity
            FROM numbered GROUP BY coin, run_id""")
        r = q(f"""SELECT COUNT(*), quantile_cont(n, 0.5), quantile_cont(n, 0.9),
                        quantile_cont(notional, 0.5), quantile_cont(notional, 0.9), quantile_cont(notional, 0.99),
                        quantile_cont(purity, 0.5), AVG(CASE WHEN purity >= 0.8 THEN 1 ELSE 0 END),
                        quantile_cont((t_end - t_start)/1000.0, 0.5), quantile_cont((t_end - t_start)/1000.0, 0.9)
                 FROM runs_{g}""")[0]
        print(f'   {g:>5} {r[0]:>9,} {r[1]:>13.0f} {r[2]:>6.0f} {r[3]:>13,.0f} {r[4]:>13,.0f} {r[5]:>13,.0f} '
              f'{r[6]:>10.2f} {r[7]:>14.2f} {r[8]:>10.0f} {r[9]:>6.0f}')
    print('\n== runs vs the coin\'s trailing 30-day median DAILY liquidation notional (G = 60 s)')
    c.execute("""CREATE OR REPLACE TEMP TABLE med AS
        SELECT coin, day, MEDIAN(notional) OVER (PARTITION BY coin ORDER BY day
               RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND INTERVAL 1 DAY PRECEDING) AS med30 FROM daily""")
    c.execute("""CREATE OR REPLACE TEMP TABLE runs_rel AS
        SELECT r.*, m.med30, r.notional / m.med30 AS rel
        FROM runs_60 r LEFT JOIN med m ON m.coin = r.coin AND m.day = CAST(to_timestamp(r.t_start / 1000) AS DATE)""")
    print('   runs with a trailing median:', q("SELECT COUNT(*) FILTER (WHERE med30 IS NOT NULL), COUNT(*) FROM runs_rel")[0])
    for r in q("SELECT quantile_cont(rel, [0.5, 0.9, 0.99, 0.999]) FROM runs_rel WHERE med30 IS NOT NULL"):
        print('   run notional / trailing median daily, p50/p90/p99/p99.9:', [f'{x:.3f}' for x in r[0]])
    print(f'   {"K":>6} {"F $":>10} {"runs":>7} {"coins":>6} {"per day":>8} {"purity>=.8":>10} {"majors share":>12} {"notional p50":>13}')
    for k in (0.1, 0.25, 0.5, 1.0):
        for f_ in (50_000, 250_000, 1_000_000):
            r = q(f"""SELECT COUNT(*), COUNT(DISTINCT coin), COUNT(*) / 338.0,
                             AVG(CASE WHEN purity >= 0.8 THEN 1 ELSE 0 END),
                             AVG(CASE WHEN coin IN ('BTC','ETH','SOL') THEN 1 ELSE 0 END),
                             quantile_cont(notional, 0.5)
                      FROM runs_rel WHERE med30 IS NOT NULL AND rel >= {k} AND notional >= {f_}""")[0]
            print(f'   {k:>6} {f_:>10,} {r[0]:>7,} {r[1]:>6} {r[2]:>8.1f} {r[3]:>10.2f} {r[4]:>12.2f} {r[5]:>13,.0f}')
    print('\n== open-interest proxy (gross notional per coin per snapshot, $): BTC/ETH/SOL p50')
    for r in q(f"SELECT coin, quantile_cont(gross_notional, 0.5), quantile_cont(positions, 0.5), AVG(with_liq_price * 1.0 / positions) FROM read_parquet('{OI}') WHERE coin IN ('BTC','ETH','SOL','HYPE') GROUP BY coin"):
        print(f'   {r[0]:<5} gross ${r[1]:,.0f}  positions {r[2]:,.0f}  share with liquidation price {r[3]:.2f}')


if __name__ == '__main__':
    main()
