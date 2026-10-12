"""N1 scout, item 2: universe and activity by session (behaviour only).

Per coin: first fill, notional and trade counts by session, liquidation share,
and counts of distinct off-hours events with any trade. Calendar hours per
session after listing normalise volume to $/hour. No price is compared with
any other price here.

    .venv/bin/python -I scouts/n1_activity.py
Writes data/scout_n1/derived/minute_agg.parquet, activity_by_coin.csv, activity_by_class.csv.
"""
import csv
import re
import sys

import duckdb

sys.path.insert(0, __import__('os').path.dirname(__file__))
from n1_common import OUT, CUTOFF, FILLS_GLOB, setup, fills_view, session_sql, register_classes  # noqa: E402

SESS = ['REG', 'XH', 'BRK', 'WKD', 'HOL']


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    setup(con)
    fills_view(con)
    agg = OUT / 'minute_agg.parquet'
    if not agg.exists():
        import glob
        days = sorted(d for d in glob.glob(FILLS_GLOB) if re.search(r'date=(\d{4}-\d{2}-\d{2})', d).group(1) < CUTOFF)
        con.execute("""CREATE OR REPLACE TEMP TABLE agg_t (coin VARCHAR, m TIMESTAMPTZ, notional_rows DOUBLE,
                       n_rows BIGINT, n_taker_rows BIGINT, taker_notional DOUBLE, liq_notional DOUBLE, n_trades BIGINT)""")
        for path in days:
            con.execute(f"""
              INSERT INTO agg_t
              SELECT coin, time_bucket(INTERVAL 1 MINUTE, timestamp) AS m,
                     sum(CAST(size AS DOUBLE) * CAST(price AS DOUBLE)),
                     count(*), count(*) FILTER (WHERE crossed),
                     sum(CAST(size AS DOUBLE) * CAST(price AS DOUBLE)) FILTER (WHERE crossed),
                     sum(CAST(size AS DOUBLE) * CAST(price AS DOUBLE)) FILTER (WHERE is_liquidation),
                     count(DISTINCT trade_id)
              FROM read_parquet('{path}') WHERE timestamp < TIMESTAMPTZ '{CUTOFF} 00:00:00+00'
              GROUP BY ALL""")
        con.execute(f"COPY (SELECT * FROM agg_t) TO '{agg}' (FORMAT parquet)")
    con.execute(f"CREATE OR REPLACE TEMP VIEW a AS SELECT * FROM read_parquet('{agg}') WHERE m < TIMESTAMPTZ '{CUTOFF} 00:00:00+00'")
    coins = [r[0] for r in con.execute('SELECT DISTINCT coin FROM a').fetchall()]
    register_classes(con, coins)
    print('rows check: sum n_rows / 2 vs n_trades vs taker rows:',
          con.execute('SELECT sum(n_rows) / 2, sum(n_trades), sum(n_taker_rows) FROM a').fetchone())
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE s AS
        SELECT a.*, cc.sym, cc.cls, {session_sql('m', 'cc.cls')} AS sess,
               CASE WHEN cc.cls IN ('korea', 'japan') THEN 9 ELSE 8 END AS off_h,
               CASE cc.cls WHEN 'korea' THEN timezone('Asia/Seoul', m) WHEN 'japan' THEN timezone('Asia/Tokyo', m)
                           WHEN 'hk' THEN timezone('Asia/Hong_Kong', m) ELSE timezone('America/New_York', m) END AS loc
        FROM a JOIN cc USING (coin)
    """)
    # calendar minutes per session after listing (exposure)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE grid AS
        SELECT generate_series AS m FROM generate_series(TIMESTAMPTZ '2025-10-13 00:00:00+00',
               TIMESTAMPTZ '{CUTOFF} 00:00:00+00' - INTERVAL 1 MINUTE, INTERVAL 1 MINUTE)
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE first_m AS SELECT coin, min(m) AS m0 FROM s GROUP BY coin
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE cls_grid AS
        SELECT c.cls, g.m, {session_sql('g.m', 'c.cls')} AS sess
        FROM grid g CROSS JOIN (SELECT DISTINCT cls FROM cc) c
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE expo AS
        SELECT f.coin, g.sess, count(*) / 60.0 AS hours
        FROM first_m f JOIN cc USING (coin) JOIN cls_grid g ON g.cls = cc.cls AND g.m >= f.m0
        GROUP BY ALL
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE ev AS
        SELECT coin, sess,
               count(DISTINCT CASE WHEN sess = 'WKD' THEN date_trunc('week', loc + INTERVAL 3 DAY)
                                   ELSE CAST(loc + to_hours(off_h) AS DATE) END) AS n_events
        FROM s WHERE sess <> 'REG' GROUP BY ALL
    """)
    rows = con.execute("""
        SELECT s.coin, any_value(sym), any_value(cls), CAST(min(m) AS DATE) AS first_day, sess,
               sum(notional_rows) / 2 AS notional, sum(n_trades) AS trades,
               sum(taker_notional) AS taker_notional, coalesce(sum(liq_notional), 0) AS liq_notional,
               any_value(e.hours) AS hours, any_value(v.n_events) AS n_events
        FROM s LEFT JOIN expo e USING (coin, sess) LEFT JOIN ev v USING (coin, sess)
        GROUP BY s.coin, sess ORDER BY s.coin, sess
    """).fetchall()
    # coins' first day
    first = {r[0]: r[1] for r in con.execute('SELECT coin, CAST(m0 AS DATE) FROM first_m').fetchall()}
    by = {}
    for coin, sym, cls, _, sess, notl, trades, tk, liq, hours, nev in rows:
        d = by.setdefault(coin, {'sym': sym, 'cls': cls, 'first': first[coin]})
        d[sess] = (notl, trades, tk, liq, hours, nev)
    # exposure hours for sessions with no trades
    for coin, sess, hours in con.execute('SELECT coin, sess, hours FROM expo').fetchall():
        d = by[coin]
        if sess not in d:
            d[sess] = (0.0, 0, 0.0, 0.0, hours, 0)
    wk_total = {r[0]: r[1] for r in con.execute(f"""
        SELECT coin, count(DISTINCT date_trunc('week', timezone('America/New_York', g.m) + INTERVAL 3 DAY))
        FROM first_m f JOIN cc USING (coin) JOIN cls_grid g ON g.cls = cc.cls AND g.m >= f.m0 AND g.sess = 'WKD'
        GROUP BY coin""").fetchall()}

    with open(OUT / 'activity_by_coin.csv', 'w', newline='') as fh:
        w = csv.writer(fh)
        hdr = ['coin', 'class', 'first_day', 'total_notional_musd', 'total_trades', 'weekends_in_calendar']
        for s_ in SESS:
            hdr += [f'{s_}_share_notional', f'{s_}_trades', f'{s_}_usd_per_hour', f'{s_}_taker_liq_share', f'{s_}_events']
        w.writerow(hdr)
        out = []
        for coin, d in by.items():
            tot = sum(v[0] for k, v in d.items() if k in SESS)
            trd = sum(v[1] for k, v in d.items() if k in SESS)
            row = [d['sym'], d['cls'], d['first'], round(tot / 1e6, 2), trd, wk_total.get(coin, 0)]
            for s_ in SESS:
                v = d.get(s_)
                if v is None:
                    row += ['', '', '', '', '']
                    continue
                notl, trades, tk, liq, hours, nev = v
                row += [round(notl / tot, 4) if tot else '', trades,
                        round(notl / hours) if hours else '', round(liq / notl, 4) if notl else '', nev or 0]
            out.append((tot, row))
        for _, row in sorted(out, key=lambda x: -x[0]):
            w.writerow(row)

    cls_rows = con.execute("""
        SELECT cls, sess, sum(notional_rows) / 2 / 1e6, sum(n_trades), sum(coalesce(liq_notional, 0)) / (sum(notional_rows) / 2),
               sum(taker_notional) / (sum(notional_rows) / 2)
        FROM s GROUP BY ALL ORDER BY cls, sess
    """).fetchall()
    with open(OUT / 'activity_by_class.csv', 'w', newline='') as fh:
        w = csv.writer(fh)
        w.writerow(['class', 'session', 'notional_musd', 'trades', 'liq_share_of_notional', 'taker_rows_notional_over_notional'])
        for r in cls_rows:
            w.writerow([r[0], r[1], round(r[2], 1), r[3], round(r[4], 5), round(r[5], 4)])
            print(r[0], r[1], round(r[2], 1), r[3], round(r[4], 5), round(r[5], 4))


if __name__ == '__main__':
    main()
