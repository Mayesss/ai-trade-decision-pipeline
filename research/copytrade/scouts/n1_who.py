"""N1 scout, item 4: who trades off-hours (counts and volume shares only).

Accounts are classed ONLY from the existing 001-style xyz features at each
selection date S (lookback [S - 91 d, S), file data/derived/xyz/features_S.parquet).
There is no skill score in those files and none is computed here (a skill
score needs round-trip P&L, which this scout must not compute). Classes, in
priority order, all past-only:
  mm_like      maker_share >= 0.8 and n_fills >= 10,000 in the lookback
  liquidated   >= 1 liquidation in the lookback (the loser cohort proxy)
  large        account value >= $100k at S
  retail       $1k <= account value < $100k
  micro        account value < $1k
  no_value     in features, no account value at S
  new          no xyz fill in the lookback
Volume is then measured in the following hold window [S, S + 56 d) by session.
Each fill row is one account's side, so class shares are of two-sided notional;
taker shares use crossed rows only. Nothing is split by any later quantity.

    .venv/bin/python -I scouts/n1_who.py
"""
import csv
import datetime as dt
import sys

import duckdb

sys.path.insert(0, __import__('os').path.dirname(__file__))
from n1_common import ROOT, OUT, CUTOFF, setup, session_sql, register_classes  # noqa: E402

SELECTIONS = ['2026-01-12', '2026-03-09', '2026-05-04']
HOLD = 56


def main():
    con = duckdb.connect()
    setup(con)
    con.execute("""CREATE OR REPLACE TEMP TABLE w (sel VARCHAR, acls VARCHAR, coin VARCHAR, m TIMESTAMPTZ,
                   notional DOUBLE, taker_notional DOUBLE, liq_notional DOUBLE)""")
    for sel in SELECTIONS:
        feat = ROOT / f'data/derived/xyz/features_{sel}.parquet'
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE ac AS
            SELECT address, CASE
                WHEN maker_share >= 0.8 AND n_fills >= 10000 THEN 'mm_like'
                WHEN n_liquidations >= 1 THEN 'liquidated'
                WHEN account_value >= 100000 THEN 'large'
                WHEN account_value >= 1000 THEN 'retail'
                WHEN account_value IS NOT NULL THEN 'micro'
                ELSE 'no_value' END AS acls
            FROM read_parquet('{feat}')""")
        print(sel, con.execute('SELECT acls, count(*) FROM ac GROUP BY 1 ORDER BY 1').fetchall())
        s = dt.date.fromisoformat(sel)
        for i in range(HOLD):
            d = (s + dt.timedelta(days=i)).isoformat()
            if d >= CUTOFF:
                break
            p = ROOT / f'data/pruned/by_dex/xyz/fills/perp/all/date={d}/fills.parquet'
            if not p.exists():
                continue
            con.execute(f"""
                INSERT INTO w
                SELECT '{sel}', coalesce(ac.acls, 'new'), coin, time_bucket(INTERVAL 1 MINUTE, timestamp),
                       sum(CAST(size AS DOUBLE) * CAST(price AS DOUBLE)),
                       sum(CAST(size AS DOUBLE) * CAST(price AS DOUBLE)) FILTER (WHERE crossed),
                       sum(CAST(size AS DOUBLE) * CAST(price AS DOUBLE)) FILTER (WHERE is_liquidation)
                FROM read_parquet('{p}') f LEFT JOIN ac USING (address)
                WHERE timestamp < TIMESTAMPTZ '{CUTOFF} 00:00:00+00'
                GROUP BY ALL""")
    coins = [r[0] for r in con.execute('SELECT DISTINCT coin FROM w').fetchall()]
    register_classes(con, coins)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE ws AS
        SELECT w.*, cc.cls, CASE WHEN cc.cls IN ('commodity', 'fx') THEN 'cmdty_fx' ELSE 'equity' END AS grp,
               CASE WHEN {session_sql('m', 'cc.cls')} = 'REG' THEN 'REG'
                    WHEN {session_sql('m', 'cc.cls')} IN ('WKD', 'HOL') THEN 'WKD_HOL' ELSE 'XH_BRK' END AS sess
        FROM w JOIN cc USING (coin) WHERE cc.cls <> 'ipop'""")
    rows = con.execute("""
        SELECT grp, sess, acls, sum(notional) AS n, sum(taker_notional) AS t, sum(coalesce(liq_notional, 0)) AS l
        FROM ws GROUP BY ALL ORDER BY 1, 2, 3""").fetchall()
    tot = {}
    for g, s_, a, n, t, l in rows:
        k = tot.setdefault((g, s_), [0.0, 0.0])
        k[0] += n
        k[1] += t or 0
    with open(OUT / 'who_by_session.csv', 'w', newline='') as fh:
        wr = csv.writer(fh)
        wr.writerow(['group', 'session', 'account_class', 'share_of_notional', 'share_of_taker_notional',
                     'class_taker_ratio', 'class_liq_share', 'notional_musd'])
        for g, s_, a, n, t, l in rows:
            T = tot[(g, s_)]
            r = [g, s_, a, round(n / T[0], 4), round((t or 0) / T[1], 4), round((t or 0) / n, 3), round(l / n, 4), round(n / 1e6, 1)]
            wr.writerow(r)
            print(*r)


if __name__ == '__main__':
    main()
