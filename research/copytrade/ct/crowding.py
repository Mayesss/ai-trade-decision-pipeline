"""Crowding features of a leader — registration 001 §4b (revision 4).

Three measures of "is this wallet already being copied / watched":

1. shadower excess (behaviour only): for each leader OPEN (flat -> position,
   or a flip) in coin c at t, the number of distinct other wallets opening c
   in the same direction from flat within [t, t + 60 min), minus the coin's
   expected count from its base rate of such opens per hour over
   [t - 24 h, t + 24 h) excluding the hour containing t. Per leader: the mean
   excess over its opens, and the repeat-shadower share — the share of all
   shadow-opens (wallet x window appearances) that come from wallets seen
   on >= 3 of the leader's opens. Count-weighted, because a share of
   distinct wallets is diluted by one-off random openers when the coin's
   base rate is high (found on synthetic data: 5 persistent copiers among 36
   observed wallets read as 0.14 by wallet, 0.65 by appearance).
2. post-trade path: the coin's venue return from the leader's open to +10 min,
   +1 h, +6 h, +24 h, +72 h, signed by direction, in daily-ATR units. A
   return measure of the leader's trades: computed only after registration.
3. watchability: the leader's trailing 30-day realized-PnL rank among all
   wallets as-of S (the public leaderboard ranks by dollar PnL), and its
   account-value rank. After registration only.

Everything takes a DuckDB connection with an `opens` temp table so the
synthetic tests can feed hand-built rows; `build_opens` fills it from the
archive day files.
"""
import statistics

from .archive_fills import day_file

MINUTE = 60_000
HOUR = 3_600_000
DAY = 86_400_000
WINDOW = 60 * MINUTE
BASE_HOURS = 47            # the 48 hours around t minus the hour containing t


def build_opens(con, dex, days, coins=None, table='opens'):
    """Temp table `opens(address, coin, dir, t)` of every open-from-flat and flip on `days`.

    An open is a fill whose start position is on the other side of zero (or
    at zero) from its end position: start <= 0 < end is a long open, start >=
    0 > end a short open. Later fills of the same order have a non-zero start
    and are not opens, so a position opened in many slices counts once.
    """
    files = [str(day_file(dex, d.isoformat())) for d in days if day_file(dex, d.isoformat()).exists()]
    coin_filter = ''
    if coins is not None:
        con.execute('CREATE OR REPLACE TEMP TABLE open_coins(coin VARCHAR)')
        con.executemany('INSERT INTO open_coins VALUES (?)', [(c,) for c in coins])
        coin_filter = 'AND coin IN (SELECT coin FROM open_coins)'
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {table} AS
        WITH f AS (
            SELECT address, coin, epoch_ms(timestamp) AS t,
                   CAST(start_position AS DOUBLE) AS sp,
                   CAST(start_position AS DOUBLE) + CASE side WHEN 'buy' THEN CAST(size AS DOUBLE)
                                                              ELSE -CAST(size AS DOUBLE) END AS ep
            FROM read_parquet({files!r})
            WHERE 1 = 1 {coin_filter}
        )
        SELECT address, coin, CASE WHEN ep > 0 THEN 'L' ELSE 'S' END AS dir, t
        FROM f WHERE (sp <= 0 AND ep > 0) OR (sp >= 0 AND ep < 0)
    """)


def shadowers(con, leader_opens, table='opens', window_ms=WINDOW):
    """{leader: {'n_opens', 'excess_mean', 'repeat_share', 'shadowers'}}.

    leader_opens: [(leader address, coin, dir, t)]. Counts come from `table`
    (see build_opens); the leader's own rows are excluded.
    """
    con.execute('CREATE OR REPLACE TEMP TABLE lo(leader VARCHAR, coin VARCHAR, dir VARCHAR, t BIGINT, k INTEGER)')
    con.executemany('INSERT INTO lo VALUES (?, ?, ?, ?, ?)',
                    [(a, c, d, int(t), i) for i, (a, c, d, t) in enumerate(leader_opens)])
    # observed: distinct other wallets in [t, t + window)
    obs = con.execute(f"""
        SELECT lo.k, COUNT(DISTINCT o.address) AS n, LIST(DISTINCT o.address) AS who
        FROM lo LEFT JOIN {table} o
          ON o.coin = lo.coin AND o.dir = lo.dir AND o.address != lo.leader
         AND o.t >= lo.t AND o.t < lo.t + {window_ms}
        GROUP BY lo.k
    """).fetchall()
    # expected: distinct other wallets per clock hour over the 47 surrounding hours
    base = con.execute(f"""
        WITH hours AS (
            SELECT lo.k, lo.leader, lo.coin, lo.dir, (lo.t // {HOUR}) * {HOUR} + h * {HOUR} AS hour_start
            FROM lo, range(-24, 25) r(h) WHERE h != 0
        )
        SELECT hours.k, SUM(c.n) AS total
        FROM hours LEFT JOIN (
            SELECT coin, dir, (t // {HOUR}) * {HOUR} AS hour_start, address, COUNT(*) AS n0
            FROM {table} GROUP BY ALL
        ) a ON a.coin = hours.coin AND a.dir = hours.dir AND a.hour_start = hours.hour_start
             AND a.address != hours.leader
        LEFT JOIN LATERAL (SELECT 1 AS n) c ON a.address IS NOT NULL
        GROUP BY hours.k
    """).fetchall()
    expected = {k: (total or 0) / BASE_HOURS for k, total in base}
    per_leader = {}
    for k, n, who in obs:
        leader = leader_opens[k][0]
        d = per_leader.setdefault(leader, {'excess': [], 'seen': {}})
        d['excess'].append(n - expected.get(k, 0.0))
        for w in (who or []):
            if w is not None:
                d['seen'][w] = d['seen'].get(w, 0) + 1
    out = {}
    for leader, d in per_leader.items():
        seen = d['seen']
        appearances = sum(seen.values())
        repeat = sum(c for c in seen.values() if c >= 3)
        out[leader] = {'n_opens': len(d['excess']), 'excess_mean': statistics.fmean(d['excess']),
                       'repeat_share': repeat / appearances if appearances else 0.0,
                       'shadowers': len(seen), 'repeat_shadowers': sum(1 for c in seen.values() if c >= 3)}
    return out


def leader_opens_from_fills(address, fills, symtab):
    """[(address, coin, dir, t)] of a leader's opens and flips, mapped coins only. Behaviour only."""
    out = []
    for f in fills:
        m = symtab.get(f['coin'])
        if m is None or m['status'] != 'ok':
            continue
        sp = float(f['startPosition'])
        ep = sp + (float(f['sz']) if f['side'] == 'B' else -float(f['sz']))
        if (sp <= 0 < ep) or (sp >= 0 > ep):
            out.append((address, f['coin'], 'L' if ep > 0 else 'S', f['time']))
    return out


HORIZONS = {'10m': 10 * MINUTE, '1h': HOUR, '6h': 6 * HOUR, '24h': 24 * HOUR, '72h': 72 * HOUR}


def post_trade_path(opens, venue, symtab):
    """{horizon: mean signed return in ATR units} over a leader's opens. After registration only.

    Entry = the open of the venue minute after the leader's open; exits at
    +10 min (minute candle) and +1 h / +6 h / +24 h / +72 h (1H closes).
    Opens with no venue price are skipped per horizon.
    """
    acc = {h: [] for h in HORIZONS}
    for _, coin, d, t in opens:
        sym = symtab[coin]['symbol']
        sign = 1.0 if d == 'L' else -1.0
        entry = venue.candle_minute(sym, t - t % MINUTE + MINUTE)
        atr = venue.atr_pct(sym, t)
        if not entry or not atr:
            continue
        for name, h in HORIZONS.items():
            if name == '10m':
                c = venue.candle_minute(sym, t - t % MINUTE + MINUTE + h)
            else:
                c = venue.candle_hour(sym, (t + h) - (t + h) % HOUR - HOUR)
            if c:
                acc[name].append(sign * (c[4] / entry[1] - 1) / atr)
    return {h: (statistics.fmean(v) if v else None) for h, v in acc.items()}, {h: len(v) for h, v in acc.items()}


def watchability(con, dex, days, addresses, equity_rows=None):
    """{address: {'pnl_rank', 'pnl_30d'}} — trailing realized-PnL percentile rank among ALL wallets
    on `days` (the 30 days before S). After registration only: reads realized_pnl.

    equity_rows: optional [(address, account_value)] for every wallet with a
    snapshot before S; adds 'value_rank' when given.
    """
    files = [str(day_file(dex, d.isoformat())) for d in days if day_file(dex, d.isoformat()).exists()]
    con.execute('CREATE OR REPLACE TEMP TABLE want(address VARCHAR)')
    con.executemany('INSERT INTO want VALUES (?)', [(a,) for a in addresses])
    rows = con.execute(f"""
        WITH p AS (
            SELECT address, SUM(CAST(realized_pnl AS DOUBLE)) AS pnl
            FROM read_parquet({files!r}) GROUP BY address
        ), r AS (
            SELECT address, pnl, PERCENT_RANK() OVER (ORDER BY pnl) AS rk FROM p
        )
        SELECT address, pnl, rk FROM r WHERE address IN (SELECT address FROM want)
    """).fetchall()
    out = {a: {'pnl_30d': pnl, 'pnl_rank': rk} for a, pnl, rk in rows}
    if equity_rows:
        ranked = sorted(v for _, v in equity_rows if v is not None)
        import bisect
        values = dict(equity_rows)
        for a in addresses:
            v = values.get(a)
            if a in out and v is not None and ranked:
                out[a]['value_rank'] = bisect.bisect_left(ranked, v) / len(ranked)
    return out
