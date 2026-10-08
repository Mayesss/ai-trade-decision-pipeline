"""Idea B — skilled vs crowd positioning (registration 001 §8).

signal(c) = tilt_cohort(c) - tilt_crowd(c), where tilt_X(c) is group X's net
signed notional in coin c divided by group X's gross notional across all
coins — "how much of its book this group puts on c, and which way".

The cohort (top-quintile slow wallets by cross-regime score) only exists after
registration; before it, `tilts` runs with arbitrary address sets to check
the mechanics, and `long_short` runs on synthetic data.
"""
import statistics

import duckdb

from .net import DATA


def tilts(snapshot_path, cohort, min_cohort_holders=3):
    """{coin: signal} for one snapshot file, coins with >= min_cohort_holders cohort holders."""
    con = duckdb.connect()
    con.execute('CREATE TEMP TABLE cohort(address VARCHAR)')
    con.executemany('INSERT INTO cohort VALUES (?)', [(a,) for a in cohort])
    rows = con.execute(f"""
        WITH s AS (
            SELECT market, "user" IN (SELECT address FROM cohort) AS skilled,
                   sign(size) * abs(notional) AS signed, abs(notional) AS gross
            FROM read_parquet('{snapshot_path}')
            WHERE size != 0
        ), totals AS (
            SELECT skilled, SUM(gross) AS book FROM s GROUP BY skilled
        )
        SELECT s.market,
               SUM(s.signed) FILTER (WHERE s.skilled) / MAX(tc.book) AS tilt_cohort,
               COALESCE(SUM(s.signed) FILTER (WHERE NOT s.skilled), 0) / MAX(tw.book) AS tilt_crowd,
               COUNT(*) FILTER (WHERE s.skilled) AS cohort_holders
        FROM s, (SELECT book FROM totals WHERE skilled) tc, (SELECT book FROM totals WHERE NOT skilled) tw
        GROUP BY s.market
    """).fetchall()
    return {m: tc - tw for m, tc, tw, n in rows if n >= min_cohort_holders and tc is not None}


def quintiles(signal):
    """(long symbols, short symbols): top and bottom fifth by signal (>= 5 coins needed)."""
    ranked = sorted(signal, key=signal.get)
    k = len(ranked) // 5
    return (ranked[-k:], ranked[:k]) if k else ([], [])


def long_short(days, ret, vol, cost_rate, horizon=3):
    """Daily net returns of a costed, equal-risk long/short book.

    days:   [(day key, {symbol: signal})] in time order (one signal per day)
    ret:    (symbol, day key) -> that coin's return over the day AFTER entry
            day k covers entry-time k+1 .. k+2 (no look-ahead in the caller)
    vol:    (symbol, day key) -> trailing volatility known at entry
    cost_rate: one-way cost as a fraction of notional (fee + slippage)

    Each day a new tranche opens (top quintile long, bottom short, weights
    proportional to 1 / vol, gross 1) and lives `horizon` days; the book is
    the average of the live tranches, so 1/horizon of it turns over daily.
    Entry and exit each cost cost_rate on the tranche's gross.
    """
    tranches, out = [], []
    for key, signal in days:
        longs, shorts = quintiles(signal)
        w = {}
        for side, syms in ((1, longs), (-1, shorts)):
            inv = {s: 1 / vol(s, key) for s in syms if vol(s, key)}
            total = sum(inv.values())
            for s, v in inv.items():
                w[s] = side * 0.5 * v / total
        cost_today = 0.0
        if w:
            tranches.append({'w': w, 'age': 0})
            cost_today += cost_rate * sum(abs(x) for x in w.values())
        live = [t for t in tranches if t['age'] < horizon]
        gross_ret = 0.0
        for t in live:
            gross_ret += sum(x * (ret(s, key) or 0.0) for s, x in t['w'].items())
            t['age'] += 1
            if t['age'] == horizon:
                cost_today += cost_rate * sum(abs(x) for x in t['w'].values())
        tranches = [t for t in tranches if t['age'] < horizon]
        out.append((key, (gross_ret - cost_today) / horizon))
    return out


def summary(series):
    vals = [v for _, v in series]
    return {'n': len(vals), 'mean': statistics.fmean(vals) if vals else None,
            'median': statistics.median(vals) if vals else None}
