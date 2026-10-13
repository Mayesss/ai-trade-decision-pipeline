"""Idea B — skilled vs losers positioning (registration 001 §8, revision 4).

signal(c) = tilt_cohort(c) - tilt_crowd(c), where tilt_X(c) is group X's net
signed notional in coin c divided by group X's gross notional across all
coins — "how much of its book this group puts on c, and which way".

The crowd is a NAMED address set (the bottom score quintile; or retail,
reported). Revision 3 used "everyone else" as the crowd, and that carried no
information: in a perp market the signed notional of all accounts sums to
zero per coin (verified on the 2026-03-02 snapshot, 190/190 markets), so
"everyone else" is the cohort's negative scaled by a per-day constant and the
signal's ranking equals the cohort's own tilt. `tilts(..., crowd=None)` keeps
that form so the identity can be demonstrated (tests), never for a result.

The cohort and crowd only exist after registration; before it, `tilts` runs
with arbitrary or synthetic address sets to check the mechanics, and
`long_short` runs on synthetic data.
"""
import statistics

import duckdb


def tilts(snapshot_path, cohort, crowd=None, min_cohort_holders=3):
    """{coin: signal} for one snapshot file, coins with >= min_cohort_holders cohort holders.

    crowd: address set. None means "every account not in the cohort" — the
    degenerate revision-3 form, kept only for the identity test. A crowd with
    no position in a coin contributes a tilt of 0 there.
    """
    con = duckdb.connect()
    con.execute('CREATE TEMP TABLE cohort(address VARCHAR)')
    con.executemany('INSERT INTO cohort VALUES (?)', [(a,) for a in cohort])
    if crowd is None:
        crowd_expr = '"user" NOT IN (SELECT address FROM cohort)'
    else:
        con.execute('CREATE TEMP TABLE crowd(address VARCHAR)')
        con.executemany('INSERT INTO crowd VALUES (?)', [(a,) for a in crowd])
        crowd_expr = '"user" IN (SELECT address FROM crowd)'
    rows = con.execute(f"""
        WITH s AS (
            SELECT market, "user" IN (SELECT address FROM cohort) AS skilled,
                   {crowd_expr} AS crowd,
                   sign(size) * abs(notional) AS signed, abs(notional) AS gross
            FROM read_parquet('{snapshot_path}')
            WHERE size != 0
        ), books AS (
            SELECT SUM(gross) FILTER (WHERE skilled) AS cohort_book,
                   SUM(gross) FILTER (WHERE crowd) AS crowd_book
            FROM s
        )
        SELECT s.market,
               SUM(s.signed) FILTER (WHERE s.skilled) / b.cohort_book AS tilt_cohort,
               COALESCE(SUM(s.signed) FILTER (WHERE s.crowd), 0) / NULLIF(b.crowd_book, 0) AS tilt_crowd,
               COUNT(*) FILTER (WHERE s.skilled) AS cohort_holders
        FROM s, books b
        GROUP BY s.market, b.cohort_book, b.crowd_book
    """).fetchall()
    return {m: tc - (tw or 0.0) for m, tc, tw, n in rows if n >= min_cohort_holders and tc is not None}


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


def breadth(days):
    """Per-day coin count and names per side — reported (registration §8)."""
    per_day = [(len(sig), len(quintiles(sig)[0])) for _, sig in days]
    if not per_day:
        return {'days': 0}
    return {'days': len(per_day), 'coins_median': statistics.median(c for c, _ in per_day),
            'coins_min': min(c for c, _ in per_day), 'per_side_median': statistics.median(k for _, k in per_day),
            'days_under_5_coins': sum(c < 5 for c, _ in per_day)}


def summary(series):
    vals = [v for _, v in series]
    return {'n': len(vals), 'mean': statistics.fmean(vals) if vals else None,
            'median': statistics.median(vals) if vals else None}
