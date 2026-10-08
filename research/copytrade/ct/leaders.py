"""Leaders from the archive: fills in execution order, equity and leverage over time.

Point-in-time throughout: equity and leverage are read as-of the latest
snapshot strictly BEFORE the moment asked about (P2 timing gate — 16% of
snapshots are taken hours into their date, so the file date is not the time).
"""
import bisect
import datetime as dt

import duckdb

from .archive_fills import day_file
from .net import DATA

_con = None


def con():
    global _con
    if _con is None:
        tmp = DATA / 'duckdb_tmp'
        tmp.mkdir(parents=True, exist_ok=True)
        _con = duckdb.connect()
        _con.execute(f"SET memory_limit = '9GB'; SET threads = 8; SET temp_directory = '{tmp}'")
    return _con


def _days(start, end):
    return [start + dt.timedelta(days=i) for i in range((end - start).days)]


def load_fills(dex, addresses, start, end):
    """{address: [fill dict, API format]} for [start, end) dates, execution order.

    Ordered by (timestamp, file, row within file): within a millisecond, file
    order is execution order (P3 gate G3); across days the timestamp orders.
    """
    files = [str(day_file(dex, d.isoformat())) for d in _days(start, end) if day_file(dex, d.isoformat()).exists()]
    out = {a: [] for a in addresses}
    if not files or not addresses:
        return out
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE wanted(address VARCHAR)')
    c.executemany('INSERT INTO wanted VALUES (?)', [(a,) for a in addresses])
    rows = c.execute(f"""
        SELECT address, coin, epoch_ms(timestamp) AS t, side, CAST(size AS DOUBLE), CAST(price AS DOUBLE),
               CAST(start_position AS DOUBLE), CAST(realized_pnl AS DOUBLE), trade_id, direction, crossed
        FROM read_parquet({files!r}, filename = true, file_row_number = true)
        WHERE address IN (SELECT address FROM wanted)
        ORDER BY timestamp, filename, file_row_number
    """).fetchall()
    for a, coin, t, side, sz, px, sp, pnl, tid, d, crossed in rows:
        out[a].append({'coin': coin, 'time': t, 'side': 'B' if side == 'buy' else 'A', 'sz': sz, 'px': px,
                       'startPosition': sp, 'closedPnl': pnl, 'tid': tid, 'dir': d, 'crossed': crossed})
    return out


class Equity:
    """As-of account value and gross leverage per address from the snapshot table."""

    def __init__(self, dex, addresses, start_ms, end_ms):
        c = con()
        c.execute('CREATE OR REPLACE TEMP TABLE wanted_eq(address VARCHAR)')
        c.executemany('INSERT INTO wanted_eq VALUES (?)', [(a,) for a in addresses])
        path = DATA / f'derived/{dex}/snapshot_accounts.parquet'
        rows = c.execute(f"""
            SELECT "user", snapshot_ms, account_value, gross_notional
            FROM read_parquet('{path}')
            WHERE "user" IN (SELECT address FROM wanted_eq)
              AND snapshot_ms >= {start_ms} AND snapshot_ms < {end_ms}
            ORDER BY "user", snapshot_ms
        """).fetchall()
        self.series = {}
        for a, t, av, gross in rows:
            self.series.setdefault(a, ([], [], []))
            ts, avs, levs = self.series[a]
            ts.append(t)
            avs.append(av)
            levs.append(gross / av if av else None)

    def at(self, address, t_ms):
        """(account value, snapshot time) as-of the latest snapshot strictly before t, or (None, None)."""
        ts, avs, _ = self.series.get(address, ([], [], []))
        i = bisect.bisect_left(ts, t_ms) - 1
        return (avs[i], ts[i]) if i >= 0 else (None, None)

    def equity_fn(self, address):
        """Callable t -> account value, holding the last known value (for LeaderBook)."""
        def f(t):
            v, _ = self.at(address, t)
            return v
        return f

    def typical_leverage(self, address, start_ms, end_ms):
        """Median gross leverage over snapshots in [start, end)."""
        ts, _, levs = self.series.get(address, ([], [], []))
        vals = sorted(l for t, l in zip(ts, levs) if start_ms <= t < end_ms and l is not None and l > 0)
        return vals[len(vals) // 2] if vals else None
