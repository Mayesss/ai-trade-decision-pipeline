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
        # 2 GB, not 9: with the Python heap and the price-block cache a 9 GB pool pushed the
        # 17 GB machine into swap thrash on 2026-10-09 (RSS fell to 264 MB of an 8.7 GB footprint).
        # DuckDB spills to temp_directory instead.
        _con.execute(f"SET memory_limit = '2GB'; SET threads = 4; SET temp_directory = '{tmp}'")
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


def iter_fills(dex, addresses, start, end, batch=50_000):
    """Yield (address, [fill dict, API format]) one wallet at a time, execution order within a wallet.

    Same rows as load_fills, streamed ordered by address so that only one
    wallet's fills are in Python memory at once. Registration 001 amendment
    2: holding all fills of a window's ~4,500 wallets as dicts pushed the run
    into 15 GB of swap. Wallets with no fills are not yielded.
    """
    files = [str(day_file(dex, d.isoformat())) for d in _days(start, end) if day_file(dex, d.isoformat()).exists()]
    if not files or not addresses:
        return
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE wanted_stream(address VARCHAR)')
    c.executemany('INSERT INTO wanted_stream VALUES (?)', [(a,) for a in addresses])
    cur = c.execute(f"""
        SELECT address, coin, epoch_ms(timestamp) AS t, side, CAST(size AS DOUBLE), CAST(price AS DOUBLE),
               CAST(start_position AS DOUBLE), CAST(realized_pnl AS DOUBLE), trade_id, direction, crossed
        FROM read_parquet({files!r}, filename = true, file_row_number = true)
        WHERE address IN (SELECT address FROM wanted_stream)
        ORDER BY address, timestamp, filename, file_row_number
    """)
    current, fills = None, []
    # Arrow record batches stream from DuckDB without materialising the whole sorted result in
    # memory (fetchmany materialises first).
    reader = cur.fetch_record_batch(batch)
    for rb in reader:
        cols = [rb.column(i).to_pylist() for i in range(rb.num_columns)]
        for a, coin, t, side, sz, px, sp, pnl, tid, d, crossed in zip(*cols):
            if a != current:
                if current is not None:
                    yield current, fills
                current, fills = a, []
            fills.append({'coin': coin, 'time': t, 'side': 'B' if side == 'buy' else 'A', 'sz': sz, 'px': px,
                          'startPosition': sp, 'closedPnl': pnl, 'tid': tid, 'dir': d, 'crossed': crossed})
    if current is not None:
        yield current, fills


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


def average_beta(fills, equity_at, venue, symtab, start_ms, end_ms):
    """The leader's lookback average net beta to BTC — registration 001 §7 (static hedge).

    Time-average over UTC day marks in [start, end) of
        sum_c position_c x price_c x beta_c / equity,
    positions from the fill path (a coin's position before its first fill in
    the window is that fill's startPosition, so holdings at the window start
    are known for every coin that trades in it; coins with no fill in the
    window are unknown and ignored — stated limitation), prices the Bitget
    1H close before the mark, beta the venue's point-in-time 30-day hourly
    beta (BTC = 1; no beta -> 1), equity as-of the mark. Flat days count 0.
    Days without a known equity are skipped. Returns (mean, days used) —
    (None, 0) if no day had an equity.

    Behaviour and exposure only: no cash flow, no PnL.
    """
    import math
    fills = sorted((f for f in fills if f['coin'] in symtab and symtab[f['coin']]['status'] == 'ok'),
                   key=lambda f: f['time'])
    pos = {}
    for f in fills:                      # holdings at the window start
        pos.setdefault(f['coin'], float(f['startPosition']))
    i, vals = 0, []
    first_mark = start_ms - start_ms % 86_400_000
    if first_mark < start_ms:
        first_mark += 86_400_000
    for mark in range(first_mark, end_ms, 86_400_000):
        while i < len(fills) and fills[i]['time'] < mark:
            f = fills[i]
            pos[f['coin']] = float(f['startPosition']) + (float(f['sz']) if f['side'] == 'B' else -float(f['sz']))
            i += 1
        equity = equity_at(mark)
        if not equity:
            continue
        exposure = 0.0
        for coin, q in pos.items():
            if abs(q) < 1e-12:
                continue
            m = symtab[coin]
            c = venue.candle_hour(m['symbol'], mark - 3_600_000)
            if not c:
                continue
            b = venue.beta(m['symbol'], mark)
            exposure += q * c[4] * m['qty_mult'] * (1.0 if b is None else b)
        v = exposure / equity
        if math.isfinite(v):
            vals.append(v)
    return (sum(vals) / len(vals), len(vals)) if vals else (None, 0)
