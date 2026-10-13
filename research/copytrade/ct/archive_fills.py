"""Read pruned archive fills (data/pruned/...) in the API's fill format.

The replay code (replay.leader_events, chain_breaks, closed_pnl_check) was
written against the Hyperliquid API's fill dicts. Archive rows carry the same
facts under other names and types; this adapter maps them so one code path
serves both sources:

    archive                 API
    address                 (the wallet; implicit in API calls)
    side 'buy' / 'sell'     side 'B' / 'A'
    timestamp (ms, UTC)     time (epoch ms)
    size, price             sz, px            (Decimal -> float)
    start_position          startPosition
    realized_pnl            closedPnl
    trade_id                tid
    direction               dir
    crossed                 crossed           (True = taker; exactly half of all
                                               rows, one per side of each match)

Row order within a file is kept: within a millisecond it is the only
execution order available (checked in p3_checks.py).
"""
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .net import DATA

PREFIX = {'hyperliquid': 'by_dex/hyperliquid/fills/perp/all', 'xyz': 'by_dex/xyz/fills/perp/all'}


def day_file(dex, day):
    return DATA / 'pruned' / PREFIX[dex] / f'date={day}' / 'fills.parquet'


def read_day(dex, day, addresses=None):
    """Arrow table of one day's fills, optionally only some addresses."""
    t = pq.read_table(day_file(dex, day))
    if addresses is not None:
        t = t.filter(pc.is_in(t.column('address'), value_set=pa.array(list(addresses), pa.string())))
    return t


def to_api(table):
    """{address: [fill dict in API format, file order]}."""
    out = {}
    cols = {c: table.column(c).to_pylist() for c in
            ('address', 'coin', 'timestamp', 'side', 'size', 'price', 'start_position',
             'realized_pnl', 'trade_id', 'direction', 'crossed', 'twap_id')}
    for i in range(table.num_rows):
        out.setdefault(cols['address'][i], []).append({
            'coin': cols['coin'][i],
            'time': int(cols['timestamp'][i].timestamp() * 1000),
            'side': 'B' if cols['side'][i] == 'buy' else 'A',
            'sz': float(cols['size'][i]),
            'px': float(cols['price'][i]),
            'startPosition': float(cols['start_position'][i]),
            'closedPnl': float(cols['realized_pnl'][i]),
            'tid': cols['trade_id'][i],
            'dir': cols['direction'][i],
            'crossed': cols['crossed'][i],
            'twapId': cols['twap_id'][i],
        })
    return out
