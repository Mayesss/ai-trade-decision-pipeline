"""Market regimes from long BTC price history (plan §5.6; registration 001).

Two uses, kept apart:

1. **Selection (point-in-time).** `trend_at(t)`: BTC's trailing 30-day
   return at time t, from hourly closes strictly before t. A round trip is
   "rising" if it opened when that return was >= 0, else "falling". The
   cross-regime skill rule needs a leader to be good in both.
2. **Reporting (descriptive, may use hindsight).** `month_label(month)`:
   UP / DOWN / FLAT from the calendar month's BTC return (±8%). Results are
   split by these labels; they decide nothing.

Data: Bitget BTCUSDT 1H candles, paged backwards with endTime only (a
start+end window is capped), back to the first candle Bitget serves.
Stored once in data/derived/regimes/btc_1h.parquet.
"""
import bisect
import datetime as dt

from . import bitget as bg
from .net import DATA

HOUR = 3_600_000
DAY = 86_400_000
TREND_DAYS = 30
MONTH_THRESHOLD = 0.08
PATH = DATA / 'derived' / 'regimes' / 'btc_1h.parquet'


def fetch_btc_1h(until_ms):
    """All BTCUSDT 1H candles Bitget serves before until_ms, oldest first."""
    rows, cur = {}, until_ms
    while True:
        batch = bg._candles_before('BTCUSDT', '1H', cur, 200)
        fresh = [r for r in batch if r[0] not in rows]
        if not fresh:
            break
        for r in fresh:
            rows[r[0]] = r
        cur = min(r[0] for r in fresh)
    return [rows[k] for k in sorted(rows)]


def save(rows):
    import pyarrow as pa
    import pyarrow.parquet as pq
    PATH.parent.mkdir(parents=True, exist_ok=True)
    cols = list(zip(*rows))
    pq.write_table(pa.table({'ts': pa.array(cols[0], pa.int64()), 'open': cols[1], 'high': cols[2],
                             'low': cols[3], 'close': cols[4]}), PATH)


class Regimes:
    def __init__(self):
        import pyarrow.parquet as pq
        t = pq.read_table(PATH)
        self.ts = t.column('ts').to_pylist()
        self.close = t.column('close').to_pylist()

    def _close_before(self, t_ms):
        i = bisect.bisect_left(self.ts, t_ms) - 1  # last candle starting before t
        # a 1H candle starting at ts closes at ts + 1h; use only fully closed ones
        while i >= 0 and self.ts[i] + HOUR > t_ms:
            i -= 1
        return self.close[i] if i >= 0 else None

    def trend_at(self, t_ms):
        """BTC trailing 30-day return at t, from closes strictly before t."""
        now, then = self._close_before(t_ms), self._close_before(t_ms - TREND_DAYS * DAY)
        return now / then - 1 if now and then else None

    def is_rising(self, t_ms):
        r = self.trend_at(t_ms)
        return None if r is None else r >= 0

    def month_label(self, month):
        """'UP' / 'DOWN' / 'FLAT' for 'YYYY-MM' — descriptive, uses hindsight."""
        y, m = map(int, month.split('-'))
        start = int(dt.datetime(y, m, 1, tzinfo=dt.UTC).timestamp() * 1000)
        end = int(dt.datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=dt.UTC).timestamp() * 1000)
        a, b = self._close_before(start + HOUR), self._close_before(end + HOUR)
        if not a or not b:
            return None
        r = b / a - 1
        return 'UP' if r > MONTH_THRESHOLD else 'DOWN' if r < -MONTH_THRESHOLD else 'FLAT'
