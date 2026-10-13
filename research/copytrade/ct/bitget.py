"""Bitget public market data (USDT-FUTURES), the execution venue.

Facts verified 2026-10-07:
- `launchTime` is empty on all 817 contracts, so listing dates come from the
  first daily candle (`first_candle_day`).
- 1m history candles reach back at least to 2025-08.
- Funding history (`history-fund-rate`) only reaches back ~90 days. Older
  periods need a proxy (Hyperliquid funding) — see the plan.
- Delisted contracts are absent from the contract list, so a coin listed in
  the past and delisted since reads as "never listed" (execution-side
  survivorship; small, but it biases toward fewer tradeable fills).
"""
import time

from .net import RatePacer, cached, get_json

BASE = 'https://api.bitget.com'
PRODUCT = 'USDT-FUTURES'
MINUTE = 60_000
BLOCK = 200 * MINUTE  # one history-candles page of 1m bars

_pacer = RatePacer(15)  # documented market limit is 20/s/IP; stay under it


def _get(path):
    _pacer.wait()
    res = get_json(BASE + path)
    if res.get('code') != '00000':
        raise RuntimeError(f'bitget {path}: {res.get("code")} {res.get("msg")}')
    return res['data']


def contracts(day):
    return cached('bg-contracts', day, lambda: _get(f'/api/v2/mix/market/contracts?productType={PRODUCT}'))


def tickers():
    return _get(f'/api/v2/mix/market/tickers?productType={PRODUCT}')


def _candles(symbol, granularity, start_ms, end_ms, limit=200):
    rows = _get(f'/api/v2/mix/market/history-candles?symbol={symbol}&productType={PRODUCT}'
                f'&granularity={granularity}&startTime={start_ms}&endTime={end_ms}&limit={limit}')
    # [ts, open, high, low, close, base vol, quote vol] as strings
    return [[int(r[0])] + [float(x) for x in r[1:5]] for r in rows]


def _block(symbol, block_start):
    """The 200 one-minute candles starting at block_start, keyed by minute."""
    fetch = lambda: _candles(symbol, '1m', block_start, block_start + BLOCK - MINUTE)
    if block_start + BLOCK < time.time() * 1000 - 10 * MINUTE:
        rows = cached('bg-1m', f'{symbol}:{block_start}', fetch)
    else:
        rows = fetch()  # still forming: never cache
    return {r[0]: r for r in rows}


class _LRU(dict):
    """In-memory block cache with a size cap.

    Registration 001's run touches ~225k one-minute blocks (~60 KB each in
    Python objects); keeping them all would need ~13 GB. Wallets are replayed
    one at a time and each touches its own blocks, so a cap of 8k blocks
    (~0.5 GB) keeps the working set warm and re-reads the rest from disk
    (24k on the first try; cut after the 2026-10-09 swap thrash).
    """
    CAP = 8_000

    def __getitem__(self, key):
        value = super().__getitem__(key)
        super().__delitem__(key)           # move to the end (most recent)
        super().__setitem__(key, value)
        return value

    def __setitem__(self, key, value):
        if key in self:
            super().__delitem__(key)
        super().__setitem__(key, value)
        while len(self) > self.CAP:
            super().__delitem__(next(iter(self)))


_blocks = _LRU()


def candle_at(symbol, minute_ms):
    """[ts, o, h, l, c] of the 1m candle starting at minute_ms.

    A minute with no trades has no candle; fall back to the latest earlier
    candle in the same block (its close is the last traded price). None if the
    block holds nothing at or before the minute.
    """
    start = minute_ms - minute_ms % BLOCK
    key = (symbol, start)
    if key not in _blocks:
        _blocks[key] = _block(symbol, start)
    rows = _blocks[key]
    if minute_ms in rows:
        return rows[minute_ms]
    earlier = [t for t in rows if t < minute_ms]
    if not earlier:
        return None
    _, _, _, _, close = rows[max(earlier)]
    return [minute_ms, close, close, close, close]


HOUR = 3_600_000
DAY = 86_400_000
HOUR_BLOCK = 200 * HOUR


def candle_1h(symbol, hour_ms):
    """[ts, o, h, l, c] of the 1H candle starting at hour_ms, or None."""
    start = hour_ms - hour_ms % HOUR_BLOCK
    key = ('1H', symbol, start)
    if key not in _blocks:
        fetch = lambda: _candles(symbol, '1H', start, start + HOUR_BLOCK - HOUR)
        if start + HOUR_BLOCK < time.time() * 1000 - 2 * HOUR:
            rows = cached('bg-1h', f'{symbol}:{start}', fetch)
        else:
            rows = fetch()
        _blocks[key] = {r[0]: r for r in rows}
    return _blocks[key].get(hour_ms)


def atr_pct(symbol, t_ms, n=14):
    """Daily ATR(n) as a fraction of price, from candles CLOSED before t_ms.

    Bitget daily candles run 16:00 -> 16:00 UTC, not midnight, so the candle
    containing t_ms is still forming and is dropped explicitly. Cached per
    (symbol, UTC day): within a day the closed set changes at most once, at
    16:00, and the day-start view is the conservative (older) one.
    """
    day_start = t_ms - t_ms % DAY

    def compute():
        rows = [r for r in _candles_before(symbol, '1D', day_start, n + 2) if r[0] + DAY <= day_start]
        rows = rows[-(n + 1):]
        if len(rows) < n + 1:
            return None
        trs = [max(h - l, abs(h - prev[4]), abs(l - prev[4])) for prev, (_, _, h, l, _) in zip(rows, rows[1:])]
        return sum(trs) / len(trs) / rows[-1][4]
    return cached('bg-atr', f'{symbol}:{day_start}:{n}', compute)


def funding_history(symbol, day):
    """All funding settlements Bitget still serves (~90 days), oldest first."""
    def fetch():
        out, page = [], 1
        while True:
            rows = _get(f'/api/v2/mix/market/history-fund-rate?symbol={symbol}'
                        f'&productType={PRODUCT}&pageSize=100&pageNo={page}')
            out.extend((int(r['fundingTime']), float(r['fundingRate'])) for r in rows)
            if len(rows) < 100:
                return sorted(out)
            page += 1
    return cached('bg-funding', f'{symbol}:{day}', fetch)


def _candles_before(symbol, granularity, end_ms, limit):
    # A start+end window is capped (~90 days at 1D: wider fails with 40017);
    # endTime alone returns the `limit` newest candles before it.
    rows = _get(f'/api/v2/mix/market/history-candles?symbol={symbol}&productType={PRODUCT}'
                f'&granularity={granularity}&endTime={end_ms}&limit={limit}')
    return [[int(r[0])] + [float(x) for x in r[1:5]] for r in rows]


def first_candle_day(symbol, day, floor_ms=1_546_300_800_000):
    """Timestamp of the earliest daily candle (listing-date proxy).

    Binary search on "does any daily candle exist before t".
    """
    day_ms = 86_400_000

    def search():
        lo, hi = floor_ms, int(time.time() * 1000)
        if not _candles_before(symbol, '1D', hi, 1):
            return None
        while hi - lo > day_ms:
            mid = (lo + hi) // 2
            if _candles_before(symbol, '1D', mid, 1):
                hi = mid
            else:
                lo = mid
        rows = _candles_before(symbol, '1D', hi + day_ms, 5)
        return min(r[0] for r in rows) if rows else hi
    return cached('bg-first-day', f'{symbol}:{day}', search)
