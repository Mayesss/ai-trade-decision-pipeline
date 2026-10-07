"""Execution venues for the follower: prices, costs, funding and calendar.

The follower simulation (replay.follow) only talks to a venue through this
interface, so the same engine runs crypto copies on Bitget and, later,
traditional-asset copies (Hyperliquid HIP-3 markets) on Capital.

CapitalVenue is NOT implemented yet. It needs, unverified as of 2026-10-07:
- Capital minute-price history depth (nothing in lib/capital.ts records it),
  fetched from a DEMO account so research never opens sessions on the live one;
- spread + overnight-financing costs per instrument instead of a taker fee;
- the trading calendar (closed nights / weekends, holidays) — lib/market has
  market-hours metadata to port;
- FX quote direction (Hyperliquid xyz:JPY vs Capital USDJPY), see targets.hl_px.
"""
from . import bitget as bg


class BitgetVenue:
    """Bitget USDT-FUTURES: always open, taker fee, 8h (or per-symbol) funding."""

    name = 'bitget'
    taker_fee = 0.0006  # identical on all 817 contracts, 2026-10-07

    def __init__(self, day):
        self.day = day  # cache key for funding history (Bitget serves ~90 days)
        self._funding = {}

    def is_open(self, t_ms):
        return True

    def next_open(self, t_ms):
        return t_ms

    def candle_minute(self, symbol, minute_ms):
        return bg.candle_at(symbol, minute_ms)

    def candle_hour(self, symbol, hour_ms):
        return bg.candle_1h(symbol, hour_ms)

    def atr_pct(self, symbol, t_ms):
        return bg.atr_pct(symbol, t_ms)

    def funding(self, symbol):
        if symbol not in self._funding:
            self._funding[symbol] = bg.funding_history(symbol, self.day)
        return self._funding[symbol]
