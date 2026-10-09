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

HOUR = 3_600_000
DAY = 86_400_000
HEDGE_SYMBOL = 'BTCUSDT'


class BitgetVenue:
    """Bitget USDT-FUTURES: always open, taker fee, 8h (or per-symbol) funding."""

    name = 'bitget'
    taker_fee = 0.0006  # identical on all 817 contracts, 2026-10-07

    def __init__(self, day, funding_override=None):
        self.day = day  # cache key for funding history (Bitget serves ~90 days)
        self._funding = {}
        self._beta = {}
        self._atr = {}
        # {symbol: [(ms, rate)]} replacing Bitget's history — registration 001
        # uses Hyperliquid funding as the proxy before Bitget's ~90-day window.
        self.funding_override = funding_override

    def is_open(self, t_ms):
        return True

    def next_open(self, t_ms):
        return t_ms

    def candle_minute(self, symbol, minute_ms):
        return bg.candle_at(symbol, minute_ms)

    def candle_hour(self, symbol, hour_ms):
        return bg.candle_1h(symbol, hour_ms)

    def atr_pct(self, symbol, t_ms, n=14):
        """Daily ATR(n) as a fraction of price from UTC-day bars built out of the cached 1H
        candles closed before t's UTC day. Offline: bg.atr_pct fetched Bitget's 16:00-anchored
        daily candles per (symbol, day) over the network (amendment 1, 2026-10-09). Cached per
        (symbol, day); None with fewer than n + 1 complete days."""
        day = t_ms - t_ms % DAY
        key = (symbol, day)
        if key not in self._atr:
            days = []
            for d in range(day - (n + 1) * DAY, day, DAY):
                bars = [bg.candle_1h(symbol, h) for h in range(d, d + DAY, HOUR)]
                bars = [b for b in bars if b]
                if len(bars) < 20:
                    days.append(None)
                    continue
                days.append((max(b[2] for b in bars), min(b[3] for b in bars), bars[-1][4]))
            days = [x for x in days if x]
            if len(days) < n + 1:
                self._atr[key] = None
            else:
                days = days[-(n + 1):]
                trs = [max(h - l, abs(h - prev[2]), abs(l - prev[2])) for prev, (h, l, _) in zip(days, days[1:])]
                self._atr[key] = sum(trs) / len(trs) / days[-1][2]
        return self._atr[key]

    def funding(self, symbol):
        if self.funding_override is not None:
            return self.funding_override.get(symbol, [])
        if symbol not in self._funding:
            self._funding[symbol] = bg.funding_history(symbol, self.day)
        return self._funding[symbol]

    def beta(self, symbol, t_ms, days=30):
        """Beta of symbol to BTC from hourly returns over the 30 days before t's UTC day.

        Point-in-time (only closed candles before the day starts), cached per
        (symbol, day). None if fewer than 200 paired hours exist.
        """
        if symbol == HEDGE_SYMBOL:
            return 1.0
        day = t_ms - t_ms % DAY
        key = (symbol, day)
        if key not in self._beta:
            xs, ys = [], []
            prev = None
            for h in range(day - days * DAY, day, HOUR):
                a, b = bg.candle_1h(HEDGE_SYMBOL, h), bg.candle_1h(symbol, h)
                cur = (a[4], b[4]) if a and b else None
                if cur and prev:
                    xs.append(cur[0] / prev[0] - 1)
                    ys.append(cur[1] / prev[1] - 1)
                prev = cur
            if len(xs) < 200:
                self._beta[key] = None
            else:
                mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
                var = sum((x - mx) ** 2 for x in xs)
                self._beta[key] = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / var if var else None
        return self._beta[key]
