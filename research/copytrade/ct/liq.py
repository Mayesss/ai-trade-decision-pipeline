"""Liquidation cascades and the liquidation map — registration 002 §3, §5. Pure functions
over rows; no forward price anywhere.
"""
import bisect
import statistics

LONG_LIQ = {'Close Long', 'Liquidated Cross Long', 'Liquidated Isolated Long'}
SHORT_LIQ = {'Close Short', 'Liquidated Cross Short', 'Liquidated Isolated Short'}


def side_of(direction):
    """'L' if longs were liquidated (price fell), 'S' if shorts, None otherwise."""
    return 'L' if direction in LONG_LIQ else 'S' if direction in SHORT_LIQ else None


def runs(fills, gap_ms):
    """Maximal runs of one coin's liquidation fills with consecutive gaps <= gap_ms.

    fills: [(t_ms, direction, notional, price)] for ONE coin, time order.
    Returns [{'t_start', 't_end', 'n', 'notional', 'long_notional', 'short_notional',
              'purity', 'side', 'start_price', 'end_price'}].
    """
    out, cur = [], None
    for t, d, notional, px in fills:
        s = side_of(d)
        if s is None:
            continue
        if cur is None or t - cur['t_end'] > gap_ms:
            if cur is not None:
                out.append(_finish(cur))
            cur = {'t_start': t, 't_end': t, 'n': 0, 'long_notional': 0.0, 'short_notional': 0.0,
                   'start_price': px, 'end_price': px}
        cur['t_end'] = t
        cur['end_price'] = px
        cur['n'] += 1
        cur['long_notional' if s == 'L' else 'short_notional'] += notional
    if cur is not None:
        out.append(_finish(cur))
    return out


def _finish(r):
    r['notional'] = r['long_notional'] + r['short_notional']
    big = max(r['long_notional'], r['short_notional'])
    r['purity'] = big / r['notional'] if r['notional'] else 0.0
    r['side'] = 'L' if r['long_notional'] >= r['short_notional'] else 'S'
    return r


def cascades(fills, gap_ms, purity_min, k, floor_usd, trailing_median_daily):
    """Runs that qualify as cascades (registration 002 §3).

    trailing_median_daily(t_ms) -> the coin's trailing 30-day median daily liquidation
    notional as of the run's start day (None if unknown: the caller supplies a fallback).
    """
    out = []
    for r in runs(fills, gap_ms):
        med = trailing_median_daily(r['t_start'])
        if med is None or med <= 0:
            continue
        if r['purity'] >= purity_min and r['notional'] >= k * med and r['notional'] >= floor_usd:
            out.append({**r, 'rel': r['notional'] / med})
    return out


def trailing_median_fn(daily, days=30):
    """daily: [(day_start_ms, notional)] sorted. Returns f(t_ms) -> median over the `days`
    days strictly before t's day, or None with fewer than 5 observations."""
    ts = [d for d, _ in daily]
    vals = [v for _, v in daily]
    DAY = 86_400_000

    def f(t_ms):
        day = t_ms - t_ms % DAY
        hi = bisect.bisect_left(ts, day)
        lo = bisect.bisect_left(ts, day - days * DAY)
        window = vals[lo:hi]
        return statistics.median(window) if len(window) >= 5 else None
    return f


def density(positions, side, price, band):
    """Liquidation-map density for a cascade (registration 002 §5).

    positions: [(size, notional, liquidation_price)] of the coin in the latest snapshot.
    side: 'L' (longs being liquidated, price falling) or 'S'. Density = notional of
    positions on that side whose liquidation price lies within `band` (fraction) of
    `price` AND on the side the price is moving to (below for 'L', above for 'S'),
    divided by the coin's gross notional. (density, gross).
    """
    gross = sum(abs(n) for _, n, _ in positions)
    hit = 0.0
    for size, notional, lp in positions:
        if lp is None or size == 0:
            continue
        if side == 'L' and size > 0 and price * (1 - band) <= lp <= price:
            hit += abs(notional)
        elif side == 'S' and size < 0 and price <= lp <= price * (1 + band):
            hit += abs(notional)
    return (hit / gross if gross else None), gross
