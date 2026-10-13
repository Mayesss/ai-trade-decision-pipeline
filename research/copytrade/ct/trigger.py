"""Event-triggered passive orders — registration 003 T2 (revision 2).

A causal trigger: when a coin's liquidation notional over the trailing WINDOW crosses the
threshold, an order is posted LATENCY after the crossing print, at the crossing print's price
moved DELTA against the forced flow (a bid below for longs being liquidated, an ask above for
shorts). It lives TTL; it fills at the first tape trade at or beyond the level; it is closed
HORIZON after the fill as a taker. Pure functions; the tape is passed in as a callable.
"""
from . import liq


def triggers(fills, threshold_fn, window_ms=60_000, cooldown_ms=15 * 60_000):
    """fills: one coin's liquidation fills [(t, direction, notional, price)], time order.
    threshold_fn(t) -> notional threshold at t (None = unknown, no trigger).
    Returns [{'t', 'side', 'price', 'window_notional'}] — one per crossing, with a cooldown."""
    out, win, cooldown_until = [], [], 0
    for t, d, n, px in fills:
        s = liq.side_of(d)
        if s is None:
            continue
        win.append((t, n, s))
        while win and win[0][0] < t - window_ms:
            win.pop(0)
        thr = threshold_fn(t)
        if thr is None or t < cooldown_until:
            continue
        total = sum(x for _, x, _ in win)
        if total >= thr:
            long_n = sum(x for _, x, ss in win if ss == 'L')
            out.append({'t': t, 'side': 'L' if long_n >= total / 2 else 'S', 'price': px, 'window_notional': total})
            cooldown_until = t + cooldown_ms
    return out


def order_for(trig, delta, latency_ms):
    """The resting order a trigger posts: level and the time it is live."""
    sign = -1.0 if trig['side'] == 'L' else 1.0           # bid below for longs liquidated, ask above for shorts
    return {'side': trig['side'], 'level': trig['price'] * (1 + sign * delta), 't_post': trig['t'] + latency_ms}
