"""Passive liquidity into forced flow — registration 003 outcomes. Pure functions; prices come in
as arguments (from ct/tape at run time, from constants in tests).

Costs on Hyperliquid (base tier, confirmed 2026-10-09): maker 0.015%, taker 0.045%.
"""
MAKER, TAKER = 0.00015, 0.00045


def passive_return(side, fill_price, exit_price, exit_slip=0.0005, maker=MAKER, taker=TAKER):
    """Net return of a resting order filled at `fill_price`, closed as a taker at `exit_price`
    adverse by exit_slip. side 'L' = a bid (long after fill), 'S' = an ask (short after fill)."""
    if not fill_price or not exit_price:
        return None
    sign = 1.0 if side == 'L' else -1.0
    exit_ = exit_price * (1 - sign * exit_slip)
    return sign * (exit_ / fill_price - 1) - maker - taker


def cascade_fill_price(fills, side):
    """The price a resting order at the cascade's median forced-fill price would have filled at:
    the notional-weighted median of the cascade's liquidation fill prices (half the forced
    notional traded at or beyond it). fills: [(notional, price)]."""
    rows = sorted(fills, key=lambda x: x[1], reverse=(side == 'L'))   # for bids: from high to low
    total = sum(n for n, _ in rows)
    acc = 0.0
    for n, px in rows:
        acc += n
        if acc >= total / 2:
            return px
    return rows[-1][1] if rows else None


def order_outcome(side, level, first_fill_t, exit_price, horizon_ok, exit_slip=0.0005):
    """One resting order for one day: (filled, net return or 0.0 if unfilled)."""
    if first_fill_t is None or not horizon_ok:
        return False, 0.0
    r = passive_return(side, level, exit_price, exit_slip)
    return True, (r if r is not None else 0.0)


def ev_summary(outcomes):
    """outcomes: [(filled, ret)] -> fill rate, conditional mean, EV per order."""
    n = len(outcomes)
    filled = [r for f, r in outcomes if f]
    if not n:
        return {'orders': 0}
    return {'orders': n, 'fill_rate': len(filled) / n,
            'conditional_mean': (sum(filled) / len(filled)) if filled else None,
            'ev_per_order': sum(filled) / n}
