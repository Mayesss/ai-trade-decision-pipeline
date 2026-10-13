"""Selection scores from a wallet's fills — registration 001 §4.

Before registration these functions run on SYNTHETIC fills only
(tests/test_scores.py). Applying them to real wallets is computing the score,
which waits for the REGISTERED commit.
"""
import math
import statistics

EPS = 1e-12
# Hyperliquid base-tier fees — TO CONFIRM against the fee schedule before the
# first real score (registration 001 §4); a correction is an amendment.
HL_TAKER = 0.00045
HL_MAKER = 0.00015


def round_trips(fills):
    """Completed flat -> flat round trips per coin, in execution order.

    Returns [{'coin', 'open_t', 'close_t', 'cash', 'fees', 'max_notional', 'long'}].
    A flip fill (long -> short in one fill) closes one trip and opens the
    next; its size and fee are split at the zero crossing. A position already
    open when the fills start is ignored until it is flat again — its entry
    is unknown. `cash` is the exact signed cash flow (not closedPnl, which
    drifts on sub-cent coins — P3 G4).
    """
    open_, done = {}, []
    for f in fills:
        coin, px = f['coin'], f['px']
        signed = f['sz'] if f['side'] == 'B' else -f['sz']
        sp = f['startPosition']
        ep = sp + signed
        rate = HL_TAKER if f['crossed'] else HL_MAKER
        flip = sp * ep < 0
        parts = [(-sp, sp), (ep, 0.0)] if flip else [(signed, sp)]  # (quantity, position before)
        for q, before in parts:
            after = before + q
            if abs(before) < EPS and abs(after) > EPS:
                open_[coin] = {'coin': coin, 'open_t': f['time'], 'cash': 0.0, 'fees': 0.0,
                               'max_notional': 0.0, 'long': after > 0}
            trip = open_.get(coin)
            if trip is None:
                continue
            trip['cash'] -= q * px
            trip['fees'] += abs(q) * px * rate
            trip['max_notional'] = max(trip['max_notional'], abs(before) * px, abs(after) * px)
            if abs(after) <= 1e-9 * max(1.0, abs(before)):
                trip['close_t'] = f['time']
                done.append(open_.pop(coin))
    return done


def ron(trip):
    """Return on notional, net of estimated Hyperliquid fees."""
    return (trip['cash'] - trip['fees']) / trip['max_notional'] if trip['max_notional'] > 0 else None


def t_stat(values):
    v = [x for x in values if x is not None]
    if len(v) < 2:
        return None
    sd = statistics.stdev(v)
    return statistics.fmean(v) / sd * math.sqrt(len(v)) if sd > 0 else None


def plain_score(trips):
    return t_stat(ron(t) for t in trips)


def cross_regime_score(trips, is_rising, min_trips=4):
    """min(t_rising, t_falling), each over >= min_trips trips; None otherwise.

    is_rising(t_ms) -> bool | None is the point-in-time BTC regime
    (ct/regimes.Regimes.is_rising). Trips whose regime is unknown are skipped.
    """
    up, down = [], []
    for t in trips:
        r = is_rising(t['open_t'])
        if r is True:
            up.append(ron(t))
        elif r is False:
            down.append(ron(t))
    if len(up) < min_trips or len(down) < min_trips:
        return None
    a, b = t_stat(up), t_stat(down)
    return None if a is None or b is None else min(a, b)


def long_bias(fills):
    """Long-opening notional / all opening notional (the direction control)."""
    long_open = all_open = 0.0
    for f in fills:
        signed = f['sz'] if f['side'] == 'B' else -f['sz']
        sp = f['startPosition']
        ep = sp + signed
        if sp * ep < 0:
            opened = abs(ep)            # the part beyond zero opens a new position
        elif abs(ep) > abs(sp):
            opened = abs(ep) - abs(sp)  # adding to (or opening) a position
        else:
            continue                    # reducing
        all_open += opened * f['px']
        if ep > 0:
            long_open += opened * f['px']
    return long_open / all_open if all_open else None
