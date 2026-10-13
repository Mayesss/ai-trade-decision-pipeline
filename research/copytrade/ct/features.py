"""Copyability features of a leader — behaviour only, never returns.

Selection and cleaning rules may use these (plan §5.2); copy returns may not
be computed before the P4 registration exists. Callers pass only fills from
before the selection date, so every feature is point-in-time.
"""
import statistics

EPS = 1e-12
HOUR = 3_600_000


def leader_trips(events):
    """Leader round trips (flat -> flat, per coin) as (open_t, close_t)."""
    opened, trips = {}, []
    for ev in events:
        coin, before, after = ev['coin'], ev['pos_before'], ev['pos_after']
        flat_before, flat_after = abs(before) < EPS, abs(after) < EPS
        flipped = not flat_before and not flat_after and (before > 0) != (after > 0)
        if coin in opened and (flat_after or flipped):
            trips.append((opened.pop(coin), ev['t_last']))
        if (flat_before or flipped) and not flat_after:
            opened[coin] = ev['t']
    return trips


def typical_leverage(events, equity):
    """Median gross leverage after each position change.

    Gross notional uses each coin's latest fill VWAP — the price the leader
    actually traded at — over account equity.
    """
    pos, px, levs = {}, {}, []
    for ev in events:
        pos[ev['coin']] = ev['pos_after']
        px[ev['coin']] = ev['vwap']
        gross = sum(abs(q) * px[c] for c, q in pos.items())
        if gross > EPS:
            levs.append(gross / equity)
    return statistics.median(levs) if levs else None


def profile(fills, events, equity, window_days):
    """Behavioural profile of one leader over a window of fills.

    maker_share is by notional: `crossed` false means the fill rested on the
    book. A leader who earns as a maker pays Hyperliquid maker fees while the
    follower always pays Bitget's taker fee — an edge no delay can copy.
    """
    perp = [f for f in fills if not f['coin'].startswith('@') and '/' not in f['coin']]
    notional = sum(float(f['sz']) * float(f['px']) for f in perp)
    maker = sum(float(f['sz']) * float(f['px']) for f in perp if not f['crossed'])
    holds = [(c - o) / HOUR for o, c in leader_trips(events)]
    return {
        'fills_per_day': len(perp) / window_days,
        'maker_share': maker / notional if notional else None,
        'round_trips': len(holds),
        'median_hold_hours': statistics.median(holds) if holds else None,
        'share_trips_under_1h': sum(h < 1 for h in holds) / len(holds) if holds else None,
        'typical_leverage': typical_leverage(events, equity),
    }
