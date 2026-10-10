"""Forced-flow liquidity provider — registration 004 mechanics. Pure functions; the tape and
Bitget candles come in as arguments.

Pieces: a ladder of resting rungs posted at a trigger relative to the cascade's first print;
a counterparty check (hold a forced fill, scratch any other); an exit on the end of forced
flow, the reference price or a time cap; a market-stress switch; and the cross-venue capture
(hedge a Hyperliquid fill on Bitget at once, unwind both legs later).
"""
from .passive import MAKER, TAKER, passive_return

BG_TAKER = 0.0006


def ladder(side, ref_price, current_price, rungs, sizes):
    """Resting rungs beyond the cascade's first print, skipping any already passed by the
    current print (a bid above the market would cross the book). [(level, size, rung)]."""
    sign = -1.0 if side == 'L' else 1.0
    out = []
    for r, w in zip(rungs, sizes):
        level = ref_price * (1 + sign * r)
        passed = level >= current_price if side == 'L' else level <= current_price
        if not passed:
            out.append((level, w, r))
    return out


def exit_time(fill_t, flow_end_t, ref_touch_t, after_flow_ms, cap_ms):
    """Earliest of: after_flow_ms past the end of forced flow (but never before the fill), the
    reference-price touch, the time cap after the fill."""
    cands = [fill_t + cap_ms, max(flow_end_t + after_flow_ms, fill_t + 1)]
    if ref_touch_t is not None and ref_touch_t > fill_t:
        cands.append(ref_touch_t)
    return min(cands)


def fill_outcome(side, level, forced, exit_price, scratch_price, exit_slip=0.0005):
    """Net return of one filled rung: held to exit_price if the filling trade was forced,
    scratched at the next print otherwise (taker, adverse by exit_slip). (held?, return)."""
    if forced:
        return True, passive_return(side, level, exit_price, exit_slip)
    return False, passive_return(side, level, scratch_price, exit_slip)


def ladder_return(outcomes):
    """Size-weighted return of a ladder's filled rungs: [(size, return)] -> return per unit posted
    size, counting unfilled rungs as 0 (the EV view) and the mean over filled rungs (conditional)."""
    posted = sum(w for w, _ in outcomes)
    filled = [(w, r) for w, r in outcomes if r is not None]
    if not posted:
        return None, None
    ev = sum(w * r for w, r in filled) / posted
    cond = (sum(w * r for w, r in filled) / sum(w for w, _ in filled)) if filled else None
    return ev, cond


def stress(market_rate, threshold):
    """True when the trailing market-wide liquidation rate says 'spiral, stand aside'."""
    return market_rate is not None and market_rate >= threshold


def hedged_capture(side, hl_fill, hl_exit, bg_entry, bg_exit, hl_exit_slip=0.0005, bg_slip_frac=0.10):
    """Cross-venue capture: a Hyperliquid fill hedged on Bitget at the next minute's open, both
    legs unwound later. side 'L' = long HL (bid filled), short Bitget. Candles [ts, o, h, l, c].
    Returns (total, hl_leg, bg_leg) as returns on the HL notional."""
    if not (hl_fill and hl_exit and bg_entry and bg_exit):
        return None, None, None
    sign = 1.0 if side == 'L' else -1.0
    hl_leg = passive_return(side, hl_fill, hl_exit, hl_exit_slip)
    e_slip = bg_slip_frac * (bg_entry[2] - bg_entry[3])
    x_slip = bg_slip_frac * (bg_exit[2] - bg_exit[3])
    bg_in = bg_entry[1] - sign * e_slip          # selling on Bitget when long on HL: adverse = lower
    bg_out = bg_exit[1] + sign * x_slip
    bg_leg = -sign * (bg_out / bg_in - 1) - 2 * BG_TAKER
    return hl_leg + bg_leg, hl_leg, bg_leg
