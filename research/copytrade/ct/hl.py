"""Hyperliquid public API (no key, no account).

Rate limit, verified 2026-10-07 against the docs: 1200 weight / minute / IP.
Info requests weigh 20 (allMids, clearinghouseState: 2); userFills /
userFillsByTime / fundingHistory add 1 per 20 items returned. We budget 800
to leave headroom for anything else on the same IP.
"""
from .net import WeightBudget, cached, get_json, post_json

INFO = 'https://api.hyperliquid.xyz/info'
# Undocumented stats endpoint behind app.hyperliquid.xyz/leaderboard (~40 MB).
LEADERBOARD = 'https://stats-data.hyperliquid.xyz/Mainnet/leaderboard'

FILLS_PAGE_MAX = 2000
FILLS_RETAINED = 10_000  # the API keeps only each wallet's most recent 10k fills

_budget = WeightBudget(1100)


def _info(body, weight=20, items_per_weight=None):
    _budget.spend(weight)
    res = post_json(INFO, body)
    if items_per_weight and isinstance(res, list):
        _budget.spend(len(res) // items_per_weight)
    return res


def perp_universe():
    return _info({'type': 'meta'})['universe']


def all_mids():
    return _info({'type': 'allMids'}, weight=2)


def leaderboard(day):
    """Snapshot of the leaderboard as served on `day` (cache key only)."""
    return cached('hl-leaderboard', day, lambda: get_json(LEADERBOARD, timeout=180))['leaderboardRows']


def _paged(req_type, user, start_ms, end_ms, unwrap=lambda x: x):
    """Every item of a time-ranged info request, in the API's order.

    Pages advance on the last item's timestamp; items sharing that millisecond
    can straddle a page boundary, so pages are de-duplicated on `tid`. The
    API's order within a millisecond is execution order — keep it.
    """
    out, seen, t = [], set(), start_ms
    while True:
        page = [unwrap(x) for x in _info({'type': req_type, 'user': user, 'startTime': t, 'endTime': end_ms},
                                         items_per_weight=20)]
        fresh = [f for f in page if f['tid'] not in seen]
        seen.update(f['tid'] for f in fresh)
        out.extend(fresh)
        if len(page) < FILLS_PAGE_MAX or not fresh:
            return out
        t = page[-1]['time']


def user_fills(user, start_ms, end_ms):
    """Every fill for `user` in [start_ms, end_ms], oldest first.

    userFillsByTime does NOT return TWAP slice fills (found in P0: positions
    moved with no fill until userTwapSliceFillsByTime was merged in), so both
    are fetched and merged on time, regular fills first within a millisecond.

    Returns (fills, possibly_truncated). The 10k retention figure from
    third-party docs did not hold in P0 (wallets returned up to 30,722 fills
    over 60 days, starting at the window start), so this flag is a weak hint
    only; the startPosition chain check in P0 is the real completeness test.
    """
    key = f'{user}:{start_ms}:{end_ms}'
    regular = cached('hl-fills', key, lambda: _paged('userFillsByTime', user, start_ms, end_ms))
    twap = cached('hl-twap-fills', key,
                  lambda: _paged('userTwapSliceFillsByTime', user, start_ms, end_ms, unwrap=lambda x: x['fill']))
    known = {f['tid'] for f in regular}
    fills = sorted(regular + [f for f in twap if f['tid'] not in known], key=lambda f: f['time'])
    return fills, len(regular) >= FILLS_RETAINED - FILLS_PAGE_MAX // 10
