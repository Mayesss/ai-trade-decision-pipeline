"""Event study for registration 002 §4–§5: per-event net returns, cluster-robust t,
placebo, tercile split. Pure functions; prices come in through a callable.
"""
import math
import random
import statistics

MINUTE = 60_000


def event_return(side, entry_candle, exit_candle, taker, slip_frac):
    """Net return of one against-the-cascade trade (long after longs were liquidated).

    Candles are [ts, o, h, l, c]. Entry at the open adverse by slip_frac x range, exit at the
    open adverse the same way, taker fee both sides. None without both candles.
    """
    if not entry_candle or not exit_candle:
        return None
    sign = 1.0 if side == 'L' else -1.0           # longs liquidated -> price fell -> go long
    e_slip = slip_frac * (entry_candle[2] - entry_candle[3])
    x_slip = slip_frac * (exit_candle[2] - exit_candle[3])
    entry = entry_candle[1] + sign * e_slip
    exit_ = exit_candle[1] - sign * x_slip
    gross = sign * (exit_ / entry - 1)
    return gross - 2 * taker


def cluster_t(values, clusters):
    """t-statistic of the mean with standard errors clustered on `clusters` (same length).

    Cluster-robust variance of the mean: sum over clusters of (sum of demeaned values)^2 / n^2.
    (mean, se, t, n, n_clusters); None where undefined.
    """
    pairs = [(v, c) for v, c in zip(values, clusters) if v is not None]
    n = len(pairs)
    if n < 3:
        return (None, None, None, n, 0)
    m = statistics.fmean(v for v, _ in pairs)
    sums = {}
    for v, c in pairs:
        sums[c] = sums.get(c, 0.0) + (v - m)
    var = sum(x * x for x in sums.values()) / (n * n)
    g = len(sums)
    if g > 1:
        var *= g / (g - 1)
    se = math.sqrt(var) if var > 0 else None
    return (m, se, (m / se) if se else None, n, g)


def placebo(events, draw_return, n_seeds=200, seed=0, z=1.96):
    """Share of seeds whose placebo |t| reaches z. draw_return(rng, event) -> return at a
    random time of the same coin and day (the caller does the price lookup)."""
    hits = 0
    for s in range(n_seeds):
        rng = random.Random(seed + s)
        vals = [draw_return(rng, e) for e in events]
        _, _, t, _, _ = cluster_t(vals, [e['cluster'] for e in events])
        hits += t is not None and abs(t) >= z
    return hits / n_seeds


def tercile_split(events, key, value, cluster, within):
    """Top minus bottom tercile of `key`, terciles formed within `within` groups.

    Returns {'diff', 'se', 'z', 'means': [bottom, middle, top], 'n': [...]} using a
    cluster-robust SE of the difference (events of a cluster move together).
    """
    groups = {}
    for e in events:
        if key(e) is not None and value(e) is not None:
            groups.setdefault(within(e), []).append(e)
    labelled = []  # (tercile, value, cluster)
    for g in groups.values():
        g = sorted(g, key=key)
        k = len(g) // 3
        if k == 0:
            continue
        for i, e in enumerate(g):
            t = 0 if i < k else 1 if i < 2 * k else 2
            labelled.append((t, value(e), cluster(e)))
    means = [statistics.fmean(v for t, v, _ in labelled if t == q) if any(t == q for t, _, _ in labelled) else None
             for q in range(3)]
    ns = [sum(1 for t, _, _ in labelled if t == q) for q in range(3)]
    # difference top - bottom as a mean of signed contributions, clustered
    top = [(v, c) for t, v, c in labelled if t == 2]
    bot = [(v, c) for t, v, c in labelled if t == 0]
    if len(top) < 3 or len(bot) < 3:
        return {'diff': None, 'se': None, 'z': None, 'means': means, 'n': ns}
    mt, mb = statistics.fmean(v for v, _ in top), statistics.fmean(v for v, _ in bot)
    sums = {}
    for v, c in top:
        sums[c] = sums.get(c, 0.0) + (v - mt) / len(top)
    for v, c in bot:
        sums[c] = sums.get(c, 0.0) - (v - mb) / len(bot)
    var = sum(x * x for x in sums.values())
    g = len(sums)
    if g > 1:
        var *= g / (g - 1)
    se = math.sqrt(var) if var > 0 else None
    d = mt - mb
    return {'diff': d, 'se': se, 'z': (d / se) if se else None, 'means': means, 'n': ns}
