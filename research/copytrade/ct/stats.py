"""Test statistics for registration 001 (§5, §6, §8). Pure functions."""
import math
import statistics


def ranks(values):
    """Average ranks (ties share their mean rank), 1-based."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    out = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        for k in range(i, j + 1):
            out[order[k]] = (i + j) / 2 + 1
        i = j + 1
    return out


def spearman(x, y):
    if len(x) < 3:
        return None
    rx, ry = ranks(x), ranks(y)
    mx, my = statistics.fmean(rx), statistics.fmean(ry)
    sxy = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    sxx = sum((a - mx) ** 2 for a in rx)
    syy = sum((b - my) ** 2 for b in ry)
    return sxy / math.sqrt(sxx * syy) if sxx and syy else None


def tercile_ic(rows, min_per_group=10):
    """Count-weighted Spearman IC within terciles of a control variable.

    rows: [(score, outcome, control)]. Returns (ic, n) — n is the number of
    rows that entered a tercile with at least min_per_group members.
    """
    rows = sorted(rows, key=lambda r: r[2])
    k = len(rows) // 3
    groups = [rows[:k], rows[k:2 * k], rows[2 * k:]]
    num = n = 0
    for g in groups:
        if len(g) < min_per_group:
            continue
        ic = spearman([r[0] for r in g], [r[1] for r in g])
        if ic is None:
            continue
        num += ic * len(g)
        n += len(g)
    return (num / n, n) if n else (None, 0)


def fisher_z(ic, n):
    ic = max(min(ic, 0.999999), -0.999999)
    return math.atanh(ic) * math.sqrt(n - 3)


def combine(ics):
    """Z = sum(z_w) / sqrt(W) over windows [(ic, n)]."""
    zs = [fisher_z(ic, n) for ic, n in ics if ic is not None and n > 3]
    return sum(zs) / math.sqrt(len(zs)) if zs else None


def newey_west_t(series, lags):
    """t-statistic of the mean with Newey-West (Bartlett) standard errors."""
    n = len(series)
    if n < lags + 2:
        return None
    m = statistics.fmean(series)
    d = [x - m for x in series]
    var = sum(x * x for x in d) / n
    for l in range(1, lags + 1):
        w = 1 - l / (lags + 1)
        var += 2 * w * sum(d[i] * d[i - l] for i in range(l, n)) / n
    return m / math.sqrt(var / n) if var > 0 else None
