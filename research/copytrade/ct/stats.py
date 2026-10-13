"""Test statistics for registration 001 (§5, §6, §8). Pure functions."""
import math
import random
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


def alpha(y, x):
    """OLS intercept of y on x (daily follower return on daily BTC return) — registration 001 §6.

    None with fewer than 3 pairs or a constant x (then alpha is the mean of y
    only if x never moves — returned as the mean, since no beta can be fit).
    """
    if len(y) != len(x) or len(y) < 3:
        return None
    mx, my = statistics.fmean(x), statistics.fmean(y)
    sxx = sum((a - mx) ** 2 for a in x)
    if sxx <= 0:
        return my
    beta = sum((a - mx) * (b - my) for a, b in zip(x, y)) / sxx
    return my - beta * mx


# --- revision 4 additions (registration 001 §5, §6, §6c) ---

def top_half_ic(rows, min_per_group=10):
    """tercile_ic on the rows whose score is at or above the median score."""
    if len(rows) < 2 * min_per_group:
        return (None, 0)
    med = statistics.median(r[0] for r in rows)
    return tercile_ic([r for r in rows if r[0] >= med], min_per_group)


def decile_means(rows):
    """Mean outcome by score decile, lowest first — [(decile, n, mean)]."""
    srt = sorted(rows, key=lambda r: r[0])
    k = len(srt) // 10
    if k == 0:
        return []
    out = []
    for d in range(10):
        g = srt[d * k:(d + 1) * k] if d < 9 else srt[9 * k:]
        out.append((d + 1, len(g), statistics.fmean(r[1] for r in g)))
    return out


def zero_share_by_quintile(rows):
    """Share of exactly-zero outcomes by score quintile, lowest first (flat follower = 0)."""
    srt = sorted(rows, key=lambda r: r[0])
    k = len(srt) // 5
    if k == 0:
        return []
    out = []
    for q in range(5):
        g = srt[q * k:(q + 1) * k] if q < 4 else srt[4 * k:]
        out.append((q + 1, sum(1 for r in g if r[1] == 0.0) / len(g)))
    return out


def paired_diff(a, b):
    """Mean and standard error of a - b over paired observations: (mean, se, n)."""
    if len(a) != len(b) or len(a) < 2:
        return (None, None, len(a))
    d = [x - y for x, y in zip(a, b)]
    return (statistics.fmean(d), statistics.stdev(d) / math.sqrt(len(d)), len(d))


def mean_se(values):
    """(mean, standard error, n) of a list."""
    if len(values) < 2:
        return (statistics.fmean(values) if values else None, None, len(values))
    return (statistics.fmean(values), statistics.stdev(values) / math.sqrt(len(values)), len(values))


def cluster_bootstrap(windows, n_boot=400, seed=0, ic=tercile_ic):
    """Wallet-clustered bootstrap of the combined Z over windows.

    windows: [[(wallet, score, outcome, control)] per window]. Wallets are
    resampled with replacement, and a wallet's rows in EVERY window move
    together, so a wallet-level effect that repeats across windows widens the
    bootstrap. Under independent wallets the bootstrap sd of Z is about 1,
    so Z / sd is the overlap-corrected statistic; sd > 1 means the windows are
    not independent and the plain Z overstates.
    Returns {'z': observed, 'sd': bootstrap sd, 'z_clustered': z / sd,
             'overlap': share of wallets present in more than one window}.
    """
    z = combine([ic([(s, o, c) for _, s, o, c in w]) for w in windows])
    wallets = sorted({r[0] for w in windows for r in w})
    presence = {}
    for w in windows:
        for r in w:
            presence[r[0]] = presence.get(r[0], 0) + 1
    overlap = sum(1 for v in presence.values() if v > 1) / len(wallets) if wallets else 0.0
    by_wallet = [{} for _ in windows]
    for i, w in enumerate(windows):
        for r in w:
            by_wallet[i].setdefault(r[0], []).append((r[1], r[2], r[3]))
    rng = random.Random(seed)
    zs = []
    for _ in range(n_boot):
        draw = [wallets[rng.randrange(len(wallets))] for _ in wallets]
        ics = []
        for i in range(len(windows)):
            rows = [r for a in draw for r in by_wallet[i].get(a, [])]
            ics.append(ic(rows))
        zb = combine(ics)
        if zb is not None:
            zs.append(zb)
    sd = statistics.stdev(zs) if len(zs) > 2 else None
    return {'z': z, 'sd': sd, 'z_clustered': (z / sd) if (z is not None and sd) else None, 'overlap': overlap,
            'n_boot': len(zs)}


def split_test(rows, n_boot=1000, seed=0):
    """T4: mean outcome of the low-crowding half minus the high half, pooled over windows.

    rows: [(window, wallet, crowding, outcome)]. The split is at each window's
    median crowding. Standard error from a wallet bootstrap (rows of one
    wallet move together). Returns {'diff', 'se', 'z', 'n_low', 'n_high'}.
    """
    by_w = {}
    for w, a, c, o in rows:
        by_w.setdefault(w, []).append((a, c, o))
    labelled = []   # (wallet, low?, outcome)
    for w, rs in by_w.items():
        med = statistics.median(c for _, c, _ in rs)
        for a, c, o in rs:
            labelled.append((a, c <= med, o))

    def diff(items):
        lo = [o for _, l, o in items if l]
        hi = [o for _, l, o in items if not l]
        return (statistics.fmean(lo) - statistics.fmean(hi)) if lo and hi else None

    d = diff(labelled)
    by_wallet = {}
    for a, l, o in labelled:
        by_wallet.setdefault(a, []).append((a, l, o))
    wallets = sorted(by_wallet)
    rng = random.Random(seed)
    ds = []
    for _ in range(n_boot):
        draw = [wallets[rng.randrange(len(wallets))] for _ in wallets]
        v = diff([r for a in draw for r in by_wallet[a]])
        if v is not None:
            ds.append(v)
    se = statistics.stdev(ds) if len(ds) > 2 else None
    return {'diff': d, 'se': se, 'z': (d / se) if (d is not None and se) else None,
            'n_low': sum(1 for _, l, _ in labelled if l), 'n_high': sum(1 for _, l, _ in labelled if not l)}
