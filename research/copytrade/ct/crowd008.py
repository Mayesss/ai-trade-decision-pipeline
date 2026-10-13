"""Registration 008 (L3) — 001 T3 INVERTED, decided on the Hyperliquid holdout only.

The rule is 001 §8 unchanged except for one sign: the book goes long where the
bottom-quintile (losers') tilt exceeds the top-quintile (skilled slow money)
tilt, i.e. signal = -(tilt_top - tilt_bottom). Cohorts, scores, universe,
3-day horizon, one-third daily rebalance, costs and the funding proxy are
001's, reused from `ct/crowd.py`, `ct/scores.py`, `p4_features.py`,
`p5_population.py` and `ct/leaders.py` without modification.

This module holds only what 008 adds:
- the holdout calendar: 001's selection grid continued past 2026-07-01
  (`grid_windows`), the snapshot filter (`signal_snapshots`) and the holdout
  fill days the lookbacks need (`holdout_fill_days`);
- 001's T3 data path re-expressed over 008's own price and funding stores
  (`Closes`, `Funding`, `signals_for`, `books`, `ic3`), proven equal to
  `p5_run.crowd_trial` + `finish_t3` on synthetic inputs (tests/test_r008.py);
- the cohort rule as 001 applied it (`score_t3`, `quintiles_by_score`);
- the factors of the confirm regression (Revision 1): BTC, an equal-weight alt index and a short-term
  reversal factor, all point-in-time from the same Bitget closes (`day_factors`), and the per-(window,
  month) BTC-slope cells (`beta_cells`);
- inference: week-clustered multi-regressor OLS (`ols_multi`), the existence test (`confirm`), the
  economic bar on the half-shrunk estimate (`economic`), `decide` and `recheck` (the decision recomputed
  from stored numbers);
- the synthetic power / false-confirm simulation (`simulate`).

Nothing here reads real data by itself; callers pass paths and stores. No
function computes anything from a holdout date unless the guarded runner
calls it.
"""
import bisect
import datetime as dt
import json
import math
import random
import statistics
from pathlib import Path

from . import crowd, schedule, scores

HOUR, DAY = 3_600_000, 86_400_000
DEX = 'hyperliquid'
HOLDOUT_START = dt.date(2026, 7, 1)
HOLDOUT_START_MS = 1_782_864_000_000            # 2026-07-01 00:00 UTC
B_COST = 0.0006 + 0.0005                        # 001 §8 / p5_run.B_COST: Bitget taker + flat 5 bp, one way
B_HORIZON_H = 72                                # 001 §8 / p5_run.B_HORIZON_H
HORIZON_D = B_HORIZON_H // 24
LISTED_MIN_DAYS = 31                            # 001 §8: listed >= 31 days
MIN_COHORT_HOLDERS = 3                          # 001 §8 / crowd.tilts default
TRIP_FLOOR = 4                                  # 001 D7 outcome (population_summary.json regime_trip_floor)
IC_MIN_PAIRS = 10                               # p5_run.crowd_trial
Q = 0.95                                        # one-sided existence test, one look
MIN_CLUSTERS = 6                                # fewer week clusters -> no test possible -> not demonstrated
BAR_ANNUAL = 0.03                               # HLP's lending leg, %/yr on gross capital (economic bar)
DAYS_PER_YEAR = 365
OFFERED_ENDS = {'2026-09-28': dt.date(2026, 9, 28), '2026-11-23': dt.date(2026, 11, 23)}
GIB = 1 << 30
CAP_BYTES = 15 * GIB                            # owner, 2026-10-12: hard cap on the requester-pays pull
REPORT_COSTS = (0.0, 0.0002, B_COST, 2 * B_COST)   # one-way cost sensitivity, reported only
SHRINK = 0.5                                    # economic bar on half the gross alpha (Revision 1, A-M2)
PERSIST_T = 2.18                                # reported only: 1.782 <= t < 2.18 is "persistence-sensitive"
SURV_SHARE = 0.10                               # survivorship reading rule (reported): group share in dropped coins
SURV_MIN_COINS, SURV_RATIO = 5, 2.0             # ... or >= 5 dropped coins ranked, long >= 2 x short coin-days
MIN_CELL_DAYS = 10                              # BTC-slope cells under 10 days merge into a neighbour
MIN_COHORT = 20                                 # a window whose cohort is under 20 wallets is excluded (A-m6)
MIN_MEDIAN_COINS = 10                           # ... or whose median signal breadth is under 10 coins
REV_MIN_NAMES = 4                               # reversal / momentum factor needs >= 4 names that day
LABELS = ('not_demonstrated', 'confirm_gross_only', 'confirm_gross_and_shrunk_net')


def read_bound(end):
    """The last instant any outcome needs: the last entry (< end - 72 h + 2 h) plus 72 h. Prices and funding
    at or after this bound are never fetched or loaded (Revision 1, B-m4)."""
    return ms(end) + 2 * HOUR


def ms(d):
    return schedule.ms(d)


# ------------------------------------------------------------------ calendar
def grid_windows(end):
    """001's selection grid (ct/schedule: first S = archive start + 91 d, step 56 d) continued, keeping
    every window whose hold [S, S + 56 d) overlaps the holdout and ends on or before `end` (an offered
    end is always a hold end: windows are dropped, never truncated). [(S, lookback start, hold end)]."""
    out = []
    s = schedule.ARCHIVE_START[DEX] + dt.timedelta(days=schedule.LOOKBACK_DAYS)
    while s + dt.timedelta(days=schedule.HOLD_DAYS) <= end:
        he = s + dt.timedelta(days=schedule.HOLD_DAYS)
        if he > HOLDOUT_START:
            out.append((s, s - dt.timedelta(days=schedule.LOOKBACK_DAYS), he))
        s += dt.timedelta(days=schedule.HOLD_DAYS)
    return out


def holdout_fill_days(end):
    """Fill days dated >= 2026-07-01 inside some window's lookback [S - 91 d, S): the only fills the
    requester-pays pull fetches."""
    days = set()
    for s, lb, _ in grid_windows(end):
        d = max(lb, HOLDOUT_START)
        while d < s:
            days.add(d)
            d += dt.timedelta(days=1)
    return sorted(days)


def lookback_fill_days(end):
    """Every fill day any window's lookback reads (discovery and holdout)."""
    days = set()
    for s, lb, _ in grid_windows(end):
        days.update(lb + dt.timedelta(days=i) for i in range((s - lb).days))
    return sorted(days)


def snapshot_ms_of(path):
    """The snapshot's own timestamp from its file name <block>_<ms>.parquet (P2)."""
    return int(Path(path).stem.split('_')[1])


def signal_snapshots(paths, end):
    """[(window index, path, t_s)] — 001's crowd_trial filter (sel_ms <= t_s < hold_end - 72 h) per grid
    window, AND t_s >= 2026-07-01 00:00 UTC: no snapshot before the holdout ever enters a signal."""
    out = []
    for i, (s, _, he) in enumerate(grid_windows(end)):
        lo, hi = ms(s), ms(he) - B_HORIZON_H * HOUR
        for p in paths:
            t = snapshot_ms_of(p)
            if t >= HOLDOUT_START_MS and lo <= t < hi:
                out.append((i, p, t))
    return sorted(out, key=lambda x: x[2])


def entry_of(t_s):
    """p5_run.crowd_trial: the first full hour >= snapshot + 1 h."""
    return t_s - t_s % HOUR + 2 * HOUR


def week_of(t_ms):
    y, w, _ = dt.datetime.fromtimestamp(t_ms / 1000, dt.UTC).isocalendar()
    return f'{y}-W{w:02d}'


def month_of(t_ms):
    return dt.datetime.fromtimestamp(t_ms / 1000, dt.UTC).strftime('%Y-%m')


# ------------------------------------------------------------------ stores (008's own pulls)
class Closes:
    """Bitget 1H candles {symbol: {open ts: close}}. Same semantics as p5_run.b_returns over
    bg.candle_1h: close_at(sym, h) is the close of the 1H candle that STARTS at h - 1 h (None if absent);
    ret(sym, h, span) = close_at(h + span) / close_at(h) - 1; vol is the stdev of the 30 trailing daily
    returns ending at h, with >= 20 present."""

    def __init__(self, closes):
        self.c = closes

    @classmethod
    def from_store(cls, root, max_ts=None):
        """Load 008's store; rows with open time >= max_ts are dropped (never parsed into the series)."""
        closes = {}
        for sym_dir in sorted(Path(root).iterdir()):
            if not sym_dir.is_dir():
                continue
            rows = {}
            for f in sorted(sym_dir.glob('*.json')):
                for r in json.loads(f.read_text()):
                    if max_ts is None or int(r[0]) < max_ts:
                        rows[int(r[0])] = float(r[4])
            closes[sym_dir.name] = rows
        return cls(closes)

    def close_at(self, sym, h):
        return self.c.get(sym, {}).get(h - HOUR)

    def ret(self, sym, h, span=DAY):
        a, b = self.close_at(sym, h), self.close_at(sym, h + span)
        return b / a - 1 if a and b else None

    def vol(self, sym, h):
        rs = [self.ret(sym, h - (k + 1) * DAY) for k in range(30)]
        rs = [r for r in rs if r is not None]
        return statistics.stdev(rs) if len(rs) >= 20 else None


class Funding:
    """Hyperliquid funding as the proxy (001 D4), {bitget symbol: [(t, rate)] sorted}. fund(s, h, span)
    = sum of rates with h <= t < h + span — p5_run's lambda, with bisect instead of a scan."""

    def __init__(self, series):
        self.s = {k: sorted(v) for k, v in series.items()}
        self.t = {k: [x[0] for x in v] for k, v in self.s.items()}
        self.cum = {}
        for k, v in self.s.items():
            acc, c = 0.0, [0.0]
            for _, r in v:
                acc += r
                c.append(acc)
            self.cum[k] = c

    @classmethod
    def from_store(cls, root, coin_to_sym, max_ts=None):
        out = {}
        for coin, sym in coin_to_sym.items():
            f = Path(root) / f'{coin}.json'
            if f.exists():
                out[sym] = [(int(t), float(r)) for t, r in json.loads(f.read_text())
                            if max_ts is None or int(t) < max_ts]
        return cls(out)

    def fund(self, s, h, span=DAY):
        ts = self.t.get(s)
        if not ts:
            return 0
        i, j = bisect.bisect_left(ts, h), bisect.bisect_left(ts, h + span)
        return self.cum[s][j] - self.cum[s][i]


# ------------------------------------------------------------------ the rule (001 §8, sign flipped)
def signals_for(snapshot_path, cohort, bottom, listed, coin_to_sym, entry):
    """001's per-snapshot signal {bitget symbol: tilt_top - tilt_bottom} exactly as p5_run.crowd_trial
    builds it (Bitget-mapped, listed >= 31 days before entry, >= 3 cohort holders)."""
    sig = {}
    for coin, v in crowd.tilts(snapshot_path, cohort, crowd=bottom, min_cohort_holders=MIN_COHORT_HOLDERS).items():
        sym = coin_to_sym.get(coin)
        if sym and listed.get(sym) and listed[sym] < entry - LISTED_MIN_DAYS * DAY:
            sig[sym] = v
    return dict(sorted(sig.items()))        # DuckDB's GROUP BY order is not stable: fix it for determinism


def invert(sig):
    """THE one change of 008: long where the losers' tilt exceeds the skilled tilt."""
    return {s: -v for s, v in sig.items()}


def book(days, closes, funding, cost_rate):
    """001's costed book (crowd.long_short, horizon 3, inverse-vol equal risk, funding proxy subtracted
    as in p5_run.finish_t3). days: [(entry, signal)] — pass INVERTED signals for 008."""
    return crowd.long_short(days, lambda s, h: (closes.ret(s, h) or 0) - funding.fund(s, h), closes.vol,
                            cost_rate, horizon=HORIZON_D)


def ic3(days, closes, funding):
    """Daily Spearman IC of the signal against the 3-day return net of funding (p5_run.crowd_trial):
    [(entry, ic)] for days with >= 10 pairs."""
    from . import stats
    out = []
    for entry, sig in days:
        pairs = [(v, r - funding.fund(s, entry, B_HORIZON_H * HOUR)) for s, v in sig.items()
                 for r in [closes.ret(s, entry, B_HORIZON_H * HOUR)] if r is not None]
        if len(pairs) >= IC_MIN_PAIRS:
            out.append((entry, stats.spearman([p[0] for p in pairs], [p[1] for p in pairs])))
    return out


def score_t3(lb_fills, equity_at_s, typical_lev, is_rising, floor=TRIP_FLOOR):
    """The cohort score exactly as p5_run.score_wallet decides membership: the cross-regime score, or
    None when it, the long-bias, the equity as-of S or the typical leverage is missing."""
    trips = scores.round_trips(lb_fills)
    sc = scores.cross_regime_score(trips, is_rising, min_trips=floor)
    bias = scores.long_bias(lb_fills)
    if sc is None or bias is None or not equity_at_s or not typical_lev:
        return None
    return sc


def quintiles_by_score(scored):
    """(top, bottom) address lists as p5_run.finish_copy_window: wallets in address order, stable-sorted
    by score, k = n // 5."""
    ranked = sorted(sorted(scored, key=lambda x: x[0]), key=lambda x: x[1])
    k = len(ranked) // 5
    return ([a for a, _ in ranked[-k:]], [a for a, _ in ranked[:k]]) if k else ([], [])


# ------------------------------------------------------------------ inference
def _betacf(a, b, x):
    tiny, qab, qap, qam = 1e-300, a + b, a + 1.0, a - 1.0
    c, d = 1.0, 1.0 - qab * x / qap
    d = 1.0 / (d if abs(d) > tiny else tiny)
    h = d
    for m in range(1, 300):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > tiny else tiny)
        c = 1.0 + aa / c
        c = c if abs(c) > tiny else tiny
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > tiny else tiny)
        c = 1.0 + aa / c
        c = c if abs(c) > tiny else tiny
        de = d * c
        h *= de
        if abs(de - 1.0) < 1e-15:
            break
    return h


def _ibeta(a, b, x):
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    lbt = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1 - x)
    if x < (a + 1) / (a + b + 2):
        return math.exp(lbt) * _betacf(a, b, x) / a
    return 1.0 - math.exp(lbt) * _betacf(b, a, 1 - x) / b


def t_cdf(t, df):
    x = df / (df + t * t)
    tail = 0.5 * _ibeta(df / 2, 0.5, x)
    return 1 - tail if t >= 0 else tail


def t_quantile(df, q):
    """One-sided Student-t quantile by bisection on the exact CDF; df < 1 -> inf (no test possible)."""
    if df < 1:
        return math.inf
    lo, hi = 0.0, 1000.0
    for _ in range(200):
        mid = (lo + hi) / 2
        if t_cdf(mid, df) < q:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def cluster_mean_t(values, clusters):
    """(mean, se, t, n, G): mean with CR1 week-clustered SE (ct.eventstudy.cluster_t's formula)."""
    n = len(values)
    if n < 3:
        return (None, None, None, n, 0)
    m = statistics.fmean(values)
    sums = {}
    for v, c in zip(values, clusters):
        sums[c] = sums.get(c, 0.0) + (v - m)
    g = len(sums)
    var = sum(x * x for x in sums.values()) / (n * n) * (g / (g - 1) if g > 1 else 1)
    se = math.sqrt(var) if var > 0 else None
    return (m, se, m / se if se else None, n, g)


# ------------------------------------------------------------------ factors of the confirm regression
def _ew_half_spread(names, sort_val, ret):
    """0.5 x mean(ret of the half with the LOWEST sort_val) - 0.5 x mean(ret of the half with the HIGHEST);
    the middle name of an odd count is left out. None under REV_MIN_NAMES names."""
    pairs = sorted((sort_val[s], s) for s in names)
    if len(pairs) < REV_MIN_NAMES:
        return None
    h = len(pairs) // 2
    low, high = [s for _, s in pairs[:h]], [s for _, s in pairs[-h:]]
    return 0.5 * statistics.fmean(ret[s] for s in low) - 0.5 * statistics.fmean(ret[s] for s in high)


def day_factors(days, closes):
    """{entry: {'btc', 'alt', 'rev', 'mom5', 'universe'}} for every book day, point-in-time.

    The day's universe is the union of the signal coins of the tranches live that day (this day's and
    the two previous signal days', as crowd.long_short ages them), BTCUSDT excluded, with a return over
    [entry, entry + 24 h). From the same Bitget closes as the book:
      btc   BTCUSDT's return over the same span;
      alt   the equal-weight mean return of the universe (an alt-market index);
      rev   short-term reversal: long the lower half / short the upper half of the universe by the
            return over [entry - 24 h, entry) (known at entry), 0.5 gross a side, equal weight;
      mom5  (reported only) long the upper / short the lower half by the return over [entry - 5 d, entry).
    rev / mom5 are 0.0 on a day with fewer than 4 usable names (counted by the caller)."""
    out, recent = {}, []
    for k, sig in days:
        recent = (recent + [set(sig)])[-HORIZON_D:]
        names = sorted(set().union(*recent) - {'BTCUSDT'})
        r = {s: closes.ret(s, k) for s in names}
        r = {s: v for s, v in r.items() if v is not None}
        prev = {s: closes.ret(s, k - DAY) for s in r}
        prev = {s: v for s, v in prev.items() if v is not None}
        p5 = {s: closes.ret(s, k - 5 * DAY, 5 * DAY) for s in r}
        p5 = {s: v for s, v in p5.items() if v is not None}
        rev = _ew_half_spread(list(prev), prev, r)
        mom = _ew_half_spread(list(p5), p5, r)
        out[k] = {'btc': closes.ret('BTCUSDT', k), 'alt': statistics.fmean(r.values()) if r else None,
                  'rev': rev if rev is not None else 0.0, 'mom5': -mom if mom is not None else 0.0,
                  'universe': len(r), 'rev_missing': rev is None}
    return out


def beta_cells(keys, window_of):
    """{entry: cell label}: one BTC slope per (grid window, calendar month) cell. Cells of fewer than
    MIN_CELL_DAYS book days merge into the preceding cell in time (into the following one if they come
    first), repeatedly, until every cell has >= MIN_CELL_DAYS days or one cell is left. For the
    2026-09-28 end the cells are (A, Jul), (B, Aug), (B, Sep): one slope per calendar month."""
    raw = [f'{window_of[k]}:{month_of(k)}' for k in sorted(keys)]
    order = list(dict.fromkeys(raw))
    label = {c: c for c in order}
    while True:
        groups = list(dict.fromkeys(label[c] for c in order))
        if len(groups) <= 1:
            break
        counts = {g: sum(1 for c in raw if label[c] == g) for g in groups}
        small = [g for g in groups if counts[g] < MIN_CELL_DAYS]
        if not small:
            break
        g = small[0]
        i = groups.index(g)
        into = groups[i - 1] if i > 0 else groups[i + 1]
        for c in order:
            if label[c] == g:
                label[c] = into
    return {k: label[f'{window_of[k]}:{month_of(k)}'] for k in keys}


# ------------------------------------------------------------------ OLS with cluster-robust variance
def _inv(m):
    n = len(m)
    a = [list(row) + [1.0 if i == j else 0.0 for j in range(n)] for i, row in enumerate(m)]
    scale = max(abs(x) for row in m for x in row) or 1.0
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(a[r][c]))
        if abs(a[p][c]) < 1e-12 * scale:
            return None
        a[c], a[p] = a[p], a[c]
        piv = a[c][c]
        a[c] = [x / piv for x in a[c]]
        for r in range(n):
            if r != c and a[r][c]:
                f = a[r][c]
                a[r] = [x - f * y for x, y in zip(a[r], a[c])]
    return [row[n:] for row in a]


def ols_multi(y, cols, clusters):
    """OLS y = a + sum_j b_j x_j with CR1 cluster-robust variance (G/(G-1) * (n-1)/(n-k)).
    cols: [[x_j for each obs]]. Returns {'coef', 'alpha', 'se_alpha', 't_alpha', 'resid', 'n', 'k', 'G'}
    or None (rank-deficient, n <= k, or fewer than 2 clusters)."""
    n = len(y)
    X = [[1.0] + [c[i] for c in cols] for i in range(n)]
    k = len(X[0]) if X else 0
    if n <= k:
        return None
    xtx = [[sum(X[i][a] * X[i][b] for i in range(n)) for b in range(k)] for a in range(k)]
    inv = _inv(xtx)
    if inv is None:
        return None
    xty = [sum(X[i][a] * y[i] for i in range(n)) for a in range(k)]
    coef = [sum(inv[a][b] * xty[b] for b in range(k)) for a in range(k)]
    resid = [y[i] - sum(coef[a] * X[i][a] for a in range(k)) for i in range(n)]
    g = {}
    for i, c in enumerate(clusters):
        u = g.setdefault(c, [0.0] * k)
        for a in range(k):
            u[a] += X[i][a] * resid[i]
    G = len(g)
    if G < 2:
        return None
    meat = [[sum(u[a] * u[b] for u in g.values()) for b in range(k)] for a in range(k)]
    adj = G / (G - 1) * (n - 1) / (n - k)
    tmp = [[sum(inv[a][m] * meat[m][b] for m in range(k)) for b in range(k)] for a in range(k)]
    v00 = adj * sum(tmp[0][m] * inv[m][0] for m in range(k))
    se = math.sqrt(v00) if v00 > 0 else None
    return {'coef': coef, 'alpha': coef[0], 'se_alpha': se, 't_alpha': coef[0] / se if se else None,
            'resid': resid, 'n': n, 'k': k, 'G': G}


def ols_cluster(y, x, clusters):
    """Single-regressor form (kept for the reported BTC-only alpha and the tests)."""
    fit = ols_multi(y, [x], clusters)
    if not fit:
        return None
    return {'alpha': fit['alpha'], 'beta': fit['coef'][1], 'se_alpha': fit['se_alpha'], 't_alpha': fit['t_alpha'],
            'n': fit['n'], 'G': fit['G']}


def design(keys, fac, cells, with_mom=False, per_cell=True):
    """Regressor columns and names. Revision 2 (review r1, B1): for EACH BTC cell c (window x month) the
    BTC return, the EW alt index and the reversal factor each get their own loading, so k = 1 + 3 x cells
    (10 at 2026-09-28, 16 at 2026-11-23); 5-day momentum (reported only) likewise. per_cell=False is
    Revision 1's design (alt and reversal pooled), kept for the comparison in §3."""
    groups = list(dict.fromkeys(cells[k] for k in keys))
    cols, names = [], []
    factors = [('btc', 'btc'), ('alt', 'alt_ew'), ('rev', 'rev_1d')] + ([('mom5', 'mom_5d')] if with_mom else [])
    for f, label in factors:
        if f == 'btc' or per_cell:
            for g in groups:
                cols.append([fac[k][f] if cells[k] == g else 0.0 for k in keys])
                names.append(f'{label}[{g}]')
        else:
            cols.append([fac[k][f] for k in keys])
            names.append(label)
    return cols, names


def confirm(gross, keys, fac, cells):
    """The existence test (registration §5). gross: daily book returns before trading costs (funding
    included), keyed by entry; fac: day_factors; cells: beta_cells.
        gross_t = alpha + sum_c (b_c BTC_t + g_c ALT_t + d_c REV_t) 1[cell(t) = c] + e_t   (one common alpha)
    The factor-adjusted daily return is a_t = alpha_hat + e_hat_t = gross_t - (fitted factor part); its
    mean is alpha_hat. CONFIRM iff all of
      C1  alpha_hat > 0 with week-clustered (CR1) t >= t_{G-1}(0.95), G >= 6 ISO-week clusters;
      C2  the median of a_t > 0;
      C3  both halves of a_t (by day count) have a mean > 0.
    Returns (passed, numbers) — numbers are what `recheck` recomputes from."""
    cols, names = design(keys, fac, cells)
    fit = ols_multi(gross, cols, [week_of(k) for k in keys])
    num = {'n': len(gross), 'G': fit['G'] if fit else len({week_of(k) for k in keys}), 'k': fit['k'] if fit else None,
           'alpha': fit and fit['alpha'], 't_alpha': fit and fit['t_alpha'], 'se_alpha': fit and fit['se_alpha'],
           'coef': dict(zip(['alpha'] + names, fit['coef'])) if fit else None, 'cells': sorted(set(cells.values()))}
    if not fit:
        return False, {**num, 'crit': None, 'median_adj': None, 'halves_adj': None,
                       'criteria': {'C1': False, 'C2': False, 'C3': False}}
    adj = [fit['alpha'] + e for e in fit['resid']]
    half = len(adj) // 2
    num['crit'] = t_quantile(fit['G'] - 1, Q) if fit['G'] >= MIN_CLUSTERS else None
    num['median_adj'] = statistics.median(adj)
    num['halves_adj'] = [statistics.fmean(adj[:half]), statistics.fmean(adj[half:])] if half else None
    num['criteria'] = criteria_from(num)
    return all(num['criteria'].values()), num


def criteria_from(num):
    t, crit = num.get('t_alpha'), num.get('crit')
    halves = num.get('halves_adj')
    return {'C1': (num.get('G') or 0) >= MIN_CLUSTERS and t is not None and crit is not None
            and num['alpha'] is not None and num['alpha'] > 0 and t >= crit,
            'C2': num.get('median_adj') is not None and num['median_adj'] > 0,
            'C3': bool(halves) and all(h is not None and h > 0 for h in halves)}


def economic(alpha_gross, mean_cost):
    """The bar applied AFTER a confirm (Revision 1, A-M2): HALF the factor-adjusted gross alpha, minus the
    book's mean daily cost at 001's costs, x 365, >= 3 %/yr on gross capital. The point estimate is
    reported beside it, never decisive."""
    if alpha_gross is None or mean_cost is None:
        return {'alpha_gross': alpha_gross, 'mean_cost_per_day': mean_cost, 'shrunk_net_annualised': None,
                'point_net_annualised': None, 'bar': BAR_ANNUAL, 'meets_bar': False}
    shrunk = (SHRINK * alpha_gross - mean_cost) * DAYS_PER_YEAR
    return {'alpha_gross': alpha_gross, 'mean_cost_per_day': mean_cost, 'shrunk_net_annualised': shrunk,
            'point_net_annualised': (alpha_gross - mean_cost) * DAYS_PER_YEAR, 'bar': BAR_ANNUAL,
            'meets_bar': shrunk >= BAR_ANNUAL}


def decide(confirmed, econ):
    if not confirmed:
        return 'not_demonstrated'
    return 'confirm_gross_and_shrunk_net' if econ['meets_bar'] else 'confirm_gross_only'


def survivorship_flag(surv):
    """The §5 reading rule (reported; review r1 m4), from the per-window survivorship numbers: flagged if in
    any window the cohort's or the crowd's gross-notional share in coins dropped only for table status is
    >= 10 %, or at least 5 distinct dropped coins would have ranked in a long or short quintile and their
    long-quintile coin-days are >= 2 x their short-quintile coin-days."""
    why = []
    for w, v in sorted((surv or {}).items()):
        for g in ('cohort_gross_share_in_dropped', 'crowd_gross_share_in_dropped'):
            if v.get(g) is not None and v[g] >= SURV_SHARE:
                why.append(f'{w}: {g} >= {SURV_SHARE}')
        place = v.get('dropped_would_rank') or {}
        if (v.get('dropped_coins_ranked') or 0) >= SURV_MIN_COINS and \
                place.get('long', 0) >= SURV_RATIO * place.get('short', 0) and place.get('long', 0) > 0:
            why.append(f'{w}: >= {SURV_MIN_COINS} dropped coins ranked, long >= {SURV_RATIO} x short')
    return {'flag': bool(why), 'why': why}


def recheck(row):
    """(decision, failures) recomputed from a stored holdout row's numbers — never from its labels."""
    fails = []
    c = row.get('confirm') or {}
    e = row.get('economic') or {}
    try:
        g = int(c.get('G') or 0)
        crit = t_quantile(g - 1, Q) if g >= MIN_CLUSTERS else None
        if (crit is None) != (c.get('crit') is None) or (crit is not None and abs(crit - c['crit']) > 1e-9):
            fails.append('stored critical value differs from t_{G-1}(0.95)')
        crits = criteria_from({**c, 'crit': crit})
        if crits != c.get('criteria'):
            fails.append('stored criteria differ from the recomputed ones')
        if e.get('alpha_gross') != c.get('alpha'):
            fails.append('the economic alpha is not the confirm alpha')
        econ = economic(c.get('alpha'), e.get('mean_cost_per_day'))
        if bool(e.get('meets_bar')) != econ['meets_bar']:
            fails.append('stored economic bar differs from the recomputed one')
        dec = decide(all(crits.values()), econ)
    except Exception as err:                       # a malformed row decides nothing
        return None, [f'malformed row: {type(err).__name__}']
    if row.get('decision') != dec:
        fails.append('stored decision differs from the recomputed one')
    if 'survivorship_reading' in row:
        sr = survivorship_flag((row.get('reported') or {}).get('survivorship'))
        if sr != row['survivorship_reading']:
            fails.append('stored survivorship reading differs from the recomputed one')
    return dec, fails


# ------------------------------------------------------------------ power (synthetic only)
SIGMA_DAY = 0.001475 / 3.4159384 * math.sqrt(164)     # 001 §12: NW se x sqrt(164 days) = 0.553 %/day
DISC_GROSS_INV = 0.0014752161554898798 - 0.000728862  # 001 net mean flipped, minus the measured book cost
COST_DAY = 0.000728862                               # crowd.long_short at B_COST over a 164-day run


def planned_days(end, first_hour=2):
    """Synthetic [(entry, window index)]: one snapshot per calendar day at 00:00 UTC (entry 02:00) inside
    each window's signal range from 2026-07-01 — the day pattern of `signal_snapshots`, without files."""
    out = []
    for i, (s, _, he) in enumerate(grid_windows(end)):
        d = max(s, HOLDOUT_START)
        while ms(d) < ms(he) - B_HORIZON_H * HOUR:
            out.append((ms(d) + first_hour * HOUR, i))
            d += dt.timedelta(days=1)
    return out


def planned_keys(end, first_hour=2):
    return [k for k, _ in planned_days(end, first_hour)]


def _z(rng, tails):
    if tails:
        return rng.gauss(0, 1) / math.sqrt(sum(rng.gauss(0, 1) ** 2 for _ in range(4)) / 4) / math.sqrt(2)
    return rng.gauss(0, 1)


def _ar(rng, n, rho, tails):
    out, prev, sc = [], 0.0, math.sqrt(1 - rho * rho)
    for _ in range(n):
        prev = rho * prev + sc * _z(rng, tails)
        out.append(prev)
    return out


def _val(x, k, wi):
    return x(k, wi) if callable(x) else x


def simulate(days, mu, sims=2000, seed=0, sigma=SIGMA_DAY, rho=0.0, tails=False, beta=0.3, btc_drift=0.0,
             btc_sd=0.025, alt_load=0.0, alt_drift=0.0, alt_sd=0.015, alt_btc=1.1, rev_load=0.0, rev_drift=0.0,
             rev_sd=0.008, slow_share=0.0, slow_rho=0.9, cost=COST_DAY, btc_t=None, hetero=False):
    """Rates over synthetic holdouts on the planned [(entry, window)] days. Per day:
        BTC = btc_drift + btc_sd x z (z Gaussian, or Student-t(btc_t) scaled to unit variance);
        U (alt minus BTC) ~ N(alt_drift, alt_sd); ALT = alt_btc*BTC + U; REV ~ N(rev_drift, rev_sd);
        y = mu + beta*BTC + alt_load*U + rev_load*REV + e,
    e = sigma x [sqrt(1-s) AR(1, rho) x h + sqrt(s) AR(1, slow_rho)] (Gaussian or t4 innovations), with
    h = sqrt(0.5 + 0.5 z^2) when hetero (residual SD rising with |BTC| shock), else 1.
    beta, btc_drift, alt_load, alt_drift, rev_load and rev_drift may be callables (entry, window) -> value.
    Returns the registered rule's confirm rate (Revision 2: per-cell BTC, alt and reversal loadings);
    Revision 1's rule (pooled alt and reversal loadings) and Revision 0's (one BTC slope); the raw-mean rule;
    P(confirm and the half-shrunk bar)."""
    rng = random.Random(seed)
    keys = [k for k, _ in days]
    wins = {k: w for k, w in days}
    wk = [week_of(k) for k in keys]
    cells = beta_cells(keys, wins)
    n = len(keys)
    half = n // 2
    G = len(set(wk))
    crit = t_quantile(G - 1, Q) if G >= MIN_CLUSTERS else math.inf
    hits = r1_hits = old_hits = raw_hits = bar = 0
    s = slow_share

    def shock():
        if btc_t:
            z = rng.gauss(0, 1) / math.sqrt(sum(rng.gauss(0, 1) ** 2 for _ in range(btc_t)) / btc_t)
            return z * math.sqrt((btc_t - 2) / btc_t)
        return rng.gauss(0, 1)

    def passes(fit):
        if not fit or fit['t_alpha'] is None or fit['alpha'] <= 0 or fit['t_alpha'] < crit:
            return False
        adj = [fit['alpha'] + e for e in fit['resid']]
        return statistics.median(adj) > 0 and statistics.fmean(adj[:half]) > 0 and statistics.fmean(adj[half:]) > 0

    for _ in range(sims):
        z = [shock() for _ in days]
        btc = [_val(btc_drift, k, w) + btc_sd * zi for (k, w), zi in zip(days, z)]
        u = [rng.gauss(_val(alt_drift, k, w), alt_sd) for k, w in days]
        rv = [rng.gauss(_val(rev_drift, k, w), rev_sd) for k, w in days]
        fast = _ar(rng, n, rho, tails)
        slow = _ar(rng, n, slow_rho, tails) if s else [0.0] * n
        y = [mu + _val(beta, k, w) * b + _val(alt_load, k, w) * ui + _val(rev_load, k, w) * r
             + sigma * (math.sqrt(1 - s) * f * (math.sqrt(0.5 + 0.5 * zi * zi) if hetero else 1.0) + math.sqrt(s) * sl)
             for (k, w), b, ui, r, f, sl, zi in zip(days, btc, u, rv, fast, slow, z)]
        fac = {k: {'btc': b, 'alt': alt_btc * b + ui, 'rev': r} for k, b, ui, r in zip(keys, btc, u, rv)}
        ok, num = confirm(y, keys, fac, cells)
        hits += ok
        if ok:
            bar += economic(num['alpha'], cost)['meets_bar']
        cols1, _ = design(keys, fac, cells, per_cell=False)
        r1_hits += passes(ols_multi(y, cols1, wk))
        old_hits += passes(ols_multi(y, [btc], wk))
        m, _, t, _, _ = cluster_mean_t(y, wk)
        raw_hits += (t is not None and t >= crit and statistics.median(y) > 0
                     and statistics.fmean(y[:half]) > 0 and statistics.fmean(y[half:]) > 0)
    return {'confirm': hits / sims, 'rev1_rule_pooled_alt_rev': r1_hits / sims, 'draft_rule_btc_only': old_hits / sims,
            'raw_rule': raw_hits / sims, 'confirm_and_bar': bar / sims, 'n_days': n, 'G': G,
            'cells': len(set(cells.values())), 'sims': sims}
