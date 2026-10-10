"""Post-cascade drift — registration 005 (H4) mechanics. Pure functions; the tape, the
liquidation stream and the exit prices come in as arguments.

The trade: after a qualifying cascade (002 'primary') has ENDED — its last liquidation print
t_end followed by a full minute without one in the coin — enter AGAINST the forced flow as a
taker at the first tape print at or after t_end + 60 s + 2 s, hold 120 min, exit as a taker at
the tape's last trade at or before fill + 120 min. Longs liquidated -> buy; shorts -> sell.

Pieces, in the order the runner applies them:
  entry_time        t_end + QUIET + LATENCY (never before the flow has ended);
  StressRate        exchange-wide trailing-5-min liquidation notional (004 D8 switch), its
                    maximum over [first print, entry] — primary EXCLUDING the cascade coin's own
                    liquidations (registration D5);
  plan              warm-up (cross-coin fallback median), holdout edge, stress switch, missing
                    entry print, one position per coin;
  net_return        taker both sides, adverse slippage both sides;
  summarise         the six criteria (hour- and day-clustered), placebo, splits;
  decide            the proposed decision table; holdout_confirm: the holdout read (D14);
  beta / hedged     the reported BTC-hedged variant for ETH and SOL;
  null_se / mde     minimum detectable mean from unconditional volatility and entry timings.

Nothing here reads a price after an entry; the exit prices are an input that only the guarded
runner supplies.
"""
import bisect
import math
import random
import statistics

from . import eventstudy as es
from .provider import stress as stress_switch

SECOND, MINUTE, HOUR, DAY = 1_000, 60_000, 3_600_000, 86_400_000
TAKER = 0.00045                          # Hyperliquid base-tier taker, each side
SLIP = 0.0005                            # 5 bp adverse at entry and at exit; 0 and 10 bp reported
SLIPS = (0.0, 0.0005, 0.001)
QUIET = 60 * SECOND                      # the cascade gap G: a minute without a print closes the run
LATENCY = 2 * SECOND
ENTRY_WINDOW = 60 * SECOND               # no tape print within this after the entry time -> no trade
HOLD = 120 * MINUTE
HORIZONS = {'60m': 60 * MINUTE, '120m': 120 * MINUTE, '240m': 240 * MINUTE}
PRIMARY = '120m'
STRESS_WINDOW, STRESS_THRESHOLD = 5 * MINUTE, 77_000_000.0   # 004 D8 (p99.9)
HOLDOUT_START = 1_782_864_000_000        # 2026-07-01 00:00 UTC: nothing at or after is read
Z_HOUR, Z_DAY = 2.33, 1.96
Z_HOLDOUT = 1.64                         # one-sided 5%, one look (D14)
BETA_DAYS, BETA_MIN_OBS = 30, 24 * 20    # hedge beta: trailing 30 days of 1H closes before the entry
BAR = 0.0025                             # the H7 bar: >= 25 bp net per major event

# the event fields the trade may use; every one is known at t_end (see registration §2)
ENTRY_FIELDS = ('coin', 'side', 't_start', 't_end', 'fallback_median')


def direction(side):
    """+1 = buy (longs were liquidated, price fell), -1 = sell (shorts were liquidated)."""
    return 1.0 if side == 'L' else -1.0


def entry_time(event):
    """The earliest moment the run is known to be over, plus latency. Never before t_end + G."""
    return event['t_end'] + QUIET + LATENCY


def net_return(side, entry_px, exit_px, slip=SLIP, taker=TAKER):
    """Net return of the against-the-flow taker round trip; None without both prices.
    Entry adverse by `slip` (pay up), exit adverse by `slip`, taker fee on both sides."""
    if not entry_px or not exit_px:
        return None
    s = direction(side)
    e = entry_px * (1 + s * slip)
    x = exit_px * (1 - s * slip)
    return s * (x / e - 1) - 2 * taker


def round_trip_cost(slip=SLIP, taker=TAKER):
    """Cost of a flat round trip in return units (entry = exit price)."""
    return -net_return('L', 100.0, 100.0, slip, taker)


class StressRate:
    """Exchange-wide liquidation notional in the trailing window, from one-second buckets
    (the 004 construction: bucket = floor(t / 1 s), rate(t) = sum of buckets in (t - W, t])."""

    def __init__(self, buckets, window=STRESS_WINDOW):
        """buckets: [(second_ms, notional)] sorted by time, all coins."""
        self.t = [s for s, _ in buckets]
        self.cum = [0.0]
        for _, n in buckets:
            self.cum.append(self.cum[-1] + n)
        self.window = window

    def rate(self, t):
        hi = bisect.bisect_right(self.t, t)
        lo = bisect.bisect_right(self.t, t - self.window)
        return self.cum[hi] - self.cum[lo]

    def max_between(self, a, b, exclude=None):
        """Maximum of rate(t) - exclude.rate(t) over every t in [a, b] (exclude: a StressRate of
        a subset of the buckets, e.g. the cascade coin's own, or None). The difference only rises
        at a bucket of the remainder, which is a bucket of this series, so the maximum is at a or
        at a bucket inside (a, b]."""
        f = (lambda t: self.rate(t) - exclude.rate(t)) if exclude is not None else self.rate
        best = f(a)
        i = bisect.bisect_right(self.t, a)
        j = bisect.bisect_right(self.t, b)
        for k in range(i, j):
            best = max(best, f(self.t[k]))
        return best


def stress_window(event):
    """[first print, entry - 1 s]: a one-second bucket labelled s holds prints in [s, s + 1 s), so
    evaluating up to t_e - 1 s sees only buckets complete before the entry — no print at or after
    t_e (the last partial second before t_e is left out: conservative)."""
    return event['t_start'], entry_time(event) - SECOND


def stressed(event, rate, threshold=STRESS_THRESHOLD, own=None):
    """004 D8 switch over the decision window (stress_window). own: the cascade coin's own
    StressRate, excluded from the market series (primary, D5); None = the inclusive variant."""
    a, b = stress_window(event)
    return stress_switch(rate.max_between(a, max(a, b), exclude=own), threshold)


def plan(events, rate, entry_prints, majors, own_rates=None, drop_fallback=True, hold=HOLD,
         max_horizon=max(HORIZONS.values()), threshold=STRESS_THRESHOLD, cutoff=HOLDOUT_START):
    """The positions the rule would have opened. Behaviour only: times and entry prices.

    events: cascades (only ENTRY_FIELDS are read); rate: StressRate of all coins; own_rates:
    {coin: StressRate of that coin's own liquidations} — excluded from the stress series for the
    cascade's coin (primary, D5); None = inclusive variant. entry_prints: {event index:
    (t_fill, price)} = first tape print in [entry_time, entry_time + ENTRY_WINDOW].
    Filters, in order: warm-up (qualified on the cross-coin fallback median; primary, D1 —
    drop_fallback=False is the inclusive 002 set), holdout edge (any reported exit at or after
    the cutoff), stress switch, no entry print, one position per coin (skipped while an earlier
    position in the coin is held, i.e. entry_time < previous fill + hold). Skipped events hold
    nothing. Returns (positions, skipped) with skipped = {reason: [event index]}.
    """
    skipped = {'warmup_fallback': [], 'holdout_edge': [], 'stress': [], 'no_entry_print': [], 'overlap': []}
    order = sorted(range(len(events)), key=lambda i: (events[i]['coin'], entry_time(events[i]), i))
    held_until = {}
    out = []
    for i in order:
        e = {k: events[i].get(k) for k in ENTRY_FIELDS}
        te = entry_time(e)
        if drop_fallback and e['fallback_median']:
            skipped['warmup_fallback'].append(i)
            continue
        if te + ENTRY_WINDOW + max_horizon >= cutoff:
            skipped['holdout_edge'].append(i)
            continue
        own = own_rates.get(e['coin']) if own_rates is not None else None
        if own_rates is not None and own is None:
            own = StressRate([])
        if stressed(e, rate, threshold, own):
            skipped['stress'].append(i)
            continue
        fill = entry_prints.get(i)
        if not fill or fill[0] < te or fill[0] > te + ENTRY_WINDOW or not fill[1]:
            skipped['no_entry_print'].append(i)
            continue
        if te < held_until.get(e['coin'], -1):
            skipped['overlap'].append(i)
            continue
        t_fill, px = fill
        held_until[e['coin']] = t_fill + hold
        out.append({'i': i, 'coin': e['coin'], 'side': e['side'], 't_start': e['t_start'], 't_end': e['t_end'],
                    't_entry': te, 'entry': t_fill, 'entry_px': px, 'major': e['coin'] in majors,
                    'hour': t_fill // HOUR, 'day': t_fill // DAY,
                    'exit_t': {h: t_fill + ms for h, ms in HORIZONS.items()}})
    out.sort(key=lambda p: (p['entry'], p['coin']))
    return out, skipped


def exit_queries(positions):
    """[(key, coin, t)] for the tape's last trade at or before each horizon. Only the guarded
    runner calls this; every t is before the holdout by plan()'s edge filter."""
    q = []
    for p in positions:
        for h, t in p['exit_t'].items():
            assert t < HOLDOUT_START
            q.append((f"{p['i']}:{h}", p['coin'], t))
    return q


def outcomes(positions, exit_prices):
    """Attach net returns per horizon and per exit slippage, and the GROSS return (no fee, no
    slippage) the placebo flips. exit_prices: {f'{i}:{h}': price}."""
    rows = []
    for p in positions:
        r = dict(p, ret={}, ret_slip={}, gross={})
        for h in HORIZONS:
            px = exit_prices.get(f"{p['i']}:{h}")
            r['ret'][h] = net_return(p['side'], p['entry_px'], px, SLIP)
            r['gross'][h] = net_return(p['side'], p['entry_px'], px, 0.0, 0.0)
            r['ret_slip'][h] = {str(s): net_return(p['side'], p['entry_px'], px, s) for s in SLIPS}
        rows.append(r)
    return rows


def _block(rows, key='hour', h=PRIMARY, val=None):
    v = [(val(r) if val else r['ret'].get(h)) for r in rows]
    m, se, t, n, g = es.cluster_t(v, [r[key] for r in rows])
    vv = [x for x in v if x is not None]
    return {'mean': m, 'se': se, 't': t, 'n': n, 'clusters': g,
            'median': statistics.median(vv) if vv else None,
            'hit': (sum(1 for x in vv if x > 0) / len(vv)) if vv else None}


def _cluster_sums(rows, key, h):
    sums = {}
    for r in rows:
        x = r['ret'].get(h)
        if x is not None:
            sums[r[key]] = sums.get(r[key], 0.0) + x
    return sums


def largest_cluster(rows, key='hour', h=PRIMARY, how='positive'):
    """The cluster to remove for criterion 6. how='positive' (the criterion, D9): the cluster with
    the largest POSITIVE summed net return — the one most favourable to the hypothesis; removing a
    large loss could only help a pass, so it is never the one removed. None if no cluster sums
    above zero. how='abs': largest absolute sum; how='count': most entries (both reported; in
    majors one position per coin caps an hour at three entries, so 'count' is mostly a tie-break).
    Ties go to the earliest cluster."""
    sums = _cluster_sums(rows, key, h)
    if not sums:
        return None
    if how == 'positive':
        pos = {c: v for c, v in sums.items() if v > 0}
        return min(pos, key=lambda c: (-pos[c], c)) if pos else None
    if how == 'abs':
        return min(sums, key=lambda c: (-abs(sums[c]), c))
    counts = {}
    for r in rows:
        counts[r[key]] = counts.get(r[key], 0) + 1
    return min(counts, key=lambda c: (-counts[c], c))


def _without(rows, cluster, key='hour'):
    ex = [r for r in rows if r[key] != cluster]
    return _block(ex) if ex else None


def placebo(rows, flip_key='hour', seeds=200, z=Z_HOUR, h=PRIMARY):
    """Sign-flip placebo on the GROSS return, costs kept: per seed, every cluster of `flip_key`
    (all its positions together) gets one random sign; net' = sign x gross - cost, cost = gross -
    net. The statistic is the trial's own (hour-clustered t). Returns the share of seeds that would
    pass criterion 1 (mean > 0, t >= z) and, for a symmetric check, the share with |t| >= z on the
    flipped gross alone (no costs). Rows without 'gross' are treated as cost-free (gross = net)."""
    use = [r for r in rows if r['ret'].get(h) is not None]
    if not use or not seeds:
        return {'pass_share': None, 'gross_abs_share': None, 'seeds': seeds, 'flip': flip_key}
    g = [(r.get('gross') or r['ret'])[h] for r in use]
    c = [gi - r['ret'][h] for gi, r in zip(g, use)]
    hours = [r['hour'] for r in use]
    keys = [r[flip_key] for r in use]
    passes = abs_hits = 0
    for s_ in range(seeds):
        rng = random.Random(s_)
        sign = {}
        for k in keys:
            if k not in sign:
                sign[k] = rng.choice((-1.0, 1.0))
        m, _, t, _, _ = es.cluster_t([sign[k] * gi - ci for k, gi, ci in zip(keys, g, c)], hours)
        passes += t is not None and m > 0 and t >= z
        _, _, t2, _, _ = es.cluster_t([sign[k] * gi for k, gi in zip(keys, g)], hours)
        abs_hits += t2 is not None and abs(t2) >= z
    return {'pass_share': passes / seeds, 'gross_abs_share': abs_hits / seeds, 'seeds': seeds, 'flip': flip_key}


def summarise(rows, placebo_seeds=200, z_hour=Z_HOUR, z_day=Z_DAY):
    """The six proposed criteria on one universe, plus what is reported with them.
    rows: outcomes() rows (entry, hour, day, side, ret)."""
    rows = [r for r in rows if r['ret'].get(PRIMARY) is not None]
    main = _block(rows, 'hour')
    day = _block(rows, 'day')
    srt = sorted(rows, key=lambda r: (r['entry'], r['coin']))
    half = len(srt) // 2
    halves = [_block(srt[:half])['mean'], _block(srt[half:])['mean']] if half >= 3 else [None, None]
    sides = {s: _block([r for r in rows if r['side'] == s]) for s in ('L', 'S')}
    big = largest_cluster(rows, how='positive')
    without = _without(rows, big)
    crit = {
        '1_mean_pos_hour_t': main['t'] is not None and main['mean'] > 0 and main['t'] >= z_hour,
        '2_day_t': day['t'] is not None and day['mean'] > 0 and day['t'] >= z_day,
        '3_mean_median_same_sign': main['median'] is not None and main['mean'] is not None
                                   and (main['mean'] > 0) == (main['median'] > 0),
        '4_both_halves_positive': all(x is not None and x > 0 for x in halves),
        '5_both_sides_positive': all(sides[s]['mean'] is not None and sides[s]['mean'] > 0 for s in ('L', 'S')),
        '6_without_largest_positive_hour_positive': without is not None and without['mean'] is not None
                                                    and without['mean'] > 0,
    }
    return {'primary_hour': main, 'primary_day': day, 'criteria': crit, 'pass': all(crit.values()),
            'halves': halves, 'by_side': sides, 'largest_positive_hour': big, 'without_largest_positive_hour': without,
            'without_largest_abs_hour': _without(rows, largest_cluster(rows, how='abs')),
            'without_largest_count_hour': _without(rows, largest_cluster(rows, how='count')),
            'placebo_hour_flip': placebo(rows, 'hour', placebo_seeds, z_hour),
            'placebo_day_flip': placebo(rows, 'day', placebo_seeds, z_hour),
            'by_horizon': {h: {'hour': _block(rows, 'hour', h), 'day': _block(rows, 'day', h)} for h in HORIZONS},
            'by_slip': {str(s): _block(rows, 'hour', val=lambda r, s=s: r['ret_slip'].get(PRIMARY, {}).get(str(s)))
                        for s in SLIPS} if rows and 'ret_slip' in rows[0] else None}


def decide(summary):
    """The registered decision table (registration §9, D11-D13): a discovery pass -> 'pull_holdout'
    (the owner approves the pull with a byte count first); otherwise 'close'. The H7 bar (BAR) is
    not a gate here: it is applied in the paper period that a holdout CONFIRM would lead to."""
    return 'pull_holdout' if summary['pass'] else 'close'


def holdout_confirm(rows, z=Z_HOLDOUT):
    """The holdout read (D14), on majors positions built by the same rules: CONFIRM if all of
    mean net > 0 with one-sided hour-clustered t >= 1.64; median > 0; mean >= 0 without the
    largest-positive hour. Returns (confirm?, details)."""
    rows = [r for r in rows if r['ret'].get(PRIMARY) is not None]
    main = _block(rows, 'hour')
    without = _without(rows, largest_cluster(rows, how='positive'))
    crit = {'mean_pos_hour_t': main['t'] is not None and main['mean'] > 0 and main['t'] >= z,
            'median_pos': main['median'] is not None and main['median'] > 0,
            'without_largest_positive_hour_nonneg': without is not None and without['mean'] is not None
                                                    and without['mean'] >= 0}
    return all(crit.values()), {'primary_hour': main, 'criteria': crit, 'without_largest_positive_hour': without}


def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def power_at(mu, se_cluster, se_true, z):
    """P(mean / se_cluster >= z) when the true mean is mu and the estimate's SD is se_true."""
    return norm_cdf((mu - z * se_cluster) / se_true)


def beta(coin_closes, btc_closes, t, days=BETA_DAYS, min_obs=BETA_MIN_OBS):
    """OLS beta of the coin's 1H log returns on BTC's, from candles CLOSED before t (open time
    + 1 h <= t) within the trailing `days`. Closes: {open_ts: close}. None with too few hours."""
    lo = t - days * DAY
    hours = sorted(k for k in coin_closes if lo <= k and k + HOUR <= t and k in btc_closes)
    xs, ys = [], []
    for a, b in zip(hours, hours[1:]):
        if b - a == HOUR:
            xs.append(math.log(btc_closes[b] / btc_closes[a]))
            ys.append(math.log(coin_closes[b] / coin_closes[a]))
    if len(xs) < min_obs:
        return None
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    vx = sum((x - mx) ** 2 for x in xs)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / vx if vx else None


def hedged_return(side, entry_px, exit_px, btc_entry, btc_exit, b, slip=SLIP, taker=TAKER):
    """Reported variant: the coin trade plus an opposite BTC position of b x the notional, both
    legs taker with adverse slippage. None without every price or a beta."""
    leg = net_return(side, entry_px, exit_px, slip, taker)
    hedge = net_return('S' if side == 'L' else 'L', btc_entry, btc_exit, slip, taker)
    if leg is None or hedge is None or b is None:
        return None
    return leg + b * hedge


def null_se(positions, sigma, rho, hold=HOLD, key=None):
    """Standard error of the mean return under a driftless random walk, from entry timings only.
    sigma: {coin: sd of the hold-length log return}; rho: {(coin_a, coin_b): correlation}.
    cov(i, j) = rho * sigma_i * sigma_j * overlap(i, j) / hold. key=None: all pairs (the true SE);
    key='hour'/'day': only pairs inside a cluster (what the clustered estimator expects to see)."""
    n = len(positions)
    if n == 0:
        return None
    ps = sorted(positions, key=lambda p: p['entry'])
    tot = 0.0
    for a in range(n):
        pa = ps[a]
        tot += sigma[pa['coin']] ** 2
        for b in range(a + 1, n):
            pb = ps[b]
            if pb['entry'] >= pa['entry'] + hold:
                break                                   # sorted: no later position overlaps pa
            if key is not None and pa[key] != pb[key]:
                continue
            ov = pa['entry'] + hold - pb['entry']
            r = 1.0 if pa['coin'] == pb['coin'] else rho.get((pa['coin'], pb['coin']), rho.get((pb['coin'], pa['coin']), 0.0))
            tot += 2 * r * sigma[pa['coin']] * sigma[pb['coin']] * ov / hold
    return math.sqrt(tot) / n


def mde(se_cluster, se_true, z_crit, power_z=0.8416):
    """Mean needed for P(t >= z_crit) = 80% (z 0.84): z_crit * se_cluster + power_z * se_true;
    at 50% power it is z_crit * se_cluster."""
    return {'p50': z_crit * se_cluster, 'p80': z_crit * se_cluster + power_z * se_true}


def null_cov(positions, sigma, rho, hold=HOLD, signed=True):
    """Covariance of the positions' returns under a driftless random walk (the null_se model),
    signed by direction when signed=True (a buy and a sell in correlated coins move oppositely).
    Returns (order, rows) with rows[a] = {b: cov} for b <= a (lower triangle, sparse)."""
    ps = sorted(range(len(positions)), key=lambda k: positions[k]['entry'])
    low = [dict() for _ in ps]
    for a in range(len(ps)):
        pa = positions[ps[a]]
        low[a][a] = sigma[pa['coin']] ** 2
        for b in range(a - 1, -1, -1):
            pb = positions[ps[b]]
            if pa['entry'] >= pb['entry'] + hold:
                break                       # equal-length holds sorted by entry: no earlier overlap
            ov = pb['entry'] + hold - pa['entry']
            r = 1.0 if pa['coin'] == pb['coin'] else rho.get((pa['coin'], pb['coin']), rho.get((pb['coin'], pa['coin']), 0.0))
            v = r * sigma[pa['coin']] * sigma[pb['coin']] * ov / hold
            if signed:
                v *= direction(pa['side']) * direction(pb['side'])
            low[a][b] = v
    return ps, low


def cholesky(low):
    """Sparse-row Cholesky of a lower-triangle covariance {a: {b: v}} (pure Python). With equal
    holds ordered by entry, the overlapping later neighbours of any position all contain its exit
    time, so they form a clique and elimination creates no fill-in outside the stored pattern
    (null_cov stores every overlapping pair, zero or not)."""
    L = [dict() for _ in low]
    for i in range(len(low)):
        for j in sorted(low[i]):
            acc = low[i][j] - sum(L[i][k] * L[j][k] for k in L[i] if k < j and k in L[j])
            if i == j:
                L[i][i] = math.sqrt(max(acc, 1e-18))
            else:
                L[i][j] = acc / L[j][j]
    return L


def mvn_draw(L, rng):
    z = [rng.gauss(0, 1) for _ in L]
    return [sum(v * z[k] for k, v in row.items()) for row in L]


def simulate_power(positions, sigma, rho, mu, sims=2000, seed=0, holdout_blocks=(), hold_sims=None):
    """Joint power by simulation at the actual entry times, sides and halves: each position's net
    return = mu + correlated random-walk noise (null_cov). Returns the share of simulations that
    pass all six criteria, and that pass with a point estimate >= BAR. holdout_blocks: [(name, n)]
    — P(holdout_confirm) on contiguous blocks of n positions of the same entry pattern."""
    order, low = null_cov(positions, sigma, rho)
    L = cholesky(low)
    srt = [positions[k] for k in order]
    rng = random.Random(seed)
    passes = pass_bar = 0
    for _ in range(sims):
        x = mvn_draw(L, rng)
        rows = [dict(p, ret={PRIMARY: mu + xi}) for p, xi in zip(srt, x)]
        sm = summarise(rows, placebo_seeds=0)
        passes += sm['pass']
        pass_bar += sm['pass'] and sm['primary_hour']['mean'] >= BAR
    out = {'mu': mu, 'sims': sims, 'p_pass': passes / sims, 'p_pass_and_bar': pass_bar / sims, 'holdout': {}}
    for name, n in holdout_blocks:
        hs = hold_sims or sims
        conf = 0
        for k in range(hs):
            assert n <= len(srt), 'holdout block longer than the discovery pattern'
            start = rng.randrange(0, len(srt) - n + 1)
            blk = srt[start:start + n]
            o2, l2 = null_cov(blk, sigma, rho)
            xb = mvn_draw(cholesky(l2), rng)
            rows = [dict(blk[o], ret={PRIMARY: mu + xi}) for o, xi in zip(o2, xb)]
            conf += holdout_confirm(rows)[0]
        out['holdout'][name] = {'n': n, 'p_confirm': conf / hs}
    return out


def holdout_end_for(n_needed, rate_per_day, start=HOLDOUT_START):
    """The first UTC day end by which the expected number of positions reaches n_needed:
    returns (end_day_start_ms inclusive, days)."""
    days = math.ceil(n_needed / rate_per_day)
    return start + (days - 1) * DAY, days
