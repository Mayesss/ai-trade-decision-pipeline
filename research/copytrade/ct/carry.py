"""Cross-venue perpetual funding carry — registration 010 (L2) mechanics. Pure functions; funding
series, price observations and price paths come in as arguments.

The trade: on an 8-hour decision grid (00/08/16 UTC), for each coin listed on two of Hyperliquid
(HL), Binance (BN) and Bybit (BY) — OKX (OK) only in a reported variant (survivorship, Revision 1 B2)
— compute the trailing 24-hour realised funding of each venue from settlements STRICTLY BEFORE t (the
rate at t is never read). When the annualised spread of a pair clears its entry threshold, go short
the higher-funding leg and long the lower-funding leg, equal size, notional N, 1x per leg, separate
margin per venue, taker on all four fills. Hold a FIXED number of hours H, then close. One position
per coin at a time.

H and the threshold are cost arithmetic, FROZEN at their drafted values (Revision 1, B1):
  C_design = 2 x (taker_A + taker_B) + 4 x design slippage (0.5 bp core, 2 bp others)
  m        = 6 %/yr on N (HLP's 3 %/yr lending leg on gross capital 2N)
  H        = C_design / m rounded up to the 8-hour grid;  th = C_design / H + m  (~12 %/yr)
The P&L charges the TIERED slippage of each leg (Revision 1, M3): 1 / 2 / 4 / 6 bp per fill by the
leg venue's trailing 30-day mean daily quote volume (>= $1 B / $200 M / $50 M / $10 M; 009's tiers).

Pieces, in the order the runner applies them:
  Funding        one venue-coin settlement series: window [t - W, t), accrual (t_in, t_out]
  spread         annualised trailing-W spread of a pair (A minus B)
  plan           the positions the rule opens (behaviour only); prints unconditional counts only
  outcome        funding at each venue's own interval, the price-gap change, fees, tiered slippage,
                 rebalances and liquidation on the price path — FORWARD: guarded runner only
  criteria / summarise / placebo / decide / holdout_confirm
  simulate_power / simulate_holdout   joint power with behaviour-level noise and a monthly regime shock
"""
import bisect
import math
import random
import statistics

from . import eventstudy as es

HOUR, DAY = 3_600_000, 86_400_000
WEEK = 7 * DAY
YEAR = 365 * DAY                              # 8,760 hours, the scout's annualisation
MONDAY0 = 4 * DAY                             # 1970-01-05 00:00 UTC, a Monday: ISO week origin
GRID = 8 * HOUR                               # decisions at 00/08/16 UTC; every CEX interval divides it
W = 24 * HOUR                                 # trailing window: a multiple of the longest interval (8 h)
SEASON = 7 * DAY                              # a venue's first settlement at least this before t
MARGIN = 0.06                                 # m, per year on N  (= 2 x HLP lending leg 3 %/yr)
BENCH = 0.03                                  # HLP lending leg, per year on gross capital
VENUES = ('HL', 'BN', 'BY')
PAIRS = (('HL', 'BN'), ('HL', 'BY'), ('BN', 'BY'))                       # primary (B2: no OKX)
PAIRS_OKX = (('HL', 'BN'), ('HL', 'BY'), ('HL', 'OK'), ('BN', 'BY'), ('BN', 'OK'), ('BY', 'OK'))
TAKER = {'HL': 0.00045, 'BN': 0.0005, 'BY': 0.00055, 'OK': 0.0005}   # base tier (scout B1)
CORE = frozenset({'BTC', 'ETH', 'SOL'})
SLIP_DESIGN = {'core': 0.00005, 'other': 0.0002}   # per fill: sets H and th only (frozen, B1)
SLIP_TIERS = ((1e9, 0.0001), (2e8, 0.0002), (5e7, 0.0004), (0.0, 0.0006))   # per fill, by 30-d mean volume
LIQ_FLOOR = 10_000_000.0                      # each leg's venue: trailing 30-day mean daily quote volume
LIQ_DAYS = 30
LEVEL_TOL = 0.05                              # |ln price ratio| after the 10^k scale, name check
TOPUP = 0.5                                   # adverse move of one leg that forces a rebalance
TRANSFER = 0.001                              # cost of moving margin: 10 bp of the moved amount
MM = {('HL', 'core'): 0.025, ('HL', 'other'): 0.167, ('BN', 'core'): 0.005, ('BN', 'other'): 0.025,
      ('BY', 'core'): 0.005, ('BY', 'other'): 0.025, ('OK', 'core'): 0.005, ('OK', 'other'): 0.025}

DISC_START = 1_686_182_400_000                # 2023-06-08 00:00 UTC: HL hourly funding starts
DISC_END = 1_735_689_600_000                  # 2025-01-01 00:00 UTC: every discovery exit is before
EXPOSED_START = 1_736_899_200_000             # 2025-01-15: the exposed 2025-26 block (reported, 000f)
EXPOSED_END = 1_782_864_000_000               # 2026-07-01 00:00 UTC
HOLDOUT_START = 1_782_864_000_000             # 2026-07-01: the decisive holdout, unread
HOLDOUT_END = {'A': 1_790_812_800_000,        # 2026-10-01 00:00 UTC (option A: end 2026-09-30)
               'B': 1_798_761_600_000}        # 2027-01-01 00:00 UTC (option B: end 2026-12-31)
Z = 2.24                                      # week-clustered, one-sided 2.5 % Bonferroni over T1, T2
Q_CONFIRM = 0.95                              # holdout existence test, one look: month t >= t_{G-1}(0.95) (Rev. 2a)
Q_CONFIRM_TWO_LOOKS = 0.975                   # with the pre-registered extension: t_{G-1}(0.975) at each look
                                              # (alpha 5 % split 2.5 % + 2.5 %, Bonferroni over two looks)
EXTENSION_END = 1_814_400_000_000             # 2027-07-01 00:00 UTC: the extension's fixed end (2027-06-30)

# Student t quantiles (one-sided), df 1..30; beyond 30 the normal value is used
_T95 = (6.314, 2.920, 2.353, 2.132, 2.015, 1.943, 1.895, 1.860, 1.833, 1.812, 1.796, 1.782, 1.771, 1.761,
        1.753, 1.746, 1.740, 1.734, 1.729, 1.725, 1.721, 1.717, 1.714, 1.711, 1.708, 1.706, 1.703, 1.701,
        1.699, 1.697)
_T975 = (12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160, 2.145,
         2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048,
         2.045, 2.042)


def t_crit(df, q):
    """One-sided Student t quantile, q in {0.95, 0.975}; df < 1 -> inf (no test possible)."""
    if df < 1:
        return math.inf
    tab, z = (_T95, 1.645) if q == 0.95 else (_T975, 1.960)
    return tab[df - 1] if df <= 30 else z


# ------------------------------------------------------------------ arithmetic (no data)
def cls(coin):
    return 'core' if coin in CORE else 'other'


def round_trip(pair, coin):
    """C_design: the four taker fills plus four DESIGN slippages, as a fraction of N (sets H, th)."""
    a, b = pair
    return 2 * (TAKER[a] + TAKER[b]) + 4 * SLIP_DESIGN[cls(coin)]


def hold_ms(c, margin=MARGIN):
    """H = C / m rounded UP to the 8-hour grid, in ms."""
    raw = c / margin * YEAR
    return int(math.ceil(raw / GRID - 1e-9)) * GRID


def threshold(c, h, margin=MARGIN):
    """Entry threshold, per year on N: the round trip amortised over H plus the margin."""
    return c * YEAR / h + margin


def pair_terms(pair, coin):
    c = round_trip(pair, coin)
    h = hold_ms(c)
    return c, h, threshold(c, h)


def slip_tier(volume):
    """Per-fill slippage for a leg from its venue's trailing 30-day mean daily quote volume."""
    for floor, s in SLIP_TIERS:
        if volume >= floor:
            return s
    return SLIP_TIERS[-1][1]


def trade_cost(pair, slip_a, slip_b):
    """The P&L round trip on N: four taker fills plus each leg's tiered slippage on its two fills."""
    return 2 * (TAKER[pair[0]] + TAKER[pair[1]]) + 2 * (slip_a + slip_b)


def hour_round(t):
    return (t + HOUR // 2) // HOUR * HOUR


def week_of(t):
    return (t - MONDAY0) // WEEK


def month_of(t):
    import datetime as dt
    d = dt.datetime.fromtimestamp(t / 1000, dt.UTC)
    return d.year * 100 + d.month


def liq_distance(venue, coin):
    """Short leg at 1x: liquidated after a rise of (1 - mm) / (1 + mm). The long leg at 1x cannot be
    liquidated by a fall (distance 100 %)."""
    mm = MM[(venue, cls(coin))]
    return (1 - mm) / (1 + mm)


# ------------------------------------------------------------------ funding series
class Funding:
    """One venue-coin settlement series: [(t_ms, rate)], times rounded to the hour.
    A settlement at T is paid to positions open at T; a position open over [t_in, t_out] collects
    T with t_in < T <= t_out (opened at t_in just after the hour, closed at t_out just after)."""

    def __init__(self, rows):
        from array import array
        d = {}
        for t, r in rows:
            d[hour_round(int(t))] = float(r)
        self.t = array('q', sorted(d))
        self.r = array('d', (d[t] for t in self.t))
        self.cum = array('d', [0.0])
        acc = 0.0
        for r in self.r:
            acc += r
            self.cum.append(acc)

    def __bool__(self):
        return bool(self.t)

    @property
    def first(self):
        return self.t[0] if self.t else None

    @property
    def last(self):
        return self.t[-1] if self.t else None

    def _idx(self, t, inclusive):
        return (bisect.bisect_right if inclusive else bisect.bisect_left)(self.t, t)

    def window(self, t, w=W):
        """Sum of rates with t - w <= T < t. The rate AT t is excluded."""
        return self.cum[self._idx(t, False)] - self.cum[self._idx(t - w, False)]

    def count(self, a, b):
        """Number of prints with a <= T < b."""
        return self._idx(b, False) - self._idx(a, False)

    def accrue(self, t_in, t_out):
        """Sum of rates with t_in < T <= t_out."""
        return self.cum[self._idx(t_out, True)] - self.cum[self._idx(t_in, True)]

    def accrue_weighted(self, t_in, t_out, weight, skip=()):
        """Sum of rate x weight(T) over t_in < T <= t_out (funding is paid on size x price at T),
        leaving out settlements inside any skip span (a, b] (a funding-data hole, m-d)."""
        lo, hi = self._idx(t_in, True), self._idx(t_out, True)
        return sum(self.r[k] * weight(self.t[k]) for k in range(lo, hi)
                   if not any(a < self.t[k] <= b for a, b in skip))

    def holes(self, t_in, t_out, lookback=7 * DAY):
        """Funding-data holes inside (t_in, t_out] (m-d): the leg's interval is the median spacing of
        its prints over [t_in - 7 d, t_out]; a hole is a spacing > 1.5 intervals between consecutive
        prints. Returns ([(a, b_minus_interval)] spans whose settlements are missing, n_missing)."""
        lo, hi = self._idx(t_in - lookback, False), self._idx(t_out, True)
        ts = list(self.t[lo:hi])
        if len(ts) < 3:
            return [], 0
        gaps = [b - a for a, b in zip(ts, ts[1:])]
        iv = statistics.median(gaps)
        spans, n = [], 0
        for a, b in zip(ts, ts[1:]):
            if b - a > 1.5 * iv and b > t_in and a < t_out:
                spans.append((a, b - iv))
                n += int(round((b - a) / iv)) - 1
        return spans, n

    def seasoned(self, t, d=SEASON):
        return bool(self.t) and self.t[0] <= t - d

    def active(self, t, gap=GRID):
        """A print in [t - gap, t): still settling (every interval here is <= 8 h)."""
        return self.count(t - gap, t) > 0


def spread(fa, fb, t, w=W):
    """Annualised trailing spread A minus B (per year on N), from prints in [t - w, t) only."""
    return (fa.window(t, w) - fb.window(t, w)) * YEAR / w


# ------------------------------------------------------------------ the rule (behaviour)
def candidates(coin, funding, t, eligible, pairs=PAIRS):
    """Every pair of the coin that is listed, seasoned, active and eligible at t, with its spread and
    terms: [(excess, pair_index, pair, s, c, h, th)]. Reads prints before t only."""
    out = []
    for k, pair in enumerate(pairs):
        fa, fb = funding.get((pair[0], coin)), funding.get((pair[1], coin))
        if not fa or not fb:
            continue
        if not (fa.seasoned(t) and fb.seasoned(t) and fa.active(t) and fb.active(t)):
            continue
        if not eligible(coin, pair, t):
            continue
        s = spread(fa, fb, t)
        c, h, th = pair_terms(pair, coin)
        out.append((abs(s) - th, k, pair, s, c, h, th))
    return out


def plan(coins, funding, eligible, times, start, end, pairs=PAIRS, slip=None):
    """The positions the rule opens. Behaviour only: entry time, pair, direction, hold, entry cost.

    funding: {(venue, coin): Funding}; eligible(coin, pair, t) -> bool from information known at t
    (each leg's liquidity floor and name check, an entry price observation on both venues);
    slip(coin, venue, t) -> per-fill slippage from the trailing volume (None: the design slippage).
    A coin that is held is SKIPPED before anything is evaluated: nothing about the spread during a
    hold is computed or counted (Revision 1, B1). Entries at t >= start; a position whose exit t + H
    would reach `end` is not opened (edge). Among the pairs clearing their threshold the largest |s|
    wins; ties go to the cheaper design round trip, then to the earlier pair. Direction: short the
    higher-funding venue. Returns (positions, counts) — counts hold the number opened only: any count
    of qualifying times split by whether the coin is held or free is a persistence statistic (000f)."""
    busy = {}
    out = []
    counts = {'opened': 0}
    for t in times:
        if t < start or t >= end:
            continue
        for coin in coins:
            if t < busy.get(coin, -1):
                continue
            q = [x for x in candidates(coin, funding, t, eligible, pairs) if x[0] >= 0]
            if not q:
                continue
            excess, k, pair, s, c, h, th = max(q, key=lambda x: (abs(x[3]), -x[4], -x[1]))
            if t + h >= end:
                continue
            short, long_ = (pair[0], pair[1]) if s > 0 else (pair[1], pair[0])
            sa = slip(coin, pair[0], t) if slip else SLIP_DESIGN[cls(coin)]
            sb = slip(coin, pair[1], t) if slip else SLIP_DESIGN[cls(coin)]
            busy[coin] = t + h
            counts['opened'] += 1
            out.append({'coin': coin, 'pair': pair, 'short': short, 'long': long_, 'dir': 1 if s > 0 else -1,
                        't_in': t, 't_out': t + h, 'hold_ms': h, 'cost': trade_cost(pair, sa, sb),
                        'cost_design': c, 'slip': (sa, sb), 'threshold': th, 'signal': s,
                        'cls': cls(coin), 'week': week_of(t), 'month': month_of(t)})
    out.sort(key=lambda p: (p['t_in'], p['coin']))
    return out, counts


# ------------------------------------------------------------------ outcome (FORWARD: runner only)
def path_events(pos, path, t_end):
    """Rebalances and liquidation on the coin's price path over the hold. path(coin, a, b) ->
    [(bar_end, high, low, close)] for bars ending in [a, b], the first one ending at a (its close is
    the entry reference). The short leg is liquidated in the first bar whose high reaches its 1x
    liquidation distance above the reference (no time to top up within a bar). Otherwise a bar with
    an adverse move >= TOPUP on either leg forces a rebalance at its close: both legs closed and
    reopened at notional N (one more round trip, plus TRANSFER on the moved margin); the reference
    resets to that close. Returns a dict: topups, topup_cost, liq_at, liq_close, ref (in force at the
    end), resets [(t, close)], max_up, max_down."""
    out = {'topups': 0, 'topup_cost': 0.0, 'liq_at': None, 'liq_close': None, 'ref': None, 'resets': [],
           'max_up': None, 'max_down': None}
    bars = path(pos['coin'], pos['t_in'], t_end)
    if not bars or bars[0][0] != pos['t_in'] or not bars[0][3]:
        return out
    ref = bars[0][3]
    out['resets'].append((pos['t_in'], ref))
    d_liq = liq_distance(pos['short'], pos['coin'])
    max_up = max_down = 0.0
    for bar_end, hi, lo, close in bars[1:]:
        up, down = hi / ref - 1, 1 - lo / ref
        max_up, max_down = max(max_up, up), max(max_down, down)
        if up >= d_liq:
            out.update(liq_at=bar_end, liq_close=close)
            break
        if up >= TOPUP or down >= TOPUP:
            out['topups'] += 1
            out['topup_cost'] += pos['cost'] + TRANSFER * max(up, down)
            ref = close
            out['resets'].append((bar_end, close))
    out.update(ref=ref, max_up=max_up, max_down=max_down)
    return out


def latest(obs, t, max_lag=DAY):
    """obs(t) -> value or None; the latest value at or before t on the hour grid within max_lag."""
    k = t - t % HOUR
    while k >= t - max_lag:
        v = obs(k)
        if v is not None:
            return v, t - k
        k -= HOUR
    return None, None


def price_weight(pos, path, t_end, resets=None):
    """weight(T) = P(T) / P(ref) with P(ref) the close at the latest reset at or before T (the entry,
    or a rebalance that reopened both legs at N); 1.0 without a path."""
    bars = path(pos['coin'], pos['t_in'], t_end)
    if not bars or bars[0][0] != pos['t_in'] or not bars[0][3]:
        return lambda T: 1.0
    ts = [b[0] for b in bars]
    rs = resets or [(pos['t_in'], bars[0][3])]
    rts = [r[0] for r in rs]

    def w(T):
        p = bars[max(0, bisect.bisect_right(ts, T) - 1)][3]
        ref = rs[max(0, bisect.bisect_right(rts, T - 1) - 1)][1]    # a reset at T applies after T
        return p / ref
    return w


def outcome(pos, funding, gap, path, gap_ok=None, fill=None):
    """One position's components, per unit of notional N, and its net return on GROSS capital 2N.

    funding: settlements of each leg at its own interval, (t_in, t_x]; short receives, long pays;
    each scaled by P(T) / P(last reset) (paid on size x price; equal coin size, reset when a
    rebalance reopens both legs at N). t_x = the planned exit, earlier if a leg stops settling
    (delisted: the last settlement of either leg) or the short leg is liquidated.
    gap / fill: fill(coin, a, b, t) = ln P_a - ln P_b at the POST-settlement fill at t (the first trade
    after t: the open of the bar starting at t; Revision 2, N6), default gap. P&L = fill_dir(t_in) -
    fill_dir(t_x); the exit observation is the latest at or before t_x within 24 h at which
    gap_ok(coin, a, b, t) holds (both legs pass the name check); if none passes, the latest RAW
    observation within 24 h (exit_gap='raw', N4: a divergence stays in the primary); if no price
    exists within 24 h, no outcome (the runner fills the worst observed return of the pair and span,
    N4).
    funding holes (m-d): a leg's missing settlements (spacing > 1.5 x its interval) are counted, and
    both legs' settlements inside the hole are left out of the accrual.
    liquidation (Revision 1, B3): the short leg loses its whole posted margin (-N); the long leg is
    closed at the liquidation bar's close and gains close / ref - 1; the gap term is dropped for that
    position; fees and slippage are charged in full.
    costs: the tiered round trip, rebalances. Returns a dict, or None without an entry or exit gap."""
    coin, s_v, l_v = pos['coin'], pos['short'], pos['long']
    fs, fl = funding[(s_v, coin)], funding[(l_v, coin)]
    t_in, t_x = pos['t_in'], pos['t_out']
    delisted = False
    last = min(fs.last, fl.last)
    if last < t_x - GRID:
        t_x, delisted = max(t_in, last), True
    ev = path_events(pos, path, t_x)
    fill = fill or gap
    g_in = fill(coin, s_v, l_v, t_in)
    if g_in is None:
        return None
    ok = gap_ok or (lambda c, a, b, k: True)
    exit_gap = 'valid'
    if ev['liq_at'] is not None:
        t_x = ev['liq_at']
        gp, g_out, lag, exit_gap = -1.0 + (ev['liq_close'] / ev['ref'] - 1.0), None, 0, 'liquidated'
    else:
        g_out, lag = latest(lambda k: fill(coin, s_v, l_v, k) if ok(coin, s_v, l_v, k) else None, t_x, DAY)
        if g_out is None:
            g_out, lag = latest(lambda k: fill(coin, s_v, l_v, k), t_x, DAY)
            exit_gap = 'raw'
        if g_out is None:
            return None
        gp = g_in - g_out
    w = price_weight(pos, path, t_x, ev['resets'])
    hs, ns = fs.holes(t_in, t_x)
    hl_, nl = fl.holes(t_in, t_x)
    skip = hs + hl_
    f = fs.accrue_weighted(t_in, t_x, w, skip) - fl.accrue_weighted(t_in, t_x, w, skip)
    gross = f + gp
    cost = pos['cost'] + ev['topup_cost']
    return dict(pos, t_x=t_x, delisted=delisted, liquidated=ev['liq_at'] is not None, topups=ev['topups'],
                funding=f, funding_holes=ns + nl, gap_pnl=gp, gap_in=g_in, gap_out=g_out, exit_lag_ms=lag,
                exit_gap=exit_gap,
                fees=pos['cost'], topup_cost=ev['topup_cost'],
                gross=gross / 2, costs=cost / 2, ret=(gross - cost) / 2, hold_years=(t_x - t_in) / YEAR,
                max_up=ev['max_up'], max_down=ev['max_down'])


# ------------------------------------------------------------------ inference
def _block(rows, key='week', val=lambda r: r['ret']):
    v = [val(r) for r in rows]
    m, se, t, n, g = es.cluster_t(v, [r[key] for r in rows])
    vv = [x for x in v if x is not None]
    return {'mean': m, 'se': se, 't': t, 'n': n, 'clusters': g,
            'median': statistics.median(vv) if vv else None,
            'hit': (sum(1 for x in vv if x > 0) / len(vv)) if vv else None}


def annualised(rows):
    """Net return on gross capital per year of deployment: sum of returns / sum of hold years."""
    yrs = sum(r['hold_years'] for r in rows)
    return sum(r['ret'] for r in rows) / yrs if yrs > 0 else None


def largest_positive(rows, key='month'):
    """The cluster with the largest POSITIVE summed net return (ties: earliest); None if none > 0."""
    sums = {}
    for r in rows:
        sums[r[key]] = sums.get(r[key], 0.0) + r['ret']
    pos = {c: v for c, v in sums.items() if v > 0}
    return min(pos, key=lambda c: (-pos[c], c)) if pos else None


def criteria(rows, z=Z):
    """The six pass criteria on one trial's positions (ret, hold_years, t_in, week, month):
    1 week-clustered t >= z; 1b month-clustered t >= t_{G-1}(0.975) (2.10 at 19 months);
    2 mean and median same sign; 3 both count halves positive; 4 positive without the largest
    positive month; 5 annualised net on gross capital >= 3 %/yr (the economic point bar)."""
    rows = [r for r in rows if r.get('ret') is not None]
    main = _block(rows, 'week')
    mon = _block(rows, 'month')
    z_month = t_crit(mon['clusters'] - 1, 0.975)
    srt = sorted(rows, key=lambda r: (r['t_in'], r['coin']))
    half = len(srt) // 2
    halves = [_block(srt[:half])['mean'], _block(srt[half:])['mean']] if half >= 3 else [None, None]
    big = largest_positive(rows, 'month')
    ex = [r for r in rows if r['month'] != big]
    without = _block(ex) if len(ex) >= 3 else None
    ann = annualised(rows)
    crit = {
        '1_mean_pos_week_t': main['t'] is not None and main['mean'] > 0 and main['t'] >= z,
        '1b_month_t': mon['t'] is not None and mon['mean'] > 0 and mon['t'] >= z_month,
        '2_mean_median_same_sign': main['median'] is not None and main['mean'] is not None
                                   and (main['mean'] > 0) == (main['median'] > 0),
        '3_both_halves_positive': all(x is not None and x > 0 for x in halves),
        '4_without_largest_positive_month_positive': without is not None and without['mean'] is not None
                                                     and without['mean'] > 0,
        '5_annualised_ge_benchmark': ann is not None and ann >= BENCH,
    }
    return crit, {'primary_week': main, 'primary_month': mon, 'z_month': z_month, 'halves': halves,
                  'largest_positive_month': big, 'without_largest_positive_month': without, 'annualised': ann}


def placebo(rows, seeds=200, flip_key='month'):
    """Reverse the trade direction per cluster: each `flip_key` cluster's positions get one random
    sign on their GROSS (funding + gap flip with the direction). Reported honestly: with costs kept
    (net' = sign x gross - costs) the pass share is ~0 BY CONSTRUCTION (half the clusters lose gross
    plus costs), so it calibrates nothing; the informative number is the share of seeds whose
    clustered |t| on the flipped GROSS alone reaches the trial's critical value (nominal 5 % two-
    sided at 1.96 / t_{G-1}(0.975)). The statistic is clustered on the same key as the flip."""
    use = [r for r in rows if r.get('ret') is not None]
    if not use or not seeds:
        return {'pass_share_costs_kept': None, 'gross_abs_share': None, 'seeds': seeds, 'flip': flip_key}
    keys = [r[flip_key] for r in use]
    g = len(set(keys))
    zc = t_crit(g - 1, 0.975) if flip_key == 'month' else 1.96
    passes = hits = 0
    for s_ in range(seeds):
        rng = random.Random(s_)
        sign = {}
        for k in keys:
            if k not in sign:
                sign[k] = rng.choice((-1.0, 1.0))
        m, _, t, _, _ = es.cluster_t([sign[k] * r['gross'] - r['costs'] for k, r in zip(keys, use)], keys)
        passes += t is not None and m > 0 and t >= zc
        _, _, t2, _, _ = es.cluster_t([sign[k] * r['gross'] for k, r in zip(keys, use)], keys)
        hits += t2 is not None and abs(t2) >= zc
    return {'pass_share_costs_kept': passes / seeds, 'gross_abs_share': hits / seeds, 'nominal': 0.05,
            'critical': zc, 'seeds': seeds, 'flip': flip_key}


def summarise(rows, z=Z, placebo_seeds=200):
    rows = [r for r in rows if r.get('ret') is not None]
    crit, detail = criteria(rows, z)
    comp = {k: _block(rows, val=lambda r, k=k: r[k]) for k in ('funding', 'gap_pnl', 'fees', 'topup_cost')} \
        if rows and 'funding' in rows[0] else None
    return dict(detail, criteria=crit, passed=all(crit.values()), components_on_N=comp,
                placebo_month_flip=placebo(rows, placebo_seeds, 'month'),
                placebo_week_flip=placebo(rows, placebo_seeds, 'week'))


def decide(t1, t2):
    """Proposed decision table (§9): either trial passing all six -> ask the owner to approve the
    holdout fetch and read (2026-07-01 -> the chosen end); otherwise close L2."""
    return 'read_holdout' if (t1['passed'] or t2['passed']) else 'close'


def holdout_confirm(rows, q=Q_CONFIRM):
    """The decisive holdout, positions built by the same rule — an EXISTENCE test (Revision 2, N5;
    Revision 2a): CONFIRM if all of mean net > 0 with month-clustered t >= t_{G-1}(q), G the number of
    month clusters actually in the window (q = 0.95 for one look; 0.975 at each of two looks when the
    owner pre-registers the extension); median > 0; mean > 0 without the largest-positive month. The
    annualised net (the 3 %/yr point bar) is REPORTED, not a criterion: it is the deploy threshold of
    the follow-up paper registration. Returns (confirm?, details)."""
    rows = [r for r in rows if r.get('ret') is not None]
    main = _block(rows, 'week')
    mon = _block(rows, 'month')
    z = t_crit(mon['clusters'] - 1, q)
    big = largest_positive(rows, 'month')
    ex = [r for r in rows if r['month'] != big]
    without = _block(ex) if len(ex) >= 3 else None
    crit = {'month_t': mon['t'] is not None and mon['mean'] > 0 and mon['t'] >= z,
            'median_pos': main['median'] is not None and main['median'] > 0,
            'without_largest_positive_month_pos': without is not None and without['mean'] is not None
                                                  and without['mean'] > 0}
    return all(crit.values()), {'primary_week': main, 'primary_month': mon, 'q': q, 'z': z, 'criteria': crit,
                                'annualised_reported': annualised(rows),
                                'without_largest_positive_month': without}


def extension_due(details):
    """The pre-registered extension (N5): taken only if the owner chose it at registration and the
    first look ends with a month-clustered t strictly between 0 and that look's critical value
    t_{G-1}(0.975)."""
    t = details['primary_month']['t']
    return t is not None and 0 < t < details['z']


def recheck_discovery(row, z=Z):
    """Revision 2 (N2, 009's pattern): recompute each trial's six criteria, its pass and the decision
    from the NUMBERS stored in a 010 discovery row — never from a label. Returns (decision,
    [failures])."""
    num = lambda x: isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)
    fails, passed = [], {}
    for k in ('T1', 'T2'):
        ev = row.get(k) or {}
        wk, mo = ev.get('primary_week') or {}, ev.get('primary_month') or {}
        halves = ev.get('halves') or [None, None]
        wo = ev.get('without_largest_positive_month') or {}
        zm = t_crit((mo.get('clusters') or 0) - 1, 0.975)
        crit = {
            '1_mean_pos_week_t': num(wk.get('mean')) and num(wk.get('t')) and wk['mean'] > 0 and wk['t'] >= z,
            '1b_month_t': num(mo.get('mean')) and num(mo.get('t')) and mo['mean'] > 0 and mo['t'] >= zm,
            '2_mean_median_same_sign': num(wk.get('mean')) and num(wk.get('median'))
                                       and (wk['mean'] > 0) == (wk['median'] > 0),
            '3_both_halves_positive': len(halves) == 2 and all(num(h) and h > 0 for h in halves),
            '4_without_largest_positive_month_positive': num(wo.get('mean')) and wo['mean'] > 0,
            '5_annualised_ge_benchmark': num(ev.get('annualised')) and ev['annualised'] >= BENCH,
        }
        if (ev.get('criteria') or {}) != crit:
            fails.append(f'{k}: stored criteria differ from those recomputed from the stored statistics')
        if ev.get('passed') != all(crit.values()):
            fails.append(f'{k}: stored pass label differs from the recomputed pass')
        passed[k] = {'passed': all(crit.values())}
    dec = decide(passed['T1'], passed['T2'])
    if row.get('decision') != dec:
        fails.append(f"stored decision {row.get('decision')} differs from the recomputed {dec}")
    return dec, fails


# ------------------------------------------------------------------ power (behaviour-level noise)
def _draw(positions, sigma_s, sigma_g, rho, mu_yr, rng, persist=1.0, sigma_month=0.0):
    """One simulated outcome set. Per position, on gross capital: (mu_yr + month shock) x H_years +
    funding noise / 2 + gap noise / 2. Funding noise = sigma_s[cls] x H_years x f x (sqrt(rho) x dir x
    weekly common + sqrt(1 - rho) x idiosyncratic); gap noise = sqrt(2) x sigma_g[cls] x N(0, 1);
    month shock ~ N(0, sigma_month) per entry month, on the annualised edge (a regime common to
    every position entered that month — the reviewer's probe, Revision 1 M1)."""
    sr, s1 = math.sqrt(rho), math.sqrt(1 - rho)
    wk = {}
    shock = {}
    rows = []
    for p in positions:
        ws = range(week_of(p['t_in']), week_of(p['t_out'] - 1) + 1)
        for w in ws:
            if w not in wk:
                wk[w] = rng.gauss(0, 1)
        if p['month'] not in shock:
            shock[p['month']] = rng.gauss(0, sigma_month) if sigma_month else 0.0
        common = sum(wk[w] for w in ws) / math.sqrt(len(ws))
        zf = sr * p['dir'] * common + s1 * rng.gauss(0, 1)
        hy = p['hold_ms'] / YEAR
        f = persist(p) if callable(persist) else persist
        noise = (sigma_s[p['cls']] * hy * f * zf + math.sqrt(2) * sigma_g[p['cls']] * rng.gauss(0, 1)) / 2
        rows.append(dict(p, ret=(mu_yr + shock[p['month']]) * hy + noise, hold_years=hy))
    return rows


def simulate_power(positions, sigma_s, sigma_g, rho, mu_yr, sims=1000, seed=0, z=Z, persist=1.0,
                   sigma_month=0.0, judge=None):
    """Joint power at a planted net edge mu_yr (per year on gross capital) at the actual entry pattern.
    judge(rows) -> bool; default: all six criteria. sigma_s / sigma_g: UNCONDITIONAL SDs of the spread
    and gap levels (behaviour); persist f = 1 (conservative) or persist_iid (optimistic)."""
    judge = judge or (lambda rows: all(criteria(rows, z)[0].values()))
    rng = random.Random(seed)
    passes = c1 = 0
    anns = []
    for _ in range(sims):
        rows = _draw(positions, sigma_s, sigma_g, rho, mu_yr, rng, persist, sigma_month)
        passes += judge(rows)
        crit, det = criteria(rows, z)
        c1 += crit['1_mean_pos_week_t'] and crit['1b_month_t']
        anns.append(det['annualised'])
    return {'mu_yr': mu_yr, 'sigma_month': sigma_month, 'sims': sims, 'p_pass': passes / sims,
            'p_crit1_1b': c1 / sims, 'annualised_sd': statistics.pstdev(anns)}


def simulate_holdout(pattern, span_ms, sigma_s, sigma_g, rho, mu_yr, sims=1000, seed=0, persist=1.0,
                     sigma_month=0.0):
    """P(holdout CONFIRM) for a holdout of span_ms at the entry rate and pattern of `pattern`
    (positions of a recent period): each draw takes a random contiguous window of that length, keeps
    the positions entered in it whose exit is inside it (the edge rule), and judges holdout_confirm.
    Returns the share confirming and the mean number of positions and months."""
    rng = random.Random(seed)
    srt = sorted(pattern, key=lambda p: p['t_in'])
    t0, t1 = srt[0]['t_in'], srt[-1]['t_out']
    conf = n = mo = 0
    for _ in range(sims):
        a = rng.randrange(t0, max(t0 + 1, t1 - span_ms))
        a -= a % GRID
        blk = [p for p in srt if a <= p['t_in'] and p['t_out'] < a + span_ms]
        n += len(blk)
        mo += len({p['month'] for p in blk})
        if len(blk) < 3:
            continue
        rows = _draw(blk, sigma_s, sigma_g, rho, mu_yr, rng, persist, sigma_month)
        conf += holdout_confirm(rows)[0]
    return {'mu_yr': mu_yr, 'sigma_month': sigma_month, 'span_days': span_ms / DAY, 'sims': sims,
            'p_confirm': conf / sims, 'mean_positions': n / sims, 'mean_months': mo / sims}


def simulate_joint(disc, pattern, span_ms, sigma_s, sigma_g, rho, mu_yr, sims=2000, seed=0, sigma_month=0.0,
                   hold_sigma_s=None, rho_hold=None, extension_span_ms=None, which='T1'):
    """Joint power of the whole path (N5): P(discovery pass of `which`) x P(holdout CONFIRM), drawn
    together in each simulation (independent periods, independent month shocks). `disc`: discovery
    entry pattern; `pattern`: the recent (2025-26) entry pattern from which a contiguous holdout window
    of span_ms is drawn. extension_span_ms: the pre-registered extension — both looks at
    t_{G-1}(0.975); the second look (on the whole cumulative span) is taken only when the first ends
    with 0 < t < its critical value.
    Returns P(discovery pass), P(confirm | pass), P(joint)."""
    rng = random.Random(seed)
    srt = sorted(pattern, key=lambda p: p['t_in'])
    t0, t1 = srt[0]['t_in'], srt[-1]['t_out']
    hs = hold_sigma_s or sigma_s
    rh = rho if rho_hold is None else rho_hold
    longest = extension_span_ms or span_ms
    d_pass = joint = 0
    for _ in range(sims):
        rows = _draw(disc, sigma_s, sigma_g, rho, mu_yr, rng, 1.0, sigma_month)
        ok_d = all(criteria(rows)[0].values())
        d_pass += ok_d
        a = rng.randrange(t0, max(t0 + 1, t1 - longest))
        a -= a % GRID
        full = [p for p in srt if a <= p['t_in'] and p['t_out'] < a + longest]
        drawn = _draw(full, hs, sigma_g, rh, mu_yr, rng, 1.0, sigma_month) if len(full) >= 3 else []
        first = [r for r in drawn if r['t_out'] < a + span_ms]
        if extension_span_ms is None:
            ok_h = len(first) >= 3 and holdout_confirm(first)[0]
        else:
            ok1, det = holdout_confirm(first, Q_CONFIRM_TWO_LOOKS) if len(first) >= 3 else (False, None)
            ok_h = ok1 or (det is not None and extension_due(det)
                           and holdout_confirm(drawn, Q_CONFIRM_TWO_LOOKS)[0])
        joint += ok_d and ok_h
    return {'mu_yr': mu_yr, 'sigma_month': sigma_month, 'sims': sims, 'p_discovery_pass': d_pass / sims,
            'p_joint': joint / sims, 'p_confirm_given_pass': joint / d_pass if d_pass else None}


def persist_iid(p):
    """The optimistic noise bound: the hold as H / W independent trailing windows."""
    return math.sqrt(W / p['hold_ms'])
