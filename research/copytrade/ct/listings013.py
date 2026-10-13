"""Registration 013 (N3 i) — short new Binance USDⓈ-M perps: rules, trade mechanics, statistics, decisions.
Revision 1 (after reviews A and B).

PURE FUNCTIONS ONLY. No file, network or clock access here; `r013_fetch.py`, `r013_build.py` and
`r013_run.py` do the I/O. Every parameter below is fixed in the registration (§4-§7, the D-table) before any
outcome is computed; a change is a change here AND there, with a new §10 hash table.

The trade (T1): every new perp (the first archive segment of a USDT symbol; relistings, continuations,
pre-market perps and censored symbols out) is shorted at the open of the first TRADED 1m bar at or after
FIRST TRADE + 24 h, 1x ISOLATED margin (notional = the event's capital), held 7 days, exited at the open of
the first traded 1m bar at or after entry target + 7 d. No stop: the isolated margin bounds the loss at -100 %
of the event's capital (liquidation at the 1m LAST-PRICE high, maintenance margin 5 %). Funding at the archived
values on Binance's actual settlement stamps. T2: T1 restricted to events whose funding settled in (first
trade, entry] sums to >= 0 (the perp was not at a material discount over its first day).

Decisions (Revision 1): discovery criterion 5 and the confirmation are decided on the INTERCEPT alpha of a
pooled OLS of each event's net on its own market return over the same hold (beta estimated), listing-week
clustered (CR1), critical value max(t_{G-2}(q), the registered calibrated value). The discovery robustness criteria
use the adjusted net (net - beta_hat * mkt); the raw net feeds the economic bars only.

Units: times in ms UTC; returns are fractions of the event's capital (= its 1x notional at entry).
"""
import bisect
import datetime as dt
import functools
import math
import random
import re
import statistics

MIN, HOUR, DAY = 60_000, 3_600_000, 86_400_000
WEEK = 7 * DAY
YEAR = 365.25 * DAY


class Refused(Exception):
    """A rule refusal. Never an `assert` (which `python -O` strips)."""


def refuse_unless(cond, msg):
    if not cond:
        raise Refused(msg)


def ms(day):
    """'YYYY-MM-DD' -> ms at 00:00 UTC."""
    return int(dt.datetime.fromisoformat(day).replace(tzinfo=dt.UTC).timestamp() * 1000)


def day_of(t):
    return dt.datetime.fromtimestamp(t / 1000, dt.UTC).date().isoformat()


# ------------------------------------------------------------------ calendars (fixed; §2, D2)
CUTOFF_DAY = '2026-07-01'                       # sealed from here: nothing dated >= this is read
CUTOFF = ms(CUTOFF_DAY)
ARCHIVE_START = '2020-01-01'                    # first files on or before this day are left-censored
CALENDARS = {
    'reported': (ms('2020-01-02'), ms('2023-12-23')),       # 2020-2023, spot-first regime: reported only
    'discovery': (ms('2024-01-01'), ms('2025-07-19')),      # 009 holdout A (closed, released)
    'confirmation': (ms('2025-07-28'), ms('2026-06-22')),   # 009 holdout-B calendar, Binance only
}
DATA_END = {'reported': ms('2024-01-01'), 'discovery': ms('2025-07-28'), 'confirmation': CUTOFF}
EMBARGO = ((ms('2023-12-23'), ms('2024-01-01')), (ms('2025-07-19'), ms('2025-07-28')),
           (ms('2026-06-22'), CUTOFF))
PULL_DAYS = 10                                  # daily 1m files d0 .. d0+9 per event (entry + hold + 6 h margin)
PRE_ENTRY = 25 * HOUR                           # build coverage is limited to [d0, first trade + 25 h) (000n)

# ------------------------------------------------------------------ the trade (fixed; §4, D3-D9)
ENTRY_DELAY = 24 * HOUR
ENTRY_WINDOW = 60 * MIN                         # entry bar: first TRADED bar with t in [t_in, t_in + 60 min)
HOLD = 7 * DAY
EXIT_WINDOW = 6 * HOUR                          # exit bar: first traded bar in [t_x, t_x + 6 h)
FEE_DECISION = 0.0006                           # taker per fill (conservative; Binance base is 5 bp)
FEE_BINANCE = 0.0005                            # Binance base-tier taker (reported)
# slippage per fill by the AVERAGE HOURLY quote volume of the 24 h before the entry target (known at entry);
# the exit fill is charged ONE TIER WORSE (day 8 is thinner than day 1; review A m3)
SLIP_TIERS = ((50e6, 0.0005), (10e6, 0.0008), (2e6, 0.0015), (0.0, 0.0030))
SLIP_WORSE = 0.0050                             # the tier below the last one (exit of a < $2 M/h listing)
MMR = 0.05
FUNDING_CAP = 0.02
STRESS = {'slip_mult': 2.0, 'pay_mult': 2.0, 'receive_mult': 0.5}

# ------------------------------------------------------------------ book (fixed; §6, D13; Revision 1)
# Rolling-window caps make the tail bounds exact: every exit of an ISO week comes from entries inside a 14.3-day
# window, every exit of a calendar month from entries inside a 38.3-day window (entry <= target + 1 h; exit <=
# target + 7 d + 6 h; liquidations earlier). So exits per week <= WEEK_MAX and per month <= MONTH_MAX.
C_EVENT = 0.025                                 # capital per event (1x isolated: margin = notional)
WEEK_WINDOW, WEEK_MAX = 15 * DAY, 7             # <= 7 entries in any trailing 15 days
MONTH_WINDOW, MONTH_MAX = 39 * DAY, 19          # <= 19 entries in any trailing 39 days
SQUEEZE_MIX = 0.625                             # plausible squeeze month: half the exits liquidated, half -25 %
MONTH_LOSS_LIMIT = -0.30                        # the mandate's worst-month line
PLAUSIBLE_MONTH = -SQUEEZE_MIX * MONTH_MAX * C_EVENT          # -0.297
STRUCTURAL_MONTH = -MONTH_MAX * C_EVENT                         # -0.475
STRUCTURAL_WEEK = -WEEK_MAX * C_EVENT                           # -0.175

# ------------------------------------------------------------------ inference (fixed; §5, §7, D8-D12)
TRIALS = ('T1', 'T2')
FAMILY = len(TRIALS)
Q_DISCOVERY = 1 - 0.05 / FAMILY                 # one-sided 2.5 % per trial (Bonferroni over the family of 2)
Q_CONFIRM = 0.95
# Calibrated critical values for the alpha t (Revision 1, decision 1; §3). `r013_run.py --power`: the real first-trade
# times, overlapping 7-day holds, a continuous alt market (7-day SD 10.4 %, x1.5 stress), beta 1 / 1.5 / 2, alt drift
# 0 / 1 / 2 %/week, idiosyncratic k 1 / 2, t_4.5 tails; 1,500 draws per world.
#   DISCOVERY STAGE, t_{G-2}(0.975) per trial: family-wise size 0.008-0.042 (nominal 0.05): within nominal + 0.01 -> no
#     calibration (CAL 0).
#   CONFIRMATION, t_{G-2}(0.95): size 0.040-0.064 (nominal 0.05): above nominal + 0.01 in one world -> calibrated to the
#     largest 95th percentile of the alpha t over every null world and either trial: 1.90 (k 1, beta 1.5, drift 1 %, T2).
# The test uses max(t_{G-2}(q), CAL).
CAL_ALPHA_DISCOVERY = 0.0
CAL_ALPHA_CONFIRM = 1.90
MIN_EVENTS, MIN_WEEKS = 20, 10
MIN_EVENTS_CONF, MIN_WEEKS_CONF = 10, 5
ECON_MEAN = 0.01                                # mean RAW net per event >= 100 bp (economic bar)
ECON_ANNUAL = 0.03                              # book on capital >= HLP's lending leg (3 %/yr)
PLACEBO_SEEDS = 200
INDEX_TRIM = 0.05                               # market index: 5 %-each-tail trimmed mean of seasoned perps
INDEX_MIN = 20

UNIV = re.compile(r'^[A-Z0-9]+USDT$')
NOT_COIN = {'BTCDOMUSDT', 'DEFIUSDT', 'BLUEBIRDUSDT', 'FOOTBALLUSDT', 'USDCUSDT'}   # index composites, stablecoin
SEG_GAP_DAYS = 7

# ------------------------------------------------------------------ continuations (Revision 1, decision 5)
# Rule (fixed now, from mechanism and point-in-time public facts; names only): an event is a CONTINUATION, out of
# T1/T2 and reported as its own stratum, when its token is not new at listing:
#   (a) a ticker change / migration / redenomination of a token that had a Binance USDⓈ-M perp, the new perp
#       listed within 90 days of the change;
#   (b) a relisting, under a new ticker, of a token whose Binance USDⓈ-M perp was delisted earlier;
#   (c) a rebrand or token swap of a token without a Binance perp, completed <= 180 days before d0;
#   (d) a commodity- or fiat-backed token.
# Every change is announced before the new perp lists, so the category is known at entry. Names from the
# drafter's knowledge of public announcements; '?' marks the ones a reviewer should verify (§2.5).
CONTINUATIONS = {
    # discovery
    'POLUSDT@2024-09-13': 'a MATIC->POL', 'RENDERUSDT@2024-07-26': 'a RNDR->RENDER', 'GUSDT@2024-08-15': 'a GAL->G',
    'KAIAUSDT@2024-12-04': 'a KLAY->KAIA', 'SUSDT@2025-01-16': 'a FTM->S', 'DUSDT@2025-01-09': 'a DAR->D',
    'HEIUSDT@2025-02-13': 'a LIT->HEI', 'FORMUSDT@2025-03-19': 'a BNX->FORM', 'AWEUSDT@2025-05-21': 'a STPT->AWE',
    'AUSDT@2025-05-28': 'a EOS->A', 'PUMPBTCUSDT@2025-06-13': 'a PUMPUSDT->PUMPBTC ?',
    'RAYSOLUSDT@2024-12-10': 'b RAY relisted', 'VICUSDT@2025-03-12': 'b TOMO relisted as VIC',
    'LUMIAUSDT@2024-12-18': 'c ORN->LUMIA', 'SYRUPUSDT@2025-05-07': 'c MPL->SYRUP',
    'PAXGUSDT@2025-03-27': 'd gold-backed',
    # confirmation
    'SKYUSDT@2025-09-09': 'a MKR->SKY', 'FRAXUSDT@2026-01-15': 'a FXS->FRAX', 'MANTRAUSDT@2026-03-04': 'a OM->MANTRA',
    'NOMUSDT@2025-10-01': 'a OMNI->NOM', 'CFGUSDT@2026-03-16': 'c CFG migration ?', 'ASTERUSDT@2025-09-19': 'c APX->ASTER ?',
    'XAUTUSDT@2026-03-26': 'd gold-backed',
    # reported block (reported stratum only)
    '1000BTTCUSDT@2022-01-26': 'a BTT redenomination', 'TUSDT@2023-02-01': 'a KEEP+NU->T',
    '1000LUNCUSDT@2022-09-09': 'b LUNA (classic) relisted', 'USTCUSDT@2023-11-27': 'b UST (classic) listed',
    'DODOXUSDT@2023-08-08': 'b DODO relisted ?', 'BEAMXUSDT@2023-11-17': 'c MC->BEAM',
}


# ------------------------------------------------------------------ events (no market value is used)
def excluded_symbols(exchange_info):
    out = set(NOT_COIN)
    for s in exchange_info.get('symbols', []):
        if s.get('contractType') == 'TRADIFI_PERPETUAL' or s.get('underlyingType') not in (None, 'COIN'):
            out.add(s['symbol'])
    return out


def segments(days, gap_days=SEG_GAP_DAYS):
    seg = []
    for x in sorted(days):
        if seg and (dt.date.fromisoformat(x) - dt.date.fromisoformat(seg[-1][1])).days <= gap_days:
            seg[-1][1] = x
        else:
            seg.append([x, x])
    return seg


def events_from_listing(days_by_symbol, excluded):
    """{symbol: [ISO days of daily 1d files, all < cutoff]} -> event rows: the FIRST file of each universe symbol.
    Archive files of dead contracts never stop (scout L4 §B), so file gaps cannot detect relistings; later TRADING
    segments are found from trade counts by `later_trading_segments` and are never events."""
    out = []
    for s in sorted(days_by_symbol):
        if not UNIV.match(s) or s in excluded:
            continue
        days = sorted(d for d in days_by_symbol[s] if d < CUTOFF_DAY)
        if not days:
            continue
        out.append({'id': f'{s}@{days[0]}', 'symbol': s, 'd0': days[0], 'kind': 'listing',
                    'censored': int(days[0] <= ARCHIVE_START)})
    return out


def later_trading_segments(traded_hours, not_before, gap=SEG_GAP_DAYS * DAY):
    """Starts of trading runs (hours with trades) that follow >= `gap` without trades, using only hours >=
    `not_before` (for an event symbol: its d0 + 30 days, so nothing about its own window is examined)."""
    hs = sorted(h for h in traded_hours if h >= not_before)
    return [b for a, b in zip(hs, hs[1:]) if b - a > gap]


PREMARKET_FROM = '2025-01-01'


def is_premarket(d0, index_file_on_d0):
    return d0 >= PREMARKET_FROM and not index_file_on_d0


def calendar_of_day(d0):
    t = ms(d0)
    for role, (a, b) in CALENDARS.items():
        if a <= t < b:
            return role
    return 'embargo' if any(a <= t < b for a, b in EMBARGO) else 'outside'


def calendar_of(first_trade):
    for role, (a, b) in CALENDARS.items():
        if a <= first_trade < b:
            return role
    return 'embargo' if any(a <= first_trade < b for a, b in EMBARGO) else 'outside'


def pull_days(d0):
    d = dt.date.fromisoformat(d0)
    return [x for x in ((d + dt.timedelta(days=i)).isoformat() for i in range(PULL_DAYS)) if x < CUTOFF_DAY]


def months_between(a, b):
    """Calendar months (YYYY-MM) touched by [a, b] (ms)."""
    out, m = [], day_of(a)[:7]
    while m <= day_of(b)[:7]:
        out.append(m)
        y, mo = map(int, m.split('-'))
        m = f'{y + (mo == 12)}-{mo % 12 + 1:02d}'
    return out


def iso_week(t):
    y, w, _ = dt.datetime.fromtimestamp(t / 1000, dt.UTC).isocalendar()
    return f'{y}-W{w:02d}'


def month_of(t):
    return day_of(t)[:7]


def slip_tiers(avg_hourly_qv):
    """(entry slippage, exit slippage = one tier worse) per fill."""
    vals = [s for _, s in SLIP_TIERS] + [SLIP_WORSE]
    for i, (floor, s) in enumerate(SLIP_TIERS):
        if avg_hourly_qv >= floor:
            return s, vals[i + 1]
    return SLIP_TIERS[-1][1], SLIP_WORSE


# ------------------------------------------------------------------ one event (the trade)
def simulate(ev, bars, funding, end, fee=FEE_DECISION, slip_mult=1.0, pay_mult=1.0, receive_mult=1.0):
    """One short. ev: {'id', 'first_trade'}; bars: [(t, o, h, l, c, qv, n)] sorted by t (1m opens, ms), all < end;
    funding: [(calc_time_ms, interval_h, rate)] sorted. Returns a dict; status in
      no_entry          no TRADED bar within 60 min of first trade + 24 h (counted, no return)
      exit              exit at the first traded bar in [t_x, t_x + 6 h)
      exit_late         no traded bar in that window, but trading resumes later (a halt or a data hole)
      exit_last_trade   trading never resumes before the data end: exit at the LAST TRADED close (delisting;
                        zero-trade rows after the last traded bar are treated as absent, review B M1)
      liquidated        -1 (the whole isolated margin)
    Settlement s counts iff entry bar t < floor_min(s) <= exit time, paid at the price at s: rate * P_s / P_in.
    Liquidation at the first held bar whose last-price high H gives (1 - fee) + F + (P_e - H)/P_in <= MMR * H/P_in.
    'marks': [(UTC midnight, unrealised net)] while open (daily mark to market, entry costs paid)."""
    traded = [b for b in bars if b[6] > 0]
    t_in = ev['first_trade'] + ENTRY_DELAY
    eb = next((b for b in traded if t_in <= b[0] < min(t_in + ENTRY_WINDOW, end)), None)
    if eb is None:
        return {'id': ev['id'], 'status': 'no_entry'}
    qv24 = sum(b[5] for b in bars if t_in - 24 * HOUR <= b[0] < t_in)
    s_in, s_out = (x * slip_mult for x in slip_tiers(qv24 / 24))
    p_in = eb[1]
    t_x = t_in + HOLD
    xb = next((b for b in traded if t_x <= b[0] < min(t_x + EXIT_WINDOW, end)), None)
    status = 'exit'
    if xb is None:
        xb = next((b for b in traded if t_x + EXIT_WINDOW <= b[0] < end), None)
        status = 'exit_late'
    if xb is not None:
        held = [b for b in traded if eb[0] <= b[0] < xb[0]]
        p_x, exit_t = xb[1], xb[0]
    else:
        held = [b for b in traded if b[0] >= eb[0]]
        p_x, exit_t, status = held[-1][4], held[-1][0] + MIN, 'exit_last_trade'
    opens = {b[0]: b[1] for b in traded}
    tts = [b[0] for b in traded]

    def price_at(m):
        if m in opens:
            return opens[m]
        i = bisect.bisect_left(tts, m) - 1
        return traded[i][4] if i >= 0 else p_in
    sett = []
    for calc, _iv, rate in funding:
        m = calc - calc % MIN
        if eb[0] < m <= exit_t:
            r = rate * (pay_mult if rate < 0 else receive_mult)
            if pay_mult != 1.0 or receive_mult != 1.0:
                r = max(-FUNDING_CAP, min(FUNDING_CAP, r))
            sett.append((m, r * price_at(m) / p_in))
    p_e = p_in * (1 - s_in)
    k, F, marks = 0, 0.0, []
    midnight = eb[0] - eb[0] % DAY + DAY
    last_c = p_in
    for b in held:
        while midnight <= b[0] and midnight < exit_t:
            Fm = sum(x[1] for x in sett if x[0] <= midnight)
            marks.append((midnight, -fee - s_in + (p_in - last_c) / p_in + Fm))
            midnight += DAY
        while k < len(sett) and sett[k][0] <= b[0]:
            F += sett[k][1]
            k += 1
        if (1 - fee) + F + (p_e - b[2]) / p_in <= MMR * b[2] / p_in:
            return {'id': ev['id'], 'status': 'liquidated', 'liquidated': True, 'entry_t': eb[0], 'exit_t': b[0],
                    'p_in': p_in, 'net': -1.0, 'price': -1.0 - F + fee + s_in, 'funding': F, 'fees': fee,
                    'slip': s_in, 'n_settlements': k, 'marks': marks, 'slip_tier': s_in}
        last_c = b[4]
    F = sum(x[1] for x in sett)
    price = (p_in - p_x) / p_in
    fees = fee + fee * p_x / p_in
    slip = s_in + s_out * p_x / p_in
    net = max(-1.0, price + F - fees - slip)
    return {'id': ev['id'], 'status': status, 'liquidated': False, 'entry_t': eb[0], 'exit_t': exit_t,
            'p_in': p_in, 'p_x': p_x, 'net': net, 'price': price, 'funding': F, 'fees': fees, 'slip': slip,
            'n_settlements': len(sett), 'marks': marks, 'slip_tier': s_in}


def funding_before_entry(first_trade, entry_t, funding):
    """T2's feature, known at entry: sum of rates settled in (first trade, entry bar t]; None if none."""
    rs = [r for calc, _iv, r in funding if first_trade < calc - calc % MIN <= entry_t]
    return sum(rs) if rs else None


def trimmed_mean(xs, trim=INDEX_TRIM):
    s = sorted(xs)
    k = int(len(s) * trim)
    s = s[k:len(s) - k] if len(s) - 2 * k > 0 else s
    return sum(s) / len(s)


# ------------------------------------------------------------------ statistics (no numpy)
def _betacf(a, b, x):
    qab, qap, qam = a + b, a + 1, a - 1
    c, d = 1.0, 1 - qab * x / qap
    d = 1 / (d if abs(d) > 1e-300 else 1e-300)
    h = d
    for m in range(1, 300):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1 + aa * d
        d = 1 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1 + aa / c
        c = c if abs(c) > 1e-300 else 1e-300
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1 + aa * d
        d = 1 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1 + aa / c
        c = c if abs(c) > 1e-300 else 1e-300
        de = d * c
        h *= de
        if abs(de - 1) < 1e-14:
            break
    return h


def _ibeta(a, b, x):
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    lb = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1 - x)
    if x < (a + 1) / (a + b + 2):
        return math.exp(lb) * _betacf(a, b, x) / a
    return 1 - math.exp(lb) * _betacf(b, a, 1 - x) / b


def t_cdf(t, df):
    x = df / (df + t * t)
    tail = 0.5 * _ibeta(df / 2, 0.5, x)
    return 1 - tail if t >= 0 else tail


@functools.lru_cache(maxsize=None)
def t_crit(df, q):
    if df < 1:
        return math.inf
    lo, hi = 0.0, 1000.0
    for _ in range(200):
        mid = (lo + hi) / 2
        if t_cdf(mid, df) < q:
            lo = mid
        else:
            hi = mid
    return round((lo + hi) / 2, 6)


def clustered_t(values, clusters):
    """Mean and its cluster-robust t (CR1), clusters = listing weeks. (mean, t, G); t None if G < 2 or var 0."""
    n = len(values)
    if n == 0:
        return None, None, 0
    m = sum(values) / n
    g = {}
    for v, c in zip(values, clusters):
        g[c] = g.get(c, 0.0) + (v - m)
    G = len(g)
    if G < 2:
        return m, None, G
    var = G / (G - 1) * sum(x * x for x in g.values()) / (n * n)
    return m, (m / math.sqrt(var) if var > 0 else None), G


def ols_alpha(y, x, clusters):
    """Pooled OLS y = a + b x + e with CR1 cluster-robust SEs (G/(G-1) * (n-1)/(n-2)).
    Returns {'alpha', 'beta', 't', 'G', 'n'}; t None when undefined."""
    n = len(y)
    out = {'alpha': None, 'beta': None, 't': None, 'G': len(set(clusters)), 'n': n}
    if n < 3:
        return out
    sx, sy = sum(x), sum(y)
    sxx = sum(v * v for v in x)
    sxy = sum(a * b for a, b in zip(x, y))
    det = n * sxx - sx * sx
    if det <= 0:
        return out
    b = (n * sxy - sx * sy) / det
    a = (sy - b * sx) / n
    i00, i01, i11 = sxx / det, -sx / det, n / det            # (X'X)^-1
    g = {}
    for yi, xi, c in zip(y, x, clusters):
        e = yi - a - b * xi
        u0, u1 = g.get(c, (0.0, 0.0))
        g[c] = (u0 + e, u1 + e * xi)
    G = len(g)
    out.update(alpha=a, beta=b, G=G)
    if G < 2:
        return out
    m00 = sum(u0 * u0 for u0, _ in g.values())
    m01 = sum(u0 * u1 for u0, u1 in g.values())
    m11 = sum(u1 * u1 for _, u1 in g.values())
    v00 = i00 * (i00 * m00 + i01 * m01) + i01 * (i00 * m01 + i01 * m11)
    v00 *= G / (G - 1) * (n - 1) / (n - 2)
    out['t'] = a / math.sqrt(v00) if v00 > 0 else None
    return out


def alpha_crit(G, q):
    cal = CAL_ALPHA_DISCOVERY if q == Q_DISCOVERY else CAL_ALPHA_CONFIRM
    return max(t_crit(G - 2, q), cal) if G >= 3 else None


def halves(values, clusters):
    weeks = sorted(set(clusters))
    first = set(weeks[:(len(weeks) + 1) // 2])
    a = [v for v, c in zip(values, clusters) if c in first]
    b = [v for v, c in zip(values, clusters) if c not in first]
    return (sum(a) / len(a) if a else None), (sum(b) / len(b) if b else None)


def largest_positive_week(values, clusters):
    s = {}
    for v, c in zip(values, clusters):
        s[c] = s.get(c, 0.0) + v
    pos = [(x, c) for c, x in s.items() if x > 0]
    return max(pos)[1] if pos else None


def without_largest_positive_week(values, clusters):
    drop = largest_positive_week(values, clusters)
    rest = [v for v, c in zip(values, clusters) if c != drop]
    return sum(rest) / len(rest) if rest else None


# ------------------------------------------------------------------ the book (caps, annualisation, tails, Kelly)
def take(rows):
    """Entries in entry order while the trailing 15-day window holds < WEEK_MAX and the trailing 39-day window
    < MONTH_MAX book entries (causal)."""
    taken, times = [], []
    for r in sorted(rows, key=lambda r: (r['entry_t'], r['id'])):
        t = r['entry_t']
        if sum(1 for x in times if t - x < WEEK_WINDOW) < WEEK_MAX and sum(1 for x in times if t - x < MONTH_WINDOW) < MONTH_MAX:
            taken.append(r)
            times.append(t)
    return taken


def book(rows, span_years, c=C_EVENT):
    """P&L (c * net) booked at EXIT; worst week / month over exit ISO weeks / calendar months (carried-in positions
    included by construction); realised max drawdown; DAILY MARK-TO-MARKET drawdown from each open position's
    'marks' (if present); Kelly per event = mu_w / var_w over every calendar week of the span (per unit c)."""
    taken = take(rows)
    pnl_w, pnl_m = {}, {}
    for r in taken:
        pnl_w[iso_week(r['exit_t'])] = pnl_w.get(iso_week(r['exit_t']), 0.0) + c * r['net']
        pnl_m[month_of(r['exit_t'])] = pnl_m.get(month_of(r['exit_t']), 0.0) + c * r['net']
    cum = peak = dd = 0.0
    for r in sorted(taken, key=lambda r: r['exit_t']):
        cum += c * r['net']
        peak = max(peak, cum)
        dd = min(dd, cum - peak)
    mtm_dd = None
    if taken and all('marks' in r for r in taken):
        days = sorted({m for r in taken for m, _ in r['marks']} | {r['exit_t'] - r['exit_t'] % DAY + DAY for r in taken})
        peak_m, mtm_dd = 0.0, 0.0
        for D in days:
            eq = 0.0
            for r in taken:
                if r['exit_t'] < D:
                    eq += c * r['net']
                elif r['entry_t'] < D:
                    mk = [v for m, v in r['marks'] if m <= D]
                    eq += c * (mk[-1] if mk else 0.0)
            peak_m = max(peak_m, eq)
            mtm_dd = min(mtm_dd, eq - peak_m)
    n_weeks = max(1, round(span_years * 52.18))
    wvals = list(pnl_w.values()) + [0.0] * max(0, n_weeks - len(pnl_w))
    mu = sum(wvals) / len(wvals)
    var = sum((x - mu) ** 2 for x in wvals) / max(1, len(wvals) - 1)
    kelly = (mu / c) / (var / (c * c)) if var > 0 else None
    total = sum(c * r['net'] for r in taken)
    return {'events_taken': len(taken), 'events_skipped_by_caps': len(rows) - len(taken), 'c': c,
            'events_per_year': len(taken) / span_years if span_years > 0 else None,
            'total_on_capital': total, 'annual_on_capital': total / span_years if span_years > 0 else None,
            'worst_event': min((c * r['net'] for r in taken), default=None),
            'worst_week': min(pnl_w.values(), default=None), 'worst_month': min(pnl_m.values(), default=None),
            'monthly': pnl_m, 'max_drawdown_realised': dd, 'max_drawdown_daily_mtm': mtm_dd,
            'kelly_fraction_per_event': kelly, 'quarter_kelly_per_event': kelly / 4 if kelly is not None else None}


def squeeze_months(monthly, c=C_EVENT):
    """Exit months whose book P&L at c is at or below half the plausible squeeze month at c."""
    line = 0.5 * SQUEEZE_MIX * MONTH_MAX * c
    return sorted(m for m, v in monthly.items() if v <= -line)


def deploy(conf_rows, conf_stress_rows, pooled, span_conf):
    """THE deploy rule (Revision 1, decision 3; one definition, for the carried trial, at the deployed size).
    conf_rows / conf_stress_rows: the carried trial's confirmation events (base / stressed costs).
    pooled: {calendar: (rows, stress_rows, span_years)} for reported, discovery and confirmation, carried rule.
      c*  = min(C_EVENT, quarter Kelly of the CONFIRMATION book, C_EVENT * (-0.30) / W) where W is the worst exit
            month at C_EVENT over the pooled calendars, base and stressed costs.
      Peso charges (decision 2): pi = liquidations per event; mu_tail = mu_conf - max(0, pi_pool - pi_conf) *
            (1 + mean net of the confirmation's non-liquidated events); squeeze months per year f (`squeeze_months`)
            with f_eff = max(f_pool, 1 / Y_pool); the annual book at c* is charged
            (mu_conf - mu_tail) * events per year * c*  +  max(0, f_eff - f_conf) * |plausible month at c*|.
      Bars: mu_tail >= 100 bp and the charged annual book at c* >= 3 %/yr."""
    b0 = book(conf_rows, span_conf)
    worst = []
    for rows, srows, span in pooled.values():
        for rs in (rows, srows):
            w = book(rs, span)['worst_month'] if rs else None
            if w is not None:
                worst.append(w)
    w = min(worst) if worst else None
    cands = [C_EVENT]
    qk = b0['quarter_kelly_per_event']
    if qk is not None:
        cands.append(max(0.0, qk))
    if w is not None and w < MONTH_LOSS_LIMIT:
        cands.append(C_EVENT * MONTH_LOSS_LIMIT / w)
    c_star = min(cands)
    pool_rows = [r for rows, _s, _y in pooled.values() for r in rows]
    y_pool = sum(y for _r, _s, y in pooled.values())
    pi_pool = sum(r['liquidated'] for r in pool_rows) / len(pool_rows) if pool_rows else 0.0
    pi_conf = sum(r['liquidated'] for r in conf_rows) / len(conf_rows) if conf_rows else 0.0
    nonliq = [r['net'] for r in conf_rows if not r['liquidated']]
    mu_conf = sum(r['net'] for r in conf_rows) / len(conf_rows) if conf_rows else None
    mu_tail = (mu_conf - max(0.0, pi_pool - pi_conf) * (1 + (sum(nonliq) / len(nonliq) if nonliq else 0.0))
               if mu_conf is not None else None)
    sq_pool = sum(len(squeeze_months(book(rows, y)['monthly'])) for rows, _s, y in pooled.values() if rows)
    f_eff = max(sq_pool / y_pool, 1 / y_pool) if y_pool > 0 else None
    f_conf = len(squeeze_months(b0['monthly'])) / span_conf
    annual_star = b0['annual_on_capital'] * c_star / C_EVENT if b0['annual_on_capital'] is not None else None
    plaus_star = SQUEEZE_MIX * MONTH_MAX * c_star
    charge = ((mu_conf - mu_tail) * (b0['events_per_year'] or 0) * c_star if mu_tail is not None else 0.0) \
        + (max(0.0, f_eff - f_conf) * plaus_star if f_eff is not None else 0.0)
    annual_charged = annual_star - charge if annual_star is not None else None
    bars = {'mean_tail_ge_100bp': mu_tail is not None and mu_tail >= ECON_MEAN,
            'book_charged_ge_3pct_yr': annual_charged is not None and annual_charged >= ECON_ANNUAL,
            'size_positive': c_star > 0}
    return {'c_star': c_star, 'worst_month_pooled_at_c0': w, 'quarter_kelly_conf': qk, 'pi_pool': pi_pool,
            'pi_conf': pi_conf, 'mu_conf': mu_conf, 'mu_tail': mu_tail, 'squeeze_months_pool': sq_pool,
            'f_eff_per_year': f_eff, 'f_conf_per_year': f_conf, 'annual_at_c_star': annual_star,
            'tail_charge_per_year': charge, 'annual_charged': annual_charged, 'bars': bars,
            'deploy': all(bars.values())}


# ------------------------------------------------------------------ criteria and decisions
def trial_stats(rows, span_years, q):
    """rows: events of one trial (dicts with 'net', 'week', 'entry_t', 'exit_t', 'id', 'mkt' (market return over the
    hold, or None)). Every number the criteria use, computed from the per-event records.
    Market-adjusted series (Revision 1): alpha and beta from the pooled OLS net = alpha + beta * mkt over the events
    with a market return; adj_i = net_i - beta * mkt_i (mean(adj) = alpha). The robustness criteria use adj; the raw
    net feeds the economic bars and is reported."""
    v = [r['net'] for r in rows]
    cl = [r['week'] for r in rows]
    m, t, G = clustered_t(v, cl)
    adj_rows = [r for r in rows if r.get('mkt') is not None]
    a = ols_alpha([r['net'] for r in adj_rows], [r['mkt'] for r in adj_rows], [r['week'] for r in adj_rows])
    b = a['beta'] if a['beta'] is not None else 0.0
    adj = [r['net'] - b * r['mkt'] for r in adj_rows]
    acl = [r['week'] for r in adj_rows]
    h1, h2 = halves(adj, acl)
    drop = largest_positive_week(adj, acl)
    keep = [r for r in adj_rows if r['week'] != drop]
    a_wo = ols_alpha([r['net'] for r in keep], [r['mkt'] for r in keep], [r['week'] for r in keep])
    bk = book(rows, span_years)
    return {'n': len(v), 'G': G, 'mean': m, 't': t, 'median_raw': statistics.median(v) if v else None, 'q': q,
            'alpha': a['alpha'], 'alpha_beta': a['beta'], 'alpha_t': a['t'], 'alpha_G': a['G'], 'alpha_n': a['n'],
            'alpha_crit': alpha_crit(a['G'], q), 'adj_median': statistics.median(adj) if adj else None,
            'adj_half_1': h1, 'adj_half_2': h2, 'alpha_without_largest_positive_week': a_wo['alpha'],
            'annual_on_capital': bk['annual_on_capital'], 'span_years': span_years}


def discovery_criteria(s):
    """Significance and robustness on the MARKET-ADJUSTED series (alpha; adj = net - beta * mkt); the raw net only in
    the economic bars (orchestrator decision 1)."""
    def pos(x):
        return x is not None and x > 0
    acrit = alpha_crit(s.get('alpha_G', 0), Q_DISCOVERY)
    c = {
        '0_enough_events_and_weeks': s['alpha_n'] >= MIN_EVENTS and s['alpha_G'] >= MIN_WEEKS,
        '1_alpha_pos_t_ge_crit': pos(s['alpha']) and s['alpha_t'] is not None and acrit is not None and s['alpha_t'] >= acrit,
        '2_adj_median_pos': pos(s['adj_median']),
        '3_adj_both_halves_pos': pos(s['adj_half_1']) and pos(s['adj_half_2']),
        '4_alpha_without_largest_positive_week_pos': pos(s['alpha_without_largest_positive_week']),
        '5_econ_raw_mean_ge_100bp': s['mean'] is not None and s['mean'] >= ECON_MEAN,
        '6_econ_book_ge_3pct_yr': s['annual_on_capital'] is not None and s['annual_on_capital'] >= ECON_ANNUAL,
    }
    return c, all(c.values()), acrit


def decide_discovery(passes):
    if passes.get('T1'):
        return ['read_confirmation', 'T1']
    if passes.get('T2'):
        return ['read_confirmation', 'T2']
    return ['close', None]


def records_by_trial(events):
    """Per-event compact records of a row -> {trial: rows} (T1: entered, primary; T2: T1 with t2)."""
    t1 = [e for e in events if e.get('status') not in (None, 'no_entry') and e.get('primary')]
    return {'T1': t1, 'T2': [e for e in t1 if e.get('t2')]}


STAT_KEYS = ('n', 'G', 'mean', 't', 'median_raw', 'alpha', 'alpha_beta', 'alpha_t', 'alpha_G', 'alpha_n', 'adj_median',
             'adj_half_1', 'adj_half_2', 'alpha_without_largest_positive_week', 'annual_on_capital')


def _close(a, b):
    if a is None or b is None:
        return a is b
    return abs(a - b) <= 1e-9 * max(1.0, abs(a), abs(b))


def recheck_discovery(row):
    """(decision, [failures]) recomputed from a stored discovery row: the trial statistics are recomputed from
    the row's PER-EVENT records (review B m8) and compared with the stored aggregates; every criterion, each
    trial's pass, the critical values and the decision are recomputed; any disagreement is a failure."""
    fails, passes = [], {}
    res = (row or {}).get('trials') or {}
    recs = records_by_trial((row or {}).get('events') or [])
    span = (row or {}).get('span_years')
    for tr in TRIALS:
        s = res.get(tr)
        if not isinstance(s, dict):
            fails.append(f'{tr}: no stored statistics')
            passes[tr] = False
            continue
        try:
            re_s = trial_stats(recs[tr], span, Q_DISCOVERY) if span else None
            if re_s is None or any(not _close(re_s[k], s.get(k)) for k in STAT_KEYS):
                fails.append(f'{tr}: stored statistics differ from those recomputed from the per-event records')
            c, ok, crit = discovery_criteria(s)
        except (KeyError, TypeError) as e:
            fails.append(f'{tr}: malformed statistics ({e!r})')
            passes[tr] = False
            continue
        if s.get('criteria') != c:
            fails.append(f'{tr}: stored criteria differ from those recomputed from the numbers')
        if s.get('pass') is not ok:
            fails.append(f'{tr}: stored pass {s.get("pass")} differs from the recomputed {ok}')
        if crit is not None and (s.get('alpha_crit') is None or abs(s['alpha_crit'] - crit) > 1e-6):
            fails.append(f'{tr}: stored critical value differs from max(t_(G-2)({Q_DISCOVERY}), CAL)')
        passes[tr] = ok and not any(f.startswith(f'{tr}:') for f in fails)
    dec = decide_discovery(passes)
    if (row or {}).get('decision') != dec:
        fails.append(f'stored decision {(row or {}).get("decision")} differs from the recomputed {dec}')
    return dec, fails


def confirm(s):
    """The existence test on the carried trial, market-adjusted: alpha > 0, alpha t >= max(t_{G-2}(0.95), CAL), and alpha
    > 0 without the listing week with the largest positive adjusted sum (its power cost is 0.000 in every simulated
    world, <= 0.03: orchestrator decision 1)."""
    crit = alpha_crit(s.get('alpha_G', 0), Q_CONFIRM)
    wo = s.get('alpha_without_largest_positive_week')
    ok = (s['alpha_n'] >= MIN_EVENTS_CONF and s['alpha_G'] >= MIN_WEEKS_CONF and s['alpha'] is not None
          and s['alpha'] > 0 and s['alpha_t'] is not None and crit is not None and s['alpha_t'] >= crit
          and wo is not None and wo > 0)
    return ok, crit


def decide_confirmation(ok, dep):
    if not ok:
        return 'not_confirmed'
    return 'confirm_paper' if dep['deploy'] else 'confirm_below_bar'


# ------------------------------------------------------------------ placebo (reported)
def placebo(rows, crit, seeds=PLACEBO_SEEDS):
    """Calendar-MONTH-block sign flips of the gross (net + costs), costs kept (adjacent weeks flip together,
    keeping most of the overlap covariance; review A M1). Share of seeds with t >= crit; the |t| >= crit share
    of the flipped gross; the SD of the flipped gross t."""
    if len(rows) < 3:
        return None
    months = sorted({month_of(r['entry_t']) for r in rows})
    hits = hits_gross = 0
    ts = []
    for seed in range(seeds):
        rnd = random.Random(seed)
        sgn = {m: rnd.choice((-1, 1)) for m in months}
        cost = [r['fees'] + r['slip'] for r in rows]
        gross = [sgn[month_of(r['entry_t'])] * (r['net'] + c) for r, c in zip(rows, cost)]
        net = [g - c for g, c in zip(gross, cost)]
        cl = [r['week'] for r in rows]
        _, t, _ = clustered_t(net, cl)
        _, tg, _ = clustered_t(gross, cl)
        hits += t is not None and t >= crit
        hits_gross += tg is not None and abs(tg) >= crit
        ts.append(tg if tg is not None else 0.0)
    return {'seeds': seeds, 'pass_share_net_t': hits / seeds, 'abs_t_share_gross': hits_gross / seeds,
            'gross_t_sd': statistics.pstdev(ts)}


# ------------------------------------------------------------------ simulation (SYNTHETIC only; §3)
NU = 4.5
SIGMA_E = 0.083                                 # scout §E: 7-d market-excess SD, non-event small-cap coin-weeks
SIGMA_M = math.sqrt(0.133 ** 2 - 0.083 ** 2)    # 7-d market SD (10.4 %)


def _tdraw(rnd, nu=NU):
    z = rnd.gauss(0, 1)
    chi = sum(rnd.gauss(0, 1) ** 2 for _ in range(int(nu))) + (nu - int(nu)) * rnd.gauss(0, 1) ** 2
    return z / math.sqrt(chi / nu) / math.sqrt(nu / (nu - 2))


def _market_path(t0, t1, msd, drift_wk, rnd):
    """Continuous alt market (log level) sampled daily, linear inside the day: daily SD msd*SIGMA_M/sqrt(7) (SIGMA_M =
    10.4 % per 7 days, the measured market part of non-event coin-weeks; msd 1 or a 1.5 stress), drift -drift_wk/7
    per day (a FALLING alt market for drift > 0, which a short earns beta * drift from)."""
    days = int((t1 - t0) // DAY) + 3
    lv, x = [0.0], 0.0
    for _ in range(days):
        x += rnd.gauss(-drift_wk / 7, msd * SIGMA_M / math.sqrt(7))
        lv.append(x)

    def at(t):
        u = (t - t0) / DAY
        i = max(0, min(int(u), days - 1))
        f = u - i
        return lv[i] + f * (lv[i + 1] - lv[i])
    return at


def sim_events(schedule, mu, beta, k, drift_wk, rnd, p_t2=0.7, liq_p=0.0, liq_week_p=0.0, msd=1.0):
    """Synthetic events at the REAL first-trade times: entry = first trade + 24 h, exit = entry + 7 d (overlapping
    holds); mkt = market return over the hold; net = mu - beta * mkt + k*SIGMA_E*t_4.5 (k scales the
    idiosyncratic part only: listing windows are more volatile than other coins, the alt market is not) (+ squeeze atoms: an
    event returns -1 w.p. liq_p, every event of a listing week w.p. liq_week_p; the non-atom mean is shifted so
    the event mean stays mu), clipped to [-1, 1]."""
    if not schedule:
        return []
    t0 = min(schedule) - DAY
    at = _market_path(t0, max(schedule) + 10 * DAY, msd, drift_wk, rnd)
    wk_hit = {}
    out = []
    shift = (mu + liq_p) / (1 - liq_p) if liq_p > 0 else mu
    for i, ft in enumerate(sorted(schedule)):
        e_t, x_t = ft + ENTRY_DELAY, ft + ENTRY_DELAY + HOLD
        mkt = math.exp(at(x_t) - at(e_t)) - 1
        w = iso_week(ft)
        if w not in wk_hit:
            wk_hit[w] = rnd.random() < liq_week_p
        if wk_hit[w] or rnd.random() < liq_p:
            net = -1.0
        else:
            net = max(-1.0, min(1.0, shift - beta * mkt + k * SIGMA_E * _tdraw(rnd)))
        out.append({'id': str(i), 'net': net, 'week': w, 'entry_t': e_t, 'exit_t': x_t, 'mkt': mkt,
                    't2': rnd.random() < p_t2, 'liquidated': net == -1.0})
    return out


def _rec(s, q, conf=False):
    """(alpha t, alpha G, every other condition of the stage) of one simulated trial, so the pass can be re-evaluated
    at any critical value."""
    if conf:
        other = (s['alpha_n'] >= MIN_EVENTS_CONF and s['alpha_G'] >= MIN_WEEKS_CONF and s['alpha'] is not None and s['alpha'] > 0
                 and (s['alpha_without_largest_positive_week'] or 0) > 0)
        other_no_wo = (s['alpha_n'] >= MIN_EVENTS_CONF and s['alpha_G'] >= MIN_WEEKS_CONF and s['alpha'] is not None
                       and s['alpha'] > 0)
        return (s['alpha_t'] if s['alpha_t'] is not None else -99.0, s['alpha_G'], other, other_no_wo)
    c, _ok, _ = discovery_criteria(s)
    other = all(v for k, v in c.items() if k != '1_alpha_pos_t_ge_crit') and s['alpha'] is not None and s['alpha'] > 0
    return (s['alpha_t'] if s['alpha_t'] is not None else -99.0, s['alpha_G'], other)


def simulate_worlds(disc_sched, conf_sched, disc_years, conf_years, worlds, sims=1000, seed=0):
    """Per world, `sims` draws of the registered path: discovery (both trials) and the confirmation calendar (both
    trials). Each draw stores (alpha t, G, other conditions) per stage and trial, so `world_rates` can evaluate the
    pass at any critical value (calibration and power from the same draws)."""
    res = []
    for wi, w in enumerate(worlds):
        rnd = random.Random(seed * 1000 + wi)
        recs = []
        for _ in range(sims):
            kw = dict(p_t2=w.get('p_t2', 0.7), liq_p=w.get('liq_p', 0.0), liq_week_p=w.get('liq_week_p', 0.0), msd=w.get('msd', 1.0))
            d = sim_events(disc_sched, w['mu_d'], w['beta'], w['k'], w['drift_d'], rnd, **kw)
            cd = sim_events(conf_sched, w['mu_c'], w['beta'], w['k'], w['drift_c'], rnd, **kw)
            r = {}
            for tr in TRIALS:
                dr = d if tr == 'T1' else [x for x in d if x['t2']]
                cr = cd if tr == 'T1' else [x for x in cd if x['t2']]
                r['d' + tr] = _rec(trial_stats(dr, disc_years, Q_DISCOVERY), Q_DISCOVERY)
                r['c' + tr] = _rec(trial_stats(cr, conf_years, Q_CONFIRM), Q_CONFIRM, conf=True)
            recs.append(r)
        res.append({**w, 'records': recs})
    return res


def world_rates(records, cal_d, cal_c):
    """Pass rates of the registered path at critical values max(t_{G-2}(q), cal)."""
    n = len(records)
    cd = lambda G: max(t_crit(G - 2, Q_DISCOVERY), cal_d) if G >= 3 else math.inf
    cc = lambda G: max(t_crit(G - 2, Q_CONFIRM), cal_c) if G >= 3 else math.inf
    tal = {'disc_T1': 0, 'disc_T2': 0, 'disc': 0, 'conf_given': 0, 'joint': 0, 'conf_alone': 0, 'conf_alone_no_wo': 0}
    for r in records:
        p = {tr: r['d' + tr][2] and r['d' + tr][0] >= cd(r['d' + tr][1]) for tr in TRIALS}
        tal['disc_T1'] += p['T1']
        tal['disc_T2'] += p['T2']
        t, G, other, other_no_wo = r['cT1']
        tal['conf_alone'] += other and t >= cc(G)
        tal['conf_alone_no_wo'] += other_no_wo and t >= cc(G)
        dec = decide_discovery(p)
        if dec[0] == 'read_confirmation':
            tal['disc'] += 1
            t, G, other, _ = r['c' + dec[1]]
            ok = other and t >= cc(G)
            tal['conf_given'] += ok
            tal['joint'] += ok
    out = {k: v / n for k, v in tal.items() if k != 'conf_given'}
    out['conf_given'] = tal['conf_given'] / tal['disc'] if tal['disc'] else None
    return out


def alpha_t_quantile(records, key, q):
    """The q-quantile of the alpha t of one stage and trial (key 'dT1', 'dT2', 'cT1', 'cT2') among draws whose other
    conditions hold with alpha > 0 counted as failing below 0 (the one-sided calibration target)."""
    ts = sorted((r[key][0] if r[key][2] else -99.0) for r in records)
    return ts[min(len(ts) - 1, int(q * len(ts)))]
