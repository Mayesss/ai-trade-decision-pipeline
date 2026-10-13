"""Registration 014 (light N2(b)) — a sized short-volatility book on Deribit BTC/ETH options. Pure
functions only: no I/O, no network, no data paths. The runner (`r014_run.py`) feeds them real data
behind its guard; the tests and the power simulation (`ct/shortvol_sim.py`) feed them synthetic data.

The primary construction (registration §4, fixed before any outcome):
  every Friday 08:00 UTC, per coin (BTC, ETH; equal USD notional), SELL the 7-day ATM straddle
  (the traded strike of the next Friday expiry nearest the index, within 2.5 %), marked from the
  latest Deribit trade at that strike in [entry - 4 h, entry) (mark IV inverted with Black-76 on that
  trade's index, re-priced at the entry index), filled at mark x (1 - half-spread), taker fees
  capped at 12.5 % of the price; DELTA-HEDGE on the coin's perpetual every HEDGE_EVERY_H hours from
  entry (perp close at the hedge time, taker fee + slippage, hourly funding); HOLD TO EXPIRY, settled
  on the 07:30-08:00 index TWAP (per-minute medians of the trades' index price; >= 10 of 30 minutes,
  else the perp close at 08:00), delivery fee on the ITM leg. Standard margin per currency account;
  an account whose equity falls below maintenance at an hourly mark is liquidated at that mark.

  T1  the book every week.
  T2  the same book standing aside (zero return, idle) in any week whose entry is in a STRESS state:
      for either coin, DVOL (last hourly bar closing <= entry - 1 h) / the minimum DVOL close of the
      bars closing in [entry - 7 d - 1 h, entry - 1 h] >= STRESS_RATIO (1.25).

Gates live ONLY in `discovery_criteria`, `decide_discovery`, `holdout_existence`, `decide_holdout`;
the outcome functions (`straddle_entry`, `position_pnl`, `weekly_book`) carry no pass/fail logic and
are shared by the primary and every reported variant.
"""
import datetime as dt
import math
import random
import re
import statistics

HOUR = 3_600_000
DAY = 86_400_000
WEEK = 7 * DAY
YEAR_D = 365.0
CUTOFF_MS = 1_782_864_000_000                 # 2026-07-01 00:00 UTC: nothing on or after is ever read


def ms(y, m, d, h=0, mi=0):
    return int(dt.datetime(y, m, d, h, mi, tzinfo=dt.timezone.utc).timestamp() * 1000)


def iso(t):
    return dt.datetime.fromtimestamp(int(t) / 1000, dt.timezone.utc).strftime('%Y-%m-%d %H:%M')


# ------------------------------------------------------------------ calendars (fixed now; §2)
DVOL_START = ms(2021, 3, 24)                  # first DVOL row
DISC_FIRST_ENTRY = ms(2021, 4, 2, 8)          # first Friday with 7 days of DVOL history
DISC_END = ms(2024, 1, 1)                     # discovery: every EXPIRY strictly before (2023-12-31 inclusive)
HOLD_START = ms(2024, 1, 1)                   # decision holdout: entries on/after ...
HOLD_END = ms(2026, 7, 1)                     # ... and every expiry strictly before 2026-07-01
CALENDARS = {'discovery': (DISC_FIRST_ENTRY, DISC_END), 'holdout': (HOLD_START, HOLD_END)}
COINS = ('BTC', 'ETH')

# ------------------------------------------------------------------ construction (fixed now; §4)
TENOR_D = 7
ENTRY_HOUR = 8
MARK_WINDOW_H = 4                             # marks from trades in [entry - 4 h, entry)
ATM_BAND = 0.025                              # the chosen strike lies within 2.5 % of the entry index
INDEX_MAX_AGE = 30 * 60_000                   # entry index from a trade <= 30 min old, else the perp close
SETTLE_WINDOW = 30 * 60_000                   # Deribit settles on the 07:30-08:00 index TWAP
SETTLE_MIN_MINUTES = 10                       # minutes with >= 1 trade needed, else the perp close
HEDGE_EVERY_H = 24                            # §3's rule: tied with 8 h on power at VRP 0.10, smaller fat-world tail
BETA_IV = 1.0                                 # leg IV at time t = entry IV + BETA_IV x (DVOL_t - DVOL_entry)
IV_FLOOR = 0.05

# costs (§4.3). Half-spreads are the scout's contemporaneous trade-vs-mark p75 for 3-7 d, 30-45 delta
# options (the ATM-most bucket measured), as a fraction of the premium; p50 is a reported variant.
OPT_FEE = 0.0003                              # of the underlying per contract per leg ...
OPT_FEE_CAP = 0.125                           # ... capped at 12.5 % of the option price
DELIV_FEE = 0.00015                           # delivery (weekly expiries), capped at 12.5 % of the value
DELIV_CAP = 0.125
HALF_SPREAD = {'BTC': 0.0260, 'ETH': 0.0231}
HALF_SPREAD_P50 = {'BTC': 0.0039, 'ETH': 0.0083}
PERP_TAKER = 0.0005
PERP_SLIP = 0.0001

# margin (§4.4): Deribit standard margin, one account per currency (BTC- and ETH-margined accounts are
# separate). Short option: IM = max(0.15 - OTM/S, 0.10) + mark/S, MM = 0.075 + mark/S (coin per
# contract, times S in USD); perpetual MM 1 % of the hedge notional.
OPT_MM = 0.075
PERP_MM = 0.01

# stress rule (T2; §4.6). DVOL at least 25 % above its trailing 7-day minimum (scale-free, same for both
# coins: their 7-day DVOL-change SDs relative to level are alike, 8.4/59.4 and 9.9/72.6, per the scout).
STRESS_RATIO = 1.25
STRESS_LOOKBACK = 7 * DAY
STRESS_MIN_BARS = 120                         # fewer DVOL bars in the lookback -> undefined (trade; counted)

# sizing and tail (§5; owner decision D9)
SIZE_N = 0.40                                 # book notional per unit of equity (each currency account: n); D9 (000m)
HLP_BAR = 0.03                                # economic bar, per year on equity (HLP's lending leg)
# REPORTED STRESS since 000m: adversarial stylised book losses per unit notional (scout's printed week
# excursions; premium 7 % / 9 % of spot; book = 1/2 BTC + 1/2 ETH): the whole excursion as ONE gap from the
# strike between two hedges. It still caps the size (worst week <= 25 % of equity at n).
ADVERSARIAL_WEEK = {                          # event: (BTC, ETH, book)
    '2020-03-06': (-0.534, -0.542, -0.538),
    '2021-01-01': (-0.320, -0.663, -0.4915),
    '2021-05-14': (-0.361, -0.445, -0.403),
    '2022-06-10': (-0.264, -0.345, -0.3045),
    '2022-11-04': (-0.183, -0.240, -0.2115),
    '2025-10-10': (-0.089, -0.129, -0.109),
    '2026-01-30': (-0.206, -0.276, -0.241),
}
WORST_MONTH_BOOK = -0.671                     # adversarial month (scout's 4-week stylised, BTC -61.1 %, ETH -73.1 %)
TAIL_OUT_OF_SAMPLE = ('2020-03-06', '2021-01-01')   # the adversarial atom's two events (reported stress)
HIST_WEEKS = 390                              # Fridays 2019-01-04 -> 2026-06-26 (the scout's history span)
# REALISTIC tail (000m; `r014_tail.py`): the registered H24 book, run unchanged on every out-of-calendar week
# (entry 2019-01-04 -> 2021-03-12, hold ending before 2021-03-24) whose BTC or ETH weekly excursion is
# >= 25 %, selected on the underlying alone (21 weeks). Book net return per unit notional; no liquidation
# at n = 0.10, 0.25 or 0.45 in any of them.
REALISTIC_WEEK = {
    '2019-03-29': -0.0386,
    '2019-05-10': -0.1094,
    '2019-06-21': -0.0169,
    '2019-07-12': -0.0361,
    '2019-09-13': 0.0144,
    '2019-09-20': -0.0241,
    '2019-10-25': -0.141,
    '2020-01-10': -0.0178,
    '2020-02-07': -0.0614,
    '2020-03-06': -0.0525,
    '2020-07-24': -0.0423,
    '2020-08-28': -0.0374,
    '2020-11-20': -0.0359,
    '2020-12-11': -0.0048,
    '2021-01-01': -0.1235,
    '2021-01-15': 0.037,
    '2021-01-22': 0.0565,
    '2021-01-29': 0.0239,
    '2021-02-05': -0.0038,
    '2021-02-19': -0.0532,
    '2021-03-05': -0.004,
}
REALISTIC_WORST_MONTH = -0.141                # worst 4-week window of computed weeks (uncomputed weeks at 0)


def tail_charge():
    """Per week, per unit notional: the realistic out-of-calendar stress weeks at their historical frequency."""
    return -sum(REALISTIC_WEEK.values()) / HIST_WEEKS


def tail_charge_adversarial():
    """Reported stress: the two adversarial events at their historical frequency."""
    return -sum(ADVERSARIAL_WEEK[e][2] for e in TAIL_OUT_OF_SAMPLE) / HIST_WEEKS


# ------------------------------------------------------------------ criteria (fixed now; §6)
FAMILY = ('T1', 'T2')
Q_DISCOVERY = 0.975                           # one-sided 2.5 % per trial: Bonferroni over the family of 2
Q_HOLDOUT = {1: 0.95, 2: 0.975}               # existence test; Bonferroni over the trials carried forward
# Critical-value FLOORS (§3, D7). A short-vol book's weekly series is negatively skewed and its worst
# weeks are rare, so even the skew-corrected t (`skew_t`, Hall 1992) rejects a true zero mean too often at the Student
# critical values (synthetic: 2x-6x nominal). The floors are the synthetic null quantiles of the primary
# construction's skew-corrected t in the FAT-tailed synthetic world (`shortvol_sim.registered_floors('H24')`:
# seeds 101 and 202, 20,000 weeks each, 6,000 block draws, the max rounded up), frozen here; the
# critical value is max(t_{G-1}(q), floor).
DISC_T_FLOOR = 2.59                           # q 0.975, G 143
HOLD_T_FLOOR = {1: 2.16, 2: 2.52}             # q 0.95 / 0.975, G 129
PLACEBO_SEEDS = 200


# ================================================================== maths (no numpy)
def ncdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def b76(F, K, T, s, call):
    """Black-76 price in USD per contract (no discounting: Deribit options are short-dated)."""
    if T <= 0 or s <= 0:
        return max(F - K, 0.0) if call else max(K - F, 0.0)
    v = s * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * v * v) / v
    d2 = d1 - v
    return F * ncdf(d1) - K * ncdf(d2) if call else K * ncdf(-d2) - F * ncdf(-d1)


def call_delta(F, K, T, s):
    if T <= 0 or s <= 0:
        return 1.0 if F > K else (0.0 if F < K else 0.5)
    v = s * math.sqrt(T)
    return ncdf((math.log(F / K) + 0.5 * v * v) / v)


def implied_vol(px, F, K, T, call):
    """Black-76 implied vol from a USD price by bisection; None outside [0.01, 6]."""
    lo, hi = 0.01, 6.0
    if T <= 0 or not (b76(F, K, T, lo, call) <= px <= b76(F, K, T, hi, call)):
        return None
    for _ in range(80):
        mid = 0.5 * (lo + hi)
        if b76(F, K, T, mid, call) < px:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def _ninv(p):
    """Inverse standard normal (Acklam's rational approximation, |error| < 1.2e-9)."""
    a = (-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02,
         -3.066479806614716e+01, 2.506628277459239e+00)
    b = (-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01,
         -1.328068155288572e+01)
    c = (-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00,
         4.374664141464968e+00, 2.938163982698783e+00)
    d = (7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00)
    if p < 0.02425:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p > 1 - 0.02425:
        return -_ninv(1 - p)
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
        (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)


def t_crit(df, q):
    """One-sided Student-t quantile t_df(q) (Hill's 1970 expansion; exact table for df <= 30 at the two
    levels used). df < 1 -> inf."""
    if df < 1:
        return math.inf
    tab = {0.95: (6.314, 2.920, 2.353, 2.132, 2.015, 1.943, 1.895, 1.860, 1.833, 1.812, 1.796, 1.782, 1.771,
                  1.761, 1.753, 1.746, 1.740, 1.734, 1.729, 1.725, 1.721, 1.717, 1.714, 1.711, 1.708, 1.706,
                  1.703, 1.701, 1.699, 1.697),
           0.975: (12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160,
                   2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056,
                   2.052, 2.048, 2.045, 2.042)}
    if q in tab and df <= 30:
        return tab[q][df - 1]
    z = _ninv(q)
    n = float(df)
    g1 = (z ** 3 + z) / 4
    g2 = (5 * z ** 5 + 16 * z ** 3 + 3 * z) / 96
    g3 = (3 * z ** 7 + 19 * z ** 5 + 17 * z ** 3 - 15 * z) / 384
    return z + g1 / n + g2 / n ** 2 + g3 / n ** 3


# ================================================================== instruments and windows
MON = {m: i for i, m in enumerate('JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC'.split(), 1)}
NAME = re.compile(r'^(BTC|ETH)-(\d{1,2})([A-Z]{3})(\d{2})-(\d+(?:d\d+)?)-([CP])$')


def parse_name(name):
    """'BTC-9APR21-58000-C' -> (coin, expiry_ms at 08:00 UTC, strike, is_call); None if not an option."""
    m = NAME.match(name)
    if not m:
        return None
    cur, d, mon, yy, k, cp = m.groups()
    return cur, ms(2000 + int(yy), MON[mon], int(d), 8), float(k.replace('d', '.')), cp == 'C'


def entry_times(start, end):
    """Friday 08:00 UTC entries whose 7-day expiry is < `end` and entry >= `start`. A position whose
    life crosses a calendar boundary belongs to neither calendar (the 2023-12-29 entry is the embargo)."""
    t = start
    while dt.datetime.fromtimestamp(t / 1000, dt.timezone.utc).weekday() != 4 or t % DAY != ENTRY_HOUR * HOUR:
        t += HOUR
    out = []
    while t + TENOR_D * DAY < end:
        out.append(t)
        t += WEEK
    return out


def calendar_entries(name):
    a, b = CALENDARS[name]
    return entry_times(a, b)


def window_rows(trades, a, b):
    """Rows of `trades` (dicts with 'timestamp') with a <= t < b."""
    return [r for r in trades if a <= r['timestamp'] < b]


def entry_index(window, entry, perp_close_at_entry):
    """(S, source): the latest trade's index price if it is <= 30 min old, else the perp close at entry."""
    if window:
        last = max(window, key=lambda r: (r['timestamp'], r.get('trade_seq', 0)))
        if entry - last['timestamp'] <= INDEX_MAX_AGE and last.get('index_price'):
            return float(last['index_price']), 'trade_index'
    if perp_close_at_entry:
        return float(perp_close_at_entry), 'perp_close'
    return None, 'missing'


def straddle_entry(coin, entry, trades, perp_close_at_entry):
    """Mark the 7-day ATM straddle at `entry` from window trades only ([entry - 4 h, entry)).
    Returns a dict (K, expiry, S, iv, mark per leg, mark age, ...) or {'missing': reason}."""
    win = window_rows(trades, entry - MARK_WINDOW_H * HOUR, entry)
    S, src = entry_index(win, entry, perp_close_at_entry)
    if S is None:
        return {'missing': 'no_index'}
    expiry = entry + TENOR_D * DAY
    at = {}
    for r in win:
        p = parse_name(r['instrument_name'])
        if p and p[0] == coin and p[1] == expiry:
            at.setdefault(p[2], []).append((r, p[3]))
    if not at:
        return {'missing': 'no_trade_in_expiry'}
    K = min(at, key=lambda k: (abs(k - S), k))
    if abs(K / S - 1) > ATM_BAND:
        return {'missing': 'no_strike_within_band'}
    r, is_call = max(at[K], key=lambda x: (x[0]['timestamp'], x[0].get('trade_seq', 0)))
    F0 = float(r['index_price'])
    T0 = (expiry - r['timestamp']) / DAY / YEAR_D
    iv = implied_vol(float(r['mark_price']) * F0, F0, K, T0, is_call)
    if iv is None:
        return {'missing': 'mark_iv_not_invertible'}
    T = (expiry - entry) / DAY / YEAR_D
    return {'coin': coin, 'entry': entry, 'expiry': expiry, 'K': K, 'S': S, 'S_source': src, 'iv': iv,
            'mark_call': b76(S, K, T, iv, True), 'mark_put': b76(S, K, T, iv, False),
            'mark_age_min': (entry - r['timestamp']) / 60_000}


def entry_coverage(coin, entry, trades, perp_close_at_entry):
    """COVERAGE ONLY (the build): the same availability logic as `straddle_entry` WITHOUT any mark, IV or
    premium: ('ok' | missing reason, index source, age in minutes of the chosen strike's latest trade)."""
    win = window_rows(trades, entry - MARK_WINDOW_H * HOUR, entry)
    S, src = entry_index(win, entry, perp_close_at_entry)
    if S is None:
        return 'no_index', src, None
    expiry = entry + TENOR_D * DAY
    at = {}
    for r in win:
        p = parse_name(r['instrument_name'])
        if p and p[0] == coin and p[1] == expiry:
            at.setdefault(p[2], []).append(r['timestamp'])
    if not at:
        return 'no_trade_in_expiry', src, None
    K = min(at, key=lambda k: (abs(k - S), k))
    if abs(K / S - 1) > ATM_BAND:
        return 'no_strike_within_band', src, None
    return 'ok', src, (entry - max(at[K])) / 60_000


def settlement_coverage(trades, expiry):
    """COVERAGE ONLY: the number of minutes of [expiry - 30 min, expiry) with >= 1 trade carrying an
    index price (no price is averaged)."""
    return len({(r['timestamp'] - (expiry - SETTLE_WINDOW)) // 60_000
                for r in window_rows(trades, expiry - SETTLE_WINDOW, expiry) if r.get('index_price')})


def settlement(trades, expiry, perp_close_at_expiry):
    """(price, source, minutes covered): Deribit's 07:30-08:00 index TWAP approximated by the mean of
    per-minute medians of the trades' index price; fewer than 10 covered minutes -> the perp close."""
    per = {}
    for r in window_rows(trades, expiry - SETTLE_WINDOW, expiry):
        if r.get('index_price'):
            per.setdefault((r['timestamp'] - (expiry - SETTLE_WINDOW)) // 60_000, []).append(float(r['index_price']))
    if len(per) >= SETTLE_MIN_MINUTES:
        return statistics.fmean(statistics.median(v) for v in per.values()), 'index_twap', len(per)
    if perp_close_at_expiry:
        return float(perp_close_at_expiry), 'perp_close', len(per)
    return None, 'missing', len(per)


# ================================================================== the position
def leg_fee(S, price):
    return min(OPT_FEE * S, OPT_FEE_CAP * price)


def prev_t_ok(path, X):
    """The path starts at the entry, steps by exactly one hour and ends at the expiry."""
    ts = [p[0] for p in path]
    return ts[-1] == X and all(b - a == HOUR for a, b in zip(ts, ts[1:]))


def position_pnl(entry_mark, path, settle_px, *, hedge_every_h=HEDGE_EVERY_H, half_spread=None,
                 funding=None, liq_n=None, beta_iv=BETA_IV, delivery=True, perp_cost=PERP_TAKER + PERP_SLIP):
    """Net USD P&L of one short straddle (one contract per leg) held to expiry, delta-hedged on the perp.

    entry_mark: from `straddle_entry` (K, S, iv, expiry, entry, mark_call, mark_put, coin).
    path: [(t, S_t, dvol_t)] hourly, t = entry ... expiry inclusive (S_t: perp price at t; dvol_t: the
          coin's DVOL close at t, or None). Missing hours are filled by the caller (counted there).
    settle_px: the settlement price at expiry.
    funding: {t_hour: rate} hourly funding of the perp (a long pays rate x notional), or None.
    liq_n: book leverage n for the liquidation check (None: no check).
    hedge_every_h: None -> unhedged.
    Returns a dict of components; `net` = premium_mark - spread - fees - payoff - delivery + hedge_pnl
    + funding_pnl - hedge_fees (or the crystallised value if liquidated)."""
    K, S0, iv0, X, E = entry_mark['K'], entry_mark['S'], entry_mark['iv'], entry_mark['expiry'], entry_mark['entry']
    hs = (half_spread if half_spread is not None else HALF_SPREAD[entry_mark['coin']])
    prem = entry_mark['mark_call'] + entry_mark['mark_put']
    spread = hs * prem
    sold_c, sold_p = entry_mark['mark_call'] * (1 - hs), entry_mark['mark_put'] * (1 - hs)
    fees = leg_fee(S0, sold_c) + leg_fee(S0, sold_p)
    assert path[0][0] == E, 'the path must start at the entry'
    dvol0 = path[0][2]

    def leg_iv(dv):
        if dv is None or dvol0 is None:
            return iv0
        return max(IV_FLOOR, iv0 + beta_iv * (dv - dvol0) / 100.0)

    hedge_t = set()
    if hedge_every_h:
        k = 0
        while E + k * hedge_every_h * HOUR < X:
            hedge_t.add(E + k * hedge_every_h * HOUR)
            k += 1
    h = 0.0
    hedge_pnl = fund_pnl = hedge_fees = 0.0
    liquidated = None
    prev_S = path[0][1]
    for i, (t, S, dv) in enumerate(path):
        if i:
            hedge_pnl += h * (S - prev_S)
            if funding is not None:
                fund_pnl -= h * prev_S * funding.get(t, 0.0)   # the rate stamped t covers (t - 1 h, t]
        prev_S = S
        if t >= X:
            break
        T = (X - t) / DAY / YEAR_D
        if liq_n is not None and i:
            s_ = leg_iv(dv)
            v = b76(S, K, T, s_, True) + b76(S, K, T, s_, False)
            pnl_now = sold_c + sold_p - fees - v + hedge_pnl + fund_pnl - hedge_fees
            mm = 2 * OPT_MM * S + v + PERP_MM * abs(h) * S
            if 1 + liq_n * pnl_now / S0 < liq_n * mm / S0:
                close_cost = hs * v + leg_fee(S, b76(S, K, T, s_, True)) + leg_fee(S, b76(S, K, T, s_, False)) \
                    + abs(h) * S * perp_cost
                net = pnl_now - close_cost
                liquidated = {'t': t, 'value': v}
                return {'net': net, 'premium_mark': prem, 'spread': spread, 'fees': fees, 'payoff': None,
                        'delivery': 0.0, 'hedge_pnl': hedge_pnl, 'funding_pnl': fund_pnl,
                        'hedge_fees': hedge_fees + abs(h) * S * perp_cost, 'liquidated': liquidated,
                        'S0': S0, 'ret_notional': net / S0, 'ret_premium': net / prem}
        if t in hedge_t:
            dc = call_delta(S, K, T, leg_iv(dv))
            target = 2 * dc - 1                    # buy this many units to neutralise the short straddle
            hedge_fees += abs(target - h) * S * perp_cost
            h = target
    assert prev_t_ok(path, X), 'the path must run from the entry to the expiry, hourly'
    S_X = prev_S
    hedge_fees += abs(h) * S_X * perp_cost         # unwind at expiry
    payoff = abs(settle_px - K)
    deliv = min(DELIV_FEE * settle_px, DELIV_CAP * payoff) if (delivery and payoff > 0) else 0.0
    net = prem - spread - fees - payoff - deliv + hedge_pnl + fund_pnl - hedge_fees
    return {'net': net, 'premium_mark': prem, 'spread': spread, 'fees': fees, 'payoff': payoff,
            'delivery': deliv, 'hedge_pnl': hedge_pnl, 'funding_pnl': fund_pnl, 'hedge_fees': hedge_fees,
            'liquidated': None, 'S0': S0, 'ret_notional': net / S0, 'ret_premium': net / prem}


# ================================================================== the stress state (T2)
def stress_flag(dvol_bars, entry, coin):
    """True / False / None (undefined) for one coin at `entry`, from DVOL bars [(t_open, close)] only.
    Uses bars CLOSING at or before entry - 1 h (open <= entry - 2 h): nothing at or after the entry."""
    last_open = entry - 2 * HOUR
    first_open = entry - STRESS_LOOKBACK - 2 * HOUR
    win = [(t, c) for t, c in dvol_bars if first_open <= t <= last_open and c is not None]
    if len(win) < STRESS_MIN_BARS or not any(t == last_open for t, _ in win):
        return None
    last = [c for t, c in win if t == last_open][0]
    lo = min(c for _, c in win)
    return lo > 0 and last / lo >= STRESS_RATIO


def book_stressed(flags):
    """The book stands aside if ANY coin is stressed; undefined flags count as not stressed."""
    return any(f is True for f in flags.values())


# ================================================================== weekly book and statistics
def weekly_book(positions):
    """positions: [{'entry', 'coin', 'ret_notional', 'gross_notional', 'cost_notional'}] ->
    [{'entry', 'r', 'gross', 'cost', 'coins'}] sorted by entry; equal notional across the coins present."""
    by = {}
    for p in positions:
        by.setdefault(p['entry'], []).append(p)
    out = []
    for e in sorted(by):
        ps = by[e]
        out.append({'entry': e, 'r': statistics.fmean(p['ret_notional'] for p in ps),
                    'gross': statistics.fmean(p['gross_notional'] for p in ps),
                    'cost': statistics.fmean(p['cost_notional'] for p in ps),
                    'coins': sorted(p['coin'] for p in ps)})
    return out


def stand_aside(book, stressed_entries):
    """T2: the same weekly book with every stressed week replaced by an idle (zero-return) week."""
    return [dict(w, r=0.0, gross=0.0, cost=0.0, idle=True) if w['entry'] in stressed_entries else w for w in book]


def skew_t(rs, mu0=0.0):
    """Hall's (1992) monotone skewness-corrected t for H0: mean = mu0 (Johnson's 1978 correction plus the
    cubic term that makes it monotone in the mean). A short-vol series is negatively skewed, which pushes
    the plain t's null distribution to the right (a one-sided 'mean > 0' test rejects too often); the
    correction subtracts that bias:
        T = sqrt(G) * (th + g th^2 / 3 + g^2 th^3 / 27 + g / (6 G)),   th = d / s,  g = m3 / s^3,
    d = mean - mu0, s the sample SD, m3 the third central sample moment. dT/dth = (1 + g th / 3)^2 >= 0."""
    G = len(rs)
    if G < 3:
        return None
    m = statistics.fmean(rs)
    s = statistics.stdev(rs)
    if s <= 0:
        return None
    g = statistics.fmean((r - m) ** 3 for r in rs) / s ** 3
    th = (m - mu0) / s
    return math.sqrt(G) * (th + g * th * th / 3 + g * g * th ** 3 / 27 + g / (6 * G))


def summary(rs, n=SIZE_N):
    """Statistics of a weekly return series (per unit notional). Pure; no decision."""
    G = len(rs)
    if G < 3:
        return {'G': G, 'mean': statistics.fmean(rs) if rs else None, 't': None}
    m = statistics.fmean(rs)
    sd = statistics.stdev(rs)
    half = G // 2
    srt = sorted(rs)
    eq, peak, mdd = 1.0, 1.0, 0.0
    for r in rs:
        eq *= max(1 + n * r, 0.0)
        peak = max(peak, eq)
        mdd = min(mdd, eq / peak - 1)
    worst4 = min((sum(rs[i:i + 4]) for i in range(G - 3)), default=None)
    return {'G': G, 'mean': m, 'sd': sd, 't': skew_t(rs), 't_plain_reported': (m / (sd / math.sqrt(G))) if sd > 0 else None,
            'skew_reported': (statistics.fmean((r - m) ** 3 for r in rs) / statistics.pstdev(rs) ** 3) if sd > 0 else None,
            'median': statistics.median(rs), 'first_half_mean': statistics.fmean(rs[:half]),
            'second_half_mean': statistics.fmean(rs[half:]),
            'mean_without_largest_positive': statistics.fmean(srt[:-1]) if srt[-1] > 0 else m,
            'mean_without_worst_reported': statistics.fmean(srt[1:]),
            'worst_week': srt[0], 'best_week': srt[-1], 'worst_4_weeks_sum': worst4,
            'annualised_on_equity': m * 52 * n, 'max_drawdown_at_n': mdd, 'n': n,
            'hit_reported': sum(1 for r in rs if r > 0) / G}


def discovery_criteria(s, q=Q_DISCOVERY, n=SIZE_N, floor=None):
    """The five discovery criteria from a `summary` dict (the numbers stored in the row). n=None switches
    criterion 5 off: used ONLY by the synthetic construction comparison (`shortvol_sim`), never by a run."""
    G = s.get('G') or 0
    tc = max(t_crit(G - 1, q), DISC_T_FLOOR if floor is None else floor)
    t = s.get('t')
    return {
        '1_mean_pos_t_ge_tcrit': bool(s.get('mean') is not None and s['mean'] > 0 and t is not None and t >= tc),
        '2_median_pos': bool(s.get('median') is not None and s['median'] > 0),
        '3_both_halves_pos': bool(s.get('first_half_mean') is not None and s['first_half_mean'] > 0
                                  and s['second_half_mean'] > 0),
        '4_without_largest_positive_week_pos': bool(s.get('mean_without_largest_positive') is not None
                                                    and s['mean_without_largest_positive'] > 0),
        '5_annualised_at_n_ge_hlp_bar': True if n is None else
        bool(s.get('mean') is not None and s['mean'] * 52 * n >= HLP_BAR),
    }, tc


def decide_discovery(results):
    """results: {trial: {'summary': ..., 'criteria': ..., 'pass': bool}} -> ('read_holdout', [passed]) or
    ('close', [])."""
    passed = [k for k in FAMILY if results.get(k, {}).get('pass')]
    return ('read_holdout', passed) if passed else ('close', [])


def evaluate_discovery(series, floor=None, n=SIZE_N):
    """series: {trial: [weekly r]} -> results dict and decision. The ONLY place discovery gates run."""
    results = {}
    for k in FAMILY:
        s = summary(series[k], n=n if n is not None else SIZE_N)
        c, tc = discovery_criteria(s, floor=floor, n=n)
        results[k] = {'summary': s, 'criteria': c, 't_crit': tc, 'pass': all(c.values())}
    return results, decide_discovery(results)


def recheck_discovery(results, weekly=None):
    """Recompute every criterion and the decision from the stored NUMBERS (never a label). With
    `weekly` ({trial: [[entry, r], ...]}, stored in the row), the summary itself is recomputed from the
    weekly returns and must equal the stored one. Returns (decision, [failures])."""
    fails = []
    for k in FAMILY:
        r = results.get(k)
        if not r or 'summary' not in r:
            fails.append(f'{k}: no stored summary')
            continue
        s = r['summary']
        if s.get('n') != SIZE_N:
            fails.append(f'{k}: stored size n {s.get("n")} is not the registered {SIZE_N}')
        if weekly is not None:
            w = weekly.get(k)
            if not w:
                fails.append(f'{k}: no stored weekly series')
            else:
                again = json_round(summary([x[1] for x in w]))
                diff = [key for key in again if again[key] != s.get(key)]
                if diff:
                    fails.append(f'{k}: the stored summary differs from the one recomputed from the weekly series: {diff}')
        c, tc = discovery_criteria(s)
        if c != r.get('criteria'):
            fails.append(f'{k}: stored criteria {r.get("criteria")} differ from those recomputed {c}')
        if bool(r.get('pass')) != all(c.values()):
            fails.append(f'{k}: stored pass {r.get("pass")} differs from the recomputed {all(c.values())}')
    dec = decide_discovery({k: {'pass': all(discovery_criteria(results[k]['summary'])[0].values())}
                            for k in FAMILY if results.get(k, {}).get('summary')})
    return dec, fails


def json_round(obj):
    """What a value looks like after a JSON round trip (the row is stored as JSON)."""
    import json
    return json.loads(json.dumps(obj, default=str))


def holdout_existence(rs, k_trials, floor=None):
    """EXISTENCE test on the holdout weeks: mean > 0 and one-sided skew-corrected t >= max(t_{G-1}(q), floor),
    q by the number of trials carried forward (Bonferroni). Returns (confirm, details)."""
    s = summary(rs)
    q = Q_HOLDOUT[k_trials]
    tc = max(t_crit((s.get('G') or 0) - 1, q), HOLD_T_FLOOR[k_trials] if floor is None else floor)
    ok = bool(s.get('mean') is not None and s['mean'] > 0 and s.get('t') is not None and s['t'] >= tc)
    return ok, {'summary': s, 'q': q, 't_crit': tc}


def economic_after(s, n=SIZE_N):
    """Deploy bar, applied only AFTER an existence confirm: (mean - realistic tail charge) x 52 x n >= 3 %/yr,
    and the realised max drawdown at n no worse than the pre-registered ADVERSARIAL month at n (a breach of
    the bound itself; the realistic month is reported beside it)."""
    adj = (s['mean'] - tail_charge()) * 52 * n
    dd_ok = s['max_drawdown_at_n'] >= WORST_MONTH_BOOK * n
    return {'tail_charge_per_week': tail_charge(), 'annualised_tail_charged': adj,
            'annualised_adversarial_charged_reported': (s['mean'] - tail_charge_adversarial()) * 52 * n,
            'drawdown_vs_realistic_month_reported': s['max_drawdown_at_n'] >= REALISTIC_WORST_MONTH * n,
            'bar_met': adj >= HLP_BAR, 'drawdown_within_preregistered_month': dd_ok}


def decide_holdout(confirms, econ):
    """confirms: {trial: bool}; econ: {trial: economic_after dict}."""
    ok = [k for k in FAMILY if confirms.get(k)]
    if not ok:
        return 'not_demonstrated'
    dep = [k for k in ok if econ[k]['bar_met'] and econ[k]['drawdown_within_preregistered_month']]
    return 'deploy_paper_registration' if dep else 'exists_not_economic'


def placebo(book, seeds=PLACEBO_SEEDS, q=Q_DISCOVERY):
    """Sign-flip by week of the GROSS weekly return, costs kept: share of seeds passing criterion 1
    (reported; decides nothing)."""
    hits, ts = 0, []
    for sd in range(seeds):
        rng = random.Random(sd)
        rs = [(w['gross'] * (1 if rng.random() < 0.5 else -1)) - w['cost'] for w in book]
        s = summary(rs)
        c, _ = discovery_criteria(s, q)
        hits += c['1_mean_pos_t_ge_tcrit']
        ts.append(s.get('t') or 0.0)
    return {'seeds': seeds, 'share_c1_pass': hits / seeds, 't_p95': sorted(ts)[int(0.95 * len(ts)) - 1]}


def sizing_statement(n=SIZE_N):
    """The pre-registered tail at size n (equity fractions)."""
    worst_week = min(REALISTIC_WEEK.values())
    adv = min(v[2] for v in ADVERSARIAL_WEEK.values())
    return {'n': n, 'worst_week_equity': n * worst_week, 'worst_month_equity': n * REALISTIC_WORST_MONTH,
            'stress_adversarial_week_equity': n * adv, 'stress_adversarial_month_equity': n * WORST_MONTH_BOOK,
            'tail_charge_adversarial_per_week_notional': tail_charge_adversarial(),
            'tail_charge_per_week_notional': tail_charge(),
            'events_equity': {e: round(n * v[2], 4) for e, v in ADVERSARIAL_WEEK.items()}}
