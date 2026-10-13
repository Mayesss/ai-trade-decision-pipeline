"""Cross-venue capture of the forced-flow gap at second granularity — registration 006 (H3)
mechanics. Pure functions; the Hyperliquid tape, the Binance aggTrades and the liquidation
stream come in as arguments. Statistics are 005's (`ct/drift.py`, imported, not copied).

The trade (T1). For a causal trigger (004's trailing-60-s rule, fallback-median events dropped),
scan the forced BOOK prints from the trigger print while the run is alive; at each print time t
the signed gap is

    g(t) = s * (P_bn(t) - P_hl(t)) / P_hl(t),   s = +1 longs liquidated, -1 shorts,

with P_hl(t) the forced print's price and P_bn(t) the last Binance aggTrade at or before t
(on HL's clock: transact_time + OFFSET_MS). The FIRST print with g >= G_MIN triggers, one
trade per event. Primary signal (D4, revision 3): the SWEEP gap — the forced book print vs Binance's
last trade at or before it; the gap on EXECUTABLE-QUOTE proxies (HL ask = last buy-aggressor print
within 1 s vs Binance bid = last seller-taker trade within 1 s for longs liquidated, mirrored for
shorts) is a reported variant. At
t + L buy HL / sell Binance (s = +1; mirrored for s = -1) with market orders that fill at
max(first print of our taker side, the highest print in between) (D8); close both legs the same
way when the unwind gap (HL bid vs Binance ask) is <= 0 (orders L later), or at t_open + CAP.

Pieces, in the order the runner applies them:
  Series            one venue's prints on HL's clock, deterministic order, causal lookups;
  signed_gap        the gap, signed so that > 0 is the convergence trade's edge;
  first_trigger     the signal over a cascade's scan [(t, p_hl, p_bn)];
  plan              warm-up, holdout edge, Binance coverage, trigger, stress switch (own coin
                    excluded, up to the decision time - 1 s), one position per coin;
  simulate          entry at t + L, exit at gap <= 0 or the cap, fixed exits, both legs;
  net_return        HL leg + Binance leg on the HL notional, taker + adverse slippage each;
  summarise         the seven criteria (hour- and day-clustered), placebo, splits;
  decide            the proposed decision table;
  gap_profile       the reported half-life measurement (guarded runner only);
  null / power      drift.null_se / mde and a joint-power simulation over the seven criteria.

Nothing here fetches a price. Which prices exist at which time is the caller's input; the
functions that would read after a decision time (simulate, gap_profile, minute_gap) are called
only by the guarded runner (l8_run.py) and by the synthetic tests.
"""
import bisect
import random
import statistics

from . import drift
from . import eventstudy as es
from .provider import stress as stress_switch

SECOND, MINUTE, HOUR, DAY = drift.SECOND, drift.MINUTE, drift.HOUR, drift.DAY
TAKER_HL = drift.TAKER                    # Hyperliquid base-tier taker, 0.045% each side
TAKER_BN = 0.0005                         # Binance USDⓈ-M base-tier taker, 0.05%, no BNB discount
SLIP_FLOOR = 0.0002                       # adverse slippage per leg and side: at least 2 bp
# per coin and venue (D6): max(2 bp, the coin's p75 half-bounce in the 10 s before its trigger prints and before its
# sweep-signal prints, §3.4), rounded to 0.1 bp
SLIP_HL = {'BTC': 0.00024, 'ETH': 0.0002, 'SOL': 0.00025, 'XRP': 0.00021, 'DOGE': 0.00072, 'HYPE': 0.0002}
SLIP_BN = {'BTC': 0.0002, 'ETH': 0.0002, 'SOL': 0.00023, 'XRP': 0.0002, 'DOGE': 0.0002, 'HYPE': 0.0002}
SLIP_FLOOR_REPORTED = 0.0004              # the 4-bp-per-fill block (reported)
QUOTE_WINDOW = 1 * SECOND                 # an executable-quote proxy older than this is no quote
L_PRIMARY = 1 * SECOND
LATENCIES = (250, 1 * SECOND, 3 * SECOND)
CAP = 60 * SECOND
FIXED = {'5s': 5 * SECOND, '15s': 15 * SECOND, '30s': 30 * SECOND, '60s': 60 * SECOND, '300s': 300 * SECOND}
PRIMARY = 'conv'
FILL_WINDOW = 5 * SECOND                  # entry: no print on a venue within this after t + L -> no trade
EXIT_WINDOW = 30 * SECOND                 # exit: the first print within this after the exit time (else counted missing)
OFFSET_MS = 0                             # Binance -> HL clock (registration §3: estimated, fixed before any outcome)
EXTRA_G = 0.0010                          # G_MIN = round-trip cost incl. slippage + 10 bp
SIDE_AWARE = True                         # fills on prints of OUR taker side (HL: aggressor side of the crossed
                                          # row; Binance: is_buyer_maker) — registration D8; side-blind reported
MARKET_ONLY = True                        # signal on book prints only: 'Close Long/Short' forced fills, not the
                                          # backstop 'Liquidated Cross/Isolated' transfers (registration D3)
PROFILE = (0, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000)
STRESS_THRESHOLD = drift.STRESS_THRESHOLD
HOLDOUT_START = drift.HOLDOUT_START
Z_HOUR, Z_DAY = 2.33, 1.96
BAR = 0.0025                              # the H7 bar (yardstick, not a gate): >= 25 bp net per major fill
GATE = 0.0010                             # D13: a pass leads to the holdout only with a pooled estimate >= 10 bp
Z_HOLDOUT = 1.64                          # holdout CONFIRM: one-sided hour t (005 D14 rules)
MAJORS = frozenset({'BTC', 'ETH', 'SOL'})
COINS = ('BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'HYPE')

# the event fields the trade may use (the scan of forced prints comes separately, each print at its time)
ENTRY_FIELDS = ('coin', 'side', 't_start', 'fallback_median')


def direction(side):
    return drift.direction(side)


def klass(coin):
    return 'major' if coin in MAJORS else 'alt'


def round_trip_cost(coin, slip_hl=None, slip_bn=None):
    """Cost of a flat round trip, both legs, in return units of the HL notional."""
    sh = SLIP_HL[coin] if slip_hl is None else slip_hl
    sb = SLIP_BN[coin] if slip_bn is None else slip_bn
    return -net_return('L', 100.0, 100.0, 100.0, 100.0, sh, sb)


def g_min(coin, extra=EXTRA_G, slip_hl=None, slip_bn=None):
    """The trigger threshold: round-trip cost (fees + adverse slippage, four fills) + 10 bp."""
    return round_trip_cost(coin, slip_hl, slip_bn) + extra


# ----------------------------------------------------------------------------- series

class Series:
    """One venue's prints on HL's clock, in execution order: (t, price, taker) with taker 'buy' /
    'sell' / None. Input must already be in execution order (HL: timestamp, file, row; Binance:
    transact_time, agg_trade_id); a stable sort on t keeps it. offset_ms shifts every time
    (Binance transact_time -> HL clock)."""

    def __init__(self, prints, offset_ms=0):
        ps = []
        for row in prints:
            ps.append((row[0] + offset_ms, row[1], row[2] if len(row) > 2 else None))
        ps.sort(key=lambda x: x[0])                       # stable: same-ms prints keep their order
        self.t = [x[0] for x in ps]
        self.px = [x[1] for x in ps]
        self.taker = [x[2] for x in ps]

    def last_at_or_before(self, t, taker=None, window=None):
        """The last print at or before t (of the given taker side; within `window` ms if given)."""
        j = bisect.bisect_right(self.t, t) - 1
        while j >= 0:
            if window is not None and self.t[j] < t - window:
                return None
            if taker is None or self.taker[j] == taker:
                return self.t[j], self.px[j]
            j -= 1
        return None

    def extreme(self, a, b, high):
        """Highest (high=True) or lowest print price of either side in [a, b]; None if no print."""
        i, j = bisect.bisect_left(self.t, a), bisect.bisect_right(self.t, b)
        if i >= j:
            return None
        return max(self.px[i:j]) if high else min(self.px[i:j])

    def first_at_or_after(self, t, window=None, taker=None):
        j = bisect.bisect_left(self.t, t)
        while j < len(self.t):
            if window is not None and self.t[j] > t + window:
                return None
            if taker is None or self.taker[j] == taker:
                return self.t[j], self.px[j]
            j += 1
        return None

    def times_between(self, a, b):
        """Distinct print times in [a, b]."""
        i, j = bisect.bisect_left(self.t, a), bisect.bisect_right(self.t, b)
        return sorted(set(self.t[i:j]))


def signed_gap(side, p_bn, p_hl):
    """s * (P_bn - P_hl) / P_hl: > 0 when HL sits beyond Binance in the direction of the forced
    flow (longs liquidated: HL below Binance), i.e. when the convergence trade has an edge."""
    if not p_bn or not p_hl:
        return None
    return direction(side) * (p_bn - p_hl) / p_hl


def scan_from_series(prints, bn):
    """[(t, p_hl, p_bn)] for forced prints [(t, p_hl)] in execution order, P_bn = the last Binance
    trade at or before t (a Series on HL's clock). Only prices at or before each t are read."""
    out = []
    for t, p in prints:
        b = bn.last_at_or_before(t)
        out.append((t, p, b[1] if b else None))
    return out


def exec_scan(side, times, hl, bn, window=QUOTE_WINDOW):
    """[(t, HL quote, Binance quote)] at each scan time (forced book print times, in order, one per
    distinct t): the executable-quote proxies of `quotes` — prices at or before t only."""
    out, seen = [], set()
    for t in times:
        if t in seen:
            continue
        seen.add(t)
        h, b = quotes(side, hl, bn, t)
        out.append((t, h, b))
    return out


def first_trigger(side, scan, threshold):
    """The first forced print whose contemporaneous signed gap reaches the threshold:
    {'t', 'g', 'p_hl', 'p_bn', 'k'} or None. scan: [(t, p_hl, p_bn)] in execution order."""
    for k, (t, p_hl, p_bn) in enumerate(scan):
        g = signed_gap(side, p_bn, p_hl)
        if g is not None and g >= threshold:
            return {'t': t, 'g': g, 'p_hl': p_hl, 'p_bn': p_bn, 'k': k}
    return None


# ----------------------------------------------------------------------------- the plan (behaviour only)

def busy_ms(latency=L_PRIMARY):
    """The longest a position can live after its signal: entry fills within L + FILL_WINDOW, the
    60-s cap, the exit L and the exit fill window."""
    return latency + FILL_WINDOW + CAP + latency + EXIT_WINDOW


def decision_window(event, t):
    """Stress switch window for a trigger at t: every evaluation point in [min(first print, t - 1 s),
    t - 1 s]. A one-second bucket labelled s holds prints in [s, s + 1 s), so a rate evaluated at
    x <= t - 1 s reads only buckets complete before t."""
    b = t - SECOND
    return min(event['t_start'], b), b


def stressed(event, t, rate, own=None, threshold=STRESS_THRESHOLD):
    a, b = decision_window(event, t)
    return stress_switch(rate.max_between(a, b, exclude=own), threshold)


def plan(events, scans, rate, own_rates=None, bn_ok=None, thresholds=None, drop_fallback=True,
         latency=L_PRIMARY, cutoff=HOLDOUT_START, majors=MAJORS):
    """The positions the rule would open. Behaviour only: trigger times and contemporaneous gaps.

    events: cascades (only ENTRY_FIELDS read); scans: {event index: [(t, p_hl, p_bn)]} — each
    cascade's forced prints with the Binance price at or before each; rate / own_rates: drift
    StressRate of all coins / per coin (own coin excluded when own_rates is given; None = the
    inclusive series); bn_ok: {event index: Binance files cover the cascade and its exits};
    thresholds: {coin: G_MIN} (default g_min(coin)).
    Filters, in order: fallback median, no Binance data, no signal, holdout edge (signal + the
    longest possible trade + the 300-s fixed exit reaches the cutoff), stress at the signal, one
    position per coin (skip while an earlier position in the coin can still be open: t < previous t
    + busy_ms(L) — a bound known in advance, so the plan never depends on an exit; the 300-s fixed
    exit is reported and may overlap). Scans hold (t, HL quote, Binance quote) at each scan point:
    executable-quote proxies (primary) or the forced print and Binance's last trade (sweep variant).
    Returns (positions, skipped)."""
    skipped = {'warmup_fallback': [], 'holdout_edge': [], 'no_binance': [], 'no_trigger': [], 'stress': [],
               'overlap': []}
    thresholds = thresholds or {}
    order = sorted(range(len(events)), key=lambda i: (events[i]['coin'], events[i]['t_start'], i))
    busy_until = {}
    out = []
    for i in order:
        e = {k: events[i].get(k) for k in ENTRY_FIELDS}
        if drop_fallback and e['fallback_median']:
            skipped['warmup_fallback'].append(i)
            continue
        scan = scans.get(i) or []
        if bn_ok is not None and not bn_ok.get(i):
            skipped['no_binance'].append(i)
            continue
        th = thresholds.get(e['coin'], g_min(e['coin']))
        trig = first_trigger(e['side'], scan, th)
        if trig is None:
            skipped['no_trigger'].append(i)
            continue
        if trig['t'] + busy_ms(latency) + max(FIXED.values()) >= cutoff:
            skipped['holdout_edge'].append(i)
            continue
        own = None
        if own_rates is not None:
            own = own_rates.get(e['coin']) or drift.StressRate([])
        if stressed(e, trig['t'], rate, own):
            skipped['stress'].append(i)
            continue
        if trig['t'] < busy_until.get(e['coin'], -1):
            skipped['overlap'].append(i)
            continue
        busy_until[e['coin']] = trig['t'] + busy_ms(latency)
        out.append({'i': i, 'coin': e['coin'], 'side': e['side'], 't_start': e['t_start'], 't_sig': trig['t'],
                    'g_sig': trig['g'], 'p_hl_sig': trig['p_hl'], 'p_bn_sig': trig['p_bn'], 'k_sig': trig['k'],
                    'major': e['coin'] in majors, 'entry': trig['t'] + latency,
                    'hour': trig['t'] // HOUR, 'day': trig['t'] // DAY})
    out.sort(key=lambda p: (p['t_sig'], p['coin']))
    return out, skipped


# ----------------------------------------------------------------------------- the trade (guarded)

def net_return(side, e_hl, x_hl, e_bn, x_bn, slip_hl, slip_bn, taker_hl=TAKER_HL, taker_bn=TAKER_BN):
    """Both legs on the HL notional (equal notional on Binance at entry). side 'L' (longs
    liquidated): long HL, short Binance; 'S': short HL, long Binance. Each fill adverse by its
    slippage, taker fee on each of the four fills. None without all four prices."""
    if not (e_hl and x_hl and e_bn and x_bn):
        return None
    other = 'S' if side == 'L' else 'L'
    return (drift.net_return(side, e_hl, x_hl, slip_hl, taker_hl)
            + drift.net_return(other, e_bn, x_bn, slip_bn, taker_bn))


def legs(side, e_hl, x_hl, e_bn, x_bn, slip_hl, slip_bn):
    """(HL leg, Binance leg) net — which venue paid."""
    other = 'S' if side == 'L' else 'L'
    return drift.net_return(side, e_hl, x_hl, slip_hl, TAKER_HL), drift.net_return(other, e_bn, x_bn, slip_bn, TAKER_BN)


def _takers(side):
    """Taker side of (HL entry, Binance entry, HL exit, Binance exit): longs liquidated -> buy HL
    (at the ask), sell Binance (at the bid); exits reversed."""
    return ('buy', 'sell', 'sell', 'buy') if side == 'L' else ('sell', 'buy', 'buy', 'sell')


def quotes(side, hl, bn, t, unwind=False, window=QUOTE_WINDOW):
    """Executable-quote proxies at t (D4): the last print of the side we would TAKE, within `window`.
    Opening a longs-liquidated position (buy HL, sell Binance): HL ask = last buy-aggressor print,
    Binance bid = last seller-taker trade. unwind=True: the prices we would close at (HL bid, Binance
    ask). Shorts liquidated mirrored. Returns (hl_quote, bn_quote) or None for a missing one."""
    tk = _takers(side)
    th, tb = (tk[2], tk[3]) if unwind else (tk[0], tk[1])
    h = hl.last_at_or_before(t, th, window)
    b = bn.last_at_or_before(t, tb, window)
    return (h[1] if h else None), (b[1] if b else None)


def exec_gap(side, hl, bn, t, unwind=False):
    """s (BN quote - HL quote) / HL quote on the executable proxies; None without both."""
    h, b = quotes(side, hl, bn, t, unwind)
    return signed_gap(side, b, h)


def observed_gap(side, hl, bn, tau):
    """Side-blind observed gap (last prints of either side) — the reported side-blind variant's exit."""
    h, b = hl.last_at_or_before(tau), bn.last_at_or_before(tau)
    return signed_gap(side, b[1] if b else None, h[1] if h else None)


def exit_decision(side, hl, bn, t_open, cap=CAP, side_aware=True):
    """First tau in [t_open, t_open + cap] at which the unwind gap (side-aware: HL bid vs Binance ask
    for a long-HL position, executable proxies within 1 s; side-blind variant: last prints) is <= 0
    — checked at t_open and at every print of either venue after it — else the cap.
    Returns (tau, 'gap' | 'cap')."""
    taus = sorted(set([t_open] + hl.times_between(t_open, t_open + cap) + bn.times_between(t_open, t_open + cap)))
    for tau in taus:
        g = exec_gap(side, hl, bn, tau, unwind=True) if side_aware else observed_gap(side, hl, bn, tau)
        if g is not None and g <= 0:
            return tau, 'gap'
    return t_open + cap, 'cap'


def fill(series, t0, window, taker):
    """A market order sent at t0 (D8): BUY ('buy') fills at max(first buy-aggressor print at or after
    t0, the highest print of either side in [t0, that print's time]); SELL mirrors with the minimum.
    taker None: the side-blind variant, the first print of either side. Returns (t, price, delay) or
    None when no qualifying print falls within `window` (forced rows are never in the series)."""
    first = series.first_at_or_after(t0, window, taker)
    if first is None:
        return None
    if taker is None:
        return first[0], first[1], first[0] - t0
    ext = series.extreme(t0, first[0], high=(taker == 'buy'))
    px = max(first[1], ext) if taker == 'buy' else min(first[1], ext)
    return first[0], px, first[0] - t0


def worst_fill(series, t0, window, taker):
    """An exit with no qualifying print in its window: the worst print of the window (highest for a
    buy, lowest for a sell) at the window's end; None if the window holds no print at all."""
    ext = series.extreme(t0, t0 + window, high=(taker != 'sell'))
    return (t0 + window, ext, window) if ext is not None else None


def _exit_fill(series, t0, taker, counts, direction_taker):
    f = fill(series, t0, EXIT_WINDOW, taker)
    if f is None:
        f = worst_fill(series, t0, EXIT_WINDOW, direction_taker)
        counts['exit_fallback'] += 1
        if f is None:
            counts['exit_missing'] += 1
    return f


def simulate(pos, hl, bn, latency=L_PRIMARY, slip_hl=None, slip_bn=None, cap=CAP, fixed=FIXED,
             side_aware=SIDE_AWARE):
    """One position. Entry: market orders sent at t0 = t_sig + latency, filled by `fill` (side-aware,
    the primary: our taker side, never better than the extreme print between t0 and that fill)
    within FILL_WINDOW on both venues — else no position (returned as {'skipped': ...}, counted).
    t_open = the later entry fill. Exit: the convergence exit (unwind gap <= 0 at tau >= t_open ->
    orders at tau + latency) or the cap (orders at t_open + cap, scheduled); fixed exits at t_open + x.
    An exit with no qualifying print in EXIT_WINDOW fills at the window's worst print (counted).
    Both legs taker with adverse slippage per fill (per coin, D6)."""
    sh = SLIP_HL[pos['coin']] if slip_hl is None else slip_hl
    sb = SLIP_BN[pos['coin']] if slip_bn is None else slip_bn
    side = pos['side']
    wt = _takers(side)
    tk = wt if side_aware else (None,) * 4
    t0 = pos['t_sig'] + latency
    eh, eb = fill(hl, t0, FILL_WINDOW, tk[0]), fill(bn, t0, FILL_WINDOW, tk[1])
    if not eh or not eb:
        return dict(pos, skipped='no_entry_fill', missing=('hl' if not eh else '') + ('bn' if not eb else ''))
    t_open = max(eh[0], eb[0])
    counts = {'exit_fallback': 0, 'exit_missing': 0}
    tau, why = exit_decision(side, hl, bn, t_open, cap, side_aware)
    t_x = tau + latency if why == 'gap' else t_open + cap
    exits = {PRIMARY: (t_x, _exit_fill(hl, t_x, tk[2], counts, wt[2]), _exit_fill(bn, t_x, tk[3], counts, wt[3]))}
    prim_counts = dict(counts)
    for name, ms in fixed.items():
        exits[name] = (t_open + ms, _exit_fill(hl, t_open + ms, tk[2], counts, wt[2]),
                       _exit_fill(bn, t_open + ms, tk[3], counts, wt[3]))
    ret, gross, leg = {}, {}, {}
    for name, (_, xh, xb) in exits.items():
        a = (eh[1], xh[1] if xh else None, eb[1], xb[1] if xb else None)
        ret[name] = net_return(side, a[0], a[1], a[2], a[3], sh, sb)
        gross[name] = net_return(side, a[0], a[1], a[2], a[3], 0.0, 0.0, 0.0, 0.0)
        leg[name] = legs(side, *a, sh, sb) if ret[name] is not None else (None, None)
    xh, xb = exits[PRIMARY][1], exits[PRIMARY][2]
    return dict(pos, ret=ret, gross=gross, legs=leg, exit_reason=why, exit_decision=tau, t_open=t_open,
                hold_ms=(max(xh[0], xb[0]) - t0) if xh and xb else None,
                fill_hl=eh[:2], fill_bn=eb[:2], delay_hl=eh[2], delay_bn=eb[2],
                exit_delay_hl=xh[2] if xh else None, exit_delay_bn=xb[2] if xb else None,
                exit_fallback=prim_counts['exit_fallback'], exit_missing=prim_counts['exit_missing'],
                entry_gap=signed_gap(side, eb[1], eh[1]), slip=(sh, sb), latency=latency)


# ----------------------------------------------------------------------------- statistics (005's)

def _block(rows, key='hour', h=PRIMARY):
    return drift._block(rows, key, h)


def _without(rows, cluster, key='hour', h=PRIMARY):
    ex = [r for r in rows if r[key] != cluster]
    return _block(ex, 'hour', h) if ex else None


def summarise(rows, placebo_seeds=200, z_hour=Z_HOUR, z_day=Z_DAY, h=PRIMARY):
    """The eight proposed criteria on the pooled universe (all six coins):
    1 mean > 0, hour-clustered t >= z_hour; 2 day-clustered t >= z_day; 3 mean and median the same
    sign; 4 both count halves positive; 5 both liquidation sides positive; 6 majors and alts each
    positive; 7 positive without the hour with the largest POSITIVE summed net return; 8 the same
    without the largest-positive DAY."""
    rows = [r for r in rows if r['ret'].get(h) is not None]
    main = _block(rows, 'hour', h)
    day = _block(rows, 'day', h)
    srt = sorted(rows, key=lambda r: (r['entry'], r['coin']))
    half = len(srt) // 2
    halves = [_block(srt[:half], 'hour', h)['mean'], _block(srt[half:], 'hour', h)['mean']] if half >= 3 else [None, None]
    sides = {s: _block([r for r in rows if r['side'] == s], 'hour', h) for s in ('L', 'S')}
    klasses = {k: _block([r for r in rows if r['major'] == (k == 'majors')], 'hour', h) for k in ('majors', 'alts')}
    big = drift.largest_cluster(rows, 'hour', h, how='positive')
    without = _without(rows, big, h=h)
    big_day = drift.largest_cluster(rows, 'day', h, how='positive')
    without_day = _without(rows, big_day, key='day', h=h)
    pos = lambda b: b is not None and b['mean'] is not None and b['mean'] > 0
    crit = {
        '1_mean_pos_hour_t': main['t'] is not None and main['mean'] > 0 and main['t'] >= z_hour,
        '2_day_t': day['t'] is not None and day['mean'] > 0 and day['t'] >= z_day,
        '3_mean_median_same_sign': main['median'] is not None and main['mean'] is not None
                                   and (main['mean'] > 0) == (main['median'] > 0),
        '4_both_halves_positive': all(x is not None and x > 0 for x in halves),
        '5_both_sides_positive': all(pos(sides[s]) for s in ('L', 'S')),
        '6_majors_and_alts_positive': all(pos(klasses[k]) for k in ('majors', 'alts')),
        '7_without_largest_positive_hour_positive': pos(without),
        '8_without_largest_positive_day_positive': pos(without_day),
    }
    out = {'primary_hour': main, 'primary_day': day, 'criteria': crit, 'pass': all(crit.values()),
           'halves': halves, 'by_side': sides, 'by_class': klasses, 'largest_positive_hour': big,
           'without_largest_positive_hour': without, 'largest_positive_day': big_day,
           'without_largest_positive_day': without_day,
           'without_largest_abs_hour': _without(rows, drift.largest_cluster(rows, 'hour', h, how='abs'), h=h),
           'without_largest_count_hour': _without(rows, drift.largest_cluster(rows, 'hour', h, how='count'), h=h)}
    if placebo_seeds:
        out['placebo_hour_flip'] = drift.placebo(rows, 'hour', placebo_seeds, z_hour, h=h)
        out['placebo_day_flip'] = drift.placebo(rows, 'day', placebo_seeds, z_hour, h=h)
    return out


def decide(summary, l3_mean, gate=GATE):
    """Decision table (registration §9, D13/D15): a discovery pass (all eight criteria) with a pooled
    point estimate >= GATE and a positive mean at L = 3 s -> 'ask_holdout' (the owner approves the
    Binance holdout pull and the HL holdout fills first); a pass failing the floor or the 3-s check
    -> 'close_below_floor' / 'close_latency'; otherwise 'close' (H3 closes)."""
    if not summary['pass']:
        return 'close'
    if summary['primary_hour']['mean'] < gate:
        return 'close_below_floor'
    return 'ask_holdout' if (l3_mean is not None and l3_mean > 0) else 'close_latency'


def holdout_confirm(rows, z=Z_HOLDOUT, h=PRIMARY):
    """The holdout read (005 D14 rules): CONFIRM if mean > 0 with one-sided hour-clustered t >= 1.64,
    median > 0, and mean >= 0 without the largest-positive hour."""
    rows = [r for r in rows if r['ret'].get(h) is not None]
    main = _block(rows, 'hour', h)
    without = _without(rows, drift.largest_cluster(rows, 'hour', h, how='positive'), h=h)
    crit = {'mean_pos_hour_t': main['t'] is not None and main['mean'] > 0 and main['t'] >= z,
            'median_pos': main['median'] is not None and main['median'] > 0,
            'without_largest_positive_hour_nonneg': without is not None and without['mean'] is not None
                                                    and without['mean'] >= 0}
    return all(crit.values()), {'primary_hour': main, 'criteria': crit}


# ----------------------------------------------------------------------------- reported measurements (guarded)

def gap_profile(side, t0, p_hl0, p_bn0, hl, bn, offsets=PROFILE, mode='sweep'):
    """The gap's path after a forced print at t0 (reference prices p_hl0 / p_bn0 at t0: the forced print
    and Binance's last trade for mode='sweep', the quote proxies for mode='exec'): for each offset x,
    (gap, hl_close, bn_close) with gap = the signed gap at t0 + x of HL's last non-forced book print vs
    Binance's last trade (sweep) or of the opening-side quote proxies within 1 s (exec); x = 0: the
    reference prices. Under 'sweep', hl_close at +0.25..+1 s says whether HL's book reverts above the
    forced print within a second — the price a resting order filled AT the sweep would earn (H1), hl_close = s * (HL(x) - p_hl0) / p_hl0 (> 0: HL moved to close),
    bn_close = -s * (BN(x) - p_bn0) / p_hl0 (> 0: Binance moved to close); gap(0) - gap(x) =
    hl_close + bn_close up to the denominator. Guarded runner only: reads prices after t0."""
    s = direction(side)
    out = {}
    for x in offsets:
        if x == 0:
            ph, pb = p_hl0, p_bn0
        elif mode == 'exec':
            ph, pb = quotes(side, hl, bn, t0 + x)
        else:
            h, b = hl.last_at_or_before(t0 + x), bn.last_at_or_before(t0 + x)
            ph, pb = (h[1] if h else None), (b[1] if b else None)
        if not (ph and pb and p_hl0 and p_bn0):
            out[x] = (None, None, None)
            continue
        out[x] = (signed_gap(side, pb, ph), s * (ph - p_hl0) / p_hl0, -s * (pb - p_bn0) / p_hl0)
    return out


def weighted_profile(profiles):
    """Notional-weighted mean of gap profiles: [(weight, profile)] -> {x: (gap, hl_close, bn_close)}."""
    acc = {}
    for w, prof in profiles:
        for x, vals in prof.items():
            if vals[0] is None:
                continue
            a = acc.setdefault(x, [0.0, 0.0, 0.0, 0.0])
            a[0] += w
            for j in range(3):
                a[j + 1] += w * vals[j]
    return {x: tuple(a[j + 1] / a[0] for j in range(3)) for x, a in acc.items() if a[0] > 0}


def minute_gap(side, p_hl_last, bn_next_minute_open):
    """002 §11's consistency number, signed: s * (Binance next-minute open - HL last forced print) /
    HL last forced print. Guarded runner only."""
    return signed_gap(side, bn_next_minute_open, p_hl_last)


# ----------------------------------------------------------------------------- power (behaviour only)

def null_se(positions, sigma, rho, hold=CAP, key=None):
    """drift.null_se with the 60-s hold: SE of the mean under no effect, sigma = {coin: SD of the
    60-s change of the cross-venue gap}."""
    return drift.null_se(positions, sigma, rho, hold=hold, key=key)


def simulate_power(positions, sigma, rho, mu, sims=2000, seed=0, hold=CAP, z_hour=Z_HOUR, holdout_n=None,
                   hold_sims=None):
    """Joint power of the seven criteria at the actual trigger times, sides, coins and halves:
    net return = mu + correlated noise (drift.null_cov with a 60-s hold). Returns P(pass all seven),
    P(pass with estimate >= GATE) (the D13 route to the holdout), P(pass with estimate >= BAR), and
    with holdout_n: P(holdout CONFIRM) on contiguous blocks of holdout_n positions of the same
    pattern, and the end-to-end P(pass, >= GATE, CONFIRM)."""
    order, low = drift.null_cov(positions, sigma, rho, hold=hold, signed=False)
    L = drift.cholesky(low)
    srt = [positions[k] for k in order]
    rng = random.Random(seed)
    passes = bar = gate = 0
    for _ in range(sims):
        x = drift.mvn_draw(L, rng)
        rows = [dict(p, ret={PRIMARY: mu + xi}) for p, xi in zip(srt, x)]
        sm = summarise(rows, placebo_seeds=0, z_hour=z_hour)
        passes += sm['pass']
        gate += sm['pass'] and sm['primary_hour']['mean'] >= GATE
        bar += sm['pass'] and sm['primary_hour']['mean'] >= BAR
    out = {'mu': mu, 'sims': sims, 'p_pass': passes / sims, 'p_pass_and_gate': gate / sims, 'p_pass_and_bar': bar / sims}
    if holdout_n:
        n = min(holdout_n, len(srt))
        hs = hold_sims or sims
        conf = 0
        for _ in range(hs):
            start = rng.randrange(0, len(srt) - n + 1)
            blk = srt[start:start + n]
            o2, l2 = drift.null_cov(blk, sigma, rho, hold=hold, signed=False)
            xb = drift.mvn_draw(drift.cholesky(l2), rng)
            conf += holdout_confirm([dict(blk[o], ret={PRIMARY: mu + xi}) for o, xi in zip(o2, xb)])[0]
        out['holdout_n'] = n
        out['p_confirm'] = conf / hs
        out['end_to_end'] = out['p_pass_and_gate'] * out['p_confirm']
    return out


def quantiles(xs, qs=(0.1, 0.25, 0.5, 0.75, 0.9)):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return {}
    return {q: xs[min(len(xs) - 1, int(q * len(xs)))] for q in qs}


def median(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


__all__ = ['Series', 'signed_gap', 'first_trigger', 'plan', 'simulate', 'net_return', 'summarise', 'decide',
           'gap_profile', 'minute_gap', 'null_se', 'simulate_power', 'es']
