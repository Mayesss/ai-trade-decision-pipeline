"""Leader events, the exact leader baseline, and the polling follower.

PILOT VALUES. Nothing in Params is frozen — P4 freezes one primary
configuration (docs/copy-trade-plan-2026-10-07.md §5.5).

Units: HL quotes per HL unit, Bitget per Bitget unit, and
bitget_qty = hl_qty * qty_mult, so bitget_px = hl_px / qty_mult.
"""
import bisect
import heapq
import math
import statistics
from dataclasses import dataclass

from . import bitget as bg  # leader_pnl marks at Bitget prices
from .venue import HEDGE_SYMBOL

MINUTE = 60_000
HOUR = 3_600_000
DAY = 86_400_000
EPS = 1e-12


def chain_breaks(fills):
    """{coin: n} where a fill's startPosition is not the previous fill's end.

    A break means a position change with no fill to explain it: a fill the
    source did not return. P0 found TWAP slices are absent from
    userFillsByTime and only ~13 days of them survive on the API, so wallets
    that TWAP have unrecoverable gaps on the free API. A coin with a break
    cannot be replayed exactly; callers exclude it.
    """
    breaks, pos = {}, {}
    for f in sorted(fills, key=lambda f: f['time']):
        coin, start = f['coin'], float(f['startPosition'])
        if coin in pos and abs(pos[coin] - start) > 1e-9 * max(1.0, abs(start)):
            breaks[coin] = breaks.get(coin, 0) + 1
        sz = float(f['sz'])
        pos[coin] = start + (sz if f['side'] == 'B' else -sz)
    return breaks


def leader_events(fills, symtab, listed_from, group_gap_ms=1_000):
    """Group a leader's perp fills into per-coin position changes.

    Each event carries the leader's position before and after, taken from the
    fills' own `startPosition` — so a fill missing from the window cannot
    corrupt the position.

    A coin is copied only after the leader is seen opening it from flat while
    it is listed on Bitget ("armed"). A position already open at the window
    start, or opened before Bitget listed the coin, was never enterable by a
    follower at the leader's time.
    """
    stats = {}
    bump = lambda k: stats.__setitem__(k, stats.get(k, 0) + 1)
    events, growing, armed = [], {}, set()
    # Stable sort on time ONLY. Fills sharing a millisecond are not in tid
    # order; the API's own order is the execution order. Measured on the P0
    # sample (224,949 perp fills): API order breaks the startPosition chain
    # 289 times, a (time, tid) sort 171,872 times.
    for f in sorted(fills, key=lambda f: f['time']):
        m = symtab.get(f['coin'])
        if m is None:
            bump('fills_not_hl_perp')  # spot ('@107', 'PURR/USDC') or HIP-3 ('dex:COIN')
            continue
        if m['status'] != 'ok':
            bump(f'fills_{m["status"]}')
            continue
        coin, t = f['coin'], f['time']
        sz, px = float(f['sz']), float(f['px'])
        signed = sz if f['side'] == 'B' else -sz
        before = float(f['startPosition'])
        after = before + signed

        ev = growing.get(coin)
        if ev is not None and t - ev['t_last'] <= group_gap_ms:
            ev['t_last'], ev['pos_after'] = t, after
            ev['notional'] += sz * px
            ev['signed_notional'] += signed * px
            ev['size'] += sz
            ev['n_fills'] += 1
            continue

        if coin not in armed:
            listed = listed_from.get(m['symbol'])
            if listed is None or t < listed:
                bump('fills_before_bitget_listing')
                continue
            if abs(before) > EPS:
                bump('fills_on_preexisting_position')
                continue
            armed.add(coin)
        ev = {'coin': coin, 'symbol': m['symbol'], 'qty_mult': m['qty_mult'],
              'min_usdt': m['min_usdt'], 'size_step': m['size_step'], 'min_qty': m['min_qty'],
              't': t, 't_last': t, 'pos_before': before, 'pos_after': after,
              'notional': sz * px, 'signed_notional': signed * px, 'size': sz, 'n_fills': 1}
        growing[coin] = ev
        events.append(ev)
    for ev in events:
        ev['vwap'] = ev['notional'] / ev['size']
    stats['events'] = len(events)
    stats['coins'] = len(armed)
    return events, stats


def _mark_hl_px(ev, mark_minute):
    c = bg.candle_at(ev['symbol'], mark_minute)
    return (c[4] * ev['qty_mult']) if c else ev['vwap']


def leader_pnl(events, scale, end_ms):
    """What the leader's copied positions earned, exactly, at `scale`.

    Cash flow is the sum of signed size x price over the leader's own fills
    (buys and sells inside one event are not netted at an average price), no
    costs. Open positions are marked at the Bitget close at end_ms. This is the
    ceiling every follower run is measured against.
    """
    cash, last = 0.0, {}
    for ev in events:
        cash -= ev['signed_notional'] * scale
        last[ev['coin']] = ev
    mark = end_ms - end_ms % MINUTE - MINUTE
    for ev in last.values():
        if abs(ev['pos_after']) > EPS:
            cash += ev['pos_after'] * scale * _mark_hl_px(ev, mark)
    return cash


def closed_pnl_check(fills, events, scale, end_ms):
    """Exact replay vs the leader's own closedPnl, per coin that ends flat.

    A round trip replayed from fills must equal what Hyperliquid reports the
    leader realized. Tolerance is 0.1% of the largest position notional, not a
    share of the PnL: Hyperliquid's reported PnL uses a rounded entry price,
    which on sub-cent coins traded in millions of units drifts by up to ~0.05%
    of notional (P3 gate G4, plan §8). Returns (agree, total, mismatches).
    """
    by = {}
    for e in events:
        by.setdefault(e['coin'], []).append(e)
    agree, total, bad = 0, 0, []
    for coin, evs in by.items():
        if abs(evs[-1]['pos_after']) > EPS:
            continue
        replayed = leader_pnl(evs, scale, end_ms)
        reported = scale * sum(float(f['closedPnl']) for f in fills
                               if f['coin'] == coin and f['time'] >= evs[0]['t'])
        notional = scale * max(max(abs(e['pos_before']), abs(e['pos_after'])) * e['vwap'] for e in evs)
        total += 1
        if abs(replayed - reported) <= max(0.01, 0.001 * notional):
            agree += 1
        else:
            bad.append((coin, round(replayed, 2), round(reported, 2)))
    return agree, total, bad


@dataclass(frozen=True)
class Params:
    poll_min: int                     # follower reads the leaders every poll_min minutes, acts the next minute
    capital_usd: float                # sizing base; fixed, not compounded, so runs are comparable
    max_leverage: float = 3.0         # hard cap on follower gross / equity, applied at every poll
    band: float = 0.25                # rebalance a symbol only when off target by more than this fraction
    slip_range_frac: float = 0.25     # adverse fill = open +/- this x the minute's high-low. PILOT GUESS
    maintenance_margin: float = 0.01  # of gross notional. PILOT GUESS of Bitget's small-size tiers
    frictionless: bool = False        # no fees, slippage, funding, liquidation, lot or minimum-size rules:
                                      # mechanics checks only
    hedge: bool = True                # BTC hedge on a separate ledger (registration 001 §7)
    hedge_mode: str = 'static'        # 'static': one position sized from the leader's lookback average beta
                                      #           (static_beta, the FOLLOWER's beta), re-trued daily on the band;
                                      # 'holdings': -(sum of holdings x beta), rebalanced after every trade —
                                      #           removes timing skill too; reported variant (revision 4)
    static_beta: float = 0.0          # follower net beta to hedge in 'static' mode


def _round_qty(qty, step):
    n = math.floor(abs(qty) / step + 1e-9)
    return math.copysign(n * step, qty)


def follow(target, p, venue, end_ms):
    """Simulate a follower that polls its leaders every p.poll_min minutes.

    `target` is a builder from ct/targets.py (single leader, top-K copy or
    consensus); it turns the leaders' positions at poll time into signed
    weights of capital. `venue` (ct/venue.py) supplies prices, fees, funding
    and the trading calendar.

    At each poll the follower reads the leaders (fills completed by then),
    converts weights to venue quantities at the execution price, scales the
    book down to max_leverage if needed, and rebalances symbols off target by
    more than the band — opens, closes and side flips always trade. Orders fill
    at the open of the minute after the poll, adverse by slippage, paying the
    venue fee, rounded down to the lot step, skipped below the venue minimum.
    A poll that lands while the venue is closed is re-run at the next open,
    reading the leaders afresh then. Funding is charged at venue settlements;
    an hourly check liquidates the whole book if equity at the hour's adverse
    extremes falls under maintenance margin, and following stops there.
    Everything still open at end_ms is closed, paying costs.

    Every follower round trip (flat -> flat per symbol) is recorded with its
    net result in ATR units: net / (max notional held x daily ATR% at entry).
    The denominator is the notional the follower actually held, not a leader's.
    """
    poll = p.poll_min * MINUTE
    meta = target.meta
    polls = sorted({-(-t // poll) * poll for t in target.change_times()})
    polls = [t for t in polls if t < end_ms]
    if not polls:
        return None
    first_hour = polls[0] - polls[0] % HOUR + HOUR
    heap = [(h, 0) for h in range(first_hour, end_ms, HOUR)] + [(t, 1) for t in polls]
    heapq.heapify(heap)
    seen_polls = set()

    pos, last_px, trips, closed, fund_ptr = {}, {}, {}, [], {}
    st = {'cash': 0.0, 'fees': 0.0, 'slippage': 0.0, 'funding_paid': 0.0, 'turnover': 0.0,
          'max_gross_leverage': 0.0}
    n = {'orders': 0, 'below_min_size': 0, 'below_lot_step': 0, 'band_skips': 0,
         'leverage_capped_polls': 0, 'no_venue_price': 0, 'polls_deferred_closed': 0,
         'funding_history_too_short': 0}
    daily = []
    liquidated = None
    fee_rate = 0.0 if p.frictionless else venue.taker_fee
    # Hedge ledger: a BTCUSDT position sized daily to -(sum of holdings x beta),
    # traded with the same fees and slippage, kept apart from the copy book so
    # hedged and unhedged results are both reported.
    hedging = p.hedge and not p.frictionless
    static = hedging and p.hedge_mode == 'static'
    if hedging and p.hedge_mode not in ('static', 'holdings'):
        raise ValueError(f'unknown hedge_mode {p.hedge_mode!r}')

    def static_target(btc_px):
        """Hedge quantity for the static mode: a constant dollar notional."""
        return -p.static_beta * p.capital_usd / btc_px

    def retrue_static(candle):
        """Re-true the static hedge when price drift has moved it off by more than the band."""
        if candle is None:
            return
        tgt = static_target(candle[1])
        if abs(tgt - hg['pos']) > p.band * abs(tgt) or (abs(tgt) < 1e-12 and abs(hg['pos']) > 1e-12):
            hedge_to(tgt, candle)
    hg = {'pos': 0.0, 'cash': 0.0, 'fees': 0.0, 'slippage': 0.0, 'trades': 0, 'beta_fallbacks': 0}
    daily_h = []

    def hedge_to(target, candle):
        """Move the hedge to `target` BTC at this candle's open, Bitget lot rules."""
        if candle is None:
            return
        qty = _round_qty(target - hg['pos'], 0.0001)
        if abs(qty) < 1e-12 or (abs(target) > 1e-12 and abs(qty) * candle[1] < 5.0):
            return
        slip_px = p.slip_range_frac * (candle[2] - candle[3])
        px = candle[1] + slip_px if qty > 0 else candle[1] - slip_px
        fee = abs(qty) * px * fee_rate
        hg['cash'] -= qty * px + fee
        hg['fees'] += fee
        hg['slippage'] += abs(qty) * slip_px
        hg['pos'] += qty
        hg['trades'] += 1

    def fill(sym, q, px, fee, slip_px, t):
        before = pos.get(sym, 0.0)
        after = before + q
        if abs(before) < EPS:
            trips[sym] = {'symbol': sym, 'open_t': t, 'cash': 0.0, 'fees': 0.0, 'slippage': 0.0,
                          'funding': 0.0, 'max_notional': 0.0, 'atr_pct': venue.atr_pct(sym, t)}
        trip = trips[sym]
        trip['cash'] -= q * px + fee
        trip['fees'] += fee
        trip['slippage'] += abs(q) * slip_px
        st['cash'] -= q * px + fee
        st['fees'] += fee
        st['slippage'] += abs(q) * slip_px
        st['turnover'] += abs(q) * px
        n['orders'] += 1
        last_px[sym] = px
        if abs(after) < EPS * max(1.0, abs(before)):
            pos[sym] = 0.0
            trip['close_t'] = t
            closed.append(_finish_trip(trips.pop(sym), 0.0))
        else:
            pos[sym] = after
            trip['max_notional'] = max(trip['max_notional'], abs(after) * px)

    def trade(sym, qty, candle, t):
        o, h, l = candle[1], candle[2], candle[3]
        slip_px = 0.0 if p.frictionless else p.slip_range_frac * (h - l)
        cur = pos.get(sym, 0.0)
        parts = [qty]
        if abs(cur) > EPS and abs(cur + qty) > EPS and math.copysign(1, cur + qty) != math.copysign(1, cur):
            parts = [-cur, cur + qty]  # side flip: close one trip, open the next
        for q in parts:
            px = o + slip_px if q > 0 else o - slip_px
            fill(sym, q, px, abs(q) * px * fee_rate, slip_px, t)

    def settle_funding(up_to):
        if p.frictionless:
            return
        for sym, q in pos.items():
            if sym not in fund_ptr:
                continue
            series = venue.funding(sym)
            i = fund_ptr[sym]
            while i < len(series) and series[i][0] <= up_to:
                ts, rate = series[i]
                if abs(q) > EPS:
                    # Marked at the 1H close of the settlement hour (cached offline for every
                    # symbol); the 1-minute candle at the settlement needed a network fetch
                    # per held coin-hour (amendment 1, 2026-10-09).
                    c = venue.candle_hour(sym, ts - ts % HOUR)
                    paid = q * (c[4] if c else last_px[sym]) * rate  # longs pay a positive rate
                    st['cash'] -= paid
                    st['funding_paid'] += paid
                    trips[sym]['cash'] -= paid
                    trips[sym]['funding'] += paid
                i += 1
            fund_ptr[sym] = i

    while heap:
        t, kind = heapq.heappop(heap)
        if kind == 0:  # hourly: funding, liquidation check, daily mark
            settle_funding(t)
            held = {s: q for s, q in pos.items() if abs(q) > EPS}
            closes = {}
            if held:
                adverse_eq, gross, adverse = p.capital_usd + st['cash'], 0.0, {}
                for s, q in held.items():
                    c = venue.candle_hour(s, t - HOUR)
                    hi, lo, cl = (c[2], c[3], c[4]) if c else (last_px[s],) * 3
                    adverse[s] = lo if q > 0 else hi
                    closes[s] = cl
                    adverse_eq += q * adverse[s]
                    gross += abs(q) * cl
                    last_px[s] = cl
                if not p.frictionless and adverse_eq <= p.maintenance_margin * gross:
                    for s, q in held.items():
                        fill(s, -q, adverse[s], abs(q) * adverse[s] * fee_rate, 0.0, t)
                    liquidated = t
                    daily.append((t, st['cash']))
                    if hedging:
                        hedge_to(0.0, venue.candle_minute(HEDGE_SYMBOL, t + MINUTE))
                        daily_h.append((t, st['cash'] + hg['cash']))
                    break
            if t % DAY == 0:
                book = st['cash'] + sum(q * closes.get(s, last_px[s]) for s, q in held.items())
                daily.append((t, book))
                if hedging:
                    btc = venue.candle_hour(HEDGE_SYMBOL, t - HOUR)
                    if btc:
                        daily_h.append((t, book + hg['cash'] + hg['pos'] * btc[4]))
                        if static:
                            retrue_static(venue.candle_minute(HEDGE_SYMBOL, t + MINUTE))
                        else:
                            exposure = 0.0
                            for s, q in held.items():
                                b = venue.beta(s, t)
                                if b is None:
                                    b = 1.0
                                    hg['beta_fallbacks'] += 1
                                exposure += q * closes.get(s, last_px[s]) * b
                            hedge_to(-exposure / btc[4], venue.candle_minute(HEDGE_SYMBOL, t + MINUTE))
            continue

        # poll
        if t in seen_polls:
            continue
        seen_polls.add(t)
        x = t + MINUTE
        if not venue.is_open(x):
            n['polls_deferred_closed'] += 1
            reopen = venue.next_open(x)
            if reopen is not None and reopen - MINUTE < end_ms:
                heapq.heappush(heap, (reopen - MINUTE, 1))
            continue
        target.advance(t)
        settle_funding(x)
        candles = {}
        for s in target.symbols() | {s for s, q in pos.items() if abs(q) > EPS}:
            c = venue.candle_minute(s, x)
            if c is None:
                n['no_venue_price'] += 1
            else:
                candles[s] = c
        weights = target.weights(t, lambda s: candles[s][1] if s in candles else None)
        targets = {s: 0.0 for s in candles}
        targets.update({s: w * p.capital_usd / candles[s][1] for s, w in weights.items() if s in candles})
        # st['cash'] holds trading cash flows only; equity adds the capital back.
        equity = p.capital_usd + st['cash'] + sum(q * (candles[s][1] if s in candles else last_px[s])
                                                  for s, q in pos.items())
        gross = sum(abs(q) * candles[s][1] for s, q in targets.items())
        cap = p.max_leverage * max(equity, 0.0)
        if gross > cap:
            f = cap / gross if gross > 0 else 0.0
            targets = {s: q * f for s, q in targets.items()}
            n['leverage_capped_polls'] += 1
        orders_before = n['orders']
        for s, tgt in targets.items():
            cur = pos.get(s, 0.0)
            if s not in fund_ptr:
                series = venue.funding(s) if not p.frictionless else []
                fund_ptr[s] = bisect.bisect_right([r[0] for r in series], x)
                if series and series[0][0] > x:
                    n['funding_history_too_short'] += 1
            if abs(tgt) < EPS:
                if abs(cur) > EPS:
                    trade(s, -cur, candles[s], x)
                continue
            flip = abs(cur) < EPS or math.copysign(1, tgt) != math.copysign(1, cur)
            if not flip and abs(tgt - cur) <= p.band * abs(tgt):
                n['band_skips'] += 1
                continue
            qty = tgt - cur
            if not p.frictionless:
                m = meta[s]
                qty = _round_qty(qty, m['size_step'])
                if abs(qty) < max(m['min_qty'], EPS):
                    n['below_lot_step'] += 1
                    continue
                if abs(qty) * candles[s][1] < m['min_usdt']:
                    n['below_min_size'] += 1
                    continue
            trade(s, qty, candles[s], x)
        gross_now = sum(abs(q) * last_px[s] for s, q in pos.items())
        st['max_gross_leverage'] = max(st['max_gross_leverage'], gross_now / p.capital_usd)
        # Re-hedge right after a poll that traded, not only at the daily mark:
        # a daily-only hedge lags every entry and exit by up to a day (measured
        # on a pure-BTC synthetic book: 70% of the BTC move removed vs ~all).
        if static and hg['trades'] == 0 and abs(p.static_beta) > 1e-12:
            btc = venue.candle_minute(HEDGE_SYMBOL, x)   # open the static hedge at the first poll
            if btc:
                hedge_to(static_target(btc[1]), btc)
        if hedging and not static and n['orders'] > orders_before:
            btc = venue.candle_minute(HEDGE_SYMBOL, x)
            if btc:
                exposure = 0.0
                for s, q in pos.items():
                    if abs(q) > EPS:
                        b = venue.beta(s, x)
                        if b is None:
                            b = 1.0
                            hg['beta_fallbacks'] += 1
                        exposure += q * (candles[s][1] if s in candles else last_px[s]) * b
                hedge_to(-exposure / btc[1], btc)

    if liquidated is None:
        settle_funding(end_ms)
    # Window end: CLOSE everything at the last minute, paying costs, rather than
    # marking to market. In P0b a mark-to-market end put 56% of one wallet's
    # result in 5 positions that were never realized.
    mark = end_ms - end_ms % MINUTE - MINUTE
    forced = 0
    for s, q in list(pos.items()):
        if abs(q) > EPS:
            c = venue.candle_minute(s, mark) or [mark] + [last_px[s]] * 4
            trips[s]['closed_by_window_end'] = True
            trade(s, -q, c, mark)
            forced += 1
    net = st['cash']
    daily.append((end_ms, net))
    if hedging:
        hedge_to(0.0, venue.candle_minute(HEDGE_SYMBOL, mark))
        daily_h.append((end_ms, net + hg['cash']))

    peak, max_dd = p.capital_usd, 0.0
    for _, eq in daily:
        peak = max(peak, p.capital_usd + eq)
        max_dd = max(max_dd, (peak - (p.capital_usd + eq)) / peak)
    def daily_returns(series, final):
        """[(t, return on capital)] at each UTC day mark and the window end."""
        marks = [(t, eq) for t, eq in series if t % DAY == 0] + [(end_ms, final)]
        prev, out = 0.0, []
        for t, eq in marks:
            out.append((t, (eq - prev) / p.capital_usd))
            prev = eq
        return out

    def daily_stats(series, final):
        rets = [r for _, r in daily_returns(series, final)]
        return {'n': len(rets), 'mean': statistics.fmean(rets),
                'sd': statistics.stdev(rets) if len(rets) > 1 else None,
                'returns': daily_returns(series, final)}

    hedged = None
    if hedging:
        net_h = net + hg['cash']
        hedged = {'mode': p.hedge_mode, 'static_beta': p.static_beta if static else None,
                  'net_usd': net_h, 'net_return': net_h / p.capital_usd,
                  'daily': daily_stats(daily_h, net_h),
                  **{f'hedge_{k}': v for k, v in hg.items() if k not in ('cash', 'pos')}}
    return {
        'net_usd': net,
        'net_return': net / p.capital_usd,
        'gross_usd': net + st['fees'] + st['slippage'] + st['funding_paid'],
        **{k: v for k, v in st.items() if k != 'cash'},
        'max_drawdown': max_dd,
        'liquidated_at': liquidated,
        'closed_by_window_end': forced,
        **n,
        'daily': daily_stats(daily, net),
        'hedged': hedged,
        'r': r_summary(closed),
        'trips': closed,
        'daily_equity': daily,
    }


def _finish_trip(trip, mark_value):
    net = trip['cash'] + mark_value
    risk = trip['max_notional'] * trip['atr_pct'] if trip['atr_pct'] else None
    return {**{k: v for k, v in trip.items() if k != 'cash'}, 'net_usd': net,
            'risk_usd': risk, 'r_atr': net / risk if risk else None}


def r_summary(trips):
    """Per-trip net ATR-R, equal-weighted and risk-weighted.

    Equal-weighted (mean, median, 10%-trimmed mean) asks "is the typical copied
    trade good"; risk-weighted (sum of net / sum of risk) asks "are the trades
    the leader sized up good". They disagree when a few large trades carry the
    result — in P0 the median trip was -0.11 R while dollars were positive. Copy
    returns are skewed, and the spec's reversion study died on a signal that
    predicted the median and anti-predicted the mean, so all are reported.
    """
    with_r = [t for t in trips if t['r_atr'] is not None]
    rs = sorted(t['r_atr'] for t in with_r)
    if not rs:
        return {'n': 0, 'no_atr': len(trips)}
    k = len(rs) // 10
    trimmed = rs[k:len(rs) - k] or rs
    return {'n': len(rs), 'mean': statistics.fmean(rs), 'median': statistics.median(rs),
            'trimmed_mean_10': statistics.fmean(trimmed),
            'risk_weighted': sum(t['net_usd'] for t in with_r) / sum(t['risk_usd'] for t in with_r),
            'no_atr': len(trips) - len(with_r)}


def benchmark_daily_returns(venue, times, start_ms, symbol=HEDGE_SYMBOL):
    """[(t, return)] of `symbol` between consecutive marks in `times` (1H closes before each mark).

    Pairs with a follower's daily returns for the regression-alpha outcome
    (registration 001 §6). The first return runs from the close before
    start_ms (the window start, where the follower's capital starts), so both
    series have one entry per mark covering the same span.
    """
    out = []
    c0 = venue.candle_hour(symbol, start_ms - start_ms % HOUR - HOUR)
    prev = c0[4] if c0 else None
    for t in times:
        c = venue.candle_hour(symbol, t - t % HOUR - HOUR)
        px = c[4] if c else None
        if prev is None:
            prev = px
        out.append((t, (px / prev - 1) if px and prev else 0.0))
        prev = px or prev
    return out
