"""Registration 016 — the Zarattini, Pagani & Barbon (2025) crypto trend rule, verbatim: pure functions.

Everything here is a function of its arguments (no file I/O, no clock, no network). The runner (`r016_run.py`)
loads the data behind its guard and calls these; `ct16.sim` calls them on synthetic panels; the tests call them on
both. Page and section numbers refer to the paper's printed page numbers (SFI RP 25-80, version 2025-04-09).

Vocabulary
  day      a UTC calendar date as an ordinal (`datetime.date.toordinal()`); the daily close is the 1d kline close
           (00:00 UTC of the next day), so "the close of day d" ends day d.
  row      one 1d kline of one symbol: (close, quote volume in USDT, trade count).
  traded   a row with count > 0. Rows with count == 0 are dead (a delisted or halted contract keeps flat rows).
  sleeve   one universe member's 1/B of book capital (§7.2), running the Combo (§4.3) independently.
  h_n      a sleeve's held weight in lookback n's sub-model, in units of sleeve equity (§4.3: w_n).
  H        a sleeve's exposure, mean over the nine h_n (w_Combo).
"""
import bisect
import datetime as dt
import math
import re
from collections import deque

# ------------------------------------------------------------------ the rule (paper §4.1-§4.3, §5.1, §7.1-§7.2)
LOOKBACKS = (5, 10, 20, 30, 60, 90, 150, 250, 360)     # §4.3, p. 10
VOL_TARGET = 0.25                                      # §4.2, p. 8: "target annualized volatility is 25%"
LEV_CAP = 2.0                                          # §4.2, p. 9: "we do not allow the allocation to exceed 200%"
VOL_WIN = 90                                           # §4.2, p. 9: "90-day annualized volatility"; §4.3 "3-month"
ANN_DAYS = 365                                         # D3: crypto trades every calendar day (paper silent)
THRESHOLD = 0.20                                       # §5.1, p. 14-15: rebalance only if |target - current| > 20%
COST_PAPER = 0.0010                                    # §5.1, p. 15: "net of a transaction cost of 10 basis points"
COST_DEPLOY = 0.0006                                   # D7: Binance taker 5 bp (4.5 with BNB) + 1 bp slippage
B = 20                                                 # §7.1-§7.2, Table 4: the headline top-20 book
MIN_AGE_DAYS = 365                                     # §7.1, p. 18: "listed for at least 365 calendar days"
MIN_MEDIAN_QV = 2e6                                    # §7.1: "median daily trading volume of at least $2 million"
MEDIAN_WIN = 30                                        # §7.1: "over the preceding 30 days"; Fig. 5 "trailing 30-day"
EXIT_MEDIAN_QV = 1e6                                   # §7.2, p. 18: removed if 30-day median volume < $1 million
EXIT_MEDIAN_ABS_RET = 0.005                            # §7.2: removed if 30-day median daily price change < 0.5%
TRADED_TAIL_DAYS = 7                                   # D9 (delisting cut): traded on each of the month's last 7 days

# ------------------------------------------------------------------ the calendars (registration §2)
PAPER_END = dt.date(2025, 3, 19)                       # §5-§7: every backtest ends 2025-03-19
SPAN_A = (dt.date(2025, 3, 20), dt.date(2026, 6, 30))  # out of the paper's sample, before the programme's cut
SPAN_C = (dt.date(2026, 7, 1), dt.date(2027, 12, 31))  # forward, unread, claimed openly
START, END = SPAN_A[0], SPAN_C[1]
READ_NOT_BEFORE = dt.date(2028, 1, 8)                  # END + 7 days: the guard refuses any fetch or read before it
CELLS = ((dt.date(2025, 3, 20), dt.date(2025, 6, 30)), (dt.date(2025, 7, 1), dt.date(2025, 12, 31)),
         (dt.date(2026, 1, 1), dt.date(2026, 6, 30)), (dt.date(2026, 7, 1), dt.date(2026, 12, 31)),
         (dt.date(2027, 1, 1), dt.date(2027, 6, 30)), (dt.date(2027, 7, 1), dt.date(2027, 12, 31)))
YEAR_CELLS = ((dt.date(2025, 3, 20), dt.date(2025, 12, 31)), (dt.date(2026, 1, 1), dt.date(2026, 12, 31)),
              (dt.date(2027, 1, 1), dt.date(2027, 12, 31)))   # reported variant (D14)
STUB_MONTH, STUB_INTO = '2025-03', '2025-04'           # the 12-day March 2025 stub joins April's cluster (G = 33)

# ------------------------------------------------------------------ the universe exclusions (§3, §7.1; D8)
STABLE_BASES = frozenset('USDC BUSD TUSD FDUSD USDP DAI USDE PYUSD USD1 RLUSD XUSD BFUSD USDS EUR'.split())
WRAPPED_BASES = frozenset('WBTC WETH BETH WBETH STETH WSTETH CBBTC'.split())   # §3: "wrapped tokens"
NON_CRYPTO_BASES = frozenset((                         # the C1 scout's frozen name list (gold, TradFi), plus index perps
    'XAU XAG XPT XPD XAUT PAXG CL BZ NATGAS COPPER TSLA MSTR NVDA AMD INTC QCOM MU MRVL SNDK ARM AAOI LITE NBIS RKLB '
    'COIN HOOD CRCL PLTR AMZN GOOGL NOK SAMSUNG SKHYNIX QQQ TQQQ SPY SOXL EWY KORU AXTI DEFI BTCDOM ALL').split())


SYMBOL_RE = re.compile(r'^[A-Z0-9]+USDT$')


class Refused(Exception):
    """A refusal (never an assert: `python -O` cannot strip it)."""


def refuse_unless(cond, msg):
    if not cond:
        raise Refused(msg)


# ------------------------------------------------------------------ dates
def day(d):
    return d.toordinal() if isinstance(d, dt.date) else int(d)


def date_of(o):
    return dt.date.fromordinal(o)


def month_of(o):
    d = date_of(o)
    return f'{d.year:04d}-{d.month:02d}'


def cluster_of(o):
    m = month_of(o)
    return STUB_INTO if m == STUB_MONTH else m


def cell_of(o, cells=CELLS):
    for i, (a, b) in enumerate(cells):
        if day(a) <= o <= day(b):
            return i
    return None


def is_month_end(o):
    return date_of(o + 1).day == 1


def ms_to_day(v):
    """Binance open_time (ms; microseconds in some newer files) -> UTC day ordinal."""
    v = int(v)
    sec = v / 1e6 if v >= 10 ** 14 else v / 1e3
    return dt.datetime.fromtimestamp(sec, dt.UTC).date().toordinal()


# ------------------------------------------------------------------ the universe filter by name
def crypto_symbol(sym, non_coin=frozenset()):
    """True for a USDT-quoted crypto perp. `non_coin` holds symbols that an exchangeInfo snapshot marks as not
    (contractType PERPETUAL and underlyingType COIN): TradFi, index, delivery contracts (D8)."""
    if not SYMBOL_RE.match(sym) or sym in non_coin:
        return False
    base = sym[:-4]
    return base not in STABLE_BASES and base not in WRAPPED_BASES and base not in NON_CRYPTO_BASES


# ------------------------------------------------------------------ one symbol's data
class Sym:
    """One symbol's rows. rows: {day: (close, quote_volume, count)}. Precomputes, on its traded rows (count > 0),
    the nine Donchian states, the change flags and the 90-day volatility (§4.1-§4.3): all functions of past
    closes only."""
    __slots__ = ('name', 'rows', 'tdays', 'tclose', 'first_traded', 'sig', 'sig_ls', 'sig_on')

    def __init__(self, name, rows):
        self.name = name
        self.rows = rows
        self.tdays = sorted(d for d, r in rows.items() if r[2] > 0 and r[0] > 0)
        self.tclose = [rows[d][0] for d in self.tdays]
        self.first_traded = self.tdays[0] if self.tdays else None
        self.sig = None
        self.sig_ls = None
        self.sig_on = None

    def signals(self, mode='long'):
        """{day: (pos tuple, changed tuple, sigma)} on traded days; mode long | long_short | always."""
        attr = {'long': 'sig', 'long_short': 'sig_ls', 'always': 'sig_on'}[mode]
        cur = getattr(self, attr)
        if cur is None:
            cur = dict(zip(self.tdays, signal_path(self.tclose, mode)))
            setattr(self, attr, cur)
        return cur

    def last_traded_close(self, before):
        """The last traded close strictly before day `before` (None if none)."""
        i = bisect.bisect_left(self.tdays, before) - 1
        return self.tclose[i] if i >= 0 else None


def signal_path(closes, mode='long'):
    """[(pos, changed, sigma)] per close, in order. pos is a 9-tuple in {0, 1} (long) or {-1, 0, 1} (long_short,
    Appendix p. 33); mode 'always' holds pos = 1 throughout (the vol-managed factor, D15).

    Per lookback n, at close c_t (§4.1 p. 7, §4.3 p. 10; D2):
      Up = max(c_{t-n+1..t}), Dn = min(...), Mid = (Up + Dn) / 2 — undefined (pos 0) until n closes exist;
      pos_t = 1 if c_t == Up; else 0 if (long and c_t <= stop_t); else pos_{t-1}   (the paper's case order);
      the stop used on day t is stop_t = max(stop_{t-1}, Mid_{t-1}); a NEW entry sets stop_{t+1} = Mid_t
      (footnote 3); a long that prints a new high keeps its stop (it is not a new position).
    Long/short adds pos = -1 if c_t == Dn (when not == Up), a short exits on c_t >= stop_t, stop = min(...).
    sigma_t: the sample SD (n - 1) of the last 90 close-to-close simple returns ending at t, x sqrt(365) (D3);
    None until 90 returns exist."""
    nl = len(LOOKBACKS)
    maxq = [deque() for _ in LOOKBACKS]
    minq = [deque() for _ in LOOKBACKS]
    pos = [0] * nl
    stop = [None] * nl
    out = []
    rets = deque()
    s1 = s2 = 0.0
    recount = 0
    for t, c in enumerate(closes):
        if t:
            r = c / closes[t - 1] - 1.0
            rets.append(r)
            s1 += r
            s2 += r * r
            if len(rets) > VOL_WIN:
                o = rets.popleft()
                s1 -= o
                s2 -= o * o
            recount += 1
            if recount >= 512:                          # exact recompute: no drift in the rolling sums
                s1 = math.fsum(rets)
                s2 = math.fsum(x * x for x in rets)
                recount = 0
        sigma = None
        if len(rets) == VOL_WIN:
            var = (s2 - s1 * s1 / VOL_WIN) / (VOL_WIN - 1)
            sigma = math.sqrt(max(var, 0.0) * ANN_DAYS)
        prev = tuple(pos)
        for k, n in enumerate(LOOKBACKS):
            mq, nq = maxq[k], minq[k]
            while mq and closes[mq[-1]] <= c:
                mq.pop()
            mq.append(t)
            while nq and closes[nq[-1]] >= c:
                nq.pop()
            nq.append(t)
            while mq[0] <= t - n:
                mq.popleft()
            while nq[0] <= t - n:
                nq.popleft()
            if mode == 'always':
                pos[k] = 1
                continue
            if t < n - 1:
                pos[k], stop[k] = 0, None
                continue
            up, dn = closes[mq[0]], closes[nq[0]]
            mid = 0.5 * (up + dn)
            p, s = pos[k], stop[k]
            if c >= up:                                 # c is in the window, so c >= up means c == Up
                if p == 1:
                    stop[k] = max(s, mid)
                else:
                    stop[k] = mid
                pos[k] = 1
            elif mode == 'long_short' and c <= dn:
                if p == -1:
                    stop[k] = min(s, mid)
                else:
                    stop[k] = mid
                pos[k] = -1
            elif p == 1 and c <= s:
                pos[k], stop[k] = 0, None
            elif p == -1 and c >= s:
                pos[k], stop[k] = 0, None
            elif p == 1:
                stop[k] = max(s, mid)
            elif p == -1:
                stop[k] = min(s, mid)
        cur = tuple(pos)
        out.append((cur, tuple(a != b for a, b in zip(cur, prev)), sigma))
    return out


def targets(pos, sigma):
    """§4.3 p. 10: w_n = min(0.25 / sigma, 200%) x Pos_n. No sigma -> no position (D3)."""
    if sigma is None:
        return (0.0,) * len(pos)
    w = LEV_CAP if sigma <= 0 else min(VOL_TARGET / sigma, LEV_CAP)
    return tuple(w * p for p in pos)


def rebalance(held, tgt, changed, threshold=THRESHOLD, relative=False):
    """§5.1 p. 15: a sub-model whose signal changed today trades to its target at once; otherwise it trades only if
    |target - held| exceeds the threshold (absolute weight points, D5; `relative` is the reported variant)."""
    out = []
    for h, w, ch in zip(held, tgt, changed):
        if ch:
            out.append(w)
        elif relative:
            out.append(w if (h == 0 and w != 0) or (h != 0 and abs(w - h) > threshold * abs(h)) else h)
        else:
            out.append(w if abs(w - h) > threshold else h)
    return tuple(out)


# ------------------------------------------------------------------ the universe (§7.1; D8, D9)
def _window_rows(s, d, win=MEDIAN_WIN):
    return [s.rows[x] for x in range(d - win + 1, d + 1) if x in s.rows]


def median(v):
    v = sorted(v)
    n = len(v)
    if not n:
        return None
    return v[n // 2] if n % 2 else 0.5 * (v[n // 2 - 1] + v[n // 2])


def median_qv(s, d):
    return median([r[1] for r in _window_rows(s, d)])


def median_abs_ret(s, d):
    i = bisect.bisect_right(s.tdays, d)
    lo = bisect.bisect_left(s.tdays, d - MEDIAN_WIN + 1)
    rs = [abs(s.tclose[j] / s.tclose[j - 1] - 1.0) for j in range(max(lo, 1), i)]
    return median(rs)


def select_universe(syms, d, non_coin=frozenset(), b=B):
    """The universe for the month after the close of day d (a month end), from data through d only (§7.1):
    a crypto USDT perp (D8), first traded >= 365 days before d, traded on each of the last 7 days (D9: the
    delisting cut — a dead contract keeps flat rows), median daily quote volume >= $2 M over the 30 days to d;
    the top b by that median (ties by name). Returns [(symbol, median)]."""
    cand = []
    for name, s in syms.items():
        if not crypto_symbol(name, non_coin) or s.first_traded is None or s.first_traded > d - MIN_AGE_DAYS:
            continue
        if any((x not in s.rows) or s.rows[x][2] <= 0 for x in range(d - TRADED_TAIL_DAYS + 1, d + 1)):
            continue
        m = median_qv(s, d)
        if m is not None and m >= MIN_MEDIAN_QV:
            cand.append((-m, name))
    cand.sort()
    return [(n, -m) for m, n in cand[:b]]


def liquidity_exit(s, d):
    """§7.2 p. 18: removed if the 30-day median volume < $1 M or the 30-day median |daily change| < 0.5%."""
    mq, ma = median_qv(s, d), median_abs_ret(s, d)
    return (mq is not None and mq < EXIT_MEDIAN_QV) or (ma is not None and ma < EXIT_MEDIAN_ABS_RET)


# ------------------------------------------------------------------ the book (§7.2; D4-D12)
BTC = 'BTCUSDT'


def run_book(syms, start, end, universe_fn, cost=COST_PAPER, funding=None, mode='long', relative=False,
             lag=0, keep_paths=False):
    """The book's daily net returns on [start, end] (inclusive day ordinals).

    universe_fn(d) -> [symbol] for the month after month-end day d (called at start - 1's month and at each month
    end inside the span). The book is initialised at the close of start - 1 with 1/B of capital per member, each
    holding its current Combo target (D10, D12). Each day, per active sleeve (D4, D6, D9):
      no row          -> carried (no return, no decision; counted);
      a dead row      -> the delisting cut: the position settles at its last traded close, cost charged, the
                         sleeve is cash until the month end (counted);
      a traded row    -> P&L at H x equity x return; funding (if given: {sym: {day: summed rate}}) on the
                         notional held into the day, long pays a positive rate; holdings drift; the nine
                         sub-models rebalance (§5.1); cost x |net change in notional|; the §7.2 liquidity exit.
    At a month end the universe is rebuilt and every sleeve reset to 1/B of book value (§7.2), cost charged on
    every change in notional. `lag` = 1 trades at each close on the previous close's state (reported execution check).
    Returns a dict: days, ret (net), ew_alt (D15 factor 2), gross_exposure, counts, members by month, and the
    turnover; `keep_paths` adds per-day sleeve notionals (tests)."""
    nl = len(LOOKBACKS)
    zero = (0.0,) * nl
    counts = {'missing_rows': 0, 'delisting_cuts': 0, 'liquidity_exits': 0, 'member_days': 0, 'resets': 0,
              'sleeves_without_sigma': 0}
    V = 1.0
    cash = 0.0
    sleeves = {}                                   # sym -> [equity, holdings tuple, last traded close]
    members_by_month = {}
    turnover = 0.0
    out_days, out_ret, out_alt, out_gross = [], [], [], []
    paths = [] if keep_paths else None

    def sig(sym, d):
        return syms[sym].signals(mode).get(d)

    def build(month_end_day, value):
        """Month end (or the initialisation): the new members; continuing members keep their held sub-model
        weights (the threshold rule continues), new members take their current Combo target (D12); every sleeve
        gets 1/B of book value; cost on every change in notional."""
        nonlocal cash
        members = list(universe_fn(month_end_day))
        members_by_month[month_of(month_end_day + 1)] = members
        old = dict(sleeves)
        new = {}
        for sym in members:
            if sym in old:
                new[sym] = old[sym][1]
                continue
            st = sig(sym, month_end_day)
            if st is not None and st[2] is None:
                counts['sleeves_without_sigma'] += 1
            new[sym] = targets(st[0], st[2]) if st else zero
        target_eq = value / B
        moved = 0.0
        for sym in set(old) | set(new):
            n_old = sum(old[sym][1]) / nl * old[sym][0] if sym in old else 0.0
            n_new = sum(new[sym]) / nl * target_eq if sym in new else 0.0
            moved += abs(n_new - n_old)
        value -= cost * moved
        sleeves.clear()
        for sym in members:
            last = syms[sym].rows.get(month_end_day)
            lc = last[0] if last and last[2] > 0 else syms[sym].last_traded_close(month_end_day + 1)
            sleeves[sym] = [value / B, new[sym], lc]
        cash = value - len(members) * value / B
        return value, moved

    V, moved = build(start - 1, V)
    turnover += moved
    for d in range(start, end + 1):
        v_prev = V
        alt = []
        gross = 0.0
        for sym in list(sleeves):
            sl = sleeves[sym]
            eq, held, lc = sl
            s = syms[sym]
            row = s.rows.get(d)
            counts['member_days'] += 1
            if row is None:
                counts['missing_rows'] += 1
                sl[1] = held
                continue
            H = sum(held) / nl
            if row[2] <= 0 or row[0] <= 0:           # dead row: settle at the last traded close
                counts['delisting_cuts'] += 1
                fee = cost * abs(H) * eq
                turnover += abs(H) * eq
                cash += eq - fee
                del sleeves[sym]
                continue
            r = row[0] / lc - 1.0
            if sym != BTC:
                alt.append(r)
            notional = H * eq
            gross += abs(notional)
            pnl = notional * r
            fund = 0.0
            if funding is not None:
                fund = funding.get(sym, {}).get(d, 0.0) * notional
            eq_new = eq + pnl - fund
            g = (1.0 + r) / (1.0 + H * r) if (1.0 + H * r) != 0 else 0.0
            drifted = tuple(h * g for h in held)
            if lag:                                 # trade at close d on the state of the previous traded close
                i = bisect.bisect_left(s.tdays, d) - 1
                st = sig(sym, s.tdays[i]) if i >= 0 else None
            else:
                st = sig(sym, d)
            tg = targets(st[0], st[2]) if st else zero
            ch = st[1] if st else (False,) * nl
            new = rebalance(drifted, tg, ch, relative=relative)
            dH = (sum(new) - sum(drifted)) / nl
            fee = cost * abs(dH) * eq_new
            turnover += abs(dH) * eq_new
            eq_new -= fee
            sl[0], sl[1], sl[2] = eq_new, new, row[0]
            if liquidity_exit(s, d):
                counts['liquidity_exits'] += 1
                Hn = sum(new) / nl
                fee = cost * abs(Hn) * eq_new
                turnover += abs(Hn) * eq_new
                cash += eq_new - fee
                del sleeves[sym]
        V = cash + sum(sl[0] for sl in sleeves.values())
        if is_month_end(d) and d < end:
            V, moved = build(d, V)
            turnover += moved
            counts['resets'] += 1
        out_days.append(d)
        out_ret.append(V / v_prev - 1.0)
        out_alt.append(sum(alt) / len(alt) if alt else 0.0)
        out_gross.append(gross / v_prev if v_prev else 0.0)
        if keep_paths:
            paths.append({k: (v[0], sum(v[1]) / nl) for k, v in sleeves.items()})
    years = (end - start + 1) / 365.0
    return {'days': out_days, 'ret': out_ret, 'ew_alt': out_alt, 'gross': out_gross, 'counts': counts,
            'members': members_by_month, 'turnover_per_year': turnover / years if years else None,
            'paths': paths}


def factor_series(syms, start, end, universe_fn):
    """D15: BTC's close-to-close return; the equal-weight alt basket (members other than BTC, as held by the
    book); the vol-managed EW basket (the same book with Pos = 1 throughout, gross of costs)."""
    refuse_unless(BTC in syms, 'BTCUSDT is required for the market factor')
    btc = syms[BTC]
    f1 = []
    for d in range(start, end + 1):
        row = btc.rows.get(d)
        prev = btc.last_traded_close(d)
        f1.append(row[0] / prev - 1.0 if row and row[2] > 0 and prev else 0.0)
    vm = run_book(syms, start, end, universe_fn, cost=0.0, mode='always')
    return f1, vm['ret']


def universe_fn_for(syms, non_coin=frozenset()):
    cache = {}

    def fn(d):
        if d not in cache:
            cache[d] = [n for n, _ in select_universe(syms, d, non_coin)]
        return cache[d]
    return fn


def compute(syms, start=None, end=None, non_coin=frozenset(), cost=COST_PAPER, funding=None, mode='long',
            relative=False, lag=0):
    """The registered path: the primary book, its EW alt factor and the other two factors."""
    a, b = day(start or START), day(end or END)
    ufn = universe_fn_for(syms, non_coin)
    book = run_book(syms, a, b, ufn, cost=cost, funding=funding, mode=mode, relative=relative, lag=lag)
    f1, f3 = factor_series(syms, a, b, ufn)
    return {'book': book, 'F1': f1, 'F2': book['ew_alt'], 'F3': f3, 'universe_fn': ufn}
