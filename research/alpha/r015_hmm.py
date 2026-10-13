"""Registration 015, reported block only: the Harvey-Mazzoleni-Melone rebalancing front-run, a LABELLED REPLICATION.

Harvey, Mazzoleni & Melone, "The Unintended Consequences of Rebalancing" (NBER w33554; draft of 2026-01-14), §4 and
Appendix B, eqs. (B.1)-(B.2); their sample is 1997-09-10 -> 2023-03-17, extended "through 2025" (footnote 32). Every
day of 015's calendar is inside the authors' sample or their extension, so nothing here is out of sample. It decides
nothing in 015.

Proxies (fixed before any data, registration §8):
  S&P 500 total return        -> French US Mkt (Mkt-RF + RF), total market;
  10-year Treasury note return -> par-yield duration return r = y_{t-1}/252 - D(y_{t-1}) * (y_t - y_{t-1}), with D the
                                 modified duration of a 10-year semiannual par bond at y_{t-1};
  futures excess returns       -> the strategy earns (R_SP - R_10Y) on a long/short, so the financing rate cancels.
Interpretations of the paper's text (stated, not tuned):
  * Threshold: one 60/40 book per delta in {0, 0.1, ..., 2.5 %}; each day the weight drifts with the two returns; the
    signal is the drifted weight minus 60 %; the book rebalances to 60 % on a day its |signal| >= delta (delta 0:
    every day). The Threshold signal is the mean over the 26 books.
  * Calendar: one 60/40 book that drifts and rebalances to 60 % at the close of the last business day of each month;
    the signal is the drifted weight minus 60 %.
  * Strategy weight at close t: w_t = mean(-Threshold_t / 1.5 %, Cal'_t), Cal'_t = sign(-Calendar_t) on the last 5
    business days of a month, sign(Calendar_{t-4}) (the Calendar signal four business days earlier) on the first
    business day of a month, else 0. The return on day t+1 is w_t * (R_SP,t+1 - R_10Y,t+1).
  * Business days are the French US trading days; a day with no Treasury yield carries the last yield forward.
"""
import math

DELTAS = [round(0.001 * k, 3) for k in range(26)]
TARGET = 0.60
SCALE = 0.015
WEEK4 = 5
PAPER_END = '2023-03-17'


def drift(w, rs, rb):
    a = w * (1 + rs)
    return a / (a + (1 - w) * (1 + rb))


def mod_duration(y, years=10, freq=2):
    """Modified duration of a par bond with coupon = yield y (annual, decimal), semiannual."""
    if y <= 1e-9:
        return float(years)
    i = y / freq
    n = years * freq
    mac = (1 + i) / i * (1 - (1 + i) ** (-n))    # Macaulay duration in periods of a par bond
    return mac / freq / (1 + i)


def bond_returns(dates, y_by_date):
    """10y note total-return proxy per French trading day (first day 0). Yields carried forward when missing."""
    out, last = [], None
    ys = []
    for d in dates:
        if d in y_by_date:
            last = y_by_date[d]
        ys.append(last)
    for i, d in enumerate(dates):
        if i == 0 or ys[i] is None or ys[i - 1] is None:
            out.append(0.0)
            continue
        y0, y1 = ys[i - 1], ys[i]
        out.append(y0 / 252.0 - mod_duration(y0) * (y1 - y0))
    return out


def last_business_flags(dates):
    """(is_last_day_of_month, in_last_5_days, is_first_day_of_month) per index."""
    n = len(dates)
    last = [i == n - 1 or dates[i + 1].month != dates[i].month for i in range(n)]
    first = [i == 0 or dates[i - 1].month != dates[i].month for i in range(n)]
    pos_from_end = [0] * n
    k = 0
    for i in range(n - 1, -1, -1):
        k = 0 if last[i] else k + 1
        pos_from_end[i] = k
    week4 = [p < WEEK4 for p in pos_from_end]
    return last, week4, first


def strategy(dates, r_sp, r_bond, r_eq_excess=None):
    """Daily strategy returns [(date, return)] from day 1 on, the equity leg alone (excess, futures-like), weights."""
    n = len(dates)
    last, week4, first = last_business_flags(dates)
    wt = {d: TARGET for d in DELTAS}
    wc = TARGET
    thr, cal = [0.0] * n, [0.0] * n
    for i in range(1, n):
        rs, rb = r_sp[i], r_bond[i]
        sig = []
        for d in DELTAS:
            w = drift(wt[d], rs, rb)
            s = w - TARGET
            sig.append(s)
            wt[d] = TARGET if abs(s) >= d else w
        thr[i] = sum(sig) / len(sig)
        w = drift(wc, rs, rb)
        cal[i] = w - TARGET
        wc = TARGET if last[i] else w
    sign = lambda v: (v > 0) - (v < 0)
    weights = [0.0] * n
    for i in range(n):
        if week4[i]:
            cm = sign(-cal[i])
        elif first[i] and i >= 4:
            cm = sign(cal[i - 4])
        else:
            cm = 0
        weights[i] = 0.5 * (-thr[i] / SCALE + cm)
    rets = [(dates[i], weights[i - 1] * (r_sp[i] - r_bond[i])) for i in range(1, n)]
    ex = r_eq_excess if r_eq_excess is not None else r_sp
    eq = [(dates[i], weights[i - 1] * ex[i]) for i in range(1, n)]
    return rets, eq, weights


def summarise(rets):
    """Annualised mean / vol / Sharpe and the month-clustered t of the daily returns (sum per month)."""
    import statistics
    v = [r for _, r in rets]
    if len(v) < 3:
        return None
    m, sd = statistics.fmean(v), statistics.stdev(v)
    months = {}
    for d, r in rets:
        months.setdefault((d.year, d.month), 0.0)
        months[(d.year, d.month)] += r
    mv = list(months.values())
    mm, msd = statistics.fmean(mv), statistics.stdev(mv) if len(mv) > 1 else None
    skew = sum((x - m) ** 3 for x in v) / len(v) / sd ** 3 if sd > 0 else None
    return {'days': len(v), 'ann_mean': m * 252, 'ann_vol': sd * math.sqrt(252), 'sharpe': (m / sd * math.sqrt(252)) if sd > 0 else None,
            'skew_daily': skew, 'months': len(mv), 'month_t': (mm / (msd / math.sqrt(len(mv)))) if msd else None}


def block(dates, mkt_excess, rf, y_by_date, split=PAPER_END):
    r_sp = [a + b for a, b in zip(mkt_excess, rf)]
    r_b = bond_returns(dates, y_by_date)
    rets, eq, _ = strategy(dates, r_sp, r_b, mkt_excess)
    cut = __import__('datetime').date.fromisoformat(split)
    return {'label': 'REPLICATION inside the authors\' sample (1997-09 -> 2023-03, extended through 2025); decides nothing',
            'all': summarise(rets), 'to_2023_03_17': summarise([p for p in rets if p[0] <= cut]),
            'after_2023_03_17': summarise([p for p in rets if p[0] > cut]),
            'equity_leg_only': summarise(eq),
            'days_without_treasury_yield': sum(1 for d in dates if d not in y_by_date)}
