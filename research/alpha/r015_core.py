"""Registration 015 — the settlement-shifted month-end liquidity cycle: pure functions (no file I/O).

Everything here is a function of arguments only. The runner (`r015_run.py`) loads the data behind its guard and
calls these; the power module (`r015_power.py`) calls them on synthetic returns; the tests call them on both.

Vocabulary (registration §1-§4):
  T       the last trading day of a calendar month (dates of the Ken French US daily file);
  s       the US settlement lag in force for that month's last selling day (3, 2 or 1);
  P_m     the pressure window, trading days T-8 ... T-(s+1);
  R_m     the rebound window, trading days T-s ... T+3;
  out_m   the month's out-of-window days: from the 4th trading day of the month to T-9;
  x_d     the daily US market EXCESS return (French Mkt-RF / 100);
  mu_s    the mean of x over the out-of-window days of every month of settlement era s;
  y_m     sum_{R_m}(x - mu_s) - sum_{P_m}(x - mu_s)   (the primary, long/short, abnormal);
  v_m     sum_{R_m} x - sum_{P_m} x                   (the tradable 1x long/short excess P&L; economics).
"""
import datetime as dt
import math
import statistics

# ------------------------------------------------------------------ fixed constants (registration §2-§7)
DATA_END = dt.date(2026, 6, 10)            # rows dated on or after this are dropped at load (after 2026-05's T+3)
DATA_START = dt.date(2014, 1, 1)           # rows before this are dropped at load (not needed: Etula's sample)
FIRST_MONTH, LAST_MONTH = '2014-01', '2026-05'
US_T2 = dt.date(2017, 9, 5)                # SEC Rel. 34-80295, compliance date (trade date basis)
US_T1 = dt.date(2024, 5, 28)               # SEC Rel. 34-96930, compliance date (trade date basis)
EU_T2 = dt.date(2014, 10, 6)               # Etula et al. (2020) §2.1: the concerted European move T+3 -> T+2
EXPECTED_MONTHS = {3: 44, 2: 80, 1: 25}    # recounted by the orchestrator from the B1 calendar (000s); refused if different
G_EXPECTED = 149
PRE, POST = 8, 3                           # pressure starts at T-8; rebound ends at T+3 (Etula's windows)
PLACEBO_SHIFT = 12                         # the placebo: the same shapes anchored at T-12
MID_DAYS = 3                               # the first 3 trading days of a month belong to the previous month's R

DAILY_SD = 0.0112                          # unconditional US daily vol 2014-01 -> 2026-06 (B1 scout, 000s): power only
EFFECT_P, EFFECT_R = -0.0037, 0.0048       # Etula: abnormal T-8..T-4 -37 bp, T-3..T+3 +48 bp (CRSP VW, 1995-07 -> 2013-12)

Q = 0.95                                   # one-sided existence test
CAL_CRIT = 1.91                            # calibrated critical value (r015_power.py; registration §5). Never below t_148(0.95)
SHRINK = 0.5                               # the economic bar runs on half the estimate (as 008)
COST_RT_MES = 0.8e-4                       # MES round trip, fraction of notional (B1 §2.2)
RT_PER_MONTH = 2                           # open short at T-9 close, flip at T-(s+1) close (2 sides), close at T+3
COST_MONTH = COST_RT_MES * RT_PER_MONTH    # per unit of notional, per month
COST_RT_XYZ = 2.2e-4                       # xyz SP500 taker round trip (reported)
XYZ_FUNDING_DAY = 0.8e-4                   # long pays / short receives per calendar day, excess terms (B1 §2.2; reported)
L_CAP = 1.2                                # notional cap from the tail (worst plausible month -25% at 1x -> -30% at 1.2x)
L_PLAN = 1.0                               # the proposed size (registration §6)
BAR = 0.04                                 # HLP calm, %/yr, applied to the shrunk net at min(1/2 Kelly, L_CAP)
MONTHS_PER_YEAR = 12


class Refused(Exception):
    """A refusal: never an assert (python -O must not remove a check)."""


def refuse_unless(cond, msg):
    if not cond:
        raise Refused(msg)


# ------------------------------------------------------------------ calendar
def us_lag(d):
    """The US equity settlement lag (business days) for a trade executed on date d."""
    return 3 if d < US_T2 else 2 if d < US_T1 else 1


def eu_lag(d):
    """Europe (French 'Europe' region): T+3 -> T+2 on 2014-10-06 (Etula's treatment group; reported only)."""
    return 3 if d < EU_T2 else 2


def month_of(d):
    return f'{d.year:04d}-{d.month:02d}'


def months_between(first, last):
    y, m = map(int, first.split('-'))
    out = []
    while f'{y:04d}-{m:02d}' <= last:
        out.append(f'{y:04d}-{m:02d}')
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def lag_for_month(dates, iT, lag_fn):
    """The lag s in force on the month's last selling day T-(s+1): the unique s in {3, 2, 1} with
    lag_fn(date of T-(s+1)) == s. Refused when it is not unique (it is unique on the registered calendar)."""
    cands = [s for s in (3, 2, 1) if iT - (s + 1) >= 0 and lag_fn(dates[iT - (s + 1)]) == s]
    refuse_unless(len(cands) == 1, f'settlement lag for T={dates[iT]} is not unique: {cands}')
    return cands[0]


def build_calendar(dates, lag_fn=us_lag, first=FIRST_MONTH, last=LAST_MONTH):
    """The month-end calendar from a sorted list of trading dates (dates only; no return is read).

    Returns a list of month dicts with index lists into `dates`: P, R (shifted), P3, R3 (unshifted T+3), PP, RP (placebo,
    with days inside any real window dropped), out (out-of-window days). Refuses on a gap: every month must have its T,
    T-20 and T+3 inside `dates`, and the real windows of different months may not overlap."""
    refuse_unless(all(a < b for a, b in zip(dates, dates[1:])), 'dates must be strictly increasing')
    by_month = {}
    for i, d in enumerate(dates):
        by_month.setdefault(month_of(d), []).append(i)
    months = months_between(first, last)
    cal, real = [], set()
    for m in months:
        refuse_unless(m in by_month, f'month {m} has no trading day in the data')
        idx = by_month[m]
        iT = idx[-1]
        refuse_unless(iT + POST < len(dates), f'month {m}: T+3 is beyond the data')
        refuse_unless(iT - PRE - PLACEBO_SHIFT >= 0, f'month {m}: T-20 is before the data')
        refuse_unless(month_of(dates[iT + 1]) != m, f'month {m}: T is not the last trading day')
        s = lag_for_month(dates, iT, lag_fn)
        P = list(range(iT - PRE, iT - s))           # T-8 .. T-(s+1)
        R = list(range(iT - s, iT + POST + 1))      # T-s .. T+3
        out = [i for i in idx[MID_DAYS:] if i < iT - PRE]
        cal.append({'month': m, 'T': dates[iT].isoformat(), 'iT': iT, 'weekday': dates[iT].weekday(), 's': s,
                    'P': P, 'R': R, 'P3': list(range(iT - PRE, iT - 3)), 'R3': list(range(iT - 3, iT + POST + 1)),
                    'out': out})
        w = set(P) | set(R)
        refuse_unless(not (w & real), f'month {m}: windows overlap the previous month')
        real |= w
    # the first MID_DAYS trading days of the first month belong to the previous month's rebound window
    first_idx = by_month[months[0]]
    real |= set(first_idx[:MID_DAYS])
    for c in cal:
        iT, s = c['iT'], c['s']
        pp = [i for i in range(iT - PRE - PLACEBO_SHIFT, iT - s - PLACEBO_SHIFT) if i not in real]
        rp = [i for i in range(iT - s - PLACEBO_SHIFT, iT + POST + 1 - PLACEBO_SHIFT) if i not in real]
        c['PP'], c['RP'] = pp, rp
        c['placebo_dropped'] = (PRE - s) + (s + POST + 1) - len(pp) - len(rp)
        refuse_unless(not (set(c['out']) & real), f"month {c['month']}: an out-of-window day lies in a window")
    return cal


def calendar_summary(cal, dates):
    """Counts and dates only (stored in calendar.json and in the result row)."""
    lags = {}
    for c in cal:
        lags[c['s']] = lags.get(c['s'], 0) + 1
    return {'months': len(cal), 'first': cal[0]['month'] if cal else None, 'last': cal[-1]['month'] if cal else None,
            'by_lag': {str(k): v for k, v in sorted(lags.items(), reverse=True)},
            'friday_T': sum(1 for c in cal if c['weekday'] == 4),
            'out_days': sum(len(c['out']) for c in cal),
            'placebo_days_dropped': sum(c['placebo_dropped'] for c in cal),
            'rows': [{'month': c['month'], 'T': c['T'], 's': c['s'], 'P': [dates[c['P'][0]].isoformat(), dates[c['P'][-1]].isoformat()],
                      'R': [dates[c['R'][0]].isoformat(), dates[c['R'][-1]].isoformat()], 'n_out': len(c['out'])} for c in cal]}


def calendar_failures(cal, expected=EXPECTED_MONTHS, g=G_EXPECTED):
    out = []
    if len(cal) != g:
        out.append(f'{len(cal)} months, {g} required')
    got = {}
    for c in cal:
        got[c['s']] = got.get(c['s'], 0) + 1
    if got != expected:
        out.append(f'months by lag {got}, {expected} required')
    return out


# ------------------------------------------------------------------ the statistic
def era_mu(x, cal):
    """mu_s: mean excess return over the out-of-window days of the months of era s."""
    acc = {}
    for c in cal:
        a = acc.setdefault(c['s'], [0.0, 0])
        for i in c['out']:
            a[0] += x[i]
            a[1] += 1
    refuse_unless(all(n > 0 for _, n in acc.values()), 'an era has no out-of-window day')
    return {s: tot / n for s, (tot, n) in acc.items()}


def window_sums(x, cal, pk='P', rk='R', mu=None):
    """[(sum over R of (x - mu_s)) - (sum over P of (x - mu_s))] per month; mu None -> raw."""
    out = []
    for c in cal:
        m = mu[c['s']] if mu is not None else 0.0
        out.append(sum(x[i] - m for i in c[rk]) - sum(x[i] - m for i in c[pk]))
    return out


def rebound_sums(x, cal, mu=None):
    out = []
    for c in cal:
        m = mu[c['s']] if mu is not None else 0.0
        out.append(sum(x[i] - m for i in c['R']))
    return out


def y_series(x, cal):
    mu = era_mu(x, cal)
    return window_sums(x, cal, mu=mu), mu


def v_series(x, cal):
    return window_sums(x, cal)


def mean_t(v):
    """(mean, sd, t, n) of a list, one observation per cluster (month)."""
    n = len(v)
    if n < 2:
        return (v[0] if v else None), None, None, n
    m = statistics.fmean(v)
    sd = statistics.stdev(v)
    return m, sd, (m / (sd / math.sqrt(n)) if sd > 0 else None), n


# ------------------------------------------------------------------ Student t (no numpy / scipy)
def _betacf(a, b, x):
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c, d = 1.0, 1.0 - qab * x / qap
    d = 1.0 / (d if abs(d) > 1e-300 else 1e-300)
    h = d
    for m in range(1, 400):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1.0 + aa / c
        c = c if abs(c) > 1e-300 else 1e-300
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1.0 + aa / c
        c = c if abs(c) > 1e-300 else 1e-300
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < 1e-15:
            break
    return h


def betainc(a, b, x):
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
    p = 0.5 * betainc(df / 2.0, 0.5, x)
    return 1 - p if t > 0 else p


def t_ppf(q, df):
    lo, hi = -100.0, 100.0
    for _ in range(200):
        mid = (lo + hi) / 2
        if t_cdf(mid, df) < q:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def crit(g):
    """The registered critical value: max(t_{G-1}(0.95), CAL_CRIT)."""
    return max(t_ppf(Q, g - 1), CAL_CRIT)


# ------------------------------------------------------------------ the decision (existence, then economics)
def existence(y, c=None):
    """Criteria on the per-month y (one observation per month)."""
    c = crit(len(y)) if c is None else c
    m, sd, t, g = mean_t(y)
    wo = sorted(y)[:-1] if y else []
    m_wo = statistics.fmean(wo) if wo else None
    criteria = {'G_is_149': g == G_EXPECTED, 'mean_positive': m is not None and m > 0,
                't_at_or_above_crit': t is not None and t >= c,
                'mean_positive_without_largest_month': m_wo is not None and m_wo > 0}
    return {'G': g, 'mean': m, 'sd': sd, 't': t, 'crit': c, 'mean_without_largest': m_wo,
            'criteria': criteria, 'exists': all(criteria.values())}


def economic(v, cost=COST_MONTH, shrink=SHRINK, l_cap=L_CAP, bar=BAR):
    """The bar, applied only after an existence pass. On the tradable raw long/short v (excess), shrunk by half:
    net_s = shrink * mean(v) - cost; full Kelly f = net_s / var(v); L = min(f / 2, l_cap); annual = 12 * L * net_s.
    Meets the bar iff net_s > 0 and annual >= bar."""
    m, sd, _, n = mean_t(v)
    if m is None or sd is None or sd <= 0:
        return {'meets_bar': False, 'reason': 'undefined'}
    net_s = shrink * m - cost
    kelly = net_s / (sd * sd)
    size = max(0.0, min(kelly / 2.0, l_cap))
    annual = MONTHS_PER_YEAR * size * net_s
    return {'mean_v': m, 'sd_v': sd, 'net_shrunk_month': net_s, 'kelly_full': kelly, 'size_half_kelly_capped': size,
            'annual_net_at_size': annual, 'annual_net_at_plan_size': MONTHS_PER_YEAR * L_PLAN * net_s,
            'bar': bar, 'meets_bar': net_s > 0 and annual >= bar}


LABELS = ('not_demonstrated', 'exists_below_bar', 'exists_paper')


def decide(ex, econ):
    if not ex['exists']:
        return 'not_demonstrated'
    return 'exists_paper' if econ.get('meets_bar') else 'exists_below_bar'


def recheck(row):
    """Recompute the decision from the per-month records stored in the row: ([label], [failures])."""
    fails = []
    try:
        recs = row['months']
        y = [r['y'] for r in recs]
        v = [r['v'] for r in recs]
        ex = existence(y, row['existence']['crit'])
        econ = economic(v)
    except Exception as e:  # noqa: BLE001 - a malformed row recomputes nothing
        return None, [f'malformed row: {e!r}'[:200]]
    if abs(row['existence']['crit'] - crit(len(y))) > 1e-12:
        fails.append('stored critical value differs from the registered one')
    for k in ('mean', 't', 'mean_without_largest'):
        a, b = ex[k], row['existence'][k]
        if (a is None) != (b is None) or (a is not None and abs(a - b) > 1e-12):
            fails.append(f'existence {k} does not recompute')
    if ex['criteria'] != row['existence']['criteria']:
        fails.append('existence criteria do not recompute')
    if bool(econ.get('meets_bar')) != bool(row['economic'].get('meets_bar')):
        fails.append('economic bar does not recompute')
    lab = decide(ex, econ)
    if lab != row.get('decision'):
        fails.append(f'decision recomputes to {lab}, row says {row.get("decision")}')
    return lab, fails


# ------------------------------------------------------------------ reported blocks (decide nothing)
def welch_t(a, b):
    ma, sa, _, na = mean_t(a)
    mb, sb, _, nb = mean_t(b)
    if None in (sa, sb) or na < 2 or nb < 2:
        return None
    se = math.sqrt(sa * sa / na + sb * sb / nb)
    return {'mean_a': ma, 'n_a': na, 'mean_b': mb, 'n_b': nb, 'diff': ma - mb, 't': (ma - mb) / se if se > 0 else None}


def summary(v):
    m, sd, t, n = mean_t(v)
    return {'n': n, 'mean': m, 'sd': sd, 't': t, 'median': statistics.median(v) if v else None}


def mechanism_contrast(x, cal, mu):
    """Shifted minus unshifted (T+3) windows, paired by month, in the T+2 and T+1 eras (105 months)."""
    ys = window_sums(x, cal, mu=mu)
    yu = window_sums(x, cal, 'P3', 'R3', mu=mu)
    d = [a - b for a, b, c in zip(ys, yu, cal) if c['s'] < 3]
    by = {s: [a - b for a, b, c in zip(ys, yu, cal) if c['s'] == s] for s in (2, 1)}
    return {'paired_all_t2_t1': summary(d), 'paired_t2': summary(by[2]), 'paired_t1': summary(by[1]),
            'unshifted_y_all_months': summary(yu)}


def placebo(x, cal, mu):
    return summary(window_sums(x, cal, 'PP', 'RP', mu=mu))


def friday(y, cal):
    return welch_t([v for v, c in zip(y, cal) if c['weekday'] == 4], [v for v, c in zip(y, cal) if c['weekday'] != 4])


def by_era(y, cal):
    return {str(s): summary([v for v, c in zip(y, cal) if c['s'] == s]) for s in (3, 2, 1)}


def calendar_days_long_short(dates, c):
    """Calendar days held long (close T-(s+1) -> close T+3) and short (close T-9 -> close T-(s+1)): xyz funding."""
    d9, dsw, dend = dates[c['P'][0] - 1], dates[c['P'][-1]], dates[c['R'][-1]]
    return (dend - dsw).days, (dsw - d9).days


def xyz_cost_month(dates, c, rt=COST_RT_XYZ, fday=XYZ_FUNDING_DAY):
    long_d, short_d = calendar_days_long_short(dates, c)
    return RT_PER_MONTH * rt + fday * (long_d - short_d)
