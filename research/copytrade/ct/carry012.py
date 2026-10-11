"""Registration 012 (L2, trial 3) — Hyperliquid–CEX funding carry. A thin wrapper over 010's frozen
mechanics (`ct/carry.py`, imported READ-ONLY and never edited): the same signal, theta, H, costs,
eligibility, exits, liquidation model and fills, with the PAIR SET restricted to HL-BN and HL-BY.

  PAIRS_012        the primary pair set: the rule chooses among HL pairs only (an HL entry is never
                   displaced by a wider Binance-Bybit spread on the same coin)
  hl_subset_of     reported variant: 010's own plan (all three pairs) with its HL-pair positions kept,
                   i.e. the block that produced the post-hoc lead
  stage1_gate      Stage 1 (exposed 2025-01-15 -> 2026-06-30), a SIGN filter (Revision 1): the point
                   estimate of the mean net > 0; month t, median and the rest are reported only
  recheck_stage1   recompute the Stage-1 decision from the NUMBERS stored in a row, never a label
  stage2_confirm   Stage 2 (fresh, 2026-07-01 -> the owner's end), DECISIVE: 010's existence test
                   (`carry.holdout_confirm`) at t_{G-1}(0.95), or t_{G-1}(0.975) at each look when the
                   owner pre-registers the extension to 2027-06-30
  simulate_stages  power of Stage 1 alone, Stage 2 alone and the joint path, with 010's noise model
                   (`carry._draw`) and a monthly regime shock
"""
import math
import random

from . import carry

PAIRS_012 = (('HL', 'BN'), ('HL', 'BY'))
STAGE1_START = carry.EXPOSED_START            # 2025-01-15 00:00 UTC
STAGE1_END = carry.EXPOSED_END                # 2026-07-01 00:00 UTC (exclusive; every exit before it)
STAGE2_START = carry.HOLDOUT_START            # 2026-07-01 00:00 UTC: never read
STAGE2_ENDS = {'2026-12-31': 1_798_761_600_000}   # the offered end (exclusive 2027-01-01): 010's owner choice
EXTENSION_END = carry.EXTENSION_END           # 2027-07-01 00:00 UTC (the extension's fixed end, 2027-06-30)
Q_STAGE1_REPORTED = 0.95                      # Stage 1: t_{G-1}(0.95) is REPORTED beside the month t, never required
Q_ONE_LOOK = carry.Q_CONFIRM                  # 0.95
Q_TWO_LOOKS = carry.Q_CONFIRM_TWO_LOOKS       # 0.975 at each look (extension YES)
FAMILY_TRIAL = 3                              # L2: 010 registered T1, T2; this is trial 3


def is_hl_pair(pair):
    return tuple(pair) in PAIRS_012


def plan(coins, funding, eligible, times, start, end, slip=None):
    """010's `carry.plan`, unchanged, on the HL pair set. Returns (positions, counts)."""
    return carry.plan(coins, funding, eligible, times, start, end, pairs=PAIRS_012, slip=slip)


def hl_subset_of(positions):
    """The reported variant: positions of 010's three-pair plan whose pair has an HL leg."""
    return [p for p in positions if is_hl_pair(p['pair'])]


# ------------------------------------------------------------------ Stage 1 (sign filter)
def stage1_gate(rows):
    """Stage 1 on the exposed block (Revision 1): PASS iff the point estimate of the mean net per
    position is > 0. Stage 1 is exposed (000f), so it can only filter; the month-clustered t, its
    t_{G-1}(0.95) reference, the median, the week t, the annualised net and the mean without the
    largest positive month are REPORTED and decide nothing."""
    rows = [r for r in rows if r.get('ret') is not None]
    mon = carry._block(rows, 'month')
    wk = carry._block(rows, 'week')
    crit = {'mean_pos': mon['mean'] is not None and mon['mean'] > 0}
    big = carry.largest_positive(rows, 'month')
    ex = [r for r in rows if r['month'] != big]
    return all(crit.values()), {'primary_month': mon, 'primary_week_reported': wk, 'criteria': crit,
                                'mean': mon['mean'], 'month_t_reported': mon['t'],
                                'z_reported': carry.t_crit(mon['clusters'] - 1, Q_STAGE1_REPORTED),
                                'median_reported': mon['median'],
                                'annualised_reported': carry.annualised(rows),
                                'without_largest_positive_month_reported': carry._block(ex) if len(ex) >= 3 else None}


def decide_stage1(passed):
    return 'read_stage2' if passed else 'close'


def recheck_stage1(row):
    """Recompute the Stage-1 criterion, pass and decision from the NUMBERS stored in a 012 stage1 row
    (row['stage1']: mean and primary_month {mean, n}); never from a label. Returns (decision,
    [failures])."""
    num = lambda x: isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)
    ev = row.get('stage1') or {}
    mo = ev.get('primary_month') or {}
    fails = []
    n = mo.get('n')
    if not (isinstance(n, int) and not isinstance(n, bool) and n >= 3):
        fails.append('stage1: fewer than 3 positions stored (no mean)')
    if not (num(ev.get('mean')) and num(mo.get('mean')) and ev['mean'] == mo['mean']):
        fails.append('stage1: the stored mean is missing, non-numeric or inconsistent')
    crit = {'mean_pos': num(ev.get('mean')) and num(mo.get('mean')) and ev['mean'] == mo['mean'] and ev['mean'] > 0}
    if (ev.get('criteria') or {}) != crit:
        fails.append('stage1: stored criteria differ from those recomputed from the stored statistics')
    passed = all(crit.values()) and not fails
    if ev.get('passed') != passed:
        fails.append('stage1: stored pass label differs from the recomputed pass')
    dec = decide_stage1(passed)
    if row.get('decision') != dec:
        fails.append(f"stored decision {row.get('decision')} differs from the recomputed {dec}")
    return dec, fails


# ------------------------------------------------------------------ Stage 2 (decisive)
def stage2_confirm(rows, extension_registered):
    """010's existence test on fresh data: month t >= t_{G-1}(q), median > 0, mean > 0 without the
    largest positive month; q = 0.975 at each look when the extension is pre-registered, else 0.95.
    The annualised net (3 %/yr) is reported only."""
    return carry.holdout_confirm(rows, Q_TWO_LOOKS if extension_registered else Q_ONE_LOOK)


def decide_stage2(confirm, details, extension_registered, is_extension_look):
    """confirm -> 'confirm'; first look with the extension registered and 0 < month t < its critical
    value -> 'extension_due'; otherwise 'not_demonstrated' (never 'no edge')."""
    if confirm:
        return 'confirm'
    if extension_registered and not is_extension_look and carry.extension_due(details):
        return 'extension_due'
    return 'not_demonstrated'


def extension_due_from_row(row):
    """The extension is due only if the stored first-look month t lies strictly between 0 and
    t_{G-1}(0.975) at the stored G and the look was taken at q = 0.975 (recomputed, not a label)."""
    ev = row.get('stage2') or {}
    mo = ev.get('primary_month') or {}
    t, g = mo.get('t'), mo.get('clusters')
    if row.get('q') != Q_TWO_LOOKS or not isinstance(t, (int, float)) or isinstance(t, bool) \
            or not isinstance(g, int) or isinstance(g, bool):
        return False
    return 0 < t < carry.t_crit(g - 1, Q_TWO_LOOKS) and not (ev.get('confirm') is True)


# ------------------------------------------------------------------ power (010's noise model)
def _month_start(ym):
    import datetime as dt
    return int(dt.datetime(ym // 100, ym % 100, 1, tzinfo=dt.UTC).timestamp() * 1000)


def _add_months(ym, k):
    y, m = divmod(ym // 100 * 12 + ym % 100 - 1 + k, 12)
    return y * 100 + m + 1


def month_windows(pattern, first_months=6, ext_months=12):
    """Month-ALIGNED Stage-2 windows from a recent entry pattern (Revision 2, m1): for every calendar
    month m with `ext_months` full months available, the first look holds the positions entered in
    [m, m + first_months) whose exit is before m + first_months; the extension holds those entered in
    [m, m + ext_months) with exit before m + ext_months. One common set serves both designs."""
    srt = sorted(pattern, key=lambda p: p['t_in'])
    months = sorted({p['month'] for p in srt})
    first_full = months[1] if srt and _month_start(months[0]) < srt[0]['t_in'] - carry.DAY else months[0]
    out = []
    for m in months:
        if m < first_full or _add_months(m, ext_months - 1) > months[-1]:
            continue
        a, b1, b2 = _month_start(m), _month_start(_add_months(m, first_months)), _month_start(_add_months(m, ext_months))
        out.append({'first': [p for p in srt if a <= p['t_in'] and p['t_out'] < b1],
                    'full': [p for p in srt if a <= p['t_in'] and p['t_out'] < b2], 'first_end': b1})
    return out


def replicate_half_year(h1, shift_ms):
    """A 12-month pattern at the rate of one half-year: the half-year as recorded, then a copy shifted by
    `shift_ms` with its week and month recomputed. Returns one window (first = the half-year)."""
    twin = [dict(p, t_in=p['t_in'] + shift_ms, t_out=p['t_out'] + shift_ms, week=carry.week_of(p['t_in'] + shift_ms),
                 month=carry.month_of(p['t_in'] + shift_ms)) for p in h1]
    first_end = max(p['t_out'] for p in h1) + 1
    return [{'first': list(h1), 'full': list(h1) + twin, 'first_end': first_end}]


def simulate_stages(stage1_pos, windows, sigma_s, sigma_g, rho, mu_yr, sims=2000, seed=0, sigma_month=0.06,
                    extension=False, one_look_q=Q_ONE_LOOK):
    """Power of 012's path at a planted net edge mu_yr (per year on gross capital), with 010's noise
    model (`carry._draw`: funding noise at sigma_s x hold with a weekly common factor rho, gap noise
    sqrt(2) sigma_g, and a N(0, sigma_month) shock on the annualised edge per entry month). The model
    is Gaussian: it has no liquidation jump (a liquidated HL alt short is R ~ -0.15), so pass rates at a
    positive edge are optimistic (Revision 2, m7).

    Stage 1: the counted Stage-1 entry pattern, judged by the SIGN gate.
    Stage 2: one of `windows` (month-aligned; see `month_windows` / `replicate_half_year`), judged by
    the existence test: one look at t_{G-1}(one_look_q), or (extension=True) t_{G-1}(0.975) at the first
    look and, only when it ends 0 < t < crit, at the cumulative 12-month look.
    The stages draw independent noise and shocks. Returns P(stage 1), P(stage 2 alone), P(joint), and
    the mean Stage-2 first-look positions and months."""
    rng = random.Random(seed)
    p1 = p2 = joint = 0
    n2 = m2 = 0
    for _ in range(sims):
        ok1 = stage1_gate(carry._draw(stage1_pos, sigma_s, sigma_g, rho, mu_yr, rng, 1.0, sigma_month))[0]
        w = windows[rng.randrange(len(windows))]
        if extension:
            drawn = carry._draw(w['full'], sigma_s, sigma_g, rho, mu_yr, rng, 1.0, sigma_month)
            first = [r for r in drawn if r['t_out'] < w['first_end']]
            c1, det = carry.holdout_confirm(first, Q_TWO_LOOKS) if len(first) >= 3 else (False, None)
            ok2 = c1 or (det is not None and carry.extension_due(det) and carry.holdout_confirm(drawn, Q_TWO_LOOKS)[0])
        else:
            first = carry._draw(w['first'], sigma_s, sigma_g, rho, mu_yr, rng, 1.0, sigma_month)
            ok2 = len(first) >= 3 and carry.holdout_confirm(first, one_look_q)[0]
        n2 += len(first)
        m2 += len({r['month'] for r in first})
        p1 += ok1
        p2 += ok2
        joint += ok1 and ok2
    return {'mu_yr': mu_yr, 'sigma_month': sigma_month, 'sims': sims, 'p_stage1': p1 / sims,
            'p_stage2_alone': p2 / sims, 'p_joint': joint / sims,
            'p_stage2_given_stage1': joint / p1 if p1 else None,
            'stage2_mean_positions': n2 / sims, 'stage2_mean_months': m2 / sims}
