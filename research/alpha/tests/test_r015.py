"""Tests for registration 015 (synthetic data only; no outcome of the real files is computed).

    python3 -I research/alpha/tests/test_r015.py

The real files are touched only for: the registration hash table and file hashes; the dates-only calendar
(calendar.json, its rebuild from the US file's date column, and the B1 scout's dates-only calendar); refusals of
ungated reads on the real tree; the pinned power log's calibration fields. No return, mean or statistic of a real
file is computed here. Every check holds at DRAFT and at REGISTERED and before and after a commit (no commit-state or
status assertion on the real repository).
"""
import contextlib
import datetime as dt
import io
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent          # research/alpha
sys.path.insert(0, str(HERE))
import r015_run as R  # noqa: E402  (sets a fresh sys.pycache_prefix first)
import r015_core as C  # noqa: E402
import r015_hmm as H  # noqa: E402
import r015_power as P  # noqa: E402

PASSED, FAILED = [], []


def check(name, cond, detail=''):
    (PASSED if cond else FAILED).append(name)
    print(('  ok   ' if cond else '  FAIL ') + name + ('' if cond else f'   [{str(detail)[:300]}]'))


def raises(fn, exc=(C.Refused,)):
    try:
        fn()
    except exc:
        return True
    except Exception as e:  # noqa: BLE001
        print('    (raised another exception:', repr(e)[:200], ')')
        return False
    return False


# ------------------------------------------------------------------ synthetic calendars and files
US_HOLIDAYS = {dt.date(2017, 9, 4), dt.date(2024, 5, 27), dt.date(2020, 4, 10), dt.date(2019, 4, 19)}


def trading_days(a, b, holidays=US_HOLIDAYS):
    out, d = [], a
    while d <= b:
        if d.weekday() < 5 and not (d.month == 1 and d.day == 1) and not (d.month == 12 and d.day == 25) \
                and not (d.month == 7 and d.day == 4) and d not in holidays:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def french_zip(path, dates, values=None, junk_dates=(), extra_cols=False, header=',Mkt-RF,SMB,HML,RF'):
    """A French-format zip. `junk_dates` rows carry non-numeric values (parsing them would raise)."""
    lines = ['This file was created by a test.', '', header]
    rows = [(d, v) for d, v in zip(dates, values or [(0.0, 0.01)] * len(dates))] + [(d, None) for d in junk_dates]
    rows.sort(key=lambda r: r[0])
    for d, v in rows:
        if v is None:
            lines.append(f'{d:%Y%m%d},XX,XX,XX,XX')
        else:
            lines.append(f'{d:%Y%m%d},{v[0]:.10f},0.00,0.00,{v[1]:.10f}')
    lines += ['', 'Copyright test']
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as z:
        z.writestr(Path(path).stem.replace('_CSV', '') + '.csv', '\n'.join(lines) + '\n')
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_bytes(buf.getvalue())


def treasury_csv(path, dates, y=0.04, ncol=12, junk_after=None):
    hdr = ['Date', '1 Mo', '3 Mo', '6 Mo', '1 Yr', '2 Yr', '3 Yr', '5 Yr', '7 Yr', '10 Yr', '20 Yr', '30 Yr']
    if ncol == 14:
        hdr = ['Date', '1 Mo', '2 Mo', '3 Mo', '4 Mo', '6 Mo', '1 Yr', '2 Yr', '3 Yr', '5 Yr', '7 Yr', '10 Yr', '20 Yr', '30 Yr']
    k = hdr.index('10 Yr')
    lines = [','.join(hdr)]
    for d in sorted(dates, reverse=True):
        r = ['4.00'] * len(hdr)
        r[0] = f'{d:%m/%d/%Y}'
        r[k] = 'XX' if (junk_after and d >= junk_after) else f'{y * 100:.2f}'
        lines.append(','.join(r))
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text('\n'.join(lines) + '\n')


# ------------------------------------------------------------------ calendar
def calendar_checks():
    print('calendar: settlement lags, windows, boundary months, overlaps, placebo')
    check('us_lag: 2017-09-01 is T+3, 2017-09-05 T+2, 2024-05-24 T+2, 2024-05-28 T+1',
          (C.us_lag(dt.date(2017, 9, 1)), C.us_lag(dt.date(2017, 9, 5)), C.us_lag(dt.date(2024, 5, 24)),
           C.us_lag(dt.date(2024, 5, 28))) == (3, 2, 2, 1))
    days = trading_days(dt.date(2014, 1, 1), dt.date(2026, 6, 9))
    cal = C.build_calendar(days)
    check('149 months, 44 / 80 / 25 by lag on a synthetic trading calendar', C.calendar_failures(cal) == [], C.calendar_failures(cal))
    by = {c['month']: c for c in cal}
    check('boundary months: 2017-08 T+3, 2017-09 T+2, 2024-04 T+2, 2024-05 T+1',
          [by[m]['s'] for m in ('2017-08', '2017-09', '2024-04', '2024-05')] == [3, 2, 2, 1])
    ok = True
    for c in cal:
        s, iT = c['s'], c['iT']
        ok &= c['P'] == list(range(iT - 8, iT - s)) and c['R'] == list(range(iT - s, iT + 4))
        ok &= len(c['P']) == 8 - s and len(c['R']) == s + 4
        ok &= days[c['P'][-1]] < days[c['R'][0]] and not (set(c['out']) & (set(c['P']) | set(c['R'])))
        ok &= c['P3'] == list(range(iT - 8, iT - 3)) and c['R3'] == list(range(iT - 3, iT + 4))
    check('windows: P = T-8..T-(s+1) (5/6/7 days), R = T-s..T+3 (7/6/5), unshifted P3/R3 = Etula', ok)
    allw = [i for c in cal for i in c['P'] + c['R']]
    check('no trading day belongs to two months\' windows', len(allw) == len(set(allw)))
    realw = set(allw) | set(range(3))
    check('placebo windows (anchored at T-12) never contain a real window day', all(not ((set(c['PP']) | set(c['RP'])) & realw) for c in cal))
    check('out-of-window days: day 4 of the month to T-9', all(c['out'] == [i for i in range(c['iT'] - 30, c['iT'] - 8)
                                                                             if i >= 0 and C.month_of(days[i]) == c['month']][3:] for c in cal))
    s24 = by['2024-05']
    check('2024-05 (T+1 starts 2024-05-28 = T-3): last selling day T-2 = 2024-05-29, P ends there',
          days[s24['P'][-1]] == dt.date(2024, 5, 29) and days[s24['iT']] == dt.date(2024, 5, 31))
    check('2017-08: P ends T-4 = 2017-08-25 (T+3 in force); its R runs across the change to T+3 = 2017-09-06',
          days[by['2017-08']['P'][-1]] == dt.date(2017, 8, 25) and days[by['2017-08']['R'][-1]] == dt.date(2017, 9, 6))
    weird = lambda d: 3 if d < dt.date(2017, 8, 28) else 2       # a change on T-3: both s = 3 and s = 2 are fixed points
    check('a lag change landing between T-4 and T-3 that gives two fixed points is refused',
          raises(lambda: C.build_calendar(days, weird, '2017-08', '2017-08')))
    check('a gap (missing month) is refused', raises(lambda: C.build_calendar([d for d in days if C.month_of(d) != '2019-06'])))
    check('data ending before 2026-05\'s T+3 is refused', raises(lambda: C.build_calendar([d for d in days if d < dt.date(2026, 6, 2)])))
    check('non-increasing dates are refused', raises(lambda: C.build_calendar(days[:10] + days[9:])))
    check('calendar_failures: a wrong lag count is named', C.calendar_failures(cal, {3: 45, 2: 79, 1: 25}) != [])
    eu = C.build_calendar(trading_days(dt.date(2014, 1, 1), dt.date(2026, 6, 9), set()), C.eu_lag)
    check('Europe: T+3 through 2014-09, T+2 from 2014-10 (EU move 2014-10-06)',
          [c['s'] for c in eu if c['month'] <= '2014-09'] == [3] * 9 and all(c['s'] == 2 for c in eu if c['month'] >= '2014-10'))


# ------------------------------------------------------------------ the statistic
def statistic_checks():
    print('statistic: era mu, y, v, the existence rule, the economic bar, recheck')
    days = trading_days(dt.date(2014, 1, 1), dt.date(2026, 6, 9))
    cal = C.build_calendar(days)
    rng = random.Random(1)
    x = [rng.gauss(0.0003, 0.01) for _ in days]
    y, mu = C.y_series(x, cal)
    v = C.v_series(x, cal)
    check('y = v - (2s - 4) * mu_s exactly (the mu weight is +2 / 0 / -2 days)',
          all(abs(a - (b - (2 * c['s'] - 4) * mu[c['s']])) < 1e-15 for a, b, c in zip(y, v, cal)))
    x2 = list(x)
    for c in cal:
        for i in c['P'] + c['R']:
            x2[i] = 5.0
    check('era mu uses out-of-window days only (window days set to +500 % leave it unchanged)', C.era_mu(x2, cal) == mu)
    z = [0.0] * len(days)
    c0 = cal[10]
    z[c0['R'][0]] = 0.01
    z[c0['P'][0]] = 0.02
    check('v: +1 % on an R day and +2 % on a P day give -1 % that month and 0 elsewhere',
          abs(C.v_series(z, cal)[10] + 0.01) < 1e-15 and all(a == 0 for k, a in enumerate(C.v_series(z, cal)) if k != 10))
    eff = [0.0] * len(days)
    for c in cal:
        for i in c['P']:
            eff[i] += C.EFFECT_P / len(c['P'])
        for i in c['R']:
            eff[i] += C.EFFECT_R / len(c['R'])
    ye, _ = C.y_series(eff, cal)
    check('the full published effect, noiseless, gives y = 85 bp in every month', all(abs(a - 0.0085) < 1e-12 for a in ye))
    check('t quantiles: t_148(0.95) 1.6552, t_10(0.975) 2.2281, t_1(0.95) 6.3138',
          abs(C.t_ppf(0.95, 148) - 1.6552) < 2e-4 and abs(C.t_ppf(0.975, 10) - 2.2281) < 2e-4 and abs(C.t_ppf(0.95, 1) - 6.3138) < 2e-3)
    check('t cdf symmetric', abs(C.t_cdf(1.3, 20) + C.t_cdf(-1.3, 20) - 1) < 1e-12)
    check('registered critical value = max(t_148(0.95), CAL_CRIT) and never below t_148(0.95)',
          C.crit(149) == max(C.t_ppf(0.95, 148), C.CAL_CRIT) and C.crit(149) >= C.t_ppf(0.95, 148))
    good = [0.0085 + 0.039 * rng.gauss(0, 1) for _ in range(149)]
    good = [g - (sum(good) / 149) + 0.0085 for g in good]       # mean exactly 85 bp, sd ~ 3.9 %
    exg = C.existence(good)
    check('a 149-month series at 85 bp, sd 3.9 % passes the existence rule', exg['exists'], exg)
    check('criterion: mean <= 0 fails alone', not C.existence([-a for a in good])['criteria']['mean_positive'])
    small = [g - 0.0085 + 0.0010 for g in good]
    exs = C.existence(small)
    check('criterion: t below the critical value fails alone (mean +10 bp)',
          not exs['criteria']['t_at_or_above_crit'] and exs['criteria']['mean_positive'], exs)
    spike = [-0.001] * 148 + [3.0]
    exk = C.existence(spike)
    check('criterion: one huge month carrying the mean fails "mean > 0 without the largest month"',
          exk['criteria']['mean_positive'] and not exk['criteria']['mean_positive_without_largest_month'], exk['criteria'])
    check('criterion: G != 149 fails', not C.existence(good[:148])['criteria']['G_is_149'])
    e1 = C.economic([g for g in good])
    check('economic: 85 bp mean, sd 3.9 % -> shrunk net 40.9 bp/month, size capped at 1.2x, 5.9 %/yr, meets the bar',
          abs(e1['net_shrunk_month'] - (0.5 * 0.0085 - C.COST_MONTH)) < 1e-12 and e1['size_half_kelly_capped'] == C.L_CAP
          and e1['meets_bar'], e1)
    e2 = C.economic([g - 0.0085 + 0.0060 for g in good])
    check('economic: 60 bp mean -> below the 4 %/yr bar at 1/2 Kelly of the shrunk net', not e2['meets_bar'], e2)
    check('decide: no existence -> not_demonstrated; existence + bar -> exists_paper; existence without -> exists_below_bar',
          C.decide({'exists': False}, e1) == 'not_demonstrated' and C.decide({'exists': True}, e1) == 'exists_paper'
          and C.decide({'exists': True}, e2) == 'exists_below_bar')
    row = {'months': [{'y': a, 'v': a} for a in good], 'existence': exg, 'economic': e1, 'decision': C.decide(exg, e1)}
    row = json.loads(json.dumps(row))
    lab, fails = C.recheck(row)
    check('recheck: a genuine row recomputes', lab == 'exists_paper' and fails == [], fails)
    forged = json.loads(json.dumps(row))
    for r in forged['months']:
        r['y'] -= 0.0085
    check('recheck: a passing summary stored over failing per-month records is caught', C.recheck(forged)[1] != [])
    f2 = json.loads(json.dumps(row))
    f2['existence']['crit'] = 1.0
    check('recheck: a lowered stored critical value is caught', C.recheck(f2)[1] != [])
    f3 = json.loads(json.dumps(row))
    f3['decision'] = 'exists_paper'
    f3['months'] = [{'y': a, 'v': a - 0.0085 + 0.004} for a in good]
    check('recheck: a forged economic label is caught', C.recheck(f3)[1] != [])
    mc = C.mechanism_contrast(eff, cal, {3: 0.0, 2: 0.0, 1: 0.0})
    check('mechanism contrast, noiseless shifted effect: T+2 months gain 2 x 37/6 bp, T+1 months 2 x 2 x 37/7 bp',
          abs(mc['paired_t2']['mean'] - 2 * 0.0037 / 6 - (0.0048 / 6 - 0.0048 / 6)) < 1e-4
          and mc['paired_t1']['mean'] > mc['paired_t2']['mean'] > 0, mc)
    xc = [0.0] * len(days)
    for c in cal:
        for i in c['R'][:1]:
            xc[i] = 0.01
    check('xyz cost: 2 round trips + funding on (long - short) calendar days',
          abs(C.xyz_cost_month(days, cal[0]) - (2 * C.COST_RT_XYZ + C.XYZ_FUNDING_DAY * (
              (days[cal[0]['R'][-1]] - days[cal[0]['P'][-1]]).days - (days[cal[0]['P'][-1]] - days[cal[0]['P'][0] - 1]).days))) < 1e-15)
    fr = C.friday(good, cal)
    check('Friday split counts', fr['n_a'] + fr['n_b'] == 149 and fr['n_a'] == sum(1 for c in cal if c['weekday'] == 4))


# ------------------------------------------------------------------ loaders and the gate
def loader_checks():
    print('loaders: rows >= 2026-06-10 dropped before parsing, hashes, missing values, the gate on the real tree')
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        days = trading_days(dt.date(2014, 1, 1), dt.date(2026, 6, 9))
        junk = trading_days(dt.date(2013, 12, 2), dt.date(2013, 12, 31)) + trading_days(dt.date(2026, 6, 10), dt.date(2026, 8, 31))
        z = d / 'us.zip'
        french_zip(z, days, [(0.01, 0.002)] * len(days), junk)
        h = R.sha256(z)
        dd, vv = R.load_french(z, h)
        check('load_french keeps exactly [2014-01-01, 2026-06-10): non-numeric rows outside are never parsed',
              dd == days and len(vv['Mkt-RF']) == len(days) and abs(vv['Mkt-RF'][0] - 0.0001) < 1e-15)
        check('load_french refuses a hash mismatch', raises(lambda: R.load_french(z, '0' * 64)))
        check('read_dates_only returns the same dates and never parses a value', R.read_dates_only(z, h) == days)
        zbad = d / 'bad.zip'
        vals = [(0.01, 0.002)] * len(days)
        vals[100] = (-99.99, 0.002)
        french_zip(zbad, days, vals)
        check('load_french refuses a -99.99 (missing) value inside the span', raises(lambda: R.load_french(zbad, R.sha256(zbad))))
        zdup = d / 'dup.zip'
        french_zip(zdup, days + [days[-1]])
        check('load_french refuses a repeated date', raises(lambda: R.load_french(zdup, R.sha256(zdup))))
        t1, t2 = d / 't2014.csv', d / 't2026.csv'
        treasury_csv(t1, trading_days(dt.date(2014, 1, 1), dt.date(2014, 12, 31)), 0.03, 12)
        treasury_csv(t2, trading_days(dt.date(2026, 1, 1), dt.date(2026, 10, 9)), 0.045, 14, junk_after=dt.date(2026, 6, 10))
        ys = R.load_treasury([t1, t2], {t1: R.sha256(t1), t2: R.sha256(t2)})
        check('load_treasury: 10 Yr found by header name across layouts; rows >= 2026-06-10 dropped before parsing',
              max(ys) < dt.date(2026, 6, 10) and ys[dt.date(2014, 1, 2)] == 0.03 and ys[dt.date(2026, 6, 9)] == 0.045)
        check('load_treasury refuses a hash mismatch', raises(lambda: R.load_treasury([t1], {t1: 'f' * 64})))
    real_us = R.HERE / R.US_ZIP
    check('the gate: load_french on the real US file without the guard is refused',
          raises(lambda: R.load_french(real_us, 'x' * 64)))
    check('the gate: load_treasury on a real Treasury file without the guard is refused',
          raises(lambda: R.load_treasury([R.HERE / R.TREASURY[0]], {R.HERE / R.TREASURY[0]: 'x'})))
    saved = R._GATE
    R._GATE = (str(R.HERE.resolve()), 'forged')
    orig = R.guard
    R.guard = lambda: 'stub'
    try:
        check('the gate: a forged gate token with a stubbed guard is refused on the real tree', raises(lambda: R._gate(real_us)))
    finally:
        R.guard, R._GATE = orig, saved
    saved_h = dict(R.HOOKS)
    R.HOOKS['commit'] = lambda msg: None
    try:
        check('a mocked commit hook on the real results / data tree is refused', R.real_tree_failures() != [])
    finally:
        R.HOOKS.clear(), R.HOOKS.update(saved_h)
    sr = R.RESULTS
    R.RESULTS = Path(tempfile.gettempdir()) / 'r015-x' / 'results.jsonl'
    try:
        check('a mixed tree (synthetic results, real data) is refused', R.real_tree_failures() != [])
    finally:
        R.RESULTS = sr
    check('default hooks, real guard, real tree together: no real-tree refusal', R.real_tree_failures() == [])


# ------------------------------------------------------------------ HMM (reported replication)
def hmm_checks():
    print('HMM block: drift, duration, rebalancing, calendar signal timing')
    check('drift: +10 % equity day on 60/40 -> 62.26 %', abs(H.drift(0.6, 0.10, 0.0) - 66 / 106) < 1e-12)
    check('modified duration of a 10y par bond at 4 % ~ 8.18', abs(H.mod_duration(0.04) - 8.1757) < 2e-3, H.mod_duration(0.04))
    days = trading_days(dt.date(2014, 1, 1), dt.date(2014, 6, 30))
    r_sp = [0.0] * len(days)
    r_b = [0.0] * len(days)
    r_sp[5] = 0.10
    rets, eq, w = H.strategy(days, r_sp, r_b)
    check('threshold: an equity jump makes the weight negative the same close (front-runner sells equity)', w[5] < 0, w[5])
    last, week4, first = H.last_business_flags(days)
    check('week4 = the last 5 business days of each month', all(sum(1 for i in range(len(days)) if week4[i] and C.month_of(days[i]) == m) == 5
                                                               for m in ('2014-02', '2014-03', '2014-04')))
    check('strategy return on day t+1 = w_t * (R_SP - R_10Y)', all(abs(r - w[i] * (r_sp[i + 1] - r_b[i + 1])) < 1e-15
                                                                  for i, (_, r) in enumerate(rets)))
    ys = {d: 0.04 for d in days[:50]}
    br = H.bond_returns(days, ys)
    check('bond proxy: flat yield -> carry y/252 per day; carried forward on missing yields', abs(br[60] - 0.04 / 252) < 1e-15)


# ------------------------------------------------------------------ power module
def power_checks():
    print('power: worlds draw the intended structure (synthetic)')
    days = trading_days(dt.date(2014, 1, 1), dt.date(2026, 6, 9))
    cal = C.build_calendar(days)
    rng = random.Random(3)
    era = [3] * len(days)
    yr = [d.year for d in days]
    x = P.draw({'name': 'a', 'shocks': [('2020', 'aligned')]}, days, cal, rng, era, yr)
    shape = P.SHOCK_2020
    j = [shape[i] + shape[i + 1] for i in range(len(shape) - 1)]
    j = j.index(min(j)) + 1
    hit = [c for c in cal if x[c['P'][-1]] == shape[j] and x[c['P'][-1] - 1] == shape[j - 1]]
    check('aligned placement: the worst two-day down-leg ends on a P window\'s last day', len(hit) >= 1)
    x0 = P.draw({'name': 'e', 'effect': 1.0}, days, cal, random.Random(4), era, yr)
    xn = P.draw({'name': 'n'}, days, cal, random.Random(4), era, yr)
    d = [a - b for a, b in zip(x0, xn)]
    ye, _ = C.y_series(d, cal)
    check('effect worlds add exactly the published effect (85 bp L/S per month) to the same draws', all(abs(a - 0.0085) < 1e-12 for a in ye))
    xu = P.draw({'name': 'u', 'effect': 1.0, 'effect_mode': 'unshifted'}, days, cal, random.Random(4), era, yr)
    du = [a - b for a, b in zip(xu, xn)]
    yu = C.window_sums(du, cal, 'P3', 'R3', mu={3: 0.0, 2: 0.0, 1: 0.0})
    check('unshifted world: the effect sits on Etula\'s T+3 windows in every era', all(abs(a - 0.0085) < 1e-12 for a in yu))
    recs = [{'t': t, 'pos': True, 'wo': True} for t in [0.1 * k for k in range(100)]]
    check('gate score quantile', abs(P.quantile([P.gate_score(r) for r in recs], 0.95) - 9.405) < 1e-9)
    check('calibrate: never below t_148(0.95), rounded up', P.calibrate({'w': recs[:10]})[0] == 1.66
          and P.calibrate({'w': recs})[0] == 9.41)


# ------------------------------------------------------------------ guard in throwaway repositories (two layouts)
def _git(root, *a):
    return subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root, capture_output=True, text=True)


def guard_checks(sub=''):
    """Both layouts: runner files at the repository top level, and in `research/alpha` (the real layout). git commands
    run with cwd = the runner directory (014 review B B1: `<rev>:<path>` resolves from the repository root)."""
    print(f"guard (throwaway git repo, runner at {sub or 'the top level'}): (s), (a)-(e), (k), (w), (f), record() in HEAD")
    code, data = ('r015_core.py', 'r015_run.py'), ('data/r015/x.zip',)
    reg_rel = 'registrations/015-month-end-liquidity-cycle.md'
    with tempfile.TemporaryDirectory() as top:
        top = Path(top).resolve()
        root = top / sub
        root.mkdir(parents=True, exist_ok=True)
        subprocess.run(['git', 'init', '-q', '-b', 'main'], cwd=top, capture_output=True, check=True)
        g = lambda *a: _git(root, *a)
        for f, body in (('r015_core.py', 'x = 1\n'), ('r015_run.py', 'y = 2\n'), ('data/r015/x.zip', 'z\n'),
                        ('results.jsonl', '')):
            (root / f).parent.mkdir(parents=True, exist_ok=True)
            (root / f).write_text(body)
        (root / '.gitignore').write_text('data/\n')

        def write_reg(status='REGISTERED', extra=''):
            rows = '\n'.join(f'| `{f}` | `{R.sha256(root / f)}` |' for f in (*code, *data))
            (root / reg_rel).parent.mkdir(exist_ok=True)
            (root / reg_rel).write_text(f'# 015\n\n**Status: {status} 2026-10-14**\n\n| file | sha256 |\n|---|---|\n{rows}\n{extra}')
        fails = lambda **k: R.guard_failures(root, reg_rel, code, data, toplevel=top, **k)[0]
        write_reg('DRAFT')
        g('add', '.')
        g('commit', '-q', '-m', 'draft')
        check('DRAFT -> refused (s)', any(x.startswith('(s)') for x in fails()))
        write_reg()
        check('REGISTERED but uncommitted -> refused (b)', any(x.startswith('(b)') for x in fails()))
        g('commit', '-q', '-am', 'register')
        anchor = g('rev-parse', 'HEAD').stdout.strip()
        check('REGISTERED, committed, clean, hashes match, no row -> the guard opens', fails(anchor=anchor) == [], fails())
        (root / 'r015_core.py').write_text('x = 3\n')
        f4 = fails()
        check('(d) + (e) guarded code edited -> refused', any(x.startswith('(d)') for x in f4) and any(x.startswith('(e) r015_core') for x in f4))
        g('checkout', '--', 'r015_core.py')
        (root / data[0]).write_text('changed\n')
        check('(e) a data file changed -> refused', any(x.startswith('(e) data/r015') for x in fails()))
        (root / data[0]).write_text('z\n')
        write_reg(extra=f'| `r015_core.py` | `{"0" * 64}` |\n')
        g('commit', '-q', '-am', 'dup')
        check('(e) a file listed twice in the hash table -> refused', any('more than once' in x for x in fails()))
        g('reset', '-q', '--hard', 'HEAD~1')
        (root / 'results.jsonl').write_text(json.dumps({'registration': '005-y.md'}) + '\n')
        check('(k) an uncommitted foreign row in results.jsonl -> refused', any(x.startswith('(k)') for x in fails()))
        g('checkout', '--', 'results.jsonl')
        check('(w) a wrong expected top level -> refused', any(x.startswith('(w)') for x in R.guard_failures(root, reg_rel, code, data, toplevel=top / 'elsewhere')[0]))
        check('(w) an anchor that is not an ancestor -> refused', any(x.startswith('(w)') for x in fails(anchor='1' * 40)))
        # run once: a 015 row anywhere blocks
        add = lambda row: open(root / 'results.jsonl', 'a').write(json.dumps(row, sort_keys=True) + '\n')
        add({'registration': '015-month-end-liquidity-cycle.md', 'mode': 'started'})
        check('(f) an uncommitted started row blocks', any(x.startswith('(f)') for x in fails()))
        g('commit', '-q', '-am', 'row')
        (root / 'results.jsonl').write_text('')
        g('commit', '-q', '-am', 'erase')
        check('(f) a committed then erased row is still found in history', any(x.startswith('(f)') for x in fails()))
        g('reset', '-q', '--hard', 'HEAD~2')
        check('(f) after reset --hard the row is still found through the reflog', any(x.startswith('(f)') for x in fails()))
        subprocess.run(['git', 'reflog', 'expire', '--expire=now', '--all'], cwd=root, capture_output=True)
        subprocess.run(['git', 'gc', '-q', '--prune=now'], cwd=root, capture_output=True)
        check('... history purged (throwaway repo only): the guard opens again', fails() == [], fails())
        add({'registration': '015-other-name.md', 'mode': 'result'})
        g('stash', '-q')
        check('(f) a renamed 015 registration row hidden in a stash is found (prefix match, -m)', any(x.startswith('(f)') for x in fails()))
        g('stash', 'drop', '-q')
        subprocess.run(['git', 'reflog', 'expire', '--expire=now', '--all'], cwd=root, capture_output=True)
        subprocess.run(['git', 'gc', '-q', '--prune=now'], cwd=root, capture_output=True)
        g('checkout', '-q', '-b', 'side')
        add({'registration': '015-month-end-liquidity-cycle.md', 'mode': 'result'})
        g('commit', '-q', '-am', 'side row')
        g('checkout', '-q', 'main')
        check('(f) a row on another branch blocks', any(x.startswith('(f)') for x in fails()))
        (root / '.gitattributes').write_text('results.jsonl -diff\n')
        g('add', '.gitattributes')
        g('commit', '-q', '-m', 'attr')
        g('config', 'diff.external', '/bin/true')
        check('(f) a -diff attribute and diff.external do not hide the row (--text --no-ext-diff)', any(x.startswith('(f)') for x in fails()))
        g('config', '--unset', 'diff.external')
        with tempfile.TemporaryDirectory() as decoy:
            subprocess.run(['git', 'init', '-q', decoy], capture_output=True)
            old = os.environ.get('GIT_DIR')
            os.environ['GIT_DIR'] = str(Path(decoy) / '.git')
            try:
                check('(f) GIT_DIR pointing at a decoy repository is stripped', any(x.startswith('(f)') for x in fails()))
            finally:
                if old is None:
                    os.environ.pop('GIT_DIR', None)
                else:
                    os.environ['GIT_DIR'] = old
        if sub:
            subprocess.run(['git', 'init', '-q'], cwd=root, capture_output=True)
            check('(w) a nested `git init` in the runner directory is refused (top level differs)',
                  any(x.startswith('(w)') for x in fails()))
            shutil.rmtree(root / '.git')


def record_checks(sub=''):
    """record() appends, commits through the hook and verifies the line in HEAD via `HEAD:./results.jsonl`. In the
    subdirectory layout the unfixed `HEAD:results.jsonl` reads nothing, so record() refuses: shown both ways."""
    print(f"record() and HEAD:./ paths (runner at {sub or 'the top level'})")
    with tempfile.TemporaryDirectory() as top:
        top = Path(top).resolve()
        root = top / sub
        root.mkdir(parents=True, exist_ok=True)
        subprocess.run(['git', 'init', '-q', '-b', 'main'], cwd=top, capture_output=True, check=True)
        (root / 'results.jsonl').write_text('')
        _git(root, 'add', 'results.jsonl')
        _git(root, 'commit', '-q', '-m', 'init')
        saved = (R.RESULTS, R.DATA, R.revpath)
        R.RESULTS, R.DATA = root / 'results.jsonl', root / 'data' / 'r015'
        hooks = dict(R.HOOKS)
        R.HOOKS['commit'] = lambda msg: _git(root, 'commit', '--only', '-q', '-m', msg, '--', 'results.jsonl')
        try:
            ok = not raises(lambda: R.record({'registration': '015-x.md', 'mode': 'started'}, 'row'))
            check('record(): the row is committed and verified in HEAD (fixed HEAD:./ path)', ok)
            R.revpath = lambda rev, rel: f'{rev}:{rel}'           # the unfixed form, for the demonstration only
            unfixed = raises(lambda: R.record({'registration': '015-x.md', 'mode': 'result'}, 'row 2'))
            if sub:
                check('UNFIXED `HEAD:results.jsonl` in the subdirectory layout: record() refuses (the 014 B1 failure)', unfixed)
            else:
                check('unfixed path at the top level still works (why the bug hid in top-level tests)', not unfixed)
            R.revpath = saved[2]
            R.HOOKS['commit'] = lambda msg: None
            check('record(): a no-op commit hook (row never committed) is refused', raises(lambda: R.record({'registration': '015-x.md', 'mode': 'x'}, 'm')))
        finally:
            R.RESULTS, R.DATA, R.revpath = saved
            R.HOOKS.clear(), R.HOOKS.update(hooks)


# ------------------------------------------------------------------ end to end (synthetic data, throwaway repo)
def build_synthetic_tree(root, seed=7, effect=0.0):
    days = trading_days(dt.date(2014, 1, 1), dt.date(2026, 6, 9))
    junk = trading_days(dt.date(2026, 6, 10), dt.date(2026, 8, 31))
    cal = C.build_calendar(days)
    rng = random.Random(seed)
    x = [rng.gauss(0.0, C.DAILY_SD) for _ in days]
    for c in cal:
        for i in c['P']:
            x[i] += effect * C.EFFECT_P / len(c['P'])
        for i in c['R']:
            x[i] += effect * C.EFFECT_R / len(c['R'])
    vals = [(100 * a, 0.01) for a in x]
    french_zip(root / R.US_ZIP, days, vals, junk)
    eu_days = trading_days(dt.date(2014, 1, 1), dt.date(2026, 6, 9), set())
    french_zip(root / R.EU_ZIP, eu_days, [(100 * rng.gauss(0, 0.01), 0.0) for _ in eu_days], [], header=',Mkt-RF,SMB,HML,RF')
    for p in R.TREASURY:
        y = int(p[-8:-4])
        treasury_csv(root / p, trading_days(dt.date(y, 1, 1), dt.date(y, 12, 31) if y < 2026 else dt.date(2026, 10, 9)),
                     0.03 + 0.001 * (y - 2014), 12 if y < 2022 else 14, junk_after=dt.date(2026, 6, 10))
    for dname in (R.FRENCH_DIR, R.TREASURY_DIR):
        dd = root / dname
        (dd / 'SHA256SUMS').write_text(''.join(f'{R.sha256(f)}  {f.name}\n' for f in sorted(dd.iterdir()) if f.name != 'SHA256SUMS'))
    with contextlib.redirect_stdout(io.StringIO()):
        R.build_calendar_file(root)
    (root / R.POWER_LOG).parent.mkdir(parents=True, exist_ok=True)
    (root / R.POWER_LOG).write_text('{}\n')
    return x, cal


def e2e_checks(sub='research/alpha'):
    print(f"end to end (throwaway repo, runner at {sub or 'the top level'}): started -> result -> reported, each in HEAD; "
          "run once; recheck")
    with tempfile.TemporaryDirectory() as top:
        top = Path(top).resolve()
        root = top / sub
        root.mkdir(parents=True, exist_ok=True)
        subprocess.run(['git', 'init', '-q', '-b', 'main'], cwd=top, capture_output=True, check=True)
        _git(top, 'config', 'user.name', 't')
        _git(top, 'config', 'user.email', 't@t')
        for f in R.GUARDED_CODE:
            shutil.copy(R.HERE / f, root / f)
        (root / 'results.jsonl').write_text('')
        (root / '.gitignore').write_text('data/\n')
        x, cal = build_synthetic_tree(root, effect=1.0)
        rebuilt = json.loads((root / R.CALENDAR).read_text())
        check('calendar.json from the synthetic file: 149 months, 44 / 80 / 25, dates end 2026-06-09',
              rebuilt['summary']['by_lag'] == {'3': 44, '2': 80, '1': 25} and rebuilt['dates'][-1] == '2026-06-09')
        check('calendar rebuild is byte-identical, and a differing pinned calendar is refused (never overwritten)',
              not raises(lambda: quiet(R.build_calendar_file, root))
              and raises(lambda: quiet(R.build_calendar_file, root, out=_tamper(root / R.CALENDAR))))
        reg_rel = 'registrations/015-month-end-liquidity-cycle.md'
        rows = '\n'.join(f'| `{f}` | `{R.sha256(root / f)}` |' for f in (*R.GUARDED_CODE, *R.GUARDED_DATA))
        (root / 'registrations').mkdir()
        (root / reg_rel).write_text(f'# 015\n\n**Status: REGISTERED 2026-10-14**\n\n| file | sha256 |\n|---|---|\n{rows}\n')
        _git(root, 'add', '.')
        _git(root, 'commit', '-q', '-m', 'registered')
        saved = (R.RESULTS, R.DATA, R.guard, R._GATE)

        def fake_guard():
            f, c = R.guard_failures(root, reg_rel, toplevel=top)
            if f:
                raise C.Refused('\n'.join(f))
            return c
        R.RESULTS, R.DATA, R.guard = root / 'results.jsonl', root / 'data' / 'r015', fake_guard
        try:
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                row, rep = R.run(root)
            head = R.head_lines(root)
            modes = [json.loads(l)['mode'] for l in head]
            check('run: started, result and reported rows, each committed in HEAD in that order', modes == ['started', 'result', 'reported'], modes)
            check('run: the result recomputes from its per-month records', C.recheck(json.loads(head[1]))[1] == [])
            ye, _ = C.y_series(x, cal)
            check('run: y stored per month equals y recomputed from the synthetic returns', all(abs(a['y'] - b) < 1e-9 for a, b in zip(row['months'], ye)))
            check('run: synthetic full effect -> the existence rule passes (sanity of the pipeline, synthetic)', row['existence']['exists'], row['existence'])
            check('run: the reported row links to the result row by its sha', rep['result_row_sha256'] == R.line_sha(head[1]))
            errs = [k for k, v in rep['blocks'].items() if isinstance(v, dict) and 'error' in v]
            check('run: every reported block computed (Europe, HMM, contrast, placebo, Friday, xyz, long-only)', errs == [], errs)
            check('run: the synthetic file\'s non-numeric rows dated >= 2026-06-10 were never parsed (the run completed); last T 2026-05-29',
                  row['months'][-1]['T'] == '2026-05-29' and '2026-07' not in head[1])
            check('run once: a second run is refused by (f)', raises(lambda: R.run(root)))
        finally:
            R.RESULTS, R.DATA, R.guard, R._GATE = saved


def quiet(fn, *a, **k):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*a, **k)


def _tamper(p):
    q = p.with_name('calendar_tampered.json')
    q.write_text(p.read_text().replace('"2014-01-02"', '"2014-01-03"', 1))
    return q


# ------------------------------------------------------------------ environment and shadow modules
def shadow_checks():
    print('shadow modules and bytecode')
    with tempfile.TemporaryDirectory() as d:
        root = Path(d).resolve()
        subprocess.run(['git', 'init', '-q', '-b', 'main'], cwd=root, capture_output=True, check=True)
        (root / 'r015_core.py').write_text('x = 1\n')
        (root / '.gitignore').write_text('csv.py\n')
        _git(root, 'add', '.')
        _git(root, 'commit', '-q', '-m', 'init')
        check('clean tracked files: no shadow', R.shadow_modules(root) == [])
        (root / 'json.py').write_text('')
        (root / 'random.pyc').write_bytes(b'\0')
        (root / 'csv.py').write_text('')
        (root / 'email').mkdir()
        (root / 'email' / '__init__.py').write_text('')
        (root / 'r015_hmm.cpython-314-darwin.so').write_bytes(b'\0')
        (root / 'r015_core.py').write_text('x = 2\n')
        found = set(R.shadow_modules(root))
        want = {'json.py', 'random.pyc', 'csv.py', 'email/__init__.py', 'r015_hmm.cpython-314-darwin.so', 'r015_core.py'}
        check('untracked .py, sourceless .pyc, ignored .py, a package, an extension module and a modified file are caught',
              want <= found, found)
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        src = d / 'modx.py'
        src.write_text('X = 2  # evil\n')
        import py_compile
        py_compile.compile(str(src), cfile=str(d / '__pycache__' / f'modx.{sys.implementation.cache_tag}.pyc'))
        st = src.stat()
        src.write_text('X = 1  # good\n')
        os.utime(src, ns=(st.st_atime_ns, st.st_mtime_ns))
        plain = subprocess.run([sys.executable, '-I', '-c', f'import sys; sys.path.insert(0, {str(d)!r}); import modx; print(modx.X)'],
                               capture_output=True, text=True).stdout.strip()
        fresh = subprocess.run([sys.executable, '-I', '-c', 'import sys, tempfile; sys.pycache_prefix = tempfile.mkdtemp(); '
                                f'sys.path.insert(0, {str(d)!r}); import modx; print(modx.X)'], capture_output=True, text=True).stdout.strip()
        check('a forged __pycache__ .pyc loads under plain python and NOT under a fresh pycache_prefix', plain == '2' and fresh == '1', (plain, fresh))
    check('this process: r015_core / r015_hmm loaded from source, pycache_prefix fresh and outside the repo', R.loaded_from_source() == [], R.loaded_from_source())
    o = subprocess.run([sys.executable, '-O', '-I', '-c', f'import sys; sys.path.insert(0, {str(R.HERE)!r}); import r015_run as R; '
                        'print(any("-O" in x for x in R.environment_failures(R.HERE)))'], capture_output=True, text=True)
    check('python -O is refused by the environment check', o.stdout.strip() == 'True', o.stderr[-300:])
    fp = R.env_fingerprint()
    check('environment fingerprint: python version, executable, stdlib source hash', fp['python'] and fp['stdlib_sha256'] and fp['stdlib_files'])


# ------------------------------------------------------------------ real files (names, hashes, dates only)
def real_file_checks():
    print('real files: the registration table, hashes, the dates-only calendar, the power log (no outcome)')
    reg = R.REG
    if not reg.exists():
        check('registration file exists', False)
        return
    rows = R.HASH_ROW.findall(reg.read_text())
    names = [f for f, _ in rows]
    table = dict(rows)
    check('every guarded code and data file is in the hash table exactly once',
          all(names.count(f) == 1 for f in (*R.GUARDED_CODE, *R.GUARDED_DATA)), [f for f in (*R.GUARDED_CODE, *R.GUARDED_DATA) if names.count(f) != 1])
    bad = [f for f in (*R.GUARDED_CODE, *R.GUARDED_DATA) if not (R.HERE / f).is_file() or R.sha256(R.HERE / f) != table.get(f)]
    check('every guarded file matches its registered SHA-256', bad == [], bad)
    sums = {l.split()[1]: l.split()[0] for l in (R.HERE / R.FRENCH_DIR / 'SHA256SUMS').read_text().splitlines() if l.strip()}
    check('the French pull matches its SHA256SUMS and the B1 scout\'s pinned copy (same bytes)',
          all(R.sha256(R.HERE / R.FRENCH_DIR / n) == h for n, h in sums.items())
          and sums['F-F_Research_Data_Factors_daily_CSV.zip'] == '2f29e22546069914890a712680a6f81f3680ebc3543b52864484d209ca13a7db')
    pin = json.loads((R.HERE / R.CALENDAR).read_text())
    check('calendar.json: 149 months, 44 / 80 / 25, 64 Friday-T, dates 2014-01-02 -> 2026-06-09 (dates only)',
          pin['summary']['by_lag'] == {'3': 44, '2': 80, '1': 25} and pin['summary']['friday_T'] == 64
          and pin['dates'][0] == '2014-01-02' and pin['dates'][-1] == '2026-06-09' and pin['summary']['months'] == 149)
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / 'calendar.json'
        with contextlib.redirect_stdout(io.StringIO()):
            R.build_calendar_file(out=out)
        check('calendar.json rebuilds byte-identically from the US file\'s date column (dates only)', out.read_text() == (R.HERE / R.CALENDAR).read_text())
    b1 = R.HERE / 'data' / 'scout_b1' / 'derived_20261013T092459Z' / 'month_end_calendar_US.csv'
    if b1.exists():
        import csv
        rows_b1 = {r['month']: r for r in csv.DictReader(open(b1))}
        mism = [r['month'] for r in pin['summary']['rows'] if not (
            rows_b1[r['month']]['T'] == r['T'] and rows_b1[r['month']]['settle_lag'] == str(r['s'])
            and r['P'] == [rows_b1[r['month']]['pressure_start_T-8'], rows_b1[r['month']]['pressure_end_T-(s+1)']]
            and r['R'] == [rows_b1[r['month']]['rebound_start_T-s'], rows_b1[r['month']]['rebound_end_T+3']])]
        check('the calendar equals the B1 scout\'s independently built calendar for all 149 months (dates only)', mism == [], mism)
    pw = json.loads((R.HERE / R.POWER_LOG).read_text())
    check('the pinned power log was produced at the registered critical value (CAL_CRIT = calibrated)',
          pw.get('calibrated_crit') == C.CAL_CRIT and pw.get('calibrated_equals_registered') is True,
          (pw.get('calibrated_crit'), C.CAL_CRIT))
    f, _ = R.guard_failures(R.HERE, str(R.REG.relative_to(R.HERE)), toplevel=R.REPO, anchor=R.ANCHOR)
    check('real repository: the guard sees this repository as top level and the anchor as an ancestor (no (w))',
          not any(x.startswith('(w)') for x in f), f)
    check('real repository: no (e) hash failure', not any(x.startswith('(e)') for x in f), [x for x in f if x.startswith('(e)')])


def main():
    calendar_checks()
    statistic_checks()
    loader_checks()
    hmm_checks()
    power_checks()
    guard_checks()
    guard_checks('research/alpha')
    record_checks()
    record_checks('research/alpha')
    e2e_checks('')
    e2e_checks('research/alpha')
    shadow_checks()
    real_file_checks()
    print(f'\n{len(PASSED)} passed, {len(FAILED)} failed')
    for f in FAILED:
        print('  FAILED:', f)
    sys.exit(1 if FAILED else 0)


if __name__ == '__main__':
    main()
