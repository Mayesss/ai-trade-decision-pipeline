"""Tests for registration 015, Revision 1 (synthetic data only; no outcome of the real files is computed).

    python3 -I research/alpha/tests/test_r015.py

The real files are touched only for: the HEAD registration's hash table and file hashes; the dates-only calendar
(calendar.json, its rebuild from the US file's date column, and the B1 scout's dates-only calendar); refusals of
ungated reads on the real tree; the pinned power log's calibration fields. No return, mean or statistic of a real
file is computed here. Every check holds at DRAFT and at REGISTERED, before and after a commit, and in a clone that
holds only synthetic data (a marker file `data/r015/SYNTHETIC` switches off the two real-byte identity checks).
"""
import contextlib
import datetime as dt
import hashlib
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
import r015_power as P  # noqa: E402

PASSED, FAILED = [], []
GIT = R.GIT_BIN


def check(name, cond, detail=''):
    (PASSED if cond else FAILED).append(name)
    print(('  ok   ' if cond else '  FAIL ') + name + ('' if cond else f'   [{str(detail)[:300]}]'))


def raises(fn, exc=(C.Refused,), text=None):
    try:
        fn()
    except exc as e:
        return text is None or text in str(e)
    except Exception as e:  # noqa: BLE001
        print('    (raised another exception:', repr(e)[:200], ')')
        return False
    return False


def quiet(fn, *a, **k):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*a, **k)


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


DAYS = trading_days(C.DATA_START, C.DATA_END - dt.timedelta(days=1))


def french_zip(path, dates, values=None, junk_dates=(), header=',Mkt-RF,SMB,HML,RF'):
    """A French-format zip. `junk_dates` rows carry non-numeric values (parsing them would raise)."""
    lines = ['This file was created by a test.', '', header]
    rows = [(d, v) for d, v in zip(dates, values or [(0.0, 0.01)] * len(dates))] + [(d, None) for d in junk_dates]
    rows.sort(key=lambda r: r[0])
    for d, v in rows:
        lines.append(f'{d:%Y%m%d},XX,XX,XX,XX' if v is None else f'{d:%Y%m%d},{v[0]:.10f},0.00,0.00,{v[1]:.10f}')
    lines += ['', 'Copyright test']
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as z:
        z.writestr(Path(path).stem.replace('_CSV', '') + '.csv', '\n'.join(lines) + '\n')
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_bytes(buf.getvalue())


def g(root, *a, check_=False):
    env = R.git_env()
    return subprocess.run([GIT, '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root, capture_output=True,
                          text=True, env=env, check=check_)


# ------------------------------------------------------------------ calendar
def calendar_checks():
    print('calendar: settlement lags, windows, vol window, boundary months, overlaps, placebo')
    check('us_lag: 2017-09-01 is T+3, 2017-09-05 T+2, 2024-05-24 T+2, 2024-05-28 T+1',
          (C.us_lag(dt.date(2017, 9, 1)), C.us_lag(dt.date(2017, 9, 5)), C.us_lag(dt.date(2024, 5, 24)),
           C.us_lag(dt.date(2024, 5, 28))) == (3, 2, 2, 1))
    days = DAYS
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
        ok &= not (set(c['out']) & (set(c['P']) | set(c['R'])))
        ok &= c['P3'] == list(range(iT - 8, iT - 3)) and c['R3'] == list(range(iT - 3, iT + 4))
        ok &= c['early'] + c['tom'] == c['R']
    check('windows: P = T-8..T-(s+1), R = T-s..T+3 = early (T-s..T-1) + tom (T..T+3); unshifted P3/R3 = Etula', ok)
    check('vol window: the 20 trading days T-28 .. T-9, ending before the pressure window opens',
          all(c['vol'] == list(range(c['iT'] - 28, c['iT'] - 8)) and max(c['vol']) < min(c['P']) for c in cal))
    allw = [i for c in cal for i in c['P'] + c['R']]
    check('no trading day belongs to two months\' windows', len(allw) == len(set(allw)))
    first = [i for i, d in enumerate(days) if C.month_of(d) == '2014-01'][:3]
    realw = set(allw) | set(first)
    check('placebo windows (anchored at T-12) never contain a real window day', all(not ((set(c['PP']) | set(c['RP'])) & realw) for c in cal))
    s24 = by['2024-05']
    check('2024-05 (T+1 starts 2024-05-28 = T-3): last selling day T-2 = 2024-05-29',
          days[s24['P'][-1]] == dt.date(2024, 5, 29) and days[s24['iT']] == dt.date(2024, 5, 31))
    check('2017-08: P ends T-4 = 2017-08-25; R runs across the change to T+3 = 2017-09-06',
          days[by['2017-08']['P'][-1]] == dt.date(2017, 8, 25) and days[by['2017-08']['R'][-1]] == dt.date(2017, 9, 6))
    weird = lambda d: 3 if d < dt.date(2017, 8, 28) else 2
    check('a lag change giving two fixed points is refused', raises(lambda: C.build_calendar(days, weird, '2017-08', '2017-08')))
    check('a gap (missing month) is refused', raises(lambda: C.build_calendar([d for d in days if C.month_of(d) != '2019-06'])))
    check('data ending before 2026-05\'s T+3 is refused', raises(lambda: C.build_calendar([d for d in days if d < dt.date(2026, 6, 2)])))
    check('data starting too late for 2014-01\'s vol window is refused',
          raises(lambda: C.build_calendar([d for d in days if d >= dt.date(2014, 1, 1)])))
    check('non-increasing dates are refused', raises(lambda: C.build_calendar(days[:10] + days[9:])))
    check('DATA_END 2026-06-04: 2026-05\'s T+3 (2026-06-03) is the last day loaded (review B m9)',
          C.DATA_END == dt.date(2026, 6, 4) and days[cal[-1]['R'][-1]] == dt.date(2026, 6, 3) and days[-1] == dt.date(2026, 6, 3))


# ------------------------------------------------------------------ the statistic
def statistic_checks():
    print('statistic: era mu, y, v, sigma, z, the existence rule, the T+1-regime bar, recheck')
    days = DAYS
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
    check('era mu uses out-of-window days only', C.era_mu(x2, cal) == mu)
    sig = C.sigma_series(x, cal)
    c5 = cal[5]
    want = max(C.VOL_FLOOR, (sum(x[i] ** 2 for i in c5['vol']) / 20) ** 0.5)
    check('sigma_m = max(0.4 %, RMS of x over T-28..T-9)', abs(sig[5] - want) < 1e-15)
    x3 = list(x)
    for i in range(min(c5['P']), len(x3)):
        x3[i] = 9.0
    check('sigma_m uses nothing on or after the month\'s first pressure day (known when the position opens)',
          C.sigma_series(x3, cal)[5] == sig[5])
    xs = [0.0] * len(days)
    check('sigma floor 0.4 %/day applies on a flat tape', all(s_ == C.VOL_FLOOR for s_ in C.sigma_series(xs, cal)))
    z, y2, sg, _ = C.z_series(x, cal)
    check('z = y / (sqrt(12) sigma)', all(abs(a - b / (12 ** 0.5 * s_)) < 1e-15 for a, b, s_ in zip(z, y2, sg)))
    z0 = [0.0] * len(days)
    c0 = cal[10]
    z0[c0['R'][0]] = 0.01
    z0[c0['P'][0]] = 0.02
    check('v: +1 % on an R day and +2 % on a P day give -1 % that month and 0 elsewhere',
          abs(C.v_series(z0, cal)[10] + 0.01) < 1e-15 and all(a == 0 for k, a in enumerate(C.v_series(z0, cal)) if k != 10))
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
    check('registered critical value = max(t_148(0.95), CAL_CRIT) and never below t_148(0.95)',
          C.crit(149) == max(C.T148, C.CAL_CRIT) and C.crit(149) >= C.T148)
    good = [0.25 + rng.gauss(0, 1) for _ in range(149)]
    good = [a - sum(good) / 149 + 0.25 for a in good]
    exg = C.existence(good)
    check('a 149-month z series at mean 0.25 SD 1 passes the existence rule', exg['exists'], exg)
    check('criterion: mean <= 0 fails alone', not C.existence([-a for a in good])['criteria']['mean_positive'])
    exs = C.existence([a - 0.25 + 0.02 for a in good])
    check('criterion: t below the critical value fails alone', not exs['criteria']['t_at_or_above_crit'] and exs['criteria']['mean_positive'])
    exk = C.existence([-0.01] * 148 + [30.0])
    check('criterion: one huge month carrying the mean fails "mean > 0 without the largest month"',
          exk['criteria']['mean_positive'] and not exk['criteria']['mean_positive_without_largest_month'])
    check('criterion: G != 149 fails', not C.existence(good[:148])['criteria']['G_is_149'])
    # economics in the traded (T+1) regime
    yv = [0.0085] * 149
    mu0 = {3: 0.0003, 2: 0.0003, 1: 0.0003}
    vv = [a + (2 * c['s'] - 4) * mu0[c['s']] + (0.039 if k % 2 else -0.039) for k, (a, c) in enumerate(zip(yv, cal))]
    e1 = C.economic(yv, vv, cal, 0.0003)
    check('pooled mu: the mean over every era\'s out-of-window days (Revision 2, A-n1)',
          abs(C.pooled_mu(x, cal) - sum(x[i] for c in cal for i in c['out']) / sum(len(c['out']) for c in cal)) < 1e-15)
    check('bar: net = 1/2 mean(y) - 2 mu_all - cost (T+1 tilt with the pooled mu); vol = sd of v over the 25 T+1 months',
          abs(e1['net_shrunk_month'] - (0.5 * 0.0085 - 2 * 0.0003 - C.COST_MONTH)) < 1e-15 and e1['months_traded_era'] == 25)
    check('bar: 85 bp with 3.9 % T+1 vol meets 4 %/yr at min(1/2 Kelly, 1.2x)', e1['meets_bar'] and e1['size_half_kelly_capped'] <= C.L_CAP, e1)
    e2 = C.economic([0.0060] * 149, vv, cal, 0.0003)
    check('bar: 60 bp does not meet it', not e2['meets_bar'], e2)
    check('decide: labels', C.decide({'exists': False}, e1) == 'not_demonstrated' and C.decide({'exists': True}, e1) == 'exists_paper'
          and C.decide({'exists': True}, e2) == 'exists_below_bar')
    # recheck
    sg1 = [0.01] * 149
    zz = C.z_from(yv, sg1)
    zz = [a + (0.5 if k % 2 else -0.5) for k, a in enumerate(zz)]
    yy = [a * 12 ** 0.5 * 0.01 for a in zz]
    recs = [{'month': c['month'], 's': c['s'], 'y': a, 'v': a + (2 * c['s'] - 4) * mu0[c['s']], 'sigma': 0.01, 'z': b}
            for a, b, c in zip(yy, zz, cal)]
    table = {'r015_core.py': 'a' * 64, 'data/x': 'b' * 64}
    ex = C.existence(zz)
    econ = C.economic(yy, [r['v'] for r in recs], cal, 0.0003)
    row = json.loads(json.dumps({'months': recs, 'mu_by_era': {str(k): v_ for k, v_ in mu0.items()}, 'mu_all': 0.0003, 'existence': ex,
                                 'economic': econ, 'decision': C.decide(ex, econ),
                                 'code_hashes': {'r015_core.py': 'a' * 64}, 'data_hashes': {'data/x': 'b' * 64}}))
    lab, fails = C.recheck(row, table)
    check('recheck: a genuine row recomputes (with the table)', fails == [] and lab == row['decision'], fails)
    f1 = json.loads(json.dumps(row))
    for r in f1['months']:
        r['z'] -= 0.3
    check('recheck: forged z (not y / (sqrt(12) sigma)) is caught', C.recheck(f1, table)[1] != [])
    f2 = json.loads(json.dumps(row))
    f2['months'][3]['y'] += 0.01
    check('recheck: y inconsistent with v - (2s-4) mu is caught (review B m11)', C.recheck(f2, table)[1] != [])
    f3 = json.loads(json.dumps(row))
    f3['existence']['crit'] = 1.0
    check('recheck: a lowered stored critical value is caught', C.recheck(f3, table)[1] != [])
    f4 = json.loads(json.dumps(row))
    f4['data_hashes'] = {'data/x': 'c' * 64}
    check('recheck: data hashes differing from the registered table are caught (review B M1)', C.recheck(f4, table)[1] != [])
    mc = C.mechanism_contrast(eff, cal, {3: 0.0, 2: 0.0, 1: 0.0})
    check('mechanism contrast, noiseless shifted effect: T+2 months gain 2 x 37/6 bp; T+1 more',
          abs(mc['paired_t2']['mean'] - 2 * 0.0037 / 6) < 1e-9 and mc['paired_t1']['mean'] > mc['paired_t2']['mean'] > 0)
    dec = C.decomposition(eff, cal, {3: 0.0, 2: 0.0, 1: 0.0})
    check('decomposition: P = -37 bp, early + tom = +48 bp in every era',
          all(abs(dec[s]['P']['mean'] + 0.0037) < 1e-12 and abs(dec[s]['early']['mean'] + dec[s]['tom']['mean'] - 0.0048) < 1e-12 for s in '321'))
    lo = C.long_only_corrected(x, cal, mu)
    check('long-only corrected t is smaller in magnitude than the naive t (era-mu error added)', abs(lo['corrected_t']) <= abs(lo['naive_t']))
    check('recent block: months 2021-01 on (65)', C.recent(list(range(149)), cal)['n'] == 65)
    check('quarter-end exclusion keeps 100 months', C.without_quarter_ends(list(range(149)), cal)['n'] == 100)
    check('xyz cost: 2 round trips + funding on (long - short) calendar days',
          abs(C.xyz_cost_month(days, cal[0]) - (2 * C.COST_RT_XYZ + C.XYZ_FUNDING_DAY * (
              (days[cal[0]['R'][-1]] - days[cal[0]['P'][-1]]).days - (days[cal[0]['P'][-1]] - days[cal[0]['P'][0] - 1]).days))) < 1e-15)


# ------------------------------------------------------------------ loaders and the gate
def gate_test_real_us():
    """Non-vacuous (review B m5): the refusal must be the GATE's, raised before any hashing."""
    real_us = R.HERE / R.US_ZIP
    orig = R.hashlib.sha256

    def boom(*a, **k):
        raise RuntimeError('hashing reached: the gate did not refuse first')
    R.hashlib.sha256 = boom
    try:
        return raises(lambda: R.load_french(real_us, 'x' * 64), text='opens only after the real guard')
    finally:
        R.hashlib.sha256 = orig


def loader_checks():
    print('loaders: rows outside [2013-11-01, 2026-06-04) dropped before parsing, hashes, missing values, the gate')
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        junk = trading_days(dt.date(2013, 10, 1), dt.date(2013, 10, 31)) + trading_days(dt.date(2026, 6, 4), dt.date(2026, 8, 31))
        z = d / 'us.zip'
        french_zip(z, DAYS, [(0.01, 0.002)] * len(DAYS), junk)
        h = R.sha256(z)
        dd, vv = R.load_french(z, h)
        check('load_french keeps exactly [2013-11-01, 2026-06-04): non-numeric rows outside are never parsed',
              dd == DAYS and len(vv['Mkt-RF']) == len(DAYS) and abs(vv['Mkt-RF'][0] - 0.0001) < 1e-15)
        check('load_french refuses a hash mismatch', raises(lambda: R.load_french(z, '0' * 64), text='pinned SHA-256'))
        check('read_dates_only returns the same dates and never parses a value', R.read_dates_only(z, h) == DAYS)
        vals = [(0.01, 0.002)] * len(DAYS)
        vals[100] = (-99.99, 0.002)
        zbad = d / 'bad.zip'
        french_zip(zbad, DAYS, vals)
        check('load_french refuses a -99.99 value inside the span', raises(lambda: R.load_french(zbad, R.sha256(zbad)), text='missing value'))
        zdup = d / 'dup.zip'
        french_zip(zdup, DAYS + [DAYS[-1]])
        check('load_french refuses a repeated date', raises(lambda: R.load_french(zdup, R.sha256(zdup)), text='strictly increasing'))
        # a byte copy of a registered real file (simulated with a synthetic file whose hash is declared registered)
        saved = dict(R._REG_HASHES)
        R._REG_HASHES['v'] = {h}
        try:
            check('the gate: a byte copy of a registered real data file outside data/r015 is refused (review B m2)',
                  raises(lambda: R.load_french(z, h), text='opens only after the real guard'))
        finally:
            R._REG_HASHES.clear(), R._REG_HASHES.update(saved)
    check('the gate: load_french on the real US file without the guard is refused BY THE GATE, before any hashing',
          gate_test_real_us())
    orig_gate = R._gate
    R._gate = lambda *a, **k: None
    try:
        mutant = gate_test_real_us()
    finally:
        R._gate = orig_gate
    check('mutation: with the gate deleted, the gate test above FAILS (it is not vacuous; review B m5)', mutant is False)
    saved_gp = (R._GATE, R._GUARD_PASSED)
    R._GUARD_PASSED, R._GATE = (str(R._HERE0.resolve()), 'c'), None
    try:
        check('r1-m1: a passed guard alone does not open the data; the token is issued only after the started row is '
              'committed in the real ledger', raises(lambda: R._gate(R.HERE / R.US_ZIP), text='opens only after'))
    finally:
        R._GATE, R._GUARD_PASSED = saved_gp
    saved = (R._GATE, R._GUARD_FAILURES)
    R._GATE = (str(R._HERE0.resolve()), 'forged')
    R._GUARD_FAILURES = lambda *a, **k: ([], 'c', {})
    try:
        check('the gate: a stubbed guard_failures with a forged token is refused on the real tree (review B m2)',
              raises(lambda: R._gate(R.HERE / R.US_ZIP), text='opens only after'))
    finally:
        R._GATE, R._GUARD_FAILURES = saved
    saved_h = dict(R.HOOKS)
    R.HOOKS['commit'] = lambda msg, res: None
    try:
        check('a mocked commit hook on the real tree is refused', R.real_tree_failures() != [])
    finally:
        R.HOOKS.clear(), R.HOOKS.update(saved_h)
    sr = R.RESULTS
    R.RESULTS = Path(tempfile.gettempdir()) / 'r015-x' / 'results.jsonl'
    try:
        check('a mixed tree (synthetic results, real data and directory) is refused', R.real_tree_failures() != [])
        check('run() with a non-real RESULTS is refused before any row (review B M1)', raises(R.run, text='(M1)'))
    finally:
        R.RESULTS = sr
    check('default hooks, real guard, real tree together: no real-tree refusal', R.real_tree_failures() == [])
    real_res = R.REAL_RESULTS
    before = real_res.read_bytes() if real_res.exists() else None
    saved_h = dict(R.HOOKS)
    R.HOOKS['commit'] = lambda msg, res: None
    try:
        check('record() into the REAL results.jsonl with a mock hook is refused before any write (016 review B M1)',
              raises(lambda: R.record({'registration': '015-test-only', 'mode': 'x'}, 'm'), text='(M1)'))
    finally:
        R.HOOKS.clear(), R.HOOKS.update(saved_h)
    check('record() into the REAL results.jsonl without the real guard is refused too',
          raises(lambda: R.record({'registration': '015-test-only', 'mode': 'x'}, 'm'), text='(M1)'))
    check('... and the real results.jsonl is byte-unchanged', (real_res.read_bytes() if real_res.exists() else None) == before)
    check('the gate refuses any path inside the real repository (any role), e.g. the real calendar.json',
          raises(lambda: R._gate(R.REAL_REPO / 'research' / 'alpha' / R.CALENDAR), text='opens only after'))
    check('_run_in() refuses the real directory', raises(lambda: R._run_in(R.HERE, lambda: 'c'), text='(M1)'))


# ------------------------------------------------------------------ power module
def power_checks():
    print('power: worlds draw the intended structure (synthetic)')
    cal = C.build_calendar(DAYS)
    rng = random.Random(3)
    era = [3] * len(DAYS)
    yr = [d.year for d in DAYS]
    x = P.draw({'name': 'a', 'shocks': [('2020', 'aligned')]}, DAYS, cal, rng, era, yr)
    shape = P.SHOCK_2020
    j = [shape[i] + shape[i + 1] for i in range(len(shape) - 1)]
    j = j.index(min(j)) + 1
    check('aligned placement: the worst two-day down-leg ends on a P window\'s last day',
          any(x[c['P'][-1]] == shape[j] and x[c['P'][-1] - 1] == shape[j - 1] for c in cal))
    xn = P.draw({'name': 'n'}, DAYS, cal, random.Random(4), era, yr)
    for mode, key, want in (('shifted', ('P', 'R'), 0.0085), ('unshifted', ('P3', 'R3'), 0.0085)):
        xe = P.draw({'name': 'e', 'effect': 1.0, 'effect_mode': mode}, DAYS, cal, random.Random(4), era, yr)
        d = [a - b for a, b in zip(xe, xn)]
        ye = C.window_sums(d, cal, key[0], key[1], mu={3: 0.0, 2: 0.0, 1: 0.0})
        check(f'{mode} effect world adds exactly 85 bp L/S per month on its windows', all(abs(a - want) < 1e-12 for a in ye))
    xd = P.draw({'name': 'd', 'effect': 1.0, 'effect_mode': 'died2019'}, DAYS, cal, random.Random(4), era, yr)
    yd = C.window_sums([a - b for a, b in zip(xd, xn)], cal, mu={3: 0.0, 2: 0.0, 1: 0.0})
    check('died-at-publication world: 85 bp to 2019-12, zero after',
          all(abs(a - (0.0085 if c['month'] <= '2019-12' else 0.0)) < 1e-12 for a, c in zip(yd, cal)))
    recs = [{'t': t, 'pos': True, 'wo': True} for t in [0.1 * k for k in range(100)]]
    check('gate score quantile', abs(P.quantile([P.gate_score(r) for r in recs], 0.95) - 9.405) < 1e-9)
    c, q, se, k = P.calibrate({'w': [{'t': 1.5 + 0.0005 * i, 'pos': True, 'wo': True} for i in range(1000)]})
    check('calibrate: q95 + 2 MC SE, rounded up to 0.01, never below t_148',
          c >= q + 2 * se - 1e-9 and c - (q + 2 * se) < 0.01 + 1e-9 and c >= C.T148 and se > 0, (c, q, se))


# ------------------------------------------------------------------ guard in throwaway repositories (two layouts)
CODE_T, DATA_T = ('r015_core.py', 'r015_run.py'), ('data/r015/x.zip',)
REG_REL = R.REG_REL


def make_repo(top, sub):
    root = top / sub
    root.mkdir(parents=True, exist_ok=True)
    subprocess.run([GIT, 'init', '-q', '-b', 'main'], cwd=top, capture_output=True, check=True)
    for f, body in (('r015_core.py', 'x = 1\n'), ('r015_run.py', 'y = 2\n'), ('data/r015/x.zip', 'z\n'), ('results.jsonl', '')):
        (root / f).parent.mkdir(parents=True, exist_ok=True)
        (root / f).write_text(body)
    (root / '.gitignore').write_text('data/\n')
    return root


def write_reg(root, status='REGISTERED', extra=''):
    rows = '\n'.join(f'| `{f}` | `{R.sha256(root / f)}` |' for f in (*CODE_T, *DATA_T))
    (root / REG_REL).parent.mkdir(exist_ok=True)
    (root / REG_REL).write_text(f'# 015\n\n**Status: {status} 2026-10-14**\n\n| file | sha256 |\n|---|---|\n{rows}\n{extra}')


def guard_checks(sub=''):
    """Both layouts: runner files at the repository top level, and in `research/alpha` (the real layout)."""
    print(f"guard (throwaway repo, runner at {sub or 'the top level'}): (s), (a)-(e), flags, (k), (w), (f), restart")
    with tempfile.TemporaryDirectory() as top:
        top = Path(top).resolve()
        root = make_repo(top, sub)
        fails = lambda r=root, **k: R.guard_failures(r, REG_REL, CODE_T, DATA_T, real_repo=top, **k)[0]
        write_reg(root, 'DRAFT')
        g(root, 'add', '.')
        g(root, 'commit', '-q', '-m', 'draft')
        check('DRAFT -> refused (s)', any(x.startswith('(s)') for x in fails()))
        write_reg(root)
        f0 = fails()
        check('REGISTERED in the working tree but not in HEAD -> (s) (status read from HEAD) and (b)',
              any(x.startswith('(s)') for x in f0) and any(x.startswith('(b)') for x in f0), f0)
        g(root, 'commit', '-q', '-am', 'register')
        anchor = g(root, 'rev-parse', 'HEAD').stdout.strip()
        check('REGISTERED, committed, clean, hashes match, no row, main worktree, named branch -> the guard opens',
              fails(anchor=anchor) == [], fails())
        reg0 = anchor
        check('(r) the returned registration commit is the REGISTERED commit (reg0)',
              R.guard_failures(root, REG_REL, CODE_T, DATA_T, real_repo=top)[1] == reg0)
        (root / 'r015_core.py').write_text('x = 7\n')
        write_reg(root)
        g(root, 'commit', '-q', '-am', 'a revision after REGISTERED, with the table updated')
        fr = fails()
        check('(r) a committed revision of code + table after REGISTERED is refused, naming reg0 (016 review B M4)',
              any(x.startswith('(r)') and reg0[:12] in x for x in fr) and not any(x.startswith('(e)') for x in fr), fr)
        g(root, 'reset', '-q', '--hard', 'HEAD~1')
        # R1-M1: a revision on a side branch, merged with --no-ff, must not re-register
        g(root, 'checkout', '-q', '-b', 'side')
        (root / 'r015_core.py').write_text('x = 8\n')
        write_reg(root)
        g(root, 'commit', '-q', '-am', 'post-REGISTERED revision on a side branch')
        g(root, 'checkout', '-q', 'main')
        g(root, 'checkout', '-q', '-b', 'merged', anchor + '~1')
        g(root, 'merge', '-q', '--no-ff', '-m', 'merge side', 'side')
        fm = fails()
        check('(r) R1-M1: a post-REGISTERED revision merged --no-ff from a side branch is refused, naming the original '
              'REGISTERED commit', any(x.startswith('(r)') and reg0[:12] in x for x in fm), fm)
        g(root, 'checkout', '-q', 'main')
        g(root, 'branch', '-q', '-D', 'merged', 'side')
        purge_reflog(root)
        g(root, 'checkout', '-q', '-b', 'ff', anchor + '~1')
        g(root, 'merge', '-q', '--ff-only', 'main')
        check('(r) a fast-forward merge of the REGISTERED commit with no revision still opens', fails() == [], fails())
        g(root, 'checkout', '-q', 'main')
        g(root, 'branch', '-q', '-D', 'ff')
        (root / 'r015_core.py').write_text('x = 3\n')
        f4 = fails()
        check('(d) + (e) guarded code edited -> refused', any(x.startswith('(d)') for x in f4) and any(x.startswith('(e) r015_core') for x in f4))
        # review B M3: index flags and filters
        for flag in ('--skip-worktree', '--assume-unchanged'):
            g(root, 'update-index', flag, 'r015_core.py', REG_REL)
            ff = fails()
            check(f'(d) {flag} on an edited core and the registration still refuses (blob compare + flag check)',
                  any(x.startswith('(d) guarded code r015_core') for x in ff) and any('index flag' in x for x in ff), ff)
            g(root, 'update-index', flag.replace('--', '--no-'), 'r015_core.py', REG_REL)
        g(root, 'checkout', '--', 'r015_core.py')
        (root / '.gitattributes').write_text('r015_core.py filter=x\n')
        g(root, 'add', '.gitattributes')
        g(root, 'commit', '-q', '-m', 'attr')
        g(root, 'config', 'filter.x.clean', 'cat')
        check('(d) a filter attribute on guarded code refuses', any('filter attribute' in x for x in fails()))
        g(root, 'rm', '-q', '--cached', '.gitattributes')
        (root / '.gitattributes').unlink()
        g(root, 'commit', '-q', '-m', 'no attr')
        (root / DATA_T[0]).write_text('changed\n')
        check('(e) a data file changed -> refused', any(x.startswith('(e) data/r015') for x in fails()))
        (root / DATA_T[0]).write_text('z\n')
        write_reg(root, extra=f'| `r015_core.py` | `{"0" * 64}` |\n')
        g(root, 'commit', '-q', '-am', 'dup')
        check('(e) a file listed twice in the HEAD hash table -> refused', any('more than once' in x for x in fails()))
        g(root, 'reset', '-q', '--hard', 'HEAD~1')
        (root / 'results.jsonl').write_text(json.dumps({'registration': '016-y.md'}) + '\n')
        check('(k) an uncommitted foreign row -> refused', any(x.startswith('(k)') for x in fails()))
        (root / 'results.jsonl').write_text(json.dumps({'registration': '016-y.md'}))
        g(root, 'commit', '-q', '-am', 'no newline')
        check('(k) results.jsonl without a trailing newline -> refused (review B m3)', any('newline' in x for x in fails()))
        check('append_row refuses to glue a row onto a line without a newline',
              raises(lambda: R.append_row({'registration': '015-x'}, root / 'results.jsonl'), text='newline'))
        (root / 'results.jsonl').write_text('')
        g(root, 'commit', '-q', '-am', 'empty')
        # (w): the real repository only
        check('(w) a different expected repository -> refused', any(x.startswith('(w)') for x in R.guard_failures(root, REG_REL, CODE_T, DATA_T, real_repo=top / 'elsewhere')[0]))
        check('(w) an anchor that is not an ancestor -> refused', any(x.startswith('(w)') for x in fails(anchor='1' * 40)))
        with tempfile.TemporaryDirectory() as d2:
            clone = Path(d2) / 'clone'
            subprocess.run([GIT, 'clone', '-q', '--local', str(top), str(clone)], capture_output=True, check=True)
            croot = clone / sub
            (croot / 'data/r015').mkdir(parents=True, exist_ok=True)
            (croot / DATA_T[0]).write_text('z\n')
            fc = R.guard_failures(croot, REG_REL, CODE_T, DATA_T, real_repo=top)[0]
            check('(w) a clone of the real repository at another path, with the data copied in, is refused (review B B1)',
                  fc != [] and all(x.startswith('(w)') for x in fc), fc)
            shutil.rmtree(croot / 'data')
            (croot / 'data').symlink_to(root / 'data', target_is_directory=True)
            fc2 = R.guard_failures(croot, REG_REL, CODE_T, DATA_T, real_repo=top)[0]
            check('(w) a clone with the real data SYMLINKED is refused', any(x.startswith('(w)') for x in fc2), fc2)
        wt = top.parent / (top.name + '-wt')
        g(root, 'worktree', 'add', '-q', '--detach', str(wt))
        try:
            wroot = wt / sub
            (wroot / 'data').symlink_to(root / 'data', target_is_directory=True)
            fw = R.guard_failures(wroot, REG_REL, CODE_T, DATA_T, real_repo=top)[0]
            check('(w) a linked worktree of the real repository is refused (review B B1)', any('main worktree' in x for x in fw), fw)
        finally:
            g(root, 'worktree', 'remove', '--force', str(wt))
        g(root, 'checkout', '-q', '--detach')
        check('(w) a detached HEAD is refused', any('detached' in x for x in fails()))
        g(root, 'checkout', '-q', 'main')
        if sub:
            subprocess.run([GIT, 'init', '-q'], cwd=root, capture_output=True)
            check('(w) a nested `git init` in the runner directory is refused', any(x.startswith('(w)') for x in fails()))
            shutil.rmtree(root / '.git')
        # (f) run once
        add = lambda row: open(root / 'results.jsonl', 'a').write(json.dumps(row, sort_keys=True) + '\n')
        add({'registration': '015-month-end-liquidity-cycle.md', 'mode': 'result'})
        check('(f) an uncommitted row blocks', any(x.startswith('(f)') for x in fails()))
        g(root, 'commit', '-q', '-am', 'row')
        (root / 'results.jsonl').write_text('')
        g(root, 'commit', '-q', '-am', 'erase')
        check('(f) a committed then erased row is still found in history', any(x.startswith('(f)') for x in fails()))
        g(root, 'reset', '-q', '--hard', 'HEAD~2')
        check('(f) after reset --hard the row is found through the reflog', any(x.startswith('(f)') for x in fails()))
        purge(root)
        check('... history purged (throwaway repo only): the guard opens again', fails() == [], fails())
        add({'registration': '015-other-name.md', 'mode': 'result'})
        g(root, 'stash', '-q')
        check('(f) a renamed 015 registration row hidden in a stash is found', any(x.startswith('(f)') for x in fails()))
        g(root, 'stash', 'drop', '-q')
        purge(root)
        g(root, 'checkout', '-q', '--orphan', 'orph')
        g(root, 'rm', '-rq', '--cached', '.')
        (root / 'results.jsonl').write_text(json.dumps({'registration': '015-month-end-liquidity-cycle.md', 'mode': 'result'}) + '\n')
        g(root, 'add', 'results.jsonl')
        g(root, 'commit', '-q', '-m', 'orphan root row')
        g(root, 'checkout', '-q', '-f', 'main')
        g(root, 'config', 'log.showRoot', 'false')
        check('(f) a row in an orphan root commit is found even with log.showRoot=false (review B m4)', any(x.startswith('(f)') for x in fails()))
        g(root, 'config', '--unset', 'log.showRoot')
        g(root, 'branch', '-q', '-D', 'orph')
        purge(root)
        add({'registration': '015-month-end-liquidity-cycle.md', 'mode': 'result'})
        g(root, 'commit', '-q', '-am', 'row')
        rowc = g(root, 'rev-parse', 'HEAD').stdout.strip()
        g(root, 'reset', '-q', '--hard', 'HEAD~1')
        g(root, 'commit', '-q', '--allow-empty', '-m', 'decoy')
        decoy = g(root, 'rev-parse', 'HEAD').stdout.strip()
        g(root, 'reset', '-q', '--hard', 'HEAD~1')
        g(root, 'replace', rowc, decoy)
        check('(f) `git replace <row commit> <decoy>` does not hide the row (review B m4)', any(x.startswith('(f)') for x in fails()))
        g(root, 'replace', '-d', rowc)
        purge(root)
        (root / 'results.jsonl').write_text('{"registration": "015-month-end-liquidity-cycle.md", "mode": "res\n')
        check('(f) an unparseable line mentioning 015 refuses', any('unparseable' in x for x in fails()))
        (root / 'results.jsonl').write_text('')
        g(root, 'config', 'diff.external', '/bin/true')
        add({'registration': '015-month-end-liquidity-cycle.md', 'mode': 'result'})
        g(root, 'commit', '-q', '-am', 'row ext')
        (root / 'results.jsonl').write_text('')
        g(root, 'commit', '-q', '-am', 'erase ext')
        check('(f) diff.external does not blank the history scan', any(x.startswith('(f)') for x in fails()))
        g(root, 'config', '--unset', 'diff.external')
        with tempfile.TemporaryDirectory() as decoy_dir:
            subprocess.run([GIT, 'init', '-q', decoy_dir], capture_output=True)
            old = os.environ.get('GIT_DIR')
            os.environ['GIT_DIR'] = str(Path(decoy_dir) / '.git')
            try:
                check('(f) GIT_DIR pointing at a decoy repository is stripped', any(x.startswith('(f)') for x in fails()))
            finally:
                os.environ.pop('GIT_DIR', None) if old is None else os.environ.__setitem__('GIT_DIR', old)
        restart_checks(root, top, fails)


def purge_reflog(root):
    subprocess.run([GIT, 'reflog', 'expire', '--expire=now', '--all'], cwd=root, capture_output=True)
    subprocess.run([GIT, 'gc', '-q', '--prune=now'], cwd=root, capture_output=True)


def purge(root):
    for b in g(root, 'for-each-ref', '--format=%(refname)', 'refs/replace').stdout.split():
        g(root, 'update-ref', '-d', b)
    g(root, 'reset', '-q', '--hard', 'HEAD')
    subprocess.run([GIT, 'reflog', 'expire', '--expire=now', '--all'], cwd=root, capture_output=True)
    subprocess.run([GIT, 'gc', '-q', '--prune=now'], cwd=root, capture_output=True)
    # rewrite main without any 015 row: reset to the first commit that has an empty results.jsonl after 'empty'
    log = g(root, 'log', '--format=%H %s').stdout.splitlines()
    for line in log:
        h, s = line.split(' ', 1)
        if s == 'empty':
            g(root, 'reset', '-q', '--hard', h)
            break
    subprocess.run([GIT, 'reflog', 'expire', '--expire=now', '--all'], cwd=root, capture_output=True)
    subprocess.run([GIT, 'gc', '-q', '--prune=now'], cwd=root, capture_output=True)


def restart_checks(root, top, fails):
    purge(root)
    check('restart: a clean history opens', fails() == [], fails())
    started = {'registration': '015-month-end-liquidity-cycle.md', 'mode': 'started'}
    runner = lambda *a: subprocess.run([GIT, '-c', f'user.name={R.RUNNER_NAME}', '-c', f'user.email={R.RUNNER_EMAIL}', *a],
                                       cwd=root, capture_output=True, env=R.git_env())
    open(root / 'results.jsonl', 'a').write(json.dumps(started, sort_keys=True) + '\n')
    g(root, 'commit', '-q', '-am', 'started row committed by hand after the runner commit failed')
    check('restart (R1-M2): one hand-committed started row, no runner commit anywhere -> one restart allowed', fails() == [], fails())
    purge(root)
    open(root / 'results.jsonl', 'a').write(json.dumps(started, sort_keys=True) + '\n')
    runner('commit', '-q', '-am', 'runner')
    check('restart: a started row committed BY THE RUNNER -> refused', any(x.startswith('(f)') for x in fails()))
    g(root, 'reset', '-q', '--hard', 'HEAD~1')
    open(root / 'results.jsonl', 'a').write(json.dumps(started, sort_keys=True) + '\n')
    g(root, 'commit', '-q', '-am', 'the same line re-committed by hand after reset --hard')
    check('restart (R1-M2): runner commit, reset --hard, hand re-commit -> REFUSED (the runner commit is in the reflog)',
          any(x.startswith('(f)') for x in fails()), fails())
    purge(root)
    open(root / 'results.jsonl', 'a').write(json.dumps(started, sort_keys=True) + '\n')
    g(root, 'commit', '-q', '-am', 'hand')
    subprocess.run([GIT, '-c', 'user.name=someone', '-c', 'user.email=s@s', 'commit', '-q', '--amend', '-m', 'amend',
                    '--reset-author'], cwd=root, capture_output=True, env=R.git_env())
    check('restart: a hand amend of a hand commit still allows the one restart', fails() == [], fails())
    purge(root)
    open(root / 'results.jsonl', 'a').write(json.dumps(started, sort_keys=True) + '\n' + json.dumps(dict(started, run_at=1), sort_keys=True) + '\n')
    g(root, 'commit', '-q', '-am', 'hand two')
    check('restart: two started rows -> refused (one restart only)', any(x.startswith('(f)') for x in fails()))
    purge(root)


def record_checks(sub=''):
    """record() verifies the line via `HEAD:./results.jsonl`; the unfixed `HEAD:results.jsonl` fails in the
    subdirectory layout (014 review B B1): shown both ways."""
    print(f"record() and HEAD:./ paths (runner at {sub or 'the top level'})")
    with tempfile.TemporaryDirectory() as top:
        top = Path(top).resolve()
        root = top / sub
        root.mkdir(parents=True, exist_ok=True)
        subprocess.run([GIT, 'init', '-q', '-b', 'main'], cwd=top, capture_output=True, check=True)
        (root / 'results.jsonl').write_text('')
        g(root, 'add', 'results.jsonl')
        g(root, 'commit', '-q', '-m', 'init')
        saved = (R.RESULTS, R.DATA, R.revpath)
        R.RESULTS, R.DATA = root / 'results.jsonl', root / 'data' / 'r015'
        try:
            check('record(): the row is committed by the runner and verified in HEAD (fixed HEAD:./ path)',
                  not raises(lambda: R.record({'registration': '015-x.md', 'mode': 'started'}, 'row')))
            check('the runner\'s commit is authored by r015_run (the restart rule relies on it)',
                  g(root, 'log', '-1', '--format=%an').stdout.strip() == R.RUNNER_NAME)
            R.revpath = lambda rev, rel: f'{rev}:{rel}'
            unfixed = raises(lambda: R.record({'registration': '015-x.md', 'mode': 'result'}, 'row 2'))
            if sub:
                check('UNFIXED `HEAD:results.jsonl` in the subdirectory layout: record() refuses (the 014 B1 failure)', unfixed)
            else:
                check('unfixed path at the top level still works (why the bug hid in top-level tests)', not unfixed)
            R.revpath = saved[2]
            hooks = dict(R.HOOKS)
            R.HOOKS['commit'] = lambda msg, res: None
            try:
                check('record(): a no-op commit hook is refused', raises(lambda: R.record({'registration': '015-x.md', 'mode': 'x'}, 'm')))
            finally:
                R.HOOKS.clear(), R.HOOKS.update(hooks)
            (root / '.git' if not sub else top / '.git').joinpath('index.lock').write_text('')
            lock = top / '.git' / 'index.lock'
            import threading
            threading.Timer(1.5, lambda: lock.exists() and lock.unlink()).start()
            check('commit_results retries through a transient index.lock (review B m6)',
                  not raises(lambda: R.record({'registration': '015-x.md', 'mode': 'y'}, 'lock')))
        finally:
            R.RESULTS, R.DATA, R.revpath = saved


# ------------------------------------------------------------------ end to end (synthetic data, throwaway repo)
def build_synthetic_tree(root, seed=7, effect=0.0):
    cal = C.build_calendar(DAYS)
    rng = random.Random(seed)
    x = [rng.gauss(0.0, C.DAILY_SD) for _ in DAYS]
    for c in cal:
        for i in c['P']:
            x[i] += effect * C.EFFECT_P / len(c['P'])
        for i in c['R']:
            x[i] += effect * C.EFFECT_R / len(c['R'])
    junk = trading_days(dt.date(2026, 6, 4), dt.date(2026, 8, 31)) + trading_days(dt.date(2013, 9, 2), dt.date(2013, 10, 31))
    french_zip(root / R.US_ZIP, DAYS, [(100 * a, 0.01) for a in x], junk)
    dd = root / R.FRENCH_DIR
    (dd / 'SHA256SUMS').write_text(''.join(f'{R.sha256(f)}  {f.name}\n' for f in sorted(dd.iterdir()) if f.name != 'SHA256SUMS'))
    quiet(R.build_calendar_file, root)
    (root / R.POWER_LOG).parent.mkdir(parents=True, exist_ok=True)
    (root / R.POWER_LOG).write_text(json.dumps({'calibrated_crit': C.CAL_CRIT, 'calibrated_equals_registered': True,
                                                'synthetic': True}) + '\n')
    (root / 'data' / 'r015' / 'SYNTHETIC').write_text('synthetic tree written by tests/test_r015.py\n')
    return x, cal


def e2e_checks(sub='research/alpha'):
    print(f"end to end (throwaway repo, runner at {sub or 'the top level'}): started -> result -> reported; run once")
    with tempfile.TemporaryDirectory() as top:
        top = Path(top).resolve()
        root = top / sub
        root.mkdir(parents=True, exist_ok=True)
        subprocess.run([GIT, 'init', '-q', '-b', 'main'], cwd=top, capture_output=True, check=True)
        for f in R.GUARDED_CODE:
            shutil.copy(R.HERE / f, root / f)
        (root / 'results.jsonl').write_text('')
        (root / '.gitignore').write_text('data/\n')
        x, cal = build_synthetic_tree(root, effect=1.0)
        rebuilt = json.loads((root / R.CALENDAR).read_text())
        check('calendar.json from the synthetic file: 149 months, 44 / 80 / 25, dates 2013-11-01 -> 2026-06-03',
              rebuilt['summary']['by_lag'] == {'3': 44, '2': 80, '1': 25} and rebuilt['dates'][-1] == '2026-06-03'
              and rebuilt['dates'][0] == '2013-11-01')
        tq = root / R.CALENDAR
        bad = tq.with_name('cal_tampered.json')
        bad.write_text(tq.read_text().replace('"2014-01-02"', '"2014-01-03"', 1))
        check('calendar rebuild is byte-identical; a differing pinned calendar is refused (never overwritten)',
              not raises(lambda: quiet(R.build_calendar_file, root)) and raises(lambda: quiet(R.build_calendar_file, root, out=bad)))
        rows = '\n'.join(f'| `{f}` | `{R.sha256(root / f)}` |' for f in (*R.GUARDED_CODE, *R.GUARDED_DATA))
        (root / 'registrations').mkdir()
        (root / REG_REL).write_text(f'# 015\n\n**Status: REGISTERED 2026-10-14**\n\n| file | sha256 |\n|---|---|\n{rows}\n')
        g(root, 'add', '.')
        g(root, 'commit', '-q', '-m', 'registered')
        saved = (R.RESULTS, R.DATA)

        def bound_guard():
            f, c, _ = R.guard_failures(root, REG_REL, real_repo=top)
            if f:
                raise C.Refused('\n'.join(f))
            return c
        R.RESULTS, R.DATA = root / 'results.jsonl', root / 'data' / 'r015'
        try:
            row, rep = quiet(R._run_in, root, bound_guard)
            head = R.head_lines(root)
            modes = [json.loads(l)['mode'] for l in head]
            check('run: started, result and reported rows, each committed in HEAD in that order', modes == ['started', 'result', 'reported'], modes)
            st = json.loads(head[0])
            check('run: the started row carries the REGISTERED commit (reg0)',
                  st.get('registration_commit') == R.guard_failures(root, REG_REL, real_repo=top)[1])
            table = dict(R.HASH_ROW.findall(R.head_text(root, REG_REL)))
            check('run: the result recomputes from its per-month records and its hashes equal the HEAD table',
                  C.recheck(json.loads(head[1]), table)[1] == [])
            z, y, sig, mu = C.z_series(x, cal)
            check('run: z stored per month equals z recomputed from the synthetic returns', all(abs(a['z'] - b) < 1e-9 for a, b in zip(row['months'], z)))
            check('run: synthetic full effect -> the existence rule passes (pipeline sanity, synthetic)', row['existence']['exists'], row['existence'])
            check('run: the reported row links to the result row by its sha', rep['result_row_sha256'] == R.line_sha(head[1]))
            errs = [k for k, v in rep['blocks'].items() if isinstance(v, dict) and 'error' in v]
            check('run: every reported block computed; no Europe and no HMM block (review A M4)',
                  errs == [] and not any('europe' in k or 'hmm' in k for k in rep['blocks']), errs)
            check('run: last T 2026-05-29; non-numeric rows before 2013-11 and from 2026-06-04 never parsed (the run completed)',
                  row['months'][-1]['T'] == '2026-05-29')
            check('run once: a second run is refused by (f)', raises(lambda: quiet(R._run_in, root, bound_guard), text='(f)'))
        finally:
            R.RESULTS, R.DATA = saved
        # review B M1: a run bound to one tree may not compute from another
        other = top.parent / (top.name + '-other')
        try:
            n_before = len((root / 'results.jsonl').read_text().splitlines())
            R.RESULTS, R.DATA = root / 'results.jsonl', root / 'data' / 'r015'
            check('M1: _run(other tree) with this tree\'s results/data is refused before any row',
                  raises(lambda: R._run(other, bound_guard), text='(M1)')
                  and len((root / 'results.jsonl').read_text().splitlines()) == n_before)
        finally:
            R.RESULTS, R.DATA = saved


# ------------------------------------------------------------------ environment and shadow modules
def shadow_checks():
    print('environment: isolation, module origins, bytecode, shadow modules')
    with tempfile.TemporaryDirectory() as d:
        root = Path(d).resolve()
        subprocess.run([GIT, 'init', '-q', '-b', 'main'], cwd=root, capture_output=True, check=True)
        (root / 'r015_core.py').write_text('x = 1\n')
        (root / '.gitignore').write_text('csv.py\n')
        g(root, 'add', '.')
        g(root, 'commit', '-q', '-m', 'init')
        check('clean tracked files: no shadow', R.shadow_modules(root) == [])
        (root / 'json.py').write_text('')
        (root / 'random.pyc').write_bytes(b'\0')
        (root / 'csv.py').write_text('')
        (root / 'email').mkdir()
        (root / 'email' / '__init__.py').write_text('')
        (root / 'r015_hmm.cpython-314-darwin.so').write_bytes(b'\0')
        (root / 'r015_core.py').write_text('x = 2\n')
        g(root, 'update-index', '--skip-worktree', 'r015_core.py')
        found = set(R.shadow_modules(root))
        want = {'json.py (stdlib name)', 'random.pyc (stdlib name)', 'csv.py (stdlib name)', 'email/__init__.py (stdlib name)',
                'r015_hmm.cpython-314-darwin.so', 'r015_core.py'}
        check('untracked .py, sourceless .pyc, ignored .py, a package, an extension module and a modified file HIDDEN by '
              'skip-worktree are caught (blob compare)', want <= found, found)
    with tempfile.TemporaryDirectory() as d:
        root = Path(d).resolve()
        subprocess.run([GIT, 'init', '-q', '-b', 'main'], cwd=root, capture_output=True, check=True)
        (root / 'csv.py').write_text('MARK = 1\n')
        (root / 'statistics.py').write_text('MARK = 1\n')
        (root / 'r015_core.py').write_text('x = 1\n')
        g(root, 'add', '.')
        g(root, 'commit', '-q', '-m', 'committed stdlib names')
        found = set(R.shadow_modules(root))
        check('COMMITTED top-level csv.py / statistics.py are refused by name (016 review B M2.4)',
              {'csv.py (stdlib name)', 'statistics.py (stdlib name)'} <= found and 'r015_core.py' not in found, found)
        o = subprocess.run([sys.executable, '-I', '-S', '-c', f'import sys; sys.path.append({str(root)!r}); import csv; '
                            'print(hasattr(csv, "MARK"), csv.__file__)'], capture_output=True, text=True)
        check('with research/alpha APPENDED to sys.path (as r015_run does) the stdlib csv wins over a committed csv.py',
              o.stdout.startswith('False'), o.stdout)
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
    here = str(R.HERE)
    o = subprocess.run([sys.executable, '-O', '-I', '-c', f'import sys; sys.path.insert(0, {here!r}); import r015_run as R; '
                        'print(any("-O" in x for x in R.environment_failures(R.HERE)))'], capture_output=True, text=True)
    check('python -O is refused', o.stdout.strip() == 'True', o.stderr[-300:])
    o = subprocess.run([sys.executable, '-c', f'import sys; sys.path.insert(0, {here!r}); import r015_run as R; '
                        'print(any("isolated" in x for x in R.environment_failures(R.HERE)))'], capture_output=True, text=True)
    check('without -I the environment check refuses (review B M2)', o.stdout.strip() == 'True', o.stderr[-300:])
    o = subprocess.run([sys.executable, '-I', '-c', f'import sys; sys.path.append({here!r}); import r015_run as R; '
                        'print(any("no site" in x for x in R.environment_failures(R.HERE)))'], capture_output=True, text=True)
    check('-I without -S refuses (a site-packages .pth would run; 016 review B M2.2)', o.stdout.strip() == 'True', o.stderr[-300:])
    o = subprocess.run([sys.executable, '-I', '-S', '-c', f'import sys; sys.path.append({here!r}); import r015_run as R; '
                        'f = R.environment_failures(R.HERE); print(any("no site" in x or "isolated" in x for x in f))'],
                       capture_output=True, text=True)
    check('-I -S satisfies the isolation checks', o.stdout.strip() == 'False', (o.stdout, o.stderr[-300:]))
    o = subprocess.run([sys.executable, '-I', '-c', f'import sys; sys.path.append({here!r}); import r015_run as R; '
                        'print(any("site module ran" in x for x in R.environment_failures(R.HERE)))'], capture_output=True, text=True)
    check('r1-m2: when site ran (no -S) the "site module ran" check fires as well', o.stdout.strip() == 'True', o.stderr[-300:])
    o = subprocess.run([sys.executable, '-I', '-S', '-c', f'import sys; sys.path.append({here!r}); import r015_run as R; '
                        'real = sys.flags\nclass F:\n    isolated = 1\n    no_site = 1\n    def __getattr__(self, k): return getattr(real, k)\nsys.flags = F()\n'
                        'print(any("struct" in x for x in R.environment_failures(R.HERE)))'], capture_output=True, text=True)
    check('r1-m2: a look-alike sys.flags object is refused (cheap hardening; a .pth tamper is a stated residual)',
          o.stdout.strip() == 'True', o.stderr[-300:])
    o = subprocess.run([sys.executable, '-I', '-S', '-c', f'import sys; sys.path.append({here!r}); import r015_run as R; '
                        'sys.modules["r015_run"].__cached__ = None; '
                        'print(any("no __cached__" in x for x in R.module_origin_failures()))'], capture_output=True, text=True)
    check('r1-m1: a project module with __cached__ = None (tampered import state) is refused', o.stdout.strip() == 'True', o.stderr[-300:])
    orig = R.pth_files
    R.pth_files = lambda: ['/opt/x/site-packages/evil.pth']
    try:
        check('a .pth file in site-packages that is not pinned is refused', any('.pth files' in x for x in R.environment_failures(R.HERE)))
    finally:
        R.pth_files = orig
    check('this interpreter\'s site-packages hold no .pth (the pinned list is empty)', [f for f in R.pth_files() if Path(f).name not in R.PTH_PINNED] == [])
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / 'statistics.py').write_text('from fractions import Fraction\nfmean = lambda v: 0.0\n')
        o = subprocess.run([sys.executable, '-I', '-c', f'import sys; sys.path.insert(0, {d!r}); sys.path.insert(1, {here!r}); '
                            'import r015_run as R; print([x for x in R.module_origin_failures() if "statistics" in x])'],
                           capture_output=True, text=True)
        check('a forged `statistics` ahead on sys.path is named by the module-origin check (review B M2)',
              'statistics' in o.stdout and 'outside the pinned stdlib' in o.stdout, (o.stdout[-200:], o.stderr[-200:]))
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        shutil.copy(R.HERE / 'r015_core.py', d / 'r015_core.py')
        shutil.copy(R.HERE / 'r015_run.py', d / 'r015_run.py')
        import py_compile
        py_compile.compile(str(d / 'r015_core.py'), cfile=str(d / '__pycache__' / f'r015_core.{sys.implementation.cache_tag}.pyc'))
        o = subprocess.run([sys.executable, '-I', '-c', f'import sys; sys.path.insert(0, {str(d)!r}); import r015_core; import r015_run as R; '
                            'print([x for x in R.module_origin_failures() if "r015_core" in x])'], capture_output=True, text=True)
        check('r015_core imported from an existing __pycache__ BEFORE r015_run is caught by the bytecode-origin check (review B m1)',
              'pycache_prefix' in o.stdout, (o.stdout[-300:], o.stderr[-300:]))
    fp = R.env_fingerprint()
    check('environment fingerprint: version, interpreter hash, stdlib tree hash, git path and version',
          fp['python'] and fp['executable_sha256'] and fp['stdlib_sha256'] and fp['git_version'].startswith('git version'))
    check('this interpreter matches the pins (version, binary SHA-256, stdlib tree)',
          fp['python'] == R.PY_PIN and fp['executable_sha256'] == R.PY_EXE_SHA256 and fp['stdlib_sha256'] == R.STDLIB_SHA256, fp)
    check('git is called by absolute path with no system / global config and no replace objects',
          R.GIT_BIN.startswith('/') and R.git_env()['GIT_CONFIG_NOSYSTEM'] == '1' and R.git_env()['GIT_NO_REPLACE_OBJECTS'] == '1'
          and R.git_env()['GIT_CONFIG_GLOBAL'] == os.devnull)


# ------------------------------------------------------------------ real files (names, hashes, dates only)
def real_file_checks():
    print('real (or clone) files: the HEAD registration table, hashes, the dates-only calendar, the power log (no outcome)')
    text = (R.HERE / R.REG_REL).read_text()        # the working-tree registration (HEAD is checked below when equal)
    rows = R.HASH_ROW.findall(text)
    names = [f for f, _ in rows]
    table = dict(rows)
    check('every guarded code and data file is in the registration hash table exactly once',
          all(names.count(f) == 1 for f in (*R.GUARDED_CODE, *R.GUARDED_DATA)))
    bad = [f for f in (*R.GUARDED_CODE, *R.GUARDED_DATA) if not (R.HERE / f).is_file() or R.sha256(R.HERE / f) != table.get(f)]
    check('every guarded file matches its registered SHA-256', bad == [], bad)
    check('no HMM module and no Treasury file in the table (review A M4)', not any('hmm' in f or 'treasury' in f for f in names))
    synthetic = (R.HERE / 'data' / 'r015' / 'SYNTHETIC').exists()
    sums = {l.split()[1]: l.split()[0] for l in (R.HERE / R.FRENCH_DIR / 'SHA256SUMS').read_text().splitlines() if l.strip()}
    check('the US file matches its SHA256SUMS line and the registered table',
          sums.get(Path(R.US_ZIP).name) == table.get(R.US_ZIP) == R.sha256(R.HERE / R.US_ZIP))
    if not synthetic:
        check('the French US pull is byte-identical to the B1 scout\'s copy (real tree only)',
              table.get(R.US_ZIP) == '2f29e22546069914890a712680a6f81f3680ebc3543b52864484d209ca13a7db')
    pin = json.loads((R.HERE / R.CALENDAR).read_text())
    check('calendar.json: 149 months, 44 / 80 / 25, 64 Friday-T, dates 2013-11-01 -> 2026-06-03 (dates only)',
          pin['summary']['by_lag'] == {'3': 44, '2': 80, '1': 25} and pin['summary']['months'] == 149
          and pin['dates'][-1] == '2026-06-03' and pin['dates'][0] >= '2013-11-01')
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / 'calendar.json'
        quiet(R.build_calendar_file, out=out)
        check('calendar.json rebuilds byte-identically from the US file\'s date column (dates only)', out.read_text() == (R.HERE / R.CALENDAR).read_text())
    b1 = R.HERE / 'data' / 'scout_b1' / 'derived_20261013T092459Z' / 'month_end_calendar_US.csv'
    if b1.exists() and not synthetic:
        import csv
        rb = {r['month']: r for r in csv.DictReader(open(b1))}
        mism = [r['month'] for r in pin['summary']['rows'] if not (
            rb[r['month']]['T'] == r['T'] and rb[r['month']]['settle_lag'] == str(r['s'])
            and r['P'] == [rb[r['month']]['pressure_start_T-8'], rb[r['month']]['pressure_end_T-(s+1)']]
            and r['R'] == [rb[r['month']]['rebound_start_T-s'], rb[r['month']]['rebound_end_T+3']])]
        check('the calendar equals the B1 scout\'s independently built calendar for all 149 months (dates only)', mism == [], mism)
    pw = json.loads((R.HERE / R.POWER_LOG).read_text())
    check('the pinned power log was produced at the registered critical value (CAL_CRIT = calibrated)',
          pw.get('calibrated_crit') == C.CAL_CRIT and pw.get('calibrated_equals_registered') is True, (pw.get('calibrated_crit'), C.CAL_CRIT))
    f, _, _ = R.guard_failures(R.HERE, R.REG_REL, real_repo=R.REAL_REPO, anchor=R.ANCHOR)
    in_real = R.REPO.resolve() == R.REAL_REPO
    check('(w) refuses exactly when this checkout is not the real repository (real: none; clone: refused)',
          any(x.startswith('(w)') for x in f) != in_real, f)
    if R.same_as_head(R.HERE, R.REG_REL) and all(R.same_as_head(R.HERE, c_) for c_ in R.GUARDED_CODE):
        check('committed state: the guard (reading HEAD) sees no (e) hash failure', not any(x.startswith('(e)') for x in f),
              [x for x in f if x.startswith('(e)')])


def main():
    calendar_checks()
    statistic_checks()
    loader_checks()
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
