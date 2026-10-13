"""Registration 012 — HL-CEX funding carry mechanics on SYNTHETIC numbers (010's code is imported
read-only; its own suite is tests/test_carry.py).

    .venv/bin/python -m tests.test_carry012
"""
import json
import math
import random
import subprocess
import tempfile
from pathlib import Path

from ct import carry, carry012 as c12

H, D = carry.HOUR, carry.DAY
T0 = c12.STAGE1_START + 30 * D                    # 2025-02-14 00:00 UTC, on the 8-hour grid
HERE = Path(__file__).resolve().parent.parent
N_CHECKS = [0]


def check(name, ok, detail=''):
    N_CHECKS[0] += 1
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def close(a, b, tol=1e-12):
    return a is not None and b is not None and abs(a - b) <= tol


def _raises(f, exc=(AssertionError,)):
    try:
        f()
    except exc:
        return True
    return False


def series(rate_fn, interval_h, t0, t1):
    out, t = [], t0 - t0 % (interval_h * H)
    while t < t1:
        if t >= t0:
            out.append((t, rate_fn(t)))
        t += interval_h * H
    return carry.Funding(out)


def syn_rows(n_months=18, per_month=30, mu=0.001, sd=0.002, month_sd=0.0, seed=0, start=T0):
    """Synthetic position rows over `n_months` calendar months from `start`, about `per_month` entries
    a month: ret = mu + month shock + noise."""
    rng = random.Random(seed)
    rows, shocks = [], {}
    step = max(carry.GRID, (30 * D) // per_month // carry.GRID * carry.GRID)
    t, k = start, 0
    while True:
        m = carry.month_of(t)
        if m not in shocks:
            if len(shocks) == n_months:
                break
            shocks[m] = rng.gauss(0, month_sd) if month_sd else 0.0
        rows.append({'coin': f'C{k % 7}', 't_in': t, 't_out': t + 400 * H, 'week': carry.week_of(t), 'month': m,
                     'ret': mu + shocks[m] + rng.gauss(0, sd), 'hold_years': 400 / 8760, 'gross': 0.003,
                     'costs': 0.0013, 'pair': ('HL', 'BN'), 'cls': 'other', 'hold_ms': 400 * H, 'dir': 1})
        k += 1
        t += step
    return rows


def main():
    pair_filter_checks()
    stage1_checks()
    stage2_checks()
    null_calibration_checks()
    power_checks()
    fetch_checks()
    coverage_checks()
    read_lock_checks()
    owner_line_checks()
    whitelist_checks()
    panel_checks()
    e2e_checks()
    guard_checks()
    real_file_checks()
    print(f'all checks passed ({N_CHECKS[0]})')


# ------------------------------------------------------------------ the pair filter
def pair_filter_checks():
    print('pair filter: HL-BN and HL-BY only; 010 frozen terms')
    check('PAIRS_012 is exactly (HL-BN, HL-BY), a subset of 010\'s primary pairs in 010\'s order',
          c12.PAIRS_012 == (('HL', 'BN'), ('HL', 'BY')) and all(p in carry.PAIRS for p in c12.PAIRS_012))
    check('frozen terms are 010\'s: HL-BN core 21 bp / 312 h, HL-BY other 28 bp / 416 h, theta ~11.9 %/yr',
          close(carry.pair_terms(('HL', 'BN'), 'BTC')[0], 0.0021) and carry.pair_terms(('HL', 'BN'), 'BTC')[1] == 312 * H
          and close(carry.pair_terms(('HL', 'BY'), 'X')[0], 0.0028) and carry.pair_terms(('HL', 'BY'), 'X')[1] == 416 * H)
    end = T0 + 80 * D
    grid = [T0 + k * carry.GRID for k in range(80 * 3)]
    # HL 13.1 %/yr above BN, BY 30 %/yr above BN: BN-BY is the WIDEST spread; HL-BN clears theta
    fund = {('HL', 'X'): series(lambda T: 1.5e-5, 1, T0 - 10 * D, end + 30 * D),
            ('BN', 'X'): series(lambda T: 0.0, 8, T0 - 10 * D, end + 30 * D),
            ('BY', 'X'): series(lambda T: 0.30 / 1095, 8, T0 - 10 * D, end + 30 * D)}
    p12, c = c12.plan(['X'], fund, lambda *a: True, grid, T0, end)
    p10, _ = carry.plan(['X'], fund, lambda *a: True, grid, T0, end)
    check('010\'s three-pair rule takes the wider BN-BY spread (short BY, long BN)',
          p10 and p10[0]['pair'] == ('BN', 'BY') and p10[0]['short'] == 'BY')
    check('012\'s rule never opens BN-BY: among HL pairs the largest |s| wins (HL-BY: BY 30 %/yr above HL 13 %/yr '
          '-> short BY, long HL)', p12 and all(c12.is_hl_pair(p['pair']) for p in p12)
          and p12[0]['pair'] == ('HL', 'BY') and p12[0]['short'] == 'BY' and p12[0]['long'] == 'HL', str(p12[0]['pair']))
    check('... the variant "010 plan, HL subset" drops the BN-BY positions (here: everything)',
          c12.hl_subset_of(p10) == [] and set(c) == {'opened'})
    check('012\'s plan is carry.plan on PAIRS_012 (identical positions)',
          p12 == carry.plan(['X'], fund, lambda *a: True, grid, T0, end, pairs=c12.PAIRS_012)[0])
    fund2 = {k: v for k, v in fund.items() if k[0] != 'HL'}
    check('a coin with no HL leg never trades under 012', c12.plan(['X'], fund2, lambda *a: True, grid, T0, end)[0] == [])
    fund3 = {**fund, ('BY', 'X'): series(lambda T: 0.0, 8, T0 - 10 * D, end + 30 * D)}
    p3, _ = c12.plan(['X'], fund3, lambda *a: True, grid, T0, end)
    p3b = c12.hl_subset_of(carry.plan(['X'], fund3, lambda *a: True, grid, T0, end)[0])
    check('without a wider CEX-CEX spread the primary and the 010-subset variant coincide', p3 == p3b and len(p3) > 0)
    check('the eligibility callback (floor, name check, entry price) still removes an HL pair',
          c12.plan(['X'], fund3, lambda coin, pair, t: pair != ('HL', 'BN'), grid, T0, end)[0][0]['pair'] == ('HL', 'BY'))


# ------------------------------------------------------------------ Stage 1
def stage1_checks():
    print('Stage-1 SIGN gate (Revision 1): the point estimate of the mean net > 0; everything else reported')
    good = syn_rows(mu=0.001, sd=0.002, seed=1)
    ok, det = c12.stage1_gate(good)
    g = det['primary_month']['clusters']
    check('a clean positive series passes; the only criterion is mean_pos',
          ok and det['criteria'] == {'mean_pos': True})
    check('the month t and t_17(0.95) = 1.740 are REPORTED (18 clusters)', g == 18 and close(det['z_reported'], 1.740)
          and det['month_t_reported'] == det['primary_month']['t'], f"G {g} z {det['z_reported']}")
    base = syn_rows(mu=0.0, sd=0.0, seed=3)
    order = sorted({r['month'] for r in base})
    weak = [dict(r, ret=(0.003 if order.index(r['month']) % 2 == 0 else -0.0026)) for r in base]
    okw, detw = c12.stage1_gate(weak)
    check('a positive mean with month t far below 1.740 PASSES (the t is not required)', detw['primary_month']['mean'] > 0
          and detw['primary_month']['t'] < 1.0 and okw, f"t {detw['primary_month']['t']:.2f}")
    skew = [dict(r, ret=(-0.0005 if i % 3 else 0.004)) for i, r in enumerate(good)]
    oks, dets = c12.stage1_gate(skew)
    check('a positive mean with a negative median passes (the median is reported)',
          dets['mean'] > 0 and dets['median_reported'] < 0 and oks)
    tiny = [dict(r, ret=1e-7) for r in base]
    check('a mean of +1e-7 passes; a mean of exactly 0 or below fails',
          c12.stage1_gate(tiny)[0] and not c12.stage1_gate([dict(r, ret=0.0) for r in base])[0]
          and not c12.stage1_gate([dict(r, ret=-r['ret']) for r in good])[0])
    check('the 3 %/yr bar is not a Stage-1 criterion (a ~1 %/yr series passes; annualised reported)',
          c12.stage1_gate(syn_rows(mu=0.0004, sd=0.002, seed=2))[0]
          and c12.stage1_gate(syn_rows(mu=0.0004, sd=0.002, seed=2))[1]['annualised_reported'] < carry.BENCH)
    check('no positions -> no pass', not c12.stage1_gate([])[0])
    check('decide_stage1: pass -> read_stage2, else close', c12.decide_stage1(True) == 'read_stage2' and c12.decide_stage1(False) == 'close')

    print('recheck of a Stage-1 row from its numbers')
    row = json.loads(json.dumps({'stage1': {**det, 'passed': ok}, 'decision': c12.decide_stage1(ok)}, default=str))
    dec, fails = c12.recheck_stage1(row)
    check('a genuine row recomputes to read_stage2 with no failures', dec == 'read_stage2' and fails == [], str(fails))
    f1 = json.loads(json.dumps(row))
    f1['stage1']['mean'] = f1['stage1']['primary_month']['mean'] = -0.0001
    check('forged mean (<= 0) under a pass label is caught; recomputed: close',
          c12.recheck_stage1(f1)[1] != [] and c12.recheck_stage1(f1)[0] == 'close')
    f2 = json.loads(json.dumps(row))
    f2['stage1']['mean'] = 0.5
    check('a top-level mean that disagrees with the month block is caught', c12.recheck_stage1(f2)[1] != []
          and c12.recheck_stage1(f2)[0] == 'close')
    f3 = json.loads(json.dumps(row))
    f3['stage1']['primary_month']['n'] = 2
    check('fewer than 3 stored positions is caught', c12.recheck_stage1(f3)[1] != [])
    f4 = json.loads(json.dumps(row))
    f4['stage1']['criteria'] = {'mean_pos': True, 'extra': True}
    check('stored criteria that differ from the recomputed ones are caught', c12.recheck_stage1(f4)[1] != [])
    neg = [dict(r, ret=-r['ret']) for r in good]
    okn, detn = c12.stage1_gate(neg)
    bad = json.loads(json.dumps({'stage1': {**detn, 'passed': okn}, 'decision': 'read_stage2'}, default=str))
    check('a failing row labelled read_stage2 is caught (decision recomputed: close)',
          c12.recheck_stage1(bad)[0] == 'close' and c12.recheck_stage1(bad)[1] != [])
    f5 = json.loads(json.dumps(row))
    f5['stage1']['mean'] = f5['stage1']['primary_month']['mean'] = True
    check('a non-numeric mean is caught', c12.recheck_stage1(f5)[1] != [] and c12.recheck_stage1(f5)[0] == 'close')
    f6 = json.loads(json.dumps(row))
    f6['stage1']['primary_month']['t'] = -5.0
    check('the month t is not part of the recheck (a stored t of -5 changes nothing)', c12.recheck_stage1(f6) == ('read_stage2', []))


# ------------------------------------------------------------------ Stage 2
def stage2_checks():
    print('Stage-2 CONFIRM (existence test) and the extension')
    start = c12.STAGE2_START + 10 * D
    six = syn_rows(n_months=6, per_month=40, mu=0.0008, sd=0.002, seed=5, start=start)
    ok, det = c12.stage2_confirm(six, extension_registered=True)
    check('extension YES: the first look uses t_{G-1}(0.975) (6 months: t_5 = 2.571)',
          det['q'] == 0.975 and close(det['z'], 2.571), f"G {det['primary_month']['clusters']} z {det['z']}")
    ok1, det1 = c12.stage2_confirm(six, extension_registered=False)
    check('extension NO: one look at t_{G-1}(0.95) (t_5 = 2.015)', det1['q'] == 0.95 and close(det1['z'], 2.015))
    check('a clean positive series confirms below the 3 %/yr bar (the bar is reported only)',
          ok and det['annualised_reported'] < carry.BENCH, f"{det['annualised_reported']:.4f}")
    big = [dict(r, ret=-0.0003 + (0.2 if r['month'] == six[0]['month'] else 0.0)) for r in six]
    check('one huge month carries the mean -> no confirm', not c12.stage2_confirm(big, True)[0])
    check('decide_stage2: confirm', c12.decide_stage2(True, det, True, False) == 'confirm')
    mid = {'primary_month': {'t': 1.5, 'clusters': 6}, 'z': 2.571}
    check('decide_stage2: extension registered, first look 0 < t < 2.571 -> extension_due',
          c12.decide_stage2(False, mid, True, False) == 'extension_due')
    check('... not on the extension look itself, and not if unregistered -> not_demonstrated',
          c12.decide_stage2(False, mid, True, True) == 'not_demonstrated'
          and c12.decide_stage2(False, mid, False, False) == 'not_demonstrated')
    check('... t <= 0 -> not_demonstrated (never "no edge")',
          c12.decide_stage2(False, {'primary_month': {'t': -0.4}, 'z': 2.571}, True, False) == 'not_demonstrated')
    r2 = {'q': 0.975, 'stage2': {'confirm': False, 'primary_month': {'t': 1.5, 'clusters': 6}}}
    check('extension_due_from_row recomputes from the stored month t and G', c12.extension_due_from_row(r2))
    check('... refuses t >= t_5(0.975), a 0.95 look, a confirmed look, or a non-numeric t',
          not c12.extension_due_from_row({**r2, 'stage2': {'confirm': False, 'primary_month': {'t': 2.6, 'clusters': 6}}})
          and not c12.extension_due_from_row({**r2, 'q': 0.95})
          and not c12.extension_due_from_row({**r2, 'stage2': {'confirm': True, 'primary_month': {'t': 1.5, 'clusters': 6}}})
          and not c12.extension_due_from_row({**r2, 'stage2': {'confirm': False, 'primary_month': {'t': '1.5', 'clusters': 6}}}))


# ------------------------------------------------------------------ null calibration
def null_calibration_checks():
    print('null calibration of the t_{G-1} thresholds (month-level shocks, 2,000 nulls each)')
    rng = random.Random(2026)

    def null_rate(n_months, q, gate, sims=2000, per_month=12, start=T0):
        base = syn_rows(n_months=n_months, per_month=per_month, mu=0.0, sd=0.002, seed=0, start=start)
        months = sorted({r['month'] for r in base})
        hits = 0
        for _ in range(sims):
            sh = {m: rng.gauss(0, 0.001) for m in months}
            rows = [dict(r, ret=sh[r['month']] + rng.gauss(0, 0.002)) for r in base]
            hits += gate(rows, q)
        return hits / sims
    r1 = null_rate(18, None, lambda rows, q: c12.stage1_gate(rows)[0])
    check('Stage 1 is a SIGN gate: under the null it passes about half the time (0.45-0.55)', 0.45 <= r1 <= 0.55, f'{r1:.3f}')
    r2 = null_rate(6, 0.975, lambda rows, q: carry.holdout_confirm(rows, q)[0], start=c12.STAGE2_START)
    check('Stage 2, G = 6 at t_5(0.975): false confirm about 2.5 % or less (<= 0.035)', r2 <= 0.035, f'{r2:.3f}')
    r3 = null_rate(6, 0.95, lambda rows, q: carry.holdout_confirm(rows, q)[0], start=c12.STAGE2_START)
    check('Stage 2, G = 6 at t_5(0.95) (one look): false confirm about 5 % or less (<= 0.06)', r3 <= 0.06, f'{r3:.3f}')
    r4 = null_rate(4, 0.95, lambda rows, q: carry.holdout_confirm(rows, q)[0])
    check('few clusters (G = 4) at t_3(0.95) stay near nominal (<= 0.07)', r4 <= 0.07, f'{r4:.3f}')
    zero = syn_rows(n_months=18, per_month=12, mu=0.0, sd=0.002, seed=0)
    hits = 0
    for k in range(2000):                        # the whole two-look path under the null
        sh = {m: rng.gauss(0, 0.001) for m in {r['month'] for r in zero}}
        rows = [dict(r, ret=sh[r['month']] + rng.gauss(0, 0.002)) for r in zero]
        first = [r for r in rows if r['month'] in sorted(sh)[:6]]
        okf, det = carry.holdout_confirm(first, c12.Q_TWO_LOOKS)
        whole = [r for r in rows if r['month'] in sorted(sh)[:12]]
        hits += okf or (carry.extension_due(det) and carry.holdout_confirm(whole, c12.Q_TWO_LOOKS)[0])
    check('the two-look Stage-2 path (6 then 12 months, each at t_{G-1}(0.975)) under the null: <= 0.06',
          hits / 2000 <= 0.06, f'{hits / 2000:.3f}')


# ------------------------------------------------------------------ power simulation
def power_checks():
    print('power simulation (010\'s noise model, month-aligned windows)')
    pos = [dict(r) for r in syn_rows(n_months=18, per_month=20, seed=9)]
    sig_s, sig_g = {'core': 0.10, 'other': 0.10}, {'core': 0.0006, 'other': 0.001}
    wins = c12.month_windows(pos)
    check('month_windows: one common set; every first look spans exactly 6 calendar months and every extension 12',
          len(wins) >= 5 and all(len({p['month'] for p in w['first']}) == 6 and len({p['month'] for p in w['full']}) == 12
                                 and all(p['t_out'] < w['first_end'] for p in w['first']) for w in wins),
          str([(len(w['first']), len(w['full'])) for w in wins]))
    h1 = [p for p in pos if p['month'] >= sorted({q['month'] for q in pos})[-6]]
    rep = c12.replicate_half_year(h1, 181 * D)
    check('replicate_half_year: the first look is the half-year as counted; the extension adds its shifted copy with '
          'months recomputed (12 distinct months)', len(rep) == 1 and rep[0]['first'] == h1
          and len(rep[0]['full']) == 2 * len(h1) and len({p['month'] for p in rep[0]['full']}) == 12)
    big = c12.simulate_stages(pos, wins, sig_s, sig_g, 0.05, 0.25, sims=80, sigma_month=0.0)
    none = c12.simulate_stages(pos, wins, sig_s, sig_g, 0.05, 0.0, sims=300, seed=3, sigma_month=0.06)
    ext = c12.simulate_stages(pos, wins, sig_s, sig_g, 0.05, 0.25, sims=80, sigma_month=0.0, extension=True)
    check('a 25 %/yr edge passes Stage 1, Stage 2 and the joint path in most draws',
          big['p_stage1'] > 0.9 and big['p_stage2_alone'] > 0.8 and big['p_joint'] > 0.8, json.dumps(big))
    check('no edge: the Stage-1 sign gate passes about half; the joint path stays below Stage 2 alone (<= 0.06)',
          0.35 <= none['p_stage1'] <= 0.65 and none['p_joint'] <= min(0.06, none['p_stage2_alone'] + 1e-12), json.dumps(none))
    check('the extension design runs and keeps power at a large edge', ext['p_joint'] > 0.8)
    check('Stage-2 first looks hold exactly 6 month clusters', big['stage2_mean_months'] == 6.0, str(big['stage2_mean_months']))


# ------------------------------------------------------------------ fetch rules (M1)
def fetch_checks():
    import urllib.error
    import r012_fetch as F
    print('fetch: no cached errors or nulls, typed failures, forward paging, HL 500 is an error')
    old = (F._by_raw, F._hl_raw, F.SLEEP)
    F.SLEEP = lambda s: None
    try:
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            F._by_raw = lambda path, q: {'retCode': 10006, 'retMsg': 'rate limit'}
            e = _catch(lambda: F.by_kline(base, 'AVAXUSDT', '240', F.CUT - 10 * D, F.CUT))
            check('a persistent Bybit error (retCode 10006) raises a typed FetchError and caches NOTHING',
                  isinstance(e, F.FetchError) and e.kind == 'bybit' and not any(base.rglob('*.json')), str(e))
            calls = []

            def flaky(path, q):
                calls.append(1)
                return {'retCode': 10006} if len(calls) < 3 else {'retCode': 0, 'result': {'list': []}}
            F._by_raw = flaky
            check('a transient Bybit error is retried, then the answer is used', F.by_kline(base, 'X', '240', F.CUT - 2 * D, F.CUT) == []
                  and len(calls) == 3)
            F._by_raw = lambda path, q: {'retCode': 10001, 'retMsg': 'symbol invalid'}
            check('only Bybit\'s explicit "invalid symbol" (10001) is an unknown symbol (an empty answer)',
                  F.by_funding(base, 'NOPEUSDT', F.CUT - 2 * D, F.CUT) == [])
            check('_memo never caches None: a primitive returning None raises', isinstance(_catch(lambda: F._memo(base / 'n.json', lambda: None)), F.FetchError)
                  and not (base / 'n.json').exists())
            (base / 'old.json').write_text('null')
            check('a null left in the cache by an older version is ignored and re-fetched',
                  F._memo(base / 'old.json', lambda: [[1, 2]]) == [[1, 2]] and json.loads((base / 'old.json').read_text()) == [[1, 2]])
            iv = 4 * H
            delist = F.FETCH_START + 60 * D                  # a coin that stops trading 60 days into the window

            def bybit_like(path, q):
                """Bybit's real behaviour: only the last `limit` bars before `end` (within [start, end])."""
                lo_ = max(q['start'], q['end'] - q['limit'] * iv + 1)
                bars = [t for t in range(F.FETCH_START, delist, iv) if lo_ <= t <= q['end']]
                return {'retCode': 0, 'result': {'list': [[str(t), '1', '1', '1', '1', '0', '5'] for t in reversed(bars)]}}
            F._by_raw = bybit_like
            got = F.by_kline(base, 'DEADUSDT', '240', F.FETCH_START, F.CUT)
            check('forward paging recovers a coin delisted early in the window (backward paging returned nothing)',
                  len(got) == 60 * 6 and got[0][0] == F.FETCH_START and got[-1][0] == delist - iv, str(len(got)))
            n = []

            def hl500(body):
                n.append(1)
                if len(n) < 3:
                    raise urllib.error.HTTPError('u', 500, 'err', None, None)
                return [{'time': F.CUT - 5 * H, 'fundingRate': '0.0001', 'premium': '0'}]
            F._hl_raw = hl500
            check('an HL 500 is an error to retry, never "unknown coin": the answer after two 500s is used',
                  len(F.hl_funding(base, 'BBB', F.CUT - D, F.CUT)) == 1 and len(n) == 3)
            F._hl_raw = lambda body: (_ for _ in ()).throw(urllib.error.HTTPError('u', 500, 'err', None, None))
            e = _catch(lambda: F.hl_candles(base, 'ZZZ', F.CUT - D, F.CUT))
            check('persistent HL 500s raise a typed FetchError (kind hl), nothing cached', isinstance(e, F.FetchError)
                  and e.kind == 'hl' and not (base / 'hl' / 'candles').exists())
    finally:
        F._by_raw, F._hl_raw, F.SLEEP = old


def _catch(f):
    """The exception (SystemExit included) that f() raises, else None."""
    try:
        f()
    except (Exception, SystemExit) as e:      # noqa: BLE001
        return e
    return None


# ------------------------------------------------------------------ coverage (M2)
def coverage_checks():
    import r012_build as B
    import r012_fetch as F
    print('coverage checks of a Stage-2 panel (refusals vs counted venue events)')
    end, lo = c12.STAGE2_ENDS['2026-12-31'], F.STAGE2_FETCH_START
    G = carry.GRID

    def series(v, c, a, b, step=G):
        return [(v, c, t, 0.0001) for t in range(a - a % step, b, step) if t >= a]

    def px(v, c, a, b):
        return [(v, c, t, 0.0, 0.0) for t in range(a - a % G, b, G) if t >= a]
    full = lambda v, c: (series(v, c, lo, end), px(v, c, lo, end))
    uni = {'AAA': {'BN': 'AAAUSDT', 'HL': 'AAA'}}
    months = {'AAAUSDT': F.ym_range(lo, end)}
    f1, p1 = full('BN', 'AAA')
    f2, p2 = full('HL', 'AAA')
    path = [('AAA', t, 1, 1, 1) for t in range(lo, end + 1, G)]
    probs, cov = B.coverage(f1 + f2, p1 + p2, path, uni, months, end, lo)
    check('a complete panel: no refusal', probs == [], str(probs))
    m_no_dec = {'AAAUSDT': [m for m in months['AAAUSDT'] if m != '2026-12']}
    dec = int(F.datetime(2026, 12, 1, tzinfo=F.timezone.utc).timestamp() * 1000)
    probs, _ = B.coverage([r for r in f1 if r[2] < dec] + f2, [r for r in p1 if r[2] <= dec] + p2, path, uni, m_no_dec, end, lo)
    check('the December Binance file is not yet listed while November runs to its end -> refused (wait)',
          any(p[0] == 'binance_end_month_missing' for p in probs), str(probs))
    probs, _ = B.coverage([r for r in f1 if r[2] < dec] + f2, p1 + p2, path, uni, months, end, lo)
    check('Binance funding stops at the December boundary while prices run on -> month_boundary_truncation (refused)',
          any(p[0] == 'month_boundary_truncation' for p in probs), str(probs))
    probs, _ = B.coverage(f1 + f2, p1, path, uni, months, end, lo)
    check('HL funding reaches the end but HL has no prices at all -> series_without_prices (refused)',
          any(p[0] == 'series_without_prices' and p[1] == 'HL' for p in probs), str(probs))
    probs, cov = B.coverage(f1 + f2, p2, path, uni, months, end, lo, kline_months={'AAAUSDT': []})
    check('Binance funding with NO kline files listed is counted (bn_no_klines_listed), not refused: the venue has none',
          not any(p[0] == 'series_without_prices' for p in probs) and cov['bn_no_klines_listed'] == 1, str(probs))
    mid = dec + 12 * D
    probs, cov = B.coverage([r for r in f1 if r[2] < mid] + f2, p1 + p2, path, uni, months, end, lo)
    check('a mid-month stop of one series is counted (tail_mismatch_midmonth), not refused', probs == []
          and cov['tail_mismatch_midmonth'] == 1, str(cov))
    gap = [r for r in f2 if not (lo + 40 * D <= r[2] < lo + 50 * D)]
    gp = [r for r in p2 if not (lo + 40 * D <= r[2] < lo + 50 * D)]
    probs, cov = B.coverage(f1 + gap, p1 + gp, path, uni, months, end, lo)
    check('a 10-day venue suspension in funding and prices is counted, not refused', probs == []
          and cov['funding_gap'] == 1 and cov['price_gap'] == 1, str(cov))
    stop = lo + 100 * D
    probs, cov = B.coverage([r for r in f1 if r[2] < stop] + f2, [r for r in p1 if r[2] < stop] + p2,
                            [r for r in path if r[1] < stop], uni, months, end, lo)
    check('both series stopping together mid-window is a delisting (counted)', probs == [] and cov['ended_before_end'] == 1, str(cov))


# ------------------------------------------------------------------ the read lock (M6 e)
def read_lock_checks():
    import r012_build as B
    import r012_fetch as F
    import r012_run as run
    print('012 reads nothing dated on or after 2026-07-01 outside its Stage-2 gate')
    data = HERE / 'data' / 'carry012'
    check('Panel: an end past 2026-07-01 is refused without the Stage-2 flag, even with a later price span',
          _raises(lambda: run.Panel(c12.STAGE1_END + D, run.STAGE1_PANEL, price_end=c12.STAGE1_END + D)))
    check('build: a file under a Stage-2 or extension directory is refused while the limit is 2026-07-01',
          F.LIMIT == F.CUT and _raises(lambda: B.load(data / 'stage2_2026-12-31' / 'hl' / 'coins.json'))
          and _raises(lambda: B.load(data / 'extension_2027-06-30' / 'binance' / 'months.json'))
          and _raises(lambda: B.load(data / 'panel_stage2_2026-12-31' / 'universe.json')))
    check('build: assembling a Stage-2 panel without the gate is refused', _raises(lambda: B.assemble_stage2(data / 'stage2_2026-12-31', c12.STAGE2_ENDS['2026-12-31'])))
    check('build: the Stage-1 sources stay readable (dated before the cut)', not _raises(lambda: B.load(HERE / 'data' / 'carry' / 'panel' / 'universe.json')))
    import duckdb
    mx = duckdb.connect().execute(f"SELECT max(t) FROM '{run.STAGE1_PANEL}/funding.parquet'").fetchone()[0]
    check('the hashed Stage-1 panel holds no funding stamp at or after 2026-07-01', mx < F.CUT)


# ------------------------------------------------------------------ end to end (B1)
def e2e_checks():
    """The gated fetch -> build -> run path on a FAKE Stage-2 window, with fixture HTTP answers: a Binance
    listing (one symbol's December files not yet published on the first attempt), an HL 500, a Bybit
    coin with no data, a Binance symbol HL does not list. The guard itself is replaced (it is tested in
    guard_checks); everything else is the real code."""
    import hashlib
    import urllib.error
    import r012_build as B
    import r012_fetch as F
    import r012_run as run
    print('end to end: gated fetch -> build -> run on a fixture Stage-2 window (stage2 and extension)')
    G = carry.GRID
    late = {'AAAUSDT': {'2026-12'}}                     # December files not yet published at the first attempt
    n500 = {'BBB': 0}

    def rate(coin, t):
        h = int(hashlib.sha256(f'{coin}{t}'.encode()).hexdigest()[:6], 16) / 0xFFFFFF - 0.5
        return {'AAA': 2.2e-5, 'BBB': 0.0, 'CCC': 1e-6}[coin] + 4e-6 * h
    syms = ('AAAUSDT', 'BBBUSDT', 'CCCUSDT', 'DDDUSDT')

    def months_of(s, kind):
        out = F.ym_range(F.STAGE2_FETCH_START, c12.EXTENSION_END)
        return [m for m in out if m not in late.get(s, set())]

    def list_raw(prefix, delimiter=None):
        if prefix == 'data/futures/um/monthly/fundingRate/':
            return [('pre', f'{prefix}{s}/', 0) for s in syms]
        s = prefix.split('/')[5]
        if '/klines/' in prefix:
            return [('obj', f'{prefix}{s}-8h-{m}.zip', 1) for m in months_of(s, 'k')]
        return [('obj', f'{prefix}{s}-fundingRate-{m}.zip', 1) for m in months_of(s, 'f')]

    def zip_raw(url):
        parts = url.split('/')
        kind, s, ym = parts[7], parts[8], parts[-1][-11:-4]
        if ym in late.get(s, set()):
            return None, 0
        y, m = map(int, ym.split('-'))
        a = int(F.datetime(y, m, 1, tzinfo=F.timezone.utc).timestamp() * 1000)
        b = int(F.datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=F.timezone.utc).timestamp() * 1000)
        coin = s[:-4]
        if kind == 'fundingRate':
            return [['calc_time', 'funding_interval_hours', 'last_funding_rate']] + \
                [[str(t), '8', '0.0'] for t in range(a, b, G)], 1
        return [['open_time']] + [[str(t), '1', '1.001', '0.999', '1', '1', str(t + G - 1), '1e9', '1', '1', '1', '0']
                                  for t in range(a, b, G)], 1

    def by_raw(path, q):
        s = q['symbol']
        if s not in ('AAAUSDT', 'BBBUSDT', 'CCCUSDT'):
            return {'retCode': 10001, 'retMsg': 'symbol invalid'}
        if s == 'CCCUSDT':
            return {'retCode': 0, 'result': {'list': []}}      # an empty Bybit coin
        if path == 'funding/history':
            ts = [t for t in range(q['startTime'] - q['startTime'] % G, q['endTime'] + 1, G) if t >= q['startTime']][-q['limit']:]
            return {'retCode': 0, 'result': {'list': [{'fundingRateTimestamp': str(t), 'fundingRate': '0.0'} for t in reversed(ts)]}}
        iv = F.IV_MS[q['interval']]
        lo_ = max(q['start'], q['end'] - q['limit'] * iv + 1)
        ts = [t for t in range(lo_ - lo_ % iv + (iv if lo_ % iv else 0), q['end'] + 1, iv)]
        return {'retCode': 0, 'result': {'list': [[str(t), '1', '1.001', '0.999', '1', '1', '1e9'] for t in reversed(ts)]}}

    def hl_raw(body):
        if body['type'] == 'meta':
            return {'universe': [{'name': 'AAA'}, {'name': 'BBB'}, {'name': 'CCC'}]}
        if body['type'] == 'fundingHistory':
            c = body['coin']
            if c == 'BBB' and n500['BBB'] < 1:
                n500['BBB'] += 1
                raise urllib.error.HTTPError('u', 500, 'err', None, None)
            a = body['startTime'] - body['startTime'] % H + (H if body['startTime'] % H else 0)
            return [{'time': t, 'fundingRate': str(rate(c, t)), 'premium': '0.0001'}
                    for t in range(a, body['endTime'] + 1, H)][:500]
        r = body['req']
        a = r['startTime'] - r['startTime'] % G + (G if r['startTime'] % G else 0)
        return [{'t': t, 'o': '1', 'h': '1.001', 'l': '0.999', 'c': '1', 'v': '1e8'} for t in range(a, r['endTime'] + 1, G)][:5000]

    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        data = root / 'data'
        c012 = data / 'carry012'
        (data / 'carry').mkdir(parents=True)
        reg = root / 'reg.md'
        reg.write_text('x\n\nStage-2 end (owner): 2026-12-31\n\nStage-2 extension (owner): YES\n')
        (root / 'results.jsonl').write_text(json.dumps({'registration': 'reg.md', 'mode': 'stage1'}) + '\n')
        saved_run = {k: getattr(run, k) for k in ('RESULTS', 'HERE', 'REG', 'CARRY012', 'ALLOWED', 'EXTENSION_DIR', 'guard_failures')}
        saved_f = {k: getattr(F, k) for k in ('OUT', 'ROOT', 'REG', 'SRC010', 'SLEEP', 'LIMIT', '_by_raw', '_hl_raw', '_zip_raw', '_list_raw')}
        try:
            run.RESULTS, run.HERE, run.REG, run.CARRY012 = root / 'results.jsonl', root, reg, c012
            run.ALLOWED, run.EXTENSION_DIR = (data / 'carry', c012), c012 / 'panel_extension_2027-06-30'
            run.guard_failures = lambda *a, **k: ([], 'c' * 40)
            F.OUT, F.ROOT, F.REG, F.SRC010, F.SLEEP = c012, root, reg, data / 'carry', (lambda s: None)
            F._by_raw, F._hl_raw, F._zip_raw, F._list_raw = by_raw, hl_raw, zip_raw, list_raw
            rows = lambda: [json.loads(l) for l in (root / 'results.jsonl').read_text().splitlines()]
            e = _catch(lambda: F.stage2('stage2'))
            check('attempt 1: the December Binance files are missing -> a clean stop (SystemExit naming the coverage '
                  'refusal), no stage2_fetched row, nothing crashes', isinstance(e, SystemExit) and 'coverage' in str(e)
                  and not any(r['mode'] == 'stage2_fetched' for r in rows())
                  and (c012 / 'stage2_2026-12-31' / 'coverage_failures.json').exists(), str(e)[:200])
            check('... the listing was SAVED (B1) and the HL 500 was retried, not read as "unknown coin"',
                  (c012 / 'stage2_2026-12-31' / 'binance' / 'months.json').exists() and n500['BBB'] == 1
                  and json.loads((c012 / 'stage2_2026-12-31' / 'hl' / 'coins.json').read_text()).get('BBBUSDT') == 'BBB')
            late.clear()
            F.LIMIT = F.CUT
            e = _catch(lambda: F.stage2('stage2'))
            fetched = [r for r in rows() if r['mode'] == 'stage2_fetched']
            check('attempt 2 (December published): the fetch resumes and completes with one stage2_fetched row and one '
                  'stage2_fetch_started row', e is None and len(fetched) == 1
                  and sum(r['mode'] == 'stage2_fetch_started' for r in rows()) == 1, str(e)[:200])
            check('... the report counts the Bybit-empty coin and the Binance symbol HL does not list',
                  fetched[0]['report']['bybit_unknown'] == 1 and fetched[0]['report']['hl_not_in_meta'] == 1, str(fetched[0]['report']))
            F.LIMIT = F.CUT
            e = _catch(lambda: B.stage2('stage2'))
            built = [r for r in rows() if r['mode'] == 'stage2_built']
            uni = json.loads((c012 / 'panel_stage2_2026-12-31' / 'universe.json').read_text())
            check('build: one stage2_built row with the panel hashes; the universe holds AAA (HL, BN, BY), BBB and CCC '
                  '(no Bybit leg), not DDD', e is None and len(built) == 1 and set(uni) == {'AAA', 'BBB', 'CCC'}
                  and 'BY' in uni['AAA'] and 'BY' not in uni['CCC'], str(e)[:200])
            F.LIMIT = F.CUT
            e = _catch(lambda: run.run_stage2(False))
            s2 = [r for r in rows() if r['mode'] == 'stage2']
            check('run: one stage2 row with a decision (confirm / extension_due / not_demonstrated), q 0.975, positions '
                  'opened on AAA only', e is None and len(s2) == 1
                  and s2[0]['decision'] in ('confirm', 'extension_due', 'not_demonstrated') and s2[0]['q'] == 0.975
                  and s2[0]['counts']['opened'] > 5 and s2[0]['positions']['coins'] == 1,
                  f"{str(e)[:200]} {s2[0]['decision'] if s2 else None} {s2[0]['counts'] if s2 else None}")
            F.LIMIT = F.CUT
            e1 = _catch(lambda: F.stage2('extension'))
            F.LIMIT = F.CUT
            e2 = _catch(lambda: B.stage2('extension'))
            F.LIMIT = F.CUT
            e3 = _catch(lambda: run.run_stage2(True))
            ex = [r for r in rows() if r['mode'] == 'extension']
            check('extension: fetch -> build -> run completes the same way (cumulative from 2026-07-01 to 2027-06-30)',
                  e1 is None and e2 is None and e3 is None and len(ex) == 1
                  and ex[0]['decision'] in ('confirm', 'not_demonstrated'),
                  f'{str(e1)[:120]} {str(e2)[:120]} {str(e3)[:120]}')
            check('the run refuses a panel whose files changed after the build (hash check)', True if (
                (c012 / 'panel_stage2_2026-12-31' / 'levels.parquet').write_bytes(b'x') or True) and
                isinstance(_catch(lambda: run.run_stage2(False)), SystemExit) else False)
        finally:
            for k, v in saved_run.items():
                setattr(run, k, v)
            for k, v in saved_f.items():
                setattr(F, k, v)
            F.LIMIT = F.CUT




# ------------------------------------------------------------------ owner lines
def owner_line_checks():
    import r012_run as run
    print('owner lines (anchored, exactly one each)')
    base = 'x\n\nStage-2 end (owner): {e}\n\nStage-2 extension (owner): {x}\n'
    ok = run.owner_choice(base.format(e='2026-12-31', x='YES'))
    check('2026-12-31 and YES -> end exclusive 2027-01-01, extension on', ok == (1_798_761_600_000, '2026-12-31', True, []))
    check('NO is accepted', run.owner_choice(base.format(e='2026-12-31', x='NO'))[2] is False)
    check('UNSET refuses (both lines)', len(run.owner_choice(base.format(e='UNSET', x='UNSET'))[3]) == 2)
    check('a date outside the offer refuses (2026-09-30 is not offered for 012)',
          run.owner_choice(base.format(e='2026-09-30', x='YES'))[3] != [])
    check('two end lines refuse', run.owner_choice(base.format(e='2026-12-31', x='YES') + 'Stage-2 end (owner): 2026-12-31\n')[3] != [])
    check('a quoted or indented line does not count',
          run.owner_choice('see `Stage-2 end (owner): 2026-12-31`\n  Stage-2 end (owner): 2026-12-31\n'
                           'Stage-2 extension (owner): YES\n')[0] is None)
    check("010's owner lines do not satisfy 012's parser",
          run.owner_choice('Holdout end (owner): 2026-12-31\n\nHoldout extension (owner): YES\n')[0] is None)
    sha = 'a' * 64
    check('approval regexes are anchored and distinct',
          run.APPROVAL.findall(f'Stage-2 fetch: APPROVED 2027-01-08 row {sha}\n') == [('2027-01-08', sha)]
          and run.APPROVAL.findall(f'`Stage-2 fetch: APPROVED 2027-01-08 row {sha}`\n') == []
          and run.EXT_APPROVAL.findall(f'Stage-2 fetch: APPROVED 2027-01-08 row {sha}\n') == []
          and run.APPROVAL.findall(f'Stage-2 extension fetch: APPROVED 2027-07-09 row {sha}\n') == [])


# ------------------------------------------------------------------ whitelist
def whitelist_checks():
    import r012_fetch as F
    import r012_run as run
    print('whitelist: data/carry/ and data/carry012/ only')
    data = HERE / 'data'
    check('data/carry/ and data/carry012/ (and below) are allowed',
          run.allowed(data / 'carry' / 'panel') and run.allowed(data / 'carry012' / 'panel_stage1') and run.allowed(data / 'carry012'))
    for bad in ('binance_years', 'binance_years_holdoutA', 'scout_l2', 'binance', 'cache', 'cache/hl-funding', 'derived',
                'derived/hyperliquid', 'vaults', 'carry_x', 'carry0'):
        check(f'data/{bad}/ is refused', not run.allowed(data / bad) and _raises(lambda b=bad: run.assert_allowed(data / b)))
    check('a traversal out of the whitelist is refused', not run.allowed(data / 'carry012' / '..' / 'binance_years'))
    check('the fetcher writes only under data/carry012/ and reads 010 inputs under data/carry/',
          F.OUT.resolve() == (data / 'carry012').resolve() and F.SRC010.resolve() == (data / 'carry').resolve())
    src = {f: (HERE / f).read_text() for f in ('ct/carry012.py', 'r012_run.py', 'r012_fetch.py', 'r012_build.py', 'r012_describe.py')}
    check('no 012 source names binance_years, scout_l2, data/binance, data/cache, hl-funding, derived or the HL snapshots '
          '(D18 a: the HL holdout data on disk is never readable by 012)',
          all('binance_years' not in t and 'scout_l2' not in t and "'data' / 'binance'" not in t and "'cache'" not in t
              and 'hl-funding' not in t and "'derived'" not in t and 'snapshot_accounts' not in t for t in src.values()))
    check('before the gate the fetch limit is 2026-07-01, and every primitive refuses a later end before any request',
          F.LIMIT == F.CUT == carry.EXPOSED_END
          and _raises(lambda: F.by_kline(data / 'carry012' / 'never', 'X', '240', F.CUT - D, F.CUT + D))
          and _raises(lambda: F.by_funding(data / 'carry012' / 'never', 'X', F.CUT - D, F.CUT + D))
          and _raises(lambda: F.hl_funding(data / 'carry012' / 'never', 'X', F.CUT - D, F.CUT + 1))
          and _raises(lambda: F.hl_candles(data / 'carry012' / 'never', 'X', F.CUT - D, F.CUT + 1))
          and _raises(lambda: F.bn_file(data / 'carry012' / 'never', 'fundingRate', 'XUSDT', '2026-07'))
          and not (data / 'carry012' / 'never').exists())
    check('the fetcher\'s Stage-2 gate refuses on the real registration (DRAFT) and leaves the limit at 2026-07-01',
          _raises(lambda: F.stage2_gate('stage2', 'fetch'), (SystemExit,)) and F.LIMIT == F.CUT)
    F.drop([[F.CUT - 1], [F.CUT]], lambda r: r[0])
    check('rows at or after the limit are dropped and counted', F.EXPOSURE['dropped_rows'] >= 1)
    F.EXPOSURE['dropped_rows'] = 0
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / 'x.json'
        old = F.LIMIT
        F.LIMIT = F.CUT + D
        try:
            F._memo(p, lambda: [])
            cached_after = p.exists()
        finally:
            F.LIMIT = old
        F._memo(p, lambda: [])
        cached_before = p.exists()
    check('an empty answer is never cached once the limit is past 2026-07-01 (it is cached before)',
          not cached_after and cached_before)


# ------------------------------------------------------------------ panel
def panel_checks():
    import pyarrow as pa
    import pyarrow.parquet as pq
    import r012_run as run
    print('Panel (synthetic parquet, inherited 010 methods)')
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / 'carry012' / 'panel_stage1'
        root.mkdir(parents=True)
        t0 = T0

        def write(name, cols, rows):
            pq.write_table(pa.table([pa.array([r[i] for r in rows]) for i in range(len(cols))], names=cols),
                           root / f'{name}.parquet')
        (root / 'universe.json').write_text(json.dumps({'BTC': {'BN': 'BTCUSDT', 'BY': 'BTCUSDT', 'HL': 'BTC'},
                                                        'NOHL': {'BN': 'NOHLUSDT', 'BY': 'NOHLUSDT'}}))
        fund = [(v, c, t0 + k * 8 * H, 0.0001) for v, c in (('BN', 'BTC'), ('BY', 'BTC'), ('HL', 'BTC'),
                                                          ('BN', 'NOHL'), ('BY', 'NOHL')) for k in range(-60, 80)]
        write('funding', ['venue', 'coin', 't', 'rate'], fund)
        write('prices', ['venue', 'coin', 't', 'lp', 'lpo'],
              [('BN', 'BTC', t0, math.log(100.0), math.log(100.5)), ('HL', 'BTC', t0, math.log(100.1), math.log(100.6))])
        write('path', ['coin', 't', 'high', 'low', 'close'], [('BTC', t0, 100.0, 100.0, 100.0)])
        days = [t0 + k * D for k in range(-40, 30)]
        write('volume', ['venue', 'coin', 'day', 'qvol'], [(v, 'BTC', dd, q) for dd in days
                                                           for v, q in (('BN', 2e9), ('BY', 3e8), ('HL', 6e7))])
        write('levels', ['venue', 'coin', 't', 'close'], [(v, 'BTC', dd, 100.0) for dd in days for v in ('BN', 'BY', 'HL')])
        roots = (Path(d) / 'carry012',)
        old = run.ALLOWED
        run.ALLOWED = roots
        try:
            pnl = run.Panel(c12.STAGE1_END, root, price_end=c12.STAGE1_END)
            check('only coins with an HL leg are in the 012 panel', pnl.coins == ['BTC'], str(pnl.coins))
            check('inherited 010 tiers per leg (BN 1, BY 2, HL 4 bp), floor, name check and the post-settlement fill',
                  (pnl.slip('BTC', 'BN', t0), pnl.slip('BTC', 'BY', t0), pnl.slip('BTC', 'HL', t0)) == (0.0001, 0.0002, 0.0004)
                  and pnl.liq_ok('BTC', 'HL', t0) and pnl.name_ok('HL', 'BTC', t0)
                  and close(pnl.fill('BTC', 'HL', 'BN', t0), math.log(100.6 / 100.5)))
            check('prices past the panel\'s price span are refused',
                  _raises(lambda: run.Panel(c12.STAGE1_END + D, root, price_end=c12.STAGE1_END)))
            s2 = Path(d) / 'carry012' / 'panel_stage2_2026-12-31'
            s2.mkdir()
            for f in root.iterdir():
                (s2 / f.name).write_bytes(f.read_bytes())
            check('counting code never opens a Stage-2 panel; the guarded run may (allow_stage2)',
                  _raises(lambda: run.Panel(c12.STAGE1_END, s2, price_end=c12.STAGE1_END))
                  and not _raises(lambda: run.Panel(c12.STAGE1_END, s2, price_end=c12.STAGE1_END, allow_stage2=True)))
            check('a panel outside the whitelist is refused', _raises(lambda: run.Panel(c12.STAGE1_END, Path(d), price_end=c12.STAGE1_END)))
        finally:
            run.ALLOWED = old


# ------------------------------------------------------------------ guard (throwaway repo)
def _row009(commit):
    from ct import factors as fx
    ev = {'ic_mean': -0.01, 'ic_t': -1.0, 'net_mean': -0.001, 'net_t': -1.0, 'net_median': -0.001,
          'halves': [-1.0, -1.0], 'without_largest_positive_month': -0.001,
          'criteria': {'1_ic_mean_pos_nw_t': False, '2_book_net_pos_nw_t': False, '3_mean_median_same_sign': True,
                       '4_both_halves_positive': False, '5_without_largest_positive_month_positive': False},
          'pass': False, 'pull_conditions': {'all': False}}
    return {'registration': '009-binance-weekly-factors.md', 'mode': 'discovery', 'registration_commit': commit,
            'results': {f: ev for f in fx.FAMILY}, 'decision': ['close', []]}


OWNER = {'end_date': '2026-12-31', 'extension': True}


def _row012(code_hashes, data_hashes, commit, good=True, owner=OWNER, mean=None):
    rows = syn_rows(mu=(0.001 if good else -0.0005), sd=0.002, seed=21)
    ok, det = c12.stage1_gate(rows)
    row = {'registration': '012-x.md', 'mode': 'stage1', 'registration_commit': commit, 'owner': owner,
           'code_hashes': code_hashes, 'data_hashes': data_hashes, 'stage1': {**det, 'passed': ok},
           'decision': c12.decide_stage1(ok)}
    row = json.loads(json.dumps(row, default=str))
    if mean is not None:                                   # a forged mean under the stored labels
        row['stage1']['mean'] = row['stage1']['primary_month']['mean'] = mean
    return row


def guard_checks():
    import r012_run as run
    code, data = ('ct/carry012.py', 'r012_run.py'), ('data/c.json',)
    reg_rel = 'registrations/012-x.md'
    print('guard (throwaway git repo): status, (a)-(f), (h), the Stage-2 gate, the owner freeze, single fetch/build')
    print("  (010's (h) bypass probes — off-branch, foreign commit, mislabelled, wrong sha, deleted — are in "
          "tests/test_carry.py; r010_run.py is hash-locked in 012's §8)")
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root,
                                        capture_output=True, text=True, check=True)
        head = lambda: git('rev-parse', 'HEAD').stdout.strip()
        git('init', '-q', '-b', 'main')
        for f, body in (('ct/carry012.py', 'x = 1\n'), ('r012_run.py', 'y = 2\n'), ('data/c.json', '{}\n'),
                        ('results.jsonl', json.dumps({'registration': '005-y.md'}) + '\n')):
            (root / f).parent.mkdir(parents=True, exist_ok=True)
            (root / f).write_text(body)
        (root / '.gitignore').write_text('data/\n')
        owner = '\nStage-2 end (owner): 2026-12-31\n\nStage-2 extension (owner): {x}\n'

        def write_reg(status='REGISTERED', x='YES', extra=''):
            hashes = {f: run.sha256(root / f) for f in (*code, *data)}
            rows = '\n'.join(f'| `{f}` | `{h}` |' for f, h in hashes.items())
            (root / reg_rel).parent.mkdir(exist_ok=True)
            (root / reg_rel).write_text(f'# 012\n\n**Status: {status} 2026-10-12**\n\n| file | sha256 |\n|---|---|\n{rows}\n'
                                        + owner.format(x=x) + extra)
        fails = lambda mode='stage1', now=None: run.guard_failures(root, reg_rel, code, data, mode=mode, now_ms=now)[0]
        add = lambda row: open(root / 'results.jsonl', 'a').write(json.dumps(row) + '\n')
        lines = lambda: (root / 'results.jsonl').read_text().splitlines()
        write_reg()
        git('add', '.gitignore', 'ct', 'r012_run.py', 'results.jsonl')
        git('commit', '-q', '-m', 'code')
        f0 = fails()
        check('(a)+(c): an untracked registration is refused', any(x.startswith('(a)') for x in f0) and any(x.startswith('(c)') for x in f0))
        git('add', reg_rel)
        git('commit', '-q', '-m', 'register')
        reg_c = head()
        f1 = fails()
        check('(h): committed and clean, but no verified 009 terminal state -> only the ordering refuses',
              f1 and all(x.startswith('(h)') for x in f1), str(f1))
        add(_row009(reg_c))
        check('(h) probe: an UNCOMMITTED 009 close unlocks nothing', any(x.startswith('(h)') for x in fails()))
        git('commit', '-q', '-am', '009 closed')
        check('(h): a committed, verified 009 close unlocks; Stage 1 may run', fails() == [], str(fails()))
        write_reg(x='UNSET')
        check('(n1): Stage 1 itself refuses until the owner lines are set', any(x.startswith('(n1)') for x in fails()))
        write_reg()
        (root / reg_rel).write_text((root / reg_rel).read_text() + 'edit\n')
        check('(b): registration modified after commit -> refused', any(x.startswith('(b)') for x in fails()))
        write_reg()
        (root / 'ct/carry012.py').write_text('x = 3\n')
        f4 = fails()
        check('(d)+(e): guarded code edited -> not clean and hash mismatch',
              any(x.startswith('(d)') for x in f4) and any(x.startswith('(e) ct/carry012.py') for x in f4))
        git('checkout', '--', 'ct/carry012.py')
        write_reg()
        (root / 'data/c.json').write_text('{"changed": 1}\n')
        check('(e): a data file changed -> hash mismatch', any(x == '(e) data/c.json does not match its registered SHA-256' for x in fails()))
        (root / 'data/c.json').write_text('{}\n')
        write_reg()
        add({'registration': '012-x.md', 'mode': 'stage1_started'})
        check('(f): any 012 row (a started row) blocks a second Stage-1 run', any(x.startswith('(f)') for x in fails()))
        git('commit', '-q', '-am', 'started row')
        (root / 'results.jsonl').write_text('\n'.join(l for l in lines() if '012-x' not in l) + '\n')
        git('commit', '-q', '-am', 'started row deleted')
        check('(f): a deleted 012 row is still found in history -> refused', any(x.startswith('(f)') for x in fails()))
        git('checkout', '-q', '-b', 'side')
        add({'registration': '012-x.md', 'mode': 'stage1'})
        git('commit', '-q', '-am', 'row on a side branch')
        git('checkout', '-q', 'main')
        check('(f): a 012 row on another branch blocks too', any(x.startswith('(f)') for x in fails()))

        def purge(n):
            git('branch', '-q', '-D', 'side') if 'side' in git('branch').stdout else None
            git('reset', '-q', '--hard', f'HEAD~{n}')
            subprocess.run(['git', 'reflog', 'expire', '--expire=now', '--all'], cwd=root, capture_output=True)
            subprocess.run(['git', 'gc', '-q', '--prune=now'], cwd=root, capture_output=True)
        purge(2)
        check('... history cleaned: Stage 1 may run again (throwaway repo only)', fails() == [], str(fails()))

        # ---- Stage 1 done: rows, then the Stage-2 gate
        late = c12.STAGE2_ENDS['2026-12-31'] + 30 * D
        g_ = lambda mode, now=late: fails(mode, now)
        reg_c2 = head()
        code_h = {f: run.sha256(root / f) for f in code}
        data_h = {f: run.sha256(root / f) for f in data}

        def stage1_rows(**kw):
            add({'registration': '012-x.md', 'mode': 'stage1_started', 'registration_commit': kw.pop('started_commit', reg_c2)})
            add(_row012(code_h, data_h, reg_c2, **kw))
            return lines()[-1]
        add(_row012(code_h, data_h, reg_c2, good=False) | {})
        git('commit', '-q', '-am', 'failing stage1 row without started row')
        gf = g_('stage2_fetch')
        check('(s) no stage1_started row, and (g4) a genuine Stage-1 FAIL -> refused',
              any(x.startswith('(s)') for x in gf) and any('not read_stage2' in x for x in gf), str(gf))
        purge(1)
        row_line = stage1_rows()
        check('(g2) an uncommitted Stage-1 row -> refused', any(x.startswith('(g2)') for x in g_('stage2_fetch')))
        git('commit', '-q', '-am', 'stage1 rows')
        gh = g_('stage2_fetch')
        check("(h') no approval line -> refused; (s), (g2)-(g4), (o), (r) satisfied",
              any(x.startswith("(h')") for x in gh) and not any(x[:3] in ('(g2', '(g3', '(g4', '(s)', '(o)', '(r)') for x in gh), str(gh))
        sha = run.line_sha(row_line)
        write_reg(extra=f'\nStage-2 fetch: APPROVED 2027-01-08 row {"b" * 64}\n')
        git('commit', '-q', '-am', 'approval, wrong sha')
        check("(h') an approval quoting another row's sha -> refused", any(x.startswith("(h')") for x in g_('stage2_fetch')))
        appr = f'\nStage-2 fetch: APPROVED 2027-01-08 row {sha}\n'
        write_reg(extra=appr)
        git('commit', '-q', '-am', 'approved')
        approved = head()
        check('approval committed after the row -> the fetch gate is open (late enough)', g_('stage2_fetch') == [], str(g_('stage2_fetch')))
        check('(t) before end + 7 days -> refused',
              any(x.startswith('(t)') for x in g_('stage2_fetch', c12.STAGE2_ENDS['2026-12-31'] + 6 * D)))

        # ---- M3: the owner lines are frozen at Stage 1
        write_reg(x='NO', extra=appr)
        git('commit', '-q', '-am', 'extension flipped to NO after Stage 1')
        gx = g_('stage2_fetch')
        check('M3: flipping the extension YES -> NO after Stage 1 is refused ((o) owner lines, (r) registration changed)',
              any(x.startswith('(o)') for x in gx) and any(x.startswith('(r)') for x in gx), str(gx))
        purge(1)
        write_reg(extra=appr + '\nSome new prose added after Stage 1.\n')
        git('commit', '-q', '-am', 'prose after Stage 1')
        check('(r): any registration change after Stage 1 other than an approval line is refused',
              any(x.startswith('(r)') for x in g_('stage2_fetch')))
        purge(1)

        # ---- M3: one fetch, one build, committed in order
        add({'registration': '012-x.md', 'mode': 'stage2_fetch_started', 'owner': OWNER})
        check('(i) building before the fetch completed -> refused', any('stage2_fetched' in x for x in g_('stage2_build')))
        add({'registration': '012-x.md', 'mode': 'stage2_fetched', 'owner': OWNER})
        check('(i) building with UNCOMMITTED fetch rows -> refused', any('not committed' in x for x in g_('stage2_build')))
        check('(i) a second fetch after the fetch completed -> refused', any('second fetch' in x for x in g_('stage2_fetch')))
        git('commit', '-q', '-am', 'fetch rows')
        check('fetch rows committed -> the build gate is open', g_('stage2_build') == [], str(g_('stage2_build')))
        add({'registration': '012-x.md', 'mode': 'stage2_built', 'panel_sha256': {}, 'owner': OWNER})
        check('(i) the run with an UNCOMMITTED built row -> refused', any('not committed' in x for x in g_('stage2')))
        git('commit', '-q', '-am', 'built row')
        check('built row committed -> the run gate is open', g_('stage2') == [], str(g_('stage2')))
        check('(i) a second build -> refused', any('second build' in x for x in g_('stage2_build')))
        add({'registration': '012-x.md', 'mode': 'stage2_fetch_started', 'owner': OWNER, 'again': 1})
        check('(i) a second stage2_fetch_started row -> refused', any('exactly one is allowed' in x for x in g_('stage2')))
        git('checkout', '--', 'results.jsonl')
        add({'registration': '012-x.md', 'mode': 'stage2_built', 'panel_sha256': {'x': 'y'}, 'owner': OWNER})
        check('(i) a repeated build row -> refused', any('exactly one is allowed' in x for x in g_('stage2')))
        git('checkout', '--', 'results.jsonl')
        add({'registration': '012-x.md', 'mode': 'stage2_started'})
        check('(i) a started Stage 2 is never read again', any(x.startswith('(i)') for x in g_('stage2')))
        git('checkout', '--', 'results.jsonl')
        check('extension: no Stage-2 row yet -> refused', any(x.startswith('(x)') for x in g_('extension_fetch', c12.EXTENSION_END + 8 * D)))
        add({'registration': '012-x.md', 'mode': 'stage2', 'q': 0.975, 'owner': OWNER,
             'stage2': {'confirm': False, 'primary_month': {'t': 1.4, 'clusters': 6}}})
        s2_line = lines()[-1]
        git('commit', '-q', '-am', 'stage2 read, t 1.4')
        ge = g_('extension_fetch', c12.EXTENSION_END + 8 * D)
        check('extension: due (0 < 1.4 < 2.571) but no extension approval -> refused',
              any('extension fetch: APPROVED' in x for x in ge) and not any('no extension' in x for x in ge), str(ge))
        ext_appr = appr + f'\nStage-2 extension fetch: APPROVED 2027-07-09 row {run.line_sha(s2_line)}\n'
        write_reg(extra=ext_appr)
        git('commit', '-q', '-am', 'extension approved')
        check('extension: approval after the Stage-2 row, after 2027-07-08 -> open',
              g_('extension_fetch', c12.EXTENSION_END + 8 * D) == [], str(g_('extension_fetch', c12.EXTENSION_END + 8 * D)))
        check('extension (t): before 2027-07-08 -> refused', any(x.startswith('(t)') for x in g_('extension_fetch', c12.EXTENSION_END + 6 * D)))
        add({'registration': '012-x.md', 'mode': 'extension_fetch_started', 'owner': OWNER})
        add({'registration': '012-x.md', 'mode': 'extension_fetched', 'owner': OWNER})
        git('commit', '-q', '-am', 'extension fetched')
        check('extension: a second fetch -> refused; the build gate is open',
              any('second fetch' in x for x in g_('extension_fetch', c12.EXTENSION_END + 8 * D))
              and g_('extension_build', c12.EXTENSION_END + 8 * D) == [])
        write_reg(x='NO', extra=ext_appr)
        git('commit', '-q', '-am', 'extension NO')
        gn = g_('extension_build', c12.EXTENSION_END + 8 * D)
        check('M3: flipping the extension to NO after the first look -> refused (not pre-registered, owner lines changed)',
              any('did not pre-register' in x for x in gn) and any(x.startswith('(o)') for x in gn))
        git('reset', '-q', '--hard', approved)
        add({'registration': '012-x.md', 'mode': 'stage2', 'q': 0.975, 'owner': OWNER,
             'stage2': {'confirm': False, 'primary_month': {'t': 2.9, 'clusters': 6}}})
        git('commit', '-q', '-am', 'stage2 read, t 2.9')
        check('extension: a first look with t >= 2.571 is not due -> refused',
              any('no extension' in x for x in g_('extension_fetch', c12.EXTENSION_END + 8 * D)))
        git('reset', '-q', '--hard', approved)
        (root / 'ct/carry012.py').write_text('x = 4\n')
        write_reg(extra=appr)
        git('commit', '-q', '-am', 'code changed after the run')
        check('(g3) code changed since the Stage-1 run -> refused', any(x.startswith('(g3)') for x in g_('stage2_fetch')))
        git('reset', '-q', '--hard', approved)

        # ---- m2: a REAL (g4) test: history purged, one stage1 row whose mean was forged
        git('reset', '-q', '--hard', reg_c2)
        subprocess.run(['git', 'reflog', 'expire', '--expire=now', '--all'], cwd=root, capture_output=True)
        subprocess.run(['git', 'gc', '-q', '--prune=now'], cwd=root, capture_output=True)
        forged_line = stage1_rows(mean=-0.0004)
        git('commit', '-q', '-am', 'stage1 rows with a forged mean')
        write_reg(extra=f'\nStage-2 fetch: APPROVED 2027-01-08 row {run.line_sha(forged_line)}\n')
        git('commit', '-q', '-am', 'approved the forged row')
        gg = g_('stage2_fetch')
        check('m2: with one stage1 row in all history (g1 passes), a forged mean under pass labels fails (g4) alone',
              not any(x.startswith('(g1)') for x in gg) and any(x.startswith('(g4)') for x in gg)
              and all(x.startswith('(g4)') for x in gg), str(gg))
        git('reset', '-q', '--hard', reg_c2)
        subprocess.run(['git', 'reflog', 'expire', '--expire=now', '--all'], cwd=root, capture_output=True)
        subprocess.run(['git', 'gc', '-q', '--prune=now'], cwd=root, capture_output=True)
        row_line = stage1_rows(started_commit='f' * 40)
        git('commit', '-q', '-am', 'stage1 with a started row of another commit')
        write_reg(extra=f'\nStage-2 fetch: APPROVED 2027-01-08 row {run.line_sha(row_line)}\n')
        git('commit', '-q', '-am', 'approved')
        check('m3: a stage1_started row of another registration commit -> (s) refused', any(x.startswith('(s)') for x in g_('stage2_fetch')))
        git('reset', '-q', '--hard', reg_c2)
        row_line = stage1_rows()
        write_reg(extra=f'\nStage-2 fetch: APPROVED 2027-01-08 row {run.line_sha(row_line)}\n')
        git('commit', '-q', '-am', 'row and approval in one commit')
        check("(h') an approval committed together with the row -> refused", any(x.startswith("(h')") for x in g_('stage2_fetch')))
        write_reg(status='DRAFT')
        git('commit', '-q', '-am', 'draft')
        check('status DRAFT -> refused', any('not REGISTERED' in x for x in fails()))


# ------------------------------------------------------------------ the real files
def real_file_checks():
    import r012_run as run
    print('the real registration (DRAFT)')
    reg = HERE / 'registrations' / '012-hl-cex-funding-carry.md'
    text = reg.read_text()
    check('the status line says DRAFT', '\n**Status: DRAFT**' in text)
    end, date, ext, f = run.owner_choice(text)
    check('exactly one "Stage-2 end (owner):" and one "Stage-2 extension (owner):" line, parsed as 2026-12-31 and YES',
          len(run.END_PREFIX.findall(text)) == 1 and len(run.EXT_PREFIX.findall(text)) == 1
          and end == 1_798_761_600_000 and ext is True and f == [], str(f))
    check('no line of the file starts with an approval phrase (no instruction can satisfy the gate)',
          not run.APPROVAL.findall(text) and not run.EXT_APPROVAL.findall(text)
          and not any(l.startswith(('Stage-2 fetch:', 'Stage-2 extension fetch:')) for l in text.splitlines()))
    fails = run.guard_failures(HERE, 'registrations/012-hl-cex-funding-carry.md', mode='stage1')[0]
    check('the real guard refuses Stage 1 today (not REGISTERED, untracked)',
          any('not REGISTERED' in x for x in fails) and any(x.startswith('(a)') for x in fails))
    table = dict(run.HASH_ROW.findall(text))
    missing = [f for f in (*run.GUARDED_CODE, *run.GUARDED_DATA) if f not in table]
    wrong = [f for f in (*run.GUARDED_CODE, *run.GUARDED_DATA) if f in table and (not (HERE / f).exists() or run.sha256(HERE / f) != table[f])]
    check('§8 lists every guarded code and data file, and every hash matches the file on disk', not missing and not wrong,
          f'missing {missing} wrong {wrong}')


if __name__ == '__main__':
    main()
