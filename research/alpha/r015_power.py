"""Registration 015 — synthetic size, calibration and power (no real return is read). Revision 1: the primary is the
vol-scaled z (review A M2); CAL_CRIT = the largest calibration-world q95 plus 2 Monte Carlo SE, rounded up.

    python3 -I r015_power.py                 # every world, SIMS draws each; writes data/r015/logs/power.json
    python3 -I r015_power.py --quick         # 200 draws per world (smoke test)

The only real input is `data/r015/calendar.json`: the trading dates 2014-01-02 -> 2026-06-09 and the month-end
calendar built from them by `r015_run.py --calendar` (dates only). Daily returns are drawn from regime worlds whose
unconditional daily SD is the one number already read (US 1.12 %, `000s`). The registered statistic, criteria and
economic bar are computed by `r015_core` exactly as the guarded run computes them.
"""
import json
import math
import random
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import r015_core as C  # noqa: E402

SIMS = 10000
SD = C.DAILY_SD

# Shapes of documented episodes: rounded public daily moves of the S&P 500 (search summaries / public record), used
# only as SHAPES placed at random or adversarial positions in synthetic histories. They are never placed at their
# real dates, and nothing about where they fell relative to any window is used.
SHOCK_2020 = [-0.034, -0.044, -0.004, 0.046, -0.028, 0.042, -0.034, -0.076, 0.049, -0.049, -0.095, 0.093, -0.120,
              0.060, -0.052, 0.047, -0.043, -0.029, 0.094, 0.012, 0.062, -0.016, 0.034, -0.016]   # 2020-02-24 .. 03-31
SHOCK_2025 = [-0.048, -0.060, -0.002, -0.016, 0.095, -0.035, 0.018, -0.002, 0.004, -0.022]          # 2025-04-03 .. 04-16
SHOCK_2008 = [-0.004, -0.040, -0.014, -0.039, -0.057, -0.011, -0.076, -0.012, 0.116, -0.005, -0.090, 0.043, -0.006,
              0.048, -0.031, -0.061, 0.013, -0.035, -0.032, 0.108, -0.011, 0.026, 0.015]           # 2008-10-01 .. 10-31
SHOCKS = {'2020': SHOCK_2020, '2025': SHOCK_2025, '2008': SHOCK_2008}

GARCH_N = {'garch': (0.10, 0.88), 'innov': 'normal'}
GARCH_T = {'garch': (0.12, 0.87), 'innov': 't4'}

NULL_WORLDS = [
    {'name': 'iid normal'},
    {'name': 'iid t3', 'innov': 't3'},
    {'name': 'garch normal (a .10 b .88)', **GARCH_N},
    {'name': 'garch t4 (a .12 b .87)', **GARCH_T},
    {'name': 'drift by era (+8 / 0 / -8 bp/day) garch', **GARCH_N, 'drift_era': {3: 0.0008, 2: 0.0, 1: -0.0008}},
    {'name': 'drift by year (+-10 bp/day, random sign) garch', **GARCH_N, 'drift_year': 0.0010},
    {'name': 'slow AR drift (phi .995, sd 8 bp/day) garch', **GARCH_N, 'drift_ar': (0.995, 0.0008)},
    {'name': 'shocks 2008/2020/2025 anywhere, garch', **GARCH_N, 'shocks': [('2008', 'anywhere'), ('2020', 'anywhere'),
                                                                          ('2025', 'anywhere')]},
    {'name': 'shocks 2008/2020/2025 inside random month windows, garch', **GARCH_N,
     'shocks': [('2008', 'window'), ('2020', 'window'), ('2025', 'window')]},
    {'name': 'crash aligned: 2020 down-leg ends on a P window end, garch t4', **GARCH_T, 'shocks': [('2020', 'aligned')]},
    {'name': 'crash aligned: 2008 down-leg ends on a P window end, garch t4', **GARCH_T, 'shocks': [('2008', 'aligned')]},
    # STRESS (reported, not in the calibration set): two of the largest documented crashes BOTH aligned adversarially
    {'name': 'STRESS two crashes aligned: 2020 and 2025 down-legs end on P window ends, garch t4', **GARCH_T,
     'shocks': [('2020', 'aligned'), ('2025', 'aligned')], 'stress': True},
    {'name': 'reversal structure without mean (k .33), iid normal', 'reversal_k': 0.33},
    {'name': 'window-day vol x1.3, garch t4', **GARCH_T, 'window_vol': 1.3},
]

POWER_WORLDS = [
    *({'name': f'shifted effect x{k} iid normal', 'effect': k} for k in (1.0, 0.5)),
    *({'name': f'shifted effect x{k} garch normal', **GARCH_N, 'effect': k} for k in (1.0, 0.5)),
    *({'name': f'shifted effect x{k} garch t4', **GARCH_T, 'effect': k} for k in (1.0, 0.5)),
    *({'name': f'shifted effect x{k} garch, shocks anywhere', **GARCH_N, 'effect': k,
       'shocks': [('2008', 'anywhere'), ('2020', 'anywhere'), ('2025', 'anywhere')]} for k in (1.0, 0.5)),
    *({'name': f'shifted effect x{k} with reversal correlation (k .33) iid', 'effect': k, 'reversal_k': 0.33} for k in (1.0, 0.5)),
    *({'name': f'unshifted (T+3 windows in every era) effect x{k} iid', 'effect': k, 'effect_mode': 'unshifted'} for k in (1.0, 0.5)),
    *({'name': f'decayed: full in the T+3 era only, x{k} iid', 'effect': k, 'effect_mode': 'decayed'} for k in (1.0,)),
    {'name': 'shifted effect x0.75 iid normal', 'effect': 0.75},
    *({'name': f'shifted effect x{k} proportional to trailing vol, garch t4', **GARCH_T, 'effect': k, 'effect_vol': True}
      for k in (1.0, 0.5)),
    {'name': 'died at publication: full to 2019-12, zero after, iid', 'effect': 1.0, 'effect_mode': 'died2019'},
    {'name': 'linear decay: full 2014-01 -> zero 2026-05, iid', 'effect': 1.0, 'effect_mode': 'linear'},
    {'name': 'turn of month only: T..T+3 +23 bp abnormal, iid', 'effect': 1.0, 'effect_mode': 'tom'},
    {'name': 'shifted effect x1.5 iid normal', 'effect': 1.5},
]


# ------------------------------------------------------------------ the calendar input
def load_calendar(path=None):
    """(dates, cal) from calendar.json (dates only)."""
    import datetime as dt
    j = json.loads(Path(path or HERE / 'data' / 'r015' / 'calendar.json').read_text())
    dates = [dt.date.fromisoformat(d) for d in j['dates']]
    return dates, C.build_calendar(dates)


# ------------------------------------------------------------------ synthetic returns
def _innov(rng, kind):
    if kind == 'normal':
        return rng.gauss(0.0, 1.0)
    nu = 3 if kind == 't3' else 4
    z = rng.gauss(0.0, 1.0) / math.sqrt(rng.gammavariate(nu / 2.0, 2.0) / nu)
    return z / math.sqrt(nu / (nu - 2.0))


def draw(world, dates, cal, rng, day_era, day_year):
    n = len(dates)
    kind = world.get('innov', 'normal')
    x = [0.0] * n
    if 'garch' in world:
        a, b = world['garch']
        om = SD * SD * (1 - a - b)
        h = SD * SD
        e = 0.0
        for i in range(n):
            h = om + a * e * e + b * h
            e = math.sqrt(h) * _innov(rng, kind)
            x[i] = e
    else:
        for i in range(n):
            x[i] = SD * _innov(rng, kind)
    if 'window_vol' in world:
        f = world['window_vol']
        for c in cal:
            for i in c['P'] + c['R']:
                x[i] *= f
    if 'reversal_k' in world:
        k = world['reversal_k']
        for c in cal:
            sp = sum(x[i] for i in c['P'])
            r3 = c['R'][:3]
            for i in r3:
                x[i] -= k * sp / len(r3)
    if 'drift_era' in world:
        de = world['drift_era']
        for i in range(n):
            x[i] += de[day_era[i]]
    if 'drift_year' in world:
        amp = world['drift_year']
        signs = {y: rng.choice((-1, 1)) for y in set(day_year)}
        for i in range(n):
            x[i] += amp * signs[day_year[i]]
    if 'drift_ar' in world:
        phi, sd = world['drift_ar']
        u = rng.gauss(0.0, sd)
        k = sd * math.sqrt(1 - phi * phi)
        for i in range(n):
            u = phi * u + k * rng.gauss(0.0, 1.0)
            x[i] += u
    for name, how in world.get('shocks', ()):
        shape = SHOCKS[name]
        L = len(shape)
        if how == 'anywhere':
            start = rng.randrange(0, n - L)
        elif how == 'window':
            c = rng.choice(cal)
            start = rng.randrange(max(0, c['P'][0] - L // 2), min(n - L, c['R'][-1]) + 1)
        elif how == 'aligned':
            pairs = [shape[i] + shape[i + 1] for i in range(L - 1)]
            j = pairs.index(min(pairs)) + 1          # the worst two-day down-leg ends at shape[j]
            c = rng.choice(cal)
            start = min(max(0, c['P'][-1] - j), n - L)
        else:
            raise C.Refused(f'unknown placement {how}')
        x[start:start + L] = shape
    k = world.get('effect')
    if k:
        mode = world.get('effect_mode', 'shifted')
        scale = [1.0] * len(cal)
        if world.get('effect_vol'):
            sig = C.sigma_series(x, cal)
            mean_sig = sum(sig) / len(sig)
            scale = [a / mean_sig for a in sig]
        G = len(cal)
        for j, c in enumerate(cal):
            kk = k * scale[j]
            if mode == 'decayed' and c['s'] != 3:
                continue
            if mode == 'died2019' and c['month'] > '2019-12':
                continue
            if mode == 'linear':
                kk *= 1 - j / (G - 1)
            if mode == 'tom':
                for i in c['tom']:
                    x[i] += kk * 0.0023 / len(c['tom'])
                continue
            pk, rk = ('P3', 'R3') if mode == 'unshifted' else ('P', 'R')
            ap, ar = kk * C.EFFECT_P / len(c[pk]), kk * C.EFFECT_R / len(c[rk])
            for i in c[pk]:
                x[i] += ap
            for i in c[rk]:
                x[i] += ar
    return x


def one(x, cal, c_reg):
    z, y, sig, mu = C.z_series(x, cal)
    ex = C.existence(z, c_reg)
    econ = C.economic(y, C.v_series(x, cal), cal, C.pooled_mu(x, cal))
    ty = C.mean_t(y)[2]
    lo = C.mean_t(C.rebound_sums(x, cal, mu))
    mc = C.mechanism_contrast(x, cal, mu)['paired_all_t2_t1']
    pl = C.placebo(x, cal, mu)
    rz = C.mean_t([a for a, c in zip(z, cal) if c['month'] >= C.RECENT_FROM])[2]
    return {'t': ex['t'], 'mean': ex['mean'], 'pos': ex['criteria']['mean_positive'],
            'wo': ex['criteria']['mean_positive_without_largest_month'], 'exists': ex['exists'],
            'bar': econ['meets_bar'], 'size': econ.get('size_half_kelly_capped'), 'long_t': lo[2], 'contrast_t': mc['t'],
            'placebo_t': pl['t'], 'raw_y_t': ty, 'raw_y_pos': statistics.fmean(y) > 0,
            'raw_y_wo': statistics.fmean(sorted(y)[:-1]) > 0, 'recent_t': rz}


def simulate(world, dates, cal, sims, seed, c_reg):
    rng = random.Random(seed)
    month_s = {c['month']: c['s'] for c in cal}
    last_s = cal[-1]['s']
    day_era = [month_s.get(C.month_of(d), last_s if d.isoformat() > cal[-1]['T'] else cal[0]['s']) for d in dates]
    day_year = [d.year for d in dates]
    return [one(draw(world, dates, cal, rng, day_era, day_year), cal, c_reg) for _ in range(sims)]


def quantile(v, q):
    s = sorted(v)
    if not s:
        return None
    k = (len(s) - 1) * q
    lo = math.floor(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def gate_score(r):
    """The t that the full existence rule needs to clear: -inf unless mean > 0 and mean without the largest month > 0."""
    return r['t'] if (r['pos'] and r['wo'] and r['t'] is not None) else -1e9


def rates(recs, c):
    n = len(recs)
    t148 = C.t_ppf(C.Q, C.G_EXPECTED - 1)
    t104 = C.t_ppf(C.Q, 104)
    ge = lambda key, k: sum(1 for r in recs if r[key] is not None and r[key] >= k) / n
    return {'pass_at_crit': sum(1 for r in recs if gate_score(r) >= c) / n,
            'pass_at_t148': sum(1 for r in recs if gate_score(r) >= t148) / n,
            'pass_and_bar_at_crit': sum(1 for r in recs if gate_score(r) >= c and r['bar']) / n,
            't_only_at_t148': ge('t', t148), 'long_only_t_ge_t148': ge('long_t', t148),
            'contrast_t_ge_t104': ge('contrast_t', t104), 'placebo_t_ge_t148': ge('placebo_t', t148),
            'mean_t': statistics.fmean(r['t'] for r in recs), 'sd_t': statistics.stdev(r['t'] for r in recs),
            'mean_contrast_t': statistics.fmean(r['contrast_t'] for r in recs),
            'q95_gate_score': quantile([gate_score(r) for r in recs], 0.95),
            'q95_mc_se': q95_se([gate_score(r) for r in recs]),
            'draft_raw_y_rule_at_1.91': sum(1 for r in recs if r['raw_y_pos'] and r['raw_y_wo'] and r['raw_y_t'] is not None
                                            and r['raw_y_t'] >= 1.91) / n,
            'recent_t_ge_1': ge('recent_t', 1.0)}


def q95_se(scores, batches=10):
    """Monte Carlo SE of the 95th percentile: SD of the batch q95s / sqrt(batches)."""
    k = len(scores) // batches
    if k < 20:
        return None
    qs = [quantile(scores[b * k:(b + 1) * k], 0.95) for b in range(batches)]
    return statistics.stdev(qs) / math.sqrt(batches)


def calibrate(null_results, stress=()):
    """The registered rule (Revision 1): CAL_CRIT = the largest 95th percentile of the gate score over the calibration
    worlds (stress worlds excluded) PLUS 2 Monte Carlo SE of that percentile (the margin), rounded UP to 0.01, never
    below t_148(0.95). Returns (crit, largest q95, its MC SE, the binding world)."""
    best = None
    for k, recs in null_results.items():
        if k in stress:
            continue
        sc = [gate_score(r) for r in recs]
        q = quantile(sc, 0.95)
        if best is None or q > best[0]:
            best = (q, q95_se(sc) or 0.0, k)
    q, se, k = best
    return math.ceil(max(C.T148, q + 2 * se) * 100 - 1e-9) / 100, q, se, k


def placement_scan(shape_name, dates, cal, n_place=150, draws=200, c=None, seed=777):
    """Reported (review A M3): the false-pass rate at c conditional on where one documented shape is placed, over
    n_place evenly spaced starts on the real calendar, iid normal days otherwise."""
    c = C.CAL_CRIT if c is None else c
    shape = SHOCKS[shape_name]
    n = len(dates)
    starts = [round(i * (n - len(shape) - 1) / (n_place - 1)) for i in range(n_place)]
    rng = random.Random(seed)
    rates_ = []
    for st in starts:
        hit = 0
        for _ in range(draws):
            x = [SD * rng.gauss(0.0, 1.0) for _ in range(n)]
            x[st:st + len(shape)] = shape
            r = one(x, cal, c)
            hit += gate_score(r) >= c
        rates_.append(hit / draws)
    return {'shape': shape_name, 'placements': n_place, 'draws_each': draws, 'mean_rate': statistics.fmean(rates_),
            'max_rate': max(rates_), 'share_above_0.05': sum(1 for r in rates_ if r > 0.05) / n_place,
            'note': 'each rate has MC SE about 0.015 at 200 draws'}


def run(sims=SIMS, cal_path=None, out_path=None, worlds_null=None, worlds_power=None):
    dates, cal = load_calendar(cal_path)
    worlds_null = NULL_WORLDS if worlds_null is None else worlds_null
    worlds_power = POWER_WORLDS if worlds_power is None else worlds_power
    t0 = time.time()
    null = {}
    for i, w in enumerate(worlds_null):
        null[w['name']] = simulate(w, dates, cal, sims, 15000 + i, C.CAL_CRIT)
        print(f"null {w['name']}: q95 gate score {quantile([gate_score(r) for r in null[w['name']]], 0.95):.3f} "
              f"({time.time() - t0:.0f}s)", flush=True)
    stress = {w['name'] for w in worlds_null if w.get('stress')}
    c_star, worst, se, wname = calibrate(null, stress)
    c_stress = calibrate(null)[0]
    print(f'calibrated critical value {c_star} (largest q95 {worst:.4f} +- {se:.4f} MC SE in {wname}; '
          f't_148(0.95) {C.T148:.4f}); registered CAL_CRIT {C.CAL_CRIT}', flush=True)
    out = {'sims': sims, 'months': len(cal), 'calibrated_crit': c_star, 'largest_q95': worst, 'largest_q95_mc_se': se,
           'binding_world': wname,
           't148_95': C.t_ppf(C.Q, 148), 'registered_CAL_CRIT': C.CAL_CRIT,
           'calibrated_equals_registered': c_star == C.CAL_CRIT, 'rates_at': c_star, 'stress_worlds': sorted(stress),
           'crit_if_stress_calibrated': c_stress, 'null': {}, 'power': {}, 'power_at_stress_crit': {}}
    for name, recs in null.items():
        out['null'][name] = rates(recs, c_star)
    for i, w in enumerate(worlds_power):
        recs = simulate(w, dates, cal, sims, 25000 + i, c_star)
        out['power'][w['name']] = rates(recs, c_star)
        out['power_at_stress_crit'][w['name']] = rates(recs, c_stress)['pass_at_crit']
        print(f"power {w['name']}: {json.dumps({k: round(v, 3) for k, v in out['power'][w['name']].items() if v is not None})}",
              flush=True)
    out['placement_scan'] = [placement_scan(nm, dates, cal, c=c_star) for nm in (('2020', '2008', '2025') if sims >= 1000 else ())]
    for p_ in out['placement_scan']:
        print('placement', json.dumps(p_), flush=True)
    for name, r in out['null'].items():
        print(f"null {name}: {json.dumps({k: round(v, 3) for k, v in r.items() if v is not None})}", flush=True)
    if out_path:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).write_text(json.dumps(out, indent=1, sort_keys=True))
    return out


if __name__ == '__main__':
    quick = '--quick' in sys.argv
    run(sims=200 if quick else SIMS,
        out_path=None if quick else HERE / 'data' / 'r015' / 'logs' / 'power.json')
