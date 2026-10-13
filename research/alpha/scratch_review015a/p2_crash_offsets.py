"""Review A of 015, probe 2: crash alignment, EXHAUSTIVELY over every placement on the real trading calendar.

For each shape (2020, 2025, 2008) and every start index, the exact deterministic contribution of the shape to every
month's y (window sums and the era-mu shift) is computed; the false-pass rate CONDITIONAL on that placement is then
estimated against a common bank of iid-normal month noise (12 window days x 1.12 %; overwritten days removed).
Reported: the uniform-placement average (= the honest null, crash timing independent of month ends), the worst
placement, the draft's 'aligned' placement, and pairs (2020 and 2025 both present).

    python3 -I research/alpha/scratch_review015a/p2_crash_offsets.py
"""
import json
import math
import random

import common as K

dates, rows = K.load()
n = len(dates)
G = len(rows)
era_n = {}
for r in rows:
    era_n[r['s']] = era_n.get(r['s'], 0) + len(r['out'])
era_months = {s: [k for k, r in enumerate(rows) if r['s'] == s] for s in (3, 2, 1)}
day_role = {}
for k, r in enumerate(rows):
    for i in r['P']:
        day_role[i] = ('P', k)
    for i in r['R']:
        day_role[i] = ('R', k)
    for i in r['out']:
        day_role[i] = ('out', r['s'])


def contribution(shape, start):
    """({month: c}, {month: overwritten window days}) for the shape placed at start (the noise it replaces is
    accounted for by k)."""
    c, kk = {}, {}
    dmu = {3: 0.0, 2: 0.0, 1: 0.0}
    for j, val in enumerate(shape):
        role = day_role.get(start + j)
        if role is None:
            continue
        if role[0] == 'R':
            c[role[1]] = c.get(role[1], 0.0) + val
            kk[role[1]] = kk.get(role[1], 0) + 1
        elif role[0] == 'P':
            c[role[1]] = c.get(role[1], 0.0) - val
            kk[role[1]] = kk.get(role[1], 0) + 1
        else:
            dmu[role[1]] += val / era_n[role[1]]
    return c, kk, dmu


BANK = 3000
rng = random.Random(7)
s12 = K.SD * math.sqrt(12)
bank = [[rng.gauss(0, s12) for _ in range(G)] for _ in range(BANK)]
weights = [len(r['R']) - len(r['P']) for r in rows]
CR = [K.tq(148), 1.91, 2.13]


def rates(cs):
    """Conditional pass rates at CR for a list of placements [(c, k, dmu)] applied together."""
    c, kk = {}, {}
    dmu = {3: 0.0, 2: 0.0, 1: 0.0}
    for ci, ki, di in cs:
        for m, v in ci.items():
            c[m] = c.get(m, 0.0) + v
        for m, v in ki.items():
            kk[m] = min(12, kk.get(m, 0) + v)
        for s in dmu:
            dmu[s] += di[s]
    hits = [0, 0, 0]
    for b in bank:
        y = list(b)
        for m in kk:
            y[m] = y[m] * math.sqrt((12 - kk[m]) / 12)
        for m, v in c.items():
            y[m] += v
        if any(dmu.values()):
            for k in range(G):
                y[k] -= weights[k] * dmu[rows[k]['s']]
        mean = sum(y) / G
        if mean <= 0:
            continue
        mx = max(y)
        if (sum(y) - mx) / (G - 1) <= 0:
            continue
        var = sum((a - mean) ** 2 for a in y) / (G - 1)
        t = mean / math.sqrt(var / G)
        for j, cr in enumerate(CR):
            hits[j] += t >= cr
    return [h / BANK for h in hits]


def aligned_start(shape, r):
    pairs = [shape[i] + shape[i + 1] for i in range(len(shape) - 1)]
    j = pairs.index(min(pairs)) + 1
    return min(max(0, r['P'][-1] - j), n - len(shape))


out = {'bank': BANK, 'crits': CR}
per = {}
for name in ('2020', '2025', '2008'):
    shape = K.SHOCKS[name]
    L = len(shape)
    starts = list(range(rows[0]['P'][0], rows[-1]['R'][-1] - L + 1))
    cache = {}
    res = []
    for st in starts:
        c, kk, dmu = contribution(shape, st)
        key = (tuple(sorted((m - min(c, default=0), round(v, 5)) for m, v in c.items())),
               tuple(sorted((m - min(c, default=0), v) for m, v in kk.items())), rows[min(c, default=0)]['s'] if c else 0)
        if key not in cache:
            cache[key] = rates([(c, kk, dmu)])
        res.append((st, cache[key], max([0.0] + list(c.values())), sum(c.values())))
    per[name] = res
    avg = [sum(r[1][j] for r in res) / len(res) for j in range(3)]
    worst = max(res, key=lambda r: r[1][1])
    al = [rates([contribution(shape, aligned_start(shape, r))]) for r in rows]
    al_avg = [sum(a[j] for a in al) / len(al) for j in range(3)]
    frac10 = sum(1 for r in res if r[1][1] > 0.10) / len(res)
    frac05 = sum(1 for r in res if r[1][1] > 0.05) / len(res)
    top_c = max(r[2] for r in res)
    out[name] = {'placements': len(res), 'unique': len(cache), 'uniform_avg_rates': avg,
                 'worst_placement_rates': worst[1], 'worst_start_date': dates[worst[0]].isoformat(),
                 'worst_max_month_contribution_pct': 100 * worst[2],
                 'max_single_month_contribution_pct_any_placement': 100 * top_c,
                 'draft_aligned_avg_rates': al_avg,
                 'share_placements_cond_rate_gt_0.10_at_1.91': frac10,
                 'share_placements_cond_rate_gt_0.05_at_1.91': frac05}
    print(name, json.dumps(out[name]), flush=True)

# both 2020 and 2025 present: uniform independent placements (sampled), the draft's double alignment, the worst pair
prng = random.Random(11)
pairs = []
for _ in range(600):
    a = prng.choice(per['2020'])[0]
    b = prng.choice(per['2025'])[0]
    if abs(a - b) < 30:
        continue
    pairs.append(rates([contribution(K.SHOCK_2020, a), contribution(K.SHOCK_2025, b)]))
pa = [sum(p[j] for p in pairs) / len(pairs) for j in range(3)]
w20 = max(per['2020'], key=lambda r: r[1][1])[0]
w25 = max(per['2025'], key=lambda r: r[1][1])[0]
worst_pair = rates([contribution(K.SHOCK_2020, w20), contribution(K.SHOCK_2025, w25)])
dbl = []
for _ in range(300):
    r1, r2 = prng.choice(rows), prng.choice(rows)
    if r1 is r2:
        continue
    dbl.append(rates([contribution(K.SHOCK_2020, aligned_start(K.SHOCK_2020, r1)),
                      contribution(K.SHOCK_2025, aligned_start(K.SHOCK_2025, r2))]))
da = [sum(p[j] for p in dbl) / len(dbl) for j in range(3)]
p_both_bad = (sum(1 for r in per['2020'] if r[1][1] > 0.05) / len(per['2020'])) * \
             (sum(1 for r in per['2025'] if r[1][1] > 0.05) / len(per['2025']))
out['pair_uniform_avg_rates'] = pa
out['pair_draft_double_aligned_avg_rates'] = da
out['pair_worst_rates'] = worst_pair
out['P_both_placements_cond_rate_gt_0.05'] = p_both_bad
print('pair uniform', pa, 'double aligned', da, 'worst pair', worst_pair, 'P(both bad)', p_both_bad)
# baseline with no crash (bank only)
out['no_crash_rates'] = rates([])
print('no crash', out['no_crash_rates'])
(K.HERE / 'p2_out.json').write_text(json.dumps(out, indent=1))
