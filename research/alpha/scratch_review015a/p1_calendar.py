"""Review A of 015, probe 1 (DATES ONLY): an independent rebuild of the month-end windows from the pinned trading dates,
compared with calendar.json's rows; the structure of the statistic (weights, contrast algebra); and calendar
coincidences that could put a non-liquidity mean inside P or R (FOMC announcement days, quarter/year ends, the Russell
reconstitution). No return value is read anywhere.

    python3 -I research/alpha/scratch_review015a/p1_calendar.py
"""
import collections
import csv
import datetime as dt
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ALPHA = HERE.parent
cal_j = json.loads((ALPHA / 'data/r015/calendar.json').read_text())
dates = [dt.date.fromisoformat(d) for d in cal_j['dates']]
T2, T1 = dt.date(2017, 9, 5), dt.date(2024, 5, 28)


def lag(d):
    return 3 if d < T2 else 2 if d < T1 else 1


# independent rebuild: T = last date of each calendar month; s = the s whose last selling day T-(s+1) is under lag s
by_m = collections.OrderedDict()
for i, d in enumerate(dates):
    by_m.setdefault((d.year, d.month), []).append(i)
rows = []
for (y, m), idx in by_m.items():
    if (y, m) > (2026, 5) or (y, m) < (2014, 1):
        continue
    iT = idx[-1]
    ss = [s for s in (3, 2, 1) if lag(dates[iT - s - 1]) == s]
    assert len(ss) == 1, (y, m, ss)
    s = ss[0]
    rows.append({'month': f'{y:04d}-{m:02d}', 'T': dates[iT], 's': s, 'iT': iT,
                 'P': list(range(iT - 8, iT - s)), 'R': list(range(iT - s, iT + 4)),
                 'out': [i for i in idx[3:] if i < iT - 8]})
mism = 0
for r, p in zip(rows, cal_j['summary']['rows']):
    got = {'month': r['month'], 'T': r['T'].isoformat(), 's': r['s'],
           'P': [dates[r['P'][0]].isoformat(), dates[r['P'][-1]].isoformat()],
           'R': [dates[r['R'][0]].isoformat(), dates[r['R'][-1]].isoformat()], 'n_out': len(r['out'])}
    if 'vol' in p:
        got['vol'] = [dates[r['iT'] - 28].isoformat(), dates[r['iT'] - 9].isoformat()]
    mism += got != p
print(f'independent rebuild: {len(rows)} months, mismatches vs calendar.json rows: {mism}')
print('by lag', collections.Counter(r['s'] for r in rows), 'friday T', sum(r['T'].weekday() == 4 for r in rows))
print('weights |R|-|P| by lag', {s: (s + 4) - (8 - s) for s in (3, 2, 1)})
for r in rows:
    if r['month'] in ('2017-08', '2017-09', '2024-04', '2024-05', '2024-06'):
        print('  boundary', r['month'], 'T', r['T'], 's', r['s'], 'P', dates[r['P'][0]], '..', dates[r['P'][-1]],
              'R', dates[r['R'][0]], '..', dates[r['R'][-1]])

# contrast algebra: shifted minus unshifted windows, per era, as signed day sets
for s in (2, 1):
    iT = 100
    P, R = set(range(iT - 8, iT - s)), set(range(iT - s, iT + 4))
    P3, R3 = set(range(iT - 8, iT - 3)), set(range(iT - 3, iT + 4))
    w = collections.Counter()
    for i in R:
        w[i - iT] += 1
    for i in P:
        w[i - iT] -= 1
    for i in R3:
        w[i - iT] -= 1
    for i in P3:
        w[i - iT] += 1
    print(f'contrast d = y_shift - y_unshift under s={s}: day weights', {f'T{k:+d}': v for k, v in sorted(w.items()) if v})

# FOMC announcement days (dates file from the B1 scout) and where they fall
fomc = set()
with open(ALPHA / 'data/scout_b1/derived_20261013T092459Z/fomc_announcement_days.csv') as f:
    for row in csv.DictReader(f):
        fomc.add(dt.date.fromisoformat(row['announcement_date']))
pos = {}
for r in rows:
    for i in r['P']:
        pos[i] = ('P', r['s'], i - r['iT'])
    for i in r['R']:
        pos[i] = ('R', r['s'], i - r['iT'])
    for i in r['out']:
        pos[i] = ('out', r['s'], None)
cnt = collections.Counter()
rel = collections.Counter()
idx_of = {d: i for i, d in enumerate(dates)}
for d in sorted(fomc):
    if d not in idx_of or d > dt.date(2026, 6, 3):
        continue
    for k, tag in ((0, 'day'), (-1, 'eve')):
        i = idx_of[d] + k
        where = pos.get(i, ('first3', None, None))
        cnt[(tag, where[0], where[1])] += 1
        if tag == 'day' and where[0] in ('P', 'R'):
            rel[(where[0], where[2])] += 1
n_f = sum(1 for d in fomc if d in idx_of and d <= dt.date(2026, 6, 3))
print(f'FOMC announcement days in sample: {n_f}')
for k in sorted(cnt, key=str):
    print('  ', k, cnt[k])
print('  announcement day position (window, T+k):', dict(sorted(rel.items(), key=str)))
days_P = sum(len(r['P']) for r in rows)
days_R = sum(len(r['R']) for r in rows)
print(f'window days: P {days_P}, R {days_R}, out {sum(len(r["out"]) for r in rows)}; '
      f'FOMC share of R days {sum(v for (w, _), v in rel.items() if w == "R") / days_R:.3f}, '
      f'of P days {sum(v for (w, _), v in rel.items() if w == "P") / days_P:.3f}, '
      f'of all days {n_f / len(dates):.3f}')
# quarter ends and Russell reconstitution (last Friday of June; dates only)
qe = sum(1 for r in rows if int(r['month'][5:]) in (3, 6, 9, 12))
print('quarter-end months', qe, 'year-end months', sum(1 for r in rows if r['month'].endswith('-12')))
russ = []
for y in range(2014, 2026 + 1):
    june = [d for d in dates if d.year == y and d.month == 6 and d.weekday() == 4]
    if june:
        i = idx_of[june[-1]]
        russ.append((y, pos.get(i, ('?',))[0], pos.get(i, (None, None, None))[2]))
print('Russell reconstitution (last Friday of June) position:', russ)
