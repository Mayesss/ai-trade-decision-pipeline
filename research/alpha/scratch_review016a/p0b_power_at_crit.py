"""Power of H1 (t >= c and forward alpha > 0) at alternative critical values, constant / year / half-year slopes,
from the drafter's synthetic records (no real data)."""
import json
d = json.load(open('../data/r016/logs/power_records.json'))
for key, fk in (('t_const', 'fwd_const_pos'), ('t_year', 'fwd_year_pos'), ('t_half', 'fwd_half_pos')):
    for c in (1.714, 1.731, 1.85, 2.0, 2.06, 2.2):
        row = []
        for s in ('0.75', '1', '1.5'):
            row.append('/'.join(f"{sum(r[key] >= c and r[fk] for r in d[f'trend h{h} SR {s}'])/len(d[f'trend h{h} SR {s}']):.2f}" for h in (20, 60, 120)))
        print(f'{key:8s} crit {c:.3f}  SR0.75 {row[0]}  SR1 {row[1]}  SR1.5 {row[2]}')
