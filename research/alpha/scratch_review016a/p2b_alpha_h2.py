"""Probe 2b: the alpha-t decay statistic at conservative critical values (paper-IR based), from p23 records."""
import json, common as C
d = json.load(open('p23_records.json'))
H = [n for n in d if n.startswith('H ')]
print('world'.ljust(34), ' '.join(f'P(t<{c})' for c in (0.5, 0.8, 1.2, 1.856)), ' sd(t)')
for n in H:
    rs = d[n]; t = [r['t'] for r in rs]; m = C.mean(t)
    sd = (sum((x - m) ** 2 for x in t) / (len(t) - 1)) ** 0.5
    print(n[2:].ljust(34), ' '.join(f'{sum(x < c for x in t) / len(t):8.3f}' for c in (0.5, 0.8, 1.2, 1.856)), f' {sd:.2f}')
