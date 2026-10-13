"""Summaries of p7_power_records.json / p7_size_records.json (synthetic): Revision 1 rule at three end dates."""
import json

import common as C

P = json.load(open('p7_power_records.json'))
try:
    Z = json.load(open('p7_size_records.json'))
except Exception:  # noqa: BLE001
    Z = {}
ENDS = ('2027-12-31', '2028-12-31', '2029-06-30')


def g(name, end, src=P):
    return src.get(f'{name} | {end}')


def rate(rs, f):
    return sum(f(r) for r in rs) / len(rs)


for end in ENDS:
    if not g('SR1 h20', end):
        continue
    G = g('SR1 h20', end)[0]['G']
    pubs = [g(f'pub h{h}', end) for h in (20, 60, 120)]
    c2 = min(C.q([r['t'] for r in rs], .05) for rs in pubs if rs)
    sizes = [g('VT vol timing sym (drafter binding)', end), g('N6 vol timing asym GJR t3', end)]
    sizes += [rs for k, rs in Z.items() if k.endswith(end) and not k.startswith('S')]
    cal = max(1.75, max(C.q([r['t'] for r in rs], .95) for rs in sizes if rs))
    c2s = C.q([r['t'] for r in g('SR0.75 h60', end)], .05)
    print(f'\n== end {end}  G {G}  C2 (min q05, as-published GJR, drift 0) {c2:+.2f} (registered -1.26); '
          f'max q95 over size worlds {cal:.2f} (registered 1.75); C2 if as-published ~ SR 0.75 h60 (symmetric-GARCH Table-4 kappa) {c2s:+.2f}')
    for nm in ('SR1 h20', 'SR1 h60', 'SR1 h120', 'SR0.75 h60', 'pub h20', 'pub h60', 'pub h120', 'SR0 h60 (dead)',
               'kappa0 (dead, no trend)', 'VT vol timing sym (drafter binding)', 'N6 vol timing asym GJR t3'):
        rs = g(nm, end)
        if not rs:
            continue
        a_reg = rate(rs, lambda r: r['t'] >= 1.75 and r['t_c'] >= 1.282)
        a_cal = rate(rs, lambda r: r['t'] >= cal and r['t_c'] >= 1.282)
        a_pw = rate(rs, lambda r: r['t'] >= cal and r['t_c_pooledwbar'] >= 1.282)
        t_only = rate(rs, lambda r: r['t'] >= cal)
        d_reg = rate(rs, lambda r: r['t'] < -1.26)
        d_cal = rate(rs, lambda r: r['t'] < c2)
        d_sym = rate(rs, lambda r: r['t'] < c2s)
        print(f'  {nm:38s} alive: reg {a_reg:.3f} | at end-cal {a_cal:.3f} | pooled-wbar t_c {a_pw:.3f} | t-only {t_only:.3f}'
              f'   decayed: C2 -1.26 {d_reg:.3f} | end-cal C2 {d_cal:.3f} | symmetric-cal C2 {d_sym:.3f}')
print('\n== size / hindsight worlds (p7_size_records)')
for k, rs in Z.items():
    print(f'  {k:52s} q95 {C.q([r["t"] for r in rs], .95):.2f}  P(t>=1.75) {rate(rs, lambda r: r["t"] >= 1.75):.3f}  '
          f'alive {rate(rs, lambda r: r["outcome"] == "alive"):.3f}  alive(pooled-wbar t_c) '
          f'{rate(rs, lambda r: r["t"] >= 1.75 and r["t_c_pooledwbar"] >= 1.282):.3f}  decayed {rate(rs, lambda r: r["outcome"] == "decayed"):.3f}')
