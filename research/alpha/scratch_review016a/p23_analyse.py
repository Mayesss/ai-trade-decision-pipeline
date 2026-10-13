"""Analysis of p23_records.json (synthetic) and p1_records.json: span-(a) designs (probe 3) and decay statistics
(probe 2)."""
import json

import common as C

d = json.load(open('p23_records.json'))
p1 = json.load(open('p1_records.json'))
NULLS = [n for n in p1 if n.startswith('N')]

print('== probe 3: critical value for (c) alone (k 3, G 18): floor t_14 = 1.761; q95 of t_c over the reviewer nulls')
qc = {n: C.q([r['t_c'] for r in p1[n]], 0.95) for n in NULLS}
print('   ', {k[:3]: round(v, 2) for k, v in qc.items()}, 'max', round(max(qc.values()), 3))
CC = max(1.761, max(qc.values()))
print('== probe 3: q95 of t pooled (const) over reviewer nulls:', round(max(C.q([r['t'] for r in p1[n]], .95) for n in NULLS), 3))


def designs(cal):
    return {
        f'D0 registered (t>={cal}, alpha_c>0)': lambda r: r['t'] >= cal and r['alpha_c_pos'],
        f'D2 t>={cal} and t_c>=0.5': lambda r: r['t'] >= cal and r['t_c'] >= 0.5,
        f'D3 t>={cal} and t_c>=1.0': lambda r: r['t'] >= cal and r['t_c'] >= 1.0,
        f'D4 t>={cal} and t_c>=1.282': lambda r: r['t'] >= cal and r['t_c'] >= 1.282,
        f'D1 (c) alone t_c>={CC:.3f}': lambda r: r['t_c'] >= CC,
        'D1f (c) alone t_c>=1.761 (floor)': lambda r: r['t_c'] >= 1.761,
    }


for cal in (1.731, 2.1):
    print(f'\n== probe 3 at pooled CAL {cal}')
    ds = designs(cal)
    names = [n for n in d if n.startswith('S')]
    print('design'.ljust(40), ' '.join(n[:3] for n in names))
    for k, f in ds.items():
        print(k.ljust(40), ' '.join(f'{sum(f(r) for r in d[n]) / len(d[n]):.3f}' for n in names))
    # selection model on the reviewer's nulls
    allnull = [r for n in NULLS for r in p1[n]]
    for x in (1.0, 1.5, 2.0):
        s = [r for r in allnull if r['t_a'] >= x]
        print(f'   reviewer nulls, t_a >= {x}: n {len(s)} ' + ' '.join(
            f'{k[:3]} {sum(f(r) for r in s) / len(s):.3f}' for k, f in ds.items()))
for n in d:
    if n.startswith('S'):
        print(n, 'mean sr', round(C.mean([r['sr'] for r in d[n]]), 2), 't', round(C.mean([r['t'] for r in d[n]]), 2),
              't_a', round(C.mean([r['t_a'] for r in d[n]]), 2), 't_c', round(C.mean([r['t_c'] for r in d[n]]), 2))

print('\n== probe 2: decay statistics')
H = [n for n in d if n.startswith('H ')]
pub = [n for n in H if 'SR 1.57' in n]
cT = 0.117
c_t = min(C.q([r['t'] for r in d[n]], 0.05) for n in pub)
c_b = min(C.q([r['t_btc'] for r in d[n]], 0.05) for n in pub)
c_T = min(C.q([r['T'] for r in d[n]], 0.05) for n in pub)
print('q05 in the SR-1.57 worlds:', {n[2:]: (round(C.q([r['T'] for r in d[n]], .05), 2), round(C.q([r['t'] for r in d[n]], .05), 2),
                                        round(C.q([r['t_btc'] for r in d[n]], .05), 2)) for n in pub})
print(f'calibrated: T {c_T:.3f} (registered 0.117), alpha-t {c_t:.3f}, BTC-only alpha-t {c_b:.3f}')
print('world'.ljust(34), 'meanSR  meanT  mean_t  rejT(0.117) rejT(recal) rej_alpha_t rej_btc_t | outcomes registered: AAP AD close undec | with alpha-t H2: close')
for n in H:
    rs = d[n]
    m = len(rs)
    h1 = [r['t'] >= 1.731 and r['alpha_c_pos'] for r in rs]
    h2 = [r['T'] < cT for r in rs]
    aap = sum(a and not b for a, b in zip(h1, h2)) / m
    ad = sum(a and b for a, b in zip(h1, h2)) / m
    cl = sum((not a) and b for a, b in zip(h1, h2)) / m
    un = sum((not a) and (not b) for a, b in zip(h1, h2)) / m
    cl2 = sum((not a) and r['t'] < c_t for a, r in zip(h1, rs)) / m
    print(n[2:].ljust(34), f"{C.mean([r['sr'] for r in rs]):5.2f} {C.mean([r['T'] for r in rs]):6.2f} {C.mean([r['t'] for r in rs]):6.2f}"
          f"  {sum(h2) / m:9.3f} {sum(r['T'] < c_T for r in rs) / m:9.3f} {sum(r['t'] < c_t for r in rs) / m:9.3f}"
          f" {sum(r['t_btc'] < c_b for r in rs) / m:9.3f} | {aap:.3f} {ad:.3f} {cl:.3f} {un:.3f} | {cl2:.3f}")
