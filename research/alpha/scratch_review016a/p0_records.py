"""Re-reads of the drafter's synthetic power_records.json (no real data): selection on span (a), the economic bar,
the alpha-t as an alternative decay statistic."""
import json, math
d = json.load(open('../data/r016/logs/power_records.json'))
CAL = 1.731
def q(v, p):
    v = sorted(v); i = p*(len(v)-1); lo = int(i); hi = min(lo+1, len(v)-1); return v[lo]+(v[hi]-v[lo])*(i-lo)
print('== selection on span (a) in the 11 trend-free calibration worlds (pooled)')
null = [r for n, rs in d.items() if n.startswith('null') for r in rs]
for x in (-9, 0.0, 1.0, 1.5, 2.0, 2.5):
    s = [r for r in null if r['a_t'] >= x]
    if not s: continue
    p = sum(r['t_const'] >= CAL and r['fwd_const_pos'] for r in s)/len(s)
    p2 = sum(r['t_const'] >= CAL and r['c_t'] >= 1.0 for r in s)/len(s)
    print(f'  t_a >= {x:4}: n {len(s):5d}  P(pass registered) {p:.3f}  P(pass with t_c>=1) {p2:.3f}')
print('== economic bar on H1 passes (drafter trend worlds; no funding; raw=mean, adj=alpha)')
for n, rs in d.items():
    if not n.startswith('trend'): continue
    ps = [r for r in rs if r['t_const'] >= CAL and r['fwd_const_pos']]
    if not ps: continue
    vals = [2.8*min(r['sr']*r['vol'], r['alpha_const'])*(1-1/r['t_const']**2) for r in ps]
    print(f'  {n:20s} passes {len(ps):3d}  P(bar clears | pass) {sum(v>=0.04 for v in vals)/len(vals):.3f}  min {min(vals):.3f} median {q(vals,.5):.3f}')
print('== alpha t as the decay statistic: c2 = min q05 of t_const over the SR-1.57 worlds')
pub = [n for n in d if n.startswith('trend') and n.endswith('SR 1.57')]
c2 = min(q([r['t_const'] for r in d[n]], 0.05) for n in pub)
print('  c2 =', round(c2, 3), {n: round(q([r['t_const'] for r in d[n]], 0.05), 3) for n in pub})
for n, rs in d.items():
    if not n.startswith('trend'): continue
    T = sum(r['T'] < 0.117 for r in rs)/len(rs)
    A = sum(r['t_const'] < c2 for r in rs)/len(rs)
    print(f'  {n:20s} reject 1.57: T-based {T:.3f}   alpha-t-based {A:.3f}')
