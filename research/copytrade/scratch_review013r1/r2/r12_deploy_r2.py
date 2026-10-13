"""Revision 2 recheck of r5_deploy.py: the revised L.trial_stats(pool) + L.confirm + L.deploy(betas) after a confirm."""
import json, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import r1_own_sim as S
from ct import listings013 as L
W = {'squeeze_p2_null_bear': dict(mu_c=0.0, b=2.0, d=0.02, liq=0.02), 'nosq_null_bear': dict(mu_c=0.0, b=2.0, d=0.02, liq=0.0),
     'alpha4_flat': dict(mu_c=0.04, b=1.5, d=0.0, liq=0.0), 'alpha4_bear': dict(mu_c=0.04, b=1.5, d=0.02, liq=0.0)}
def rows(ev):
    return [{'id': str(i), 'net': e['net'], 'entry_t': e['en'], 'exit_t': e['ex'], 'liquidated': e['liq'], 'mkt': e['m'],
             'week': e['w'], 'quarter': L.quarter_of(e['en'] - L.ENTRY_DELAY)} for i, e in enumerate(ev)]
name, sims = sys.argv[1], int(sys.argv[2]); w = W[name]
D, C, R = S.sched('discovery'), S.sched('confirmation'), S.sched('reported')
r = random.Random(5); nc = dep = 0
for _ in range(sims):
    c = rows(S.events(C, w['mu_c'], S.CONST(w['b']), 1.5, S.DRIFT(w['d']), r, w['liq']))
    d = rows(S.events(D, 0.04, S.CONST(w['b']), 1.5, S.DRIFT(0.0), r, w['liq']))
    rp = rows(S.events(R, 0.0, S.CONST(1.5), 1.5, S.DRIFT(0.0), r, w['liq']))
    pool = sum(x['liquidated'] for x in c + d + rp) / len(c + d + rp)
    s = L.trial_stats(c, S.SPAN['confirmation'], L.Q_CONFIRM, pool=pool)
    if not L.confirm(s)[0]:
        continue
    nc += 1
    pooled = {'reported': (rp, rp, S.SPAN['reported']), 'discovery': (d, d, S.SPAN['discovery']), 'confirmation': (c, c, S.SPAN['confirmation'])}
    dep += L.deploy(c, c, pooled, S.SPAN['confirmation'], s['betas'])['deploy']
print(name, sims, json.dumps({'confirm': nc / sims, 'deploy_joint': dep / sims, 'deploy_given_confirm': dep / nc if nc else None}))
