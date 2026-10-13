"""Review r1 of 013: the registered deploy rule (L.deploy, unmodified) after a confirm, on synthetic calendars.
Rows carry id/net/entry_t/exit_t/liquidated. Pooled = reported (mu 0, beta 1.5, no drift), discovery, confirmation.
Compares the registered (raw-net) deploy with the same rule fed the market-adjusted nets (net - beta_hat * mkt).
Usage: python -I r5_deploy.py WORLD SIMS"""
import json, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import r1_own_sim as S
from ct import listings013 as L

def rows(ev, adj_b=None):
    return [{'id': str(i), 'net': (e['net'] - adj_b * e['m']) if adj_b is not None else e['net'], 'entry_t': e['en'],
             'exit_t': e['ex'], 'liquidated': e['liq']} for i, e in enumerate(ev)]

W = {
  'squeeze_p2_null':        dict(mu_c=0.0, b=2.0, d=0.0, liq=0.02),
  'squeeze_p2_null_bear':   dict(mu_c=0.0, b=2.0, d=0.02, liq=0.02),
  'nosq_null_bear':         dict(mu_c=0.0, b=2.0, d=0.02, liq=0.0),
  'alpha3_flat':            dict(mu_c=0.03, b=2.0, d=0.0, liq=0.0),
  'alpha3_bear':            dict(mu_c=0.03, b=2.0, d=0.02, liq=0.0),
  'alpha4_flat':            dict(mu_c=0.04, b=1.5, d=0.0, liq=0.0),
  'alpha4_bear':            dict(mu_c=0.04, b=1.5, d=0.02, liq=0.0),
}
name, sims = sys.argv[1], int(sys.argv[2])
w = W[name]
D, C, R = S.sched('discovery'), S.sched('confirmation'), S.sched('reported')
r = random.Random(99)
n_conf = dep = dep_adj = 0
for _ in range(sims):
    c = S.events(C, w['mu_c'], S.CONST(w['b']), 1.5, S.DRIFT(w['d']), r, w['liq'])
    s = S.stats(c, S.SPAN['confirmation'])
    if not S.conf_pass(s):
        continue
    n_conf += 1
    d = S.events(D, 0.04, S.CONST(w['b']), 1.5, S.DRIFT(0.0), r, w['liq'])
    rp = S.events(R, 0.0, S.CONST(1.5), 1.5, S.DRIFT(0.0), r, w['liq'])
    pooled = {'reported': (rows(rp), rows(rp), S.SPAN['reported']), 'discovery': (rows(d), rows(d), S.SPAN['discovery']),
              'confirmation': (rows(c), rows(c), S.SPAN['confirmation'])}
    dep += L.deploy(rows(c), rows(c), pooled, S.SPAN['confirmation'])['deploy']
    b = s['o']['b']
    pooled_a = dict(pooled, confirmation=(rows(c, b), rows(c, b), S.SPAN['confirmation']))
    dep_adj += L.deploy(rows(c, b), rows(c, b), pooled_a, S.SPAN['confirmation'])['deploy']
print(name, sims, json.dumps({'confirm': n_conf / sims, 'deploy_registered': dep / sims, 'deploy_given_confirm': dep / n_conf if n_conf else None,
                              'deploy_adjusted_series': dep_adj / sims, 'deploy_adj_given_confirm': dep_adj / n_conf if n_conf else None}))
