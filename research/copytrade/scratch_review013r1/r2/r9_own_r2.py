"""Review r1, Revision 2 recheck: an INDEPENDENT implementation of the Revision 2 decision path (own code; no
import of ct.listings013): alpha with quarter betas (own merge rule: quarters < 10 events join the previous group,
a short first group joins the next), CR1 by week, df G-2; discovery criteria 2-4 on adj, 5-6 on min(raw, adjusted);
confirmation on the tail-adjusted alpha (pool = discovery + confirmation + reported-block liquidations), CAL_C 2.05.
Usage: python -I r9_own_r2.py WORLD SIMS"""
import datetime as dt, json, math, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import r1_own_sim as S
import r4_regime as R4

CAL_C = 2.05
REG = (S.ms('2025-10-06'), S.ms('2026-03-01'))
DREG = (S.ms('2024-12-09'), S.ms('2025-04-08'))


def q_of(t):
    d = dt.datetime.fromtimestamp(t / 1000, dt.UTC)
    return f'{d.year}Q{(d.month - 1) // 3 + 1}'


def groups(qs):
    cnt = {}
    for q in qs:
        cnt[q] = cnt.get(q, 0) + 1
    gs = []
    for q in sorted(cnt):
        if gs and cnt[q] < 10:
            gs[-1].append(q)
        else:
            gs.append([q])
    if len(gs) > 1 and sum(cnt[q] for q in gs[0]) < 10:
        gs = [gs[0] + gs[1]] + gs[2:]
    return {q: i for i, g in enumerate(gs) for q in g}


def fit(ev):
    gm = groups([e['q'] for e in ev])
    ng = len(set(gm.values()))
    X = [[1.0] + [e['m'] if gm[e['q']] == j else 0.0 for j in range(ng)] for e in ev]
    y = [e['net'] for e in ev]
    o = R4.ols_k(y, X, [e['w'] for e in ev])
    if o is None:
        return None, None
    # recover betas by a second solve (own): OLS normal equations
    n, k = len(y), len(X[0])
    A = [[sum(X[i][a] * X[i][b] for i in range(n)) for b in range(k)] for a in range(k)]
    bt = R4.solve(A, [sum(X[i][a] * y[i] for i in range(n)) for a in range(k)])
    beta = {q: bt[1 + j] for q, j in gm.items()}
    o['se'] = o['a'] / o['t'] if o['t'] else None
    o['n'] = len(ev)
    return o, beta


def stats(ev, span, pool=None):
    o, beta = fit(ev)
    adj = [e['net'] - beta[e['q']] * e['m'] for e in ev]
    cl = [e['w'] for e in ev]
    ws = {}
    for a, c in zip(adj, cl):
        ws[c] = ws.get(c, 0.0) + a
    pos = [(v, c) for c, v in ws.items() if v > 0]
    drop = max(pos)[1] if pos else None
    o2, _ = fit([e for e in ev if e['w'] != drop])
    weeks = sorted(set(cl))
    first = set(weeks[:(len(weeks) + 1) // 2])
    h1 = [a for a, c in zip(adj, cl) if c in first]
    h2 = [a for a, c in zip(adj, cl) if c not in first]
    sa = sorted(adj)
    med = (sa[len(sa) // 2] + sa[(len(sa) - 1) // 2]) / 2
    raw_mean = sum(e['net'] for e in ev) / len(ev)
    adj_mean = sum(adj) / len(adj)
    tk = S.take(ev)
    ids = {id(e) for e in tk}
    book_raw = sum(0.025 * e['net'] for e in tk) / span
    book_adj = sum(0.025 * a for e, a in zip(ev, adj) if id(e) in ids) / span
    pi = sum(e['liq'] for e in ev) / len(ev)
    nl = [e['net'] for e in ev if not e['liq']]
    charge = max(0.0, pool - pi) * (1 + (sum(nl) / len(nl) if nl else 0.0)) if pool is not None else 0.0
    return {'o': o, 'wo': (o2['a'] - charge) if o2 else None, 'med': med, 'h1': sum(h1) / len(h1), 'h2': sum(h2) / len(h2) if h2 else None,
            'econ_mean': min(raw_mean, adj_mean), 'econ_book': min(book_raw, book_adj), 'a_tail': o['a'] - charge,
            't_tail': (o['a'] - charge) / o['se'] if o['se'] else None, 'wo_plain': o2['a'] if o2 else None}


def disc_pass(s):
    o = s['o']
    return (o['n'] >= 20 and o['G'] >= 10 and o['a'] > 0 and o['t'] >= S.t_crit(o['G'] - 2, 0.975) and s['med'] > 0
            and s['h1'] > 0 and (s['h2'] or 0) > 0 and (s['wo_plain'] or 0) > 0 and s['econ_mean'] >= 0.01 and s['econ_book'] >= 0.03)


def conf_pass(s):
    o = s['o']
    return (o['n'] >= 10 and o['G'] >= 5 and s['a_tail'] > 0 and s['t_tail'] is not None
            and s['t_tail'] >= max(S.t_crit(o['G'] - 2, 0.95), CAL_C) and (s['wo'] or 0) > 0)


def events(sch, mu, b_out, b_in, k, d_out, d_in, reg, r, liq=0.0):
    ev = S.events(sch, mu, (lambda ft, m: b_in if reg and reg[0] <= ft < reg[1] else b_out), k,
                  (lambda t: d_in if reg and reg[0] <= t < reg[1] else d_out), r, liq)
    for e, ft in zip(ev, sch):
        e['q'] = q_of(ft)
    return ev


W = {
    'regime_b25_10_crash3_k15': dict(mu_d=0, mu_c=0, b_out=1.0, b_in=2.5, k=1.5, d=0.0, d_in=0.03, reg=True, liq=0.0),
    'regime_b25_10_crash3_k1': dict(mu_d=0, mu_c=0, b_out=1.0, b_in=2.5, k=1.0, d=0.0, d_in=0.03, reg=True, liq=0.0),
    'power_mu4_k15': dict(mu_d=0.04, mu_c=0.04, b_out=1.5, b_in=1.5, k=1.5, d=0.0, d_in=0.0, reg=False, liq=0.0),
    'squeeze_p2_k15': dict(mu_d=0, mu_c=0, b_out=1.5, b_in=1.5, k=1.5, d=0.0, d_in=0.0, reg=False, liq=0.02),
    'squeeze_p1_k1': dict(mu_d=0, mu_c=0, b_out=1.5, b_in=1.5, k=1.0, d=0.0, d_in=0.0, reg=False, liq=0.01),
}
name, sims = sys.argv[1], int(sys.argv[2])
w = W[name]
D, C, Rp = S.sched('discovery'), S.sched('confirmation'), S.sched('reported')
r = random.Random(2024)
tal = {'disc': 0, 'conf_alone': 0, 'conf_given': 0}
for _ in range(sims):
    d = events(D, w['mu_d'], w['b_out'], w['b_in'], w['k'], w['d'], w['d_in'], DREG if w['reg'] else None, r, w['liq'])
    c = events(C, w['mu_c'], w['b_out'], w['b_in'], w['k'], w['d'], w['d_in'], REG if w['reg'] else None, r, w['liq'])
    nrep = sum(r.random() < w['liq'] for _ in Rp) if w['liq'] else 0
    p = {}
    for tr in ('T1', 'T2'):
        dd = d if tr == 'T1' else [e for e in d if e['t2']]
        p[tr] = disc_pass(stats(dd, S.SPAN['discovery']))
    pool1 = (sum(e['liq'] for e in d) + sum(e['liq'] for e in c) + nrep) / (len(d) + len(c) + len(Rp))
    tal['conf_alone'] += conf_pass(stats(c, S.SPAN['confirmation'], pool1))
    car = 'T1' if p['T1'] else ('T2' if p['T2'] else None)
    if car:
        tal['disc'] += 1
        dd = d if car == 'T1' else [e for e in d if e['t2']]
        cc = c if car == 'T1' else [e for e in c if e['t2']]
        pool = (sum(e['liq'] for e in dd) + sum(e['liq'] for e in cc) + nrep) / (len(dd) + len(cc) + len(Rp))
        tal['conf_given'] += conf_pass(stats(cc, S.SPAN['confirmation'], pool))
print(name, sims, json.dumps({'disc': tal['disc'] / sims, 'conf_alone_T1': tal['conf_alone'] / sims,
      'conf_given': tal['conf_given'] / tal['disc'] if tal['disc'] else None, 'joint': tal['conf_given'] / sims}))
