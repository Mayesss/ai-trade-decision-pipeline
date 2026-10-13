"""D1 scout: SYNTHETIC power for one trial on 013's unread confirmation calendar — "T2 frozen" against a T2 rule with a
price stop (the squeeze atom shrunk from -1 to about -0.55 and made more frequent).

The ONLY real input is data/r013/schedule.json (first-trade times and the premarket / continuation flags: pre-entry
facts, already read by 013's power runs). Imports ct.listings013 READ-ONLY for its estimators (alpha_fit with quarter
betas, CR1 by listing week; t_crit; the market path); run with `python -I -B` so no bytecode is written under
research/copytrade. No outcome of any listing window is read or computed.

Calibration target (013 Result section, cited, discovery): alpha se = alpha / t: T1 1.20 / 0.49 = 2.45 % on 239 events
(65 weeks); T2 3.37 / 1.24 = 2.72 % on 115 events (43 weeks). The model's body scale k and atom rate p are chosen so
that the simulated discovery se matches (the atoms dominate the variance).

Usage: python -I -B d1_power.py <copytrade_root> <out_json>
"""
import json
import math
import random
import statistics
import sys
from pathlib import Path

sys.pycache_prefix = None
ROOT = Path(sys.argv[1])
sys.path.insert(0, str(ROOT))
import ct.listings013 as L  # noqa: E402  (read-only import of the frozen module)

DAY, HOUR = L.DAY, L.HOUR


def schedule(cal):
    sch = json.loads((ROOT / 'data/r013/schedule.json').read_text())
    return sorted(e['first_trade'] for e in sch[cal] if e['first_trade'] and not e['premarket'] and not e['continuation'])


def span(cal):
    a, b = L.CALENDARS[cal]
    return (b - a) / L.YEAR


def sim(sched, mu, k, p, atom, rnd, beta=1.5, drift=0.0, share=1.0, regime=None, cap=None):
    """Events at the real first-trade times (subset with prob `share`, the T2 share), entry +24 h, exit +7 d.
    net = mu - beta*mkt + body, body = shift + k*SIGMA_E*t4.5 (clipped at -cap), or the atom -atom w.p. p.
    shift keeps the event mean at mu. regime = (start, end, beta_in, drift_in)."""
    t0 = min(sched) - DAY
    drift_at = (lambda t: regime[3] if regime[0] <= t < regime[1] else drift) if regime else (lambda t: drift)
    at = L._market_path(t0, max(sched) + 11 * DAY, 1.0, drift_at, rnd)
    shift = (mu + p * atom) / (1 - p)
    cap = atom if cap is None else cap
    out = []
    for i, ft in enumerate(sched):
        if rnd.random() >= share:
            continue
        e_t, x_t = ft + L.ENTRY_DELAY, ft + L.ENTRY_DELAY + L.HOLD
        mkt = math.exp(at(x_t) - at(e_t)) - 1
        b = regime[2] if regime and regime[0] <= ft < regime[1] else beta
        if rnd.random() < p:
            net, liq = -atom, True
        else:
            net, liq = max(-cap, shift - b * mkt + k * L.SIGMA_E * L._tdraw(rnd)), False
        out.append({'net': net, 'week': L.iso_week(ft), 'quarter': L.quarter_of(ft), 'mkt': mkt, 'liquidated': liq,
                    'entry_t': e_t, 'exit_t': x_t, 'id': str(i)})
    return out


def stats(rows, pool=None, atom=1.0):
    fit, betas = L.alpha_fit(rows)
    if fit['alpha'] is None or not fit['se']:
        return None
    pi = sum(r['liquidated'] for r in rows) / len(rows)
    nonliq = [r['net'] for r in rows if not r['liquidated']]
    charge = max(0.0, pool - pi) * (atom + (sum(nonliq) / len(nonliq) if nonliq else 0.0)) if pool is not None else 0.0
    a = fit['alpha'] - charge
    # best-week refit (013's confirmation requires alpha_tail > 0 without the largest positive adjusted week)
    adj = [r['net'] - betas.get(r['quarter'], 0.0) * r['mkt'] for r in rows]
    drop = L.largest_positive_week(adj, [r['week'] for r in rows])
    keep = [r for r in rows if r['week'] != drop]
    wo = L.alpha_fit(keep)[0]['alpha'] if keep else None
    return {'alpha': fit['alpha'], 'se': fit['se'], 'G': fit['G'], 'n': fit['n'], 'a_tail': a, 't_tail': a / fit['se'],
            'wo_ok': wo is not None and wo - charge > 0, 'pi': pi}


def crit(G, q, cal):
    return max(L.t_crit(G - 2, q), cal) if G >= 3 else math.inf


def run_world(dsch, csch, w, draws, seed):
    rnd = random.Random(seed)
    res = []
    for _ in range(draws):
        d = sim(dsch, w['mu'], w['k'], w['p'], w['atom'], rnd, share=w['share'], drift=w.get('drift', 0.0),
                regime=w.get('regime_d'), beta=w.get('beta', 1.5))
        c = sim(csch, w['mu'], w['k'], w['p'], w['atom'], rnd, share=w['share'], drift=w.get('drift', 0.0),
                regime=w.get('regime_c'), beta=w.get('beta', 1.5))
        if len(c) < 12 or len(d) < 12:
            continue
        pool = (sum(r['liquidated'] for r in d) + sum(r['liquidated'] for r in c)) / (len(d) + len(c))
        sd, sc = stats(d), stats(c, pool=pool, atom=w['atom'])
        if sd is None or sc is None:
            continue
        res.append({'d_se': sd['se'], 'c_se': sc['se'], 'c_t': sc['t_tail'], 'c_G': sc['G'], 'c_n': sc['n'],
                    'c_ok_other': sc['a_tail'] > 0 and sc['wo_ok'] and sc['n'] >= 10 and sc['G'] >= 5})
    return res


def rates(res, q, cal):
    n = len(res)
    return sum(r['c_ok_other'] and r['c_t'] >= crit(r['c_G'], q, cal) for r in res) / n if n else None


def q_t(res, q):
    ts = sorted((r['c_t'] if r['c_ok_other'] else -99.0) for r in res)
    return ts[min(len(ts) - 1, int(q * len(ts)))]


def main():
    out_path = Path(sys.argv[2])
    dsch, csch = schedule('discovery'), schedule('confirmation')
    out = {'n_discovery_primary': len(dsch), 'n_confirmation_primary': len(csch), 'worlds': []}

    def go(name, draws=600, seed=1, **w):
        res = run_world(dsch, csch, w, draws, seed)
        row = {'name': name, **{k: v for k, v in w.items() if not k.startswith('regime')},
               'draws': len(res), 'disc_se_median': statistics.median(r['d_se'] for r in res),
               'conf_se_median': statistics.median(r['c_se'] for r in res),
               'conf_n_median': statistics.median(r['c_n'] for r in res),
               'conf_G_median': statistics.median(r['c_G'] for r in res),
               'pass_q95_cal2.05': rates(res, 0.95, 2.05), 'pass_q975_cal0': rates(res, 0.975, 0.0),
               'pass_q975_cal2.40': rates(res, 0.975, 2.40), 'q95_t': q_t(res, 0.95), 'q975_t': q_t(res, 0.975)}
        out['worlds'].append(row)
        print(json.dumps(row), flush=True)
        return row

    # 1) calibration of the variance to 013's cited discovery se (T1: share 1, p 6.6 %; T2: share 0.48)
    for k in (1.5, 2.0, 2.5, 3.0):
        go(f'calib T1 k{k} p0.066', draws=300, mu=0.0, k=k, p=0.066, atom=1.0, share=1.0)
    for k, p in ((2.0, 0.03), (2.5, 0.03), (2.0, 0.045), (2.5, 0.045), (1.5, 0.066), (2.0, 0.066)):
        go(f'calib T2 k{k} p{p}', draws=300, mu=0.0, k=k, p=p, atom=1.0, share=0.48)
    open(out_path, 'w').write(json.dumps(out, indent=1))


def main_power():
    out_path = Path(sys.argv[2])
    K, P = float(sys.argv[4]), float(sys.argv[5])          # calibrated body scale and T2 atom rate
    dsch, csch = schedule('discovery'), schedule('confirmation')
    out = {'k': K, 'p': P, 'worlds': []}
    CONF_MIS = (L.ms('2025-11-15'), L.ms('2026-01-16'))
    DISC = (L.ms('2024-12-09'), L.ms('2025-04-08'))

    def go(name, draws=600, seed=7, **w):
        res = run_world(dsch, csch, w, draws, seed)
        row = {'name': name, **{k: v for k, v in w.items() if not k.startswith('regime')}, 'draws': len(res),
               'conf_se_median': statistics.median(r['c_se'] for r in res),
               'conf_n_median': statistics.median(r['c_n'] for r in res),
               'conf_G_median': statistics.median(r['c_G'] for r in res),
               'pass_q95_cal2.05': rates(res, 0.95, 2.05), 'pass_q975_cal0': rates(res, 0.975, 0.0),
               'q95_t': q_t(res, 0.95), 'q975_t': q_t(res, 0.975)}
        out['worlds'].append(row)
        print(json.dumps(row), flush=True)

    rules = {'T2frozen': dict(atom=1.0, p=P, k=K), 'T2stop50': dict(atom=0.55, p=min(0.5, 2.5 * P), k=K)}
    for share in (0.48,):
        for rn, r in rules.items():
            # nulls (size; calibration of the single-stage critical value)
            go(f'{rn} null', mu=0.0, share=share, **r)
            go(f'{rn} null drift2', mu=0.0, share=share, drift=0.02, **r)
            go(f'{rn} null regime misaligned b2.5/1.0', mu=0.0, share=share, beta=1.0,
               regime_d=(*DISC, 2.5, 0.03), regime_c=(*CONF_MIS, 2.5, 0.03), **r)
            for mu in (0.01, 0.02, 0.034, 0.05, 0.08):
                go(f'{rn} mu{mu}', mu=mu, share=share, **r)
    open(out_path, 'w').write(json.dumps(out, indent=1))


if __name__ == '__main__':
    if len(sys.argv) > 3 and sys.argv[3] == '--power':
        main_power()
    else:
        main()
