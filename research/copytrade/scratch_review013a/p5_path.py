"""SYNTHETIC. The registered decision path (discovery criteria 0-7 via listings013.discovery_criteria, D10 carry rule,
raw-net confirmation via listings013.confirm, deploy bar via listings013.deploy_bar/book) in beta worlds and in a
'decayed edge' world, on the real event times with a continuous market (cross-week overlap present)."""
import random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from p2_overlap import S, tdraw, market_increments, L

YRS = {c: (L.CALENDARS[c][1] - L.CALENDARS[c][0]) / L.YEAR for c in ('discovery', 'confirmation')}

def draw(cal, mu, beta, drift, se, sm, r, p_t2=0.5):
    ft = S[cal]; wk = [L.iso_week(t) for t in ft]
    ent = [t + L.ENTRY_DELAY for t in ft]; ext = [t + L.HOLD for t in ent]
    M = market_increments(ent + ext, sm, r)
    rows = []
    for i, (a, b) in enumerate(zip(ent, ext)):
        u = -(M[b] - M[a]) + drift
        net = max(-1.0, min(1.0, mu + beta * u + se * tdraw(r)))
        rows.append({'id': str(i), 'net': net, 'week': wk[i], 'entry_t': a, 'exit_t': b, 'adj': net - u,
                     't2': r.random() < p_t2})
    return {'T1': rows, 'T2': [x for x in rows if x['t2']]}

def stats(rows, cal, q):
    return L.trial_stats(rows, [(x['adj'], x['week']) for x in rows], YRS[cal], q)

def path(disc, conf, sims=1500, seed=3):
    r = random.Random(seed); h = {'disc_pass': 0, 'confirm': 0, 'confirm_paper': 0}
    for _ in range(sims):
        d = draw('discovery', *disc, r)
        passes = {}
        for tr in L.TRIALS:
            _, ok, _ = L.discovery_criteria(stats(d[tr], 'discovery', L.Q_DISCOVERY)); passes[tr] = ok
        dec = L.decide_discovery(passes)
        if dec[0] != 'read_confirmation':
            continue
        h['disc_pass'] += 1
        c = draw('confirmation', *conf, r)[dec[1]]
        s = stats(c, 'confirmation', L.Q_CONFIRM)
        ok, _ = L.confirm(s)
        if ok:
            h['confirm'] += 1
            bb = L.book(c, YRS['confirmation']); bs = L.book(c, YRS['confirmation'])
            h['confirm_paper'] += L.decide_confirmation(ok, L.deploy_bar(s, bb, bs)) == 'confirm_paper'
    return {k: round(v / sims, 3) for k, v in h.items()}

if __name__ == '__main__':
    se, sm = 0.083, 0.104
    print('A. beta-only in both calendars (mu 0):')
    for beta in (1.0, 1.5, 2.0):
        for drift in (0.01, 0.02):
            print(f'   beta {beta} drift/wk {drift}', path((0.0, beta, drift, se, sm), (0.0, beta, drift, se, sm)), flush=True)
    print('B. decayed edge: discovery mu 4% (no drift); confirmation mu 0 in a bear:')
    for beta in (1.0, 1.5):
        for drift in (0.0, 0.01, 0.02):
            print(f'   beta {beta} conf drift/wk {drift}', path((0.04, beta, 0.0, se, sm), (0.0, beta, drift, se, sm)), flush=True)
    print('C. reference: real edge mu 4% both, beta 1, no drift', path((0.04, 1.0, 0.0, se, sm), (0.04, 1.0, 0.0, se, sm)))
