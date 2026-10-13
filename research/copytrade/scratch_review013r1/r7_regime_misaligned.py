"""Review r1 of 013: the quarter-beta remedy when the regime does NOT align with calendar quarters
(crash 2025-11-15 .. 2026-01-15), and a month-beta variant; power cost at k 2. Synthetic only."""
import random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import r1_own_sim as S
import r4_regime as R4
import datetime as dt

def month(t):
    return dt.datetime.fromtimestamp(t / 1000, dt.UTC).strftime('%Y-%m')

def run(sims, b_in, b_out, d, mu, k, bear):
    S.BEAR = bear
    C = S.sched('confirmation')
    r = random.Random(11)
    reg = q = m = 0
    for _ in range(sims):
        ev = S.events(C, mu, S.bear_beta(b_in, b_out), k, S.bear_drift(d), r)
        s = S.stats(ev, S.SPAN['confirmation'])
        reg += S.conf_pass(s)
        for key, fn in (('q', R4.quarter), ('m', month)):
            gs = sorted({fn(e['en'] - S.DAY) for e in ev})
            X = [[1.0] + [e['m'] if fn(e['en'] - S.DAY) == g else 0.0 for g in gs] for e in ev]
            o = R4.ols_k([e['net'] for e in ev], X, [e['w'] for e in ev])
            ok = bool(o and o['t'] is not None and o['a'] > 0 and o['t'] >= max(S.t_crit(o['G'] - 2, 0.95), 1.90) and (s['wo'] or 0) > 0)
            if key == 'q':
                q += ok
            else:
                m += ok
    return reg / sims, q / sims, m / sims

sims = int(sys.argv[1])
mis = (S.ms('2025-11-15'), S.ms('2026-01-15'))
for args in ((2.5, 1.0, 0.04, 0.0, 1.5, mis), (2.0, 1.25, 0.03, 0.0, 1.5, mis), (1.5, 1.5, 0.0, 0.04, 2.0, mis), (1.5, 1.5, 0.0, 0.03, 1.5, mis)):
    a, q, m = run(sims, *args)
    print(f'beta {args[0]}/{args[1]} drift {args[2]} mu {args[3]} k {args[4]} crash 11-15..01-15: registered {a:.3f} quarter-beta {q:.3f} month-beta {m:.3f}', flush=True)
