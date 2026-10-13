"""Review r1 of 013: the regime-beta worlds of r4 run through the REGISTERED code (L.trial_stats, L.confirm)
instead of the reviewer's own OLS, to rule out an implementation difference. Synthetic only."""
import random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import r1_own_sim as S
from ct import listings013 as L
sims = int(sys.argv[1])
C = S.sched('confirmation')
for b_in, b_out, d, k in ((2.5, 1.0, 0.03, 1.5), (2.0, 1.25, 0.02, 1.5), (2.0, 1.25, 0.03, 1.5)):
    r = random.Random(3)
    hits = 0
    for _ in range(sims):
        ev = S.events(C, 0.0, S.bear_beta(b_in, b_out), k, S.bear_drift(d), r)
        rows = [{'id': str(i), 'net': e['net'], 'week': e['w'], 'entry_t': e['en'], 'exit_t': e['ex'], 'mkt': e['m']} for i, e in enumerate(ev)]
        ok, _ = L.confirm(L.trial_stats(rows, S.SPAN['confirmation'], L.Q_CONFIRM))
        hits += ok
    print(f'registered path: beta in/out {b_in}/{b_out}, crash drift {d}/wk, k {k}: CONFIRM rate {hits / sims:.3f} ({sims} draws)', flush=True)
