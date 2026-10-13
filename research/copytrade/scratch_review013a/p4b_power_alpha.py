"""SYNTHETIC. Confirmation power, raw-net t (registered) vs OLS-alpha t (estimated beta), real event times, overlap present."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent)); sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from p4_beta import run, L
for lab, sm, se in (('k2 (market 20.8, idio 16.6)', 0.208, 0.166), ('idio-heavy (market 10.4, idio 16.6)', 0.104, 0.166),
                    ('idio x3 (market 10.4, idio 24.9)', 0.104, 0.249)):
    for mu in (0.0, 0.04, 0.08):
        print('confirmation', lab, f'mu {mu}', run('confirmation', L.Q_CONFIRM, 1.0, 0.0, mu=mu, sm=sm, se=se, sims=2000), flush=True)
