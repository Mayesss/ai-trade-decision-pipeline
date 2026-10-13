"""Within-window month change of the alt / reversal loading (the N3 analogue for the factors), registered vs fixes."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sims = int(sys.argv[1]) if len(sys.argv) > 1 else 4000
sys.argv = [sys.argv[0], '0']
import probe_stats as P  # noqa: E402
AUGM = lambda i: P.MON[i] == '2026-08'  # noqa: E731
P.rate_v('N14 alt loading Jul 0 / Aug 0.4 / Sep 0; U drift +0.3 %/day in Aug only', P.VARIANTS, sims=sims, seed=71,
         alt_load=lambda i: 0.4 if AUGM(i) else 0.0, u_drift=lambda i: 0.003 if AUGM(i) else 0.0)
P.rate_v('N15 rev loading Jul 0 / Aug 1.0 / Sep 0; REV drift +0.15 %/day in Aug only', P.VARIANTS, sims=sims, seed=72,
         rev_load=lambda i: 1.0 if AUGM(i) else 0.0, rev_drift=lambda i: 0.0015 if AUGM(i) else 0.0)
P.rate_v('N9 persistence 3 %, rho 0.97', P.VARIANTS, sims=sims, seed=73, slow=0.03, slow_rho=0.97)
