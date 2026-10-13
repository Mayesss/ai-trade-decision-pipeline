"""Probe 1 (SYNTHETIC only): bank-to-bank dispersion of the registered floor procedure.
Runs ct.shortvol_sim.calibrate_floors('H24') on new seeds (FAT world, 20,000 weeks, 6,000 draws)."""
import json, sys, time
sys.path.insert(0, '.')
from ct import shortvol_sim as sim
out = {}
for s in [int(x) for x in sys.argv[1:]]:
    t0 = time.time()
    out[s] = sim.calibrate_floors('H24', n_weeks=20000, seed=s, draws=6000)
    out[s]['secs'] = round(time.time() - t0)
    print(s, out[s], flush=True)
