"""Probe 7 (SYNTHETIC + the frozen REALISTIC_WEEK constants only): sensitivity of the tail-aware Kelly
(shortvol_sim.kelly_atoms, fat and base worlds, VRP 0.05/0.075/0.10) to (a) the registered 21 atoms,
(b) + one single-gap book week of -45 % (probe 6: a -50 % gap between hedges), (c) + two such weeks."""
import statistics, sys
sys.path.insert(0, '.')
from ct import shortvol as sv, shortvol_sim as sim
atoms = list(sv.REALISTIC_WEEK.values())
for name, params, sd in (('base', sim.PARAMS, 11), ('fat', sim.FAT, 31)):
    bank = sim.gen_bank(3000, sd, params); nz = sim.noises(bank, sd + 1)
    g = [(m, sim.gross_ratio(sim.book(bank, nz, 'H24', m))) for m in sim.M_GRID]
    for v in (0.05, 0.075, 0.10):
        b = sim.book(bank, nz, 'H24', sim._m_for(g, v))
        k = [sim.kelly_atoms(b, a, 1 / sv.HIST_WEEKS) for a in (atoms, atoms + [-0.45], atoms + [-0.45, -0.45])]
        print(name, v, 'mean/wk', round(statistics.fmean(r['r'] for r in b) * 100, 3), 'tail Kelly (a,b,c)', k, 'quarter', [round(x / 4, 3) for x in k], flush=True)
