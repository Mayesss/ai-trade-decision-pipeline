"""Probes 2 (H2 decay statistic) and 3 (span (a) and designer knowledge): trend worlds with the trend switched on or
off by span, and with a market drift added in the forward years. Synthetic only. Writes p23_records.json."""
import json
import multiprocessing as mp
import sys
import time

import common as C

ALL = ('2023-09-01', '2027-12-31')
A = ('2023-09-01', '2026-06-30')          # trend in the warm-up and (a): the designer-known year
CC = ('2026-07-01', '2027-12-31')
K60 = {0.0: 0.045, 0.5: 0.079, 0.75: 0.096, 1.0: 0.116, 1.5: 0.161, 1.57: 0.170}   # the drafter's pilot (h60)
K20 = {1.57: 0.212}
K120 = {1.57: 0.156}

WORLDS = []
# --- probe 3: who carries the pass
WORLDS += [
    {'name': 'S1 (a) trend SR~1.5, (c) dead', 'half_life': 60, 'kappa_windows': [(*A, 0.161)]},
    {'name': 'S2 (a) trend SR~2.5, (c) dead', 'half_life': 60, 'kappa_windows': [(*A, 0.25)]},
    {'name': 'S3 (a) trend SR~1.5, (c) SR~0.75', 'half_life': 60,
     'kappa_windows': [(*A, 0.161), (*CC, 0.096)]},
    {'name': 'S4 (a) dead, (c) SR~1', 'half_life': 60, 'kappa_windows': [(*CC, 0.116)]},
    {'name': 'S5 both SR~1', 'half_life': 60, 'kappa': 0.116},
    {'name': 'S6 both SR~0.75', 'half_life': 60, 'kappa': 0.096},
    {'name': 'S7 both SR~1.5', 'half_life': 60, 'kappa': 0.161},
    {'name': 'S8 both dead (kappa 0)', 'half_life': 60},
]
# --- probe 2: H2 with and without a forward market drift
for s in (0.0, 0.5, 0.75, 1.0, 1.57):
    for drift in (0.0, -0.4, 0.4):
        w = {'name': f'H h60 SR {s:g} fwd drift {drift:+.0%}', 'half_life': 60, 'kappa': K60[s]}
        if drift:
            w['drift'] = [('2025-03-20', '2027-12-31', drift)]
        WORLDS.append(w)
WORLDS += [
    {'name': 'H h20 SR 1.57 fwd drift +0%', 'half_life': 20, 'kappa': K20[1.57]},
    {'name': 'H h120 SR 1.57 fwd drift +0%', 'half_life': 120, 'kappa': K120[1.57]},
    {'name': 'H h120 SR 1.57 fwd drift -40%', 'half_life': 120, 'kappa': K120[1.57],
     'drift': [('2025-03-20', '2027-12-31', -0.4)]},
]


def main():
    draws = int(sys.argv[1]) if len(sys.argv) > 1 else 600
    only = sys.argv[2] if len(sys.argv) > 2 else ''
    keep = {}
    t0 = time.time()
    with mp.Pool(10) as pool:
        for i, w in enumerate(WORLDS):
            if only and not w['name'].startswith(only):
                continue
            recs = C.run_world(w, draws, 3_000_000 + 10_000 * i, pool, cells=False)
            keep[w['name']] = recs
            print(w['name'], f"t {C.mean([r['t'] for r in recs]):.2f} T {C.mean([r['T'] for r in recs]):.2f} "
                  f"sr {C.mean([r['sr'] for r in recs]):.2f} t_a {C.mean([r['t_a'] for r in recs]):.2f} "
                  f"t_c {C.mean([r['t_c'] for r in recs]):.2f} {time.time() - t0:.0f}s", flush=True)
            json.dump(keep, open(f'p23_records{("_" + only) if only else ""}.json', 'w'))


if __name__ == '__main__':
    main()
