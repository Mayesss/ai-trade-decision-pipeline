#!/bin/bash
# Reviewer B, 016 R2: every Revision-1 probe re-run against Revision 2 (fresh stand-ins under rb016r2; the R2 fetch
# harness). Network denied by the caller (sandbox-exec).
export RB_S=/private/tmp/claude-502/-Users-mayess-sammoud-Documents-persogit-ai-trade-decision-pipeline/7b2e3f5e-bd1a-4fe0-b780-0c6d54079801/scratchpad/rb016r2
export HARNESS=r2_fetch_harness.py.txt
H=$(cd "$(dirname "$0")" && pwd)
for p in probe_r1_b1 probe_r1_m1 probe_r1_m3 probe_r1_m4 probe_r1_minors; do echo "################ $p"; "$H/$p.sh" 2>&1; done
