#!/bin/bash
# Reviewer B, 015, B1: the guard opens -- and --run completes -- in ANY clone that has the data, and the rows stay in
# the clone. "ORIGIN" is the synthetic stand-in for the real repository (make_synth_clone.py: real history, byte-
# identical r015 code, synthetic data); "CLONE" is `git clone --local ORIGIN` + the data copied (as commit_sim.sh does).
set -u
ORIGIN=$1; S=$2
rm -rf "$S/clone_b1"; git clone -q --local "$ORIGIN" "$S/clone_b1"
cp -R "$ORIGIN/research/alpha/data" "$S/clone_b1/research/alpha/data"
cd "$S/clone_b1" && git checkout -q alpha-discovery
echo "== CLONE --check";  python3 -I research/alpha/r015_run.py --check
echo "== CLONE --run (synthetic data)"; python3 -I research/alpha/r015_run.py --run 2>&1 | grep -E '^decision|counts before' | cut -c1-120
echo "== CLONE rows:"; git log --oneline -3
echo "== ORIGIN --check after the clone's run (run once should refuse here):"; cd "$ORIGIN" && python3 -I research/alpha/r015_run.py --check
echo "== ORIGIN sees the clone's result commit?"; git cat-file -t "$(git -C "$S/clone_b1" rev-parse HEAD)" 2>&1 | head -1
