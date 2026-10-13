#!/bin/bash
# Reviewer B, 015: (1) staleness -- the suite in a `git clone --local` at 012eff7 (DRAFT, committed) and after a
# REGISTERED commit; (2) B1 with the REAL data: the same clone, with research/alpha/data symlinked to the real data
# tree, gets "guard open" from `--check` (hashes only; --run is NOT executed). The real repository is only read.
set -u
REPO=$1; S=$2
C=$S/c1; rm -rf "$C"; git clone -q --local "$REPO" "$C"; cd "$C"
git -c advice.detachedHead=false checkout -q 012eff7 && git checkout -q -b sim
ln -s "$REPO/research/alpha/data" research/alpha/data
echo "== suite at DRAFT (committed):"; python3 -I research/alpha/tests/test_r015.py 2>&1 | tail -1
sed -i '' 's/^\*\*Status: DRAFT/**Status: REGISTERED/' research/alpha/registrations/015-month-end-liquidity-cycle.md
git -c user.name=rb -c user.email=rb@rb commit -q -am "sim REGISTERED"
echo "== suite at REGISTERED (committed):"; python3 -I research/alpha/tests/test_r015.py 2>&1 | tail -1
echo "== real r015_run.py --check in this CLONE (outside the real repository; real data through a symlink):"
python3 -I research/alpha/r015_run.py --check
cd "$S" && rm -rf "$C"
