#!/bin/bash
# Reviewer B, 016, staleness and the 015 interplay. Clones of the real repository (code and registrations only; no data).
# $1 real repo, $2 scratch
set -u
REAL=$1; S=$2; G="git -c user.name=rb -c user.email=rb@rb"
R=$S/stale; rm -rf "$R"; git clone -q --local "$REAL" "$R"; cd "$R/research/alpha"; git checkout -q alpha-discovery
echo "== clone at $(git log -1 --format=%h) (016 DRAFT committed): $(python3 -I tests/test_r016.py 2>&1 | tail -1)"
sed -i '' 's/^\*\*Status: DRAFT\*\*/**Status: REGISTERED 2026-10-20**/' registrations/016-crypto-trend-verbatim.md
$G commit -q -am "sim: 016 REGISTERED"
echo "== after a REGISTERED commit: $(python3 -I tests/test_r016.py 2>&1 | tail -1)"
echo "== the real guard in the REGISTERED clone (no data): "; python3 -I r016_run.py --check | cut -c1-120
echo "== 015's revision IN PROGRESS (the real working tree's r015 code copied in, r015_hmm.py deleted; code only):"
cp "$REAL"/research/alpha/r015_core.py "$REAL"/research/alpha/r015_power.py "$REAL"/research/alpha/r015_run.py .
[ -e "$REAL/research/alpha/r015_hmm.py" ] || rm -f r015_hmm.py
git status --short | sed 's/^/   /'
python3 -I -c "import sys; sys.path.insert(0, '.'); import r016_run as R; print('   016 shadow_modules:', R.shadow_modules(R.HERE))"
python3 -I r016_run.py --check | grep '(d)' | cut -c1-150
echo "   016 suite with 015 mid-revision: $(python3 -I tests/test_r016.py 2>&1 | tail -1)"
echo "== a stale __pycache__/r015_hmm.cpython-314.pyc after the deletion (the real tree has none today):"
ls "$REAL"/research/alpha/__pycache__/ | sed 's/^/   /'
