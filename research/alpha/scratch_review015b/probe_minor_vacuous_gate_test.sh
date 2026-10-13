#!/bin/bash
# Reviewer B, 015, m5: the loader-gate tests are vacuous -- they pass a wrong hash ('x'*64), so the hash check refuses
# even with the gate deleted. Mutation test in a synthetic clone: delete both _gate() calls, run the suite.
set -u
ORIGIN=$1; S=$2
R=$S/repo_m5; rm -rf "$R"; git clone -q --local "$ORIGIN" "$R"; cp -R "$ORIGIN/research/alpha/data" "$R/research/alpha/data"
cd "$R/research/alpha" && git checkout -q alpha-discovery
sed -i '' -e 's/^    _gate(zip_path)$/    pass  # MUTANT: gate removed/' -e 's/^        _gate(p)$/        pass  # MUTANT: gate removed/' r015_run.py
grep -c "MUTANT" r015_run.py
python3 -I tests/test_r015.py 2>&1 | grep -E "the gate:|passed,"
