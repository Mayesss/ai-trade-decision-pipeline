#!/bin/bash
# Reviewer B, 015, M2: the guard does not enforce `python -I`. Without it, PYTHONPATH (or a user-site .pth) puts a
# forged stdlib module on the decision path; the guard opens and the run records a pass on NULL synthetic data.
set -u
ORIGIN=$1; S=$2
# the forged module is stored as .txt (never importable in the repo) and copied to a temp dir for the probe
EVIL=$(mktemp -d); cp "$(dirname "$0")/evil_pythonpath/statistics.py.txt" "$EVIL/statistics.py"
R=$S/repo_m2; rm -rf "$R"; git clone -q --local "$ORIGIN" "$R"; cp -R "$ORIGIN/research/alpha/data" "$R/research/alpha/data"
cd "$R" && git checkout -q alpha-discovery
echo "== with -I, PYTHONPATH is ignored:"; PYTHONPATH=$EVIL python3 -I -c "import statistics; print(statistics.__file__)"
echo "== without -I:"; PYTHONPATH=$EVIL python3 -c "import statistics; print(statistics.__file__)"
echo "== --check without -I, PYTHONPATH=evil:"; PYTHONPATH=$EVIL python3 research/alpha/r015_run.py --check
echo "== --run without -I, PYTHONPATH=evil (NULL synthetic data, effect 0):"
PYTHONPATH=$EVIL python3 research/alpha/r015_run.py --run 2>&1 | grep -E '^decision|^ "t":|^ "mean":'
python3 -I -c "
import json; rows=[json.loads(l) for l in open('research/alpha/results.jsonl')]
r=[x for x in rows if x['mode']=='result'][0]
print('row env stdlib_files', r['env']['stdlib_files'][:3], 'stdlib_sha256', r['env']['stdlib_sha256'][:12], 'recheck_failed' in r)"
echo "== the honest -I fingerprint for comparison:"; python3 -I -c "import sys; sys.path.insert(0,'research/alpha'); import r015_run as R; print(R.env_fingerprint()['stdlib_sha256'][:12])"
