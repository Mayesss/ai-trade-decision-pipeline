#!/bin/bash
# Recheck 016-B M4 / 015 (r) through merge topology: reg0 is searched on HEAD's FIRST-PARENT history only. Register on
# alpha-discovery, commit a revision there (refused by (r) on that branch), then merge the branch --no-ff into a
# branch made from main: on the merged branch the merge commit is the "first REGISTERED commit" and (r) passes.
set -u; source "$(dirname "$0")/r1_lib.sh"
REG=registrations/015-month-end-liquidity-cycle.md
mk m4m; cd "$S/m4m/research/alpha"
R0=$(git rev-parse --short HEAD)
OLD=$(shasum -a 256 r015_core.py | cut -d' ' -f1)
sed -i '' -e 's/^    m = statistics.fmean(v)$/    m = statistics.fmean(v) + 0.30/' -e 's/m_wo = statistics.fmean(wo) if wo/m_wo = statistics.fmean(wo) + 0.30 if wo/' r015_core.py
NEW=$(shasum -a 256 r015_core.py | cut -d' ' -f1); sed -i '' "s/$OLD/$NEW/" "$REG"
$G commit -q -am "revision after REGISTERED (core: mean +0.30 in z units, table updated)"
echo "== on alpha-discovery (REGISTERED at $R0, then a revision):"; $PY r015_run.py --check | cut -c1-120
git checkout -q -b main-merge e8372fa
$G merge -q --no-ff -m "merge alpha-discovery into main" alpha-discovery
echo "== on a branch from main after 'git merge --no-ff alpha-discovery':"; $PY r015_run.py --check | cut -c1-120
echo "== --run there (NULL synthetic data):"; $PY r015_run.py --run 2>&1 | grep -E '^decision|^ "t":|Refused' | cut -c1-120
python3 -I -c "import json; r=[json.loads(l) for l in open('results.jsonl') if '\"result\"' in l]; print('row registration_commit', r[0]['registration_commit'][:12] if r else None)"
