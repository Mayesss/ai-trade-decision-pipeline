#!/bin/bash
# Reviewer B, 015, M3: the guard trusts git's view of the working tree. With `git update-index --skip-worktree`
# (or --assume-unchanged) on r015_core.py and the registration, an edited core and an edited hash table pass
# (b), (d), (e) and the shadow scan; the run records the edited code's decision. Synthetic clone only.
set -u
ORIGIN=$1; S=$2
R=$S/repo_m3; rm -rf "$R"; git clone -q --local "$ORIGIN" "$R"; cp -R "$ORIGIN/research/alpha/data" "$R/research/alpha/data"
cd "$R/research/alpha" && git checkout -q alpha-discovery
REG=registrations/015-month-end-liquidity-cycle.md
OLD=$(shasum -a 256 r015_core.py | cut -d' ' -f1)
sed -i '' -e 's/^    m = statistics.fmean(v)$/    m = statistics.fmean(v) + 0.03/' -e 's/m_wo = statistics.fmean(wo) if wo/m_wo = statistics.fmean(wo) + 0.03 if wo/' r015_core.py
NEW=$(shasum -a 256 r015_core.py | cut -d' ' -f1)
sed -i '' "s/$OLD/$NEW/" "$REG"
echo "== edited, no flags:"; python3 -I r015_run.py --check | cut -c1-110
git update-index --skip-worktree r015_core.py "$REG"
echo "== edited + skip-worktree:"; python3 -I r015_run.py --check
echo "git status --porcelain: [$(git status --porcelain -- r015_core.py "$REG")]  ls-files -v: $(git ls-files -v r015_core.py "$REG" | cut -c1-2 | tr '\n' ' ')"
echo "== --run (NULL synthetic data):"; python3 -I r015_run.py --run 2>&1 | grep -E '^decision|^ "t":'
python3 -I -c "
import json; r=[json.loads(l) for l in open('results.jsonl') if '\"result\"' in l][0]
print('row code hash of r015_core.py:', r['code_hashes']['r015_core.py'][:12], '| committed registration says', '$OLD'[:12])"
