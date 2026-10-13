#!/bin/bash
# Reviewer B, 016, M3: the guard trusts git's view of the working tree. SYNTHETIC stand-in only.
# Edit ct16/stats.py (CAL_H1 1.731 -> 99.0), put the edited hash into the WORKING-TREE registration, then hide both
# from git with --skip-worktree (and, separately, --assume-unchanged). $1 origin, $2 scratch, $3 objs, $4 clock venv
set -u
O=$1; S=$2; OBJ=$3; V=$4; D=$(cd "$(dirname "$0")" && pwd); PY="$V/bin/python -I"
G="git -c user.name=rb -c user.email=rb@rb"
for FLAG in --skip-worktree --assume-unchanged; do
  echo "################ $FLAG"
  R=$S/repo_m3$FLAG; rm -rf "$R"; git clone -q --local "$O" "$R"; cp -R "$O/research/alpha/data" "$R/research/alpha/data"
  cd "$R/research/alpha" && git checkout -q alpha-discovery
  REG=registrations/016-crypto-trend-verbatim.md
  OLD=$(shasum -a 256 ct16/stats.py | cut -d' ' -f1)
  sed -i '' 's/^CAL_H1 = 1.731 /CAL_H1 = 99.0  /' ct16/stats.py
  NEW=$(shasum -a 256 ct16/stats.py | cut -d' ' -f1); sed -i '' "s/$OLD/$NEW/" "$REG"
  echo "== edited, no flags (after the date):"; $PY r016_run.py --check | cut -c1-120
  git update-index $FLAG ct16/stats.py "$REG"
  echo "== edited + $FLAG:"; $PY r016_run.py --check
  echo "   git status --porcelain: [$(git status --porcelain)]  ls-files -v: $(git ls-files -v ct16/stats.py "$REG" | cut -c1-2 | tr '\n' ' ')"
  APPR=$(python3 -I "$D/mock_fetch_into.py" "$R/research/alpha" "$OBJ" | tail -1)
  # the approval must be committed: write it into the (flagged) registration and commit it explicitly
  cp "$REG" "$S/w.md"; git update-index --no-skip-worktree "$REG"; git update-index --no-assume-unchanged "$REG"; git show HEAD:./"$REG" > "$REG"
  printf '\n%s\n' "$APPR" >> "$REG"; $G commit -q -m approval -- "$REG"
  cp "$S/w.md" "$REG"; printf '\n%s\n' "$APPR" >> "$REG"; git update-index $FLAG "$REG"
  echo "== --check after fetch + approval:"; $PY r016_run.py --check
  echo "== --run:"; $PY r016_run.py --run 2>&1 | grep -E '^outcome|"crit"|Refused' | head -3
  python3 -I -c "
import json; r=[json.loads(l) for l in open('results.jsonl') if '\"mode\": \"read\"' in l][0]
print('   read row: params.cal_h1 =', r['params']['cal_h1'], '| code hash of ct16/stats.py', r['code_hashes']['ct16/stats.py'][:12], '| committed table says', '$OLD'[:12])"
done
