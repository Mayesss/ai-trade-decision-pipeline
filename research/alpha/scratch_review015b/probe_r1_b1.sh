#!/bin/bash
# Recheck B1 (Revision 1): legitimate open + full run; clone (copied / symlinked data), run in a clone, linked
# worktree, detached HEAD.
set -u; source "$(dirname "$0")/r1_lib.sh"
mk base
cd "$S/base/research/alpha"
echo "== legit REGISTERED stand-in: --check"; $PY r015_run.py --check
echo "== legit --run"; $PY r015_run.py --run 2>&1 | grep -E '^decision|Refused' ; git log --format='%an | %s' -3
echo "== second --run:"; $PY r015_run.py --run 2>&1 | grep -E 'Refused|^\(f\)|015 row' | head -3
mk origin
for mode in copy symlink; do
  rm -rf "$S/cl_$mode"; git clone -q --local "$S/origin" "$S/cl_$mode"; (cd "$S/cl_$mode" && git checkout -q alpha-discovery)
  if [ $mode = copy ]; then cp -R "$S/origin/research/alpha/data" "$S/cl_$mode/research/alpha/data"; else ln -s "$S/origin/research/alpha/data" "$S/cl_$mode/research/alpha/data"; fi
  echo "== clone ($mode data) --check:"; (cd "$S/cl_$mode/research/alpha" && $PY r015_run.py --check)
  echo "== clone ($mode data) --run:"; (cd "$S/cl_$mode/research/alpha" && $PY r015_run.py --run 2>&1 | grep -E 'Refused' | cut -c1-150; echo "rows in clone: $(nrows results.jsonl)")
done
cd "$S/origin/research/alpha"
git worktree add -q --detach "$S/wt" HEAD; ln -s "$S/origin/research/alpha/data" "$S/wt/research/alpha/data"
echo "== linked worktree --check:"; (cd "$S/wt/research/alpha" && $PY r015_run.py --check)
echo "== linked worktree --run:"; (cd "$S/wt/research/alpha" && $PY r015_run.py --run 2>&1 | grep Refused | cut -c1-150; echo "rows: $(nrows results.jsonl)")
git worktree remove --force "$S/wt"
git -c advice.detachedHead=false checkout -q --detach HEAD
echo "== detached HEAD in the main worktree --check:"; $PY r015_run.py --check
git checkout -q alpha-discovery
echo "== back on the branch --check:"; $PY r015_run.py --check
