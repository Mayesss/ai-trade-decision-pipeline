#!/bin/bash
# Reviewer B, 015, B1 (second leg): inside ONE repository, a run from a linked worktree on a detached HEAD leaves rows
# that `git worktree remove` makes unreachable: --all/--reflog no longer see them and the guard reopens.
# Also: a run on a detached HEAD in the main worktree is protected only by the HEAD reflog (expires).
set -u
ORIGIN=$1; S=$2
R=$S/repo_wt; rm -rf "$R" "$S/wt"; git clone -q --local "$ORIGIN" "$R"; cp -R "$ORIGIN/research/alpha/data" "$R/research/alpha/data"
cd "$R" && git checkout -q alpha-discovery
git worktree add -q --detach "$S/wt" HEAD
ln -s "$R/research/alpha/data" "$S/wt/research/alpha/data"
echo "== worktree (detached) --check"; (cd "$S/wt" && python3 -I research/alpha/r015_run.py --check)
echo "== worktree --run"; (cd "$S/wt" && python3 -I research/alpha/r015_run.py --run 2>&1 | grep -E '^decision')
echo "== main worktree --check while the linked worktree exists:"; python3 -I research/alpha/r015_run.py --check | cut -c1-90
git worktree remove --force "$S/wt"
echo "== main worktree --check after 'git worktree remove':"; python3 -I research/alpha/r015_run.py --check
