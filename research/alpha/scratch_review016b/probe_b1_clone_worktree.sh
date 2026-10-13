#!/bin/bash
# Reviewer B, 016, B1: the guard binds to "a repository", not to the real one. SYNTHETIC stand-in only.
#   $1 = stand-in ORIGIN repo (playing the real repository), $2 = scratch dir, $3 = objs.pickle, $4 = forged-clock venv
# The date gate is passed with the forged-clock interpreter of probe_t_dategate.sh (-I honoured); that is the only
# stand-in for "after 2028-01-08". Everything else is the runner's own CLI.
set -u
O=$1; S=$2; OBJ=$3; V=$4; D=$(cd "$(dirname "$0")" && pwd)
PY="$V/bin/python -I"
G="git -c user.name=rb -c user.email=rb@rb"
echo "### 1. a clone with the origin's data SYMLINKED: the CLI guard"
C=$S/clone_b1; rm -rf "$C"; git clone -q --local "$O" "$C"; (cd "$C" && git checkout -q alpha-discovery)
ln -s "$O/research/alpha/data" "$C/research/alpha/data"
echo "-- origin --check (plain python3 -I):"; (cd "$O/research/alpha" && python3 -I r016_run.py --check)
echo "-- clone  --check (plain python3 -I):"; (cd "$C/research/alpha" && python3 -I r016_run.py --check)
echo "-- clone  --check (after the date):"; (cd "$C/research/alpha" && $PY r016_run.py --check)
echo "### 2. the whole run in the clone: fetch (mock hooks, synthetic objects; store copied into the symlinked data tree),"
echo "###    approval line, then the runner's own --run"
cd "$C/research/alpha"
APPR=$(python3 -I "$D/mock_fetch_into.py" "$C/research/alpha" "$OBJ" | tail -1)
printf '\n%s\n' "$APPR" >> registrations/016-crypto-trend-verbatim.md; $G commit -q -am "approval (clone)"
echo "-- clone --check:"; $PY r016_run.py --check
echo "-- clone --run:"; $PY r016_run.py --run 2>&1 | grep -E '^outcome|"pass"|Refused|Error' | head -5
echo "-- clone HEAD:results.jsonl 016 modes: $(git show HEAD:./results.jsonl | grep -o '"mode": "[a-z_]*"' | tr '\n' ' ')"
echo "-- ORIGIN after the clone's run (--check, after the date):"; (cd "$O/research/alpha" && $PY r016_run.py --check)
echo "-- ORIGIN results.jsonl 016 rows: $(grep -c 016-crypto "$O/research/alpha/results.jsonl")"
echo "-- origin can resolve the clone's read commit? $(git -C "$O" cat-file -t "$(git rev-parse HEAD)" 2>&1 | head -1)"
rm -rf "$O/research/alpha/data/r016/forward"     # restore the stand-in (the clone wrote the store through the symlink)
echo "### 3. a linked worktree (detached) of the ORIGIN, data symlinked"
cd "$O"; rm -rf "$S/wt_b1"; git worktree add -q --detach "$S/wt_b1" HEAD
ln -s "$O/research/alpha/data" "$S/wt_b1/research/alpha/data"
cd "$S/wt_b1/research/alpha"
APPR=$(python3 -I "$D/mock_fetch_into.py" "$S/wt_b1/research/alpha" "$OBJ" | tail -1)
printf '\n%s\n' "$APPR" >> registrations/016-crypto-trend-verbatim.md; $G commit -q -am "approval (worktree)"
echo "-- worktree --run:"; $PY r016_run.py --run 2>&1 | grep -E '^outcome|Refused' | head -3
echo "-- origin --check while the worktree exists:"; (cd "$O/research/alpha" && $PY r016_run.py --check | cut -c1-110)
cd "$O"; git worktree remove --force "$S/wt_b1"; rm -rf "$O/research/alpha/data/r016/forward"
echo "-- origin --check after 'git worktree remove':"; (cd "$O/research/alpha" && $PY r016_run.py --check)
