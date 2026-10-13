#!/bin/bash
# Reviewer B, 016 R1: B1 re-run, plus the repo-path recovery line and a symlink at the pinned path.
source "$(dirname "$0")/r1_lib.sh"
mk b1 --gate-open; O=$S/b1; OA=$O/research/alpha
echo "== origin --check:"; (cd $OA && $PY r016_run.py --check)
C=$S/b1_clone; rm -rf $C; /usr/bin/git clone -q --local $O $C; (cd $C && /usr/bin/git checkout -q alpha-discovery)
ln -s $OA/data $C/research/alpha/data
echo "== clone, data SYMLINKED, --check:"; (cd $C/research/alpha && $PY r016_run.py --check | grep '(w)')
echo "== clone, fetch CLI:"; (cd $C/research/alpha && $PY r016_fetch.py 2>&1 | tail -1)
echo "== clone, fetch via in-process mock harness:"; (cd $C/research/alpha && fetch_in $C/research/alpha $O.objs.pickle)
rm $C/research/alpha/data; cp -R $OA/data $C/research/alpha/data
echo "== clone, data COPIED, --check:"; (cd $C/research/alpha && $PY r016_run.py --check | grep '(w)')
cd $O; rm -rf $S/b1_wt; /usr/bin/git worktree add -q --detach $S/b1_wt HEAD; ln -s $OA/data $S/b1_wt/research/alpha/data
echo "== linked worktree (detached):"; (cd $S/b1_wt/research/alpha && $PY r016_run.py --check | grep '(w)')
(cd $S/b1_wt && /usr/bin/git checkout -q -b wtb)
echo "== linked worktree (named branch):"; (cd $S/b1_wt/research/alpha && $PY r016_run.py --check | grep '(w)')
/usr/bin/git worktree remove --force $S/b1_wt; /usr/bin/git branch -q -D wtb
/usr/bin/git checkout -q --detach
echo "== main worktree, detached HEAD:"; (cd $OA && $PY r016_run.py --check | grep '(w)'); /usr/bin/git checkout -q alpha-discovery
echo "== recovery line while the pinned path EXISTS (in the clone, line names the clone):"
cd $C/research/alpha && printf '\n016 repo-path: APPROVED 2028-01-10 %s\n' "$C" >> $REG && $G commit -q -m "repo-path" -- $REG
$PY r016_run.py --check | grep -E '^\(w\)|guard open|^\(g1'
echo "== ABUSE 1: the pinned path moved away (mv origin origin.away); the clone's self-issued repo-path line:"
mv $O $O.away
$PY r016_run.py --check | grep -E '^\(w\)|^\(r\)|guard open|^\(g1|^\(e'
echo "   fetch (mock harness) + approval + --run in the CLONE:"
fetch_in $C/research/alpha $O.objs.pickle; approve
$PY r016_run.py --run 2>&1 | grep -E '^outcome|Refused' | head -2
echo "   clone ledger 016 modes: $(modes results.jsonl)"
mv $O.away $O
echo "   origin back in place, origin --check:"; (cd $OA && $PY r016_run.py --check)
echo "   origin ledger 016 modes: $(modes $OA/results.jsonl)"
echo "== ABUSE 2: a SYMLINK at the pinned path pointing at a fresh clone (no repo-path line needed):"
C2=$S/b1_clone2; rm -rf $C2; /usr/bin/git clone -q --local $O $C2; (cd $C2 && /usr/bin/git checkout -q alpha-discovery); cp -R $OA/data $C2/research/alpha/data
mv $O $O.away; ln -s $C2 $O
(cd $C2/research/alpha && $PY r016_run.py --check | grep -E '^\(w\)|guard open|^\(g1')
rm $O; mv $O.away $O
