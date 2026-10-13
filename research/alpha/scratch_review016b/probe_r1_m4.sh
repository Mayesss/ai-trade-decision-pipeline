#!/bin/bash
# Reviewer B, 016 R1: M4 re-run and the 015-recheck merge attacks. Gate-open stand-ins; network denied.
source "$(dirname "$0")/r1_lib.sh"
w() { grep -E '^\(r\)|^\(i\)|^\(w\)|guard open|^\(g1|^\(h|^outcome|Refused' | cut -c1-150; }
revise() { OLD=$(shasum -a 256 ct16/stats.py | cut -d' ' -f1); printf '\nFORGED_BY_REVIEWER = True\n' >> ct16/stats.py
  NEW=$(shasum -a 256 ct16/stats.py | cut -d' ' -f1); sed -i '' "s/$OLD/$NEW/" $REG; $G commit -q -am "revision after REGISTERED"; }
echo "### E1: a committed revision after REGISTERED"
mk m4a --gate-open; cd $S/m4a/research/alpha; revise; $PY r016_run.py --check | w
echo "### E2: --no-ff merge of REGISTERED + revision into another branch"
mk m4b --gate-open; cd $S/m4b/research/alpha; R0=$(/usr/bin/git rev-parse HEAD); revise
/usr/bin/git checkout -q -b main-merge "$R0~1"; $G merge -q --no-ff -m merge alpha-discovery
echo "  on main-merge:"; $PY r016_run.py --check | w
echo "### E3: REGISTERED + revision only in a linked worktree, squash-merged into main, worktree removed, branch deleted"
mk m4c --gate-open --draft; O=$S/m4c; cd $O
rm -rf $S/m4c_wt; /usr/bin/git worktree add -q -b reg $S/m4c_wt HEAD
cd $S/m4c_wt/research/alpha; sed -i '' 's/^\*\*Status: DRAFT/**Status: REGISTERED/' $REG; $G commit -q -am "REGISTERED (in a worktree)"; RW=$(/usr/bin/git rev-parse HEAD); revise
cd $O; $G merge -q --squash reg; $G commit -q -m "squash of reg"
/usr/bin/git worktree remove --force $S/m4c_wt; /usr/bin/git branch -q -D reg
echo "  original REGISTERED commit ${RW:0:12} reachable via --all --reflog? $(/usr/bin/git log --all --reflog --format=%H | grep -c $RW)"
cd $O/research/alpha; echo "  main --check:"; $PY r016_run.py --check | w
python3 -I -S -c "import sys; sys.path.append('.'); import r016_run as R; print('  ct16.stats forged:', getattr(R.S, 'FORGED_BY_REVIEWER', False))"
echo "### E4: the data gate opens only after the started row is in HEAD"
mk m4d --gate-open; A=$S/m4d/research/alpha; cd $A; fetch_in $A $S/m4d.objs.pickle; approve
$PY -c "
import sys, tempfile; sys.pycache_prefix = tempfile.mkdtemp(prefix='r016f-pyc-'); sys.path.append('$A'); import r016_run as R
ctx = R.guard('run'); print('  guard(run) passed; _GATE =', R._GATE)
try:
    R.Panel('run', table=ctx['table']).load(); print('  Panel OPENED before the started row')
except R.Refused as e: print('  Panel refused:', str(e)[:100])
" 2>&1 | cut -c1-150
echo "### E5: restart after a RUNNER-committed read_started (data opened, then a hard crash): reset, re-commit by hand, restart line"
LINE=$($PY -c "
import sys, json, tempfile; sys.pycache_prefix = tempfile.mkdtemp(prefix='r016f-pyc-'); sys.path.append('$A'); import r016_run as R
ctx = R.guard('run')
print(json.dumps({'registration': R.REG_NAME, 'registration_commit': ctx['commit'], 'reg0': ctx['reg0'], 'fetched_row_sha256': ctx['fetched_row_sha256'], 'mode': 'read_started', 'restart_number': 0}, sort_keys=True))")
[ -n "$LINE" ] || { echo "  NO LINE"; exit 1; }; echo "$LINE" >> results.jsonl; /usr/bin/git -c user.name=r016_run -c user.email=r016_run@localhost commit -q -m "016: read_started (as the runner)" -- results.jsonl
echo "  after a runner-committed read_started:"; $PY r016_run.py --check | w
/usr/bin/git reset -q --hard HEAD~1; echo "$LINE" >> results.jsonl; $G commit -q -m "read_started re-committed by hand" -- results.jsonl
SSHA=$(printf '%s' "$LINE" | shasum -a 256 | cut -d' ' -f1); printf '016 read restart: APPROVED 2028-01-12 started-row %s\n' $SSHA >> $REG; $G commit -q -m restart -- $REG
echo "  after reset + hand re-commit + restart line:"; $PY r016_run.py --check | w
echo "### E6: control - a hand-committed read_started (the runner never committed one) + restart line"
mk m4e --gate-open; A=$S/m4e/research/alpha; cd $A; fetch_in $A $S/m4e.objs.pickle; approve
LINE='{"mode": "read_started", "registration": "016-crypto-trend-verbatim.md", "restart_number": 0}'
echo "$LINE" >> results.jsonl; $G commit -q -m "read_started by hand" -- results.jsonl
SSHA=$(printf '%s' "$LINE" | shasum -a 256 | cut -d' ' -f1); printf '016 read restart: APPROVED 2028-01-12 started-row %s\n' $SSHA >> $REG; $G commit -q -m restart -- $REG
$PY r016_run.py --check | w
