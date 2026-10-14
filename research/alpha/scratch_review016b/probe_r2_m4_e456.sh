#!/bin/bash
# Reviewer B, 016 R2: probe_r1_m4 E4-E6 again. Revision 2 keeps a pycache prefix only for r016_fetch.py as __main__,
# so an in-process `-c` harness that imports r016_run is refused by (v) on bytecode origin (probe_r2_rerun_r1.out). Here
# the harness dresses __main__ as r016_fetch.py with its own fresh prefix (an in-process liberty, r1-m4) so that the
# guard logic itself can be exercised. Network denied by the caller.
export RB_S=/private/tmp/claude-502/-Users-mayess-sammoud-Documents-persogit-ai-trade-decision-pipeline/7b2e3f5e-bd1a-4fe0-b780-0c6d54079801/scratchpad/rb016r2
export HARNESS=r2_fetch_harness.py.txt
source "$(dirname "$0")/r1_lib.sh"
w() { grep -E '^\(i\)|guard open|^\(g1|^\(h|Refused' | cut -c1-150; }
PRE="import sys, tempfile; m = sys.modules['__main__']; m.__file__ = '\$A/r016_fetch.py'; m._R016_PREFIX = sys.pycache_prefix = tempfile.mkdtemp(prefix='r016f-pyc-'); sys.dont_write_bytecode = True; sys.path.append('\$A'); import r016_run as R"
echo "### E4: the data gate opens only after the started row is in HEAD"
mk m4d --gate-open; A=$S/m4d/research/alpha; cd $A; fetch_in $A $S/m4d.objs.pickle; approve
$PY -c "${PRE//\$A/$A}
ctx = R.guard('run'); print('  guard(run) passed; _GATE =', R._GATE)
try:
    R.Panel('run', table=ctx['table']).load(); print('  Panel OPENED before the started row')
except R.Refused as e: print('  Panel refused:', str(e)[:100])
" 2>&1 | cut -c1-150
echo "### E5: restart after a RUNNER-committed read_started: reset, re-commit by hand, restart line"
LINE=$($PY -c "${PRE//\$A/$A}
import json; ctx = R.guard('run')
print(json.dumps({'registration': R.REG_NAME, 'registration_commit': ctx['commit'], 'reg0': ctx['reg0'], 'fetched_row_sha256': ctx['fetched_row_sha256'], 'mode': 'read_started', 'restart_number': 0}, sort_keys=True))")
[ -n "$LINE" ] || { echo "  NO LINE"; exit 1; }
echo "$LINE" >> results.jsonl; /usr/bin/git -c user.name=r016_run -c user.email=r016_run@localhost commit -q -m "016: read_started (as the runner)" -- results.jsonl
echo "  after a runner-committed read_started:"; $PY r016_run.py --check | w
/usr/bin/git reset -q --hard HEAD~1; echo "$LINE" >> results.jsonl; $G commit -q -m "read_started re-committed by hand" -- results.jsonl
SSHA=$(printf '%s' "$LINE" | shasum -a 256 | cut -d' ' -f1); printf '016 read restart: APPROVED 2029-01-12 started-row %s\n' $SSHA >> $REG; $G commit -q -m restart -- $REG
echo "  after reset + hand re-commit + restart line:"; $PY r016_run.py --check | w
echo "### E6: control - a hand-committed read_started (the runner never committed one) + restart line"
mk m4e --gate-open; A=$S/m4e/research/alpha; cd $A; fetch_in $A $S/m4e.objs.pickle; approve
LINE='{"mode": "read_started", "registration": "016-crypto-trend-verbatim.md", "restart_number": 0}'
echo "$LINE" >> results.jsonl; $G commit -q -m "read_started by hand" -- results.jsonl
SSHA=$(printf '%s' "$LINE" | shasum -a 256 | cut -d' ' -f1); printf '016 read restart: APPROVED 2029-01-12 started-row %s\n' $SSHA >> $REG; $G commit -q -m restart -- $REG
$PY r016_run.py --check | w
