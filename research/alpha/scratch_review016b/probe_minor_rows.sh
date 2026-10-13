#!/bin/bash
# Reviewer B, 016, minors on run-once (f)/(i): the 015 m3/m4 cases, applied to 016. SYNTHETIC stand-in clones only.
# $1 origin, $2 scratch, $3 clock venv
set -u
O=$1; S=$2; V=$3; PY="$V/bin/python -I"
G="git -c user.name=rb -c user.email=rb@rb"
fresh() { rm -rf "$1"; git clone -q --local "$O" "$1"; cp -R "$O/research/alpha/data" "$1/research/alpha/data"; (cd "$1" && git checkout -q alpha-discovery); }
ROW='{"registration": "016-crypto-trend-verbatim.md", "mode": "read"}'
echo "### a: results.jsonl (shared with 015) ends WITHOUT a newline; 016's append glues its row onto it"
R=$S/rows_a; fresh "$R"; cd "$R/research/alpha"
printf '{"registration": "015-month-end-liquidity-cycle.md", "mode": "result"}' > results.jsonl; $G commit -q -m "015 row by hand, no newline" -- results.jsonl
python3 -I -c "
import sys; sys.path.insert(0, '.'); import r016_run as R
R.RESULTS = R.HERE / 'results.jsonl'
try: R.record({'registration': R.REG_NAME, 'mode': 'read'}, '016 read row (probe)')
except Exception as e: print('  record():', type(e).__name__, str(e)[:90])
print('  HEAD line 1:', R._git(R.HERE, 'show', 'HEAD:./results.jsonl').stdout.splitlines()[0][:130])
print('  016 rows the guard sees (result_rows):', [r['mode'] for _, r in R.result_rows(R.HERE)])"
echo "  --check (after the date):"; $PY r016_run.py --check | cut -c1-100
echo "### b: log.showRoot=false hides a read row committed as the ROOT of an orphan branch"
R=$S/rows_b; fresh "$R"; cd "$R/research/alpha"
git checkout -q --orphan orphan016; git rm -rq --cached . >/dev/null; echo "$ROW" > results.jsonl; git add results.jsonl; $G commit -q -m "orphan read row"
git checkout -q -f alpha-discovery
echo "  default config:"; $PY r016_run.py --check | cut -c1-80 | head -1
git config log.showRoot false
echo "  log.showRoot=false:"; $PY r016_run.py --check | head -1
echo "### c: git replace <row commit> <decoy> hides the commit that added a read row (no gc, no reflog expiry)"
R=$S/rows_c; fresh "$R"; cd "$R/research/alpha"
echo "$ROW" >> results.jsonl; $G commit -q -m "row" -- results.jsonl; RC=$(git rev-parse HEAD)
echo "  with the row committed:"; $PY r016_run.py --check | head -1 | cut -c1-80
git reset -q --hard HEAD~1; $G commit -q --allow-empty -m decoy; DEC=$(git rev-parse HEAD); git reset -q --hard HEAD~1
git replace "$RC" "$DEC"
echo "  after reset + git replace:"; $PY r016_run.py --check | head -1
