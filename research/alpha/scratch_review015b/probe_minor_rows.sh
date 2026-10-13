#!/bin/bash
# Reviewer B, 015, minors m3/m4 on (f) run-once (synthetic clones only).
set -u
ORIGIN=$1; S=$2
fresh() { rm -rf "$1"; git clone -q --local "$ORIGIN" "$1"; cp -R "$ORIGIN/research/alpha/data" "$1/research/alpha/data"; (cd "$1" && git checkout -q alpha-discovery); }
G="git -c user.name=rb -c user.email=rb@rb"

echo "### m3: results.jsonl (shared with 016) ends without a newline -> the 015 row is glued onto the previous line"
R=$S/repo_m3n; fresh "$R"; cd "$R/research/alpha"
printf '{"registration": "016-crypto-trend-verbatim.md", "mode": "fetch_started"}' > results.jsonl
$G commit -q -m "a 016 row committed by hand, no trailing newline" -- results.jsonl
python3 -I r015_run.py --run 2>&1 | grep -E "Refused|not in HEAD" | cut -c1-140
echo "HEAD:results.jsonl line 1: $(git show HEAD:./results.jsonl | head -1 | cut -c1-140)"
echo "--check now:"; python3 -I r015_run.py --check

echo; echo "### m4a: log.showRoot=false hides a row committed as the root of an orphan branch"
R=$S/repo_m4a; fresh "$R"; cd "$R/research/alpha"
git checkout -q --orphan orphan015
git rm -rq --cached . >/dev/null; echo '{"registration": "015-month-end-liquidity-cycle.md", "mode": "result"}' > /tmp/rb015_row.$$
cp /tmp/rb015_row.$$ results.jsonl; rm /tmp/rb015_row.$$
$G commit -q -m "orphan row" -- results.jsonl 2>/dev/null || { git add results.jsonl; $G commit -q -m "orphan row"; }
git checkout -q -f alpha-discovery
echo "default config:"; python3 -I r015_run.py --check | cut -c1-80
git config log.showRoot false
echo "log.showRoot=false:"; python3 -I r015_run.py --check

echo; echo "### m4b: git replace hides the commit that added the row"
R=$S/repo_m4b; fresh "$R"; cd "$R/research/alpha"
echo '{"registration": "015-month-end-liquidity-cycle.md", "mode": "result"}' >> results.jsonl
$G commit -q -m "row" -- results.jsonl; ROW=$(git rev-parse HEAD)
echo "with the row committed:"; python3 -I r015_run.py --check | cut -c1-80
git reset -q --hard HEAD~1
$G commit -q --allow-empty -m "decoy"; DECOY=$(git rev-parse HEAD); git reset -q --hard HEAD~1
git replace "$ROW" "$DECOY"
echo "after reset + 'git replace <row commit> <decoy>' (no gc, no reflog expiry):"; python3 -I r015_run.py --check
