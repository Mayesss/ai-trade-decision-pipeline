#!/bin/bash
# Reviewer B, 015 Revision 2 recheck (e2a01b8): the restart rule and the new reg0 collection, plus residuals.
# Synthetic stand-ins only (make_standin_r1.py with R015_REV=e2a01b8).
set -u; export R015_REV=e2a01b8; source "$(dirname "$0")/r1_lib.sh"
REG=registrations/015-month-end-liquidity-cycle.md
c() { $PY r015_run.py --check | cut -c1-140; }
revise() {  # a post-REGISTERED revision: core mean +0.30 (z units), table updated
  OLD=$(shasum -a 256 r015_core.py | cut -d' ' -f1)
  sed -i '' -e 's/^    m = statistics.fmean(v)$/    m = statistics.fmean(v) + 0.30/' -e 's/m_wo = statistics.fmean(wo) if wo/m_wo = statistics.fmean(wo) + 0.30 if wo/' r015_core.py
  NEW=$(shasum -a 256 r015_core.py | cut -d' ' -f1); sed -i '' "s/$OLD/$NEW/" "$REG"; }

echo "### A. restart abuse: the runner commits its started row, reads the data, then its result commit fails (lock)"
mk ra; cd "$S/ra/research/alpha"; GD=$(git rev-parse --absolute-git-dir)
$PY r015_run.py --run > "$S/ra_run.txt" 2>&1 &
PID=$!
while ! git log -1 --format=%s 2>/dev/null | grep -q '015 started'; do :; done; touch "$GD/index.lock"
wait $PID; rm -f "$GD/index.lock"
echo "  run ended: $(grep -E '^r015_core.Refused' "$S/ra_run.txt" | cut -c1-90)"
echo "  HEAD: $(git log -1 --format='%an | %s')"
python3 -I -c "
import json; rows=[json.loads(l) for l in open('results.jsonl')]
print('  working tree rows:', [r['mode'] for r in rows], '| uncommitted result row carries t =', [round(r['existence']['t'],2) for r in rows if r['mode']=='result'])"
git checkout -q -- results.jsonl; LINE=$(git show HEAD:./results.jsonl | tail -1)
git reset -q --hard HEAD~1; printf '%s\n' "$LINE" >> results.jsonl; $G commit -q -m "re-commit by hand" -- results.jsonl
echo "  after checkout + reset --hard HEAD~1 + hand re-commit of the started line:"; c
git reflog expire --expire-unreachable=now --all
echo "  ... then 'git reflog expire --expire-unreachable=now --all' (no gc; a history purge):"; c

echo; echo "### B. two REGISTERED commits with IDENTICAL content on different branches"
mk rb; cd "$S/rb/research/alpha"
git checkout -q -b twin HEAD~1; git checkout -q alpha-discovery -- "$REG" r015_run.py; $G commit -q -m "same REGISTERED content, other branch"
git checkout -q alpha-discovery; c

echo; echo "### C. a REGISTERED commit with a typo, reset, corrected REGISTERED commit (liveness)"
mk rc; cd "$S/rc/research/alpha"
sed -i '' 's/^\*\*Status: REGISTERED/**Status: REGISTERED (typo)/' "$REG"; $G commit -q -am "typo"; git reset -q --hard HEAD~1
echo "  REGISTERED commit, a typo'd REGISTERED commit reset away (still in the reflog):"; c

echo; echo "### D. REGISTERED + revision made in a LINKED WORKTREE, squash-merged, worktree removed, branch deleted"
mk rd --draft; cd "$S/rd/research/alpha"
git worktree add -q -b reg "$S/rd_wt" alpha-discovery
( cd "$S/rd_wt/research/alpha"; sed -i '' 's/^\*\*Status: DRAFT/**Status: REGISTERED/' "$REG"; $G commit -q -am "REGISTERED (in a worktree)"
  revise; $G commit -q -am "revision after REGISTERED (in the worktree)" )
$G merge -q --squash reg; $G commit -q -m "squash-merge of the worktree branch"
git worktree remove --force "$S/rd_wt"; git branch -q -D reg
echo "  main worktree:"; c
echo "  --run (NULL synthetic data; the registered code gives t -1.80 on this tree):"; $PY r015_run.py --run 2>&1 | grep -E '^ "t":|^decision|Refused' | cut -c1-120
echo "  original REGISTERED commit still in the object store: $(git fsck --lost-found 2>/dev/null | wc -l | tr -d ' ') dangling objects"

echo; echo "### E. rebase/squash of REGISTERED + revision in the main worktree"
mk re; cd "$S/re/research/alpha"; revise; $G commit -q -am "revision"
git reset -q --soft HEAD~2; $G commit -q -m "squashed: REGISTERED (revised)"
echo "  after the squash (originals in the reflog):"; c
git reflog expire --expire=now --all
echo "  after 'git reflog expire --expire=now --all' (history purge):"; c
