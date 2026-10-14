#!/bin/bash
# Reviewer B, 016 R2: r1-m1 with the Revision 2 procedure (annotated tag r016-registered on reg0). Network denied.
export RB_S=/private/tmp/claude-502/-Users-mayess-sammoud-Documents-persogit-ai-trade-decision-pipeline/7b2e3f5e-bd1a-4fe0-b780-0c6d54079801/scratchpad/rb016r2
source "$(dirname "$0")/r1_lib.sh"
w() { grep -E '^\(r\)|guard open|^\(g1' | cut -c1-140; }
revise() { OLD=$(shasum -a 256 ct16/stats.py | cut -d' ' -f1); printf '\nFORGED_BY_REVIEWER = True\n' >> ct16/stats.py
  NEW=$(shasum -a 256 ct16/stats.py | cut -d' ' -f1); sed -i '' "s/$OLD/$NEW/" $REG; $G commit -q -am "revision after REGISTERED"; }
mk tg --gate-open --draft; O=$S/tg; cd $O
rm -rf $S/tg_wt; /usr/bin/git worktree add -q -b reg $S/tg_wt HEAD
cd $S/tg_wt/research/alpha; sed -i '' 's/^\*\*Status: DRAFT/**Status: REGISTERED/' $REG; $G commit -q -am "REGISTERED (in a worktree)"
$G tag -a r016-registered -m "016 reg0" HEAD; revise
cd $O; $G merge -q --squash reg 2>/dev/null; $G commit -q -m "squash of reg"; /usr/bin/git worktree remove --force $S/tg_wt; /usr/bin/git branch -q -D reg
echo "== squash attack WITH the tag on reg0:"; (cd $O/research/alpha && $PY r016_run.py --check | w)
/usr/bin/git tag -d r016-registered >/dev/null
echo "== after 'git tag -d r016-registered' (an explicit act; the guard does not require the tag):"; (cd $O/research/alpha && $PY r016_run.py --check | w)
