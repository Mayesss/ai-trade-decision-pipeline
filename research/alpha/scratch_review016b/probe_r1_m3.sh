#!/bin/bash
# Reviewer B, 016 R1: M3 re-run. Edited ct16/stats.py + edited working-tree hash table, hidden by index flags or a filter.
source "$(dirname "$0")/r1_lib.sh"
w() { grep -E '^\(b\)|^\(d\)|^\(e\)|^\(r\)|^\(s\)|guard open|^\(g1' | cut -c1-140; }
for MODE in --skip-worktree --assume-unchanged filter; do
  mk m3 --gate-open; A=$S/m3/research/alpha; cd $A
  OLD=$(shasum -a 256 ct16/stats.py | cut -d' ' -f1)
  printf '\nFORGED_BY_REVIEWER = True\n' >> ct16/stats.py; NEW=$(shasum -a 256 ct16/stats.py | cut -d' ' -f1); sed -i '' "s/$OLD/$NEW/" $REG
  if [ $MODE = filter ]; then
    printf 'ct16/stats.py filter=rb\nregistrations/*.md filter=rb\n' > .gitattributes; $G add .gitattributes; $G commit -q -m attrs
    /usr/bin/git config filter.rb.clean "/usr/bin/git show HEAD:research/alpha/\$(basename %f) 2>/dev/null || cat"
  else
    /usr/bin/git update-index $MODE ct16/stats.py $REG
  fi
  echo "== $MODE: git status: [$(/usr/bin/git status --porcelain | tr '\n' ' ')]"; $PY r016_run.py --check | w
done
