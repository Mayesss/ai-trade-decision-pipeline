#!/bin/bash
# Recheck M3 (index flags, filters, registration read from HEAD, hash-object vs HEAD blobs) and 016-B M4 (reg0).
set -u; source "$(dirname "$0")/r1_lib.sh"
REG=registrations/015-month-end-liquidity-cycle.md
edit() {  # edit r015_core.py (+3 % on both means) and put its new hash in the working-tree registration
  OLD=$(shasum -a 256 r015_core.py | cut -d' ' -f1)
  sed -i '' -e 's/^    m = statistics.fmean(v)$/    m = statistics.fmean(v) + 0.03/' r015_core.py
  NEW=$(shasum -a 256 r015_core.py | cut -d' ' -f1); [ "$OLD" != "$NEW" ] || echo "  (edit did not apply)"
  sed -i '' "s/$OLD/$NEW/" "$REG"; }
f() { $PY r015_run.py --check | cut -c1-130; }
for flag in --skip-worktree --assume-unchanged; do
  mk "m3${flag#--}"; cd "$S/m3${flag#--}/research/alpha"; edit; git update-index $flag r015_core.py "$REG"
  echo "== edited core + table, git update-index $flag:"; f
  echo "   --run:"; $PY r015_run.py --run 2>&1 | grep -E '^decision|^r015_core.Refused' | cut -c1-120; echo "   rows: $(nrows results.jsonl)"
done
mk m3f; cd "$S/m3f/research/alpha"
echo 'r015_core.py filter=rb' > .gitattributes; $G add .gitattributes; $G commit -q -m attr
git config filter.rb.clean cat; git config filter.rb.smudge cat
echo "== a committed filter attribute on r015_core.py (no edit):"; f
mk m3h; cd "$S/m3h/research/alpha"
sed -i '' 's/^\*\*Status: REGISTERED/**Status: DRAFT/' "$REG"; $G commit -q -am "back to DRAFT"
sed -i '' 's/^\*\*Status: DRAFT/**Status: REGISTERED/' "$REG"
echo "== HEAD says DRAFT, working tree says REGISTERED:"; f
mk m3r; cd "$S/m3r/research/alpha"
echo "== 016-B M4: a committed revision after REGISTERED (core edited, table updated, both committed):"
edit; $G commit -q -am "revision after REGISTERED"; f
echo "   --run:"; $PY r015_run.py --run 2>&1 | grep -E '^decision|^r015_core.Refused' | cut -c1-120
