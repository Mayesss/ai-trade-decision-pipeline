#!/bin/bash
# Recheck the minors on Revision 1 (synthetic stand-ins): trailing newline, showRoot / replace, index lock (transient
# and persistent), the restart clause (legitimate use and an abuse), the non-vacuous gate test (mutation).
set -u; source "$(dirname "$0")/r1_lib.sh"
c() { $PY r015_run.py --check | cut -c1-130; }
ROW015='{"registration": "015-month-end-liquidity-cycle.md", "mode": "result"}'

echo "### m3 trailing newline"
mk n1; cd "$S/n1/research/alpha"
printf '{"registration": "016-crypto-trend-verbatim.md", "mode": "fetch_started"}' > results.jsonl; $G commit -q -m "016 row, no newline" -- results.jsonl
c; $PY r015_run.py --run 2>&1 | grep -E '^r015_core.Refused' | cut -c1-130; echo "HEAD:results.jsonl lines: $(git show HEAD:./results.jsonl | wc -l | tr -d ' ') (unchanged: the 016 row only)"

echo; echo "### m4 showRoot=false + orphan root row; git replace"
mk n2; cd "$S/n2/research/alpha"
git checkout -q --orphan orphan015; git rm -rq --cached . >/dev/null; echo "$ROW015" > results.jsonl; git add results.jsonl; $G commit -q -m "orphan row"
git checkout -q -f alpha-discovery; git config log.showRoot false; echo "showRoot=false:"; c
git branch -q -D orphan015; git reflog expire --expire=now --all; git gc -q --prune=now; git config --unset log.showRoot
echo "$ROW015" >> results.jsonl; $G commit -q -m row -- results.jsonl; ROW=$(git rev-parse HEAD); git reset -q --hard HEAD~1
$G commit -q --allow-empty -m decoy; DEC=$(git rev-parse HEAD); git reset -q --hard HEAD~1; git replace "$ROW" "$DEC"
echo "git replace <row commit> <decoy>:"; c

echo; echo "### m6 index lock: transient (1.5 s) and persistent"
mk n3; cd "$S/n3/research/alpha"; GD=$(git rev-parse --absolute-git-dir)
touch "$GD/index.lock"; (sleep 1.5; rm -f "$GD/index.lock") &
$PY r015_run.py --run 2>&1 | grep -E '^decision|^r015_core.Refused' | cut -c1-120; wait
git log --format='  %an | %s' -3
mk n4; cd "$S/n4/research/alpha"; GD=$(git rev-parse --absolute-git-dir); touch "$GD/index.lock"
$PY r015_run.py --run 2>&1 | grep -E '^decision|^r015_core.Refused' | cut -c1-120; rm -f "$GD/index.lock"
echo "  working tree rows: $(nrows results.jsonl)"; c
echo "-- the owner commits the started row by hand (restart clause):"; $G commit -q -m "015 started row, committed by hand" -- results.jsonl; c
echo "-- restart --run:"; $PY r015_run.py --run 2>&1 | grep -E '^decision|^r015_core.Refused' | cut -c1-120
echo "-- third --run:"; c

echo; echo "### restart clause abuse: runner-authored started row, data read, then reset + re-commit by hand"
mk n5; cd "$S/n5/research/alpha"
# simulate a run that committed its started row (author r015_run), opened the data and died without a failed row
$PY -c "
import sys; sys.argv=['x']; sys.path.append('.'); import r015_run as R
R.__cached__ = None                     # (only needed because this is not the CLI entry; the CLI run is identical up to here)
c = R.guard(); R.record({'registration': R.REG_NAME, 'mode': 'started', 'registration_commit': c, 'data_opened': False}, '015 started (r015_run.py)')
t = dict(R.HASH_ROW.findall(R.head_text(R.HERE, R.REG_REL))); d, v = R.load_french(R.HERE / R.US_ZIP, t[R.US_ZIP])
print('  started committed by', R._git(R.HERE, 'log', '-1', '--format=%an').stdout.strip(), '| data rows read:', len(d), '| process dies here')"
echo "  --check now:"; c
LINE=$(tail -1 results.jsonl); git reset -q --hard HEAD~1; printf '%s\n' "$LINE" >> results.jsonl; $G commit -q -m "re-commit by hand" -- results.jsonl
echo "  after 'git reset --hard HEAD~1' and re-committing the same line as the owner:"; c
echo "  runner-authored commit still in the reflog: $(git log --reflog --author=r015_run --format=%h | head -1)"

echo; echo "### m5 the loader-gate test against a mutant without _gate() in load_french"
mk n6; cd "$S/n6/research/alpha"
python3 -I tests/test_r015.py > "$S/n6_plain.txt" 2>&1; tail -1 "$S/n6_plain.txt"
python3 -I - <<'PY'
from pathlib import Path
p = Path('r015_run.py'); s = p.read_text()
s2 = s.replace('    _gate(zip_path)\n', '    pass  # MUTANT\n', 1).replace('    _gate(zip_path, digest)\n', '    pass  # MUTANT\n', 1)
assert s2.count('MUTANT') == 2; p.write_text(s2)
PY
python3 -I tests/test_r015.py > "$S/n6_mut.txt" 2>&1; tail -1 "$S/n6_mut.txt"
echo "  failures only in the mutant:"; diff <(grep FAILED: "$S/n6_plain.txt") <(grep FAILED: "$S/n6_mut.txt") | grep '^>' | cut -c1-140
