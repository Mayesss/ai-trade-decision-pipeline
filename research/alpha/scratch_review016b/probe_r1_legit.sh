#!/bin/bash
# Reviewer B, 016 R1: a LEGITIMATE run on a gate-open stand-in in the real layout (015's three committed rows already in
# results.jsonl): real fetch() with mock hooks swapped in-process -> owner's approval line -> CLI --run -> second --run.
# $1 stand-in research/alpha, $2 objs.pickle
set -u
A=$1; OBJ=$2; D=$(cd "$(dirname "$0")" && pwd); G="/usr/bin/git -c user.name=owner -c user.email=owner@x"
cd "$A"
echo "== ledger before: $(wc -l < results.jsonl) lines, registrations: $(grep -o '"registration": "[0-9]*-' results.jsonl | sort | uniq -c | tr '\n' ' ')"
echo "== fetch:"; python3 -I -S -c "$(cat "$D/r1_fetch_harness.py.txt")" "$A" "$OBJ" 2>&1 | tail -3
echo "== commits by the runner: $(/usr/bin/git log --format='%an|%s' -5 | tr '\n' ' ')"
FL=$(grep '"mode": "fetched"' results.jsonl | tail -1)
SHA=$(printf '%s' "$FL" | shasum -a 256 | cut -d' ' -f1)
echo "== --check before approval:"; python3 -I -S r016_run.py --check
printf '\n016 read: APPROVED 2028-01-11 row %s\n' "$SHA" >> registrations/016-crypto-trend-verbatim.md
$G commit -q -m "016 read: approval (owner)" -- registrations/016-crypto-trend-verbatim.md
echo "== --check after approval:"; python3 -I -S r016_run.py --check
echo "== --run:"; python3 -I -S r016_run.py --run 2>&1 | grep -E '^outcome|^coverage|Refused|Error' | cut -c1-160
echo "== ledger modes now: $(grep -o '"mode": "[a-z_]*"' results.jsonl | tr '\n' ' ')"
echo "== second --run:"; python3 -I -S r016_run.py --run 2>&1 | tail -2 | cut -c1-160
echo "== fetch again:"; python3 -I -S r016_fetch.py 2>&1 | tail -2 | cut -c1-160
