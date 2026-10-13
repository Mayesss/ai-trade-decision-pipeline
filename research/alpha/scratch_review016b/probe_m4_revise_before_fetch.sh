#!/bin/bash
# Reviewer B, 016, M4: nothing binds the fetch or the read to the REGISTERED commit. A COMMITTED revision of the code and
# the hash table at any time in the 15 months before the fetch (e.g. after the owner's interim look at live P&L, §14)
# passes every check. SYNTHETIC stand-in only. $1 origin, $2 scratch, $3 objs, $4 clock venv
set -u
O=$1; S=$2; OBJ=$3; V=$4; D=$(cd "$(dirname "$0")" && pwd); PY="$V/bin/python -I"
G="git -c user.name=rb -c user.email=rb@rb"
R=$S/repo_m4; rm -rf "$R"; git clone -q --local "$O" "$R"; cp -R "$O/research/alpha/data" "$R/research/alpha/data"
cd "$R/research/alpha" && git checkout -q alpha-discovery
REG=registrations/016-crypto-trend-verbatim.md
REG0=$(git rev-parse HEAD)
echo "REGISTERED commit (stand-in): ${REG0:0:12}"
echo "== --check before the revision (after the date):"; $PY r016_run.py --check
OLD=$(shasum -a 256 ct16/stats.py | cut -d' ' -f1)
sed -i '' 's/^CAL_H1 = 1.731 /CAL_H1 = 1.20  /' ct16/stats.py
NEW=$(shasum -a 256 ct16/stats.py | cut -d' ' -f1); sed -i '' "s/$OLD/$NEW/" "$REG"
GIT_COMMITTER_DATE=2027-06-01T00:00:00Z $G commit -q --date=2027-06-01T00:00:00Z -am "016: CAL_H1 revised (a committed revision, 2027-06)"
echo "== --check after a committed revision of CAL_H1 and the hash table:"; $PY r016_run.py --check
APPR=$(python3 -I "$D/mock_fetch_into.py" "$R/research/alpha" "$OBJ" | tail -1)
printf '\n%s\n' "$APPR" >> "$REG"; $G commit -q -am approval
echo "== --run:"; $PY r016_run.py --run 2>&1 | grep -E '^outcome|"crit": 1' | head -3
python3 -I -c "
import json; r=[json.loads(l) for l in open('results.jsonl') if '\"mode\": \"read\"' in l][0]
print('   read row: registration_commit', r['registration_commit'][:12], '(the REGISTERED commit was ${REG0:0:12}) | cal_h1', r['params']['cal_h1'])"
echo "== what a binding check would see: git diff REGISTERED..HEAD on the guarded code:"; git diff --stat "$REG0" HEAD -- ct16 r016_run.py r016_fetch.py "$REG" | tail -3
