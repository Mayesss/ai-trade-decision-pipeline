#!/bin/bash
# Reviewer B, 015, m7: a transient .git/index.lock (an IDE's background `git status`, or 016's runner committing at
# the same moment) makes the STARTED commit fail before any data is opened; under D14 (no restart) the trial is dead.
set -u
ORIGIN=$1; S=$2
R=$S/repo_m7; rm -rf "$R"; git clone -q --local "$ORIGIN" "$R"; cp -R "$ORIGIN/research/alpha/data" "$R/research/alpha/data"
cd "$R/research/alpha" && git checkout -q alpha-discovery
touch "$R/.git/index.lock"
python3 -I r015_run.py --run 2>&1 | grep -E "^r015_core.Refused" | cut -c1-160
rm "$R/.git/index.lock"
echo "rows in the working tree: $(wc -l < results.jsonl) (mode: $(python3 -I -c "import json;print([json.loads(l)['mode'] for l in open('results.jsonl')])"))"
echo "--check after the lock is gone:"; python3 -I r015_run.py --check
