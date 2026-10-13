# Reviewer B, 016 Revision 1 recheck: shared helpers (sourced). Synthetic stand-ins only.
REPO=/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline
P=$REPO/research/alpha/scratch_review016b
S=/private/tmp/claude-502/-Users-mayess-sammoud-Documents-persogit-ai-trade-decision-pipeline/7b2e3f5e-bd1a-4fe0-b780-0c6d54079801/scratchpad/rb016r1
PY="python3 -I -S"
G="/usr/bin/git -c user.name=owner -c user.email=owner@x"
REG=registrations/016-crypto-trend-verbatim.md
# mk <name> [flags...]: a fresh stand-in at $S/<name> (prints nothing)
mk() { local n=$1; shift; cd "${S:?}" && rm -rf "${S:?}/${n:?}" "$S/$n.objs.pickle"; $PY "$P/make_standin016_r1.py" "$REPO" "$S/$n" "$@" >/dev/null || echo "MK FAILED $n"; }
# fetch_in <alpha>: the stand-in's own real fetch(), mock hooks swapped in process (r1_fetch_harness.py.txt)
fetch_in() { $PY -c "$(cat "$P/r1_fetch_harness.py.txt")" "$1" "$2" 2>&1 | tail -1; }
approve() { local FL SHA; FL=$(grep '"mode": "fetched"' results.jsonl | tail -1); SHA=$(printf '%s' "$FL" | shasum -a 256 | cut -d' ' -f1)
  printf '\n016 read: APPROVED 2028-01-11 row %s\n' "$SHA" >> $REG; $G commit -q -m approval -- $REG; }
modes() { grep -o '"registration": "016[^"]*", "reg0[^}]*' results.jsonl >/dev/null; python3 -I -S -c "import sys,json;print([json.loads(l).get('mode') for l in open(sys.argv[1]) if '016-' in l])" "$1"; }
# Every probe from here on runs under `sandbox-exec` with the network denied (see probe_r1_m2.out's header note).
nonet() { sandbox-exec -p '(version 1)(allow default)(deny network*)' "$@"; }
