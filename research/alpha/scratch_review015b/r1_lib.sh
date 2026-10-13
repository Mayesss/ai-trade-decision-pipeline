# Reviewer B, 015 Revision 1 recheck: shared helpers (sourced). Synthetic stand-ins only.
REPO=/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline
P=$REPO/research/alpha/scratch_review015b
S=/private/tmp/claude-502/-Users-mayess-sammoud-Documents-persogit-ai-trade-decision-pipeline/7b2e3f5e-bd1a-4fe0-b780-0c6d54079801/scratchpad/rb015r1
PY="python3 -I -S"
mk() { cd "${S:?}" && rm -rf "${S:?}/${1:?}"; python3 -I "$P/make_standin_r1.py" "$REPO" "$S/$1" ${2:-} >/dev/null || echo "MK FAILED $1"; }
G="git -c user.name=owner -c user.email=owner@x"
nrows() { python3 -I -c "import sys,json;print(sum(1 for l in open(sys.argv[1]) if l.strip()))" "$1"; }
