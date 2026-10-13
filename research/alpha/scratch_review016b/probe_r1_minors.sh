#!/bin/bash
# Reviewer B, 016 R1: the eleven minors re-run where they are executable. Network denied; synthetic stand-ins only.
source "$(dirname "$0")/r1_lib.sh"
w() { grep -E '^\([a-z0-9]+\)|guard open' | cut -c1-150; }
echo "### m1: the witness refuses before any row or file (gate-open stand-in, witness without the 2027-12 key)"
mk n1 --gate-open --no-pin-patch; A=$S/n1/research/alpha; cd $A; N0=$(wc -l < results.jsonl)
$PY -c "
import sys, pickle, tempfile, contextlib, io, datetime as dt
from pathlib import Path
A = Path('$A'); sys.path.append(str(A)); sys.path.append(str(A / 'tests'))
with contextlib.redirect_stdout(io.StringIO()):
    import test_r016 as T
F = T.F
objs = pickle.loads(Path('$S/n1.objs.pickle').read_bytes())
early = {k: v for k, v in objs.items() if (F.month_of_key(k) or '0') <= '2026-09'}
for wit, name in ((None, 'keys to 2026-09, server date 2028-01-09'), (([k for k in objs if 'BTCUSDT/1d' in k], dt.datetime(2026, 9, 15, tzinfo=dt.UTC)), 'all keys, server date 2026-09-15, before the stand-in gate 2026-10-01')):
    h = T._mock_hooks(early if wit is None else objs, page=50, witness=wit); h.pop('calls')
    st = Path(tempfile.mkdtemp())
    try:
        F._fetch_in(A, st, h, dt.datetime(2028, 1, 10, tzinfo=dt.UTC)); print('  OPEN', name)
    except SystemExit as e:
        print('  refused (' + name + '):', str(e).splitlines()[0][:90], '| store files:', sum(1 for _ in st.rglob('*')))
"
echo "  ledger lines $N0 -> $(wc -l < results.jsonl)"
echo "### m2: the read gate"
mk n2 --gate-open; A=$S/n2/research/alpha; cd $A
$PY -c "
import sys, shutil, tempfile; sys.pycache_prefix = tempfile.mkdtemp(prefix='r016f-pyc-'); sys.path.append('$A'); import r016_run as R
from pathlib import Path
cp = Path(tempfile.mkdtemp()) / 'pull'; shutil.copytree(R.HERE / R.KLINES_A_REL, cp)
try:
    R.Panel('run', klines_a=cp, store=tempfile.mkdtemp()); print('  a byte copy of the pull: OPENED')
except R.Refused as e: print('  a byte copy of the pull: refused:', str(e)[:70])
R._GATE = (str(R.HERE), 'run', 'x')
try:
    R.Panel('run'); print('  _GATE assigned by hand: OPENED')
except R.Refused as e: print('  _GATE assigned by hand: refused')
R._GUARD_PASSED = R._GATE
try:
    R.Panel('run'); print('  _GATE and _GUARD_PASSED both assigned by hand: OPENED (in-process residual)')
except R.Refused as e: print('  both assigned by hand: refused')
R._GUARD_FAILURES = lambda *a, **k: ([], {'commit': 'x', 'reg0': 'x', 'table': {}})
try:
    R.guard('run'); print('  guard() with a stubbed _GUARD_FAILURES: passed, _guard_ok =', R._guard_ok('run'))
except BaseException as e: print('  stubbed _GUARD_FAILURES: refused', str(e)[:60])
" 2>&1
echo "### m2b (new): the fetcher's network hooks are module globals: replaced in process, the REAL fetch() fills the REAL store"
echo "  (probe_r1_legit.out: r1_fetch_harness.py.txt replaces REAL_HOOKS and _real_witness_hooks, then calls fetch():"
grep -m1 'fetched:' $P/probe_r1_legit.out | sed 's/^/   /'
echo "### m3: rows"
mk n3 --gate-open; A=$S/n3/research/alpha; cd $A
printf '{"registration": "015-x.md", "mode": "result"}' >> results.jsonl; $G commit -q -m "no newline" -- results.jsonl
echo "  results.jsonl without a trailing newline:"; $PY r016_run.py --check | grep '(k)' | cut -c1-120
/usr/bin/git reset -q --hard HEAD~1
/usr/bin/git checkout -q --orphan orph; /usr/bin/git rm -rq --cached . >/dev/null; echo '{"registration": "016-crypto-trend-verbatim.md", "mode": "read"}' > results.jsonl; /usr/bin/git add results.jsonl; $G commit -q -m orphan-row; /usr/bin/git checkout -q -f alpha-discovery
/usr/bin/git config log.showRoot false
echo "  orphan-root read row with log.showRoot=false in the repository config:"; $PY r016_run.py --check | grep '(i)' | cut -c1-100
mk n3b --gate-open; A=$S/n3b/research/alpha; cd $A
echo '{"registration": "016-crypto-trend-verbatim.md", "mode": "read"}' >> results.jsonl; $G commit -q -m row -- results.jsonl; RC=$(/usr/bin/git rev-parse HEAD)
/usr/bin/git reset -q --hard HEAD~1; $G commit -q --allow-empty -m decoy; DEC=$(/usr/bin/git rev-parse HEAD); /usr/bin/git reset -q --hard HEAD~1; /usr/bin/git replace $RC $DEC
echo "  git replace <row commit> <decoy>:"; $PY r016_run.py --check | grep '(i)' | cut -c1-100
echo "### m6, m7, m9, m10, m11: text and code (grep of the real repository's committed files)"
cd $REPO/research/alpha
grep -n "MATIC\|POLUSDT\|migration" registrations/016-crypto-trend-verbatim.md | head -3 | cut -c1-170
grep -n "known_2026" r016_run.py | head -2 | cut -c1-120
grep -n "uv\|recovery\|--fingerprint" registrations/016-crypto-trend-verbatim.md | head -3 | cut -c1-170
grep -n "no approval or restart line" tests/test_r016.py | cut -c1-170
grep -n "every top-level" registrations/016-crypto-trend-verbatim.md | head -2 | cut -c1-170
grep -n "+ 8 days\|+ 7 days" registrations/016-crypto-trend-verbatim.md r016_run.py ct16/rule.py | head -4 | cut -c1-140
