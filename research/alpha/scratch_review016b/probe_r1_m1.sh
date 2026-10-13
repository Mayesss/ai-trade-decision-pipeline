#!/bin/bash
# Reviewer B, 016 R1: M1 re-run. The stand-in plays the real repository (REAL_REPO -> stand-in; gate CLOSED: today's
# real date). Every mock / `now` route into it must refuse before any write.
source "$(dirname "$0")/r1_lib.sh"
mk m1; A=$S/m1/research/alpha; cd $A
N0=$(wc -l < results.jsonl); H0=$(/usr/bin/git rev-parse HEAD)
$PY -c "
import sys, datetime as dt, pickle, tempfile, contextlib, io
from pathlib import Path
A = Path('$A'); sys.path.append(str(A)); sys.path.append(str(A / 'tests'))
with contextlib.redirect_stdout(io.StringIO()):
    import test_r016 as T
F, RUN = T.F, T.RUN
objs = pickle.loads(Path('$S/m1.objs.pickle').read_bytes())
h = T._mock_hooks(objs, page=50); h.pop('calls')
NOW = dt.datetime(2028, 1, 10, tzinfo=dt.UTC)
def tryit(name, f):
    try:
        f(); print('  OPEN   ', name)
    except BaseException as e:
        print('  refused', name, '->', str(e).splitlines()[0][:110])
tryit('_fetch_in(root=REAL, store=tmp, mocks, now)', lambda: F._fetch_in(A, tempfile.mkdtemp(), h, NOW))
tryit('_fetch_in(root=tmp, store=REAL store, mocks, now)', lambda: F._fetch_in(tempfile.mkdtemp(), A / RUN.STORE_REL, h, NOW))
tryit('_fetch_in(root=REAL/research, ...) (a parent)', lambda: F._fetch_in(A.parent, tempfile.mkdtemp(), h, NOW))
tryit('fetch(now=...) (the real fetch takes no argument)', lambda: F.fetch(now=NOW))
tryit('fetch(hooks=...)', lambda: F.fetch(hooks=h))
fs = RUN.guard_failures(A, 'fetch', now=NOW)[0]
print('  guard_failures(REAL, now=2028):', [x[:80] for x in fs if x.startswith('(t)')])
RUN.RESULTS = A / 'results.jsonl'
tryit('record() into the real ledger without the real guard', lambda: RUN.record({'registration': RUN.REG_NAME, 'mode': 'read'}, 'x'))
tryit('run() (real guard: today < gate)', lambda: RUN.run())
tryit('Panel(run) on the real pull', lambda: RUN.Panel('run').load())
" 2>&1
echo "== ledger lines $N0 -> $(wc -l < results.jsonl); HEAD moved: $([ "$H0" = "$(/usr/bin/git rev-parse HEAD)" ] && echo no || echo YES); store exists: $([ -e data/r016/forward ] && echo YES || echo no)"
