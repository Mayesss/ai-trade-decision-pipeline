#!/bin/bash
# Reviewer B, 016 R2: R1-M1 and everything around it. Gate-open stand-in playing the real repository; network denied.
# A forged ct16/stats bytecode writes a MARKER file when it executes, so "loaded or not" is observed, not inferred.
source "$(dirname "$0")/r2_lib.sh"
mk x --gate-open; A=$S/x/research/alpha; cd $A
TMP=$(python3 -I -S -c 'import tempfile;print(tempfile.gettempdir())'); EV=$TMP/r016f-pyc-rbevil; MK=$S/MARKER
w() { grep -E '^\(v\)|^\(d\)|guard open|^\(g1|^\(t\)|witness|fetch stopped' | cut -c1-130; }
seed() { rm -rf $EV $MK; $PY $P/forge_pyc_r1.py stats $A $EV $MK >/dev/null; }
loaded() { [ -e $MK ] && echo "   MARKER: forged ct16.stats EXECUTED" || echo "   marker: forged code not executed"; }
echo "== R1-M1: run CLI, -X pycache_prefix=<forged>:"; seed; python3 -I -S -X pycache_prefix=$EV r016_run.py --check | w; loaded
echo "== R1-M1: fetch CLI, -X pycache_prefix=<forged>:"; seed; python3 -I -S -X pycache_prefix=$EV r016_fetch.py 2>&1 | tail -3 | w; loaded
for X in importtime frozen_modules=off dev utf8 warn_default_encoding int_max_str_digits=0 no_debug_ranges; do
  echo "== -X $X:"; python3 -I -S -X $X r016_run.py --check 2>/dev/null | w
done
echo "== PYTHONPYCACHEPREFIX=<forged> with -I -S (ignored by -E):"; seed; PYTHONPYCACHEPREFIX=$EV $PY r016_run.py --check | w; loaded
echo "== PYTHONPYCACHEPREFIX=<forged> WITHOUT -I -S:"; seed; PYTHONPYCACHEPREFIX=$EV python3 r016_run.py --check | w; loaded
echo "== PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 PYTHONNODEBUGRANGES=1 with -I -S:"; PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 PYTHONNODEBUGRANGES=1 $PY r016_run.py --check | w
echo "== PYTHONSAFEPATH=1 without -I (and -S only):"; PYTHONSAFEPATH=1 python3 -S r016_run.py --check | w
echo "== an OLD fetcher-style prefix pre-created in TMP (r016f-pyc-*), forged, then the run CLI:"; seed; $PY r016_run.py --check | w; loaded
echo "== RACE: a watcher plants forged ct16/stats bytecode into every NEW r016*-pyc-* directory as it appears; 20 runs each"
rm -f $MK
python3 -I -S -c "
import os, sys, time, subprocess
tmp, A, mk, P = '$TMP', '$A', '$MK', '$P'
seen = set(n for n in os.listdir(tmp) if n.startswith('r016'))
t_end = time.time() + float(sys.argv[1])
while time.time() < t_end:
    for n in os.listdir(tmp):
        if n.startswith('r016') and n not in seen:
            seen.add(n)
            subprocess.run([sys.executable, '-I', '-S', P + '/forge_pyc_r1.py', 'stats', A, os.path.join(tmp, n), mk], capture_output=True)
" 120 &
WP=$!; sleep 1
for i in $(seq 20); do python3 -I -S r016_run.py --check >/dev/null 2>&1; done; echo "   run CLI x20:"; loaded
for i in $(seq 20); do python3 -I -S r016_fetch.py >/dev/null 2>&1; done; echo "   fetch CLI x20:"; loaded
kill $WP 2>/dev/null; wait $WP 2>/dev/null
echo "== in-process: a -c program that dresses __main__ as r016_fetch.py with a forged prefix (cf. r1-m4):"
seed
python3 -I -S -c "
import sys; m = sys.modules['__main__']; m.__file__ = '$A/r016_fetch.py'; m._R016_PREFIX = sys.pycache_prefix = '$EV'
sys.path.append('$A'); import r016_run as R
print('   forged stats loaded:', getattr(R.S, 'FORGED_BY_REVIEWER', False), '| (v) failures:', [x[:60] for x in R.environment_failures(R.HERE)])"
loaded
rm -rf $EV $MK
