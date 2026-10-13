#!/bin/bash
# Reviewer B, 016 R1: M2 re-run (gate-open stand-in playing the real repository).
source "$(dirname "$0")/r1_lib.sh"
mk m2 --gate-open; A=$S/m2/research/alpha; cd $A
w() { grep -E '^\(v\)|^\(d\)|guard open|FORGED|^\(g1' | cut -c1-150; }
echo "== baseline -I -S:"; $PY r016_run.py --check | w
echo "== no flags:"; python3 r016_run.py --check | w
echo "== -I only:"; python3 -I r016_run.py --check | w
echo "== -I -S with PYTHONPATH=<forged datetime> (ignored):"; E=$S/evil_pp; mkdir -p $E; echo 'raise SystemExit("FORGED datetime")' > $E/datetime.py; PYTHONPATH=$E $PY r016_run.py --check | w
echo "== throwaway venv, same 3.14.8, a .pth in its site-packages, run with -I -S:"
V=$S/venv_pth; rm -rf $V; python3 -m venv $V; SITE=$($V/bin/python -I -c 'import site; print(site.getsitepackages()[0])')
cp $P/fakeclock_module.py.txt $SITE/_rb016clock.py; echo 'import _rb016clock' > $SITE/zz_rb016.pth
$V/bin/python -I -S r016_run.py --check | w
echo "   same venv with -I only (the .pth runs):"; $V/bin/python -I r016_run.py --check | w
echo "== forged __pycache__/r016_run pyc, fetch CLI (network denied):"; $PY $P/forge_pyc_r1.py run $A >/dev/null; nonet python3 -I -S r016_fetch.py 2>&1 | tail -1 | cut -c1-150; rm -f __pycache__/r016_run.*.pyc
echo "== ATTACK: -X pycache_prefix=<tmp>/r016f-pyc-evil (the name r016_run keeps), forged ct16/stats bytecode there:"
EV=$(python3 -I -S -c 'import tempfile;print(tempfile.gettempdir())')/r016f-pyc-rbevil; rm -rf $EV
$PY $P/forge_pyc_r1.py stats $A $EV
python3 -I -S -X pycache_prefix=$EV r016_run.py --check | w
python3 -I -S -X pycache_prefix=$EV -c "
import sys; sys.argv=['r016_run.py']; sys.path.append('$A')
import r016_run as R
print('   ct16.stats forged:', getattr(R.S, 'FORGED_BY_REVIEWER', False), '| stats __cached__ under', R.S.__cached__.split('/r016f')[0][-12:] + '/r016f' + R.S.__cached__.split('/r016f')[1][:12], '| module_origin_failures:', R.module_origin_failures())"
rm -rf $EV
echo "== committed top-level csv.py:"; printf 'X = 1\n' > csv.py; $G add csv.py; $G commit -q -m csv; $PY r016_run.py --check | w; $G rm -q csv.py; $G commit -q -m rm
echo "== committed top-level tokenize/ package:"; mkdir -p tokenize; printf '\n' > tokenize/__init__.py; $G add tokenize; $G commit -q -m pkg; $PY r016_run.py --check | w; $G rm -rq tokenize; $G commit -q -m rm
echo "== git: absolute path and neutral config (grep):"; grep -n "^GIT_BIN\|GIT_CONFIG_GLOBAL\|core.hooksPath\|no-replace" r016_run.py | cut -c1-140
