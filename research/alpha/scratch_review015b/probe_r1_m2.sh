#!/bin/bash
# Recheck M2 (Revision 1) + 016-B M2 routes: isolation flags, PYTHONPATH, forged stdlib, forged __pycache__, module
# origin, committed top-level stdlib names, a site-packages .pth (in a throwaway venv), -O. Synthetic stand-in.
set -u; source "$(dirname "$0")/r1_lib.sh"
mk m2; A=$S/m2/research/alpha; cd "$A"
EV=$(mktemp -d); cp "$P/evil_pythonpath/statistics.py.txt" "$EV/statistics.py"
v() { grep -E '^\(v\)|guard open' | cut -c1-140; }
echo "== baseline -I -S:"; $PY r015_run.py --check
echo "== no flags:"; python3 r015_run.py --check | v
echo "== -I only (no -S):"; python3 -I r015_run.py --check | v
echo "== no -I, PYTHONPATH=forged statistics:"; PYTHONPATH=$EV python3 -S r015_run.py --check | v
echo "== -I -S, forged statistics put first on sys.path by a -c entry, then the runner's main():"
$PY -c "import sys; sys.path.insert(0, '$EV'); sys.argv=['x','--check']; sys.path.append('$A'); import r015_run as R; R.main()" | v
echo "== -I -S, forged __pycache__ r015_core (CAL_CRIT 9.99, mtime/size kept) imported before r015_run:"
B=$(mktemp -d); cp -R "$A"/. "$B"/ 2>/dev/null; python3 -I - "$A" <<'PY'
import os, py_compile, sys
from pathlib import Path
A = Path(sys.argv[1]); f = A / 'r015_core.py'; good = f.read_text()
evil = good.replace('CAL_CRIT = 1.92', 'CAL_CRIT = 9.99', 1); assert evil != good and len(evil) == len(good)
st = f.stat(); f.write_text(evil); os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns))
py_compile.compile(str(f), cfile=str(A / '__pycache__' / f'r015_core.{sys.implementation.cache_tag}.pyc'))
f.write_text(good); os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns))
PY
$PY -c "import sys; sys.path.append('$A'); import r015_core; sys.argv=['x','--check']; import r015_run as R; print('CAL_CRIT in use', R.C.CAL_CRIT); R.main()" | grep -E 'CAL_CRIT|^\(v\)|guard open' | cut -c1-140
rm -f "$A/__pycache__/r015_core.cpython-314.pyc"
echo "== -O:"; python3 -I -S -O r015_run.py --check | v
for name in csv statistics; do
  echo "== committed top-level $name.py (stdlib name):"
  echo "X = 1" > $name.py; $G add $name.py; $G commit -q -m "add $name.py"
  $PY r015_run.py --check | grep -E '^\(v\)|^\(r\)|guard open' | cut -c1-140
  $PY -c "import sys; sys.path.append('$A'); import $name; print('  import $name resolves to', $name.__file__)"
  $G rm -q $name.py; $G commit -q -m "rm $name.py"
done
echo "== after removing them (registration and code unchanged since reg0):"; $PY r015_run.py --check
echo "== a .pth in site-packages (throwaway venv; the .pth runs code at startup unless -S):"
VE=$S/venv_pth; rm -rf "$VE"; python3 -m venv --without-pip "$VE"
SP=$("$VE/bin/python" -c "import sysconfig;print(sysconfig.get_paths()['purelib'])")
echo "import sys; sys.stderr.write('PTH EXECUTED\n')" > "$SP/zz_evil.pth"
echo "-- venv python -I (no -S):"; "$VE/bin/python" -I r015_run.py --check 2>&1 | grep -E 'PTH|^\(v\)|guard open' | cut -c1-140
echo "-- venv python -I -S:"; "$VE/bin/python" -I -S r015_run.py --check 2>&1 | grep -E 'PTH|^\(v\)|guard open' | cut -c1-140
rm "$SP/zz_evil.pth"
echo "-- venv python -I -S, .pth removed:"; "$VE/bin/python" -I -S r015_run.py --check 2>&1 | grep -E '^\(v\)|guard open' | cut -c1-140
