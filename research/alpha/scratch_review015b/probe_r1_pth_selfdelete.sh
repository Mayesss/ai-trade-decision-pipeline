#!/bin/bash
# Recheck, edge of M2: a one-shot .pth (deletes itself) that replaces sys.flags with a look-alike saying -I -S, run
# WITHOUT -S. Needs a write into the interpreter's site-packages (here: a throwaway venv).
set -u; source "$(dirname "$0")/r1_lib.sh"
mk pth; A=$S/pth/research/alpha; cd "$A"
VE=$S/venv_pth2; rm -rf "$VE"; python3 -m venv --without-pip "$VE"
SP=$("$VE/bin/python" -c "import sysconfig;print(sysconfig.get_paths()['purelib'])")
python3 -I - "$SP/zz_once.pth" <<'PY'
import sys
p = sys.argv[1]
code = ("f=sys.flags; d={k: getattr(f, k) for k in dir(f) if not k.startswith('_') and k not in "
        "('n_fields','n_sequence_fields','n_unnamed_fields','count','index')}; d.update(isolated=1, no_site=1); "
        f"sys.flags=types.SimpleNamespace(**d); os.remove({p!r})")
open(p, 'w').write('import os, sys, types; exec(' + repr(code) + ')\n')
PY
echo "-- venv python -I (no -S), one-shot .pth replacing sys.flags:"; "$VE/bin/python" -I r015_run.py --check 2>&1 | tail -4 | cut -c1-160
echo "-- .pth left behind: $(ls "$SP"/*.pth 2>/dev/null | wc -l)"
