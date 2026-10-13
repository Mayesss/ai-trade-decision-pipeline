#!/bin/bash
# Reviewer B, 015 Revision 2: the stated residual. A one-shot .pth (throwaway venv) that (1) deletes itself, (2) swaps
# sys.flags for a tuple subclass named 'flags' with isolated=1/no_site=1, (3) rewrites sys.orig_argv to carry -I -S,
# (4) installs a meta-path finder that drops sys.modules['site'] when r015_core is first imported. Run WITHOUT -S.
set -u; export R015_REV=e2a01b8; source "$(dirname "$0")/r1_lib.sh"
mk pf; A=$S/pf/research/alpha; cd "$A"
VE=$S/venv_pf; rm -rf "${VE:?}"; python3 -m venv --without-pip "$VE"
SP=$("$VE/bin/python" -c "import sysconfig;print(sysconfig.get_paths()['purelib'])")
python3 -I - "$SP/zz_full.pth" <<'PY'
import sys
p = sys.argv[1]
code = '''
f = sys.flags
names = [k for k in dir(f) if not k.startswith('_') and k not in ('n_fields','n_sequence_fields','n_unnamed_fields','count','index')]
vals = {k: getattr(f, k) for k in names}
vals.update(isolated=1, no_site=1)
flags = type('flags', (tuple,), {k: property(lambda self, k=k, v=vals: v[k]) for k in names})
sys.flags = flags(tuple(vals.values()))
sys.orig_argv = [sys.orig_argv[0], '-I', '-S'] + sys.orig_argv[1:]
def _fs(self, name, path=None, target=None, _sys=sys):
    if name == 'r015_core':
        _sys.modules.pop('site', None)
    return None
sys.meta_path.insert(0, type('F', (), {'find_spec': _fs})())
os.remove(PTH)
'''.replace('PTH', repr(p))
open(p, 'w').write('import os, sys; exec(' + repr(code) + ')\n')
PY
echo "-- venv python -I (no -S), full one-shot .pth:"; "$VE/bin/python" -I r015_run.py --check 2>&1 | tail -3 | cut -c1-140
echo "-- .pth left behind: $(ls "$SP"/*.pth 2>/dev/null | wc -l | tr -d ' ')"
