"""Reviewer B, 015, minors m1/m2 (throwaway copies only).

    python3 -I probe_minor_bytecode_gate.py <synthetic research/alpha (stand-in for the real tree)> <tmpdir>

m1  loaded_from_source() checks __file__ only: when r015_core is imported BEFORE r015_run (any entry other than
    `python r015_run.py`, e.g. a harness or the tests), a forged __pycache__ .pyc with the source's mtime and size
    is loaded and the check still passes.
m2  the gate is a PATH check bound to mutable module globals: (a) a byte copy of a guarded data file outside
    data/r015 loads with no guard at all; (b) stubbing guard_failures (not guard) makes the REAL guard() set the
    token, and the 'real' tree opens."""
import os, py_compile, shutil, subprocess, sys
from pathlib import Path
A, tmp = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()

# ---- m1
B = tmp / 'm1'
shutil.rmtree(B, ignore_errors=True); B.mkdir(parents=True)
for f in ('r015_core.py', 'r015_hmm.py', 'r015_run.py'):
    shutil.copy(A / f, B / f)
good = (B / 'r015_core.py').read_text()
evil = good.replace('CAL_CRIT = 1.91 ', 'CAL_CRIT = 9.99 ', 1)
assert evil != good and len(evil) == len(good)
st = (B / 'r015_core.py').stat()
(B / 'r015_core.py').write_text(evil)
os.utime(B / 'r015_core.py', ns=(st.st_atime_ns, st.st_mtime_ns))
py_compile.compile(str(B / 'r015_core.py'), cfile=str(B / '__pycache__' / f'r015_core.{sys.implementation.cache_tag}.pyc'))
(B / 'r015_core.py').write_text(good)
os.utime(B / 'r015_core.py', ns=(st.st_atime_ns, st.st_mtime_ns))
code = (f'import sys; sys.path.insert(0, {str(B)!r}); import r015_core; import r015_run as R; '
        'print("m1: CAL_CRIT in use", R.C.CAL_CRIT, "| loaded_from_source() ->", R.loaded_from_source(), '
        '"| r015_core.__cached__ =", r015_core.__cached__.split("/")[-2:])')
print(subprocess.run([sys.executable, '-I', '-c', code], capture_output=True, text=True).stdout.strip())
code2 = f'import sys; sys.path.insert(0, {str(B)!r}); import r015_run as R; print("m1 control (r015_run first):", R.C.CAL_CRIT)'
print(subprocess.run([sys.executable, '-I', '-c', code2], capture_output=True, text=True).stdout.strip())

# ---- m2
sys.path.insert(0, str(A))
import r015_run as R
real_us = A / R.US_ZIP
h = R.reg_table(A)[R.US_ZIP]
try:
    R.load_french(real_us, h); print('m2: UNEXPECTED, opened without guard')
except R.Refused:
    print('m2: control: the tree path is refused without the guard')
cp = tmp / 'copy.zip'
shutil.copy(real_us, cp)
d, v = R.load_french(cp, h)
print(f'm2a: a byte copy outside data/r015 loads with no guard: {len(d)} rows (same SHA-256 as the guarded file)')
R.guard_failures = lambda *a, **k: ([], 'stub-commit')
print('m2b: real guard() with guard_failures stubbed ->', R.guard(), '| guard is _REAL_GUARD:', R.guard is R._REAL_GUARD)
d, v = R.load_french(real_us, h)
print(f'm2b: the guarded tree opens: {len(d)} rows')
