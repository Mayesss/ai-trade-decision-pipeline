"""Shadow modules: a COPY of r014_run.py + ct/ in a temp dir (never the real tree). Shows that an untracked
statistics.py next to r014_run.py, an untracked package ct/shortvol/, or a stale ct/__pycache__/*.pyc whose
header matches the hashed source replace decision-path code while §9's hashes still match."""
import os, py_compile, shutil, subprocess, sys, tempfile, importlib.util
from pathlib import Path
SRC = Path(__file__).resolve().parent.parent
PY = str(SRC / '.venv/bin/python')

def fresh(d):
    (d / 'ct').mkdir(parents=True)
    for f in ('r014_run.py', 'ct/__init__.py', 'ct/shortvol.py'):
        shutil.copy2(SRC / f, d / f)

def run(d, code):
    return subprocess.run([PY, '-I', '-c', code], cwd=d, capture_output=True, text=True)

with tempfile.TemporaryDirectory() as t:
    t = Path(t)
    # 1. untracked statistics.py next to the runner (sys.path[0] = HERE is inserted BEFORE ct.shortvol imports statistics)
    d = t / 'a'; fresh(d)
    (d / 'statistics.py').write_text("print('SHADOW statistics loaded')\nfrom math import fsum\ndef fmean(x):\n    x=list(x); return fsum(x)/len(x)+0.01\nimport importlib\n")
    r = subprocess.run([PY, '-I', str(d / 'r014_run.py')], capture_output=True, text=True)
    print('1 statistics.py shadow via the real CLI entry:', 'SHADOW statistics loaded' in r.stdout, '| exit doc printed:', 'DISCOVERY refuses' in r.stderr)
    # 2. untracked package ct/shortvol/ beside the hashed ct/shortvol.py
    d = t / 'b'; fresh(d)
    (d / 'ct/shortvol').mkdir()
    (d / 'ct/shortvol/__init__.py').write_text("print('SHADOW package ct.shortvol')\nfrom math import *\n")
    r = subprocess.run([PY, '-I', str(d / 'r014_run.py')], capture_output=True, text=True)
    print('2 package ct/shortvol/ shadows ct/shortvol.py:', 'SHADOW package ct.shortvol' in r.stdout, (r.stderr.strip().splitlines() or [''])[-1][:100])
    # 3. stale pyc: compile a tampered source of the same size and mtime, then restore the hashed source
    d = t / 'c'; fresh(d)
    src = d / 'ct/shortvol.py'
    orig = src.read_bytes(); st = src.stat()
    tampered = orig.replace(b'SIZE_N = 0.40', b'SIZE_N = 9.99')
    assert len(tampered) == len(orig)
    src.write_bytes(tampered); os.utime(src, (st.st_atime, st.st_mtime))
    py_compile.compile(str(src), cfile=importlib.util.cache_from_source(str(src)))
    src.write_bytes(orig); os.utime(src, (st.st_atime, st.st_mtime))
    import hashlib
    print('3 source sha256 unchanged:', hashlib.sha256(src.read_bytes()).hexdigest() == hashlib.sha256((SRC / 'ct/shortvol.py').read_bytes()).hexdigest())
    r = run(d, "import sys; sys.path.insert(0,'.'); from ct import shortvol as sv; print('SIZE_N', sv.SIZE_N)")
    print('3 stale pyc loaded instead of the hashed source:', r.stdout.strip(), r.stderr.strip()[-200:])
