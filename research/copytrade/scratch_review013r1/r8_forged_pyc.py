"""Review r1 of 013: a forged __pycache__ .pyc whose header carries the SOURCE's mtime and size is loaded instead of
the (hash-verified) source. Throwaway directory only; shows the mechanism, which neither r013's (v) nor r008's
shadow_modules (which skips __pycache__) checks."""
import importlib.util, os, subprocess, sys, tempfile
from importlib._bootstrap_external import _code_to_timestamp_pyc
from pathlib import Path
d = Path(tempfile.mkdtemp(prefix='pyc_'))
(d / 'ct').mkdir()
(d / 'ct' / '__init__.py').write_text('')
src = d / 'ct' / 'mod.py'
src.write_text('VALUE = "hashed source"\n')
st = src.stat()
code = compile('VALUE = "FORGED bytecode"\n', str(src), 'exec')
pyc = Path(importlib.util.cache_from_source(str(src)))
pyc.parent.mkdir(exist_ok=True)
pyc.write_bytes(_code_to_timestamp_pyc(code, int(st.st_mtime), st.st_size))
out = subprocess.run([sys.executable, '-I', '-c', 'import sys; sys.path.insert(0, sys.argv[1]); from ct import mod; print(mod.VALUE)', str(d)],
                     capture_output=True, text=True)
print('loaded:', out.stdout.strip(), '| source on disk says:', src.read_text().strip())
out2 = subprocess.run([sys.executable, '-I', '-c', 'import sys, tempfile; sys.pycache_prefix = tempfile.mkdtemp(); sys.path.insert(0, sys.argv[1]); from ct import mod; print(mod.VALUE)', str(d)],
                      capture_output=True, text=True)
print('with sys.pycache_prefix set to a fresh empty dir first:', out2.stdout.strip())
