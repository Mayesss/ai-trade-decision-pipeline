"""Reviewer B, 016 helper: write <A>/__pycache__/r016_run.<tag>.pyc = r016_run.py's source PLUS a wrapper that drops
the (t) refusal and then stops (SystemExit) before any network, stamped with the source's mtime and size so the
default (timestamp) pyc validation accepts it.   python3 -I forge_pyc.py <research/alpha of a SYNTHETIC tree>"""
import importlib.util as iu, marshal, struct, sys
from pathlib import Path
A = Path(sys.argv[1]).resolve()
if str(A).startswith('/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline'):
    raise SystemExit('reviewer probe: refusing the REAL repository; synthetic stand-ins only')
src = A / 'r016_run.py'
body = src.read_text() + '''
_rb_orig = guard_failures
def guard_failures(*a, **k):
    f, c = _rb_orig(*a, **k)
    f = [x for x in f if not x.startswith('(t)')]
    raise SystemExit(f'FORGED r016_run loaded from __pycache__ (pycache_prefix={sys.pycache_prefix}): the guard would return {f}')
'''
st = src.stat()
pyc = A / '__pycache__' / f'r016_run.{sys.implementation.cache_tag}.pyc'
pyc.parent.mkdir(exist_ok=True)
pyc.write_bytes(iu.MAGIC_NUMBER + struct.pack('<III', 0, int(st.st_mtime) & 0xFFFFFFFF, st.st_size & 0xFFFFFFFF)
                + marshal.dumps(compile(body, str(src), 'exec')))
print('forged', pyc)
