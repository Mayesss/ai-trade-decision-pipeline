"""Reviewer B, 016 R1 helper: forged bytecode, stamped with the source's mtime and size.
  python3 -I -S forge_pyc_r1.py run   <alpha>            -> <alpha>/__pycache__/r016_run.<tag>.pyc (prints FORGED at import)
  python3 -I -S forge_pyc_r1.py stats <alpha> <prefix>   -> <prefix>/<alpha abs>/ct16/__pycache__-less path for
                                                            ct16/stats.py under a pycache_prefix, with C_TA = 0.0"""
import importlib.util as iu, marshal, struct, sys
from pathlib import Path
kind, A = sys.argv[1], Path(sys.argv[2]).resolve()
if str(A).startswith('/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline'):
    raise SystemExit('reviewer probe: refusing the REAL repository; synthetic stand-ins only')
tag = sys.implementation.cache_tag
def write(src, body, pyc):
    st = src.stat()
    pyc.parent.mkdir(parents=True, exist_ok=True)
    pyc.write_bytes(iu.MAGIC_NUMBER + struct.pack('<III', 0, int(st.st_mtime) & 0xFFFFFFFF, st.st_size & 0xFFFFFFFF)
                    + marshal.dumps(compile(body, str(src), 'exec')))
    print('forged', pyc)
if kind == 'run':
    src = A / 'r016_run.py'
    write(src, 'raise SystemExit("FORGED r016_run loaded from __pycache__")\n', A / '__pycache__' / f'r016_run.{tag}.pyc')
else:
    prefix = Path(sys.argv[3])
    src = A / 'ct16' / 'stats.py'
    body = src.read_text() + '\nFORGED_BY_REVIEWER = True\n'
    write(src, body, prefix / str(src.parent).lstrip('/') / f'stats.{tag}.pyc')
