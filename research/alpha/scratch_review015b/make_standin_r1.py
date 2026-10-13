"""Reviewer B, 015 Revision 1 recheck: a SYNTHETIC stand-in for the real repository.

    python3 -I make_standin_r1.py <real repo> <dest> [effect] [--no-pin-patch]

`git clone --local` of the real repository at 7b265d8 (real history, anchor, byte-identical Revision 1 code), on
branch alpha-discovery; SYNTHETIC data from the suite's own build_synthetic_tree(); then, so that the stand-in can
play the real repository, the ONE line `REAL_REPO = Path('…')` of r015_run.py is pointed at <dest> (unless
--no-pin-patch); the HEAD registration's hash rows for r015_run.py and every data file are rewritten; status
REGISTERED; committed. Nothing is copied or linked from the real data tree. Prints <dest>."""
import contextlib, io, re, subprocess, sys
from pathlib import Path

real, dest = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()
effect = float(sys.argv[3]) if len(sys.argv) > 3 and not sys.argv[3].startswith('--') else 0.0
patch = '--no-pin-patch' not in sys.argv
subprocess.run(['git', 'clone', '-q', '--local', str(real), str(dest)], check=True)
subprocess.run(['git', '-c', 'advice.detachedHead=false', 'checkout', '-q', '-B', 'alpha-discovery', '7b265d8'], cwd=dest, check=True)
A = dest / 'research' / 'alpha'
if patch:
    src = (A / 'r015_run.py').read_text()
    new = re.sub(r"^REAL_REPO = Path\('[^']*'\)", f"REAL_REPO = Path('{dest}')", src, count=1, flags=re.M)
    assert new != src
    (A / 'r015_run.py').write_text(new)
sys.path.insert(0, str(A / 'tests'))
with contextlib.redirect_stdout(io.StringIO()):
    import test_r015 as T
R = T.R
assert R.HERE == A, R.HERE
with contextlib.redirect_stdout(io.StringIO()):
    T.build_synthetic_tree(A, seed=11, effect=effect)
reg = A / 'registrations' / '015-month-end-liquidity-cycle.md'
text = reg.read_text()
for f in (*R.GUARDED_DATA, 'r015_run.py'):
    text, n = re.subn(r'(\|\s*`' + re.escape(f) + r'`\s*\|\s*`)[0-9a-f]{64}(`)',
                      lambda m: m.group(1) + R.sha256(A / f) + m.group(2), text)
    assert n == 1, f
text = re.sub(r'^\*\*Status: DRAFT', '**Status: REGISTERED', text, count=1, flags=re.M)
reg.write_text(text)
g = lambda *a: subprocess.run(['git', '-c', 'user.name=owner', '-c', 'user.email=owner@x', *a], cwd=A, capture_output=True, text=True)
g('add', 'r015_run.py', 'registrations/015-month-end-liquidity-cycle.md')
r = g('commit', '-q', '-m', 'STANDIN: 015 REGISTERED (synthetic data; REAL_REPO -> this path)')
assert r.returncode == 0, r.stderr
print(dest)
