"""Reviewer B, 016 Revision 1 recheck: a SYNTHETIC stand-in for the real repository.

    python3 -I -S make_standin016_r1.py <real repo> <dest> [--gate-open] [--no-pin-patch] [--draft]

`git clone --local` of the real repository at R016_REV (default: the real HEAD) on branch alpha-discovery: real history
(anchor 9c131e2, 015's committed ledger rows), byte-identical Revision 1 code. SYNTHETIC data from the suite's own
`_synthetic_tree()` (nothing is copied or linked from the real data tree); the forward objects are pickled to
<dest>.objs.pickle for mock hooks. So that the stand-in can play the real repository, the ONE line `REAL_REPO = ...`
of r016_run.py is pointed at <dest> (unless --no-pin-patch). --gate-open also moves the ONE line `READ_NOT_BEFORE` of
ct16/rule.py to 2026-10-01: the only stand-in for "after 2028-01-08" now that -I -S closes the .pth route. The hash
rows of the patched files and of the two data files are rewritten; status REGISTERED (unless --draft); committed as
the owner. Prints <dest>."""
import contextlib, io, os, pickle, re, subprocess, sys
from pathlib import Path

real, dest = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()
if str(dest).startswith(str(real)):
    raise SystemExit('refusing: the stand-in must live outside the real repository')
subprocess.run(['/usr/bin/git', 'clone', '-q', '--local', str(real), str(dest)], check=True)
REV = os.environ.get('R016_REV', 'HEAD')
rev = subprocess.run(['/usr/bin/git', 'rev-parse', REV], cwd=real, capture_output=True, text=True).stdout.strip()
subprocess.run(['/usr/bin/git', '-c', 'advice.detachedHead=false', 'checkout', '-q', '-B', 'alpha-discovery', rev],
               cwd=dest, check=True)
A = dest / 'research' / 'alpha'
patched = []
if '--no-pin-patch' not in sys.argv:
    src = (A / 'r016_run.py').read_text()
    new = re.sub(r"^REAL_REPO = Path\('[^']*'\)", f"REAL_REPO = Path('{dest}')", src, count=1, flags=re.M)
    assert new != src
    (A / 'r016_run.py').write_text(new); patched.append('r016_run.py')
if '--gate-open' in sys.argv:
    src = (A / 'ct16/rule.py').read_text()
    new = re.sub(r'^READ_NOT_BEFORE = dt\.date\(20[0-9][0-9], 1, 8\)', 'READ_NOT_BEFORE = dt.date(2026, 10, 1)', src, count=1,
                 flags=re.M)
    assert new != src
    (A / 'ct16/rule.py').write_text(new); patched.append('ct16/rule.py')
sys.path.append(str(A / 'tests'))
with contextlib.redirect_stdout(io.StringIO()):
    import test_r016 as T             # also disables the network for this process
RUN = T.RUN
assert RUN.HERE == A, RUN.HERE
with contextlib.redirect_stdout(io.StringIO()):
    objs = T._synthetic_tree(A)
Path(str(dest) + '.objs.pickle').write_bytes(pickle.dumps(objs))
reg = A / 'registrations' / RUN.REG_NAME
text = reg.read_text()
for f in (*RUN.GUARDED_DATA, *patched):
    text, n = re.subn(r'(\|\s*`' + re.escape(f) + r'`\s*\|\s*`)[0-9a-f]{64}(`)',
                      lambda m: m.group(1) + RUN.sha256(A / f) + m.group(2), text)
    assert n == 1, f
if '--draft' not in sys.argv:
    text = re.sub(r'^\*\*Status: DRAFT', '**Status: REGISTERED', text, count=1, flags=re.M)
reg.write_text(text)
g = lambda *a: subprocess.run(['/usr/bin/git', '-c', 'user.name=owner', '-c', 'user.email=owner@x', *a], cwd=A,
                              capture_output=True, text=True)
g('add', *patched, 'registrations/' + RUN.REG_NAME)
r = g('commit', '-q', '-m', 'STANDIN: 016 REGISTERED (synthetic data; REAL_REPO -> this path)')
assert r.returncode == 0, r.stderr
print(dest)
