"""Reviewer B, 016: build a SYNTHETIC stand-in of the real layout for attacks.

    python3 -I make_synth_clone016.py <real repo> <dest dir>

`git clone --local` of the real repository at 66df71a (the real history, the anchor 9c131e2 and byte-identical 016
code), then SYNTHETIC data written by the suite's own `_synthetic_tree()` into <dest>/research/alpha/data/ (a scout-like
pull <= 2026-06 with its SHA256SUMS, an empty static exchangeInfo). Nothing is copied from the real data tree. The
forward months (2026-07 -> 2027-12) and funding are pickled next to the clone (<dest>.objs.pickle) for mock fetches.
The registration's two data hash rows are rewritten to the synthetic hashes, status REGISTERED, committed."""
import contextlib, io, pickle, re, subprocess, sys
from pathlib import Path

real, dest = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()
subprocess.run(['git', 'clone', '-q', '--local', str(real), str(dest)], check=True)
subprocess.run(['git', '-c', 'advice.detachedHead=false', 'checkout', '-q', '-B', 'alpha-discovery', '66df71a'], cwd=dest,
               check=True)
A = dest / 'research' / 'alpha'
sys.path.insert(0, str(A / 'tests'))
sys.path.insert(0, str(A))
import r016_run as RUN            # the CLONE's copy (HERE = dest/research/alpha)
import test_r016 as T
assert RUN.HERE == A, RUN.HERE
with contextlib.redirect_stdout(io.StringIO()):
    objs = T._synthetic_tree(A)
Path(str(dest) + '.objs.pickle').write_bytes(pickle.dumps(objs))
reg = A / 'registrations' / RUN.REG_NAME
text = reg.read_text()
for f in RUN.GUARDED_DATA:
    text = re.sub(r'(\|\s*`' + re.escape(f) + r'`\s*\|\s*`)[0-9a-f]{64}(`)', lambda m: m.group(1) + RUN.sha256(A / f)
                  + m.group(2), text)
text = text.replace('**Status: DRAFT', '**Status: REGISTERED', 1)
reg.write_text(text)
g = lambda *a: subprocess.run(['git', '-c', 'user.name=rb', '-c', 'user.email=rb@rb', *a], cwd=A, capture_output=True,
                              text=True)
g('commit', '-q', '-am', 'SYNTH: 016 REGISTERED (synthetic data hashes)')
print(dest)
