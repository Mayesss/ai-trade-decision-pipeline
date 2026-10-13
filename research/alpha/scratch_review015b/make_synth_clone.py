"""Reviewer B, 015: build a SYNTHETIC copy of the real layout for attacks.

    python3 -I make_synth_clone.py <real repo> <dest dir> [effect]

`git clone --local` of the real repository (so the real history, the anchor 15c6421 and byte-identical r015 code are
present), then SYNTHETIC data written by the suite's own build_synthetic_tree() into <dest>/research/alpha/data/r015
(nothing is copied from the real data tree), the registration's data hash rows rewritten to the synthetic hashes,
status REGISTERED, committed. The real repository is only read (clone). Prints the clone path."""
import re, subprocess, sys, io, contextlib
from pathlib import Path

real, dest = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()
effect = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0
subprocess.run(['git', 'clone', '-q', '--local', str(real), str(dest)], check=True)
subprocess.run(['git', '-c', 'advice.detachedHead=false', 'checkout', '-q', '-B', 'alpha-discovery', '012eff7'], cwd=dest, check=True)
A = dest / 'research' / 'alpha'
sys.path.insert(0, str(A / 'tests'))
sys.path.insert(0, str(A))
import r015_run as R            # the CLONE's copy (HERE = dest/research/alpha)
import test_r015 as T
assert R.HERE == A, R.HERE
with contextlib.redirect_stdout(io.StringIO()):
    T.build_synthetic_tree(A, seed=11, effect=effect)
reg = A / 'registrations' / '015-month-end-liquidity-cycle.md'
text = reg.read_text()
for f in R.GUARDED_DATA:
    text = re.sub(r'(\|\s*`' + re.escape(f) + r'`\s*\|\s*`)[0-9a-f]{64}(`)', lambda m: m.group(1) + R.sha256(A / f) + m.group(2), text)
text = text.replace('**Status: DRAFT', '**Status: REGISTERED', 1)
reg.write_text(text)
g = lambda *a: subprocess.run(['git', '-c', 'user.name=rb', '-c', 'user.email=rb@rb', *a], cwd=A, capture_output=True, text=True)
print(g('commit', '-q', '-am', 'SYNTH: 015 REGISTERED (synthetic data hashes)').stdout, end='')
print(dest)
