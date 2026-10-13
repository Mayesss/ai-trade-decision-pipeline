"""Reviewer B, 016, minor: the read gate is a PATH check on a mutable global. SYNTHETIC stand-in only.
    python3 -I probe_minor_gate.py <stand-in research/alpha>"""
import shutil, sys, tempfile
from pathlib import Path
A = Path(sys.argv[1]).resolve()
if str(A).startswith('/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline'):
    raise SystemExit('reviewer probe: refusing the REAL repository; synthetic stand-ins only')
sys.path.insert(0, str(A))
import r016_run as RUN
from ct16 import data as D
try:
    RUN.Panel('run'); print('real path, no gate: OPENED')
except RUN.Refused as e:
    print('real path, no gate: refused (correct):', str(e)[:70])
cp = Path(tempfile.mkdtemp(prefix='rb016-copy-')) / 'pull'
shutil.copytree(A / RUN.KLINES_A_REL, cp)
p = RUN.Panel('run', klines_a=cp, store=tempfile.mkdtemp())
sums = D.parse_sums((p.a / 'SHA256SUMS').read_text())
print('a byte COPY of the pinned pull, no gate: Panel opened; its SHA256SUMS hash equals the registered one:',
      RUN.sha256(cp / 'SHA256SUMS') == dict(RUN.HASH_ROW.findall((A / RUN.REG_REL).read_text()))[RUN.GUARDED_DATA[0]],
      '| files readable through it:', len(sums))
RUN._GATE = (str(RUN.HERE), 'run', 'x')
RUN.Panel('run'); print('_GATE assigned by hand: the real path opens')
