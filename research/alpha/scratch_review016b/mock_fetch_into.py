"""Reviewer B, 016 helper: run the fetcher with the suite's mock hooks (synthetic objects) for the runner tree <A>
into a scratch store, then copy the store to <A>/data/r016/forward (which may be a symlink: that is the attack).
    python3 -I mock_fetch_into.py <research/alpha of a SYNTHETIC tree> <objs.pickle>"""
import datetime as dt, pickle, shutil, sys, tempfile
from pathlib import Path
A = Path(sys.argv[1]).absolute()
if str(A).startswith('/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline'):
    raise SystemExit('reviewer probe: refusing the REAL repository; synthetic stand-ins only')
sys.path.insert(0, str(A / 'tests')); sys.path.insert(0, str(A))
import r016_fetch as F, r016_run as RUN, test_r016 as T
objs = pickle.loads(Path(sys.argv[2]).read_bytes())
hooks = T._mock_hooks(objs, page=50); hooks.pop('calls')
store = Path(tempfile.mkdtemp(prefix='rb016-store-'))
row = F.fetch(root=RUN.HERE, store=store, hooks=hooks, now=dt.datetime(2028, 1, 10, tzinfo=dt.UTC))
dst = A / RUN.STORE_REL
dst.parent.mkdir(parents=True, exist_ok=True)
shutil.copytree(store, dst, dirs_exist_ok=True)
fl = [l for l in (A / 'results.jsonl').read_text().splitlines() if '"fetched"' in l][-1]
print(f'016 read: APPROVED 2028-01-11 row {RUN.line_sha(fl)}')
