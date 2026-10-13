"""Reviewer B, 016, (t): what an EARLY fetch (forged clock) leaves behind. Mock hooks that serve only what exists
"today" (klines and funding months <= 2026-09). Run under the forged-clock interpreter with now=None, so the guard's
own wall-clock path is used. SYNTHETIC stand-in only; the store is a scratch directory (mocks are refused on the
stand-in's real store), which is the only difference from the real CLI path.
    <clock venv>/bin/python -I probe_t_early_fetch.py <stand-in research/alpha> <objs.pickle>"""
import datetime as dt, pickle, subprocess, sys, tempfile
from pathlib import Path
A = Path(sys.argv[1]).resolve()
if str(A).startswith('/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline'):
    raise SystemExit('reviewer probe: refusing the REAL repository; synthetic stand-ins only')
sys.path.insert(0, str(A / 'tests')); sys.path.insert(0, str(A))
import r016_fetch as F, r016_run as RUN, test_r016 as T
objs = pickle.loads(Path(sys.argv[2]).read_bytes())
today = {k: v for k, v in objs.items() if (F.month_of_key(k) or '9999') <= '2026-09'}
hooks = T._mock_hooks(today, page=50); hooks.pop('calls')
print('wall clock as the guard sees it:', dt.datetime.now(dt.UTC).date(), '| objects served:', len(today), 'of', len(objs))
store = Path(tempfile.mkdtemp(prefix='rb016-early-'))
try:
    F.fetch(root=RUN.HERE, store=store, hooks=hooks)
except (SystemExit, F.FetchError) as e:
    print('fetch stopped:', str(e).splitlines()[0][:80], '|', [l.strip()[:40] for l in str(e).splitlines()[1:4]])
head = subprocess.run(['git', 'show', 'HEAD:./results.jsonl'], cwd=A, capture_output=True, text=True).stdout
print('committed 016 rows left behind:', [l.split('"mode": "')[1].split('"')[0] for l in head.splitlines() if '016-crypto' in l])
kl = sorted(p.name for p in (store / 'klines').rglob('*.zip'))
print('forward kline files written to the store:', len(kl), '| months', sorted({F.month_of_key('-' + n.split('-1d-')[1]) for n in kl}))
print('guard (fetch mode) now:', RUN.guard_failures(RUN.HERE, 'fetch', toplevel=RUN.REPO, anchor=RUN.ANCHOR, env_check=True))
