"""Reviewer B, 016, M1 (the 015 M1 analogue, in the fetcher): `fetch(root=<the real repository>, store=<elsewhere>,
hooks=<mocks>, now=<2028>)` passes every pre-check, because the pre-checks only protect the real STORE. The real
guard then runs on the real repository with the caller's `now` (the date gate is a parameter here) and the mock
fetch commits fetch_started / fetch_attempt / fetched rows into the REAL results.jsonl. The real fetch is then
closed forever by (f), and the read refuses on (g3).

    python3 -I probe_m1_fetch_root.py <stand-in research/alpha> <objs.pickle>      (synthetic stand-in only)
"""
import datetime as dt, pickle, subprocess, sys, tempfile
from pathlib import Path
A = Path(sys.argv[1]).resolve()
if str(A).startswith('/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline'):
    raise SystemExit('reviewer probe: refusing the REAL repository; synthetic stand-ins only')
sys.path.insert(0, str(A / 'tests')); sys.path.insert(0, str(A))
import r016_fetch as F, r016_run as RUN, test_r016 as T
assert RUN.HERE == A
objs = pickle.loads(Path(sys.argv[2]).read_bytes())
hooks = T._mock_hooks(objs, page=50); hooks.pop('calls')
NOW = dt.datetime(2028, 1, 10, tzinfo=dt.UTC)
print('today (wall clock):', dt.date.today(), '| real guard (t) present:', any(x.startswith('(t)') for x in
      RUN.guard_failures(RUN.HERE, 'fetch', toplevel=RUN.REPO, anchor=RUN.ANCHOR, env_check=True)[0]))
store = Path(tempfile.mkdtemp(prefix='rb016-store-'))
print('call: F.fetch(root=RUN.HERE (the REAL-standin), store=<tmp>, hooks=<mocks>, now=2028-01-10)')
try:
    row = F.fetch(root=RUN.HERE, store=store, hooks=hooks, now=NOW)
    print('  -> returned a fetched row; manifest_files', row['manifest_files'])
except SystemExit as e:
    print('  -> refused:', str(e)[:200])
head = subprocess.run(['git', 'show', 'HEAD:./results.jsonl'], cwd=A, capture_output=True, text=True).stdout
print('REAL-standin HEAD:results.jsonl 016 modes:', [l.split('"mode": "')[1].split('"')[0] for l in head.splitlines()
                                                     if '016-crypto' in l])
print('REAL-standin git log -3:', subprocess.run(['git', 'log', '-3', '--format=%h %s'], cwd=A, capture_output=True,
                                                  text=True).stdout.strip().replace('\n', ' | '))
f2 = RUN.guard_failures(RUN.HERE, 'fetch', now=NOW, toplevel=RUN.REPO, anchor=RUN.ANCHOR, env_check=True)[0]
print('the REAL fetch on 2028-01-10 (guard() arguments, now passed):', f2)
r2 = RUN.guard_failures(RUN.HERE, 'run', now=NOW, toplevel=RUN.REPO, anchor=RUN.ANCHOR, env_check=True)[0]
print('the REAL read on 2028-01-10:', [x[:90] for x in r2])
