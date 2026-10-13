"""Throwaway-repo probe of r013_run.guard_failures (discovery gate (f)) after a row is erased:
(1) uncommitted row discarded with checkout; (2) committed row removed by reset --hard (reflog kept);
(3) committed row removed by commit --amend; (4) row stashed then stash dropped. No real data is touched."""
import json, subprocess, sys, tempfile
from pathlib import Path
HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import r013_run as R
from tests.test_r013 import _row009

code, data, reg_rel = ('ct/listings013.py', 'r013_run.py'), ('data/r013/panel_discovery/events.json',), 'registrations/013-x.md'


def repo():
    d = tempfile.mkdtemp()
    root = Path(d)
    git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root,
                                    capture_output=True, text=True, check=True)
    git('init', '-q', '-b', 'main')
    for f, body in (('ct/listings013.py', 'x = 1\n'), ('r013_run.py', 'y = 2\n'), (data[0], '[]\n'), ('results.jsonl', '')):
        (root / f).parent.mkdir(parents=True, exist_ok=True)
        (root / f).write_text(body)
    (root / '.gitignore').write_text('data/\n')
    rows = '\n'.join(f'| `{f}` | `{R.sha256(root / f)}` |' for f in (*code, *data))
    (root / reg_rel).parent.mkdir(exist_ok=True)
    (root / reg_rel).write_text(f'# 013\n\n**Status: REGISTERED 2026-10-13**\n\n| file | sha256 |\n|---|---|\n{rows}\n')
    git('add', '.')
    git('commit', '-q', '-m', 'register')
    rc = git('rev-parse', 'HEAD').stdout.strip()
    with open(root / 'results.jsonl', 'a') as f:
        f.write(json.dumps(_row009(rc), sort_keys=True) + '\n')
    git('commit', '-q', '-am', '009 closed')
    return root, git


def fails(root):
    return R.guard_failures(root, reg_rel, code, data, mode='discovery')[0]


def add(root):
    with open(root / 'results.jsonl', 'a') as f:
        f.write(json.dumps({'registration': '013-x.md', 'mode': 'discovery_started'}) + '\n')
        f.write(json.dumps({'registration': '013-x.md', 'mode': 'discovery', 'decision': ['close', None]}) + '\n')


root, git = repo()
print('baseline gate:', fails(root))
add(root); print('after a run (rows uncommitted):', [x[:3] for x in fails(root)])
git('checkout', '--', 'results.jsonl'); print('(1) rows discarded with checkout:', fails(root))

root, git = repo()
add(root); git('commit', '-q', '-am', 'rows')
print('rows committed:', [x[:3] for x in fails(root)])
git('reset', '-q', '--hard', 'HEAD~1')
print('(2) reset --hard HEAD~1, reflog intact:', fails(root),
      '| reflog still holds it:', bool(subprocess.run(['git', 'log', '--reflog', '-p', '--format=', '--', 'results.jsonl'],
                                                         cwd=root, capture_output=True, text=True).stdout.count('013-x')))

root, git = repo()
add(root); git('commit', '-q', '-am', 'rows')
(root / 'results.jsonl').write_text('\n'.join(l for l in (root / 'results.jsonl').read_text().splitlines() if '013-x' not in l) + '\n')
git('commit', '-q', '--amend', '--allow-empty', '-am', 'rows (amended away)')
print('(3) commit --amend removing the rows:', fails(root))

root, git = repo()
add(root); git('stash', '-q'); print('(4a) rows stashed:', [x[:3] for x in fails(root)])
git('stash', 'drop', '-q'); print('(4b) stash dropped:', fails(root))

print('--- the same cases with the history scan widened to `log --all --reflog -m -p` (proposed fix)')
orig = R._git
def widened(root, *a):
    if a[:3] == ('log', '--all', '-p'):
        a = ('log', '--all', '--reflog', '-m', '-p') + a[3:]
    return orig(root, *a)
R._git = widened
root, git = repo(); add(root); git('checkout', '--', 'results.jsonl'); print('(1) discarded uncommitted:', fails(root), '(still open: needs an auto-commit)')
root, git = repo(); add(root); git('commit', '-q', '-am', 'rows'); git('reset', '-q', '--hard', 'HEAD~1'); print('(2) reset --hard:', [x[:3] for x in fails(root)])
root, git = repo(); add(root); git('commit', '-q', '-am', 'rows')
(root / 'results.jsonl').write_text('\n'.join(l for l in (root / 'results.jsonl').read_text().splitlines() if '013-x' not in l) + '\n')
git('commit', '-q', '--amend', '--allow-empty', '-am', 'x'); print('(3) amend:', [x[:3] for x in fails(root)])
root, git = repo(); add(root); git('stash', '-q'); print('(4a) stashed:', [x[:3] for x in fails(root)])
R._git = orig
