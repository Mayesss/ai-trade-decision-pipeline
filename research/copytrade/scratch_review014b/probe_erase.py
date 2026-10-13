"""Throwaway repos only (no real data, no real repo). r014_run.guard_failures after 014 rows are erased:
discovery gate (f) and holdout gate (i). Cases: (1) uncommitted rows discarded with checkout; (2) committed
rows removed by reset --hard (reflog intact); (3) commit --amend; (4) stash (stash commit is a merge);
(5) nested `git init` in the runner dir; (6) GIT_DIR pointing at another repo."""
import json, os, subprocess, sys, tempfile
from pathlib import Path
HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import r014_run as R
from ct import shortvol as sv

code, data, reg_rel, NAME = ('ct/shortvol.py', 'r014_run.py'), ('data/r014/panel_discovery/x.json',), 'registrations/014-x.md', '014-x.md'


def gitf(root):
    return lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root,
                                     capture_output=True, text=True, check=True)


def repo(extra=''):
    root = Path(tempfile.mkdtemp()); git = gitf(root)
    git('init', '-q', '-b', 'main')
    for f, body in (('ct/shortvol.py', 'x = 1\n'), ('r014_run.py', 'y = 2\n'), (data[0], '{}\n'), ('results.jsonl', '')):
        (root / f).parent.mkdir(parents=True, exist_ok=True); (root / f).write_text(body)
    (root / '.gitignore').write_text('data/\n')
    rows = '\n'.join(f'| `{f}` | `{R.sha256(root / f)}` |' for f in (*code, *data))
    (root / reg_rel).parent.mkdir(exist_ok=True)
    (root / reg_rel).write_text(f'# 014\n\n**Status: REGISTERED 2026-10-13**\n\n| file | sha256 |\n|---|---|\n{rows}\n{extra}')
    git('add', '.'); git('commit', '-q', '-m', 'register')
    return root, git


def fails(root, mode='discovery'):
    return R.guard_failures(root, reg_rel, code, data, mode=mode)[0]


def add_disc(root):
    with open(root / 'results.jsonl', 'a') as f:
        f.write(json.dumps({'registration': NAME, 'mode': 'discovery_started'}) + '\n')
        f.write(json.dumps({'registration': NAME, 'mode': 'discovery', 'decision': ['close', []]}) + '\n')


def tag(fs):
    return sorted({x[:4] for x in fs}) or 'OPEN'


print('== discovery gate (f)')
root, git = repo(); print('baseline:', tag(fails(root)))
add_disc(root); print('rows uncommitted:', tag(fails(root)))
git('checkout', '--', 'results.jsonl'); print('(1) checkout:', tag(fails(root)))
root, git = repo(); add_disc(root); git('commit', '-q', '-am', 'rows'); print('rows committed:', tag(fails(root)))
git('reset', '-q', '--hard', 'HEAD~1'); print('(2) reset --hard HEAD~1 (reflog intact):', tag(fails(root)))
root, git = repo(); add_disc(root); git('commit', '-q', '-am', 'rows')
(root / 'results.jsonl').write_text(''); git('commit', '-q', '--amend', '--allow-empty', '-am', 'x'); print('(3) amend:', tag(fails(root)))
root, git = repo(); add_disc(root); git('stash', '-q'); print('(4) stashed (refs/stash present):', tag(fails(root)))

print('== holdout gate (i): a valid discovery row + approval, then the holdout_started row erased')
def holdout_ready():
    root, git = repo()
    rc = git('rev-parse', 'HEAD').stdout.strip()
    good = [0.012 + 0.02 * ((i * 7919) % 101 - 50) / 50 for i in range(143)]
    res, dec = sv.evaluate_discovery({'T1': good, 'T2': good})
    row = json.loads(json.dumps({'registration': NAME, 'mode': 'discovery', 'registration_commit': rc,
                                 'code_hashes': {f: R.sha256(root / f) for f in code}, 'data_hashes': {f: R.sha256(root / f) for f in data},
                                 'results': res, 'weekly': {k: [[i, x] for i, x in enumerate(good)] for k in sv.FAMILY},
                                 'decision': [dec[0], list(dec[1])]}, default=str))
    with open(root / 'results.jsonl', 'a') as f:
        f.write(json.dumps({'registration': NAME, 'mode': 'discovery_started', 'registration_commit': rc}, sort_keys=True) + '\n')
        f.write(json.dumps(row, sort_keys=True) + '\n')
    git('commit', '-q', '-am', 'rows')
    line = (root / 'results.jsonl').read_text().splitlines()[-1]
    with open(root / reg_rel, 'a') as f:
        f.write(f'\nHoldout: APPROVED 2026-10-20 row {R.line_sha(line)}\n')
    git('commit', '-q', '-am', 'approval')
    return root, git, dec
root, git, dec = holdout_ready(); print('decision of the synthetic row:', dec[0], '| gate:', tag(fails(root, 'holdout')))
with open(root / 'results.jsonl', 'a') as f:
    f.write(json.dumps({'registration': NAME, 'mode': 'holdout_started'}) + '\n')
    f.write(json.dumps({'registration': NAME, 'mode': 'holdout', 'decision': 'not_demonstrated'}) + '\n')
print('holdout rows uncommitted:', tag(fails(root, 'holdout')))
git('checkout', '--', 'results.jsonl'); print('(1) checkout -> holdout readable AGAIN:', tag(fails(root, 'holdout')))
root, git, _ = holdout_ready()
with open(root / 'results.jsonl', 'a') as f:
    f.write(json.dumps({'registration': NAME, 'mode': 'holdout_started'}) + '\n')
git('commit', '-q', '-am', 'holdout started'); git('reset', '-q', '--hard', 'HEAD~1'); print('(2) reset --hard -> holdout readable AGAIN:', tag(fails(root, 'holdout')))

print('== repository identity')
outer = Path(tempfile.mkdtemp()); g = gitf(outer); g('init', '-q', '-b', 'main')
sub = outer / 'research' / 'copytrade'; sub.mkdir(parents=True)
(sub / 'results.jsonl').write_text(json.dumps({'registration': NAME, 'mode': 'discovery'}) + '\n')
g('add', '.'); g('commit', '-q', '-m', 'row')
(sub / 'results.jsonl').write_text(''); g('commit', '-q', '-am', 'row erased (still in outer history)')
print('outer repo rows seen from runner dir:', len(R.result_rows(sub, 'results.jsonl', NAME)))
gs = gitf(sub); gs('init', '-q', '-b', 'main'); gs('add', '.'); gs('commit', '-q', '-m', 'fresh')
print('(5) after nested `git init` in runner dir, rows seen:', len(R.result_rows(sub, 'results.jsonl', NAME)))
root, git = repo(); add_disc(root); git('commit', '-q', '-am', 'rows')
decoy, _ = repo()
os.environ['GIT_DIR'] = str(decoy / '.git'); os.environ['GIT_WORK_TREE'] = str(root)
r = tag(fails(root)); del os.environ['GIT_DIR'], os.environ['GIT_WORK_TREE']
(root / 'results.jsonl').write_text('')  # working-tree rows gone too (e.g. checkout), history only in the real repo
os.environ['GIT_DIR'] = str(decoy / '.git'); os.environ['GIT_WORK_TREE'] = str(root)
r2 = tag(fails(root)); del os.environ['GIT_DIR'], os.environ['GIT_WORK_TREE']
print('(6) GIT_DIR -> a decoy repo: with working-tree rows', r, '| after clearing the working tree:', r2, '| real repo still holds them:', tag(fails(root)))

print('== proposed fix: `log --all --reflog -m -p`')
orig = R._git
def widened(root, *a):
    if a[:3] == ('log', '--all', '-p'):
        a = ('log', '--all', '--reflog', '-m', '-p') + a[3:]
    return orig(root, *a)
R._git = widened
root, git = repo(); add_disc(root); git('commit', '-q', '-am', 'rows'); git('reset', '-q', '--hard', 'HEAD~1'); print('(2)', tag(fails(root)))
root, git = repo(); add_disc(root); git('commit', '-q', '-am', 'rows'); (root / 'results.jsonl').write_text(''); git('commit', '-q', '--amend', '--allow-empty', '-am', 'x'); print('(3)', tag(fails(root)))
root, git = repo(); add_disc(root); git('stash', '-q'); print('(4)', tag(fails(root)))
root, git = repo(); add_disc(root); git('checkout', '--', 'results.jsonl'); print('(1)', tag(fails(root)), '(needs the runner to commit the started row itself)')
R._git = orig
