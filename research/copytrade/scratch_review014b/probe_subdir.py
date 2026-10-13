"""Throwaway repo with the REAL layout: the runner lives in research/copytrade/ of a larger repo. The guard's
`git show HEAD:results.jsonl` and `git show <c>:<rel>` are resolved from the repo ROOT, not from cwd, so in the
real layout (g2) and (h') can never be satisfied. Same steps as the test's holdout path, which passes only
because its throwaway repo has the runner at the top level."""
import json, subprocess, sys, tempfile
from pathlib import Path
HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import r014_run as R
from ct import shortvol as sv

code, data, reg_rel, NAME = ('ct/shortvol.py', 'r014_run.py'), ('data/r014/panel_discovery/x.json',), 'registrations/014-x.md', '014-x.md'

def build(top_level):
    top = Path(tempfile.mkdtemp())
    root = top if top_level else top / 'research' / 'copytrade'
    git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root, capture_output=True, text=True, check=True)
    root.mkdir(parents=True, exist_ok=True)
    subprocess.run(['git', 'init', '-q', '-b', 'main'], cwd=top, check=True)
    for f, body in (('ct/shortvol.py', 'x = 1\n'), ('r014_run.py', 'y = 2\n'), (data[0], '{}\n'), ('results.jsonl', '')):
        (root / f).parent.mkdir(parents=True, exist_ok=True); (root / f).write_text(body)
    (root / '.gitignore').write_text('data/\n')
    rows = '\n'.join(f'| `{f}` | `{R.sha256(root / f)}` |' for f in (*code, *data))
    (root / reg_rel).parent.mkdir(exist_ok=True)
    (root / reg_rel).write_text(f'# 014\n\n**Status: REGISTERED 2026-10-13**\n\n| file | sha256 |\n|---|---|\n{rows}\n')
    git('add', '.'); git('commit', '-q', '-m', 'register')
    rc = git('rev-parse', 'HEAD').stdout.strip()
    print('  discovery gate:', R.guard_failures(root, reg_rel, code, data, mode='discovery')[0] or 'OPEN')
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
    print('  holdout gate after rows committed and approval committed after them:')
    for x in R.guard_failures(root, reg_rel, code, data, mode='holdout')[0] or ['OPEN']:
        print('    ', x[:120])
    print('  `git show HEAD:results.jsonl` lines:', len(git('show', 'HEAD:./results.jsonl').stdout.splitlines()), '(with ./)',
          '|', len(subprocess.run(['git', 'show', 'HEAD:results.jsonl'], cwd=root, capture_output=True, text=True).stdout.splitlines()), '(as written)')

print('runner at the repo top level (the tests\' layout):'); build(True)
print('runner in research/copytrade/ (the real layout):'); build(False)
