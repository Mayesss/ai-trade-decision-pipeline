"""Throwaway probe: the guard's git calls run in `root` and trust whichever repository git discovers there.
An outer repo whose history holds a 013 row; a nested `git init` in the runner's directory hides it."""
import json, subprocess, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import r013_run as R
d = Path(tempfile.mkdtemp())
g = lambda cwd, *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=cwd, capture_output=True, text=True, check=True)
g(d, 'init', '-q', '-b', 'main')
sub = d / 'research' / 'copytrade'
sub.mkdir(parents=True)
(sub / 'results.jsonl').write_text(json.dumps({'registration': '013-x.md', 'mode': 'discovery'}) + '\n')
g(d, 'add', '.'); g(d, 'commit', '-q', '-m', 'row')
(sub / 'results.jsonl').write_text('')
g(d, 'commit', '-q', '-am', 'row erased (still in history)')
print('outer repo: 013 rows seen from the runner dir:', len(R.result_rows(sub, 'results.jsonl', '013-x.md')))
g(sub, 'init', '-q', '-b', 'main'); g(sub, 'add', '.'); g(sub, 'commit', '-q', '-m', 'fresh')
print('after `git init` in the runner dir: rows seen:', len(R.result_rows(sub, 'results.jsonl', '013-x.md')),
      '| toplevel now:', g(sub, 'rev-parse', '--show-toplevel').stdout.strip() == str(sub.resolve()))
