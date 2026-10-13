"""Review r1 of 013: integrity probes in THROWAWAY git repos (no real data, no real results.jsonl touched).

P1  an attributes line (`results.jsonl -diff`, in .git/info/attributes: untracked, invisible to status) hides
    every historical row from r013's `result_rows` (no --text); r008's version (--text --no-textconv) still sees it.
P2  a textconv driver (config + attributes) rewrites what `git log -p` shows.
P3  shadow modules that r013's (v) does not see but r008's shadow_modules does: a top-level package, a sourceless
    .pyc, a git-excluded .py, an extension module inside ct/.
P4  the commit hook: run_discovery-style sequence with a mock HOOKS['commit'] leaves the started row uncommitted
    while the outcome is computed (B-M3 case 1 reopened through the test hook).
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))
import r013_run as R  # noqa: E402  (import only: reads no data)
import r008_run as R8  # noqa: E402


def git(root, *a):
    env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_')}
    env.update(GIT_AUTHOR_NAME='p', GIT_AUTHOR_EMAIL='p@x', GIT_COMMITTER_NAME='p', GIT_COMMITTER_EMAIL='p@x')
    r = subprocess.run(['git', *a], cwd=root, capture_output=True, text=True, env=env)
    return r


def repo():
    d = Path(tempfile.mkdtemp(prefix='r013r1_'))
    git(d, 'init', '-q', '-b', 'main')
    git(d, 'config', 'commit.gpgsign', 'false')
    (d / 'results.jsonl').write_text('')
    (d / 'README').write_text('x\n')
    git(d, 'add', '.')
    git(d, 'commit', '-qm', 'init')
    return d


ROW = json.dumps({'registration': '013-new-perp-listings.md', 'mode': 'discovery_started'}, sort_keys=True)


def p1():
    d = repo()
    (d / 'results.jsonl').write_text(ROW + '\n')
    git(d, 'commit', '-qam', 'row')
    git(d, 'reset', '-q', '--hard', 'HEAD~1')
    before = len(R.result_rows(d, 'results.jsonl'))
    (d / '.git' / 'info').mkdir(exist_ok=True)
    (d / '.git' / 'info' / 'attributes').write_text('results.jsonl -diff\n')
    after = len(R.result_rows(d, 'results.jsonl'))
    r8 = len(R8.result_rows(d, 'results.jsonl', name='013-new-perp-listings.md'))
    st = git(d, 'status', '--porcelain').stdout.strip()
    print(f'P1 -diff via .git/info/attributes: r013 rows before {before}, after {after}; r008-style scan {r8}; '
          f'git status shows: {st!r}')
    shutil.rmtree(d)


def p2():
    d = repo()
    (d / 'results.jsonl').write_text(ROW + '\n')
    git(d, 'commit', '-qam', 'row')
    git(d, 'reset', '-q', '--hard', 'HEAD~1')
    git(d, 'config', 'diff.hide.textconv', 'sh -c "true"')
    (d / '.git' / 'info').mkdir(exist_ok=True)
    (d / '.git' / 'info' / 'attributes').write_text('results.jsonl diff=hide\n')
    after = len(R.result_rows(d, 'results.jsonl'))
    r8 = len(R8.result_rows(d, 'results.jsonl', name='013-new-perp-listings.md'))
    print(f'P2 textconv driver: r013 rows {after}; r008-style scan {r8}')
    shutil.rmtree(d)


def p3():
    d = repo()
    (d / 'ct').mkdir()
    for f in ('__init__.py', 'listings013.py'):
        shutil.copy(HERE / 'ct' / f, d / 'ct' / f)
    git(d, 'add', '.')
    git(d, 'commit', '-qm', 'code')
    planted = {}
    # (a) a top-level package that `import duckdb` pulls in after sys.path[0] = HERE
    (d / 'email').mkdir()
    (d / 'email' / '__init__.py').write_text('print("SHADOWED: email package from the research directory")\n')
    # (b) a sourceless .pyc at the top level
    src = d / 'ipaddress_src.py'
    src.write_text('print("SHADOWED: sourceless ipaddress.pyc")\n')
    import py_compile
    py_compile.compile(str(src), cfile=str(d / 'ipaddress.pyc'))
    src.unlink()
    # (c) an untracked .py hidden by .git/info/exclude
    (d / 'quopri.py').write_text('print("SHADOWED: quopri.py hidden by info/exclude")\n')
    (d / '.git' / 'info').mkdir(exist_ok=True)
    (d / '.git' / 'info' / 'exclude').write_text('quopri.py\n')
    # (d) an extension-module file next to listings013.py (the loader tries extensions BEFORE sources)
    (d / 'ct' / f'listings013{__import__("importlib.machinery").machinery.EXTENSION_SUFFIXES[0]}').write_bytes(b'not a real extension')
    fl = [x for x in R.environment_failures(d) if 'untracked' in x]
    print('P3 r013 (v) untracked-file failures:', fl or 'NONE')
    print('P3 r008 shadow_modules on the same tree:', R8.shadow_modules(d))
    # does the planted code actually run? import duckdb and ct.listings013 with sys.path[0] = the throwaway root
    code = ('import sys; sys.path.insert(0, sys.argv[1]);\n'
            'try:\n  import duckdb\nexcept Exception as e: print("duckdb import error", type(e).__name__)\n'
            'try:\n  from ct import listings013\n  print("ct.listings013 from", listings013.__file__)\n'
            'except Exception as e: print("ct.listings013 import error:", type(e).__name__, str(e)[:90])\n')
    out = subprocess.run([sys.executable, '-I', '-c', code, str(d)], capture_output=True, text=True)
    print('P3 effect:', out.stdout.strip().replace('\n', ' | '), out.stderr.strip()[-200:])
    shutil.rmtree(d)


def p4():
    """The runner's own sequence with a mocked commit hook: the started row stays uncommitted, the outcome code runs,
    and `git checkout -- results.jsonl` erases every trace (no commit, no reflog entry)."""
    d = repo()
    res = d / 'results.jsonl'
    commits = []
    saved = (dict(R.HOOKS), R.RESULTS)
    try:
        R.RESULTS = res
        R.HOOKS['commit'] = lambda msg: commits.append(msg)      # the e2e test's recipe
        R.append_row({'registration': R.REG.name, 'mode': 'discovery_started'})
        R.HOOKS['commit']('013 discovery_started')
        computed = True                                          # <- _discovery_row would run here
        R.append_row({'registration': R.REG.name, 'mode': 'discovery', 'decision': ['close', None]})
        R.HOOKS['commit']('013 discovery result')
    finally:
        R.HOOKS.clear()
        R.HOOKS.update(saved[0])
        R.RESULTS = saved[1]
    head_has = ROW.split('"mode"')[0] in git(d, 'show', 'HEAD:results.jsonl').stdout
    git(d, 'checkout', '--', 'results.jsonl')
    left = len(R.result_rows(d, 'results.jsonl'))
    print(f'P4 mock hook: computed={computed}, started row committed in HEAD={head_has}, rows visible after '
          f'`git checkout -- results.jsonl` = {left}; nothing in run_discovery verifies HEAD after the hook')
    shutil.rmtree(d)


if __name__ == '__main__':
    p1()
    p2()
    p3()
    p4()
