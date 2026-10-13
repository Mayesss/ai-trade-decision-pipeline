"""Registration 015 — the month-end liquidity cycle, settlement-shifted: the guarded run (once). Revision 1.

    python3 -I -S research/alpha/r015_run.py --check      # every reason the run may not start; computes nothing
    python3 -I -S research/alpha/r015_run.py --fingerprint # the interpreter / stdlib fingerprint (pin recovery); no data
    python3 -I -S research/alpha/r015_run.py --calendar    # DATES ONLY: rebuild data/r015/calendar.json (pre-registration)
    python3 -I -S research/alpha/r015_run.py --run         # the read, once: started, result, reported rows (each committed)

The run refuses unless (every check is a refusal, never an assert):
  (s)  the registration AS COMMITTED IN HEAD says REGISTERED (status and hash table are read from HEAD:./, review B M3);
  (a)  it is tracked;  (b) its working-tree bytes (`git hash-object --no-filters`) equal its HEAD blob;  (c) it has a commit;
  (d)  every guarded code file's working-tree bytes equal its HEAD blob; no guarded path or results.jsonl carries an
       assume-unchanged / skip-worktree flag or a filter attribute (review B M3);
  (e)  every guarded code and data file is listed exactly once in the HEAD table and matches it;
  (k)  results.jsonl is tracked, equals its HEAD blob, and is empty or ends with a newline (review B m3);
  (w)  the repository is THE real one: this file's repository equals the pinned absolute path REAL_REPO, git's top level
       is that path, the working tree is the main worktree (--git-dir == --git-common-dir == REAL_REPO/.git), HEAD is
       a named branch (not detached), and the anchor commit is an ancestor of HEAD (review B B1);
  (v)  python -I -S (isolated, no site: a site-packages .pth cannot run) and no -O; no .pth file in the interpreter's
       site-packages (pinned empty list); the interpreter version, the interpreter binary's SHA-256 and the stdlib tree
       fingerprint equal the pins; every loaded module comes from the pinned stdlib or is r015_core / r015_run in this
       directory, and every cached bytecode from the fresh sys.pycache_prefix (review B M2); no untracked, ignored or
       modified importable file (*.py, sourceless *.pyc, *.so / *.pyd, packages) at research/alpha's top level,
       judged by blob equality, not by `git status`, and NO module at that level, tracked or not, whose name is a
       stdlib module name (research/alpha is appended to sys.path, after the stdlib) — **every top-level .py and package, 016's included, must be
       committed before 015 runs** (review B m7);
  (r)  across the WHOLE history (`--all --reflog`, no first-parent restriction) exactly one REGISTERED content of the
       registration exists; reg0 = the earliest commit carrying it (stored in every row); HEAD's registration and
       every guarded code blob equal reg0's (016 review B M4; Revision 2, R1-M1: a --no-ff merge cannot re-register);
  (f)  no row of any registration whose name starts '015-' exists anywhere (working tree; `git log --all --reflog -m
       --root -p --text --no-textconv --no-ext-diff` with log.showRoot=true and no replace objects), and no unparseable
       added line mentions 015 — except the narrow restart below.
Restart (review B m6; Revision 2, R1-M2): one restart is allowed only when the sole 015 row anywhere is a `started`
row committed in HEAD and NO commit anywhere (`--all --reflog`) that adds a 015 line was authored or committed by the
runner. The data gate token is issued only after the runner's own started commit is verified in HEAD of the real
ledger, so "no runner commit" is the verifiable fact that no data was opened.
git is called by absolute path with GIT_* stripped, no system or global config, no replace objects, no hooks.
Every `<rev>:<path>` is `<rev>:./<path>`. The loaders hash the bytes they parse, refuse a hash differing from the
HEAD table, drop rows dated outside [2013-11-01, 2026-06-04) on their date before parsing values, and open the real
tree (or any byte copy of a registered real file) only after the real guard passed AND the started row is committed
in HEAD of the real ledger, in this process. run() takes no
argument: guard, data, table and ledger are all the real tree's (review B M1). The runner never pushes.
"""
import datetime as dt
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import sysconfig
import tempfile
import time
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.pycache_prefix = tempfile.mkdtemp(prefix='r015-pyc-')    # never load bytecode from an existing __pycache__
sys.path.append(str(HERE))                                    # after the stdlib (016 review B M2)
import r015_core as C  # noqa: E402

Refused = C.Refused
_HERE0 = HERE                                                 # captured at import; production uses only these
REPO = HERE.parent.parent
REAL_REPO = Path('/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline')   # (w), review B B1
REG_NAME = '015-month-end-liquidity-cycle.md'
REG_REL = f'registrations/{REG_NAME}'
REG = HERE / REG_REL
REG_PREFIX = '015-'
RESULTS = HERE / 'results.jsonl'
DATA = HERE / 'data' / 'r015'
DATA_REAL = (HERE / 'data' / 'r015').resolve()
REAL_RESULTS = (HERE / 'results.jsonl').resolve()
ANCHOR = '15c6421e31b345f209cfbee7f73ef47b88ffc3cd'          # "B1 scout ...; decision to draft 015" (2026-10-13)
GIT_BIN = '/usr/bin/git'
RUNNER_NAME, RUNNER_EMAIL = 'r015_run', 'r015_run@localhost'
PY_PIN = '3.14.8'
PY_EXE_SHA256 = '7ebde552773a4045532637b114d645a313e045ac07b26f5e520bf039f1e34063'
STDLIB_SHA256 = 'c99bbd7540f6c89e5a15b371f3fe3a853da11f6556ed2bbe55609286c650071e'
PTH_PINNED = ()                                               # .pth files allowed in site-packages: none
STDLIB_SKIP = {'site-packages', '__pycache__', 'test', 'tests', 'idlelib', 'tkinter', 'turtledemo', 'ensurepip'}

FRENCH_DIR = 'data/r015/french_20261013T093725Z'
US_ZIP = f'{FRENCH_DIR}/F-F_Research_Data_Factors_daily_CSV.zip'
CALENDAR = 'data/r015/calendar.json'
POWER_LOG = 'data/r015/logs/power.json'
GUARDED_CODE = ('r015_core.py', 'r015_power.py', 'r015_run.py')
GUARDED_DATA = (US_ZIP, f'{FRENCH_DIR}/SHA256SUMS', CALENDAR, POWER_LOG)
ALLOWED_PROJECT_MODULES = ('r015_core.py', 'r015_run.py')
HASH_ROW = re.compile(r'^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|', re.M)
STATUS = re.compile(r'^\*\*Status: (\w+)', re.M)
_GATE = None                                                  # (dir, commit): issued by _run() after the started row
_GUARD_PASSED = None                                          # (dir, commit): set only by the real guard()
_STATE = {'data_opened': False}


# ------------------------------------------------------------------ git and hashing helpers
def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def line_sha(line):
    return hashlib.sha256(line.encode()).hexdigest()


def git_env():
    env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_')}
    env.update({'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_NO_REPLACE_OBJECTS': '1',
                'GIT_TERMINAL_PROMPT': '0'})
    return env


def _git(root, *a):
    return subprocess.run([GIT_BIN, '-c', 'core.hooksPath=' + os.devnull, '-c', 'log.showRoot=true', '-c',
                           'core.fsmonitor=false', '--no-replace-objects', *a],
                          cwd=root, capture_output=True, text=True, env=git_env())


def revpath(rev, rel):
    """`<rev>:./<rel>`: relative to cwd. A bare `<rev>:<rel>` resolves from the repository root (014 review B B1)."""
    return f'{rev}:./{rel}'


def head_text(root, rel):
    r = _git(root, 'show', '--no-textconv', revpath('HEAD', rel))
    return r.stdout if r.returncode == 0 else None


def head_blob(root, rel):
    r = _git(root, 'rev-parse', '--verify', '-q', revpath('HEAD', rel))
    return r.stdout.strip() if r.returncode == 0 else None


def work_blob(root, rel):
    r = _git(root, 'hash-object', '--no-filters', '--', rel)
    return r.stdout.strip() if r.returncode == 0 else None


def same_as_head(root, rel):
    hb = head_blob(root, rel)
    return hb is not None and (Path(root) / rel).is_file() and work_blob(root, rel) == hb


def is_015(row):
    return isinstance(row, dict) and str(row.get('registration', '')).startswith(REG_PREFIX)


def _added_lines(root, rel):
    r = _git(root, 'log', '--all', '--reflog', '-m', '--root', '-p', '--text', '--no-textconv', '--no-ext-diff',
             '--no-color', '--format=', '--', rel)
    return r.returncode, [l[1:] for l in r.stdout.splitlines() if l.startswith('+') and not l.startswith('+++')]


def result_rows(root, results_rel='results.jsonl', match=is_015):
    """([(line, row)], [problems]). Every distinct matching line in the working tree and every ADDED line in the
    history of every branch, the reflog, merges, stashes and root commits. A line that mentions a 015 registration but
    does not parse, or a failing git call, is a problem (fail closed)."""
    texts, problems = [], []
    res = Path(root) / results_rel
    if res.exists():
        texts.extend(res.read_text().splitlines())
    rc, lines = _added_lines(root, results_rel)
    if rc != 0:
        problems.append(f'git log over {results_rel} failed')
    texts.extend(lines)
    other = Path(root).parent / 'copytrade' / 'results.jsonl'          # the ledger spans both roots (review B m11)
    if other.exists():
        texts.extend(_added_lines(root, '../copytrade/results.jsonl')[1])
        texts.extend(other.read_text().splitlines())
    out, seen = [], set()
    for line in texts:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            if f'"registration": "{REG_PREFIX}' in line or f'"registration":"{REG_PREFIX}' in line:
                problems.append('an unparseable line mentions a 015 registration')
            continue
        if match(row) and line not in seen:
            seen.add(line)
            out.append((line, row))
    return out, problems


def head_lines(root, results_rel='results.jsonl'):
    return _git(root, 'show', '--no-textconv', revpath('HEAD', results_rel)).stdout.splitlines()


def first_commit_containing(root, rel, needle):
    for c in _git(root, 'log', '--format=%H', '--reverse', '--', rel).stdout.split():
        if needle in _git(root, 'show', '--no-textconv', revpath(c, rel)).stdout.splitlines():
            return c
    return None


def is_ancestor(root, c):
    return bool(c) and _git(root, 'merge-base', '--is-ancestor', c, 'HEAD').returncode == 0


# ------------------------------------------------------------------ (v) environment
def stdlib_root():
    return Path(os.path.realpath(sysconfig.get_paths()['stdlib']))


def stdlib_fingerprint():
    root = stdlib_root()
    h, n = hashlib.sha256(), 0
    for d, dirs, files in os.walk(root):
        dirs[:] = sorted(x for x in dirs if x not in STDLIB_SKIP)
        for f in sorted(files):
            if f.endswith(('.py', '.so')):
                p = os.path.join(d, f)
                with open(p, 'rb') as fh:
                    b = fh.read()
                h.update(os.path.relpath(p, root).encode())
                h.update(hashlib.sha256(b).digest())
                n += 1
    return h.hexdigest(), n


def env_fingerprint():
    exe = os.path.realpath(sys.executable)
    fp, n = stdlib_fingerprint()
    return {'python': sys.version.split()[0], 'executable': exe, 'executable_sha256': sha256(exe),
            'stdlib_sha256': fp, 'stdlib_files': n, 'isolated': sys.flags.isolated, 'no_site': sys.flags.no_site,
            'optimize': sys.flags.optimize, 'pth_files': pth_files(),
            'git': GIT_BIN, 'git_version': _git(HERE, '--version').stdout.strip()}


def shadow_modules(root):
    """Anything importable at research/alpha's top level (sys.path[0]) whose bytes are not its HEAD blob: *.py,
    sourceless *.pyc, *.so / *.pyd, and packages (a directory with __init__.py). Scanned on the FILE SYSTEM, so
    git-ignored files count; judged by `hash-object --no-filters` against HEAD (review B M3), not by `git status`."""
    root = Path(root)
    bad = set()
    std = set(sys.stdlib_module_names)
    for p in sorted(root.iterdir()):
        if p.name == '__pycache__':
            continue
        if p.is_file() and p.suffix in ('.py', '.pyc', '.so', '.pyd'):
            if p.name.split('.')[0] in std:
                bad.add(p.name + ' (stdlib name)')
            elif not same_as_head(root, p.name):
                bad.add(p.name)
        elif p.is_dir() and (p / '__init__.py').exists():
            if p.name in std:
                bad.add(f'{p.name}/__init__.py (stdlib name)')
            elif not same_as_head(root, f'{p.name}/__init__.py'):
                bad.add(f'{p.name}/__init__.py')
    return sorted(bad)


def pth_files():
    """Every .pth in the interpreter's site-packages directories (they execute at startup unless -S)."""
    out = []
    for k in ('purelib', 'platlib'):
        d = Path(sysconfig.get_paths()[k])
        if d.is_dir():
            out += sorted(str(f) for f in d.iterdir() if f.suffix == '.pth')
    return sorted(set(out))


def module_origin_failures(here=None):
    """Every loaded module with a file is either in the pinned stdlib tree or one of the allowed r015 modules in this
    directory; every cached bytecode is under the fresh sys.pycache_prefix (review B M2, m1)."""
    here = Path(here or HERE).resolve()
    std = stdlib_root()
    allowed = {(here / f).resolve() for f in ALLOWED_PROJECT_MODULES}
    pyc = Path(sys.pycache_prefix).resolve() if sys.pycache_prefix else None
    out = []
    for name, mod in sorted(sys.modules.items()):
        f = getattr(mod, '__file__', None)
        if f:
            p = Path(f).resolve()
            if not (p.is_relative_to(std) or p in allowed):
                out.append(f'(v) module {name} is loaded from {p}, outside the pinned stdlib and the allowed r015 files')
        cached = getattr(mod, '__cached__', None)
        if f and Path(f).resolve() in allowed and name != '__main__' and not cached:
            out.append(f'(v) module {name} has no __cached__ record (tampered import state)')
        if cached and f and not Path(f).resolve().is_relative_to(std):
            if pyc is None or not Path(cached).resolve().is_relative_to(pyc):
                out.append(f'(v) module {name} bytecode {cached} is not under the fresh pycache_prefix')
    return out


def environment_failures(root):
    out = []
    if not sys.flags.isolated:
        out.append('(v) run with python -I (isolated mode) — PYTHONPATH, user site and the script directory must not load')
    if not sys.flags.no_site:
        out.append('(v) run with python -I -S (no site) — a site-packages .pth would execute at startup')
    if 'site' in sys.modules or type(sys.flags).__name__ != 'flags' or not isinstance(sys.flags, tuple):
        out.append('(v) the site module ran or sys.flags is not the interpreter\'s own struct (Revision 2, r1-m2)')
    opts = [a for a in sys.orig_argv[1:] if a.startswith('-') and not a.startswith('--')]
    if not (any('I' in a for a in opts) and any('S' in a for a in opts)):
        out.append('(v) the command line does not carry -I and -S (sys.orig_argv)')
    extra_pth = [f for f in pth_files() if Path(f).name not in PTH_PINNED]
    if extra_pth:
        out.append(f'(v) .pth files in site-packages that are not pinned: {extra_pth[:4]}')
    if sys.flags.optimize:
        out.append('(v) python -O / -OO is refused')
    if sys.version.split()[0] != PY_PIN:
        out.append(f'(v) python {sys.version.split()[0]} differs from the pin {PY_PIN}')
    exe = os.path.realpath(sys.executable)
    if not Path(exe).is_file() or sha256(exe) != PY_EXE_SHA256:
        out.append(f'(v) the interpreter binary {exe} does not match its pinned SHA-256 (recovery: reinstall python '
                   f'{PY_PIN} until `--fingerprint` equals the pins; the pins are never edited after REGISTERED)')
    if stdlib_fingerprint()[0] != STDLIB_SHA256:
        out.append('(v) the stdlib tree fingerprint differs from the pin')
    shadow = shadow_modules(root)
    if shadow:
        out.append(f'(v) untracked, ignored or modified importable files at research/alpha top level could shadow an '
                   f'import: {shadow[:6]} — commit every top-level .py and package (016 included) before 015 runs')
    out += module_origin_failures()
    if not sys.pycache_prefix or Path(sys.pycache_prefix).resolve().is_relative_to(REAL_REPO):
        out.append('(v) sys.pycache_prefix must be a fresh directory outside the repository')
    return out


# ------------------------------------------------------------------ the guard
def flag_failures(root, files):
    """assume-unchanged (lowercase tag) / skip-worktree (S) flags and filter attributes on guarded paths."""
    out = []
    for line in _git(root, 'ls-files', '-v', '--', *files).stdout.splitlines():
        tag, name = line[:1], line[2:]
        if tag == 'S' or tag.islower():
            out.append(f'(d) {name} carries an index flag ({tag}) that hides working-tree changes')
    for line in _git(root, 'check-attr', 'filter', '--', *files).stdout.splitlines():
        if not line.rstrip().endswith(': unspecified'):
            out.append(f'(d) a filter attribute applies: {line.strip()}')
    return out


def registered_versions(root, reg_rel):
    """Every commit anywhere (`--all --reflog`, no first-parent restriction) whose registration says REGISTERED, as
    [(commit time, commit, registration blob)] sorted by time (Revision 2, R1-M1)."""
    out = []
    r = _git(root, 'log', '--all', '--reflog', '--format=%H %ct', '--', reg_rel)
    for line in sorted(set(r.stdout.split('\n'))):
        if not line.strip():
            continue
        c, t = line.split()
        m = STATUS.search(_git(root, 'show', '--no-textconv', revpath(c, reg_rel)).stdout or '')
        if m and m.group(1) == 'REGISTERED':
            out.append((int(t), c, _git(root, 'rev-parse', revpath(c, reg_rel)).stdout.strip()))
    return sorted(out)


def reg0_failures(root, reg_rel, code):
    """(reg0 or None, [failures]): exactly one REGISTERED content of the registration anywhere in history; reg0 is the
    earliest commit carrying it; HEAD's registration and every guarded code blob equal reg0's."""
    vers = registered_versions(root, reg_rel)
    if not vers:
        return None, []
    blobs = sorted({b for _, _, b in vers})
    reg0 = vers[0][1]
    out = []
    if len(blobs) != 1:
        out.append(f'(r) {len(blobs)} different REGISTERED versions of the registration exist in the history '
                   f'(first: {reg0[:12]}) — an amendment after REGISTERED is a new registration')
    if head_blob(root, reg_rel) != vers[0][2]:
        out.append(f'(r) HEAD\'s registration differs from the REGISTERED commit {reg0[:12]}')
    for f in code:
        a = _git(root, 'rev-parse', '--verify', '-q', revpath(reg0, f)).stdout.strip()
        if not a or a != head_blob(root, f):
            out.append(f'(r) {f} at HEAD differs from the REGISTERED commit {reg0[:12]}')
    return reg0, out


def runner_added_lines(root, results_rel):
    """Lines added to results.jsonl by any commit anywhere (`--all --reflog -m --root`) whose author or committer is
    the runner identity."""
    r = _git(root, 'log', '--all', '--reflog', '-m', '--root', '-p', '--text', '--no-textconv', '--no-ext-diff',
             '--no-color', '--format=@@COMMIT %an%x09%cn', '--', results_rel)
    out, runner = set(), False
    for l in r.stdout.splitlines():
        if l.startswith('@@COMMIT '):
            an, _, cn = l[9:].partition('\t')
            runner = RUNNER_NAME in (an, cn)
        elif runner and l.startswith('+') and not l.startswith('+++'):
            out.add(l[1:])
    return out


def restart_allowed(root, rows, results_rel):
    """One restart (Revision 2, R1-M2): the only 015 row anywhere is a `started` row committed in HEAD, and NO commit
    anywhere in the history or reflog that adds a 015 line was authored or committed by the runner. The runner opens
    data only after its own started commit is verified in HEAD (the gate is issued then), so the absence of any
    runner commit is the verifiable fact that no data was opened."""
    if len(rows) != 1:
        return False
    line, row = rows[0]
    if row.get('mode') != 'started' or line not in head_lines(root, results_rel):
        return False
    added = runner_added_lines(root, results_rel)
    return not any(is_015(_safe_json(l)) for l in added)


def _safe_json(line):
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def guard_failures(root, reg_rel, code=GUARDED_CODE, data=GUARDED_DATA, results_rel='results.jsonl', real_repo=None,
                   anchor=None, env_check=False):
    """([reasons], registration commit, table). The run may start only when the list is empty."""
    root = Path(root)
    out = []
    text = head_text(root, reg_rel) or ''
    m = STATUS.search(text)
    if not m or m.group(1) != 'REGISTERED':
        out.append(f'(s) {Path(reg_rel).name} is not REGISTERED in HEAD — refusing to compute any outcome.')
    if _git(root, 'ls-files', '--error-unmatch', '--', reg_rel).returncode:
        out.append(f'(a) {Path(reg_rel).name} is not tracked by git')
    if not same_as_head(root, reg_rel):
        out.append(f'(b) {Path(reg_rel).name}: the working-tree bytes differ from the committed version')
    commit = _git(root, 'log', '-1', '--format=%H', '--', reg_rel).stdout.strip()
    if not commit:
        out.append(f'(c) {Path(reg_rel).name} has no commit')
    reg0, rf = reg0_failures(root, reg_rel, code)
    out += rf
    commit = reg0 or commit
    for f in code:
        if not same_as_head(root, f):
            out.append(f'(d) guarded code {f}: the working-tree bytes differ from HEAD (or it is not committed)')
    out += flag_failures(root, [reg_rel, *code, results_rel])
    rows_tbl = HASH_ROW.findall(text)
    table = dict(rows_tbl)
    dup = sorted({f for f, _ in rows_tbl if sum(1 for g, _ in rows_tbl if g == f) > 1})
    if dup:
        out.append(f'(e) files listed more than once in the hash table: {dup}')
    for f in (*code, *data):
        want = table.get(f)
        if want is None:
            out.append(f'(e) {f} missing from the registration hash table')
        elif not (root / f).is_file() or sha256(root / f) != want:
            out.append(f'(e) {f} does not match its registered SHA-256')
    res = root / results_rel
    if not same_as_head(root, results_rel):
        out.append(f'(k) {results_rel} is untracked or differs from HEAD — every row is committed when written')
    elif res.stat().st_size and not res.read_bytes().endswith(b'\n'):
        out.append(f'(k) {results_rel} does not end with a newline — a row appended now would merge into another')
    if real_repo is not None:
        real_repo = Path(real_repo).resolve()
        top = _git(root, 'rev-parse', '--show-toplevel').stdout.strip()
        if not top or Path(top).resolve() != real_repo:
            out.append(f'(w) git top level is {top!r}, not the real repository {real_repo}')
        gd = _git(root, 'rev-parse', '--git-dir').stdout.strip()
        gc = _git(root, 'rev-parse', '--git-common-dir').stdout.strip()
        if not gd or (root / gd).resolve() != (root / gc).resolve() or (root / gd).resolve() != real_repo / '.git':
            out.append('(w) not the main worktree of the real repository (a linked worktree or another .git)')
        if _git(root, 'symbolic-ref', '-q', 'HEAD').returncode:
            out.append('(w) HEAD is detached — run on a named branch of the real repository')
    if anchor is not None and not is_ancestor(root, anchor):
        out.append(f'(w) the anchor commit {anchor} is not an ancestor of HEAD')
    if env_check:
        out += environment_failures(root)
    rows, problems = result_rows(root, results_rel)
    out += [f'(f) {p}' for p in problems]
    if rows and not restart_allowed(root, rows, results_rel):
        out.append('(f) a 015 row exists (working tree, any branch, reflog, merge, stash or root commit) — 015 runs once')
    return out, commit, table


_GUARD_FAILURES = guard_failures                               # captured at import (review B m2)


def guard():
    """The real guard: this file's own repository, which must be the pinned real one; constants captured at import;
    every check. It does NOT open the data: the gate token is issued by _run() only after the started row is committed
    and verified in HEAD:./results.jsonl of the real ledger (Revision 2, r1-m1)."""
    global _GATE, _GUARD_PASSED
    _GATE = None
    _GUARD_PASSED = None
    if REPO.resolve() != REAL_REPO or HERE != _HERE0:
        raise Refused(f'(w) this runner lives in {REPO}, not in the real repository {REAL_REPO}')
    failures, commit, _table = _GUARD_FAILURES(_HERE0, REG_REL, GUARDED_CODE, GUARDED_DATA, 'results.jsonl',
                                               real_repo=REAL_REPO, anchor=ANCHOR, env_check=True)
    if failures:
        raise Refused('\n'.join(failures))
    _GUARD_PASSED = (str(_HERE0.resolve()), commit)
    return commit


_REAL_GUARD = guard
_REG_HASHES = {}


def registered_real_hashes():
    """SHA-256s of the real registered data files, from the REAL repository's HEAD registration (cached)."""
    if 'v' not in _REG_HASHES:
        t = head_text(REAL_REPO / 'research' / 'alpha', REG_REL) if (REAL_REPO / 'research' / 'alpha').is_dir() else None
        tbl = dict(HASH_ROW.findall(t or ''))
        _REG_HASHES['v'] = {h for f, h in tbl.items() if f.startswith('data/')}
    return _REG_HASHES['v']


def _guard_ok():
    return (_GUARD_PASSED is not None and _GUARD_PASSED[0] == str(_HERE0.resolve()) and guard is _REAL_GUARD
            and _GUARD_FAILURES is guard_failures)


def _gate_ok():
    return (_guard_ok() and _GATE is not None and _GATE == _GUARD_PASSED
            and Path(RESULTS).resolve() == REAL_RESULTS and HOOKS.get('commit') is commit_results)


def _gate(path, digest=None):
    """Refuse any read under the real data tree, or of any byte copy of a registered real data file, unless the real
    guard passed in this process."""
    p = Path(path).resolve()
    real = (p == DATA_REAL or p.is_relative_to(DATA_REAL) or p.is_relative_to(REAL_REPO)
            or (digest is not None and digest in registered_real_hashes()))
    if real and not _gate_ok():
        raise Refused('the real 015 data opens only after the real guard() passed in this process')


# ------------------------------------------------------------------ loaders (hash the bytes that are parsed)
def _csv_lines_from(b, label):
    with zipfile.ZipFile(io.BytesIO(b)) as z:
        names = [n for n in z.namelist() if n.lower().endswith('.csv')]
        if len(names) != 1:
            raise Refused(f'{label}: expected one csv, found {names}')
        return z.read(names[0]).decode('latin-1').splitlines()


def _date8(s):
    return dt.date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def read_dates_only(zip_path, want_sha):
    """Trading dates in [DATA_START, DATA_END) — only the first field of each line is read; no value is parsed.
    Ungated by design (the calendar is a pre-registration fact); verified against the pinned hash."""
    b = Path(zip_path).read_bytes()
    if hashlib.sha256(b).hexdigest() != want_sha:
        raise Refused(f'{zip_path} does not match its pinned SHA-256')
    out = []
    for line in _csv_lines_from(b, zip_path):
        head = line.split(',', 1)[0].strip()
        if len(head) == 8 and head.isdigit():
            d = _date8(head)
            if C.DATA_START <= d < C.DATA_END:
                out.append(d)
    return out


def load_french(zip_path, want_sha, cols=('Mkt-RF', 'RF')):
    """(dates, {col: [fraction]}) for rows dated in [DATA_START, DATA_END). Rows outside are skipped on their date
    BEFORE any value is parsed. Gated; refuses a hash mismatch, a missing value (-99.99) or a non-increasing date."""
    _gate(zip_path)
    b = Path(zip_path).read_bytes()
    digest = hashlib.sha256(b).hexdigest()
    _gate(zip_path, digest)
    if digest != want_sha:
        raise Refused(f'{zip_path} does not match its pinned SHA-256')
    _STATE['data_opened'] = True
    lines = _csv_lines_from(b, zip_path)
    hdr = next((l for l in lines if l.replace(' ', '').startswith(',Mkt-RF')), None)
    if hdr is None:
        raise Refused(f'{zip_path}: no header line')
    names = [h.strip() for h in hdr.split(',')]
    idx = [names.index(c) for c in cols]
    dates, vals = [], {c: [] for c in cols}
    for line in lines:
        head = line.split(',', 1)[0].strip()
        if not (len(head) == 8 and head.isdigit()):
            continue
        d = _date8(head)
        if d < C.DATA_START or d >= C.DATA_END:
            continue                                      # dropped at load; its values are never parsed
        parts = line.split(',')
        for c, i in zip(cols, idx):
            v = float(parts[i])
            if v <= -99.0:
                raise Refused(f'{zip_path}: missing value on {d}')
            vals[c].append(v / 100.0)
        if dates and d <= dates[-1]:
            raise Refused(f'{zip_path}: dates not strictly increasing at {d}')
        dates.append(d)
    return dates, vals


# ------------------------------------------------------------------ results: append, commit, verify
def append_row(row, results):
    results = Path(results)
    if results.exists() and results.stat().st_size and not results.read_bytes().endswith(b'\n'):
        raise Refused(f'{results.name} does not end with a newline — refusing to append (review B m3)')
    line = json.dumps(row, default=str, sort_keys=True)
    with open(results, 'a') as f:
        f.write(line + '\n')
    return line


def commit_results(msg, results, retries=3, wait=1.0):
    """Commit results.jsonl ALONE, as the runner (author r015_run), no hooks; retried on a transient lock. Never pushes."""
    results = Path(results)
    for attempt in range(retries + 1):
        r = _git(results.parent, '-c', f'user.name={RUNNER_NAME}', '-c', f'user.email={RUNNER_EMAIL}', 'commit',
                 '--no-verify', '--only', '-m', msg, '--', results.name)
        if not r.returncode:
            return
        if attempt < retries:
            time.sleep(wait)
    raise Refused(f'could not commit {results.name}: {r.stderr.strip()[:300]} — stop; commit the row BY HAND, as it '
                  'is; (k) refuses every later run until it is committed')


HOOKS = {'commit': commit_results}


def real_tree_failures(root=None):
    """The real results file, the real data tree and the real directory are used only together, only with the default
    commit hook, and only with the real guard."""
    root_real = Path(root or _HERE0).resolve() == _HERE0.resolve()
    real_r, real_d = Path(RESULTS).resolve() == REAL_RESULTS, Path(DATA).resolve() == DATA_REAL
    out = []
    if not (real_r == real_d == root_real):
        out.append('(M1) the real directory, the real results.jsonl and the real data tree are used only together')
    if (real_r or real_d or root_real) and HOOKS.get('commit') is not commit_results:
        out.append('(M1) a non-default commit hook is never used on the real tree')
    if (real_r or real_d or root_real) and (guard is not _REAL_GUARD or _GUARD_FAILURES is not guard_failures):
        out.append('(M1) the real tree is opened only by the real guard')
    return out


def record(row, msg):
    """Append, commit through the hook, then REFUSE unless the line is in HEAD:./results.jsonl (checked with git)."""
    res = Path(RESULTS).resolve()
    if (res.is_relative_to(REAL_REPO) or res == REAL_RESULTS) and (
            not _guard_ok() or HOOKS.get('commit') is not commit_results or res != REAL_RESULTS):
        raise Refused('(M1) rows are written into the real repository only by the real run, after the real guard, '
                      'with the default commit hook')
    line = append_row(row, RESULTS)
    HOOKS['commit'](msg, RESULTS)
    if line not in head_lines(RESULTS.parent, RESULTS.name):
        raise Refused(f'the {row.get("mode")} row is not in HEAD:./{RESULTS.name} after the commit — stop')
    return line


def hashes(files, root):
    root = Path(root)
    return {f: sha256(root / f) for f in files if (root / f).is_file()}


# ------------------------------------------------------------------ the computation (after the guard)
def primary(commit, root, table):
    """The decision. Pure function of the US file and the pinned calendar (deterministic)."""
    root = Path(root)
    dates, v = load_french(root / US_ZIP, table[US_ZIP])
    pin = json.loads((root / CALENDAR).read_text())
    if [d.isoformat() for d in dates] != pin['dates']:
        raise Refused('the loaded trading dates differ from the pinned calendar.json')
    cal = C.build_calendar(dates)
    fails = C.calendar_failures(cal)
    if fails:
        raise Refused('; '.join(fails))
    summ = C.calendar_summary(cal, dates)
    if summ['rows'] != pin['summary']['rows']:
        raise Refused('the rebuilt calendar differs from the pinned calendar.json')
    print('counts before the decision:', json.dumps({k: v_ for k, v_ in summ.items() if k != 'rows'}))
    x = v['Mkt-RF']
    z, y, sig, mu = C.z_series(x, cal)
    vv = C.v_series(x, cal)
    ex = C.existence(z)
    mu_all = C.pooled_mu(x, cal)
    econ = C.economic(y, vv, cal, mu_all)
    months = [{'month': c['month'], 'T': c['T'], 's': c['s'], 'friday': c['weekday'] == 4, 'y': a, 'v': b, 'sigma': sg,
               'z': zz} for c, a, b, sg, zz in zip(cal, y, vv, sig, z)]
    row = {'registration': REG_NAME, 'mode': 'result', 'registration_commit': commit,
           'run_at': dt.datetime.now(dt.UTC).isoformat(),
           'code_hashes': hashes(GUARDED_CODE, root), 'data_hashes': {f: sha256(root / f) for f in GUARDED_DATA},
           'params': {'data_start': C.DATA_START.isoformat(), 'data_end_exclusive': C.DATA_END.isoformat(),
                      'cal_crit': C.CAL_CRIT, 'q': C.Q, 'vol_days': C.VOL_DAYS, 'vol_end': C.VOL_END,
                      'vol_floor': C.VOL_FLOOR, 'shrink': C.SHRINK, 'cost_month': C.COST_MONTH, 'l_cap': C.L_CAP,
                      'bar': C.BAR, 'us_t2': C.US_T2.isoformat(), 'us_t1': C.US_T1.isoformat()},
           'mu_by_era': {str(k): val for k, val in mu.items()}, 'mu_all': mu_all, 'existence': ex, 'economic': econ, 'months': months}
    row['decision'] = C.decide(ex, econ)
    lab, rfails = C.recheck(row, table)
    if rfails or lab != row['decision']:
        row['recheck_failed'] = rfails
    return row, (dates, x, cal, mu, y, z, vv)


def reported(state):
    """Reported blocks: decide nothing. Each block is computed independently; a failing block stores its error.
    Revision 1: no Europe block (held back for the confirmation, §8) and no HMM block (dropped; review A M4)."""
    dates, x, cal, mu, y, z, vv = state
    out = {}

    def blk(name, fn):
        try:
            out[name] = fn()
        except Exception as e:  # noqa: BLE001 - a reported block never stops the run
            out[name] = {'error': repr(e)[:300]}
    blk('raw_y_equal_weight (the draft primary)', lambda: C.existence(y, C.crit(len(y))))
    blk('long_only_rebound_era_mu_corrected', lambda: C.long_only_corrected(x, cal, mu))
    blk('long_short_raw_v', lambda: C.summary(vv))
    blk('by_era_z', lambda: C.by_era(z, cal))
    blk('by_era_y', lambda: C.by_era(y, cal))
    blk('recent_2021_01_on_z', lambda: C.recent(z, cal))
    blk('recent_2021_01_on_y', lambda: C.recent(y, cal))
    blk('decomposition_by_era', lambda: C.decomposition(x, cal, mu))
    blk('friday_T_vs_other_z', lambda: C.friday(z, cal))
    blk('without_quarter_end_months_z', lambda: C.without_quarter_ends(z, cal))
    blk('mechanism_contrast', lambda: C.mechanism_contrast(x, cal, mu))
    blk('placebo_T_minus_12', lambda: C.placebo(x, cal, mu))
    blk('mes_net', lambda: C.summary([b - C.COST_MONTH for b in vv]))
    blk('xyz_net', lambda: C.summary([b - C.xyz_cost_month(dates, c) for b, c in zip(vv, cal)]))
    blk('traded_era_v', lambda: C.summary([b for b, c in zip(vv, cal) if c['s'] == C.TRADED_ERA]))
    return out


def _run(root, guard_fn):
    root = Path(root)
    if RESULTS.resolve() != (root / 'results.jsonl').resolve() or Path(DATA).resolve() != (root / 'data' / 'r015').resolve():
        raise Refused('(M1) results.jsonl and data/r015 must be those of the tree being run')
    global _GATE
    _GATE = None
    commit = guard_fn()
    table = dict(HASH_ROW.findall(head_text(root, REG_REL) or ''))
    _STATE['data_opened'] = False
    stage = 'started'
    sline = record({'registration': REG_NAME, 'mode': 'started', 'registration_commit': commit,
                    'env': env_fingerprint(), 'code_hashes': hashes(GUARDED_CODE, root),
                    'run_at': dt.datetime.now(dt.UTC).isoformat()}, '015 started (r015_run.py)')
    if guard_fn is _REAL_GUARD and _guard_ok() and Path(RESULTS).resolve() == REAL_RESULTS \
            and sline in head_lines(REAL_RESULTS.parent, REAL_RESULTS.name):
        _GATE = _GUARD_PASSED                                   # the data gate opens only now (Revision 2, r1-m1)
    try:
        stage = 'primary'
        row, state = primary(commit, root, table)
        stage = 'result_commit'
        line = record(row, '015 result (r015_run.py)')
    except BaseException as e:
        try:
            record({'registration': REG_NAME, 'mode': 'failed', 'registration_commit': commit, 'stage': stage,
                    'data_opened': _STATE['data_opened'], 'error': repr(e)[:500],
                    'run_at': dt.datetime.now(dt.UTC).isoformat()}, '015 failed (r015_run.py)')
        except BaseException:  # noqa: BLE001 - the original error is what propagates
            pass
        raise
    if row.get('recheck_failed'):
        raise Refused('internal: the decision does not recompute from its records; stored with recheck_failed, not printed')
    print(json.dumps({k: row['existence'][k] for k in ('G', 'mean', 'sd', 't', 'crit', 'mean_without_largest', 'criteria')},
                     indent=1, default=str))
    print('economic (applies only after an existence pass):', json.dumps(row['economic'], default=str))
    print('decision:', row['decision'])
    rep = {'registration': REG_NAME, 'mode': 'reported', 'registration_commit': commit, 'result_row_sha256': line_sha(line),
           'run_at': dt.datetime.now(dt.UTC).isoformat(), 'blocks': reported(state)}
    record(rep, '015 reported blocks (r015_run.py)')
    print(json.dumps(rep['blocks'], indent=1, default=str))
    return row, rep


def run():
    """The read, once, on the real tree only (no argument; review B M1)."""
    f = real_tree_failures(_HERE0)
    if f:
        raise Refused('; '.join(f))
    return _run(_HERE0, guard)


def _run_in(root, guard_fn):
    """TESTS ONLY: run on a throwaway tree with a guard bound to it. Refused unless the tree, RESULTS and DATA are all
    outside the real repository and the real data tree."""
    root = Path(root).resolve()
    if root.is_relative_to(REAL_REPO) or root == _HERE0.resolve() or real_tree_failures(root) or \
            Path(RESULTS).resolve().is_relative_to(REAL_REPO) or Path(DATA).resolve().is_relative_to(REAL_REPO):
        raise Refused('(M1) _run_in() never touches the real repository, its results or its data')
    return _run(root, guard_fn)


def build_calendar_file(root=None, out=None):
    """Pre-registration, DATES ONLY: the trading dates in [DATA_START, DATA_END) and the month-end calendar."""
    root = Path(root or HERE)
    want = {l.split()[1]: l.split()[0] for l in (root / FRENCH_DIR / 'SHA256SUMS').read_text().splitlines() if l.strip()}
    dates = read_dates_only(root / US_ZIP, want[Path(US_ZIP).name])
    cal = C.build_calendar(dates)
    fails = C.calendar_failures(cal)
    if fails:
        raise Refused('; '.join(fails))
    body = json.dumps({'source': US_ZIP, 'source_sha256': want[Path(US_ZIP).name], 'dates': [d.isoformat() for d in dates],
                       'summary': C.calendar_summary(cal, dates)}, indent=1, sort_keys=True) + '\n'
    path = Path(out or root / CALENDAR)
    if path.exists() and path.read_text() != body:
        raise Refused(f'{path} exists and differs from the rebuild — it is pinned; investigate, never overwrite')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    s = C.calendar_summary(cal, dates)
    print(json.dumps({k: v for k, v in s.items() if k != 'rows'}), 'dates', len(dates), dates[0], dates[-1])
    return path


def main():
    if '--check' in sys.argv:
        if REPO.resolve() != REAL_REPO:
            print(f'(w) this runner lives in {REPO}, not in the real repository {REAL_REPO}')
        f, c, _ = guard_failures(HERE, REG_REL, real_repo=REAL_REPO, anchor=ANCHOR, env_check=True)
        print('\n'.join(f) if f else f'guard open (registration commit {c})')
    elif '--fingerprint' in sys.argv:
        print(json.dumps(env_fingerprint(), indent=1))
    elif '--calendar' in sys.argv:
        build_calendar_file()
    elif '--run' in sys.argv:
        run()
    else:
        sys.exit(__doc__)


if __name__ == '__main__':
    main()
