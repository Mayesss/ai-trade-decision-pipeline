"""Registration 016 — crypto trend following, the Zarattini-Pagani-Barbon (2025) rule verbatim: the guard, the one
read, and the synthetic power run. Revision 1.

    python3 -I -S research/alpha/r016_run.py --check        # every reason the read may not start; computes nothing
    python3 -I -S research/alpha/r016_run.py --fingerprint  # the interpreter / stdlib fingerprint (pin recovery)
    python3 -I -S research/alpha/r016_run.py --run          # the read, once (after r016_fetch.py and the approval)
    python3 -I -S research/alpha/r016_run.py --power        # SYNTHETIC size, calibration and power; reads no data

The read refuses unless (every check is a refusal, never an assert; `r016_fetch.py` runs the same checks in mode
'fetch'):
  (s)  the registration AS COMMITTED IN HEAD says REGISTERED (status, hash table and approval / restart / repo-path
       lines are read from HEAD:./, never from the working tree; review B M3);
  (a)  it is tracked;  (b) its working-tree bytes (`git hash-object --no-filters`) equal its HEAD blob;  (c) committed;
  (r)  across the WHOLE history (`--all --reflog`, no first-parent restriction) exactly one REGISTERED content of the
       registration exists once approval, restart and repo-path lines are removed; reg0 = the earliest commit carrying
       it; HEAD's registration (minus those lines) and every guarded code blob equal reg0's; both fetch rows carry reg0
       (review B M4: a revision committed in the 15 months before the read cannot pass);
  (d)  every guarded code file's working-tree bytes equal its HEAD blob; no guarded path, the registration or
       results.jsonl carries an assume-unchanged / skip-worktree flag or a filter attribute; no untracked, ignored or
       modified importable file at research/alpha's top level or in ct16/, and no module there named like a stdlib
       module (research/alpha is APPENDED to sys.path, after the stdlib) — commit every top-level .py first;
  (e)  every guarded code and data file is listed exactly once in the HEAD table and matches it;
  (t)  today (UTC) >= 2029-01-08 (the registered end 2028-12-31 + 8 days); a `now` argument is refused whenever the
       real repository is involved (review B M1);
  (k)  results.jsonl is tracked, equals its HEAD blob, and is empty or ends with a newline;
  (w)  the repository is THE real one: this file's repository is the pinned REAL_REPO (or the path of a committed
       `016 repo-path: APPROVED <date> <abs path>` line, accepted only when REAL_REPO no longer exists), git's top
       level is that path, the main worktree (--git-dir == --git-common-dir == REAL_REPO/.git), HEAD a named branch
       (not detached), and the anchor an ancestor of HEAD (review B B1);
  (v)  python -I -S (isolated, no site) and no -O; no .pth in site-packages; the interpreter version, the binary's
       SHA-256 and the stdlib tree fingerprint equal the pins; every loaded module is from the pinned stdlib or a hashed
       016 file in this directory, every cached bytecode under the fresh sys.pycache_prefix (review B M2);
  (f)  fetch: no 016 `fetched`, `read_started`, `read`, `read_failed` row anywhere (working tree; `git log --all
       --reflog -m --root -p --text --no-textconv --no-ext-diff`, log.showRoot=true, no replace objects), no
       unparseable added line that mentions a 016 registration;
  read: (g1) exactly one `fetch_started` and one `fetched` row, committed in HEAD:./results.jsonl, both carrying reg0;
       (g2) the fetched row's code hashes and environment equal today's; (g3) the fetch manifest hashes to the digest
       stored in the fetched row; (h') a committed line `016 read: APPROVED <date> row <sha256 of the fetched row>`
       added strictly after the commit that added the fetched row; (i) no `read` or `read_failed` row anywhere, and a
       `read_started` row blocks unless a later committed `016 read restart: APPROVED <date> started-row <sha256>`
       names it AND no commit anywhere (`--all --reflog`) that adds a read_started / read / read_failed line was made
       by the runner (the data open only after the runner's own started commit is in HEAD, so "no runner commit" is
       the verifiable fact that no data was opened).
git is called by absolute path with GIT_* stripped, no system or global config, no replace objects, no hooks.
The runner commits results.jsonl ALONE, as the runner identity, after each row, and refuses unless the line is in
HEAD:./results.jsonl. It never pushes. A failed automatic commit leaves the row (and, for the read row, the decision)
in plain text in the working tree: commit it by hand, as it is.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
import random
import re
import subprocess
import sys
import sysconfig
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
_MAIN = sys.modules.get('__main__')
if not (_MAIN is not None and sys.pycache_prefix
        and Path(getattr(_MAIN, '__file__', '') or '/').resolve() == (HERE / 'r016_fetch.py').resolve()
        and getattr(_MAIN, '_R016_PREFIX', None) == sys.pycache_prefix):
    # Revision 2 (review B R1-M1): a prefix is kept ONLY when it is the one r016_fetch.py itself just created as the
    # running program; any other pre-existing prefix (`-X pycache_prefix=...`, PYTHONPYCACHEPREFIX) is replaced.
    sys.pycache_prefix = tempfile.mkdtemp(prefix='r016-pyc-')   # never load bytecode from an existing __pycache__
_PYC_OWN = sys.pycache_prefix
sys.dont_write_bytecode = True
sys.path.append(str(HERE))                                   # AFTER the stdlib (review B M2)
from ct16 import data as D  # noqa: E402
from ct16 import rule as R  # noqa: E402
from ct16 import stats as S  # noqa: E402

Refused = R.Refused
_HERE0 = HERE                                                # captured at import; production uses only these
REPO = HERE.parent.parent
REAL_REPO = Path('/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline')   # (w), review B B1
REG_NAME = '016-crypto-trend-verbatim.md'
REG_REL = f'registrations/{REG_NAME}'
REG_PREFIX = '016-'
RESULTS_REL = 'results.jsonl'
KLINES_A_REL = 'data/scout_c1/bn_um_klines_1d_monthly'      # the C1 scout's pull (months <= 2026-06), pinned
STORE_REL = 'data/r016/forward'                              # the guarded fetch (klines 2026-07 ->, all funding)
STATIC_EXCHANGE_REL = 'data/r016/static/fapi_exchangeInfo_20261011.json'   # serverTime 2026-10-09T07:16Z (m6)
REAL_DATA = ((HERE / KLINES_A_REL).resolve(), (HERE / STORE_REL).resolve())
ANCHOR = '9c131e222d99a93d41596a2ba3a3066b437f142e'           # the C1 scout and 000t (2026-10-13)
GIT_BIN = '/usr/bin/git'
RUNNER_NAME, RUNNER_EMAIL = 'r016_run', 'r016_run@localhost'
PY_PIN = '3.14.8'
PY_EXE_SHA256 = '7ebde552773a4045532637b114d645a313e045ac07b26f5e520bf039f1e34063'
STDLIB_SHA256 = 'c99bbd7540f6c89e5a15b371f3fe3a853da11f6556ed2bbe55609286c650071e'
PTH_PINNED = ()                                              # .pth files allowed in site-packages: none
STDLIB_SKIP = {'site-packages', '__pycache__', 'test', 'tests', 'idlelib', 'tkinter', 'turtledemo', 'ensurepip'}
GUARDED_CODE = ('ct16/__init__.py', 'ct16/rule.py', 'ct16/stats.py', 'ct16/data.py', 'ct16/sim.py', 'r016_run.py',
                'r016_fetch.py')
GUARDED_DATA = (f'{KLINES_A_REL}/SHA256SUMS', STATIC_EXCHANGE_REL)
ALLOWED_MODULES = ('ct16/__init__.py', 'ct16/rule.py', 'ct16/stats.py', 'ct16/data.py', 'r016_run.py', 'r016_fetch.py')
A_MONTH_LAST = '2026-06'                                     # the scout pull's last month (the programme's cut)
FWD_MONTHS = ('2026-07', '2028-12')                          # klines fetched by r016_fetch.py
FUNDING_MONTHS = ('2025-03', '2028-12')                      # funding fetched by r016_fetch.py (the test span)

HASH_ROW = re.compile(r'^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|', re.M)
STATUS = re.compile(r'^\*\*Status: (\w+)', re.M)
APPROVAL = re.compile(r'^016 read: APPROVED (\d{4}-\d{2}-\d{2}) row ([0-9a-f]{64})\s*$', re.M)
RESTART = re.compile(r'^016 read restart: APPROVED (\d{4}-\d{2}-\d{2}) started-row ([0-9a-f]{64})\s*$', re.M)
REPO_PATH = re.compile(r'^016 repo-path: APPROVED (\d{4}-\d{2}-\d{2}) (/\S+)\s*$', re.M)
ALLOWED_LINES = (APPROVAL, RESTART, REPO_PATH)
READ_MODES = ('read_started', 'read', 'read_failed')
_GATE = None                                                 # (dir, commit): issued by run() after the started row
_GUARD_PASSED = None                                         # (dir, mode, commit): set only by the real guard()
PLACEBO_SEEDS = 200


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


def head_text(root, rel, rev='HEAD'):
    r = _git(root, 'show', '--no-textconv', revpath(rev, rel))
    return r.stdout if r.returncode == 0 else None


def head_blob(root, rel, rev='HEAD'):
    r = _git(root, 'rev-parse', '--verify', '-q', revpath(rev, rel))
    return r.stdout.strip() if r.returncode == 0 else None


def work_blob(root, rel):
    r = _git(root, 'hash-object', '--no-filters', '--', rel)
    return r.stdout.strip() if r.returncode == 0 else None


def same_as_head(root, rel):
    hb = head_blob(root, rel)
    return hb is not None and (Path(root) / rel).is_file() and work_blob(root, rel) == hb


def is_016(row):
    return isinstance(row, dict) and str(row.get('registration', '')).startswith(REG_PREFIX)


def _safe_json(line):
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def _added_lines(root, rel):
    r = _git(root, 'log', '--all', '--reflog', '-m', '--root', '-p', '--text', '--no-textconv', '--no-ext-diff',
             '--no-color', '--format=', '--', rel)
    return r.returncode, [l[1:] for l in r.stdout.splitlines() if l.startswith('+') and not l.startswith('+++')]


def result_rows(root, results_rel=RESULTS_REL):
    """([(line, row)], [problems]): every distinct 016 line in the working tree and every ADDED line in the history of
    every branch, the reflog, merges, stashes and root commits. A line that mentions a 016 registration but does not
    parse, or a failing git call, is a problem (fail closed; review B m3)."""
    texts, problems = [], []
    res = Path(root) / results_rel
    if res.exists():
        texts.extend(res.read_text().splitlines())
    rc, lines = _added_lines(root, results_rel)
    if rc != 0:
        problems.append(f'git log over {results_rel} failed')
    texts.extend(lines)
    out, seen = [], set()
    for line in texts:
        row = _safe_json(line)
        if row is None:
            if f'"registration": "{REG_PREFIX}' in line or f'"registration":"{REG_PREFIX}' in line:
                problems.append('an unparseable line mentions a 016 registration')
            continue
        if is_016(row) and line not in seen:
            seen.add(line)
            out.append((line, row))
    return out, problems


def head_lines(root, results_rel=RESULTS_REL):
    return set(_git(root, 'show', '--no-textconv', revpath('HEAD', results_rel)).stdout.splitlines())


def first_commit_containing(root, rel, needle):
    for c in _git(root, 'log', '--format=%H', '--reverse', '--', rel).stdout.split():
        if needle in (head_text(root, rel, c) or '').splitlines():
            return c
    return None


def committed_strictly_after(root, row_rel, row_line, reg_rel, approval_line):
    row_c = first_commit_containing(root, row_rel, row_line)
    app_c = first_commit_containing(root, reg_rel, approval_line)
    return bool(row_c and app_c and row_c != app_c
                and _git(root, 'merge-base', '--is-ancestor', row_c, app_c).returncode == 0)


def is_ancestor(root, c):
    return bool(c) and _git(root, 'merge-base', '--is-ancestor', c, 'HEAD').returncode == 0


def hashes(root, files):
    return {f: sha256(Path(root) / f) for f in files if (Path(root) / f).is_file()}


def manifest_digest(files_to_sha):
    body = '\n'.join(f'{k}\t{v}' for k, v in sorted(files_to_sha.items()))
    return hashlib.sha256(body.encode()).hexdigest()


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


def pth_files():
    out = []
    for k in ('purelib', 'platlib'):
        d = Path(sysconfig.get_paths()[k])
        if d.is_dir():
            out += sorted(str(f) for f in d.iterdir() if f.suffix == '.pth')
    return sorted(set(out))


def env_fingerprint():
    exe = os.path.realpath(sys.executable)
    fp, n = stdlib_fingerprint()
    return {'python': sys.version.split()[0], 'executable': exe, 'executable_sha256': sha256(exe),
            'stdlib_sha256': fp, 'stdlib_files': n, 'isolated': sys.flags.isolated, 'no_site': sys.flags.no_site,
            'optimize': sys.flags.optimize, 'pth_files': pth_files(),
            'git': GIT_BIN, 'git_version': _git(HERE, '--version').stdout.strip()}


def shadow_modules(root):
    """Anything importable at research/alpha's top level or in ct16/ whose bytes are not its HEAD blob (*.py, sourceless
    *.pyc, *.so / *.pyd, packages), and anything there named like a stdlib module, tracked or not. Scanned on the
    file system (ignored files count), judged by `hash-object --no-filters` against HEAD (review B M2, M3)."""
    root = Path(root)
    bad = set()
    std = set(sys.stdlib_module_names)
    for base in (root, root / 'ct16'):
        if not base.is_dir():
            continue
        for p in sorted(base.iterdir()):
            rel = str(p.relative_to(root))
            if p.name == '__pycache__':
                continue
            if p.is_file() and p.suffix in ('.py', '.pyc', '.so', '.pyd'):
                if base == root and p.name.split('.')[0] in std:
                    bad.add(rel + ' (stdlib name)')
                elif not same_as_head(root, rel):
                    bad.add(rel)
            elif p.is_dir() and (p / '__init__.py').exists():
                if base == root and p.name in std:
                    bad.add(f'{rel}/__init__.py (stdlib name)')
                files = [f for f in p.rglob('*') if f.is_file() and '__pycache__' not in f.parts] if base != root \
                    else [p / '__init__.py']
                bad.update(str(f.relative_to(root)) for f in files if not same_as_head(root, str(f.relative_to(root))))
    return sorted(bad)


def module_origin_failures(here=None, allowed=ALLOWED_MODULES):
    """Every loaded module with a file is in the pinned stdlib tree or a hashed 016 file in this directory; every cached
    bytecode is under the fresh sys.pycache_prefix (review B M2: the fetcher once loaded the guard from __pycache__)."""
    here = Path(here or _HERE0).resolve()
    std = stdlib_root()
    ok = {(here / f).resolve() for f in allowed}
    pyc = Path(sys.pycache_prefix).resolve() if sys.pycache_prefix else None
    out = []
    for name, mod in sorted(sys.modules.items()):
        f = getattr(mod, '__file__', None)
        if not f:
            continue
        p = Path(f).resolve()
        if not (p.is_relative_to(std) or p in ok):
            out.append(f'(v) module {name} is loaded from {p}, outside the pinned stdlib and the hashed 016 files')
        cached = getattr(mod, '__cached__', None)
        if p in ok and name != '__main__' and not cached:
            out.append(f'(v) module {name} has no __cached__ record (tampered import state)')
        if cached and not p.is_relative_to(std) and (pyc is None or not Path(cached).resolve().is_relative_to(pyc)):
            out.append(f'(v) module {name} bytecode {cached} is not under the fresh pycache_prefix')
    return out


def environment_failures(root):
    out = []
    if not sys.flags.isolated:
        out.append('(v) run with python3 -I -S (isolated mode): PYTHONPATH, user site and the script directory must not load')
    if not sys.flags.no_site:
        out.append('(v) run with python3 -I -S (no site): a site-packages .pth would execute at startup')
    if 'site' in sys.modules or type(sys.flags).__name__ != 'flags' or not isinstance(sys.flags, tuple):
        out.append("(v) the site module ran or sys.flags is not the interpreter's own struct")
    if sys._xoptions or any(a.startswith('-X') for a in sys.orig_argv[1:]):
        out.append(f'(v) -X options are refused (review B R1-M1): {sorted(sys._xoptions)}')
    if sys.pycache_prefix != _PYC_OWN:
        out.append('(v) sys.pycache_prefix is not the fresh prefix this program created')
    opts = [a for a in sys.orig_argv[1:] if a.startswith('-') and not a.startswith('--')]
    if not (any('I' in a for a in opts) and any('S' in a for a in opts)):
        out.append('(v) the command line does not carry -I and -S (sys.orig_argv)')
    extra = [f for f in pth_files() if Path(f).name not in PTH_PINNED]
    if extra:
        out.append(f'(v) .pth files in site-packages that are not pinned: {extra[:4]}')
    if sys.flags.optimize:
        out.append('(v) python -O / -OO is refused')
    if sys.version.split()[0] != PY_PIN:
        out.append(f'(v) python {sys.version.split()[0]} differs from the pin {PY_PIN}')
    exe = os.path.realpath(sys.executable)
    if not Path(exe).is_file() or sha256(exe) != PY_EXE_SHA256:
        out.append(f'(v) the interpreter binary {exe} does not match its pinned SHA-256 (recovery: install python '
                   f'{PY_PIN} until `--fingerprint` equals the pins; the pins are never edited after REGISTERED)')
    if stdlib_fingerprint()[0] != STDLIB_SHA256:
        out.append('(v) the stdlib tree fingerprint differs from the pin')
    out += module_origin_failures()
    if not sys.pycache_prefix or Path(sys.pycache_prefix).resolve().is_relative_to(REAL_REPO):
        out.append('(v) sys.pycache_prefix must be a fresh directory outside the repository')
    return out


# ------------------------------------------------------------------ registration history (reg0) and flags
def flag_failures(root, files):
    out = []
    for line in _git(root, 'ls-files', '-v', '--', *files).stdout.splitlines():
        tag, name = line[:1], line[2:]
        if tag == 'S' or tag.islower():
            out.append(f'(d) {name} carries an index flag ({tag}) that hides working-tree changes')
    for line in _git(root, 'check-attr', 'filter', '--', *files).stdout.splitlines():
        if not line.rstrip().endswith(': unspecified'):
            out.append(f'(d) a filter attribute applies: {line.strip()}')
    return out


def canonical(text):
    """The registration without approval, restart and repo-path lines (the only lines added after REGISTERED)."""
    return '\n'.join(l for l in (text or '').split('\n')
                     if not any(p.fullmatch(l.rstrip()) for p in ALLOWED_LINES)).rstrip()   # trailing blank lines ignored


def registered_versions(root, reg_rel):
    """[(commit time, commit, canonical sha)] of every commit anywhere (`--all --reflog`) whose registration says
    REGISTERED, sorted by time."""
    out = []
    r = _git(root, 'log', '--all', '--reflog', '--format=%H %ct', '--', reg_rel)
    for line in sorted(set(r.stdout.split('\n'))):
        if not line.strip():
            continue
        c, t = line.split()
        text = head_text(root, reg_rel, c) or ''
        m = STATUS.search(text)
        if m and m.group(1) == 'REGISTERED':
            out.append((int(t), c, hashlib.sha256(canonical(text).encode()).hexdigest()))
    return sorted(out)


def reg0_failures(root, reg_rel, code):
    """(reg0 or None, [failures]): exactly one canonical REGISTERED content anywhere; reg0 = the earliest commit with
    it; HEAD's canonical registration and every guarded code blob equal reg0's (review B M4)."""
    vers = registered_versions(root, reg_rel)
    if not vers:
        return None, []
    cands = [v for v in vers if not any(o[1] != v[1] and _git(root, 'merge-base', '--is-ancestor', o[1], v[1]).returncode
                                        == 0 for o in vers)] or vers   # earliest by ancestry, then by time
    reg0, want = cands[0][1], cands[0][2]
    out = []
    if len({v for _, _, v in vers}) != 1:
        out.append(f'(r) {len({v for _, _, v in vers})} different REGISTERED versions of the registration exist in the '
                   f'history (first {reg0[:12]}) — an amendment after REGISTERED is a new registration')
    if hashlib.sha256(canonical(head_text(root, reg_rel)).encode()).hexdigest() != want:
        out.append(f"(r) HEAD's registration differs from the REGISTERED commit {reg0[:12]} beyond approval, restart "
                   'and repo-path lines')
    if not is_ancestor(root, reg0):
        out.append(f'(r) the REGISTERED commit {reg0[:12]} is not an ancestor of HEAD')
    for f in code:
        a = head_blob(root, f, reg0)
        if not a or a != head_blob(root, f):
            out.append(f'(r) {f} at HEAD differs from the REGISTERED commit {reg0[:12]}')
    return reg0, out


def runner_added_lines(root, results_rel):
    """Lines added to results.jsonl by any commit anywhere whose author or committer is the runner identity."""
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


def bound_repo(text):
    """REAL_REPO, or the path of a committed repo-path line when REAL_REPO no longer exists on this machine."""
    lines = REPO_PATH.findall(text or '')
    if lines and not REAL_REPO.exists():
        return Path(lines[-1][1])
    return REAL_REPO


def binding_failures(root, real_repo):
    """(w): the main worktree of the real repository, on a named branch."""
    out = []
    real_repo = Path(real_repo).resolve()
    top = _git(root, 'rev-parse', '--show-toplevel').stdout.strip()
    if not top or Path(top).resolve() != real_repo:
        out.append(f'(w) git top level is {top!r}, not the real repository {real_repo}')
    gd = _git(root, 'rev-parse', '--absolute-git-dir').stdout.strip()
    gc = _git(root, 'rev-parse', '--git-common-dir').stdout.strip()
    if not gd or Path(gd).resolve() != (Path(root) / gc).resolve() or Path(gd).resolve() != real_repo / '.git':
        out.append('(w) not the main worktree of the real repository (a clone, a linked worktree or another .git)')
    if _git(root, 'symbolic-ref', '-q', 'HEAD').returncode:
        out.append('(w) HEAD is detached — run on a named branch of the real repository')
    return out


# ------------------------------------------------------------------ the guard
def guard_failures(root=HERE, mode='run', now=None, code=GUARDED_CODE, data=GUARDED_DATA, reg_rel=REG_REL,
                   results_rel=RESULTS_REL, store_rel=STORE_REL, real_repo=None, anchor=None, env_check=False):
    """([reasons], context): the fetch or read may start only when the list is empty. mode: fetch | run."""
    R.refuse_unless(mode in ('fetch', 'run'), f'unknown mode {mode}')
    root = Path(root)
    out = []
    if now is not None and (root.resolve().is_relative_to(REAL_REPO) or REAL_REPO.is_relative_to(root.resolve())):
        out.append('(t) a `now` override is never accepted on the real repository (review B M1)')
    text = head_text(root, reg_rel) or ''
    m = STATUS.search(text)
    if not m or m.group(1) != 'REGISTERED':
        out.append(f'(s) {Path(reg_rel).name} is not REGISTERED in HEAD — refusing to fetch or compute anything.')
    if _git(root, 'ls-files', '--error-unmatch', '--', reg_rel).returncode:
        out.append(f'(a) {Path(reg_rel).name} is not tracked by git')
    if not same_as_head(root, reg_rel):
        out.append(f'(b) {Path(reg_rel).name}: the working-tree bytes differ from the committed version')
    last = _git(root, 'log', '-1', '--format=%H', '--', reg_rel).stdout.strip()
    if not last:
        out.append(f'(c) {Path(reg_rel).name} has no commit')
    reg0, rf = reg0_failures(root, reg_rel, code)
    out += rf
    for f in code:
        if not same_as_head(root, f):
            out.append(f'(d) guarded code {f}: the working-tree bytes differ from HEAD (or it is not committed)')
    out += flag_failures(root, [reg_rel, *code, results_rel])
    shadow = shadow_modules(root)
    if shadow:
        out.append(f'(d) untracked, ignored, modified or stdlib-named importable files at research/alpha top level or in '
                   f'ct16/: {shadow[:6]} — commit every top-level .py and package (any study) before 016 fetches or reads')
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
    if (now or dt.datetime.now(dt.UTC)).date() < R.READ_NOT_BEFORE:
        out.append(f'(t) the forward span ends {R.END}; nothing is fetched or read before {R.READ_NOT_BEFORE} (UTC)')
    res = root / results_rel
    if not same_as_head(root, results_rel):
        out.append(f'(k) {results_rel} is untracked or differs from HEAD — every row is committed when written')
    elif res.stat().st_size and not res.read_bytes().endswith(b'\n'):
        out.append(f'(k) {results_rel} does not end with a newline — a row appended now would merge into another')
    if real_repo is not None:
        out += binding_failures(root, real_repo)
    if anchor is not None and not is_ancestor(root, anchor):
        out.append(f'(w) the anchor commit {anchor} is not an ancestor of HEAD')
    if env_check:
        out += environment_failures(root)
    rows, problems = result_rows(root, results_rel)
    out += [f'(f) {p}' for p in problems]
    heads = head_lines(root, results_rel)
    commit = reg0 or last
    ctx = {'commit': commit, 'reg0': reg0, 'table': table}
    if any(r.get('mode') in ('read', 'read_failed') for _, r in rows):
        out.append('(i) a 016 read or read_failed row exists (tree, any branch, reflog, merge, stash or root) — once')
    started_reads = [(l, r) for l, r in rows if r.get('mode') == 'read_started']
    fetch_started = [(l, r) for l, r in rows if r.get('mode') == 'fetch_started']
    fetched = [(l, r) for l, r in rows if r.get('mode') == 'fetched']
    if mode == 'fetch':
        if fetched:
            out.append('(f) the fetch has completed (a fetched row exists) — it runs once')
        if started_reads:
            out.append('(f) a read_started row exists — the fetch is closed')
        if len(fetch_started) > 1:
            out.append(f'(f) {len(fetch_started)} fetch_started rows; at most one is allowed')
        for l, r in fetch_started:
            if r.get('registration_commit') != commit or l not in heads:
                out.append('(f) the existing fetch_started row does not carry the REGISTERED commit or is not committed')
        ctx['resume'] = bool(fetch_started)
        return out, ctx
    runner_lines = runner_added_lines(root, results_rel)
    for sl0, _ in started_reads:
        rs = [a for a in RESTART.finditer(text) if a.group(2) == line_sha(sl0)]
        by_runner = any((_safe_json(l) or {}).get('mode') in READ_MODES and is_016(_safe_json(l)) for l in runner_lines)
        if by_runner or not rs or sl0 not in heads or not committed_strictly_after(
                root, results_rel, sl0, reg_rel, rs[0].group(0).rstrip()):
            out.append("(i) a read_started row exists and the runner committed a read row, or no committed, later "
                       "'016 read restart: APPROVED <date> started-row <sha256>' line names it — the read runs once")
    if len(fetch_started) != 1 or len(fetched) != 1:
        out.append(f'(g1) {len(fetch_started)} fetch_started and {len(fetched)} fetched rows; exactly one of each is '
                   'required (fetch first)')
        return out, ctx
    (sl, sr), (fl, fr) = fetch_started[0], fetched[0]
    if sl not in heads or fl not in heads:
        out.append('(g1) the fetch rows are not committed in HEAD:./results.jsonl')
    if sr.get('registration_commit') != commit or fr.get('registration_commit') != commit:
        out.append('(g1) the fetch rows do not carry the REGISTERED commit reg0')
    if fr.get('code_hashes') != hashes(root, code):
        out.append('(g2) the code hashes stored in the fetched row differ from the code on disk')
    if env_check and fr.get('env') != env_fingerprint():
        out.append('(g2) the Python environment differs from the fetch')
    man = root / store_rel / 'manifest.json'
    if not man.exists():
        out.append('(g3) the fetch manifest is missing')
    else:
        mj = json.loads(man.read_text())
        files = mj.get('files', {})
        now_h = {f: (sha256(root / store_rel / f) if (root / store_rel / f).is_file() else None) for f in files}
        if manifest_digest(now_h) != fr.get('manifest_digest') or manifest_digest(files) != fr.get('manifest_digest'):
            out.append('(g3) the fetched data do not hash to the digest stored in the fetched row')
    appr = [a for a in APPROVAL.finditer(text) if a.group(2) == line_sha(fl)]
    if not appr:
        out.append("(h') no committed line '016 read: APPROVED <date> row <sha256 of the fetched row>'")
    elif not committed_strictly_after(root, results_rel, fl, reg_rel, appr[0].group(0).rstrip()):
        out.append("(h') the approval must be committed strictly after the commit that added the fetched row")
    ctx.update(fetch_commit=fr.get('registration_commit'), fetched_row_sha256=line_sha(fl), fetched_row=fr)
    return out, ctx


_GUARD_FAILURES = guard_failures                              # captured at import (review B m2)


def guard(mode):
    """The real guard: this file's own repository, which must be the real one; constants captured at import; every
    check. It does NOT open the data (run() issues the gate after the started row is in HEAD)."""
    global _GATE, _GUARD_PASSED
    _GATE = None
    _GUARD_PASSED = None
    text = head_text(_HERE0, REG_REL)
    real = bound_repo(text)
    if REPO.resolve() != real.resolve() or HERE != _HERE0:
        raise Refused(f'(w) this runner lives in {REPO}, not in the real repository {real}')
    failures, ctx = _GUARD_FAILURES(_HERE0, mode, None, GUARDED_CODE, GUARDED_DATA, REG_REL, RESULTS_REL, STORE_REL,
                                    real_repo=real, anchor=ANCHOR, env_check=True)
    if failures:
        raise Refused('\n'.join(failures))
    _GUARD_PASSED = (str(_HERE0), mode, ctx['commit'])
    return ctx


_REAL_GUARD = guard
RESULTS = HERE / RESULTS_REL
REAL_RESULTS = (_HERE0 / RESULTS_REL).resolve()


def _guard_ok(mode):
    return (_GUARD_PASSED is not None and _GUARD_PASSED[0] == str(_HERE0) and _GUARD_PASSED[1] == mode
            and guard is _REAL_GUARD and _GUARD_FAILURES is guard_failures)


def _gate_ok():
    return (_guard_ok('run') and _GATE is not None and _GATE == _GUARD_PASSED
            and Path(RESULTS).resolve() == REAL_RESULTS and HOOKS.get('commit') is commit_results)


def append_row(row, results=None):
    results = Path(results or RESULTS)
    if results.exists() and results.stat().st_size and not results.read_bytes().endswith(b'\n'):
        raise Refused(f'{results.name} does not end with a newline — refusing to append (review B m3)')
    line = json.dumps(row, default=str, sort_keys=True)
    with open(results, 'a') as f:
        f.write(line + '\n')
    return line


def commit_results(msg, results=None, retries=3, wait=1.0):
    """Commit results.jsonl ALONE, as the runner identity, no hooks; retried on a transient lock. Never pushes."""
    results = Path(results or RESULTS)
    for attempt in range(retries + 1):
        r = _git(results.parent, '-c', f'user.name={RUNNER_NAME}', '-c', f'user.email={RUNNER_EMAIL}', 'commit',
                 '--no-verify', '--only', '-m', msg, '--', results.name)
        if not r.returncode:
            return
        if attempt < retries:
            time.sleep(wait)
    raise Refused(f'could not commit {results.name}: {r.stderr.strip()[:300]} — stop; commit the row BY HAND, as it '
                  'is; (k) refuses every later fetch or read until it is committed')


HOOKS = {'commit': commit_results}


def in_real(p):
    p = Path(p).resolve()
    return p.is_relative_to(REAL_REPO) or REAL_REPO.is_relative_to(p)


def real_tree_failures(data_dirs=None):
    """The real results file and the real data tree only together, only with the default commit hook, only with the
    real guard."""
    real_r = Path(RESULTS).resolve() == REAL_RESULTS
    dirs = [Path(x).resolve() for x in (data_dirs or (HERE / KLINES_A_REL, HERE / STORE_REL))]
    real_d = any(d == r or d.is_relative_to(r) or r.is_relative_to(d) for d in dirs for r in REAL_DATA) or \
        any(in_real(d) for d in dirs)
    out = []
    if real_r != real_d:
        out.append('(M1) the real results.jsonl and the real data tree are used only together')
    if (real_r or real_d) and HOOKS.get('commit') is not commit_results:
        out.append('(M1) a non-default commit hook is never used on the real results file or data tree')
    if (real_r or real_d) and (guard is not _REAL_GUARD or _GUARD_FAILURES is not guard_failures):
        out.append('(M1) the real tree is opened only by the real guard')
    return out


def record(row, msg):
    """Append, commit through the hook, then REFUSE unless the line is in HEAD:./results.jsonl (checked with git).
    Rows go into the real repository only after the real guard, with the default hook."""
    res = Path(RESULTS).resolve()
    if in_real(res) and (not (_guard_ok('run') or _guard_ok('fetch')) or HOOKS.get('commit') is not commit_results
                         or res != REAL_RESULTS):
        raise Refused('(M1) rows are written into the real repository only by the real run, after the real guard, '
                      'with the default commit hook')
    line = append_row(row, RESULTS)
    HOOKS['commit'](msg, RESULTS)
    if line not in head_lines(Path(RESULTS).parent, Path(RESULTS).name):
        raise Refused(f'the {row.get("mode")} row is not in HEAD:./{Path(RESULTS).name} after the commit — stop')
    return line


# ------------------------------------------------------------------ the gated panel
def registered_real_hashes():
    """SHA-256s of the registered real data files, from the REAL repository's HEAD registration and the pins."""
    t = head_text(REAL_REPO / 'research' / 'alpha', REG_REL) if (REAL_REPO / 'research' / 'alpha').is_dir() else None
    return {h for f, h in HASH_ROW.findall(t or '') if f.startswith('data/')}


class Panel:
    """The data of the read. The real directories, or any byte copy of a registered real data file (review B m2), open
    only after the real guard passed AND run() issued the gate after the started row was committed in HEAD."""

    def __init__(self, mode, klines_a=None, store=None, table=None):
        a = Path(klines_a or _HERE0 / KLINES_A_REL).resolve()
        f = Path(store or _HERE0 / STORE_REL).resolve()
        sums = a / 'SHA256SUMS'
        digest = D.sha256_bytes(sums.read_bytes()) if sums.is_file() else None
        real = any(p == r or p.is_relative_to(r) or r.is_relative_to(p) for p in (a, f) for r in REAL_DATA) or \
            in_real(a) or in_real(f) or (digest is not None and digest in registered_real_hashes())
        if real and not _gate_ok():
            raise Refused('the real 016 data (or a byte copy of it) open only after the real guard passed and the '
                          'started row is committed, in this process')
        R.refuse_unless(mode == 'run', f'the panel is read only in mode run, not {mode}')
        self.a, self.f, self.table = a, f, table

    def _read_verified(self, path, want):
        blob = path.read_bytes()
        R.refuse_unless(D.sha256_bytes(blob) == want, f'{path.name} does not match its pinned SHA-256')
        return blob

    def load(self, static_exchange=None):
        """(syms, funding, non_coin, hl, counts). Span (a) and the warm-up from the scout's pull (each file checked
        against its pinned SHA256SUMS, itself checked against the registered table; months <= 2026-06; rows before
        2026-07-01), the forward span from the fetch manifest (months 2026-07 -> 2028-12, each file checked)."""
        before = R.day(R.END) + 1
        sums_blob = (self.a / 'SHA256SUMS').read_bytes()
        static_path = Path(static_exchange or _HERE0 / STATIC_EXCHANGE_REL)
        static_blob = static_path.read_bytes()
        if self.table is not None:                               # review B m8: hash the bytes that are used
            R.refuse_unless(D.sha256_bytes(sums_blob) == self.table.get(f'{KLINES_A_REL}/SHA256SUMS'),
                            'the pull SHA256SUMS differs from the registered table')
            R.refuse_unless(D.sha256_bytes(static_blob) == self.table.get(STATIC_EXCHANGE_REL),
                            'the static exchangeInfo differs from the registered table')
        sums = D.parse_sums(sums_blob.decode())
        man = json.loads((self.f / 'manifest.json').read_text())
        files = man['files']
        rows = {}
        counts = {'a_files': 0, 'f_files': 0, 'funding_files': 0, 'symbols': 0}
        for rel, want in sorted(sums.items()):
            fm = D.file_month(rel)
            R.refuse_unless(fm is not None and fm[0] == 'klines', f'unexpected file in the scout pull: {rel}')
            R.refuse_unless(fm[2] <= A_MONTH_LAST, f'the scout pull holds a month after {A_MONTH_LAST}: {rel}')
            part = D.parse_klines(D.zip_text(self._read_verified(self.a / rel, want)), before=R.day(R.SPAN_C[0]))
            rows.setdefault(fm[1], {}).update(part)
            counts['a_files'] += 1
        funding = {}
        for rel, want in sorted(files.items()):
            fm = D.file_month(rel)
            if fm is None:
                continue
            blob = self._read_verified(self.f / rel, want)
            if fm[0] == 'klines':
                R.refuse_unless(FWD_MONTHS[0] <= fm[2] <= FWD_MONTHS[1], f'forward klines outside the span: {rel}')
                part = D.parse_klines(D.zip_text(blob), not_before=R.day(R.SPAN_C[0]), before=before)
                cur = rows.setdefault(fm[1], {})
                R.refuse_unless(not (set(part) & set(cur)), f'overlapping klines days for {fm[1]}')
                cur.update(part)
                counts['f_files'] += 1
            else:
                R.refuse_unless(FUNDING_MONTHS[0] <= fm[2] <= FUNDING_MONTHS[1], f'funding outside the span: {rel}')
                part = D.parse_funding(D.zip_text(blob), not_before=R.day(R.START), before=before)
                cur = funding.setdefault(fm[1], {})
                for d_, v in part.items():
                    cur[d_] = cur.get(d_, 0.0) + v
                counts['funding_files'] += 1
        syms = {n: R.Sym(n, r) for n, r in rows.items() if r}
        counts['symbols'] = len(syms)
        R.refuse_unless('fapi_exchangeInfo.json' in files and 'hl_presence.json' in files,
                        'the fetch manifest lacks the exchangeInfo or the HL probe')
        non_coin, known_2026 = set(), set()
        for x in json.loads(static_blob).get('symbols', []):
            known_2026.add(x.get('symbol'))
            if x.get('contractType') != 'PERPETUAL' or x.get('underlyingType') != 'COIN':
                non_coin.add(x.get('symbol'))
        for x in json.loads(self._read_verified(self.f / 'fapi_exchangeInfo.json', files['fapi_exchangeInfo.json'])
                            ).get('symbols', []):
            # review B m6: the 2028 snapshot classifies only symbols the 2026 snapshot did not know
            if x.get('symbol') not in known_2026 and (x.get('contractType') != 'PERPETUAL'
                                                      or x.get('underlyingType') != 'COIN'):
                non_coin.add(x.get('symbol'))
        hl = json.loads(self._read_verified(self.f / 'hl_presence.json', files['hl_presence.json']))
        return syms, funding, frozenset(non_coin), hl, counts


# ------------------------------------------------------------------ the computation (after the gate)
def hl_coin(sym, hl):
    """The HL coin of a Binance symbol (reported block): BASE, kBASE for 1000BASE, or BASE without a 1000000 prefix."""
    base = sym[:-4]
    for c in (base, 'k' + base[4:] if base.startswith('1000') and not base.startswith('1000000') else None,
              base[7:] if base.startswith('1000000') else None):
        if c and c in hl:
            return c
    return None


def hl_universe_fn(ufn, hl):
    """Members listed on Hyperliquid at the month start (first funding before it and still funding at it)."""
    def fn(d):
        ms = (dt.datetime.combine(R.date_of(d + 1), dt.time(), dt.UTC).timestamp()) * 1000
        keep = []
        for s in ufn(d):
            c = hl_coin(s, hl)
            if c and hl[c].get('first') is not None and hl[c]['first'] <= ms and \
                    (hl[c].get('last') is None or hl[c]['last'] >= ms):
                keep.append(s)
        return keep
    return fn


def tail_stats(days, y, k=1.0):
    """Worst day, worst 7-day and 30-day compounded windows, max drawdown, at a gross multiplier k."""
    z = [k * r for r in y]
    eq, peak, mdd = 1.0, 1.0, 0.0
    for r in z:
        eq *= 1.0 + r
        peak = max(peak, eq)
        mdd = min(mdd, eq / peak - 1.0)

    def worst(w):
        best = 0.0
        for i in range(len(z) - w + 1):
            p = 1.0
            for r in z[i:i + w]:
                p *= 1.0 + r
            best = min(best, p - 1.0)
        return best
    return {'k': k, 'worst_day': min(z), 'worst_7d': worst(7), 'worst_30d': worst(30), 'max_drawdown': mdd}


PLACEBO_MARGIN = 400                                         # review A m6: offsets in [400, n - 400]


def placebo_syms(syms, rng):
    """Each symbol's Donchian states circularly shifted by an offset in [400, n - 400] traded days (flips and turnover
    kept, timing broken; never a near-identity lag); sigma stays on its own day."""
    out = {}
    for n, s in syms.items():
        t = R.Sym(n, s.rows)
        sig = s.signals('long')
        days = s.tdays
        if len(days) > 2 * PLACEBO_MARGIN:
            k = rng.randrange(PLACEBO_MARGIN, len(days) - PLACEBO_MARGIN)
            st = [sig[d] for d in days]
            t.sig = {d: (st[(i + k) % len(days)][0], st[(i + k) % len(days)][1], st[i][2]) for i, d in enumerate(days)}
        out[n] = t
    return out


def _pack(contrib):
    return [[[s, w, r] for s, w, r in day] for day in contrib]


def compute(syms, funding, non_coin, hl, placebo_seeds=PLACEBO_SEEDS):
    """Everything after the read_started row: the primary book, the timing statistic and the ladder, reported blocks."""
    a, b = R.day(R.START), R.day(R.END)
    prim = R.compute(syms, non_coin=non_coin)
    bk, ufn = prim['book'], prim['universe_fn']
    days, net = bk['days'], bk['ret']
    fac = [prim['F1'], prim['F2'], prim['F3']]
    dep = R.run_book(syms, a, b, ufn, cost=R.COST_DEPLOY, funding=funding)
    dec = S.decision_from_series(days, bk['contrib'], bk['cost'], dep['contrib'], dep['cost'])
    sa = [i for i, d in enumerate(days) if d <= R.day(R.SPAN_A[1])]
    sc = S.forward_idx(days)

    def sub(idx, y):
        dd = [days[i] for i in idx]
        return {'timing': S.timing(days, bk['contrib'], bk['cost'], idx),
                'alpha': S.alpha_fit(dd, [y[i] for i in idx], [[f[i] for i in idx] for f in fac]),
                'sharpe': S.sharpe(dd, [y[i] for i in idx])}

    def variant(**kw):
        v = R.run_book(syms, a, b, kw.pop('ufn', ufn), **kw)
        return {'timing': S.timing(days, v['contrib'], v['cost']), 'alpha': S.alpha_fit(days, v['ret'], fac),
                'sharpe': S.sharpe(days, v['ret']), 'turnover_per_year': v['turnover_per_year'], 'counts': v['counts']}
    nofund = R.run_book(syms, a, b, ufn, cost=R.COST_DEPLOY)
    rng = random.Random(16016)
    plac = []
    for _ in range(placebo_seeds):
        pr = R.run_book(placebo_syms(syms, rng), a, b, ufn)
        plac.append(S.timing(days, pr['contrib'], pr['cost'])['t'])
    t_act = dec['h1']['pooled']['t']
    vol = S.sharpe(days, net)['sd_month'] * 12 ** 0.5
    k25 = 0.25 / vol if vol else None
    month_ends = [m for m in range(a - 1, b) if R.is_month_end(m) or m == a - 1]
    reported = {
        'strata': {'a': sub(sa, net), 'c': sub(sc, net)},
        'factor_alpha_constant': S.alpha_fit(days, net, fac, per_cell=False),
        'factor_alpha_half_year_cells': S.alpha_fit(days, net, fac, per_cell=True),
        'factor_alpha_year_cells': S.alpha_fit(days, net, fac, per_cell=True, cells=R.YEAR_CELLS),
        'alpha_btc_only_constant': S.alpha_fit(days, net, [prim['F1']], per_cell=False),
        'raw_sharpe_vs_paper': S.raw_sharpe_vs_paper(days, net),
        'deployable_6bp_funding': {'timing': S.timing(days, dep['contrib'], dep['cost']),
                                   'alpha': S.alpha_fit(days, dep['ret'], fac), 'sharpe': S.sharpe(days, dep['ret']),
                                   'mean_annual': sum(dep['ret']) / len(dep['ret']) * 365},
        'funding_drag_annual': (sum(nofund['ret']) - sum(dep['ret'])) / len(days) * 365,
        'costs': {f'{c:.4f}': variant(cost=c) for c in (0.0, 0.0025, 0.005)},
        'relative_threshold': variant(relative=True),
        'one_close_lag': variant(lag=1),
        'long_short': variant(mode='long_short'),
        'hl_tradable_subbook': variant(ufn=hl_universe_fn(ufn, hl)) if hl else None,
        'tail_paper_sizing': tail_stats(days, net, 1.0),
        'tail_gross_x_k25': tail_stats(days, net, k25) if k25 else None,
        'gross_exposure': {'mean': sum(bk['gross']) / len(bk['gross']), 'max': max(bk['gross'])},
        'turnover_per_year': bk['turnover_per_year'],
        'age_excluded_by_month': {R.month_of(m + 1): R.age_excluded(syms, m, non_coin) for m in month_ends},
        'placebo_shifted_signals': {'seeds': placebo_seeds, 'offsets': f'[{PLACEBO_MARGIN}, n - {PLACEBO_MARGIN}]',
                                    'share_t_at_or_above_actual': sum(1 for t in plac if t >= t_act) / len(plac),
                                    'share_t_at_or_above_crit': sum(1 for t in plac if t >= dec['h1']['crit']) / len(plac)},
    }
    series = {'days': [R.date_of(d).isoformat() for d in days], 'contrib': _pack(bk['contrib']), 'cost': bk['cost'],
              'dep_contrib': _pack(dep['contrib']), 'dep_cost': dep['cost'], 'net': net}
    return dec, reported, series, bk


def _row(base, dec, reported, series, bk, counts):
    return {**base, 'mode': 'read', 'run_at': dt.datetime.now(dt.UTC).isoformat(), 'env': env_fingerprint(),
            'code_hashes': hashes(_HERE0, GUARDED_CODE), 'counts': counts, 'book_counts': bk['counts'],
            'members': bk['members'],
            'params': {'span_a': [str(x) for x in R.SPAN_A], 'span_c': [str(x) for x in R.SPAN_C],
                       'cost': R.COST_PAPER, 'cost_deploy': R.COST_DEPLOY, 'cal_h1': S.CAL_H1, 'c2': S.C2,
                       't_forward_min': S.T_FWD},
            'h1': dec['h1'], 'outcome': dec['outcome'], 'economic': dec['economic'], 'route': dec['route'],
            'h1_t': dec['h1']['pooled']['t'], 'h1_t_forward': (dec['h1']['forward'] or {}).get('t'),
            'reported': reported, 'series': series}


def run():
    """The read, once, on the real tree only (no argument)."""
    global _GATE
    f = real_tree_failures()
    if f:
        raise Refused('; '.join(f))
    ctx = guard('run')
    f = real_tree_failures()
    if f:
        raise Refused('; '.join(f))
    restarts = sum(1 for _, r in result_rows(_HERE0)[0] if r.get('mode') == 'read_started')
    base = {'registration': REG_NAME, 'registration_commit': ctx['commit'], 'reg0': ctx['reg0'],
            'fetched_row_sha256': ctx['fetched_row_sha256']}
    sline = record({**base, 'mode': 'read_started', 'restart_number': restarts, 'env': env_fingerprint(),
                    'code_hashes': hashes(_HERE0, GUARDED_CODE), 'run_at': dt.datetime.now(dt.UTC).isoformat()},
                   '016: read_started (automatic, r016_run.py)')
    if not (_guard_ok('run') and sline in head_lines(REAL_RESULTS.parent, REAL_RESULTS.name)):
        raise Refused('the read_started row is not in HEAD of the real ledger — the data stay closed')
    _GATE = _GUARD_PASSED                                       # the data gate opens only now (review B M4)
    try:
        syms, funding, non_coin, hl, counts = Panel('run', table=ctx['table']).load()
        print('coverage before the read:', json.dumps(counts))
        dec, reported, series, bk = compute(syms, funding, non_coin, hl)
        row = _row(base, dec, reported, series, bk, counts)
        fails, _ = S.recheck(json.loads(json.dumps(row, default=str)))
        if fails:
            row['recheck_failed'] = fails
    except Exception as e:
        record({**base, 'mode': 'read_failed', 'error': repr(e)[:500], 'run_at': dt.datetime.now(dt.UTC).isoformat()},
               '016: read_failed (automatic)')
        raise
    record(row, '016: read row (automatic, r016_run.py)')
    if row.get('recheck_failed'):
        raise SystemExit('internal: the decision does not recompute from its stored records; stored with '
                         'recheck_failed and not printed. The orchestrator ledgers it.')
    print(json.dumps({'h1': {k: row['h1'][k] for k in ('crit', 'floor', 'pass_t', 'pass_forward', 'pass')},
                      'tA': row['h1']['pooled'], 'tA_forward': row['h1']['forward'], 'c2': S.C2,
                      'economic': row['economic']}, indent=1, default=str))
    print('outcome:', row['outcome'], 'route:', row['route'])
    return row


# ------------------------------------------------------------------ synthetic size, calibration and power
def power(out_dir=None):
    """Reads no data. Writes data/r016/logs/power.json (per-world summaries) and power_records.json (per-draw records).
    Resumable: each finished world is kept in data/r016/logs/worlds/ (keyed by spec, draws, seeds) and not re-run."""
    import multiprocessing as mp
    from ct16 import sim
    out_dir = Path(out_dir or HERE / 'data' / 'r016' / 'logs')
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = []

    def log(r):
        rows.append(r)
        print(json.dumps(r), flush=True)
    with mp.Pool(min(10, os.cpu_count() or 1)) as pool:
        keep = sim.power_run(pool, log, cache_dir=out_dir / 'worlds')      # resumable: one file per finished world
    (out_dir / 'power_records.json').write_text(json.dumps(keep))
    (out_dir / 'power.json').write_text(json.dumps(rows, indent=1))
    return rows


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--check', action='store_true')
    g.add_argument('--fingerprint', action='store_true')
    g.add_argument('--run', action='store_true')
    g.add_argument('--power', action='store_true')
    args = ap.parse_args()
    if args.check:
        text = head_text(_HERE0, REG_REL)
        real = bound_repo(text)
        if REPO.resolve() != real.resolve():
            print(f'(w) this runner lives in {REPO}, not in the real repository {real}')
        failures, ctx = guard_failures(_HERE0, 'run', real_repo=real, anchor=ANCHOR, env_check=True)
        print('\n'.join(failures) if failures else f"guard open: REGISTERED commit {ctx['commit']}")
    elif args.fingerprint:
        print(json.dumps(env_fingerprint(), indent=1))
    elif args.run:
        run()
    else:
        power()


if __name__ == '__main__':
    main()
