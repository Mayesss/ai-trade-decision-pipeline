"""Registration 016 — crypto trend following, the Zarattini-Pagani-Barbon (2025) rule verbatim: the guard, the one
read, and the synthetic power run.

    research/copytrade/.venv/bin/python -I r016_run.py --check    # every reason the read may not start; computes nothing
    research/copytrade/.venv/bin/python -I r016_run.py --run      # the read, once (after r016_fetch.py and the approval)
    research/copytrade/.venv/bin/python -I r016_run.py --power    # SYNTHETIC size, calibration and power; reads no data

The read refuses unless (and `r016_fetch.py` refuses unless the same checks in mode 'fetch' pass):
  (s)  the registration says REGISTERED; (a) it is tracked; (b) unmodified against HEAD; (c) it has a commit;
  (d)  the guarded code is clean, and no untracked, ignored or modified importable file (*.py, *.pyc, *.so, *.pyd,
       a package) sits at the top level of research/alpha (sys.path[0]) or in ct16/;
  (e)  code and data match the registration's SHA-256 table, each file listed exactly once;
  (t)  today (UTC) is on or after 2028-01-08 (the registered end 2027-12-31 + 7 days);
  (k)  results.jsonl is tracked and clean against HEAD;
  (w)  git's top level is this repository and the anchor commit is an ancestor of HEAD; every git call runs with
       the GIT_* variables removed;
  (v)  `python -O` is refused; the Python version equals the pin; ct16 is loaded from its hashed sources and no
       bytecode from an existing __pycache__ (a fresh sys.pycache_prefix outside the repository);
  (f)  fetch: no 016 `fetched`, `read_started`, `read` or `read_failed` row anywhere (working tree, every branch, the
       reflog, merge and stash diffs: `git log --all --reflog -m -p --text --no-textconv --no-ext-diff`);
  read: (g1) exactly one `fetch_started` and one `fetched` row exist anywhere, both committed in HEAD, with the same
       registration commit, an ancestor of HEAD; (g2) since that commit the guarded code is unchanged and the
       registration changed only by added approval or restart lines; the fetched row's code hashes and environment
       equal today's; (g3) the fetch manifest's files hash to the digest stored in the fetched row;
       (h') a committed line `016 read: APPROVED <date> row <sha256 of the fetched row line>` added strictly after
       the commit that added the fetched row; (i) no `read` or `read_failed` row anywhere, and a `read_started` row
       blocks unless a later committed line `016 read restart: APPROVED <date> started-row <sha256>` names it.
Reads: the gated `Panel` opens the real data only when the real `guard()` passed for the same mode in this process
(a gate token; a refusal, never an assert). The runner commits results.jsonl ALONE after each row, then refuses
unless the line is in HEAD:./results.jsonl (checked with the real git). It never pushes; the owner pushes.
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
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.pycache_prefix = tempfile.mkdtemp(prefix='r016-pyc-')   # never load bytecode from an existing __pycache__
sys.path.insert(0, str(HERE))
from ct16 import data as D  # noqa: E402
from ct16 import rule as R  # noqa: E402
from ct16 import stats as S  # noqa: E402

Refused = R.Refused
REPO = HERE.parent.parent
REG_NAME = '016-crypto-trend-verbatim.md'
REG_REL = f'registrations/{REG_NAME}'
REG_PREFIX = '016-'
RESULTS_REL = 'results.jsonl'
KLINES_A_REL = 'data/scout_c1/bn_um_klines_1d_monthly'      # the C1 scout's pull (months <= 2026-06), pinned
STORE_REL = 'data/r016/forward'                              # the guarded fetch (klines 2026-07 ->, all funding)
STATIC_EXCHANGE_REL = 'data/r016/static/fapi_exchangeInfo_20261011.json'
REAL_DATA = ((HERE / KLINES_A_REL).resolve(), (HERE / STORE_REL).resolve())
ANCHOR = '9c131e222d99a93d41596a2ba3a3066b437f142e'           # the C1 scout and 000t (2026-10-13)
PINS = {'python': '3.14.8'}
GUARDED_CODE = ('ct16/__init__.py', 'ct16/rule.py', 'ct16/stats.py', 'ct16/data.py', 'ct16/sim.py', 'r016_run.py',
                'r016_fetch.py')
GUARDED_DATA = (f'{KLINES_A_REL}/SHA256SUMS', STATIC_EXCHANGE_REL)
LOADED = (('ct16', 'ct16/__init__.py'), ('ct16.rule', 'ct16/rule.py'), ('ct16.stats', 'ct16/stats.py'),
          ('ct16.data', 'ct16/data.py'))
A_MONTH_LAST = '2026-06'                                     # the scout pull's last month (the programme's cut)
FWD_MONTHS = ('2026-07', '2027-12')                          # klines fetched by r016_fetch.py
FUNDING_MONTHS = ('2025-03', '2027-12')                      # funding fetched by r016_fetch.py (the test span)

HASH_ROW = re.compile(r'^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|', re.M)
STATUS = re.compile(r'^\*\*Status: (\w+)', re.M)
APPROVAL = re.compile(r'^016 read: APPROVED (\d{4}-\d{2}-\d{2}) row ([0-9a-f]{64})\s*$', re.M)
RESTART = re.compile(r'^016 read restart: APPROVED (\d{4}-\d{2}-\d{2}) started-row ([0-9a-f]{64})\s*$', re.M)
_GATE = None                                                 # (HERE, mode, registration commit), set only by guard()
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


def _git(root, *a):
    env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_')}
    return subprocess.run(['git', *a], cwd=root, capture_output=True, text=True, env=env)


def is_016(row):
    return isinstance(row, dict) and str(row.get('registration', '')).startswith(REG_PREFIX)


def result_rows(root, results_rel=RESULTS_REL):
    """[(line, row)] of every distinct 016 line in the working tree and every ADDED line of every branch, the reflog,
    merge and stash diffs. For BLOCKING checks: a deleted, reset, amended or stashed row still blocks."""
    texts = []
    res = Path(root) / results_rel
    if res.exists():
        texts.extend(res.read_text().splitlines())
    hist = _git(root, 'log', '--all', '--reflog', '-m', '-p', '--text', '--no-textconv', '--no-ext-diff', '--no-color',
                '--format=', '--', results_rel)
    texts.extend(l[1:] for l in hist.stdout.splitlines() if l.startswith('+') and not l.startswith('+++'))
    out, seen = [], set()
    for line in texts:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if is_016(row) and line not in seen:
            seen.add(line)
            out.append((line, row))
    return out


def head_lines(root, results_rel=RESULTS_REL):
    """HEAD's version of results.jsonl. `HEAD:./<path>` resolves from cwd (`HEAD:<path>` would resolve from the
    repository root and never find research/alpha/results.jsonl: 014 review B, B1)."""
    return set(_git(root, 'show', '--no-textconv', f'HEAD:./{results_rel}').stdout.splitlines())


def first_commit_containing(root, rel, needle):
    for c in _git(root, 'log', '--format=%H', '--reverse', '--', rel).stdout.split():
        if needle in _git(root, 'show', '--no-textconv', f'{c}:./{rel}').stdout.splitlines():
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
    return {f: sha256(Path(root) / f) for f in files if (Path(root) / f).exists()}


def manifest_digest(files_to_sha):
    body = '\n'.join(f'{k}\t{v}' for k, v in sorted(files_to_sha.items()))
    return hashlib.sha256(body.encode()).hexdigest()


# ------------------------------------------------------------------ (v) environment
def versions():
    return {'python': '.'.join(map(str, sys.version_info[:3]))}


def env_fingerprint():
    """The decision path is standard-library only: the Python version string, implementation and flags."""
    return {'python': versions()['python'], 'version': sys.version, 'implementation': sys.implementation.name,
            'optimize': sys.flags.optimize}


def shadow_modules(root):
    """Anything importable that is not a tracked, unmodified file and could shadow a module the read imports, scanned
    on the FILE SYSTEM (git-ignored files count) at the top level of research/alpha (sys.path[0]) and in ct16/:
    *.py, *.pyc (sourceless bytecode outside __pycache__), *.so / *.pyd, and packages (a new package's __init__.py;
    every file of a package inside ct16/)."""
    root = Path(root)
    tracked = set(_git(root, 'ls-files', '-z').stdout.split('\0'))
    modified = set(_git(root, 'ls-files', '-m', '-z').stdout.split('\0'))
    bad = set()

    def unclean(f):
        rel = str(f.relative_to(root))
        return rel not in tracked or rel in modified
    for base, inside in ((root, False), (root / 'ct16', True)):
        if not base.is_dir():
            continue
        for p in sorted(base.iterdir()):
            if p.name == '__pycache__':
                continue
            if p.is_file() and p.suffix in ('.py', '.pyc', '.so', '.pyd'):
                if unclean(p):
                    bad.add(str(p.relative_to(root)))
            elif p.is_dir() and (p / '__init__.py').exists():
                files = [f for f in p.rglob('*') if f.is_file() and '__pycache__' not in f.parts] if inside \
                    else [p / '__init__.py']
                bad.update(str(f.relative_to(root)) for f in files if unclean(f))
    return sorted(bad)


def loaded_from_source():
    out = []
    for name, rel in LOADED:
        mod = sys.modules.get(name)
        if mod is None or Path(getattr(mod, '__file__', '') or '').resolve() != (HERE / rel).resolve():
            out.append(f'(v) {name} was not loaded from {rel}')
    if not sys.pycache_prefix or Path(sys.pycache_prefix).resolve().is_relative_to(REPO):
        out.append('(v) sys.pycache_prefix must be a fresh directory outside the repository')
    return out


def environment_failures(root):
    out = []
    if sys.flags.optimize:
        out.append('(v) python -O / -OO is refused')
    if versions() != PINS:
        out.append(f'(v) python {versions()} differs from the pin {PINS}')
    out += loaded_from_source()
    return out


# ------------------------------------------------------------------ the guard
def guard_failures(root=HERE, mode='run', now=None, code=GUARDED_CODE, data=GUARDED_DATA, reg_rel=REG_REL,
                   results_rel=RESULTS_REL, store_rel=STORE_REL, toplevel=None, anchor=None, env_check=False):
    """([reasons], context): the fetch or read may start only when the list is empty. mode: fetch | run."""
    R.refuse_unless(mode in ('fetch', 'run'), f'unknown mode {mode}')
    root = Path(root)
    reg = root / reg_rel
    out = []
    text = reg.read_text() if reg.exists() else ''
    m = STATUS.search(text)
    if not m or m.group(1) != 'REGISTERED':
        out.append(f'(s) {reg.name} is not REGISTERED — refusing to fetch or compute anything.')
    if _git(root, 'ls-files', '--error-unmatch', '--', reg_rel).returncode:
        out.append(f'(a) {reg.name} is not tracked by git')
    if _git(root, 'diff', '--quiet', '--no-textconv', '--no-ext-diff', 'HEAD', '--', reg_rel).returncode:
        out.append(f'(b) {reg.name} differs from the committed version')
    commit = _git(root, 'log', '-1', '--format=%H', '--', reg_rel).stdout.strip()
    if not commit:
        out.append(f'(c) {reg.name} has no commit')
    dirty = _git(root, 'status', '--porcelain', '--', *code).stdout.strip() if code else ''
    if dirty:
        out.append(f'(d) guarded code not clean: {dirty.splitlines()[:6]}')
    shadow = shadow_modules(root)
    if shadow:
        out.append(f'(d) untracked, ignored or modified importable files could shadow an import: {shadow[:6]}')
    rows_tbl = HASH_ROW.findall(text)
    table = dict(rows_tbl)
    dup = sorted({f for f, _ in rows_tbl if sum(1 for g, _ in rows_tbl if g == f) > 1})
    if dup:
        out.append(f'(e) files listed more than once in the hash table: {dup}')
    for f in (*code, *data):
        want = table.get(f)
        if want is None:
            out.append(f'(e) {f} missing from the registration hash table')
        elif not (root / f).exists() or sha256(root / f) != want:
            out.append(f'(e) {f} does not match its registered SHA-256')
    now = now or dt.datetime.now(dt.UTC)
    if now.date() < R.READ_NOT_BEFORE:
        out.append(f'(t) the forward span ends {R.END}; nothing is fetched or read before {R.READ_NOT_BEFORE} (UTC)')
    if _git(root, 'ls-files', '--error-unmatch', '--', results_rel).returncode or \
            _git(root, 'diff', '--quiet', '--no-textconv', '--no-ext-diff', 'HEAD', '--', results_rel).returncode:
        out.append(f'(k) {results_rel} is untracked or differs from HEAD — every 016 row is committed when written')
    if toplevel is not None:
        top = _git(root, 'rev-parse', '--show-toplevel').stdout.strip()
        if not top or Path(top).resolve() != Path(toplevel).resolve():
            out.append(f'(w) git top level is {top!r}, not {toplevel}')
    if anchor is not None and not is_ancestor(root, anchor):
        out.append(f'(w) the anchor commit {anchor} is not an ancestor of HEAD')
    if env_check:
        out += environment_failures(root)
    rows = result_rows(root, results_rel)
    heads = head_lines(root, results_rel)
    ctx = {'commit': commit}
    if any(r.get('mode') in ('read', 'read_failed') for _, r in rows):
        out.append('(i) a 016 read or read_failed row exists (tree, any branch, reflog or stash) — the read runs once')
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
                out.append('(f) the existing fetch_started row has another registration commit or is not committed')
        ctx['resume'] = bool(fetch_started)
        return out, ctx
    for sl0, _ in started_reads:
        rs = [a for a in RESTART.finditer(text) if a.group(2) == line_sha(sl0)]
        if not rs or sl0 not in heads or not committed_strictly_after(root, results_rel, sl0, reg_rel,
                                                                      rs[0].group(0).rstrip()):
            out.append("(i) a read_started row exists without a committed, later "
                       "'016 read restart: APPROVED <date> started-row <sha256>' line — the read runs once")
    if len(fetch_started) != 1 or len(fetched) != 1:
        out.append(f'(g1) {len(fetch_started)} fetch_started and {len(fetched)} fetched rows; exactly one of each is '
                   'required (fetch first)')
        return out, ctx
    (sl, sr), (fl, fr) = fetch_started[0], fetched[0]
    if sl not in heads or fl not in heads:
        out.append('(g1) the fetch rows are not committed in HEAD:./results.jsonl')
    rc = fr.get('registration_commit') or ''
    if sr.get('registration_commit') != rc:
        out.append('(g1) the two fetch rows carry different registration commits')
    if not is_ancestor(root, rc):
        out.append("(g1) the fetched row's registration commit is not an ancestor of HEAD")
    else:
        if code and _git(root, 'diff', '--quiet', '--no-textconv', '--no-ext-diff', rc, 'HEAD', '--', *code).returncode:
            out.append('(g2) the guarded code changed since the fetch')
        changed = [l[1:] for l in _git(root, 'diff', '-U0', '--no-color', '--text', '--no-textconv', '--no-ext-diff',
                                       rc, 'HEAD', '--', reg_rel).stdout.splitlines()
                   if l[:1] in '+-' and not l.startswith(('+++', '---'))]
        if any(l.strip() and not (APPROVAL.fullmatch(l.rstrip()) or RESTART.fullmatch(l.rstrip())) for l in changed):
            out.append('(r) the registration changed since the fetch beyond added approval or restart lines')
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
        now_h = {f: (sha256(root / store_rel / f) if (root / store_rel / f).exists() else None) for f in files}
        if manifest_digest(now_h) != fr.get('manifest_digest') or manifest_digest(files) != fr.get('manifest_digest'):
            out.append('(g3) the fetched data do not hash to the digest stored in the fetched row')
    appr = [a for a in APPROVAL.finditer(text) if a.group(2) == line_sha(fl)]
    if not appr:
        out.append("(h') no committed line '016 read: APPROVED <date> row <sha256 of the fetched row>'")
    elif not committed_strictly_after(root, results_rel, fl, reg_rel, appr[0].group(0).rstrip()):
        out.append("(h') the approval must be committed strictly after the commit that added the fetched row")
    ctx.update(fetch_commit=rc, fetched_row_sha256=line_sha(fl), fetched_row=fr)
    return out, ctx


def guard(mode):
    """The ONLY way to open the real data: the real repository, default arguments, every check. Sets the gate."""
    global _GATE
    _GATE = None
    failures, ctx = guard_failures(HERE, mode, toplevel=REPO, anchor=ANCHOR, env_check=True)
    if failures:
        raise SystemExit('\n'.join(failures))
    _GATE = (str(HERE), mode, ctx['commit'])
    return ctx


_REAL_GUARD = guard
RESULTS = HERE / RESULTS_REL
REAL_RESULTS = RESULTS.resolve()


def append_row(row, results=None):
    line = json.dumps(row, default=str, sort_keys=True)
    with open(results or RESULTS, 'a') as f:
        f.write(line + '\n')
    return line


def commit_results(msg):
    """Commit results.jsonl ALONE (`git commit --only`). Never pushes. On failure: stop, commit BY HAND as it is."""
    r = _git(RESULTS.parent, 'commit', '--only', '-m', msg, '--', RESULTS.name)
    if r.returncode:
        raise Refused(f'could not commit {RESULTS.name}: {r.stderr.strip()[:300]} — stop; commit the row BY HAND as it '
                      'is; (k) refuses every later fetch or read until it is committed')


HOOKS = {'commit': commit_results}


def real_tree_failures(data_dirs=None):
    """The real results file and the real data tree only together, only with the default commit hook, only with the
    real guard."""
    real_r = RESULTS.resolve() == REAL_RESULTS
    dirs = [Path(x).resolve() for x in (data_dirs or (HERE / KLINES_A_REL, HERE / STORE_REL))]
    real_d = any(any(d == r or d.is_relative_to(r) or r.is_relative_to(d) for r in REAL_DATA) for d in dirs)
    out = []
    if real_r != real_d:
        out.append('(M4) the real results.jsonl and the real data tree are used only together')
    if (real_r or real_d) and HOOKS.get('commit') is not commit_results:
        out.append('(M4) a non-default commit hook is never used on the real results file or data tree')
    if (real_r or real_d) and guard is not _REAL_GUARD:
        out.append('(M4) the real tree is opened only by the real guard')
    return out


def record(row, msg):
    """Append a row, commit it through the hook, then REFUSE unless the line is in HEAD:./results.jsonl (real git)."""
    line = append_row(row)
    HOOKS['commit'](msg)
    if line not in set(_git(RESULTS.parent, 'show', '--no-textconv', f'HEAD:./{RESULTS.name}').stdout.splitlines()):
        raise Refused(f'the {row.get("mode")} row is not in HEAD:./{RESULTS.name} after the commit — stop')
    return line


# ------------------------------------------------------------------ the gated panel
class Panel:
    """The data of the read. The real directories open only under the gate set by `guard()` for the same mode in this
    process; synthetic panels (tests) pass their own directories outside the real ones."""

    def __init__(self, mode, klines_a=None, store=None):
        a = Path(klines_a or HERE / KLINES_A_REL).resolve()
        f = Path(store or HERE / STORE_REL).resolve()
        for p in (a, f):
            if any(p == r or p.is_relative_to(r) or r.is_relative_to(p) for r in REAL_DATA):
                if _GATE is None or _GATE[0] != str(HERE) or _GATE[1] != mode:
                    raise Refused('the real 016 data open only after guard() passed for this mode in this process')
        R.refuse_unless(mode == 'run', f'the panel is read only in mode run, not {mode}')
        self.a, self.f = a, f

    def _read_verified(self, path, want):
        blob = path.read_bytes()
        R.refuse_unless(D.sha256_bytes(blob) == want, f'{path.name} does not match its pinned SHA-256')
        return blob

    def load(self, static_exchange=None):
        """(syms, funding, non_coin, hl, counts). Span (a) and the warm-up come from the scout's pull (each file checked
        against its pinned SHA256SUMS; months <= 2026-06), the forward span from the fetch manifest (months 2026-07 ->
        2027-12, each file checked against the manifest). Rows dated on or after 2028-01-01 are dropped."""
        before = R.day(R.END) + 1
        sums = D.parse_sums((self.a / 'SHA256SUMS').read_text())
        man = json.loads((self.f / 'manifest.json').read_text())
        files = man['files']
        rows = {}
        counts = {'a_files': 0, 'f_files': 0, 'funding_files': 0, 'symbols': 0}
        for rel, want in sorted(sums.items()):
            fm = D.file_month(rel)
            R.refuse_unless(fm is not None and fm[0] == 'klines', f'unexpected file in the scout pull: {rel}')
            if fm[2] > A_MONTH_LAST:
                raise Refused(f'the scout pull holds a month after {A_MONTH_LAST}: {rel}')
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
        non_coin = set()
        for blob in (Path(static_exchange or HERE / STATIC_EXCHANGE_REL).read_bytes(),
                     self._read_verified(self.f / 'fapi_exchangeInfo.json', files['fapi_exchangeInfo.json'])):
            for x in json.loads(blob).get('symbols', []):
                if x.get('contractType') != 'PERPETUAL' or x.get('underlyingType') != 'COIN':
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
    """Worst day, worst 7-day and 30-day compounded windows, max drawdown, at scale k (weights x k)."""
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


def placebo_syms(syms, rng):
    """Each symbol's Donchian states circularly shifted by a random offset over its traded days (flip counts and
    turnover kept, timing broken); sigma stays on its own day."""
    out = {}
    for n, s in syms.items():
        t = R.Sym(n, s.rows)
        sig = s.signals('long')
        days = s.tdays
        if days:
            k = rng.randrange(len(days))
            st = [sig[d] for d in days]
            t.sig = {d: (st[(i + k) % len(days)][0], st[(i + k) % len(days)][1], st[i][2]) for i, d in enumerate(days)}
        out[n] = t
    return out


def compute(syms, funding, non_coin, hl, placebo_seeds=PLACEBO_SEEDS):
    """Everything after the read_started row: the primary book and factors, the decision, the reported blocks."""
    a, b = R.day(R.START), R.day(R.END)
    prim = R.compute(syms, non_coin=non_coin)
    bk, ufn = prim['book'], prim['universe_fn']
    days, net = bk['days'], bk['ret']
    fac = [prim['F1'], prim['F2'], prim['F3']]
    dep = R.run_book(syms, a, b, ufn, cost=R.COST_DEPLOY, funding=funding)
    dec = S.decision_from_series(days, net, fac, days, dep['ret'])
    sa = [i for i, d in enumerate(days) if d <= R.day(R.SPAN_A[1])]
    sc = [i for i, d in enumerate(days) if d >= R.day(R.SPAN_C[0])]

    def sub(idx, y):
        dd = [days[i] for i in idx]
        yy = [y[i] for i in idx]
        return {'alpha': S.alpha_fit(dd, yy, [[f[i] for i in idx] for f in fac]), 'sharpe': S.sharpe(dd, yy)}

    def variant(**kw):
        v = R.run_book(syms, a, b, kw.pop('ufn', ufn), **kw)
        return {'alpha': S.alpha_fit(days, v['ret'], fac), 'sharpe': S.sharpe(days, v['ret']),
                'turnover_per_year': v['turnover_per_year'], 'counts': v['counts']}
    nofund = R.run_book(syms, a, b, ufn, cost=R.COST_DEPLOY)
    rng = random.Random(16016)
    plac = []
    for _ in range(placebo_seeds):
        ps = placebo_syms(syms, rng)
        pr = R.run_book(ps, a, b, ufn)
        plac.append(S.alpha_fit(days, pr['ret'], fac)['t'])
    t_act = dec['h1']['pooled']['t']
    vol = S.sharpe(days, net)['sd_month'] * 12 ** 0.5
    k25 = 0.25 / vol if vol else None
    reported = {
        'strata': {'a': sub(sa, net), 'c': sub(sc, net)},
        'alpha_half_year_cells': S.alpha_fit(days, net, fac, per_cell=True),
        'alpha_year_cells': S.alpha_fit(days, net, fac, per_cell=True, cells=R.YEAR_CELLS),
        'alpha_btc_only_constant': S.alpha_fit(days, net, [prim['F1']], per_cell=False),
        'deployable_6bp_funding': {'alpha': S.alpha_fit(days, dep['ret'], fac), 'sharpe': S.sharpe(days, dep['ret']),
                                   'mean_annual': sum(dep['ret']) / len(dep['ret']) * 365},
        'funding_drag_annual': (sum(nofund['ret']) - sum(dep['ret'])) / len(days) * 365,
        'costs': {f'{c:.4f}': variant(cost=c) for c in (0.0, 0.0025, 0.005)},
        'relative_threshold': variant(relative=True),
        'one_close_lag': variant(lag=1),
        'long_short': variant(mode='long_short'),
        'hl_tradable_subbook': variant(ufn=hl_universe_fn(ufn, hl)) if hl else None,
        'tail_paper_sizing': tail_stats(days, net, 1.0),
        'tail_25pct_vol': tail_stats(days, net, k25) if k25 else None,
        'gross_exposure': {'mean': sum(bk['gross']) / len(bk['gross']), 'max': max(bk['gross'])},
        'turnover_per_year': bk['turnover_per_year'],
        'placebo_shifted_signals': {'seeds': placebo_seeds,
                                    'share_t_at_or_above_actual': sum(1 for t in plac if t >= t_act) / len(plac),
                                    'share_t_at_or_above_crit': sum(1 for t in plac if t >= dec['h1']['crit']) / len(plac)},
    }
    series = {'days': [R.date_of(d).isoformat() for d in days], 'net': net, 'F1': fac[0], 'F2': fac[1], 'F3': fac[2],
              'deploy': dep['ret']}
    return dec, reported, series, bk


def _strip(dec):
    h1 = dict(dec['h1'])
    h1['pooled'] = {k: v for k, v in h1['pooled'].items() if k != 'slopes'}
    if h1.get('forward'):
        h1['forward'] = {k: v for k, v in h1['forward'].items() if k != 'slopes'}
    return h1


def _refuse_real_tree():
    f = real_tree_failures()
    if f:
        raise Refused('; '.join(f))


def run():
    """The read, once."""
    _refuse_real_tree()
    ctx = guard('run')
    _refuse_real_tree()
    panel = Panel('run')
    syms, funding, non_coin, hl, counts = panel.load()
    ufn = R.universe_fn_for(syms, non_coin)
    members = {R.month_of(m + 1): len(ufn(m)) for m in range(R.day(R.START) - 1, R.day(R.END))
               if R.is_month_end(m) or m == R.day(R.START) - 1}
    counts['months_with_fewer_than_20_members'] = sum(1 for v in members.values() if v < R.B)
    print('coverage before the read:', json.dumps(counts))
    restarts = sum(1 for _, r in result_rows(HERE) if r.get('mode') == 'read_started')
    base = {'registration': REG_NAME, 'registration_commit': ctx['commit'], 'fetch_commit': ctx['fetch_commit'],
            'fetched_row_sha256': ctx['fetched_row_sha256']}
    record({**base, 'mode': 'read_started', 'restart_number': restarts, 'env': env_fingerprint(),
            'code_hashes': hashes(HERE, GUARDED_CODE), 'run_at': dt.datetime.now(dt.UTC).isoformat()},
           '016: read_started (automatic, r016_run.py)')
    try:
        dec, reported, series, bk = compute(syms, funding, non_coin, hl)
    except Exception as e:
        record({**base, 'mode': 'read_failed', 'error': repr(e)[:500], 'run_at': dt.datetime.now(dt.UTC).isoformat()},
               '016: read_failed (automatic)')
        raise
    row = {**base, 'mode': 'read', 'run_at': dt.datetime.now(dt.UTC).isoformat(), 'env': env_fingerprint(),
           'code_hashes': hashes(HERE, GUARDED_CODE), 'data_hashes': hashes(HERE, GUARDED_DATA), 'counts': counts,
           'book_counts': bk['counts'], 'members': bk['members'],
           'params': {'span_a': [str(x) for x in R.SPAN_A], 'span_c': [str(x) for x in R.SPAN_C],
                      'cells': [[str(a), str(b)] for a, b in R.CELLS], 'cost': R.COST_PAPER, 'cost_deploy': R.COST_DEPLOY,
                      'cal_h1': S.CAL_H1, 'cal_h2': S.CAL_H2, 'sr_paper': S.SR_PAPER, 'k_proposed': S.K_PROPOSED},
           'h1': _strip(dec), 'h2': dec['h2'], 'outcome': dec['outcome'], 'economic': dec['economic'],
           'route': dec['route'], 'h1_t': dec['h1']['pooled']['t'], 'h2_T': dec['h2']['T'],
           'reported': reported, 'series': series}
    fails, _ = S.recheck(json.loads(json.dumps(row, default=str)))
    if fails:
        row['recheck_failed'] = fails
        record(row, '016: read row, RECHECK FAILED (automatic)')
        raise SystemExit('internal: the decision does not recompute from its stored series. The row is stored with '
                         'recheck_failed; nothing is printed. The orchestrator ledgers it.')
    record(row, '016: read row (automatic, r016_run.py)')
    print(json.dumps({'h1': {k: row['h1'][k] for k in ('crit', 'floor', 'pass_t', 'pass_forward_sign', 'pass')},
                      'h1_alpha': row['h1']['pooled'], 'h2': row['h2'], 'economic': row['economic']}, indent=1,
                     default=str))
    print('outcome:', row['outcome'], 'route:', row['route'])
    return row


# ------------------------------------------------------------------ synthetic size, calibration and power
def power(draws_cal=600, draws_trend=400, out_dir=None):
    """Reads no data. Writes data/r016/logs/power.json (summaries) and power_records.json (per-draw records)."""
    import multiprocessing as mp
    from ct16 import sim
    out_dir = Path(out_dir or HERE / 'data' / 'r016' / 'logs')
    out_dir.mkdir(parents=True, exist_ok=True)
    keep = {}
    with mp.Pool(min(10, os.cpu_count() or 1)) as pool:
        cal = sim.run_set(sim.CALIBRATION, draws_cal, pool, 7, log=lambda r: print(json.dumps(r), flush=True), keep=keep)
        other = sim.run_set(sim.HINDSIGHT + sim.MISALIGNED, draws_cal, pool, 8,
                            log=lambda r: print(json.dumps(r), flush=True), keep=keep)
        trend = sim.run_set(sim.target_worlds(), draws_trend, pool, 9, log=lambda r: print(json.dumps(r), flush=True),
                            keep=keep)
    (out_dir / 'power_records.json').write_text(json.dumps(keep))
    summary = {'calibration': cal, 'hindsight_misaligned': other, 'trend': trend,
               'tables': sim.tables(keep)}
    (out_dir / 'power.json').write_text(json.dumps(summary, indent=1))
    return summary


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--check', action='store_true')
    g.add_argument('--run', action='store_true')
    g.add_argument('--power', action='store_true')
    args = ap.parse_args()
    if args.check:
        failures, ctx = guard_failures(HERE, 'run', toplevel=REPO, anchor=ANCHOR, env_check=True)
        print('\n'.join(failures) if failures else f"guard open: registration commit {ctx['commit']}")
    elif args.run:
        run()
    else:
        print(json.dumps(power()['tables'], indent=1))


if __name__ == '__main__':
    main()
