"""Registration 015 — the month-end liquidity cycle, settlement-shifted: the guarded run (once).

    python3 -I r015_run.py --check       # print every reason the run may not start; computes nothing
    python3 -I r015_run.py --calendar    # DATES ONLY: rebuild data/r015/calendar.json (pre-registration; no value parsed)
    python3 -I r015_run.py --run         # the read, once: started row, result row, reported row (each committed)

The run refuses unless (every check is a refusal, never an assert):
  (s)  the registration says REGISTERED;  (a) it is tracked;  (b) unmodified against HEAD;  (c) it has a commit;
  (d)  the guarded code is clean;  (e) every guarded code and data file is listed exactly once in the registration's
       SHA-256 table and matches it;
  (k)  results.jsonl is tracked and clean against HEAD (a row is never left uncommitted);
  (w)  git's top level, asked from THIS file's directory, is the repository two levels above it, and the anchor commit
       is an ancestor of HEAD; GIT_* variables are stripped from every git call;
  (v)  python -O is refused; the Python version equals the pin; no untracked, ignored or modified importable file
       (*.py, sourceless *.pyc, *.so / *.pyd, packages) sits at research/alpha's top level; r015_core and r015_hmm
       were loaded from their hashed sources; sys.pycache_prefix is a fresh directory outside the repository;
  (f)  no row of any registration whose name starts '015-' exists in the working tree, in any branch, in the reflog,
       or in a merge or stash diff (`git log --all --reflog -m -p --text --no-textconv --no-ext-diff`): run once.
Every `<rev>:<path>` is written `<rev>:./<path>` (git resolves it from the repository root otherwise; 014 review B B1).
Reads: the data loaders open the real tree (data/r015) only after the real guard passed in this process (a gate
token); mock hooks, a stubbed guard or a mixed real/synthetic tree are refused on the real files. The loader drops
every row dated on or after 2026-06-10 before parsing its values, and refuses a file whose SHA-256 differs from the
registration table. The runner commits results.jsonl alone (`git commit --only`) after each row, verifies the row in
HEAD, and never pushes.
"""
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.pycache_prefix = tempfile.mkdtemp(prefix='r015-pyc-')    # never load bytecode from an existing __pycache__
sys.path.insert(0, str(HERE))
import r015_core as C  # noqa: E402
import r015_hmm as H  # noqa: E402

Refused = C.Refused
REPO = HERE.parent.parent
REG = HERE / 'registrations' / '015-month-end-liquidity-cycle.md'
REG_PREFIX = '015-'
RESULTS = HERE / 'results.jsonl'
DATA = HERE / 'data' / 'r015'
DATA_REAL = (HERE / 'data' / 'r015').resolve()             # never reassigned: the gate compares against it
REAL_RESULTS = (HERE / 'results.jsonl').resolve()
ANCHOR = '15c6421e31b345f209cfbee7f73ef47b88ffc3cd'         # "B1 scout ...; decision to draft 015" (2026-10-13), (w)
PY_PIN = '3.14.8'

FRENCH_DIR = 'data/r015/french_20261013T093725Z'
TREASURY_DIR = 'data/r015/treasury_copy_20261013T093812Z'
US_ZIP = f'{FRENCH_DIR}/F-F_Research_Data_Factors_daily_CSV.zip'
EU_ZIP = f'{FRENCH_DIR}/Europe_3_Factors_Daily_CSV.zip'
TREASURY = tuple(f'{TREASURY_DIR}/daily_treasury_yield_curve_{y}.csv' for y in range(2014, 2027))
CALENDAR = 'data/r015/calendar.json'
POWER_LOG = 'data/r015/logs/power.json'
GUARDED_CODE = ('r015_core.py', 'r015_hmm.py', 'r015_power.py', 'r015_run.py')
GUARDED_DATA = (US_ZIP, EU_ZIP, *TREASURY, f'{FRENCH_DIR}/SHA256SUMS', f'{TREASURY_DIR}/SHA256SUMS', CALENDAR, POWER_LOG)
HASH_ROW = re.compile(r'^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|', re.M)
STATUS = re.compile(r'^\*\*Status: (\w+)', re.M)
_GATE = None                                                 # (dir, commit), set only by the real guard()


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


def revpath(rev, rel):
    """`<rev>:./<rel>`: relative to cwd. A bare `<rev>:<rel>` resolves from the repository root (014 review B B1)."""
    return f'{rev}:./{rel}'


def is_015(row):
    return isinstance(row, dict) and str(row.get('registration', '')).startswith(REG_PREFIX)


def result_rows(root, results_rel='results.jsonl', match=is_015):
    """[(line, row)] of every distinct matching line in the working tree and every ADDED line in
    `git log --all --reflog -m -p` (every branch, the reflog, merge and stash diffs)."""
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
        if match(row) and line not in seen:
            seen.add(line)
            out.append((line, row))
    return out


def head_lines(root, results_rel='results.jsonl'):
    return _git(root, 'show', '--no-textconv', revpath('HEAD', results_rel)).stdout.splitlines()


def is_ancestor(root, c):
    return bool(c) and _git(root, 'merge-base', '--is-ancestor', c, 'HEAD').returncode == 0


# ------------------------------------------------------------------ (v) environment
def env_fingerprint():
    """Python version and executable, and the SHA-256 of the stdlib sources the decision path imports."""
    import csv
    import math
    import statistics
    h, files = hashlib.sha256(), []
    for mod in (csv, statistics, zipfile, json, dt, hashlib, math):
        f = getattr(mod, '__file__', None)
        if f and Path(f).is_file():
            h.update(Path(f).name.encode())
            h.update(sha256(f).encode())
            files.append(Path(f).name)
    return {'python': sys.version.split()[0], 'executable': sys.executable, 'stdlib_files': files,
            'stdlib_sha256': h.hexdigest(), 'flags_optimize': sys.flags.optimize}


def shadow_modules(root):
    """Anything importable at research/alpha's top level (sys.path[0]) that is not a tracked, unmodified file and could
    shadow an import: *.py, sourceless *.pyc, *.so / *.pyd, and packages (a directory with __init__.py). Scanned on
    the FILE SYSTEM, so git-ignored files count (ported from r008 / r013)."""
    root = Path(root)
    tracked = set(_git(root, 'ls-files', '-z').stdout.split('\0'))
    modified = set(_git(root, 'ls-files', '-m', '-z').stdout.split('\0'))
    bad = set()

    def unclean(f):
        rel = str(f.relative_to(root))
        return rel not in tracked or rel in modified
    for p in sorted(root.iterdir()):
        if p.name == '__pycache__':
            continue
        if p.is_file() and p.suffix in ('.py', '.pyc', '.so', '.pyd'):
            if unclean(p):
                bad.add(str(p.relative_to(root)))
        elif p.is_dir() and (p / '__init__.py').exists():
            if unclean(p / '__init__.py'):
                bad.add(str((p / '__init__.py').relative_to(root)))
    return sorted(bad)


def loaded_from_source():
    out = []
    for name, rel in (('r015_core', 'r015_core.py'), ('r015_hmm', 'r015_hmm.py')):
        mod = sys.modules.get(name)
        if mod is None or Path(getattr(mod, '__file__', '') or '').resolve() != (HERE / rel).resolve():
            out.append(f'(v) {name} was not loaded from {rel}')
    if not sys.pycache_prefix or Path(sys.pycache_prefix).resolve().is_relative_to(REPO.resolve()):
        out.append('(v) sys.pycache_prefix must be a fresh directory outside the repository')
    return out


def environment_failures(root):
    out = []
    if sys.flags.optimize:
        out.append('(v) python -O / -OO is refused')
    if sys.version.split()[0] != PY_PIN:
        out.append(f'(v) python {sys.version.split()[0]} differs from the pin {PY_PIN}')
    shadow = shadow_modules(root)
    if shadow:
        out.append(f'(v) untracked, ignored or modified importable files could shadow an import: {shadow[:6]}')
    out += loaded_from_source()
    return out


# ------------------------------------------------------------------ the guard
def guard_failures(root, reg_rel, code=GUARDED_CODE, data=GUARDED_DATA, results_rel='results.jsonl', toplevel=None,
                   anchor=None, env_check=False):
    """([reasons], registration commit). The run may start only when the list is empty."""
    root = Path(root)
    reg = root / reg_rel
    out = []
    text = reg.read_text() if reg.exists() else ''
    m = STATUS.search(text)
    if not m or m.group(1) != 'REGISTERED':
        out.append(f'(s) {reg.name} is not REGISTERED — refusing to compute any outcome.')
    if _git(root, 'ls-files', '--error-unmatch', '--', reg_rel).returncode:
        out.append(f'(a) {reg.name} is not tracked by git')
    if _git(root, 'diff', '--quiet', '--no-textconv', '--no-ext-diff', 'HEAD', '--', reg_rel).returncode:
        out.append(f'(b) {reg.name} differs from the committed version')
    commit = _git(root, 'log', '-1', '--format=%H', '--', reg_rel).stdout.strip()
    if not commit:
        out.append(f'(c) {reg.name} has no commit')
    dirty = _git(root, 'status', '--porcelain', '--', *code).stdout.strip() if code else ''
    if dirty:
        out.append(f'(d) guarded code not clean: {dirty.splitlines()}')
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
    if _git(root, 'ls-files', '--error-unmatch', '--', results_rel).returncode or \
            _git(root, 'diff', '--quiet', '--no-textconv', '--no-ext-diff', 'HEAD', '--', results_rel).returncode:
        out.append(f'(k) {results_rel} is untracked or differs from HEAD — every row is committed when written')
    if toplevel is not None:
        top = _git(root, 'rev-parse', '--show-toplevel').stdout.strip()
        if not top or Path(top).resolve() != Path(toplevel).resolve():
            out.append(f'(w) git top level is {top!r}, not {toplevel}')
    if anchor is not None and not is_ancestor(root, anchor):
        out.append(f'(w) the anchor commit {anchor} is not an ancestor of HEAD')
    if env_check:
        out += environment_failures(root)
    if result_rows(root, results_rel):
        out.append('(f) a 015 row exists (working tree, any branch, reflog, merge or stash) — 015 runs once')
    return out, commit


def guard():
    """The ONLY way to open the real data: this file's own repository, default arguments, every check."""
    global _GATE
    _GATE = None
    failures, commit = guard_failures(HERE, str(REG.relative_to(HERE)), toplevel=REPO, anchor=ANCHOR, env_check=True)
    if failures:
        raise Refused('\n'.join(failures))
    _GATE = (str(HERE.resolve()), commit)
    return commit


_REAL_GUARD = guard


def _gate(path):
    """Refuse any read under the real data tree unless the real guard passed in this process."""
    p = Path(path).resolve()
    if p == DATA_REAL or p.is_relative_to(DATA_REAL):
        if _GATE is None or _GATE[0] != str(HERE.resolve()) or guard is not _REAL_GUARD:
            raise Refused('the real 015 data opens only after the real guard() passed in this process')


# ------------------------------------------------------------------ loaders
def _csv_lines(zip_path):
    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if n.lower().endswith('.csv')]
        if len(names) != 1:
            raise Refused(f'{zip_path}: expected one csv, found {names}')
        return z.read(names[0]).decode('latin-1').splitlines()


def _date8(s):
    return dt.date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def read_dates_only(zip_path, want_sha):
    """Trading dates in [DATA_START, DATA_END) — only the first field of each line is read; no value is parsed.
    Ungated by design (the calendar is a pre-registration fact); verified against the pinned hash."""
    if sha256(zip_path) != want_sha:
        raise Refused(f'{zip_path} does not match its pinned SHA-256')
    out = []
    for line in _csv_lines(zip_path):
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
    if sha256(zip_path) != want_sha:
        raise Refused(f'{zip_path} does not match its pinned SHA-256')
    lines = _csv_lines(zip_path)
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


def load_treasury(paths, want):
    """{date: 10-year par yield (fraction)} for dates < DATA_END. Gated; every file hash-checked."""
    import csv
    out = {}
    for p in paths:
        _gate(p)
        if sha256(p) != want[p]:
            raise Refused(f'{p} does not match its pinned SHA-256')
        with open(p, newline='') as f:
            rows = list(csv.reader(f))
        hdr = rows[0]
        if '10 Yr' not in hdr or hdr[0] != 'Date':
            raise Refused(f'{p}: unexpected header')
        k = hdr.index('10 Yr')
        for r in rows[1:]:
            mth, day, yr = r[0].split('/')
            d = dt.date(int(yr), int(mth), int(day))
            if d < C.DATA_START or d >= C.DATA_END:
                continue
            if r[k].strip():
                out[d] = float(r[k]) / 100.0
    return out


# ------------------------------------------------------------------ results: append, commit, verify
def append_row(row, results=None):
    line = json.dumps(row, default=str, sort_keys=True)
    with open(results or RESULTS, 'a') as f:
        f.write(line + '\n')
    return line


def commit_results(msg):
    """Commit results.jsonl ALONE, immediately. Never pushes: the owner pushes."""
    r = _git(RESULTS.parent, 'commit', '--only', '-m', msg, '--', RESULTS.name)
    if r.returncode:
        raise Refused(f'could not commit {RESULTS.name}: {r.stderr.strip()[:300]} — stop; commit the row BY HAND, as '
                      'it is; (k) refuses every later run until it is committed')


HOOKS = {'commit': commit_results}


def real_tree_failures():
    """The real results file and the real data tree are used only together, only with the default commit hook, and
    only with the real guard."""
    real_r, real_d = Path(RESULTS).resolve() == REAL_RESULTS, Path(DATA).resolve() == DATA_REAL
    out = []
    if real_r != real_d:
        out.append('(M4) the real results.jsonl and the real data tree are used only together')
    if (real_r or real_d) and HOOKS.get('commit') is not commit_results:
        out.append('(M4) a non-default commit hook is never used on the real results file or data tree')
    if (real_r or real_d) and guard is not _REAL_GUARD:
        out.append('(M4) the real tree is opened only by the real guard')
    return out


def _refuse_real_tree():
    f = real_tree_failures()
    if f:
        raise Refused('; '.join(f))


def record(row, msg):
    """Append, commit through the hook, then REFUSE unless the line is in HEAD:results.jsonl (checked with git)."""
    line = append_row(row)
    HOOKS['commit'](msg)
    if line not in head_lines(RESULTS.parent, RESULTS.name):
        raise Refused(f'the {row.get("mode")} row is not in HEAD:{RESULTS.name} after the commit — stop')
    return line


def hashes(files, root=None):
    root = Path(root or HERE)
    return {f: sha256(root / f) for f in files if (root / f).is_file()}


def reg_table(root=None):
    root = Path(root or HERE)
    return dict(HASH_ROW.findall((root / REG.relative_to(HERE)).read_text()))


# ------------------------------------------------------------------ the computation (after the guard)
def pinned_calendar(root=None):
    return json.loads((Path(root or HERE) / CALENDAR).read_text())


def primary(commit, root=None, table=None):
    """The decision. Pure function of the US file and the pinned calendar (deterministic)."""
    root = Path(root or HERE)
    table = table or reg_table(root)
    dates, v = load_french(root / US_ZIP, table[US_ZIP])
    pin = pinned_calendar(root)
    if [d.isoformat() for d in dates] != pin['dates']:
        raise Refused('the loaded trading dates differ from the pinned calendar.json')
    cal = C.build_calendar(dates)
    fails = C.calendar_failures(cal)
    if fails:
        raise Refused('; '.join(fails))
    if C.calendar_summary(cal, dates)['rows'] != pin['summary']['rows']:
        raise Refused('the rebuilt calendar differs from the pinned calendar.json')
    print('counts before the decision:', json.dumps({k: v_ for k, v_ in C.calendar_summary(cal, dates).items() if k != 'rows'}))
    x = v['Mkt-RF']
    y, mu = C.y_series(x, cal)
    vv = C.v_series(x, cal)
    z = C.rebound_sums(x, cal, mu)
    ex = C.existence(y)
    econ = C.economic(vv)
    months = [{'month': c['month'], 'T': c['T'], 's': c['s'], 'friday': c['weekday'] == 4, 'y': a, 'v': b, 'z': cz}
              for c, a, b, cz in zip(cal, y, vv, z)]
    row = {'registration': REG.name, 'mode': 'result', 'registration_commit': commit,
           'run_at': dt.datetime.now(dt.UTC).isoformat(), 'env': env_fingerprint(),
           'code_hashes': hashes(GUARDED_CODE, root), 'data_hashes': {f: table.get(f) for f in GUARDED_DATA},
           'params': {'data_start': C.DATA_START.isoformat(), 'data_end_exclusive': C.DATA_END.isoformat(),
                      'cal_crit': C.CAL_CRIT, 'q': C.Q, 'shrink': C.SHRINK, 'cost_month': C.COST_MONTH,
                      'l_cap': C.L_CAP, 'bar': C.BAR, 'us_t2': C.US_T2.isoformat(), 'us_t1': C.US_T1.isoformat()},
           'mu_by_era': {str(k): val for k, val in mu.items()}, 'existence': ex, 'economic': econ, 'months': months}
    row['decision'] = C.decide(ex, econ)
    lab, rfails = C.recheck(row)
    if rfails or lab != row['decision']:
        row['recheck_failed'] = rfails
    return row, (dates, x, v['RF'], cal, mu, y)


def reported(state, root=None, table=None):
    """Reported blocks: decide nothing. Each block is computed independently; a failing block stores its error."""
    root = Path(root or HERE)
    table = table or reg_table(root)
    dates, x, rf, cal, mu, y = state
    out = {}

    def blk(name, fn):
        try:
            out[name] = fn()
        except Exception as e:  # noqa: BLE001 - a reported block never stops the run
            out[name] = {'error': repr(e)[:300]}
    blk('long_only_rebound_abnormal', lambda: C.summary(C.rebound_sums(x, cal, mu)))
    blk('long_short_raw_v', lambda: C.summary(C.v_series(x, cal)))
    blk('by_era', lambda: C.by_era(y, cal))
    blk('friday_T_vs_other', lambda: C.friday(y, cal))
    blk('mechanism_contrast', lambda: C.mechanism_contrast(x, cal, mu))
    blk('placebo_T_minus_12', lambda: C.placebo(x, cal, mu))
    blk('xyz_net', lambda: C.summary([b - C.xyz_cost_month(dates, c) for b, c in zip(C.v_series(x, cal), cal)]))
    blk('mes_net', lambda: C.summary([b - C.COST_MONTH for b in C.v_series(x, cal)]))

    def europe():
        de, ve = load_french(root / EU_ZIP, table[EU_ZIP], cols=('Mkt-RF',))
        ce = C.build_calendar(de, C.eu_lag)
        xe = ve['Mkt-RF']
        ye, mue = C.y_series(xe, ce)
        return {'label': 'French Europe region (USD; weekday calendar, country holidays mixed); EU T+2 from 2014-10-06',
                'months': len(ce), 'by_lag': C.calendar_summary(ce, de)['by_lag'], 'y': C.summary(ye),
                'by_era': C.by_era(ye, ce), 'mechanism_contrast': C.mechanism_contrast(xe, ce, mue)}
    blk('europe', europe)

    def hmm():
        ys = load_treasury([root / p for p in TREASURY], {root / p: table[p] for p in TREASURY})
        return H.block(dates, x, rf, ys)
    blk('hmm_replication', hmm)
    return out


def run(root=None):
    """The read, once. `root` is this directory in production; tests pass a throwaway repository's directory (with a
    stubbed guard bound to it), and real_tree_failures() refuses any mix with the real files."""
    root = Path(root or HERE)
    _refuse_real_tree()
    commit = guard()
    _refuse_real_tree()
    record({'registration': REG.name, 'mode': 'started', 'registration_commit': commit, 'env': env_fingerprint(),
            'code_hashes': hashes(GUARDED_CODE, root), 'run_at': dt.datetime.now(dt.UTC).isoformat()},
           '015 started (r015_run.py)')
    try:
        row, state = primary(commit, root)
    except Exception as e:
        record({'registration': REG.name, 'mode': 'failed', 'registration_commit': commit, 'error': repr(e)[:500],
                'run_at': dt.datetime.now(dt.UTC).isoformat()}, '015 failed (r015_run.py)')
        raise
    line = record(row, '015 result (r015_run.py)')
    if row.get('recheck_failed'):
        raise Refused('internal: the decision does not recompute from its records; stored with recheck_failed, not printed')
    print(json.dumps({k: row['existence'][k] for k in ('G', 'mean', 'sd', 't', 'crit', 'mean_without_largest', 'criteria')},
                     indent=1, default=str))
    print('economic (applies only after an existence pass):', json.dumps(row['economic'], default=str))
    print('decision:', row['decision'])
    rep = {'registration': REG.name, 'mode': 'reported', 'registration_commit': commit, 'result_row_sha256': line_sha(line),
           'run_at': dt.datetime.now(dt.UTC).isoformat(), 'blocks': reported(state, root)}
    record(rep, '015 reported blocks (r015_run.py)')
    print(json.dumps(rep['blocks'], indent=1, default=str))
    return row, rep


def build_calendar_file(root=None, out=None):
    """Pre-registration, DATES ONLY: the trading dates in [2014-01-01, 2026-06-10) and the month-end calendar."""
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
        f, c = guard_failures(HERE, str(REG.relative_to(HERE)), toplevel=REPO, anchor=ANCHOR, env_check=True)
        print('\n'.join(f) if f else f'guard open (registration commit {c})')
    elif '--calendar' in sys.argv:
        build_calendar_file()
    elif '--run' in sys.argv:
        run()
    else:
        sys.exit(__doc__)


if __name__ == '__main__':
    main()
