"""Registration 008 (L3: 001 T3 inverted, holdout-only) — the guard and the one holdout read. Revision 1.

    .venv/bin/python -I r008_run.py --check     # print every reason the read may not start; computes nothing
    .venv/bin/python -I r008_run.py --run       # the read, once: holdout_started row, then one holdout row

`r008_fetch.py` pulls the holdout inputs first (its own gate, below, mode 'fetch'). On the REAL data tree
both refuse unless `root` is this repository and no guard argument (code, data, paths, clock) is
overridden (Revision 1, B-B1). The read refuses unless:
  (s)  the registration says REGISTERED; (a) it is tracked; (b) unmodified vs HEAD; (c) it has a commit;
  (d)  the guarded code is clean, and no untracked or modified *.py sits in research/copytrade or ct/;
  (e)  code, static data and the pre-registration manifest match the §11 SHA-256 table (each file listed
       exactly once), and every file the manifest lists matches its hash;
  (n)  exactly one line starts `008 holdout end (owner):`, and its date is an offered end;
  (t)  now >= the end + 7 days;
  (k)  results.jsonl is tracked and clean against HEAD (rows are committed as they are written);
  (f)  no 008 holdout row exists in the tree, in any branch's history or in the reflog (read once); a
       holdout_started row (a mechanical stop) blocks unless a committed line
       `008 holdout restart: APPROVED <date> started-row <sha256 of that row>` was added after it;
  (g1) exactly one holdout_fetch_started and one holdout_fetched row exist anywhere, both committed in
       HEAD:results.jsonl, with the same registration commit, an ancestor of HEAD;
  (g2) since that commit the guarded code is unchanged and the registration changed only by added approval
       or restart lines (r); the fetched row's owner end, code hashes and Python environment equal today's;
  (g3) the fetch manifest's files hash to the digest stored in the fetched row, and the snapshot files the
       read would select from a full glob are exactly the manifest's, one per needed date;
  (h') a committed line `008 holdout read: APPROVED <date> row <sha256 of the fetched row line>`, added in a
       commit strictly after the one that added the fetched row.
The run opens no network connection (locked), commits `holdout_started` before it scores a single wallet,
and appends and commits exactly one `holdout` row. Nothing is printed before the coverage counts; a
decision that does not recompute from its numbers is stored with recheck_failed and NOT printed.
"""
import argparse
import datetime as dt
import hashlib
import json
import random
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from ct import crowd008 as c8  # noqa: E402

REAL_DATA = (HERE / 'data').resolve()
REG_NAME = '008-inverted-positioning-holdout.md'
REG_REL = f'registrations/{REG_NAME}'
RESULTS_REL = 'results.jsonl'
PRE_MANIFEST_REL = 'registrations/008-manifest.json'
STORE_REL = 'data/r008'
DEX = c8.DEX
HOUR, DAY = c8.HOUR, c8.DAY
PLACEBO_SEEDS = 200

GUARDED_CODE = ('ct/__init__.py', 'ct/crowd008.py', 'ct/crowd.py', 'ct/scores.py', 'ct/stats.py', 'ct/schedule.py',
                'ct/leaders.py', 'ct/archive_fills.py', 'ct/archive.py', 'ct/regimes.py', 'ct/net.py',
                'ct/bitget.py', 'ct/hl.py', 'p3_checks.py', 'p4_features.py', 'p5_population.py', 'r008_run.py',
                'r008_fetch.py')
GUARDED_DATA = ('data/p0/2026-10-07/symbols.json', 'data/p0/2026-10-07/report.json',
                'data/derived/bitget_listing.json', 'data/cache/hl-vaults/903952e394cd91c74dc593a3.json',
                'data/derived/regimes/btc_1h.parquet', 'data/derived/hyperliquid/snapshot_accounts.parquet',
                PRE_MANIFEST_REL)
ENV_PACKAGES = ('duckdb', 'pyarrow')

HASH_ROW = re.compile(r'^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|', re.M)
END_PREFIX = re.compile(r'^008 holdout end \(owner\):', re.M)
END_LINE = re.compile(r'^008 holdout end \(owner\): (\S+)\s*$', re.M)
APPROVAL = re.compile(r'^008 holdout read: APPROVED (\d{4}-\d{2}-\d{2}) row ([0-9a-f]{64})\s*$', re.M)
RESTART = re.compile(r'^008 holdout restart: APPROVED (\d{4}-\d{2}-\d{2}) started-row ([0-9a-f]{64})\s*$', re.M)
STATUS = re.compile(r'^\*\*Status: (\w+)', re.M)
AMEND_PREFIX = re.compile(r'^008 amendment \d+:', re.M)
AMEND_LINE = re.compile(r'^008 amendment 1: APPROVED (\d{4}-\d{2}-\d{2}) prior-commit ([0-9a-f]{40})\s*$', re.M)
_GATE = {'pair': None}             # (root, data_dir) that passed the guard inside run(); None otherwise


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
    return subprocess.run(['git', *a], cwd=root, capture_output=True, text=True)


def result_rows(root, results_rel=RESULTS_REL, name=REG_NAME):
    """[(line, row)] of every distinct 008 line in the working tree and every ADDED line in the history of
    every branch AND of every commit the reflog remembers (`git log --all --reflog -p`). For BLOCKING
    checks: a deleted, reset or off-branch row still blocks."""
    root = Path(root)
    texts = []
    res = root / results_rel
    if res.exists():
        texts.extend(res.read_text().splitlines())
    hist = _git(root, 'log', '--all', '--reflog', '-p', '--text', '--no-textconv', '--no-ext-diff', '--format=', '--',
                results_rel)
    texts.extend(l[1:] for l in hist.stdout.splitlines() if l.startswith('+') and not l.startswith('+++ '))
    out, seen = [], set()
    for line in texts:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict) and row.get('registration') == name and line not in seen:
            seen.add(line)
            out.append((line, row))
    return out


def head_lines(root, results_rel=RESULTS_REL):
    return set(_git(root, 'show', f'HEAD:./{results_rel}').stdout.splitlines())


def first_commit_containing(root, rel, needle):
    """The oldest commit on HEAD's history whose version of `rel` contains the line `needle`."""
    for c in _git(root, 'log', '--format=%H', '--reverse', '--', rel).stdout.split():
        if needle in _git(root, 'show', '--no-textconv', f'{c}:./{rel}').stdout.splitlines():
            return c
    return None


def commit_time(root, commit):
    """Committer time (s) of a commit; the persistence clock of the fetch (review r1, m2)."""
    out = _git(root, 'show', '-s', '--format=%ct', commit).stdout.strip()
    return int(out) if out else None


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


def owner_end(text):
    """(end date, [failures]) from the single anchored owner line."""
    if len(END_PREFIX.findall(text)) != 1 or len(END_LINE.findall(text)) != 1:
        return None, [f'(n) {len(END_PREFIX.findall(text))} lines start "008 holdout end (owner):"; exactly one is required']
    v = END_LINE.search(text).group(1)
    if v not in c8.OFFERED_ENDS:
        return None, [f'(n) holdout end {v!r} is not one of {sorted(c8.OFFERED_ENDS)}']
    return c8.OFFERED_ENDS[v], []


def env_fingerprint():
    """The Python version and, for duckdb and pyarrow, the version and the SHA-256 digest of EVERY installed
    distribution file (Python sources, binaries, data), __pycache__ / *.pyc excluded (B-m3; review r1, m3).
    Recovery after an interpreter or package change between fetch and run: rebuild the venv to the pinned
    versions (the same Python patch release and the same wheels) until this fingerprint equals the fetched
    row's; `r008_run.py --env-record` writes and commits the check as a row."""
    import importlib.metadata as md
    out = {'python': sys.version.split()[0]}
    for name in ENV_PACKAGES:
        dist = md.distribution(name)
        files = sorted(str(f) for f in (dist.files or [])
                       if '__pycache__' not in str(f) and not str(f).endswith('.pyc'))
        h, n = hashlib.sha256(), 0
        for f in files:
            path = Path(dist.locate_file(f))
            if path.is_file():
                h.update(f.encode())
                h.update(sha256(path).encode())
                n += 1
        out[name] = {'version': dist.version, 'files_sha256': h.hexdigest(), 'files': n}
    return out


def shadow_modules(root):
    """B-m11 / review r1 m1: anything importable that is not a tracked, unmodified file and could shadow a
    module the read imports. Scanned on the FILE SYSTEM (so git-ignored files count), in the top level of
    research/copytrade (sys.path[0]) and in ct/: *.py, *.pyc (sourceless bytecode outside __pycache__),
    *.so / *.pyd, and packages (a directory with __init__.py: a new package's __init__.py, and every file
    of a package inside ct/)."""
    root = Path(root)
    tracked = set(_git(root, 'ls-files', '-z').stdout.split('\0'))
    modified = set(_git(root, 'ls-files', '-m', '-z').stdout.split('\0'))
    bad = set()

    def unclean(f):
        rel = str(f.relative_to(root))
        return rel not in tracked or rel in modified

    for base, inside_ct in ((root, False), (root / 'ct', True)):
        if not base.is_dir():
            continue
        for p in sorted(base.iterdir()):
            if p.name == '__pycache__':
                continue
            if p.is_file() and (p.suffix in ('.py', '.pyc', '.so', '.pyd')):
                if unclean(p):
                    bad.add(str(p.relative_to(root)))
            elif p.is_dir() and (p / '__init__.py').exists():
                if inside_ct:
                    files = [f for f in p.rglob('*') if f.is_file() and '__pycache__' not in f.parts]
                else:
                    files = [p / '__init__.py']
                bad.update(str(f.relative_to(root)) for f in files if unclean(f))
    return sorted(bad)


def real_tree_failures(root, data_dir, overrides, hooks=None):
    """B-B1 / review r1 M1: the real data tree is opened only under the real repository's guard with default
    arguments, and never with mock network hooks (whatever the root)."""
    if Path(data_dir).resolve() != REAL_DATA:
        return []
    out = []
    if Path(root).resolve() != HERE:
        out.append('(B1) the real data tree is read only by the guard of the real repository (root is not it)')
    if overrides:
        out.append(f'(B1) the real data tree is read only with the default guard arguments (overridden: {sorted(overrides)})')
    if hooks is not None:
        out.append('(B1) mock network hooks are never used on the real data tree')
    return out


def amendment_prior(root, text, reg_rel=REG_REL):
    """Amendment 1 (2026-10-13): (accepted prior registration commit or None, [failures]).
    At most one amendment. When a line starts with `008 amendment`, there must be exactly one, of the form
    `008 amendment 1: APPROVED <date> prior-commit <40-hex sha>`; the prior commit must be a commit of the
    registration file and an ancestor of HEAD, and the commit that first holds the line must strictly descend
    from it. Then fetch rows written under the prior commit (before the amendment) are accepted; nothing else
    changes."""
    n = len(AMEND_PREFIX.findall(text))
    if n == 0:
        return None, []
    found = list(AMEND_LINE.finditer(text))
    if n != 1 or len(found) != 1:
        return None, [f'(m) {n} lines start "008 amendment"; at most one amendment, written exactly as '
                      '"008 amendment 1: APPROVED <date> prior-commit <full sha>"']
    prior, line = found[0].group(2), found[0].group(0).rstrip()
    fails = []
    if prior not in _git(root, 'log', '--format=%H', '--', reg_rel).stdout.split():
        fails.append('(m) the prior commit is not a commit of the registration')
    if not is_ancestor(root, prior):
        fails.append('(m) the prior commit is not an ancestor of HEAD')
    a_c = first_commit_containing(root, reg_rel, line)
    if not a_c or a_c == prior or _git(root, 'merge-base', '--is-ancestor', prior, a_c).returncode:
        fails.append('(m) the amendment line must be committed in a commit that strictly descends from the prior commit')
    return (None if fails else prior), fails


# ------------------------------------------------------------------ the guard (fetch and run)
def guard_failures(root=HERE, mode='run', now_ms=None, code=GUARDED_CODE, data=GUARDED_DATA,
                   reg_rel=REG_REL, results_rel=RESULTS_REL, pre_manifest_rel=PRE_MANIFEST_REL,
                   store_rel=STORE_REL):
    """([reasons], context) — the run or fetch may start only when the list is empty. mode: fetch | run."""
    root = Path(root)
    reg = root / reg_rel
    out = []
    text = reg.read_text() if reg.exists() else ''
    m = STATUS.search(text)
    if not m or m.group(1) != 'REGISTERED':
        out.append(f'(s) {reg.name} is not REGISTERED — refusing to fetch or compute anything from the holdout.')
    if _git(root, 'ls-files', '--error-unmatch', '--', reg_rel).returncode:
        out.append(f'(a) {reg.name} is not tracked by git')
    if _git(root, 'diff', '--quiet', 'HEAD', '--', reg_rel).returncode:
        out.append(f'(b) {reg.name} differs from the committed version')
    commit = _git(root, 'log', '-1', '--format=%H', '--', reg_rel).stdout.strip()
    if not commit:
        out.append(f'(c) {reg.name} has no commit')
    dirty = _git(root, 'status', '--porcelain', '--', *code).stdout.strip() if code else ''
    if dirty:
        out.append(f'(d) guarded code not clean: {dirty.splitlines()}')
    shadow = shadow_modules(root)
    if shadow:
        out.append(f'(d) untracked, ignored or modified importable files could shadow an import: {shadow[:5]}')
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
    pre = root / pre_manifest_rel
    if pre.exists() and pre_manifest_rel in data:
        listed = json.loads(pre.read_text()).get('files', {})
        bad = [f for f, h in sorted(listed.items()) if not (root / f).exists() or sha256(root / f) != h]
        if bad:
            out.append(f'(e) {len(bad)} pre-registration manifest files do not match, e.g. {bad[:3]}')
    end, n = owner_end(text)
    out += n
    prior, am = amendment_prior(root, text, reg_rel)
    out += am
    accepted = {commit} | ({prior} if prior else set())
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    if end is not None and now_ms < c8.ms(end) + 7 * DAY:
        out.append(f'(t) the holdout ends {end}; nothing is fetched or read before end + 7 days')
    if _git(root, 'ls-files', '--error-unmatch', '--', results_rel).returncode or \
            _git(root, 'diff', '--quiet', 'HEAD', '--', results_rel).returncode:
        out.append('(k) results.jsonl is untracked or differs from HEAD — every 008 row is committed when written')
    rows = result_rows(root, results_rel)
    heads = head_lines(root, results_rel)
    if [r for _, r in rows if r.get('mode') == 'holdout']:
        out.append('(f) a 008 holdout row exists (tree, history or reflog) — the holdout is read once')
    for sl0, _ in [(l, r) for l, r in rows if r.get('mode') == 'holdout_started']:
        if mode == 'fetch':
            out.append('(f) a 008 holdout_started row exists — the fetch is closed')
            break
        rs = [a for a in RESTART.finditer(text) if a.group(2) == line_sha(sl0)]
        if not rs or sl0 not in heads or not committed_strictly_after(root, results_rel, sl0, reg_rel,
                                                                      rs[0].group(0).rstrip()):
            out.append('(f) a holdout_started row exists without a committed, later '
                       "'008 holdout restart: APPROVED <date> started-row <sha256>' line — the holdout is read once")
    started = [(l, r) for l, r in rows if r.get('mode') == 'holdout_fetch_started']
    fetched = [(l, r) for l, r in rows if r.get('mode') == 'holdout_fetched']
    ctx = {'commit': commit, 'end': end, 'end_date': end.isoformat() if end else None}
    if mode == 'fetch':
        if fetched:
            out.append('(f) the holdout fetch has completed (a holdout_fetched row exists) — it runs once')
        if len(started) > 1:
            out.append(f'(f) {len(started)} holdout_fetch_started rows; at most one is allowed')
        for l, r in started:
            if r.get('registration_commit') not in accepted or r.get('owner') != {'end_date': ctx['end_date']}:
                out.append('(f) the existing holdout_fetch_started row has another registration commit or end')
            if l not in heads:
                out.append('(f) the existing holdout_fetch_started row is not committed')
        ctx['resume'] = bool(started)
        return out, ctx
    # mode == 'run'
    if len(started) != 1 or len(fetched) != 1:
        out.append(f'(g1) {len(started)} holdout_fetch_started and {len(fetched)} holdout_fetched rows; exactly one '
                   'of each is required (fetch first)')
        return out, ctx
    (sl, sr), (fl, fr) = started[0], fetched[0]
    if sl not in heads or fl not in heads:
        out.append('(g1) the fetch rows are not committed in HEAD:results.jsonl')
    rc = fr.get('registration_commit') or ''
    if sr.get('registration_commit') != rc and not (prior and sr.get('registration_commit') == prior):
        out.append('(g1) the two fetch rows carry different registration commits (and no valid Amendment 1 links them)')
    if not is_ancestor(root, rc):
        out.append("(g1) the fetched row's registration commit is not an ancestor of HEAD")
    else:
        if code and _git(root, 'diff', '--quiet', rc, 'HEAD', '--', *code).returncode:
            out.append('(g2) the guarded code changed since the fetch')
        changed = [l[1:] for l in _git(root, 'diff', '-U0', '--no-color', '--text', '--no-textconv', '--no-ext-diff', rc,
                                       'HEAD', '--', reg_rel).stdout.splitlines()
                   if l[:1] in '+-' and not l.startswith(('+++ ', '--- '))]
        if any(l.strip() and not (APPROVAL.fullmatch(l.rstrip()) or RESTART.fullmatch(l.rstrip())) for l in changed):
            out.append('(r) the registration changed since the fetch beyond added approval or restart lines')
    if fr.get('owner') != {'end_date': ctx['end_date']}:
        out.append(f"(g2) the owner end changed since the fetch: row {fr.get('owner')} vs now {ctx['end_date']}")
    if fr.get('code_hashes') != hashes(root, code):
        out.append('(g2) the code hashes stored in the fetched row differ from the code on disk')
    if fr.get('env') != env_fingerprint():
        out.append('(g2) the Python environment (versions or binaries of duckdb / pyarrow) differs from the fetch')
    man = root / store_rel / 'manifest.json'
    if not man.exists():
        out.append('(g3) the fetch manifest is missing')
    else:
        mj = json.loads(man.read_text())
        files = mj.get('files', {})
        now_h = {f: (sha256(root / f) if (root / f).exists() else None) for f in files}
        if manifest_digest(now_h) != fr.get('manifest_digest') or manifest_digest(files) != fr.get('manifest_digest'):
            out.append('(g3) the fetched data do not hash to the digest stored in the fetched row')
        if end is not None:
            out += snapshot_pin_failures(root, root / 'data', end, mj.get('snapshots'))
        ctx['snapshots'] = mj.get('snapshots')
    appr = [a for a in APPROVAL.finditer(text) if a.group(2) == line_sha(fl)]
    if not appr:
        out.append("(h') no committed line '008 holdout read: APPROVED <date> row <sha256 of the holdout_fetched row>'")
    elif not committed_strictly_after(root, results_rel, fl, reg_rel, appr[0].group(0).rstrip()):
        out.append("(h') the approval must be committed strictly after the commit that added the fetched row")
    ctx.update(fetch_commit=rc, fetched_row_sha256=line_sha(fl), fetched_row=fr)
    return out, ctx


def snapshot_pin_failures(root, data_dir, end, pinned):
    """B-M1: the files the read selects from a FULL glob must be exactly the pinned manifest list, and every
    needed date directory must hold exactly one file."""
    out = []
    base = Path(data_dir) / f'archive/by_dex/{DEX}/snapshots/perp'
    allp = sorted(base.glob('date=*/*.parquet'))
    chosen = sorted(str(p.resolve().relative_to(Path(root).resolve())) for _, p, _ in c8.signal_snapshots(allp, end))
    if pinned is None or sorted(pinned) != chosen:
        out.append(f'(g3) the snapshot files selected from the tree ({len(chosen)}) are not exactly the pinned '
                   f'manifest list ({len(pinned or [])})')
    for s, _, he in c8.grid_windows(end):
        d = max(s, c8.HOLDOUT_START)
        while c8.ms(d) < c8.ms(he) - c8.B_HORIZON_H * HOUR:
            n = len(list((base / f'date={d.isoformat()}').glob('*.parquet')))
            if n != 1:
                out.append(f'(g3) snapshot date {d}: {n} files, exactly one required')
            d += dt.timedelta(days=1)
    return out


def append_row(root, row, results_rel=RESULTS_REL):
    line = json.dumps(row, default=str, sort_keys=True)
    with open(Path(root) / results_rel, 'a') as f:
        f.write(line + '\n')
    return line


def commit_results(root, message, results_rel=RESULTS_REL, retries=1, wait=2.0):
    """Commit results.jsonl ALONE, immediately (B-m1). Retried once (an IDE's transient index.lock); on final
    failure the run stops and says so: the row must be committed BY HAND, as it is, and never discarded;
    (k) refuses every later fetch or run until it is (review r1, m7). Never pushes: the owner pushes."""
    for attempt in range(retries + 1):
        r = _git(root, 'commit', '--only', '-m', message, '--', results_rel)
        if not r.returncode:
            return _git(root, 'rev-parse', 'HEAD').stdout.strip()
        if attempt < retries:
            time.sleep(wait)
    raise SystemExit(f'STOP: could not commit {results_rel} ({r.stderr.strip()[:160]}). Commit it by hand, AS IT IS '
                     f'(git commit --only -- {results_rel}); do not discard or edit the row. Every later fetch or run '
                     'refuses (k) until it is committed.')


# ------------------------------------------------------------------ offline lock and data binding
def lock_network():
    """The read uses local files only: any HTTP request or AWS call raises."""
    import urllib.request
    from ct import archive, net

    def refuse(*a, **k):
        raise RuntimeError('r008_run is offline: the holdout read opens no network connection')
    net._request = refuse
    urllib.request.urlopen = refuse
    archive._aws = refuse


def bind_data(data_dir):
    """Point the reused 001 modules at `data_dir` (production: research/copytrade/data)."""
    import p3_checks  # noqa: F401  (imports ct.archive_fills, rebound below)
    import p4_features
    import p5_population
    from ct import archive_fills, leaders, regimes
    data_dir = Path(data_dir)
    for mod in (archive_fills, leaders, p4_features, p5_population):
        mod.DATA = data_dir
    p5_population.CACHE = data_dir / 'cache'
    regimes.PATH = data_dir / 'derived' / 'regimes' / 'btc_1h.parquet'


# ------------------------------------------------------------------ the computation
def coverage(closes, keys, symbols, listed):
    """Existence counts only (no return): planned entries with both closes present, per universe."""
    need = have = 0
    for s in symbols:
        for k in keys:
            if listed.get(s) and listed[s] < k - c8.LISTED_MIN_DAYS * DAY:
                need += 1
                have += closes.close_at(s, k) is not None and closes.close_at(s, k + DAY) is not None
    btc_missing = sum(1 for k in keys if closes.ret('BTCUSDT', k) is None)
    return {'universe_symbol_days': need, 'with_both_closes': have, 'btc_days_missing': btc_missing}


def cohorts(window, reg, vaults, duck_mem='6GB'):
    """001's T3 cohort at selection S: features -> population (slow, not vault, >= 4 trips in both regimes)
    -> scored -> top / bottom quintile. Reuses p4_features, p5_population, ct.leaders and ct.scores."""
    import duckdb
    import p4_features
    import p5_population as pop
    from ct.leaders import Equity, load_fills
    s, lb, _ = window
    sel_ms, lb_ms = c8.ms(s), c8.ms(lb)
    tmp = p4_features.DATA / 'duckdb_tmp'
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{duck_mem}'; SET threads = 4; SET temp_directory = '{tmp}'; "
                "SET preserve_insertion_order = false")
    table, _ = p4_features.features(con, DEX, s, lb)
    rows = table.to_pylist()
    con.close()
    not_vault = [r for r in rows if r['address'].lower() not in vaults]
    slow = [r['address'] for r in not_vault if pop.passes(r, pop.SLOW)]
    fills = load_fills(DEX, sorted(set(slow)), lb, s)
    slow4 = pop.both_regimes(slow, fills, reg)[c8.TRIP_FLOOR]
    eq = Equity(DEX, sorted(slow4), lb_ms, sel_ms)
    scored = []
    for a in sorted(slow4):
        sc = c8.score_t3(fills[a], eq.at(a, sel_ms)[0], eq.typical_leverage(a, lb_ms, sel_ms), reg.is_rising)
        if sc is not None:
            scored.append((a, sc))
    top, bottom = c8.quintiles_by_score(scored)
    return top, bottom, {'features': len(rows), 'slow': len(slow), 'slow_both_regimes': len(slow4),
                         'scored': len(scored), 'cohort': len(top), 'crowd': len(bottom)}


def group_gross(snapshot_path, users):
    """{(user, market): gross notional} for the given users in one snapshot (reported blocks only)."""
    import duckdb
    con = duckdb.connect()
    con.execute('CREATE TEMP TABLE u(address VARCHAR)')
    if users:
        con.executemany('INSERT INTO u VALUES (?)', [(a,) for a in sorted(users)])
    rows = con.execute(f"""
        SELECT "user", market, SUM(abs(notional)) FROM read_parquet('{snapshot_path}')
        WHERE size != 0 AND "user" IN (SELECT address FROM u) GROUP BY 1, 2""").fetchall()
    con.close()
    return {(u, m): g for u, m, g in rows}


def raw_summary(rows):
    from ct import stats
    keys = [k for k, _ in rows]
    v = [x for _, x in rows]
    m, se, t, n, g = c8.cluster_mean_t(v, [c8.week_of(k) for k in keys])
    return {'n': n, 'G': g, 'mean': m, 'median': statistics.median(v) if v else None, 'cluster_t': t,
            'nw_t_lag3': stats.newey_west_t(v, 3) if v else None}


def factor_alpha(rows, fac, cells, with_mom=False):
    keys = [k for k, _ in rows]
    if not keys:
        return None
    cols, names = c8.design(keys, fac, cells, with_mom=with_mom)
    fit = c8.ols_multi([v for _, v in rows], cols, [c8.week_of(k) for k in keys])
    if not fit:
        return None
    return {'alpha': fit['alpha'], 't_alpha': fit['t_alpha'], 'annualised': fit['alpha'] * c8.DAYS_PER_YEAR,
            'coef': dict(zip(['alpha'] + names, fit['coef']))}


def build_days(windows, plan, cohorts_by_w, listed, coin_to_sym, exclude=()):
    days, win_of = [], {}
    for wi in range(len(windows)):
        top, bottom = cohorts_by_w[wi]
        top = [a for a in top if a not in exclude]
        bottom = [a for a in bottom if a not in exclude]
        for i, path, t_s in plan:
            if i != wi:
                continue
            entry = c8.entry_of(t_s)
            days.append((entry, c8.invert(c8.signals_for(path, top, bottom, listed, coin_to_sym, entry))))
            win_of[entry] = wi
    days.sort(key=lambda x: x[0])
    return days, win_of


def compute(data_dir, end, p0_wallets=(), snap_paths=None):
    """Everything after the started row: cohorts, inverted signals, books, the decision and the reported
    blocks. Pure function of the files under data_dir and the pinned snapshot list (deterministic)."""
    from ct import crowd, stats
    import p5_population as pop
    from ct.regimes import Regimes
    data_dir = Path(data_dir).resolve()
    if data_dir == REAL_DATA and _GATE['pair'] != (HERE, REAL_DATA):
        raise SystemExit('read lock: compute() on the real data tree runs only inside run(), after the real guard')
    if snap_paths is None:
        raise SystemExit('compute() needs the pinned snapshot list from the fetch manifest')
    bind_data(data_dir)
    table_all = json.loads((data_dir / 'p0/2026-10-07/symbols.json').read_text())
    table = {c: r for c, r in table_all.items() if r['status'] == 'ok'}
    coin_to_sym = {c: r['symbol'] for c, r in table.items()}
    listed = json.loads((data_dir / 'derived/bitget_listing.json').read_text())
    vaults = pop.vault_addresses()
    reg = Regimes()
    store = data_dir / 'r008'
    bound = c8.read_bound(end)
    closes = c8.Closes.from_store(store / 'bitget_1h', max_ts=bound)
    funding = c8.Funding.from_store(store / 'funding', coin_to_sym, max_ts=bound)
    unavailable = json.loads((store / 'report.json').read_text()).get('unavailable', {}) \
        if (store / 'report.json').exists() else {}
    windows = c8.grid_windows(end)
    plan = c8.signal_snapshots(snap_paths, end)
    counts = {'windows': [], 'signal_days_planned': len(plan), 'unavailable': {k: len(v) for k, v in unavailable.items()}}
    cohorts_by_w = []
    for w in windows:
        top, bottom, cnt = cohorts(w, reg, vaults)
        cohorts_by_w.append((top, bottom))
        counts['windows'].append({'selection': w[0].isoformat(), **cnt})
    days_all, win_all = build_days(windows, plan, cohorts_by_w, listed, coin_to_sym)
    # A-m6: a window with a cohort under 20 or a median breadth under 10 coins is excluded (fixed before returns)
    excluded = set()
    for wi, w in enumerate(windows):
        widths = [len(s) for k, s in days_all if win_all[k] == wi]
        med = statistics.median(widths) if widths else 0
        counts['windows'][wi]['median_signal_coins'] = med
        if len(cohorts_by_w[wi][0]) < c8.MIN_COHORT or med < c8.MIN_MEDIAN_COINS:
            excluded.add(wi)
        counts['windows'][wi]['excluded'] = wi in excluded
    days = [(k, s) for k, s in days_all if win_all[k] not in excluded]
    win_of = {k: win_all[k] for k, _ in days}
    counts['signal_days'] = len(days)
    counts['signal_symbol_days'] = sum(len(sig) for _, sig in days)
    counts['signal_symbol_days_without_return'] = sum(1 for k, sig in days for s in sig if closes.ret(s, k) is None)
    gross = c8.book(days, closes, funding, 0.0)
    net = c8.book(days, closes, funding, c8.B_COST)
    keys = [k for k, _ in gross]
    fac = c8.day_factors(days, closes)
    if any(fac[k]['btc'] is None for k in keys):
        raise SystemExit('BTCUSDT return missing on a book day — the fetch coverage should have refused')
    counts['alt_index_missing_days'] = sum(1 for k in keys if fac[k]['alt'] is None)
    counts['reversal_factor_zero_days'] = sum(1 for k in keys if fac[k]['rev_missing'])
    for k in keys:
        if fac[k]['alt'] is None:
            fac[k]['alt'] = 0.0
    cells = c8.beta_cells(keys, win_of)
    passed, num = c8.confirm([v for _, v in gross], keys, fac, cells)
    mean_cost = statistics.fmean(g - n_ for (_, g), (_, n_) in zip(gross, net)) if gross else None
    econ = c8.economic(num.get('alpha'), mean_cost)
    decision = c8.decide(passed, econ)
    # ---------------- reported, decides nothing
    btc = {k: fac[k]['btc'] for k in keys}
    ic = c8.ic3(days, closes, funding)
    ic_v = [x for _, x in ic if x is not None]
    by_window, by_month = {}, {}
    for k, g in gross:
        by_window.setdefault(windows[win_of[k]][0].isoformat(), []).append((k, g))
        by_month.setdefault(c8.month_of(k), []).append((k, g))
    costs = {f'{cr:.4f}': factor_alpha(c8.book(days, closes, funding, cr), fac, cells) for cr in c8.REPORT_COSTS}
    orig_net = [(k, -g - (g - n_)) for (k, g), (_, n_) in zip(gross, net)]   # 001's own sign, 001's costs
    rng = random.Random(8008)
    plac_t = []
    for _ in range(PLACEBO_SEEDS):
        pdays = []
        for k, sig in days:
            syms = sorted(sig)
            vals = [sig[s] for s in syms]
            rng.shuffle(vals)
            pdays.append((k, dict(zip(syms, vals))))
        fa = factor_alpha(c8.book(pdays, closes, funding, 0.0), fac, cells)
        plac_t.append(fa['t_alpha'] if fa and fa['t_alpha'] is not None else 0.0)
    surv = survivorship(windows, plan, cohorts_by_w, table_all, listed, coin_to_sym, excluded)
    p0 = p0_exposure(windows, plan, cohorts_by_w, set(p0_wallets), excluded)
    no_p0 = None
    if p0_wallets:
        d2, w2 = build_days(windows, plan, cohorts_by_w, listed, coin_to_sym, exclude=set(p0_wallets))
        d2 = [(k, s) for k, s in d2 if w2[k] not in excluded]
        g2 = c8.book(d2, closes, funding, 0.0)
        k2 = [k for k, _ in g2]
        f2 = c8.day_factors(d2, closes)
        for k in k2:
            f2[k]['alt'] = 0.0 if f2[k]['alt'] is None else f2[k]['alt']
        ok2, n2 = c8.confirm([v for _, v in g2], k2, f2, c8.beta_cells(k2, {k: w2[k] for k in k2}))
        no_p0 = {'alpha': n2.get('alpha'), 't_alpha': n2.get('t_alpha'), 'criteria': n2.get('criteria'), 'would_confirm': ok2}
    reported = {
        'economic_point_and_shrunk': econ,
        'gross_book_raw': raw_summary(gross),
        'net_book_001_costs_raw': raw_summary(net),
        'draft_rule_btc_only_alpha': c8.ols_cluster([v for _, v in gross], [btc[k] for k in keys],
                                                    [c8.week_of(k) for k in keys]) if keys else None,
        'factor_alpha_with_5d_momentum': factor_alpha(gross, fac, cells, with_mom=True),
        'cost_sensitivity_one_way': costs,
        'original_sign_net_001': {**raw_summary(orig_net), 'factor': factor_alpha(orig_net, fac, cells)},
        'ic3_inverted': {'days': len(ic_v), 'mean': statistics.fmean(ic_v) if ic_v else None,
                         'nw_t_lag3': stats.newey_west_t(ic_v, 3) if ic_v else None},
        'by_window_raw': {w: raw_summary(v) for w, v in by_window.items()},
        'by_month_raw': {mth: raw_summary(v) for mth, v in sorted(by_month.items())},
        'breadth': crowd.breadth(days),
        'survivorship': surv,
        'p0': {**p0, 'confirm_without_p0_wallets': no_p0},
        'placebo_permuted_signal': {'seeds': PLACEBO_SEEDS,
                                    'share_t_at_or_above_actual': sum(1 for t in plac_t if num.get('t_alpha') is not None
                                                                      and t >= num['t_alpha']) / PLACEBO_SEEDS,
                                    'share_t_at_or_above_crit': sum(1 for t in plac_t if num.get('crit') is not None
                                                                    and t >= num['crit']) / PLACEBO_SEEDS},
    }
    t = num.get('t_alpha')
    reported['persistence_sensitive'] = {
        'flag': t is not None and num.get('crit') is not None and num['crit'] <= t < c8.PERSIST_T,
        'note': f'reported only: t in [t_(G-1)(0.95), {c8.PERSIST_T}) is where month-scale persistence could carry a '
                'confirm (§3, §10)'}
    return {'counts': counts, 'confirm': num, 'economic': econ, 'decision': decision, 'reported': reported,
            'survivorship_reading': c8.survivorship_flag(surv)}


def survivorship(windows, plan, cohorts_by_w, table_all, listed, coin_to_sym, excluded):
    """A-m4 / B-M4, reported: coins with >= 3 cohort holders that leave the signal only because of their
    2026-10-07 table status (or because the table lacks them), by status; where they would have ranked
    (long / short quintile of the full inverted signal); the cohort's and crowd's gross-notional share in
    them. No price is needed: survivorship is measured on positions."""
    from ct import crowd
    out = {}
    for wi, w in enumerate(windows):
        if wi in excluded:
            continue
        top, bottom = cohorts_by_w[wi]
        by_status, place, ranked = {}, {'long': 0, 'short': 0, 'middle': 0}, set()
        g_top = g_top_drop = g_bot = g_bot_drop = 0.0
        too_recent = 0
        for i, path, t_s in plan:
            if i != wi:
                continue
            entry = c8.entry_of(t_s)
            tl = {c: -v for c, v in crowd.tilts(path, top, crowd=bottom).items()}
            dropped = set()
            for coin in tl:
                st = table_all.get(coin, {}).get('status', 'not_in_table')
                if st != 'ok':
                    by_status[st] = by_status.get(st, 0) + 1
                    dropped.add(coin)
                elif not (listed.get(coin_to_sym[coin]) and listed[coin_to_sym[coin]] < entry - c8.LISTED_MIN_DAYS * DAY):
                    too_recent += 1
            longs, shorts = crowd.quintiles(tl)
            for coin in dropped:
                where = 'long' if coin in longs else 'short' if coin in shorts else 'middle'
                place[where] += 1
                if where != 'middle':
                    ranked.add(coin)
            gg = group_gross(path, set(top) | set(bottom))
            ts, bs = set(top), set(bottom)
            for (u, mkt), g in sorted(gg.items()):      # fixed order: float sums are deterministic
                if u in ts:
                    g_top += g
                    g_top_drop += g if mkt in dropped else 0.0
                if u in bs:
                    g_bot += g
                    g_bot_drop += g if mkt in dropped else 0.0
        out[w[0].isoformat()] = {'dropped_coin_days_by_status': by_status, 'listed_under_31_days_coin_days': too_recent,
                                 'dropped_would_rank': place, 'dropped_coins_ranked': len(ranked),
                                 'cohort_gross_share_in_dropped': g_top_drop / g_top if g_top else None,
                                 'crowd_gross_share_in_dropped': g_bot_drop / g_bot if g_bot else None}
    return out


def p0_exposure(windows, plan, cohorts_by_w, p0, excluded):
    """B-m7, reported: P0 wallets in each cohort / crowd, by count and by gross-notional share on book days."""
    out = {}
    for wi, w in enumerate(windows):
        if wi in excluded:
            continue
        top, bottom = set(cohorts_by_w[wi][0]), set(cohorts_by_w[wi][1])
        gt = gtp = gb = gbp = 0.0
        for i, path, _ in plan:
            if i != wi:
                continue
            for (u, _), g in sorted(group_gross(path, top | bottom).items()):
                if u in top:
                    gt += g
                    gtp += g if u in p0 else 0.0
                if u in bottom:
                    gb += g
                    gbp += g if u in p0 else 0.0
        out[w[0].isoformat()] = {'in_cohort': len(top & p0), 'in_crowd': len(bottom & p0),
                                 'cohort_gross_share': gtp / gt if gt else None, 'crowd_gross_share': gbp / gb if gb else None}
    return out


def run(root=HERE, data_dir=None, now_ms=None, offline=True, **guard_kw):
    """The holdout read, once."""
    root = Path(root).resolve()
    data_dir = Path(data_dir).resolve() if data_dir else root / 'data'
    over = set(guard_kw) | ({'now_ms'} if now_ms is not None else set()) | ({'offline'} if not offline else set())
    pre = real_tree_failures(root, data_dir, over)
    if pre:
        raise SystemExit('\n'.join(pre))
    failures, ctx = guard_failures(root, 'run', now_ms=now_ms, **guard_kw)
    if failures:
        raise SystemExit('\n'.join(failures))
    end = ctx['end']
    snap_paths = [root / p for p in ctx['snapshots']]
    bind_data(data_dir)
    closes = c8.Closes.from_store(data_dir / 'r008' / 'bitget_1h', max_ts=c8.read_bound(end))
    listed = json.loads((data_dir / 'derived/bitget_listing.json').read_text())
    syms = sorted({r['symbol'] for r in json.loads((data_dir / 'p0/2026-10-07/symbols.json').read_text()).values()
                   if r['status'] == 'ok'})
    plan_keys = [c8.entry_of(t) for _, _, t in c8.signal_snapshots(snap_paths, end)]
    cov = coverage(closes, plan_keys, syms, listed)
    cov['vault_files'] = len(list((data_dir / 'cache' / 'hl-vaults').glob('*.json')))
    print('coverage before the read:', json.dumps(cov))
    if cov['btc_days_missing']:
        raise SystemExit('refusing before the read: BTCUSDT closes missing on planned book days')
    if cov['vault_files'] != 1:
        raise SystemExit('refusing before the read: p5_population.vault_addresses needs exactly one vault list file')
    if offline:
        lock_network()
    _GATE['pair'] = (root, data_dir)
    code = guard_kw.get('code', GUARDED_CODE)
    restarts = sum(1 for _, r in result_rows(root) if r.get('mode') == 'holdout_started')
    env = env_fingerprint()
    base = {'registration': REG_NAME, 'registration_commit': ctx['commit'], 'fetch_commit': ctx['fetch_commit'],
            'fetched_row_sha256': ctx['fetched_row_sha256'], 'owner': {'end_date': ctx['end_date']}}
    append_row(root, {**base, 'mode': 'holdout_started', 'restart_number': restarts, 'env': env,
                      'code_hashes': hashes(root, code), 'run_at': dt.datetime.now(dt.UTC).isoformat()})
    commit_results(root, '008: holdout_started (automatic, r008_run.py)')
    p0 = []
    rep = data_dir / 'p0/2026-10-07/report.json'
    if rep.exists():
        p0 = [w['user'] for w in json.loads(rep.read_text()).get('wallets', []) if isinstance(w, dict)]
    body = compute(data_dir, end, p0, snap_paths)
    row = {**base, 'mode': 'holdout', 'code_hashes': hashes(root, code), 'run_at': dt.datetime.now(dt.UTC).isoformat(),
           'env': env, 'coverage': cov, 'restarts_after_mechanical_stop': restarts,
           'params': {'b_cost_one_way': c8.B_COST, 'horizon_days': c8.HORIZON_D, 'q': c8.Q, 'shrink': c8.SHRINK,
                      'min_clusters': c8.MIN_CLUSTERS, 'bar_annual': c8.BAR_ANNUAL, 'trip_floor': c8.TRIP_FLOOR,
                      'min_cell_days': c8.MIN_CELL_DAYS, 'min_cohort': c8.MIN_COHORT,
                      'min_median_coins': c8.MIN_MEDIAN_COINS,
                      'windows': [[w[0].isoformat(), w[1].isoformat(), w[2].isoformat()] for w in c8.grid_windows(end)]},
           **body}
    dec, fails = c8.recheck(row)
    if fails or dec != row['decision']:
        row['recheck_failed'] = fails
        append_row(root, row)
        commit_results(root, '008: holdout row, RECHECK FAILED (automatic)')
        raise SystemExit('internal: the decision does not recompute from its numbers. The row is stored with '
                         'recheck_failed; nothing is printed. The orchestrator ledgers it.')
    append_row(root, row)
    commit_results(root, '008: holdout row (automatic, r008_run.py)')
    print(json.dumps({k: row[k] for k in ('counts', 'confirm', 'economic')}, indent=1, default=str))
    print('survivorship reading (§5, reported):', json.dumps(row['survivorship_reading']))
    print('persistence-sensitive (reported):', row['reported']['persistence_sensitive']['flag'])
    print('decision:', row['decision'])
    return row


def env_record(root=HERE):
    """Recovery record (review r1, m3): compare today's environment with the fetched row's and commit the
    comparison as a row. It reads no data; the read itself still requires equality (g2)."""
    rows = [r for _, r in result_rows(root) if r.get('mode') == 'holdout_fetched']
    env = env_fingerprint()
    row = {'registration': REG_NAME, 'mode': 'holdout_env_record', 'env': env,
           'equal_to_fetched': bool(rows) and rows[0].get('env') == env, 'run_at': dt.datetime.now(dt.UTC).isoformat()}
    append_row(root, row)
    commit_results(root, '008: environment record (automatic, r008_run.py --env-record)')
    print(json.dumps(row, indent=1))
    return row


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--check', action='store_true')
    g.add_argument('--run', action='store_true')
    g.add_argument('--env-record', action='store_true')
    args = ap.parse_args()
    if args.env_record:
        env_record()
        return
    if args.check:
        failures, ctx = guard_failures(HERE, 'run')
        print('\n'.join(failures) if failures else f"guard open: end {ctx['end_date']}")
        return
    run()


if __name__ == '__main__':
    main()
