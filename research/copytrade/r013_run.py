"""Registration 013 (N3 i) — short new Binance USDⓈ-M perps: the guarded run (Revision 1).

    .venv/bin/python -I r013_run.py --discovery      # T1, T2 on listings 2024-01-01 -> 2025-07-18 (+ the reported 2020-23 block)
    .venv/bin/python -I r013_run.py --confirmation   # the carried trial on 2025-07-28 -> 2026-06-21, once, after approval
    .venv/bin/python -I r013_run.py --power          # SYNTHETIC size and power (reads only data/r013/schedule.json)

Every outcome mode refuses unless (the guard; Revision 1 additions marked +):
  status  the registration says REGISTERED;  (a)-(c) tracked, committed, unmodified against HEAD;
  (d)     the guarded code is clean;  (e) code AND data match the §10 SHA-256 table;
  (h)     D17: a committed, recomputed 009 terminal state exists;
  + (k)   results.jsonl is clean against HEAD (an uncommitted row is never "forgotten");
  + (w)   git's top level is this repository and the anchor commit is an ancestor of HEAD; GIT_* variables
          are stripped from every git call (a nested `git init` or GIT_DIR cannot hide history);
  + (v)   no untracked *.py at the top level of research/copytrade or in ct/ (no shadowed module), duckdb and
          pyarrow load from the venv, `python -O` refused, python/duckdb/pyarrow versions equal the pins;
discovery: (f) no row of any registration whose name starts '013-', in the working tree or in
          `git log --all --reflog -m -p` (+ reflog and merge diffs: reset, amend and stash cannot erase a row);
confirmation: (g1) exactly one 013 discovery row; (s) exactly one committed discovery_started row with the same
  registration commit and the same pinned versions; (g2) committed; (g3) the registration commit is an ancestor
  of HEAD, the code is unchanged since, the stored hashes equal the table, and the registration changed only by
  an added approval line; (g4) the decision RECOMPUTED from the row's per-event records and numbers is
  read_confirmation; (h') `Confirmation: APPROVED <date> row <sha256 of the discovery row line>` committed
  strictly after the row; (i) no confirmation_started / confirmation / confirmation_failed row anywhere.
Reads: `Panel` opens the real data only when the real `guard()` has passed for the same mode in this process
(a gate token; a refusal, not an assert). The runner commits results.jsonl alone (`git commit --only`) right
after each started, failed and result row. A failure after a started row writes a `<mode>_failed` row: a
terminal state.
"""
import datetime as dt
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import re
import statistics
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from ct import listings013 as L  # noqa: E402

Refused = L.Refused
REPO = HERE.parent.parent
REG = HERE / 'registrations' / '013-new-perp-listings.md'
REG_PREFIX = '013-'
RESULTS = HERE / 'results.jsonl'
DATA = HERE / 'data' / 'r013'
DATA_REAL = (HERE / 'data' / 'r013').resolve()          # never reassigned: the gate compares against it
ANCHOR = '72124e2c91cf8540b9e4fcba135da7e69d369f37'      # "Tests: state checks updated ..." (2026-10-12), (w)
PINS = {'python': '3.14.8', 'duckdb': '1.5.6', 'pyarrow': '25.0.1'}
ALLOWED = {'discovery': ('discovery', 'reported'), 'confirmation': ('confirmation', 'discovery', 'reported')}
PANEL_FILES = {'reported': ('events.json', 'symbols.json', 'bars_1m.parquet', 'funding.parquet', 'coverage.json'),
               'discovery': ('events.json', 'symbols.json', 'bars_1m.parquet', 'funding.parquet', 'market_1h.parquet',
                             'coverage.json'),
               'confirmation': ('events.json', 'symbols.json', 'bars_1m.parquet', 'funding.parquet', 'market_1h.parquet',
                                'coverage.json')}
GUARDED_CODE = ('ct/__init__.py', 'ct/listings013.py', 'ct/factors.py', 'ct/stats.py', 'r013_run.py', 'r013_build.py',
                'r013_fetch.py')
GUARDED_DATA = tuple(f'data/r013/panel_{c}/{f}' for c, fs in PANEL_FILES.items() for f in fs) + (
    'data/r013/MANIFEST.json', 'data/r013/schedule.json', 'data/r013/later_segments.json',
    'data/r013/listing/um_daily_1d_days.json', 'data/r013/listing/event_keys.json',
    'data/r013/inputs/fapi_exchangeInfo.json', 'data/r013/raw_klines1m/SHA256SUMS', 'data/r013/raw_funding/SHA256SUMS',
    'data/r013/raw_klines1h/SHA256SUMS')
HASH_ROW = re.compile(r'^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|', re.M)
APPROVAL = re.compile(r'^Confirmation: APPROVED (\d{4}-\d{2}-\d{2}) row ([0-9a-f]{64})\s*$', re.M)
REG_009 = '009-binance-weekly-factors.md'
_GATE = None                                              # (repo dir, mode, commit), set only by guard()


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


def is_013(row):
    return isinstance(row, dict) and str(row.get('registration', '')).startswith(REG_PREFIX)


def result_rows(root, results_rel, match=is_013):
    """[(line, row)] of every distinct results line matching `match`, from the working tree and every ADDED line in
    `git log --all --reflog -m -p` (every branch, the reflog, merge and stash diffs). For BLOCKING checks."""
    texts = []
    res = Path(root) / results_rel
    if res.exists():
        texts.extend(res.read_text().splitlines())
    hist = _git(root, 'log', '--all', '--reflog', '-m', '-p', '--format=', '--', results_rel)
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


def head_rows(root, results_rel, name):
    out = []
    for line in _git(root, 'show', f'HEAD:./{results_rel}').stdout.splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict) and row.get('registration') == name:
            out.append((line, row))
    return out


def first_commit_containing(root, rel, needle):
    for c in _git(root, 'log', '--format=%H', '--reverse', '--', rel).stdout.split():
        if needle in _git(root, 'show', f'{c}:./{rel}').stdout.splitlines():
            return c
    return None


def is_ancestor(root, c):
    return bool(c) and _git(root, 'merge-base', '--is-ancestor', c, 'HEAD').returncode == 0


def versions():
    return {'python': '.'.join(map(str, sys.version_info[:3])), 'duckdb': importlib.metadata.version('duckdb'),
            'pyarrow': importlib.metadata.version('pyarrow')}


# ------------------------------------------------------------------ (h) D17: 009's terminal state, recomputed
def ordering_failures(root, results_rel='results.jsonl'):
    from ct import factors as fx                  # 009's frozen module, imported read-only (hashed in §10)
    rows = [(l, r) for l, r in head_rows(root, results_rel, REG_009) if is_ancestor(root, r.get('registration_commit'))]
    disc = [(l, r) for l, r in rows if r.get('mode', 'discovery') == 'discovery']
    msg = ['(h) D17: no committed, recomputed 009 terminal state in HEAD — 013 may not read 009 holdout-A data']
    if len(disc) != 1:
        return msg
    line, row = disc[0]
    sha = line_sha(line)
    if any(r.get('mode') in ('holdout_A', 'holdout_A_declined') and r.get('discovery_row_sha256') == sha for _, r in rows):
        return []
    try:
        dec, fails = fx.recheck_discovery(row.get('results') or {})
    except Exception:  # noqa: BLE001 - a malformed row unlocks nothing
        return msg
    if not fails and [dec[0], list(dec[1])] == row.get('decision') and dec[0] in ('close', 'close_conditions_unmet'):
        return []
    return msg


# ------------------------------------------------------------------ (v) environment
def environment_failures(root):
    out = []
    if sys.flags.optimize:
        out.append('(v) python -O / -OO is refused')
    if versions() != PINS:
        out.append(f'(v) versions {versions()} differ from the pins {PINS}')
    for mod in ('duckdb', 'pyarrow'):
        spec = importlib.util.find_spec(mod)
        origin = Path(spec.origin).resolve() if spec and spec.origin else None
        if origin is None or not origin.is_relative_to(Path(sys.prefix).resolve()):
            out.append(f'(v) {mod} does not load from the interpreter prefix {sys.prefix}: {origin}')
    untracked = _git(root, 'ls-files', '--others', '--exclude-standard', '--', ':(glob)*.py', ':(glob)ct/*.py').stdout.split()
    if untracked:
        out.append(f'(v) untracked python files could shadow imports: {untracked}')
    return out


# ------------------------------------------------------------------ the guard
def guard_failures(root, reg_rel, code=GUARDED_CODE, data=GUARDED_DATA, results_rel='results.jsonl', mode='discovery',
                   toplevel=None, anchor=None, env_check=False):
    root = Path(root)
    reg = root / reg_rel
    out = list(ordering_failures(root, results_rel))
    text = reg.read_text() if reg.exists() else ''
    m = re.search(r'^\*\*Status: (\w+)', text, re.M)
    if not m or m.group(1) != 'REGISTERED':
        out.append(f'{reg.name} is not REGISTERED — refusing to compute any outcome.')
    if _git(root, 'ls-files', '--error-unmatch', '--', reg_rel).returncode:
        out.append(f'(a) {reg.name} is not tracked by git')
    if _git(root, 'diff', '--quiet', 'HEAD', '--', reg_rel).returncode:
        out.append(f'(b) {reg.name} differs from the committed version')
    commit = _git(root, 'log', '-1', '--format=%H', '--', reg_rel).stdout.strip()
    if not commit:
        out.append(f'(c) {reg.name} has no commit')
    dirty = _git(root, 'status', '--porcelain', '--', *code).stdout.strip()
    if dirty:
        out.append(f'(d) guarded code not clean: {dirty.splitlines()}')
    table = dict(HASH_ROW.findall(text))
    for f in (*code, *data):
        want = table.get(f)
        if want is None:
            out.append(f'(e) {f} missing from the registration hash table')
        elif not (root / f).exists() or sha256(root / f) != want:
            out.append(f'(e) {f} does not match its registered SHA-256')
    if _git(root, 'status', '--porcelain', '--', results_rel).stdout.strip():
        out.append(f'(k) {results_rel} is not clean against HEAD — commit or explain every row first')
    if toplevel is not None:
        top = _git(root, 'rev-parse', '--show-toplevel').stdout.strip()
        if not top or Path(top).resolve() != Path(toplevel).resolve():
            out.append(f'(w) git top level is {top!r}, not {toplevel}')
    if anchor is not None and not is_ancestor(root, anchor):
        out.append(f'(w) the anchor commit {anchor} is not an ancestor of HEAD')
    if env_check:
        out += environment_failures(root)
    rows = result_rows(root, results_rel)
    if mode == 'discovery':
        if rows:
            out.append(f'(f) a 013 row exists (working tree, any branch, reflog or stash) — discovery runs once')
        return out, commit
    if mode != 'confirmation':
        raise Refused(f'unknown mode {mode}')
    head_lines = set(_git(root, 'show', f'HEAD:./{results_rel}').stdout.splitlines())
    d = [(l, r) for l, r in rows if r.get('mode') == 'discovery']
    if len(d) != 1:
        out.append(f'(g1) {len(d)} 013 discovery rows anywhere; exactly one is required')
        return out, commit
    line, row = d[0]
    if line not in head_lines:
        out.append('(g2) the discovery row is not committed in HEAD:results.jsonl')
    rc = row.get('registration_commit') or ''
    st = [(l, r) for l, r in rows if r.get('mode') == 'discovery_started']
    if len(st) != 1 or st[0][0] not in head_lines or st[0][1].get('registration_commit') != rc:
        out.append('(s) exactly one committed discovery_started row with the same registration commit is required')
    elif env_check and st[0][1].get('versions') != versions():
        out.append('(s) the pinned versions differ from those of the discovery run')
    if not is_ancestor(root, rc):
        out.append("(g3) the discovery row's registration commit is not an ancestor of HEAD")
    else:
        if _git(root, 'diff', '--quiet', rc, 'HEAD', '--', *code).returncode:
            out.append('(g3) the guarded code changed since the discovery run')
        changed = [l[1:] for l in _git(root, 'diff', '-U0', rc, 'HEAD', '--', reg_rel).stdout.splitlines()
                   if l[:1] in '+-' and not l.startswith(('+++', '---'))]
        if any(l.strip() and not APPROVAL.fullmatch(l.rstrip()) for l in changed):
            out.append('(r) the registration changed since the discovery run beyond an added approval line')
    if row.get('code_hashes') != {f: sha256(root / f) for f in code if (root / f).exists()}:
        out.append('(g3) the code hashes stored in the discovery row differ from the code on disk')
    if row.get('data_hashes') != {f: table.get(f) for f in data}:
        out.append('(g3) the data hashes stored in the discovery row differ from the registration table')
    dec, fails = L.recheck_discovery(row)
    out.extend(f'(g4) {x}' for x in fails)
    if dec[0] != 'read_confirmation':
        out.append(f'(g4) the decision recomputed from the records and numbers is {dec}, not read_confirmation')
    appr = [a for a in APPROVAL.finditer(text) if a.group(2) == line_sha(line)]
    if not appr:
        out.append("(h') no committed line 'Confirmation: APPROVED <date> row <sha256 of the discovery row>'")
    else:
        row_c = first_commit_containing(root, results_rel, line)
        app_c = first_commit_containing(root, reg_rel, appr[0].group(0).rstrip())
        if not row_c or not app_c or row_c == app_c or _git(root, 'merge-base', '--is-ancestor', row_c, app_c).returncode:
            out.append("(h') the approval must be committed strictly after the commit that added the discovery row")
    if any(r.get('mode') in ('confirmation_started', 'confirmation', 'confirmation_failed') for _, r in rows):
        out.append('(i) the confirmation has already been started or read — it is read once')
    return out, commit


def guard(mode):
    """The ONLY way to open the real data: the real repository, default arguments, every check. Sets the gate."""
    global _GATE
    _GATE = None
    failures, commit = guard_failures(HERE, str(REG.relative_to(HERE)), mode=mode, toplevel=REPO,
                                      anchor=ANCHOR, env_check=True)
    if failures:
        sys.exit('\n'.join(failures))
    _GATE = (str(HERE.resolve()), mode, commit)
    return commit


def append_row(row, results=None):
    line = json.dumps(row, default=str, sort_keys=True)
    with open(results or RESULTS, 'a') as f:
        f.write(line + '\n')
    return line


def commit_results(msg):
    """Commit results.jsonl ALONE (`git commit --only`), right after a row is appended (review B M3)."""
    r = _git(RESULTS.parent, 'commit', '--only', RESULTS.name, '-m', msg)
    if r.returncode:
        raise Refused(f'could not commit {RESULTS.name}: {r.stderr.strip()[:300]} — stop; the row stays uncommitted '
                      'and (k) refuses every later run until the owner commits it')


HOOKS = {'commit': commit_results}


def hashes(files, root=HERE):
    return {f: sha256(root / f) for f in files if (root / f).exists()}


# ------------------------------------------------------------------ the panel (gated)
class Panel:
    """One calendar's panel. The real data opens only under the gate set by `guard()` for the same mode;
    synthetic panels (tests) pass their own data_root outside data/r013."""

    def __init__(self, cal, mode, data_root=None):
        import duckdb
        root = Path(data_root or DATA).resolve()
        if root == DATA_REAL or root.is_relative_to(DATA_REAL) or DATA_REAL.is_relative_to(root):
            if _GATE is None or _GATE[0] != str(HERE.resolve()) or _GATE[1] != mode:
                raise Refused('the real 013 panels open only after guard() passed for this mode in this process')
        if mode not in ALLOWED or cal not in ALLOWED[mode]:
            raise Refused(f'013 {mode} mode may not open the {cal} panel')
        self.dir = (root / f'panel_{cal}').resolve()
        if not (self.dir.is_relative_to(root) and self.dir.name == f'panel_{cal}'):
            raise Refused(f'bad panel path {self.dir}')
        self.cal, self.end = cal, L.DATA_END[cal]
        self.events = json.loads((self.dir / 'events.json').read_text())
        self.symbols = json.loads((self.dir / 'symbols.json').read_text())
        self.con = duckdb.connect()
        self.has_market = (self.dir / 'market_1h.parquet').exists()

    def bars(self, eid):
        return [tuple(r) for r in self.con.execute(
            f"SELECT t, o, h, l, c, qv, n FROM '{self.dir}/bars_1m.parquet' WHERE id = ? AND t < ? ORDER BY t",
            [eid, self.end]).fetchall()]

    def funding(self, eid):
        return [tuple(r) for r in self.con.execute(
            f"SELECT calc_time, interval_h, rate FROM '{self.dir}/funding.parquet' WHERE id = ? AND calc_time < ? "
            "ORDER BY calc_time", [eid, self.end]).fetchall()]

    def market(self, entry_t, exit_t, symbol):
        """5 %-trimmed equal-weight return of seasoned perps (first daily file >= 30 days before entry) that TRADED
        in the 24 h before the entry hour, from the last traded close before the entry hour to the last traded
        close before the exit hour (zero-trade rows are absent: a dead contract never enters, a perp that stops
        trading keeps its last traded price). None if fewer than 20 symbols."""
        if not self.has_market:
            return None
        a, b = entry_t - entry_t % L.HOUR, exit_t - exit_t % L.HOUR
        if b >= self.end:
            raise Refused('market read past the data end')
        day = L.day_of(entry_t - 30 * L.DAY)
        elig = {s for s, d in self.symbols.items() if d <= day and s != symbol}
        rows = self.con.execute(
            f"""SELECT symbol, arg_max(c, t) FILTER (WHERE t < ?), arg_max(c, t)
                FROM '{self.dir}/market_1h.parquet' WHERE n > 0 AND t >= ? AND t < ? GROUP BY symbol""",
            [a, a - 24 * L.HOUR, b]).fetchall()
        rs = [pb / pa - 1 for s, pa, pb in rows if s in elig and pa and pb]
        return L.trimmed_mean(rs) if len(rs) >= L.INDEX_MIN else None


# ------------------------------------------------------------------ outcomes (called only after the guard)
def evaluate(panel, with_market=True):
    recs = []
    counts = {k: 0 for k in ('events', 'premarket', 'continuation', 'no_first_trade', 'no_entry_primary',
                             'no_entry_other', 'liquidated', 'exit_late', 'exit_last_trade', 't2_undefined',
                             'partial_funding', 'no_funding_rows', 'market_missing')}
    counts['events'] = len(panel.events)
    for e in sorted(panel.events, key=lambda e: e['id']):
        prim = not e['premarket'] and not e.get('continuation')
        counts['premarket'] += e['premarket']
        counts['continuation'] += bool(e.get('continuation'))
        if e['first_trade'] is None:
            counts['no_first_trade'] += 1
            continue
        bars, fund = panel.bars(e['id']), panel.funding(e['id'])
        ev = {'id': e['id'], 'first_trade': e['first_trade']}
        base = L.simulate(ev, bars, fund, panel.end)
        if base['status'] == 'no_entry':
            counts['no_entry_primary' if prim else 'no_entry_other'] += 1
            recs.append({'id': e['id'], 'primary': prim, 'premarket': e['premarket'], 'status': 'no_entry'})
            continue
        stress = L.simulate(ev, bars, fund, panel.end, slip_mult=L.STRESS['slip_mult'], pay_mult=L.STRESS['pay_mult'],
                            receive_mult=L.STRESS['receive_mult'])
        bn = L.simulate(ev, bars, fund, panel.end, fee=L.FEE_BINANCE)
        missing = set(e.get('funding_months_missing') or [])
        pre_months = set(L.months_between(e['first_trade'], base['entry_t']))
        hold_months = set(L.months_between(base['entry_t'], base['exit_t']))
        fpre = None if pre_months & missing else L.funding_before_entry(e['first_trade'], base['entry_t'], fund)
        mk = panel.market(base['entry_t'], base['exit_t'], e['symbol']) if with_market else None
        r = dict(base, symbol=e['symbol'], primary=prim, premarket=e['premarket'], continuation=e.get('continuation'),
                 spot_first=e['spot_first'], week=L.iso_week(e['first_trade']), year=L.day_of(e['first_trade'])[:4],
                 net_stress=stress['net'], net_binance_fee=bn['net'], f_pre=fpre, t2=fpre is not None and fpre >= 0,
                 mkt=mk, partial_funding=bool(hold_months & missing))
        recs.append(r)
        if not prim:
            continue
        counts['liquidated'] += base['status'] == 'liquidated'
        counts['exit_late'] += base['status'] == 'exit_late'
        counts['exit_last_trade'] += base['status'] == 'exit_last_trade'
        counts['t2_undefined'] += fpre is None
        counts['partial_funding'] += r['partial_funding']
        counts['no_funding_rows'] += not fund
        counts['market_missing'] += with_market and mk is None
    return recs, counts


def primary(recs):
    t1 = [r for r in recs if r.get('status') not in (None, 'no_entry') and r['primary']]
    return {'T1': t1, 'T2': [r for r in t1 if r['t2']]}


def span_years(cal):
    a, b = L.CALENDARS[cal]
    return (b - a) / L.YEAR


def summary(rows, key='net'):
    v = [r[key] for r in rows if r.get(key) is not None]
    if not v:
        return None
    m, t, G = L.clustered_t(v, [r['week'] for r in rows if r.get(key) is not None])
    return {'n': len(v), 'G': G, 'mean': m, 't': t, 'median': statistics.median(v)}


def alpha_summary(rows):
    adj = [r for r in rows if r.get('mkt') is not None]
    return L.ols_alpha([r['net'] for r in adj], [r['mkt'] for r in adj], [r['week'] for r in adj]) if adj else None


def reported_blocks(recs, cal, crit, trials=L.TRIALS):
    """Reported, decides nothing. No reported block may become a primary."""
    tr = primary(recs)
    t1 = tr['T1']
    comp = {k: (sum(r[k] for r in t1) / len(t1)) if t1 else None for k in ('price', 'funding', 'fees', 'slip')}
    other = [r for r in recs if r.get('status') not in (None, 'no_entry') and not r['primary']]
    return {
        'components_mean_all_events': comp,   # price absorbs the margin lost at liquidation
        'stressed_costs': {tn: summary([dict(r, net=r['net_stress']) for r in tr[tn]]) for tn in trials},
        'binance_fee_5bp': {tn: summary([dict(r, net=r['net_binance_fee']) for r in tr[tn]]) for tn in trials},
        'alpha_ols': {tn: alpha_summary(tr[tn]) for tn in trials},
        'by_year': {y: summary([r for r in t1 if r['year'] == y]) for y in sorted({r['year'] for r in t1})},
        'spot_first': summary([r for r in t1 if r['spot_first']]), 'perp_first': summary([r for r in t1 if not r['spot_first']]),
        'premarket_stratum': summary([r for r in other if r['premarket']]),
        'continuation_stratum': summary([r for r in other if r.get('continuation')]),
        'by_slippage_tier': {str(s): summary([r for r in t1 if r['slip_tier'] == s]) for s in sorted({r['slip_tier'] for r in t1})},
        'books': {tn: L.book(tr[tn], span_years(cal)) for tn in trials},
        'books_stressed': {tn: L.book([dict(r, net=r['net_stress']) for r in tr[tn]], span_years(cal)) for tn in trials},
        'placebo_month_flips': {tn: L.placebo(tr[tn], crit[tn]) for tn in trials if crit.get(tn) is not None},
    }


def compact(recs):
    keys = ('id', 'week', 'entry_t', 'exit_t', 'status', 'net', 'net_stress', 'primary', 'premarket', 'continuation',
            't2', 'mkt', 'liquidated', 'slip_tier')
    return [{k: r.get(k) for k in keys} for r in recs]


def _failed(mode, commit, err):
    append_row({'registration': REG.name, 'mode': f'{mode}_failed', 'registration_commit': commit, 'error': repr(err)[:500],
                'run_at': dt.datetime.now(dt.UTC).isoformat()})
    HOOKS['commit'](f'013 {mode}_failed')


def run_discovery():
    commit = guard('discovery')
    append_row({'registration': REG.name, 'mode': 'discovery_started', 'registration_commit': commit,
                'versions': versions(), 'run_at': dt.datetime.now(dt.UTC).isoformat()})
    HOOKS['commit']('013 discovery_started')
    try:
        out = _discovery_row(commit)
    except Exception as e:
        _failed('discovery', commit, e)
        raise
    append_row(out)
    HOOKS['commit']('013 discovery result')
    print(json.dumps({tn: {k: out['trials'][tn][k] for k in ('n', 'G', 'mean', 't', 'alpha', 'alpha_beta', 'alpha_t', 'alpha_crit',
                                                             'criteria', 'pass')} for tn in L.TRIALS}, indent=1, default=str))
    print('decision', out['decision'])


def _discovery_row(commit):
    panel = Panel('discovery', 'discovery')
    recs, counts = evaluate(panel)
    print('counts before the decision:', json.dumps(counts))
    tr = primary(recs)
    span = span_years('discovery')
    trials, crit = {}, {}
    for tn in L.TRIALS:
        s = L.trial_stats(tr[tn], span, L.Q_DISCOVERY)
        c, ok, _cr = L.discovery_criteria(s)
        trials[tn] = {**s, 'criteria': c, 'pass': ok}
        crit[tn] = L.t_crit(s['G'] - 1, L.Q_DISCOVERY) if s['G'] >= 2 else None   # the raw placebo's reference
    decision = L.decide_discovery({tn: trials[tn]['pass'] for tn in L.TRIALS})
    rep = reported_blocks(recs, 'discovery', crit)
    rp = Panel('reported', 'discovery')
    rrecs, rcounts = evaluate(rp, with_market=False)
    rtr = primary(rrecs)
    return {'registration': REG.name, 'mode': 'discovery', 'registration_commit': commit,
            'run_at': dt.datetime.now(dt.UTC).isoformat(), 'versions': versions(),
            'code_hashes': hashes(GUARDED_CODE), 'data_hashes': hashes(GUARDED_DATA), 'span_years': span,
            'params': {'calendar': [L.day_of(x) for x in L.CALENDARS['discovery']], 'q': L.Q_DISCOVERY,
                       'cal_alpha': L.CAL_ALPHA_DISCOVERY, 'entry_delay_h': L.ENTRY_DELAY / L.HOUR,
                       'hold_d': L.HOLD / L.DAY, 'fee': L.FEE_DECISION, 'mmr': L.MMR, 'c_event': L.C_EVENT},
            'counts': counts, 'trials': trials, 'decision': decision,
            'reported': {**rep, 'block_2020_2023': {'counts': rcounts, 'T1': summary(rtr['T1']), 'T2': summary(rtr['T2']),
                                                    'stressed': summary([dict(r, net=r['net_stress']) for r in rtr['T1']]),
                                                    'continuation_stratum': summary([r for r in rrecs if r.get('continuation')
                                                                                     and r.get('status') not in (None, 'no_entry')]),
                                                    'by_year': {y: summary([r for r in rtr['T1'] if r['year'] == y])
                                                                for y in sorted({r['year'] for r in rtr['T1']})}}},
            'events': compact(recs)}


def run_confirmation():
    commit = guard('confirmation')
    rows_all = result_rows(RESULTS.parent, RESULTS.name)
    (dline, drow), = [(l, r) for l, r in rows_all if r.get('mode') == 'discovery']
    dec, _ = L.recheck_discovery(drow)
    carried = dec[1]
    append_row({'registration': REG.name, 'mode': 'confirmation_started', 'registration_commit': commit,
                'discovery_row_sha256': line_sha(dline), 'carried': carried, 'versions': versions(),
                'run_at': dt.datetime.now(dt.UTC).isoformat()})
    HOOKS['commit']('013 confirmation_started')
    try:
        out = _confirmation_row(commit, dline, carried)
    except Exception as e:
        _failed('confirmation', commit, e)
        raise
    append_row(out)
    HOOKS['commit']('013 confirmation result')
    print(json.dumps({k: out['stats'][k] for k in ('alpha_n', 'alpha_G', 'alpha', 'alpha_t', 'alpha_crit', 'mean', 't')},
                     indent=1, default=str))
    print('confirm', out['confirm'], 'deploy', json.dumps(out['deploy'], default=str), 'decision', out['decision'])


def _confirmation_row(commit, dline, carried):
    pooled, conf = {}, None
    for cal in ('reported', 'discovery', 'confirmation'):
        p = Panel(cal, 'confirmation')
        recs, counts = evaluate(p, with_market=(cal != 'reported'))
        rows = primary(recs)[carried]
        pooled[cal] = (rows, [dict(r, net=r['net_stress']) for r in rows], span_years(cal))
        if cal == 'confirmation':
            conf = (recs, counts, rows)
            print('counts before the decision:', json.dumps(counts))
    recs, counts, rows = conf
    s = L.trial_stats(rows, span_years('confirmation'), L.Q_CONFIRM)
    ok, cr = L.confirm(s)
    rcrit = L.t_crit(s['G'] - 1, L.Q_CONFIRM) if s['G'] >= 2 else None
    dep = L.deploy(rows, pooled['confirmation'][1], pooled, span_years('confirmation'))
    return {'registration': REG.name, 'mode': 'confirmation', 'registration_commit': commit,
            'discovery_row_sha256': line_sha(dline), 'carried': carried, 'run_at': dt.datetime.now(dt.UTC).isoformat(),
            'versions': versions(), 'code_hashes': hashes(GUARDED_CODE), 'data_hashes': hashes(GUARDED_DATA),
            'span_years': span_years('confirmation'), 'counts': counts, 'stats': {**s, 'alpha_crit': cr}, 'confirm': ok,
            'deploy': dep, 'decision': L.decide_confirmation(ok, dep),
            'reported': {**reported_blocks(recs, 'confirmation', {carried: rcrit}, trials=(carried,)),
                         'pooled_liquidation_rate': dep['pi_pool'], 'pooled_counts': {c: len(v[0]) for c, v in pooled.items()}},
            'events': compact(recs)}


# ------------------------------------------------------------------ synthetic size and power (no outcome data)
def schedule(cal, path=None):
    """Primary events' first-trade times (pre-entry facts) from data/r013/schedule.json."""
    sch = json.loads(Path(path or DATA / 'schedule.json').read_text())
    return [e['first_trade'] for e in sch[cal] if e['first_trade'] and not e['premarket'] and not e['continuation']]


WORLDS = [
    # alt market SD fixed at 10.4 % per 7 days (msd 1; a 1.5 stress); k scales the idiosyncratic part (listing windows)
    *({'name': f'null k{k} b{b} drift{d}', 'mu_d': 0.0, 'mu_c': 0.0, 'beta': b, 'k': k, 'drift_d': d, 'drift_c': d}
      for k in (1.0, 2.0) for b in (1.0, 1.5, 2.0) for d in (0.0, 0.01, 0.02)),
    *({'name': f'null k{k} b2 drift0.02 msd1.5', 'mu_d': 0.0, 'mu_c': 0.0, 'beta': 2.0, 'k': k, 'drift_d': 0.02,
       'drift_c': 0.02, 'msd': 1.5} for k in (1.0, 2.0)),
    *({'name': f'decayed b{b} cdrift{d}', 'mu_d': 0.04, 'mu_c': 0.0, 'beta': b, 'k': 1.5, 'drift_d': 0.0, 'drift_c': d}
      for b in (1.0, 1.5, 2.0) for d in (0.0, 0.01, 0.02)),
    {'name': 'null squeeze atoms p2%', 'mu_d': 0.0, 'mu_c': 0.0, 'beta': 1.5, 'k': 1.5, 'drift_d': 0.0, 'drift_c': 0.0,
     'liq_p': 0.02},
    {'name': 'null squeeze weeks 3%', 'mu_d': 0.0, 'mu_c': 0.0, 'beta': 1.5, 'k': 1.5, 'drift_d': 0.0, 'drift_c': 0.0,
     'liq_week_p': 0.03},
    *({'name': f'power mu{mu} k{k}', 'mu_d': mu, 'mu_c': mu, 'beta': 1.5, 'k': k, 'drift_d': 0.0, 'drift_c': 0.0}
      for k in (1.0, 1.5, 2.0) for mu in (0.02, 0.04, 0.06, 0.08)),
    {'name': 'power mu0.04 k1.5 drift1', 'mu_d': 0.04, 'mu_c': 0.04, 'beta': 1.5, 'k': 1.5, 'drift_d': 0.01, 'drift_c': 0.01},
]
NULL_PREFIX = 'null k'                                   # the calibration set (beta / drift / market worlds; not squeeze)


def power(sims=1500, worlds=None, path=None, keep=None):
    """Synthetic size, calibration and power from the same draws. Prints, per world, the rates at the uncalibrated
    t_{G-2}(q) and at the registered critical values, and the alpha-t quantiles used for calibration."""
    dsch, csch = schedule('discovery', path), schedule('confirmation', path)
    res = L.simulate_worlds(dsch, csch, span_years('discovery'), span_years('confirmation'), worlds or WORLDS, sims=sims)
    out = []
    for w in res:
        recs = w.pop('records')
        if keep is not None:
            keep[w['name']] = recs
        row = {**w, 'uncalibrated': L.world_rates(recs, 0.0, 0.0),
               'registered': L.world_rates(recs, L.CAL_ALPHA_DISCOVERY, L.CAL_ALPHA_CONFIRM),
               'q975_dT1': L.alpha_t_quantile(recs, 'dT1', 0.975), 'q975_dT2': L.alpha_t_quantile(recs, 'dT2', 0.975),
               'q95_cT1': L.alpha_t_quantile(recs, 'cT1', 0.95), 'q95_cT2': L.alpha_t_quantile(recs, 'cT2', 0.95)}
        out.append(row)
        print(json.dumps(row), flush=True)
    return {'discovery_events': len(dsch), 'confirmation_events': len(csch), 'sims': sims, 'worlds': out}


if __name__ == '__main__':
    if '--discovery' in sys.argv:
        run_discovery()
    elif '--confirmation' in sys.argv:
        run_confirmation()
    elif '--power' in sys.argv:
        keep = {}
        out = power(keep=keep)
        (DATA / 'logs').mkdir(parents=True, exist_ok=True)
        (DATA / 'logs' / 'power_r1.json').write_text(json.dumps(out, indent=1))
        (DATA / 'logs' / 'power_r1_records.json').write_text(json.dumps(keep))
    else:
        sys.exit(__doc__)
