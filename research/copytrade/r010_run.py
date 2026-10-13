"""Registration 010 (L2, cross-venue funding carry) — the run.

    .venv/bin/python -I r010_run.py              # discovery (guarded): discovery_started row, then T1/T2
    .venv/bin/python -I r010_run.py --holdout    # the decisive holdout (guarded): holdout_started, then the read
    .venv/bin/python -I r010_run.py --extension  # the pre-registered extension (guarded), read once
    .venv/bin/python -I r010_run.py --events     # UNCONDITIONAL counts only (positions opened, by pair /
                                                 # class / year / tier); refuses any holdout panel (N7)

Every outcome mode refuses unless: the registration says REGISTERED, is tracked, committed and
unmodified; the guarded code is clean; code and data match the §8 SHA-256 table; and the 000g
ordering (guard h) allows it. Discovery: no 010 row of any kind anywhere (tree, or any branch's
history). Holdout / extension: the gate of 009's pattern (Revision 2, N2):
  (n1) exactly one line `Holdout end (owner): <date>` and one `Holdout extension (owner): <YES|NO>`;
       UNSET refuses; the date is one of the offered ends;
  (g1) exactly one 010 discovery result row across the tree and every branch's history;
  (g2) that row is committed (in HEAD:results.jsonl);
  (g3) its registration commit is an ancestor of HEAD and the guarded code is unchanged since;
  (g4) the decision recomputed from the row's NUMBERS (`carry.recheck_discovery`) is read_holdout;
  (h') a committed line `Holdout fetch: APPROVED <date> row <sha256 of the row line>`, committed
       strictly after the row;
  (t)  now >= the span end + 7 days (complete monthly files; no early, truncated read);
  (i)  no holdout_started / holdout row yet (the read happens once); the run also needs the
       holdout_fetch_started and holdout_built rows and the built panel's hashes.
"""
import bisect
import datetime as dt
import hashlib
import json
import math
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from ct import carry  # noqa: E402
from ct import factors as fx  # noqa: E402   (read-only: 009's recheck_discovery for guard h, N3)

REG = HERE / 'registrations/010-cross-venue-funding-carry.md'
REG_009 = '009-binance-weekly-factors.md'
RESULTS = HERE / 'results.jsonl'
PANEL = HERE / 'data/carry/panel'
ALLOWED = HERE / 'data' / 'carry'                 # whitelist: 010 reads only data/carry/ (000g)
FORBIDDEN = HERE / 'data' / 'binance_years'       # named for the message and the tests; outside the whitelist
TRIALS = 2                                       # T1 pooled, T2 core coins
HOUR, DAY, GRID = carry.HOUR, carry.DAY, carry.GRID

GUARDED_CODE = ('ct/__init__.py', 'ct/carry.py', 'ct/eventstudy.py', 'ct/factors.py', 'ct/stats.py', 'ct/net.py',
                'r010_run.py', 'r010_fetch.py', 'r010_build.py', 'scouts/l2_fetch.py', 'scouts/h2_binance_scout.py')
GUARDED_DATA = tuple(f'data/carry/panel/{f}' for f in ('universe.json', 'funding.parquet', 'prices.parquet',
                                                       'path.parquet', 'volume.parquet', 'levels.parquet')) + \
    ('data/carry/inputs/um_fundingRate_symbols.json', 'data/carry/inputs/hl_first.json',
     'data/carry/binance/months.json', 'data/carry/bybit/symbols.json')
HASH_ROW = re.compile(r'^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|', re.M)
END_PREFIX = re.compile(r'^Holdout end \(owner\):', re.M)
END_LINE = re.compile(r'^Holdout end \(owner\): (\S+)\s*$', re.M)
EXT_PREFIX = re.compile(r'^Holdout extension \(owner\):', re.M)
EXT_LINE = re.compile(r'^Holdout extension \(owner\): (\S+)\s*$', re.M)
OFFERED_ENDS = {'2026-09-30': 1_790_812_800_000, '2026-12-31': 1_798_761_600_000}   # exclusive ends
APPROVAL = re.compile(r'^Holdout fetch: APPROVED (\d{4}-\d{2}-\d{2}) row ([0-9a-f]{64})\s*$', re.M)
EXT_APPROVAL = re.compile(r'^Extension fetch: APPROVED (\d{4}-\d{2}-\d{2}) row ([0-9a-f]{64})\s*$', re.M)
HOLDOUT_MODES = ('holdout_fetch_started', 'holdout_built', 'holdout_started', 'holdout')
EXT_MODES = ('extension_fetch_started', 'extension_built', 'extension_started', 'extension')


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


def result_rows(root, results_rel, name=None):
    """[(line, row)] of every distinct results.jsonl line (of registration `name`, if given), from the
    working tree and every ADDED line in the history of every branch (`git log --all -p`). For
    BLOCKING checks only: a deleted or off-branch row still blocks."""
    root = Path(root)
    texts = []
    res = root / results_rel
    if res.exists():
        texts.extend(res.read_text().splitlines())
    hist = _git(root, 'log', '--all', '-p', '--format=', '--', results_rel)
    texts.extend(l[1:] for l in hist.stdout.splitlines() if l.startswith('+') and not l.startswith('+++'))
    out, seen = [], set()
    for line in texts:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict) and (name is None or row.get('registration') == name) and line not in seen:
            seen.add(line)
            out.append((line, row))
    return out


def head_rows(root, results_rel, name):
    """[(line, row)] of `name` in HEAD:results.jsonl only — for UNLOCKING checks (N3)."""
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
    """The oldest commit on HEAD's history whose version of `rel` contains the line `needle`."""
    for c in _git(root, 'log', '--format=%H', '--reverse', '--', rel).stdout.split():
        if needle in _git(root, 'show', f'{c}:./{rel}').stdout.splitlines():
            return c
    return None


def is_ancestor(root, c):
    return bool(c) and _git(root, 'merge-base', '--is-ancestor', c, 'HEAD').returncode == 0


# ------------------------------------------------------------------ guard (h): 000g ordering, N3
def ordering_state(root, results_rel='results.jsonl'):
    """000g / programme-wide D17: 010 computes no outcome on Binance data dated >= 2024-01-01 until 009's
    holdout A reaches a terminal state. Unlocks ONLY on a 009 row that is in HEAD:results.jsonl and
    whose registration commit is an ancestor of HEAD (Revision 2, N3), and:
      'close' / 'close_conditions_unmet'  the single 009 discovery row, whose decision 009's own
                                          `factors.recheck_discovery` reproduces from its stored numbers
                                          with no failures;
      'holdout_A_declined' / 'holdout_A'  a row whose discovery_row_sha256 equals that discovery row's.
    Uncommitted, deleted, off-branch, label-only or mismatched rows unlock nothing. Returns the state
    or None."""
    root = Path(root)
    rows = [(l, r) for l, r in head_rows(root, results_rel, REG_009) if is_ancestor(root, r.get('registration_commit'))]
    disc = [(l, r) for l, r in rows if r.get('mode', 'discovery') == 'discovery']
    if len(disc) != 1:
        return None
    line, row = disc[0]
    try:
        dec, fails = fx.recheck_discovery(row.get('results') or {})
    except Exception:                                  # a malformed row unlocks nothing
        return None
    stored = row.get('decision')
    sha = line_sha(line)
    for state in ('holdout_A', 'holdout_A_declined'):
        if any(r.get('mode') == state and r.get('discovery_row_sha256') == sha for _, r in rows):
            return state
    if not fails and [dec[0], list(dec[1])] == stored and dec[0] in ('close', 'close_conditions_unmet'):
        return dec[0]
    return None


def ordering_failures(root, results_rel='results.jsonl'):
    if ordering_state(root, results_rel):
        return []
    return ['(h) ordering (000g, D17): no committed, verified terminal 009 holdout-A state in HEAD (discovery '
            'close / close_conditions_unmet recomputed from its numbers, or holdout_A_declined / holdout_A '
            'matching its discovery row) — 010 may not compute outcomes on Binance data from 2024-01-01 yet']


# ------------------------------------------------------------------ owner lines, N1
def owner_choice(text):
    """(end_exclusive_ms, end_date, extension: bool, [failures]) from the two anchored owner lines.
    Exactly one line starts `Holdout end (owner):` and one `Holdout extension (owner):`; UNSET (or any
    value outside the offer) refuses."""
    fails = []
    end = ext = None
    date = None
    if len(END_PREFIX.findall(text)) != 1 or len(END_LINE.findall(text)) != 1:
        fails.append(f'(n1) {len(END_PREFIX.findall(text))} lines start "Holdout end (owner):"; exactly one is required')
    else:
        date = END_LINE.search(text).group(1)
        if date not in OFFERED_ENDS:
            fails.append(f'(n1) holdout end is {date!r}: the owner has not chosen one of {sorted(OFFERED_ENDS)}')
        else:
            end = OFFERED_ENDS[date]
    if len(EXT_PREFIX.findall(text)) != 1 or len(EXT_LINE.findall(text)) != 1:
        fails.append(f'(n1) {len(EXT_PREFIX.findall(text))} lines start "Holdout extension (owner):"; exactly one is required')
    else:
        v = EXT_LINE.search(text).group(1)
        if v not in ('YES', 'NO'):
            fails.append(f'(n1) holdout extension is {v!r}: the owner has not chosen YES or NO')
        else:
            ext = v == 'YES'
    return end, date, ext, fails


def holdout_dir(date):
    return ALLOWED / f'panel_holdout_{date}'


EXTENSION_DIR = ALLOWED / 'panel_extension_2027-06-30'


# ------------------------------------------------------------------ the guard
def guard_failures(root, reg_rel, code=GUARDED_CODE, data=GUARDED_DATA, results_rel='results.jsonl',
                   mode='discovery', now_ms=None):
    """Every reason the run must not start; ([], commit) when it may. mode: discovery,
    holdout_fetch, holdout_build, holdout, extension_fetch, extension_build, extension."""
    root = Path(root)
    reg = root / reg_rel
    out = list(ordering_failures(root, results_rel))
    text = reg.read_text() if reg.exists() else ''
    m = re.search(r'^\*\*Status: (\w+)', text, re.M)
    if not m or m.group(1) != 'REGISTERED':
        out.append(f'{reg.name} is not REGISTERED — refusing to compute any forward return.')
    if _git(root, 'ls-files', '--error-unmatch', '--', reg_rel).returncode:
        out.append(f'(a) {reg.name} is not tracked by git')
    if _git(root, 'diff', '--quiet', 'HEAD', '--', reg_rel).returncode:
        out.append(f'(b) {reg.name} differs from the committed version — commit (as an amendment) first.')
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
    rows = result_rows(root, results_rel, reg.name)
    if mode == 'discovery':
        if rows:
            out.append(f'(f) {results_rel} (or the history of any branch) already holds a {reg.name} row — the trial runs once')
        return out, commit
    end, date, ext, n1 = owner_choice(text)
    out += n1
    disc = [(l, r) for l, r in rows if r.get('mode') == 'discovery']
    if len(disc) != 1:
        out.append(f'(g1) {len(disc)} {reg.name} discovery result rows across the tree and all branches; exactly one is required')
    else:
        line, row = disc[0]
        if line not in _git(root, 'show', f'HEAD:./{results_rel}').stdout.splitlines():
            out.append('(g2) the discovery row is not committed in HEAD:results.jsonl')
        rc = row.get('registration_commit') or ''
        if not is_ancestor(root, rc):
            out.append("(g3) the row's registration commit is not an ancestor of HEAD")
        elif _git(root, 'diff', '--quiet', rc, 'HEAD', '--', *code).returncode:
            out.append('(g3) the guarded code changed since the discovery run')
        if row.get('code_hashes') != {f: sha256(root / f) for f in code if (root / f).exists()}:
            out.append('(g3) the code hashes stored in the row differ from the code on disk')
        dec, fails = carry.recheck_discovery(row)
        out.extend(f'(g4) {x}' for x in fails)
        if dec != 'read_holdout':
            out.append(f'(g4) the decision recomputed from the numbers is {dec}, not read_holdout')
        appr = [a for a in APPROVAL.finditer(text) if a.group(2) == line_sha(line)]
        if not appr:
            out.append("(h') no committed line 'Holdout fetch: APPROVED <date> row <sha256 of the discovery row>'")
        else:
            row_c = first_commit_containing(root, results_rel, line)
            app_c = first_commit_containing(root, reg_rel, appr[0].group(0).rstrip())
            if not row_c or not app_c or row_c == app_c or _git(root, 'merge-base', '--is-ancestor', row_c, app_c).returncode:
                out.append("(h') the approval must be committed strictly after the commit that added the discovery row")
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    kinds = {r.get('mode') for _, r in rows}
    if mode.startswith('holdout'):
        if end is not None and now_ms < end + 7 * DAY:
            out.append(f'(t) the holdout ends {date}; nothing is fetched, built or read before end + 7 days')
        if kinds & {'holdout_started', 'holdout'}:
            out.append('(i) the holdout has already been started or read — it is read once')
        if mode in ('holdout_build', 'holdout') and 'holdout_fetch_started' not in kinds:
            out.append('(i) no holdout_fetch_started row: fetch first')
        if mode == 'holdout' and 'holdout_built' not in kinds:
            out.append('(i) no holdout_built row: build first')
    if mode.startswith('extension'):
        if not ext:
            out.append('(x) the owner did not pre-register the extension')
        hold = [(l, r) for l, r in rows if r.get('mode') == 'holdout']
        if len(hold) != 1:
            out.append(f'(x) {len(hold)} holdout result rows; exactly one is required')
        else:
            hl, hr = hold[0]
            if hl not in _git(root, 'show', f'HEAD:./{results_rel}').stdout.splitlines():
                out.append('(x) the holdout row is not committed in HEAD:results.jsonl')
            zr = hr.get('q')
            due = []
            for k in ('T1', 'T2'):
                ev = hr.get(k) or {}
                t_ = (ev.get('primary_month') or {}).get('t')
                zk = carry.t_crit(((ev.get('primary_month') or {}).get('clusters') or 0) - 1, carry.Q_CONFIRM_TWO_LOOKS)
                if (disc and disc[0][1].get(k, {}).get('passed') and zr == carry.Q_CONFIRM_TWO_LOOKS
                        and isinstance(t_, (int, float)) and 0 < t_ < zk):
                    due.append(k)          # recomputed from the stored month t, not from a label
            if not due:
                out.append('(x) no trial ended its first look with 0 < month t < t_{G-1}(0.975): no extension')
            ea = [a for a in EXT_APPROVAL.finditer(text) if a.group(2) == line_sha(hl)]
            if not ea:
                out.append("(x) no committed line 'Extension fetch: APPROVED <date> row <sha256 of the holdout row>'")
        if now_ms < carry.EXTENSION_END + 7 * DAY:
            out.append('(t) the extension ends 2027-06-30; nothing is fetched, built or read before end + 7 days')
        if kinds & {'extension_started', 'extension'}:
            out.append('(i) the extension has already been started or read — it is read once')
        if mode in ('extension_build', 'extension') and 'extension_fetch_started' not in kinds:
            out.append('(i) no extension_fetch_started row: fetch first')
        if mode == 'extension' and 'extension_built' not in kinds:
            out.append('(i) no extension_built row: build first')
    return out, commit


def guard(mode='discovery'):
    failures, commit = guard_failures(HERE, str(REG.relative_to(HERE)), mode=mode)
    if failures:
        sys.exit('\n'.join(failures))
    return commit


def append_row(row):
    line = json.dumps(row, default=str)
    with open(RESULTS, 'a') as f:
        f.write(line + '\n')
    return line


def hashes(files):
    return {f: sha256(HERE / f) for f in files if (HERE / f).exists()}


def panel_hashes(pdir):
    return {p.name: sha256(p) for p in sorted(Path(pdir).iterdir()) if p.is_file()}


# ------------------------------------------------------------------ the panel
class Panel:
    """A panel read up to `end` (exclusive). The discovery panel has prices only before 2025-01-01;
    with_prices=False reads funding, volume and levels only (unconditional counts). A holdout or
    extension panel is opened only by the guarded runs (allow_holdout=True; N7)."""

    def __init__(self, end, with_prices=True, panel_dir=None, price_end=None, allow_holdout=False):
        import duckdb
        panel_dir = Path(panel_dir or PANEL)
        assert panel_dir.resolve().is_relative_to(ALLOWED.resolve()), \
            f'010 reads only data/carry/ (000g whitelist); refused: {panel_dir}'
        if not allow_holdout:
            assert not panel_dir.name.startswith(('panel_holdout', 'panel_extension')), \
                'counting and describing scripts never open a holdout panel (N7)'
        price_end = price_end or carry.DISC_END
        if with_prices:
            assert end <= price_end, 'prices are read only inside the panel\'s price span'
        self.end = end
        con = duckdb.connect()
        q = lambda sql: con.execute(sql).fetchall()
        self.universe = json.loads((panel_dir / 'universe.json').read_text())
        rows = {}
        for v, c, t, r in q(f"SELECT venue, coin, t, rate FROM '{panel_dir}/funding.parquet' WHERE t < {end}"):
            rows.setdefault((v, c), []).append((t, r))
        self.funding = {k: carry.Funding(v) for k, v in rows.items()}
        self.lp, self.lpo = {}, {}
        self.path_rows = {}
        self.fill_fallbacks = 0
        if with_prices:
            for v, c, t, lp, lpo in q(f"SELECT venue, coin, t, lp, lpo FROM '{panel_dir}/prices.parquet' WHERE t < {end}"):
                if lp is not None:
                    self.lp[(v, c, t)] = lp
                if lpo is not None:
                    self.lpo[(v, c, t)] = lpo
            for c, t, hi, lo, cl in q(f"SELECT coin, t, high, low, close FROM '{panel_dir}/path.parquet' "
                                      f"WHERE t < {end} ORDER BY coin, t"):
                self.path_rows.setdefault(c, []).append((t, hi, lo, cl))
        self.path_t = {c: [r[0] for r in v] for c, v in self.path_rows.items()}
        self.vol = {}
        for v, c, d, qv in q(f"SELECT venue, coin, day, qvol FROM '{panel_dir}/volume.parquet' WHERE day < {end}"):
            self.vol[(v, c, d)] = qv
        lev = {}
        for v, c, t, cl in q(f"SELECT venue, coin, t, close FROM '{panel_dir}/levels.parquet' WHERE t < {end}"):
            lev[(v, c, t)] = cl
        self.name_res = {}
        for (v, c, t), cl in lev.items():
            if v == 'BN':
                continue
            ref = lev.get(('BN', c, t))
            if ref and cl and ref > 0 and cl > 0:
                x = math.log(cl / ref)
                self.name_res.setdefault((v, c), []).append((t, abs(x - round(x / math.log(10)) * math.log(10))))
        for k in self.name_res:
            self.name_res[k].sort()
        self._vol, self._name = {}, {}
        self.coins = sorted(c for c in self.universe if sum(1 for v in ('HL', 'BN', 'BY', 'OK') if (v, c) in self.funding) >= 2)

    # --- information known at t
    def mean_volume(self, coin, venue, t):
        """Trailing 30-day mean daily quote volume of the leg's own venue, UTC days before t's day
        (missing days count as 0)."""
        day = t - t % DAY
        k = (venue, coin, day)
        if k not in self._vol:
            self._vol[k] = sum(self.vol.get((venue, coin, day - i * DAY), 0.0)
                               for i in range(1, carry.LIQ_DAYS + 1)) / carry.LIQ_DAYS
        return self._vol[k]

    def liq_ok(self, coin, venue, t):
        return self.mean_volume(coin, venue, t) >= carry.LIQ_FLOOR

    def slip(self, coin, venue, t):
        return carry.slip_tier(self.mean_volume(coin, venue, t))

    def name_ok(self, venue, coin, t):
        """HL, BY, OK: the median |ln(close / Binance close)|, after the nearest 10^k scale, over the
        closes in [t - 7 d, t) is <= 5 %, with at least 3 closes. BN is the reference."""
        if venue == 'BN':
            return True
        day = t - t % DAY
        k = (venue, coin, day)
        if k not in self._name:
            rs = self.name_res.get((venue, coin), [])
            ts = [x[0] for x in rs]
            lo, hi = bisect.bisect_left(ts, t - 7 * DAY), bisect.bisect_left(ts, t)
            vals = [x[1] for x in rs[lo:hi]]
            self._name[k] = len(vals) >= 3 and statistics.median(vals) <= carry.LEVEL_TOL
        return self._name[k]

    def gap(self, coin, a, b, t):
        """ln P_a - ln P_b at t from the last-trade closes of the bar ENDING at t (known at t: used
        for eligibility)."""
        x, y = self.lp.get((a, coin, t)), self.lp.get((b, coin, t))
        return None if x is None or y is None else x - y

    def fill(self, coin, a, b, t):
        """The FILL gap at t (N6): ln of the first trade after t on each venue (the open of the bar
        starting at t), i.e. after the settlement at t; the close of the bar ending at t where an open
        is missing (counted in fill_fallbacks)."""
        x, y = self.lpo.get((a, coin, t)), self.lpo.get((b, coin, t))
        if x is not None and y is not None:
            return x - y
        g = self.gap(coin, a, b, t)
        if g is not None:
            self.fill_fallbacks += 1
        return g

    def gap_ok(self, coin, a, b, t):
        return self.name_ok(a, coin, t) and self.name_ok(b, coin, t)

    def path(self, coin, a, b):
        ts = self.path_t.get(coin, [])
        lo, hi = bisect.bisect_left(ts, a), bisect.bisect_right(ts, b)
        return self.path_rows[coin][lo:hi] if coin in self.path_rows else []

    def eligible(self, need_gap=True):
        def f(coin, pair, t):
            if not all(self.liq_ok(coin, v, t) and self.name_ok(v, coin, t) for v in pair):
                return False
            return not need_gap or self.gap(coin, pair[0], pair[1], t) is not None
        return f


def grid(start, end):
    t = start - start % GRID
    out = []
    while t < end:
        if t >= start:
            out.append(t)
        t += GRID
    return out


def plan_on(panel, start, end, pairs=carry.PAIRS, need_gap=True):
    return carry.plan(panel.coins, panel.funding, panel.eligible(need_gap), grid(start, end), start, end,
                      pairs=pairs, slip=panel.slip)


def describe_positions(pos):
    """Unconditional description of the positions opened: counts by pair, class, year, short venue,
    hold, clusters, tier. Never a share of qualifying times, a re-entry count or a per-coin count
    (N7)."""
    by = lambda key: dict(sorted(((k, sum(1 for p in pos if key(p) == k)) for k in {key(p) for p in pos})))
    year = lambda p: dt.datetime.fromtimestamp(p['t_in'] / 1000, dt.UTC).year
    return {'n': len(pos), 'by_pair': by(lambda p: '-'.join(p['pair'])), 'by_cls': by(lambda p: p['cls']),
            'by_year': by(year), 'short_venue': by(lambda p: p['short']), 'weeks': len({p['week'] for p in pos}),
            'months': len({p['month'] for p in pos}), 'coins': len({p['coin'] for p in pos}),
            'hold_h': by(lambda p: p['hold_ms'] // HOUR),
            'slip_bp_per_fill': by(lambda p: '+'.join(str(round(1e4 * s)) for s in p['slip']))}


def stress(rows):
    """Leg-liquidation stress (reported): the largest adverse single-leg move during holds against
    the 1x margin distance (short: (1 - mm)/(1 + mm); long: 100 %)."""
    ups = sorted(r['max_up'] for r in rows if r.get('max_up') is not None)
    downs = sorted(r['max_down'] for r in rows if r.get('max_down') is not None)
    q = lambda xs, p: xs[min(len(xs) - 1, int(p * len(xs)))] if xs else None
    return {'short_leg_max_up': {'p50': q(ups, .5), 'p99': q(ups, .99), 'max': ups[-1] if ups else None},
            'long_leg_max_down': {'p50': q(downs, .5), 'p99': q(downs, .99), 'max': downs[-1] if downs else None},
            'topups': sum(r['topups'] for r in rows), 'liquidated': sum(r['liquidated'] for r in rows),
            'min_short_margin_distance': min((carry.liq_distance(r['short'], r['coin']) for r in rows), default=None)}


def outcomes(panel, pos):
    """Every position stays in the primary (N4): a failed name check at exit uses the raw exit gap
    (carry.outcome), and a position with no price within 24 h of its exit gets the WORST observed
    return of its pair in the span. Returns (rows, counts)."""
    res = [carry.outcome(p, panel.funding, panel.gap, panel.path, panel.gap_ok, fill=panel.fill) for p in pos]
    worst = {}
    for r in res:
        if r is not None:
            worst[r['pair']] = min(worst.get(r['pair'], r['ret']), r['ret'])
    allw = min(worst.values()) if worst else None
    rows, filled = [], 0
    for p, r in zip(pos, res):
        if r is None:
            w = worst.get(p['pair'], allw)
            if w is None:
                continue
            r = dict(p, ret=w, gross=w + p['cost'] / 2, costs=p['cost'] / 2, hold_years=p['hold_ms'] / carry.YEAR, exit_gap='worst_fill',
                     funding=None, gap_pnl=None, fees=p['cost'], topup_cost=0.0, topups=0, liquidated=False,
                     delisted=False, funding_holes=0, max_up=None, max_down=None, exit_lag_ms=None, t_x=p['t_out'])
            filled += 1
        rows.append(r)
    counts = {'positions': len(pos), 'raw_exit_gap': sum(1 for r in rows if r.get('exit_gap') == 'raw'),
              'worst_fill': filled, 'liquidated': sum(1 for r in rows if r.get('liquidated')),
              'funding_holes_settlements': sum(r.get('funding_holes') or 0 for r in rows),
              'positions_with_holes': sum(1 for r in rows if r.get('funding_holes')),
              'fill_fallback_to_close': panel.fill_fallbacks}
    return rows, counts


def reported(panel, rows, pos):
    real = [r for r in rows if r.get('exit_gap') != 'worst_fill']
    valid_only = [r for r in real if r.get('exit_gap') in ('valid', 'liquidated')]
    sm = lambda rs: carry.summarise(rs, placebo_seeds=0)
    comp = [r for r in real if r.get('funding') is not None]
    return {
        'by_pair': {'-'.join(k): sm([r for r in rows if r['pair'] == k]) for k in carry.PAIRS
                    if any(r['pair'] == k for r in rows)},
        'others': sm([r for r in rows if r['cls'] == 'other']),
        'hl_pairs': sm([r for r in rows if 'HL' in r['pair']]),
        'cex_pairs': sm([r for r in rows if 'HL' not in r['pair']]),
        'slippage_x2': sm([dict(r, ret=r['ret'] - (r['slip'][0] + r['slip'][1]) * (1 + r['topups'])) for r in rows]),
        'funding_only': sm([dict(r, ret=(r['funding'] - r['fees']) / 2) for r in comp]),
        'drop_missing_variant': sm(real),
        'valid_exit_only_variant': sm(valid_only),
        'stress': stress(real),
        'delisted': sum(1 for r in real if r.get('delisted')),
        'exit_gap_lag_h_median': statistics.median([r['exit_lag_ms'] / HOUR for r in real if r.get('exit_lag_ms') is not None])
        if real else None,
    }


def versions():
    import duckdb
    import pyarrow
    return {'python': sys.version.split()[0], 'duckdb': duckdb.__version__, 'pyarrow': pyarrow.__version__}


def events():
    """Unconditional counts only (B1, N7): positions opened; never a qualifying share, a re-entry count
    or per-coin counts; never a holdout panel."""
    pdisc = Panel(carry.DISC_END)
    pos, cnt = plan_on(pdisc, carry.DISC_START, carry.DISC_END)
    pos_okx, cnt_okx = plan_on(pdisc, carry.DISC_START, carry.DISC_END, pairs=carry.PAIRS_OKX)
    pexp = Panel(carry.EXPOSED_END, with_prices=False)
    pos_e, cnt_e = plan_on(pexp, carry.EXPOSED_START, carry.EXPOSED_END, need_gap=False)
    return {'discovery': {'counts': cnt, 'positions': describe_positions(pos)},
            'discovery_okx_variant': {'counts': cnt_okx, 'positions': describe_positions(pos_okx)},
            'exposed_2025_26_counts_only': {'counts': cnt_e, 'positions': describe_positions(pos_e)}}, \
        (pdisc, pos, pexp, pos_e)


def run_discovery():
    commit = guard('discovery')
    panel = Panel(carry.DISC_END)
    append_row({'registration': REG.name, 'mode': 'discovery_started', 'registration_commit': commit,
                'run_at': dt.datetime.now(dt.UTC).isoformat()})          # m-a: before any outcome
    pos, cnt = plan_on(panel, carry.DISC_START, carry.DISC_END)
    rows, oc = outcomes(panel, pos)
    assert all(r['t_x'] < carry.DISC_END for r in rows)
    core = [r for r in rows if r['cls'] == 'core']
    out = {'registration': REG.name, 'mode': 'discovery', 'registration_commit': commit,
           'code_hashes': hashes(GUARDED_CODE), 'data_hashes': hashes(GUARDED_DATA),
           'run_at': dt.datetime.now(dt.UTC).isoformat(), 'versions': versions(),
           'params': {'grid_h': GRID // HOUR, 'window_h': carry.W // HOUR, 'margin': carry.MARGIN,
                      'bench': carry.BENCH, 'taker': carry.TAKER, 'slip_design': carry.SLIP_DESIGN,
                      'slip_tiers': carry.SLIP_TIERS, 'season_d': carry.SEASON // DAY, 'liq_floor': carry.LIQ_FLOOR,
                      'topup': carry.TOPUP, 'transfer': carry.TRANSFER, 'mm': {f'{a}-{b}': v for (a, b), v in carry.MM.items()},
                      'z_week': carry.Z, 'trials': TRIALS, 'pairs': carry.PAIRS},
           'counts': {**cnt, **oc}, 'positions': describe_positions(pos)}
    print('primary counts before the decision:', json.dumps(oc))
    out['T1'] = carry.summarise(rows)
    out['T2'] = carry.summarise(core)
    out['decision'] = carry.decide(out['T1'], out['T2'])
    out['reported'] = reported(panel, rows, pos)
    pos_okx, _ = plan_on(panel, carry.DISC_START, carry.DISC_END, pairs=carry.PAIRS_OKX)
    rows_okx, oc_okx = outcomes(panel, pos_okx)
    out['reported']['okx_variant_survivorship_biased'] = dict(carry.summarise(rows_okx, placebo_seeds=0), counts=oc_okx)
    append_row(out)
    print(json.dumps({k: {kk: out[k][kk] for kk in ('primary_week', 'primary_month', 'annualised', 'criteria',
                                                     'passed', 'placebo_month_flip')} for k in ('T1', 'T2')},
                     indent=1, default=str))
    print('decision', out['decision'])


def confirm_block(rows, q):
    ok, det = carry.holdout_confirm(rows, q)
    return {'confirm': ok, **det, 'extension_due_recomputed': (not ok) and carry.extension_due(det)}


def run_holdout(extension=False):
    """The decisive holdout (2026-07-01 -> the owner's end), or the pre-registered extension
    (2026-07-01 -> 2027-06-30, cumulative), on the panel built after the gated fetch. Same rule, same
    code. The exposed 2025-26 block is reported in the holdout run and decides nothing."""
    mode = 'extension' if extension else 'holdout'
    commit = guard(mode)
    end, date, ext, _ = owner_choice(REG.read_text())
    if extension:
        end, pdir = carry.EXTENSION_END, EXTENSION_DIR
    else:
        pdir = holdout_dir(date)
    rows_all = result_rows(HERE, 'results.jsonl', REG.name)
    built = [r for _, r in rows_all if r.get('mode') == f'{mode}_built']
    if not built or built[-1].get('panel_sha256') != panel_hashes(pdir):
        sys.exit(f'the {mode} panel differs from the hashes recorded when it was built — refusing')
    (dline, drow), = [(l, r) for l, r in rows_all if r.get('mode') == 'discovery']
    append_row({'registration': REG.name, 'mode': f'{mode}_started', 'registration_commit': commit,
                'discovery_row_sha256': line_sha(dline), 'run_at': dt.datetime.now(dt.UTC).isoformat()})
    panel = Panel(end, panel_dir=pdir, price_end=end, allow_holdout=True)
    pos, cnt = plan_on(panel, carry.HOLDOUT_START, end)
    rows, oc = outcomes(panel, pos)
    q = carry.Q_CONFIRM_TWO_LOOKS if ext else carry.Q_CONFIRM
    passed = {k: drow[k]['passed'] for k in ('T1', 'T2')}
    res = {'T1': confirm_block(rows, q), 'T2': confirm_block([r for r in rows if r['cls'] == 'core'], q)}
    out = {'registration': REG.name, 'mode': mode, 'end': end, 'end_date': date, 'extension_registered': ext,
           'q': q, 'registration_commit': commit, 'discovery_row_sha256': line_sha(dline),
           'run_at': dt.datetime.now(dt.UTC).isoformat(), 'versions': versions(), 'panel_sha256': panel_hashes(pdir),
           'code_hashes': hashes(GUARDED_CODE), 'counts': {**cnt, **oc}, 'positions': describe_positions(pos),
           **res,
           'decision': 'confirm' if any(passed[k] and res[k]['confirm'] for k in res) else
                       ('extension_due' if (ext and not extension and any(passed[k] and res[k]['extension_due_recomputed'] for k in res))
                        else 'not_demonstrated')}
    if not extension:
        pos_e, cnt_e = plan_on(panel, carry.EXPOSED_START, carry.EXPOSED_END)
        rows_e, oc_e = outcomes(panel, pos_e)
        out['exposed_2025_26_reported'] = dict(carry.summarise(rows_e, placebo_seeds=0), counts={**cnt_e, **oc_e})
    append_row(out)
    print(json.dumps({k: out[k] for k in ('T1', 'T2', 'decision')}, indent=1, default=str))


def main():
    if '--events' in sys.argv:
        print(json.dumps(events()[0], indent=1, default=str))
    elif '--holdout' in sys.argv:
        run_holdout(False)
    elif '--extension' in sys.argv:
        run_holdout(True)
    else:
        run_discovery()


if __name__ == '__main__':
    main()
