"""Registration 012 (L2, trial 3, HL-CEX funding carry) — the run. 010's frozen rule on the HL-BN and
HL-BY pairs only; 010's code (`ct/carry.py`, `r010_run.py`) is imported READ-ONLY.

    .venv/bin/python -I r012_run.py --stage1     # Stage 1, the exposed block 2025-01-15 -> 2026-06-30 (a FILTER)
    .venv/bin/python -I r012_run.py --stage2     # Stage 2, fresh data 2026-07-01 -> the owner's end (DECISIVE)
    .venv/bin/python -I r012_run.py --extension  # the pre-registered extension, read once if due
    .venv/bin/python -I r012_run.py --counts     # UNCONDITIONAL counts of positions opened; refuses Stage-2 panels

Every outcome mode refuses unless: the registration says REGISTERED, is tracked, committed and
unmodified; the guarded code is clean; code and data match the §8 SHA-256 table; and the 000g ordering
(010's guard h, verified 009 terminal state) allows it. Stage 1: no 012 row of any kind anywhere (the
tree, or any branch's history). Stage 2 / extension: 010 Revision 2's gate, ported:
  (n1) exactly one line `Stage-2 end (owner): <date>` and one `Stage-2 extension (owner): <YES|NO>`;
  (g1) exactly one 012 stage1 result row across the tree and every branch's history;
  (g2) that row is committed (in HEAD:results.jsonl);
  (g3) its registration commit is an ancestor of HEAD and the guarded code is unchanged since;
  (g4) the decision recomputed from the row's NUMBERS (`carry012.recheck_stage1`) is read_stage2;
  (h') a committed line `Stage-2 fetch: APPROVED <date> row <sha256 of the row line>`, committed
       strictly after the row;
  (t)  now >= the end + 7 days;
  (i)  read once; fetch before build before run; the run checks the built panel's hashes.
"""
import datetime as dt
import json
import math
import re
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from ct import carry, carry012 as c12  # noqa: E402
import r010_run as r10  # noqa: E402   (read-only: Panel methods, outcomes, git helpers, 000g ordering)

REG = HERE / 'registrations/012-hl-cex-funding-carry.md'
RESULTS = HERE / 'results.jsonl'
DATA = HERE / 'data'
CARRY010 = DATA / 'carry'                         # 010's raw downloads and registered panel (read only)
CARRY012 = DATA / 'carry012'                      # 012's own downloads and panels
ALLOWED = (CARRY010, CARRY012)                    # whitelist: 012 reads nothing else (000g)
STAGE1_PANEL = CARRY012 / 'panel_stage1'
EXTENSION_DIR = CARRY012 / 'panel_extension_2027-06-30'
HOUR, DAY, GRID = carry.HOUR, carry.DAY, carry.GRID

GUARDED_CODE = ('ct/__init__.py', 'ct/carry.py', 'ct/carry012.py', 'ct/eventstudy.py', 'ct/factors.py',
                'ct/stats.py', 'ct/net.py', 'r010_run.py', 'r010_fetch.py', 'r010_build.py',
                'r012_run.py', 'r012_fetch.py', 'r012_build.py', 'scouts/l2_fetch.py', 'scouts/h2_binance_scout.py')
GUARDED_DATA = tuple(f'data/carry012/panel_stage1/{f}' for f in (
    'universe.json', 'funding.parquet', 'prices.parquet', 'path.parquet', 'volume.parquet', 'levels.parquet',
    'hlpath.parquet')) + \
    r10.GUARDED_DATA                              # 010's registered panel and name lists: the Stage-1 sources

END_PREFIX = re.compile(r'^Stage-2 end \(owner\):', re.M)
END_LINE = re.compile(r'^Stage-2 end \(owner\): (\S+)\s*$', re.M)
EXT_PREFIX = re.compile(r'^Stage-2 extension \(owner\):', re.M)
EXT_LINE = re.compile(r'^Stage-2 extension \(owner\): (\S+)\s*$', re.M)
APPROVAL = re.compile(r'^Stage-2 fetch: APPROVED (\d{4}-\d{2}-\d{2}) row ([0-9a-f]{64})\s*$', re.M)
EXT_APPROVAL = re.compile(r'^Stage-2 extension fetch: APPROVED (\d{4}-\d{2}-\d{2}) row ([0-9a-f]{64})\s*$', re.M)
STAGE2_MODES = ('stage2_fetch_started', 'stage2_built', 'stage2_started', 'stage2')
EXT_MODES = ('extension_fetch_started', 'extension_built', 'extension_started', 'extension')

sha256, line_sha, result_rows, head_rows = r10.sha256, r10.line_sha, r10.result_rows, r10.head_rows
first_commit_containing, is_ancestor, _git, HASH_ROW = r10.first_commit_containing, r10.is_ancestor, r10._git, r10.HASH_ROW


# ------------------------------------------------------------------ whitelist
def allowed(path, roots=None):
    """True iff `path` lies under data/carry/ or data/carry012/ (000g whitelist)."""
    p = Path(path).resolve()
    return any(p.is_relative_to(Path(r).resolve()) for r in (roots or ALLOWED))


def assert_allowed(path, roots=None):
    assert allowed(path, roots), f'012 reads only data/carry/ and data/carry012/ (000g whitelist); refused: {path}'


# ------------------------------------------------------------------ owner lines
def owner_choice(text):
    """(end_exclusive_ms, end_date, extension: bool, [failures]) from the two anchored owner lines.
    Exactly one line starts `Stage-2 end (owner):` and one `Stage-2 extension (owner):`; anything
    outside the offer (UNSET included) refuses."""
    fails = []
    end = ext = date = None
    if len(END_PREFIX.findall(text)) != 1 or len(END_LINE.findall(text)) != 1:
        fails.append(f'(n1) {len(END_PREFIX.findall(text))} lines start "Stage-2 end (owner):"; exactly one is required')
    else:
        date = END_LINE.search(text).group(1)
        if date not in c12.STAGE2_ENDS:
            fails.append(f'(n1) Stage-2 end is {date!r}: not one of {sorted(c12.STAGE2_ENDS)}')
        else:
            end = c12.STAGE2_ENDS[date]
    if len(EXT_PREFIX.findall(text)) != 1 or len(EXT_LINE.findall(text)) != 1:
        fails.append(f'(n1) {len(EXT_PREFIX.findall(text))} lines start "Stage-2 extension (owner):"; exactly one is required')
    else:
        v = EXT_LINE.search(text).group(1)
        if v not in ('YES', 'NO'):
            fails.append(f'(n1) Stage-2 extension is {v!r}: not YES or NO')
        else:
            ext = v == 'YES'
    return end, date, ext, fails


def stage2_dir(date):
    return CARRY012 / f'panel_stage2_{date}'


# ------------------------------------------------------------------ the guard
def guard_failures(root, reg_rel, code=GUARDED_CODE, data=GUARDED_DATA, results_rel='results.jsonl',
                   mode='stage1', now_ms=None):
    """Every reason the run must not start; ([], commit) when it may. mode: stage1, stage2_fetch,
    stage2_build, stage2, extension_fetch, extension_build, extension."""
    root = Path(root)
    reg = root / reg_rel
    out = list(r10.ordering_failures(root, results_rel))
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
    end, date, ext, n1 = owner_choice(text)
    out += n1
    owner = {'end_date': date, 'extension': ext}
    if mode == 'stage1':
        if rows:
            out.append(f'(f) {results_rel} (or the history of any branch) already holds a {reg.name} row — Stage 1 runs once')
        return out, commit
    head_lines = set(_git(root, 'show', f'HEAD:./{results_rel}').stdout.splitlines())
    s1 = [(l, r) for l, r in rows if r.get('mode') == 'stage1']
    if len(s1) != 1:
        out.append(f'(g1) {len(s1)} {reg.name} stage1 result rows across the tree and all branches; exactly one is required')
    else:
        line, row = s1[0]
        if line not in head_lines:
            out.append('(g2) the stage1 row is not committed in HEAD:results.jsonl')
        rc = row.get('registration_commit') or ''
        st = [(l, r) for l, r in rows if r.get('mode') == 'stage1_started']
        if len(st) != 1 or st[0][0] not in head_lines or st[0][1].get('registration_commit') != rc:
            out.append('(s) exactly one committed stage1_started row with the same registration commit is required')
        if not is_ancestor(root, rc):
            out.append("(g3) the row's registration commit is not an ancestor of HEAD")
        else:
            if _git(root, 'diff', '--quiet', rc, 'HEAD', '--', *code).returncode:
                out.append('(g3) the guarded code changed since the Stage-1 run')
            changed = [l[1:] for l in _git(root, 'diff', '-U0', rc, 'HEAD', '--', reg_rel).stdout.splitlines()
                       if l[:1] in '+-' and not l.startswith(('+++', '---'))]
            if any(l.strip() and not (APPROVAL.fullmatch(l.rstrip()) or EXT_APPROVAL.fullmatch(l.rstrip())) for l in changed):
                out.append('(r) the registration changed since the Stage-1 run beyond added approval lines')
        if row.get('code_hashes') != {f: sha256(root / f) for f in code if (root / f).exists()}:
            out.append('(g3) the code hashes stored in the row differ from the code on disk')
        if row.get('data_hashes') != {f: table.get(f) for f in data}:
            out.append('(g3) the data hashes stored in the row differ from the registration table')
        if row.get('owner') != owner:
            out.append(f"(o) the owner lines changed since Stage 1: row {row.get('owner')} vs now {owner}")
        dec, fails = c12.recheck_stage1(row)
        out.extend(f'(g4) {x}' for x in fails)
        if dec != 'read_stage2':
            out.append(f'(g4) the decision recomputed from the numbers is {dec}, not read_stage2')
        appr = [a for a in APPROVAL.finditer(text) if a.group(2) == line_sha(line)]
        if not appr:
            out.append("(h') no committed line 'Stage-2 fetch: APPROVED <date> row <sha256 of the stage1 row>'")
        else:
            row_c = first_commit_containing(root, results_rel, line)
            app_c = first_commit_containing(root, reg_rel, appr[0].group(0).rstrip())
            if not row_c or not app_c or row_c == app_c or _git(root, 'merge-base', '--is-ancestor', row_c, app_c).returncode:
                out.append("(h') the approval must be committed strictly after the commit that added the stage1 row")
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)

    def sequence(kind, stage):
        """(i) Read once; exactly one fetch (resumable until its `_fetched` row), one build, one run;
        each later step needs the earlier rows COMMITTED, with the owner lines unchanged (M3)."""
        msgs = []
        by = {m: [(l, r) for l, r in rows if r.get('mode') == f'{kind}_{m}']
              for m in ('fetch_started', 'fetched', 'built', 'started')}
        if [r for _, r in rows if r.get('mode') == kind] or by['started']:
            msgs.append(f'(i) {kind} has already been started or read — it is read once')
        for m, lst in by.items():
            if len(lst) > 1:
                msgs.append(f'(i) {len(lst)} {kind}_{m} rows: exactly one is allowed')
            for _, r in lst:
                if 'owner' in r and r['owner'] != owner:
                    msgs.append(f'(o) the owner lines differ from those in the {kind}_{m} row')
        need = {'fetch': [], 'build': ['fetch_started', 'fetched'], 'run': ['fetch_started', 'fetched', 'built']}[stage]
        for m in need:
            if not by[m]:
                msgs.append(f'(i) no {kind}_{m} row: ' + ('fetch first' if m.startswith('fetch') else 'build first'))
            elif by[m][0][0] not in head_lines:
                msgs.append(f'(i) the {kind}_{m} row is not committed')
        if stage == 'fetch' and by['fetched']:
            msgs.append(f'(i) the {kind} fetch has completed — a second fetch is refused')
        if stage == 'build' and by['built']:
            msgs.append(f'(i) the {kind} panel has been built — a second build is refused')
        return msgs
    stage = {'fetch': 'fetch', 'build': 'build'}.get(mode.rsplit('_', 1)[-1], 'run')
    if mode.startswith('stage2'):
        if end is not None and now_ms < end + 7 * DAY:
            out.append(f'(t) Stage 2 ends {date}; nothing is fetched, built or read before end + 7 days')
        out += sequence('stage2', stage)
    if mode.startswith('extension'):
        if not ext:
            out.append('(x) the owner did not pre-register the extension')
        s2 = [(l, r) for l, r in rows if r.get('mode') == 'stage2']
        if len(s2) != 1:
            out.append(f'(x) {len(s2)} stage2 result rows; exactly one is required')
        else:
            l2, r2 = s2[0]
            if l2 not in head_lines:
                out.append('(x) the stage2 row is not committed in HEAD:results.jsonl')
            if not c12.extension_due_from_row(r2):
                out.append('(x) the first look did not end with 0 < month t < t_{G-1}(0.975) (recomputed): no extension')
            ea = [a for a in EXT_APPROVAL.finditer(text) if a.group(2) == line_sha(l2)]
            if not ea:
                out.append("(x) no committed line 'Stage-2 extension fetch: APPROVED <date> row <sha256 of the stage2 row>'")
            else:
                row_c = first_commit_containing(root, results_rel, l2)
                app_c = first_commit_containing(root, reg_rel, ea[0].group(0).rstrip())
                if not row_c or not app_c or row_c == app_c or _git(root, 'merge-base', '--is-ancestor', row_c, app_c).returncode:
                    out.append('(x) the extension approval must be committed strictly after the commit that added the stage2 row')
        if now_ms < c12.EXTENSION_END + 7 * DAY:
            out.append('(t) the extension ends 2027-06-30; nothing is fetched, built or read before end + 7 days')
        out += sequence('extension', stage)
    return out, commit


def guard(mode='stage1'):
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
class Panel(r10.Panel):
    """010's Panel (the per-leg floor, tiers, name check incl. HL, fill, path: inherited unchanged),
    loaded from a whitelisted directory: data/carry/ or data/carry012/. A Stage-2 or extension panel is
    opened only by the guarded runs (allow_stage2=True), and without that flag nothing dated on or after
    2026-07-01 is read (Revision 2, M6 e). `hl_path` (reported only) is HL's own 8h high/low/close."""

    def __init__(self, end, panel_dir, with_prices=True, price_end=None, allow_stage2=False):
        import duckdb
        panel_dir = Path(panel_dir)
        assert_allowed(panel_dir)
        if not allow_stage2:
            assert not panel_dir.name.startswith(('panel_stage2', 'panel_extension', 'panel_holdout')), \
                'counting and describing code never opens a Stage-2 or extension panel'
        assert allow_stage2 or end <= c12.STAGE1_END, \
            '012 reads nothing dated on or after 2026-07-01 outside its Stage-2 gate'
        price_end = price_end or carry.DISC_END
        if with_prices:
            assert end <= price_end, "prices are read only inside the panel's price span"
        self.end = end
        con = duckdb.connect()
        q = lambda sql: con.execute(sql).fetchall()
        self.universe = json.loads((panel_dir / 'universe.json').read_text())
        rows = {}
        for v, c, t, r in q(f"SELECT venue, coin, t, rate FROM '{panel_dir}/funding.parquet' WHERE t < {end}"):
            rows.setdefault((v, c), []).append((t, r))
        self.funding = {k: carry.Funding(v) for k, v in rows.items()}
        self.lp, self.lpo, self.path_rows = {}, {}, {}
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
        self.hl_path_rows = {}
        if with_prices and (panel_dir / 'hlpath.parquet').exists():
            for c, t, hi, lo, cl in q(f"SELECT coin, t, high, low, close FROM '{panel_dir}/hlpath.parquet' "
                                      f"WHERE t < {end} ORDER BY coin, t"):
                self.hl_path_rows.setdefault(c, []).append((t, hi, lo, cl))
        self.hl_path_t = {c: [r[0] for r in v] for c, v in self.hl_path_rows.items()}
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
        self.coins = sorted(c for c in self.universe if 'HL' in self.universe[c]
                            and sum(1 for v in ('HL', 'BN', 'BY') if (v, c) in self.funding) >= 2)


    def hl_path(self, coin, a, b):
        """HL's own 8h bars ending in [a, b] (reported stress only)."""
        import bisect
        ts = self.hl_path_t.get(coin, [])
        lo, hi = bisect.bisect_left(ts, a), bisect.bisect_right(ts, b)
        return self.hl_path_rows[coin][lo:hi] if coin in self.hl_path_rows else []


def price_holes(panel, start):
    """Venue-coins of the panel's coins with funding at or after `start` but no last-trade close at all
    (they can never be entered): printed before the decision (Revision 2, M1)."""
    have = {(v, c) for v, c, _ in panel.lp}
    out = {}
    for (v, c), f in panel.funding.items():
        if c in panel.coins and f.last is not None and f.last >= start and (v, c) not in have:
            out[v] = out.get(v, 0) + 1
    return out


def plan_on(panel, start, end, pairs=c12.PAIRS_012, need_gap=True):
    return carry.plan(panel.coins, panel.funding, panel.eligible(need_gap), r10.grid(start, end), start, end,
                      pairs=pairs, slip=panel.slip)


def reported(panel, rows, start, end):
    """Reported blocks; none decides anything and none may become a primary."""
    real = [r for r in rows if r.get('exit_gap') != 'worst_fill']
    sm = lambda rs: carry.summarise(rs, placebo_seeds=0) if rs else None
    comp = [r for r in real if r.get('funding') is not None]
    pos_b = c12.hl_subset_of(plan_on(panel, start, end, pairs=carry.PAIRS)[0])
    rows_b, oc_b = r10.outcomes(panel, pos_b)
    hl_short = [r for r in rows if r['short'] == 'HL' and r.get('exit_gap') != 'worst_fill']
    hl_own = [carry.outcome(dict((k, r[k]) for k in r if k in POS_KEYS), panel.funding, panel.gap, panel.hl_path,
                            panel.gap_ok, fill=panel.fill) for r in hl_short]
    hl_own = [x for x in hl_own if x is not None] + [r for r in rows if not (r['short'] == 'HL' and r.get('exit_gap') != 'worst_fill')]
    return {
        'by_pair': {'-'.join(k): sm([r for r in rows if r['pair'] == k]) for k in c12.PAIRS_012},
        'core': sm([r for r in rows if r['cls'] == 'core']),
        'others': sm([r for r in rows if r['cls'] == 'other']),
        'short_hl': sm([r for r in rows if r['short'] == 'HL']),
        'long_hl': sm([r for r in rows if r['long'] == 'HL']),
        'slippage_x2': sm([dict(r, ret=r['ret'] - (r['slip'][0] + r['slip'][1]) * (1 + r['topups'])) for r in rows]),
        'funding_only': sm([dict(r, ret=(r['funding'] - r['fees']) / 2) for r in comp]),
        'drop_missing_variant': sm(real),
        'valid_exit_only_variant': sm([r for r in real if r.get('exit_gap') in ('valid', 'liquidated')]),
        'variant_010_plan_hl_subset': dict(sm(rows_b) or {}, counts=oc_b, positions=r10.describe_positions(pos_b)),
        'stress_hl_short_on_hl_own_highs': dict(sm(hl_own) or {}, liquidated=sum(1 for r in hl_own if r.get('liquidated')),
                                                topups=sum(r.get('topups') or 0 for r in hl_own)),
        'stress': r10.stress(real),
        'delisted': sum(1 for r in real if r.get('delisted')),
    }


POS_KEYS = ('coin', 'pair', 'short', 'long', 'dir', 't_in', 't_out', 'hold_ms', 'cost', 'cost_design', 'slip',
            'threshold', 'signal', 'cls', 'week', 'month')


def run_stage1():
    commit = guard('stage1')
    panel = Panel(c12.STAGE1_END, STAGE1_PANEL, price_end=c12.STAGE1_END)
    append_row({'registration': REG.name, 'mode': 'stage1_started', 'registration_commit': commit,
                'run_at': dt.datetime.now(dt.UTC).isoformat()})
    pos, cnt = plan_on(panel, c12.STAGE1_START, c12.STAGE1_END)
    rows, oc = r10.outcomes(panel, pos)
    assert all(r['t_x'] < c12.STAGE1_END for r in rows)
    holes = price_holes(panel, c12.STAGE1_START)
    print('primary counts before the decision:', json.dumps(oc), '; venue-coins with funding but no prices:', holes)
    ok, det = c12.stage1_gate(rows)
    end_, date_, ext_, _ = owner_choice(REG.read_text())
    out = {'registration': REG.name, 'mode': 'stage1', 'registration_commit': commit,
           'owner': {'end_date': date_, 'extension': ext_}, 'price_holes': holes,
           'code_hashes': hashes(GUARDED_CODE), 'data_hashes': hashes(GUARDED_DATA),
           'run_at': dt.datetime.now(dt.UTC).isoformat(), 'versions': r10.versions(),
           'params': {'pairs': c12.PAIRS_012, 'start': c12.STAGE1_START, 'end': c12.STAGE1_END, 'stage1_gate': 'sign: mean net > 0 (point estimate)',
                      'family_trial': c12.FAMILY_TRIAL},
           'counts': {**cnt, **oc}, 'positions': r10.describe_positions(pos),
           'stage1': {**det, 'passed': ok}, 'decision': c12.decide_stage1(ok)}
    out['reported'] = {'six_criteria_010': carry.summarise(rows), **reported(panel, rows, c12.STAGE1_START, c12.STAGE1_END)}
    append_row(out)
    print(json.dumps({k: out['stage1'][k] for k in ('primary_month', 'median_reported', 'z_reported', 'criteria',
                                                     'passed', 'annualised_reported')}, indent=1, default=str))
    print('decision', out['decision'])


def run_stage2(extension=False):
    """Stage 2 (2026-07-01 -> the owner's end) or the pre-registered extension (2026-07-01 ->
    2027-06-30, cumulative), on the panel built after the gated fetch. Same rule, same code."""
    mode = 'extension' if extension else 'stage2'
    commit = guard(mode)
    end, date, ext, _ = owner_choice(REG.read_text())
    pdir = stage2_dir(date)
    if extension:
        end, pdir = c12.EXTENSION_END, EXTENSION_DIR
    rows_all = result_rows(HERE, 'results.jsonl', REG.name)
    built = [r for _, r in rows_all if r.get('mode') == f'{mode}_built']
    if len(built) != 1 or built[0].get('panel_sha256') != panel_hashes(pdir):
        sys.exit(f'the {mode} panel differs from the hashes recorded when it was built — refusing')
    (sline, _), = [(l, r) for l, r in rows_all if r.get('mode') == 'stage1']
    append_row({'registration': REG.name, 'mode': f'{mode}_started', 'registration_commit': commit,
                'stage1_row_sha256': line_sha(sline), 'run_at': dt.datetime.now(dt.UTC).isoformat()})
    panel = Panel(end, pdir, price_end=end, allow_stage2=True)
    pos, cnt = plan_on(panel, c12.STAGE2_START, end)
    rows, oc = r10.outcomes(panel, pos)
    assert all(r['t_x'] < end for r in rows)
    holes = price_holes(panel, c12.STAGE2_START)
    print('primary counts before the decision:', json.dumps(oc), '; venue-coins with funding but no prices:', holes,
          '; coverage at build:', built[0].get('coverage'))
    ok, det = c12.stage2_confirm(rows, ext)
    out = {'registration': REG.name, 'mode': mode, 'end': end, 'end_date': date, 'extension_registered': ext,
           'q': det['q'], 'registration_commit': commit, 'stage1_row_sha256': line_sha(sline),
           'run_at': dt.datetime.now(dt.UTC).isoformat(), 'versions': r10.versions(),
           'panel_sha256': panel_hashes(pdir), 'code_hashes': hashes(GUARDED_CODE),
           'counts': {**cnt, **oc}, 'positions': r10.describe_positions(pos), 'price_holes': holes,
           'owner': {'end_date': date, 'extension': ext},
           'stage2': {'confirm': ok, **det},
           'decision': c12.decide_stage2(ok, det, ext, extension)}
    out['reported'] = {'six_criteria_010': carry.summarise(rows, placebo_seeds=200),
                       **reported(panel, rows, c12.STAGE2_START, end)}
    append_row(out)
    print(json.dumps({k: out['stage2'][k] for k in ('primary_month', 'z', 'criteria', 'annualised_reported')},
                     indent=1, default=str))
    print('decision', out['decision'])


def counts():
    """Unconditional counts only (000f): positions OPENED by the HL-pair rule, by pair / class / year /
    tier; never split by position state, never a share of qualifying times, never per coin. Stage-1
    block on 012's Stage-1 panel (entry prices required); discovery on 010's registered panel. Never a
    Stage-2 panel."""
    p1 = Panel(c12.STAGE1_END, STAGE1_PANEL, price_end=c12.STAGE1_END)
    pos1, cnt1 = plan_on(p1, c12.STAGE1_START, c12.STAGE1_END)
    pos1b = c12.hl_subset_of(plan_on(p1, c12.STAGE1_START, c12.STAGE1_END, pairs=carry.PAIRS)[0])
    pd = r10.Panel(carry.DISC_END)
    posd, cntd = plan_on(pd, carry.DISC_START, carry.DISC_END)
    return {'stage1_primary': {'counts': cnt1, 'positions': r10.describe_positions(pos1)},
            'stage1_variant_010_plan_hl_subset': {'positions': r10.describe_positions(pos1b)},
            'discovery_2023_24_primary_rule_counts_only': {'counts': cntd, 'positions': r10.describe_positions(posd)},
            'stage1_venue_coins_with_funding_but_no_prices': price_holes(p1, c12.STAGE1_START),
            'stage1_coins_in_panel': len(p1.coins)}, pos1


def main():
    if '--counts' in sys.argv:
        print(json.dumps(counts()[0], indent=1, default=str))
    elif '--stage1' in sys.argv:
        run_stage1()
    elif '--stage2' in sys.argv:
        run_stage2(False)
    elif '--extension' in sys.argv:
        run_stage2(True)
    else:
        sys.exit(__doc__)


if __name__ == '__main__':
    main()
