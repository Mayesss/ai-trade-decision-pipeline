"""Registration 014 (N2(b), Deribit options family, T1 + T2) — the guarded run.

    .venv/bin/python -I r014_run.py --discovery   # discovery 2021-04-02 -> 2023-12-29 (guarded; once)
    .venv/bin/python -I r014_run.py --holdout     # decision holdout 2024-01-05 -> 2026-06-26 (its own gate; once)
    .venv/bin/python -I r014_run.py --power       # SYNTHETIC only (no data read): §3 tables -> data/r014/logs/power.json

DISCOVERY refuses unless registrations/014-short-vol-deribit.md says REGISTERED, is tracked by git,
committed and unmodified; the guarded code is clean; code and data match the registration's §9 SHA-256
table; and no 014 row of ANY kind exists in results.jsonl — in the working tree or in the history of any
branch (`git log --all -p`). It appends a `discovery_started` row BEFORE any outcome is computed, then
exactly one `discovery` result row (decision, every number the decision is recomputed from, the
registration commit, the code and data hashes).

HOLDOUT additionally requires ALL of:
  (g1) exactly one 014 discovery result row across the working tree and every branch's history, and
       exactly one `discovery_started` row with the same registration commit;
  (g2) both rows committed (present in HEAD:results.jsonl);
  (g3) the row's registration commit is an ancestor of HEAD; the guarded code is unchanged since (git
       diff and the stored hashes); the stored data hashes equal the §9 table; the registration changed
       since only by added approval lines;
  (g4) every criterion and the decision recomputed from the NUMBERS stored in the row
       (`shortvol.recheck_discovery`), never from a label — and the decision is read_holdout;
  (h') a line "Holdout: APPROVED <YYYY-MM-DD> row <sha256 of the discovery row line>" in the
       registration, committed strictly after the commit that added the row;
  (i)  no holdout row (started or result) anywhere: the holdout is read once.

READ WHITELIST: the run opens only data/r014/panel_discovery/ (discovery) or data/r014/panel_holdout/
(holdout, after the gate), the registration and results.jsonl. `Panel` refuses any other path, refuses
the holdout panel without the gate's flag, checks every panel file against its MANIFEST, and asserts
every row lies inside its calendar's bounds (nothing dated on or after 2026-07-01 exists in either).
"""
import datetime as dt
import gzip
import hashlib
import json
import platform
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from ct import shortvol as sv  # noqa: E402

REG = HERE / 'registrations/014-short-vol-deribit.md'
RESULTS = HERE / 'results.jsonl'
DATA_ROOT = HERE / 'data' / 'r014'
PANELS = {'discovery': DATA_ROOT / 'panel_discovery', 'holdout': DATA_ROOT / 'panel_holdout'}
SERIES_BOUNDS = {'discovery': (sv.ms(2021, 3, 1), sv.DISC_END), 'holdout': (sv.ms(2023, 12, 24), sv.HOLD_END)}
PANEL_FILES = ('dvol.json', 'perp.json', 'funding.json', 'trades.jsonl.gz', 'MANIFEST.json')
APPROVAL = re.compile(r'^Holdout: APPROVED (\d{4}-\d{2}-\d{2}) row ([0-9a-f]{64})\s*$', re.M)
HASH_ROW = re.compile(r'^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|', re.M)

GUARDED_CODE = ('ct/__init__.py', 'ct/shortvol.py', 'ct/shortvol_sim.py', 'r014_run.py', 'r014_fetch.py', 'r014_tail.py',
                'r014_build.py')
GUARDED_DATA = tuple(f'data/r014/{p}/{f}' for p in ('panel_discovery', 'panel_holdout') for f in PANEL_FILES)


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


def result_rows(root, results_rel, name):
    """[(line, row)] of every distinct results.jsonl line of registration `name`: the working tree plus
    every ADDED line in the history of every branch (`git log --all -p`)."""
    root = Path(root)
    texts = []
    res = root / results_rel
    if res.exists():
        texts.extend(res.read_text().splitlines())
    hist = _git(root, 'log', '--all', '-p', '--format=', '--', results_rel)
    texts.extend(line[1:] for line in hist.stdout.splitlines() if line.startswith('+') and not line.startswith('+++'))
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


def first_commit_containing(root, rel, needle):
    """The oldest commit on HEAD's history whose version of `rel` contains the line `needle`."""
    for c in _git(root, 'log', '--format=%H', '--reverse', '--', rel).stdout.split():
        if needle in _git(root, 'show', f'{c}:{rel}').stdout.splitlines():
            return c
    return None


# ------------------------------------------------------------------ the guard
def guard_failures(root, reg_rel, code=GUARDED_CODE, data=GUARDED_DATA, results_rel='results.jsonl', mode='discovery'):
    """Every reason the run must not start; ([], commit) when it may."""
    root = Path(root)
    reg = root / reg_rel
    out = []
    text = reg.read_text() if reg.exists() else ''
    m = re.search(r'^\*\*Status: (\w+)', text, re.M)
    if not m or m.group(1) != 'REGISTERED':
        out.append(f'{reg.name} is not REGISTERED — refusing to compute any outcome.')
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
    head = set(_git(root, 'show', f'HEAD:{results_rel}').stdout.splitlines())
    disc = [(ln, r) for ln, r in rows if r.get('mode') == 'discovery']
    started = [(ln, r) for ln, r in rows if r.get('mode') == 'discovery_started']
    if len(disc) != 1:
        out.append(f'(g1) {len(disc)} {reg.name} discovery result rows across the tree and all branches; exactly one is required')
    else:
        line, row = disc[0]
        rc = row.get('registration_commit') or ''
        if len(started) != 1 or started[0][1].get('registration_commit') != rc:
            out.append('(g1) exactly one discovery_started row with the same registration commit is required')
        elif started[0][0] not in head:
            out.append('(g2) the discovery_started row is not committed in HEAD:results.jsonl')
        if line not in head:
            out.append('(g2) the discovery row is not committed in HEAD:results.jsonl')
        if not rc or _git(root, 'merge-base', '--is-ancestor', rc, 'HEAD').returncode:
            out.append("(g3) the row's registration commit is not an ancestor of HEAD")
        else:
            if _git(root, 'diff', '--quiet', rc, 'HEAD', '--', *code).returncode:
                out.append('(g3) the guarded code changed since the discovery run')
            changed = [l[1:] for l in _git(root, 'diff', '-U0', rc, 'HEAD', '--', reg_rel).stdout.splitlines()
                       if l[:1] in '+-' and not l.startswith(('+++', '---'))]
            if any(l.strip() and not APPROVAL.fullmatch(l.rstrip()) for l in changed):
                out.append('(g3) the registration changed since the discovery run beyond added approval lines')
        if row.get('code_hashes') != {f: sha256(root / f) for f in code if (root / f).exists()}:
            out.append('(g3) the code hashes stored in the row differ from the code on disk')
        if row.get('data_hashes') != {f: table.get(f) for f in data}:
            out.append('(g3) the data hashes stored in the row differ from the registration table')
        if not row.get('weekly'):
            out.append('(g4) the discovery row stores no weekly series to recompute its summary from')
        dec, fails = sv.recheck_discovery(row.get('results') or {}, weekly=row.get('weekly') or {})
        out.extend(f'(g4) {x}' for x in fails)
        if [dec[0], list(dec[1])] != row.get('decision'):
            out.append(f"(g4) the stored decision {row.get('decision')} differs from the recomputed {[dec[0], list(dec[1])]}")
        if dec[0] != 'read_holdout':
            out.append(f'(g4) the recomputed decision is {dec[0]}, not read_holdout')
        appr = [a for a in APPROVAL.finditer(text) if a.group(2) == line_sha(line)]
        if not appr:
            out.append("(h') no committed line 'Holdout: APPROVED <date> row <sha256 of the discovery row>'")
        else:
            row_c = first_commit_containing(root, results_rel, line)
            app_c = first_commit_containing(root, reg_rel, appr[0].group(0).rstrip())
            if not row_c or not app_c or row_c == app_c or _git(root, 'merge-base', '--is-ancestor', row_c, app_c).returncode:
                out.append("(h') the approval must be committed strictly after the commit that added the discovery row")
    if any(r.get('mode') in ('holdout_started', 'holdout') for _, r in rows):
        out.append(f'(i) a holdout row (started or result) for {reg.name} already exists — the holdout is read once')
    return out, commit


def guard(mode):
    failures, commit = guard_failures(HERE, str(REG.relative_to(HERE)), mode=mode)
    if failures:
        sys.exit('\n'.join(failures))
    return commit


def append_row(row, path=None):
    line = json.dumps(row, default=str, sort_keys=True)
    with open(path or RESULTS, 'a') as f:
        f.write(line + '\n')
    return line


# ------------------------------------------------------------------ the panel (read whitelist)
def assert_allowed(path, root=None):
    p = Path(path).resolve()
    roots = [Path(root).resolve()] if root else [d.resolve() for d in PANELS.values()]
    if not any(p == r or p.is_relative_to(r) for r in roots):
        raise PermissionError(f'014 reads only {[str(r) for r in roots]}; refused: {p}')
    return p


class Panel:
    """One calendar's panel. The holdout panel opens only with allow_holdout=True (the guarded run)."""

    def __init__(self, cal, panel_dir=None, allow_holdout=False):
        if cal not in PANELS:
            raise ValueError(cal)
        if cal == 'holdout' and not allow_holdout:
            raise PermissionError('the holdout panel is opened only by the guarded holdout run')
        d = Path(panel_dir) if panel_dir else PANELS[cal]
        if panel_dir is None:
            assert_allowed(d)
        if cal == 'discovery' and 'holdout' in d.name:
            raise PermissionError('a discovery run never opens a holdout directory')
        man = json.loads((d / 'MANIFEST.json').read_text())
        if man.get('calendar') != cal:
            raise PermissionError(f'{d} is the {man.get("calendar")} panel, not {cal}')
        for f, h in man['files'].items():
            if sha256(d / f) != h:
                raise PermissionError(f'{d / f} differs from its MANIFEST')
        a, b = SERIES_BOUNDS[cal]
        self.cal, self.dir, self.manifest = cal, d, man
        self.series = {}
        for k in ('dvol', 'perp', 'funding'):
            part = json.loads((d / f'{k}.json').read_text())
            for c, rows in part.items():
                if any(not a <= r[0] < b for r in rows):
                    raise PermissionError(f'{k} {c}: a row outside the {cal} bounds')
            self.series[k] = part
        self.trades = {}
        with gzip.open(d / 'trades.jsonl.gz', 'rt') as g:
            for line in g:
                if line.strip():
                    r = json.loads(line)
                    if not r['timestamp'] < b:
                        raise PermissionError(f'a trade at {sv.iso(r["timestamp"])} outside the {cal} bounds')
                    self.trades.setdefault((r['coin'], r['friday']), []).append(r)
        self.perp_at = {c: {r[0] + sv.HOUR: r[4] for r in self.series['perp'][c]} for c in sv.COINS}
        self.dvol_at = {c: {r[0] + sv.HOUR: r[4] for r in self.series['dvol'][c]} for c in sv.COINS}
        self.dvol_bars = {c: [(r[0], r[4]) for r in self.series['dvol'][c]] for c in sv.COINS}
        self.funding = {c: {r[0]: r[1] for r in self.series['funding'][c]} for c in sv.COINS}


# ------------------------------------------------------------------ outcomes (no gate in here)
def path_for(panel, coin, E, X, fills):
    """Hourly (t, perp price at t, DVOL at t) from E to X; a missing hour takes the last value (counted)."""
    out, lastS, lastD = [], None, None
    for t in range(E, X + 1, sv.HOUR):
        S, D = panel.perp_at[coin].get(t), panel.dvol_at[coin].get(t)
        if S is None:
            fills['perp_hours_filled'] += 1
            S = lastS
        if D is None:
            fills['dvol_hours_filled'] += 1
            D = lastD
        if S is None:
            return None
        out.append((t, S, D))
        lastS, lastD = S, D
    return out


def positions(panel, cal, *, hedge_every_h=sv.HEDGE_EVERY_H, half_spread=None, funding=True,
              beta_iv=sv.BETA_IV, perp_cost=sv.PERP_TAKER + sv.PERP_SLIP, liq_n=sv.SIZE_N):
    """Every position of the calendar under one parameter set. Returns (rows, counts)."""
    counts = {'missing': {}, 'index_source': {}, 'settle_source': {}, 'perp_hours_filled': 0, 'dvol_hours_filled': 0,
              'liquidated': 0, 'positions': 0, 'no_path': 0}
    rows = []
    for E in sv.calendar_entries(cal):
        for c in sv.COINS:
            em = sv.straddle_entry(c, E, panel.trades.get((c, E), []), panel.perp_at[c].get(E))
            if 'missing' in em:
                counts['missing'][em['missing']] = counts['missing'].get(em['missing'], 0) + 1
                continue
            X = em['expiry']
            path = path_for(panel, c, E, X, counts)
            if path is None:
                counts['no_path'] += 1
                continue
            px, src, _ = sv.settlement(panel.trades.get((c, X), []), X, panel.perp_at[c].get(X))
            counts['settle_source'][src] = counts['settle_source'].get(src, 0) + 1
            if px is None:
                counts['missing']['no_settlement'] = counts['missing'].get('no_settlement', 0) + 1
                continue
            counts['index_source'][em['S_source']] = counts['index_source'].get(em['S_source'], 0) + 1
            hs = None if half_spread is None else half_spread[c]
            o = sv.position_pnl(em, path, px, hedge_every_h=hedge_every_h, half_spread=hs,
                                funding=panel.funding[c] if funding else None, liq_n=liq_n, beta_iv=beta_iv,
                                perp_cost=perp_cost)
            cost = o['spread'] + o['fees'] + o['delivery'] + o['hedge_fees']
            counts['positions'] += 1
            counts['liquidated'] += bool(o['liquidated'])
            rows.append({'entry': E, 'coin': c, 'ret_notional': o['ret_notional'], 'ret_premium': o['ret_premium'],
                         'gross_notional': (o['net'] + cost) / o['S0'], 'cost_notional': cost / o['S0'],
                         'premium_notional': o['premium_mark'] / o['S0'], 'liquidated': bool(o['liquidated']),
                         'components_notional': {k: (o[k] or 0.0) / o['S0'] for k in
                                                 ('premium_mark', 'spread', 'fees', 'payoff', 'delivery', 'hedge_pnl',
                                                  'funding_pnl', 'hedge_fees')}})
    return rows, counts


def stressed_entries(panel, cal):
    out, undefined = set(), 0
    for E in sv.calendar_entries(cal):
        flags = {c: sv.stress_flag(panel.dvol_bars[c], E, c) for c in sv.COINS}
        undefined += sum(1 for f in flags.values() if f is None)
        if sv.book_stressed(flags):
            out.add(E)
    return out, undefined


def series_of(rows, stressed):
    t1 = sv.weekly_book(rows)
    t2 = sv.stand_aside(t1, stressed)
    return t1, t2


def reported_blocks(panel, cal, rows, t1, t2):
    """Reported only; none decides anything and none may become a primary."""
    def blk(rs):
        if not rs:
            return None
        b = sv.weekly_book(rs)
        return sv.summary([w['r'] for w in b])

    def mean(xs):
        return sum(xs) / len(xs) if xs else None
    var = {}
    for name, kw in (('p50_half_spread', {'half_spread': sv.HALF_SPREAD_P50}),
                     ('hedge_cost_x2', {'perp_cost': 2 * (sv.PERP_TAKER + sv.PERP_SLIP)}),
                     ('unhedged', {'hedge_every_h': None}), ('hedge_8h', {'hedge_every_h': 8}),
                     ('no_funding', {'funding': False}), ('beta_iv_2_liquidation_stress', {'beta_iv': 2.0}),
                     ('no_liquidation_check', {'liq_n': None})):
        r_, c_ = positions(panel, cal, **kw)
        var[name] = {'summary': blk(r_), 'liquidated': c_['liquidated']}
    comps = {k: mean([r['components_notional'][k] for r in rows]) for k in rows[0]['components_notional']} if rows else {}
    return {
        'variants': var,
        'by_coin': {c: blk([r for r in rows if r['coin'] == c]) for c in sv.COINS},
        'return_on_premium_mean': mean([r['ret_premium'] for r in rows]),
        'components_mean_per_notional': comps,
        'placebo_T1': sv.placebo(t1), 'placebo_T2': sv.placebo(t2),
        'sizing_statement': sv.sizing_statement(),
        'economic_after_reported_T1': sv.economic_after(sv.summary([w['r'] for w in t1])) if len(t1) > 2 else None,
        'economic_after_reported_T2': sv.economic_after(sv.summary([w['r'] for w in t2])) if len(t2) > 2 else None,
        'drawdown_vs_preregistered_month_T1': {'max_drawdown_at_n': sv.summary([w['r'] for w in t1])['max_drawdown_at_n'],
                                               'preregistered_worst_month_at_n': sv.WORST_MONTH_BOOK * sv.SIZE_N},
    }


def versions():
    return {'python': platform.python_version()}


def hashes(files):
    return {f: sha256(HERE / f) for f in files if (HERE / f).exists()}


# ------------------------------------------------------------------ the two modes
def run_discovery():
    commit = guard('discovery')
    panel = Panel('discovery')
    append_row({'registration': REG.name, 'mode': 'discovery_started', 'registration_commit': commit,
                'run_at': dt.datetime.now(dt.UTC).isoformat()})
    rows, counts = positions(panel, 'discovery')
    stressed, undefined = stressed_entries(panel, 'discovery')
    t1, t2 = series_of(rows, stressed)
    counts.update({'weeks_T1': len(t1), 'stressed_weeks': sum(1 for w in t1 if w['entry'] in stressed),
                   'stress_flags_undefined': undefined, 'entries': len(sv.calendar_entries('discovery'))})
    print('counts before the decision:', json.dumps(counts, default=str))
    results, decision = sv.evaluate_discovery({'T1': [w['r'] for w in t1], 'T2': [w['r'] for w in t2]})
    out = {'registration': REG.name, 'mode': 'discovery', 'registration_commit': commit,
           'code_hashes': hashes(GUARDED_CODE), 'data_hashes': hashes(GUARDED_DATA),
           'run_at': dt.datetime.now(dt.UTC).isoformat(), 'versions': versions(),
           'params': {'size_n': sv.SIZE_N, 'hedge_every_h': sv.HEDGE_EVERY_H, 'disc_t_floor': sv.DISC_T_FLOOR,
                      'q': sv.Q_DISCOVERY, 'stress_ratio': sv.STRESS_RATIO, 'half_spread': sv.HALF_SPREAD},
           'counts': counts, 'results': results, 'decision': [decision[0], list(decision[1])],
           'weekly': {'T1': [[w['entry'], w['r']] for w in t1], 'T2': [[w['entry'], w['r']] for w in t2]}}
    out['reported'] = reported_blocks(panel, 'discovery', rows, t1, t2)
    append_row(out)
    print(json.dumps({k: {'summary': v['summary'], 'criteria': v['criteria'], 't_crit': v['t_crit'], 'pass': v['pass']}
                      for k, v in results.items()}, indent=1, default=str))
    print('decision', out['decision'])


def run_holdout():
    commit = guard('holdout')
    rows_all = result_rows(HERE, 'results.jsonl', REG.name)
    (dline, drow), = [(l, r) for l, r in rows_all if r.get('mode') == 'discovery']
    dec, _ = sv.recheck_discovery(drow['results'], weekly=drow['weekly'])
    trials = list(dec[1])
    k = len(trials)
    panel = Panel('holdout', allow_holdout=True)
    append_row({'registration': REG.name, 'mode': 'holdout_started', 'registration_commit': commit,
                'discovery_row_sha256': line_sha(dline), 'trials': trials, 'run_at': dt.datetime.now(dt.UTC).isoformat()})
    rows, counts = positions(panel, 'holdout')
    stressed, undefined = stressed_entries(panel, 'holdout')
    t1, t2 = series_of(rows, stressed)
    counts.update({'weeks_T1': len(t1), 'stressed_weeks': sum(1 for w in t1 if w['entry'] in stressed),
                   'stress_flags_undefined': undefined, 'entries': len(sv.calendar_entries('holdout'))})
    print('counts before the decision:', json.dumps(counts, default=str))
    series = {'T1': [w['r'] for w in t1], 'T2': [w['r'] for w in t2]}
    confirms, details, econ = {}, {}, {}
    for t in trials:
        ok, det = sv.holdout_existence(series[t], k)
        confirms[t], details[t] = ok, det
        econ[t] = sv.economic_after(det['summary'])
    decision = sv.decide_holdout(confirms, econ)
    out = {'registration': REG.name, 'mode': 'holdout', 'registration_commit': commit,
           'discovery_row_sha256': line_sha(dline), 'trials': trials, 'k': k,
           'code_hashes': hashes(GUARDED_CODE), 'data_hashes': hashes(GUARDED_DATA),
           'run_at': dt.datetime.now(dt.UTC).isoformat(), 'versions': versions(), 'counts': counts,
           'existence': {t: {'confirm': confirms[t], **details[t]} for t in trials}, 'economic_after': econ,
           'decision': decision, 'weekly_T1': [[w['entry'], w['r']] for w in t1]}
    out['reported'] = reported_blocks(panel, 'holdout', rows, t1, t2)
    append_row(out)
    print(json.dumps(out['existence'], indent=1, default=str))
    print('economic (applied after existence):', json.dumps(econ, default=str))
    print('decision', decision)


def run_power():
    """SYNTHETIC ONLY: reads no data. Writes data/r014/logs/power.json (the registration's §3 tables)."""
    from ct import shortvol_sim as sim
    out = {'constructions': sim.construction_table(n_weeks=3000, sims=1000),
           'primary': sim.primary_table(),
           'sizing': {f'{nm}_{v}': sim.sizing(params=P, vrp=v) for P, nm in ((sim.PARAMS, 'base'), (sim.FAT, 'fat'))
                      for v in (0.10, 0.15)},
           'sizing_realistic_000m': sim.sizing_realistic(list(sv.REALISTIC_WEEK.values())),
           'floors_registered_procedure': sim.registered_floors('H24'),
           'sizing_statement': sv.sizing_statement()}
    (DATA_ROOT / 'logs').mkdir(parents=True, exist_ok=True)
    (DATA_ROOT / 'logs' / 'power.json').write_text(json.dumps(out, indent=1, default=str) + '\n')
    print('wrote', DATA_ROOT / 'logs' / 'power.json')


def main():
    if '--discovery' in sys.argv:
        run_discovery()
    elif '--holdout' in sys.argv:
        run_holdout()
    elif '--power' in sys.argv:
        run_power()
    else:
        sys.exit(__doc__)


if __name__ == '__main__':
    main()
