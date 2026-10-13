"""Registration 009 (L4, weekly funding carry F1 on Binance USDⓈ-M) — the run.

    .venv/bin/python -I r009_run.py                     # discovery (guarded); writes a started row, then the result row
    .venv/bin/python -I r009_run.py --holdout-A         # holdout A (its own guard; read once)
    .venv/bin/python -I r009_run.py --decline-holdout-A # owner declines the holdout-A pull (a terminal 009 state)

DISCOVERY refuses unless registrations/009-binance-weekly-factors.md says REGISTERED, is tracked and
committed, is unmodified, the guarded code is clean, the code and data match the registration's §8
SHA-256 table, and no 009 row of any kind exists in results.jsonl — in the working tree OR anywhere
in the history of any branch (`git log --all`). It appends a `discovery_started` row BEFORE any
outcome is computed (a crash cannot silently re-run), then the result row, which stores the
registration commit and the SHA-256 of the guarded code and data.

HOLDOUT A (Revision 4, review M1) additionally requires ALL of:
  (g1) exactly one 009 discovery result row across the working tree and every branch's history;
  (g2) that row is committed (present in HEAD:results.jsonl);
  (g3) its registration commit is an ancestor of HEAD, the guarded code is unchanged since (git diff
       and the stored hashes);
  (g4) every criterion, both pull conditions and the decision recomputed from the NUMBERS stored in
       the row (`factors.recheck_discovery`), never from a label — and the decision is pull_holdout_A;
  (h') a line "Holdout A: APPROVED <YYYY-MM-DD> row <sha256 of the row line>" in the registration,
       committed in a commit strictly after the commit that added the row;
  (i)  no holdout row (started, result or declined) anywhere.
It checks the holdout panel against its MANIFEST and stores the hashes in its row.

WHITELIST (Revision 4, review M4; 000g): 009 reads only data/binance_years/, its holdout directory,
the hashed L4 listing and exchangeInfo, the registrations and results.jsonl. `checked` refuses any
other path (data/carry/, data/scout_l2/, data/binance/ …; carry-guard).
"""
import datetime as dt
import hashlib
import json
import platform
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))                    # -I drops the script dir; the code dir is added explicitly

import duckdb  # noqa: E402

from ct import binance_years as by  # noqa: E402
from ct import factors as fx  # noqa: E402

REG = HERE / 'registrations/009-binance-weekly-factors.md'
RESULTS = HERE / 'results.jsonl'
DATA = HERE / 'data/binance_years'
PANEL = DATA / 'panel'
HOLD_DIR = HERE / 'data/binance_years_holdoutA'
HOLD_PANEL = HOLD_DIR / 'panel'
LISTING = HERE / 'data/scout_l4/listings'
ALLOWED = ('data/binance_years/', 'data/binance_years_holdoutA/', 'data/scout_l4/listings/',
           'data/scout_l4/exchangeinfo/', 'registrations/', 'results.jsonl')
APPROVAL = re.compile(r'^Holdout A: APPROVED (\d{4}-\d{2}-\d{2}) row ([0-9a-f]{64})\s*$', re.M)

GUARDED_CODE = ('ct/__init__.py', 'ct/factors.py', 'ct/binance_years.py', 'ct/stats.py', 'r009_run.py',
                'r009_pull.py', 'r009_pull_holdout.py')
GUARDED_DATA = ('data/binance_years/MANIFEST.tsv', 'data/binance_years/panel/bars_1h.parquet',
                'data/binance_years/panel/funding.parquet', 'data/binance_years/panel/metrics_1h.parquet',
                'data/binance_years/panel/life.json',
                'data/scout_l4/listings/daily_metrics.csv', 'data/scout_l4/listings/daily_klines_1d.csv',
                'data/scout_l4/listings/monthly_klines_1h.csv', 'data/scout_l4/listings/monthly_fundingRate.csv',
                'data/scout_l4/exchangeinfo/fapi_exchangeInfo.json', 'data/binance_years_holdoutA/plan.csv')
HASH_ROW = re.compile(r'^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|', re.M)
DISCOVERY_MODES = ('discovery',)
HOLDOUT_MODES = ('holdout_A_started', 'holdout_A', 'holdout_A_declined')


def checked(path, root=HERE):
    """WHITELIST: every path 009 opens must lie under one of ALLOWED (relative to the research root);
    anything else — data/carry/, data/scout_l2/, data/binance/ — is refused (000g, review M4; carry-guard)."""
    p = Path(path).resolve()
    try:
        rel = str(p.relative_to(Path(root).resolve())).replace('\\', '/')
    except ValueError:
        raise PermissionError(f'009 reads only inside {root}: {p}')
    if not any(rel == a.rstrip('/') or rel.startswith(a) for a in ALLOWED):
        raise PermissionError(f'009 never reads {rel} (whitelist: {ALLOWED}; 000g)')
    return Path(path)


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
    """[(line, row)] of every distinct results.jsonl line of registration `name`, from the working tree
    and from every ADDED line in the history of every branch (`git log --all -p`)."""
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
        body = _git(root, 'show', f'{c}:{rel}').stdout
        if needle in body.splitlines():
            return c
    return None


def guard_failures(root, reg_rel, code=GUARDED_CODE, data=GUARDED_DATA, results_rel='results.jsonl', mode='discovery'):
    """Every reason the run must not start; ([], commit) when it may. Common: status REGISTERED;
    (a) tracked; (b) unmodified vs HEAD; (c) has a commit; (d) guarded code clean; (e) code and data
    match the SHA-256 table. Discovery: (f) no 009 row of any kind anywhere. Holdout A / decline:
    (g1)-(g4), (h') (holdout only) and (i), as in the module docstring."""
    root = Path(root)
    reg = root / reg_rel
    out = []
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
    disc = [(ln, r) for ln, r in rows if r.get('mode') in DISCOVERY_MODES]
    if len(disc) != 1:
        out.append(f'(g1) {len(disc)} {reg.name} discovery result rows across the tree and all branches; exactly one is required')
    else:
        line, row = disc[0]
        head = _git(root, 'show', f'HEAD:{results_rel}').stdout.splitlines()
        if line not in head:
            out.append('(g2) the discovery row is not committed in HEAD:results.jsonl')
        rc = row.get('registration_commit') or ''
        if not rc or _git(root, 'merge-base', '--is-ancestor', rc, 'HEAD').returncode:
            out.append('(g3) the row\'s registration commit is not an ancestor of HEAD')
        elif _git(root, 'diff', '--quiet', rc, 'HEAD', '--', *code).returncode:
            out.append('(g3) the guarded code changed since the discovery run')
        if row.get('code_hashes') != {f: sha256(root / f) for f in code if (root / f).exists()}:
            out.append('(g3) the code hashes stored in the row differ from the code on disk')
        dec, fails = fx.recheck_discovery(row.get('results') or {})
        out.extend(f'(g4) {x}' for x in fails)
        stored = row.get('decision')
        if [dec[0], list(dec[1])] != stored:
            out.append(f'(g4) the stored decision {stored} differs from the decision recomputed from the numbers {list(dec)}')
        if dec[0] != 'pull_holdout_A':
            out.append(f'(g4) the recomputed decision is {dec[0]}, not pull_holdout_A')
        if mode == 'holdout_A':
            appr = [a for a in APPROVAL.finditer(text) if a.group(2) == line_sha(line)]
            if not appr:
                out.append("(h') no committed line 'Holdout A: APPROVED <date> row <sha256 of the discovery row>'")
            else:
                row_c = first_commit_containing(root, results_rel, line)
                app_c = first_commit_containing(root, reg_rel, appr[0].group(0).rstrip())
                if not row_c or not app_c or row_c == app_c or _git(root, 'merge-base', '--is-ancestor', row_c, app_c).returncode:
                    out.append("(h') the approval must be committed strictly after the commit that added the discovery row")
    if any(r.get('mode') in HOLDOUT_MODES for _, r in rows):
        out.append(f'(i) a holdout-A row (started, result or declined) for {reg.name} already exists — holdout A is read once')
    return out, commit


def guard(mode='discovery'):
    failures, commit = guard_failures(HERE, str(REG.relative_to(HERE)), mode=mode)
    if failures:
        sys.exit('\n'.join(failures))
    return commit


def load_universe(split='discovery', panel=PANEL, max_ts=fx.LAST_EXIT_DISCOVERY):
    """Behaviour part (also used by r009_describe.py): features and the universe for a split. Bars
    and settlements after the split's last exit are invisible. Metrics-archive holes in a coin
    that kept trading are bridged (review minor 7, ICP)."""
    con = by.connect(str(checked(panel)), max_ts=max_ts)
    life = json.loads(checked(Path(panel) / 'life.json').read_text())
    life, merged = fx.bridge_trading_gaps(con, life)
    fx.life_table(con, life)
    rows = [fx.derive(r) for r in fx.features(con, fx.signal_weeks(split))]
    return con, rows, fx.universe_by_week(rows), merged


def subset(universe, pred):
    return {S: [r for r in rows if pred(r)] for S, rows in universe.items()}


def block(universe, key, outc, z, placebo_seeds=0, buffer=True):
    ic_rows = fx.weekly_ic(universe, key, outc)
    book = fx.run_book(universe, key, outc, buffer=buffer)
    ev = fx.evaluate(ic_rows, book, z)
    n = len(book) or 1
    for k in ('turnover', 'long', 'short', 'gross_price', 'gross_funding', 'beta_book'):
        ev[f'{k}_mean'] = sum(b[k] for b in book) / n if book else None
    ev['long_t'] = fx.stats.newey_west_t([b['long'] for b in book], fx.NW_LAGS)
    ev['short_t'] = fx.stats.newey_west_t([b['short'] for b in book], fx.NW_LAGS)
    ev['gross_t'] = fx.stats.newey_west_t([b['gross'] for b in book], fx.NW_LAGS)
    if placebo_seeds:
        ev['placebo'] = fx.placebo(universe, key, outc, placebo_seeds, z)
    return ev, ic_rows, book


def weekly_series(ic_rows, book, mkt):
    """The weekly series joined ON S (review minor 1)."""
    ic = {S: (v, n) for S, v, n in ic_rows}
    adj = dict(fx.beta_adjusted(book, mkt))
    return [{'S': b['S'], 'ic': ic.get(b['S'], (None, None))[0], 'n': ic.get(b['S'], (None, None))[1],
             'mkt': mkt.get(b['S']), 'beta_adjusted_net': adj[b['S']],
             **{k: b[k] for k in ('gross', 'gross_price', 'gross_funding', 'cost', 'net', 'long', 'short',
                                  'turnover', 'beta_book', 'n_long', 'n_short')}} for b in book]


def versions():
    return {'python': platform.python_version(), 'duckdb': duckdb.__version__}


def hashes(files):
    return {f: sha256(HERE / f) for f in files if (HERE / f).exists()}


def append_row(row):
    line = json.dumps(row, default=str)
    with open(checked(RESULTS), 'a') as f:
        f.write(line + '\n')
    return line


def outcome_counts(uni, outc):
    return {'weeks': len(uni), 'coin_weeks': sum(len(v) for v in uni.values()),
            'coin_weeks_scored': sum(1 for v in uni.values() for r in v if r['F1'] is not None),
            'coin_weeks_missing_settlement_feature': sum(1 for v in uni.values() for r in v if r.get('F1_missing_settlement')),
            'with_entry': sum(len(v) for v in outc.values()),
            'hold_missing_settlement': sum(1 for v in outc.values() for o in v.values() if o.get('funding_missing')),
            'delisted_in_week': sum(1 for v in outc.values() for o in v.values() if o['delisted'])}


def run_discovery():
    commit = guard('discovery')
    con, rows, uni, merged = load_universe()
    append_row({'registration': REG.name, 'mode': 'discovery_started', 'registration_commit': commit,
                'run_at': dt.datetime.now(dt.UTC).isoformat()})           # m10: before any outcome
    outc = fx.outcomes(con, uni, hard_end=fx.LAST_EXIT_DISCOVERY)
    z = fx.Z_CRIT
    mkt = fx.market_returns(uni, outc)
    out = {'registration': REG.name, 'mode': 'discovery', 'registration_commit': commit,
           'code_hashes': hashes(GUARDED_CODE), 'data_hashes': hashes(GUARDED_DATA),
           'run_at': dt.datetime.now(dt.UTC).isoformat(), 'versions': versions(),
           'params': {'family': list(fx.FAMILY), 'z_crit': z, 'nw_lags': fx.NW_LAGS, 'taker': fx.TAKER,
                      'tiers': [list(t) for t in fx.TIERS], 'floor_adv': fx.FLOOR_ADV, 'quantile': fx.QUANTILE,
                      'band': fx.BAND, 'fund_decimals': fx.FUND_DECIMALS, 'listed_min_days': fx.LISTED_MIN // fx.DAY,
                      'vol_days': fx.VOL_DAYS, 'weeks': len(uni), 'bridged_gaps': merged},
           'counts': outcome_counts(uni, outc)}
    res = {}
    for key in fx.FAMILY:
        ev, ic_rows, book = block(uni, key, outc, z, placebo_seeds=200)
        _, _, book2 = block(fx.with_slippage(uni, 2.0), key, outc, z)
        ev['pull_conditions'] = fx.pull_conditions(book, mkt, book2)
        near_cut = subset(uni, lambda r: r['cut_end'] - r['S'] > 14 * fx.DAY)
        ev['reported'] = {
            'weekly': weekly_series(ic_rows, book, mkt),
            'market_regression': fx.market_regression(book, mkt),
            'by_year': {str(y): block({S: r for S, r in uni.items() if fx.year_of(S) == y}, key, outc, z)[0]
                        for y in sorted({fx.year_of(S) for S in uni})},
            'by_tier': {t: block(subset(uni, lambda r, t=t: r['tier'] == t), key, outc, z)[0]
                        for t in sorted({r['tier'] for v in uni.values() for r in v})},
            'unbuffered_quintile': block(uni, key, outc, z, buffer=False)[0],
            'slippage_0x': block(fx.with_slippage(uni, 0.0), key, outc, z)[0],
            'slippage_2x': fx.evaluate(fx.weekly_ic(uni, key, outc), book2, z),
            'stress_week_tiers_min90': block(fx.with_stress_tiers(uni), key, outc, z)[0],
            'FUTURE_KNOWLEDGE_excluding_14d_before_cut': block(near_cut, key, outc, z)[0],
        }
        res[key] = ev
    out['results'] = res
    out['decision'] = fx.decide(res)
    append_row(out)
    print(json.dumps({k: {m: res[k][m] for m in ('ic_mean', 'ic_t', 'net_mean', 'net_t', 'net_median', 'halves',
                                                 'without_largest_positive_month', 'criteria', 'pass',
                                                 'pull_conditions', 'placebo')}
                      for k in fx.FAMILY}, indent=1, default=str))
    print('decision:', out['decision'])


def verify_holdout_manifest():
    """m11: every raw file the holdout MANIFEST marks ok still has its SHA-256; returns the hashes to store."""
    man = checked(HOLD_DIR / 'MANIFEST.tsv')
    bad = 0
    for line in man.read_text().splitlines()[1:]:
        path, size, h, verdict = line.split('\t')
        if verdict == 'ok' and sha256(checked(HOLD_DIR / path)) != h:
            bad += 1
    if bad:
        sys.exit(f'holdout MANIFEST mismatch in {bad} files — refusing')
    return {'manifest': sha256(man), **{p.name: sha256(p) for p in sorted(checked(HOLD_PANEL).glob('*')) if p.is_file()}}


def run_holdout_a():
    commit = guard('holdout_A')
    (line, row), = [x for x in result_rows(HERE, 'results.jsonl', REG.name) if x[1].get('mode') in DISCOVERY_MODES]
    dec, _ = fx.recheck_discovery(row['results'])
    taken = list(dec[1])
    data_h = verify_holdout_manifest()
    append_row({'registration': REG.name, 'mode': 'holdout_A_started', 'registration_commit': commit,
                'discovery_row_sha256': line_sha(line), 'run_at': dt.datetime.now(dt.UTC).isoformat()})
    con, rows, uni, merged = load_universe('holdout_A', HOLD_PANEL, fx.LAST_EXIT_HOLDOUT_A)
    outc = fx.outcomes(con, uni, hard_end=fx.LAST_EXIT_HOLDOUT_A)
    mkt = fx.market_returns(uni, outc)
    z = fx.z_crit(len(taken)) if len(taken) > 1 else fx.Z_HOLDOUT_ONE
    out = {'registration': REG.name, 'mode': 'holdout_A', 'registration_commit': commit,
           'discovery_row_sha256': line_sha(line), 'holdout_data_hashes': data_h, 'code_hashes': hashes(GUARDED_CODE),
           'run_at': dt.datetime.now(dt.UTC).isoformat(), 'versions': versions(), 'taken': taken,
           'counts': {**outcome_counts(uni, outc), 'bridged_gaps': merged}, 'results': {}}
    for key in taken:
        ic_rows = fx.weekly_ic(uni, key, outc)
        book = fx.run_book(uni, key, outc)
        book2 = fx.run_book(fx.with_slippage(uni, 2.0), key, outc)
        ok, ev = fx.holdout_confirm(ic_rows, book, len(taken), book_2x=book2)
        ev['reported'] = {'weekly': weekly_series(ic_rows, book, mkt),
                          'pull_conditions_beta_adjusted_and_2x': fx.pull_conditions(book, mkt, book2),
                          'stress_week_tiers_min90': block(fx.with_stress_tiers(uni), key, outc, z)[0],
                          'market_regression': fx.market_regression(book, mkt)}
        out['results'][key] = {'confirm': ok, **ev}
    append_row(out)
    print(json.dumps({k: {m: v[m] for m in ('confirm', 'ic_mean', 'ic_t', 'net_mean', 'net_t', 'net_median', 'net_2x_mean')}
                      for k, v in out['results'].items()}, indent=1, default=str))


def decline_holdout_a():
    """The owner declines the holdout-A pull: a terminal 009 state that unlocks D17 (review m8)."""
    commit = guard('decline')
    (line, _), = [x for x in result_rows(HERE, 'results.jsonl', REG.name) if x[1].get('mode') in DISCOVERY_MODES]
    append_row({'registration': REG.name, 'mode': 'holdout_A_declined', 'registration_commit': commit,
                'discovery_row_sha256': line_sha(line), 'run_at': dt.datetime.now(dt.UTC).isoformat()})
    print('holdout A declined; 009 is in a terminal state (D17)')


def main():
    if '--holdout-A' in sys.argv:
        run_holdout_a()
    elif '--decline-holdout-A' in sys.argv:
        decline_holdout_a()
    else:
        run_discovery()


if __name__ == '__main__':
    main()
