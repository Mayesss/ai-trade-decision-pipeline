"""Registration 014 — build the two calendar panels from one completed pull, and print COVERAGE ONLY.

    .venv/bin/python -I r014_build.py data/r014/pull_<YYYYMMDD>

Steps:
  1. verify the pull's SHA256SUMS (every file; refuse on any mismatch or an incomplete pull);
  2. split by calendar (the bounds are `ct.shortvol`'s, fixed in the registration):
       panel_discovery/  series rows stamped [2021-03-01, 2024-01-01); trade windows of the Fridays
                         2021-04-02 -> 2023-12-29 (the last one settles the 2023-12-22 entry)
       panel_holdout/    series rows stamped [2023-12-24, 2026-07-01) (8 days of look-back for the
                         stress rule); trade windows of the Fridays 2024-01-05 -> 2026-06-26
     Each panel: dvol.json, perp.json, funding.json ({coin: rows}), trades.jsonl.gz (canonical JSON
     lines, gzip mtime 0: a rebuild is byte-identical) and MANIFEST.json (pull, bounds, row counts,
     SHA-256 of each file). A panel directory is written once: an existing non-empty one is refused.
  3. COVERAGE (data/r014/logs/coverage.json): requests/bytes from the pull report; rows per file; hours
     missing per series inside each calendar; per calendar and coin, entries by availability class
     (`ct.shortvol.entry_coverage`: ok / no_index / no_trade_in_expiry / no_strike_within_band), the
     index source, chosen-strike trade age quantiles, settlements by covered-minute class, Fridays with
     a next-day (daily) expiry traded. NOTHING else: no mark, IV, premium, payoff, P&L, realised vol,
     stress state or any statistic pairing two quantities.
"""
import gzip
import hashlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from ct import shortvol as sv  # noqa: E402

DATA_ROOT = HERE / 'data' / 'r014'
PANELS = {'discovery': DATA_ROOT / 'panel_discovery', 'holdout': DATA_ROOT / 'panel_holdout'}
SERIES_BOUNDS = {'discovery': (sv.ms(2021, 3, 1), sv.DISC_END), 'holdout': (sv.ms(2023, 12, 24), sv.HOLD_END)}
FRIDAY_BOUNDS = {'discovery': (sv.ms(2021, 4, 2, 8), sv.ms(2023, 12, 29, 8)),
                 'holdout': (sv.ms(2024, 1, 5, 8), sv.ms(2026, 6, 26, 8))}
PANEL_FILES = ('dvol.json', 'perp.json', 'funding.json', 'trades.jsonl.gz', 'MANIFEST.json')


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def inside(path, root=None):
    root = root or DATA_ROOT
    p = Path(path).resolve()
    if not p.is_relative_to(Path(root).resolve()):
        raise PermissionError(f'014 reads and writes only under {root}: {p}')
    return p


def verify_pull(pull):
    pull = inside(pull)
    sums = pull / 'SHA256SUMS'
    if not sums.exists():
        raise SystemExit(f'{pull}: no SHA256SUMS — the pull is incomplete')
    listed = {}
    for line in sums.read_text().splitlines():
        h, name = line.split('  ', 1)
        listed[name] = h
    on_disk = {str(f.relative_to(pull)) for f in pull.rglob('*') if f.is_file() and f.name != 'SHA256SUMS'}
    if on_disk != set(listed):
        raise SystemExit(f'{pull}: files differ from SHA256SUMS: {sorted(on_disk ^ set(listed))[:5]}')
    bad = [n for n, h in listed.items() if sha256(pull / n) != h]
    if bad:
        raise SystemExit(f'{pull}: SHA-256 mismatch: {bad[:5]}')
    return pull, hashlib.sha256(sums.read_bytes()).hexdigest()


def friday_key(F):
    import datetime as dt
    return dt.datetime.fromtimestamp(F / 1000, dt.UTC).strftime('%Y-%m-%d')


def load_pull(pull):
    pull = inside(pull)
    ser = {k: {c: json.loads((pull / k / f'{c}.json').read_text()) for c in sv.COINS} for k in ('dvol', 'perp', 'funding')}
    for k, d in ser.items():
        for c, rows in d.items():
            assert all(r[0] < sv.CUTOFF_MS for r in rows), f'{k} {c}: a row on/after 2026-07-01'
    return ser


def windows_for(pull, cal):
    a, b = FRIDAY_BOUNDS[cal]
    out, F = {}, a
    while F <= b:
        for c in sv.COINS:
            rows = json.loads((Path(pull) / 'trades' / f'{c}_{friday_key(F)}.json').read_text())
            assert all(F - sv.MARK_WINDOW_H * sv.HOUR <= r['timestamp'] < F for r in rows), 'trade outside its window'
            out[(c, F)] = rows
        F += sv.WEEK
    return out


def write_panel(cal, ser, wins, pull_id):
    d = PANELS[cal]
    if d.exists() and any(d.iterdir()):
        raise SystemExit(f'{d} exists and is not empty: a panel is built once')
    d.mkdir(parents=True, exist_ok=True)
    a, b = SERIES_BOUNDS[cal]
    counts = {}
    for k in ('dvol', 'perp', 'funding'):
        part = {c: [r for r in ser[k][c] if a <= r[0] < b] for c in sv.COINS}
        counts[k] = {c: len(v) for c, v in part.items()}
        (d / f'{k}.json').write_text(json.dumps(part, separators=(',', ':'), sort_keys=True))
    lines = []
    for (c, F) in sorted(wins):
        for r in wins[(c, F)]:
            lines.append(json.dumps({'coin': c, 'friday': F, **r}, separators=(',', ':'), sort_keys=True))
    with open(d / 'trades.jsonl.gz', 'wb') as raw:
        with gzip.GzipFile(fileobj=raw, mode='wb', mtime=0, filename='', compresslevel=6) as g:
            g.write(('\n'.join(lines) + '\n').encode())
    counts['trades'] = len(lines)
    manifest = {'calendar': cal, 'pull_sha256sums_sha256': pull_id, 'series_bounds': [a, b],
                'friday_bounds': list(FRIDAY_BOUNDS[cal]), 'rows': counts,
                'files': {f: sha256(d / f) for f in PANEL_FILES if f != 'MANIFEST.json'}}
    (d / 'MANIFEST.json').write_text(json.dumps(manifest, indent=1, sort_keys=True) + '\n')
    return manifest


def _q(v, ps):
    v = sorted(v)
    return [round(v[min(len(v) - 1, int(p * len(v)))], 1) for p in ps] if v else None


def coverage(cal, ser, wins):
    """Counts only (module docstring)."""
    out = {}
    a, b = sv.CALENDARS[cal]
    sa, sb = SERIES_BOUNDS[cal]
    lo = max(sa, sv.DVOL_START)
    for k in ('dvol', 'perp', 'funding'):
        for c in sv.COINS:
            have = {r[0] for r in ser[k][c] if lo <= r[0] < sb}
            exp = (sb - lo) // sv.HOUR
            out[f'{k}_{c}_hours_missing'] = int(exp - len(have & set(range(lo, sb, sv.HOUR))))
    perp_at = {c: {r[0] + sv.HOUR: r[4] for r in ser['perp'][c]} for c in sv.COINS}   # close at bar end
    entries = sv.calendar_entries(cal)
    out['entries_per_coin'] = len(entries)
    for c in sv.COINS:
        cls, src, ages, settle = {}, {}, [], {'index_twap_ge_10_min': 0, 'perp_fallback': 0, 'missing': 0}
        for E in entries:
            k, s, age = sv.entry_coverage(c, E, wins.get((c, E), []), perp_at[c].get(E))
            cls[k] = cls.get(k, 0) + 1
            src[s] = src.get(s, 0) + 1
            if age is not None:
                ages.append(age)
            X = E + sv.TENOR_D * sv.DAY
            m = sv.settlement_coverage(wins.get((c, X), []), X)
            if m >= sv.SETTLE_MIN_MINUTES:
                settle['index_twap_ge_10_min'] += 1
            elif perp_at[c].get(X):
                settle['perp_fallback'] += 1
            else:
                settle['missing'] += 1
        dailies = 0
        for (cc, F), rows in wins.items():
            if cc == c and any((p := sv.parse_name(r['instrument_name'])) and p[1] == F + sv.DAY for r in rows):
                dailies += 1
        out[c] = {'entry_classes': cls, 'index_source': src, 'chosen_strike_trade_age_min_p50_p90_max': _q(ages, (0.5, 0.9, 0.999)),
                  'settlements': settle, 'windows_with_next_day_expiry_traded': dailies,
                  'windows': sum(1 for (cc, _) in wins if cc == c),
                  'windows_with_zero_trades': sum(1 for (cc, _), r in wins.items() if cc == c and not r),
                  'trades': sum(len(r) for (cc, _), r in wins.items() if cc == c)}
    return out


def main(pull):
    pull, pull_id = verify_pull(pull)
    ser = load_pull(pull)
    report = json.loads((pull / 'pull_report.json').read_text())
    cov = {'pull': pull.name, 'pull_sha256sums_sha256': pull_id,
           'pull_report': report, 'bytes_on_disk': sum(f.stat().st_size for f in pull.rglob('*') if f.is_file()),
           'files': sum(1 for f in pull.rglob('*') if f.is_file())}
    for cal in ('discovery', 'holdout'):
        wins = windows_for(pull, cal)
        cov[f'{cal}_manifest'] = write_panel(cal, ser, wins, pull_id)
        cov[cal] = coverage(cal, ser, wins)
    (DATA_ROOT / 'logs').mkdir(parents=True, exist_ok=True)
    (DATA_ROOT / 'logs' / 'coverage.json').write_text(json.dumps(cov, indent=1, default=str) + '\n')
    print(json.dumps({k: v for k, v in cov.items() if not k.endswith('manifest')}, indent=1, default=str))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
