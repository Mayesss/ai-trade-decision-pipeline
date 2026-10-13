"""Timestamps and hashes only (no price, IV or mark is read into any computation): every pull file's stamp
range against 2026-07-01; trade files against their [F-4h, F) window; SHA256SUMS; panel files vs MANIFEST
and vs the registration's §9 table (no panel row is parsed)."""
import json, hashlib, re, sys
from pathlib import Path
HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from ct import shortvol as sv
P = HERE / 'data/r014/pull_20261012'
sh = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
listed = dict((n, h) for h, n in (l.split('  ', 1) for l in (P / 'SHA256SUMS').read_text().splitlines()))
on_disk = {str(f.relative_to(P)) for f in P.rglob('*') if f.is_file() and f.name != 'SHA256SUMS'}
print('pull: listed', len(listed), 'on disk', len(on_disk), 'diff', sorted(on_disk ^ set(listed))[:3],
      'mismatch', [n for n, h in listed.items() if sh(P / n) != h][:3], '| SHA256SUMS sha', sh(P / 'SHA256SUMS'))
mx, late = 0, 0
for k in ('dvol', 'perp', 'funding'):
    for c in sv.COINS:
        ts = [r[0] for r in json.loads((P / k / f'{c}.json').read_text())]
        mx = max(mx, max(ts)); late += sum(t >= sv.CUTOFF_MS for t in ts)
        print(k, c, sv.iso(min(ts)), '->', sv.iso(max(ts)), len(ts))
bad, empty, ntr = [], [], 0
for f in sorted((P / 'trades').glob('*.json')):
    F = sv.ms(*map(int, f.stem.split('_')[1].split('-')), 8)
    ts = [r['timestamp'] for r in json.loads(f.read_text())]
    ntr += len(ts)
    if not ts: empty.append(f.name)
    if any(not (F - 4 * sv.HOUR <= t < F) for t in ts): bad.append(f.name)
    if ts: mx = max(mx, max(ts)); late += sum(t >= sv.CUTOFF_MS for t in ts)
print('trade files', len(list((P / 'trades').glob('*.json'))), 'trades', ntr, 'outside window', bad, 'empty', empty)
print('latest stamp in the pull', sv.iso(mx), '| rows >= 2026-07-01:', late)
reg = (HERE / 'registrations/014-short-vol-deribit.md').read_text()
table = dict(re.findall(r'^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|', reg, re.M))
for p in ('panel_discovery', 'panel_holdout'):
    d = HERE / 'data/r014' / p
    man = json.loads((d / 'MANIFEST.json').read_text())
    ok = all(sh(d / f) == h for f, h in man['files'].items())
    ok9 = all(sh(d / f) == table.get(f'data/r014/{p}/{f}') for f in ('dvol.json', 'perp.json', 'funding.json', 'trades.jsonl.gz', 'MANIFEST.json'))
    print(p, 'files == MANIFEST:', ok, '| == §9:', ok9, '| bounds', [sv.iso(x) for x in man['series_bounds']], [sv.iso(x) for x in man['friday_bounds']],
          '| pull id matches:', man['pull_sha256sums_sha256'] == sh(P / 'SHA256SUMS'), '| extra files:', sorted(x.name for x in d.iterdir() if x.name not in man['files'] and x.name != 'MANIFEST.json'))
