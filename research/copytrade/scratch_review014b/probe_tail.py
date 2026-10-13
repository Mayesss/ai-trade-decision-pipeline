"""Out-of-calendar only (all data < 2021-03-24): bounds of every tail file, the SHA256SUMS, selection
re-derived from the underlying alone, REALISTIC_WEEK vs tail_results.json, the tail charge."""
import json, sys, hashlib, datetime as dt
from pathlib import Path
HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from ct import shortvol as sv
import r014_tail as T
ROOT = HERE / 'data/r014/tail_pre2021'
und, opt = ROOT / 'underlying_20261013', ROOT / 'options_20261013'
for d in (und, opt):
    sums = d / 'SHA256SUMS'
    bad = [n for h, n in (l.split('  ', 1) for l in sums.read_text().splitlines()) if hashlib.sha256((d / n).read_bytes()).hexdigest() != h]
    listed = {l.split('  ', 1)[1] for l in sums.read_text().splitlines()}
    on_disk = {str(f.relative_to(d)) for f in d.rglob('*') if f.is_file() and f.name != 'SHA256SUMS'}
    print(d.name, 'files', len(listed), 'mismatch', bad, 'unlisted', sorted(on_disk ^ listed), 'SHA256SUMS sha', hashlib.sha256(sums.read_bytes()).hexdigest())
mx = 0; per = []
for f in sorted(und.glob('*_[BE]*.json')):
    rows = json.loads(f.read_text()); m = max(r[0] for r in rows); mx = max(mx, m)
    per.append((f.name, sv.iso(min(r[0] for r in rows)), sv.iso(m), len(rows)))
print('underlying:', per)
win_bad = []
for f in sorted(opt.glob('[BE]*_*.json')):
    rows = json.loads(f.read_text())
    E = sv.ms(*map(int, f.stem.split('_')[1].split('-')), 8)
    ts = [r['timestamp'] for r in rows]
    mx = max(mx, max(ts))
    if not all(E - 24 * sv.HOUR <= t < E + 7 * sv.DAY for t in ts):
        win_bad.append(f.name)
    if not rows: win_bad.append(f.name + ' EMPTY')
print('options files', len(list(opt.glob('[BE]*_*.json'))), 'outside [E-24h, E+7d):', win_bad)
print('latest stamp in ANY tail file:', sv.iso(mx), '< 2021-03-24:', mx < T.TAIL_CUTOFF)
# selection re-derived (underlying only; nothing written)
perp, _ = T.load_underlying(und)
mine = [E for E in T.entries() if any((x := T.excursion(perp[c], E)) and x['excursion'] >= T.EXCURSION_MIN for c in sv.COINS) or E in T.NAMED]
sel = json.loads((ROOT / 'selected.json').read_text())
print('entries', len(T.entries()), '| re-derived selection == selected.json:', [w['entry'] for w in sel['weeks']] == mine, len(mine))
print('selected:', [sv.iso(E)[:10] for E in mine])
none_ex = [sv.iso(E)[:10] for E in T.entries() if T.excursion(perp['ETH'], E) is None]
print('weeks with ETH excursion undefined (< 160 candles):', len(none_ex), none_ex[:3], '...', none_ex[-2:])
named_only = [w['iso'] for w in sel['weeks'] if w['reason'] == 'named']
print('selected only because named:', named_only)
res = json.loads((ROOT / 'tail_results.json').read_text())
wk = res['weeks']
diff = {k[:10]: (round(w['book_0.1'], 4), sv.REALISTIC_WEEK.get(k[:10])) for k, w in wk.items() if w['selected'] and round(w['book_0.1'], 4) != sv.REALISTIC_WEEK.get(k[:10])}
print('REALISTIC_WEEK vs tail_results book_0.1 (selected weeks), mismatches:', diff)
print('same at n 0.25/0.45 (no liquidation => n-free):', all(abs(w['book_0.1'] - w['book_0.45']) < 1e-12 for w in wk.values() if w['book_0.1'] is not None))
print('weeks in tail_results:', len(wk), 'selected', sum(w['selected'] for w in wk.values()))
print('sum REALISTIC_WEEK', round(sum(sv.REALISTIC_WEEK.values()), 4), 'tail_charge', round(sv.tail_charge() * 100, 4), '%/wk')
cons = {}
for k, w in wk.items():
    for c, v in w['coins'].items():
        cons.setdefault(v.get('construction', v.get('missing', ''))[:8], []).append(k[:10] + c)
        if 'settle_source' in v and v['settle_source'] != 'index_twap': cons.setdefault('settle!=twap', []).append(k[:10] + c)
print({k: len(v) for k, v in cons.items()}, 'settle!=twap', cons.get('settle!=twap'))
print('missing coins:', [(k[:10], c, v['missing']) for k, w in wk.items() for c, v in w['coins'].items() if 'missing' in v])
