"""Reviewer B, 015: real files for HASHES, ROW COUNTS, DATES and FORMAT only. No value column is parsed.
Run: python3 -I probe_data_format.py <research/alpha dir>"""
import hashlib, json, re, sys, zipfile, datetime as dt
from pathlib import Path

A = Path(sys.argv[1]).resolve()
reg = (A / 'registrations' / '015-month-end-liquidity-cycle.md').read_text()
table = dict(re.findall(r'^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|', reg, re.M))
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
bad = [f for f, h in table.items() if not (A / f).is_file() or sha(A / f) != h]
print('hash table rows', len(table), 'mismatches', bad)

def zip_dates(p):
    with zipfile.ZipFile(p) as z:
        names = z.namelist()
        lines = z.read([n for n in names if n.lower().endswith('.csv')][0]).decode('latin-1').splitlines()
    heads = [l.split(',', 1)[0].strip() for l in lines]
    first_data = next(i for i, h in enumerate(heads) if len(h) == 8 and h.isdigit())
    ds = [dt.date(int(h[:4]), int(h[4:6]), int(h[6:8])) for h in heads if len(h) == 8 and h.isdigit()]
    # only the daily block: stop at the first non-increasing date (annual blocks use 4-digit keys anyway)
    return names, lines[:first_data], ds

for name in ('F-F_Research_Data_Factors_daily_CSV.zip', 'Europe_3_Factors_Daily_CSV.zip'):
    p = A / 'data/r015/french_20261013T093725Z' / name
    names, hdr, ds = zip_dates(p)
    inc = all(a < b for a, b in zip(ds, ds[1:]))
    print(name, 'members', names, 'header lines', len(hdr), 'header', hdr[-1][:40] if hdr else None)
    print('  rows', len(ds), 'first', ds[0], 'last', ds[-1], 'strictly increasing', inc,
          '>= 2026-06-10:', sum(d >= dt.date(2026, 6, 10) for d in ds),
          'in [2014-01-01, 2026-06-10):', sum(dt.date(2014, 1, 1) <= d < dt.date(2026, 6, 10) for d in ds))
    b1 = A / 'data/scout_b1/french_20261013T090723Z' / name
    print('  identical to B1 copy:', b1.is_file() and sha(b1) == sha(p))
for y in range(2014, 2027):
    p = A / f'data/r015/treasury_copy_20261013T093812Z/daily_treasury_yield_curve_{y}.csv'
    b1 = A / f'data/scout_b1/treasury_20261013T091152Z/daily_treasury_yield_curve_{y}.csv'
    lines = p.read_text().splitlines()
    ds = [dt.datetime.strptime(l.split(',', 1)[0], '%m/%d/%Y').date() for l in lines[1:] if l.strip()]
    print(y, 'rows', len(ds), 'min', min(ds), 'max', max(ds), 'b1 identical', b1.is_file() and sha(b1) == sha(p),
          '>= 2026-06-10:', sum(d >= dt.date(2026, 6, 10) for d in ds), 'cols', len(lines[0].split(',')))
cal = json.loads((A / 'data/r015/calendar.json').read_text())
print('calendar source', cal['source'], cal['source_sha256'][:12], 'dates', len(cal['dates']), cal['dates'][0], cal['dates'][-1])
s = cal['summary']
print('summary', {k: v for k, v in s.items() if k != 'rows'})
rows = {r['month']: r for r in s['rows']}
for m in ('2014-01', '2017-08', '2017-09', '2024-04', '2024-05', '2024-06', '2026-05'):
    print(' ', m, rows[m])
