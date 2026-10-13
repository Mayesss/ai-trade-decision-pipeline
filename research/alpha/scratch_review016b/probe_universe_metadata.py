"""Reviewer B, 016, point-in-time universe: METADATA ONLY (file names, months, counts, contract-type fields and
dates of the pinned exchangeInfo). No zip is opened; no price, volume or funding value is read.
    python3 -I probe_universe_metadata.py <real research/alpha>"""
import collections, datetime as dt, hashlib, json, re, sys
from pathlib import Path
A = Path(sys.argv[1]).resolve()
sums_p = A / 'data/scout_c1/bn_um_klines_1d_monthly/SHA256SUMS'
ex_p = A / 'data/r016/static/fapi_exchangeInfo_20261011.json'
print('SHA256SUMS sha256', hashlib.sha256(sums_p.read_bytes()).hexdigest()[:16], '| exchangeInfo sha256',
      hashlib.sha256(ex_p.read_bytes()).hexdigest()[:16])
NAME = re.compile(r'^klines/([A-Z0-9]+)/1d/\1-1d-(\d{4}-\d{2})\.zip$')
months = collections.defaultdict(set); bad = 0
for line in sums_p.read_text().splitlines():
    m = NAME.match(line.split('  ', 1)[1]) if '  ' in line else None
    if not m:
        bad += 1; continue
    months[m.group(1)].add(m.group(2))
print('lines', sum(len(v) for v in months.values()), 'symbols', len(months), 'unparsed lines', bad,
      '| last month overall', max(max(v) for v in months.values()))
def span(a, b):
    y, m = map(int, a.split('-')); out = []
    while f'{y:04d}-{m:02d}' <= b:
        out.append(f'{y:04d}-{m:02d}'); y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out
gaps = {s: [x for x in span(min(v), max(v)) if x not in v] for s, v in months.items()}
gaps = {s: g for s, g in gaps.items() if g}
print('symbols with interior missing months in the pinned pull:', len(gaps), dict(list(sorted(gaps.items()))[:8]))
ended = sorted(s for s, v in months.items() if max(v) < '2026-06')
print('symbols whose files end before 2026-06 (dead/delisted kept in the pull):', len(ended))
for s in ('LUNAUSDT', 'LUNA2USDT', 'FTTUSDT', 'SRMUSDT', 'MATICUSDT', 'POLUSDT', 'FTMUSDT', 'SUSDT', 'RNDRUSDT',
          'RENDERUSDT', 'EOSUSDT', 'MKRUSDT', 'SKYUSDT', 'BTCDOMUSDT', 'DEFIUSDT'):
    v = months.get(s)
    print(f'   {s:12s}', (min(v), max(v), len(v)) if v else 'absent')
ex = json.loads(ex_p.read_bytes())
syms = ex.get('symbols', [])
print('exchangeInfo serverTime', dt.datetime.fromtimestamp(ex.get('serverTime', 0) / 1000, dt.UTC).isoformat()[:19],
      '| symbols', len(syms))
print('  (contractType, underlyingType) counts:', collections.Counter((x.get('contractType'), x.get('underlyingType'))
                                                                     for x in syms).most_common())
print('  underlyingSubType values:', collections.Counter(t for x in syms for t in (x.get('underlyingSubType') or [])).most_common(12))
print('  status counts:', collections.Counter(x.get('status') for x in syms).most_common())
exn = {x['symbol']: x for x in syms}
non_coin = {x['symbol'] for x in syms if x.get('contractType') != 'PERPETUAL' or x.get('underlyingType') != 'COIN'}
usdt_nc = sorted(s for s in non_coin if re.fullmatch(r'[A-Z0-9]+USDT', s))
print('  USDT-named symbols marked not (PERPETUAL, COIN):', len(usdt_nc))
early = []
for s in usdt_nc:
    x = exn[s]; v = months.get(s)
    od = dt.datetime.fromtimestamp(x.get('onboardDate', 0) / 1000, dt.UTC).date() if x.get('onboardDate') else None
    if v and min(v) <= '2025-11':
        early.append((s, x.get('contractType'), x.get('underlyingType'), x.get('underlyingSubType'), str(od), min(v), max(v)))
print('  ...of which have klines months before 2025-12 (excluded for ALL of their history by the 2026-10 snapshot):',
      len(early))
for e in early:
    print('     ', e)
absent = sorted(s for s in months if s not in exn)
print('pull symbols absent from the 2026-10-11 snapshot (not excluded; delisted before the snapshot):', len(absent),
      '| e.g.', absent[:6])
# migrations: perp age resets on a new contract. Pairs whose successor started on/after 2024-03 (dates only).
for old, new in (('MATICUSDT', 'POLUSDT'), ('FTMUSDT', 'SUSDT'), ('RNDRUSDT', 'RENDERUSDT'), ('MKRUSDT', 'SKYUSDT'),
                 ('EOSUSDT', 'AUSDT')):
    print(f'   migration {old} {(min(months[old]), max(months[old])) if old in months else "absent"} -> {new} '
          f'{(min(months[new]), max(months[new])) if new in months else "absent"}')
young = sorted((min(v), s) for s, v in months.items() if '2024-04' <= min(v) <= '2025-06' and max(v) == '2026-06')
print('symbols first archived 2024-04 .. 2025-06 and alive at 2026-06 (perp-age rule binds inside span (a)):', len(young))
