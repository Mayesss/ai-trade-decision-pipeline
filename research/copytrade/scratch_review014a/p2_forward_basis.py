"""Probe 2 (OUT-OF-CALENDAR, pre-2021 tail tape only; entry windows [E-4h, E) only; no outcome).
Does inverting Deribit's option mark/trade price with Black-76 on the INDEX (as straddle_entry does)
bias the IV? Deribit prices options off the expiry's forward (underlying_price), not the index.
(a) trade price -> IV on the index vs the trade's own `iv` field (Deribit's, on its forward);
(b) at the chosen ATM strike: call-mark IV vs put-mark IV (on the index) within the window."""
import json, statistics, sys
from pathlib import Path
sys.path.insert(0, '.')
from ct import shortvol as sv
D = Path('data/r014/tail_pre2021/options_20261013')
rows = []
for f in sorted(D.glob('*.json')):
    if f.name in ('pull_report.json',):
        continue
    coin, day = f.stem.split('_')
    tr = json.loads(f.read_text())
    y, m, d = map(int, day.split('-'))
    E = sv.ms(y, m, d, 8)
    X = E + 7 * sv.DAY
    win = [r for r in sv.window_rows(tr, E - 4 * sv.HOUR, E)]
    diffs, cp = [], {True: [], False: []}
    S = None
    for r in win:
        p = sv.parse_name(r['instrument_name'])
        if not p or p[1] != X or not r.get('index_price') or r.get('iv') is None:
            continue
        F0 = float(r['index_price']); K = p[2]; T0 = (X - r['timestamp']) / sv.DAY / 365.0
        if abs(K / F0 - 1) > 0.05:
            continue
        iv_idx = sv.implied_vol(float(r['price']) * F0, F0, K, T0, p[3])
        ivm = sv.implied_vol(float(r['mark_price']) * F0, F0, K, T0, p[3])
        if iv_idx is None or ivm is None:
            continue
        diffs.append((p[3], iv_idx * 100 - float(r['iv'])))
        cp[p[3]].append(ivm * 100)
    if not diffs:
        continue
    dc = [d for c, d in diffs if c]; dp = [d for c, d in diffs if not c]
    rows.append((coin, day, len(dc), round(statistics.median(dc), 2) if dc else None, len(dp),
                 round(statistics.median(dp), 2) if dp else None,
                 round(statistics.median(cp[True]), 1) if cp[True] else None,
                 round(statistics.median(cp[False]), 1) if cp[False] else None))
print('coin day | n_calls median(IV_on_index - Deribit trade iv) | n_puts median(...) | median call-mark IV | median put-mark IV  (vol pts, strikes within 5% of index, 7-day expiry, [E-4h,E))')
for r in rows:
    print(*r)
c = [r[3] for r in rows if r[3] is not None]; p = [r[5] for r in rows if r[5] is not None]
print('median over weeks: calls', statistics.median(c), 'puts', statistics.median(p))
