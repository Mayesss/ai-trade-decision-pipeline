"""SYNTHETIC. The registered book's own worst week / month (listings013.book: caps on ENTRY week/month, P&L booked at
EXIT week/month) can exceed the draft's 'structural bounds' (-12 % week, -45 % month)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ct import listings013 as L
H, D = L.HOUR, L.DAY
rows = []
# ISO week 2025-W05 (Mon 2025-01-27): 4 entries Mon..Thu, held 7 d -> they exit Mon..Thu of W06 at -100 % (liquidated
# late in the hold, e.g. on day 6.9). ISO week 2025-W06 (Mon 2025-02-03): 4 entries, liquidated the same week.
w5 = L.ms('2025-01-27'); w6 = L.ms('2025-02-03')
for i in range(4):
    e = w5 + i * D + 12 * H
    rows.append({'id': f'a{i}', 'entry_t': e, 'exit_t': e + 7 * D - H, 'net': -1.0})
for i in range(4):
    e = w6 + i * D + 12 * H
    rows.append({'id': f'b{i}', 'entry_t': e, 'exit_t': e + 2 * D, 'net': -1.0})
b = L.book(rows, 1.0)
print('week case: taken', b['events_taken'], 'worst_week', round(b['worst_week'], 4), 'worst_month', round(b['worst_month'], 4))
# month: 4 entries in the last ISO week of a month that had < 15 entries, liquidated early next month, plus 15
# entries in the next month (<= 4 per ISO week, 4+4+4+3), all liquidated inside that month.
rows = []
last = L.ms('2025-03-24')        # Monday of the last ISO week of March 2025
for i in range(4):
    e = last + i * D + 12 * H
    rows.append({'id': f'm{i}', 'entry_t': e, 'exit_t': L.ms('2025-04-01') + i * H, 'net': -1.0})
k = 0
for wk_start, n in ((L.ms('2025-04-01'), 4), (L.ms('2025-04-07'), 4), (L.ms('2025-04-14'), 4), (L.ms('2025-04-21'), 3)):
    for i in range(n):
        e = wk_start + i * D + 12 * H
        rows.append({'id': f'n{k}', 'entry_t': e, 'exit_t': e + 3 * D, 'net': -1.0}); k += 1
b = L.book(rows, 1.0)
print('month case: taken', b['events_taken'], 'worst_month', round(b['worst_month'], 4))
print('plausible-month arithmetic in the draft: 15 * (0.5 * -1 + 0.5 * -0.25) * 0.03 =', 15 * (0.5 * -1 + 0.5 * -0.25) * 0.03)
print('liquidation threshold (no funding, fee 6bp, slip 5bp): h* =', round((2 - 0.0006 - 0.0005) / 1.05, 4))
for h in (1.9030, 1.9045):
    bars = [(0, 1.0, 1.0, 1.0, 1.0)] + [(L.ENTRY_DELAY + i * L.MIN, 1.0, 1.0, 1.0, 1.0) for i in range(3)] + \
           [(L.ENTRY_DELAY + 3 * L.MIN, 1.0, h, 1.0, 1.0)] + [(L.ENTRY_DELAY + L.HOLD + i * L.MIN, 1.0, 1.0, 1.0, 1.0) for i in range(2)]
    out = L.simulate({'id': 'x', 'first_trade': 0, 'qv1h': 60e6}, bars, [], 10 ** 13)
    print('  high', h, '->', out['status'], round(out['net'], 4))
