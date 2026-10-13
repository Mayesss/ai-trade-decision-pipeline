"""Coverage only: first-trade times and pre-market flags (already disclosed in 013 §2.4). No price, funding or outcome."""
import json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))
from ct import listings013 as L
out = {}
for cal in ('discovery', 'confirmation'):
    ev = json.loads((HERE / 'data/r013' / f'panel_{cal}' / 'events.json').read_text())
    ft = sorted(e['first_trade'] for e in ev if e['first_trade'] is not None and not e['premarket'])
    out[cal] = ft
    weeks = {}
    for t in ft:
        weeks[L.iso_week(t)] = weeks.get(L.iso_week(t), 0) + 1
    ws = sorted(weeks)
    # consecutive ISO-week pairs among listing weeks (where cross-week hold overlap can occur)
    import datetime as dt
    def wk_monday(w):
        y, n = w.split('-W'); return dt.date.fromisocalendar(int(y), int(n), 1)
    consec = sum(1 for a, b in zip(ws, ws[1:]) if (wk_monday(b) - wk_monday(a)).days == 7)
    # book cap count (entry = first trade + 24 h), structure only
    rows = [{'id': str(i), 'entry_t': t + L.ENTRY_DELAY, 'exit_t': t + L.ENTRY_DELAY + L.HOLD, 'net': 0.0} for i, t in enumerate(ft)]
    b = L.book(rows, (L.CALENDARS[cal][1] - L.CALENDARS[cal][0]) / L.YEAR)
    yrs = (L.CALENDARS[cal][1] - L.CALENDARS[cal][0]) / L.YEAR
    print(cal, 'events', len(ft), 'weeks', len(ws), 'consecutive listing-week pairs', consec,
          'book taken', b['events_taken'], 'per yr', round(b['events_taken'] / yrs, 1), 'span_yr', round(yrs, 3))
(Path(__file__).parent / 'structure.json').write_text(json.dumps(out))
