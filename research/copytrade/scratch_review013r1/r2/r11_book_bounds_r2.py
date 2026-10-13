"""Review r1 of 013: the tail bounds against L.book() with an adversarial schedule (synthetic rows only).
Normal exits lie in [entry + 7 d - 1 h, entry + 7 d + 6 h); liquidations in (entry, exit). `simulate` also allows
`exit_late` (no traded bar in [t_x, t_x + 6 h): exit at the first later traded bar < end), and `end` for an event
is d0 + 10 d (PULL_DAYS), so exit_late - entry can reach ~9 d. Dense hourly candidate entries; each exit placed
to cluster in one target week / month."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ct import listings013 as L
H, D = L.HOUR, L.DAY

def run(target0, target1, start, stop, late_ok):
    rows, i = [], 0
    for t in range(start, stop, H):
        if late_ok and t + 8 * D - L.MIN >= target0 and t < target0:
            x = t + 8 * D - L.MIN            # exit_late at the end of the pulled window
        elif t >= target0:
            x = t + L.MIN                    # liquidated at once
        else:
            x = t + 7 * D + 5 * H            # normal exit, latest
        rows.append({'id': str(i), 'entry_t': t, 'exit_t': x, 'net': -1.0})
        i += 1
    b = L.book(rows, 1.0)
    inside = sum(1 for r in L.take(rows) if target0 <= r['exit_t'] < target1)
    return b, inside

W = L.ms('2025-03-03')                      # a Monday
for late in (False, True):
    b, n = run(W, W + 7 * D, W - 10 * D, W + 7 * D, late)
    print(f'week  exit_late allowed={late}: exits in week {n}, worst_week {b["worst_week"]:.3f} (bound {L.STRUCTURAL_WEEK})')
M0, M1 = L.ms('2025-03-01'), L.ms('2025-04-01')
for late in (False, True):
    b, n = run(M0, M1, M0 - 10 * D, M1, late)
    print(f'month exit_late allowed={late}: exits in month {n}, worst_month {b["worst_month"]:.3f} (bound {L.STRUCTURAL_MONTH})')

print('-- entries start exactly 9 d before the target (exit_late reach) --')
for late in (False, True):
    off = 8 * D if late else int(7 * D + 5 * H)
    b, n = run(W, W + 7 * D, W - off, W + 7 * D, late)
    print(f'week  exit_late allowed={late}: exits in week {n}, worst_week {b["worst_week"]:.3f} (bound {L.STRUCTURAL_WEEK:.3f})')
    b, n = run(M0, M1, M0 - off, M1, late)
    print(f'month exit_late allowed={late}: exits in month {n}, worst_month {b["worst_month"]:.3f} (bound {L.STRUCTURAL_MONTH:.3f})')
