"""Selection schedule for the discovery period (plan §5.5) — PROPOSED, frozen at P4.

Selection at 00:00 UTC on each selection date S:
  lookback = [S - LOOKBACK_DAYS, S)   features, ranking, equity as-of S
  hold     = [S, S + HOLD_DAYS)       the follower copies the selected set
The first S is the archive start + lookback; S steps by HOLD_DAYS; a window
whose hold would reach into the holdout is dropped, never truncated.
"""
import datetime as dt

LOOKBACK_DAYS = 91   # ~3 months
HOLD_DAYS = 56       # 8 weeks
HOLDOUT_START = dt.date(2026, 7, 1)
ARCHIVE_START = {'hyperliquid': dt.date(2025, 7, 28), 'xyz': dt.date(2025, 10, 13)}


def windows(dex):
    """[(selection date, lookback start, hold end)] fully inside discovery."""
    out = []
    s = ARCHIVE_START[dex] + dt.timedelta(days=LOOKBACK_DAYS)
    while s + dt.timedelta(days=HOLD_DAYS) <= HOLDOUT_START:
        out.append((s, s - dt.timedelta(days=LOOKBACK_DAYS), s + dt.timedelta(days=HOLD_DAYS)))
        s += dt.timedelta(days=HOLD_DAYS)
    return out


def ms(d):
    return int(dt.datetime.combine(d, dt.time(), dt.UTC).timestamp() * 1000)


if __name__ == '__main__':
    for dex in ARCHIVE_START:
        for s, lb, he in windows(dex):
            print(f'{dex:<12} select {s}  lookback {lb} .. {s - dt.timedelta(days=1)}  hold {s} .. {he - dt.timedelta(days=1)}')
