"""P2 gates on the downloaded snapshots — run before anything uses them.

1. Timing: when is each day's snapshot actually taken? The file name carries
   `<n>_<epoch ms>`; if the snapshot for date D is taken at the END of D, then
   selection on day D may only use the file for D-1 (plan §5.5).
2. Coverage: does a snapshot list every account, or only accounts holding a
   position? If only holders, the universe cannot come from snapshots alone —
   a trader who is flat at snapshot time would vanish from it.
3. Consistency: is account_value one value per user per snapshot?

    .venv/bin/python p2_checks.py
"""
import datetime as dt
import re
from collections import Counter
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

from ct.net import DATA

ROOTS = {'hyperliquid': DATA / 'archive/by_dex/hyperliquid/snapshots/perp',
         'xyz': DATA / 'archive/by_dex/xyz/snapshots/perp'}
NAME = re.compile(r'^(\d+)_(\d{13})\.parquet$')


def files(root):
    for day_dir in sorted(root.glob('date=*')):
        day = day_dir.name.split('=')[1]
        for f in sorted(day_dir.glob('*.parquet')):
            m = NAME.match(f.name)
            yield day, f, (int(m.group(1)), int(m.group(2))) if m else (None, None)


def main():
    for dex, root in ROOTS.items():
        rows = list(files(root))
        print(f'\n== {dex}: {len(rows)} files')
        per_day = Counter(d for d, _, _ in rows)
        print('  days with >1 file:', [d for d, n in per_day.items() if n > 1])
        offsets, unnamed, prev_n = [], 0, None
        non_monotonic = 0
        for day, f, (n, ms) in rows:
            if ms is None:
                unnamed += 1
                continue
            day0 = dt.datetime.fromisoformat(day).replace(tzinfo=dt.UTC)
            taken = dt.datetime.fromtimestamp(ms / 1000, dt.UTC)
            offsets.append((taken - day0).total_seconds() / 3600)
            if prev_n is not None and n <= prev_n:
                non_monotonic += 1
            prev_n = n
        offsets.sort()
        q = lambda p: offsets[int(p * (len(offsets) - 1))]
        print(f'  snapshot time, hours after the partition date 00:00 UTC: '
              f'min {offsets[0]:.2f}  p10 {q(.1):.2f}  median {q(.5):.2f}  p90 {q(.9):.2f}  max {offsets[-1]:.2f}')
        print(f'  files without <n>_<ms> name: {unnamed}; first number not increasing: {non_monotonic}')

        # Coverage and consistency on three sample days spread across the period.
        for day, f, _ in [rows[len(rows) // 10], rows[len(rows) // 2], rows[-1]]:
            t = pq.read_table(f)
            users = t.column('user')
            n_users = len(pc.unique(users))
            zero = pc.sum(pc.equal(t.column('size'), 0)).as_py() or 0
            per_user = t.group_by('user').aggregate([('account_value', 'min'), ('account_value', 'max')])
            spread = pc.subtract(per_user.column('account_value_max'), per_user.column('account_value_min'))
            inconsistent = pc.sum(pc.greater(pc.abs(spread), 1e-6)).as_py() or 0
            print(f'  {day}: rows {t.num_rows:,}, users {n_users:,}, rows with size 0: {zero:,}, '
                  f'users with >1 account_value: {inconsistent:,}')
        print('  columns:', t.schema.names)


if __name__ == '__main__':
    main()
