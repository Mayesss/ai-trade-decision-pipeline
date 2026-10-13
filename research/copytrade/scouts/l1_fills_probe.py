"""L1 scout (D, data availability): does userFillsByTime still serve PRE-ARCHIVE fills of vaults?

For 24 seeded (11) sampled user vaults created before 2024-07-01, asks for fills in two
pre-archive 91-day windows. Keeps ONLY the number of fills returned, the first/last fill
time and whether a page hit the 2000 cap. closedPnl and every other field are discarded
at load. endTime is always < 2025-07-28 (inside discovery, far from the holdout).

    .venv/bin/python -I scouts/l1_fills_probe.py
Writes data/scout_l1/fills_probe/probe.jsonl
"""
import json
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
from ct.net import WeightBudget, post_json  # noqa: E402
from l1_common import OUT, dt, ms  # noqa: E402

budget = WeightBudget(300)
WINDOWS = [(dt.date(2024, 6, 1), dt.date(2024, 8, 31)), (dt.date(2025, 3, 1), dt.date(2025, 5, 31))]


def control():
    """Positive control: 12 seeded vaults with archive fills in 2025-07-28..08-10, created before 2025-03-01.
    Window A = 2025-07-28..08-10 (archive: compare API count with archive count); window B = 2025-05-01..07-27."""
    import duckdb
    from l1_common import load_list
    lst = {v['address']: v for v in load_list() if v['kind'] == 'user' and v['create_ms'] < ms(dt.date(2025, 3, 1))}
    con = duckdb.connect()
    rows = con.execute(f"""SELECT address, SUM(n_fills) FROM read_parquet('{OUT / 'archive' / 'daily.parquet'}')
                           WHERE day < DATE '2025-08-11' GROUP BY 1 HAVING SUM(n_fills) BETWEEN 20 AND 1500""").fetchall()
    act = sorted((a, n) for a, n in rows if a in lst)
    pick = random.Random(11).sample(act, 12)
    dest = OUT / 'fills_probe' / 'control.jsonl'
    with dest.open('w') as fh:
        for a, n_arch in pick:
            rec = {'address': a, 'archive_n_A': n_arch}
            for lab, (x, y) in (('A', (dt.date(2025, 7, 28), dt.date(2025, 8, 11))), ('B', (dt.date(2025, 5, 1), dt.date(2025, 7, 28)))):
                budget.spend(20)
                page = post_json('https://api.hyperliquid.xyz/info', {'type': 'userFillsByTime', 'user': a,
                                 'startTime': ms(x), 'endTime': ms(y) - 1}, timeout=30, retries=4)
                times = [f['time'] for f in page]
                page = None
                budget.spend(len(times) // 20)
                assert all(t < ms(y) for t in times)
                rec[f'api_n_{lab}'] = len(times)
                rec[f'api_first_{lab}'] = min(times) if times else None
            fh.write(json.dumps(rec) + '\n')
            print(rec['archive_n_A'], rec['api_n_A'], rec['api_n_B'])


def main():
    smp = [s for s in json.loads((OUT / 'sample' / 'sample.json').read_text())
           if s['kind'] == 'user' and s['create_ms'] < ms(dt.date(2024, 6, 1))]
    pick = random.Random(11).sample(sorted(smp, key=lambda s: s['address']), 24)
    dest = OUT / 'fills_probe' / 'probe.jsonl'
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open('w') as fh:
        for s in pick:
            for a, b in WINDOWS:
                budget.spend(20)
                page = post_json('https://api.hyperliquid.xyz/info',
                                 {'type': 'userFillsByTime', 'user': s['address'],
                                  'startTime': ms(a), 'endTime': ms(b) - 1}, timeout=30, retries=4)
                times = [f['time'] for f in page]
                page = None
                budget.spend(len(times) // 20)
                assert all(t < ms(b) for t in times)
                fh.write(json.dumps({'address': s['address'], 'window': a.isoformat(), 'n': len(times),
                                     'first': min(times) if times else None, 'last': max(times) if times else None,
                                     'capped': len(times) >= 2000}) + '\n')
    print('done', len(pick))


if __name__ == '__main__':
    control() if sys.argv[1:] == ['control'] else main()
