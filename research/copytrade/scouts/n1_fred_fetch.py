"""N1 scout, item 6: bounded FRED pulls (keyless CSV, server-side end date).

Each run writes a NEW directory data/scout_n1/fred_<UTC stamp>/ with one CSV
per series plus SHA256SUMS. The request carries coed=2026-06-30; any row dated
>= 2026-07-01 that still arrives is dropped before writing (and counted).
Never caches an error: a non-200 or non-CSV answer aborts that series.

    .venv/bin/python -I scouts/n1_fred_fetch.py
"""
import datetime as dt
import hashlib
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERIES = ['SP500', 'NASDAQ100', 'DJIA', 'DCOILWTICO', 'DCOILBRENTEU', 'DEXUSEU', 'DEXJPUS', 'DEXUSUK']
START, END, CUTOFF = '2023-01-01', '2026-06-30', '2026-07-01'


def main():
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out = ROOT / f'data/scout_n1/fred_{stamp}'
    out.mkdir(parents=True, exist_ok=False)
    sums = []
    for s in SERIES:
        url = f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={s}&cosd={START}&coed={END}'
        with urllib.request.urlopen(url, timeout=60) as r:
            if r.status != 200:
                print(s, 'HTTP', r.status, '- skipped, nothing written')
                continue
            body = r.read().decode('utf-8')
        lines = body.strip().splitlines()
        if not lines or not lines[0].lower().startswith('observation_date'):
            print(s, 'unexpected body - skipped, nothing written')
            continue
        keep = [lines[0]] + [ln for ln in lines[1:] if ln[:10] < CUTOFF]
        dropped = len(lines) - len(keep)
        data = '\n'.join(keep) + '\n'
        p = out / f'{s}.csv'
        p.write_text(data)
        sums.append(f'{hashlib.sha256(data.encode()).hexdigest()}  {p.name}')
        print(s, 'rows', len(keep) - 1, 'first', keep[1][:10] if len(keep) > 1 else None,
              'last', keep[-1][:10], 'dropped_>=cutoff', dropped)
        time.sleep(1)
    (out / 'SHA256SUMS').write_text('\n'.join(sums) + '\n')
    print('wrote', out)


if __name__ == '__main__':
    sys.exit(main())
