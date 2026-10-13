"""Schema probe: header lines and column counts of raw archive files (no data values printed)."""
import csv, io, sys, zipfile, collections
from pathlib import Path
base = Path(sys.argv[1])
for src in ('raw_funding', 'raw_klines1m', 'raw_klines1h'):
    files = sorted((base / src).rglob('*.zip'))
    hdr = collections.Counter()
    ncol = collections.Counter()
    for p in files[:: max(1, len(files) // 300)]:
        with zipfile.ZipFile(p) as z:
            (n,) = z.namelist()
            first = z.read(n).decode().splitlines()[0]
        row = next(csv.reader([first]))
        ncol[len(row)] += 1
        hdr[','.join(row) if not row[0][:1].isdigit() else '<no header>'] += 1
    print(src, len(files), dict(ncol), dict(hdr))
