"""Coverage probe: do 1h archive rows of a contract delisted in 2022 carry trades? Prints row counts, the
number of rows with trade count 0, and the NUMBER of distinct open values (never a value). Not a listing window."""
import csv, io, sys, zipfile
from pathlib import Path
base = Path(sys.argv[1])
for sym, month in (('FTTUSDT', '2024-01'), ('SCUSDT', '2024-01'), ('FTTUSDT', '2026-06')):
    p = base / sym / '1h' / f'{sym}-1h-{month}.zip'
    if not p.exists():
        print(sym, month, 'no file'); continue
    with zipfile.ZipFile(p) as z:
        (n,) = z.namelist()
        rows = [r for r in csv.reader(io.StringIO(z.read(n).decode())) if r]
    if not rows[0][0][:1].isdigit():
        rows = rows[1:]
    print(sym, month, 'rows', len(rows), 'rows with 0 trades', sum(1 for r in rows if int(float(r[8])) == 0),
          'distinct opens', len({r[1] for r in rows}))
