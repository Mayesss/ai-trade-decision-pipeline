"""P2 — per-account equity table from the daily snapshots.

One row per (snapshot, user): the snapshot's OWN timestamp (from the file
name, not the date partition), account value, number of open positions and
gross / net notional. This is what `equity_at(t)` and typical leverage read in
P4+, joined as-of: the latest snapshot whose timestamp is before t.

Snapshots are taken just after 00:00 UTC at the START of their date (P2 gate,
checked by p2_checks.py), so the file labelled D describes the state at the
start of D.

    .venv/bin/python p2_accounts.py
Writes data/derived/<dex>/snapshot_accounts.parquet.
"""
import re

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ct.net import DATA

DEXES = ('hyperliquid', 'xyz')
NAME = re.compile(r'^(\d+)_(\d{13})\.parquet$')


def summarize(path):
    snap_ms = int(NAME.match(path.name).group(2))
    t = pq.read_table(path, columns=['user', 'account_value', 'size', 'notional'])
    signed = pc.multiply(pc.abs(t.column('notional')), pc.sign(t.column('size')))
    t = t.append_column('signed_notional', signed).append_column('abs_notional', pc.abs(t.column('notional')))
    g = t.group_by('user').aggregate([
        ('account_value', 'max'),
        ('size', 'count'),
        ('abs_notional', 'sum'),
        ('signed_notional', 'sum'),
    ])
    n = g.num_rows
    return pa.table({
        'snapshot_ms': pa.array([snap_ms] * n, pa.int64()),
        'date': pa.array([path.parent.name.split('=')[1]] * n),
        'user': g.column('user'),
        'account_value': g.column('account_value_max'),
        'n_positions': g.column('size_count'),
        'gross_notional': g.column('abs_notional_sum'),
        'net_notional': g.column('signed_notional_sum'),
    })


def main():
    for dex in DEXES:
        root = DATA / f'archive/by_dex/{dex}/snapshots/perp'
        paths = sorted(p for p in root.glob('date=*/*.parquet') if NAME.match(p.name))
        out = DATA / f'derived/{dex}/snapshot_accounts.parquet'
        out.parent.mkdir(parents=True, exist_ok=True)
        writer = None
        rows = 0
        for p in paths:
            table = summarize(p)
            writer = writer or pq.ParquetWriter(out, table.schema)
            writer.write_table(table)
            rows += table.num_rows
        if writer:
            writer.close()
        print(f'{dex}: {len(paths)} snapshots -> {rows:,} account rows -> {out}')


if __name__ == '__main__':
    main()
