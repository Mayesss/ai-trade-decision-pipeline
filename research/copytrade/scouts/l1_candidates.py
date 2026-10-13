"""L1 scout (G): pick archive addresses NOT in the vault list for a userRole check.

Groups: 'top' = the 150 largest archive addresses by notional traded (discovery
dates only) that are not listed vaults; 'random' = 150 seeded (11) random
archive addresses not in the list; 'control' = 10 seeded listed vaults
created before 2025-07-01 (positive control: should return role 'vault').

    .venv/bin/python -I scouts/l1_candidates.py
Writes data/scout_l1/roles/candidates.json and data/scout_l1/archive/all_addresses.parquet.
"""
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import duckdb  # noqa: E402

from l1_common import CUTOFF_MS, DATA, FILLS_DIR, OUT, fill_days, load_list  # noqa: E402


def main():
    lst = load_list()
    listed = {v['address'] for v in lst}
    allp = OUT / 'archive' / 'all_addresses.parquet'
    con = duckdb.connect()
    con.execute(f"SET memory_limit='4GB'; SET threads=6; SET temp_directory='{DATA / 'duckdb_tmp'}'")
    if not allp.exists():
        files = [str(FILLS_DIR / f'date={d}' / 'fills.parquet') for d in fill_days()]
        con.execute(f"""COPY (SELECT address, COUNT(*) n_fills, SUM(CAST(size AS DOUBLE) * CAST(price AS DOUBLE)) notional
                         FROM read_parquet({files!r}) WHERE epoch_ms(timestamp) < {CUTOFF_MS} GROUP BY address)
                        TO '{allp}' (FORMAT parquet)""")
    rows = con.execute(f"SELECT address, notional FROM read_parquet('{allp}') ORDER BY notional DESC").fetchall()
    rest = [a for a, _ in rows if a not in listed]
    top = rest[:150]
    rng = random.Random(11)
    rnd = rng.sample(sorted(set(rest) - set(top)), 150)
    ctrl = rng.sample(sorted(v['address'] for v in lst if v['kind'] == 'user' and v['create_ms'] < 1751328000000), 10)
    cands = [('top', a) for a in top] + [('random', a) for a in rnd] + [('control', a) for a in ctrl]
    dest = OUT / 'roles' / 'candidates.json'
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(cands))
    print(f'archive addresses {len(rows):,}; listed vaults among them {len(rows) - len(rest):,}; candidates {len(cands)}')


if __name__ == '__main__':
    main()
