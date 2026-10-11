"""L1 vault scout — shared, holdout-safe loaders. Behaviour only.

HOLDOUT RULES enforced here, at load, before anything can be printed or saved:
  * vault list: only vaultAddress, leader, name, createTimeMillis, relationship
    are kept; apr / pnls / tvl / isClosed are never read. Vaults created on or
    after 2026-07-01 are dropped (their creation is a holdout-dated fact).
  * snapshots: every query carries snapshot_ms < CUTOFF_MS.
  * archive: only data/pruned dates < 2026-07-01 exist; days are listed and
    filtered explicitly anyway.
"""
import datetime as dt
import glob
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent                       # research/copytrade
DATA = ROOT / 'data'
OUT = DATA / 'scout_l1'
CUTOFF = dt.date(2026, 7, 1)
CUTOFF_MS = 1782864000000                # 2026-07-01T00:00:00Z
assert CUTOFF_MS == int(dt.datetime(2026, 7, 1, tzinfo=dt.UTC).timestamp() * 1000)
HLP = '0xdfc24b077bc1425ad1dea75bcb6f8158e10df303'
SNAPS = DATA / 'derived/hyperliquid/snapshot_accounts.parquet'
FILLS_DIR = DATA / 'pruned/by_dex/hyperliquid/fills/perp/all'
SELECTIONS = [dt.date(2025, 10, 27), dt.date(2025, 12, 22), dt.date(2026, 2, 16), dt.date(2026, 4, 13)]
ALLOWED = ('vaultAddress', 'leader', 'name', 'createTimeMillis', 'relationship')


def ms(d):
    return int(dt.datetime.combine(d, dt.time(), dt.UTC).timestamp() * 1000)


def day_of(ms_):
    return dt.datetime.fromtimestamp(ms_ / 1000, dt.UTC).date()


def load_list():
    """[{address, leader, name, create_ms, kind}] for vaults created before the cutoff.

    kind: 'hlp_parent' | 'hlp_child' | 'user'. Only ALLOWED summary fields are touched.
    """
    path = glob.glob(str(DATA / 'cache/hl-vaults/*.json'))
    assert len(path) == 1, path
    raw = json.loads(Path(path[0]).read_text())
    out = []
    for v in raw:
        s = v['summary']
        rec = {k: s[k] for k in ALLOWED}
        if rec['createTimeMillis'] >= CUTOFF_MS:
            continue
        rel = rec['relationship'].get('type')
        a = rec['vaultAddress'].lower()
        kind = 'hlp_parent' if a == HLP else 'hlp_child' if rel == 'child' else 'user'
        out.append({'address': a, 'leader': rec['leader'].lower(), 'name': rec['name'],
                    'create_ms': rec['createTimeMillis'], 'kind': kind, 'rel': rel})
    del raw
    return out


def fill_days():
    days = sorted(p.name.split('=')[1] for p in FILLS_DIR.iterdir() if p.name.startswith('date='))
    return [d for d in days if d < CUTOFF.isoformat()]


def quarter(d):
    return f'{d.year}Q{(d.month - 1) // 3 + 1}'
