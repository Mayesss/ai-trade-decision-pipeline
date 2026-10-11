"""Registration 007 (L1) — fetch and pin the data the runner needs. Values are NEVER printed.

    .venv/bin/python -I r007_fetch.py probe      # key names and counts of one ledger page and one
                                                 # vaultDetails response (no value printed)
    .venv/bin/python -I r007_fetch.py ledger     # userNonFundingLedgerUpdates, every candidate vault
    .venv/bin/python -I r007_fetch.py details    # vaultDetails -> truncated allTime only
    .venv/bin/python -I r007_fetch.py check      # structure by counts and timestamps only
    .venv/bin/python -I r007_fetch.py manifest   # SHA-256 of every pinned file -> data/vaults/manifest_r1.json
                                                 # (refuses to overwrite an existing manifest)

Candidates: data/vaults/describe/candidates.json (`union`: every listed user vault passing all 007
filters except the ledger one at any of the 7 dates of the 28-day grid; r007_describe.py).

HOLDOUT HANDLING (programme §3.2, 000b, 000c):
  ledger   endTime = 2026-06-30T23:59:59.999Z on every request; every returned row is re-checked
           to be < 2026-07-01 (the script stops and reports otherwise). Rows are stored as served
           (time, hash, delta): the amounts are the runner's NAV input and are never printed,
           summarised or inspected here.
  details  vaultDetails has no end parameter. Each response is projected IN MEMORY by
           ct.vaults.project_details to the allTime accountValueHistory / pnlHistory points with
           t < 1782864000000 before anything is saved; apr, followers, leaderFraction,
           maxDistributable, maxWithdrawable, isClosed, allowDeposits, description, vlm and every
           other portfolio period are dropped unread. Raw responses are never cached or printed.
Public api.hyperliquid.xyz/info only, half the documented rate limit. Downloads land in
data/vaults/ (gitignored); this script lives outside it.
"""
import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))   # run with -I
from ct import vaults as V  # noqa: E402
from ct.net import WeightBudget, post_json  # noqa: E402

INFO = 'https://api.hyperliquid.xyz/info'
PAGE_CAP = 2000
budget = WeightBudget(600)          # half the documented 1200 / minute
LEDGER_DIR = V.VAULT_DIR / 'ledger'
DETAILS_DIR = V.VAULT_DIR / 'details'
CAND = V.VAULT_DIR / 'describe' / 'candidates.json'


def info(body, weight=20):
    budget.spend(weight)
    res = post_json(INFO, body, timeout=30, retries=6)
    if isinstance(res, list):
        budget.spend(len(res) // 20)
    return res


def candidates():
    return json.loads(CAND.read_text())['union']


def created():
    return {v['address']: v['create_ms'] for v in V.load_list()}


def ledger_of(vault, start):
    """Every ledger row of `vault` in [start, LEDGER_END_MS], paging on time (page cap 2,000) with
    de-duplication on the served row. Returns (events, pages, stuck)."""
    out, seen, t, pages = [], set(), start, 0
    while True:
        page = info({'type': 'userNonFundingLedgerUpdates', 'user': vault, 'startTime': t, 'endTime': V.LEDGER_END_MS})
        pages += 1
        rows = V.project_ledger(page, vault)          # stops on any holdout-dated row
        fresh = 0
        for r in rows:
            key = json.dumps(r, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
            fresh += 1
        if len(page) < PAGE_CAP:
            return sorted(out, key=lambda r: r['time']), pages, False
        if not fresh:
            return sorted(out, key=lambda r: r['time']), pages, True
        t = max(r['time'] for r in rows)


def probe():
    """Key names and counts only, on the first candidate."""
    a = candidates()[0]
    start = created()[a] - V.DAY
    page = info({'type': 'userNonFundingLedgerUpdates', 'user': a, 'startTime': start, 'endTime': V.LEDGER_END_MS})
    V.project_ledger(page, a)
    shapes = {}
    for x in page:
        d = x['delta']
        shapes.setdefault(d.get('type'), set()).update(d.keys())
    print('ledger rows', len(page), 'row keys', sorted(page[0].keys()) if page else None)
    for typ, keys in sorted(shapes.items()):
        print(' ', typ, sorted(keys))
    raw = info({'type': 'vaultDetails', 'vaultAddress': a})
    print('vaultDetails top-level keys', sorted(raw.keys()) if isinstance(raw, dict) else type(raw).__name__)
    for name, obj in raw.get('portfolio') or []:
        n_av = sum(1 for t, _ in obj.get('accountValueHistory', []) if t < V.CUTOFF_MS)
        n_pn = sum(1 for t, _ in obj.get('pnlHistory', []) if t < V.CUTOFF_MS)
        print('  portfolio', name, sorted(obj.keys()), 'points < cutoff: av', n_av, 'pnl', n_pn)
    proj = V.project_details(raw)
    raw = None
    at = proj['allTime']
    same = [t for t, _ in at['accountValueHistory']] == [t for t, _ in at['pnlHistory']]
    print('projection keys', sorted(proj), sorted(at), 'av/pnl timestamps identical:', same)


def ledger():
    LEDGER_DIR.mkdir(parents=True, exist_ok=True)
    cre = created()
    todo = [a for a in candidates() if not (LEDGER_DIR / f'{a}.json').exists()]
    for i, a in enumerate(todo):
        start = cre[a] - V.DAY
        ev, pages, stuck = ledger_of(a, start)
        rec = {'vault': a, 'start': start, 'end': V.LEDGER_END_MS, 'pages': pages, 'stuck': stuck,
               'fetched_utc': dt.datetime.now(dt.UTC).isoformat(timespec='seconds'), 'events': ev}
        tmp = LEDGER_DIR / f'{a}.json.tmp'
        tmp.write_text(json.dumps(rec, sort_keys=True))
        tmp.replace(LEDGER_DIR / f'{a}.json')
        if i % 20 == 0:
            print(f'ledger {i + 1}/{len(todo)}', flush=True)
    print('ledger done')


def details():
    DETAILS_DIR.mkdir(parents=True, exist_ok=True)
    todo = [a for a in candidates() if not (DETAILS_DIR / f'{a}.json').exists()]
    for i, a in enumerate(todo):
        raw = info({'type': 'vaultDetails', 'vaultAddress': a})
        proj = V.project_details(raw)
        raw = None
        rec = {'vault': a, 'fetched_utc': dt.datetime.now(dt.UTC).isoformat(timespec='seconds'),
               'truncated_before_ms': V.CUTOFF_MS, **proj}
        tmp = DETAILS_DIR / f'{a}.json.tmp'
        tmp.write_text(json.dumps(rec, sort_keys=True))
        tmp.replace(DETAILS_DIR / f'{a}.json')
        if i % 20 == 0:
            print(f'details {i + 1}/{len(todo)}', flush=True)
    print('details done')


def check():
    """Structure by counts and timestamps only."""
    cand = candidates()
    led = det = stuck = bad_t = no_create = 0
    types = {}
    for a in cand:
        p = LEDGER_DIR / f'{a}.json'
        if p.exists():
            led += 1
            r = json.loads(p.read_text())
            stuck += r['stuck']
            bad_t += sum(1 for e in r['events'] if not (r['start'] <= e['time'] < V.CUTOFF_MS))
            ts = {e['delta'].get('type') for e in r['events']}
            no_create += 'vaultCreate' not in ts
            for e in r['events']:
                types[e['delta'].get('type')] = types.get(e['delta'].get('type'), 0) + 1
        q = DETAILS_DIR / f'{a}.json'
        if q.exists():
            d = json.loads(q.read_text())
            det += d.get('ok', False)
            at = d.get('allTime') or {}
            bad_t += sum(1 for t, _ in at.get('accountValueHistory', []) + at.get('pnlHistory', []) if t >= V.CUTOFF_MS)
    print(f'candidates {len(cand)}; ledgers {led} (stuck pages {stuck}, without vaultCreate {no_create}); '
          f'details ok {det}; rows outside [start, cutoff) {bad_t}')
    print('ledger event types (counts):', dict(sorted(types.items(), key=lambda kv: -kv[1])))


MANIFEST = V.VAULT_DIR / 'manifest_r1.json'   # Revision 1; the draft's manifest.json is superseded, not overwritten


def pinned_files():
    """Every file the runner reads: ledgers, truncated allTime grids, the 7 feature files, the
    candidate list, the activity table, the net-exposure files, the H7 HLP cache, the vault list,
    the snapshot table, BTC 1H closes, and the archive day files 2025-07-28 .. 2026-06-07 that the
    scores and the hold diagnostics read (review minor 2)."""
    d = V.VAULT_DIR / 'describe'
    files = (list(LEDGER_DIR.glob('*.json')) + list(DETAILS_DIR.glob('*.json')) + list(d.glob('features_*.json'))
             + list(d.glob('net_exposure_*.json')) + [CAND, d / 'activity.json'] + list(V.HLP_CACHE.glob('*.json'))
             + [Path(p) for p in V.glob.glob(str(V.DATA / 'cache/hl-vaults/*.json'))] + [V.SNAPS, V.BTC_1H]
             + [Path(p) for p in V.day_files(V.ARCHIVE_START, V.READ_END)])
    return sorted(set(files))


def manifest():
    """Written ONCE (ct/vaults.write_manifest refuses an existing file)."""
    files = pinned_files()
    digest = V.write_manifest(MANIFEST, files, V.VAULT_DIR.parent.parent,
                              extra={'written_utc': dt.datetime.now(dt.UTC).isoformat(timespec='seconds'),
                                     'read_end': V.READ_END.isoformat()})
    print(f'manifest: {len(files)} files, digest {digest}')


if __name__ == '__main__':
    {'probe': probe, 'ledger': ledger, 'details': details, 'check': check, 'manifest': manifest}[sys.argv[1]]()
