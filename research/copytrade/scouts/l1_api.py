"""L1 scout — public Hyperliquid info API calls, holdout-safe. Behaviour only.

    .venv/bin/python -I scouts/l1_api.py sample     # draw the stratified sample (seed 11), no network
    .venv/bin/python -I scouts/l1_api.py details    # vaultDetails per sampled vault
    .venv/bin/python -I scouts/l1_api.py ledger     # userNonFundingLedgerUpdates: sampled vaults + their leaders
    .venv/bin/python -I scouts/l1_api.py roles      # userRole of archive addresses not in the list (G)

HOLDOUT HANDLING. Raw responses are never cached or printed. Each response is
projected in memory, at load, to the fields below, and only the projection is
written (data/scout_l1/<cmd>/):
  vaultDetails  -> leaderCommission (static term), allowDeposits and
                   alwaysCloseOnWithdraw (TODAY's values, flagged as such),
                   relationship type, and per portfolio period the TIMESTAMPS of
                   accountValueHistory points < 2026-07-01. No values, no apr,
                   followers, leaderFraction, maxDistributable/maxWithdrawable,
                   isClosed, description.
  ledger        -> endTime = 2026-06-30T23:59:59.999Z on every request; per
                   event: time (< cutoff, re-checked), delta.type, delta.vault
                   (address), and whether delta.user is present. No amounts.
  userRole      -> role string only (today's state; no dated content).
"""
import json
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
from ct.net import WeightBudget, post_json  # noqa: E402
from l1_common import CUTOFF_MS, HLP, OUT, day_of, dt, load_list, ms, quarter  # noqa: E402

INFO = 'https://api.hyperliquid.xyz/info'
SAMPLE_BEFORE = ms(dt.date(2025, 7, 1))
SAMPLE_N = 300
SEED = 11
LEDGER_START = ms(dt.date(2023, 4, 1))
budget = WeightBudget(600)          # half the documented 1200/min


def info(body, weight=20):
    budget.spend(weight)
    res = post_json(INFO, body, timeout=30, retries=4)
    if isinstance(res, list):
        budget.spend(len(res) // 20)
    return res


def sample():
    """Stratified by creation quarter (user vaults, proportional, min 8 per quarter) + all HLP accounts."""
    vs = [v for v in load_list() if v['create_ms'] < SAMPLE_BEFORE]
    hlp = [v for v in vs if v['kind'] != 'user']
    users = [v for v in vs if v['kind'] == 'user']
    strata = {}
    for v in sorted(users, key=lambda v: v['address']):
        strata.setdefault(quarter(day_of(v['create_ms'])), []).append(v)
    rng = random.Random(SEED)
    n_user = SAMPLE_N - len(hlp)
    out = list(hlp)
    for q in sorted(strata):
        k = min(len(strata[q]), max(8, round(n_user * len(strata[q]) / len(users))))
        out += [dict(v, stratum=q) for v in rng.sample(strata[q], k)]
    for v in out:
        v.setdefault('stratum', 'hlp')
    path = OUT / 'sample' / 'sample.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out))
    print(f'population (created < 2025-07-01): {len(vs)}  user {len(users)}  hlp {len(hlp)}  sample {len(out)}')
    for q in sorted(strata):
        print(q, len(strata[q]), sum(1 for v in out if v['stratum'] == q))
    return out


def load_sample():
    return json.loads((OUT / 'sample' / 'sample.json').read_text())


def project_details(d):
    if not isinstance(d, dict):
        return {'ok': False, 'raw_type': type(d).__name__}
    series = {}
    for name, obj in d.get('portfolio') or []:
        series[name] = sorted(t for t, _ in obj.get('accountValueHistory', []) if t < CUTOFF_MS)
    rel = d.get('relationship')
    return {'ok': True, 'leaderCommission': d.get('leaderCommission'),
            'allowDeposits_today': d.get('allowDeposits'),
            'alwaysCloseOnWithdraw_today': d.get('alwaysCloseOnWithdraw'),
            'rel_type': rel.get('type') if isinstance(rel, dict) else None,
            'series_ts': series}


def details():
    dest = OUT / 'details' / 'details.jsonl'
    dest.parent.mkdir(parents=True, exist_ok=True)
    done = {json.loads(l)['address'] for l in dest.read_text().splitlines()} if dest.exists() else set()
    with dest.open('a') as fh:
        for i, v in enumerate(load_sample()):
            if v['address'] in done:
                continue
            try:
                raw = info({'type': 'vaultDetails', 'vaultAddress': v['address']})
                rec = project_details(raw)
            except Exception as e:      # noqa: BLE001 - record the failure, keep going
                rec = {'ok': False, 'error': type(e).__name__}
            raw = None
            fh.write(json.dumps({'address': v['address'], **rec}) + '\n')
            fh.flush()
            if i % 25 == 0:
                print(i, flush=True)


def ledger_of(user, start, end=CUTOFF_MS - 1):
    """Projected events in [start, end], paging on time (cap unknown: page whenever >= 500 rows)."""
    out, seen, t = [], set(), start
    while True:
        page = info({'type': 'userNonFundingLedgerUpdates', 'user': user, 'startTime': t, 'endTime': end})
        fresh = 0
        for x in page:
            if x['time'] >= CUTOFF_MS:          # defensive: endTime should already exclude these
                raise SystemExit('holdout-dated ledger row returned; stop and report')
            dlt = x['delta']
            key = (x['time'], x['hash'], dlt.get('type'), dlt.get('vault'), dlt.get('user'))
            if key in seen:
                continue
            seen.add(key)
            fresh += 1
            out.append({'time': x['time'], 'type': dlt.get('type'),
                        'vault': (dlt.get('vault') or '').lower() or None, 'has_user': 'user' in dlt})
        if len(page) < 500 or not fresh:
            return out, len(page)
        t = page[-1]['time']


def ledger():
    dest = OUT / 'ledger' / 'ledger.jsonl'
    dest.parent.mkdir(parents=True, exist_ok=True)
    done = {(r['who'], r['user']) for r in map(json.loads, dest.read_text().splitlines())} if dest.exists() else set()
    jobs = []
    for v in load_sample():
        if v['kind'] != 'user':
            continue
        jobs.append(('vault', v['address'], v['create_ms'] - 86_400_000))
    for leader in sorted({v['leader'] for v in load_sample() if v['kind'] == 'user'}):
        jobs.append(('leader', leader, LEDGER_START))
    # HLP parent: one 3-day window only, to see whether depositor flows sit on a vault's own ledger
    jobs.append(('hlp_window', HLP, ms(dt.date(2025, 1, 6))))
    with dest.open('a') as fh:
        for i, (who, user, start) in enumerate(jobs):
            if (who, user) in done:
                continue
            end = ms(dt.date(2025, 1, 9)) - 1 if who == 'hlp_window' else CUTOFF_MS - 1
            try:
                ev, last_page = ledger_of(user, start, end)
                rec = {'who': who, 'user': user, 'start': start, 'end': end, 'events': ev, 'last_page': last_page}
            except SystemExit:
                raise
            except Exception as e:      # noqa: BLE001
                rec = {'who': who, 'user': user, 'error': type(e).__name__}
            fh.write(json.dumps(rec) + '\n')
            fh.flush()
            if i % 25 == 0:
                print(i, len(jobs), flush=True)


def roles():
    """userRole for archive addresses NOT in the list: the top 150 by archive notional among
    addresses with snapshot rows, and a seeded random 150 of all archive addresses with fills."""
    src = OUT / 'roles' / 'candidates.json'
    cands = json.loads(src.read_text())
    dest = OUT / 'roles' / 'roles.jsonl'
    done = {json.loads(l)['address'] for l in dest.read_text().splitlines()} if dest.exists() else set()
    with dest.open('a') as fh:
        for i, (group, a) in enumerate(cands):
            if a in done:
                continue
            r = info({'type': 'userRole', 'user': a})
            fh.write(json.dumps({'address': a, 'group': group,
                                 'role': r.get('role') if isinstance(r, dict) else None}) + '\n')
            fh.flush()
            if i % 50 == 0:
                print(i, len(cands), flush=True)


if __name__ == '__main__':
    {'sample': sample, 'details': details, 'ledger': ledger, 'roles': roles}[sys.argv[1]]()
