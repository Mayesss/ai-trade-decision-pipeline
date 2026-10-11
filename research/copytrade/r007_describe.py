"""Registration 007 (L1) — behaviour-only description. No score, no NAV change, no return.

    .venv/bin/python -I r007_describe.py features     # lookback features of every listed user vault at the
                                                      # 7 dates of the 28-day grid (the 4 primary dates are
                                                      # a subset); equality check against 001's features_<S>
    .venv/bin/python -I r007_describe.py candidates   # vaults passing every filter except the ledger one
    .venv/bin/python -I r007_describe.py activity     # per (vault, day) fill counts / first / last fill TIME,
                                                      # fills < 2026-06-08 (pinned; closure + activity rule)
    .venv/bin/python -I r007_describe.py netexp       # lookback median snapshot net leverage (pre-S)
    .venv/bin/python -I r007_describe.py describe     # Revision 1: eligible counts (full / outside-depositor),
                                                      # entry coverage, leaders, capacity, exposure — pre-S
                                                      # reads plus grid / snapshot PRESENCE in [S, S + 10 d]
    .venv/bin/python -I r007_describe.py levels       # the allTime-vs-snapshot LEVEL cross-check

Reads: the vault list (allowed fields only), archive fills < 2026-07-01 (never realized_pnl),
snapshots with snapshot_ms < 1782864000000 in every query, the pinned ledgers (event TIMES and
TYPES only — amounts are never read here) and the pinned allTime grids (TIMESTAMPS; account-value
LEVELS only in the cross-check, where two sources' levels at the same moment are compared, never
two times of one source). Writes data/vaults/describe/*.json (gitignored).
"""
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))   # run with -I: the script dir is not on the path
import duckdb  # noqa: E402

from ct import vaults as V  # noqa: E402
from ct.net import DATA  # noqa: E402

OUT = V.VAULT_DIR / 'describe'
FEAT = DATA / 'derived/hyperliquid/features_{}.parquet'


def con():
    c = duckdb.connect()
    c.execute(f"SET memory_limit='3GB'; SET threads=4; SET temp_directory='{DATA / 'duckdb_tmp'}'; "
              "SET preserve_insertion_order=false")
    return c


def user_vaults():
    return [v for v in V.load_list() if v['kind'] == 'user']


def q(xs, p):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    k = (len(xs) - 1) * p / 100
    lo = int(k)
    return xs[lo] + (xs[min(lo + 1, len(xs) - 1)] - xs[lo]) * (k - lo)


def features():
    OUT.mkdir(parents=True, exist_ok=True)
    c = con()
    addrs = sorted(v['address'] for v in user_vaults())
    for sel in V.SELECTIONS_28:
        dest = OUT / f'features_{sel}.json'
        if dest.exists():
            continue
        lb = sel - V.dt.timedelta(days=V.LOOKBACK_DAYS)
        rows = V.features_sql(c, V.day_files(lb, sel), str(V.SNAPS), V.ms(sel), V.ms(lb), addrs)
        dest.write_text(json.dumps(rows, default=str))
        print(sel, len(rows), 'vaults with lookback fills', flush=True)
    # equality against 001's feature files (vault rows) at the four primary dates
    cols = ('n_fills', 'active_days', 'maker_share', 'round_trips', 'median_hold_h', 'account_value',
            'equity_age_h', 'fills_per_active_day', 'median_leverage')
    for sel in V.SELECTIONS:
        mine = {r['address']: r for r in json.loads((OUT / f'features_{sel}.json').read_text())}
        cur = c.execute(f"SELECT * FROM read_parquet('{str(FEAT).format(sel)}') WHERE address IN ({','.join(repr(a) for a in mine)})")
        names = [d[0] for d in cur.description]
        theirs = {r[0]: dict(zip(names, r)) for r in cur.fetchall()}
        bad = Counter()
        for a, r in mine.items():
            t = theirs.get(a)
            if t is None:
                bad['missing in 001 file'] += 1
                continue
            for k in cols:
                x, y = r.get(k), t.get(k)
                if (x is None) != (y is None) or (x is not None and abs(float(x) - float(y)) > 1e-6 * max(1, abs(float(y)))):
                    bad[k] += 1
        print(f'{sel}: {len(mine)} vault rows; 001 file has {len(theirs)}; mismatches {dict(bad) or "none"}')


def load_features(sel):
    return {r['address']: r for r in json.loads((OUT / f'features_{sel}.json').read_text())}


def candidates():
    """Vaults passing every 007 filter except the activity and outside-depositor rules (they need
    the activity table and the ledger) — the superset whose ledgers and grids are pinned."""
    out, reasons = {}, {}
    for sel in V.SELECTIONS_28:
        f = load_features(sel)
        ok = []
        why = Counter()
        for a, r in f.items():
            e, reason = V.eligible(r, V.ms(sel), last_fill_ms=V.ms(sel) - 1)   # activity / outside: later
            why[reason] += 1
            if e:
                ok.append(a)
        out[sel.isoformat()] = sorted(ok)
        reasons[sel.isoformat()] = dict(why)
        # the orchestrator's recount read equity_age_h <= 168 without the sign (age is <= 0)
        unsigned = sum(1 for r in f.values() if (r['round_trips'] or 0) >= 20 and (r['account_value'] or 0) >= 10_000
                       and r['equity_age_h'] is not None and r['equity_age_h'] <= 168)
        signed = sum(1 for r in f.values() if (r['round_trips'] or 0) >= 20 and (r['account_value'] or 0) >= 10_000
                     and r['equity_age_h'] is not None and -r['equity_age_h'] <= 168)
        print(f'{sel}: lookback rows {len(f)}; >=20 trips & eq>=$10k: any lookback snapshot {unsigned}, '
              f'<=7 d {signed}; all filters but the ledger {len(ok)}; first failing filter {dict(why)}')
    union = sorted(set().union(*out.values()))
    prim = sorted(set().union(*(out[s.isoformat()] for s in V.SELECTIONS)))
    (OUT / 'candidates.json').write_text(json.dumps({'by_selection': out, 'union': union, 'union_primary': prim,
                                                    'first_failing_filter': reasons}, indent=1))
    print(f'union over 7 dates {len(union)}; over the 4 primary dates {len(prim)}')


def activity():
    """Per (candidate vault, UTC day): fill count and first / last fill time, fills strictly before
    the discovery read bound (2026-06-08). Fill TIMES only. Pinned for the runner (the 7-day
    activity rule and the behavioural closure); this script reports only the pre-S part."""
    cand = json.loads((OUT / 'candidates.json').read_text())['union']
    c = con()
    rows = V.activity_sql(c, V.day_files(V.ARCHIVE_START, V.READ_END), cand, V.READ_END_MS)
    (OUT / 'activity.json').write_text(json.dumps(rows))
    print('activity rows', len(rows), 'vaults', len({r[0] for r in rows}))


def netexp():
    """Lookback median snapshot net leverage per candidate at each date (pre-S, behaviour)."""
    cand = json.loads((OUT / 'candidates.json').read_text())['by_selection']
    c = con()
    for sel in V.SELECTIONS_28:
        lb = sel - V.dt.timedelta(days=V.LOOKBACK_DAYS)
        ne = V.net_exposure_sql(c, str(V.SNAPS), V.ms(sel), V.ms(lb), cand[sel.isoformat()])
        (OUT / f'net_exposure_{sel}.json').write_text(json.dumps(ne))
    print('net exposure written')


def load_activity():
    out = {}
    for a, day, n, first, last in json.loads((OUT / 'activity.json').read_text()):
        out.setdefault(a, []).append((day, n, first, last))
    return out


def load_events(addr):
    path = V.VAULT_DIR / 'ledger' / f'{addr}.json'
    return V.bounded_events(json.loads(path.read_text())['events'], V.READ_END_MS)


def load_grid_times(addr):
    d = json.loads((V.VAULT_DIR / 'details' / f'{addr}.json').read_text())
    return [int(t) for t, _ in (d.get('allTime') or {}).get('accountValueHistory', []) if int(t) < V.READ_END_MS]


def eligible_sets(selections=None):
    """{S: {'full': [...], 'outside': [...]}} under the Revision-1 rule (pre-S reads only)."""
    sels = selections or V.SELECTIONS_28
    cand = json.loads((OUT / 'candidates.json').read_text())['by_selection']
    act = load_activity()
    leaders = {v['address']: v['leader'] for v in V.load_list()}
    out = {}
    for sel in sels:
        s_ms = V.ms(sel)
        f = load_features(sel)
        full, outside, why = [], [], Counter()
        for a in cand[sel.isoformat()]:
            ev = [e for e in load_events(a) if e['time'] < s_ms]          # pre-S only
            lf = V.last_fill_before(act.get(a, []), s_ms)
            has_out = V.outside_depositor(ev, leaders[a], s_ms)
            ok, reason = V.eligible(f[a], s_ms, lf, has_out)
            why[reason] += 1
            if ok:
                full.append(a)
                if has_out:
                    outside.append(a)
        out[sel] = {'full': full, 'outside': outside, 'why': dict(why)}
    return out


def describe(selections=None):
    sels = selections or V.SELECTIONS_28
    sets = eligible_sets(sels)
    leaders = {v['address']: v['leader'] for v in V.load_list()}
    report = {}
    c = con()
    for sel in sels:
        s_ms = V.ms(sel)
        end = s_ms + V.HOLD_DAYS * V.DAY
        f = load_features(sel)
        ne = json.loads((OUT / f'net_exposure_{sel}.json').read_text())
        rec = {}
        for name in ('full', 'outside'):
            el = sets[sel][name]
            no_grid_entry, pts_in = [], []
            for a in el:
                ts = load_grid_times(a)
                if not any(s_ms <= t <= s_ms + V.ENTRY_DAYS * V.DAY for t in ts):
                    no_grid_entry.append(a)
                pts_in.append(sum(1 for t in ts if s_ms <= t <= end))
            snap_entry = 0
            if no_grid_entry:
                c.execute('CREATE OR REPLACE TEMP TABLE ne_(address VARCHAR)')
                c.executemany('INSERT INTO ne_ VALUES (?)', [(a,) for a in no_grid_entry])
                snap_entry = c.execute(f"""SELECT COUNT(DISTINCT "user") FROM read_parquet('{V.SNAPS}')
                    WHERE snapshot_ms < {V.READ_END_MS} AND snapshot_ms >= {s_ms}
                      AND snapshot_ms <= {s_ms + V.ENTRY_DAYS * V.DAY} AND "user" IN (SELECT address FROM ne_)""").fetchone()[0]
            lead = Counter(leaders[a] for a in el)
            rec[name] = {
                'eligible': len(el),
                'equity_ge': {k: sum(1 for a in el if (f[a]['account_value'] or 0) >= k) for k in (100_000, 500_000, 2_500_000)},
                'no_grid_point_in_entry_window': len(no_grid_entry), 'of_which_snapshot_entry': snap_entry,
                'unmeasurable_no_entry': len(no_grid_entry) - snap_entry,
                'grid_points_in_hold_min': min(pts_in, default=None),
                'leaders': len(lead), 'vaults_sharing_a_leader': sum(n for n in lead.values() if n > 1),
                'max_vaults_per_leader': max(lead.values(), default=0),
                'net_exposure': {'p10': q([ne.get(a) for a in el], 10), 'median': q([ne.get(a) for a in el], 50),
                                 'p90': q([ne.get(a) for a in el], 90), 'missing': sum(1 for a in el if ne.get(a) is None)},
                'gross_leverage_median': q([f[a]['median_leverage'] for a in el], 50),
                'median_hold_h': q([f[a]['median_hold_h'] for a in el], 50),
            }
        report[sel.isoformat()] = {**rec, 'first_failing_filter': sets[sel]['why'],
                                   'eligible_list': sets[sel]['full'], 'outside_list': sets[sel]['outside']}
        print(sel, json.dumps({k: v for k, v in report[sel.isoformat()].items() if not k.endswith('_list')}), flush=True)
    (OUT / 'describe_r1.json').write_text(json.dumps(report, indent=1, default=str))
    overlap(report)


def overlap(report):
    prim = [s.isoformat() for s in V.SELECTIONS]
    for key in ('eligible_list', 'outside_list'):
        sets = {s: set(report[s][key]) for s in prim}
        allv = set().union(*sets.values())
        pres = Counter(a for s in prim for a in sets[s])
        print(f'{key}: primary windows: distinct vaults {len(allv)}; present in k windows:',
              dict(sorted(Counter(pres.values()).items())))


def level_check(max_dt_h=(1, 3, 6)):
    """allTime account-value LEVEL vs snapshot account-value LEVEL for the same vault at nearly the
    same moment (two sources, one quantity). Prints only aggregate agreement — pair counts and
    quantiles of |a - b| / max(a, b) — never a level, a change or a sign."""
    c = con()
    cand = json.loads((OUT / 'candidates.json').read_text())['union']
    pts = []
    for a in cand:
        p = V.VAULT_DIR / 'details' / f'{a}.json'
        if not p.exists():
            continue
        for t, v in (json.loads(p.read_text()).get('allTime') or {}).get('accountValueHistory', []):
            if int(t) < V.CUTOFF_MS:
                pts.append((a, int(t), float(v)))
    c.execute('CREATE OR REPLACE TEMP TABLE api(address VARCHAR, t BIGINT, av DOUBLE)')
    c.executemany('INSERT INTO api VALUES (?, ?, ?)', pts)
    for h in max_dt_h:
        rows = c.execute(f"""
            SELECT abs(api.av - s.account_value) / greatest(abs(api.av), abs(s.account_value), 1e-9)
            FROM api JOIN read_parquet('{V.SNAPS}') s ON s."user" = api.address
            WHERE s.snapshot_ms < {V.CUTOFF_MS} AND abs(s.snapshot_ms - api.t) <= {h * V.HOUR}
              AND api.av >= 10000""").fetchall()
        rel = [r[0] for r in rows]
        print(f'level check |dt| <= {h} h: pairs {len(rel)}, vaults {c.execute(f"""SELECT COUNT(DISTINCT api.address) FROM api JOIN read_parquet('{V.SNAPS}') s ON s."user" = api.address WHERE s.snapshot_ms < {V.CUTOFF_MS} AND abs(s.snapshot_ms - api.t) <= {h * V.HOUR} AND api.av >= 10000""").fetchone()[0]}; '
              f'|diff|/level median {q(rel, 50)}, p75 {q(rel, 75)}, p90 {q(rel, 90)}; within 1% {sum(1 for x in rel if x <= 0.01) / max(1, len(rel)):.3f}, '
              f'within 5% {sum(1 for x in rel if x <= 0.05) / max(1, len(rel)):.3f}')
    tod = Counter((t % V.DAY) // V.HOUR for _, t, _ in pts)
    print('allTime point hour-of-day (UTC) counts:', dict(sorted(tod.items())))
    stod = c.execute(f"""SELECT (snapshot_ms % {V.DAY}) // {V.HOUR} AS h, COUNT(DISTINCT snapshot_ms)
                         FROM read_parquet('{V.SNAPS}') WHERE snapshot_ms < {V.CUTOFF_MS} GROUP BY 1 ORDER BY 1""").fetchall()
    print('snapshot hour-of-day (UTC) counts:', dict(stod))


if __name__ == '__main__':
    {'features': features, 'candidates': candidates, 'activity': activity, 'netexp': netexp, 'describe': describe,
     'levels': level_check}[sys.argv[1]]()
