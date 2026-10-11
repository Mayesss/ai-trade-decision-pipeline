"""L1 scout — vault allocation on Hyperliquid: is there an opportunity set, and what data exists.
Behaviour only. Prints markdown tables for questions A–H of the scout brief.

No outcome is computed: never reads realized_pnl, apr, pnls, tvl, isClosed or any
portfolio VALUE. Equity LEVELS at a snapshot before S are read (size filter only).
Every snapshot query has snapshot_ms < 2026-07-01; the archive has only earlier days.

Prerequisites (each writes under data/scout_l1/, gitignored):
    .venv/bin/python -I scouts/l1_archive.py         # per (vault|leader, day) activity
    .venv/bin/python -I scouts/l1_candidates.py      # G: archive addresses for userRole
    .venv/bin/python -I scouts/l1_api.py sample|details|ledger|roles
Then:
    .venv/bin/python -I scouts/l1_vaults_scout.py [A B C D E F G H]
"""
import bisect
import json
import statistics as st
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import duckdb  # noqa: E402

from l1_common import (CUTOFF, CUTOFF_MS, DATA, OUT, SELECTIONS, SNAPS, day_of, dt,  # noqa: E402
                       load_list, ms, quarter)

ARCHIVE_START = dt.date(2025, 7, 28)
DAILY = OUT / 'archive' / 'daily.parquet'
FEAT = DATA / 'derived/hyperliquid/features_{}.parquet'
POP = DATA / 'derived/hyperliquid/population_{}.json'
LB = 91
HOLD = 56

con = duckdb.connect()
con.execute(f"SET memory_limit='3GB'; SET threads=6; SET temp_directory='{DATA / 'duckdb_tmp'}'")
VAULTS = load_list()
BY_ADDR = {v['address']: v for v in VAULTS}
con.execute('CREATE TABLE v(address VARCHAR, leader VARCHAR, kind VARCHAR, create_ms BIGINT)')
con.executemany('INSERT INTO v VALUES (?,?,?,?)', [(v['address'], v['leader'], v['kind'], v['create_ms']) for v in VAULTS])
# Snapshot rows of listed vaults, discovery only. account_value is a LEVEL (size), never differenced.
con.execute(f"""CREATE TABLE vs AS SELECT "user" AS address, snapshot_ms, account_value
                FROM read_parquet('{SNAPS}') WHERE snapshot_ms < {CUTOFF_MS} AND "user" IN (SELECT address FROM v)""")
con.execute(f"CREATE TABLE snap_times AS SELECT DISTINCT snapshot_ms FROM read_parquet('{SNAPS}') WHERE snapshot_ms < {CUTOFF_MS}")
con.execute(f"CREATE TABLE d AS SELECT * FROM read_parquet('{DAILY}') WHERE day < DATE '{CUTOFF}'")


def h(title):
    print(f'\n### {title}\n')


def table(header, rows):
    print('| ' + ' | '.join(header) + ' |')
    print('|' + '---|' * len(header))
    for r in rows:
        print('| ' + ' | '.join(fmt(x) for x in r) + ' |')


def fmt(x):
    if isinstance(x, float):
        return f'{x:,.2f}' if abs(x) < 100 else f'{x:,.0f}'
    if isinstance(x, int):
        return f'{x:,}'
    return '—' if x is None else str(x)


def pct(a, b):
    return f'{100 * a / b:.1f}%' if b else '—'


def q(xs, p):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    return st.quantiles(xs, n=100, method='inclusive')[p - 1] if len(xs) > 1 else xs[0]


def jl(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


SAMPLE = json.loads((OUT / 'sample' / 'sample.json').read_text()) if (OUT / 'sample' / 'sample.json').exists() else []
DETAILS = {r['address']: r for r in jl(OUT / 'details' / 'details.jsonl')}
LEDGER = jl(OUT / 'ledger' / 'ledger.jsonl')


def stratum_weights():
    """Population count per sampling stratum (user vaults created < 2025-07-01, by quarter)."""
    pop = Counter(quarter(day_of(v['create_ms'])) for v in VAULTS
                  if v['kind'] == 'user' and v['create_ms'] < ms(dt.date(2025, 7, 1)))
    smp = Counter(s['stratum'] for s in SAMPLE if s['kind'] == 'user')
    return {k: pop[k] / smp[k] for k in smp}


def weighted_share(flags):
    """flags: {address: bool} over sampled user vaults -> (population-weighted share, n)."""
    w = stratum_weights()
    strat = {s['address']: s['stratum'] for s in SAMPLE}
    num = sum(w[strat[a]] for a, f in flags.items() if f)
    den = sum(w[strat[a]] for a in flags)
    return (num / den if den else None), len(flags)


# ---------------------------------------------------------------- A
def ledger_closure():
    """{vault: first vaultDistribution time} from each sampled vault's OWN ledger (all < cutoff)."""
    out = {}
    for r in LEDGER:
        if r['who'] != 'vault' or 'events' not in r:
            continue
        ts = [e['time'] for e in r['events'] if e['type'] == 'vaultDistribution']
        out[r['user']] = min(ts) if ts else None
    return out


def A():
    h('A1. Vaults created per quarter (list createTimeMillis; vaults created on/after 2026-07-01 dropped at load)')
    cnt = defaultdict(Counter)
    for v in VAULTS:
        cnt[quarter(day_of(v['create_ms']))][v['kind']] += 1
    cum = 0
    rows = []
    for k in sorted(cnt):
        n = sum(cnt[k].values())
        cum += n
        rows.append((k, cnt[k]['user'], cnt[k]['hlp_parent'] + cnt[k]['hlp_child'], cum))
    table(['quarter created', 'user vaults', 'HLP parent/children', 'cumulative'], rows)

    h('A2. Discovery-period closure proxies from the archive (user vaults created before the archive start, 2025-07-28)')
    act = dict(con.execute("""SELECT address, MAX(day) FROM d WHERE address IN (SELECT address FROM v) GROUP BY 1""").fetchall())
    snap_last = dict(con.execute('SELECT address, MAX(snapshot_ms) FROM vs GROUP BY 1').fetchall())
    pre = [v for v in VAULTS if v['kind'] == 'user' and v['create_ms'] < ms(ARCHIVE_START)]
    rows = []
    for k in sorted({quarter(day_of(v['create_ms'])) for v in pre}):
        grp = [v for v in pre if quarter(day_of(v['create_ms'])) == k]
        fills = [v for v in grp if v['address'] in act]
        snap = [v for v in grp if v['address'] in snap_last]
        silent = [v for v in fills if act[v['address']] < dt.date(2026, 4, 1)]
        rows.append((k, len(grp), len(fills), pct(len(fills), len(grp)), len(snap), len(silent)))
    table(['created', 'vaults', 'any archive fill', 'share', 'any snapshot row', 'last fill before 2026-04-01'], rows)
    print('\n"last fill before 2026-04-01": traded in the archive year, then no fill in the last 91 days '
          'before the cutoff (closed or idle; the archive cannot tell which).')

    h('A3. Last archive activity of every listed vault that traded in the archive year (quarter of last fill)')
    lastq = Counter(quarter(dd) for a, dd in act.items() if BY_ADDR[a]['kind'] == 'user')
    table(['quarter of last fill', 'user vaults'], sorted(lastq.items()))

    if LEDGER:
        h('A4. Ledger event types (userNonFundingLedgerUpdates, endTime 2026-06-30T23:59:59.999Z; counts only)')
        for who in ('vault', 'leader', 'hlp_window'):
            rs = [r for r in LEDGER if r['who'] == who and 'events' in r]
            types = Counter(e['type'] for r in rs for e in r['events'])
            per = Counter(t for r in rs for t in {e['type'] for e in r['events']})
            print(f'\n**{who}** ledgers: {len(rs)} accounts, {sum(types.values()):,} events; '
                  f'errors {sum(1 for r in LEDGER if r["who"] == who and "error" in r)}; '
                  f'max page size {max((r["last_page"] for r in rs), default=0)}\n')
            table(['event type', 'events', 'accounts with >=1'], [(t, n, per[t]) for t, n in types.most_common()])
        own = [r for r in LEDGER if r['who'] == 'vault' and 'events' in r]
        dep = sum(1 for r in own for e in r['events'] if e['type'] in ('vaultDeposit', 'vaultWithdraw'))
        print(f'\nDepositor flow events on the sampled vaults\' OWN ledgers: {dep}.')
        clo = ledger_closure()
        pre_s = {a: t for a, t in clo.items() if BY_ADDR[a]['create_ms'] < ms(ARCHIVE_START)}
        h('A5. Closure proxy from the ledger: first vaultDistribution on the vault\'s own ledger (sampled user vaults)')
        rows = []
        for label, lo, hi in (('before 2025-07-28 (pre-archive)', 0, ms(ARCHIVE_START)),
                              ('2025-07-28 .. 2026-06-30 (archive year)', ms(ARCHIVE_START), CUTOFF_MS),
                              ('none before 2026-07-01', None, None)):
            if lo is None:
                flags = {a: t is None for a, t in clo.items()}
            else:
                flags = {a: t is not None and lo <= t < hi for a, t in clo.items()}
            share, n = weighted_share(flags)
            rows.append((label, sum(flags.values()), n, f'{100 * share:.1f}%' if share is not None else '—'))
        table(['first vaultDistribution', 'sampled vaults', 'of', 'population-weighted share'], rows)
        # agreement with the history: last allTime timestamp vs distribution time
        gaps = []
        for a, t in clo.items():
            ts = (DETAILS.get(a) or {}).get('series_ts', {}).get('allTime') or []
            if t is not None and ts:
                gaps.append((ts[-1] - t) / 86_400_000)
        if gaps:
            print(f'\nFor {len(gaps)} sampled vaults with a vaultDistribution: last pre-cutoff allTime point minus '
                  f'first distribution, days: median {q(gaps, 50):.1f}, p10 {q(gaps, 10):.1f}, p90 {q(gaps, 90):.1f}.')
        alive_pre = sum(1 for a in pre_s if pre_s[a] is None or pre_s[a] >= ms(ARCHIVE_START))
        print(f'Sampled user vaults created before the archive start: {len(pre_s)}; '
              f'without a distribution before the archive start: {alive_pre}.')


# ---------------------------------------------------------------- B, C, F
def _features(sel):
    cur = con.execute(f"SELECT * FROM read_parquet('{str(FEAT).format(sel)}') WHERE address IN (SELECT address FROM v)")
    cols = [c[0] for c in cur.description]
    return {r[0]: dict(zip(cols, r)) for r in cur.fetchall()}


def last_snap_equity(sel):
    """{vault: account_value} at the latest snapshot strictly before S (the snapshot, not the lookback)."""
    s = con.execute(f'SELECT MAX(snapshot_ms) FROM snap_times WHERE snapshot_ms < {ms(sel)}').fetchone()[0]
    return dict(con.execute(f'SELECT address, account_value FROM vs WHERE snapshot_ms = {s}').fetchall()), s


COMMON = {'account_value_min': 10_000, 'equity_age_h_max': 7 * 24, 'maker_share_max': 0.5,
          'fills_per_day_max': 500, 'mapped_share_min': 0.8}
ELIGIBLE = {**COMMON, 'round_trips_min': 20, 'median_hold_h_min': 2.0}
SLOW = {**COMMON, 'round_trips_min': 10, 'median_hold_h_min': 24.0}
DAY = {**COMMON, 'round_trips_min': 20, 'median_hold_h_min': 1.0, 'median_hold_h_max': 24.0}
SCALPER = {**COMMON, 'round_trips_min': 20, 'median_hold_h_min': 0.2, 'median_hold_h_max': 1.0, 'fills_per_day_max': 2000}


def passes(r, t):   # copy of p5_population.passes (001's filters), so 001's code stays untouched
    return ((r['round_trips'] or 0) >= t['round_trips_min']
            and (r['account_value'] or 0) >= t['account_value_min']
            and r['equity_age_h'] is not None and -r['equity_age_h'] <= t['equity_age_h_max']
            and r['maker_share'] is not None and r['maker_share'] <= t['maker_share_max']
            and r['median_hold_h'] is not None and r['median_hold_h'] >= t['median_hold_h_min']
            and r['median_hold_h'] < t.get('median_hold_h_max', float('inf'))
            and r['fills_per_active_day'] <= t['fills_per_day_max']
            and (r['mapped_share'] or 0) >= t['mapped_share_min'])


def selectable(sel, floor=10_000):
    """Listed user vaults with >= 20 completed trips in [S-91, S) and equity >= floor at a snapshot <= 7 d before S."""
    f = _features(sel)
    return {a: r for a, r in f.items() if BY_ADDR[a]['kind'] == 'user' and (r['round_trips'] or 0) >= 20
            and (r['account_value'] or 0) >= floor and r['equity_age_h'] is not None and -r['equity_age_h'] <= 168}


def B():
    h('B1. Selectable vaults at 001\'s selection dates (archive features, lookback [S-91 d, S))')
    rows = []
    for sel in SELECTIONS:
        f = _features(sel)
        user = {a: r for a, r in f.items() if BY_ADDR[a]['kind'] == 'user'}
        exist = sum(1 for v in VAULTS if v['kind'] == 'user' and v['create_ms'] < ms(sel))
        t20 = [r for r in user.values() if (r['round_trips'] or 0) >= 20]
        fresh = [r for r in t20 if r['equity_age_h'] is not None and -r['equity_age_h'] <= 168]
        n = {k: sum(1 for r in fresh if (r['account_value'] or 0) >= k) for k in (10_000, 100_000, 1_000_000)}
        any10 = sum(1 for r in t20 if (r['account_value'] or 0) >= 10_000)
        t1 = sum(1 for r in user.values() if passes(r, ELIGIBLE) or passes(r, DAY) or passes(r, SCALPER))
        slow = sum(1 for r in user.values() if passes(r, SLOW))
        rows.append((sel.isoformat(), exist, len(user), len(t20), any10, n[10_000], n[100_000], n[1_000_000], t1, slow))
    table(['S', 'user vaults existing', 'with fills in lookback', '>=20 trips', '>=20 trips & eq>=$10k (any lookback snap)',
           '>=20 trips & eq>=$10k (snap <=7 d)', '... >=$100k', '... >=$1M', 'pass all 001 T1 filters', 'pass 001 slow filters'], rows)
    print('\nHLP parent/children are excluded from every count above. Equity is the latest snapshot in the lookback; '
          'snapshots hold only accounts with >= 1 open position, so a vault flat at S has no reading.')

    h('B2. Vaults existing at quarterly dates (list fields only; no closure information pre-archive from the list)')
    rows = []
    clo = ledger_closure()
    d0 = dt.date(2023, 7, 1)
    while d0 <= dt.date(2025, 7, 1):
        exist = sum(1 for v in VAULTS if v['kind'] == 'user' and v['create_ms'] < ms(d0))
        flags = {a: (BY_ADDR[a]['create_ms'] < ms(d0) and (t is None or t >= ms(d0)))
                 for a, t in clo.items()}
        made = {a: BY_ADDR[a]['create_ms'] < ms(d0) for a in clo}
        share = None
        if clo:
            w = stratum_weights()
            strat = {s['address']: s['stratum'] for s in SAMPLE}
            num = sum(w[strat[a]] for a, f in flags.items() if f)
            den = sum(w[strat[a]] for a, f in made.items() if f)
            share = num / den if den else None
        rows.append((d0.isoformat(), exist, f'{100 * share:.0f}%' if share is not None else '—',
                     round(exist * share) if share is not None else None))
        d0 = dt.date(d0.year + (d0.month + 2) // 12, (d0.month + 2) % 12 + 1, 1)
    table(['date', 'user vaults created before', 'est. share without a prior vaultDistribution (sample)', 'est. not yet distributed'], rows)

    h('B3. Behaviour of selectable vaults (>=20 trips, eq>=$10k snap <=7 d) vs 001\'s non-vault T1 buckets')
    rows = []
    for sel in SELECTIONS:
        sv = selectable(sel)
        pop = json.loads(Path(str(POP).format(sel)).read_text())['t1_buckets']
        f = {r[0]: (r[1], r[2]) for r in con.execute(
            f"SELECT address, median_hold_h, fills_per_active_day FROM read_parquet('{str(FEAT).format(sel)}')").fetchall()}
        grp = {'vaults': [(r['median_hold_h'], r['fills_per_active_day']) for r in sv.values()]}
        for b in ('slow', 'day', 'scalper'):
            grp[f'001 {b}'] = [f[a] for a, bb in pop.items() if bb == b and a in f]
        for g, xs in grp.items():
            rows.append((sel.isoformat(), g, len(xs), q([x[0] for x in xs], 50), q([x[1] for x in xs], 50)))
        bk = Counter('scalper' if (r['median_hold_h'] or 0) < 1 else 'day' if r['median_hold_h'] < 24 else 'slow'
                     for r in sv.values() if r['median_hold_h'] is not None)
        rows.append((sel.isoformat(), 'vaults by 001 hold bucket', len(sv), f"scalper {bk['scalper']} / day {bk['day']} / slow {bk['slow']}", ''))
    table(['S', 'group', 'n', 'median of median hold (h)', 'median fills / active day'], rows)


def C():
    h('C1. Archive coverage of vaults existing at S (user vaults; HLP excluded)')
    rows = []
    for sel in SELECTIONS:
        exist = [v['address'] for v in VAULTS if v['kind'] == 'user' and v['create_ms'] < ms(sel)]
        lb = sel - dt.timedelta(days=LB)
        fl = {a for (a,) in con.execute(f"SELECT DISTINCT address FROM d WHERE day >= DATE '{lb}' AND day < DATE '{sel}'").fetchall()}
        sn = {a for (a,) in con.execute(f"SELECT DISTINCT address FROM vs WHERE snapshot_ms >= {ms(lb)} AND snapshot_ms < {ms(sel)}").fetchall()}
        eq, _ = last_snap_equity(sel)
        e_all = sum(v for a, v in eq.items() if BY_ADDR[a]['kind'] == 'user')
        e_fill = sum(v for a, v in eq.items() if BY_ADDR[a]['kind'] == 'user' and a in fl)
        sv = selectable(sel)
        e_sel = sum(v for a, v in eq.items() if a in sv)
        ex = set(exist)
        rows.append((sel.isoformat(), len(exist), pct(len(fl & ex), len(ex)), pct(len(sn & ex), len(ex)),
                     sum(1 for a in eq if a in ex), pct(e_fill, e_all), pct(e_sel, e_all)))
    table(['S', 'user vaults existing', 'with fills in lookback', 'with a snapshot row in lookback',
           'in the snapshot just before S', 'equity share (that snapshot) of vaults with lookback fills',
           'equity share of selectable vaults'], rows)

    h('C2. Who trades: vault address vs its leader\'s own address (archive year, notional traded)')
    vn = dict(con.execute("SELECT address, SUM(notional) FROM d GROUP BY 1").fetchall())
    shares, rows = [], []
    sel_any = set().union(*(selectable(s).keys() for s in SELECTIONS))
    lead_n = Counter(v['leader'] for v in VAULTS)
    for a in sorted(sel_any):
        lv = BY_ADDR[a]['leader']
        x, y = vn.get(a, 0.0), vn.get(lv, 0.0)
        shares.append(x / (x + y) if x + y else None)
    shares_ok = [s for s in shares if s is not None]
    print(f'Vaults selectable at any S: {len(sel_any)}. Vault share of (vault + leader) notional in the archive year: '
          f'p10 {q(shares_ok, 10):.2f}, p25 {q(shares_ok, 25):.2f}, median {q(shares_ok, 50):.2f}, p75 {q(shares_ok, 75):.2f}. '
          f'Leader trades more than the vault: {sum(1 for s in shares_ok if s < 0.5)}; '
          f'leader address has no archive fill: {sum(1 for a in sel_any if vn.get(BY_ADDR[a]["leader"], 0) == 0)}.')
    multi = sum(1 for a in sel_any if lead_n[BY_ADDR[a]['leader']] > 1)
    print(f'Of these, vaults whose leader runs > 1 listed vault: {multi}. '
          f'Leaders in the list: {len(lead_n):,}; leading > 1 vault: {sum(1 for n in lead_n.values() if n > 1):,}; max {max(lead_n.values())}.')
    # same-day, same-coin co-trading is not checked (would need coin-level fills); a few examples:
    rows = []
    for a in sorted(sel_any, key=lambda a: -vn.get(a, 0))[:8]:
        lv = BY_ADDR[a]['leader']
        both = con.execute(f"""SELECT COUNT(*) FILTER (WHERE address = '{a}'), COUNT(*) FILTER (WHERE address = '{lv}'),
                                COUNT(DISTINCT day) FILTER (WHERE address = '{a}' AND day IN (SELECT day FROM d WHERE address = '{lv}'))
                                FROM d WHERE address IN ('{a}', '{lv}')""").fetchone()
        rows.append((a[:10] + '…', both[0], both[1], both[2], f'{vn.get(a, 0) / max(1, vn.get(a, 0) + vn.get(lv, 0)):.2f}'))
    print()
    table(['vault (8 largest by archive notional)', 'vault active days', 'leader active days', 'days both active', 'vault notional share'], rows)


def F():
    h('F. Capacity: selectable vaults (>=20 trips, snap <=7 d) whose equity at S is >= 10x the deposit')
    rows = []
    for sel in SELECTIONS:
        sv = selectable(sel)
        rows.append((sel.isoformat(), len(sv), *(sum(1 for r in sv.values() if r['account_value'] >= 10 * k)
                                                   for k in (10_000, 50_000, 250_000))))
    table(['S', 'selectable (eq >= $10k)', '$10k deposit (eq >= $100k)', '$50k (eq >= $500k)', '$250k (eq >= $2.5M)'], rows)
    print('\nEquity = account value at the latest snapshot <= 7 d before S (vault total, leader + depositors).')


# ---------------------------------------------------------------- D, E
def D():
    ok = {a: r for a, r in DETAILS.items() if r.get('ok')}
    h(f'D1. vaultDetails history density, pre-cutoff timestamps only ({len(DETAILS)} sampled, {len(ok)} returned a dict)')
    periods = ['day', 'week', 'month', 'allTime', 'perpDay', 'perpWeek', 'perpMonth', 'perpAllTime']
    rows = []
    for p in periods:
        n = [len(r['series_ts'].get(p, [])) for r in ok.values()]
        rows.append((p, sum(1 for x in n if x), q(n, 50), max(n) if n else None))
    table(['series', 'vaults with >=1 point < 2026-07-01', 'median points', 'max points'], rows)
    h('D2. allTime series by creation stratum (points < 2026-07-01)')
    rows = []
    strat = {s['address']: s['stratum'] for s in SAMPLE}
    for k in sorted({strat[a] for a in ok}):
        grp = [r for a, r in ok.items() if strat[a] == k]
        n = [len(r['series_ts'].get('allTime', [])) for r in grp]
        sp = [st.median([(b - a) / 86_400_000 for a, b in zip(ts, ts[1:])])
              for r in grp if len(ts := r['series_ts'].get('allTime', [])) > 2]
        lag = [(r['series_ts']['allTime'][0] - BY_ADDR[a]['create_ms']) / 86_400_000
               for a, r in ok.items() if strat[a] == k and r['series_ts'].get('allTime')]
        last = [day_of(r['series_ts']['allTime'][-1]) for r in grp if r['series_ts'].get('allTime')]
        rows.append((k, len(grp), sum(1 for x in n if x == 0), q(n, 50), q(sp, 50), q(lag, 50),
                     sum(1 for x in last if x < ARCHIVE_START), min(last).isoformat() if last else None))
    table(['stratum', 'sampled', 'no allTime point', 'median points', 'median spacing (d)',
           'median first point - creation (d)', 'last point before 2025-07-28', 'earliest last point'], rows)
    # spacing structure: is spacing uniform within a series, or does it grow with age?
    sp_all, ratio = [], []
    for r in ok.values():
        ts = r['series_ts'].get('allTime', [])
        if len(ts) > 5:
            g = [(b - a) / 86_400_000 for a, b in zip(ts, ts[1:])]
            sp_all += g
            ratio.append(max(g) / max(1e-9, min(g)))
    if sp_all:
        print(f'\nAll allTime gaps: p10 {q(sp_all, 10):.2f} d, median {q(sp_all, 50):.2f} d, p90 {q(sp_all, 90):.2f} d; '
              f'within-series max/min gap ratio median {q(ratio, 50):.1f}.')
    clo = ledger_closure()
    if clo:
        dist = [a for a, t in clo.items() if t is not None and a in ok]
        dist_pre = [a for a in dist if clo[a] < ms(ARCHIVE_START)]
        has = sum(1 for a in dist if ok[a]['series_ts'].get('allTime'))
        has_pre = sum(1 for a in dist_pre if ok[a]['series_ts'].get('allTime'))
        print(f'\nSampled vaults with a vaultDistribution before the cutoff (closure proxy): {len(dist)}; '
              f'still returning allTime points: {has}. Of those distributed before 2025-07-28: {len(dist_pre)}, '
              f'returning points: {has_pre}.')
        nodist = [a for a, t in clo.items() if t is None and a in ok]
        print(f'Sampled vaults without one: {len(nodist)}; with no allTime point < cutoff: '
              f'{sum(1 for a in nodist if not ok[a]["series_ts"].get("allTime"))}.')
    h('D3. Point-in-time computability of a 91-day lookback / 56-day hold from allTime timestamps alone')
    rows = []
    for sdate in windows_hist():
        lb0, he = ms(sdate - dt.timedelta(days=LB)), ms(sdate + dt.timedelta(days=HOLD))
        exist = [a for a, r in ok.items() if BY_ADDR[a]['create_ms'] < ms(sdate - dt.timedelta(days=LB)) and BY_ADDR[a]['kind'] == 'user']
        dense = {}
        for a in exist:
            ts = ok[a]['series_ts'].get('allTime', [])
            in_lb = sum(1 for t in ts if lb0 <= t < ms(sdate))
            in_h = sum(1 for t in ts if ms(sdate) <= t < he)
            dense[a] = (in_lb, in_h)
        clo = ledger_closure()
        alive = {a: clo.get(a) is None or clo[a] >= ms(sdate) for a in exist}
        k6 = {a: dense[a][0] >= 6 and dense[a][1] >= 4 for a in exist}
        k6a = {a: k6[a] and alive[a] for a in exist}
        rows.append((sdate.isoformat(), len(exist), q([v[0] for v in dense.values()], 50), q([v[1] for v in dense.values()], 50),
                     sum(k6.values()), sum(alive.values()), sum(k6a.values()), est(k6a)))
    table(['S', 'sampled user vaults created before S-91 d', 'median allTime points in lookback', 'median points in hold',
           '>=6 lookback & >=4 hold points', 'not distributed before S (ledger)', 'both', 'est. population (both)'], rows)
    print('\nThe allTime grid keeps producing points after a vault\'s vaultDistribution (D1 note), so point '
          'density alone does not show the vault was alive; the ledger column does.')
    print('\nEst. population = sampled count weighted by stratum (creation quarter) inverse sampling rate; '
          'vaults created after 2025-06-30 are not in the sample, so windows from 2025-Q3 on are understated.')


def est(flags):
    w = stratum_weights()
    strat = {s['address']: s['stratum'] for s in SAMPLE}
    return round(sum(w[strat[a]] for a, f in flags.items() if f))


def windows_hist(start=dt.date(2023, 5, 1), end=CUTOFF):
    out, s = [], start + dt.timedelta(days=LB)
    while s + dt.timedelta(days=HOLD) <= end:
        out.append(s)
        s += dt.timedelta(days=HOLD)
    return out


def E():
    ok = [r for r in DETAILS.values() if r.get('ok')]
    h('E1. Terms from vaultDetails (sampled vaults; allowDeposits / alwaysCloseOnWithdraw are TODAY\'s values)')
    users = [r for a, r in DETAILS.items() if r.get('ok') and BY_ADDR[a]['kind'] == 'user']
    table(['leaderCommission', 'user vaults'], sorted(Counter(r['leaderCommission'] for r in users).items(), key=lambda x: str(x[0])))
    print()
    table(['allowDeposits (today)', 'user vaults'], sorted(Counter(str(r['allowDeposits_today']) for r in users).items()))
    print()
    table(['alwaysCloseOnWithdraw (today)', 'user vaults'], sorted(Counter(str(r['alwaysCloseOnWithdraw_today']) for r in users).items()))
    print()
    table(['relationship type', 'sampled'], sorted(Counter(str(r['rel_type']) for r in ok).items()))


# ---------------------------------------------------------------- G
def G():
    h('G1. Does the list contain long-dead vaults?')
    act = {a for (a,) in con.execute('SELECT DISTINCT address FROM d').fetchall()}
    sn = {a for (a,) in con.execute('SELECT DISTINCT address FROM vs').fetchall()}
    pre = [v for v in VAULTS if v['kind'] == 'user' and v['create_ms'] < ms(ARCHIVE_START)]
    dead = [v for v in pre if v['address'] not in act and v['address'] not in sn]
    print(f'User vaults created before 2025-07-28: {len(pre):,}; with no archive fill and no snapshot row in '
          f'2025-07-28 .. 2026-06-30: {len(dead):,} ({pct(len(dead), len(pre))}).')
    clo = ledger_closure()
    if clo:
        early = sum(1 for a, t in clo.items() if t is not None and t < ms(dt.date(2024, 7, 1)))
        print(f'Sampled vaults whose first vaultDistribution is before 2024-07-01: {early} of {len(clo)}.')
    h('G2. Vaults created by sampled leaders (leader ledgers, vaultCreate events < 2026-07-01) that are not in the list')
    listed = set(BY_ADDR)
    created, missing = set(), set()
    for r in LEDGER:
        if r['who'] != 'leader' or 'events' not in r:
            continue
        for e in r['events']:
            if e['type'] == 'vaultCreate' and e['vault']:
                created.add(e['vault'])
                if e['vault'] not in listed:
                    missing.add(e['vault'])
    nlead = sum(1 for r in LEDGER if r['who'] == 'leader' and 'events' in r)
    print(f'Leaders checked: {nlead}; distinct vaultCreate vaults: {len(created)}; not in the list: {len(missing)}.')
    for m in sorted(missing)[:20]:
        print(f'  missing: {m}')
    roles = jl(OUT / 'roles' / 'roles.jsonl')
    if roles:
        h('G3. userRole (today) of archive addresses not in the list')
        rows = []
        for g in ('top', 'random', 'control'):
            rows.append((g, sum(1 for r in roles if r['group'] == g),
                         ', '.join(f'{k} {n}' for k, n in Counter(r['role'] for r in roles if r['group'] == g).most_common())))
        table(['group', 'addresses', 'roles'], rows)
        vr = [r['address'] for r in roles if r['role'] == 'vault' and r['group'] != 'control']
        for a in vr[:20]:
            print(f'  role vault, not listed: {a}')


# ---------------------------------------------------------------- H
def H():
    h('H. Independent selection windows (91 d lookback, 56 d hold, step 56 d, hold must end by the stated date)')
    designs = [('pre-archive history only (2023-05-01 .. 2025-07-28)', dt.date(2023, 5, 1), ARCHIVE_START),
               ('archive year only (001 schedule)', ARCHIVE_START, CUTOFF),
               ('history across both (2023-05-01 .. 2026-07-01)', dt.date(2023, 5, 1), CUTOFF)]
    for name, a, b in designs:
        ws = windows_hist(a, b)
        print(f'- {name}: {len(ws)} windows, S = {ws[0]} .. {ws[-1]}' if ws else f'- {name}: 0')
    print('\nNon-overlapping holds by construction (step = hold). Per-window selectable counts: B1 (archive), D3 (history, sample).')


if __name__ == '__main__':
    for k in (sys.argv[1:] or list('ABCDEFGH')):
        globals()[k]()
