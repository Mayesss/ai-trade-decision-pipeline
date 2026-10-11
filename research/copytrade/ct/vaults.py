"""Registration 007 (L1) — depositing into top-scored Hyperliquid user vaults. Mechanics (Revision 1).

Pure functions plus holdout-safe loaders. Nothing here computes a score or an outcome on real
data by itself: the guarded runner (r007_run.py) is the only caller that feeds real fills, real
NAV series and real ledgers into the score / outcome / criteria functions.

Pieces, in the order the runner applies them:
  load_list          the cached vault list, ONLY address / leader / name / createTimeMillis /
                     relationship (000c; apr, pnls, tvl, isClosed, allowDeposits never read);
  features_sql       001's p4 feature SQL (copied), lookback [S - 91 d, S), snapshots < S;
  net_exposure_sql   lookback median snapshot net leverage (the criterion-1 tercile control);
  eligible           001 T1's filters as fixed for 007 + a fill in the 7 days before S (+ an
                     outside depositor in the lookback, D2 option);
  ledger_flows       signed flows on the vault's own account; a distribution is an OUTFLOW;
  measure_points     entry at the first allTime point in [S, S + 10 d] (else the first snapshot
                     there, else unmeasurable) — NEVER a point before S, never one after the hold;
  hold_return        Modified Dietz per grid interval, chain-linked, unscaled; exit at the hold
                     end, at in-hold exhaustion (AV ~ 0 after an outflow), or at the start of a
                     thin-base interval; profit share; exit cost; market_adjusted;
  gates              NAV identity (flow-free and flow intervals), profit-share check, unmeasurable
                     share — all run inside the guarded runner before any criterion;
  criteria           the five criteria (criterion 1: Fisher Z + window t-test + 3 of 4; criterion 2
                     in two versions), the window-and-leader Z (reported), leader-clustered SE,
                     placebo, holdout confirm on the market-adjusted outcome;
  simulate_power     joint power with a common market factor and the NAV attenuation.
"""
import datetime as dt
import glob
import hashlib
import json
import math
import random
import re
import statistics
import subprocess
from pathlib import Path

from . import stats
from .net import DATA

DAY = 86_400_000
HOUR = 3_600_000
CUTOFF_MS = 1_782_864_000_000            # 2026-07-01T00:00:00Z — nothing at or after is read, ever
CUTOFF = dt.date(2026, 7, 1)
LEDGER_END_MS = CUTOFF_MS - 1            # endTime of every ledger request (2026-06-30T23:59:59.999Z)
ARCHIVE_START = dt.date(2025, 7, 28)
LOOKBACK_DAYS, HOLD_DAYS = 91, 56
# 001's four selection dates (ct/schedule.windows('hyperliquid')) — the primary schedule (D3)
SELECTIONS = (dt.date(2025, 10, 27), dt.date(2025, 12, 22), dt.date(2026, 2, 16), dt.date(2026, 4, 13))
# the discovery run never reads a fill, ledger row, grid point or snapshot at or after the last
# hold's end (Revision 1, M3): 2026-04-13 + 56 d
READ_END = SELECTIONS[-1] + dt.timedelta(days=HOLD_DAYS)
# the 28-day-step alternative (overlapping holds), considered and not chosen (registration §3)
SELECTIONS_28 = tuple(dt.date(2025, 10, 27) + dt.timedelta(days=28 * k) for k in range(7))
# holdout options (D14, Revision 2; the owner chooses): holds entirely after the cutoff,
# non-overlapping, 56-day step, read once after the option's fixed end date
HOLDOUT_A = (dt.date(2026, 7, 6), dt.date(2026, 8, 31))
HOLDOUT_B = HOLDOUT_A + (dt.date(2026, 10, 26), dt.date(2026, 12, 21))
HOLDOUT_SELECTIONS = HOLDOUT_A                       # option A; option B adds S3, S4
HOLDOUT_END = HOLDOUT_SELECTIONS[-1] + dt.timedelta(days=HOLD_DAYS)          # 2026-10-26
HOLDOUT_B_END = HOLDOUT_B[-1] + dt.timedelta(days=HOLD_DAYS)                 # 2027-02-15
HOLDOUT_ENTRY_DAYS = 14                  # holdout entry: HLP's first grid point in [S, S + 14 d], common to all
PROFIT_SHARE = 0.10                      # leader profit share on positive depositor profit (docs)
Z_CRIT = 2.33                            # one-sided; trial 15 after 14 fails (005's precedent)
MIN_POSITIVE_WINDOWS = 3                 # IC > 0 in >= 3 of 4 windows
Z_HOLDOUT = 1.64
D_SE_Z = 1.64                            # criterion 2 version (b) / the holdout's D / SE
ENTRY_DAYS = 10                          # entry point must lie in [S, S + 10 d]
ACTIVITY_DAYS = 7                        # eligibility: >= 1 fill in [S - 7 d, S)
CLOSURE_AV_ABS, CLOSURE_AV_REL = 100.0, 0.01   # exhaustion: AV <= max($100, 1% of equity at S) after an outflow
UNMEASURABLE_MAX = 0.05                  # more unmeasurable vault-windows than this: the run stops
C3_MISSING_MAX = 0.10                    # criterion 3 fails if more of the top quintile's c3 values are missing
BASE_FLOOR = 1_000.0                     # an interval starting below this ends the measurement at its start
C3_TOL = DAY                             # criterion 3: vault grid point within 1 day of HLP's
ATTENUATION = 0.93                       # Spearman(measured, true) of the NAV estimator (synthetic)
GATE_TOL, GATE_FREE_SHARE, GATE_FLOW_SHARE = 0.005, 0.95, 0.90
COMMISSION_SHARE = 0.90
WITHDRAW_CANDIDATES = ('requestedUsd', 'netWithdrawnUsd')
ELIGIBILITY = {'round_trips_min': 20, 'account_value_min': 10_000.0, 'equity_age_h_max': 168.0,
               'maker_share_max': 0.5, 'fills_per_day_max': 500.0}
HLP_PARENT = '0xdfc24b077bc1425ad1dea75bcb6f8158e10df303'
LIST_ALLOWED = ('vaultAddress', 'leader', 'name', 'createTimeMillis', 'relationship')
SNAPS = DATA / 'derived/hyperliquid/snapshot_accounts.parquet'
BTC_1H = DATA / 'derived/regimes/btc_1h.parquet'
VAULT_DIR = DATA / 'vaults'
HLP_CACHE = DATA / 'cache/hl-hlp-vaults'
FILLS_DIR = DATA / 'pruned/by_dex/hyperliquid/fills/perp/all'
EPS = 1e-9


def ms(d):
    return int(dt.datetime.combine(d, dt.time(), dt.UTC).timestamp() * 1000)


assert ms(CUTOFF) == CUTOFF_MS
READ_END_MS = ms(READ_END)
assert READ_END_MS <= CUTOFF_MS


# ------------------------------------------------------------------ loaders (holdout-safe)

def load_list(path=None):
    """[{address, leader, name, create_ms, kind}] of listed vaults created before the cutoff.

    Only LIST_ALLOWED summary fields are touched (000c, programme §3.2). kind: 'hlp_parent' |
    'hlp_child' | 'user'."""
    if path is None:
        paths = glob.glob(str(DATA / 'cache/hl-vaults/*.json'))
        assert len(paths) == 1, paths
        path = paths[0]
    raw = json.loads(Path(path).read_text())
    out = []
    for v in raw:
        s = v['summary']
        rec = {k: s[k] for k in LIST_ALLOWED}
        if rec['createTimeMillis'] >= CUTOFF_MS:
            continue
        rel = (rec['relationship'] or {}).get('type')
        a = rec['vaultAddress'].lower()
        kind = 'hlp_parent' if a == HLP_PARENT else 'hlp_child' if rel == 'child' else 'user'
        out.append({'address': a, 'leader': rec['leader'].lower(), 'name': rec['name'],
                    'create_ms': rec['createTimeMillis'], 'kind': kind})
    del raw
    return out


def day_files(start, end, root=FILLS_DIR):
    """Archive day files for dates in [start, end), never on or after the cutoff."""
    end = min(end, CUTOFF)
    days = [start + dt.timedelta(days=i) for i in range((end - start).days)]
    return [str(root / f'date={d.isoformat()}' / 'fills.parquet') for d in days
            if (root / f'date={d.isoformat()}' / 'fills.parquet').exists()]


def features_sql(con, files, snaps, sel_ms, lb_ms, addresses):
    """001's lookback features (p4_features.features, copied; the SQL is unchanged except for the
    address restriction and the explicit cutoff on snapshots) for `addresses` over the given day
    files. Behaviour only: no cash flow, no realized_pnl. Returns a list of dicts.

    Snapshots: snapshot_ms in [lb_ms, sel_ms) AND < CUTOFF_MS, in the query (000c)."""
    con.execute('CREATE OR REPLACE TEMP TABLE wanted_feat(address VARCHAR)')
    con.executemany('INSERT INTO wanted_feat VALUES (?)', [(a,) for a in addresses])
    if not files:
        return []
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW f AS
        SELECT address, coin, timestamp, side,
               CAST(size AS DOUBLE) AS size, CAST(price AS DOUBLE) AS price,
               CAST(start_position AS DOUBLE) AS sp, crossed,
               twap_id IS NOT NULL AS is_twap, COALESCE(is_liquidation, false) AS is_liq,
               filename, file_row_number
        FROM read_parquet({files!r}, filename = true, file_row_number = true)
        WHERE address IN (SELECT address FROM wanted_feat)
          AND epoch_ms(timestamp) >= {lb_ms} AND epoch_ms(timestamp) < {sel_ms}
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE activity AS
        SELECT address,
               COUNT(*) AS n_fills,
               COUNT(DISTINCT CAST(timestamp AS DATE)) AS active_days,
               COUNT(DISTINCT coin) AS n_coins,
               SUM(size * price) AS notional,
               COALESCE(SUM(size * price) FILTER (WHERE NOT crossed), 0) / SUM(size * price) AS maker_share,
               COALESCE(SUM(size * price) FILTER (WHERE is_twap), 0) / SUM(size * price) AS twap_share,
               COUNT(*) FILTER (WHERE is_liq) AS n_liquidations
        FROM f GROUP BY address
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE trips AS
        WITH s AS (
            SELECT address, coin, timestamp, sp,
                   sp + CASE WHEN side = 'buy' THEN size ELSE -size END AS ep,
                   filename, file_row_number
            FROM f
        ), e AS (
            SELECT *,
                   (sp * ep < 0) AS is_flip,
                   (abs(sp) < {EPS} AND abs(ep) > {EPS}) OR (sp * ep < 0) AS is_open,
                   (abs(sp) > {EPS} AND abs(ep) <= {EPS} * greatest(1, abs(sp))) OR (sp * ep < 0) AS is_close
            FROM s
        ), q AS (
            SELECT *, SUM(CAST(is_open AS INT)) OVER (
                       PARTITION BY address, coin ORDER BY timestamp, filename, file_row_number
                       ROWS UNBOUNDED PRECEDING) AS seq
            FROM e WHERE is_open OR is_close
        ), opens AS (
            SELECT address, coin, seq, timestamp AS t_open FROM q WHERE is_open
        ), closes AS (
            SELECT address, coin, seq - CAST(is_flip AS INT) AS seq, MIN(timestamp) AS t_close
            FROM q WHERE is_close GROUP BY 1, 2, 3
        )
        SELECT o.address,
               COUNT(c.t_close) AS round_trips,
               COUNT(*) - COUNT(c.t_close) AS open_at_selection,
               MEDIAN(epoch(c.t_close) - epoch(o.t_open)) / 3600.0 AS median_hold_h
        FROM opens o LEFT JOIN closes c USING (address, coin, seq)
        GROUP BY o.address
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE equity AS
        WITH s AS (
            SELECT "user" AS address, snapshot_ms, account_value,
                   gross_notional / NULLIF(account_value, 0) AS lev
            FROM read_parquet('{snaps}')
            WHERE snapshot_ms < {CUTOFF_MS} AND snapshot_ms < {sel_ms} AND snapshot_ms >= {lb_ms}
              AND "user" IN (SELECT address FROM wanted_feat)
        )
        SELECT address,
               arg_max(account_value, snapshot_ms) AS account_value,
               MEDIAN(lev) AS median_leverage,
               (MAX(snapshot_ms) - {sel_ms}) / 3600000.0 AS equity_age_h
        FROM s GROUP BY address
    """)
    cur = con.execute("""
        SELECT a.*, t.round_trips, t.open_at_selection, t.median_hold_h,
               e.account_value, e.median_leverage, e.equity_age_h,
               a.n_fills / a.active_days AS fills_per_active_day
        FROM activity a
        LEFT JOIN trips t USING (address)
        LEFT JOIN equity e USING (address)
        ORDER BY a.address
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def net_exposure_sql(con, snaps, sel_ms, lb_ms, addresses):
    """{address: median over lookback snapshot rows of net_notional / account_value} — the
    vault's signed net exposure before S (size x direction; the criterion-1 tercile control and
    the power model's beta). Snapshots strictly before S and before the cutoff, in the query."""
    con.execute('CREATE OR REPLACE TEMP TABLE wanted_ne(address VARCHAR)')
    if not addresses:
        return {}
    con.executemany('INSERT INTO wanted_ne VALUES (?)', [(a,) for a in addresses])
    rows = con.execute(f"""
        SELECT "user", MEDIAN(net_notional / NULLIF(account_value, 0))
        FROM read_parquet('{snaps}')
        WHERE snapshot_ms < {CUTOFF_MS} AND snapshot_ms < {sel_ms} AND snapshot_ms >= {lb_ms}
          AND "user" IN (SELECT address FROM wanted_ne)
        GROUP BY 1""").fetchall()
    return {a: v for a, v in rows}


def activity_sql(con, files, addresses, end_ms):
    """[(address, day, n_fills, first_ms, last_ms)] per (vault, UTC day) over the given day files,
    fills strictly before end_ms. Fill TIMES and counts only (behaviour; the runner reads only
    the pre-S part, for the 7-day activity rule)."""
    con.execute('CREATE OR REPLACE TEMP TABLE wanted_act(address VARCHAR)')
    if not files or not addresses:
        return []
    con.executemany('INSERT INTO wanted_act VALUES (?)', [(a,) for a in addresses])
    return con.execute(f"""
        SELECT address, CAST(CAST(timestamp AS DATE) AS VARCHAR), COUNT(*), MIN(epoch_ms(timestamp)), MAX(epoch_ms(timestamp))
        FROM read_parquet({files!r})
        WHERE address IN (SELECT address FROM wanted_act) AND epoch_ms(timestamp) < {min(end_ms, CUTOFF_MS)}
        GROUP BY 1, 2 ORDER BY 1, 2""").fetchall()


def last_fill_before(day_rows, t_ms):
    """Latest fill time strictly before t from [(day, n, first_ms, last_ms)] rows of one vault."""
    best = None
    for _, n, first, last in day_rows:
        if n and first < t_ms:
            cand = last if last < t_ms else None
            if cand is None:
                cand = first          # the day straddles t: its first fill is before t (lower bound)
            best = cand if best is None or cand > best else best
    return best


# ------------------------------------------------------------------ eligibility (no look-ahead)

def outside_depositor(events, leader, sel_ms):
    """True if a vaultWithdraw by a user other than the leader occurred in [S - 91 d, S).
    (vaultDeposit rows carry no depositor field; a withdrawal names its user.)"""
    lo = sel_ms - LOOKBACK_DAYS * DAY
    return any(e['delta'].get('type') == 'vaultWithdraw' and lo <= e['time'] < sel_ms
               and (e['delta'].get('user') or '').lower() not in ('', leader) for e in events)


def eligible(row, sel_ms, last_fill_ms=None, has_outside=None, rules=ELIGIBILITY, require_outside=False):
    """(ok, reason). `row` holds lookback features computed on [S - 91 d, S) only (features_sql);
    last_fill_ms: the vault's latest fill strictly before S; has_outside: outside_depositor().

    equity_age_h is (latest snapshot - S) in hours, so it is <= 0; the 7-day rule is
    -equity_age_h <= 168 (001's p5_population.passes). Revision 1: a vaultDistribution is NOT
    an exclusion (distributions are outflows; many vaults keep trading after one). Activity is
    point-in-time: at least one fill in [S - 7 d, S)."""
    if row is None:
        return False, 'no lookback fills'
    if (row.get('round_trips') or 0) < rules['round_trips_min']:
        return False, 'trips'
    age = row.get('equity_age_h')
    if age is None or age > 0 or -age > rules['equity_age_h_max']:
        return False, 'equity age'
    if (row.get('account_value') or 0) < rules['account_value_min']:
        return False, 'equity'
    if row.get('maker_share') is None or row['maker_share'] > rules['maker_share_max']:
        return False, 'maker share'
    if (row.get('fills_per_active_day') or 0) > rules['fills_per_day_max']:
        return False, 'fills per day'
    if last_fill_ms is None or last_fill_ms >= sel_ms or last_fill_ms < sel_ms - ACTIVITY_DAYS * DAY:
        return False, 'inactive'
    if require_outside and not has_outside:
        return False, 'no outside depositor'
    return True, 'ok'


# ------------------------------------------------------------------ ledger and NAV data

def project_ledger(page, vault):
    """Ledger rows kept as served (time, hash, delta), after re-checking time < cutoff.
    Raises SystemExit on a holdout-dated row (stop and report)."""
    out = []
    for x in page:
        if x['time'] >= CUTOFF_MS:
            raise SystemExit(f'holdout-dated ledger row returned for {vault}; stop and report')
        out.append({'time': x['time'], 'hash': x.get('hash'), 'delta': x['delta']})
    return out


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def first_distribution(events, vault):
    """Time of the first vaultDistribution on the vault's own ledger, or None."""
    ts = [e['time'] for e in events if e['delta'].get('type') == 'vaultDistribution'
          and (e['delta'].get('vault') or vault).lower() == vault]
    return min(ts) if ts else None


WITHDRAW_FIELDS = ('requestedUsd', 'usdc')     # amount leaving the vault on a depositor withdrawal


def ledger_flows(events, vault, withdraw_fields=WITHDRAW_FIELDS):
    """[(t, signed USDC)] into (+) / out of (-) the vault's account, from its own ledger.

    vaultCreate / vaultDeposit +usdc; vaultWithdraw -(requestedUsd, else usdc); vaultDistribution
    -usdc; internalTransfer / send / accountClassTransfer by direction. Other types (liquidation,
    funding-free bookkeeping) carry no external flow and are skipped. Amounts are never printed."""
    out = []
    for e in events:
        d = e['delta']
        typ = d.get('type')
        if typ in ('vaultCreate', 'vaultDeposit'):
            amt = _f(d.get('usdc'))
            if amt is not None:
                out.append((e['time'], amt))
        elif typ == 'vaultWithdraw':
            amt = next((_f(d.get(k)) for k in withdraw_fields if _f(d.get(k)) is not None), None)
            if amt is not None:
                out.append((e['time'], -abs(amt)))
        elif typ == 'vaultDistribution':
            amt = _f(d.get('usdc'))
            if amt is not None:
                out.append((e['time'], -abs(amt)))
        elif typ in ('internalTransfer', 'send', 'spotTransfer', 'subAccountTransfer'):
            amt = _f(d.get('usdc') or d.get('amount') or d.get('usdcValue'))
            dest = (d.get('destination') or d.get('dest') or '').lower()
            src = (d.get('user') or '').lower()
            if amt is None:
                continue
            if dest == vault:
                out.append((e['time'], abs(amt)))
            elif src == vault or dest:
                out.append((e['time'], -abs(amt)))
    return sorted(out)


def project_details(d):
    """vaultDetails -> {'allTime': {'accountValueHistory': [[t, v]], 'pnlHistory': [[t, v]]}} with
    t < cutoff only. Every other field (apr, followers, leaderFraction, maxDistributable,
    maxWithdrawable, isClosed, allowDeposits, description, vlm, the other portfolio periods) is
    dropped here, before anything is printed or saved."""
    if not isinstance(d, dict):
        return {'ok': False}
    out = {}
    for name, obj in d.get('portfolio') or []:
        if name != 'allTime':
            continue
        out = {k: sorted([[int(t), v] for t, v in obj.get(k, []) if int(t) < CUTOFF_MS])
               for k in ('accountValueHistory', 'pnlHistory')}
    return {'ok': bool(out), 'allTime': out}


def grid_from_alltime(alltime):
    """[(t, av, pnl)] at the timestamps both series share, t < cutoff, sorted."""
    av = {int(t): _f(v) for t, v in alltime.get('accountValueHistory', []) if int(t) < CUTOFF_MS}
    pn = {int(t): _f(v) for t, v in alltime.get('pnlHistory', []) if int(t) < CUTOFF_MS}
    return [(t, av[t], pn[t]) for t in sorted(set(av) & set(pn)) if av[t] is not None and pn[t] is not None]


def load_hlp_alltime(path=None):
    """HLP parent's allTime from the H7 cache (vaultDetails as served 2026-10-10), truncated at
    load to t < cutoff; nothing else is kept. Never printed by any script."""
    if path is None:
        key = hashlib.sha256(f'2026-10-10:{HLP_PARENT}'.encode()).hexdigest()[:24]
        path = HLP_CACHE / f'{key}.json'
    return project_details(json.loads(Path(path).read_text()))['allTime']


def bounded_events(events, end_ms):
    """Ledger rows strictly before min(end_ms, cutoff) — every runner read goes through this."""
    end = min(end_ms, CUTOFF_MS)
    return [e for e in events if e['time'] < end]


def bounded_grid(grid, end_ms):
    end = min(end_ms, CUTOFF_MS)
    return [g for g in grid if g[0] < end]


# ------------------------------------------------------------------ the depositor's hold return (Revision 2)

def modified_dietz(a, b, flows, numerator='pnl', t_end=None):
    """One interval (a, b], a/b = (t, av, pnl). flows: [(t, amount)] inside (a.t, b.t], or None =
    unknown timing (the net flow av_b - av_a - (pnl_b - pnl_a) is placed at the midpoint).
    numerator 'pnl': HL's own pnl change; 'av': av change minus ALL the flows in (a, b]. An entry
    point from a snapshot has pnl None and always uses 'av' (it needs the ledger flows).
    t_end: the depositor's exit inside the interval (the exhausting payout). The capital base then
    counts only flows strictly before t_end, weighted over [a.t, t_end].
    Returns (return, base) or (None, base) if the capital base is not positive."""
    ta, av_a, pnl_a = a
    tb, av_b, pnl_b = b
    if pnl_a is None or pnl_b is None:
        if flows is None:
            return None, None
        numerator = 'av'
    te = tb if t_end is None else t_end
    span = te - ta
    if flows is None:
        net = (av_b - av_a) - (pnl_b - pnl_a)
        weighted = 0.5 * net if t_end is None else 0.0
        fsum = net
    else:
        fsum = sum(f for _, f in flows)
        live = [(t, f) for t, f in flows if t_end is None or t < t_end]
        weighted = sum(f * (te - t) / span for t, f in live) if span > 0 else 0.0
    gain = (pnl_b - pnl_a) if numerator == 'pnl' else (av_b - av_a - fsum)
    base = av_a + weighted
    if base <= 0:
        return None, base
    return gain / base, base


def measure_points(grid, sel_ms, end_ms=None, snaps=None, entry_lo=None, entry_hi=None):
    """The candidate points of a depositor's hold: (points, entry_source) or (None, reason).

    - Entry: the first allTime grid point in [max(S, entry_lo), entry_hi] (default [S, S + 10 d]);
      if none, the first snapshot (t, av) there (pnl unknown -> that interval uses AV minus the
      ledger flows); if none, 'no_entry'. NEVER a point before S.
    - Then every grid point after the entry and at or before `end` (default S + 56 d). Nothing
      after `end` is ever looked at (Revision 2, M-b). Fewer than two points: 'no_exit'."""
    end_ms = sel_ms + HOLD_DAYS * DAY if end_ms is None else end_ms
    lo = max(sel_ms, entry_lo if entry_lo is not None else sel_ms)
    hi = min(entry_hi if entry_hi is not None else sel_ms + ENTRY_DAYS * DAY, end_ms)
    entry, src = None, 'grid'
    cand = [g for g in grid if lo <= g[0] <= hi]
    if cand:
        entry = cand[0]
    elif snaps:
        s = [(t, av, None) for t, av in snaps if lo <= t <= hi]
        if s:
            entry, src = s[0], 'snapshot'
    if entry is None:
        return None, 'no_entry'
    pts = [entry] + [g for g in grid if entry[0] < g[0] <= end_ms]
    if len(pts) < 2:
        return None, 'no_exit'
    assert all(sel_ms <= p[0] <= end_ms for p in pts), 'a point outside [S, end] reached the outcome'
    return pts, src


def exhaustion_threshold(equity_at_s):
    """AV at or below max($100, 1% of the vault's equity at S) counts as emptied."""
    return max(CLOSURE_AV_ABS, CLOSURE_AV_REL * (equity_at_s or 0.0))


def hold_return(grid, flows, sel_ms, end_ms=None, snaps=None, numerator='pnl', scale=False,
                profit_share=PROFIT_SHARE, exit_cost=0.0, entry_lo=None, entry_hi=None, equity_at_s=None):
    """The depositor's return from the entry point (at or after S) to the exit point, decided from
    data inside [S, end] only (Revision 2, M-b).

    Chain-links the Modified Dietz return of each interval (flows from the vault's own ledger; a
    distribution is an outflow like a withdrawal). Exits, in order of precedence per interval:
      * thin base: an interval whose starting account value is under $1k ends the measurement at
        its START (nothing at stake; m9). If that is the entry point the row is unmeasurable;
      * exhaustion: the first grid point b whose AV <= max($100, 1% of equity at S) after an
        outflow inside (a, b]: the depositor was paid out; the exit is the last outflow in (a, b],
        and the interval's capital base counts only flows before it (a payout one day into a 7-day
        interval is NOT scaled up). Later activity (a restart) is new money and never looked at;
      * otherwise the last grid point at or before the hold end.
    Unscaled by default; scale=True (reported) multiplies the log return by hold / span for a vault
    that was not exhausted. A total loss ends at -100%.
    Returns {'gross', 'net', 'log', 'span_days', 'closed', 'exit_reason', 'entry_source', 'entry_t',
    'exit_t', 'intervals'} or {'unmeasurable': reason}."""
    end_ms = sel_ms + HOLD_DAYS * DAY if end_ms is None else end_ms
    pts, src = measure_points(grid, sel_ms, end_ms, snaps, entry_lo, entry_hi)
    if pts is None:
        return {'unmeasurable': src}
    if flows is not None:
        flows = [(t, f) for t, f in flows if sel_ms <= t <= end_ms]
    thr = exhaustion_threshold(equity_at_s)
    log, n, exit_t, reason = 0.0, 0, pts[-1][0], 'hold_end'
    for a, b in zip(pts, pts[1:]):
        if a[1] < BASE_FLOOR:
            if n == 0:
                return {'unmeasurable': 'thin_base_at_entry'}
            exit_t, reason = a[0], 'thin_base'
            break
        fl = None if flows is None else [(t, f) for t, f in flows if a[0] < t <= b[0]]
        outs = [t for t, f in (fl or []) if f < 0]
        exhausted = bool(outs) and b[1] <= thr
        t_end = max(outs) if exhausted else None
        r, _ = modified_dietz(a, b, fl, numerator, t_end)
        if r is None:
            if n == 0:
                return {'unmeasurable': 'capital_base'}
            exit_t, reason = a[0], 'capital_base'
            break
        if r <= -1:
            log, exit_t, reason = float('-inf'), b[0], 'total_loss'
            break
        log += math.log1p(r)
        n += 1
        if exhausted:
            exit_t, reason = t_end, 'exhausted'
            break
    span = (exit_t - pts[0][0]) / DAY
    if span <= 0:
        return {'unmeasurable': 'span'}
    closed = reason == 'exhausted'
    scaled = log * (HOLD_DAYS / span) if (scale and not closed and math.isfinite(log)) else log
    gross = max(math.expm1(scaled), -1.0) if math.isfinite(scaled) else -1.0
    net = max(net_of_profit_share(gross, profit_share) - exit_cost, -1.0)
    return {'gross': gross, 'net': net, 'log': log, 'span_days': span, 'closed': closed, 'exit_reason': reason,
            'entry_source': src, 'entry_t': pts[0][0], 'exit_t': exit_t, 'intervals': n}


def market_adjusted(net, beta, btc_entry, btc_exit):
    """Reported in discovery, the holdout CONFIRM outcome: net - beta x BTC's simple return over the
    vault's own measured span (beta = lookback net exposure, point-in-time)."""
    if net is None or beta is None or not btc_entry or not btc_exit:
        return None
    return net - beta * (btc_exit / btc_entry - 1)


def common_span(hlp_grid, sel_ms, entry_days=ENTRY_DAYS, end_ms=None):
    """(entry_t, exit_t) of HLP's first grid point in [S, S + entry_days] and its last at or before
    the hold end — the span criterion 3 (and the holdout, with 14 days) measures everyone on."""
    end_ms = sel_ms + HOLD_DAYS * DAY if end_ms is None else end_ms
    first = [t for t, _, _ in hlp_grid if sel_ms <= t <= sel_ms + entry_days * DAY]
    last = [t for t, _, _ in hlp_grid if sel_ms <= t <= end_ms]
    if not first or not last or last[-1] <= first[0]:
        return None
    return first[0], last[-1]


def net_of_profit_share(gross, share=PROFIT_SHARE):
    """The leader takes `share` of the depositor's positive profit; losses are not shared."""
    return gross - share * max(gross, 0.0)


def exit_cost(events, leader, sel_ms):
    """The depositor's exit cost: the median closingCost / requestedUsd over non-leader
    withdrawals in [S - 91 d, S) (point-in-time); 0 without any. Runner only (reads amounts)."""
    lo = sel_ms - LOOKBACK_DAYS * DAY
    xs = []
    for e in events:
        d = e['delta']
        if d.get('type') != 'vaultWithdraw' or not (lo <= e['time'] < sel_ms):
            continue
        if (d.get('user') or '').lower() in ('', leader):
            continue
        req, cc = _f(d.get('requestedUsd')), _f(d.get('closingCost'))
        if req and req > 0 and cc is not None:
            xs.append(max(cc, 0.0) / req)
    return statistics.median(xs) if xs else 0.0


class UnitVault:
    """Exact NAV-per-share bookkeeping (the synthetic truth the estimator is checked against).
    Equity E, units U, price p = E / U. A flow f buys / redeems f / p units at the current p; a
    distribution pays out a fraction of equity pro rata (units unchanged, price falls with E)."""

    def __init__(self, equity):
        self.e = float(equity)
        self.u = float(equity)
        self.pnl = 0.0
        self.closed = False
        self.paid = 0.0          # per-unit cash paid out by distributions (the depositor keeps it)

    @property
    def price(self):
        return self.e / self.u if self.u > 0 else None

    def trade(self, r):
        g = self.e * r
        self.e += g
        self.pnl += g

    def flow(self, f):
        p = self.price
        self.u += f / p
        self.e += f

    def distribute(self, frac=1.0):
        """Pay out `frac` of equity pro rata. A full one (frac 1) closes the vault. Returns the
        amount. A depositor's wealth per unit = price + cumulative paid per unit."""
        amt = self.e * frac
        self.paid += amt / self.u
        self.e -= amt
        if frac >= 1.0:
            self.e, self.closed = 0.0, True
        return amt


# ------------------------------------------------------------------ gates (runner, before any criterion)

def identity_checks(grid, events, vault, lo_ms, hi_ms, field):
    """Per grid interval in [lo, hi) with AV_a >= $1k: (has_flow, ok) where ok means
    |(AV_b - AV_a) - (pnl_b - pnl_a) - sum of ledger flows| <= GATE_TOL x AV_a, the withdrawal
    flow taken from `field`. Flow-free intervals test the pnl semantics; flow intervals test the
    withdrawal field (M4)."""
    flows = ledger_flows(events, vault, withdraw_fields=(field, 'usdc'))
    out = []
    for (ta, av_a, pa), (tb, av_b, pb) in zip(grid, grid[1:]):
        if ta < lo_ms or tb >= hi_ms or av_a < BASE_FLOOR:
            continue
        fl = [f for t, f in flows if ta < t <= tb]
        out.append((bool(fl), abs((av_b - av_a) - (pb - pa) - sum(fl)) <= GATE_TOL * av_a))
    return out


def identity_gate(checks_by_field):
    """checks_by_field: {field: [(has_flow, ok)]} pooled over vaults. The withdrawal field is the
    one with the higher flow-interval agreement (ties: requestedUsd). Pass if the flow-free share
    >= 95% and the chosen field's flow-interval share >= 90%. Returns (pass, field, shares)."""
    shares = {}
    for f, ch in checks_by_field.items():
        free = [ok for has, ok in ch if not has]
        flw = [ok for has, ok in ch if has]
        shares[f] = {'free': sum(free) / len(free) if free else None, 'n_free': len(free),
                     'flow': sum(flw) / len(flw) if flw else None, 'n_flow': len(flw)}
    order = [f for f in WITHDRAW_CANDIDATES if f in shares]
    best = max(order, key=lambda f: (shares[f]['flow'] or 0.0, -order.index(f)))
    s = shares[best]
    ok = (s['free'] is not None and s['free'] >= GATE_FREE_SHARE and s['flow'] is not None
          and s['flow'] >= GATE_FLOW_SHARE)
    return ok, best, shares


def commission_checks(events, leader, lo_ms, hi_ms, share=PROFIT_SHARE):
    """Per non-leader withdrawal in [lo, hi): ok if |commission - 10% x max(requested - basis, 0)|
    <= max($0.01, 0.5% of requested). Verifies D7 on lookback rows, inside the runner only."""
    out = []
    for e in events:
        d = e['delta']
        if d.get('type') != 'vaultWithdraw' or not (lo_ms <= e['time'] < hi_ms):
            continue
        if (d.get('user') or '').lower() in ('', leader):
            continue
        req, basis, com = _f(d.get('requestedUsd')), _f(d.get('basis')), _f(d.get('commission'))
        if req is None or basis is None or com is None:
            continue
        out.append(abs(com - share * max(req - basis, 0.0)) <= max(0.01, 0.005 * req))
    return out


def unmeasurable_gate(n_unmeasurable, n_total):
    """Pass if at most 5% of vault-windows are unmeasurable."""
    return n_total > 0 and n_unmeasurable / n_total <= UNMEASURABLE_MAX


def impute_unmeasurable(windows, keys=('net', 'gross'), mode='median'):
    """Rows whose outcome is None get the within-window median of the measured rows (neutral for
    the ranking and for top-minus-all; mode 'median', primary) or the window minimum (mode 'min',
    the reported worst case); flagged 'imputed'. Returns (windows, count)."""
    pick = statistics.median if mode == 'median' else min
    out, n = {}, 0
    for w, rows in windows.items():
        meds = {k: pick([r[k] for r in rows if r.get(k) is not None]) if any(r.get(k) is not None for r in rows)
                else None for k in keys}
        new = []
        for r in rows:
            if r.get(keys[0]) is None:
                n += 1
                new.append(dict(r, **{k: meds[k] for k in keys}, imputed=True))
            else:
                new.append(r)
        out[w] = new
    return out, n


def winsorise(windows, key='net', p=0.025):
    """Reported variant: outcomes clipped at the window's p and 1 - p quantiles."""
    out = {}
    for w, rows in windows.items():
        xs = sorted(r[key] for r in rows)
        if len(xs) < 5:
            out[w] = rows
            continue
        lo, hi = xs[int(p * (len(xs) - 1))], xs[int(math.ceil((1 - p) * (len(xs) - 1)))]
        out[w] = [dict(r, **{key: min(max(r[key], lo), hi)}) for r in rows]
    return out


# ------------------------------------------------------------------ criteria (registration §5)
# A window's rows: {'vault', 'cluster' (the leader), 'score', 'net', 'gross', 'control' (lookback
# net exposure), 'c3' (net return over HLP's in-hold interval, or None)}.

def top_quintile(rows):
    """The top fifth of a window's rows by score (n // 5, at least 1), ties by address."""
    srt = sorted(rows, key=lambda r: (-r['score'], r['vault']))
    return srt[:max(1, len(srt) // 5)]


def label_top(windows):
    """{(window, vault): True if in that window's top quintile}."""
    out = {}
    for w, rows in windows.items():
        top = {r['vault'] for r in top_quintile(rows)}
        for r in rows:
            out[(w, r['vault'])] = r['vault'] in top
    return out


def top_minus_all(windows, labels, drop=frozenset(), key='net'):
    """Mean over windows of (mean outcome of the top quintile - mean of all eligible), with the
    vaults in `drop` removed from both. Labels stay those of the full window."""
    diffs = []
    for w, rows in windows.items():
        use = [r for r in rows if r['vault'] not in drop]
        top = [r[key] for r in use if labels[(w, r['vault'])]]
        if top and use:
            diffs.append(statistics.fmean(top) - statistics.fmean(r[key] for r in use))
    return statistics.fmean(diffs) if diffs else None


def contributions(windows, labels, key='net'):
    """Each vault's contribution to top_minus_all (it is linear in the outcomes)."""
    W = len(windows)
    out = {}
    for w, rows in windows.items():
        n_all = len(rows)
        n_top = sum(1 for r in rows if labels[(w, r['vault'])])
        for r in rows:
            c = -r[key] / n_all + (r[key] / n_top if labels[(w, r['vault'])] else 0.0)
            out[r['vault']] = out.get(r['vault'], 0.0) + c / W
    return out


def largest_positive_vault(windows, labels, key='net'):
    """The vault with the largest positive contribution; None if none is positive."""
    c = contributions(windows, labels, key)
    best = max(c.items(), key=lambda kv: (kv[1], kv[0]), default=(None, 0.0))
    return best[0] if best[1] > 0 else None


def _cluster(r):
    return r.get('cluster') or r['vault']


def clustered_bootstrap_se(windows, labels, n_boot=1000, seed=0, key='net'):
    """Leader-clustered bootstrap SE of top_minus_all: leaders are resampled with replacement and
    all their vaults' rows in every window move together (Revision 1: by leader, not vault)."""
    by_c = {}
    for w, rows in windows.items():
        for r in rows:
            by_c.setdefault(_cluster(r), []).append((w, r))
    cl = sorted(by_c)
    rng = random.Random(seed)
    vals = []
    for _ in range(n_boot):
        per_w = {}
        for _k in cl:
            for w, r in by_c[cl[rng.randrange(len(cl))]]:
                per_w.setdefault(w, []).append(r)
        diffs = []
        for w, rows in per_w.items():
            top = [r[key] for r in rows if labels[(w, r['vault'])]]
            if top:
                diffs.append(statistics.fmean(top) - statistics.fmean(r[key] for r in rows))
        if diffs:
            vals.append(statistics.fmean(diffs))
    return statistics.stdev(vals) if len(vals) > 2 else None


def analytic_clustered_se(windows, labels, key='net'):
    """Leader-clustered (sandwich) SE of top_minus_all: the statistic is sum of c_r·y_r;
    var = sum over leaders of (sum of their c_r·(y_r - window mean))^2."""
    W = len(windows)
    per_c = {}
    for w, rows in windows.items():
        n_all = len(rows)
        n_top = sum(1 for r in rows if labels[(w, r['vault'])])
        m = statistics.fmean(r[key] for r in rows)
        for r in rows:
            c = (-1.0 / n_all + (1.0 / n_top if labels[(w, r['vault'])] else 0.0)) / W
            per_c[_cluster(r)] = per_c.get(_cluster(r), 0.0) + c * (r[key] - m)
    v = sum(x * x for x in per_c.values())
    return math.sqrt(v) if v > 0 else None


def window_ic(rows, control='control', key='net'):
    """Tercile-weighted Spearman IC, terciles of the lookback control (net exposure primary)."""
    return stats.tercile_ic([(r['score'], r[key], r[control]) for r in rows])


def window_leader_z(windows, n_boot=400, seed=0, control='control', key='net', mode='tau'):
    """Window-and-leader clustered Z for criterion 1. Each bootstrap draw resamples LEADERS with
    replacement (a leader's vaults in every window move together) and recomputes every window's
    Fisher z_w; sd_leader = SD of Z_b = sum z_w / sqrt(W). The window cluster enters as
    between-window heterogeneity:
      mode 'tau' (primary): tau² = max(0, sample variance of the observed z_w - mean bootstrap
        variance of z_w) — the spread of window results beyond sampling noise, which is where a
        market shock common to a window shows up (random-effects form);
        sd = sqrt(sd_leader² + tau²);
      mode 'resample' (reported): also resample the W windows with replacement inside each draw.
        With W = 4 this counts the sampling variance twice (leader draw + window draw), so Z is
        deflated by about 1.3 even with no common shock (registration §3).
    Returns {'z', 'sd', 'z_clustered', 'tau2', 'sd_leader'}."""
    ws = list(windows)
    W = len(ws)
    ics = [window_ic(windows[w], control, key) for w in ws]
    z = stats.combine(ics)
    z_obs = [stats.fisher_z(ic, n) if ic is not None and n > 3 else 0.0 for ic, n in ics]
    by_c = {}
    for w in ws:
        for r in windows[w]:
            by_c.setdefault(_cluster(r), {}).setdefault(w, []).append(r)
    cl = sorted(by_c)
    rng = random.Random(seed)
    zs, per_w = [], [[] for _ in ws]
    for _ in range(n_boot):
        draw = [cl[rng.randrange(len(cl))] for _ in cl]
        zw = []
        for i, w in enumerate(ws):
            rows = [r for c in draw for r in by_c[c].get(w, [])]
            ic, n = window_ic(rows, control, key)
            zw.append(stats.fisher_z(ic, n) if ic is not None and n > 3 else 0.0)
            per_w[i].append(zw[-1])
        if mode == 'resample':
            zw = [zw[rng.randrange(W)] for _ in range(W)]
        zs.append(sum(zw) / math.sqrt(W))
    if len(zs) < 3 or z is None:
        return {'z': z, 'sd': None, 'z_clustered': None, 'tau2': None, 'sd_leader': None}
    sd_leader = statistics.stdev(zs)
    tau2 = 0.0
    if mode == 'tau' and W > 1:
        within = statistics.fmean(statistics.pvariance(v) for v in per_w)
        tau2 = max(0.0, statistics.variance(z_obs) - within)
    sd = math.sqrt(sd_leader ** 2 + tau2)
    return {'z': z, 'sd': sd, 'z_clustered': z / sd if sd else None, 'tau2': tau2, 'sd_leader': sd_leader,
            'mode': mode, 'n_boot': len(zs)}


T_CRIT_975 = {2: 12.706, 3: 4.303, 4: 3.182, 5: 2.776, 6: 2.571, 7: 2.447}   # one-sided 0.975, df = W - 1


def window_t(ics):
    """t statistic of the window-level IC values: mean / (sd / sqrt(W)) (Revision 2, M-a: exact
    under any between-window heterogeneity, unlike a normal threshold on a random-effects Z)."""
    v = [ic for ic, _ in ics if ic is not None]
    if len(v) < 2:
        return None
    sd = statistics.stdev(v)
    return statistics.fmean(v) / (sd / math.sqrt(len(v))) if sd > 0 else (math.inf if statistics.fmean(v) > 0 else -math.inf)


def summarise(windows, hlp, z_crit=Z_CRIT, min_positive=MIN_POSITIVE_WINDOWS, n_boot=1000, z_boot=0,
              placebo_seeds=200, key='net', control='control', version='a', use_c3=True, t_crit=None):
    """The five criteria (registration §5) and what is reported with them (Revision 2).

    Criterion 1 needs ALL of: the tercile-weighted Fisher Z >= 2.33; the t statistic of the W
    window ICs >= t_{W-1}(0.975) (3.182 for four windows); IC > 0 in >= 3 of 4. The random-effects
    window-and-leader Z is REPORTED only (z_boot > 0). Criterion 2: version 'a' D > 0; version 'b'
    D / SE >= 1.64 (SE leader-clustered; bootstrap if n_boot else analytic). Criterion 3 compares
    the top quintile's c3 (net return over HLP's span) with HLP; it FAILS if more than 10% of the
    top quintile's c3 values are missing; use_c3=False (outcome variants) reports it as n/a and the
    pass then rests on criteria 1, 2, 4, 5."""
    ics = {w: window_ic(rows, control, key) for w, rows in windows.items()}
    z = stats.combine(list(ics.values()))
    pos = sum(1 for ic, _ in ics.values() if ic is not None and ic > 0)
    tw = window_t(list(ics.values()))
    t_crit = t_crit if t_crit is not None else T_CRIT_975.get(len(windows), 3.182)
    wl = window_leader_z(windows, z_boot, control=control, key=key) if z_boot else None
    labels = label_top(windows)
    d = top_minus_all(windows, labels, key=key)
    se = (clustered_bootstrap_se(windows, labels, n_boot=n_boot, key=key) if n_boot
          else analytic_clustered_se(windows, labels, key=key))
    top_rows = [r for w, rows in windows.items() for r in rows if labels[(w, r['vault'])]]
    top_mean = statistics.fmean(r[key] for r in top_rows) if top_rows else None
    top_median = statistics.median(r[key] for r in top_rows) if top_rows else None
    c3_missing = sum(1 for r in top_rows if r.get('c3') is None) / len(top_rows) if top_rows else 1.0
    vs_hlp = []
    for w, rows in windows.items():
        tops = [r['c3'] for r in rows if labels[(w, r['vault'])] and r.get('c3') is not None]
        if hlp.get(w) is not None and tops:
            vs_hlp.append(statistics.fmean(tops) - hlp[w])
    vs_hlp_mean = statistics.fmean(vs_hlp) if len(vs_hlp) == len(windows) else None
    big = largest_positive_vault(windows, labels, key)
    d_wo = top_minus_all(windows, labels, drop=frozenset({big}), key=key) if big else d
    c1_plain = z is not None and z >= z_crit and pos >= min_positive
    c3 = (vs_hlp_mean is not None and vs_hlp_mean > 0 and c3_missing <= C3_MISSING_MAX) if use_c3 else None
    crit = {
        '1_ic': c1_plain and tw is not None and tw >= t_crit,
        '2a_top_minus_all_positive': d is not None and d > 0,
        '2b_top_minus_all_d_se': d is not None and se is not None and d / se >= D_SE_Z,
        '3_top_beats_hlp_same_interval': c3,
        '4_top_mean_median_same_sign': top_mean is not None and top_median is not None
                                       and (top_mean > 0) == (top_median > 0),
        '5_without_largest_positive_vault': d_wo is not None and d_wo > 0,
    }
    rest = crit['1_ic'] and crit['4_top_mean_median_same_sign'] and crit['5_without_largest_positive_vault'] \
        and (c3 is None or c3)
    pass_a = rest and crit['2a_top_minus_all_positive']
    pass_b = rest and crit['2b_top_minus_all_d_se']
    return {'ic_by_window': {str(w): {'ic': ic, 'n': n} for w, (ic, n) in ics.items()}, 'z': z, 'window_t': tw,
            't_crit': t_crit, 'positive_windows': pos, 'window_leader_z_reported': wl,
            'criterion1_plain_only': c1_plain, 'top_minus_all': d, 'top_minus_all_se': se,
            'top_minus_all_z': (d / se) if (d is not None and se) else None,
            'top_mean': top_mean, 'top_median': top_median, 'top_n': len(top_rows), 'c3_missing_share': c3_missing,
            'top_minus_hlp_by_window': vs_hlp, 'top_minus_hlp': vs_hlp_mean,
            'largest_positive_vault': big, 'top_minus_all_without_it': d_wo,
            'criteria': crit, 'pass_a': pass_a, 'pass_b': pass_b, 'version': version,
            'pass': pass_a if version == 'a' else pass_b,
            'placebo': placebo(windows, hlp, placebo_seeds, z_crit, min_positive, key, control) if placebo_seeds else None}


def placebo(windows, hlp, seeds=200, z_crit=Z_CRIT, min_positive=MIN_POSITIVE_WINDOWS, key='net', control='control'):
    """Scores shuffled within each window (seed s), everything else kept. Reports the share of
    seeds passing criterion 1, passing all five under each version (analytic SE), and with
    |Z| >= z_crit (symmetric, ~2%)."""
    c1 = all_a = all_b = absz = 0
    for s in range(seeds):
        rng = random.Random(s)
        sh = {}
        for w, rows in windows.items():
            scores = [r['score'] for r in rows]
            rng.shuffle(scores)
            sh[w] = [dict(r, score=sc) for r, sc in zip(rows, scores)]
        res = summarise(sh, hlp, z_crit, min_positive, n_boot=0, z_boot=0, placebo_seeds=0, key=key, control=control)
        c1 += res['criteria']['1_ic']
        all_a += res['pass_a']
        all_b += res['pass_b']
        absz += res['z'] is not None and abs(res['z']) >= z_crit
    return {'seeds': seeds, 'criterion1_share': c1 / seeds, 'all_a_share': all_a / seeds, 'all_b_share': all_b / seeds,
            'abs_z_share': absz / seeds}


def decide(summary):
    """Registration §9: a discovery pass (under the version the owner chose, D9) -> ask the owner
    for the holdout read (D12); otherwise the light closes at the stated power (D13)."""
    return 'pull_holdout' if summary['pass'] else 'close'


def holdout_confirm(windows, hlp, z=Z_HOLDOUT, key='adj', n_boot=1000, control='control'):
    """The holdout read (D14, Revision 2). On the MARKET-ADJUSTED outcome (`adj`): combined Fisher Z
    >= 1.64; leader-clustered D / SE >= 1.64; the sign rule (mean window IC > 0 and D > 0); and
    the top quintile beats HLP over the same common span (raw net `c3`, missing share <= 10%)."""
    res = summarise(windows, hlp, z_crit=z, min_positive=0, n_boot=n_boot, z_boot=0, placebo_seeds=0, key=key,
                    control=control, t_crit=-math.inf)
    ics = [v['ic'] for v in res['ic_by_window'].values() if v['ic'] is not None]
    crit = {'z': res['z'] is not None and res['z'] >= z,
            'd_se': res['top_minus_all_z'] is not None and res['top_minus_all_z'] >= D_SE_Z,
            'sign': bool(ics) and statistics.fmean(ics) > 0 and res['top_minus_all'] is not None and res['top_minus_all'] > 0,
            'top_beats_hlp': bool(res['criteria']['3_top_beats_hlp_same_interval'])}
    return all(crit.values()), {**res, 'holdout_criteria': crit}


# ------------------------------------------------------------------ power (behaviour-only inputs)

def calibrate_skill(ic, rho_score, n=20000, seed=1, sd=None, beta=None, sigma_m=0.0):
    """The skill loading `a` that plants a population Spearman IC of `ic` between the score and the
    TRUE hold return (market factor included as beta x an independent market draw), given the
    score's reliability rho_score. Bisection on a large simulated cross-section."""
    rng = random.Random(seed)
    zs = [rng.gauss(0, 1) for _ in range(n)]
    es = [rng.gauss(0, 1) for _ in range(n)]
    ns = [rng.gauss(0, 1) for _ in range(n)]
    sds = [sd[rng.randrange(len(sd))] if sd else 1.0 for _ in range(n)]
    fac = [(beta[rng.randrange(len(beta))] if beta else 0.0) * sigma_m * rng.gauss(0, 1) for _ in range(n)]
    score = [math.sqrt(rho_score) * z + math.sqrt(1 - rho_score) * e for z, e in zip(zs, es)]

    def ic_at(a):
        ret = [math.expm1(f + s * (a * z + math.sqrt(max(0.0, 1 - a * a)) * x)) for z, x, s, f in zip(zs, ns, sds, fac)]
        return stats.spearman(score, ret)
    if ic <= 0:
        return 0.0
    lo, hi = 0.0, 0.999
    for _ in range(40):
        mid = (lo + hi) / 2
        if ic_at(mid) < ic:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def draw_windows(rng, windows, sd, beta, leader, a, rho_score, Lb, Lh, c_beta=0.0, sigma_m=0.148, mean_excess=0.0,
                 attenuation=ATTENUATION, bstd=None, control='beta', market=None, shock_sd=0.0, beta_noise=0.5):
    """One simulated discovery (or holdout) data set with real vault identities.

    skill z_v ~ N(0,1) persistent; score = sqrt(1-c²)·(sqrt(rho)·z + sqrt(1-rho)·e_w) + c·β̃_v
    (c = corr(score, net exposure)); one market move per window, m_w ~ N(0, sigma_m) or fixed
    (`market`); an optional window-common IC shock a_w ~ N(0, shock_sd) (the heterogeneity null:
    x += a_w·sd_v·scorẽ); true log return x = mean_excess - var/2 + β_v·m_w + sd_v·(a·z + sqrt(1-a²)·n_w)
    (+ shock); measured = true + noise with Spearman(measured, true) ~ attenuation.
    Rows carry net, gross, c3 (= net), control (β_v, or random), and adj = net - β̂_v·(e^m - 1), the
    market-adjusted outcome with an estimated beta β̂ = β + N(0, beta_noise·sd(β))."""
    vaults = sorted({v for w in windows for v in w})
    bs = statistics.pstdev([beta[v] for v in vaults]) or 1.0
    if bstd is None:
        bm = statistics.fmean(beta[v] for v in vaults)
        bstd = {v: (beta[v] - bm) / bs for v in vaults}
    z = {v: rng.gauss(0, 1) for v in vaults}
    e = {v: mvn(Lb, rng) for v in vaults}
    n = {v: mvn(Lh, rng) for v in vaults}
    bhat = {v: beta[v] + rng.gauss(0, beta_noise * bs) for v in vaults}
    k_noise = math.sqrt(max(0.0, 1 / attenuation ** 2 - 1)) if attenuation < 1 else 0.0
    ctrl = {v: (beta[v] if control == 'beta' else rng.random()) for v in vaults}
    out = {}
    for wi, ids in enumerate(windows):
        m = rng.gauss(0, sigma_m) if market is None else market[wi]
        shock = rng.gauss(0, shock_sd) if shock_sd else 0.0
        rows = []
        for v in ids:
            sc = math.sqrt(1 - c_beta ** 2) * (math.sqrt(rho_score) * z[v] + math.sqrt(1 - rho_score) * e[v][wi]) \
                + c_beta * bstd[v]
            var = sd[v] ** 2 + (beta[v] * sigma_m) ** 2
            x = mean_excess - var / 2 + beta[v] * m + sd[v] * (a * z[v] + math.sqrt(max(0.0, 1 - a * a)) * n[v][wi])
            x += shock * sd[v] * sc
            x += k_noise * math.sqrt(var) * rng.gauss(0, 1)
            g = max(math.expm1(x), -1.0)
            net = net_of_profit_share(g)
            rows.append({'vault': v, 'cluster': leader.get(v, v), 'score': sc, 'net': net, 'gross': g,
                         'control': ctrl[v], 'c3': net, 'adj': net - bhat[v] * math.expm1(m)})
        out[wi] = rows
    return out


def simulate_power(windows, sd, ic, beta=None, leader=None, rho_score=0.42, sims=2000, seed=0, z_crit=Z_CRIT,
                   min_positive=None, lb_overlap=None, hold_overlap=None, mean_excess=0.0, hlp=0.0,
                   c_beta=0.0, sigma_m=0.148, attenuation=ATTENUATION, control='beta', market=None, shock_sd=0.0,
                   key='net'):
    """Joint power of the five criteria at a planted IC (Revision 2: criterion 1 = Fisher Z + window
    t-test + 3 of 4; market factor; NAV attenuation; leader clusters; analytic leader-clustered SE).

    Returns pass shares: '1_plain' (Fisher Z and window count only), '1' (with the t-test), '2a',
    '2b', '3', '4', '5', 'all_a', 'all_b', 'all_a_plain1'."""
    W = len(windows)
    min_positive = min_positive if min_positive is not None else math.ceil(0.75 * W)
    vaults = sorted({v for w in windows for v in w})
    beta = beta or {v: 0.0 for v in vaults}
    leader = leader or {}
    a = calibrate_skill(ic, rho_score, sd=[sd[v] for v in vaults], beta=[beta[v] for v in vaults], sigma_m=sigma_m) if ic > 0 else 0.0
    rng = random.Random(seed)
    Lb = cholesky(lb_overlap or [[1.0 if i == j else 0.0 for j in range(W)] for i in range(W)])
    Lh = cholesky(hold_overlap or [[1.0 if i == j else 0.0 for j in range(W)] for i in range(W)])
    keys = ('1_plain', '1', '2a', '2b', '3', '4', '5', 'all_a', 'all_b', 'all_a_plain1')
    counts = dict.fromkeys(keys, 0)
    for _ in range(sims):
        win = draw_windows(rng, windows, sd, beta, leader, a, rho_score, Lb, Lh, c_beta, sigma_m, mean_excess, attenuation,
                           control=control, market=market, shock_sd=shock_sd)
        res = summarise(win, {w: hlp for w in win}, z_crit=z_crit, min_positive=min_positive, n_boot=0,
                        z_boot=0, placebo_seeds=0, key=key)
        c = res['criteria']
        rest = c['3_top_beats_hlp_same_interval'] and c['4_top_mean_median_same_sign'] and c['5_without_largest_positive_vault']
        f = {'1_plain': res['criterion1_plain_only'], '1': c['1_ic'], '2a': c['2a_top_minus_all_positive'],
             '2b': c['2b_top_minus_all_d_se'], '3': c['3_top_beats_hlp_same_interval'],
             '4': c['4_top_mean_median_same_sign'], '5': c['5_without_largest_positive_vault']}
        f['all_a'] = f['1'] and f['2a'] and rest
        f['all_b'] = f['1'] and f['2b'] and rest
        f['all_a_plain1'] = f['1_plain'] and f['2a'] and rest
        for k in keys:
            counts[k] += bool(f[k])
    out = {k: v / sims for k, v in counts.items()}
    out['skill_loading'] = a
    out['sims'] = sims
    return out


def holdout_power(windows, sd, ic, beta=None, leader=None, rho_score=0.42, sims=5000, seed=11, lb_overlap=None,
                  sigma_m=0.148, attenuation=ATTENUATION, c_beta=0.0, market=None, key='adj'):
    """P(CONFIRM) under D14 (Revision 2): market-adjusted outcome, Z >= 1.64, D / SE >= 1.64, the
    sign rule, top beats HLP over the common span. `market` fixes the per-window market moves
    (the one-regime, exposure-tilted null)."""
    W = len(windows)
    vaults = sorted({v for w in windows for v in w})
    beta = beta or {v: 0.0 for v in vaults}
    leader = leader or {}
    a = calibrate_skill(ic, rho_score, sd=[sd[v] for v in vaults], beta=[beta[v] for v in vaults], sigma_m=sigma_m) if ic > 0 else 0.0
    rng = random.Random(seed)
    Lb = cholesky(lb_overlap or [[1.0 if i == j else 0.0 for j in range(W)] for i in range(W)])
    Lh = cholesky([[1.0 if i == j else 0.0 for j in range(W)] for i in range(W)])
    hits = 0
    for _ in range(sims):
        win = draw_windows(rng, windows, sd, beta, leader, a, rho_score, Lb, Lh, c_beta, sigma_m, 0.0, attenuation,
                           market=market)
        ok, _ = holdout_confirm(win, {w: 0.0 for w in win}, n_boot=0, key=key)
        hits += ok
    return hits / sims


def binom_upper(k, n, z=1.96):
    """Wilson 95% upper bound of a share k / n (reported next to every null rate)."""
    if n == 0:
        return None
    p = k / n
    den = 1 + z * z / n
    return (p + z * z / (2 * n) + z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / den


def cholesky(m):
    n = len(m)
    low = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i + 1):
            s = sum(low[i][k] * low[j][k] for k in range(j))
            if i == j:
                low[i][j] = math.sqrt(max(m[i][i] - s, 0.0))
            else:
                low[i][j] = (m[i][j] - s) / low[j][j] if low[j][j] > 0 else 0.0
    return low


def mvn(low, rng):
    g = [rng.gauss(0, 1) for _ in range(len(low))]
    return [sum(low[i][k] * g[k] for k in range(i + 1)) for i in range(len(low))]


def overlap_matrix(selections, days):
    """Share of `days` (lookback 91 or hold 56) shared by each pair of windows: shared days / days.
    Lookbacks end at S, holds start at S."""
    return [[max(0, days - abs((b - a).days)) / days for b in selections] for a in selections]


# ------------------------------------------------------------------ BTC closes, filtered at the cutoff in the query

class Btc:
    """BTC 1H closes for the cross-regime score, the reported betas and regime labels. Replaces
    ct/regimes.Regimes for 007 because btc_1h.parquet holds rows after the cutoff (review minor 3):
    the query keeps only candles that CLOSE at or before min(end_ms, cutoff). Same rules as
    Regimes: trend = trailing 30-day return from fully closed candles strictly before t."""

    def __init__(self, path=BTC_1H, end_ms=CUTOFF_MS):
        import duckdb
        c = duckdb.connect()
        end = min(end_ms, CUTOFF_MS)
        rows = c.execute(f"SELECT ts, close FROM read_parquet('{path}') WHERE ts + {HOUR} <= {end} ORDER BY ts").fetchall()
        self.ts = [r[0] for r in rows]
        self.close = [r[1] for r in rows]
        self.end = end

    def close_before(self, t_ms):
        import bisect
        i = bisect.bisect_left(self.ts, t_ms) - 1
        while i >= 0 and self.ts[i] + HOUR > t_ms:
            i -= 1
        return self.close[i] if i >= 0 else None

    def trend_at(self, t_ms):
        now, then = self.close_before(t_ms), self.close_before(t_ms - 30 * DAY)
        return now / then - 1 if now and then else None

    def is_rising(self, t_ms):
        r = self.trend_at(t_ms)
        return None if r is None else r >= 0

    def month_label(self, month):
        """'UP' / 'DOWN' / 'FLAT' for 'YYYY-MM' (±8%) — descriptive; None past the read bound."""
        y, m = map(int, month.split('-'))
        start = int(dt.datetime(y, m, 1, tzinfo=dt.UTC).timestamp() * 1000)
        end = int(dt.datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=dt.UTC).timestamp() * 1000)
        if end > self.end:
            return None
        a, b = self.close_before(start + HOUR), self.close_before(end)
        if not a or not b:
            return None
        r = b / a - 1
        return 'UP' if r > 0.08 else 'DOWN' if r < -0.08 else 'FLAT'


# ------------------------------------------------------------------ the guard (adapted from l7_run)

HASH_ROW = re.compile(r'^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|', re.M)


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def _rows_for(text, name):
    for line in text.splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get('registration') == name:
            return True
    return False


def guard_failures(root, reg_rel, code, data, results_rel='results.jsonl', manifest_rel=None):
    """Every reason the run must not start; ([], commit) when it may. Adapted from
    l7_run.guard_failures (registration 005; 005's file untouched). Checks: status REGISTERED;
    (a) the registration is tracked; (b) unmodified vs HEAD; (c) it has a commit; (d) the guarded
    code is clean in git status; (e) code and data match the SHA-256 table in the registration;
    (f) results.jsonl holds no row of this registration — in the working copy AND anywhere in its
    committed history (git log -p; Revision 2: deleting the row, locally or in a later commit,
    does not re-enable the run); (g) the manifest's digest equals the one on the registration's
    "**Data manifest:**" line exactly."""
    root = Path(root)
    reg = root / reg_rel
    git = lambda *a: subprocess.run(['git', *a], cwd=root, capture_output=True, text=True)  # noqa: E731
    out = []
    text = reg.read_text() if reg.exists() else ''
    m = re.search(r'^\*\*Status: (\w+)', text, re.M)
    if not m or m.group(1) != 'REGISTERED':
        out.append(f'{reg.name} is not REGISTERED — refusing to compute any score or outcome.')
    if git('ls-files', '--error-unmatch', '--', reg_rel).returncode:
        out.append(f'(a) {reg.name} is not tracked by git')
    if git('diff', '--quiet', 'HEAD', '--', reg_rel).returncode:
        out.append(f'(b) {reg.name} differs from the committed version — commit (as an amendment) first.')
    commit = git('log', '-1', '--format=%H', '--', reg_rel).stdout.strip()
    if not commit:
        out.append(f'(c) {reg.name} has no commit')
    dirty = git('status', '--porcelain', '--', *code).stdout.strip()
    if dirty:
        out.append(f'(d) guarded code not clean: {dirty.splitlines()}')
    table = dict(HASH_ROW.findall(text))
    for f in (*code, *data):
        want = table.get(f)
        if want is None:
            out.append(f'(e) {f} missing from the registration hash table')
        elif not (root / f).exists() or sha256(root / f) != want:
            out.append(f'(e) {f} does not match its registered SHA-256')
    res = root / results_rel
    if res.exists() and _rows_for(res.read_text(), reg.name):
        out.append(f'(f) {results_rel} already holds a row for {reg.name} — the trial runs once')
    hist = git('log', '-p', '--format=', '--', results_rel)
    if hist.returncode == 0 and any(_rows_for(line[1:], reg.name) for line in hist.stdout.splitlines()
                                    if line.startswith(('+', '-')) and not line.startswith(('+++', '---'))):
        out.append(f'(f) the committed history of {results_rel} holds a row for {reg.name} — the trial runs once')
    if manifest_rel is not None:
        mp = root / manifest_rel
        digest = json.loads(mp.read_text()).get('digest') if mp.exists() else None
        m2 = re.search(r'^\*\*Data manifest:\*\*[^\n]*?digest `([0-9a-f]{64})`', text, re.M)
        if not digest or not m2 or m2.group(1) != digest:
            out.append(f'(g) the manifest digest differs from the one on the "Data manifest" line of {reg.name}')
    return out, commit


def manifest_rows(paths, root):
    """{relative path: sha256} for the pinned files, sorted; and one digest over all of them."""
    rows = {str(Path(p).relative_to(root)): sha256(p) for p in sorted(paths)}
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()
    return rows, digest


def write_manifest(dest, paths, root, extra=None):
    """Write the data manifest ONCE. Refuses if `dest` exists (Revision 1, minor 6): a pinned
    manifest is never regenerated; a change is an amendment with a new file name."""
    dest = Path(dest)
    if dest.exists():
        raise SystemExit(f'{dest} exists — the manifest is written once and never overwritten')
    rows, digest = manifest_rows(paths, root)
    out = {'files': rows, 'digest': digest, 'n_files': len(rows), **(extra or {})}
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix('.tmp')
    tmp.write_text(json.dumps(out, indent=1, sort_keys=True))
    tmp.replace(dest)
    return digest
