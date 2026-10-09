"""Registration 001 (revision 4) — the run. REFUSES to start unless the
registration is committed with status REGISTERED and unmodified since
(ledger invariant: no evaluation without a registration written first).

    .venv/bin/python p5_run.py            # T1, T2, T2b, T3, T4 on the discovery period

Order inside each window, so a broken pipeline never prints a statistic:
1. mechanics gates on a sample of slow wallets — leader closedPnl self-check
   and the costless-follower identity; abort on failure;
2. scores and outcomes; 3. statistics, pass flags, one line appended to
results.jsonl citing this file's commit. Every number carries the family's
trial count (5) and its critical z (2.33).
"""
import datetime as dt
import hashlib
import json
import re
import statistics
import subprocess
import sys
from pathlib import Path

from ct import crowd, crowding, replay, schedule, scores, stats, targets
from ct import bitget as bg
from ct.leaders import Equity, average_beta, con, iter_fills
from ct.net import CACHE, DATA
from ct.regimes import Regimes
from ct.venue import BitgetVenue

HERE = Path(__file__).resolve().parent
REG = HERE / 'registrations/001-slow-traders-copy-and-crowd-divergence.md'
RESULTS = HERE / 'results.jsonl'
DEX = 'hyperliquid'
MINUTE, HOUR, DAY = 60_000, 3_600_000, 86_400_000
TRIALS = 5
Z_CRIT = 2.33                         # one-sided alpha 0.05 / 5 (D3, D11)
LAGS_SLOW = [MINUTE, 10 * MINUTE, HOUR, 6 * HOUR, 24 * HOUR, 72 * HOUR]
LAGS_DAY = [MINUTE, 10 * MINUTE, HOUR, 6 * HOUR, 24 * HOUR]
T2 = dict(key='slow_both_regimes', poll=60, lags=LAGS_SLOW, primary=HOUR, mechanism=24 * HOUR,
          robust_polls=[(10, 10 * MINUTE), (1, MINUTE)])
T2B = dict(key='day_both_regimes', poll=10, lags=LAGS_DAY, primary=10 * MINUTE, mechanism=HOUR,
           robust_polls=[(1, MINUTE)])
PARAMS = dict(capital_usd=10_000.0, max_leverage=3.0, band=0.25, slip_range_frac=0.10, maintenance_margin=0.01)
PAPER_USD = 1_000.0                   # D15
B_COST = 0.0006 + 0.0005              # Bitget taker + flat 5 bp slippage, one way (registration §8)
B_HORIZON_H = 72
RETAIL_MAX_VALUE = 1_000.0            # D13 reported crowd


def guard():
    text = REG.read_text()
    m = re.search(r'^\*\*Status: (\w+)', text, re.M)
    if not m or m.group(1) != 'REGISTERED':
        sys.exit(f'{REG.name} is not REGISTERED — refusing to compute any score or outcome.')
    dirty = subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--', str(REG)], cwd=HERE).returncode
    if dirty:
        sys.exit(f'{REG.name} differs from the committed version — commit (as an amendment) first.')
    return subprocess.run(['git', 'log', '-1', '--format=%H', '--', str(REG)], cwd=HERE,
                          capture_output=True, text=True).stdout.strip()


def symtab():
    table = json.loads((DATA / 'p0/2026-10-07/symbols.json').read_text())
    return {c: r for c, r in table.items() if r['status'] == 'ok'}


def funding_proxy(table):
    """Hyperliquid funding per Bitget symbol, from p4_prefetch's cache (must exist)."""
    import p4_prefetch as pf
    out = {}
    for coin, r in table.items():
        key = f'{coin}:{pf.ms(pf.FUNDING_START)}:{pf.ms(pf.END)}'
        path = CACHE / 'hl-funding' / f'{hashlib.sha256(key.encode()).hexdigest()[:24]}.json'
        if not path.exists():
            sys.exit(f'funding history missing for {coin} — run p4_prefetch.py funding first')
        out[r['symbol']] = [tuple(x) for x in json.loads(path.read_text())]
    return out


def mechanics_gate(sample, venue, end_ms):
    """Abort unless replay and follower reproduce the leader on a sample."""
    agree = total = 0
    gaps = []
    for fills, events, equity in sample:
        a, t, _ = replay.closed_pnl_check(fills, events, 10_000 / equity, end_ms)
        agree, total = agree + a, total + t
        if events:
            base = replay.leader_pnl(events, 10_000 / equity, end_ms)
            book = targets.Copy([targets.LeaderBook(events, lambda _t, e=equity: e, 1.0)])
            res = replay.follow(book, replay.Params(1, 10_000.0, band=0.0, max_leverage=1e9, frictionless=True),
                                venue, end_ms)
            if res and abs(base) > 50:
                gaps.append(abs(res['net_usd'] - base) / abs(base))
    ok_pnl = total == 0 or agree / total >= 0.98
    ok_copy = not gaps or statistics.median(gaps) <= 0.05
    print(f'    mechanics: closedPnl {agree}/{total}, costless follower median gap '
          f'{statistics.median(gaps) if gaps else 0:.1%} over {len(gaps)} wallets')
    if not (ok_pnl and ok_copy):
        sys.exit('mechanics gate failed — no statistic computed')


def outcome(res, venue, sel_ms):
    """Per-copy outcomes (registration §6): alpha, static-hedged, unhedged daily means; trips."""
    if res is None:
        return {'alpha': 0.0, 'static': 0.0, 'unhedged': 0.0, 'r': None, 'active': False, 'res': None, 'r_trips': []}
    rets = res['daily']['returns']
    bench = replay.benchmark_daily_returns(venue, [t for t, _ in rets], sel_ms)
    a = stats.alpha([r for _, r in rets], [r for _, r in bench])
    return {'alpha': a if a is not None else res['daily']['mean'],
            'static': res['hedged']['daily']['mean'] if res.get('hedged') else res['daily']['mean'],
            'unhedged': res['daily']['mean'], 'r': res['r'], 'active': res['orders'] > 0, 'res': res,
            'r_trips': [{'r_atr': t['r_atr'], 'net_usd': t['net_usd'], 'risk_usd': t['risk_usd']} for t in res['trips']]}


def ic_rows(wallets, lag, field='alpha', active_only=False):
    rows = [(w['score'], w['outcome'][lag][field], w['bias']) for w in wallets
            if not active_only or w['outcome'][lag]['active']]
    return rows


def score_wallet(cfg, a, lb, hold, eq, table, listed, venue, reg, sel_ms, lb_ms, floor):
    """Score one wallet for T2 / T2b; None if it has no score, equity or typical leverage."""
    trips = scores.round_trips(lb)
    sc = scores.cross_regime_score(trips, reg.is_rising, min_trips=floor)
    bias = scores.long_bias(lb)
    equity_s, _ = eq.at(a, sel_ms)
    lev = eq.typical_leverage(a, lb_ms, sel_ms)
    if sc is None or bias is None or not equity_s or not lev:
        return None
    events, _ = replay.leader_events(hold, table, listed)
    beta_l, beta_days = average_beta(lb, eq.equity_fn(a), venue, table, lb_ms, sel_ms)
    return {'address': a, 'score': sc, 'plain': scores.plain_score(trips), 'bias': bias, 'events': events,
            'lev': lev, 'equity': equity_s, 'fills': hold, 'static_beta': (beta_l or 0.0) / lev,
            'beta_days': beta_days, 'outcome': {}}


def run_copies(cfg, w, eq, venue, end_ms, sel_ms):
    """All follower runs for one wallet (registration §6 / §6b / §7 / §9 / D15)."""
    book = lambda lag: targets.Copy([targets.LeaderBook(w['events'], eq.equity_fn(w['address']), w['lev'], lag)])
    for poll, lag in [(cfg['poll'], lag) for lag in cfg['lags']] + cfg['robust_polls']:
        p = replay.Params(poll, hedge_mode='static', static_beta=w['static_beta'], **PARAMS)
        w['outcome'][(poll, lag)] = outcome(replay.follow(book(lag), p, venue, end_ms), venue, sel_ms)
    p_h = replay.Params(cfg['poll'], hedge_mode='holdings', **PARAMS)
    w['holdings'] = outcome(replay.follow(book(cfg['primary']), p_h, venue, end_ms), venue, sel_ms)['static']
    p_paper = replay.Params(cfg['poll'], hedge_mode='static', static_beta=w['static_beta'],
                            **{**PARAMS, 'capital_usd': PAPER_USD})
    r = replay.follow(book(cfg['primary']), p_paper, venue, end_ms)
    w['paper'] = None if r is None else {'static': r['hedged']['daily']['mean'], 'orders': r['orders'],
                                         'below_min_size': r['below_min_size'], 'below_lot_step': r['below_lot_step']}


def slim(w, cfg):
    """Drop a wallet's fills, simulator results and non-primary trip lists once its outcomes are recorded."""
    w['fills'] = None
    prim = (cfg['poll'], cfg['primary'])
    for key, o in w['outcome'].items():
        o['res'] = None
        if key != prim:
            o['r_trips'] = []
    return w


def finish_copy_window(cfg, wallets, acc, label):
    """Accumulate one window of T2 / T2b from scored wallets with outcomes; returns (top, bottom) quintiles."""
    prim = (cfg['poll'], cfg['primary'])
    ranked = sorted(wallets, key=lambda w: w['score'])
    k = len(ranked) // 5
    top, bottom = (ranked[-k:], ranked[:k]) if k else ([], [])
    a = acc[label]
    a['ics'].append(stats.tercile_ic(ic_rows(wallets, prim)))
    a['ics_active'].append(stats.tercile_ic(ic_rows(wallets, prim, active_only=True)))
    a['ics_top_half'].append(stats.top_half_ic(ic_rows(wallets, prim)))
    a['ics_plain'].append(stats.tercile_ic([(w['plain'], w['outcome'][prim]['alpha'], w['bias']) for w in wallets
                                            if w['plain'] is not None]))
    a['ics_static'].append(stats.tercile_ic(ic_rows(wallets, prim, 'static')))
    a['windows'].append([(w['address'], w['score'], w['outcome'][prim]['alpha'], w['bias']) for w in wallets])
    a['decile_means'].append(stats.decile_means(ic_rows(wallets, prim)))
    a['zero_share'].append(stats.zero_share_by_quintile(ic_rows(wallets, prim)))
    runs = [(cfg['poll'], lag) for lag in cfg['lags']] + cfg['robust_polls']
    for poll, lag in runs:
        a['top'].setdefault(f'{poll}:{lag}', []).extend(w['outcome'][(poll, lag)]['static'] for w in top)
        a['top_alpha'].setdefault(f'{poll}:{lag}', []).extend(w['outcome'][(poll, lag)]['alpha'] for w in top)
        a['top_unhedged'].setdefault(f'{poll}:{lag}', []).extend(w['outcome'][(poll, lag)]['unhedged'] for w in top)
    a['top_holdings'].extend(w['holdings'] for w in top)
    a['top_paper'].extend(w['paper'] for w in top if w.get('paper'))
    a['top_r'].extend(t for w in top for t in w['outcome'][prim].get('r_trips', []))
    a['top_beta_days'].extend(w['beta_days'] for w in top)
    a['n'].append(len(wallets))
    return top, bottom


def b_returns():
    """ret(symbol, entry hour, horizon h) incl. funding; vol over 30 trailing daily returns. 1H Bitget closes."""
    def close_at(sym, h):
        c = bg.candle_1h(sym, h - HOUR)
        return c[4] if c else None

    def ret(sym, h, span=DAY):
        a, b = close_at(sym, h), close_at(sym, h + span)
        return b / a - 1 if a and b else None

    def vol(sym, h):
        rs = [ret(sym, h - (k + 1) * DAY) for k in range(30)]
        rs = [r for r in rs if r is not None]
        return statistics.stdev(rs) if len(rs) >= 20 else None
    return ret, vol


def retail_addresses(snapshot_path):
    return [u for (u,) in con().execute(
        f"SELECT DISTINCT \"user\" FROM read_parquet('{snapshot_path}') WHERE account_value < {RETAIL_MAX_VALUE}"
    ).fetchall()]


def crowd_trial(cohort, bottom, sel_ms, end_ms, listed, coin_to_sym, funding, acc):
    """One window of T3: daily signals from snapshots, IC on the 3-day return, book inputs."""
    snaps = sorted(p for p in (DATA / f'archive/by_dex/{DEX}/snapshots/perp').glob('date=*/*.parquet')
                   if sel_ms <= int(p.stem.split('_')[1]) < end_ms - B_HORIZON_H * HOUR)
    ret, vol = b_returns()
    fund = lambda s, h, span=DAY: sum(r for t, r in funding.get(s, []) if h <= t < h + span)
    for snap in snaps:
        t_s = int(snap.stem.split('_')[1])
        entry = t_s - t_s % HOUR + 2 * HOUR          # first full hour >= snapshot + 1 h
        for name, crowd_set in (('bottom', bottom), ('retail', retail_addresses(snap))):
            sig = {}
            for coin, v in crowd.tilts(snap, cohort, crowd=crowd_set).items():
                sym = coin_to_sym.get(coin)
                if sym and listed.get(sym) and listed[sym] < entry - 31 * DAY:
                    sig[sym] = v
            pairs = [(v, r - fund(s, entry, B_HORIZON_H * HOUR)) for s, v in sig.items()
                     for r in [ret(s, entry, B_HORIZON_H * HOUR)] if r is not None]
            if len(pairs) >= 10:
                acc['T3'][name]['ic3'].append(stats.spearman([p[0] for p in pairs], [p[1] for p in pairs]))
            acc['T3'][name]['days'].append((entry, sig))
    acc['T3']['n_days'].append(len(snaps))


def crowding_trial(wallets, lb_start, sel, table, venue, acc, sel_key):
    """One window of T4: shadower features for the top half of slow wallets; post-trade path; watchability."""
    ranked = sorted(wallets, key=lambda w: w['score'])
    half = ranked[len(ranked) // 2:]
    days = [lb_start + dt.timedelta(days=i) for i in range((sel - lb_start).days)]
    c = con()
    crowding.build_opens(c, DEX, days, coins=list(table))
    opens = {w['address']: crowding.leader_opens_from_fills(w['address'], w['fills_lb'], table) for w in half}
    flat = [o for os_ in opens.values() for o in os_]
    feats = crowding.shadowers(c, flat) if flat else {}
    watch = crowding.watchability(c, DEX, days[-30:], [w['address'] for w in half])
    prim = (T2['poll'], T2['primary'])
    mech = (T2['poll'], T2['mechanism'])
    for w in half:
        f = feats.get(w['address'])
        if not f:
            continue
        path, counts = crowding.post_trade_path(opens[w['address']], venue, table)
        acc['T4']['rows'].append((sel_key, w['address'], f['excess_mean'], w['outcome'][mech]['static']))
        acc['T4']['rows_repeat'].append((sel_key, w['address'], f['repeat_share'], w['outcome'][mech]['static']))
        wr = watch.get(w['address'], {}).get('pnl_rank')
        if wr is not None:
            acc['T4']['rows_watch'].append((sel_key, w['address'], wr, w['outcome'][mech]['static']))
        acc['T4']['features'].append({'window': sel_key, **f, 'path': path, 'path_n': counts,
                                      'watch': watch.get(w['address']), 'primary': w['outcome'][prim]['static'],
                                      'mechanism': w['outcome'][mech]['static']})
    acc['T4']['n'].append(len(half))


def new_acc():
    copy = lambda: {'ics': [], 'ics_active': [], 'ics_top_half': [], 'ics_plain': [], 'ics_static': [],
                    'windows': [], 'decile_means': [], 'zero_share': [], 'top': {}, 'top_alpha': {},
                    'top_unhedged': {}, 'top_holdings': [], 'top_paper': [], 'top_r': [], 'top_beta_days': [],
                    'n': []}
    return {'T1': {'ics': [], 'ics_zero': [], 'ics_top_half': [], 'by_bucket': {}, 'decile_means': [], 'windows': [],
                   'n': []},
            'T2': copy(), 'T2b': copy(),
            'T3': {'bottom': {'ic3': [], 'days': []}, 'retail': {'ic3': [], 'days': []}, 'n_days': []},
            'T4': {'rows': [], 'rows_repeat': [], 'rows_watch': [], 'features': [], 'n': []}}


def finish_copy(a, cfg):
    """Statistics and pass flags for T2 / T2b (registration §6, §6b)."""
    prim, mech = f'{cfg["poll"]}:{cfg["primary"]}', f'{cfg["poll"]}:{cfg["mechanism"]}'
    z = stats.combine(a['ics'])
    positive = sum(1 for ic, _ in a['ics'] if ic is not None and ic > 0)
    top1, top24 = a['top'].get(prim, []), a['top'].get(mech, [])
    m1, se1, _ = stats.mean_se(top1)
    m24, se24, _ = stats.mean_se(top24)
    pd_mean, pd_se, pd_n = stats.paired_diff(top1, top24) if top1 and top24 else (None, None, 0)
    crit = {'ic_z': z is not None and z >= Z_CRIT,
            'ic_positive_windows': positive >= 3,
            'top_quintile_earns': m1 is not None and m1 > 0,
            'mechanism_24h_positive': m24 is not None and m24 > 0,
            'mechanism_paired': (pd_mean is not None and m1 is not None and pd_mean < 0.5 * m1)}
    boot = stats.cluster_bootstrap(a['windows']) if all(a['windows']) else None
    return {
        'trials': TRIALS, 'z_crit': Z_CRIT, 'n_per_window': a['n'],
        'Z': z, 'ics': a['ics'], 'ic_positive_windows': positive,
        'pass': all(crit.values()), 'criteria': crit,
        'reading': ('NULL: IC passes but the top quintile does not earn — loser persistence'
                    if crit['ic_z'] and crit['ic_positive_windows'] and not crit['top_quintile_earns'] else
                    'reversion suspicion: 24 h mean above 1 h mean' if (m1 is not None and m24 is not None and m24 > m1)
                    else None),
        'top_quintile': {'primary_mean': m1, 'primary_se': se1, 'mechanism_mean': m24, 'mechanism_se': se24,
                         'paired_diff': pd_mean, 'paired_se': pd_se, 'paired_n': pd_n,
                         'primary_median': statistics.median(top1) if top1 else None,
                         'alpha_mean_by_run': {k: statistics.fmean(v) for k, v in a['top_alpha'].items() if v},
                         'static_mean_by_run': {k: statistics.fmean(v) for k, v in a['top'].items() if v},
                         'unhedged_mean_by_run': {k: statistics.fmean(v) for k, v in a['top_unhedged'].items() if v},
                         'holdings_hedged_mean': statistics.fmean(a['top_holdings']) if a['top_holdings'] else None,
                         'atr_r': replay.r_summary(a['top_r']),
                         'paper': {'n': len(a['top_paper']),
                                   'static_mean': statistics.fmean(x['static'] for x in a['top_paper']) if a['top_paper'] else None,
                                   'orders': sum(x['orders'] for x in a['top_paper']),
                                   'below_min_size': sum(x['below_min_size'] for x in a['top_paper']),
                                   'below_lot_step': sum(x['below_lot_step'] for x in a['top_paper'])},
                         'beta_days_median': statistics.median(a['top_beta_days']) if a['top_beta_days'] else None},
        'reported': {'ics_active_only': a['ics_active'], 'Z_active_only': stats.combine(a['ics_active']),
                     'ics_top_half': a['ics_top_half'], 'Z_top_half': stats.combine(a['ics_top_half']),
                     'ics_plain_score': a['ics_plain'], 'ics_static_outcome': a['ics_static'],
                     'decile_means': a['decile_means'], 'zero_share_by_quintile': a['zero_share'],
                     'cluster_bootstrap': boot},
    }


def finish_t3(a, name):
    ret, vol = b_returns()
    fund = a['fund']
    days = a[name]['days']
    book = crowd.long_short(days, lambda s, h: (ret(s, h) or 0) - fund(s, h), vol, B_COST, horizon=B_HORIZON_H // 24)
    series = [v for _, v in book]
    half = len(series) // 2
    ic3 = [x for x in a[name]['ic3'] if x is not None]
    ic_t = stats.newey_west_t(ic3, 3) if ic3 else None
    port_t = stats.newey_west_t(series, 3) if series else None
    crit = {'ic_nw_t': ic_t is not None and ic_t >= Z_CRIT,
            'portfolio_nw_t': port_t is not None and port_t >= Z_CRIT,
            'mean_median_same_sign': bool(series) and (statistics.fmean(series) > 0) == (statistics.median(series) > 0),
            'both_halves_positive': half > 0 and statistics.fmean(series[:half]) > 0 and statistics.fmean(series[half:]) > 0}
    return {'crowd': name, 'ic_mean': statistics.fmean(ic3) if ic3 else None, 'ic_nw_t': ic_t, 'ic_days': len(ic3),
            'portfolio_mean': statistics.fmean(series) if series else None,
            'portfolio_median': statistics.median(series) if series else None, 'portfolio_nw_t': port_t,
            'halves': [statistics.fmean(series[:half]), statistics.fmean(series[half:])] if half else None,
            'days': len(series), 'breadth': crowd.breadth(days), 'criteria': crit, 'pass': all(crit.values())}


def finish_t4(a):
    main = stats.split_test(a['rows']) if a['rows'] else None
    return {'trials': TRIALS, 'z_crit': Z_CRIT, 'n_per_window': a['n'],
            'shadower_excess': main, 'pass': bool(main and main['z'] is not None and main['z'] >= Z_CRIT),
            'reported': {'repeat_share': stats.split_test(a['rows_repeat']) if a['rows_repeat'] else None,
                         'watchability': stats.split_test(a['rows_watch']) if a['rows_watch'] else None,
                         'features': a['features']}}


def assemble(acc):
    """All trials' statistics and pass flags from the accumulated window results (pure)."""
    z1 = stats.combine(acc['T1']['ics'])
    pos1 = sum(1 for ic, _ in acc['T1']['ics'] if ic is not None and ic > 0)
    boot1 = stats.cluster_bootstrap(acc['T1']['windows']) if all(acc['T1']['windows']) else None
    out = {'T1': {'trials': TRIALS, 'z_crit': Z_CRIT, 'n_per_window': acc['T1']['n'], 'Z': z1, 'ics': acc['T1']['ics'],
                  'pass': z1 is not None and z1 >= Z_CRIT and pos1 >= 3,
                  'reported': {'ics_dropped_as_zero': acc['T1']['ics_zero'], 'ics_top_half': acc['T1']['ics_top_half'],
                               'Z_top_half': stats.combine(acc['T1']['ics_top_half']),
                               'by_bucket': {b: {'ics': v, 'Z': stats.combine(v)} for b, v in acc['T1']['by_bucket'].items()},
                               'decile_means': acc['T1']['decile_means'], 'cluster_bootstrap': boot1}},
           'T2': finish_copy(acc['T2'], T2), 'T2b': finish_copy(acc['T2b'], T2B)}
    if acc['T3'].get('fund'):
        out['T3'] = {**finish_t3(acc['T3'], 'bottom'), 'trials': TRIALS, 'z_crit': Z_CRIT,
                     'reported': {'retail_crowd': finish_t3(acc['T3'], 'retail')}, 'n_days': acc['T3']['n_days']}
    out['T4'] = finish_t4(acc['T4'])
    return out


def main():
    commit = guard()
    table = symtab()
    listed = json.loads((DATA / 'derived/bitget_listing.json').read_text())
    summary = json.loads((DATA / f'derived/{DEX}/population_summary.json').read_text())
    floor = summary['regime_trip_floor']
    reg = Regimes()
    venue = BitgetVenue('2026-10-07', funding_override=funding_proxy(table))
    coin_to_sym = {c: r['symbol'] for c, r in table.items()}
    funding = venue.funding_override
    acc = new_acc()
    acc['T3']['fund'] = lambda s, h, span=DAY: sum(r for t, r in funding.get(s, []) if h <= t < h + span)
    out = {'registration': REG.name, 'registration_commit': commit, 'run_at': dt.datetime.now(dt.UTC).isoformat(),
           'params': {**PARAMS, 'paper_usd': PAPER_USD, 'regime_trip_floor': floor, 'trials': TRIALS, 'z_crit': Z_CRIT},
           'windows': []}

    for sel, lb_start, hold_end in schedule.windows(DEX):
        print(f'\n== window {sel} (regime trip floor {floor})')
        sel_ms, lb_ms, end_ms = schedule.ms(sel), schedule.ms(lb_start), schedule.ms(hold_end)
        pop = json.loads((DATA / f'derived/{DEX}/population_{sel}.json').read_text())
        t1_set = set(pop['t1_buckets'])
        slow_set = set(pop[f'slow_both_regimes_{floor}'])
        day_set = set(pop[f'day_both_regimes_{floor}']) - slow_set
        eq = Equity(DEX, sorted(slow_set | day_set), lb_ms, end_ms)
        split = lambda fills: ([f for f in fills if f['time'] < sel_ms], [f for f in fills if f['time'] >= sel_ms])

        # Pass 1 — stream T1 and slow wallets: T1 rows per wallet, slow wallets buffered (small set).
        # Amendment 2: one wallet's fills in memory at a time.
        rows, rows0, by_bucket, with_id, slow = [], [], {}, [], []
        n_seen = 0
        for a, fills in iter_fills(DEX, sorted(t1_set | slow_set), lb_start, hold_end):
            lb, hold = split(fills)
            n_seen += 1
            if a in t1_set:
                sc = scores.plain_score(scores.round_trips(lb))
                bias = scores.long_bias(lb)
                if sc is not None and bias is not None:
                    ht = scores.round_trips(hold)
                    h = scores.plain_score(ht) if len(ht) >= 5 else None
                    rows0.append((sc, h if h is not None else 0.0, bias))
                    if h is not None:
                        rows.append((sc, h, bias))
                        with_id.append((a, sc, h, bias))
                        by_bucket.setdefault(pop['t1_buckets'][a], []).append((sc, h, bias))
            if a in slow_set:
                w = score_wallet(T2, a, lb, hold, eq, table, listed, venue, reg, sel_ms, lb_ms, floor)
                if w is not None:
                    w['fills_lb'] = lb
                    slow.append(w)
            if n_seen % 500 == 0:
                print(f'    pass 1: {n_seen} wallets streamed')
        acc['T1']['ics'].append(stats.tercile_ic(rows))
        acc['T1']['ics_zero'].append(stats.tercile_ic(rows0))
        acc['T1']['ics_top_half'].append(stats.top_half_ic(rows))
        acc['T1']['decile_means'].append(stats.decile_means(rows))
        acc['T1']['windows'].append(with_id)
        for b_, rs in by_bucket.items():
            acc['T1']['by_bucket'].setdefault(b_, []).append(stats.tercile_ic(rs))
        acc['T1']['n'].append(len(rows))
        print(f'    T1 rows {len(rows)}; slow scored {len(slow)}')

        # Mechanics gate on the slow sample, then T2 copies
        mechanics_gate([(w['fills'], w['events'], w['equity']) for w in slow[:25]], venue, end_ms)
        for i, w in enumerate(slow, 1):
            run_copies(T2, w, eq, venue, end_ms, sel_ms)
            if i % 100 == 0:
                print(f'    T2 copies {i}/{len(slow)}')
        top, bottom = finish_copy_window(T2, slow, acc, 'T2')

        # Pass 2 — stream day traders: score and copy each wallet as it arrives, keep outcomes only
        day = []
        for i, (a, fills) in enumerate(iter_fills(DEX, sorted(day_set), lb_start, hold_end), 1):
            lb, hold = split(fills)
            w = score_wallet(T2B, a, lb, hold, eq, table, listed, venue, reg, sel_ms, lb_ms, floor)
            if w is not None:
                run_copies(T2B, w, eq, venue, end_ms, sel_ms)
                w['events'] = None
                day.append(slim(w, T2B))
            if i % 200 == 0:
                print(f'    T2b copies {i}/{len(day_set)}')
        finish_copy_window(T2B, day, acc, 'T2b')
        del day

        # T3 — skilled vs losers
        crowd_trial([w['address'] for w in top], [w['address'] for w in bottom], sel_ms, end_ms, listed, coin_to_sym,
                    funding, acc)

        # T4 — uncrowded skill (lookback fills of the slow wallets)
        crowding_trial(slow, lb_start, sel, table, venue, acc, sel.isoformat())
        for w in slow:
            slim(w, T2)
            w['fills_lb'] = None

        out['windows'].append({'selection': sel.isoformat(), 't1_n': len(rows), 't2_n': len(slow),
                               't2b_n': acc['T2b']['n'][-1], 'cohort': len(top), 't3_days': acc['T3']['n_days'][-1],
                               't4_n': acc['T4']['n'][-1]})

    out.update(assemble(acc))
    with open(RESULTS, 'a') as f:
        f.write(json.dumps(out, default=str) + '\n')
    print(json.dumps({k: {kk: vv for kk, vv in out[k].items() if kk != 'reported'} for k in ('T1', 'T2', 'T2b', 'T3', 'T4')
                      if k in out}, indent=1, default=str))


if __name__ == '__main__':
    main()
