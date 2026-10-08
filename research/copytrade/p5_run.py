"""Registration 001 — the run. REFUSES to start unless the registration is
committed with status REGISTERED and unmodified since (ledger invariant:
no evaluation without a registration written first).

    .venv/bin/python p5_run.py            # T1, T2, T3 on the discovery period

Order inside each window, so a broken pipeline never prints a statistic:
1. mechanics gates on a sample of slow wallets — leader closedPnl self-check
   and the costless-follower identity; abort on failure;
2. scores and outcomes; 3. statistics, pass flags, one line appended to
results.jsonl citing the registration's commit.
"""
import datetime as dt
import json
import re
import statistics
import subprocess
import sys
from pathlib import Path

from ct import crowd, replay, schedule, scores, stats, targets
from ct import bitget as bg
from ct.leaders import Equity, load_fills
from ct.net import CACHE, DATA
from ct.regimes import Regimes
from ct.venue import BitgetVenue

HERE = Path(__file__).resolve().parent
REG = HERE / 'registrations/001-slow-traders-copy-and-crowd-divergence.md'
RESULTS = HERE / 'results.jsonl'
DEX = 'hyperliquid'
MINUTE, HOUR, DAY = 60_000, 3_600_000, 86_400_000
Z_CRIT = 2.13
LAG_PRIMARY, LAG_MECHANISM = HOUR, 24 * HOUR
LAGS = [10 * MINUTE, HOUR, 6 * HOUR, 24 * HOUR, 72 * HOUR]
PARAMS = dict(poll_min=60, capital_usd=10_000.0, max_leverage=3.0, band=0.25, slip_range_frac=0.10,
              maintenance_margin=0.01)
B_COST = 0.0006 + 0.0005            # Bitget taker + flat 5 bp slippage, one way (registration §8)
B_HORIZON_H = 72


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
        import hashlib
        path = CACHE / 'hl-funding' / f'{hashlib.sha256(key.encode()).hexdigest()[:24]}.json'
        if not path.exists():
            sys.exit(f'funding history missing for {coin} — run p4_prefetch.py funding first')
        out[r['symbol']] = [tuple(x) for x in json.loads(path.read_text())]
    return out


def hedged_daily_mean(res):
    """T2 outcome: follower's mean daily net return, hedged. No trades -> flat -> 0."""
    if res is None:
        return 0.0
    return res['hedged']['daily']['mean'] if res.get('hedged') else res['daily']['mean']


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


def b_returns(symbols_listed):
    """ret(symbol, entry hour) over 24 h incl. funding; vol over 30 trailing days. 1H Bitget closes."""
    def close_at(sym, h):
        c = bg.candle_1h(sym, h - HOUR)
        return c[4] if c else None

    def ret(sym, h):
        a, b = close_at(sym, h), close_at(sym, h + DAY)
        return b / a - 1 if a and b else None

    def vol(sym, h):
        rs = [ret(sym, h - (k + 1) * DAY) for k in range(30)]
        rs = [r for r in rs if r is not None]
        return statistics.stdev(rs) if len(rs) >= 20 else None
    return ret, vol


def main():
    commit = guard()
    table = symtab()
    listed = json.loads((DATA / 'derived/bitget_listing.json').read_text())
    reg = Regimes()
    venue = BitgetVenue('2026-10-07', funding_override=funding_proxy(table))
    coin_to_sym = {c: r['symbol'] for c, r in table.items()}
    out = {'registration': REG.name, 'registration_commit': commit, 'run_at': dt.datetime.now(dt.UTC).isoformat(),
           'params': PARAMS, 'windows': []}
    t1_ics, t1_ics_zero, t2_ics, t2_top = [], [], [], {lag: [] for lag in LAGS}
    t3_days, t3_ic = [], []
    funding = venue.funding_override

    for sel, lb_start, hold_end in schedule.windows(DEX):
        print(f'\n== window {sel}')
        sel_ms, lb_ms, end_ms = schedule.ms(sel), schedule.ms(lb_start), schedule.ms(hold_end)
        pop = json.loads((DATA / f'derived/{DEX}/population_{sel}.json').read_text())
        wallets = sorted(set(pop['eligible']) | set(pop['slow_both_regimes']))
        lb = load_fills(DEX, wallets, lb_start, sel)
        hold = load_fills(DEX, wallets, sel, hold_end)
        eq = Equity(DEX, pop['slow_both_regimes'], lb_ms, end_ms)

        # T1 — H0-leader on the eligible set
        rows, rows0 = [], []
        for a in pop['eligible']:
            s = scores.plain_score(scores.round_trips(lb[a]))
            bias = scores.long_bias(lb[a])
            if s is None or bias is None:
                continue
            ht = scores.round_trips(hold[a])
            h = scores.plain_score(ht) if len(ht) >= 5 else None
            rows0.append((s, h if h is not None else 0.0, bias))
            if h is not None:
                rows.append((s, h, bias))
        t1_ics.append(stats.tercile_ic(rows))
        t1_ics_zero.append(stats.tercile_ic(rows0))

        # T2 — slow-trader copy
        slow = []
        for a in pop['slow_both_regimes']:
            trips = scores.round_trips(lb[a])
            s = scores.cross_regime_score(trips, reg.is_rising)
            bias = scores.long_bias(lb[a])
            equity_s, _ = eq.at(a, sel_ms)
            lev = eq.typical_leverage(a, lb_ms, sel_ms)
            if s is None or bias is None or not equity_s or not lev:
                continue
            events, _ = replay.leader_events(hold[a], table, listed)
            slow.append({'address': a, 'score': s, 'bias': bias, 'events': events, 'lev': lev,
                         'equity': equity_s, 'fills': hold[a]})
        mechanics_gate([(w['fills'], w['events'], w['equity']) for w in slow[:25]], venue, end_ms)
        for i, w in enumerate(slow, 1):
            w['outcome'] = {}
            for lag in LAGS:
                book = targets.Copy([targets.LeaderBook(w['events'], eq.equity_fn(w['address']), w['lev'], lag)])
                res = replay.follow(book, replay.Params(**PARAMS), venue, end_ms)
                w['outcome'][lag] = hedged_daily_mean(res)
            if i % 50 == 0:
                print(f'    T2 copies {i}/{len(slow)}')
        t2_ics.append(stats.tercile_ic([(w['score'], w['outcome'][LAG_PRIMARY], w['bias']) for w in slow]))
        ranked = sorted(slow, key=lambda w: w['score'])
        top = ranked[-(len(ranked) // 5):] if len(ranked) >= 5 else []
        for lag in LAGS:
            t2_top[lag] += [w['outcome'][lag] for w in top]

        # T3 — skilled vs crowd
        cohort = [w['address'] for w in top]
        snaps = sorted(p for p in (DATA / f'archive/by_dex/{DEX}/snapshots/perp').glob('date=*/*.parquet')
                       if sel_ms <= int(p.stem.split('_')[1]) < end_ms - B_HORIZON_H * HOUR)
        ret, vol = b_returns(listed)
        for snap in snaps:
            t_s = int(snap.stem.split('_')[1])
            entry = t_s - t_s % HOUR + 2 * HOUR          # first full hour >= snapshot + 1 h
            sig = {}
            for coin, v in crowd.tilts(snap, cohort).items():
                sym = coin_to_sym.get(coin)
                if sym and listed.get(sym) and listed[sym] < entry - 31 * DAY:
                    sig[sym] = v
            fund = lambda s, h: sum(r for t, r in funding.get(s, []) if h <= t < h + DAY)
            day_ret = {s: (ret(s, entry), fund(s, entry)) for s in sig}
            pairs = [(v, day_ret[s][0] - day_ret[s][1]) for s, v in sig.items() if day_ret[s][0] is not None]
            if len(pairs) >= 10:
                t3_ic.append(stats.spearman([p[0] for p in pairs], [p[1] for p in pairs]))
            t3_days.append((entry, sig))
        out['windows'].append({'selection': sel.isoformat(), 't1_n': len(rows), 't2_n': len(slow),
                               'cohort': len(cohort), 't3_days': len(snaps)})

    # Statistics and pass flags (registration §5, §6, §8)
    z1 = stats.combine(t1_ics)
    t1_pass = z1 is not None and z1 >= Z_CRIT and sum(ic > 0 for ic, _ in t1_ics if ic is not None) >= 3
    z2 = stats.combine(t2_ics)
    top1, top24 = statistics.fmean(t2_top[LAG_PRIMARY]), statistics.fmean(t2_top[LAG_MECHANISM])
    t2_pass = (z2 is not None and z2 >= Z_CRIT and sum(ic > 0 for ic, _ in t2_ics if ic is not None) >= 3
               and top1 > 0 and top24 >= 0.5 * top1)
    ret, vol = b_returns(listed)
    funding_sum = lambda s, h: sum(r for t, r in funding.get(s, []) if h <= t < h + DAY)
    book = crowd.long_short(t3_days, lambda s, h: (ret(s, h) or 0) - funding_sum(s, h), vol, B_COST,
                            horizon=B_HORIZON_H // 24)
    series = [v for _, v in book]
    half = len(series) // 2
    ic_t = stats.newey_west_t([x for x in t3_ic if x is not None], 3)
    port_t = stats.newey_west_t(series, 3)
    t3_pass = (ic_t is not None and ic_t >= Z_CRIT and port_t is not None and port_t >= Z_CRIT
               and (statistics.fmean(series) > 0) == (statistics.median(series) > 0)
               and statistics.fmean(series[:half]) > 0 and statistics.fmean(series[half:]) > 0)
    out['T1'] = {'Z': z1, 'ics': t1_ics, 'ics_dropped_as_zero': t1_ics_zero, 'pass': t1_pass}
    out['T2'] = {'Z': z2, 'ics': t2_ics, 'top_quintile_mean_by_lag_ms': {str(k): statistics.fmean(v) for k, v in t2_top.items()},
                 'top_quintile_median_1h': statistics.median(t2_top[LAG_PRIMARY]), 'pass': t2_pass}
    out['T3'] = {'ic_mean': statistics.fmean([x for x in t3_ic if x is not None]), 'ic_nw_t': ic_t,
                 'portfolio_mean': statistics.fmean(series), 'portfolio_median': statistics.median(series),
                 'portfolio_nw_t': port_t, 'halves': [statistics.fmean(series[:half]), statistics.fmean(series[half:])],
                 'days': len(series), 'pass': t3_pass}
    with open(RESULTS, 'a') as f:
        f.write(json.dumps(out, default=str) + '\n')
    print(json.dumps({k: out[k] for k in ('T1', 'T2', 'T3')}, indent=1, default=str))


if __name__ == '__main__':
    main()
