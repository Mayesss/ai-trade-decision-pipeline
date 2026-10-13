"""P0 pilot — docs/copy-trade-plan-2026-10-07.md §8.

PLUMBING, NOT EVIDENCE. The wallets come from today's leaderboard (survivors
only), fills from an API that keeps only each wallet's last 10k, and leader
size is scaled by today's account value, not the value at the time. Nothing
printed here may be read as a result about copy-trading.

What it checks: the APIs answer as documented, HL coins map onto Bitget
contracts, listing dates resolve, the exact replay matches the leader's own
closedPnl, the polling follower matches the leader when costs are off, and
one wallet replays end to end. Its window lies inside the holdout — see
registrations/000-p0-holdout-exposure.md.

    python3 p0_pilot.py [--day YYYY-MM-DD] [--wallets 50] [--days 60] [--seed 7]
"""
import argparse
import datetime as dt
import json
import random
from collections import Counter

from ct import bitget as bg
from ct import features
from ct import hl
from ct import replay
from ct import symbols
from ct import targets
from ct import venue
from ct.net import DATA

DAY_MS = 86_400_000
POLLS_MIN = (1, 10, 60)    # deployable: 1-min cron, the 10-min watcher, hourly
NOMINAL_CAPITAL = 10_000.0  # "does the signal exist"
REAL_CAPITAL = 100.0        # approximate Bitget share of the live account: "does it work for me"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--day', default=dt.datetime.now(dt.UTC).strftime('%Y-%m-%d'))
    ap.add_argument('--wallets', type=int, default=50)
    ap.add_argument('--days', type=int, default=60)
    ap.add_argument('--seed', type=int, default=7)
    args = ap.parse_args()

    end_ms = int(dt.datetime.strptime(args.day, '%Y-%m-%d').replace(tzinfo=dt.UTC).timestamp() * 1000)
    start_ms = end_ms - args.days * DAY_MS
    out_dir = DATA / 'p0' / args.day
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Symbol table, verified by price.
    symtab = symbols.build(hl.perp_universe(), hl.all_mids(), bg.contracts(args.day), bg.tickers())
    (out_dir / 'symbols.json').write_text(json.dumps(symtab, indent=1))
    print('symbol table:', dict(Counter(r['status'] for r in symtab.values())))
    for coin, r in symtab.items():
        if r['status'] == 'price_mismatch':
            print(f'  mismatch {coin} -> {r["symbol"]} ratio {r["price_ratio"]}')

    # 2. Pilot sample from today's leaderboard.
    rows = hl.leaderboard(args.day)
    perf = lambda r, w: float(dict(r['windowPerformances'])[w]['vlm'])
    eligible = [r for r in rows if float(r['accountValue']) >= 10_000 and perf(r, 'month') > 0]
    sample = random.Random(args.seed).sample(eligible, min(args.wallets, len(eligible)))
    print(f'leaderboard rows {len(rows)}, eligible {len(eligible)}, sampled {len(sample)}')

    # 3. Fills per wallet.
    wallets = []
    for i, r in enumerate(sample, 1):
        fills, truncated = hl.user_fills(r['ethAddress'], start_ms, end_ms)
        coins = Counter(f['coin'] for f in fills)
        mapped = sum(n for c, n in coins.items() if symtab.get(c, {}).get('status') == 'ok')
        breaks = replay.chain_breaks(fills)
        wallets.append({'user': r['ethAddress'], 'account_value': float(r['accountValue']),
                        'fills': fills, 'n_fills': len(fills), 'truncated': truncated,
                        'broken_coins': breaks,
                        'broken_fill_share': sum(coins[c] for c in breaks) / len(fills) if fills else None,
                        'mapped_fill_share': mapped / len(fills) if fills else None})
        print(f'  [{i}/{len(sample)}] {r["ethAddress"][:10]} fills {len(fills):>5}'
              f'  broken coins {len(breaks):>2}{"  TRUNCATED?" if truncated else ""}')

    active = [w for w in wallets if w['n_fills']]
    n_trunc = sum(w['truncated'] for w in active)
    n_broken = sum(bool(w['broken_coins']) for w in active)
    broken_share = (sum(w['broken_fill_share'] * w['n_fills'] for w in active)
                    / sum(w['n_fills'] for w in active))
    print(f'wallets with an incomplete coin history: {n_broken}/{len(active)}, '
          f'fills in broken coins: {broken_share:.1%}')
    all_coins = Counter(f['coin'] for w in active for f in w['fills'])
    n_fills = sum(all_coins.values())
    by_status = Counter()
    for c, n in all_coins.items():
        by_status[symtab[c]['status'] if c in symtab else 'not_hl_perp'] += n
    print(f'active wallets {len(active)}/{len(wallets)}, possibly truncated {n_trunc}')
    print('fill share by mapping status:', {k: round(v / n_fills, 3) for k, v in by_status.items()} if n_fills else {})

    # 4. Listing dates for every mapped symbol the sample touched.
    traded = sorted({symtab[c]['symbol'] for c in all_coins if symtab.get(c, {}).get('status') == 'ok'})
    listed_from = {s: bg.first_candle_day(s, args.day) for s in traded}
    late = {s: dt.datetime.fromtimestamp(t / 1000, dt.UTC).date().isoformat()
            for s, t in listed_from.items() if t and t > start_ms}
    print(f'listing dates resolved for {len(traded)} symbols; listed inside the window: {late or "none"}')

    # 5. Self-check: replayed round trips must equal the leader's own closedPnl.
    #    Coins with a chain break are excluded everywhere from here on.
    events_by_wallet, agree, total, worst = {}, 0, 0, []
    for w in active:
        clean = [f for f in w['fills'] if f['coin'] not in w['broken_coins']]
        events, stats = replay.leader_events(clean, symtab, listed_from)
        events_by_wallet[w['user']] = (events, stats)
        a, t, bad = replay.closed_pnl_check(clean, events, 10_000 / w['account_value'], end_ms)
        agree, total = agree + a, total + t
        worst += [(w['user'][:10], *b) for b in bad]
    print(f'self-check: replay == leader closedPnl on {agree}/{total} closed coin histories')
    for b in worst[:5]:
        print('  mismatch', b)

    # 6. Replay one wallet: most copyable events, bounded size.
    candidates = []
    for w in active:
        events, stats = events_by_wallet[w['user']]
        if 20 <= len(events) <= 1500:
            candidates.append((len(events), w, events, stats))
    if not candidates:
        print('no wallet suitable for replay')
        return
    _, w, events, stats = max(candidates, key=lambda c: c[0])
    print(f'\nreplay {w["user"]}: account ${w["account_value"]:,.0f}, {stats}')

    clean = [f for f in w['fills'] if f['coin'] not in w['broken_coins']]
    # PILOT ONLY: equity and typical leverage come from the same window that is
    # replayed (look-ahead). P5 takes both from the lookback and snapshots.
    prof = features.profile(clean, events, w['account_value'], args.days)
    print('leader profile:', {k: round(v, 3) if isinstance(v, float) else v for k, v in prof.items()})
    lev = prof['typical_leverage']
    book = lambda: targets.LeaderBook(events, lambda t: w['account_value'], lev)
    bitget = venue.BitgetVenue(args.day)
    costless = lambda capital: replay.Params(1, capital, band=0.0, max_leverage=1e9, frictionless=True)

    # Mechanics check: a costless 1-minute follower with no band and no cap
    # must land near the leader's exact result at the same scale.
    base = replay.leader_pnl(events, NOMINAL_CAPITAL / w['account_value'] / lev, end_ms)
    mech = replay.follow(targets.Copy([book()]), costless(NOMINAL_CAPITAL), bitget, end_ms)
    print(f'mechanics: leader exact ${base:,.2f} vs costless 1-min follower ${mech["net_usd"]:,.2f}')

    results = {}
    for capital in (NOMINAL_CAPITAL, REAL_CAPITAL):
        for poll in POLLS_MIN:
            results[f'${capital:,.0f} poll {poll}m'] = replay.follow(
                targets.Copy([book()]), replay.Params(poll, capital), bitget, end_ms)

    # Multi-leader mechanics — identities only, no returns printed: this window
    # lies inside the holdout (registrations/000).
    others = sorted(candidates, key=lambda c: -c[0])[1:3]
    specs = [(events, w['account_value'], lev)] + [
        (ev, o['account_value'], features.typical_leverage(ev, o['account_value'])) for _, o, ev, _ in others]
    # LeaderBooks are stateful (they advance through time): build fresh ones per run.
    fresh = lambda: [targets.LeaderBook(ev, (lambda v: lambda t: v)(eq), lv) for ev, eq, lv in specs]
    single = [replay.follow(targets.Copy([b]), costless(NOMINAL_CAPITAL), bitget, end_ms)['net_usd']
              for b in fresh()]
    combined = replay.follow(targets.Copy(fresh()), costless(NOMINAL_CAPITAL), bitget, end_ms)['net_usd']
    gap = abs(combined - sum(single) / len(single))
    print(f'linearity: costless Copy(K={len(specs)}) vs mean of K single copies: gap ${gap:,.4f} '
          f'({"ok" if gap < 1e-6 * NOMINAL_CAPITAL else "FAIL"})')
    cons = targets.Consensus(fresh(), target_leverage=1.0, threshold=0.3)
    cons_run = replay.follow(cons, replay.Params(10, NOMINAL_CAPITAL), bitget, end_ms)
    print(f'consensus: runs, max gross leverage {cons_run["max_gross_leverage"]:.2f} '
          f'({"ok" if cons_run["max_gross_leverage"] <= 1.0 + 0.3 else "CHECK"}: target 1.0 plus band drift)')

    print(f'\n{"run":<20}{"net %":>7}{"fees $":>9}{"slip $":>9}{"fund $":>8}{"orders":>7}'
          f'{"<min":>6}{"<lot":>6}{"maxlev":>7}{"maxDD":>7}{"liq":>4}{"d mean%":>8}{"d sd%":>7}'
          f'{"R n":>5}{"R mean":>8}{"R med":>7}{"R trim":>7}{"R wtd":>7}')
    for name, r in results.items():
        rs, d = r['r'], r['daily']
        print(f'{name:<20}{100 * r["net_return"]:>7.2f}{r["fees"]:>9.2f}{r["slippage"]:>9.2f}'
              f'{r["funding_paid"]:>8.2f}{r["orders"]:>7}{r["below_min_size"]:>6}{r["below_lot_step"]:>6}'
              f'{r["max_gross_leverage"]:>7.2f}{100 * r["max_drawdown"]:>6.1f}%'
              f'{"yes" if r["liquidated_at"] else "no":>4}{100 * d["mean"]:>8.3f}{100 * (d["sd"] or 0):>7.2f}'
              f'{rs["n"]:>5}'
              + (f'{rs["mean"]:>8.3f}{rs["median"]:>7.3f}{rs["trimmed_mean_10"]:>7.3f}{rs["risk_weighted"]:>7.3f}'
                 if rs['n'] else ''))

    report = {
        'day': args.day, 'window': [start_ms, end_ms], 'seed': args.seed,
        'symbol_status': dict(Counter(r['status'] for r in symtab.values())),
        'wallets': [{k: v for k, v in w.items() if k != 'fills'} for w in wallets],
        'fill_share_by_status': {k: v / n_fills for k, v in by_status.items()} if n_fills else {},
        'wallets_with_broken_coins': n_broken,
        'broken_fill_share': broken_share,
        'closed_pnl_check': {'agree': agree, 'total': total, 'mismatches': worst},
        'listed_inside_window': late,
        'replay': {'user': w['user'], 'account_value': w['account_value'], 'event_stats': stats,
                   'profile': prof, 'mechanics': {'leader_exact': base, 'costless_follower': mech['net_usd']},
                   'results': results},
    }
    (out_dir / 'report.json').write_text(json.dumps(report, indent=1))
    print(f'\nreport: {out_dir / "report.json"}')


if __name__ == '__main__':
    main()
