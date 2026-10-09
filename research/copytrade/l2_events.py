"""Liquidation workstream, step 2 — cascade events at the registered thresholds and the
robustness grid, from liquidation fills only (registration 002 §3). Behaviour only: times,
sides, notionals, the cascade's own HL prices. No Bitget price, no forward return.

    .venv/bin/python l2_events.py
Writes data/derived/hyperliquid/cascades.json: {grid key: [event, ...]}.
"""
import json
import time

from ct import liq
from ct.leaders import con
from ct.net import DATA

DEX = 'hyperliquid'
LIQ = DATA / f'derived/{DEX}/liquidations.parquet'
GAP_MS = 60_000
PURITY = 0.8
GRID = {'primary': (1.0, 250_000), 'k0.5': (0.5, 250_000), 'k2': (2.0, 250_000),
        'f50k': (1.0, 50_000), 'f1m': (1.0, 1_000_000)}
DAY = 86_400_000


def symtab():
    table = json.loads((DATA / 'p0/2026-10-07/symbols.json').read_text())
    return {c: r for c, r in table.items() if r['status'] == 'ok'}


def main():
    c = con()
    table = symtab()
    listed = json.loads((DATA / 'derived/bitget_listing.json').read_text())
    coins = [r[0] for r in c.execute(f"SELECT DISTINCT coin FROM read_parquet('{LIQ}')").fetchall()]
    daily_all = {}
    for coin, day, notional in c.execute(f"""
            SELECT coin, (t // {DAY}) * {DAY} AS day, SUM(notional) FROM read_parquet('{LIQ}') GROUP BY 1, 2 ORDER BY 1, 2""").fetchall():
        daily_all.setdefault(coin, []).append((day, notional))
    # cross-coin fallback for coins without 5 days of history: median of all coin-days in the trailing window
    all_days = sorted((d, n) for v in daily_all.values() for d, n in v)
    fallback = liq.trailing_median_fn(all_days, days=30)
    out = {k: [] for k in GRID}
    stats = {'coins': 0, 'unmapped': 0, 'unlisted_events': 0, 'fallback_events': 0}
    t0 = time.time()
    for coin in sorted(coins):
        m = table.get(coin)
        if m is None:
            stats['unmapped'] += 1
            continue
        stats['coins'] += 1
        fills = c.execute(f"SELECT t, direction, notional, price FROM read_parquet('{LIQ}') WHERE coin = ? ORDER BY t",
                          [coin]).fetchall()
        own = liq.trailing_median_fn(daily_all[coin], days=30)
        used_fallback = set()

        def med(t_ms):
            v = own(t_ms)
            if v is None:
                used_fallback.add(t_ms)
                return fallback(t_ms)
            return v
        sym = m['symbol']
        for key, (k, floor) in GRID.items():
            for ev in liq.cascades(fills, GAP_MS, PURITY, k, floor, med):
                if listed.get(sym) is None or ev['t_start'] < listed[sym]:
                    stats['unlisted_events'] += 1
                    continue
                out[key].append({'coin': coin, 'symbol': sym, **{kk: ev[kk] for kk in
                                 ('t_start', 't_end', 'n', 'notional', 'purity', 'side', 'start_price', 'end_price', 'rel')},
                                 'fallback_median': ev['t_start'] in used_fallback})
    stats['fallback_events'] = sum(1 for e in out['primary'] if e['fallback_median'])
    (DATA / f'derived/{DEX}/cascades.json').write_text(json.dumps(out))
    for key, evs in out.items():
        majors = sum(1 for e in evs if e['coin'] in ('BTC', 'ETH', 'SOL'))
        n_coins = len({e['coin'] for e in evs})
        n_long = sum(1 for e in evs if e['side'] == 'L')
        print(f'{key:<8} events {len(evs):>6,}  coins {n_coins:>4}  majors {majors:>5}  longs-liquidated {n_long:>5}')
    print(stats, f'{time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
