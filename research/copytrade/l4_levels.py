"""Liquidation workstream — map levels per snapshot and coin (registration 003 §3). Behaviour
only: the snapshot's liquidation prices and the HL price at snapshot time. No forward price.

For each snapshot, mapped coin, side and fixed distance d in {1%, 2%, 3%}: P0 = last HL trade
before the snapshot; the resting level is P0 x (1 -/+ d); the feature is the cumulative
liquidation notional between P0 and the level (plus 0.5% beyond) over the coin's gross notional
— the forced flow that fires on the way to the order. (A first version placed the order at the
densest 0.5% bin within 5%; that bin was almost always the farthest, 4.5–5% away — low-leverage
positions pile up at the edge — so distance is fixed and the map decides whether to place.)

    .venv/bin/python l4_levels.py
Writes data/derived/hyperliquid/map_levels.parquet and prints distributions.
"""
import json
import math
import statistics
import time

import pyarrow as pa
import pyarrow.parquet as pq

from ct import schedule, tape
from ct.leaders import con
from ct.net import DATA

DEX = 'hyperliquid'


def symtab():
    table = json.loads((DATA / 'p0/2026-10-07/symbols.json').read_text())
    return {c: r for c, r in table.items() if r['status'] == 'ok'}


DISTANCES = (0.01, 0.02, 0.03)
PAST = 0.005          # forced flow counted up to half a percent beyond the level


def cum_density(positions, p0, side, d):
    """Notional of positions on `side` whose liquidation price lies between P0 and the level at
    distance d (plus PAST beyond it), over the coin's gross notional. The forced flow that fires
    on the way to the resting order and just past it."""
    gross = sum(abs(n) for _, n, _ in positions)
    hit = 0.0
    for size, notional, lp in positions:
        if lp is None or not lp or size == 0:
            continue
        if side == 'L' and size > 0 and p0 * (1 - d - PAST) <= lp <= p0:
            hit += abs(notional)
        elif side == 'S' and size < 0 and p0 <= lp <= p0 * (1 + d + PAST):
            hit += abs(notional)
    return (hit / gross if gross else None), gross


def main():
    table = symtab()
    coins = sorted(table)
    snaps = sorted((int(p.stem.split('_')[1]), str(p)) for p in
                   (DATA / f'archive/by_dex/{DEX}/snapshots/perp').glob('date=*/*.parquet'))
    snaps = [s for s in snaps if s[0] < schedule.ms(schedule.HOLDOUT_START)]
    c = con()
    rows = []
    t0 = time.time()
    for i, (snap_ms, path) in enumerate(snaps, 1):
        p0 = tape.last_price_before(DEX, coins, snap_ms)
        pos = c.execute(f"""SELECT market, size, notional, liquidation_price FROM read_parquet('{path}')
                            WHERE size != 0 AND market IN (SELECT coin FROM tape_coins)""").fetchall()
        by_coin = {}
        for m, s_, n, lp in pos:
            by_coin.setdefault(m, []).append((s_, n, lp))
        for coin, plist in by_coin.items():
            if coin not in p0:
                continue
            for side in ('L', 'S'):
                for d in DISTANCES:
                    dens, gross = cum_density(plist, p0[coin], side, d)
                    if dens is None:
                        continue
                    level = p0[coin] * (1 - d) if side == 'L' else p0[coin] * (1 + d)
                    rows.append({'snapshot_ms': snap_ms, 'coin': coin, 'side': side, 'd': d, 'p0': p0[coin],
                                 'gross': gross, 'level': level, 'density': dens, 'positions': len(plist)})
        if i % 50 == 0:
            print(f'  {i}/{len(snaps)} snapshots, {len(rows):,} levels, {time.time() - t0:.0f} s')
    pq.write_table(pa.Table.from_pylist(rows), DATA / f'derived/{DEX}/map_levels.parquet')
    print(f'levels: {len(rows):,} over {len(snaps)} snapshots ({len(rows) // 6:,} coin-snapshots)')
    qs = lambda v: [f'{x:.4f}' for x in statistics.quantiles(v, n=10)]
    for side in ('L', 'S'):
        for d in DISTANCES:
            v = [r['density'] for r in rows if r['side'] == side and r['d'] == d]
            maj = [r['density'] for r in rows if r['side'] == side and r['d'] == d and r['coin'] in ('BTC', 'ETH', 'SOL')]
            print(f'  {side} d={d:.0%}: n {len(v):,} cum-density deciles {qs(v)} | majors median {statistics.median(maj):.4f}')
    g = [r['gross'] for r in rows if r['d'] == 0.02 and r['side'] == 'L']
    print(f'  gross notional per coin-snapshot deciles ($): {[f"{x:,.0f}" for x in statistics.quantiles(g, n=10)]}')


if __name__ == '__main__':
    main()
