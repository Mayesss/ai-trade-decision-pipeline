"""Which Bitget 1-minute blocks T2's copies would execute in — and fetch them.

From the slow wallets' POSITION CHANGES in each hold window (no PnL), the
follower's execution minutes are known in advance for every lag and poll
interval registration 001 uses; each needs the 200-minute Bitget block that
contains it, plus BTCUSDT for the hedge and the window-end close.

    .venv/bin/python p5_blocks.py --count     # how many blocks / requests
    .venv/bin/python p5_blocks.py --fetch     # fetch them (free, cached, resumable)
"""
import argparse
import datetime as dt
import json
from concurrent.futures import ThreadPoolExecutor

from ct import bitget as bg
from ct import replay, schedule
from ct.leaders import load_fills
from ct.net import CACHE, DATA

DEX = 'hyperliquid'
MINUTE, HOUR = 60_000, 3_600_000
# Registration 001 revision 4: per population, the (poll, lag) pairs the run uses.
#   slow (T2): poll 60 at every lag of the decay curve (1 min .. 72 h); robustness polls 10 and 1
#              at their natural lag.
#   day (T2b): poll 10 at lags 1 min .. 24 h; robustness poll 1 at 1 min.
LAGS_SLOW = [MINUTE, 10 * MINUTE, HOUR, 6 * HOUR, 24 * HOUR, 72 * HOUR]
LAGS_DAY = [MINUTE, 10 * MINUTE, HOUR, 6 * HOUR, 24 * HOUR]
SPECS = {
    'slow_both_regimes_4': [(60 * MINUTE, lag) for lag in LAGS_SLOW] + [(10 * MINUTE, 10 * MINUTE), (MINUTE, MINUTE)],
    'day_both_regimes_4': [(10 * MINUTE, lag) for lag in LAGS_DAY] + [(MINUTE, MINUTE)],
}


def symtab():
    table = json.loads((DATA / 'p0/2026-10-07/symbols.json').read_text())
    return {c: r for c, r in table.items() if r['status'] == 'ok'}


def needed_blocks():
    table = symtab()
    listed = json.loads((DATA / 'derived/bitget_listing.json').read_text())
    blocks = set()
    for sel, _, hold_end in schedule.windows(DEX):
        pop = json.loads((DATA / f'derived/{DEX}/population_{sel}.json').read_text())
        end_ms = schedule.ms(hold_end)
        for key, pairs in SPECS.items():
            fills = load_fills(DEX, pop[key], sel, hold_end)
            n_events = 0
            for a, fs in fills.items():
                events, _ = replay.leader_events(fs, table, listed)
                n_events += len(events)
                held = set()
                for ev in events:
                    held.add(ev['symbol'])
                    for poll, lag in pairs:
                        t = -(-(ev['t_last'] + lag) // poll) * poll + MINUTE
                        if t < end_ms:
                            blocks.add((ev['symbol'], t - t % bg.BLOCK))
                            blocks.add(('BTCUSDT', t - t % bg.BLOCK))
                mark = end_ms - MINUTE
                for s in held:
                    blocks.add((s, mark - mark % bg.BLOCK))
            print(f'  {sel} {key}: {len(fills):,} wallets, {n_events:,} position changes')
    # the static hedge opens at the first poll and re-trues at day marks: BTC blocks at every day mark
    for sel, _, hold_end in schedule.windows(DEX):
        t = schedule.ms(sel)
        while t < schedule.ms(hold_end):
            blocks.add(('BTCUSDT', (t + MINUTE) - (t + MINUTE) % bg.BLOCK))
            t += 86_400_000
    return blocks


def cached(sym, start):
    key = f'{sym}:{start}'
    import hashlib
    return (CACHE / 'bg-1m' / f'{hashlib.sha256(key.encode()).hexdigest()[:24]}.json').exists()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--count', action='store_true')
    ap.add_argument('--fetch', action='store_true')
    args = ap.parse_args()
    blocks = sorted(needed_blocks())
    todo = [b for b in blocks if not cached(*b)]
    print(f'blocks needed {len(blocks):,}; already cached {len(blocks) - len(todo):,}; to fetch {len(todo):,} '
          f'(~{len(todo) / 15 / 3600:.1f} h at 15 req/s)')
    if args.fetch:
        done = 0
        with ThreadPoolExecutor(max_workers=4) as pool:  # the shared pacer keeps the global rate at 15/s
            for _ in pool.map(lambda b: bg._block(*b), todo):
                done += 1
                if done % 2000 == 0:
                    print(f'  fetched {done:,}/{len(todo):,}')
        print('done')


if __name__ == '__main__':
    main()
