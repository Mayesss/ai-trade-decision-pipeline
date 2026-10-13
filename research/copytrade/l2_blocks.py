"""Bitget 1-minute blocks for registration 002's event windows — from event times only.

    .venv/bin/python l2_blocks.py --count | --fetch
Primary events: [t_end - 60 min, t_end + 24 h]; robustness grid: [t_end - 60 min, t_end + 240 min].
"""
import argparse
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor

from ct import bitget as bg
from ct.net import CACHE, DATA

MINUTE, HOUR = 60_000, 3_600_000


def needed_blocks():
    grid = json.loads((DATA / 'derived/hyperliquid/cascades.json').read_text())
    blocks = set()
    for key, evs in grid.items():
        after = 24 * HOUR if key == 'primary' else 4 * HOUR + 10 * MINUTE
        for e in evs:
            b = (e['t_end'] - HOUR) - (e['t_end'] - HOUR) % bg.BLOCK
            end = e['t_end'] + after
            while b <= end:
                blocks.add((e['symbol'], b))
                blocks.add(('BTCUSDT', b))
                b += bg.BLOCK
    return blocks


def cached(sym, start):
    key = f'{sym}:{start}'
    return (CACHE / 'bg-1m' / f'{hashlib.sha256(key.encode()).hexdigest()[:24]}.json').exists()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--count', action='store_true')
    ap.add_argument('--fetch', action='store_true')
    args = ap.parse_args()
    blocks = sorted(needed_blocks())
    todo = [b for b in blocks if not cached(*b)]
    print(f'blocks needed {len(blocks):,}; already cached {len(blocks) - len(todo):,}; to fetch {len(todo):,} '
          f'(~{len(todo) / 5 / 3600:.1f} h at the observed ~5 req/s)')
    if args.fetch:
        done = 0
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(lambda b: bg._block(*b), todo):
                done += 1
                if done % 2000 == 0:
                    print(f'  fetched {done:,}/{len(todo):,}')
        print('done')


if __name__ == '__main__':
    main()
