"""Bitget 1-minute blocks for registration 004's cross-venue capture: the minute after each
possible fill inside a trigger's order life, plus 15 minutes. From trigger times only.

    .venv/bin/python l6_blocks.py --count | --fetch
"""
import hashlib
import sys
from concurrent.futures import ThreadPoolExecutor

from ct import bitget as bg
from ct.net import CACHE
import l6_run

MINUTE = 60_000


def needed():
    trigs, _ = l6_run.build_triggers()
    blocks = set()
    for t in trigs:
        b = t['t_post'] - t['t_post'] % bg.BLOCK
        end = t['t_ttl'] + l6_run.CAPTURE_HOLD + 2 * MINUTE
        while b <= end:
            blocks.add((t['symbol'], b))
            b += bg.BLOCK
    return blocks


def cached(sym, start):
    return (CACHE / 'bg-1m' / f'{hashlib.sha256(f"{sym}:{start}".encode()).hexdigest()[:24]}.json').exists()


if __name__ == '__main__':
    blocks = sorted(needed())
    todo = [b for b in blocks if not cached(*b)]
    print(f'blocks needed {len(blocks):,}; cached {len(blocks) - len(todo):,}; to fetch {len(todo):,}')
    if '--fetch' in sys.argv:
        done = 0
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(lambda b: bg._block(*b), todo):
                done += 1
                if done % 1000 == 0:
                    print(f'  fetched {done:,}/{len(todo):,}')
        print('done')
