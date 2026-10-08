"""Prepare-phase data for registration 001 — prices, funding, vaults.

Nothing here reads a wallet's PnL or computes an outcome. All free public
APIs, cached under data/cache (re-runs cost nothing).

    python3 p4_prefetch.py bitget    # listing dates + 1H candles, mapped coins
    python3 p4_prefetch.py funding   # Hyperliquid funding history, mapped coins
    python3 p4_prefetch.py vaults    # Hyperliquid vault addresses
"""
import datetime as dt
import json
import sys

from ct import bitget as bg
from ct import hl
from ct.net import DATA, cached, get_json

SYMBOLS_DAY = '2026-10-07'  # the P0 symbol table (verified by price)
START = dt.datetime(2025, 6, 1, tzinfo=dt.UTC)   # lookback + 30-day vol / ATR warm-up
END = dt.datetime(2026, 7, 10, tzinfo=dt.UTC)    # discovery end + 3-day horizon + slack
# Funding only matters while a follower holds positions: the hold windows
# start 2025-10-27 (scores use exact cash flows, no funding).
FUNDING_START = dt.datetime(2025, 10, 20, tzinfo=dt.UTC)
ms = lambda d: int(d.timestamp() * 1000)


def mapped():
    table = json.loads((DATA / f'p0/{SYMBOLS_DAY}/symbols.json').read_text())
    return {coin: r['symbol'] for coin, r in table.items() if r['status'] == 'ok'}


def bitget_data():
    syms = sorted(set(mapped().values()))
    listed = {}
    for i, s in enumerate(syms, 1):
        listed[s] = bg.first_candle_day(s, SYMBOLS_DAY)
        if i % 25 == 0:
            print(f'  listing dates {i}/{len(syms)}')
    (DATA / 'derived/bitget_listing.json').write_text(json.dumps(listed, indent=1))
    start = ms(START) - ms(START) % bg.HOUR_BLOCK
    blocks = range(start, ms(END), bg.HOUR_BLOCK)
    for i, s in enumerate(syms, 1):
        first = listed[s] or ms(END)
        for b in blocks:
            if b + bg.HOUR_BLOCK > first:   # skip blocks entirely before listing
                bg.candle_1h(s, b)
        if i % 10 == 0:
            print(f'  1H candles {i}/{len(syms)} symbols')
    print(f'bitget: {len(syms)} symbols, listing dates + 1H candles {START.date()} .. {END.date()}')


def funding():
    coins = sorted(mapped())
    for i, coin in enumerate(coins, 1):
        def fetch(coin=coin):
            out, t = [], ms(FUNDING_START)
            while t < ms(END):
                page = hl._info({'type': 'fundingHistory', 'coin': coin, 'startTime': t, 'endTime': ms(END)},
                                items_per_weight=20)
                if not page:
                    break
                out += [(int(r['time']), float(r['fundingRate'])) for r in page]
                if len(page) < 500:
                    break
                t = int(page[-1]['time']) + 1
            return out
        cached('hl-funding', f'{coin}:{ms(FUNDING_START)}:{ms(END)}', fetch)
        if i % 20 == 0:
            print(f'  funding {i}/{len(coins)} coins')
    print(f'funding: {len(coins)} coins {FUNDING_START.date()} .. {END.date()}')


def vaults():
    rows = cached('hl-vaults', SYMBOLS_DAY,
                  lambda: get_json('https://stats-data.hyperliquid.xyz/Mainnet/vaults', timeout=180))
    print('vault list:', type(rows).__name__, len(rows))
    print('sample:', json.dumps(rows[0])[:600] if rows else None)


if __name__ == '__main__':
    {'bitget': bitget_data, 'funding': funding, 'vaults': vaults}[sys.argv[1]]()
