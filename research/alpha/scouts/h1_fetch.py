"""H1 house-sleeve allocation: pull the public series the decision needs (ledger 000v).

Free, keyless endpoints only, ~1 request/s. Writes one new directory per pull
(refuses an existing one) with SHA256SUMS. Usage:

    python3 -I -S research/alpha/scouts/h1_fetch.py research/alpha/data/scout_h1/pull_YYYYMMDD
"""
import hashlib
import json
import os
import sys
import time
import urllib.request

HL = 'https://api.hyperliquid.xyz/info'
LIGHTER = 'https://mainnet.zklighter.elliot.ai/api/v1'
HLP = '0xdfc24b077bc1425ad1dea75bcb6f8158e10df303'
LLP_INDEX = 281474976710654
DAY_MS = 86_400_000


def get(url):
    req = urllib.request.Request(url, headers={'user-agent': 'research-h1'})
    with urllib.request.urlopen(req, timeout=60) as r:
        time.sleep(1.0)
        return json.load(r)


def post(body):
    req = urllib.request.Request(HL, data=json.dumps(body).encode(),
                                 headers={'content-type': 'application/json', 'user-agent': 'research-h1'})
    with urllib.request.urlopen(req, timeout=60) as r:
        time.sleep(1.0)
        return json.load(r)


def lit_funding(start_ms, end_ms):
    """HL fundingHistory returns at most 500 rows per call; page forward."""
    rows, t = [], start_ms
    while t < end_ms:
        page = post({'type': 'fundingHistory', 'coin': 'LIT', 'startTime': t, 'endTime': end_ms})
        if not page:
            break
        rows.extend(page)
        nxt = page[-1]['time'] + 1
        if nxt <= t:
            break
        t = nxt
    return rows


def cex_funding():
    """LIT funding where the hedge would sit (Binance, Bitget); the most rows each public endpoint serves."""
    rows, end = [], None
    for _ in range(10):
        q = f'&endTime={end}' if end else ''
        page = get(f'https://fapi.binance.com/fapi/v1/fundingRate?symbol=LITUSDT&limit=1000{q}')
        if not page:
            break
        rows = page + rows
        if len(page) < 1000:
            break
        end = page[0]['fundingTime'] - 1
    bitget = []
    for page_no in range(1, 20):
        page = get('https://api.bitget.com/api/v2/mix/market/history-fund-rate?productType=USDT-FUTURES'
                   f'&symbol=LITUSDT&pageSize=100&pageNo={page_no}')['data']
        bitget += page
        if len(page) < 100:
            break
    return {'binance': rows, 'bitget': bitget}


def main(out):
    if os.path.exists(out):
        sys.exit(f'refusing: {out} exists')
    os.makedirs(out)
    now = int(time.time() * 1000)
    files = {
        'llp_account.json': get(f'{LIGHTER}/account?by=index&value={LLP_INDEX}'),
        'hlp_vaultDetails.json': post({'type': 'vaultDetails', 'vaultAddress': HLP}),
        'hl_LIT_candles_1d.json': post({'type': 'candleSnapshot',
                                        'req': {'coin': 'LIT', 'interval': '1d',
                                                'startTime': now - 400 * DAY_MS, 'endTime': now}}),
        'hl_LIT_funding.json': lit_funding(now - 400 * DAY_MS, now),
        'llp_pnl_1d.json': get(f'{LIGHTER}/pnl?by=index&value={LLP_INDEX}&resolution=1d&start_timestamp=1735689600'
                               f'&end_timestamp={now // 1000}&count_back=0&ignore_transfers=false'),
        'cex_LIT_funding.json': cex_funding(),
    }
    sums = []
    for name, payload in files.items():
        path = os.path.join(out, name)
        with open(path, 'w') as f:
            json.dump(payload, f)
        with open(path, 'rb') as f:
            sums.append(f'{hashlib.sha256(f.read()).hexdigest()}  {name}')
        print(name, len(payload) if isinstance(payload, list) else 'object')
    with open(os.path.join(out, 'SHA256SUMS'), 'w') as f:
        f.write('\n'.join(sums) + '\n')
    print('pulled_at_ms', now)


if __name__ == '__main__':
    main(sys.argv[1])
