"""L2 scout, question E: Ethena sUSDe realised yield, bounded by block number.

Benchmark only (not the L2 hypothesis). DefiLlama's yields chart has no date
bound, so it is NOT called. Instead: block-at-timestamp from coins.llama.fi
(bounded by the timestamp in the URL) and sUSDe.convertToAssets(1e18) via an
eth_call pinned to that block. Every timestamp is < 2026-07-01.

  .venv/bin/python -I scouts/l2_benchmarks.py
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'data' / 'scout_l2' / 'ethena'
CUT_S = 1782864000
SUSDE = '0x9D39A5DE30e57443BfF2A8307A4256c8797A3497'
CALL = '0x07a2d13a' + f'{10 ** 18:064x}'               # convertToAssets(1e18)
RPCS = ['https://eth.drpc.org', 'https://mainnet.gateway.tenderly.co', 'https://eth-mainnet.public.blastapi.io']


def _get(url, body=None):
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body else None,
                                 headers={'Content-Type': 'application/json', 'User-Agent': 'l2-scout'})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def point(ts):
    assert ts < CUT_S
    path = OUT / f'{ts}.json'
    if path.exists():
        return json.loads(path.read_text())
    blk = _get(f'https://coins.llama.fi/block/ethereum/{ts}')
    assert blk['timestamp'] < CUT_S
    for rpc in RPCS:
        try:
            res = _get(rpc, {'jsonrpc': '2.0', 'id': 1, 'method': 'eth_call',
                             'params': [{'to': SUSDE, 'data': CALL}, hex(blk['height'])]})
            if 'result' in res:
                break
        except Exception:
            continue
        time.sleep(1)
    rate = int(res['result'], 16) / 1e18 if res.get('result') not in (None, '0x') else None
    v = {'ts': ts, 'block': blk['height'], 'block_ts': blk['timestamp'], 'rate': rate}
    OUT.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(v))
    time.sleep(0.5)
    return v


def main():
    pts = []
    for y in (2023, 2024, 2025, 2026):
        for m in range(1, 13):
            ts = int(datetime(y, m, 1, tzinfo=timezone.utc).timestamp())
            if datetime(2023, 11, 1, tzinfo=timezone.utc).timestamp() <= ts < CUT_S:
                pts.append(point(ts))
    pts.append(point(CUT_S - 3600))                     # 2026-06-30T23:00Z, last point
    for p in pts:
        print(datetime.fromtimestamp(p['ts'], timezone.utc).date(), p['block'], p['rate'])
    # per-year realised yield, annualised, from the first and last valid point in the year
    print('year  first..last  annualised')
    for y in (2023, 2024, 2025, 2026):
        ys = [p for p in pts if p['rate'] and datetime.fromtimestamp(p['ts'], timezone.utc).year == y]
        nxt = [p for p in pts if p['rate'] and datetime.fromtimestamp(p['ts'], timezone.utc).year == y + 1][:1]
        seg = ys + nxt
        if len(seg) < 2:
            continue
        a, b = seg[0], seg[-1]
        yrs = (b['block_ts'] - a['block_ts']) / (365.25 * 86400)
        print(y, datetime.fromtimestamp(a['ts'], timezone.utc).date(), datetime.fromtimestamp(b['ts'], timezone.utc).date(),
              f"{((b['rate'] / a['rate']) ** (1 / yrs) - 1) * 100:.2f}%")


if __name__ == '__main__':
    sys.exit(main())
