"""H11 scout (market-structure measurement, not a trial): who wins Aave v3
liquidations on a mid-size chain over one month.

Stdlib only, keyless public RPC only, read-only (eth_getLogs / eth_call /
eth_getTransactionByHash / eth_getTransactionReceipt). Never sends a tx.
Raw RPC responses are cached under data/scout_h11/<venue>/ (gitignored; treat
as untrusted, run this with `python -I`).

    .venv/bin/python -I scouts/h11_liquidations.py pull arbitrum 2026-09
    .venv/bin/python -I scouts/h11_liquidations.py report arbitrum 2026-09

Window: one UTC calendar month, [first day 00:00, next month 00:00).
USD values: where a keyless archive RPC exists (Arbitrum), the Aave oracle's own
price for each asset AT THE LIQUIDATION BLOCK (the price the protocol itself
liquidated at). Elsewhere: one DefiLlama price per asset at the month's
midpoint (coins.llama.fi, keyless) -- stated in the output.
"""
import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / 'data' / 'scout_h11'

LLAMA = 'https://coins.llama.fi/prices/historical/{ts}/{coins}'
# Wrapped native token per chain, to price gas.
NATIVE = {'arbitrum': '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',   # WETH
          'optimism': '0x4200000000000000000000000000000000000006',   # WETH
          'avalanche': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'}  # WAVAX
# Shared tx.to that self-reports name() "Atlas ETH" / symbol() "atlETH" (checked
# by eth_call). That is the name of FastLane's Atlas auction escrow token, i.e.
# the liquidation was dispatched through an auction contract -- inference, not
# verified against FastLane documentation.
ROUTERS = {'0x8ad1ae9d97c79aa68a0a151e83ff3942f68f86c1': 'atlas'}


def month_window(month):
    y, m = (int(x) for x in month.split('-'))
    t0 = int(datetime(y, m, 1, tzinfo=timezone.utc).timestamp())
    t1 = int(datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=timezone.utc).timestamp())
    return t0, t1

VENUES = {
    # Aave v3 Pool proxies (same address on Arbitrum/Optimism/Avalanche/Polygon).
    # 'rpc' serves wide eth_getLogs but no historical state; 'archive' serves
    # historical eth_call (oracle prices at the liquidation block). Both keyless.
    'arbitrum': {'rpc': 'https://arb1.arbitrum.io/rpc',
                 'archive': 'https://arbitrum-one.public.blastapi.io',
                 'pool': '0x794a61358d6845594f94dc1db02a252b5b4814ad', 'span': 1_000_000},
    'avalanche': {'rpc': 'https://api.avax.network/ext/bc/C/rpc',
                  'pool': '0x794a61358d6845594f94dc1db02a252b5b4814ad', 'span': 500_000},
    'optimism': {'rpc': 'https://mainnet.optimism.io',
                 'pool': '0x794a61358d6845594f94dc1db02a252b5b4814ad', 'span': 500_000},
}


# ---- keccak-256 (Ethereum's, pre-NIST padding); verified against keccak('')
# and the ERC-20 Transfer topic. Used to derive the event topic and selectors.
_RC = [0x0000000000000001, 0x0000000000008082, 0x800000000000808A, 0x8000000080008000,
       0x000000000000808B, 0x0000000080000001, 0x8000000080008081, 0x8000000000008009,
       0x000000000000008A, 0x0000000000000088, 0x0000000080008009, 0x000000008000000A,
       0x000000008000808B, 0x800000000000008B, 0x8000000000008089, 0x8000000000008003,
       0x8000000000008002, 0x8000000000000080, 0x000000000000800A, 0x800000008000000A,
       0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008]
_ROT = [[0, 36, 3, 41, 18], [1, 44, 10, 45, 2], [62, 6, 43, 15, 61], [28, 55, 25, 21, 56],
        [27, 20, 39, 8, 14]]
_M = (1 << 64) - 1


def _rol(x, n):
    return ((x << n) | (x >> (64 - n))) & _M if n else x


def keccak256(data: bytes) -> bytes:
    rate = 136
    p = bytearray(data) + b'\x01'
    while len(p) % rate:
        p.append(0)
    p[-1] |= 0x80
    a = [[0] * 5 for _ in range(5)]
    for i in range(0, len(p), rate):
        for j in range(rate // 8):
            a[j % 5][j // 5] ^= int.from_bytes(p[i + 8 * j:i + 8 * j + 8], 'little')
        for rc in _RC:
            c = [a[x][0] ^ a[x][1] ^ a[x][2] ^ a[x][3] ^ a[x][4] for x in range(5)]
            d = [c[(x - 1) % 5] ^ _rol(c[(x + 1) % 5], 1) for x in range(5)]
            a = [[a[x][y] ^ d[x] for y in range(5)] for x in range(5)]
            b = [[0] * 5 for _ in range(5)]
            for x in range(5):
                for y in range(5):
                    b[y][(2 * x + 3 * y) % 5] = _rol(a[x][y], _ROT[x][y])
            a = [[b[x][y] ^ ((~b[(x + 1) % 5][y]) & b[(x + 2) % 5][y]) for y in range(5)]
                 for x in range(5)]
            a[0][0] ^= rc
    return b''.join(a[j % 5][j // 5].to_bytes(8, 'little') for j in range(4))


assert keccak256(b'').hex().startswith('c5d2460186f7233c')
TOPIC = '0x' + keccak256(
    b'LiquidationCall(address,address,address,uint256,uint256,address,bool)').hex()
assert TOPIC == '0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286'


def sel(sig):
    return '0x' + keccak256(sig.encode()).hex()[:8]


# ---- RPC with an on-disk cache (one JSON file per venue per method family)
class Rpc:
    def __init__(self, venue):
        self.v = VENUES[venue]
        self.dir = DATA / venue
        self.dir.mkdir(parents=True, exist_ok=True)
        self.cache_path = self.dir / 'rpc_cache.jsonl'
        self.cache = {}
        if self.cache_path.exists():
            for line in self.cache_path.read_text().splitlines():
                if line.strip():
                    k, v = json.loads(line)
                    self.cache[k] = v
        self.fh = self.cache_path.open('a')
        self.calls = 0
        self.bytes = 0

    def raw(self, method, params, retries=6, url=None):
        body = json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': method,
                           'params': params}).encode()
        for attempt in range(retries + 1):
            req = urllib.request.Request(url or self.v['rpc'], data=body,
                                         headers={'Content-Type': 'application/json',
                                                  'User-Agent': 'h11-scout/0.1'})
            try:
                with urllib.request.urlopen(req, timeout=60) as res:
                    payload = res.read()
                self.calls += 1
                self.bytes += len(payload)
                time.sleep(0.12)  # gentle pacing: < ~8 req/s
                return json.loads(payload)
            except urllib.error.HTTPError as err:
                if err.code not in (429, 500, 502, 503, 504) or attempt == retries:
                    return {'error': {'http': err.code}}
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if attempt == retries:
                    raise
            time.sleep(min(30, 2 ** attempt))

    def call(self, method, params, cache=True):
        key = method + json.dumps(params, sort_keys=True)
        if cache and key in self.cache:
            return self.cache[key]
        url = self.v.get('archive') if method in ('eth_call', 'eth_getCode') else None
        if method in ('eth_call', 'eth_getCode') and params[-1] == 'latest':
            cache = False  # 'latest' is not immutable
        out = self.raw(method, params, url=url)
        if 'error' in out:
            return out
        if cache:
            self.cache[key] = out
            self.fh.write(json.dumps([key, out]) + '\n')
            self.fh.flush()
        return out

    def block_ts(self, b):
        r = self.call('eth_getBlockByNumber', [hex(b), False])
        return int(r['result']['timestamp'], 16)

    def block_at(self, t):
        """First block with timestamp >= t (binary search, cached)."""
        head = int(self.raw('eth_blockNumber', [])['result'], 16)
        lo, hi = 0, head
        while lo < hi:
            m = (lo + hi) // 2
            if self.block_ts(m) < t:
                lo = m + 1
            else:
                hi = m
        return lo

    def eth_call(self, to, data, block):
        tag = block if isinstance(block, str) else hex(block)
        r = self.call('eth_call', [{'to': to, 'data': data}, tag])
        if 'error' in r:
            return None
        return r['result']


def _addr(word):
    return '0x' + word[-40:].lower()


def _words(hexdata):
    h = hexdata[2:]
    return [h[i:i + 64] for i in range(0, len(h), 64)]


def decode_string(res):
    if not res or res == '0x':
        return None
    h = res[2:]
    if len(h) == 64:  # bytes32 symbol (MKR-style)
        return bytes.fromhex(h).rstrip(b'\x00').decode('utf-8', 'replace')
    try:
        ln = int(h[64:128], 16)
        return bytes.fromhex(h[128:128 + 2 * ln]).decode('utf-8', 'replace')
    except ValueError:
        return None


# ---- pull
def llama_prices(venue, assets, ts):
    coins = ','.join(f'{venue}:{a}' for a in sorted(assets))
    url = LLAMA.format(ts=ts, coins=coins)
    req = urllib.request.Request(url, headers={'User-Agent': 'h11-scout/0.1'})
    with urllib.request.urlopen(req, timeout=60) as res:
        raw = json.load(res)
    out = {}
    for k, v in raw.get('coins', {}).items():
        out[k.split(':', 1)[1].lower()] = float(v['price'])
    return out


def pull(venue, month):
    rpc = Rpc(venue)
    v = rpc.v
    t0, t1 = month_window(month)
    b0 = rpc.block_at(t0)
    b1 = rpc.block_at(t1)  # exclusive
    print(f'{venue} {month}: blocks [{b0}, {b1}) ts {rpc.block_ts(b0)} .. {rpc.block_ts(b1)}')
    logs = []
    start, span = b0, v['span']
    while start < b1:
        end = min(b1 - 1, start + span - 1)
        r = rpc.call('eth_getLogs', [{'address': v['pool'], 'topics': [TOPIC],
                                      'fromBlock': hex(start), 'toBlock': hex(end)}])
        if 'error' in r:
            span = max(1000, span // 2)
            print('  getLogs error, span ->', span, r['error'])
            continue
        logs.extend(r['result'])
        start = end + 1
    logs = [lg for lg in logs if not lg.get('removed')]
    print(f'  {len(logs)} LiquidationCall logs, {len({lg["transactionHash"] for lg in logs})} txs')

    archive = 'archive' in v
    state = (b1 - 1) if archive else 'latest'  # block for metadata / getCode
    pool = v['pool']
    native = NATIVE[venue]
    assets = {}
    for lg in logs:
        for a in (_addr(lg['topics'][1]), _addr(lg['topics'][2])):
            if a not in assets:
                dec = rpc.eth_call(a, sel('decimals()'), state)
                sym = decode_string(rpc.eth_call(a, sel('symbol()'), state))
                assets[a] = {'decimals': int(dec, 16) if dec else 18, 'symbol': sym}
    if archive:
        prov = _addr(rpc.eth_call(pool, sel('ADDRESSES_PROVIDER()'), b1 - 1))
        oracle = _addr(rpc.eth_call(prov, sel('getPriceOracle()'), b1 - 1))
        unit = int(rpc.eth_call(oracle, sel('BASE_CURRENCY_UNIT()'), b1 - 1), 16)
        price_source = f'Aave oracle {oracle} at each liquidation block'
        print(f'  oracle {oracle} unit {unit}')
    else:
        mid = (t0 + t1) // 2
        mid_px = llama_prices(venue, set(assets) | {native}, mid)
        price_source = f'DefiLlama one price per asset at ts {mid} (month midpoint)'
        missing = [assets[a]['symbol'] for a in assets if a not in mid_px]
        print(f'  {price_source}; unpriced: {missing}')

    rows = []
    for i, lg in enumerate(logs):
        blk = int(lg['blockNumber'], 16)
        coll, debt, user = (_addr(t) for t in lg['topics'][1:4])
        w = _words(lg['data'])
        debt_amt, coll_amt = int(w[0], 16), int(w[1], 16)
        liquidator, receive_atoken = _addr(w[2]), bool(int(w[3], 16))
        if archive:
            px = {}
            for a in (coll, debt, native):
                res = rpc.eth_call(oracle, sel('getAssetPrice(address)') + a[2:].rjust(64, '0'), blk)
                px[a] = int(res, 16) / unit if res else None
        else:
            px = {a: mid_px.get(a) for a in (coll, debt, native)}
        tx = rpc.call('eth_getTransactionByHash', [lg['transactionHash']])['result']
        rc = rpc.call('eth_getTransactionReceipt', [lg['transactionHash']])['result']
        gas_native = int(rc['gasUsed'], 16) * int(rc.get('effectiveGasPrice') or tx['gasPrice'], 16) / 1e18
        debt_usd = debt_amt / 10 ** assets[debt]['decimals'] * px[debt] if px[debt] else None
        coll_usd = coll_amt / 10 ** assets[coll]['decimals'] * px[coll] if px[coll] else None
        rows.append({
            'block': blk, 'tx': lg['transactionHash'], 'log_index': int(lg['logIndex'], 16),
            'tx_index': int(lg['transactionIndex'], 16),
            'collateral': coll, 'debt': debt, 'user': user,
            'collateral_sym': assets[coll]['symbol'], 'debt_sym': assets[debt]['symbol'],
            'debt_to_cover': str(debt_amt), 'collateral_seized': str(coll_amt),
            'liquidator': liquidator, 'receive_atoken': receive_atoken,
            'tx_from': tx['from'].lower(), 'tx_to': (tx['to'] or '').lower(),
            'tx_type': tx.get('type'), 'status': rc['status'],
            'debt_usd': debt_usd, 'collateral_usd': coll_usd,
            'gas_native': gas_native, 'gas_usd': gas_native * px[native] if px[native] else None,
            'n_logs_in_tx': len(rc['logs']),
        })
        if i % 200 == 0:
            print(f'  {i}/{len(logs)} decoded, {rpc.calls} calls, {rpc.bytes/1e6:.1f} MB')

    # Is the liquidator a contract? (eth_getCode; one call per address)
    code = {}
    for a in {r['liquidator'] for r in rows} | {r['tx_to'] for r in rows if r['tx_to']}:
        res = rpc.call('eth_getCode', [a, hex(state) if archive else 'latest'])
        code[a] = len(res.get('result', '0x')) > 2
    for r in rows:
        r['liquidator_is_contract'] = code[r['liquidator']]
        r['liquidator_is_tx_from'] = r['liquidator'] == r['tx_from']
        r['liquidator_is_tx_to'] = r['liquidator'] == r['tx_to']
        r['route'] = ROUTERS.get(r['tx_to'], 'direct' if r['liquidator_is_tx_to'] else 'other')

    meta = {'venue': venue, 'month': month, 'pool': pool, 'blocks': [b0, b1],
            'price_source': price_source,
            'rpc_calls_this_run': rpc.calls, 'rpc_bytes_this_run': rpc.bytes}
    (rpc.dir / f'liquidations_{month}.json').write_text(json.dumps({'meta': meta, 'rows': rows}, indent=1))
    print(f'  wrote {len(rows)} rows; this run {rpc.calls} calls, {rpc.bytes/1e6:.2f} MB')


# ---- report
def _conc(rows, key, weight=None):
    tot = Counter()
    for r in rows:
        tot[r[key]] += (r[weight] or 0) if weight else 1
    total = sum(tot.values())
    ranked = tot.most_common()
    share = lambda k: sum(x for _, x in ranked[:k]) / total if total else float('nan')
    return {'n': len(rows), 'distinct': len(ranked), 'top1': share(1), 'top3': share(3),
            'top10': share(10), 'outside_top10': 1 - share(10), 'ranked': ranked}


def _line(label, c):
    return (f"{label:34s} n {c['n']:5d}  distinct {c['distinct']:4d}  top1 {c['top1']:6.1%}  "
            f"top3 {c['top3']:6.1%}  top10 {c['top10']:6.1%}  OUTSIDE {c['outside_top10']:6.1%}")


def report(venue, month):
    d = json.loads((DATA / venue / f'liquidations_{month}.json').read_text())
    rows = [r for r in d['rows'] if r['status'] == '0x1' and r['debt_usd'] is not None]
    unpriced = sum(1 for r in d['rows'] if r['debt_usd'] is None)
    print(f"# {venue} {month}  pool {d['meta']['pool']}  blocks {d['meta']['blocks']}")
    print(f"prices: {d['meta']['price_source']}  (unpriced events dropped: {unpriced})")
    print(f"liquidation events: {len(rows)}   txs: {len({r['tx'] for r in rows})}   "
          f"distinct borrowers: {len({r['user'] for r in rows})}")
    usd = [r['debt_usd'] for r in rows]
    print(f"debt repaid USD: total {sum(usd):,.0f}  median {statistics.median(usd):,.2f}  "
          f"mean {statistics.mean(usd):,.0f}  max {max(usd):,.0f}")
    for lo in (10, 100, 1000):
        sub = [x for x in usd if x >= lo]
        if sub:
            print(f"  events >= ${lo}: {len(sub)}  median ${statistics.median(sub):,.0f}")
    bonus = [(r['collateral_usd'] - r['debt_usd'], r['debt_usd']) for r in rows
             if r['collateral_usd'] is not None]
    big = [b / dd for b, dd in bonus if dd >= 1000]
    print(f"gross bonus USD (seized - repaid, before gas/swap): total {sum(b for b, _ in bonus):,.0f}"
          + (f"; median bonus on >=$1k events {statistics.median(big):.1%}" if big else ''))
    gtx = {r['tx']: r['gas_usd'] for r in rows if r['gas_usd'] is not None}
    if gtx:
        print(f"gas USD per winning tx: total {sum(gtx.values()):,.2f}  median "
              f"{statistics.median(gtx.values()):,.4f}  max {max(gtx.values()):,.2f}")
    print(f"liquidator is a contract: {sum(r['liquidator_is_contract'] for r in rows)}/{len(rows)}; "
          f"liquidator == tx.from: {sum(r['liquidator_is_tx_from'] for r in rows)}; "
          f"liquidator == tx.to: {sum(r['liquidator_is_tx_to'] for r in rows)}")
    routes = Counter(r['route'] for r in rows)
    rusd = Counter()
    for r in rows:
        rusd[r['route']] += r['debt_usd']
    print('routes (count / USD share): ' + ', '.join(
        f"{k} {n} / {rusd[k] / sum(usd):.1%}" for k, n in routes.most_common()))
    out = {'meta': d['meta'], 'tiers': {}}
    for lo in (0, 100, 1000):
        sub = [r for r in rows if r['debt_usd'] >= lo]
        if not sub:
            continue
        print(f"\n-- events with debt repaid >= ${lo} --")
        tier = {}
        for key in ('liquidator', 'tx_from', 'tx_to'):
            for w in (None, 'debt_usd'):
                c = _conc(sub, key, w)
                print(_line(f'{key} by {"count" if w is None else "USD"}', c))
                tier[f'{key}_{w or "count"}'] = {k: v for k, v in c.items() if k != 'ranked'}
        # Cross-definition: who is "top 10" depends on the ranking. One big win
        # puts an address into the USD top 10, so USD-ranked is mechanically harsh.
        top_n = {a for a, _ in _conc(sub, 'liquidator')['ranked'][:10]}
        tot = sum(r['debt_usd'] for r in sub)
        x = sum(r['debt_usd'] for r in sub if r['liquidator'] not in top_n) / tot
        print(f"liquidator top-10 ranked BY COUNT -> outsiders' USD share {x:.1%}")
        tier['outside_count_ranked_top10_usd_share'] = x
        out['tiers'][lo] = tier
    c = _conc(rows, 'liquidator')
    usd_by = dict(_conc(rows, 'liquidator', 'debt_usd')['ranked'])
    froms = defaultdict(set)
    for r in rows:
        froms[r['liquidator']].add(r['tx_from'])
    route_of = defaultdict(Counter)
    for r in rows:
        route_of[r['liquidator']][r['route']] += 1
    print('\ntop 12 liquidators (events, USD repaid, distinct tx.from, routes):')
    for a, n in c['ranked'][:12]:
        print(f"  {a}  {n:4d}  ${usd_by[a]:>12,.0f}  from_eoas={len(froms[a]):3d}  {dict(route_of[a])}")
    top10 = {a for a, _ in c['ranked'][:10]}
    outs = [r for r in rows if r['liquidator'] not in top10]
    if outs:
        print(f"outside-top10 (by count): {len(outs)} events by {len({r['liquidator'] for r in outs})} "
              f"addresses; median USD {statistics.median(r['debt_usd'] for r in outs):,.0f}")
    print(f"addresses with exactly one win: {sum(1 for _, n in c['ranked'] if n == 1)}")
    (DATA / venue / f'summary_{month}.json').write_text(json.dumps(out, indent=1))


if __name__ == '__main__':
    cmd, venue, month = sys.argv[1], sys.argv[2], sys.argv[3]
    {'pull': pull, 'report': report}[cmd](venue, month)
