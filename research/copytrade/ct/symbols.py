"""Hyperliquid perp coin -> Bitget USDT-FUTURES symbol, verified by price.

Unit conventions differ: HL `kPEPE` is 1,000 PEPE per unit; Bitget may list
`PEPEUSDT` (1 per unit) or `1000PEPEUSDT`. Tickers can also collide across
unrelated tokens. So every candidate is accepted only if the two venues'
current prices agree once converted to price-per-token.

qty_mult converts HL units to Bitget units: bitget_qty = hl_qty * qty_mult.
"""
import re

PRICE_TOLERANCE = 0.03

_HL_K = re.compile(r'^k([A-Z0-9]+)$')
_BG_MULT = re.compile(r'^(10+)([A-Z0-9]+)$')


def _hl_split(coin):
    m = _HL_K.match(coin)
    return (m.group(1), 1000) if m else (coin, 1)


def _bg_split(base_coin):
    m = _BG_MULT.match(base_coin)
    return (m.group(2), int(m.group(1))) if m else (base_coin, 1)


def build(hl_universe, hl_mids, bg_contracts, bg_tickers):
    """{hl_coin: {symbol, qty_mult, status, price_ratio}} for every HL perp.

    status: ok | delisted_on_hl | no_bitget_contract | price_mismatch | no_price
    """
    bg_last = {t['symbol']: float(t['lastPr']) for t in bg_tickers if t.get('lastPr')}
    by_token = {}
    for c in bg_contracts:
        if c['quoteCoin'] != 'USDT':
            continue
        token, mult = _bg_split(c['baseCoin'])
        by_token.setdefault(token, []).append(
            (c['symbol'], mult, float(c['minTradeUSDT']), float(c['sizeMultiplier']), float(c['minTradeNum'])))

    table = {}
    for asset in hl_universe:
        coin = asset['name']
        row = {'symbol': None, 'qty_mult': None, 'min_usdt': None, 'size_step': None, 'min_qty': None,
               'price_ratio': None}
        if asset.get('isDelisted'):
            table[coin] = {**row, 'status': 'delisted_on_hl'}
            continue
        token, hl_mult = _hl_split(coin)
        candidates = by_token.get(token, [])
        if not candidates:
            table[coin] = {**row, 'status': 'no_bitget_contract'}
            continue
        hl_px = float(hl_mids[coin]) / hl_mult if coin in hl_mids else None
        best = None
        for symbol, bg_mult, min_usdt, size_step, min_qty in candidates:
            if hl_px is None or symbol not in bg_last:
                continue
            ratio = (bg_last[symbol] / bg_mult) / hl_px
            if best is None or abs(ratio - 1) < abs(best[-1] - 1):
                best = (symbol, bg_mult, min_usdt, size_step, min_qty, ratio)
        if best is None:
            table[coin] = {**row, 'status': 'no_price'}
            continue
        symbol, bg_mult, min_usdt, size_step, min_qty, ratio = best
        ok = abs(ratio - 1) <= PRICE_TOLERANCE
        table[coin] = {'symbol': symbol, 'qty_mult': hl_mult / bg_mult, 'min_usdt': min_usdt,
                       'size_step': size_step, 'min_qty': min_qty,
                       'price_ratio': round(ratio, 5), 'status': 'ok' if ok else 'price_mismatch'}
    return table
