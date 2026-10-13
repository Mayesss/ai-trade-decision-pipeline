"""Registration 010 — consolidate the raw downloads in data/carry/ into the panel the runner reads
(and whose SHA-256 the registration fixes). Behaviour data only; nothing here computes a return.

    .venv/bin/python -I r010_build.py                  # the discovery panel -> data/carry/panel/
    .venv/bin/python -I r010_build.py --with-hl-hold   # the registered discovery panel (HL 2025-26 funding included)
    .venv/bin/python -I r010_build.py --holdout        # after the gated holdout fetch -> data/carry/panel_holdout_<end>/
    .venv/bin/python -I r010_build.py --extension      # after the gated extension fetch -> data/carry/panel_extension_2027-06-30/

Discovery panel:
  universe.json     coin -> {venue: venue symbol}; coin = Binance USDT-M base (e.g. BTC, 1000PEPE).
                    A venue symbol claimed by two coins keeps the exact-name one, else the one with more
                    Binance funding months UP TO 2024-12 (no later month is counted)
  funding.parquet   venue, coin, t (hour-rounded ms), rate. HL discovery span (+ its 2025-26 span if
                    fetched); BN, BY, OK to 2026-06-30
  prices.parquet    venue, coin, t (8-hour grid), lp = ln(last-trade close of the bar ENDING at t)
                    (eligibility, known at t) and lpo = ln(open of the bar STARTING at t): the first
                    trade after the settlement at t, the fill (Revision 2, N6). BN 8h klines, BY
                    240-min klines, HL 8h candleSnapshot, OK 4H candles. t < 2025-01-01
  path.parquet      coin, t (bar close), high, low, close: Binance 8h, t <= 2025-01-01
  volume.parquet    venue, coin, day, qvol: daily quote volume per venue (BN 8h klines, BY daily
                    turnover, HL 8h candles volume x close, OK 1D quote volume), < 2026-07-01
  levels.parquet    venue, coin, t, close: closes for the name check (BN and HL 8h closes at 00:00 and
                    16:00 UTC; BY D at 00:00; OK 1D (UTC+8) at 16:00), < 2026-07-01
Every reader drops t >= the panel end at load; the count dropped is printed (count only).
"""
import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

import r010_fetch as F  # noqa: E402

HOUR, GRID, DAY = 3_600_000, 8 * 3_600_000, 86_400_000
DISC_PRICE_END = F.PRICE_END
DROPPED = {'rows_at_or_after_end': 0}
NONFINITE = {}
HL_WITH_HOLD = '--with-hl-hold' in sys.argv


def load(path):
    return json.loads(path.read_text()) if path.exists() else None


def keep(rows, tkey, end):
    out = [r for r in rows if tkey(r) < end]
    DROPPED['rows_at_or_after_end'] += len(rows) - len(out)
    return out


def hround(t):
    return (t + HOUR // 2) // HOUR * HOUR


def exact_name(venue, coin, vs):
    """The venue symbol is the plain spelling of the Binance coin (no k / 1000 re-mapping)."""
    if venue == 'HL':
        return F.hl_name_from_bn(coin + 'USDT') == vs
    if venue == 'BY':
        return vs == coin + 'USDT'
    return vs == coin + '-USDT' or (coin.startswith('1000') and vs == coin[4:] + '-USDT')


def universe(months=None, by=None, ok=None, hlmap=None, tie_months_upto='2024-12'):
    """coin -> {venue: symbol}. Discovery: Binance months 2023-05 -> 2026-06, the Bybit / OKX maps and
    HL probes from before 2026-07, ties broken on months <= 2024-12. Holdout (m-b): the point-in-time
    maps of the gated fetch (Binance funding files in the holdout window, Bybit and HL probed with
    bounded requests), ties broken on months <= the holdout end."""
    months = months if months is not None else F.binance_months()
    disc_months = {s: [m for m in ms if m <= tie_months_upto] for s, ms in months.items()}
    by = by if by is not None else (load(F.OUT / 'bybit' / 'symbols.json') or {})
    ok = ok if ok is not None else (load(F.OUT / 'okx' / 'symbols.json') or {})
    uni = {}
    for sym in sorted(months):
        coin = sym[:-4]
        u = {'BN': sym}
        if sym in by:
            u['BY'] = by[sym]
        if sym in ok:
            u['OK'] = ok[sym]
        h = hlmap.get(sym) if hlmap is not None else F.hl_coin_for(sym)
        if h:
            u['HL'] = h
        uni[coin] = u
    for v in ('BY', 'OK', 'HL'):
        claims = {}
        for coin, u in uni.items():
            if v in u:
                claims.setdefault(u[v], []).append(coin)
        for vs, coins in claims.items():
            if len(coins) < 2:
                continue
            exact = [c for c in coins if exact_name(v, c, vs)]
            win = exact[0] if len(exact) == 1 else max(coins, key=lambda c: (len(disc_months[c + 'USDT']), c))
            for c in coins:
                if c != win:
                    del uni[c][v]
                    print(f'  duplicate {v} symbol {vs}: kept {win}, dropped from {c}')
    return {c: u for c, u in uni.items() if len(u) >= 2}


def build(mode='disc'):
    """mode 'disc' (the registered discovery panel), or 'holdout' / 'extension' (after the gated
    fetch: same gate, a coverage check, then a <mode>_built row with the panel's SHA-256)."""
    assert F.OUT.resolve() == F.ALLOWED.resolve(), '010 reads only data/carry/ (000g whitelist)'
    commit = None
    if mode == 'disc':
        panel, f_lo, end, p_lo, p_end = F.OUT / 'panel', F.START, F.CUT, F.START, DISC_PRICE_END
        uni = universe()
        bn_months_of = F.binance_months()
    else:
        end, tag, commit = F.holdout_gate(mode, 'build')
        panel = F.OUT / ('panel_' + tag)
        f_lo = p_lo = F.HOLDOUT_FETCH_START
        p_end = end
        bn_months_of = json.loads((F.OUT / 'binance' / f'months_{tag}.json').read_text())
        uni = universe(bn_months_of, json.loads((F.OUT / 'bybit' / f'symbols_{tag}.json').read_text()), {},
                       json.loads((F.OUT / 'hl' / f'coins_{tag}.json').read_text()),
                       tie_months_upto=F.datetime.fromtimestamp((end - 1) / 1000, F.timezone.utc).strftime('%Y-%m'))
    panel.mkdir(parents=True, exist_ok=True)
    (panel / 'universe.json').write_text(json.dumps(uni, indent=0, sort_keys=True))
    print('coins on >= 2 venues', len(uni), {v: sum(v in u for u in uni.values()) for v in ('HL', 'BN', 'BY', 'OK')})
    fund, prices, path, vol, levels = [], [], [], {}, []

    def add_vol(v, c, day, q):
        vol[(v, c, day)] = vol.get((v, c, day), 0.0) + q
    for coin, u in uni.items():
        sym = u['BN']
        for m in bn_months_of.get(sym, []):
            f = load(F.OUT / 'binance' / 'fundingRate' / f'{sym}_{m}.json')
            if f:
                fund += [('BN', coin, hround(t), r) for t, _, r in keep(f['rows'], lambda r: r[0], end) if t >= f_lo]
            k = load(F.OUT / 'binance' / 'klines' / f'{sym}_8h_{m}.json')
            if k:
                for o, op, hi, lo, cl, qv in keep(k['rows'], lambda r: r[0], end):
                    tc = o + GRID
                    add_vol('BN', coin, o - o % DAY, qv)
                    if p_lo <= tc <= p_end:
                        path.append((coin, tc, hi, lo, cl))
                    if p_lo <= tc < p_end and cl > 0:
                        prices.append(('BN', coin, tc, math.log(cl), None))
                    if p_lo <= o < p_end and op > 0:
                        prices.append(('BN', coin, o, None, math.log(op)))      # the fill after the stamp at o (N6)
                    if tc % DAY in (0, 16 * HOUR) and tc < end:
                        levels.append(('BN', coin, tc, cl))
        if 'HL' in u:
            h = u['HL']
            spans = ('disc', 'hold') if HL_WITH_HOLD else ('disc',)
            files = [F.OUT / 'hl' / f'{h}_{s}.json' for s in spans] if mode == 'disc' else [F.OUT / 'hl' / f'{h}_h2_{end}.json']
            for fp in files:
                for t, r, pr in keep(load(fp) or [], lambda r: r[0], end):
                    if r == 0.0 and pr == 0.0:
                        continue                     # delisted HL coins keep printing exact zeros
                    if math.isfinite(r) and t >= f_lo:
                        fund.append(('HL', coin, hround(t), r))
            c0, c1 = (F.START, F.CUT) if mode == 'disc' else (F.HOLDOUT_FETCH_START, end)
            for o, op, hi, lo, cl, v in keep(load(F.OUT / 'hl' / 'candles' / f'{h}_8h_{c0}_{c1}.json') or [],
                                            lambda r: r[0], end):
                tc = o + GRID
                add_vol('HL', coin, o - o % DAY, v * cl)
                if p_lo <= tc < p_end and cl > 0:
                    prices.append(('HL', coin, tc, math.log(cl), None))
                if p_lo <= o < p_end and op > 0:
                    prices.append(('HL', coin, o, None, math.log(op)))
                if tc % DAY in (0, 16 * HOUR) and tc < end:
                    levels.append(('HL', coin, tc, cl))
        if 'BY' in u:
            s = u['BY']
            if mode == 'disc':
                rows = load(F.OUT / 'bybit' / 'funding' / f'{s}.json') or []
                k240 = F.OUT / 'bybit' / 'kline' / f'{s}_240_{F.START}_{DISC_PRICE_END}.json'
                kd = F.OUT / 'bybit' / 'kline' / f'{s}_D_{F.START}_{F.CUT}.json'
            else:
                rows = load(F.OUT / 'bybit' / 'funding' / f'{s}_{F.HOLDOUT_FETCH_START}_{end}.json') or []
                k240 = F.OUT / 'bybit' / 'kline' / f'{s}_240_{F.HOLDOUT_FETCH_START}_{end}.json'
                kd = F.OUT / 'bybit' / 'kline' / f'{s}_D_{F.HOLDOUT_FETCH_START}_{end}.json'
            fund += [('BY', coin, hround(t), r) for t, r in keep(rows, lambda r: r[0], end) if t >= f_lo]
            for r in keep(load(k240) or [], lambda r: r[0], end):
                tc = r[0] + 4 * HOUR
                if tc % GRID == 0 and p_lo <= tc < p_end and r[4] > 0:
                    prices.append(('BY', coin, tc, math.log(r[4]), None))
                if r[0] % GRID == 0 and p_lo <= r[0] < p_end and r[1] > 0:
                    prices.append(('BY', coin, r[0], None, math.log(r[1])))
            for r in keep(load(kd) or [], lambda r: r[0], end):
                add_vol('BY', coin, r[0], r[5])
                if r[0] + DAY < end:
                    levels.append(('BY', coin, r[0] + DAY, r[4]))
        if 'OK' in u and mode == 'disc':                    # OKX: reported variant only, discovery only
            fam = u['OK']
            for m in F.ym_list():
                f = load(F.OUT / 'okx' / 'funding' / f'{fam}_{m}.json')
                if f:
                    fund += [('OK', coin, hround(t), r) for t, r in keep(f['rows'], lambda r: r[0], end)]
            last = load(F.OUT / 'okx' / 'last' / f'{fam}-SWAP_4H_{F.START}_{DISC_PRICE_END}.json') or []
            for r in keep(last, lambda r: r[0], DISC_PRICE_END):
                tc = r[0] + 4 * HOUR
                if tc % GRID == 0 and tc < DISC_PRICE_END and r[4] > 0:
                    prices.append(('OK', coin, tc, math.log(r[4]), None))
                if r[0] % GRID == 0 and r[0] < DISC_PRICE_END and r[1] > 0:
                    prices.append(('OK', coin, r[0], None, math.log(r[1])))
            for r in keep(load(F.OUT / 'okx' / 'last' / f'{fam}-SWAP_1D_{F.START}_{F.CUT}.json') or [],
                          lambda r: r[0], end):
                add_vol('OK', coin, (r[0] + 8 * HOUR) - (r[0] + 8 * HOUR) % DAY, r[5])
                if r[0] + DAY < end:
                    levels.append(('OK', coin, r[0] + DAY, r[4]))
    fin = lambda x: x is None or math.isfinite(x)
    nf = sum(1 for r in fund if not math.isfinite(r[3])) + sum(1 for r in prices if not (fin(r[3]) and fin(r[4])))
    NONFINITE['dropped'] = nf
    fund = [r for r in fund if math.isfinite(r[3])]          # m-d: a NaN rate is a hole, never a zero
    pm = {}
    for v, c, t, lp, lpo in prices:
        if fin(lp) and fin(lpo):
            a = pm.setdefault((v, c, t), [None, None])
            a[0] = lp if lp is not None else a[0]
            a[1] = lpo if lpo is not None else a[1]
    if mode != 'disc':
        coverage_check(fund, end)
    write(panel, 'funding', ['venue VARCHAR', 'coin VARCHAR', 't BIGINT', 'rate DOUBLE'], sorted(set(fund)))
    write(panel, 'prices', ['venue VARCHAR', 'coin VARCHAR', 't BIGINT', 'lp DOUBLE', 'lpo DOUBLE'],
          sorted((v, c, t, a[0], a[1]) for (v, c, t), a in pm.items()))
    write(panel, 'path', ['coin VARCHAR', 't BIGINT', 'high DOUBLE', 'low DOUBLE', 'close DOUBLE'], sorted(set(path)))
    write(panel, 'volume', ['venue VARCHAR', 'coin VARCHAR', 'day BIGINT', 'qvol DOUBLE'],
          sorted((v, c, d, q) for (v, c, d), q in vol.items()))
    write(panel, 'levels', ['venue VARCHAR', 'coin VARCHAR', 't BIGINT', 'close DOUBLE'], sorted(set(levels)))
    if mode != 'disc':
        import r010_run as run
        run.append_row({'registration': run.REG.name, 'mode': f'{mode}_built', 'registration_commit': commit,
                        'panel_sha256': run.panel_hashes(panel), 'run_at': F.datetime.now(F.timezone.utc).isoformat()})
    print('rows: funding', len(set(fund)), 'prices', len(pm), 'path', len(set(path)), 'volume', len(vol),
          'levels', len(set(levels)))
    print('dropped at or after the panel end (count only):', DROPPED['rows_at_or_after_end'],
          '; non-finite values dropped:', NONFINITE['dropped'])


def coverage_check(fund, end, max_gap=3 * DAY):
    """Revision 2, N2: every venue-coin funding series must cover its life inside the window — no
    internal gap longer than 3 days between consecutive settlements — or the build stops (an
    incomplete fetch must not become 'delisted' outcomes)."""
    by = {}
    for v, c, t, r in fund:
        by.setdefault((v, c), []).append(t)
    bad = []
    for k, ts in by.items():
        ts = sorted(set(ts))
        g = max((b - a for a, b in zip(ts, ts[1:])), default=0)
        if g > max_gap:
            bad.append((k, g / DAY))
    if bad:
        sys.exit(f'coverage check failed: {len(bad)} venue-coin series have a gap > 3 days, e.g. {bad[:5]} — refusing to build')


def write(panel, name, cols, rows):
    """rows: sorted tuples; cols: 'name TYPE'. Written with pyarrow (deterministic for a fixed version)."""
    names = [c.split()[0] for c in cols]
    types = {'VARCHAR': pa.string(), 'BIGINT': pa.int64(), 'DOUBLE': pa.float64()}
    arrays = [pa.array([r[i] for r in rows], type=types[c.split()[1]]) for i, c in enumerate(cols)]
    pq.write_table(pa.table(arrays, names=names), panel / f'{name}.parquet')


if __name__ == '__main__':
    build('holdout' if '--holdout' in sys.argv else 'extension' if '--extension' in sys.argv else 'disc')
