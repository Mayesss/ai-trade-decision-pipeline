"""H7 (measurement, not a trial) — extract HLP's own fills and per-minute marks
from the pruned archive. Read-only over data/pruned; writes data/derived/hyperliquid/h7/.

HLP = parent vault 0xdfc24b07…f303 (verified: the Hyperliquid docs' vaultDetails
example response, and stats-data's vault list naming it "Hyperliquidity
Provider (HLP)" with these seven protocol child vaults).

    python3 h7_extract.py fills      # every day not yet extracted
    python3 h7_extract.py funding    # userFunding per child, public API, cached
    python3 h7_extract.py vaults     # vaultDetails per child as served today, cached
    python3 h7_extract.py tvl        # DefiLlama's daily HLP TVL as served today, cached
Outputs of `fills`, one parquet per day:
    h7/fills/date=D.parquet   every HLP-child fill + the counterparty row's
                              address / direction / is_liquidation (same trade_id)
    h7/marks/date=D.parquet   last trade price per coin per UTC minute (whole tape)
"""
import sys
from datetime import date, datetime, timezone

import duckdb

from ct.hl import _info
from ct.net import DATA, cached, get_json

HLP = {
    '0xdfc24b077bc1425ad1dea75bcb6f8158e10df303': 'parent',
    '0x010461c14e146ac35fe42271bdc1134ee31c703a': 'strat_a',
    '0x31ca8395cf837de08b24da3f660e77761dfb974b': 'strat_b',
    '0x469f690213c467c39a23efacfd2816896009d7d8': 'strat_x',
    '0x2e3d94f0562703b25c83308a05046ddaf9a8dd14': 'liq_1',
    '0xb0a55f13d22f66e6d495ac98113841b2326e9540': 'liq_2',
    '0x5e177e5e39c0f4e421f5865a6d8beed8d921cb70': 'liq_3',
    '0x2ed5c4484ea3ff8b57d5f2fb152a40d9f2b68308': 'liq_4',
}
SRC = DATA / 'pruned' / 'by_dex' / 'hyperliquid' / 'fills' / 'perp' / 'all'
OUT = DATA / 'derived' / 'hyperliquid' / 'h7'


DAY_MS = 86_400_000
FUNDING_PAGE = 500  # userFunding returns at most 500 rows per request


def funding(verbose=True):
    """Every funding payment of every child over the archive span.

    Vaults' funding comes back aggregated per coin per UTC day (nSamples 24),
    so a window is one to a few days; a full page is split until it is not.
    """
    start = int(datetime(2025, 7, 28, tzinfo=timezone.utc).timestamp() * 1000)
    end = int(datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp() * 1000)

    def window(addr, t0, t1):
        rows = cached('hl-hlp-funding', f'{addr}:{t0}:{t1}',
                      lambda: _info({'type': 'userFunding', 'user': addr, 'startTime': t0, 'endTime': t1 - 1},
                                    items_per_weight=20))
        if len(rows) < FUNDING_PAGE or t1 - t0 <= DAY_MS:
            assert len(rows) < FUNDING_PAGE, (addr, t0)
            return rows
        mid = t0 + (t1 - t0) // 2 // DAY_MS * DAY_MS
        return window(addr, t0, mid) + window(addr, mid, t1)

    out = {}
    for addr, child in HLP.items():
        rows, t = [], start
        while t < end:
            rows += window(addr, t, min(t + 2 * DAY_MS, end))
            t += 2 * DAY_MS
        out[child] = rows
        if verbose:
            print(child, len(rows), round(sum(float(r['delta']['usdc']) for r in rows)), flush=True)
    return out


def vaults():
    for addr, child in HLP.items():
        d = cached('hl-hlp-vaults', f'{date.today()}:{addr}', lambda: _info({'type': 'vaultDetails', 'vaultAddress': addr}))
        print(child, d['name'])


def tvl():
    """Daily TVL denominator. Cross-checked against vaultDetails' accountValueHistory."""
    d = cached('defillama-hlp', str(date.today()), lambda: get_json('https://api.llama.fi/protocol/hyperliquid-hlp', timeout=120))
    rows = d['chainTvls']['Hyperliquid L1']['tvl']
    print(d['name'], len(rows))


def fills():
    (OUT / 'fills').mkdir(parents=True, exist_ok=True)
    (OUT / 'marks').mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET temp_directory='{DATA / 'duckdb_tmp'}'")
    con.execute('CREATE TABLE hlp(address VARCHAR, child VARCHAR)')
    con.executemany('INSERT INTO hlp VALUES (?, ?)', list(HLP.items()))
    days = sorted(p.name.split('=')[1] for p in SRC.iterdir() if p.name.startswith('date='))
    for i, day in enumerate(days):
        f_out, m_out = OUT / 'fills' / f'date={day}.parquet', OUT / 'marks' / f'date={day}.parquet'
        if f_out.exists() and m_out.exists():
            continue
        src = SRC / f'date={day}' / 'fills.parquet'
        con.execute(f"""
            COPY (
              WITH h AS (SELECT f.*, hlp.child FROM '{src}' f JOIN hlp USING (address))
              SELECT h.child, h.address, h.coin, h.timestamp, h.side, h.size::DOUBLE size,
                     h.price::DOUBLE price, h.start_position::DOUBLE start_position,
                     h.realized_pnl::DOUBLE realized_pnl, h.trade_id, h.direction,
                     h.is_liquidation, h.crossed,
                     o.address cp_address, o.direction cp_direction, o.is_liquidation cp_is_liquidation
              FROM h LEFT JOIN '{src}' o ON o.trade_id = h.trade_id AND o.address <> h.address
            ) TO '{f_out}' (FORMAT parquet)""")
        con.execute(f"""
            COPY (
              SELECT coin, date_trunc('minute', timestamp) AS ts,
                     arg_max(price::DOUBLE, timestamp) px, sum(price::DOUBLE * size::DOUBLE) / 2 notional
              FROM '{src}' GROUP BY ALL
            ) TO '{m_out}' (FORMAT parquet)""")
        print(day, f'{i + 1}/{len(days)}', flush=True)


if __name__ == '__main__':
    {'fills': fills, 'funding': funding, 'vaults': vaults, 'tvl': tvl}[sys.argv[1]]()
