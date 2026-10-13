"""P3 gates on the pruned archive fills — all must pass before any copy return
is computed (plan §8 P3).

G1  partitions are UTC: every timestamp lies inside its date partition
G2  dex column: main-dex files hold only main-dex rows, xyz files only xyz
G3  execution order within a millisecond: the startPosition chain per
    (address, coin) in FILE order vs a (timestamp, trade_id) sort
G4  realized-PnL self-check: for every flat -> flat round trip inside a day,
    the exact cash flow (sum of -signed size x price) equals the summed
    realized_pnl the archive reports
G5  archive vs API: for a few addresses on a late discovery day, the API's
    fills match the archive's trade ids and fields

    .venv/bin/python p3_checks.py [--sample-days 6] [--dex hyperliquid]
G1-G2 read the timestamp / dex columns of every day; G3-G5 use sample days.
"""
import argparse
import datetime as dt
import random
from collections import defaultdict

import pyarrow.compute as pc
import pyarrow.parquet as pq

from ct import archive_fills as af
from ct import hl

DAY = 86_400_000


def days_on_disk(dex):
    root = af.day_file(dex, 'x').parent.parent
    return sorted(p.name.split('=')[1] for p in root.glob('date=*') if (p / 'fills.parquet').exists())


def g1_g2(dex, days):
    bad_utc, bad_dex, values = [], [], set()
    for day in days:
        t = pq.read_table(af.day_file(dex, day), columns=['timestamp', 'dex'])
        lo = dt.datetime.fromisoformat(day).replace(tzinfo=dt.UTC)
        mm = pc.min_max(t.column('timestamp')).as_py()
        if mm['min'] < lo or mm['max'] >= lo + dt.timedelta(days=1):
            bad_utc.append(day)
        vals = set(pc.unique(t.column('dex')).to_pylist())
        values |= vals
        if len(vals) != 1:
            bad_dex.append(day)
    print(f'  G1 UTC partitions: {len(days) - len(bad_utc)}/{len(days)} days clean'
          + (f'; off: {bad_utc[:5]}' if bad_utc else ''))
    print(f'  G2 dex values seen: {sorted(values)}; days with mixed values: {len(bad_dex)}')
    return not bad_utc and not bad_dex and len(values) == 1


def chain_breaks(seq):
    pos, breaks = {}, 0
    for f in seq:
        key = f['coin']
        if key in pos and abs(pos[key] - f['startPosition']) > 1e-9 * max(1.0, abs(f['startPosition'])):
            breaks += 1
        pos[key] = f['startPosition'] + (f['sz'] if f['side'] == 'B' else -f['sz'])
    return breaks


def sample_addresses(dex, day, n, seed, min_fills=1, max_fills=None):
    """n random addresses with fill counts in range — chosen in Arrow, so a
    6.8 M-row main-dex day is never turned into Python objects whole."""
    counts = pc.value_counts(pq.read_table(af.day_file(dex, day), columns=['address']).column('address'))
    pool = sorted(c['values'] for c in counts.to_pylist()
                  if c['counts'] >= min_fills and (max_fills is None or c['counts'] <= max_fills))
    return random.Random(seed).sample(pool, min(n, len(pool)))


def g3_g4(dex, day, max_addresses=3000, seed=11):
    addrs = sample_addresses(dex, day, max_addresses, seed)
    by_addr = af.to_api(af.read_day(dex, day, addresses=addrs))
    file_breaks = sorted_breaks = n_fills = 0
    trips = agree = 0
    worst = []
    for a in addrs:
        fills = by_addr[a]
        n_fills += len(fills)
        file_breaks += chain_breaks(fills)
        sorted_breaks += chain_breaks(sorted(fills, key=lambda f: (f['time'], f['tid'])))
        # G4: flat -> flat round trips within the day, per coin, file order.
        # Tolerance is relative to the POSITION (0.1% of its largest notional),
        # not to the PnL: Hyperliquid's reported realized PnL uses a rounded
        # entry price, so on very cheap coins traded in millions of units it
        # drifts from the exact cash flow by up to ~0.05% of notional — which
        # can be a large share of a small PnL (measured 2026-10-07: misses at a
        # 1%-of-PnL tolerance were 17% of round trips below $0.01, 0.3% above
        # $1). The exact cash flow is what the replay uses.
        open_ = {}
        for f in fills:
            c = f['coin']
            signed = f['sz'] if f['side'] == 'B' else -f['sz']
            after = f['startPosition'] + signed
            if abs(f['startPosition']) < 1e-12:
                open_[c] = [0.0, 0.0, 0.0]  # [cash flow, reported realized, max notional]
            if c not in open_:
                continue
            open_[c][0] -= signed * f['px']
            open_[c][1] += f['closedPnl']
            open_[c][2] = max(open_[c][2], abs(after) * f['px'], abs(f['startPosition']) * f['px'])
            if abs(after) < 1e-9 * max(1.0, abs(f['startPosition'])):
                cash, reported, notional = open_.pop(c)
                trips += 1
                if abs(cash - reported) <= max(0.01, 0.001 * notional):
                    agree += 1
                else:
                    worst.append((round(abs(cash - reported), 4), a[:10], c, round(cash, 4), round(reported, 4)))
    print(f'  {day}: G3 chain breaks in {n_fills:,} fills — file order {file_breaks:,}, '
          f'(timestamp, trade_id) sort {sorted_breaks:,}; '
          f'G4 round trips agreeing {agree:,}/{trips:,}' + (f'; worst {sorted(worst)[-1]}' if worst else ''))
    return file_breaks, sorted_breaks, n_fills, agree, trips


def g5(dex, day, n=8, seed=5):
    picked = sample_addresses(dex, day, n, seed, min_fills=5, max_fills=300)
    by_addr = af.to_api(af.read_day(dex, day, addresses=picked))
    start = int(dt.datetime.fromisoformat(day).replace(tzinfo=dt.UTC).timestamp() * 1000)
    # The API keeps only recent fills per wallet: an empty API answer for an
    # old day means "not retained", not "archive wrong". Those are counted
    # separately and do not count as comparisons.
    ok = compared = not_retained = 0
    for a in picked:
        api = [f for f in hl._paged('userFillsByTime', a, start, start + DAY - 1)
               if (f['coin'].startswith('xyz:') if dex == 'xyz' else ':' not in f['coin'])
               and not f['coin'].startswith('@') and '/' not in f['coin']]
        arch = {f['tid']: f for f in by_addr[a]}
        api_t = {f['tid']: f for f in api}
        if not api_t:
            not_retained += 1
            print(f'    {a[:10]}: archive {len(arch)} fills, API none (not retained)')
            continue
        compared += 1
        same_ids = set(arch) == set(api_t)
        fields = all(abs(float(api_t[t]['px']) - arch[t]['px']) < 1e-9 * arch[t]['px']
                     and abs(float(api_t[t]['sz']) - arch[t]['sz']) < 1e-12 + 1e-9 * arch[t]['sz']
                     and abs(float(api_t[t]['startPosition']) - arch[t]['startPosition']) < 1e-6
                     for t in set(arch) & set(api_t))
        ok += same_ids and fields
        print(f'    {a[:10]}: archive {len(arch)} fills, API {len(api_t)}; same trade ids {same_ids}; '
              f'fields match {fields}')
    print(f'  G5 {day}: {ok}/{compared} compared addresses identical; {not_retained} not retained by the API')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dex', choices=list(af.PREFIX), action='append')
    ap.add_argument('--sample-days', type=int, default=6)
    args = ap.parse_args()
    for dex in args.dex or list(af.PREFIX):
        days = days_on_disk(dex)
        print(f'\n== {dex}: {len(days)} days on disk')
        if not days:
            continue
        g1_g2(dex, days)
        step = max(1, len(days) // args.sample_days)
        totals = defaultdict(int)
        for day in days[::step][:args.sample_days]:
            fb, sb, nf, ag, tr = g3_g4(dex, day)
            for k, v in zip(('file', 'sorted', 'fills', 'agree', 'trips'), (fb, sb, nf, ag, tr)):
                totals[k] += v
        print(f'  G3 total: file order {totals["file"]:,} breaks vs sorted {totals["sorted"]:,} '
              f'in {totals["fills"]:,} fills; G4 total {totals["agree"]:,}/{totals["trips"]:,}')
        g5(dex, days[-1])


if __name__ == '__main__':
    main()
