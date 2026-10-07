"""P3 — fills for the DISCOVERY period, needed columns only (plan §8, §12).

Holdout dates (2026-07-01 onward) are refused in code, not by convention.

    .venv/bin/python p3_fills.py --verify xyz:2025-11-15   # pruned == full, one small day
    .venv/bin/python p3_fills.py --dry-run [--dex xyz]      # exact bytes, from footers only
    .venv/bin/python p3_fills.py --cap-gib 75 [--dex xyz]   # the pull

Every byte is requester-pays transfer billed to the owner's AWS account. The
dry run fetches only Parquet footers (tens of KB per day); the pull checks the
planned total against --cap-gib before the first range request.
"""
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from ct import archive

HOLDOUT_START = '2026-07-01'
DEXES = {  # discovery period: archive start -> day before the holdout
    'hyperliquid': ('by_dex/hyperliquid/fills/perp/all/', '2025-07-28'),
    'xyz': ('by_dex/xyz/fills/perp/all/', '2025-10-13'),
}
# What replay + gates need (plan §12). Not taken: tx_hash (25% of a file),
# client_order_id (17%), order_id, fee, builder / liquidation detail columns.
COLUMNS = ['address', 'coin', 'timestamp', 'side', 'size', 'price', 'start_position',
           'realized_pnl', 'trade_id', 'direction', 'twap_id', 'is_liquidation', 'crossed', 'dex']
GIB = 1 << 30


def day_of(key):
    return key.split('date=')[1].split('/')[0]


def discovery_keys(dex):
    prefix, start = DEXES[dex]
    out = []
    for key, size in archive.list_prefix(prefix):
        day = day_of(key)
        if key.endswith('.parquet') and start <= day < HOLDOUT_START:
            out.append((key, size))
    assert all(day_of(k) < HOLDOUT_START for k, _ in out)
    return out


def verify(spec):
    """Download one day both ways and require identical columns."""
    import pyarrow.parquet as pq
    dex, day = spec.split(':')
    if day >= HOLDOUT_START:
        sys.exit('refusing: holdout date')
    prefix, _ = DEXES[dex]
    (key, size), = [(k, s) for k, s in archive.list_prefix(f'{prefix}date={day}/') if k.endswith('.parquet')]
    pruned_bytes = archive.download_columns(key, COLUMNS, size)
    full_bytes = archive.download(key, size)
    full = pq.read_table(archive.local_path(key), columns=COLUMNS)
    pruned = pq.read_table(archive.pruned_path(key))
    same = full.equals(pruned)
    print(f'{key}\n  object {size / 2**20:.1f} MiB, pruned transfer {pruned_bytes / 2**20:.2f} MiB '
          f'({100 * pruned_bytes / size:.0f}%), full transfer {full_bytes / 2**20:.1f} MiB')
    print(f'  rows {pruned.num_rows:,}, columns {pruned.num_columns}; identical to full file: {same}')
    archive.local_path(key).unlink()  # the full copy was only for the comparison
    if not same:
        sys.exit('VERIFY FAILED')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dex', choices=list(DEXES), action='append')
    ap.add_argument('--verify')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--cap-gib', type=float, default=75.0)
    ap.add_argument('--workers', type=int, default=8)
    args = ap.parse_args()
    if args.verify:
        return verify(args.verify)

    plan = []
    for dex in args.dex or list(DEXES):
        keys = [(k, s) for k, s in discovery_keys(dex) if not archive.pruned_path(k).exists()]
        print(f'{dex}: {len(keys)} discovery days to fetch ({day_of(keys[0][0]) if keys else "-"} .. '
              f'{day_of(keys[-1][0]) if keys else "-"}), objects {sum(s for _, s in keys) / GIB:.1f} GiB')
        plan += keys

    if args.dry_run:
        need = footers = 0
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for n, _, f in pool.map(lambda ks: archive.plan_columns(ks[0], COLUMNS, ks[1]), plan):
                need, footers = need + n, footers + f
        whole = sum(s for _, s in plan)
        print(f'exact pruned transfer: {need / GIB:.2f} GiB ({100 * need / whole:.0f}% of {whole / GIB:.1f} GiB); '
              f'footers read for this estimate: {footers / 2**20:.1f} MiB')
        return

    cap = int(args.cap_gib * GIB)
    whole = sum(s for _, s in plan)
    if whole * 0.6 > cap:  # pruned is ~40-45% of whole; run --dry-run for the exact figure
        sys.exit('refusing: plan likely exceeds the cap — run --dry-run for the exact figure')
    done = finished = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(archive.download_columns, k, COLUMNS, s) for k, s in plan]
        for fut in as_completed(futures):
            done += fut.result()
            finished += 1
            if done > cap:
                pool.shutdown(cancel_futures=True)
                sys.exit(f'stopping: cap reached ({done / GIB:.2f} GiB)')
            if finished % 10 == 0 or finished == len(plan):
                print(f'  {finished}/{len(plan)} days, {done / GIB:.2f} GiB')
    print(f'done: {done / GIB:.2f} GiB transferred this run')


if __name__ == '__main__':
    main()
