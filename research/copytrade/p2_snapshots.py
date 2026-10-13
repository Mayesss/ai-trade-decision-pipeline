"""P2 — daily account snapshots (docs/copy-trade-plan-2026-10-07.md §8).

Downloads every snapshot day for the main Hyperliquid dex and `xyz`, all
columns (the files are small). Every byte is billed to the owner's AWS
account as requester-pays transfer, so:

- the full list of objects and sizes is fetched FIRST (LIST calls only) and
  the run refuses to start if the total exceeds --cap-gib;
- files already on disk with the right size are skipped (re-runs are free);
- the running total is checked against the cap before every file.

Holdout dates ARE included: snapshots carry no copy returns, and P4 needs
equity point-in-time across the whole period. Fills for the holdout are not
touched until P6.

    python3 p2_snapshots.py [--cap-gib 12] [--workers 8] [--dry-run]
"""
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from ct import archive

PREFIXES = {
    'hyperliquid': 'by_dex/hyperliquid/snapshots/perp/',
    'xyz': 'by_dex/xyz/snapshots/perp/',
}
GIB = 1 << 30


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cap-gib', type=float, default=12.0)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--workers', type=int, default=8)
    args = ap.parse_args()
    cap = int(args.cap_gib * GIB)

    plan = []
    for dex, prefix in PREFIXES.items():
        objs = [(k, s) for k, s in archive.list_prefix(prefix) if k.endswith('.parquet')]
        need = [(k, s) for k, s in objs if not (archive.local_path(k).exists()
                                                and archive.local_path(k).stat().st_size == s)]
        print(f'{dex:<12} {len(objs):>4} files {sum(s for _, s in objs) / GIB:6.2f} GiB, '
              f'to fetch {len(need):>4} ({sum(s for _, s in need) / GIB:6.2f} GiB)')
        plan += need
    total = sum(s for _, s in plan)
    print(f'transfer planned: {total / GIB:.2f} GiB (cap {args.cap_gib} GiB)')
    if total > cap:
        sys.exit('refusing: planned transfer exceeds the cap')
    if args.dry_run:
        return

    # The whole plan is already under the cap (checked above), so parallel
    # downloads cannot overshoot it; each file is fetched at most once per run.
    done, finished = 0, 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(archive.download, key, size) for key, size in plan]
        for fut in as_completed(futures):
            done += fut.result()
            finished += 1
            if finished % 25 == 0 or finished == len(plan):
                print(f'  {finished}/{len(plan)} files, {done / GIB:.2f} GiB')
    print(f'done: {done / GIB:.2f} GiB transferred this run')


if __name__ == '__main__':
    main()
