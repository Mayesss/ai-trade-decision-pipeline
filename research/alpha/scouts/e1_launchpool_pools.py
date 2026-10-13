"""E1 scout: Binance Launchpool per-pool terms and pool sizes (completed projects).

Usage:  python3 -I e1_launchpool_pools.py OUTDIR

Keyless public endpoint bapi/earn/v1/friendly/launchpool/project/listV3,
paged with pageIndex. Kept per pool: project id, staked asset, reward coin,
reward token amount, total staked amount (the eligible pool size), number of
stakers, hourly reward cap, share ratio, farming start/end and trading time.
Dropped at load, never stored or printed:
  * every project whose farming start is on or after 2026-07-01 (counted only);
  * `annualRate` (Binance's posted APR: a yield figure; the registration must
    rebuild yield itself from reward amounts and a pre-registered price rule);
  * the `summary` block (a current, post-cutoff aggregate);
  * free-text abstracts.
"""
import json
import os
import sys
import time
import urllib.request

CUTOFF_MS = 1782864000000
URL = ("https://www.binance.com/bapi/earn/v1/friendly/launchpool/project/listV3"
       "?pageIndex={p}&pageSize=20")
UA = {"User-Agent": "Mozilla/5.0 (personal research; low rate)"}
POOL_KEEP = ("projectId", "projectName", "asset", "shareRatio", "status",
             "totalInvestAmount", "investUsers", "minInvestAmount",
             "rebateTotalAmount", "maximumHourlyReward")


def main():
    outdir = sys.argv[1]
    os.makedirs(outdir, exist_ok=False)
    kept, dropped, seen = [], 0, set()
    total = None
    p = 1
    while True:
        req = urllib.request.Request(URL.format(p=p), headers=UA)
        with urllib.request.urlopen(req, timeout=30) as r:
            d = json.loads(r.read().decode())
        if d.get("code") != "000000":
            raise RuntimeError("api error")
        comp = d["data"]["completed"]
        total = comp["total"]
        items = comp["list"]
        if not items:
            break
        for it in items:
            start = int(float(it.get("investStartTime") or 0))
            if start >= CUTOFF_MS:
                dropped += 1
                continue
            key = (it.get("rebateCoin"), start)
            if key in seen:
                continue
            seen.add(key)
            kept.append({
                "rebateCoin": it.get("rebateCoin"),
                "rebateTotalAmount": it.get("rebateTotalAmount"),
                "investStartTime": start,
                "mineEndTime": int(float(it.get("mineEndTime") or 0)),
                "coinTradeTime": it.get("coinTradeTime"),
                "duration": it.get("duration"),
                "pools": [{k: pr.get(k) for k in POOL_KEEP} for pr in it["projects"]],
            })
        p += 1
        time.sleep(2)
        if p > 10:
            break
    with open(os.path.join(outdir, "launchpool_pools.jsonl"), "w") as f:
        for row in sorted(kept, key=lambda r: r["investStartTime"]):
            f.write(json.dumps(row) + "\n")
    meta = {"completed_total_reported": total, "kept_pre_cutoff": len(kept),
            "dropped_on_or_after_2026_07_01_count_only": dropped, "pages": p - 1}
    with open(os.path.join(outdir, "meta.json"), "w") as f:
        json.dump(meta, f, indent=1)
    print(json.dumps(meta))


if __name__ == "__main__":
    main()
