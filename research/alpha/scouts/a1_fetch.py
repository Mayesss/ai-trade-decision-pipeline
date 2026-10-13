"""A1 scout (house-side products): coverage-only pulls of free, keyless,
END-DATED history. Every query carries an end bound before 2026-07-01 UTC;
any row the server returns at or after the cutoff is dropped in memory before
anything is written (and only its count is reported). Nothing here prints or
computes a price, NAV, APR, return or P&L value.

Usage (each pull into its own NEW directory; refuses an existing one):
  python -I a1_fetch.py lighter <outdir>
  python -I a1_fetch.py gmx <outdir>
  python -I a1_fetch.py jlp <outdir> [start_epoch]  (keyless CoinGecko: last 365 days only)
"""
import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request

CUTOFF = 1782864000          # 2026-07-01T00:00:00Z
END = CUTOFF - 1             # every query ends at 2026-06-30T23:59:59Z
UA = {"User-Agent": "a1-scout/0.1 (personal research)"}


def get(url, params=None, data=None):
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=dict(UA, **({"content-type": "application/json"} if data else {})),
                                 data=json.dumps(data).encode() if data else None)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def write(outdir, name, obj):
    p = os.path.join(outdir, name)
    with open(p, "w") as f:
        json.dump(obj, f, separators=(",", ":"))
    return p


def sums(outdir):
    lines = []
    for n in sorted(os.listdir(outdir)):
        if n == "SHA256SUMS":
            continue
        h = hashlib.sha256(open(os.path.join(outdir, n), "rb").read()).hexdigest()
        lines.append(f"{h}  {n}")
    open(os.path.join(outdir, "SHA256SUMS"), "w").write("\n".join(lines) + "\n")


def lighter(outdir):
    base = "https://mainnet.zklighter.elliot.ai/api/v1"
    cfg = get(base + "/systemConfig")
    llp = cfg.get("liquidity_pool_index")
    print("systemConfig keys:", sorted(cfg.keys()))
    print("liquidity_pool_index:", llp)
    pools = {"LLP": int(llp) if llp is not None else 281474976710654, "XLP": 281474976680784}
    for name, idx in pools.items():
        time.sleep(1.0)
        d = get(base + "/pnl", {"by": "index", "value": str(idx), "resolution": "1d",
                                "start_timestamp": "1735689600", "end_timestamp": str(END),
                                "count_back": "0", "ignore_transfers": "false"})
        rows = d.get("pnl", [])
        keep = [r for r in rows if int(r["timestamp"]) < CUTOFF]
        print(name, idx, "rows", len(rows), "dropped>=cutoff", len(rows) - len(keep),
              "fields", sorted(keep[0].keys()) if keep else None)
        write(outdir, f"lighter_{name}_pnl_1d.json", {"account_index": idx, "pnl": keep})


GMX_SQUID = "https://gmx.squids.live/gmx-synthetics-arbitrum:prod/api/graphql"
GMX_TOKENS = {
    "GLV_ETH_USDC": "0x528A5bac7E746C9A509A1f4F6dF58A03d44279F9",
    "GLV_WBTC_USDC": "0xdF03EEd325b82bC1d4Db8b49c30ecc9E05104b96",
    "GM_ETH_WETH_USDC": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
    "GM_BTC_WBTC_USDC": "0x47c031236e19d024b42f8AE6780E44A573170703",
}


def gmx_page(entity, where, fields, order):
    out, off = [], 0
    while True:
        q = "{ %s(where: %s, orderBy: %s, limit: 1000, offset: %d) { %s } }" % (entity, where, order, off, fields)
        d = get(GMX_SQUID, data={"query": q})
        if "errors" in d:
            raise SystemExit(f"{entity}: {d['errors'][0].get('message')}")
        rows = d["data"][entity]
        out += rows
        if len(rows) < 1000:
            return out
        off += 1000
        time.sleep(0.5)


def gmx(outdir):
    for name, addr in GMX_TOKENS.items():
        for key in (addr, addr.lower()):
            w = '{token_eq: "%s", isSnapshot_eq: true, snapshotTimestamp_lt: %d}' % (key, CUTOFF)
            rows = gmx_page("prices", w, "snapshotTimestamp type minPrice maxPrice", "snapshotTimestamp_ASC")
            if rows:
                break
        keep = [r for r in rows if r["snapshotTimestamp"] is not None and int(r["snapshotTimestamp"]) < CUTOFF]
        print(name, "price snapshots", len(keep), "types", sorted({r["type"] for r in keep}))
        write(outdir, f"gmx_{name}_price_snapshots.json", keep)
        for ent, flds in (("aprSnapshots", "snapshotTimestamp aprByFee aprByBorrowingFee entityType"),
                          ("pnlAprSnapshots", "snapshotTimestamp apr entityType")):
            for key in (addr, addr.lower()):
                w = '{address_eq: "%s", snapshotTimestamp_lt: %d}' % (key, CUTOFF)
                r2 = gmx_page(ent, w, flds, "snapshotTimestamp_ASC")
                if r2:
                    break
            k2 = [r for r in r2 if int(r["snapshotTimestamp"]) < CUTOFF]
            print(name, ent, len(k2))
            write(outdir, f"gmx_{name}_{ent}.json", k2)
        time.sleep(0.5)


def jlp(outdir, start="1672531200"):
    url = "https://api.coingecko.com/api/v3/coins/jupiter-perpetuals-liquidity-provider-token/market_chart/range"
    try:
        d = get(url, {"vs_currency": "usd", "from": start, "to": str(END)})
    except urllib.error.HTTPError as e:
        body = e.read()[:300]
        print("coingecko HTTP", e.code, body)
        write(outdir, "coingecko_error.json", {"code": e.code, "body": body.decode("utf-8", "replace")})
        return
    out = {}
    for k, v in d.items():
        if isinstance(v, list):
            keep = [p for p in v if int(p[0]) // 1000 < CUTOFF]
            print(k, "points", len(keep), "dropped>=cutoff", len(v) - len(keep))
            out[k] = keep
        else:
            print("non-list key", k)
    write(outdir, "coingecko_jlp_range.json", out)


if __name__ == "__main__":
    cmd, outdir = sys.argv[1], sys.argv[2]
    if os.path.exists(outdir):
        raise SystemExit(f"refusing: {outdir} exists (each pull needs a new directory)")
    os.makedirs(outdir)
    fn = {"lighter": lighter, "gmx": gmx, "jlp": jlp}[cmd]
    fn(outdir, *sys.argv[3:])
    sums(outdir)
    print("wrote", sorted(os.listdir(outdir)))
