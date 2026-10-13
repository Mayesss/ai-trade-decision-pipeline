"""E1 scout: current-state metadata of the BNB perp on Hyperliquid and Bitget.

Usage:  python3 -I e1_bnb_perp_meta.py OUTDIR

Current-state only (no history, no series): existence, max leverage, open
interest and 24h notional volume in USD, and the USD depth within 10/50 bp of
mid from one order-book snapshot per venue. The mid itself is not printed or
stored. Keyless public endpoints.
"""
import json
import os
import sys
import time
import urllib.request

UA = {"User-Agent": "Mozilla/5.0 (personal research; low rate)", "Content-Type": "application/json"}


def get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30) as r:
        return json.loads(r.read().decode())


def post(url, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def depth_usd(bids, asks, bps):
    mid = (bids[0][0] + asks[0][0]) / 2
    lo, hi = mid * (1 - bps / 1e4), mid * (1 + bps / 1e4)
    b = sum(p * q for p, q in bids if p >= lo)
    a = sum(p * q for p, q in asks if p <= hi)
    spread_bp = (asks[0][0] - bids[0][0]) / mid * 1e4
    return round(b), round(a), round(spread_bp, 2)


def main():
    outdir = sys.argv[1]
    os.makedirs(outdir, exist_ok=False)
    res = {"fetched_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    # Hyperliquid main dex
    meta, ctxs = post("https://api.hyperliquid.xyz/info", {"type": "metaAndAssetCtxs"})
    hl = {"listed": False}
    for u, c in zip(meta["universe"], ctxs):
        if u["name"] == "BNB":
            mark = float(c["markPx"])
            hl = {"listed": True, "maxLeverage": u.get("maxLeverage"),
                  "onlyIsolated": u.get("onlyIsolated", False),
                  "isDelisted": u.get("isDelisted", False),
                  "open_interest_usd": round(float(c["openInterest"]) * mark),
                  "day_ntl_volume_usd": round(float(c["dayNtlVlm"]))}
    if hl["listed"]:
        time.sleep(1)
        book = post("https://api.hyperliquid.xyz/info", {"type": "l2Book", "coin": "BNB"})
        bids = [(float(x["px"]), float(x["sz"])) for x in book["levels"][0]]
        asks = [(float(x["px"]), float(x["sz"])) for x in book["levels"][1]]
        for bps in (10, 50):
            b, a, s = depth_usd(bids, asks, bps)
            hl[f"bid_usd_within_{bps}bp"], hl[f"ask_usd_within_{bps}bp"] = b, a
            hl["spread_bp"] = s
        hl["book_levels_per_side"] = (len(bids), len(asks))
    res["hyperliquid"] = hl
    # Bitget USDT-M
    time.sleep(1)
    bg = {"listed": False}
    c = get("https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES&symbol=BNBUSDT")
    if c.get("data"):
        d = c["data"][0]
        bg = {"listed": True, "symbolStatus": d.get("symbolStatus"),
              "maxLever": d.get("maxLever"), "makerFeeRate": d.get("makerFeeRate"),
              "takerFeeRate": d.get("takerFeeRate"),
              "fundInterval_h": d.get("fundInterval")}
        time.sleep(1)
        t = get("https://api.bitget.com/api/v2/mix/market/ticker?productType=USDT-FUTURES&symbol=BNBUSDT")["data"][0]
        bg["day_quote_volume_usd"] = round(float(t["usdtVolume"]))
        bg["open_interest_usd"] = round(float(t["holdingAmount"]) * float(t["lastPr"]))
        time.sleep(1)
        ob = get("https://api.bitget.com/api/v2/mix/market/merge-depth?productType=USDT-FUTURES&symbol=BNBUSDT&limit=max")["data"]
        bids = [(float(p), float(q)) for p, q in ob["bids"]]
        asks = [(float(p), float(q)) for p, q in ob["asks"]]
        for bps in (10, 50):
            b, a, s = depth_usd(bids, asks, bps)
            bg[f"bid_usd_within_{bps}bp"], bg[f"ask_usd_within_{bps}bp"] = b, a
            bg["spread_bp"] = s
        bg["book_levels_per_side"] = (len(bids), len(asks))
    res["bitget"] = bg
    with open(os.path.join(outdir, "bnb_perp_meta.json"), "w") as f:
        json.dump(res, f, indent=1)
    print(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()
