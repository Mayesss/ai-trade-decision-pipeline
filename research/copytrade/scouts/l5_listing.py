"""L5a desk scout: byte counts for a 3-coin, 3-year pull (listing / HEAD only).

No market data is downloaded or read; no returns are computed.
Window: 2023-07-01 .. 2026-06-30 (36 months). Nothing dated >= 2026-07-01 is
counted or touched (keys are filtered by the date in the file name; Bybit HEADs
are only sent for dates inside the window).

Usage:
  python -I l5_listing.py binance <out.json>
  python -I l5_listing.py bybit   <out.json>
"""
import json
import re
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

BASE = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
COINS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
START, END = "2023-07", "2026-06"  # inclusive months
CUTOFF = "2026-07-01"
MONTH = re.compile(r"(\d{4}-\d{2})\.zip$")


def list_objects(prefix):
    marker = ""
    while True:
        q = {"prefix": prefix, "marker": marker}
        with urllib.request.urlopen(BASE + "?" + urllib.parse.urlencode(q), timeout=60) as r:
            root = ET.fromstring(r.read())
        last = None
        for c in root.findall(NS + "Contents"):
            last = c.find(NS + "Key").text
            yield last, int(c.find(NS + "Size").text)
        if root.find(NS + "IsTruncated").text != "true" or last is None:
            return
        marker = root.findtext(NS + "NextMarker") or last


def binance():
    out = {}
    sets = {
        "klines_1m": "data/futures/um/monthly/klines/{c}/1m/",
        "aggTrades": "data/futures/um/monthly/aggTrades/{c}/",
        "trades": "data/futures/um/monthly/trades/{c}/",
        "fundingRate": "data/futures/um/monthly/fundingRate/{c}/",
    }
    for name, tmpl in sets.items():
        for c in COINS:
            rows = []
            for k, s in list_objects(tmpl.format(c=c)):
                m = MONTH.search(k)
                if m and START <= m.group(1) <= END:
                    rows.append((m.group(1), s))
            rows.sort()
            out[f"{name}/{c}"] = {
                "months": len(rows),
                "first": rows[0][0] if rows else None,
                "last": rows[-1][0] if rows else None,
                "bytes_zip": sum(s for _, s in rows),
                "by_year": {y: sum(s for m, s in rows if m.startswith(y)) for y in ("2023", "2024", "2025", "2026")},
            }
            print(name, c, out[f"{name}/{c}"], flush=True)
    return out


def bybit():
    # public.bybit.com directory index has no sizes: HEAD a sample of days.
    days = ["2023-07-15", "2023-11-15", "2024-03-15", "2024-08-05", "2024-12-15",
            "2025-04-15", "2025-08-15", "2025-12-15", "2026-03-15", "2026-06-15"]
    assert all(d < CUTOFF for d in days)
    out = {}
    for c in COINS:
        sizes = {}
        for d in days:
            url = f"https://public.bybit.com/trading/{c}/{c}{d}.csv.gz"
            req = urllib.request.Request(url, method="HEAD")
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    sizes[d] = int(r.headers.get("Content-Length", -1))
            except Exception as e:  # noqa: BLE001
                sizes[d] = repr(e)
        ok = [v for v in sizes.values() if isinstance(v, int) and v > 0]
        out[c] = {"sample_bytes": sizes,
                  "mean_bytes_per_day": sum(ok) / len(ok) if ok else None,
                  "est_bytes_1096_days": sum(ok) / len(ok) * 1096 if ok else None}
        print(c, out[c], flush=True)
    return out


if __name__ == "__main__":
    res = {"binance": binance, "bybit": bybit}[sys.argv[1]]()
    with open(sys.argv[2], "w") as f:
        json.dump(res, f, indent=1)
