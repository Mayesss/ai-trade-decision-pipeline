"""N3 scout: Hyperliquid perp listing calendar and first-14-day coverage (counts only).

BEHAVIOUR ONLY. For every coin in the meta snapshot (path given):
  1. candleSnapshot 1d, startTime 2022-11-01, endTime 2026-06-30 23:59:59.999 -> keeps ONLY the candle
     open times (first, last, count). OHLCV values are discarded in memory without being read.
  2. fundingHistory from the first candle to min(first + 14 d, 2026-06-30 23:59:59.999) -> keeps ONLY the
     row count and the first row's time (coverage). Rates are discarded unread.
  3. Probe: candleSnapshot 1h / 1m for an old window (does the API serve old intraday candles?) -> count.
Every query's end is asserted < 2026-07-01. Any coin whose first candle is >= 2026-07-01 cannot appear
(the window ends before it). A failed request is recorded as an error and NOT written as a value.

Usage: python -I n3_hl_events.py <meta.json> <out_dir_new>
"""
import datetime as dt
import hashlib
import json
import os
import sys
import time
import urllib.request

URL = "https://api.hyperliquid.xyz/info"
END = int(dt.datetime(2026, 6, 30, 23, 59, 59, 999000, tzinfo=dt.UTC).timestamp() * 1000)
CUT = int(dt.datetime(2026, 7, 1, tzinfo=dt.UTC).timestamp() * 1000)
START = int(dt.datetime(2022, 11, 1, tzinfo=dt.UTC).timestamp() * 1000)
DAY = 86_400_000


def post(body, tries=6):
    for k in ("endTime",):
        if k in body:
            assert body[k] < CUT, body
    data = json.dumps(body).encode()
    for i in range(tries):
        try:
            req = urllib.request.Request(URL, data=data, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except Exception:  # noqa: BLE001
            if i == tries - 1:
                raise
            time.sleep(3 * (i + 1))


def main():
    meta_path, out = sys.argv[1], sys.argv[2]
    assert not os.path.exists(out) or not os.listdir(out), "out dir must be new"
    os.makedirs(out, exist_ok=True)
    uni = json.load(open(meta_path))["universe"]
    res, errs = [], []
    for i, c in enumerate(uni):
        name = c["name"]
        try:
            cs = post({"type": "candleSnapshot", "req": {"coin": name, "interval": "1d", "startTime": START, "endTime": END}})
            ts = sorted(int(x["t"]) for x in cs)
            assert all(t < CUT for t in ts)
            rec = {"coin": name, "isDelisted_today": bool(c.get("isDelisted")), "n_1d": len(ts),
                   "first_1d": ts[0] if ts else None, "last_1d": ts[-1] if ts else None}
            if ts:
                f_end = min(ts[0] + 14 * DAY, END)
                fh = post({"type": "fundingHistory", "coin": name, "startTime": ts[0], "endTime": f_end})
                ft = sorted(int(x["time"]) for x in fh)
                assert all(t < CUT for t in ft)
                rec.update(funding_rows_14d=len(ft), first_funding=ft[0] if ft else None,
                           funding_window_h=round((f_end - ts[0]) / 3_600_000))
            res.append(rec)
        except Exception as e:  # noqa: BLE001 - recorded, never cached as a value
            errs.append({"coin": name, "error": repr(e)[:200]})
        time.sleep(0.4)
        if (i + 1) % 50 == 0:
            print(f"  {i + 1}/{len(uni)}", flush=True)
    probes = []
    for coin, iv, a, b in [("BTC", "1h", "2024-01-01", "2024-01-03"), ("BTC", "1h", "2025-06-01", "2025-06-03"),
                           ("BTC", "1h", "2025-12-01", "2025-12-03"), ("BTC", "1m", "2026-06-01", "2026-06-02"),
                           ("BTC", "1m", "2026-03-01", "2026-03-02"), ("BTC", "15m", "2025-12-01", "2025-12-02")]:
        s = int(dt.datetime.fromisoformat(a).replace(tzinfo=dt.UTC).timestamp() * 1000)
        e = int(dt.datetime.fromisoformat(b).replace(tzinfo=dt.UTC).timestamp() * 1000) - 1
        cs = post({"type": "candleSnapshot", "req": {"coin": coin, "interval": iv, "startTime": s, "endTime": e}})
        probes.append({"coin": coin, "interval": iv, "from": a, "to": b, "rows": len(cs)})
    with open(os.path.join(out, "hl_listing.json"), "w") as f:
        json.dump({"coins": res, "errors": errs, "probes": probes, "end": END}, f, indent=0)
    with open(os.path.join(out, "SHA256SUMS"), "w") as f:
        h = hashlib.sha256(open(os.path.join(out, "hl_listing.json"), "rb").read()).hexdigest()
        f.write(f"{h}  hl_listing.json\n")
    print(f"coins {len(res)}, errors {len(errs)}, probes {probes}")


if __name__ == "__main__":
    main()
