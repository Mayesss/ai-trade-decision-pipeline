"""C1 scout: was each ever-top-20 Binance perp listed on Bitget USDT-M by its first top-20 month?

Existence probe only: Bitget v2 history-candles, granularity 1D, limit 1, endTime = first
top-20 month start (00:00 UTC), always < 2026-07-01. Keeps ONLY the open time of the
returned candle; OHLCV is discarded unread. Errors (e.g. removed symbols, 40309/40034)
are recorded as 'unknown', never as values, never cached as values. ~4 req/s.

Usage: python -I c1_bitget.py <first_top20_month.csv> <out_dir_new>
"""
import csv
import datetime as dt
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request

BG = "https://api.bitget.com/api/v2/mix/market/history-candles"
CUT = int(dt.datetime(2026, 7, 1, tzinfo=dt.UTC).timestamp() * 1000)


def probe(sym, end_ms, tries=4):
    if end_ms >= CUT:
        raise SystemExit("refuse: endTime at/after cut")
    url = f"{BG}?symbol={sym}&productType=USDT-FUTURES&granularity=1D&endTime={end_ms}&limit=1"
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                d = json.loads(r.read())
            if d.get("code") != "00000":
                return ("err", d.get("code"))
            rows = d.get("data") or []
            return ("ok", int(rows[0][0]) if rows else None)
        except urllib.error.HTTPError as e:
            if e.code == 429 and i < tries - 1:
                time.sleep(2 * (i + 1))
                continue
            if e.code == 403:
                return ("err", 403)  # never retried around
            try:
                return ("err", json.loads(e.read()).get("code"))
            except Exception:  # noqa: BLE001
                return ("err", e.code)
        except Exception as e:  # noqa: BLE001
            if i == tries - 1:
                return ("err", repr(e)[:60])
            time.sleep(2 * (i + 1))


def variants(sym):
    b = sym[:-4]
    out = [sym]
    for p in ("1000000", "1000", "1M"):
        if b.startswith(p) and len(b) > len(p):
            out.append(b[len(p):] + "USDT")
    if not b.startswith("1000"):
        out.append("1000" + b + "USDT")
    return out


def main():
    src, out = sys.argv[1], sys.argv[2]
    if os.path.exists(out):
        sys.exit("refuse: out dir exists")
    os.makedirs(out)
    res = []
    for sym, ym in csv.reader(open(src)):
        y, m = map(int, ym.split("-"))
        end = int(dt.datetime(y, m, 1, tzinfo=dt.UTC).timestamp() * 1000)
        row = {"symbol": sym, "first_top20_month": ym, "status": "unknown", "bitget_symbol": None, "candle_open": None}
        for v in variants(sym):
            kind, val = probe(v, end)
            time.sleep(0.25)
            if kind == "ok" and val is not None:
                row.update(status="listed" if end - val <= 3 * 86_400_000 else "stale_candle",
                           bitget_symbol=v, candle_open=dt.datetime.fromtimestamp(val / 1000, dt.UTC).date().isoformat())
                break
            if kind == "ok" and val is None:
                row["status"] = "no_candle_before"
            elif kind == "err" and row["status"] == "unknown":
                row["err"] = val
        res.append(row)
    p = os.path.join(out, "bitget_by_first_top20.json")
    with open(p, "w") as fh:
        json.dump(res, fh, indent=1)
    with open(os.path.join(out, "SHA256SUMS"), "w") as fh:
        fh.write(f"{hashlib.sha256(open(p, 'rb').read()).hexdigest()}  bitget_by_first_top20.json\n")
    c = {}
    for r in res:
        c[r["status"]] = c.get(r["status"], 0) + 1
    print(c)


if __name__ == "__main__":
    main()
