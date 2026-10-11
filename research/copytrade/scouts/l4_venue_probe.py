"""L4 scout: probe how far back free public REST history goes (Bybit, OKX, Binance).

Every request carries an explicit end time BEFORE 2026-07-01 (holdout rule).
Prints only HTTP status, row counts and first/last timestamps of each window;
no value (OI, ratio, funding, price) is printed or saved.

Usage: ../.venv/bin/python -I l4_venue_probe.py <bybit|okx|binance>
"""
import datetime as dt
import json
import sys
import time
import urllib.parse
import urllib.request

CUTOFF_MS = int(dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)


def ms(y, m, d):
    return int(dt.datetime(y, m, d, tzinfo=dt.timezone.utc).timestamp() * 1000)


def iso(t):
    return dt.datetime.fromtimestamp(int(t) / 1000, dt.timezone.utc).strftime("%Y-%m-%d %H:%M")


def get(url, params):
    for k in ("endTime", "end", "after"):
        if k in params:
            assert int(params[k]) < CUTOFF_MS, f"{k} not before holdout cutoff"
    full = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(full, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read()[:300]
        try:
            return e.code, json.loads(body)
        except Exception:  # noqa: BLE001
            return e.code, body.decode("utf-8", "replace")


def report(tag, status, rows, tkey):
    if not isinstance(rows, list):
        print(f"{tag}: http {status} non-list -> {str(rows)[:160]}")
        return
    ts = sorted(int(r[tkey] if isinstance(r, dict) else r[tkey]) for r in rows)
    span = f"{iso(ts[0])} .. {iso(ts[-1])}" if ts else "-"
    print(f"{tag}: http {status} rows={len(rows)} span={span}")
    if ts:
        assert ts[-1] < CUTOFF_MS


WINDOWS = [(ms(2019, 6, 1), ms(2019, 12, 31)), (ms(2020, 6, 1), ms(2020, 6, 30)),
           (ms(2021, 6, 1), ms(2021, 6, 30)), (ms(2022, 6, 1), ms(2022, 6, 30)),
           (ms(2023, 6, 1), ms(2023, 6, 30)), (ms(2024, 6, 1), ms(2024, 6, 30)),
           (ms(2025, 6, 1), ms(2025, 6, 30)), (ms(2026, 6, 1), ms(2026, 6, 30))]


def bybit():
    b = "https://api.bybit.com/v5/market/"
    for s, e in WINDOWS:
        st, j = get(b + "open-interest", dict(category="linear", symbol="BTCUSDT", intervalTime="1d",
                                              startTime=s, endTime=e, limit=200))
        report(f"bybit OI 1d {iso(s)[:7]}", st, (j.get("result") or {}).get("list", j) if isinstance(j, dict) else j, "timestamp")
        st, j = get(b + "open-interest", dict(category="linear", symbol="BTCUSDT", intervalTime="5min",
                                              startTime=s, endTime=s + 86400000, limit=200))
        report(f"bybit OI 5min {iso(s)[:7]}", st, (j.get("result") or {}).get("list", j) if isinstance(j, dict) else j, "timestamp")
        st, j = get(b + "funding/history", dict(category="linear", symbol="BTCUSDT", startTime=s, endTime=e, limit=200))
        report(f"bybit funding {iso(s)[:7]}", st, (j.get("result") or {}).get("list", j) if isinstance(j, dict) else j, "fundingRateTimestamp")
        st, j = get(b + "account-ratio", dict(category="linear", symbol="BTCUSDT", period="1d",
                                              startTime=s, endTime=e, limit=500))
        report(f"bybit L/S 1d {iso(s)[:7]}", st, (j.get("result") or {}).get("list", j) if isinstance(j, dict) else j, "timestamp")
        time.sleep(0.3)


def okx():
    b = "https://www.okx.com/api/v5/"
    for s, e in WINDOWS:
        # funding-rate-history: 'after' = return records older than this ts
        st, j = get(b + "public/funding-rate-history", dict(instId="BTC-USDT-SWAP", after=e, limit=100))
        report(f"okx funding <{iso(e)[:10]}", st, j.get("data", j) if isinstance(j, dict) else j, "fundingTime")
        st, j = get(b + "rubik/stat/contracts/open-interest-history",
                    dict(instId="BTC-USDT-SWAP", period="1D", end=e, limit=100))
        rows = j.get("data", j) if isinstance(j, dict) else j
        report(f"okx OI-hist 1D <{iso(e)[:10]}", st, rows, 0)
        st, j = get(b + "rubik/stat/contracts/long-short-account-ratio-contract",
                    dict(instId="BTC-USDT-SWAP", period="1D", end=e, limit=100))
        report(f"okx L/S 1D <{iso(e)[:10]}", st, j.get("data", j) if isinstance(j, dict) else j, 0)
        st, j = get(b + "rubik/stat/contracts/open-interest-volume", dict(ccy="BTC", period="1D", end=e))
        report(f"okx OI-vol ccy 1D <{iso(e)[:10]}", st, j.get("data", j) if isinstance(j, dict) else j, 0)
        time.sleep(0.5)


def binance():
    b = "https://fapi.binance.com/futures/data/"
    for path in ("openInterestHist", "topLongShortPositionRatio", "globalLongShortAccountRatio",
                 "takerlongshortRatio"):
        for s, e in [(ms(2026, 6, 1), ms(2026, 6, 30)), (ms(2026, 6, 25), ms(2026, 6, 30))]:
            st, j = get(b + path, dict(symbol="BTCUSDT", period="1d", startTime=s, endTime=e, limit=30))
            report(f"binance {path} {iso(s)[:10]}..{iso(e)[:10]}", st, j, "timestamp")
    st, j = get("https://fapi.binance.com/fapi/v1/fundingRate",
                dict(symbol="BTCUSDT", startTime=ms(2019, 9, 1), endTime=ms(2019, 12, 31), limit=1000))
    report("binance fundingRate 2019-09..12", st, j, "fundingTime")


if __name__ == "__main__":
    {"bybit": bybit, "okx": okx, "binance": binance}[sys.argv[1]]()
