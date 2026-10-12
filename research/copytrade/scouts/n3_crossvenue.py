"""N3 scout: was each Binance perp listing tradable on the owner's venues (Bitget, Hyperliquid) by day 1, and the
HL listing calendar. COUNTS ONLY.

  bitget <binance_events.csv> <out_dir_new>
      For each Binance listing 2023-01-01 -> 2026-06-30, for each candidate Bitget symbol (same name, and the
      name without a 1000/1M prefix): history-candles 1D, endTime = Binance listing day + 2 d (00:00 UTC), limit 1,
      and endTime = Binance listing day 00:00, limit 1. Keeps ONLY the returned candle's open time (existence);
      OHLCV is discarded unread. endTime asserted < 2026-07-01. Errors are recorded as errors, never as values.
  report <binance_events.csv> <bitget_dir> <hl_listing.json> <spot_keys.csv>
      Prints the counts tables (HL calendar per year; day-1 availability per Binance listing year; spot-before-perp).
"""
import collections
import csv
import datetime as dt
import hashlib
import json
import os
import sys
import time
import urllib.request

BG = "https://api.bitget.com/api/v2/mix/market/history-candles"
CUT = int(dt.datetime(2026, 7, 1, tzinfo=dt.UTC).timestamp() * 1000)
DAY = 86_400_000


def ms(day):
    return int(dt.datetime.fromisoformat(day).replace(tzinfo=dt.UTC).timestamp() * 1000)


def day_of(t):
    return dt.datetime.fromtimestamp(t / 1000, dt.UTC).date().isoformat()


def bg_first_before(sym, end_ms, tries=4):
    assert end_ms < CUT
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
            try:
                code = json.loads(e.read()).get("code")
            except Exception:  # noqa: BLE001
                code = e.code
            if e.code == 429 and i < tries - 1:
                time.sleep(2 * (i + 1))
                continue
            return ("err", code)
        except Exception as e:  # noqa: BLE001
            if i == tries - 1:
                return ("err", repr(e)[:80])
            time.sleep(2 * (i + 1))


def variants(sym):
    b = sym[:-4]
    out = [sym]
    for p in ("1000000", "10000", "1000", "1M"):
        if b.startswith(p) and len(b) > len(p):
            out.append(b[len(p):] + "USDT")
    if not b.startswith("1000"):
        out.append("1000" + b + "USDT")
    return out


def cmd_bitget(ev, out):
    assert not os.path.exists(out) or not os.listdir(out), "out dir must be new"
    os.makedirs(out, exist_ok=True)
    L = [r for r in csv.DictReader(open(ev)) if r["event"] == "listing" and r["censored"] == "0" and r["date"] >= "2023-01-01"]
    res = []
    for i, r in enumerate(L):
        d0 = ms(r["date"])
        rec = {"symbol": r["symbol"], "date": r["date"], "found": None, "by_day1": None, "before_day0": None, "errors": []}
        for v in variants(r["symbol"]):
            st, t = bg_first_before(v, min(d0 + 2 * DAY, CUT - 1))
            time.sleep(0.12)
            if st == "err":
                rec["errors"].append([v, t])
                continue
            if t is None:
                continue
            rec["found"] = v
            rec["by_day1"] = t <= d0 + DAY
            st2, t2 = bg_first_before(v, d0)
            time.sleep(0.12)
            rec["before_day0"] = (t2 is not None) if st2 == "ok" else None
            break
        res.append(rec)
        if (i + 1) % 100 == 0:
            print(f"  {i + 1}/{len(L)}", flush=True)
    p = os.path.join(out, "bitget_day1.json")
    json.dump(res, open(p, "w"), indent=0)
    open(os.path.join(out, "SHA256SUMS"), "w").write(f"{hashlib.sha256(open(p, 'rb').read()).hexdigest()}  bitget_day1.json\n")
    print("events", len(res), "found", sum(1 for x in res if x["found"]),
          "with errors only", sum(1 for x in res if not x["found"] and x["errors"]))


def hl_to_binance(name):
    if len(name) > 1 and name[0] == "k" and name[1].isupper():
        return ["1000" + name[1:] + "USDT"]
    return [name + "USDT", "1000" + name + "USDT", "1000000" + name + "USDT", "1M" + name + "USDT"]


def cmd_report(ev, bgdir, hlp, spot_keys):
    rows = list(csv.DictReader(open(ev)))
    L = [r for r in rows if r["event"] == "listing" and r["censored"] == "0"]
    bn = {r["symbol"]: r["date"] for r in L}
    hl = json.load(open(hlp))
    coins = [c for c in hl["coins"] if c["first_1d"] is not None]
    print(f"HL: coins in meta {len(hl['coins']) + len(hl['errors'])}; errors {len(hl['errors'])}; "
          f"with a 1d candle before 2026-07-01: {len(coins)}; no candle before cutoff: {sum(1 for c in hl['coins'] if c['first_1d'] is None)}")
    print("HL probes:", hl["probes"])
    yr = collections.Counter(day_of(c["first_1d"])[:4] for c in coins)
    wk = collections.defaultdict(set)
    for c in coins:
        d = dt.date.fromisoformat(day_of(c["first_1d"]))
        wk[str(d.year)].add(d.isocalendar()[:2])
    print("HL listings per year (first 1d candle) | distinct weeks:", {y: (yr[y], len(wk[y])) for y in sorted(yr)})
    first = min(c["first_1d"] for c in coins)
    print("HL earliest first candle:", day_of(first), "| coins whose first candle == earliest day:",
          sum(1 for c in coins if c["first_1d"] == first))
    fr = collections.Counter()
    for c in coins:
        n, wh = c.get("funding_rows_14d"), c.get("funding_window_h")
        fr["full 14d hourly funding rows (>= window-2)" if n is not None and wh and n >= wh - 2 else "partial/none"] += 1
    print("HL funding coverage from first candle:", dict(fr))
    fg = collections.Counter()
    for c in coins:
        if c.get("first_funding") is not None:
            g = (c["first_funding"] - c["first_1d"]) / 3_600_000
            fg["<=24h" if g <= 24 else "1-7d" if g <= 168 else ">7d"] += 1
    print("HL first funding row minus first 1d candle open:", dict(fg))
    hlwin = sum(1 for c in coins if "2025-07-28" <= day_of(c["first_1d"]) <= "2026-06-16")
    print("HL listings with first candle 2025-07-28..2026-06-16 (inside local every-account fills archive, +14 d room):", hlwin)
    # HL vs Binance ordering
    order = collections.Counter()
    hl_first = {}
    for c in coins:
        for s in hl_to_binance(c["coin"]):
            if s in bn:
                hl_first[s] = day_of(c["first_1d"])
    for c in coins:
        hd = day_of(c["first_1d"])
        m = [s for s in hl_to_binance(c["coin"]) if s in bn]
        if hd <= "2023-06-30":
            order["HL listed <= 2023-06 (HL launch era)"] += 1
        elif not m:
            order["no Binance USDT perp (to 2026-06-30)"] += 1
        else:
            gap = (dt.date.fromisoformat(hd) - dt.date.fromisoformat(bn[m[0]])).days
            order["HL >= 2 d BEFORE Binance perp" if gap <= -2 else "HL within +-1 d" if abs(gap) <= 1 else "HL >= 2 d after"] += 1
    print("HL listings vs Binance perp listing date (HL listings after 2023-06):", dict(order))
    # day-1 availability per Binance listing year
    bgres = {x["symbol"]: x for x in json.load(open(os.path.join(bgdir, "bitget_day1.json")))}
    tab = collections.defaultdict(collections.Counter)
    for r in L:
        y = r["date"][:4]
        if y < "2023":
            continue
        s, d0 = r["symbol"], dt.date.fromisoformat(r["date"])
        t = tab[y]
        t["n"] += 1
        h = hl_first.get(s)
        hl1 = h is not None and dt.date.fromisoformat(h) <= d0 + dt.timedelta(days=1)
        b = bgres.get(s, {})
        bg1 = bool(b.get("by_day1"))
        t["HL by day1"] += hl1
        t["Bitget by day1"] += bg1
        t["Bitget before day0"] += bool(b.get("before_day0"))
        t["either by day1"] += hl1 or bg1
        t["Bitget unknown (errors only)"] += (not b.get("found")) and bool(b.get("errors"))
    print("\nBinance listings: tradable on owner venues by Binance day 1 (point-in-time first-candle dates):")
    for y in sorted(tab):
        print(y, dict(tab[y]))
    # spot before perp
    sp = collections.defaultdict(set)
    first_spot = {}
    for r in csv.DictReader(open(spot_keys)):
        if r["kind"] == "spot_d1d":
            sp[r["symbol"]].add(r["date"])
        if r["kind"] == "spot_m1d":
            first_spot[r["symbol"]] = min(first_spot.get(r["symbol"], "9999"), r["date"])
    st = collections.defaultdict(collections.Counter)
    for r in L:
        y = r["date"][:4]
        s, d0 = r["symbol"], dt.date.fromisoformat(r["date"])
        prev = (d0 - dt.timedelta(days=1)).isoformat()
        t = st[y]
        t["n"] += 1
        if prev in sp.get(s, ()):
            t["spot trading day before perp"] += 1
        elif r["date"] in sp.get(s, ()):
            t["spot starts same day as perp"] += 1
        elif s in first_spot and first_spot[s] < r["date"][:7]:
            t["spot existed earlier but no file on day-1 (delisted/gap)"] += 1
        elif s in first_spot:
            t["spot starts after perp"] += 1
        else:
            t["no Binance spot pair found (USDT/BUSD/FDUSD/USDC/BTC/BNB)"] += 1
    print("\nBinance spot vs perp listing (archive first-file dates):")
    for y in sorted(st):
        print(y, dict(st[y]))


if __name__ == "__main__":
    {"bitget": cmd_bitget, "report": cmd_report}[sys.argv[1]](*sys.argv[2:])
