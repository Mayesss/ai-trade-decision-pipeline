"""N3 scout: Binance USDⓈ-M perp listing (and relisting/delisting) calendar from archive LISTINGS only.

BEHAVIOUR ONLY. Inputs are the L4 scout's S3 key listings (symbol, date, bytes, key) and the
2026-10-11 exchangeInfo snapshot. No file contents are read except exchangeInfo metadata.
Every key dated >= 2026-07-01 is dropped at load; onboard/delivery dates >= 2026-07-01 are dropped.

Usage:
  python -I n3_binance_events.py <listings_dir> <exchangeInfo.json> <out_dir>
Writes <out_dir>/binance_events.csv and prints count tables (counts only).

Definitions (fixed before any outcome):
  univ      : ^[A-Z0-9]+USDT$, minus symbols that today's exchangeInfo marks TRADIFI_PERPETUAL or a
              non-COIN underlyingType (TradFi perps start 2025-12-11).
  listing   : first daily 1d-kline file date. Dates <= 2020-01-01 are left-censored (archive start).
  delisting : last full (>= 4,000 B) daily metrics file, when it is before 2026-06-01 (L4 rule,
              117/120 exact vs exchangeInfo deliveryDate). Only defined from 2021-12.
  relisting : a full-metrics segment starting after a > 7-day hole (L4/009 segment rule).
Delisting is a data-coverage fact; it is never crossed with anything but coverage.
"""
import collections
import csv
import datetime as dt
import json
import os
import re
import sys

CUTOFF = "2026-07-01"
ARCHIVE_START = "2020-01-01"
FULL = 4000
GAP = 7
UNIV = re.compile(r"^[A-Z0-9]+USDT$")


def d(s):
    return dt.date.fromisoformat(s)


def load_listing(path, day=True):
    out = collections.defaultdict(list)
    with open(path) as f:
        for r in csv.DictReader(f):
            if r["date"] >= (CUTOFF if day else CUTOFF[:7]):
                continue
            out[r["symbol"]].append((r["date"], int(r["bytes"])))
    for v in out.values():
        v.sort()
    return out


def segments(days):
    seg = []
    for x in days:
        if seg and (d(x) - d(seg[-1][1])).days <= GAP:
            seg[-1][1] = x
        else:
            seg.append([x, x])
    return seg


def main():
    ldir, xinfo, out_dir = sys.argv[1], sys.argv[2], sys.argv[3]
    os.makedirs(out_dir, exist_ok=True)
    info = json.load(open(xinfo))
    excl, onboard, status = set(), {}, {}
    for s in info["symbols"]:
        if s["contractType"] == "TRADIFI_PERPETUAL" or (s.get("underlyingType") not in (None, "COIN")):
            excl.add(s["symbol"])
        if s["contractType"] == "PERPETUAL":
            ob = dt.datetime.fromtimestamp(s["onboardDate"] / 1000, dt.UTC)
            if ob.date().isoformat() < CUTOFF:
                onboard[s["symbol"]] = ob
            status[s["symbol"]] = s["status"]
    k1d = load_listing(os.path.join(ldir, "daily_klines_1d.csv"))
    met = load_listing(os.path.join(ldir, "daily_metrics.csv"))
    fr = load_listing(os.path.join(ldir, "monthly_fundingRate.csv"), day=False)
    k1h = load_listing(os.path.join(ldir, "monthly_klines_1h.csv"), day=False)
    pidx = load_listing(os.path.join(ldir, "monthly_premiumIndexKlines_1h.csv"), day=False)

    univ = sorted(s for s in k1d if UNIV.match(s) and s not in excl)
    rows = []
    for s in univ:
        first = k1d[s][0][0]
        full = [x for x, b in met.get(s, []) if b >= FULL]
        seg = segments(full)
        last_full = full[-1] if full else None
        delist = last_full if (last_full and last_full < "2026-06-01") else None
        full_set = set(full)
        ld = d(first)
        m14 = sum(1 for i in range(14) if (ld + dt.timedelta(days=i)).isoformat() in full_set)
        k1d_set = {x for x, _ in k1d[s]}
        k14 = sum(1 for i in range(14) if (ld + dt.timedelta(days=i)).isoformat() in k1d_set)
        months14 = sorted({(ld + dt.timedelta(days=i)).isoformat()[:7] for i in range(14)})
        fr_m = {x for x, _ in fr.get(s, [])}
        k1h_m = {x for x, _ in k1h.get(s, [])}
        pi_m = {x for x, _ in pidx.get(s, [])}
        ob = onboard.get(s)
        rows.append({
            "symbol": s, "event": "listing", "date": first, "censored": int(first <= ARCHIVE_START),
            "onboard": ob.isoformat() if ob else "", "status_today": status.get(s, "absent"),
            "first_full_metrics": full[0] if full else "", "metrics_d0": int(first in full_set),
            "metrics_full_14d": m14, "kline1d_14d": k14,
            "funding_months_ok": int(all(m in fr_m for m in months14)),
            "kline1h_months_ok": int(all(m in k1h_m for m in months14)),
            "premium1h_months_ok": int(all(m in pi_m for m in months14)),
            "n_segments": len(seg), "delist": delist or "",
        })
        for a, _b in seg[1:]:
            rows.append({"symbol": s, "event": "relisting", "date": a, "censored": 0, "onboard": "",
                         "status_today": status.get(s, "absent"), "first_full_metrics": a, "metrics_d0": 1,
                         "metrics_full_14d": sum(1 for i in range(14) if (d(a) + dt.timedelta(days=i)).isoformat() in full_set),
                         "kline1d_14d": "", "funding_months_ok": "", "kline1h_months_ok": "",
                         "premium1h_months_ok": "", "n_segments": len(seg), "delist": ""})
    assert all(r["date"] < CUTOFF for r in rows)
    with open(os.path.join(out_dir, "binance_events.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    L = [r for r in rows if r["event"] == "listing" and not r["censored"]]
    R = [r for r in rows if r["event"] == "relisting"]
    print(f"univ symbols {len(univ)}; left-censored (first file <= {ARCHIVE_START}) "
          f"{sum(r['censored'] for r in rows if r['event'] == 'listing')}; listing events {len(L)}; relistings {len(R)}")
    yr = collections.Counter(r["date"][:4] for r in L)
    wk = collections.defaultdict(set)
    perwk = collections.Counter()
    for r in L:
        iy, iw, _ = d(r["date"]).isocalendar()
        wk[r["date"][:4]].add((iy, iw))
        perwk[(iy, iw)] += 1
    halves = collections.Counter(r["date"][:4] + ("H1" if r["date"][5:7] <= "06" else "H2") for r in L)
    print("\nyear | listings | distinct ISO weeks with >=1 | max per week | relistings | delistings (rule)")
    dl = collections.Counter(r["delist"][:4] for r in rows if r["event"] == "listing" and r["delist"])
    rl = collections.Counter(r["date"][:4] for r in R)
    for y in sorted(set(yr) | set(dl)):
        mx = max((perwk[w] for w in wk[y]), default=0)
        print(f"{y} | {yr[y]} | {len(wk[y])} | {mx} | {rl[y]} | {dl[y]}")
    print("halves:", dict(sorted(halves.items())))
    # split windows
    def win(a, b):
        sel = [r for r in L if a <= r["date"] < b]
        ws = {d(r["date"]).isocalendar()[:2] for r in sel}
        return len(sel), len(ws)
    for name, a, b in [("2020-01-02..2021-11-30", "2020-01-02", "2021-12-01"),
                       ("009 discovery 2021-12-01..2023-12-31", "2021-12-01", "2024-01-01"),
                       ("holdout A 2024-01-01..2025-07-27", "2024-01-01", "2025-07-28"),
                       ("HL year 2025-07-28..2026-06-30", "2025-07-28", "2026-07-01")]:
        print(f"window {name}: listings, distinct weeks = {win(a, b)}")
    # point-in-time agreement with onboardDate
    agree = collections.Counter()
    for r in L:
        if r["onboard"]:
            gap = (d(r["date"]) - d(r["onboard"][:10])).days
            agree["same" if gap == 0 else "+1" if gap == 1 else "+2..7" if 2 <= gap <= 7 else ">7" if gap > 7 else "earlier"] += 1
        else:
            agree["no onboardDate (dropped from exchangeInfo)"] += 1
    print("\nfirst 1d-kline day minus onboardDate:", dict(agree))
    hours = collections.Counter(r["onboard"][11:16] for r in L if r["onboard"])
    print("onboardDate UTC time-of-day, top 8:", hours.most_common(8))
    # coverage
    print("\ncoverage (listing events; counts only):")
    for y in sorted(yr):
        S = [r for r in L if r["date"][:4] == y]
        print(f"{y}: n={len(S)} | metrics on day0 {sum(r['metrics_d0'] for r in S)} | metrics full 14/14 days "
              f"{sum(r['metrics_full_14d'] == 14 for r in S)} | 1d klines 14/14 {sum(r['kline1d_14d'] == 14 for r in S)} | "
              f"funding month(s) {sum(r['funding_months_ok'] for r in S)} | 1h klines month(s) "
              f"{sum(r['kline1h_months_ok'] for r in S)} | premium 1h month(s) {sum(r['premium1h_months_ok'] for r in S)}")
    print("relisting coverage: metrics full 14/14:", sum(r["metrics_full_14d"] == 14 for r in R), "of", len(R))


if __name__ == "__main__":
    main()
