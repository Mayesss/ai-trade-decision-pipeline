"""N2 scout: behaviour-only analysis. Every number in scouts/n2-options.md comes from here.

BEHAVIOUR ONLY. Allowed: unconditional frequencies; contemporaneous trade-vs-mark costs;
coincident (same-snapshot / same-hour) association of map/stress state with implied-vol LEVEL
and skew; unconditional distributions of underlying moves and of DVOL changes.
FORBIDDEN and not computed here: any pairing of IV / option prices / map / stress with a LATER
realised vol, price move, option payoff or P&L; any split of counts by a future quantity.
No row dated >= 2026-07-01 is read (asserted); no Deribit/underlying row inside 009's holdout A
(2024-01-01 -> 2025-07-27) exists in the scout_n2 pulls; the Binance panel is cut at 2024-01-01.

Usage: ../.venv/bin/python -I n2_analyze.py <section> [...]
  sections: costs, hlmap, stress, proxy, coincident, power, tail, all
Paths are fixed below relative to research/copytrade/data (passed implicitly, read with -I).
"""
import bisect
import datetime as dt
import json
import math
import pathlib
import re
import statistics as st
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import n2_net as net  # noqa: E402

DATA = pathlib.Path(__file__).resolve().parent.parent / "data"
N2 = DATA / "scout_n2"
HL = DATA / "derived/hyperliquid"
BY = DATA / "binance_years/panel"
CUT = net.CUTOFF_MS
HOLD_A = (net.ms(2024, 1, 1), net.ms(2025, 7, 28))
HL_START = net.ms(2025, 7, 28)
H, DAY = 3_600_000, 86_400_000
MON = {m: i for i, m in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split(), 1)}


# ---------- small maths (no numpy) ----------
def ncdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def b76(F, K, T, s, call):
    if T <= 0 or s <= 0:
        return max(F - K, 0) if call else max(K - F, 0)
    d1 = (math.log(F / K) + 0.5 * s * s * T) / (s * math.sqrt(T))
    d2 = d1 - s * math.sqrt(T)
    return F * ncdf(d1) - K * ncdf(d2) if call else K * ncdf(-d2) - F * ncdf(-d1)


def delta(F, K, T, s, call):
    d1 = (math.log(F / K) + 0.5 * s * s * T) / (s * math.sqrt(T))
    return ncdf(d1) if call else ncdf(d1) - 1


def implied(px_usd, F, K, T, call):
    lo, hi = 0.01, 6.0
    if not (b76(F, K, T, lo, call) <= px_usd <= b76(F, K, T, hi, call)):
        return None
    for _ in range(60):
        mid = 0.5 * (lo + hi)
        if b76(F, K, T, mid, call) < px_usd:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def parse(name):
    m = re.match(r"^(BTC|ETH)-(\d{1,2})([A-Z]{3})(\d{2})-(\d+(?:d\d+)?)-([CP])$", name)
    if not m:
        return None
    cur, d, mon, yy, k, cp = m.groups()
    exp = net.ms(2000 + int(yy), MON[mon], int(d), 8)
    return cur, exp, float(k.replace("d", ".")), cp == "C"


def q(v, ps):
    v = sorted(v)
    if not v:
        return [None] * len(ps)
    return [v[min(len(v) - 1, max(0, int(round(p * (len(v) - 1)))))] for p in ps]


def rank(v):
    o = sorted(range(len(v)), key=lambda i: v[i])
    r = [0.0] * len(v)
    i = 0
    while i < len(o):
        j = i
        while j + 1 < len(o) and v[o[j + 1]] == v[o[i]]:
            j += 1
        for k in range(i, j + 1):
            r[o[k]] = (i + j) / 2
        i = j + 1
    return r


def spearman(x, y):
    if len(x) < 5:
        return None
    rx, ry = rank(x), rank(y)
    mx, my = st.mean(rx), st.mean(ry)
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    den = math.sqrt(sum((a - mx) ** 2 for a in rx) * sum((b - my) ** 2 for b in ry))
    return num / den if den else None


def f(x, p=2):
    return "-" if x is None else f"{x:.{p}f}"


# ---------- enrich option trades with mark IV, delta, tenor ----------
def enrich(tr):
    p = parse(tr["instrument_name"])
    if not p:
        return None
    cur, exp, K, call = p
    T = (exp - tr["timestamp"]) / (365 * DAY)
    F = tr["index_price"]
    if T <= 0 or tr["mark_price"] <= 0:
        return None
    s = implied(tr["mark_price"] * F, F, K, T, call)
    if s is None:
        return None
    return {"cur": cur, "exp": exp, "K": K, "call": call, "T": T, "days": T * 365, "F": F,
            "mark_iv": s * 100, "delta": delta(F, K, T, s, call), **tr}


# ---------- 1. costs ----------
def costs():
    print("## costs: trade price vs contemporaneous mark (per trade; taker direction = Deribit 'direction')")
    files = sorted((N2 / "deribit_trades_sample").glob("*.json"))
    agg = {}
    for fp in files:
        cur, day = fp.stem.split("_")
        for tr in json.loads(fp.read_text()):
            assert tr["timestamp"] < CUT
            e = enrich(tr)
            if not e or e["days"] > 7.5:
                continue
            ad = abs(e["delta"])
            if not 0.05 <= ad <= 0.45:
                continue
            ten = "0-1d" if e["days"] <= 1 else ("1-3d" if e["days"] <= 3 else "3-7d")
            db = "05-15d" if ad < 0.15 else ("15-30d" if ad < 0.30 else "30-45d")
            sg = 1 if tr["direction"] == "buy" else -1
            hs_pct = sg * (tr["price"] - tr["mark_price"]) / tr["mark_price"] * 100
            hs_iv = sg * (tr["iv"] - e["mark_iv"]) if tr.get("iv") else None
            fee = min(0.0003, 0.125 * tr["price"]) / tr["price"] * 100
            prem_usd = tr["price"] * e["F"]
            yr = day[:4]
            per = "calm" if day not in ("2022-11-09", "2025-10-10") else "stress-day"
            for key in ((cur, ten, db), (cur, "all", "all", per)):
                a = agg.setdefault(key, {"hs": [], "iv": [], "fee": [], "prem": [], "n": 0, "days": set()})
                a["hs"].append(hs_pct)
                if hs_iv is not None:
                    a["iv"].append(hs_iv)
                a["fee"].append(fee)
                a["prem"].append(prem_usd)
                a["n"] += 1
                a["days"].add(day)
    print("| cur | tenor | |delta| | trades | eff. half-spread % of premium p25/p50/p75 | in IV pts p25/p50/p75 | taker fee % of premium p50 | premium $ p50 |")
    print("|---|---|---|---|---|---|---|---|")
    for k in sorted(agg):
        a = agg[k]
        hs, iv, fe, pr = q(a["hs"], (.25, .5, .75)), q(a["iv"], (.25, .5, .75)), q(a["fee"], (.5,))[0], q(a["prem"], (.5,))[0]
        lab = k if len(k) == 3 else (k[0], k[3], "1-7d, 0.05-0.45")
        print(f"| {lab[0]} | {lab[1]} | {lab[2]} | {a['n']} | {f(hs[0])} / {f(hs[1])} / {f(hs[2])} | "
              f"{f(iv[0])} / {f(iv[1])} / {f(iv[2])} | {f(fe)} | {f(pr, 0)} |")
    # volume / depth proxies per sample day (counts only)
    print("\nper sample day: options trades with <=7.5 d to expiry and 0.05<=|delta|<=0.45 (count, contracts)")
    for fp in files:
        n, c = 0, 0.0
        for tr in json.loads(fp.read_text()):
            e = enrich(tr)
            if e and e["days"] <= 7.5 and 0.05 <= abs(e["delta"]) <= 0.45:
                n += 1
                c += tr["amount"]
        print(f"  {fp.stem}: {n} trades, {c:.0f} contracts")


# ---------- 2. HL map frequencies ----------
def hl_map_rows(c):
    return c.execute(f"""
        SELECT snapshot_ms, coin,
               MAX(density) FILTER (WHERE side='L' AND d=0.02) AS dl2,
               MAX(density) FILTER (WHERE side='S' AND d=0.02) AS ds2,
               MAX(density) FILTER (WHERE side='L' AND d=0.01) AS dl1,
               MAX(density) FILTER (WHERE side='S' AND d=0.01) AS ds1,
               MAX(gross) AS gross
        FROM read_parquet('{HL}/map_levels.parquet')
        WHERE snapshot_ms < {CUT} AND coin IN ('BTC','ETH')
        GROUP BY 1, 2 ORDER BY 1, 2""").fetchall()


def hlmap():
    c = duckdb.connect()
    rows = hl_map_rows(c)
    print("## HL map: share of the coin's gross notional with a liquidation price within 1% / 2% (+0.5%) of the snapshot price")
    print(f"snapshots: {len({r[0] for r in rows})}, {net.iso(min(r[0] for r in rows))} .. {net.iso(max(r[0] for r in rows))}")
    for coin in ("BTC", "ETH"):
        rr = [r for r in rows if r[1] == coin]
        for lab, idx in (("long-side 2%", 2), ("short-side 2%", 3), ("long-side 1%", 4), ("short-side 1%", 5)):
            v = [r[idx] for r in rr if r[idx] is not None]
            print(f"  {coin} {lab}: n {len(v)} deciles(%) {[round(x * 100, 2) for x in q(v, [i / 10 for i in range(1, 10)])]} max {max(v) * 100:.2f}")
        both = [(r[2] or 0) + (r[3] or 0) for r in rr]
        for thr in (0.01, 0.02, 0.05, 0.10):
            nl = sum(1 for r in rr if (r[2] or 0) >= thr)
            ns = sum(1 for r in rr if (r[3] or 0) >= thr)
            print(f"  {coin} days with long-side 2% density >= {thr:.0%}: {nl}/{len(rr)}; short-side: {ns}/{len(rr)}")
        t1, t2 = q(both, (1 / 3, 2 / 3))
        print(f"  {coin} combined (L+S) 2% density terciles: {t1 * 100:.2f}% / {t2 * 100:.2f}% "
              f"(dense = top third, sparse = bottom third, by construction 1/3 each)")
        g = [r[6] for r in rr]
        print(f"  {coin} HL gross notional per snapshot ($M) p10/p50/p90: {[round(x / 1e6) for x in q(g, (.1, .5, .9))]}")


# ---------- 3. HL stress frequencies ----------
HLP = ("0xdfc24b077bc1425ad1dea75bcb6f8158e10df303", "0x010461c14e146ac35fe42271bdc1134ee31c703a",
       "0x31ca8395cf837de08b24da3f660e77761dfb974b", "0x469f690213c467c39a23efacfd2816896009d7d8",
       "0x2e3d94f0562703b25c83308a05046ddaf9a8dd14", "0xb0a55f13d22f66e6d495ac98113841b2326e9540",
       "0x5e177e5e39c0f4e421f5865a6d8beed8d921cb70", "0x2ed5c4484ea3ff8b57d5f2fb152a40d9f2b68308")


def five_min_buckets(c, ex_hlp=False):
    w = f" AND lower(address) NOT IN {HLP}" if ex_hlp else ""
    return c.execute(f"""SELECT (t // 300000) * 300000 AS b, SUM(notional) FROM read_parquet('{HL}/liquidations.parquet')
                         WHERE t < {CUT}{w} GROUP BY 1 ORDER BY 1""").fetchall()


def stress():
    c = duckdb.connect()
    for ex in (False, True):
        stress_one(c, ex)


def stress_one(c, ex):
    b = five_min_buckets(c, ex)
    v = [n for _, n in b]
    print("## HL market-wide liquidation notional per clock 5-min bucket (all coins; "
          + ("EXCLUDING HLP's own auto-deleveraged rows)" if ex else "all liquidation rows, as in 004)"))
    print(f"quantiles over NON-ZERO buckets ($M): p99 {q(v, (.99,))[0] / 1e6:.1f} p99.9 {q(v, (.999,))[0] / 1e6:.1f}")
    span_days = (b[-1][0] - b[0][0]) / DAY
    nb = int(span_days * 288)
    print(f"buckets with any liquidation: {len(b)} of ~{nb}; span {net.iso(b[0][0])} .. {net.iso(b[-1][0])}")
    allv = v + [0.0] * (nb - len(v))
    print(f"quantiles over all buckets ($M): p50 {q(allv, (.5,))[0] / 1e6:.3f} p90 {q(allv, (.9,))[0] / 1e6:.2f} "
          f"p99 {q(allv, (.99,))[0] / 1e6:.1f} p99.9 {q(allv, (.999,))[0] / 1e6:.1f} max {max(v) / 1e6:.0f}")
    days = {}
    for t, n in b:
        days.setdefault(t // DAY, []).append(n)
    nd = int(span_days) + 1
    for thr in (5e6, 15e6, 77e6):
        hit = sorted(d for d, x in days.items() if max(x) >= thr)
        nbk = sum(1 for x in v if x >= thr)
        print(f"  >= ${thr / 1e6:.0f}M: {nbk} buckets on {len(hit)} of {nd} days"
              + (f": {[dt.datetime.fromtimestamp(d * 86400, dt.timezone.utc).strftime('%m-%d') for d in hit]}" if len(hit) <= 40 else ""))
    # weeks with any >= $15M bucket
    wk = {}
    for d, x in days.items():
        wk.setdefault((d + 3) // 7, []).append(max(x))
    for thr in (15e6, 77e6):
        print(f"  ISO-weeks with any bucket >= ${thr / 1e6:.0f}M: {sum(1 for x in wk.values() if max(x) >= thr)} of {len(wk)}")


# ---------- 4. multi-year proxies (Binance 2021-11 -> 2023-12, Bybit 2020-08 -> 2023-12) ----------
def proxy():
    c = duckdb.connect()
    end = HOLD_A[0]
    print("## Binance USDT-M panel, cut at 2024-01-01 (holdout A unread)")
    # market-wide OI change, price-neutral: sum_i oiv_{h-1,i} * (oi_h/oi_{h-1} - 1) / sum oiv_{h-1}
    rows = c.execute(f"""
      WITH m AS (SELECT symbol, ts, oi_sum / n AS oi, oiv_sum / n AS oiv FROM read_parquet('{BY}/metrics_1h.parquet')
                 WHERE ts < {end} AND n >= 10),
           j AS (SELECT a.ts, a.symbol, b.oiv AS w, a.oi / b.oi - 1 AS g FROM m a JOIN m b
                 ON a.symbol = b.symbol AND a.ts = b.ts + {H} WHERE b.oi > 0 AND a.oi > 0),
           k AS (SELECT *, LEAD(g) OVER (PARTITION BY symbol ORDER BY ts) AS g1,
                           LEAD(ts) OVER (PARTITION BY symbol ORDER BY ts) AS t1 FROM j)
      -- per-symbol artifact: a drop <= -3% that is >= 80% undone in the next hour is dropped
      SELECT ts, SUM(w * g) / SUM(w), COUNT(*) FROM k
      WHERE NOT (g <= -0.03 AND t1 = ts + {H} AND (1 + g) * (1 + g1) - 1 >= 0.2 * g)
      GROUP BY 1 ORDER BY 1""").fetchall()
    # Archive artifacts: whole-market OI "drops" of 8-100% that fully revert within the hour after
    # (e.g. 2022-03-07 16:00 -99.7%). A data-cleaning flag on the signal series itself (the drop
    # is undone in the OI data, not in any price or option outcome); counts are shown raw and clean.
    art = set()
    for i in range(1, len(rows) - 1):
        g0, g1 = rows[i][1], rows[i + 1][1]
        if rows[i + 1][0] - rows[i][0] == H and g0 <= -0.03 and (1 + g0) * (1 + g1) - 1 >= g0 * 0.2:
            art.add(rows[i][0])
    # partial-coverage hours (symbol count < 80% of the trailing-24h median) are dropped too
    cnt = [r[2] for r in rows]
    for i in range(24, len(rows)):
        if cnt[i] < 0.8 * st.median(cnt[i - 24:i]):
            art.add(rows[i][0])
    print(f"artifact / partial-coverage hours dropped: {len(art)}")
    raw_rows = rows
    rows = [r for r in rows if r[0] not in art]
    g = [r[1] for r in rows]
    print(f"market-wide hourly OI change: {len(rows)} hours {net.iso(rows[0][0])} .. {net.iso(rows[-1][0])}, "
          f"symbols/hour p50 {q([r[2] for r in rows], (.5,))[0]}")
    print(f"  quantiles (%): p0.1 {q(g, (.001,))[0] * 100:.2f} p1 {q(g, (.01,))[0] * 100:.2f} p50 {q(g, (.5,))[0] * 100:.3f} "
          f"p99 {q(g, (.99,))[0] * 100:.2f}")
    by_year = {}
    for ts, x, _ in rows:
        y = dt.datetime.fromtimestamp(ts / 1000, dt.timezone.utc).year
        by_year.setdefault(y, []).append((ts, x))
    for thr in (-0.02, -0.03, -0.05):
        cells = []
        for y, v in sorted(by_year.items()):
            hrs = [t for t, x in v if x <= thr]
            cells.append(f"{y}: {len(hrs)} h / {len({t // DAY for t in hrs})} d")
        print(f"  hours with market-wide OI change <= {thr:.0%}: " + "; ".join(cells))
    # BTC / ETH: OI drop + same-hour |return| (contemporaneous) ; taker imbalance; L/S; OI build-up
    for sym in ("BTCUSDT", "ETHUSDT"):
        r = c.execute(f"""
          WITH m AS (SELECT ts, oi_sum / n AS oi, global_ratio FROM read_parquet('{BY}/metrics_1h.parquet')
                     WHERE symbol = '{sym}' AND ts < {end} AND n >= 10),
               b AS (SELECT ts, open, close, qv, tbq FROM read_parquet('{BY}/bars_1h.parquet') WHERE symbol = '{sym}' AND ts < {end})
          SELECT m.ts, m.oi, m.global_ratio, b.close / b.open - 1, b.tbq / NULLIF(b.qv, 0)
          FROM m JOIN b USING (ts) ORDER BY 1""").fetchall()
        ts = [x[0] for x in r]
        oi = [x[1] for x in r]
        dlt = [None] + [(oi[i] / oi[i - 1] - 1) if ts[i] - ts[i - 1] == H and oi[i - 1] > 0 else None
                        for i in range(1, len(r))]
        for i in range(1, len(r) - 1):   # same artifact rule, per symbol
            if dlt[i] is not None and dlt[i + 1] is not None and dlt[i] <= -0.03 and \
                    (1 + dlt[i]) * (1 + dlt[i + 1]) - 1 >= dlt[i] * 0.2:
                dlt[i] = None
        if any(x == 0 for x in oi):
            print(f"  {sym}: {sum(1 for x in oi if x == 0)} zero-OI hours dropped")
        ev = [i for i in range(len(r)) if dlt[i] is not None and dlt[i] <= -0.03 and abs(r[i][3]) >= 0.02]
        ev5 = [i for i in range(len(r)) if dlt[i] is not None and dlt[i] <= -0.05]
        print(f"  {sym}: {len(r)} hours; deleveraging hours (OI <= -3% and same-hour |ret| >= 2%): {len(ev)} on "
              f"{len({ts[i] // DAY for i in ev})} days; OI <= -5% in an hour: {len(ev5)} on {len({ts[i] // DAY for i in ev5})} days")
        # OI build-up vs trailing 30d mean (past only)
        bu = []
        for i in range(720, len(r)):
            if dlt[i] is None and i and oi[i] < 0.5 * oi[i - 1]:
                continue
            m30 = sum(oi[i - 720:i]) / 720
            if m30 > 0 and oi[i] > 0:
                bu.append(oi[i] / m30)
        print(f"    OI / trailing-30d mean: p10/p50/p90/p99 {[round(x, 3) for x in q(bu, (.1, .5, .9, .99))]}; "
              f"hours >= 1.15: {sum(1 for x in bu if x >= 1.15)}, >= 1.25: {sum(1 for x in bu if x >= 1.25)} of {len(bu)}")
        ls = [x[2] for x in r if x[2]]
        tk = [x[4] for x in r if x[4] is not None]
        print(f"    global long/short account ratio p1/p10/p50/p90/p99 {[round(x, 2) for x in q(ls, (.01, .1, .5, .9, .99))]}")
        print(f"    hourly taker-buy share of quote volume p0.1/p1/p50/p99/p99.9 {[round(x, 3) for x in q(tk, (.001, .01, .5, .99, .999))]}")
    fr = c.execute(f"""SELECT symbol, COUNT(*), COUNT(*) FILTER (WHERE rate >= 0.0005), COUNT(*) FILTER (WHERE rate <= -0.0005)
                       FROM read_parquet('{BY}/funding.parquet') WHERE symbol IN ('BTCUSDT','ETHUSDT') AND ts < {end} GROUP BY 1""").fetchall()
    for s_, n, hi, lo in fr:
        print(f"  {s_} funding settlements: {n}; >= +0.05%: {hi}; <= -0.05%: {lo}")
    # Bybit 2020-08 -> 2023-12 BTC/ETH: OI drop hours, with Deribit perp same-hour return
    print("## Bybit linear OI 1h (2020-08 -> 2023-12) x Deribit perpetual same-hour return")
    for sym, inst in (("BTCUSDT", "BTC-PERPETUAL"), ("ETHUSDT", "ETH-PERPETUAL")):
        oi = dict(json.loads((N2 / f"bybit_oi_1h/{sym}_oi_1h.json").read_text()))
        px = {r[0]: r for r in json.loads((N2 / f"deribit_perp_1h/{inst}_1h.json").read_text())}
        ks = sorted(oi)
        by = {}
        for a, b_ in zip(ks, ks[1:]):
            if b_ - a != H or b_ not in px or oi[a] <= 0:
                continue
            g_ = oi[b_] / oi[a] - 1
            ret = px[b_][4] / px[b_][1] - 1
            y = dt.datetime.fromtimestamp(b_ / 1000, dt.timezone.utc).year
            d = by.setdefault(y, {"n": 0, "ev": set(), "evh": 0, "oi5": 0})
            d["n"] += 1
            if g_ <= -0.03 and abs(ret) >= 0.02:
                d["evh"] += 1
                d["ev"].add(b_ // DAY)
            if g_ <= -0.05:
                d["oi5"] += 1
        print(f"  {sym}: " + "; ".join(f"{y}: {d['n']} h, delev hours {d['evh']} on {len(d['ev'])} d, OI<=-5% {d['oi5']} h"
                                     for y, d in sorted(by.items())))


# ---------- 5. coincident pricing ----------
def load_dvol(cur):
    rows = json.loads((N2 / f"deribit_dvol_1h/dvol_{cur}_1h.json").read_text())
    assert all(r[0] < CUT for r in rows)
    return rows


def coincident():
    c = duckdb.connect()
    print("## coincident: DVOL / skew at the map snapshot (past-only: last hourly DVOL close at or before the snapshot)")
    maprows = hl_map_rows(c)
    for coin in ("BTC", "ETH"):
        dv = load_dvol(coin)
        closes_t = [r[0] + H for r in dv]   # an hourly row closes 1 h after its stamp
        # trailing 30-day median of DVOL closes, past-only
        rr = [r for r in maprows if r[1] == coin]
        X = []
        for sm, _, dl2, ds2, dl1, ds1, _g in rr:
            i = bisect.bisect_right(closes_t, sm) - 1
            if i < 720:
                continue
            lvl = dv[i][4]
            rel = lvl / st.median(x[4] for x in dv[i - 720:i + 1])
            X.append((sm, dl2 or 0, ds2 or 0, lvl, rel))
        both = [x[1] + x[2] for x in X]
        print(f"  {coin}: n {len(X)} snapshots")
        for lab, xs in (("long-side 2%", [x[1] for x in X]), ("short-side 2%", [x[2] for x in X]), ("L+S 2%", both)):
            print(f"    Spearman(density {lab}, DVOL level) {f(spearman(xs, [x[3] for x in X]))}; "
                  f"(density, DVOL / own trailing-30d median) {f(spearman(xs, [x[4] for x in X]))}")
        t1, t2 = q(both, (1 / 3, 2 / 3))
        for lab, sel in (("sparse (bottom third)", lambda b: b <= t1), ("middle", lambda b: t1 < b < t2), ("dense (top third)", lambda b: b >= t2)):
            v = [x for x, b in zip(X, both) if sel(b)]
            print(f"    {lab}: n {len(v)}, DVOL p50 {f(q([x[3] for x in v], (.5,))[0], 1)}, DVOL/30d-median p50 {f(q([x[4] for x in v], (.5,))[0], 3)}")
    # skew at snapshot from the hour of trades BEFORE each snapshot (mark IV inverted per trade)
    print("## coincident: skew proxy from Deribit marks in the hour before each snapshot")
    for coin in ("BTC", "ETH"):
        tr = json.loads((N2 / f"deribit_snapwin_1h/{coin}_snapwin.json").read_text())
        by = {}
        for x in tr:
            assert x["timestamp"] < x["snap"] < CUT
            e = enrich(x)
            if not e or not 2 <= e["days"] <= 14:
                continue
            by.setdefault(x["snap"], []).append(e)
        dens = {r[0]: ((r[2] or 0), (r[3] or 0)) for r in maprows if r[1] == coin}
        S = []
        for sm, es in by.items():
            exps = sorted({e["exp"] for e in es}, key=lambda ex: abs((ex - sm) / DAY - 7))
            for ex in exps:
                ee = [e for e in es if e["exp"] == ex]
                puts = [e["mark_iv"] for e in ee if not e["call"] and -0.40 <= e["delta"] <= -0.10]
                calls = [e["mark_iv"] for e in ee if e["call"] and 0.10 <= e["delta"] <= 0.40]
                atm = [e["mark_iv"] for e in ee if 0.40 <= abs(e["delta"]) <= 0.60]
                if puts and calls and atm:
                    S.append((sm, st.median(calls) - st.median(puts), st.median(atm), (ex - sm) / DAY))
                    break
        S = [s for s in S if s[0] in dens]
        print(f"  {coin}: snapshots with a usable 2-14 d expiry: {len(S)} of {len(by)}; tenor used p50 {f(q([s[3] for s in S], (.5,))[0], 1)} d")
        rrv = [s[1] for s in S]
        print(f"    RR proxy (call 10-40d IV minus put 10-40d IV, vol pts) p10/p50/p90 {[round(x, 2) for x in q(rrv, (.1, .5, .9))]}")
        dl = [dens[s[0]][0] for s in S]
        ds = [dens[s[0]][1] for s in S]
        print(f"    Spearman(long-side 2% density, RR) {f(spearman(dl, rrv))}; (short-side 2% density, RR) {f(spearman(ds, rrv))}; "
              f"(long minus short density, RR) {f(spearman([a - b for a, b in zip(dl, ds)], rrv))}")
        print(f"    Spearman(L+S density, ATM mark IV) {f(spearman([a + b for a, b in zip(dl, ds)], [s[2] for s in S]))}")
    # stress vs DVOL in the same hour
    print("## coincident: HL market-wide liquidation stress vs DVOL in the same clock hour")
    b = five_min_buckets(c)
    hmax = {}
    for t, n in b:
        hmax[t // H * H] = max(hmax.get(t // H * H, 0), n)
    for coin in ("BTC", "ETH"):
        dv = [r for r in load_dvol(coin) if HL_START <= r[0] < CUT]
        lv, ch, sv = [], [], []
        groups = {"<$5M": [], "$5-15M": [], "$15-77M": [], ">=$77M": []}
        for r in dv:
            s_ = hmax.get(r[0], 0.0)
            k = "<$5M" if s_ < 5e6 else ("$5-15M" if s_ < 15e6 else ("$15-77M" if s_ < 77e6 else ">=$77M"))
            groups[k].append((r[4], r[4] - r[1]))
            lv.append(r[4]); ch.append(r[4] - r[1]); sv.append(s_)
        print(f"  {coin}: {len(dv)} hours; Spearman(hour max 5-min stress, DVOL close) {f(spearman(sv, lv))}; "
              f"(stress, same-hour DVOL close-open) {f(spearman(sv, ch))}")
        for k, v in groups.items():
            if v:
                print(f"    {k}: {len(v)} h, DVOL close p50 {f(q([x[0] for x in v], (.5,))[0], 1)}, same-hour change p50 "
                      f"{f(q([x[1] for x in v], (.5,))[0])} pts, p90 {f(q([x[1] for x in v], (.9,))[0])}")


# ---------- 6. power: unconditional DVOL changes and underlying payoff shape ----------
def perp_series(inst):
    rows = json.loads((N2 / f"deribit_perp_1h/{inst}_1h.json").read_text())
    assert all(r[0] < CUT and not (HOLD_A[0] <= r[0] < HOLD_A[1]) for r in rows)
    return rows


def weekly_moves(rows):
    """Non-overlapping 7-day close-to-close log moves (Fri 08:00 UTC to Fri 08:00), plus the
    worst intra-week excursion from the start price, within contiguous spans only."""
    px = {r[0]: r for r in rows}
    out = []
    t = min(px)
    t = t + ((4 - dt.datetime.fromtimestamp(t / 1000, dt.timezone.utc).weekday()) % 7) * DAY
    t = t // DAY * DAY + 8 * H
    while t + 7 * DAY in px or t + 7 * DAY <= max(px):
        if t in px and t + 7 * DAY - H in px:
            hrs = [px.get(t + k * H) for k in range(7 * 24)]
            if all(hrs):
                s0 = hrs[0][1]
                s1 = hrs[-1][4]
                lo = min(h[3] for h in hrs) / s0 - 1
                hi = max(h[2] for h in hrs) / s0 - 1
                out.append((t, s1 / s0 - 1, lo, hi))
        t += 7 * DAY
    return out


def power():
    print("## power inputs (unconditional)")
    for coin in ("BTC", "ETH"):
        dv = load_dvol(coin)
        byday = {}
        for r in dv:
            byday[(r[0] // DAY)] = r[4]
        ks = sorted(byday)
        d1 = [byday[b] - byday[a] for a, b in zip(ks, ks[1:]) if b - a == 1]
        d7 = [byday[k + 7] - byday[k] for k in ks if k + 7 in byday]
        lv = list(byday.values())
        print(f"  DVOL {coin}: daily closes {len(lv)}; level p10/p50/p90 {[round(x, 1) for x in q(lv, (.1, .5, .9))]}; "
              f"1-day change sd {st.pstdev(d1):.2f} pts (p1 {q(d1, (.01,))[0]:.1f}, p99 {q(d1, (.99,))[0]:.1f}); "
              f"7-day change sd {st.pstdev(d7):.2f} pts")
    for inst in ("BTC-PERPETUAL", "ETH-PERPETUAL"):
        w = weekly_moves(perp_series(inst))
        r = [x[1] for x in w]
        a = [abs(x) for x in r]
        sd = st.pstdev(r)
        # straddle payoff in units of its own mean (scale-free; constant sigma, no IV used)
        ratio = st.pstdev(a) / st.mean(a)
        # OTM payoff at a fixed 1-sd strike distance: max(|r| - sd, 0); share from top weeks
        otm = sorted((max(x - sd, 0) for x in a), reverse=True)
        tot = sum(otm)
        put = sorted((max(-x - sd, 0) for x in r), reverse=True)
        print(f"  {inst}: {len(w)} non-overlapping weeks (2019 -> 2023 and 2025-07-28 -> 2026-06; holdout A excluded); "
              f"weekly sd {sd * 100:.1f}%; sd(|r|)/mean(|r|) {ratio:.2f}; kurtosis {st.mean([(x / sd) ** 4 for x in r]):.1f}")
        print(f"    1-sd OTM strangle payoff: weeks paying > 0: {sum(1 for x in otm if x > 0)}; share of total payoff from top 1 / 3 / 5 weeks: "
              f"{otm[0] / tot:.0%} / {sum(otm[:3]) / tot:.0%} / {sum(otm[:5]) / tot:.0%}; sd/mean of payoff {st.pstdev(otm) / st.mean(otm):.2f}")
        pt = sum(put)
        print(f"    1-sd OTM put payoff: weeks paying {sum(1 for x in put if x > 0)}; top 1 / 3 weeks share {put[0] / pt:.0%} / {sum(put[:3]) / pt:.0%}; sd/mean {st.pstdev(put) / st.mean(put):.2f}")


def tail():
    print("## tail (underlying moves only, unconditional)")
    for inst in ("BTC-PERPETUAL", "ETH-PERPETUAL"):
        rows = perp_series(inst)
        w = weekly_moves(rows)
        worst = sorted(w, key=lambda x: -max(abs(x[1]), -x[2], x[3]))[:6]
        print(f"  {inst} worst weeks by max(|close move|, intra-week excursion):")
        for t, r, lo, hi in worst:
            print(f"    week from {net.iso(t)}: close {r * 100:+.1f}%, low {lo * 100:+.1f}%, high {hi * 100:+.1f}%")
        # 30-day windows (daily step, 08:00), worst close move and excursion
        px = {r[0]: r for r in rows}
        days = sorted(t for t in px if t % DAY == 8 * H)
        mm = []
        for t in days:
            hrs = [px.get(t + k * H) for k in range(30 * 24)]
            if all(hrs):
                s0 = hrs[0][1]
                mm.append((t, hrs[-1][4] / s0 - 1, min(h[3] for h in hrs) / s0 - 1, max(h[2] for h in hrs) / s0 - 1))
        mm.sort(key=lambda x: x[2])
        seen = []
        for t, r, lo, hi in mm:
            if all(abs(t - s) > 30 * DAY for s in seen):
                seen.append(t)
                print(f"    30-day window from {net.iso(t)}: close {r * 100:+.1f}%, low {lo * 100:+.1f}%, high {hi * 100:+.1f}%")
            if len(seen) == 4:
                break
        # biggest single hours and days
        hr = sorted(rows, key=lambda r: r[3] / r[1] - 1)[:3]
        print("    worst hours (low vs open): " + ", ".join(f"{net.iso(r[0])} {(r[3] / r[1] - 1) * 100:+.1f}%" for r in hr))
        # named stress weeks (public knowledge), and the worst HL-year week, by the weekly grid
        named = {"2022-05 LUNA": net.ms(2022, 5, 9), "2022-06 3AC": net.ms(2022, 6, 13), "2022-11 FTX": net.ms(2022, 11, 9),
                 "2025-10-10": net.ms(2025, 10, 10, 21)}
        for lab, t_ in named.items():
            ww = [x for x in w if x[0] <= t_ < x[0] + 7 * DAY]
            for t, r, lo, hi in ww:
                print(f"    {lab}: week from {net.iso(t)} close {r * 100:+.1f}%, low {lo * 100:+.1f}%, high {hi * 100:+.1f}%")
        hl = [x for x in w if x[0] >= HL_START]
        t, r, lo, hi = max(hl, key=lambda x: max(abs(x[1]), -x[2], x[3]))
        print(f"    worst HL-year week: from {net.iso(t)} close {r * 100:+.1f}%, low {lo * 100:+.1f}%, high {hi * 100:+.1f}% ({len(hl)} weeks)")
        # stylised short weekly ATM straddle on 1 unit of spot notional, CONSTANT premium
        # P = 1.114 x mean|r_w| (literature VRP: sqrt(0.72/0.58), Almeida et al.), loss = |r_w| - P.
        # Underlying moves only; only the worst week and worst 4 consecutive weeks are printed
        # (no mean, no hit rate: those would be a P&L statistic on our windows).
        a = [abs(x[1]) for x in w]
        P = 1.114 * st.mean(a)
        pl = [(x[0], P - abs(x[1])) for x in w]
        wk = min(pl, key=lambda x: x[1])
        runs = [(pl[i][0], sum(y for _, y in pl[i:i + 4])) for i in range(len(pl) - 3)
                if pl[i + 3][0] - pl[i][0] == 21 * DAY]
        mo = min(runs, key=lambda x: x[1])
        print(f"    stylised short weekly straddle (P = {P * 100:.1f}% of spot): worst week {wk[1] * 100:+.1f}% of notional "
              f"(from {net.iso(wk[0])}); worst 4 consecutive weeks {mo[1] * 100:+.1f}% (from {net.iso(mo[0])})")
        for day in ("2025-10-10",):
            y, m, d = map(int, day.split("-"))
            s = net.ms(y, m, d)
            hh = [px[t] for t in range(s, s + DAY, H) if t in px]
            print(f"    {day}: day low vs open {(min(h[3] for h in hh) / hh[0][1] - 1) * 100:+.1f}%, close vs open {(hh[-1][4] / hh[0][1] - 1) * 100:+.1f}%")


if __name__ == "__main__":
    secs = {"costs": costs, "hlmap": hlmap, "stress": stress, "proxy": proxy, "coincident": coincident,
            "power": power, "tail": tail}
    for s in (sys.argv[1:] if sys.argv[1:] != ["all"] else list(secs)):
        secs[s]()
        print()
