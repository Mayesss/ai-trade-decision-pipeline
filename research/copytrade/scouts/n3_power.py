"""N3 scout: unconditional dispersion of 7-/14-day returns for ARBITRARY (non-event) small-cap coin-weeks,
and the detectable mean per event at 80% power.

NON-EVENT BASELINE ONLY. Source: the 009 discovery panel (data/binance_years/panel, 1h bars 2021-11 -> 2024-01-07,
already read by 009 for funding carry). Listing windows are EXCLUDED: a coin-week enters only when its start is
>= 30 days after the symbol's first kline day and its end is >= 7 days before the end of its alive segment.
Only dispersion is reported (SD, 1.4826*MAD, excess kurtosis): no mean, median or hit rate of anything.

Coin-week: Monday 00:00 UTC start w; r = ln(close of the bar at w+H-1h / open of the bar at w).
'excess' = r minus that week's cross-sectional median r (a market-neutral proxy).
Small-cap = BTC/ETH excluded and the top quartile by trailing-30-day quote volume (before w) excluded.

Usage: python -I n3_power.py <panel_dir> <binance_events.csv>
"""
import csv
import datetime as dt
import json
import math
import sys

import duckdb

HOUR, DAY = 3_600_000, 86_400_000


def ms(day):
    return int(dt.datetime.fromisoformat(day).replace(tzinfo=dt.UTC).timestamp() * 1000)


def disp(xs):
    n = len(xs)
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))
    s = sorted(xs)
    med = s[n // 2]
    mad = sorted(abs(x - med) for x in xs)[n // 2] * 1.4826
    k = sum((x - m) ** 4 for x in xs) / n / (sd ** 4) - 3
    return n, sd, mad, k


def main():
    panel, ev = sys.argv[1], sys.argv[2]
    life = json.load(open(f"{panel}/life.json"))
    con = duckdb.connect()
    con.execute(f"CREATE VIEW b AS SELECT symbol, ts, open, close, qv FROM '{panel}/bars_1h.parquet'")
    first_w = ms("2021-12-06")
    last_ts = con.execute("SELECT max(ts) FROM b").fetchone()[0]
    out = {}
    for H in (7, 14):
        weeks = []
        w = first_w
        while w + H * DAY <= last_ts + HOUR:
            weeks.append(w)
            w += 7 * DAY * (1 if H == 7 else 2)
        res = {"raw": [], "excess": []}
        for w in weeks:
            rows = con.execute(f"""
              WITH o AS (SELECT symbol, open FROM b WHERE ts = {w}),
                   c AS (SELECT symbol, close FROM b WHERE ts = {w + H * DAY - HOUR}),
                   v AS (SELECT symbol, sum(qv) qv FROM b WHERE ts >= {w - 30 * DAY} AND ts < {w} GROUP BY 1)
              SELECT o.symbol, ln(c.close / o.open), v.qv FROM o JOIN c USING (symbol) JOIN v USING (symbol)
              WHERE o.open > 0 AND c.close > 0""").fetchall()
            keep = []
            for s, r, qv in rows:
                L = life.get(s)
                if not L or s in ("BTCUSDT", "ETHUSDT"):
                    continue
                if w < ms(L["first_kline_day"]) + 30 * DAY:
                    continue
                if not any(ms(a) <= w and w + H * DAY <= ms(b) - 7 * DAY for a, b in L["segments"]):
                    continue
                keep.append((s, r, qv))
            if len(keep) < 20:
                continue
            qs = sorted(x[2] for x in keep)
            q75 = qs[int(0.75 * len(qs))]
            rs = sorted(x[1] for x in keep)
            med = rs[len(rs) // 2]
            for s, r, qv in keep:
                if qv < q75:
                    res["raw"].append(r)
                    res["excess"].append(r - med)
        out[H] = {k: disp(v) for k, v in res.items()}
        out[H]["weeks"] = len(weeks)
    for H in (7, 14):
        for k in ("raw", "excess"):
            n, sd, mad, ku = out[H][k]
            print(f"H={H}d {k}: coin-weeks {n} (over {out[H]['weeks']} start dates) | SD {sd:.4f} | 1.4826*MAD {mad:.4f} | excess kurtosis {ku:.1f}")
    # events per year (from the listing calendar; counts only)
    L = [r for r in csv.DictReader(open(ev)) if r["event"] == "listing" and r["censored"] == "0"]
    per = {}
    for r in L:
        y = r["date"][:4]
        iy, iw, _ = dt.date.fromisoformat(r["date"]).isocalendar()
        per.setdefault(y, [0, set()])
        per[y][0] += 1
        per[y][1].add((iy, iw))
    print("\nMDE (2-sided 5%, 80% power) = 2.80 * sigma / sqrt(N); N = events (iid) or distinct weeks (fully clustered)")
    for H in (7, 14):
        for k in ("raw", "excess"):
            sd = out[H][k][1]
            cells = []
            for name, n_ev, n_wk in [("per year 2023", per["2023"][0], len(per["2023"][1])),
                                     ("per year 2025", per["2025"][0], len(per["2025"][1])),
                                     ("2024-01..2025-07-27 (holdout A)", 260, 67),
                                     ("2021-12..2023-12 (009 disc.)", 128, 62),
                                     ("2020-01..2023-12", 260, 121)]:
                cells.append(f"{name}: iid {2.8 * sd / math.sqrt(n_ev):.3f} / clustered {2.8 * sd / math.sqrt(n_wk):.3f}"
                             f" / x2 vol clustered {2 * 2.8 * sd / math.sqrt(n_wk):.3f}")
            print(f"H={H}d {k} (sigma {sd:.3f}): " + " | ".join(cells))


if __name__ == "__main__":
    main()
