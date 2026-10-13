"""L5a behaviour-only check: trade activity by second relative to the top of the hour,
funding hours (00/08/16 UTC) vs the other 21 hours.

Reads ONLY transact_time, quantity and is_buyer_maker from the local Binance USD-M
aggTrades parquet (HL-cascade-selected days, 2025-07-28 .. 2026-06-30, not contiguous).
The price column is never selected: no return, markout or price level is computed.

Usage: python -I l5_behaviour.py <aggTrades_parquet_root> <out.json>
"""
import json
import sys

import duckdb

COINS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
FUNDING_HOURS = (0, 8, 16)
W = 600  # seconds either side of the hour boundary

BINS = {  # offset ranges in seconds, inclusive, relative to HH:00:00 UTC
    "base_-600_-301": (-600, -301),
    "pre_-300_-61": (-300, -61),
    "pre_-60_-11": (-60, -11),
    "pre_-10_-1": (-10, -1),
    "post_0_+9": (0, 9),
    "post_+10_+59": (10, 59),
    "post_+60_+299": (60, 299),
    "post_+300_+599": (300, 599),
}


def run(root, coin):
    q = f"""
    with t as (
      select transact_time as ts, quantity as qty, is_buyer_maker as bm,
             cast(transact_time % 3600000 as bigint) as ms_in_hour
      from read_parquet('{root}/{coin}/*.parquet')
      where transact_time < 1782864000000  -- 2026-07-01 00:00 UTC guard
    ), o as (
      select qty, bm,
             case when ms_in_hour < 1800000 then floor(ms_in_hour / 1000)
                  else floor(ms_in_hour / 1000) - 3600 end as off_s,
             cast(((ts + 1800000) // 3600000) % 24 as int) as bh,
             cast(ts // 86400000 as bigint) as day
      from t
    )
    select (bh in {FUNDING_HOURS}) as funding, cast(off_s as int) as off_s,
           count(*) as n, sum(qty) as qty, sum(case when bm then 0 else 1 end) as n_taker_buy,
           count(distinct day) as days
    from o where off_s between -{W} and {W - 1}
    group by 1, 2
    """
    rows = duckdb.sql(q).fetchall()
    agg = {}
    for funding, off, n, qty, ntb, days in rows:
        agg[(bool(funding), off)] = (n, qty, ntb)
    out = {}
    for fl in (True, False):
        hours = 3 if fl else 21
        per = {}
        for name, (a, b) in BINS.items():
            secs = b - a + 1
            n = sum(agg.get((fl, s), (0, 0, 0))[0] for s in range(a, b + 1))
            qty = sum(agg.get((fl, s), (0, 0, 0))[1] for s in range(a, b + 1))
            ntb = sum(agg.get((fl, s), (0, 0, 0))[2] for s in range(a, b + 1))
            per[name] = {"trades_per_sec_per_hour": n / secs / hours,
                         "qty_per_sec_per_hour": qty / secs / hours,
                         "taker_buy_share_count": ntb / n if n else None}
        base = per["base_-600_-301"]
        for name, v in per.items():
            v["trades_rel_base"] = v["trades_per_sec_per_hour"] / base["trades_per_sec_per_hour"]
            v["qty_rel_base"] = v["qty_per_sec_per_hour"] / base["qty_per_sec_per_hour"]
        out["funding" if fl else "other"] = per
    # top seconds by activity in funding hours relative to other hours, same offset
    ratio = []
    for s in range(-W, W):
        f = agg.get((True, s), (0, 0, 0))
        o = agg.get((False, s), (0, 0, 0))
        if f[0] and o[0]:
            ratio.append((s, (f[0] / 3) / (o[0] / 21), (f[1] / 3) / (o[1] / 21)))
    out["per_second_funding_over_other"] = ratio
    out["days"] = duckdb.sql(
        f"select count(distinct transact_time // 86400000) from read_parquet('{root}/{coin}/*.parquet')"
    ).fetchone()[0]
    return out


if __name__ == "__main__" and len(sys.argv) == 3:
    root, dst = sys.argv[1], sys.argv[2]
    res = {}
    for c in COINS:
        res[c] = run(root, c)
        f, o = res[c]["funding"], res[c]["other"]
        print(c, "days", res[c]["days"])
        for name in BINS:
            print(f"  {name:16s} funding trades x{f[name]['trades_rel_base']:.2f} qty x{f[name]['qty_rel_base']:.2f}"
                  f" tbuy {f[name]['taker_buy_share_count']:.3f} | other trades x{o[name]['trades_rel_base']:.2f}"
                  f" qty x{o[name]['qty_rel_base']:.2f} tbuy {o[name]['taker_buy_share_count']:.3f}")
        top = sorted(res[c]["per_second_funding_over_other"], key=lambda r: -r[2])[:8]
        print("  top seconds (funding/other qty ratio):", [(s, round(q, 2)) for s, _, q in top])
    with open(dst, "w") as fh:
        json.dump(res, fh)


def per_hour(root, coin):
    """Same measures per boundary hour (0..23), to compare funding hours (0/8/16) with
    the non-funding 4h-candle boundaries (4/12/20) and the rest. No price column read."""
    q = f"""
    with o as (
      select quantity as qty, is_buyer_maker as bm,
             case when transact_time % 3600000 < 1800000 then floor((transact_time % 3600000) / 1000)
                  else floor((transact_time % 3600000) / 1000) - 3600 end as off_s,
             cast(((transact_time + 1800000) // 3600000) % 24 as int) as bh
      from read_parquet('{root}/{coin}/*.parquet')
      where transact_time < 1782864000000
    )
    select bh,
      sum(case when off_s between -600 and -301 then qty end) / 300 as base_q,
      sum(case when off_s between 0 and 9 then qty end) / 10 as p0_q,
      sum(case when off_s between 10 and 599 then qty end) / 590 as p10_q,
      sum(case when off_s between -10 and -1 then qty end) / 10 as m10_q,
      avg(case when off_s between -10 and -1 then (case when bm then 0 else 1 end) end) as m10_tbuy,
      avg(case when off_s between -600 and -301 then (case when bm then 0 else 1 end) end) as base_tbuy
    from o where off_s between -600 and 599 group by 1 order by 1
    """
    return [dict(zip(["bh", "base_q", "p0_q", "p10_q", "m10_q", "m10_tbuy", "base_tbuy"], r))
            for r in duckdb.sql(q).fetchall()]


if __name__ == "__main__" and len(sys.argv) > 3 and sys.argv[3] == "per_hour":
    allr = {}
    for c in COINS:
        rows = per_hour(sys.argv[1], c)
        allr[c] = rows
        print(c)
        for r in rows:
            tag = "F" if r["bh"] in FUNDING_HOURS else ("4h" if r["bh"] % 4 == 0 else "")
            print(f"  {r['bh']:02d} {tag:2s} first10s x{r['p0_q']/r['base_q']:.2f}  next10min x{r['p10_q']/r['base_q']:.2f}"
                  f"  last10s x{r['m10_q']/r['base_q']:.2f}  tbuy last10s {r['m10_tbuy']:.3f} (base {r['base_tbuy']:.3f})")
    with open(sys.argv[2].replace(".json", "_per_hour.json"), "w") as fh:
        json.dump(allr, fh)
