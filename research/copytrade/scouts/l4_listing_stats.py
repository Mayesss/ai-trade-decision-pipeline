"""L4 scout: coverage statistics from LISTINGS ONLY (keys, sizes, dates).

Reads the CSVs written by l4_binance_lister.py plus Binance's current
exchangeInfo (symbol classification only). No market value is read here,
except one calibration constant for the liquidity proxy (section C), which is
taken from the BTCUSDT 1d sample (2024-06) and is printed.

Usage (from research/copytrade/scouts):
  ../.venv/bin/python -I l4_listing_stats.py [A|B|C|F|G ...]   (default: all)
"""
import csv
import datetime as dt
import io
import json
import math
import os
import sys
import zipfile

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..", "data", "scout_l4")
LIST = os.path.join(ROOT, "listings")
CUT = "2026-07-01"
FULL = 4000  # bytes; a full 288-row metrics day zips to ~6-14 KB, post-delisting stubs to < 2.1 KB

# Proposed split, decided before any outcome is read (see deliverable, F).
DISC = ("2021-12-01", "2023-12-31")
HOLD = ("2024-01-01", "2026-06-30")
HL_OVERLAP = ("2025-07-28", "2026-06-30")  # HL discovery year, read by 14 trials

con = duckdb.connect()


def q(sql):
    return con.sql(sql)


def md(rel, floatfmt="{:.3g}"):
    cols = rel.columns
    rows = rel.fetchall()
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        cells = []
        for v in r:
            if isinstance(v, float):
                cells.append(floatfmt.format(v))
            elif v is None:
                cells.append("—")
            else:
                cells.append(str(v))
        out.append("| " + " | ".join(cells) + " |")
    print("\n".join(out) + "\n")


def load():
    ex = json.load(open(os.path.join(ROOT, "exchangeinfo", "fapi_exchangeInfo.json")))["symbols"]
    d = lambda t: dt.datetime.fromtimestamp(t / 1000, dt.timezone.utc).date()  # noqa: E731
    con.execute("create table xi(symbol varchar, ctype varchar, status varchar, quote varchar,"
                " utype varchar, onboard date, delivery date)")
    con.executemany("insert into xi values (?,?,?,?,?,?,?)",
                    [(s["symbol"], s["contractType"], s["status"], s["quoteAsset"],
                      s.get("underlyingType"), d(s["onboardDate"]), d(s["deliveryDate"])) for s in ex])
    # Crypto USDT perpetuals: name pattern, minus TradFi perps and index composites
    # known to the current exchangeInfo (delisted symbols are absent from it).
    con.execute("""create macro is_univ(s) as regexp_matches(s, '^[A-Z0-9]+USDT$')
                   and s not in (select symbol from xi where ctype = 'TRADIFI_PERPETUAL'
                                 or utype = 'INDEX')""")
    names = {}
    for f in sorted(os.listdir(LIST)):
        if not f.endswith(".csv"):
            continue
        t = f[:-4]
        daily = t.startswith("daily_")
        dexpr = "cast(date as date)" if daily else "cast(date || '-01' as date)"
        con.execute(f"""create table "{t}" as select symbol, {dexpr} d, bytes,
                        is_univ(symbol) univ from read_csv('{os.path.join(LIST, f)}', header=true,
                        columns={{'symbol':'varchar','date':'varchar','bytes':'bigint','key':'varchar'}})""")
        names[t] = daily
    return names


def section_a(names):
    print("### A. Datasets on data.binance.vision, USDⓈ-M (listing only)\n")
    print("`univ` = crypto USDT perpetuals (name `^[A-Z0-9]+USDT$`, minus TradFi and index"
          " contracts in today's exchangeInfo). Bytes are zipped. `post` = files dated ≥ 2026-07-01,"
          " counted from the listing, never fetched. Monthly files are dated by month.\n")
    rows = []
    for t, daily in names.items():
        rows.append(f"""select '{t}' dataset,
            count(distinct symbol) syms_all,
            count(distinct symbol) filter (where univ) syms_univ,
            min(d) filter (where univ) first_univ,
            max(d) filter (where univ and d < '{CUT}') last_pre,
            count(*) filter (where univ and d < '{CUT}') files_univ_pre,
            round(sum(bytes) filter (where univ and d < '{CUT}') / 1e9, 3) gb_univ_pre,
            round(sum(bytes) filter (where d < '{CUT}') / 1e9, 3) gb_all_pre,
            count(*) filter (where d >= '{CUT}') files_post,
            round(sum(bytes) filter (where d >= '{CUT}') / 1e9, 3) gb_post
            from "{t}" """)
    md(q(" union all ".join(rows) + " order by dataset"))

    print("Symbols (univ) with ≥ 1 file in the year:\n")
    yrs = [2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    sel = ", ".join(f"count(distinct symbol) filter (where year(d) = {y}) \"{y}\"" for y in yrs)
    md(q(" union all ".join(f"""select '{t}' dataset, {sel} from "{t}" where univ and d < '{CUT}'"""
                            for t in names) + " order by dataset"))

    print("Gaps inside each symbol's own first..last span (pre-cutoff, univ):\n")
    rows = []
    for t, daily in names.items():
        step = "day" if daily else "month"
        rows.append(f"""select '{t}' dataset, count(*) syms,
            count(*) filter (where missing > 0) syms_with_gap,
            sum(missing) missing_units, max(missing) max_missing_one_symbol, '{step}' unit
            from (select symbol, date_diff('{step}', min(d), max(d)) + 1 - count(distinct d) missing
                  from "{t}" where univ and d < '{CUT}' group by symbol)""")
    md(q(" union all ".join(rows) + " order by dataset"))

    print("Archive-wide holes in daily metrics (days since 2021-12-01 where the univ file count"
          " falls below 80% of the previous day's):\n")
    md(q(f"""select d, n, prev from (select d, count(*) n, lag(count(*)) over (order by d) prev
             from daily_metrics where univ and d >= '2021-12-01' and d < '{CUT}' group by d)
             where n < 0.8 * prev order by d"""))
    md(q(f"""select count(*) calendar_days_without_any_metrics_file from
             (select unnest(generate_series(date '2021-12-01', date '2026-06-30', interval 1 day))::date d)
             where d not in (select distinct d from daily_metrics where univ)"""))


def section_b():
    print("### B. Survivorship (univ symbols)\n")
    for t, unit in (("daily_metrics", "day"), ("monthly_klines_1d", "month")):
        md(q(f"""select '{t}' dataset, count(*) syms_ever_pre_cutoff,
                 count(*) filter (where last_d < '2026-05-01') last_file_before_2026_05,
                 count(*) filter (where last_d >= '2026-06-01') present_june_2026,
                 count(*) filter (where symbol in (select symbol from xi where status = 'SETTLING')) in_xi_settling,
                 count(*) filter (where last_d < '2026-05-01' and symbol not in (select symbol from xi)) delisted_absent_from_xi
                 from (select symbol, max(d) last_d from "{t}" where univ and d < '{CUT}' group by symbol)"""))
        md(q(f"""select year(last_d) year_of_last_file, count(*) syms from
                 (select symbol, max(d) last_d from "{t}" where univ and d < '{CUT}' group by symbol)
                 where last_d < '2026-05-01' group by 1 order by 1"""))
    print("Point-in-time check: first metrics/1d-kline date vs exchangeInfo `onboardDate`"
          " (symbols onboarded after the dataset's archive start):\n")
    for t, start in (("daily_metrics", "2021-12-02"), ("daily_klines_1d", "2020-01-02")):
        md(q(f"""select '{t}' dataset, count(*) n,
                 count(*) filter (where diff = 0) same_day,
                 count(*) filter (where diff = 1) plus_1d,
                 count(*) filter (where diff between 2 and 7) plus_2_7d,
                 count(*) filter (where diff > 7) later_gt7d,
                 count(*) filter (where diff < 0) earlier
                 from (select symbol, date_diff('day', xi.onboard, min(t.d)) diff
                       from "{t}" t join xi using (symbol) where univ and xi.onboard >= '{start}'
                       group by symbol, xi.onboard)"""))
    print("Last metrics date vs exchangeInfo `deliveryDate` for SETTLING (delisted) symbols"
          " whose delivery is before the cutoff:\n")
    md(q(f"""select count(*) n, count(*) filter (where diff = -1) last_day_before_delivery,
             count(*) filter (where diff = 0) same_day,
             count(*) filter (where diff between -7 and -2) within_week_before,
             count(*) filter (where diff < -7 or diff > 0) other
             from (select symbol, date_diff('day', xi.delivery, max(t.d)) diff
                   from daily_metrics t join xi using (symbol)
                   where univ and xi.status = 'SETTLING' and xi.delivery < '{CUT}' group by symbol, xi.delivery)"""))
    print(f"Listing-only delisting rule: last metrics file of >= {FULL} bytes vs `deliveryDate`"
          " (SETTLING symbols, delivery before the cutoff):\n")
    md(q(f"""select count(*) n, count(*) filter (where diff = 0) same_day,
             count(*) filter (where diff = -1) day_before, count(*) filter (where diff not in (0, -1)) other
             from (select symbol, date_diff('day', xi.delivery, max(t.d)) diff from daily_metrics t join xi using (symbol)
                   where univ and t.bytes >= {FULL} and xi.status = 'SETTLING' and xi.delivery < '{CUT}'
                   and t.d < '{CUT}' group by symbol, xi.delivery)"""))
    md(q(f"""select count(*) files_trading_symbols, count(*) filter (where bytes < {FULL}) files_below_rule
             from daily_metrics t join xi using (symbol)
             where univ and xi.status = 'TRADING' and t.d < '{CUT}' and t.d > xi.onboard + 7"""))
    print("Delistings by year from the rule (last full metrics file before 2026-06-01):\n")
    md(q(f"""select year(lf) yr, count(*) delisted from (select symbol, max(d) lf from daily_metrics
             where univ and bytes >= {FULL} and d < '{CUT}' group by 1) where lf < '2026-06-01' group by 1 order by 1"""))
    print("Relistings / long internal holes (> 30 days missing inside a metrics span):\n")
    md(q(f"""select count(*) syms_with_hole_gt30d from (select symbol, max(gap) g from
             (select symbol, date_diff('day', lag(d) over (partition by symbol order by d), d) gap
              from daily_metrics where univ and d < '{CUT}') group by symbol) where g > 30"""))


def btc_dollars_per_aggtrade_byte():
    """Calibration for the liquidity proxy: BTCUSDT 2024-06 quote volume (1d sample)
    divided by BTCUSDT 2024-06 monthly aggTrades zip bytes (listing)."""
    p = os.path.join(ROOT, "samples", "klines_1d", "BTCUSDT-1d-2024-06.zip")
    with zipfile.ZipFile(p) as z, z.open(z.namelist()[0]) as fh:
        rows = list(csv.DictReader(io.TextIOWrapper(fh, "utf-8")))
    qv = sum(float(r["quote_volume"]) for r in rows)
    b = q("select bytes from monthly_aggTrades where symbol = 'BTCUSDT' and d = '2024-06-01'").fetchone()[0]
    return qv / b, qv, b


def section_c():
    print("### C. Universe per year (univ symbols with metrics coverage)\n")
    k, qv, b = btc_dollars_per_aggtrade_byte()
    print(f"Liquidity: a $-volume proxy from aggTrades bytes, calibrated on BTCUSDT 2024-06"
          f" (${qv / 1e9:.1f} B quote volume over {b / 1e6:.0f} MB of zip = ${k:,.0f} per byte), puts"
          " every qualifying symbol above $100 M/day. It is an upper bound (alt trades are smaller"
          " than BTC's), so it cannot discriminate and is not used. The columns below are activity"
          " tiers by median monthly aggTrades zip size (a trade-count measure, not dollars).\n")
    periods = [(2020, "2020-01-01", "2020-12-31", 300), (2021, "2021-01-01", "2021-12-31", 300),
               (2022, "2022-01-01", "2022-12-31", 300), (2023, "2023-01-01", "2023-12-31", 300),
               (2024, "2024-01-01", "2024-12-31", 300), (2025, "2025-01-01", "2025-12-31", 300),
               ("2026-H1", "2026-01-01", "2026-06-30", 149)]
    rows = []
    for name, a, z, thr in periods:
        rows.append(f"""select '{name}' period, {thr} min_days,
            count(*) filter (where n >= 1) any_metrics,
            count(*) filter (where n >= {thr}) metrics_ge_thr,
            count(*) filter (where n >= {thr} and med_mb >= 10) ge_thr_aggtrades_ge_10MB_month,
            count(*) filter (where n >= {thr} and med_mb >= 50) ge_thr_aggtrades_ge_50MB_month,
            count(*) filter (where n >= {thr} and med_mb >= 200) ge_thr_aggtrades_ge_200MB_month
            from (select m.symbol, count(*) n, any_value(l.med_mb) med_mb
                  from daily_metrics m left join
                    (select symbol, median(bytes / 1e6) med_mb
                     from monthly_aggTrades where d between '{a}' and '{z}' group by symbol) l using (symbol)
                  where m.univ and m.bytes >= {FULL} and m.d between '{a}' and '{z}' group by m.symbol)""")
    md(q(" union all ".join(rows)))
    print("Same, funding + 1d klines only (no metrics), months with a file in both, ≥ 10 of 12"
          " months (≥ 5 of 6 for 2026-H1):\n")
    rows = []
    for name, a, z, thr in periods:
        mthr = 5 if name == "2026-H1" else 10
        rows.append(f"""select '{name}' period, count(*) syms from (select f.symbol, count(*) n
            from monthly_fundingRate f join monthly_klines_1d k using (symbol, d)
            where f.univ and f.d between '{a}' and '{z}' group by f.symbol) where n >= {mthr}""")
    md(q(" union all ".join(rows)))


def coin_weeks():
    """Full-feature coin-weeks: metrics on all 7 days of ISO week w, funding and 1h
    klines files present for every month touched by w, 1h klines for week w+1 too."""
    con.execute(f"""create or replace table cw as
        with w as (select symbol, date_trunc('week', d) wk, count(*) nd
                   from daily_metrics where univ and bytes >= {FULL} and d < '{CUT}' group by 1, 2 having count(*) = 7)
        select w.symbol, w.wk from w
        where exists (select 1 from monthly_fundingRate f where f.symbol = w.symbol and f.d = date_trunc('month', w.wk))
          and exists (select 1 from monthly_fundingRate f where f.symbol = w.symbol and f.d = date_trunc('month', w.wk + interval 6 day))
          and exists (select 1 from monthly_klines_1h k where k.symbol = w.symbol and k.d = date_trunc('month', w.wk))
          and exists (select 1 from monthly_klines_1h k where k.symbol = w.symbol and k.d = date_trunc('month', w.wk + interval 13 day))
          and w.wk + interval 13 day < date '{CUT}'""")
    con.execute(f"""create or replace table cw_fk as
        with m as (select f.symbol, f.d from monthly_fundingRate f join monthly_klines_1h k using (symbol, d)
                   where f.univ and f.d < '{CUT}'),
        wk as (select unnest(generate_series(date '2019-12-30', date '2026-06-15', interval 7 day))::date wk)
        select m.symbol, wk.wk from wk join m on m.d = date_trunc('month', wk.wk)
        where exists (select 1 from m m2 where m2.symbol = m.symbol and m2.d = date_trunc('month', wk.wk + interval 13 day))
          and wk.wk + interval 13 day < date '{CUT}'""")


def section_f():
    coin_weeks()
    z = 1.959964 + 0.841621  # two-sided 5%, 80% power
    print("### F. Coin-weeks per split (full features; weeks start Monday 00:00 UTC)\n")
    splits = [("discovery", *DISC), ("holdout", *HOLD),
              ("holdout excl. HL year", HOLD[0], "2025-07-27"), ("holdout HL-year part", *HL_OVERLAP),
              ("pre-metrics (funding+klines only)", "2020-01-01", "2021-11-30")]
    rows = []
    for name, a, b in splits:
        tbl = "cw_fk" if name.startswith("pre-metrics") else "cw"
        rows.append(f"""select '{name}' split, '{a}' "start", '{b}' "end",
            count(distinct wk) n_weeks, count(*) coin_weeks,
            round(count(*) / count(distinct wk), 1) mean_coins,
            min(c) min_coins, max(c) max_coins,
            round({z} / sqrt(count(*)), 4) ic_mde_iid
            from (select wk, count(*) over (partition by wk) c from {tbl} where wk between '{a}' and '{b}')""")
    md(q(" union all ".join(rows)))
    print(f"`ic_mde_iid` = (z_0.975 + z_0.80) / sqrt(N_weeks × mean_coins) = {z:.3f} / sqrt(coin_weeks).\n")
    print("Sensitivity: MDE if the weekly IC has time-series s.d. σ_IC (cross-sectional"
          " correlation makes σ_IC exceed 1/sqrt(coins)): MDE = 2.80 σ_IC / sqrt(N_weeks)\n")
    nd, nh = (q(f"select count(distinct wk) from cw where wk between '{a}' and '{b}'").fetchone()[0]
              for a, b in (DISC, HOLD))
    rows = [f"| σ_IC | discovery (N={nd}) | holdout (N={nh}) |", "|---|---|---|"]
    for s in (0.05, 0.10, 0.15, 0.20):
        rows.append(f"| {s:.2f} | {z * s / math.sqrt(nd):.4f} | {z * s / math.sqrt(nh):.4f} |")
    print("\n".join(rows) + "\n")


def section_g():
    if not q("select count(*) from information_schema.tables where table_name = 'cw'").fetchone()[0]:
        coin_weeks()
    print("### G. Full pull (zipped bytes and file counts; + one .CHECKSUM per zip)\n")
    print("Qualifying symbols = univ symbols with ≥ 1 full-feature coin-week in the period;"
          " files restricted to the period dates (monthly files: months overlapping it).\n")
    rows = []
    for name, a, b in (("(i) discovery", *DISC), ("(ii) discovery + holdout", DISC[0], HOLD[1]),
                       ("holdout only", *HOLD)):
        syms = f"(select distinct symbol from cw where wk between '{a}' and '{b}')"
        parts = []
        for t, label, daily in (("daily_metrics", "metrics", True), ("monthly_fundingRate", "funding", False),
                                ("monthly_klines_1h", "klines_1h", False), ("monthly_klines_1d", "klines_1d", False),
                                ("monthly_premiumIndexKlines_1h", "premium_1h", False),
                                ("monthly_markPriceKlines_1h", "mark_1h", False)):
            lo = a if daily else a[:8] + "01"
            full = f" and bytes >= {FULL}" if daily else ""
            parts.append(f"""select '{label}' ds, count(*) files, sum(bytes) b from "{t}"
                             where symbol in {syms} and d between '{lo}' and '{b}'{full}""")
        con.execute(f"create or replace temp table g as {' union all '.join(parts)}")
        nsym = q(f"select count(*) from {syms}").fetchone()[0]
        r = {ds: (f, by) for ds, f, by in q("select * from g").fetchall()}
        core = [r["metrics"], r["funding"], r["klines_1h"]]
        rows.append((name, nsym, sum(f for f, _ in core), sum(b for _, b in core) / 1e9,
                     r["metrics"][1] / 1e9, r["funding"][1] / 1e9, r["klines_1h"][1] / 1e9,
                     r["klines_1d"][1] / 1e9, r["premium_1h"][1] / 1e9, r["mark_1h"][1] / 1e9))
    print("| pull | symbols | files (metrics+funding+1h) | GB (metrics+funding+1h) | metrics GB | funding GB |"
          " klines 1h GB | alt: klines 1d GB | +premium 1h GB | +mark 1h GB |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for x in rows:
        print(f"| {x[0]} | {x[1]} | {x[2]:,} | {x[3]:.2f} | {x[4]:.2f} | {x[5]:.3f} | {x[6]:.2f} |"
              f" {x[7]:.3f} | {x[8]:.2f} | {x[9]:.2f} |")
    print()


if __name__ == "__main__":
    want = set(sys.argv[1:]) or {"A", "B", "C", "F", "G"}
    n = load()
    if "A" in want:
        section_a(n)
    if "B" in want:
        section_b()
    if "C" in want:
        section_c()
    if "F" in want:
        section_f()
    if "G" in want:
        section_g()
