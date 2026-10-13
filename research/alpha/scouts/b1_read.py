"""B1 scout: behaviour-only reads of the scout_b1 pulls.

Allowed outputs only: calendars, settlement-regime dates, coverage counts and
first/last dates, and UNCONDITIONAL volatility of an index (or of daily
yield changes) for power arithmetic. Nothing here links any date, window,
signal or state to any return. No return is averaged by window, event or
calendar. Rows dated on or after 2026-07-01 are dropped at load and never
printed (only their count is reported).

Usage: python3 -I b1_read.py <data root: research/alpha/data/scout_b1> <out dir for derived calendars>
"""
import csv
import glob
import io
import json
import math
import os
import re
import sys
import zipfile
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

CUT = date(2026, 7, 1)
START = date(2014, 1, 1)


def latest(root, prefix):
    ds = sorted(glob.glob(os.path.join(root, prefix + "_*")))
    ds = [d for d in ds if os.path.exists(os.path.join(d, "SHA256SUMS")) and os.path.getsize(os.path.join(d, "SHA256SUMS")) > 0]
    return ds[-1] if ds else None


def sd(xs):
    m = sum(xs) / len(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


# ---------- Ken French daily ----------
def read_french(path):
    """Return list of (date, mkt_total_pct) for rows < CUT; and count of dropped rows."""
    z = zipfile.ZipFile(path)
    txt = z.read(z.namelist()[0]).decode("latin-1")
    rows, dropped, header = [], 0, None
    for line in txt.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if header is None:
            if len(parts) >= 4 and parts[1].startswith("Mkt-RF"):
                header = parts
            continue
        if not re.fullmatch(r"\d{8}", parts[0] or ""):
            if rows:
                break  # end of daily block
            continue
        d = datetime.strptime(parts[0], "%Y%m%d").date()
        if d >= CUT:
            dropped += 1
            continue
        mkt_rf = float(parts[header.index("Mkt-RF")])
        rf = float(parts[header.index("RF")])
        rows.append((d, mkt_rf + rf))
    return rows, dropped


def french_report(root):
    d = latest(root, "french")
    print("\n== Ken French daily (", os.path.basename(d), ")")
    out = {}
    for f in sorted(glob.glob(os.path.join(d, "*.zip"))):
        rows, dropped = read_french(f)
        win = [r for r in rows if r[0] >= START]
        s = sd([r[1] for r in win])
        name = os.path.basename(f).replace("_CSV.zip", "")
        print(f"{name}: first {rows[0][0]} last(<cut) {rows[-1][0]} rows {len(rows)}; post-cutoff rows dropped {dropped}; "
              f"2014-01..2026-06 days {len(win)}; UNCONDITIONAL daily SD of market total return {s:.3f}%")
        out[name] = (rows, s)
    return out


# ---------- trading-day calendar + settlement regimes ----------
US_SETTLEMENT = [  # (first trade date of regime, lag in business days); SEC 34-80295, 34-96930
    (date(1995, 6, 7), 3), (date(2017, 9, 5), 2), (date(2024, 5, 28), 1)]


def lag_on(dt, table):
    s = None
    for d0, lag in table:
        if dt >= d0:
            s = lag
    return s


def month_end_calendar(days, table, label, outdir):
    """days: sorted trading dates. For each month 2014-01..2026-06: T = last trading day;
    settlement lag s in force on T-s; rebound window per mechanism = T-s .. T+3 (needs T+3 < CUT)."""
    idx = {d: i for i, d in enumerate(days)}
    by_month = defaultdict(list)
    for d in days:
        by_month[(d.year, d.month)].append(d)
    rows = []
    for (y, m), ds in sorted(by_month.items()):
        if date(y, m, 1) < START:
            continue
        T = ds[-1]
        i = idx[T]
        # lag is the one in force on the day the cash-raising sale would be made (T - s)
        s = None
        for cand in (3, 2, 1):
            if i - cand >= 0 and lag_on(days[i - cand], table) == cand:
                s = cand
                break
        if s is None:
            s = lag_on(T, table)
        t_plus3 = days[i + 3] if i + 3 < len(days) else None
        rows.append({"month": f"{y}-{m:02d}", "T": T.isoformat(), "T_weekday": T.strftime("%a"), "settle_lag": s,
                     "pressure_start_T-8": days[i - 8].isoformat(), "pressure_end_T-(s+1)": days[i - s - 1].isoformat(),
                     "rebound_start_T-s": days[i - s].isoformat(),
                     "rebound_end_T+3": t_plus3.isoformat() if t_plus3 else "",
                     "window_complete_before_cut": bool(t_plus3 and t_plus3 < CUT)})
    p = os.path.join(outdir, f"month_end_calendar_{label}.csv")
    with open(p, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    comp = [r for r in rows if r["window_complete_before_cut"]]
    c = Counter(r["settle_lag"] for r in comp)
    fri = sum(1 for r in comp if r["T_weekday"] == "Fri")
    print(f"month-end calendar {label}: {len(comp)} complete months ({comp[0]['month']}..{comp[-1]['month']}); "
          f"by settlement lag {dict(sorted(c.items()))}; T on Friday {fri}; file {p}")
    return comp


# ---------- FOMC ----------
MONTHS = {m: i for i, m in enumerate(["January", "February", "March", "April", "May", "June", "July", "August",
                                      "September", "October", "November", "December"], 1)}
MABBR = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10,
         "Nov": 11, "Dec": 12}


def ann_date(year, month_txt, days_txt):
    """Announcement = last day of the meeting. 'Jan/Feb' + '31-1' -> Feb 1."""
    month_txt = month_txt.strip()
    days_txt = days_txt.strip().rstrip("*").strip()
    last_day = int(re.findall(r"\d+", days_txt)[-1])
    if "/" in month_txt:
        m2 = month_txt.split("/")[1].strip()
        mon = MONTHS.get(m2) or MABBR[m2[:3]]
    else:
        mon = MONTHS.get(month_txt) or MABBR[month_txt[:3]]
    return date(year, mon, last_day)


def fomc_dates(root):
    d = latest(root, "fomc")
    out = []
    for f in sorted(glob.glob(os.path.join(d, "fomchistorical*.htm"))):
        y = int(re.findall(r"(\d{4})", os.path.basename(f))[0])
        t = open(f, encoding="utf-8", errors="replace").read()
        for h in re.findall(r"<h[1-6][^>]*>(.*?)</h[1-6]>", t, re.S):
            h = re.sub(r"<[^>]+>", "", h).strip()
            m = re.match(r"([A-Za-z/]+)\s+([\d\-]+)\s+Meeting\s+-\s+(\d{4})$", h)
            if m:  # scheduled only; '(unscheduled)' and '(cancelled)' do not match
                out.append(ann_date(int(m.group(3)), m.group(1), m.group(2)))
    t = open(os.path.join(d, "fomccalendars.htm"), encoding="utf-8", errors="replace").read()
    for ym in re.finditer(r"(\d{4}) FOMC Meetings", t):
        y = int(ym.group(1))
        nxt = t.find("FOMC Meetings", ym.end())
        sec = t[ym.end(): nxt if nxt > 0 else len(t)]
        for mm in re.finditer(r'fomc-meeting__month[^>]*>\s*<strong>([^<]+)</strong>.*?fomc-meeting__date[^>]*>([^<]+)<', sec, re.S):
            dt_txt = mm.group(2)
            if "unscheduled" in dt_txt.lower() or "notation" in dt_txt.lower() or "cancel" in dt_txt.lower():
                continue
            if not re.search(r"\d", dt_txt):
                continue
            out.append(ann_date(y, mm.group(1), dt_txt))
    out = sorted(set(x for x in out if START <= x < CUT))
    return out


def fomc_report(root, days, outdir):
    ann = fomc_dates(root)
    print(f"\n== FOMC scheduled announcement days (< cut, >= 2014): {len(ann)}; first {ann[0]} last {ann[-1]}; per year "
          f"{dict(sorted(Counter(a.year for a in ann).items()))}")
    idx = {d: i for i, d in enumerate(days)}
    # CMVJ cycle time: day 0 = announcement day; week k = trading days [5k-1, 5k+3]; label each trading day
    lab = {}
    for a in ann:
        if a not in idx:
            print("  announcement not a trading day:", a)
            continue
        i0 = idx[a]
        for j in range(-1, 5 * 8):
            if 0 <= i0 + j < len(days):
                lab[days[i0 + j]] = (j + 1) // 5  # j=-1..3 -> week 0, 4..8 -> 1, ...
    rows = []
    for d in days:
        if START <= d < CUT and d in lab:
            rows.append((d, lab[d]))
    p = os.path.join(outdir, "fomc_cycle_week_by_day.csv")
    with open(p, "w") as f:
        f.write("date,fomc_cycle_week\n")
        for d, w in rows:
            f.write(f"{d},{w}\n")
    with open(os.path.join(outdir, "fomc_announcement_days.csv"), "w") as f:
        f.write("announcement_date\n" + "\n".join(a.isoformat() for a in ann) + "\n")
    c = Counter(w for _, w in rows)
    even = sum(v for k, v in c.items() if k % 2 == 0)
    post = [(d, w) for d, w in rows if d >= date(2017, 1, 1)]
    ce = sum(1 for _, w in post if w % 2 == 0)
    print(f"trading days by FOMC-cycle week 2014-01..2026-06: {dict(sorted(c.items()))}; even {even} odd {len(rows) - even}")
    print(f"post-CMVJ-sample (2017-01..2026-06): {len(post)} days, even {ce}, odd {len(post) - ce}; cycles {sum(1 for a in ann if a >= date(2017,1,1))}")
    return ann, rows


# ---------- Treasury auctions (calendar fields only) ----------
def auctions_report(root, outdir):
    d = latest(root, "auctions")
    recs = []
    for f in sorted(glob.glob(os.path.join(d, "auctions_p*.json"))):
        recs += json.load(open(f))["data"]
    print(f"\n== Treasury auctions ({os.path.basename(d)}): {len(recs)} records; fields {sorted(recs[0].keys())}")
    notes = [r for r in recs if r["security_type"] in ("Note", "Bond", "TIPS Note", "TIPS Bond", "FRN Note", "FRN")]
    c = Counter((r["security_type"], r["security_term"], r["reopening"]) for r in notes
                if date.fromisoformat(r["auction_date"]) < CUT)
    for k, v in sorted(c.items(), key=lambda x: (-x[1])):
        if v >= 20:
            print(f"  {k}: {v}")
    def fixed(r):  # exclude FRNs and TIPS when the flags were pulled
        return r.get("floating_rate", "No") == "No" and r.get("inflation_index_security", "No") == "No"

    def fam(term):
        for pre, lab in (("2-Year", "2-Year"), ("1-Year 1", "2-Year"), ("5-Year", "5-Year"), ("4-Year", "5-Year"),
                         ("10-Year", "10-Year"), ("9-Year", "10-Year")):
            if term.startswith(pre):
                return lab
        return None
    key = [dict(r, security_term=fam(r["security_term"])) for r in notes
           if r["security_type"] == "Note" and fixed(r) and fam(r["security_term"])]
    out = os.path.join(outdir, "auctions_2y5y10y.csv")
    with open(out, "w") as f:
        f.write("auction_date,security_term,reopening,announcemt_date,issue_date,offering_amt\n")
        for r in sorted(key, key=lambda r: r["auction_date"]):
            if date.fromisoformat(r["auction_date"]) < CUT:
                f.write(f"{r['auction_date']},{r['security_term']},{r['reopening']},{r.get('announcemt_date','')},{r['issue_date']},{r['offering_amt']}\n")
    for term in ("2-Year", "5-Year", "10-Year"):
        rs = [r for r in key if r["security_term"] == term and r["auction_date"] < "2026-07-01"]
        ds = sorted({r["auction_date"] for r in rs})
        print(f"  {term} fixed-rate note auction dates (new + reopenings) 2014-01..2026-06: {len(ds)} (first {ds[0]}, last {ds[-1]}); "
              f"reopenings {sum(1 for r in rs if r['reopening'] == 'Yes')}")
    # lead time between announcement and auction (calendar fact)
    leads = []
    for r in key:
        a = r.get("announcemt_date")
        if a and a != "null":
            leads.append((date.fromisoformat(r["auction_date"]) - date.fromisoformat(a)).days)
    if leads:
        print(f"  announcement->auction lead (calendar days): min {min(leads)} median {sorted(leads)[len(leads)//2]} max {max(leads)}")


# ---------- Treasury par yields ----------
def treasury_report(root):
    d = latest(root, "treasury")
    yc, bills = {}, {}
    dropped = 0
    for f in sorted(glob.glob(os.path.join(d, "*.csv"))):
        rd = csv.DictReader(io.StringIO(open(f, encoding="utf-8-sig").read()))
        for r in rd:
            dt = datetime.strptime(r["Date"], "%m/%d/%Y").date()
            if dt >= CUT:
                dropped += 1
                continue
            if "yield_curve" in f and r.get("10 Yr"):
                yc[dt] = float(r["10 Yr"])
            if "bill_rates" in f:
                k = [c for c in r if c and ("13 WEEKS" in c.upper() and "COUPON" in c.upper())]
                if k and r[k[0]]:
                    bills[dt] = float(r[k[0]])
    ds = sorted(yc)
    ch = [(yc[b] - yc[a]) * 100 for a, b in zip(ds, ds[1:])]
    print(f"\n== Treasury par yields ({os.path.basename(d)}): 10y days {len(ds)} ({ds[0]}..{ds[-1]}); post-cutoff rows dropped {dropped}")
    print(f"  UNCONDITIONAL SD of daily 10y par-yield change 2014-01..2026-06: {sd(ch):.2f} bp")
    b = sorted(x for x in bills if x >= date(2025, 7, 1))
    if b:
        vals = [bills[x] for x in b]
        print(f"  13-week bill (coupon-equivalent) 2025-07..2026-06: n {len(vals)}, min {min(vals):.2f}%, max {max(vals):.2f}%, last(<cut) {vals[-1]:.2f}% on {b[-1]}")
    return sd(ch)


# ---------- Nasdaq probes ----------
def nasdaq_report(root):
    d = latest(root, "nasdaq")
    print(f"\n== Nasdaq probes ({os.path.basename(d)})")
    p = os.path.join(d, "hist_AAPL.json")
    if os.path.exists(p):
        j = json.load(open(p))
        data = (j.get("data") or {})
        rows = ((data.get("tradesTable") or {}).get("rows")) or []
        dates = sorted(datetime.strptime(r["date"], "%m/%d/%Y").date() for r in rows)
        print(f"  historical AAPL: status {j.get('status', {}).get('rCode')}, rows {len(rows)}, totalRecords {data.get('totalRecords')}, "
              f"first {dates[0] if dates else None}, last {dates[-1] if dates else None}, any >= cut: {any(x >= CUT for x in dates)}; "
              f"fields {sorted(rows[0].keys()) if rows else []}")
    p = os.path.join(d, "earnings_2024-01-25.json")
    if os.path.exists(p):
        j = json.load(open(p))
        data = j.get("data") or {}
        rows = data.get("rows") or []
        print(f"  earnings calendar 2024-01-25: rows {len(rows)}; fields {sorted(rows[0].keys()) if rows else []}; "
              f"time labels {dict(Counter(r.get('time') for r in rows))}")


def main():
    root, outdir = sys.argv[1], sys.argv[2]
    os.makedirs(outdir, exist_ok=False)
    fr = french_report(root)
    us_rows, us_sd = fr["F-F_Research_Data_Factors_daily"]
    days = [r[0] for r in us_rows]
    print("\n== Month-end calendar (US trading days from the French file dates)")
    month_end_calendar(days, US_SETTLEMENT, "US", outdir)
    fomc_report(root, days, outdir)
    auctions_report(root, outdir)
    treasury_report(root)
    nasdaq_report(root)


if __name__ == "__main__":
    main()
