"""B1 scout: free, keyless pulls. Each pull lands in its own new directory
under research/alpha/data/scout_b1/<pull>_<UTC stamp>/ with a SHA256SUMS.

Behaviour only. This script downloads; it computes nothing. Every API query
that takes a date range is end-dated 2026-06-30. Static files that cannot be
end-dated (Ken French zips) are filtered at load by b1_read.py and their
post-cutoff rows are never printed.

Usage: python3 -I b1_pull.py <pull-name> [...]
  french   Ken French US + regional daily 3-factor zips
  fred     FRED series (end-dated)
  auctions fiscaldata.treasury.gov auctions (calendar fields only)
  fomc     federalreserve.gov FOMC calendar pages (2014-2026)
  edgar    SEC EDGAR company_tickers + submissions JSON for xyz US names
  nasdaq   one Nasdaq historical-quote probe + one earnings-calendar probe
"""
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "scout_b1")
UA = "personal-research-scout-b1 (non-commercial, low-rate; python-urllib)"
END = "2026-06-30"


def newdir(name):
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    d = os.path.abspath(os.path.join(ROOT, f"{name}_{stamp}"))
    os.makedirs(d, exist_ok=False)
    return d


def get(url, headers=None, timeout=60):
    h = {"User-Agent": UA, "Accept": "*/*"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, b""
    except Exception as e:  # network error: never cache it
        return f"ERR {type(e).__name__}", b""


def save(d, fname, body, log):
    p = os.path.join(d, fname)
    with open(p, "wb") as f:
        f.write(body)
    log.append((fname, hashlib.sha256(body).hexdigest(), len(body)))


def finish(d, log, notes):
    with open(os.path.join(d, "SHA256SUMS"), "w") as f:
        for fname, h, _ in log:
            f.write(f"{h}  {fname}\n")
    with open(os.path.join(d, "PULL_LOG.json"), "w") as f:
        json.dump({"files": [{"name": a, "sha256": b, "bytes": c} for a, b, c in log], "notes": notes}, f, indent=1)
    print(d, len(log), "files", sum(c for _, _, c in log), "bytes")
    for n in notes:
        print("  note:", n)


def pull_french():
    d, log, notes = newdir("french"), [], []
    base = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
    for f in ["F-F_Research_Data_Factors_daily_CSV.zip", "Developed_3_Factors_Daily_CSV.zip",
              "Developed_ex_US_3_Factors_Daily_CSV.zip", "Europe_3_Factors_Daily_CSV.zip",
              "Japan_3_Factors_Daily_CSV.zip", "Asia_Pacific_ex_Japan_3_Factors_Daily_CSV.zip",
              "North_America_3_Factors_Daily_CSV.zip"]:
        st, body = get(base + f)
        notes.append(f"{f}: HTTP {st}, {len(body)} bytes (static file; contains post-cutoff rows; filter at load)")
        if st == 200 and body:
            save(d, f, body, log)
        time.sleep(1)
    finish(d, log, notes)


def pull_fred():
    d, log, notes = newdir("fred"), [], []
    for sid in ["SP500", "NASDAQ100", "DGS10", "DGS5", "DGS2", "DTB3"]:
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd=2014-01-01&coed={END}"
        st, body = get(url)
        notes.append(f"{sid}: HTTP {st}, {len(body)} bytes, coed={END}")
        if st == 200 and body:
            save(d, f"{sid}.csv", body, log)
        time.sleep(1)
    finish(d, log, notes)


def pull_auctions():
    d, log, notes = newdir("auctions"), [], []
    fields = "cusip,security_type,security_term,auction_date,issue_date,maturity_date,announcemt_date,offering_amt,reopening,cash_management_bill_cmb,floating_rate,inflation_index_security"
    base = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query"
    page = 1
    while True:
        url = (f"{base}?fields={fields}&filter=auction_date:gte:2014-01-01,auction_date:lte:{END}"
               f"&sort=auction_date&page[size]=1000&page[number]={page}")
        st, body = get(url)
        notes.append(f"page {page}: HTTP {st}, {len(body)} bytes")
        if st != 200 or not body:
            break
        save(d, f"auctions_p{page:02d}.json", body, log)
        meta = json.loads(body).get("meta", {})
        if page >= int(meta.get("total-pages", 1)):
            break
        page += 1
        time.sleep(1)
    finish(d, log, notes)


def pull_fomc():
    d, log, notes = newdir("fomc"), [], []
    urls = [("fomccalendars.htm", "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm")]
    for y in range(2014, 2021):
        urls.append((f"fomchistorical{y}.htm", f"https://www.federalreserve.gov/monetarypolicy/fomchistorical{y}.htm"))
    for fname, url in urls:
        st, body = get(url)
        notes.append(f"{fname}: HTTP {st}, {len(body)} bytes")
        if st == 200 and body:
            save(d, fname, body, log)
        time.sleep(1)
    finish(d, log, notes)


# xyz US single names with a company CIK (ETFs excluded); list from N1's activity_by_coin.csv
XYZ_US = ["NVDA", "TSLA", "PLTR", "AMZN", "GOOGL", "MSFT", "META", "AAPL", "COIN", "HOOD", "MSTR", "INTC",
          "AMD", "ORCL", "NFLX", "CRCL", "MU", "SNDK", "RIVN", "BABA", "USAR", "CRWV", "TSM", "COST", "DKNG",
          "HIMS", "LLY", "BIRD", "BX", "GME", "LITE", "MRVL", "RKLB", "EBAY", "ZM", "ARM", "BB", "ASML",
          "DELL", "IBM", "AVGO", "NOW", "NBIS", "WDC", "NOK", "BE", "QCOM", "AMAT"]


def pull_edgar():
    d, log, notes = newdir("edgar"), [], []
    st, body = get("https://www.sec.gov/files/company_tickers.json")
    notes.append(f"company_tickers.json: HTTP {st}, {len(body)} bytes (current state)")
    if st != 200 or not body:
        finish(d, log, notes)
        return
    save(d, "company_tickers.json", body, log)
    tick = {v["ticker"]: int(v["cik_str"]) for v in json.loads(body).values()}
    for t in XYZ_US:
        cik = tick.get(t)
        if cik is None:
            notes.append(f"{t}: no CIK in company_tickers")
            continue
        time.sleep(0.25)  # <= 4 req/s, well under SEC's 10/s
        st, b = get(f"https://data.sec.gov/submissions/CIK{cik:010d}.json")
        notes.append(f"{t}: CIK {cik} submissions HTTP {st}, {len(b)} bytes")
        if st != 200 or not b:
            continue
        save(d, f"sub_{t}.json", b, log)
        for extra in json.loads(b).get("filings", {}).get("files", []):
            name = extra.get("name")
            if not name:
                continue
            time.sleep(0.25)
            st2, b2 = get(f"https://data.sec.gov/submissions/{name}")
            notes.append(f"{t}: {name} HTTP {st2}, {len(b2)} bytes")
            if st2 == 200 and b2:
                save(d, f"sub_{t}__{name}", b2, log)
    finish(d, log, notes)


def pull_nasdaq():
    d, log, notes = newdir("nasdaq"), [], []
    h = {"User-Agent": "Mozilla/5.0 (Macintosh) personal-research", "Accept": "application/json, text/plain, */*",
         "Origin": "https://www.nasdaq.com", "Referer": "https://www.nasdaq.com/"}
    url = ("https://api.nasdaq.com/api/quote/AAPL/historical?assetclass=stocks"
           f"&fromdate=2014-01-01&todate={END}&limit=9999")
    st, body = get(url, headers=h, timeout=30)
    notes.append(f"historical AAPL 2014-01-01..{END}: HTTP {st}, {len(body)} bytes")
    if st == 200 and body:
        save(d, "hist_AAPL.json", body, log)
    time.sleep(2)
    st, body = get("https://api.nasdaq.com/api/calendar/earnings?date=2024-01-25", headers=h, timeout=30)
    notes.append(f"earnings calendar 2024-01-25: HTTP {st}, {len(body)} bytes")
    if st == 200 and body:
        save(d, "earnings_2024-01-25.json", body, log)
    finish(d, log, notes)


def pull_treasury():
    """home.treasury.gov daily par yield curve and bill rates, one CSV per year.
    The 2026 files cannot be end-dated: they contain post-cutoff rows; filter at load."""
    d, log, notes = newdir("treasury"), [], []
    base = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv"
    for y in range(2014, 2027):
        for typ in ["daily_treasury_yield_curve", "daily_treasury_bill_rates"]:
            url = f"{base}/{y}/all?type={typ}&field_tdr_date_value={y}&page&_format=csv"
            st, body = get(url, headers={"User-Agent": "Mozilla/5.0 personal-research"})
            notes.append(f"{typ} {y}: HTTP {st}, {len(body)} bytes" + (" (post-cutoff rows; filter at load)" if y == 2026 else ""))
            if st == 200 and body:
                save(d, f"{typ}_{y}.csv", body, log)
            time.sleep(1)
    finish(d, log, notes)


if __name__ == "__main__":
    fns = {"french": pull_french, "fred": pull_fred, "auctions": pull_auctions, "fomc": pull_fomc,
           "edgar": pull_edgar, "nasdaq": pull_nasdaq, "treasury": pull_treasury}
    for a in sys.argv[1:]:
        fns[a]()
