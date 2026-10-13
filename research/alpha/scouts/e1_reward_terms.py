"""E1 scout: extract programme terms from Binance reward announcements (counts and terms only).

Usage:  python3 -I e1_reward_terms.py OUT_JSONL BODIES_JSONL [BODIES_JSONL ...]

Input: body texts written by e1_binance_detail.py (all released < 2026-07-01).
For each event-introducing article it extracts, by regex, terms only:
programme, project ordinal, token ticker, reward token amount and % of supply,
eligibility/farming window, listing time, per-user cap text, circulating
supply at listing (% of supply), and whether a second tranche is mentioned.
No price, yield or return is read or computed. Prints counts by programme and
half-year, ordinal gaps, and regex-miss counts.
"""
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone

INTRO = re.compile(r"^Introducing\s+(.*?)\((\w+)\).*?on Binance (HODLer Airdrops|Launchpool|Megadrop)", re.I)
ORD = re.compile(r"the (\d+)(?:st|nd|rd|th) project", re.I)
REWARD = re.compile(r"(HODLer Airdrops|Launchpool|Megadrop) Token Rewards:\s*([\d,\.]+)\s*(\w+)\s*\(([\d\.]+)%", re.I)
REWARD_NOPCT = re.compile(r"(?:HODLer Airdrops|Launchpool|Megadrop) Token Rewards:\s*([\d,\.]+)\s*(\w+)", re.I)
MAXSUP = re.compile(r"Max Token Supply:\s*([\d,\.]+)", re.I)
WINDOW_H = re.compile(r"from (\d{4}-\d{2}-\d{2} \d{2}:\d{2}) \(UTC\) to (\d{4}-\d{2}-\d{2} \d{2}:\d{2}) \(UTC\)")
FARM = re.compile(r"Farming Period:\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}) \(UTC\) to (\d{4}-\d{2}-\d{2} \d{2}:\d{2}) \(UTC\)")
LIST = re.compile(r"list \w+ at (\d{4}-\d{2}-\d{2} \d{2}:\d{2}) \(UTC\)")
CIRC = re.compile(r"Circulating Supply upon Listing on Binance\s*:?\s*([\d,\.]+)[^\(]*\(([\d\.]+)%")
CAP4 = re.compile(r"Hard Cap[^\n]*?≤\s*([\d\.]+)%", re.I)
CAPH = re.compile(r"Hourly Hard Cap per User", re.I)
TRANCHE = re.compile(r"Additional ([\d,\.]+) \w+ will be allocated", re.I)


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def half(ms):
    d = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return f"{d.year}H{1 if d.month <= 6 else 2}"


def main():
    out_path = sys.argv[1]
    rows = {}
    for p in sys.argv[2:]:
        with open(p) as f:
            for line in f:
                r = json.loads(line)
                rows[r["code"]] = r
    events, misses = [], Counter()
    for r in sorted(rows.values(), key=lambda r: r["releaseDate"]):
        m = INTRO.search(r["title"].strip())
        if not m:
            continue
        prog = {"hodler airdrops": "HODLer", "launchpool": "Launchpool",
                "megadrop": "Megadrop"}[m.group(3).lower()]
        t = r["text"]
        ev = {"announced": iso(r["releaseDate"]), "half": half(r["releaseDate"]),
              "programme": prog, "title": r["title"].strip(),
              "edited_on_or_after_cutoff": r["edited_on_or_after_cutoff"]}
        o = ORD.search(t)
        ev["ordinal"] = int(o.group(1)) if o else None
        rw = REWARD.findall(t)
        ev["rewards"] = [{"amount": a.replace(",", ""), "token": tok, "pct_supply": pct}
                         for _, a, tok, pct in rw]
        if not ev["rewards"]:
            m2, ms = REWARD_NOPCT.search(t), MAXSUP.search(t)
            if m2 and ms:
                amt = float(m2.group(1).replace(",", ""))
                sup = float(ms.group(1).replace(",", ""))
                ev["rewards"] = [{"amount": m2.group(1).replace(",", ""), "token": m2.group(2),
                                  "pct_supply": f"{100 * amt / sup:.2f}", "pct_derived": True}]
        w = FARM.search(t) or WINDOW_H.search(t)
        ev["window"] = list(w.groups()) if w else None
        li = LIST.search(t)
        ev["listing_utc"] = li.group(1) if li else None
        c = CIRC.search(t)
        ev["circ_at_listing_pct"] = c.group(2) if c else None
        cap = CAP4.search(t)
        ev["holding_cap_pct"] = cap.group(1) if cap else None
        ev["hourly_cap_per_user"] = bool(CAPH.search(t))
        tr = TRANCHE.search(t)
        ev["second_tranche_amount"] = tr.group(1).replace(",", "") if tr else None
        for k in ("ordinal", "window", "listing_utc"):
            if ev[k] is None:
                misses[(prog, k)] += 1
        if not ev["rewards"]:
            misses[(prog, "rewards")] += 1
        events.append(ev)
    with open(out_path, "w") as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    by = Counter((ev["programme"], ev["half"]) for ev in events)
    print("events by programme x half-year (announcement date):")
    for prog in ("HODLer", "Launchpool", "Megadrop"):
        print(" ", prog, {h: n for (p, h), n in sorted(by.items()) if p == prog},
              "total", sum(n for (p, h), n in by.items() if p == prog))
    ords = sorted(ev["ordinal"] for ev in events if ev["programme"] == "HODLer" and ev["ordinal"])
    if ords:
        gaps = sorted(set(range(ords[0], ords[-1] + 1)) - set(ords))
        print("HODLer ordinals", ords[0], "->", ords[-1], "missing:", gaps)
    lp = sorted(ev["ordinal"] for ev in events if ev["programme"] == "Launchpool" and ev["ordinal"])
    if lp:
        print("Launchpool ordinals", lp[0], "->", lp[-1], "missing:",
              sorted(set(range(lp[0], lp[-1] + 1)) - set(lp)))
    print("multi-token events:", sum(1 for ev in events if len(ev["rewards"]) > 1))
    print("second-tranche mentions:", sum(1 for ev in events if ev["second_tranche_amount"]))
    print("edited on/after cutoff:", sum(1 for ev in events if ev["edited_on_or_after_cutoff"]))
    print("regex misses:", dict(misses))
    print("HODLer reward % of max supply, by half-year (median, min, max):")
    for h in sorted({ev["half"] for ev in events}):
        v = sorted(float(r["pct_supply"]) for ev in events if ev["programme"] == "HODLer"
                   and ev["half"] == h for r in ev["rewards"])
        if v:
            print(" ", h, "n", len(v), "median", v[len(v) // 2], "min", v[0], "max", v[-1])


if __name__ == "__main__":
    main()
