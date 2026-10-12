"""N3 scout: point-in-time quality of the DefiLlama emissions-adapters unlock calendar (schedule metadata only).

BEHAVIOUR ONLY; no price is touched. Inputs (from a public fork whose history ends 2026-03-21, all < 2026-07-01):
  adds.txt  : `git log --diff-filter=A --format='C %cI' --name-only -- protocols/`
  dels.txt  : same with --diff-filter=D
  protocols : the adapter files at the fork HEAD (TypeScript, read as TEXT; never executed)
Crude regex over each adapter: 10-digit unix timestamps and ISO dates ('YYYY-MM-DD') appearing as literals,
and counts of manualCliff/manualStep/manualLinear calls. Literal dates >= 2026-07-01 are dropped and not counted.
Reports: adapters added per quarter; for each adapter, how its earliest literal schedule date compares with the
date the adapter was first committed (i.e. was the schedule recorded before or after vesting started).

Usage: python -I n3_unlocks.py <adds.txt> <dels.txt> <protocols_dir>
"""
import collections
import datetime as dt
import os
import re
import sys

CUT = dt.date(2026, 7, 1)
TS = re.compile(r"(?<![\d.])(1[4-7]\d{8})(?![\d])")
ISO = re.compile(r"[\"'](20[12]\d-[01]\d-[0-3]\d)[\"']")


def parse_log(p):
    out, cur = {}, None
    for line in open(p):
        line = line.strip()
        if line.startswith("C "):
            cur = dt.datetime.fromisoformat(line[2:]).astimezone(dt.UTC).date()
        elif line.startswith("protocols/") and cur:
            name = line.split("/", 1)[1]
            out.setdefault(name, []).append(cur)
    return {k: min(v) for k, v in out.items()}


def main():
    adds, dels, pdir = parse_log(sys.argv[1]), parse_log(sys.argv[2]), sys.argv[3]
    q = collections.Counter(f"{d.year}Q{(d.month - 1) // 3 + 1}" for d in adds.values())
    print("adapter files first added, by quarter:", dict(sorted(q.items())))
    print(f"files ever added {len(adds)}; files deleted at some point {len(dels)}")
    rel = collections.Counter()
    cliffs = collections.Counter()
    lag_days = []
    nd = 0
    for fn in sorted(os.listdir(pdir)):
        txt = open(os.path.join(pdir, fn), errors="replace").read()
        dates = []
        for m in TS.finditer(txt):
            d = dt.datetime.fromtimestamp(int(m.group(1)), dt.UTC).date()
            if d < CUT:
                dates.append(d)
        for m in ISO.finditer(txt):
            try:
                d = dt.date.fromisoformat(m.group(1))
            except ValueError:
                continue
            if d < CUT:
                dates.append(d)
        cliffs["files with manualCliff"] += "manualCliff(" in txt
        cliffs["files with manualStep"] += "manualStep(" in txt
        cliffs["files with manualLinear"] += "manualLinear(" in txt
        cliffs["files using on-chain balance/vesting adapters"] += bool(re.search(r"from \"\.\./adapters/(?!manual)", txt))
        a = adds.get(fn)
        if not dates or a is None:
            nd += 1
            continue
        first = min(dates)
        lag = (a - first).days
        lag_days.append(lag)
        rel["earliest schedule date BEFORE adapter commit (backfilled)" if lag > 0 else
            "schedule recorded on/before its first date (ex-ante)"] += 1
    print("adapter files at HEAD:", len(os.listdir(pdir)), "| no literal pre-cutoff date or no add record:", nd)
    print(dict(cliffs))
    print(dict(rel))
    lag_days.sort()
    if lag_days:
        n = len(lag_days)
        print("commit date minus earliest literal schedule date (days): p10/p25/p50/p75/p90 =",
              [lag_days[int(n * p)] for p in (0.1, 0.25, 0.5, 0.75, 0.9)])
        print("share recorded >= 30 days before vesting start:", round(sum(x <= -30 for x in lag_days) / n, 3),
              "| within +-30 d:", round(sum(-30 < x < 30 for x in lag_days) / n, 3),
              "| >= 365 d after:", round(sum(x >= 365 for x in lag_days) / n, 3))


if __name__ == "__main__":
    main()


def literal_dates_vs_commit(adds_p, pdir):
    """Literal schedule dates (< cutoff) by year, split by whether they fall AFTER the adapter's first commit
    (i.e. the calendar held them in advance) or before (backfilled). Schedule metadata only."""
    adds = parse_log(adds_p)
    tab = collections.defaultdict(lambda: [0, 0])
    for fn in sorted(os.listdir(pdir)):
        a = adds.get(fn)
        if a is None:
            continue
        txt = open(os.path.join(pdir, fn), errors="replace").read()
        ds = set()
        for m in TS.finditer(txt):
            ds.add(dt.datetime.fromtimestamp(int(m.group(1)), dt.UTC).date())
        for m in ISO.finditer(txt):
            try:
                ds.add(dt.date.fromisoformat(m.group(1)))
            except ValueError:
                pass
        for d in ds:
            if d < CUT:
                tab[d.year][int(d > a)] += 1
    print("distinct literal schedule dates per adapter, by year: [before first commit, after first commit]",
          dict(sorted(tab.items())))


if __name__ == "__main__" and len(sys.argv) > 4 and sys.argv[4] == "--literal":
    literal_dates_vs_commit(sys.argv[1], sys.argv[3])
