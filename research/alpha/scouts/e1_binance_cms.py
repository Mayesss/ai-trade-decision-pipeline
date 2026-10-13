"""E1 scout: Binance CMS announcement titles, drop-at-load end-dated fetcher.

Usage:  python3 -I e1_binance_cms.py OUTDIR CATALOG_ID [CATALOG_ID ...]

The CMS list endpoint is newest-first and cannot be end-dated. This fetcher
therefore drops every article whose releaseDate is on or after 2026-07-01
00:00 UTC at load: it is counted (a number only), never printed, never
written. Articles before 2024-01-01 are also dropped (out of scope) and paging
stops once a full page is older than that. Only keyless public JSON is read.
Writes OUTDIR/articles_<catalog>.jsonl (id, code, title, releaseDate) and
OUTDIR/counts.json (kept / dropped counts). One request per ~1.2 s.
"""
import json
import os
import sys
import time
import urllib.request

CUTOFF_MS = 1782864000000  # 2026-07-01T00:00:00Z
START_MS = 1704067200000  # 2024-01-01T00:00:00Z
URL = ("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
       "?type=1&catalogId={cat}&pageNo={page}&pageSize=50")
UA = {"User-Agent": "Mozilla/5.0 (personal research; low rate)"}


def fetch(url):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status}")
        return json.loads(r.read().decode("utf-8"))


def main():
    outdir = sys.argv[1]
    cats = [int(c) for c in sys.argv[2:]]
    os.makedirs(outdir, exist_ok=False)
    counts = {}
    for cat in cats:
        kept, dropped_post, dropped_pre, pages = [], 0, 0, 0
        page = 1
        while True:
            d = fetch(URL.format(cat=cat, page=page))
            pages += 1
            if d.get("code") != "000000":
                raise RuntimeError(f"api code {d.get('code')}")  # never cache an error
            arts = []
            for c in d["data"]["catalogs"]:
                arts.extend(c.get("articles") or [])
            if not arts:
                break
            older = 0
            for a in arts:
                ts = int(a["releaseDate"])
                if ts >= CUTOFF_MS:
                    dropped_post += 1  # count only
                    continue
                if ts < START_MS:
                    dropped_pre += 1
                    older += 1
                    continue
                kept.append({"id": a["id"], "code": a["code"], "title": a["title"],
                             "releaseDate": ts})
            if older == len(arts):
                break
            page += 1
            time.sleep(1.2)
        with open(os.path.join(outdir, f"articles_{cat}.jsonl"), "w") as f:
            for row in kept:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        counts[str(cat)] = {"kept_2024_01_to_2026_06": len(kept),
                            "dropped_on_or_after_2026_07_01_count_only": dropped_post,
                            "dropped_before_2024_01_seen_on_last_page": dropped_pre,
                            "pages": pages}
        time.sleep(1.2)
    with open(os.path.join(outdir, "counts.json"), "w") as f:
        json.dump(counts, f, indent=1)
    print(json.dumps(counts))


if __name__ == "__main__":
    main()
