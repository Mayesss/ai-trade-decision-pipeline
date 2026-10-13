"""E1 scout: fetch body text of pre-cutoff Binance reward-programme announcements.

Usage:  python3 -I e1_binance_detail.py TITLES_DIR OUTDIR [SKIP_BODIES_JSONL ...]

Codes already present in any SKIP file are not refetched (resume after a 429).
On HTTP 429 the fetcher waits 90 s and retries (max 3), then gives up on that
code; base spacing is 6 s per request.

Reads TITLES_DIR/articles_*.jsonl (already end-dated < 2026-07-01 by
e1_binance_cms.py), selects titles naming HODLer Airdrops, Launchpool or
Megadrop, and fetches each article's detail JSON. Only the body text (walked
from the rich-text tree), the title, code and publishDate are written.
`relatedArticles` (which can list post-cutoff titles) is discarded unread;
`lastUpdateTime` is reduced to a boolean "edited on/after 2026-07-01".
No price, yield or return is read. 
"""
import glob
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

CUTOFF_MS = 1782864000000
URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode={code}"
UA = {"User-Agent": "Mozilla/5.0 (personal research; low rate)"}
KEY = re.compile(r"hodler|launchpool|megadrop", re.I)


def walk(n, out):
    if isinstance(n, dict):
        if n.get("node") == "text":
            out.append(n.get("text", ""))
        for c in n.get("child", []) or []:
            walk(c, out)
        if n.get("tag") in ("p", "li", "tr", "td", "h1", "h2", "h3", "br"):
            out.append("\n")
    elif isinstance(n, list):
        for c in n:
            walk(c, out)


def main():
    tdir, outdir = sys.argv[1], sys.argv[2]
    skip = set()
    for sp in sys.argv[3:]:
        with open(sp) as f:
            for line in f:
                skip.add(json.loads(line)["code"])
    os.makedirs(outdir, exist_ok=False)
    rows = {}
    for p in sorted(glob.glob(os.path.join(tdir, "articles_*.jsonl"))):
        with open(p) as f:
            for line in f:
                r = json.loads(line)
                if (r["releaseDate"] < CUTOFF_MS and KEY.search(r["title"])
                        and r["code"] not in skip):
                    rows.setdefault(r["code"], r)
    out = open(os.path.join(outdir, "bodies.jsonl"), "w")
    n_ok = n_err = 0
    for code, r in sorted(rows.items(), key=lambda kv: kv[1]["releaseDate"]):
        try:
            d = None
            for attempt in range(4):
                try:
                    req = urllib.request.Request(URL.format(code=code), headers=UA)
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        d = json.loads(resp.read().decode("utf-8"))
                    break
                except urllib.error.HTTPError as he:
                    if he.code == 429 and attempt < 3:
                        time.sleep(90)
                        continue
                    raise
            data = d["data"]
            parts = []
            walk(json.loads(data["body"]), parts)
            text = re.sub(r"&nbsp;", " ", "".join(parts))
            text = text.replace("&amp;", "&")
            lu = data.get("lastUpdateTime")
            out.write(json.dumps({
                "code": code, "title": r["title"], "releaseDate": r["releaseDate"],
                "publishDate": data.get("publishDate"),
                "edited_on_or_after_cutoff": bool(lu and int(lu) >= CUTOFF_MS),
                "text": text}, ensure_ascii=False) + "\n")
            n_ok += 1
        except Exception as e:  # never cache an error: record and continue
            print("ERR", code, type(e).__name__)
            n_err += 1
        out.flush()
        time.sleep(6)
    out.close()
    print(json.dumps({"selected": len(rows), "ok": n_ok, "err": n_err}))


if __name__ == "__main__":
    main()
