"""N2 scout: list Binance options datasets on data.binance.vision (keys, sizes, dates only).
Counts files dated before / on-or-after 2026-07-01; fetches nothing.
Usage: ../.venv/bin/python -I n2_binance_options_list.py
"""
import re
import urllib.parse
import urllib.request

B = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"


def keys(prefix):
    marker, out = "", []
    while True:
        q = {"prefix": prefix, "max-keys": "1000"}
        if marker:
            q["marker"] = marker
        body = urllib.request.urlopen(B + "?" + urllib.parse.urlencode(q), timeout=60).read().decode()
        ks = re.findall(r"<Key>([^<]+)</Key><LastModified>[^<]*</LastModified><ETag>[^<]*</ETag><Size>(\d+)</Size>", body)
        out += ks
        if "<IsTruncated>true</IsTruncated>" not in body or not ks:
            return out
        marker = ks[-1][0]


for ds in ("BVOLIndex", "EOHSummary"):
    ks = [(k, int(s)) for k, s in keys(f"data/option/daily/{ds}/") if k.endswith(".zip")]
    by = {}
    for k, s in ks:
        sym = k.split("/")[4]
        day = re.search(r"(\d{4}-\d{2}-\d{2})\.zip$", k).group(1)
        by.setdefault(sym, []).append((day, s))
    for sym, v in sorted(by.items()):
        pre = [x for x in v if x[0] < "2026-07-01"]
        post = len(v) - len(pre)
        print(f"{ds} {sym}: files pre {len(pre)} ({sum(s for _, s in pre)/1e6:.1f} MB) "
              f"first {min(pre)[0] if pre else '-'} last-pre {max(pre)[0] if pre else '-'}; files >= cutoff {post}")
