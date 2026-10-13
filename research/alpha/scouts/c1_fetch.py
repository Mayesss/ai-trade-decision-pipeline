"""C1 scout fetcher: Binance USD-M monthly archive files (keyless, data.binance.vision).

Behaviour-only scout. Downloads, for every USDT-quoted USD-M symbol prefix in the
public bucket, the MONTHLY files of one dataset whose month is <= 2026-06. Keys dated
2026-07 or later are COUNTED ONLY (never downloaded, never stored by name).
Every zip is verified against Binance's .CHECKSUM; a SHA256SUMS file is written.

Usage:
  python -I c1_fetch.py <out_dir> klines-1d            # monthly/klines/<SYM>/1d
  python -I c1_fetch.py <out_dir> fundingRate <symfile> # monthly/fundingRate, symbols listed in symfile
out_dir must not exist yet (each pull in its own new directory).
"""
import concurrent.futures as cf
import hashlib
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

LIST = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
DL = "https://data.binance.vision/"
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
MONTH = re.compile(r"-(\d{4})-(\d{2})\.zip$")
CUT = (2026, 7)  # first month NOT allowed
SYM_RE = re.compile(r"^[A-Z0-9]+USDT$")


def _get(url, tries=5):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):  # never retry around an access refusal
                raise
            if i == tries - 1:
                raise
        except Exception:  # noqa: BLE001
            if i == tries - 1:
                raise
        time.sleep(1.5 * (i + 1))


def list_objects(prefix, delimiter=None):
    marker = ""
    while True:
        q = {"prefix": prefix, "marker": marker}
        if delimiter:
            q["delimiter"] = delimiter
        root = ET.fromstring(_get(LIST + "?" + urllib.parse.urlencode(q)))
        last = None
        for c in root.findall(NS + "Contents"):
            last = c.find(NS + "Key").text
            yield ("obj", last, int(c.find(NS + "Size").text))
        for p in root.findall(NS + "CommonPrefixes"):
            last = p.find(NS + "Prefix").text
            yield ("pre", last, 0)
        if root.find(NS + "IsTruncated").text != "true" or last is None:
            return
        marker = root.findtext(NS + "NextMarker") or last


def fetch_one(key, out_dir):
    data = _get(DL + key)
    chk = _get(DL + key + ".CHECKSUM").decode().split()[0].strip().lower()
    h = hashlib.sha256(data).hexdigest()
    if h != chk:
        raise ValueError(f"checksum mismatch {key}")
    rel = key.split("/monthly/", 1)[1]
    path = os.path.join(out_dir, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(data)
    return rel, h, len(data)


def main():
    out_dir, ds = sys.argv[1], sys.argv[2]
    if os.path.exists(out_dir):
        sys.exit("refuse: out_dir exists (each pull goes in its own new directory)")
    if ds == "klines-1d":
        base, sub = "data/futures/um/monthly/klines/", "1d/"
        syms = [k[len(base):].rstrip("/") for t, k, _ in list_objects(base, "/") if t == "pre"]
    elif ds == "fundingRate":
        base, sub = "data/futures/um/monthly/fundingRate/", ""
        with open(sys.argv[3]) as fh:
            syms = [s.strip() for s in fh if s.strip()]
    elif ds == "fundingRate-months":
        # argv[3]: CSV "SYMBOL,YYYY-MM" (e.g. top-20 member months); only those files are fetched
        base, sub = "data/futures/um/monthly/fundingRate/", ""
        want = set()
        with open(sys.argv[3]) as fh:
            for line in fh:
                if line.strip():
                    s_, m_ = line.strip().split(",")
                    if m_ < "2026-07":
                        want.add((s_, m_))
        syms = sorted({s_ for s_, _ in want})
        os.makedirs(out_dir)
        keys = [f"{base}{s_}/{s_}-fundingRate-{m_}.zip" for s_, m_ in sorted(want)]
        sums, errs, nbytes = [], [], 0
        with cf.ThreadPoolExecutor(24) as ex:
            futs = {ex.submit(fetch_one, k, out_dir): k for k in keys}
            for f in cf.as_completed(futs):
                try:
                    rel, h, n = f.result()
                    sums.append((rel, h))
                    nbytes += n
                except Exception as e:  # noqa: BLE001
                    errs.append((futs[f], repr(e)[:120]))
        sums.sort()
        with open(os.path.join(out_dir, "SHA256SUMS"), "w") as fh:
            for rel, h in sums:
                fh.write(f"{h}  {rel}\n")
        with open(os.path.join(out_dir, "FETCH_LOG.txt"), "w") as fh:
            fh.write(f"dataset={ds} pairs={len(keys)} files_ok={len(sums)} bytes={nbytes} errors={len(errs)}\n")
            for k, e in errs:
                fh.write(f"ERR {k} {e}\n")
        print(f"ok={len(sums)} bytes={nbytes} errors={len(errs)}", errs[:5])
        return
    else:
        sys.exit("unknown dataset")
    syms = [s for s in syms if SYM_RE.match(s)]
    os.makedirs(out_dir)
    keys, post = [], 0

    def lst(sym):
        ks, p = [], 0
        for t, k, _ in list_objects(base + sym + "/" + sub):
            if t != "obj" or not k.endswith(".zip"):
                continue
            m = MONTH.search(k)
            if not m:
                continue
            if (int(m.group(1)), int(m.group(2))) >= CUT:
                p += 1  # count only
                continue
            ks.append(k)
        return ks, p

    with cf.ThreadPoolExecutor(24) as ex:
        for ks, p in ex.map(lst, syms):
            keys.extend(ks)
            post += p
    keys.sort()
    print(f"symbols={len(syms)} keys_pre={len(keys)} keys_post_counted_only={post}", flush=True)
    sums, errs, nbytes = [], [], 0
    with cf.ThreadPoolExecutor(24) as ex:
        futs = {ex.submit(fetch_one, k, out_dir): k for k in keys}
        for f in cf.as_completed(futs):
            try:
                rel, h, n = f.result()
                sums.append((rel, h))
                nbytes += n
            except Exception as e:  # noqa: BLE001
                errs.append((futs[f], repr(e)[:120]))
    sums.sort()
    with open(os.path.join(out_dir, "SHA256SUMS"), "w") as fh:
        for rel, h in sums:
            fh.write(f"{h}  {rel}\n")
    with open(os.path.join(out_dir, "FETCH_LOG.txt"), "w") as fh:
        fh.write(f"dataset={ds} symbols={len(syms)} files_ok={len(sums)} bytes={nbytes} "
                 f"errors={len(errs)} post_cutoff_keys_counted_only={post}\n")
        for k, e in errs:
            fh.write(f"ERR {k} {e}\n")
    print(f"ok={len(sums)} bytes={nbytes} errors={len(errs)}", errs[:5])


if __name__ == "__main__":
    main()
