"""N2 scout: guarded HTTP for keyless public endpoints.

Holdout rule: every request must carry an explicit end bound before 2026-07-01, and any
returned row stamped on or after the cutoff is a hard error (it would be a holdout read).
Errors are raised, never returned as None, so no caller can cache an error or an empty body
as if it were data.
"""
import datetime as dt
import hashlib
import json
import pathlib
import time
import urllib.error
import urllib.parse
import urllib.request

CUTOFF_MS = int(dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)
END_KEYS = ("end_timestamp", "endTime", "end", "after", "end_ts")
UA = {"User-Agent": "Mozilla/5.0 (n2-scout; research)"}


def ms(y, m, d, h=0):
    return int(dt.datetime(y, m, d, h, tzinfo=dt.timezone.utc).timestamp() * 1000)


def iso(t):
    return dt.datetime.fromtimestamp(int(t) / 1000, dt.timezone.utc).strftime("%Y-%m-%d %H:%M")


class FetchError(RuntimeError):
    pass


def get_json(url, params, *, need_end=True, tries=5, pause=0.0):
    ends = [k for k in END_KEYS if k in params]
    if need_end:
        assert ends, f"no end bound in {params}"
    for k in ends:
        assert int(params[k]) < CUTOFF_MS, f"{k}={params[k]} not before the holdout cutoff"
    full = url + ("?" + urllib.parse.urlencode(params) if params else "")
    last = None
    for i in range(tries):
        req = urllib.request.Request(full, headers=UA)
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                body = r.read()
            out = json.loads(body)
            if isinstance(out, dict) and out.get("error"):
                raise FetchError(f"api error {out['error']}")
            if pause:
                time.sleep(pause)
            return out, len(body)
        except urllib.error.HTTPError as e:
            last = f"http {e.code}: {e.read()[:200]!r}"
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(2 ** i)
                continue
            raise FetchError(last) from e
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            last = repr(e)
            time.sleep(2 ** i)
    raise FetchError(f"gave up: {last}")


def assert_pre_cutoff(stamps, tag=""):
    bad = [t for t in stamps if int(t) >= CUTOFF_MS]
    if bad:
        raise FetchError(f"{tag}: {len(bad)} rows on/after cutoff; refusing")


def new_dir(path):
    p = pathlib.Path(path)
    if p.exists() and any(p.iterdir()):
        raise FetchError(f"{p} exists and is not empty: each pull goes in a NEW directory")
    p.mkdir(parents=True, exist_ok=True)
    return p


def write_sums(d):
    d = pathlib.Path(d)
    lines = []
    for f in sorted(d.rglob("*")):
        if f.is_file() and f.name != "SHA256SUMS":
            lines.append(f"{hashlib.sha256(f.read_bytes()).hexdigest()}  {f.relative_to(d)}")
    (d / "SHA256SUMS").write_text("\n".join(lines) + "\n")
    return len(lines)
