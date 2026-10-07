"""HTTP + on-disk cache + pacing. Stdlib only.

Everything fetched lands under research/copytrade/data/ (gitignored). Only
responses that can no longer change are cached: callers put every
time-dependent input (date, window bounds) into the cache key.
"""
import hashlib
import json
import time
import urllib.error
import urllib.request
from collections import deque
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / 'data'
CACHE = DATA / 'cache'

_RETRYABLE = {429, 500, 502, 503, 504}


def _request(url, body=None, timeout=60, retries=5):
    data = json.dumps(body).encode() if body is not None else None
    headers = {'Content-Type': 'application/json'} if body is not None else {}
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as res:
                return json.load(res)
        except urllib.error.HTTPError as err:
            if err.code not in _RETRYABLE or attempt == retries:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == retries:
                raise
        time.sleep(min(30, 2 ** attempt))


def get_json(url, **kw):
    return _request(url, **kw)


def post_json(url, body, **kw):
    return _request(url, body=body, **kw)


def cached(namespace, key, fetch):
    """Return fetch() memoised on disk under (namespace, key)."""
    digest = hashlib.sha256(key.encode()).hexdigest()[:24]
    path = CACHE / namespace / f'{digest}.json'
    if path.exists():
        return json.loads(path.read_text())
    value = fetch()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value))
    tmp.replace(path)
    return value


class WeightBudget:
    """Rolling 60-second weight limiter (Hyperliquid bills weight per IP)."""

    def __init__(self, per_minute):
        self.per_minute = per_minute
        self.spent = deque()  # (monotonic time, weight)

    def _used(self, now):
        while self.spent and now - self.spent[0][0] >= 60:
            self.spent.popleft()
        return sum(w for _, w in self.spent)

    def spend(self, weight):
        while True:
            now = time.monotonic()
            if self._used(now) + weight <= self.per_minute or not self.spent:
                self.spent.append((now, weight))
                return
            time.sleep(max(0.05, 60 - (now - self.spent[0][0])))


class RatePacer:
    """Minimum spacing between requests (Bitget limits requests per second)."""

    def __init__(self, per_second):
        self.gap = 1.0 / per_second
        self.last = 0.0

    def wait(self):
        delta = time.monotonic() - self.last
        if delta < self.gap:
            time.sleep(self.gap - delta)
        self.last = time.monotonic()
