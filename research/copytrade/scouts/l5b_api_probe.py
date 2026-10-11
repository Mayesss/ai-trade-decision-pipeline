"""L5b scout: one keyless access probe of Kalshi's archived-trades endpoint.

Access check only. One GET, no API key, limit=1, hard date bound well before
2026-07-01 (2025-04-01 .. 2025-04-02 UTC). Prints the HTTP status, the
response's top-level keys and the field NAMES of the trade object. No price,
count or outcome value is printed or stored.

Endpoints deliberately NOT called: /historical/markets (no date filter, and it
carries resolutions), /historical/cutoff (no date bound).

Run: python3 -I research/copytrade/scouts/l5b_api_probe.py
"""

import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

BASE = "https://api.elections.kalshi.com/trade-api/v2"
MIN_TS = int(datetime(2025, 4, 1, tzinfo=timezone.utc).timestamp())
MAX_TS = int(datetime(2025, 4, 2, tzinfo=timezone.utc).timestamp())
GUARD = int(datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp())


def main() -> int:
    assert MAX_TS < GUARD, "date bound must stay before 2026-07-01"
    url = f"{BASE}/historical/trades?min_ts={MIN_TS}&max_ts={MAX_TS}&limit=1"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status = resp.status
            body = json.loads(resp.read(200_000))
    except urllib.error.HTTPError as exc:
        print(json.dumps({"status": exc.code, "reason": str(exc.reason)}))
        return 0
    trades = body.get("trades") or []
    print(json.dumps({
        "status": status,
        "top_level_keys": sorted(body.keys()),
        "n_trades_returned": len(trades),
        "trade_field_names": sorted(trades[0].keys()) if trades else [],
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
