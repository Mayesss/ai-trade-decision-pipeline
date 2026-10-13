"""N2 scout: Bybit linear BTCUSDT/ETHUSDT open interest, 1h, 2020-08 -> 2023-12-31 (keyless REST).

Extends the Binance multi-year proxy back to 2020-08 (Binance's per-symbol metrics panel starts
2021-12). Stops at 2023-12-31 so 009's holdout A (2024-01 -> 2025-07) and later stay unread.
Pages FORWARD in fixed 200-hour windows (one page each; a window that returns a cursor with a
full page is an error, never silently truncated). Prints counts and timestamps only.

Usage: ../.venv/bin/python -I n2_bybit.py <new outdir>
"""
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import n2_net as net  # noqa: E402

URL = "https://api.bybit.com/v5/market/open-interest"
H = 3_600_000
END = net.ms(2024, 1, 1) - 1


def pull(sym, start):
    rows, t, nreq = {}, start, 0
    while t <= END:
        end = min(t + 199 * H - 1, END)  # <= 199 rows: a full 200-row page means truncation
        out, _ = net.get_json(URL, {"category": "linear", "symbol": sym, "intervalTime": "1h",
                                    "startTime": t, "endTime": end, "limit": 200}, pause=0.12)
        nreq += 1
        if out.get("retCode") != 0:
            raise net.FetchError(f"{sym} {net.iso(t)}: retCode {out.get('retCode')} {out.get('retMsg')}")
        lst = out["result"]["list"]
        if len(lst) >= 200:
            raise net.FetchError(f"{sym} {net.iso(t)}: window overflow")
        stamps = [int(r["timestamp"]) for r in lst]
        net.assert_pre_cutoff(stamps, sym)
        for r in lst:
            ts = int(r["timestamp"])
            if t <= ts <= end:
                rows[ts] = float(r["openInterest"])
        t = end + 1
    return sorted(rows.items()), nreq


def main(outdir):
    d = net.new_dir(outdir)
    for sym, start in (("BTCUSDT", net.ms(2020, 8, 1)), ("ETHUSDT", net.ms(2020, 10, 1))):
        rows, nreq = pull(sym, start)
        (d / f"{sym}_oi_1h.json").write_text(json.dumps(rows))
        gaps = sum(1 for a, b in zip(rows, rows[1:]) if b[0] - a[0] > H)
        print(f"{sym}: {len(rows)} hourly OI rows {net.iso(rows[0][0])} .. {net.iso(rows[-1][0])}; "
              f"gaps>1h {gaps}; {nreq} requests")
    print("files:", net.write_sums(d))


if __name__ == "__main__":
    main(sys.argv[1])
