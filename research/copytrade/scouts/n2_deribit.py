"""N2 scout: Deribit keyless public data — probes and small, end-bounded pulls.

Every request carries an end bound before 2026-07-01 (n2_net guard). Every pull goes to a NEW
directory under data/scout_n2/<source>/ and gets a SHA256SUMS. Prints counts, timestamps and
sizes only; values are analysed in n2_analyze.py.

Usage (from research/copytrade/scouts):
  ../.venv/bin/python -I n2_deribit.py dvol   <outdir>          # DVOL 1h, BTC+ETH, 2021-03 -> 2026-06-30 minus holdout A
  ../.venv/bin/python -I n2_deribit.py perp   <outdir>          # BTC/ETH-PERPETUAL 1h candles, 2019 -> 2026-06-30 minus holdout A
  ../.venv/bin/python -I n2_deribit.py trades <outdir> DAY...   # every option trade, BTC+ETH, full UTC days
  ../.venv/bin/python -I n2_deribit.py window <outdir> CUR START_MS END_MS   # option trades in one window
  ../.venv/bin/python -I n2_deribit.py snapwin <outdir> SNAPLIST [HOURS]     # trades in the hour before each map snapshot
"""
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import n2_net as net  # noqa: E402

WWW = "https://www.deribit.com/api/v2/public/"
HIST = "https://history.deribit.com/api/v2/public/"
DAY = 86_400_000
LAST_MS = net.CUTOFF_MS - 1
# 009's holdout A (2024-01-01 -> 2025-07-27) is released and UNREAD; it is a candidate holdout for
# N2, so this scout reads no Deribit or underlying data dated inside it.
SKIP = (net.ms(2024, 1, 1), net.ms(2025, 7, 28) - 1)


def skip_windows(t, end):
    """Clip [t, end] around the SKIP span; returns (t, end) or None if fully inside."""
    if end < SKIP[0] or t > SKIP[1]:
        return t, end
    if t < SKIP[0]:
        return t, SKIP[0] - 1
    return None


def dvol(outdir):
    d = net.new_dir(outdir)
    for cur in ("BTC", "ETH"):
        rows, t, nreq, nbytes = [], net.ms(2021, 3, 1), 0, 0
        step = 30 * DAY  # 720 hourly points per window; one page each (checked below)
        while t <= LAST_MS:
            end = min(t + step - 1, LAST_MS)
            w = skip_windows(t, end)
            if w is None:
                t = SKIP[1] + 1
                continue
            t, end = w
            out, nb = net.get_json(WWW + "get_volatility_index_data",
                                   {"currency": cur, "start_timestamp": t, "end_timestamp": end,
                                    "resolution": "3600"}, pause=0.15)
            nreq += 1
            nbytes += nb
            res = out["result"]
            if res.get("continuation") not in (None, "none"):
                raise net.FetchError(f"{cur} window {net.iso(t)} truncated (continuation set)")
            data = res["data"]
            net.assert_pre_cutoff([r[0] for r in data], f"dvol {cur}")
            rows.extend(r for r in data if t <= r[0] <= end)
            t = end + 1
        rows = sorted({r[0]: r for r in rows}.values())
        (d / f"dvol_{cur}_1h.json").write_text(json.dumps(rows))
        gaps = sum(1 for a, b in zip(rows, rows[1:]) if b[0] - a[0] > 3_600_000)
        print(f"DVOL {cur}: {len(rows)} hourly rows {net.iso(rows[0][0])} .. {net.iso(rows[-1][0])}; "
              f"gaps>1h {gaps}; {nreq} requests, {nbytes/1e6:.2f} MB")
    print("files:", net.write_sums(d))


def perp(outdir):
    d = net.new_dir(outdir)
    for inst in ("BTC-PERPETUAL", "ETH-PERPETUAL"):
        # 1h candles, not 1D: Deribit stamps 1D candles at 08:00 UTC, so the last daily candle
        # (2026-06-30 08:00) would span into the holdout. An hourly candle stamped <= 23:00 does not.
        rows, t = [], net.ms(2019, 1, 1)
        step = 30 * DAY
        while t <= LAST_MS:
            end = min(t + step - 1, LAST_MS)
            w = skip_windows(t, end)
            if w is None:
                t = SKIP[1] + 1
                continue
            t, end = w
            out, _ = net.get_json(WWW + "get_tradingview_chart_data",
                                  {"instrument_name": inst, "start_timestamp": t, "end_timestamp": end,
                                   "resolution": "60"}, pause=0.2)
            r = out["result"]
            if r.get("status") not in ("ok", "no_data"):
                raise net.FetchError(f"{inst}: status {r.get('status')}")
            if r.get("status") == "ok":
                net.assert_pre_cutoff(r["ticks"], inst)
                for i, tk in enumerate(r["ticks"]):
                    if t <= tk <= end - 3_600_000 + 1:  # the candle must close inside the window
                        rows.append([tk, r["open"][i], r["high"][i], r["low"][i], r["close"][i], r["volume"][i]])
            t = end + 1
        rows = sorted({r[0]: r for r in rows}.values())
        (d / f"{inst}_1h.json").write_text(json.dumps(rows))
        print(f"{inst}: {len(rows)} hourly rows {net.iso(rows[0][0])} .. {net.iso(rows[-1][0])}")
    print("files:", net.write_sums(d))


def trades_window(cur, start, end, sink=None):
    """Every option trade in [start, end], paging FORWARD by timestamp (inclusive restart,
    de-duplicated by trade_id). Returns (trades, requests, bytes)."""
    assert end < net.CUTOFF_MS
    assert end < SKIP[0] or start > SKIP[1], "window touches holdout A"
    seen, out_rows, t, nreq, nbytes = set(), [], start, 0, 0
    while True:
        out, nb = net.get_json(HIST + "get_last_trades_by_currency_and_time",
                               {"currency": cur, "kind": "option", "start_timestamp": t, "end_timestamp": end,
                                "count": 1000, "sorting": "asc", "include_old": "true"}, pause=0.05)
        nreq += 1
        nbytes += nb
        tr = out["result"]["trades"]
        net.assert_pre_cutoff([x["timestamp"] for x in tr], f"trades {cur}")
        if sink is not None:
            sink.append(out)
        new = [x for x in tr if x["trade_id"] not in seen]
        for x in new:
            seen.add(x["trade_id"])
        out_rows.extend(new)
        if not out["result"].get("has_more") or not tr:
            break
        nxt = tr[-1]["timestamp"]
        if not new and nxt == t:
            raise net.FetchError(f"{cur}: >1000 trades on one ms at {t}; cannot page by time")
        t = nxt
    return out_rows, nreq, nbytes


def trades(outdir, days):
    d = net.new_dir(outdir)
    for day in days:
        y, m, dd = map(int, day.split("-"))
        s = net.ms(y, m, dd)
        for cur in ("BTC", "ETH"):
            pages = []
            t0 = time.time()
            rows, nreq, nbytes = trades_window(cur, s, s + DAY - 1, pages)
            (d / f"{cur}_{day}.json").write_text(json.dumps(rows))
            stamps = [x["timestamp"] for x in rows]
            insts = {x["instrument_name"] for x in rows}
            print(f"{cur} {day}: {len(rows)} trades, {len(insts)} instruments, "
                  f"{net.iso(min(stamps)) if stamps else '-'} .. {net.iso(max(stamps)) if stamps else '-'}; "
                  f"{nreq} req, {nbytes/1e6:.2f} MB raw, {time.time()-t0:.0f}s")
    print("files:", net.write_sums(d))


def window(outdir, cur, start, end):
    d = net.new_dir(outdir)
    rows, nreq, nbytes = trades_window(cur, int(start), int(end))
    (d / f"{cur}_{start}_{end}.json").write_text(json.dumps(rows))
    print(f"{cur}: {len(rows)} trades, {nreq} req, {nbytes/1e6:.2f} MB")
    print("files:", net.write_sums(d))


def snapwin(outdir, listfile, hours="1"):
    """Option trades in the `hours` before each HL map snapshot (past-only windows), BTC+ETH.
    Used only for the COINCIDENT skew/level at snapshot time (no later data)."""
    d = net.new_dir(outdir)
    snaps = [int(x) for x in pathlib.Path(listfile).read_text().split()]
    keep = ("timestamp", "instrument_name", "price", "mark_price", "iv", "index_price", "direction", "amount")
    for cur in ("BTC", "ETH"):
        allrows, nreq, nbytes = [], 0, 0
        for sm in snaps:
            rows, r, b = trades_window(cur, sm - int(float(hours) * 3_600_000), sm - 1)
            nreq += r
            nbytes += b
            allrows.extend({**{k: x[k] for k in keep}, "snap": sm} for x in rows)
        (d / f"{cur}_snapwin.json").write_text(json.dumps(allrows))
        print(f"{cur}: {len(allrows)} trades over {len(snaps)} windows, {nreq} req, {nbytes/1e6:.1f} MB raw")
    print("files:", net.write_sums(d))


if __name__ == "__main__":
    cmd, outdir, *rest = sys.argv[1:]
    {"dvol": lambda: dvol(outdir), "perp": lambda: perp(outdir),
     "trades": lambda: trades(outdir, rest), "window": lambda: window(outdir, *rest), "snapwin": lambda: snapwin(outdir, *rest)}[cmd]()
