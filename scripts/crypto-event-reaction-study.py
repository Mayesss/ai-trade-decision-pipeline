#!/usr/bin/env python3
"""Crypto post-event reaction study — replicates the gold/EUR CPI/NFP/FOMC
methodology on BTC/ETH/SOL using Bitget 1m USDT-FUTURES candles.

Questions (mirroring lib/swing/eventReaction.ts doctrine):
  1. Is the PRE-release drift direction a coin flip w.r.t. the post move?
  2. Does the ~45min reaction direction persist over the following 2-4h?
  3. Do events widen the range vs baseline while the net move stays ~baseline?
Validation: the release-minute bar range must spike vs the pre-release median
1m range, else the event date is flagged suspect (likely a wrong timestamp).

Usage: python3 scripts/crypto-event-reaction-study.py
No deps, no keys (public Bitget endpoints). Candles cache under
$CRYPTO_EVENT_STUDY_DIR (default ~/.cache/crypto-event-study), so re-runs are
cheap. To extend after a new CPI/NFP/FOMC cycle, append to EVENTS below —
release timestamps are 8:30 ET (12:30Z EDT / 13:30Z EST) for CPI/NFP and
14:00 ET for FOMC statements; the spike check will flag any wrong date.
Findings as of Jul 2026 are recorded in lib/swing/eventReaction.ts.
"""
import json, os, sys, time, urllib.request, urllib.parse, statistics as st

OUT_DIR = os.environ.get("CRYPTO_EVENT_STUDY_DIR") or os.path.expanduser("~/.cache/crypto-event-study")
CACHE = os.path.join(OUT_DIR, "candle_cache")
os.makedirs(CACHE, exist_ok=True)

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# (release date, UTC time, type). 8:30 ET = 12:30Z (EDT) / 13:30Z (EST);
# FOMC statement 14:00 ET = 18:00Z / 19:00Z.
EVENTS = [
    # --- CPI ---
    ("2024-06-12", "12:30", "CPI"), ("2024-07-11", "12:30", "CPI"),
    ("2024-08-14", "12:30", "CPI"), ("2024-09-11", "12:30", "CPI"),
    ("2024-10-10", "12:30", "CPI"), ("2024-11-13", "13:30", "CPI"),
    ("2024-12-11", "13:30", "CPI"), ("2025-01-15", "13:30", "CPI"),
    ("2025-02-12", "13:30", "CPI"), ("2025-03-12", "12:30", "CPI"),
    ("2025-04-10", "12:30", "CPI"), ("2025-05-13", "12:30", "CPI"),
    ("2025-06-11", "12:30", "CPI"), ("2025-07-15", "12:30", "CPI"),
    ("2025-08-12", "12:30", "CPI"), ("2025-09-11", "12:30", "CPI"),
    ("2025-10-24", "12:30", "CPI"),  # Sep CPI, shutdown-delayed
    ("2025-12-18", "13:30", "CPI"),  # Nov CPI, rescheduled from Dec 10
    ("2026-01-13", "13:30", "CPI"), ("2026-02-13", "13:30", "CPI"),
    ("2026-03-11", "12:30", "CPI"), ("2026-04-10", "12:30", "CPI"),
    ("2026-05-12", "12:30", "CPI"), ("2026-06-10", "12:30", "CPI"),
    ("2026-07-14", "12:30", "CPI"),
    # --- NFP ---
    ("2024-06-07", "12:30", "NFP"), ("2024-07-05", "12:30", "NFP"),
    ("2024-08-02", "12:30", "NFP"), ("2024-09-06", "12:30", "NFP"),
    ("2024-10-04", "12:30", "NFP"), ("2024-11-01", "12:30", "NFP"),
    ("2024-12-06", "13:30", "NFP"), ("2025-01-10", "13:30", "NFP"),
    ("2025-02-07", "13:30", "NFP"), ("2025-03-07", "13:30", "NFP"),
    ("2025-04-04", "12:30", "NFP"), ("2025-05-02", "12:30", "NFP"),
    ("2025-06-06", "12:30", "NFP"), ("2025-07-03", "12:30", "NFP"),
    ("2025-08-01", "12:30", "NFP"), ("2025-09-05", "12:30", "NFP"),
    ("2025-11-20", "13:30", "NFP"),  # Sep jobs, shutdown-delayed
    ("2025-12-16", "13:30", "NFP"),  # Oct/Nov combined (verify via spike)
    ("2026-01-09", "13:30", "NFP"), ("2026-02-06", "13:30", "NFP"),
    ("2026-03-06", "13:30", "NFP"), ("2026-04-03", "12:30", "NFP"),
    ("2026-05-01", "12:30", "NFP"), ("2026-06-05", "12:30", "NFP"),
    ("2026-07-02", "12:30", "NFP"),  # Jun report, holiday-shifted (verify)
    # --- FOMC ---
    ("2024-06-12", "18:00", "FOMC"), ("2024-07-31", "18:00", "FOMC"),
    ("2024-09-18", "18:00", "FOMC"), ("2024-11-07", "19:00", "FOMC"),
    ("2024-12-18", "19:00", "FOMC"), ("2025-01-29", "19:00", "FOMC"),
    ("2025-03-19", "18:00", "FOMC"), ("2025-05-07", "18:00", "FOMC"),
    ("2025-06-18", "18:00", "FOMC"), ("2025-07-30", "18:00", "FOMC"),
    ("2025-09-17", "18:00", "FOMC"), ("2025-10-29", "18:00", "FOMC"),
    ("2025-12-10", "19:00", "FOMC"), ("2026-01-28", "19:00", "FOMC"),
    ("2026-03-18", "18:00", "FOMC"), ("2026-04-29", "18:00", "FOMC"),
    ("2026-06-17", "18:00", "FOMC"),
]

PRE_MIN, POST_MIN = 120, 250  # window around release
BASELINE_SHIFT_DAYS = 7


def to_ms(date, hhmm):
    import datetime as dt
    h, m = map(int, hhmm.split(":"))
    y, mo, d = map(int, date.split("-"))
    return int(dt.datetime(y, mo, d, h, m, tzinfo=dt.timezone.utc).timestamp() * 1000)


EVENT_TS = sorted((to_ms(d, t), d, t, k) for d, t, k in EVENTS)


def fetch_window(symbol, t0_ms, t1_ms):
    """1m bars covering [t0, t1), ascending. Cached on disk."""
    key = f"{symbol}_{t0_ms}_{t1_ms}.json"
    path = os.path.join(CACHE, key)
    if os.path.exists(path):
        return json.load(open(path))
    bars, end = {}, t1_ms
    for _ in range(10):
        q = urllib.parse.urlencode({
            "symbol": symbol, "productType": "USDT-FUTURES",
            "granularity": "1m", "endTime": end, "limit": 200,
        })
        url = f"https://api.bitget.com/api/v2/mix/market/history-candles?{q}"
        for attempt in range(4):
            try:
                with urllib.request.urlopen(url, timeout=20) as r:
                    payload = json.load(r)
                break
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(1.5 * (attempt + 1))
        rows = payload.get("data") or []
        if not rows:
            break
        for row in rows:
            ts = int(row[0])
            if t0_ms <= ts < t1_ms:
                bars[ts] = [ts, float(row[1]), float(row[2]), float(row[3]), float(row[4])]
        earliest = min(int(r[0]) for r in rows)
        if earliest <= t0_ms:
            break
        end = earliest
        time.sleep(0.12)
    out = [bars[k] for k in sorted(bars)]
    json.dump(out, open(path, "w"))
    time.sleep(0.12)
    return out


def ret_bp(a, b):
    return (b / a - 1) * 1e4


def close_at(bars, ts_ms, tol_min=6):
    """Close of the last bar with open-time <= ts. None if gap > tol."""
    prior = [b for b in bars if b[0] <= ts_ms]
    if not prior or (ts_ms - prior[-1][0]) > tol_min * 60_000:
        return None
    return prior[-1][4]


def analyze(symbol, rel_ms, kind, date):
    t0, t1 = rel_ms - PRE_MIN * 60_000, rel_ms + POST_MIN * 60_000
    bars = fetch_window(symbol, t0, t1)
    if len(bars) < (PRE_MIN + POST_MIN) * 0.8:
        return {"ok": False, "why": f"gap ({len(bars)} bars)"}
    base = fetch_window(symbol, t0 - BASELINE_SHIFT_DAYS * 86_400_000,
                        t1 - BASELINE_SHIFT_DAYS * 86_400_000)

    pre = [b for b in bars if b[0] < rel_ms]
    anchor = pre[-1][4] if pre else None
    if not anchor:
        return {"ok": False, "why": "no anchor"}

    # Spike validation: release-minute-vicinity max 1m range vs pre-median.
    pre_rngs = [b[2] - b[3] for b in pre[-60:]]
    med_rng = st.median(pre_rngs) if pre_rngs else 0
    rel_bars = [b for b in bars if rel_ms <= b[0] < rel_ms + 3 * 60_000]
    spike = (max((b[2] - b[3]) for b in rel_bars) / med_rng) if (rel_bars and med_rng > 0) else 0

    c45 = close_at(bars, rel_ms + 45 * 60_000)
    c120 = close_at(bars, rel_ms + 120 * 60_000)
    c240 = close_at(bars, rel_ms + 240 * 60_000)
    if None in (c45, c120, c240):
        return {"ok": False, "why": "missing post bars"}
    r45, r120, r240 = (ret_bp(anchor, c) for c in (c45, c120, c240))

    pre_start = close_at(bars, rel_ms - PRE_MIN * 60_000 + 5 * 60_000)
    pre_drift = ret_bp(pre_start, anchor) if pre_start else None

    post60 = [b for b in bars if rel_ms <= b[0] < rel_ms + 60 * 60_000]
    rng_post60 = (max(b[2] for b in post60) - min(b[3] for b in post60)) / anchor * 1e4 if post60 else None

    # Baseline: same clock window T-7d — 1h range and |4h net move|.
    b_anchor = close_at(base, rel_ms - BASELINE_SHIFT_DAYS * 86_400_000)
    b_c240 = close_at(base, rel_ms - BASELINE_SHIFT_DAYS * 86_400_000 + 240 * 60_000)
    b_post60 = [b for b in base if rel_ms - BASELINE_SHIFT_DAYS * 86_400_000 <= b[0]
                < rel_ms - BASELINE_SHIFT_DAYS * 86_400_000 + 60 * 60_000]
    b_rng60 = (max(b[2] for b in b_post60) - min(b[3] for b in b_post60)) / b_anchor * 1e4 \
        if (b_post60 and b_anchor) else None
    b_r240 = ret_bp(b_anchor, b_c240) if (b_anchor and b_c240) else None

    return {
        "ok": True, "symbol": symbol, "kind": kind, "date": date,
        "spike": round(spike, 1), "pre_drift": pre_drift,
        "r45": r45, "r120": r120, "r240": r240,
        "rng60": rng_post60, "b_rng60": b_rng60, "b_r240": b_r240,
    }


def wilson_low(w, n):
    if n == 0:
        return 0.0
    z, p = 1.645, w / n
    den = 1 + z * z / n
    return (p + z * z / (2 * n) - z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)) / den


def summarize(rows, label):
    n = len(rows)
    if n < 5:
        print(f"  {label}: n={n} (too few)")
        return
    # Q1: pre-drift direction vs 4h outcome
    pd_rows = [r for r in rows if r["pre_drift"] is not None and abs(r["pre_drift"]) > 1 and abs(r["r240"]) > 1]
    pd_agree = sum(1 for r in pd_rows if (r["pre_drift"] > 0) == (r["r240"] > 0))
    # Q2: 45min direction persistence to 2h / 4h + continuation ret
    dir_rows = [r for r in rows if abs(r["r45"]) > 2]
    p120 = sum(1 for r in dir_rows if (r["r45"] > 0) == (r["r120"] > 0))
    p240 = sum(1 for r in dir_rows if (r["r45"] > 0) == (r["r240"] > 0))
    cont = [(r["r240"] - r["r45"]) * (1 if r["r45"] > 0 else -1) for r in dir_rows]
    # big-reaction subset (|r45| top half)
    if dir_rows:
        med45 = st.median(abs(r["r45"]) for r in dir_rows)
        big = [r for r in dir_rows if abs(r["r45"]) >= med45]
        bigp240 = sum(1 for r in big if (r["r45"] > 0) == (r["r240"] > 0))
        bigcont = [(r["r240"] - r["r45"]) * (1 if r["r45"] > 0 else -1) for r in big]
    else:
        big, bigp240, bigcont = [], 0, []
    # Q3: range expansion + net-vs-baseline
    rx = [r["rng60"] / r["b_rng60"] for r in rows if r["rng60"] and r["b_rng60"]]
    net_ev = [abs(r["r240"]) for r in rows]
    net_bl = [abs(r["b_r240"]) for r in rows if r["b_r240"] is not None]
    print(f"  {label}: n={n}")
    if pd_rows:
        print(f"    pre-drift agrees with 4h move: {pd_agree}/{len(pd_rows)} = {pd_agree/len(pd_rows):.0%} (coin flip expected)")
    if dir_rows:
        print(f"    45min dir holds at 2h: {p120}/{len(dir_rows)} = {p120/len(dir_rows):.0%} | at 4h: {p240}/{len(dir_rows)} = {p240/len(dir_rows):.0%} (Wilson90 low {wilson_low(p240,len(dir_rows)):.0%})")
        print(f"    continuation 45m->4h in 45m dir: mean {st.mean(cont):+.1f}bp, median {st.median(cont):+.1f}bp")
    if big:
        print(f"    BIG reactions (|r45|>={med45:.0f}bp, n={len(big)}): dir holds 4h {bigp240}/{len(big)} = {bigp240/len(big):.0%}, cont mean {st.mean(bigcont):+.1f}bp")
    if rx:
        print(f"    1h range vs T-7d baseline: median x{st.median(rx):.2f}")
    if net_ev and net_bl:
        print(f"    |4h net move| event {st.median(net_ev):.0f}bp vs baseline {st.median(net_bl):.0f}bp (median)")


def main():
    results, suspects = [], []
    for rel_ms, date, hhmm, kind in EVENT_TS:
        if rel_ms > time.time() * 1000 - 5 * 3600 * 1000:
            continue
        for sym in SYMBOLS:
            r = analyze(sym, rel_ms, kind, date)
            if not r.get("ok"):
                print(f"SKIP {sym} {kind} {date}: {r.get('why')}", file=sys.stderr)
                continue
            if r["spike"] < 2.0 and sym == "BTCUSDT":
                suspects.append((date, hhmm, kind, r["spike"]))
            results.append(r)
    json.dump(results, open(os.path.join(OUT_DIR, "event_study_results.json"), "w"), indent=1)

    print(f"\n=== Crypto post-event study: {len(results)} symbol-events ===")
    if suspects:
        print("\nSUSPECT timestamps (BTC release-minute spike < 2x pre-median):")
        for d, t, k, s in suspects:
            print(f"  {k} {d} {t}Z spike x{s}")
    for kind in ["CPI", "NFP", "FOMC"]:
        print(f"\n-- {kind} --")
        for sym in SYMBOLS:
            summarize([r for r in results if r["kind"] == kind and r["symbol"] == sym
                       and (r["date"], "x", kind, r["spike"]) is not None], sym)
    print("\n-- ALL EVENTS pooled (per symbol) --")
    for sym in SYMBOLS:
        summarize([r for r in results if r["symbol"] == sym], sym)
    print("\n-- ALL symbols x events pooled --")
    summarize(results, "ALL")


if __name__ == "__main__":
    main()
