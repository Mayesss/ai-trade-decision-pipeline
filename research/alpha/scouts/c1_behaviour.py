"""C1 scout: behaviour-only facts for crypto trend following on Binance USD-M perps.

FORBIDDEN (honoured by construction): no return, P&L or price change AFTER any signal,
breakout or state is computed; no funding split by signal state; no "in position" split;
no comparison of lookbacks/universes/variants by any outcome; no strategy equity curve.
Every input row dated >= 2026-07-01 is dropped at load (hard cut), before use.

What IS computed:
  * point-in-time universe per month (paper's rule on Binance perps) and its size;
  * signal state from PAST closes only (Donchian ensemble, paper's verbatim rule),
    used only for flip counts, target-weight turnover and gross exposure;
  * unconditional 90-day vol levels; unconditional funding levels of universe members;
  * unconditional return correlations of underlyings (top-20 vs ranks 21-100);
  * underlying (not strategy) moves of BTC/ETH/EW top-20 in pre-named stress episodes,
    and unconditional worst rolling 1/7/30-day underlying moves.

Usage:
  python -I c1_behaviour.py <klines_dir> <out_dir> [<funding_dir>] [<hl_first.json>]
"""
import csv
import datetime as dt
import io
import json
import math
import os
import statistics as st
import sys
import zipfile

CUT = dt.date(2026, 7, 1)
LOOKBACKS = (5, 10, 20, 30, 60, 90, 150, 250, 360)
VOL_TARGET, LEV_CAP, VOL_WIN = 0.25, 2.0, 90
STABLE = {"USDC", "BUSD", "TUSD", "FDUSD", "USDP", "DAI", "USDE", "PYUSD", "USD1", "RLUSD", "XUSD",
          "BFUSD", "USDS", "EUR"}  # USTC kept: depegged, trades as a crypto asset
# Gold-backed tokens and Binance TradFi perps (equity/commodity/FX underlyings, onboarded
# from 2025-12) are not crypto assets: excluded from the crypto universe. Filled in from
# the name list printed by --names (see the scout doc for the list as used).
NON_CRYPTO = {s + "USDT" for s in (
    "XAU XAG XPT XAUT PAXG CL BZ NATGAS COPPER TSLA MSTR NVDA AMD INTC QCOM MU MRVL SNDK ARM AAOI "
    "LITE NBIS RKLB COIN HOOD CRCL PLTR AMZN GOOGL NOK SAMSUNG SKHYNIX QQQ TQQQ SPY SOXL EWY KORU AXTI").split()}
EPISODES = [("2020-03-12 covid", "2020-03-12"), ("2021-05-19 China/leverage", "2021-05-19"),
            ("2022-05 LUNA", "2022-05-09"), ("2022-11 FTX", "2022-11-08"),
            ("2025-10-10 tariff/ADL", "2025-10-10")]


def d_of(ms):
    return dt.datetime.fromtimestamp(int(ms) / 1000 if int(ms) < 10**14 else int(ms) / 1e6,
                                     dt.UTC).date()


def load_klines(root):
    bars = {}
    kroot = os.path.join(root, "klines")
    for sym in sorted(os.listdir(kroot)):
        rows = {}
        sd = os.path.join(kroot, sym, "1d")
        for fn in sorted(os.listdir(sd)):
            if not fn.endswith(".zip"):
                continue
            with zipfile.ZipFile(os.path.join(sd, fn)) as z:
                for name in z.namelist():
                    for r in csv.reader(io.TextIOWrapper(z.open(name), "utf-8")):
                        if not r or not r[0].strip().isdigit():
                            continue
                        day = d_of(r[0])
                        if day >= CUT:
                            continue
                        rows[day] = (float(r[2]), float(r[3]), float(r[4]), float(r[7]), int(float(r[8])))
        if rows:
            bars[sym] = sorted(rows.items())
    return bars


def load_funding(root):
    fr = {}
    froot = os.path.join(root, "fundingRate")
    if not os.path.isdir(froot):
        return fr
    for sym in sorted(os.listdir(froot)):
        acc = {}
        for fn in sorted(os.listdir(os.path.join(froot, sym))):
            if not fn.endswith(".zip"):
                continue
            with zipfile.ZipFile(os.path.join(froot, sym, fn)) as z:
                for name in z.namelist():
                    for r in csv.reader(io.TextIOWrapper(z.open(name), "utf-8")):
                        if not r or not r[0].strip().isdigit():
                            continue
                        day = d_of(r[0])
                        if day >= CUT:
                            continue
                        acc[day] = acc.get(day, 0.0) + float(r[-1])
        fr[sym] = acc
    return fr


def month_key(d):
    return (d.year, d.month)


def next_month(ym):
    y, m = ym
    return (y + 1, 1) if m == 12 else (y, m + 1)


def base_of(sym):
    return sym[:-4]


def is_crypto(sym):
    b = base_of(sym)
    return b not in STABLE and sym not in NON_CRYPTO


# ---------------------------------------------------------------- universe
MEDS = {}


def build_universe(bars, B_list=(20, 50, 100), min_med=2e6, age_days=365, use_age=True):
    """universe[ym] = ranked list of symbols for month ym, decided from month ym-1 data."""
    first = {s: v[0][0] for s, v in bars.items()}
    by_month = {}
    for s, v in bars.items():
        if not is_crypto(s):
            continue
        for day, (_, _, _, qv, cnt) in v:
            by_month.setdefault(month_key(day), {}).setdefault(s, []).append(qv if cnt > 0 else 0.0)
    uni, elig_n = {}, {}
    for ym in sorted(by_month):
        y, m = ym
        mend = dt.date(*next_month(ym), 1) - dt.timedelta(days=1)
        cands = []
        for s, q in by_month[ym].items():
            if use_age and (mend - first[s]).days < age_days:
                continue
            med = st.median(q)
            if med >= min_med:
                cands.append((med, s))
        cands.sort(reverse=True)
        uni[next_month(ym)] = [s for _, s in cands]
        elig_n[next_month(ym)] = len(cands)
        MEDS[next_month(ym)] = [m_ for m_, _ in cands]
    return uni, elig_n


# ---------------------------------------------------------------- signals
def signals(closes, ls=False):
    """closes: list of floats. Returns per-lookback Pos lists (verbatim rule, past closes only)."""
    out = {}
    T = len(closes)
    UPS = {n: rolling(closes, n, max) for n in LOOKBACKS}
    DNS = {n: rolling(closes, n, min) for n in LOOKBACKS}
    for n in LOOKBACKS:
        pos = [0] * T
        ts = None
        p = 0
        for t in range(T):
            if t < n - 1:
                pos[t] = 0
                continue
            up, dn = UPS[n][t], DNS[n][t]
            mid = 0.5 * (up + dn)
            c = closes[t]
            newp = p
            if c >= up:
                newp = 1
            elif ls and c <= dn:
                newp = -1
            elif p == 1 and ts is not None and c <= ts:
                newp = 0
            elif p == -1 and ts is not None and c >= ts:
                newp = 0
            # trailing stop for use on day t+1
            if newp != p and newp != 0:
                ts = mid  # initial stop = midpoint at entry
            elif newp == 1:
                ts = max(ts, mid)
            elif newp == -1:
                ts = min(ts, mid)
            else:
                ts = None
            p = newp
            pos[t] = p
        out[n] = pos
    return out


def rolling(xs, n, fn):
    """rolling max/min over the last n values including t (monotonic deque)."""
    from collections import deque
    q, out = deque(), [None] * len(xs)
    better = (lambda a, b: a >= b) if fn is max else (lambda a, b: a <= b)
    for t, x in enumerate(xs):
        while q and better(x, xs[q[-1]]):
            q.pop()
        q.append(t)
        if q[0] <= t - n:
            q.popleft()
        out[t] = xs[q[0]]
    return out


def vol90(closes):
    T = len(closes)
    r = [0.0] + [closes[i] / closes[i - 1] - 1 for i in range(1, T)]
    out = [None] * T
    s1 = s2 = 0.0
    for t in range(1, T):
        s1 += r[t]
        s2 += r[t] * r[t]
        if t - VOL_WIN >= 1:
            s1 -= r[t - VOL_WIN]
            s2 -= r[t - VOL_WIN] ** 2
        if t >= VOL_WIN:
            n = VOL_WIN
            var = max((s2 - s1 * s1 / n) / (n - 1), 0.0)
            out[t] = math.sqrt(var) * math.sqrt(365)
    return out, r


def pct(xs, q):
    xs = sorted(xs)
    if not xs:
        return float("nan")
    k = (len(xs) - 1) * q
    f = math.floor(k)
    c = min(f + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def main():
    kdir, out = sys.argv[1], sys.argv[2]
    fdir = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] != "-" else None
    hlp = sys.argv[4] if len(sys.argv) > 4 else None
    os.makedirs(out, exist_ok=True)
    bars = load_klines(kdir)
    rep = []

    def P(*a):
        s = " ".join(str(x) for x in a)
        rep.append(s)
        print(s, flush=True)

    P(f"symbols loaded: {len(bars)}; last day {max(v[-1][0] for v in bars.values())}")

    if "--names" in sys.argv:
        # print new (>= 2025-11) high-volume symbols so non-crypto underlyings can be listed
        for s, v in bars.items():
            if v[0][0] >= dt.date(2025, 11, 1):
                q = st.median([x[1][3] for x in v])
                if q > 2e6:
                    P("NEWHV", s, v[0][0], round(q / 1e6, 1))
        return

    uni_noage, elig_noage = build_universe(bars, use_age=False)
    uni, elig = build_universe(bars)
    months = [ym for ym in sorted(uni) if (2021, 1) <= ym <= (2026, 6)]

    # HL listing map (first funding time, bounded < cut)
    hl_first = {}
    if hlp:
        raw = json.load(open(hlp))
        for c, t in raw.items():
            if t is not None and t < 1782864000000:
                hl_first[c] = d_of(t)

    def hl_name(sym):
        b = base_of(sym)
        cands = [b]
        for p in ("1000000", "1000", "1M"):
            if b.startswith(p) and len(b) > len(p):
                cands += ["k" + b[len(p):], b[len(p):]]
        for c in cands:
            if c in hl_first:
                return c
        return None

    # ---- universe table
    P("\n## Universe per month (paper rule on Binance USDT perps: perp age>=365d, median daily quote vol >= $2M in prior month, rank by that median)")
    P("month | eligible (age rule) | eligible (no age rule) | top-20 on HL at month start | rank-20 median $M/day | rank-100 median $M/day")
    uni_rows = []
    for ym in months:
        top = uni[ym][:20]
        mstart = dt.date(*ym, 1)
        on_hl = sum(1 for s in top if hl_name(s) and hl_first[hl_name(s)] <= mstart)
        uni_rows.append((ym, elig.get(ym, 0), elig_noage.get(ym, 0), on_hl, top))
        md = MEDS.get(ym, [])
        r20 = f"{md[19] / 1e6:.0f}" if len(md) >= 20 else "-"
        r100 = f"{md[99] / 1e6:.0f}" if len(md) >= 100 else "-"
        P(f"{ym[0]}-{ym[1]:02d} | {elig.get(ym, 0)} | {elig_noage.get(ym, 0)} | {on_hl}/{len(top)} | {r20} | {r100}")
    with open(os.path.join(out, "universe_top100.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["month", "rank", "symbol"])
        for ym in months:
            for i, s in enumerate(uni[ym][:100]):
                w.writerow([f"{ym[0]}-{ym[1]:02d}", i + 1, s])
    union20 = sorted({s for ym in months for s in uni[ym][:20]})
    union100 = sorted({s for ym in months for s in uni[ym][:100]})
    P(f"distinct symbols ever top-20: {len(union20)}; ever top-100: {len(union100)}")
    with open(os.path.join(out, "union_top100_symbols.txt"), "w") as fh:
        fh.write("\n".join(union100) + "\n")
    first_top20 = {}
    for ym in months:
        for s in uni[ym][:20]:
            first_top20.setdefault(s, ym)
    with open(os.path.join(out, "first_top20_month.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        for s, ym in sorted(first_top20.items()):
            w.writerow([s, f"{ym[0]}-{ym[1]:02d}"])

    # membership by day
    member = {}  # (sym) -> set of months where in top-B
    for B in (20,):
        for ym in months:
            for s in uni[ym][:B]:
                member.setdefault(s, set()).add(ym)
    turnover_note = []

    # ---- signals, flips, target weights, gross (top-20, B=20)
    P("\n## Signal behaviour on top-20 members (past closes only)")
    B = 20
    flips_by_n = {n: 0 for n in LOOKBACKS}
    coin_months = 0
    on_frac = {n: [] for n in LOOKBACKS}
    gross_daily = {}
    gross_daily_ls = {}
    net_daily_ls = {}
    turn = {"abs20": {}, "rel20": {}, "none": {}}
    turn_ls = {"abs20": {}}
    sig_days = 0
    vol_by_year = {}
    w_combo_samples = []
    for s in sorted(member):
        v = bars[s]
        days = [d for d, _ in v]
        closes = [x[2] for _, x in v]
        pos = signals(closes)
        pos_ls = signals(closes, ls=True)
        vol, _ = vol90(closes)
        idx = {d: i for i, d in enumerate(days)}
        held = {k: 0.0 for k in turn}
        held_ls = 0.0
        prev_member = False
        for i, d in enumerate(days):
            ym = month_key(d)
            inm = ym in member[s]
            if vol[i] is None:
                continue
            wn = min(VOL_TARGET / vol[i], LEV_CAP) if vol[i] > 0 else LEV_CAP
            tgt = sum(wn * pos[n][i] for n in LOOKBACKS) / len(LOOKBACKS)
            tgt_ls = sum(wn * pos_ls[n][i] for n in LOOKBACKS) / len(LOOKBACKS)
            sigchg = any(pos[n][i] != pos[n][i - 1] for n in LOOKBACKS) if i > 0 else True
            sigchg_ls = any(pos_ls[n][i] != pos_ls[n][i - 1] for n in LOOKBACKS) if i > 0 else True
            if inm:
                y = d.year
                vol_by_year.setdefault(y, []).append(vol[i])
                w_combo_samples.append(tgt)
                for n in LOOKBACKS:
                    on_frac[n].append(pos[n][i])
                    if i > 0 and pos[n][i] != pos[n][i - 1]:
                        flips_by_n[n] += 1
                if d.day == 1:
                    coin_months += 1
            for k in turn:
                target = tgt if inm else 0.0
                if not inm or not prev_member or sigchg:
                    new = target
                elif k == "abs20":
                    new = target if abs(held[k] - target) > 0.20 else held[k]
                elif k == "rel20":
                    new = target if abs(held[k] - target) > 0.20 * max(target, 1e-12) else held[k]
                else:
                    new = target
                dlt = abs(new - held[k]) / B
                if dlt:
                    turn[k][d] = turn[k].get(d, 0.0) + dlt
                held[k] = new
            target = tgt_ls if inm else 0.0
            if not inm or not prev_member or sigchg_ls:
                new = target
            else:
                new = target if abs(held_ls - target) > 0.20 else held_ls
            dlt = abs(new - held_ls) / B
            if dlt:
                turn_ls["abs20"][d] = turn_ls["abs20"].get(d, 0.0) + dlt
            held_ls = new
            if inm:
                gross_daily[d] = gross_daily.get(d, 0.0) + held["abs20"] / B
                gross_daily_ls[d] = gross_daily_ls.get(d, 0.0) + abs(held_ls) / B
                net_daily_ls[d] = net_daily_ls.get(d, 0.0) + held_ls / B
            prev_member = inm
    yrs = (dt.date(2026, 7, 1) - dt.date(2021, 1, 1)).days / 365.25
    P(f"coin-months in top-20 (2021-01 -> 2026-06): ~{coin_months}")
    P("lookback | flips (entries+exits) per coin-month | fraction of member-days long")
    tot = 0
    for n in LOOKBACKS:
        tot += flips_by_n[n]
        P(f"{n} | {flips_by_n[n] / max(coin_months, 1):.2f} | {sum(on_frac[n]) / max(len(on_frac[n]), 1):.2f}")
    P(f"all 9 lookbacks | {tot / max(coin_months, 1):.2f} sub-signal flips per coin-month")
    for k in turn:
        ann = sum(v for d, v in turn[k].items() if d >= dt.date(2021, 1, 1)) / yrs
        P(f"turnover long-only, rebalance rule {k}: {ann:.2f}x capital per year (one-way, at the paper's per-sleeve sizing)")
        for fee in (4.5, 6.0):
            P(f"   fee drag at {fee} bp/fill: {ann * fee / 100:.2f}% of capital/yr at paper sizing")
    ann_ls = sum(turn_ls["abs20"].values()) / yrs
    P(f"turnover long/short abs20: {ann_ls:.2f}x/yr; fee drag 4.5/6 bp: {ann_ls * 0.045:.2f}% / {ann_ls * 0.06:.2f}%")
    g = [v for d, v in gross_daily.items()]
    gl = list(gross_daily_ls.values())
    nl = list(net_daily_ls.values())
    P(f"gross long-only (paper sizing): mean {st.mean(g):.2f}, p50 {pct(g, .5):.2f}, p90 {pct(g, .9):.2f}, p99 {pct(g, .99):.2f}, max {max(g):.2f}")
    P(f"gross L/S: mean {st.mean(gl):.2f}, p90 {pct(gl, .9):.2f}, max {max(gl):.2f}; net L/S mean {st.mean(nl):.2f}, p10 {pct(nl, .1):.2f}, p90 {pct(nl, .9):.2f}")
    P(f"combo target weight per member-day: mean {st.mean(w_combo_samples):.3f}, p90 {pct(w_combo_samples, .9):.3f}, max {max(w_combo_samples):.3f}")
    P("\n## Unconditional 90-day vol of top-20 members (annualised), by year; implied full-on sleeve weight 0.25/sigma")
    for y in sorted(vol_by_year):
        vv = vol_by_year[y]
        P(f"{y}: median sigma90 {pct(vv, .5):.2f}, p10 {pct(vv, .1):.2f}, p90 {pct(vv, .9):.2f}; full-on sleeve weight at median {min(0.25 / pct(vv, .5), 2):.2f}")
    with open(os.path.join(out, "gross_daily.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "gross_long_only", "gross_ls", "net_ls"])
        for d in sorted(gross_daily):
            w.writerow([d, round(gross_daily[d], 4), round(gross_daily_ls.get(d, 0), 4), round(net_daily_ls.get(d, 0), 4)])

    # ---- paper's mid-month exit rule: triggers among top-20 members
    trig = 0
    tot_md = 0
    for s in member:
        v = bars[s]
        for i in range(30, len(v)):
            d = v[i][0]
            if month_key(d) not in member[s]:
                continue
            tot_md += 1
            qv = [x[1][3] for x in v[i - 29:i + 1]]
            ch = [abs(v[j][1][2] / v[j - 1][1][2] - 1) for j in range(i - 29, i + 1)]
            if st.median(qv) < 1e6 or st.median(ch) < 0.005:
                trig += 1
    P(f"\npaper exit rule (30d median vol < $1M or median |daily change| < 0.5%) triggers on {trig} of {tot_md} top-20 member-days")

    # ---- unconditional funding level of the universe (equal weight over members, all days)
    if fdir:
        fr = load_funding(fdir)
        P("\n## Unconditional funding level, equal-weight over top-20 members (NOT by signal state), annualised")
        P("year | mean funding %/yr (EW members) | BTC %/yr | share of member-days with positive daily funding")
        by_y = {}
        btc = {}
        for s in member:
            f = fr.get(s, {})
            for d, x in f.items():
                if month_key(d) in member[s]:
                    by_y.setdefault(d.year, []).append(x)
        for d, x in fr.get("BTCUSDT", {}).items():
            if d >= dt.date(2021, 1, 1):
                btc.setdefault(d.year, []).append(x)
        for y in sorted(by_y):
            xs = by_y[y]
            P(f"{y} | {st.mean(xs) * 365 * 100:.1f} | {st.mean(btc.get(y, [0])) * 365 * 100:.1f} | {sum(1 for x in xs if x > 0) / len(xs):.2f}")
        allx = [x for xs in by_y.values() for x in xs]
        P(f"2021-01 -> 2026-06 | {st.mean(allx) * 365 * 100:.1f} | {st.mean([x for xs in btc.values() for x in xs]) * 365 * 100:.1f} | {sum(1 for x in allx if x > 0) / len(allx):.2f}")
        y25 = [x for (y, xs) in by_y.items() if y >= 2025 for x in xs]
        if y25:
            P(f"2025-01 -> 2026-06 | {st.mean(y25) * 365 * 100:.1f}")

    # ---- unconditional correlations: top-20 vs ranks 21-100
    P("\n## Unconditional daily-return correlations of underlyings (membership by month)")
    rets = {}
    noage20 = {s for ym in uni_noage if (2020, 1) <= ym <= (2026, 6) for s in uni_noage[ym][:20]}
    for s in set(union100) | noage20:
        v = bars[s]
        rets[s] = {v[i][0]: v[i][1][2] / v[i - 1][1][2] - 1 for i in range(1, len(v))}
    P("year | avg pairwise corr within top-20 | avg corr top-20 x ranks 21-100 | avg pairwise within 21-100 | corr(EW top-20, EW 21-100) | corr(EW 21-100, BTC)")
    for y in range(2021, 2027):
        mon = [ym for ym in months if ym[0] == y]
        if not mon:
            continue
        cnt20, cnt2 = {}, {}
        for ym in mon:
            for s in uni[ym][:20]:
                cnt20[s] = cnt20.get(s, 0) + 1
            for s in uni[ym][20:100]:
                cnt2[s] = cnt2.get(s, 0) + 1
        need = max(1, len(mon) // 2)
        A = [s for s, c in cnt20.items() if c >= need]
        Bm = [s for s, c in cnt2.items() if c >= need and s not in A]
        days_y = [d for d in sorted(rets["BTCUSDT"]) if d.year == y]

        def corr(a, b):
            ds = [d for d in days_y if d in rets[a] and d in rets[b]]
            if len(ds) < 60:
                return None
            x = [rets[a][d] for d in ds]
            z = [rets[b][d] for d in ds]
            try:
                return st.correlation(x, z)
            except st.StatisticsError:
                return None

        def avg_pairs(L1, L2, same):
            cs = []
            for i, a in enumerate(L1):
                for j, b in enumerate(L2):
                    if same and j <= i:
                        continue
                    c = corr(a, b)
                    if c is not None:
                        cs.append(c)
            return st.mean(cs) if cs else float("nan")

        Bs = Bm[:40]  # cap pairs for runtime
        ew20, ew2 = [], []
        btcs = []
        for d in days_y:
            ym = month_key(d)
            a = [rets[s][d] for s in uni[ym][:20] if d in rets.get(s, {})]
            b = [rets[s][d] for s in uni[ym][20:100] if d in rets.get(s, {})]
            if a and b:
                ew20.append(st.mean(a))
                ew2.append(st.mean(b))
                btcs.append(rets["BTCUSDT"][d])
        P(f"{y} | {avg_pairs(A, A, True):.2f} | {avg_pairs(A, Bs, False):.2f} | {avg_pairs(Bs, Bs, True):.2f} | "
          f"{st.correlation(ew20, ew2):.2f} | {st.correlation(ew2, btcs):.2f}  (n20={len(A)}, n21-100 sampled={len(Bs)})")

    # ---- stress episodes: underlying moves only
    P("\n## Underlying moves in pre-named stress episodes (prices only; no strategy P&L)")
    P("episode | asset | day-of close-to-close | worst 1d in [-3,+3] | worst 7d close-to-close in [-10,+10] | worst 30d c-c in [-30,+30] | day-of low vs prior close")

    def ew_series():
        out_ = {}
        for d in sorted(rets["BTCUSDT"]):
            ym = month_key(d)
            if ym not in uni_noage:
                continue
            a = [rets.get(s, {})[d] for s in uni_noage[ym][:20] if d in rets.get(s, {})]
            if a:
                out_[d] = st.mean(a)
        return out_

    ew = ew_series()
    lvl = {}
    for name, ser in (("BTC", rets["BTCUSDT"]), ("ETH", rets["ETHUSDT"]), ("EW top-20 (no age rule)", ew)):
        ds = sorted(ser)
        idx = {}
        L, x = [], 1.0
        for d in ds:
            x *= 1 + ser[d]
            L.append(x)
        lvl[name] = (ds, L, ser)
    lows = {"BTC": dict((d, x) for d, x in bars["BTCUSDT"]), "ETH": dict((d, x) for d, x in bars["ETHUSDT"])}
    for ename, ed in EPISODES:
        e = dt.date.fromisoformat(ed)
        for name, (ds, L, ser) in lvl.items():
            pos_ = {d: i for i, d in enumerate(ds)}
            if e not in pos_:
                P(f"{ename} | {name} | n/a")
                continue
            i0 = pos_[e]

            def worst(h, lo, hi):
                best = 0.0
                for i in range(max(h, i0 + lo), min(len(L), i0 + hi + 1)):
                    best = min(best, L[i] / L[i - h] - 1)
                return best

            day = ser[e]
            w1 = min(ser[ds[i]] for i in range(i0 - 3, i0 + 4))
            low = ""
            if name in lows:
                prev = lows[name][ds[i0 - 1]][2]
                low = f"{lows[name][e][1] / prev - 1:+.1%}"
            P(f"{ename} | {name} | {day:+.1%} | {w1:+.1%} | {worst(7, -10, 10):+.1%} | {worst(30, -30, 30):+.1%} | {low}")
    P("\nUnconditional worst rolling windows (close-to-close), full available span to 2026-06-30")
    for name, (ds, L, ser) in lvl.items():
        res = []
        for h in (1, 2, 7, 30):
            wv = min(L[i] / L[i - h] - 1 for i in range(h, len(L)))
            p1 = pct([L[i] / L[i - h] - 1 for i in range(h, len(L))], 0.01)
            res.append(f"{h}d worst {wv:+.1%} (p1 {p1:+.1%})")
        P(f"{name} from {ds[0]}: " + "; ".join(res))

    with open(os.path.join(out, "behaviour_report.txt"), "w") as fh:
        fh.write("\n".join(rep) + "\n")


if __name__ == "__main__":
    main()
