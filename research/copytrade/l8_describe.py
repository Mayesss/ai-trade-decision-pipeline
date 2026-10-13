"""Registration 006 §3 — behaviour-only numbers for H3 (cross-venue gap at second granularity).

Allowed here: event counts, timings, CONTEMPORANEOUS prices (Binance's last trade at or before a
forced print, against that print), prices strictly BEFORE an event, and unconditional prices in
NON-EVENT hours. Never a price after a trigger or a forced print for a real event: no gap at
+x s, no exit, no markout, no half-life. Times of the next print after t + L are read (never
its price) to count missing entry prints.

    .venv/bin/python -I l8_describe.py [clock] [clockdays] [hlclock] [events] [power]   (default: all)

Sections:
  clock    — offset between HL archive timestamps and Binance transact_time, from calm hours
             (no HL liquidation >= $100 k and no cascade of the coin within +-30 min): lead-lag
             cross-correlation of 100-ms log returns, per coin and month;
  hlclock  — HL timestamp semantics: shared block times, block cadence;
  events   — events through the filters, contemporaneous gap g(t) at every forced print by coin
             and side, Binance intensity / bounce / depth and HL bounce before each forced print,
             slippage, G_MIN, triggers, clusters, halves, sides, time to the next print;
  power    — SD of the 60-s change of the cross-venue gap in calm hours, null SE, MDE and the
             joint power of the seven criteria at the trigger times.

Log: data/scratch/l8.log (one block per section, appended); numbers also in data/scratch/l8_*.json.
"""
import bisect
import datetime as dt
import json
import math
import random
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:                     # allows `python -I l8_describe.py`
    sys.path.insert(0, str(HERE))

from ct import bn, drift, tape, xvenue as xv                               # noqa: E402
from ct.leaders import con                                                  # noqa: E402
from ct.net import DATA                                                     # noqa: E402
from l8_run import (BOOK, COINS, DEX, LIQ, OFFSET_MS, VARIANTS, build, hl_quotes, hl_window,  # noqa: E402
                    load_events)

LOG = DATA / 'scratch/l8.log'
H, D, S, M = xv.HOUR, xv.DAY, xv.SECOND, xv.MINUTE
CUT = xv.HOLDOUT_START
DISC_START = 1_753_660_800_000            # 2025-07-28 00:00 UTC
_out = []


_cache = {}


def built():
    if 'b' not in _cache:
        _cache['b'] = build()
    return _cache['b']


def p(*a):
    s = ' '.join(str(x) for x in a)
    print(s, flush=True)
    _out.append(s)


def iso(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).strftime('%Y-%m-%d %H:%M:%S')


def bp(x):
    return None if x is None else round(x * 1e4, 2)


def qs(xs, q=(0.1, 0.25, 0.5, 0.75, 0.9, 0.99)):
    return {k: bp(v) for k, v in xv.quantiles(xs, q).items()}


# ============================================================================ clock alignment

CALM_PAD = 30 * M
BIG_LIQ = 100_000.0
STEP = 100                      # return interval (ms)
COARSE = range(-3000, 3001, 50)
FINE_HALF = 150
HOURS_PER_MONTH = 4


def calm_hours(coin, events):
    """Whole UTC hours on days with both a Binance and an HL file, at least 30 min away from any HL
    liquidation fill >= $100 k in the coin and from any 002 cascade of the coin (all grid sets)."""
    sym = bn.SYMBOL[coin]
    bad = []
    for (t,) in con().execute(f"SELECT t FROM read_parquet('{LIQ}') WHERE coin = ? AND notional >= {BIG_LIQ}",
                              [coin]).fetchall():
        bad.append((t - CALM_PAD, t + CALM_PAD))
    for e in events:
        if e['coin'] == coin:
            bad.append((e['t_start'] - CALM_PAD, e['t_end'] + CALM_PAD))
    bad.sort()
    starts = [a for a, _ in bad]
    ends_max = []
    m = -1
    for _, b in bad:
        m = max(m, b)
        ends_max.append(m)
    root = bn.ROOT / 'aggTrades_parquet' / sym
    out = []
    for f in sorted(root.iterdir()):
        day = f.name[5:15]
        if day >= '2026-07-01' or not tape._files(DEX, _ms(day), _ms(day)):
            continue
        d0 = _ms(day)
        for hh in range(24):
            a, b = d0 + hh * H, d0 + (hh + 1) * H
            j = bisect.bisect_left(starts, b) - 1            # intervals starting before b
            if j >= 0 and ends_max[j] > a:
                continue
            out.append(a)
    return out


def _ms(day):
    return int(dt.datetime.fromisoformat(day).replace(tzinfo=dt.UTC).timestamp() * 1000)


def hl_prints(coin, t0, t1):
    """[(t, price)] of every HL book print (taker row, no backstop / ADL) of the coin in [t0, t1)."""
    return [(t, px) for t, px, _ in hl_window(coin, t0, t1 - 1)]


def grid_log(prints, t0, step, n):
    """log of the last price at or before t0 + j*step, j = 0..n-1 (None before the first print)."""
    out = [None] * n
    k = 0
    last = None
    for j in range(n):
        tj = t0 + j * step
        while k < len(prints) and prints[k][0] <= tj:
            last = prints[k][1]
            k += 1
        out[j] = math.log(last) if last else None
    return out


def xcorr_hour(coin, h0, lags):
    """{lag: (sum xy, sum xx, sum yy, n)} over the hour's 100-ms intervals: x = HL log return over
    [h0 + k*100, h0 + (k+1)*100], y = Binance log return over the same interval shifted EARLIER by
    lag ms (lag > 0: Binance as timestamped leads HL)."""
    pad = 4000
    hp = hl_prints(coin, h0 - pad, h0 + H + pad)
    bt = [(t, px) for t, px, _ in bn.window_trades(bn.SYMBOL[coin], h0 - pad, h0 + H + pad)]
    if len(hp) < 50 or len(bt) < 50:
        return None
    n = H // STEP + 1
    hl = grid_log(hp, h0, STEP, n)
    fine0 = h0 - pad
    fine = grid_log(bt, fine0, 10, (H + 2 * pad) // 10 + 1)
    x = [(k, hl[k + 1] - hl[k]) for k in range(n - 1) if hl[k] is not None and hl[k + 1] is not None]
    xx = sum(v * v for _, v in x)
    nz = [(k, v) for k, v in x if v != 0.0]
    out = {}
    for lag in lags:
        def y(k):
            a = (h0 + k * STEP - lag - fine0) // 10
            b = (h0 + (k + 1) * STEP - lag - fine0) // 10
            fa, fb = fine[a], fine[b]
            return None if fa is None or fb is None else fb - fa
        sy = syy = 0.0
        cnt = 0
        for k, _ in x:
            v = y(k)
            if v is None:
                continue
            syy += v * v
            sy += v
            cnt += 1
        sxy = 0.0
        for k, v in nz:
            w = y(k)
            if w is not None:
                sxy += v * w
        out[lag] = (sxy, xx, syy, cnt)
    return out


def corr(acc):
    sxy, sxx, syy, _ = acc
    return sxy / math.sqrt(sxx * syy) if sxx > 0 and syy > 0 else None


def peak(accs, lags):
    """Lag of the maximum pooled correlation; (lag, corr)."""
    best = None
    for lag in lags:
        tot = [0.0, 0.0, 0.0, 0]
        for a in accs:
            if a and lag in a:
                for j in range(4):
                    tot[j] += a[lag][j]
        c = corr(tot)
        if c is not None and (best is None or c > best[1]):
            best = (lag, c)
    return best


def section_clock(events):
    p('\n## 1. CLOCK ALIGNMENT — HL archive timestamp vs Binance transact_time, calm hours only')
    p(f'calm = whole UTC hour, >= 30 min from any HL liquidation fill >= $100 k in the coin and from any 002 cascade '
      f'of the coin; {HOURS_PER_MONTH} random calm hours per coin per month (seeded); x = HL 100-ms log return, '
      f'y = Binance 100-ms log return shifted by lag (lag > 0: Binance, as timestamped, LEADS HL); coarse lags '
      f'{COARSE.start}..{COARSE.stop - 1} ms by 50, then +-{FINE_HALF} ms by 10 around each coin\'s pooled peak.')
    res = {}
    for coin in COINS:
        t0 = time.time()
        hours = calm_hours(coin, events)
        by_month = {}
        for h0 in hours:
            by_month.setdefault(iso(h0)[:7], []).append(h0)
        rng = random.Random(f'clock-{coin}')
        pick = []
        for mth in sorted(by_month):
            hs = by_month[mth]
            pick += sorted(rng.sample(hs, min(HOURS_PER_MONTH, len(hs))))
        accs = {}
        for h0 in pick:
            a = xcorr_hour(coin, h0, COARSE)
            if a:
                accs[h0] = a
        pk = peak(accs.values(), COARSE)
        fine = range(pk[0] - FINE_HALF, pk[0] + FINE_HALF + 1, 10)
        for h0 in list(accs):
            accs[h0].update(xcorr_hour(coin, h0, fine) or {})
        lags = sorted(set(COARSE) | set(fine))
        pk = peak(accs.values(), lags)
        # the correlation function around the peak, pooled
        shape = {lag: round(peak([{lag: a[lag]} for a in accs.values() if lag in a], [lag])[1], 4)
                 for lag in (-1000, -500, -200, -100, 0, 100, 200, 300, 400, 500, 600, 700, 800, 1000, 1500, 2000) if all(lag in a for a in accs.values())}
        per_month = {}
        for mth in sorted(by_month):
            ha = [a for h0, a in accs.items() if iso(h0)[:7] == mth]
            if ha:
                q = peak(ha, lags)
                per_month[mth] = (q[0], round(q[1], 3), len(ha))
        per_hour = [peak([a], lags)[0] for a in accs.values()]
        # bootstrap over hours for the pooled peak
        bs = []
        rb = random.Random(1)
        al = list(accs.values())
        for _ in range(200):
            bs.append(peak([al[rb.randrange(len(al))] for _ in al], lags)[0])
        res[coin] = {'calm_hours': len(hours), 'hours_used': len(accs), 'peak_lag_ms': pk[0], 'peak_corr': round(pk[1], 4),
                     'corr_at': shape, 'per_month': per_month,
                     'per_hour_peak_q': {k: v for k, v in xv.quantiles(per_hour, (0.1, 0.25, 0.5, 0.75, 0.9)).items()},
                     'bootstrap_peak_q': {k: v for k, v in xv.quantiles(bs, (0.025, 0.5, 0.975)).items()}}
        p(f'{coin}: {len(hours)} calm hours available, {len(accs)} used; POOLED PEAK lag {pk[0]} ms (corr {pk[1]:.3f}); '
          f'bootstrap 95% {res[coin]["bootstrap_peak_q"]}; per-hour peak quantiles {res[coin]["per_hour_peak_q"]}; '
          f'corr at lags {shape}  [{time.time() - t0:.0f} s]')
        p(f'   per month (peak lag ms, corr, hours): {per_month}')
    (DATA / 'scratch/l8_clock.json').write_text(json.dumps(res, indent=1))
    return res


def section_clockdays(events, coins=('BTC', 'ETH'), first='2025-10-15', last='2025-11-20'):
    p(f'\n## 1b. CLOCK — day-by-day peak lag around the November 2025 step ({first} -> {last}, 3 calm hours a day)')
    lags = range(300, 1301, 50)
    for coin in coins:
        byday = {}
        for h0 in calm_hours(coin, events):
            byday.setdefault(iso(h0)[:10], []).append(h0)
        out = []
        for day in sorted(d for d in byday if first <= d <= last):
            rng = random.Random(day)
            accs = [a for a in (xcorr_hour(coin, h0, lags) for h0 in rng.sample(byday[day], min(3, len(byday[day])))) if a]
            if accs:
                out.append(f'{day[5:]} {peak(accs, lags)[0]}')
        p(f'{coin}: ' + ', '.join(out))


# ============================================================================ HL timestamp semantics

def section_hlclock():
    p('\n## 6. HL timestamp semantics — block times (all coins, every fill row)')
    root = DATA / 'pruned/by_dex/hyperliquid/fills/perp/all'
    days = sorted(d.name[5:] for d in root.iterdir() if d.name.startswith('date=') and d.name[5:] < '2026-07-01')
    pick = [days[int(i * (len(days) - 1) / 5)] for i in range(6)]
    c = con()
    for day in pick:
        f = root / f'date={day}' / 'fills.parquet'
        n, nt, multi = c.execute(f"""SELECT COUNT(*), COUNT(DISTINCT timestamp),
                                        (SELECT COUNT(*) FROM (SELECT timestamp FROM read_parquet('{f}') GROUP BY 1 HAVING COUNT(DISTINCT coin) > 1))
                                 FROM read_parquet('{f}')""").fetchone()
        gaps = c.execute(f"""WITH d AS (SELECT DISTINCT epoch_ms(timestamp) t FROM read_parquet('{f}')),
                                  g AS (SELECT t - lag(t) OVER (ORDER BY t) dt FROM d)
                             SELECT quantile_cont(dt, [0.1, 0.25, 0.5, 0.75, 0.9, 0.99]), COUNT(*) FILTER (WHERE dt < 20)
                             FROM g WHERE dt IS NOT NULL""").fetchone()
        top = c.execute(f"""WITH d AS (SELECT DISTINCT epoch_ms(timestamp) t FROM read_parquet('{f}')),
                                 g AS (SELECT t - lag(t) OVER (ORDER BY t) dt FROM d)
                            SELECT dt, COUNT(*) n FROM g WHERE dt IS NOT NULL GROUP BY dt ORDER BY n DESC LIMIT 6""").fetchall()
        p(f'{day}: {n:,} fill rows on {nt:,} distinct ms timestamps; {multi:,} timestamps carry fills of >1 coin; '
          f'gap between consecutive timestamps (ms) q10/25/50/75/90/99 {[round(x) for x in gaps[0]]}; '
          f'gaps < 20 ms: {gaps[1]:,}; most common gaps {top}')
    # one coin, inside cascades: same-ms forced prints
    r = c.execute(f"""SELECT COUNT(*), COUNT(DISTINCT (coin, t)) FROM read_parquet('{LIQ}') WHERE t < {CUT}""").fetchone()
    p(f'liquidations.parquet: {r[0]:,} forced fills on {r[1]:,} distinct (coin, ms) times — '
      f'{r[0] / r[1]:.1f} forced fills per coin-timestamp: a cascade prints in blocks')


# ============================================================================ events

def gtab(events, ids, scans, label):
    """Per coin and side: distribution of the scan gap (sweep or exec) at every scan point, the gap at
    the first point, per-trigger max; returns {(coin, side): [per-trigger max]}."""
    p(f'-- {label}')
    out = {}
    for c in COINS:
        for s_ in ('L', 'S'):
            ii = [i for i in ids if events[i]['coin'] == c and events[i]['side'] == s_]
            gs = [xv.signed_gap(s_, pb, ph) for i in ii for _, ph, pb in scans.get(i, [])]
            have = [g for g in gs if g is not None]
            mx = [max((g for g in (xv.signed_gap(s_, pb, ph) for _, ph, pb in scans[i]) if g is not None), default=None)
                  for i in ii if scans.get(i)]
            out[(c, s_)] = mx
            p(f'{c} {s_}: triggers {len(ii)}, scan points {len(gs):,} (with both quotes {len(have):,}); g q {qs(have)}; '
              f'per-trigger MAX g q {qs(mx)}; reaching G_MIN {bp(xv.g_min(c))} bp: '
              f'{sum(1 for x in mx if x is not None and x >= xv.g_min(c))}/{len(mx)}')
    return out


def section_events(events_unused=None):
    p('\n## 2-4. EVENTS — causal trigger (primary), executable-quote signal; filters, gaps, slippage, G_MIN, signals')
    p(f'OFFSET_MS = {OFFSET_MS}; quote window {xv.QUOTE_WINDOW} ms; side-aware fills {xv.SIDE_AWARE}')
    t0 = time.time()
    sets, plans, (rate, own) = built()
    d = sets['causal']
    events, bn_ok = d['events'], d['ok']
    sweep, ex = d['scans'][('book', 'sweep')], d['scans'][('book', 'exec')]
    prim = [i for i, e in enumerate(events) if not e['fallback_median']]
    p(f'causal triggers {len(events)} ({len(events) - len(prim)} on the fallback median); primary {len(prim)}: '
      + ', '.join(f'{c} {sum(1 for i in prim if events[i]["coin"] == c)}' for c in COINS)
      + f'; Binance coverage {sum(1 for i in prim if bn_ok.get(i))}/{len(prim)} [{time.time() - t0:.0f} s]')
    npts = sum(len(ex.get(i, [])) for i in prim)
    nq = sum(1 for i in prim for _, h, b in ex.get(i, []) if h and b)
    p(f'scan points (distinct forced book print times) {npts:,}; with both executable quotes within 1 s {nq:,} '
      f'({nq / max(npts, 1):.1%}); HL quote missing {sum(1 for i in prim for _, h, b in ex.get(i, []) if not h):,}, '
      f'Binance quote missing {sum(1 for i in prim for _, h, b in ex.get(i, []) if not b):,}')

    p('\n### 3.4 slippage inputs — 10 s before each primary CAUSAL TRIGGER print (independent of G_MIN; nothing at or after t)')
    pre = pre_stats(events, [(i, events[i]['scan_from']) for i in prim])
    sweep37, _ = xv.plan(events, sweep, rate, own, bn_ok=bn_ok,
                         thresholds={c: xv.g_min(c, slip_hl=xv.SLIP_FLOOR, slip_bn=xv.SLIP_FLOOR) for c in COINS})
    pre_sig = pre_stats(events, [(x['i'], x['t_sig']) for x in sweep37])
    p('rule (D6): per coin and venue, max(2 bp, p75 half-bounce in the 10 s before (a) every primary trigger print, '
      '(b) every SWEEP-signal print at the 2-bp G_MIN of 37 bp (fixed, so the rule is not circular) — the deepest points '
      'of the runs, where fills meet the thinnest book; '
      'the larger of the two); (b) is used, not the executable-signal prints, because those are too few and the '
      'slippage must not depend on the signal it prices')
    slip = {}
    for c in COINS:
        r = [v for (cc, _), v in pre.items() if cc == c]
        r2 = [v for (cc, _), v in pre_sig.items() if cc == c]
        hb = xv.quantiles([x['hl_bounce'] for x in r], (0.5, 0.75))
        bb = xv.quantiles([x['bn_bounce'] for x in r], (0.5, 0.75))
        hb2 = xv.quantiles([x['hl_bounce'] for x in r2], (0.75,))
        bb2 = xv.quantiles([x['bn_bounce'] for x in r2], (0.75,))
        slip[c] = (max(xv.SLIP_FLOOR, (hb.get(0.75) or 0) / 2, (hb2.get(0.75) or 0) / 2),
                   max(xv.SLIP_FLOOR, (bb.get(0.75) or 0) / 2, (bb2.get(0.75) or 0) / 2))
        p(f'{c}: (b) before sweep-signal prints (n {len(r2)}): HL bounce p75 {bp(hb2.get(0.75))} bp, Binance p75 {bp(bb2.get(0.75))} bp')
        p(f'{c} (n {len(r)}): HL bounce median {bp(hb.get(0.5))} / p75 {bp(hb.get(0.75))} bp, HL taker prints / 10 s '
          f'{xv.median([x["hl_n"] for x in r])}; Binance bounce median {bp(bb.get(0.5))} / p75 {bp(bb.get(0.75))} bp, trades / 10 s '
          f'{xv.median([x["bn_n"] for x in r])}; depth +-0.2% median ${xv.median([x["depth"] for x in r]) or 0:,.0f}, '
          f'p10 ${xv.quantiles([x["depth"] for x in r], (0.1,)).get(0.1) or 0:,.0f} -> slippage per fill HL {bp(slip[c][0])} '
          f'bp, Binance {bp(slip[c][1])} bp')
    p(f'code constants SLIP_HL {xv.SLIP_HL}, SLIP_BN {xv.SLIP_BN}')
    for c in COINS:
        p(f'{c}: round trip (4 fills) {bp(xv.round_trip_cost(c))} bp; G_MIN {bp(xv.g_min(c))} bp')

    p('\n### 3.3 gaps at the scan points (prices at or before each forced book print)')
    mx_exec = gtab(events, prim, ex, 'EXECUTABLE quotes (primary signal): HL ask vs Binance bid (longs liq.), HL bid vs Binance ask (shorts)')
    gtab(events, prim, sweep, 'SWEEP (reported variant): the forced print vs Binance last trade')
    reach = sum(1 for c in COINS for s_ in ('L', 'S') for x in mx_exec[(c, s_)] if x is not None and x >= xv.g_min(c))
    p(f'triggers whose max executable gap reaches their coin G_MIN: {reach} / {sum(len(v) for v in mx_exec.values())}')

    p('\n### 3.5 filters, positions, clusters')
    for name, (pp, sk) in plans.items():
        evn = sets[VARIANTS[name][0]]['events']
        sel_sk = {k_: sum(1 for i in v if evn[i]['coin'] in COINS) for k_, v in sk.items()}
        maj = sum(1 for x in pp if x['major'])
        p(f'[{name}] skipped {sel_sk}; POSITIONS {len(pp)} (majors {maj}, alts {len(pp) - maj}): '
          + ', '.join(f'{c} {sum(1 for x in pp if x["coin"] == c)}' for c in COINS))
    pos = plans['primary'][0]
    srt = sorted(pos, key=lambda x: (x['entry'], x['coin']))
    half = len(srt) // 2
    p(f'sides: L {sum(1 for x in pos if x["side"] == "L")} / S {sum(1 for x in pos if x["side"] == "S")}; majors '
      f'L {sum(1 for x in pos if x["major"] and x["side"] == "L")} / S {sum(1 for x in pos if x["major"] and x["side"] == "S")}')
    if half:
        p(f'count halves {half} / {len(srt) - half}, split at {iso(srt[half]["entry"])}; majors / alts per half '
          f'{sum(1 for x in srt[:half] if x["major"])}/{sum(1 for x in srt[:half] if not x["major"])} and '
          f'{sum(1 for x in srt[half:] if x["major"])}/{sum(1 for x in srt[half:] if not x["major"])}')
    per_h, per_d = {}, {}
    for x in pos:
        per_h[x['hour']] = per_h.get(x['hour'], 0) + 1
        per_d[x['day']] = per_d.get(x['day'], 0) + 1
    big_d = max(per_d.items(), key=lambda kv: kv[1]) if per_d else (None, 0)
    p(f'clusters: {len(per_h)} UTC-hour, {len(per_d)} UTC-day; max per hour {max(per_h.values()) if per_h else 0}, '
      f'max per day {big_d[1]} ({iso(big_d[0] * D)[:10] if big_d[0] else "-"}); 2025-10-10: '
      f'{sum(1 for x in pos if iso(x["t_sig"])[:10] == "2025-10-10")}')
    p(f'signal gap q {qs([x["g_sig"] for x in pos])}; signal = scan point no. q {xv.quantiles([x["k_sig"] for x in pos], (0.1, 0.5, 0.9))}; '
      f'signal - trigger (s) q { {k: round(v / 1000, 1) for k, v in xv.quantiles([x["t_sig"] - events[x["i"]]["scan_from"] for x in pos], (0.1, 0.5, 0.9)).items()} }')
    # per-month trend: triggers, positions, per-trigger max exec gap (median)
    p('per month: triggers / positions / median per-trigger max executable gap (bp):')
    months = sorted({iso(events[i]['scan_from'])[:7] for i in prim})
    trend = {}
    for m in months:
        ii = [i for i in prim if iso(events[i]['scan_from'])[:7] == m]
        mxs = [max((g for g in (xv.signed_gap(events[i]['side'], b, h) for _, h, b in ex.get(i, [])) if g is not None), default=None)
               for i in ii]
        mxw = [max((g for g in (xv.signed_gap(events[i]['side'], b, h) for _, h, b in sweep.get(i, [])) if g is not None), default=None)
               for i in ii]
        trend[m] = (len(ii), sum(1 for x in pos if iso(x['t_sig'])[:7] == m), xv.median(mxs), xv.median(mxw))
    p('   (month: triggers / positions (primary) / median per-trigger max EXEC gap / max SWEEP gap, bp)')
    p('   ' + ', '.join(f'{m}: {a} / {b} / {bp(c)} / {bp(w)}' for m, (a, b, c, w) in trend.items()))
    # fill delay (times only, never prices): from t + L to the first print of OUR side (book, not forced)
    for lat in xv.LATENCIES:
        dh, db, mh, mb = [], [], 0, 0
        for c in COINS:
            ps = [x for x in pos if x['coin'] == c]
            for side in ('L', 'S'):
                tk = xv._takers(side)
                q = [(str(x['i']), x['t_sig'] + lat) for x in ps if x['side'] == side]
                if not q:
                    continue
                nh = next_print_times(c, q, tk[0])
                nb = {}
                for day, qq in _by_day(q).items():
                    nb.update(bn.first_at_or_after(bn.SYMBOL[c], qq, OFFSET_MS, xv.FILL_WINDOW, taker=tk[1], times_only=True))
                for k, t in q:
                    if k in nh:
                        dh.append(nh[k] - t)
                    else:
                        mh += 1
                    if k in nb:
                        db.append(nb[k][0] - t)
                    else:
                        mb += 1
        p(f'L = {lat} ms: entry FILL DELAY to our side (ms) HL q {xv.quantiles(dh, (0.5, 0.9, 0.99))} max {max(dh) if dh else None}, '
          f'> 1 s: {sum(1 for x in dh if x > 1000)}, none within {xv.FILL_WINDOW // 1000} s: {mh}; Binance q '
          f'{xv.quantiles(db, (0.5, 0.9, 0.99))} max {max(db) if db else None}, none: {mb}')
    (DATA / 'scratch/l8_events.json').write_text(json.dumps(
        {'positions': pos, 'slip': slip, 'trend': trend, 'skipped': plans['primary'][1]}, default=str))
    return pos


def calm_bounce(coin, hours):
    """Median |relative price change| between consecutive HL BOOK taker prints whose aggressor side
    flipped, in calm hours (a full-spread proxy), and the same for Binance."""
    c = con()
    hb, bb = [], []
    for h0 in hours:
        files = tape._files(DEX, h0, h0 + H)
        r = c.execute(f"""
            WITH a AS (SELECT CAST(price AS DOUBLE) px, side,
                              lag(CAST(price AS DOUBLE)) OVER w pp, lag(side) OVER w ps
                       FROM read_parquet({files!r}, filename = true, file_row_number = true)
                       WHERE coin = '{coin}' AND {BOOK} AND epoch_ms(timestamp) >= {h0} AND epoch_ms(timestamp) < {h0 + H}
                       WINDOW w AS (ORDER BY epoch_ms(timestamp), filename, file_row_number))
            SELECT median(abs(px / pp - 1)) FROM a WHERE ps IS NOT NULL AND ps <> side""").fetchone()[0]
        if r is not None:
            hb.append(r)
        fs = bn.files('aggTrades', bn.SYMBOL[coin], h0, h0 + H)
        r = c.execute(f"""
            WITH a AS (SELECT price px, is_buyer_maker m, lag(price) OVER w pp, lag(is_buyer_maker) OVER w pm
                       FROM read_parquet({fs!r}) WHERE transact_time >= {h0} AND transact_time < {h0 + H}
                       WINDOW w AS (ORDER BY transact_time, agg_trade_id))
            SELECT median(abs(px / pp - 1)) FROM a WHERE pm IS NOT NULL AND pm <> m""").fetchone()[0]
        if r is not None:
            bb.append(r)
    return xv.median(hb), xv.median(bb)


def section_stale(events_unused=None):
    p('\n## 3.3b STALENESS of the executable-quote proxies (behaviour only, prices at or before each scan time)')
    sets, plans, _ = built()
    d = sets['causal']
    ev, sweep, quotes = d['events'], d['scans'][('book', 'sweep')], d['quotes']
    prim = [i for i, e in enumerate(ev) if not e['fallback_median']]
    cal_events = load_events()
    spread = {}
    for coin in COINS:
        hours = calm_hours(coin, cal_events)
        rng = random.Random(f'stale-{coin}')
        hb, bb = calm_bounce(coin, sorted(rng.sample(hours, min(24, len(hours)))))
        spread[coin] = (hb, bb)
        p(f'{coin}: calm-hour spread proxy (p50 bounce = 2 x p50 half-bounce) HL {bp(hb)} bp, Binance {bp(bb)} bp')
    rows = {}
    for i in prim:
        e = ev[i]
        s_ = xv.direction(e['side'])
        last_px, times = {}, []
        for t, ph, _ in sweep.get(i, []):
            if t not in last_px:
                times.append(t)
            last_px[t] = ph
        for k, t in enumerate(times):
            q = quotes.get((i, t)) or (None, None, None, None)
            hq, bq, ht, bt = q
            r = rows.setdefault((xv.klass(e['coin']), e['side']), {'age_hl': [], 'age_bn': [], 'dist': [], 'older': [],
                                                                     'n': 0, 'coins': []})
            r['n'] += 1
            r['coins'].append(e['coin'])
            if ht is not None:
                r['age_hl'].append(t - ht)
                r['dist'].append(s_ * (hq - last_px[t]) / last_px[t])
                if k > 0:
                    r['older'].append(ht < times[k - 1])
            if bt is not None:
                r['age_bn'].append(t - bt)
    out = {}
    for (k, side), r in sorted(rows.items()):
        sp = xv.median([spread[c][0] for c in set(r['coins'])])
        md = xv.median(r['dist'])
        out[f'{k}|{side}'] = {'n': r['n'], 'age_hl_q': xv.quantiles(r['age_hl'], (0.5, 0.75, 0.9)),
                              'age_bn_q': xv.quantiles(r['age_bn'], (0.5, 0.75, 0.9)), 'dist_q': xv.quantiles(r['dist'], (0.25, 0.5, 0.75, 0.9)),
                              'older_share': (sum(r['older']) / len(r['older'])) if r['older'] else None, 'calm_spread_hl': sp,
                              'bias_upper_bound': (md - sp) if md is not None and sp is not None else None}
        o = out[f'{k}|{side}']
        p(f'{k}s {side} (scan points {r["n"]:,}): age of the HL proxy (ms) q {o["age_hl_q"]}; Binance proxy q {o["age_bn_q"]}; '
          f'HL proxy beyond the forced print, s (HL_q - P_forced)/P_forced, q { {q: bp(v) for q, v in o["dist_q"].items()} } bp; '
          f'calm HL spread {bp(sp)} bp; share of points whose HL proxy predates the run\'s previous forced print '
          f'{o["older_share"]:.1%}; no HL proxy within 1 s at {r["n"] - len(r["age_hl"]):,} points '
          f'({(r["n"] - len(r["age_hl"])) / max(r["n"], 1):.1%}, the stale cases: no signal there); UPPER BOUND on the staleness bias (median distance - calm spread) {bp(o["bias_upper_bound"])} bp')
    (DATA / 'scratch/l8_stale.json').write_text(json.dumps(out, indent=1, default=str))


def _by_day(q):
    out = {}
    for k, t in q:
        out.setdefault(t // D, []).append((k, t))
    return out


def next_print_times(coin, q, taker=None):
    """{key: time of the first HL BOOK print (not forced) at or after t, of the given aggressor side,
    within FILL_WINDOW} — times only, never the price."""
    out = {}
    c = con()
    cond = '' if taker is None else f"AND f.side = '{taker}'"
    for day, qq in _by_day(q).items():
        files = tape._files(DEX, min(t for _, t in qq), max(t for _, t in qq) + xv.FILL_WINDOW)
        c.execute('CREATE OR REPLACE TEMP TABLE nq(k VARCHAR, t BIGINT)')
        c.executemany('INSERT INTO nq VALUES (?, ?)', qq)
        rows = c.execute(f"""
            SELECT nq.k, MIN(epoch_ms(f.timestamp)) FROM read_parquet({files!r}) f JOIN nq ON f.coin = '{coin}'
            WHERE epoch_ms(f.timestamp) >= nq.t AND epoch_ms(f.timestamp) <= nq.t + {xv.FILL_WINDOW} {cond}
              AND {BOOK_F}
            GROUP BY 1""").fetchall()
        out.update(dict(rows))
    return out


BOOK_F = BOOK.replace('crossed', 'f.crossed').replace('direction', 'f.direction').replace('is_liquidation', 'f.is_liquidation')


def pre_stats(events, points):
    """points: [(event index, t)]. Per (coin, event): Binance intensity and bounce (consecutive trades
    whose taker side flips: a full-spread proxy) in [t - 10 s, t), depth +-0.2% at the last bookDepth
    snapshot before t; HL taker prints and bounce in the same 10 s. Everything strictly before t."""
    out = {}
    c = con()
    for coin in COINS:
        pts = [(i, t) for i, t in points if events[i]['coin'] == coin]
        q = [(str(i), t) for i, t in pts]
        st, dp, hl = {}, {}, {}
        for day, qq in _by_day(q).items():
            st.update(bn.pre_window_stats(bn.SYMBOL[coin], qq, 10 * S, OFFSET_MS))
            dp.update(bn.depth_before(bn.SYMBOL[coin], qq, 0.2, OFFSET_MS))
            files = tape._files(DEX, min(t for _, t in qq) - 10 * S, max(t for _, t in qq))
            c.execute('CREATE OR REPLACE TEMP TABLE hq(k VARCHAR, t BIGINT)')
            c.executemany('INSERT INTO hq VALUES (?, ?)', qq)
            rows = c.execute(f"""
                WITH a AS (SELECT epoch_ms(timestamp) tt, filename fn, file_row_number rn, CAST(price AS DOUBLE) px, side
                           FROM read_parquet({files!r}, filename = true, file_row_number = true)
                           WHERE coin = '{coin}' AND {BOOK}),
                j AS (SELECT hq.k, a.px, a.side, lag(a.px) OVER w pp, lag(a.side) OVER w ps
                      FROM hq JOIN a ON a.tt >= hq.t - {10 * S} AND a.tt < hq.t
                      WINDOW w AS (PARTITION BY hq.k ORDER BY a.tt, a.fn, a.rn))
                SELECT k, COUNT(*), median(CASE WHEN ps IS NOT NULL AND ps <> side THEN abs(px / pp - 1) END)
                FROM j GROUP BY k""").fetchall()
            hl.update({k: (n, b) for k, n, b in rows})
        for i, t in pts:
            k = str(i)
            s_ = st.get(k, (0, None, None, 0))
            d_ = dp.get(k)
            h_ = hl.get(k, (0, None))
            out[(coin, i)] = {'bn_n': s_[0], 'bn_dt': s_[1], 'bn_bounce': s_[2],
                              'depth': (min(d_[1], d_[2]) if d_ and d_[1] and d_[2] else None),
                              'depth_age': ((t - d_[0]) / 1000 if d_ else None),
                              'hl_n': h_[0], 'hl_bounce': h_[1]}
    return out


# ============================================================================ power

def hl_last_before(coin, q, lookback=10 * M):
    """{key: last HL book print price at or before t} for [(key, t)] (within lookback)."""
    out = {}
    c = con()
    for day, qq in _by_day(q).items():
        files = tape._files(DEX, min(t for _, t in qq) - lookback, max(t for _, t in qq))
        c.execute('CREATE OR REPLACE TEMP TABLE lq(k VARCHAR, t BIGINT)')
        c.executemany('INSERT INTO lq VALUES (?, ?)', qq)
        rows = c.execute(f"""
            SELECT lq.k, arg_max(CAST(f.price AS DOUBLE), (epoch_ms(f.timestamp), f.filename, f.file_row_number))
            FROM read_parquet({files!r}, filename = true, file_row_number = true) f JOIN lq ON f.coin = '{coin}'
            WHERE epoch_ms(f.timestamp) <= lq.t AND epoch_ms(f.timestamp) > lq.t - {lookback}
              AND {BOOK_F}
            GROUP BY 1""").fetchall()
        out.update(dict(rows))
    return out


def pre_signal_gap_changes(pos):
    """BEFORE each signal (prices at or before t only): (coin, g(t-60)-g(t-120) on last prints — the
    pre-signal, mostly PRE-CASCADE noise; g_exec(t) - g_exec(t-60) on executable quotes, only where
    t - 60 s is inside the trigger window or the run (t - 60 >= t_start))."""
    out = []
    for coin in COINS:
        ps = [x for x in pos if x['coin'] == coin]
        if not ps:
            continue
        q = [(f"{x['i']}:{k}", x['t_sig'] - k * S) for x in ps for k in (60, 120)]
        hl = hl_last_before(coin, q)
        bq = {}
        for day, qq in _by_day(q).items():
            bq.update(bn.last_at_or_before(bn.SYMBOL[coin], qq, OFFSET_MS, lookback_ms=10 * M))
        inside = [x for x in ps if x['t_sig'] - 60 * S >= x['t_start']]
        ex = {}
        for side in ('L', 'S'):
            tk = xv._takers(side)
            qq_ = [(str(x['i']), coin, x['t_sig'] - 60 * S) for x in inside if x['side'] == side]
            if not qq_:
                continue
            hq = hl_quotes(qq_, tk[0])
            bqq = {}
            for day, qd in _by_day([(k, t) for k, _, t in qq_]).items():
                bqq.update(bn.last_at_or_before(bn.SYMBOL[coin], qd, OFFSET_MS, lookback_ms=xv.QUOTE_WINDOW, taker=tk[1]))
            for k, _, _ in qq_:
                b_, h_ = bqq.get(k), hq.get(k)
                ex[k] = (h_[1] if h_ else None, b_[1] if b_ else None)
        for x in ps:
            g = {}
            for k in (60, 120):
                h_, b_ = hl.get(f"{x['i']}:{k}"), bq.get(f"{x['i']}:{k}")
                g[k] = xv.signed_gap(x['side'], b_[1] if b_ else None, h_)
            a_ = g[60] - g[120] if g[60] is not None and g[120] is not None else None
            e_ = None
            if str(x['i']) in ex:
                h_, b_ = ex[str(x['i'])]
                g60 = xv.signed_gap(x['side'], b_, h_)
                e_ = x['g_sig'] - g60 if g60 is not None else None
            out.append((coin, a_, e_))
    return out


def in_run_changes(events, ids, ex, pos):
    """Executable gap changes over ~60 s between two scan points of the same run, both at or before the
    signal (or the run end when there is none) — contemporaneous quotes at forced prints only."""
    sig = {x['i']: x['t_sig'] for x in pos}
    out = []
    for i in ids:
        pts = [(t, xv.signed_gap(events[i]['side'], b, h)) for t, h, b in ex.get(i, []) if t <= sig.get(i, 1 << 62)]
        pts = [(t, g) for t, g in pts if g is not None]
        ts = [t for t, _ in pts]
        for t, g in pts:
            j = bisect.bisect_left(ts, t + 55 * S)
            if j < len(pts) and pts[j][0] <= t + 65 * S:
                out.append((events[i]['coin'], pts[j][1] - g))
                break                                     # one pair per trigger: no overweighting long runs
    return out


def p_confirm(pos, n, sigma, mu, sims=2000, seed=5):
    """P(holdout CONFIRM) for n positions drawn as contiguous blocks of the discovery pattern."""
    rng = random.Random(seed)
    srt = sorted(pos, key=lambda x: x['entry'])
    n = min(n, len(srt))
    hits = 0
    for _ in range(sims):
        st = rng.randrange(0, len(srt) - n + 1)
        blk = srt[st:st + n]
        rows = [dict(x, ret={xv.PRIMARY: mu + rng.gauss(0, sigma[x['coin']])}) for x in blk]
        hits += xv.holdout_confirm(rows)[0]
    return hits / sims


def section_power(events_unused=None, pos=None):
    p('\n## 5. POWER — null SE and joint power at the signal times; holdout size')
    if pos is None:
        pos = [dict(x) for x in json.loads((DATA / 'scratch/l8_events.json').read_text())['positions']]
    sig, sig_ret = {}, {}
    p('(a) calm hours (as section 1): SD of the 60-s change of log Binance - log HL on a 60-s grid')
    cal_events = load_events()
    for coin in COINS:
        hours = calm_hours(coin, cal_events)
        rng = random.Random(f'power-{coin}')
        pick = sorted(rng.sample(hours, min(36, len(hours))))
        dg, dr = [], []
        for h0 in pick:
            hp = hl_prints(coin, h0 - 60 * S, h0 + H)
            bt = [(t, px) for t, px, _ in bn.window_trades(bn.SYMBOL[coin], h0 - 60 * S, h0 + H, OFFSET_MS)]
            if len(hp) < 20 or len(bt) < 20:
                continue
            n = 61
            hg = grid_log(hp, h0, 60 * S, n)
            bg = grid_log(bt, h0, 60 * S, n)
            gap = [None if a is None or b is None else (b - a) for a, b in zip(hg, bg)]
            for k in range(n - 1):
                if gap[k] is not None and gap[k + 1] is not None:
                    dg.append(gap[k + 1] - gap[k])
                if hg[k] is not None and hg[k + 1] is not None:
                    dr.append(hg[k + 1] - hg[k])
        sig[coin] = statistics.pstdev(dg) if len(dg) > 10 else None
        sig_ret[coin] = statistics.pstdev(dr) if len(dr) > 10 else None
        p(f'{coin}: SD of the 60-s gap change {bp(sig[coin])} bp (robust {bp(_iqr_sd(dg))}); HL 60-s return {bp(sig_ret[coin])} bp')
    ch = pre_signal_gap_changes(pos)
    sig_pre, sig_in = {}, {}
    p('(b) PRE-SIGNAL, mostly PRE-CASCADE noise: SD of g(t-60 s) - g(t-120 s) on last prints (t = the signal)')
    p('(c) IN-CASCADE: g_exec(t) - g_exec(t-60 s), only where t - 60 s >= the trigger window\'s first print (selection-inflated: g(t) >= G_MIN)')
    for k in ('major', 'alt'):
        a_ = [x[1] for x in ch if xv.klass(x[0]) == k and x[1] is not None]
        e_ = [x[2] for x in ch if xv.klass(x[0]) == k and x[2] is not None]
        sa = statistics.pstdev(a_) if len(a_) > 2 else None
        se_ = statistics.pstdev(e_) if len(e_) > 2 else None
        p(f'{k}s: (b) n {len(a_)}, SD {bp(sa)} bp (robust {bp(_iqr_sd(a_))}); (c) n {len(e_)}, SD {bp(se_)} bp '
          f'(robust {bp(_iqr_sd(e_))}), median {bp(xv.median(e_))} bp')
        for c in COINS:
            if xv.klass(c) == k:
                sig_pre[c], sig_in[c] = sa, se_
    sets_, plans, _ = built()
    d = sets_['causal']
    prim = [i for i, e in enumerate(d['events']) if not e['fallback_median']]
    ir = in_run_changes(d['events'], prim, d['scans'][('book', 'exec')], pos)
    irs = in_run_changes(d['events'], prim, d['scans'][('book', 'sweep')], pos)
    sig_sw = {}
    for k in ('major', 'alt'):
        v = [g for c, g in irs if xv.klass(c) == k]
        sv = statistics.pstdev(v) if len(v) > 2 else None
        p(f'(d2) IN-RUN SWEEP-gap change ~60 s apart (both at or before the signal): {k}s n {len(v)}, SD '
          f'{bp(sv)} bp (robust {bp(_iqr_sd(v))})')
        for c in COINS:
            if xv.klass(c) == k:
                sig_sw[c] = sv
    sig_run = {}
    for k in ('major', 'alt'):
        v = [g for c, g in ir if xv.klass(c) == k]
        sv = statistics.pstdev(v) if len(v) > 2 else None
        p(f'(d) IN-RUN, all primary triggers: executable-gap change between two scan points ~60 s apart, both at or before '
          f'the signal: {k}s n {len(v)}, SD {bp(sv)} bp (robust {bp(_iqr_sd(v))})')
        for c in COINS:
            if xv.klass(c) == k:
                sig_run[c] = sv
    hold_max = xv.CAP + 2 * xv.L_PRIMARY
    scale = math.sqrt(hold_max / 60_000)
    # holdout size at the RECENT rate (last four discovery months)
    mar = int(dt.datetime(2026, 3, 1, tzinfo=dt.UTC).timestamp() * 1000)
    recent = [x for x in pos if x['t_sig'] >= mar]
    rate = len(recent) / ((CUT - mar) / D)
    n_a = max(1, round(rate * 101))
    p(f'positions {len(pos)}; recent rate (2026-03-01 -> 06-30) {len(recent)} / {(CUT - mar) / D:.0f} days = {rate:.3f} a day '
      f'-> option A (2026-07-01 -> 10-09, 101 days): n = {n_a}; full-year rate would give {round(len(pos) / ((CUT - DISC_START) / D) * 101)}')
    flat = lambda v: {c: v for c in COINS}
    scen = [('calm', sig), ('pre-signal (mostly pre-cascade) x hold', {c: (v or 0) * scale for c, v in sig_pre.items()}),
            ('in-cascade g_exec(t)-g_exec(t-60) x hold (selection-inflated)', {c: (v or 0) * scale for c, v in sig_in.items()}),
            ('in-run executable 60-s change x hold', {c: (v or 0) * scale for c, v in sig_run.items()}),
            ('in-run sweep 60-s change x hold', {c: (v or 0) * scale for c, v in sig_sw.items()}),
            ('25 bp per trade', flat(0.0025)), ('50 bp per trade', flat(0.005)), ('100 bp per trade', flat(0.01))]
    out = {'sigma_calm': sig, 'sigma_pre_signal': sig_pre, 'sigma_in_cascade': sig_in, 'sigma_in_run': sig_run,
           'rate_recent': rate, 'n_holdout_A': n_a, 'scenarios': {}}
    rho = {}
    for lab, sg in scen:
        se = [xv.null_se(pos, sg, rho, key=k) for k in (None, 'hour', 'day')]
        m = drift.mde(se[1], se[0], xv.Z_HOUR)
        p(f'[{lab}] null SE {bp(se[0])} / hour {bp(se[1])} / day {bp(se[2])} bp; MDE at t 2.33: 50% {bp(m["p50"])} / 80% {bp(m["p80"])} bp')
        for mu in (0.0, 0.0010, 0.0015, 0.0025, 0.0050):
            r = xv.simulate_power(pos, sg, rho, mu, sims=1000, seed=3, holdout_n=n_a, hold_sims=1000)
            out['scenarios'][f'{lab}|{mu}'] = r
            p(f'   mu {bp(mu)} bp: P(pass all eight) {r["p_pass"]:.3f}; P(pass, est >= 10 bp) {r["p_pass_and_gate"]:.3f}; '
              f'P(CONFIRM, n {r["holdout_n"]}) {r["p_confirm"]:.3f}; END-TO-END {r["end_to_end"]:.3f}; P(pass, est >= 25 bp) '
              f'{r["p_pass_and_bar"]:.3f}')
    # option B: the n giving P(CONFIRM) >= 0.8 at a true 25 bp under the 25-bp noise row, at the recent rate
    p('holdout options (P(CONFIRM) at a true 25 bp):')
    for lab, sg in (('25 bp noise', flat(0.0025)), ('50 bp noise', flat(0.005))):
        pa = p_confirm(pos, n_a, sg, 0.0025)
        n_b = None
        for n in range(3, len(pos) + 1):
            if p_confirm(pos, n, sg, 0.0025, sims=1000) >= 0.8:
                n_b = n
                break
        if n_b is None and len(pos) >= 3:
            n_b = f'> {len(pos)} (beyond the discovery pattern)'
        end_b, days_b = drift.holdout_end_for(n_b, rate) if isinstance(n_b, int) and rate > 0 else (None, None)
        full = len(pos) / ((CUT - DISC_START) / D)
        end_f, days_f = drift.holdout_end_for(n_b, full) if isinstance(n_b, int) and full > 0 else (None, None)
        out[f'option|{lab}'] = {'n_A': n_a, 'p_confirm_A': pa, 'n_B': n_b, 'end_B_recent': iso(end_b)[:10] if end_b else None,
                                'end_B_full_year_rate': iso(end_f)[:10] if end_f else None}
        p(f'   [{lab}] A: n {n_a}, P(CONFIRM) {pa:.3f}; B: n {n_b} for P >= 0.8 -> at the recent rate '
          f'{f"{days_b} days, end {iso(end_b)[:10]}" if end_b else "never (zero positions since 2026-03-01)"}; at the full-year '
          f'rate {full:.4f}/day: {f"{days_f} days, end {iso(end_f)[:10]}" if end_f else "-"}')
    (DATA / 'scratch/l8_power.json').write_text(json.dumps(out, indent=1, default=str))


def _iqr_sd(xs):
    q = xv.quantiles(xs, (0.25, 0.75))
    return (q[0.75] - q[0.25]) / 1.349 if q else None


def main():
    secs = [a for a in sys.argv[1:] if not a.startswith('-')] or ['clock', 'clockdays', 'hlclock', 'events', 'stale', 'power']
    events = load_events()
    p(f'# l8_describe {dt.datetime.now(dt.UTC).isoformat()} sections {secs}')
    pos = None
    try:
        run_sections(secs, events)
    finally:
        with open(LOG, 'a') as f:
            f.write('\n'.join(_out) + '\n')
    p(f'\nlog: {LOG}')


def run_sections(secs, events):
    pos = None
    if 'clock' in secs:
        section_clock(events)
    if 'clockdays' in secs:
        section_clockdays(events)
    if 'hlclock' in secs:
        section_hlclock()
    if 'events' in secs:
        pos = section_events()
    if 'stale' in secs:
        section_stale()
    if 'power' in secs:
        section_power(None, pos)


if __name__ == '__main__':
    main()
