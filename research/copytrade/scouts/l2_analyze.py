"""L2 carry scout: coverage and UNCONDITIONAL level distributions (behaviour only).

Reads what l2_fetch.py saved under data/scout_l2/. Every row dated >= CUT is
dropped at load (count reported). Computes, per venue pair and coin group:
- coverage (first date, interval mix, gaps, overlapping-coin counts);
- quantiles of the annualised hourly funding spread HL - CEX, and the share of
  hours with |spread| above round-trip cost amortised over 1 / 7 / 30 days;
- Binance perp-spot basis levels and Binance funding (perp-spot carry) levels;
- tail ingredients: largest prints, HL cap prints, monthly COUNTS of sign changes.

Forbidden here, and absent: autocorrelation, persistence, half-life, anything
conditional on an earlier value, any entry/exit rule or P&L.

  .venv/bin/python -I scouts/l2_analyze.py > data/scout_l2/analysis.txt
"""
import bisect
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scouts'))
import l2_fetch as F  # noqa: E402

CUT, HOUR, OUT = F.CUT, F.HOUR, F.OUT
CORE = F.CORE
HL_HOURLY_FROM = F.ms(2023, 6, 8) + HOUR // 2                        # first hourly HL print (BTC: 2023-06-08 00:00)
DROPPED = Counter()
INACTIVE = defaultdict(set)                                 # HL coin -> hours with rate == premium == 0

# Round-trip cost, bp of notional per leg pair: open + close both legs, taker,
# base tier (see l2-carry.md §B), plus a half-spread per fill (core 0.5 bp,
# others 2 bp; the HL tape proxy says <= 1 tick, this is deliberately above it).
TAKER = {'hl': 4.5, 'binance': 5.0, 'bybit': 5.5, 'okx': 5.0, 'bn_spot': 10.0}
HALF_SPREAD = {'core': 0.5, 'others': 2.0}
HOLDS = (1, 7, 30)


def rt_cost_bp(a, b, group):
    return 2 * (TAKER[a] + TAKER[b]) + 4 * HALF_SPREAD[group]


def amort_ann(rt_bp, days):
    return rt_bp / 1e4 * 365 / days                      # as an annualised fraction


def keep(rows, src, t=lambda r: r[0]):
    k = [r for r in rows if t(r) < CUT]
    DROPPED[src] += len(rows) - len(k)
    return k


def hr(t):
    return (t + HOUR // 2) // HOUR * HOUR                  # nearest hour


def year(t):
    return datetime.fromtimestamp(t / 1000, timezone.utc).year


def ymonth(t):
    return datetime.fromtimestamp(t / 1000, timezone.utc).strftime('%Y-%m')


def day(t):
    return datetime.fromtimestamp(t / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M')


def quant(xs, ps=(.1, .25, .5, .75, .9)):
    s = sorted(xs)
    out = []
    for p in ps:
        if not s:
            out.append(None)
            continue
        i = p * (len(s) - 1)
        lo = int(i)
        hi = min(lo + 1, len(s) - 1)
        out.append(s[lo] + (s[hi] - s[lo]) * (i - lo))
    return out


# ---------------------------------------------------------------- loaders
def load_hl():
    """coin -> {hour: rate per hour} from every saved HL file (probe files included)."""
    out = defaultdict(dict)
    for f in (OUT / 'hl').glob('*.json'):
        coin = f.name.rsplit('_', 3)[0]
        for t, r, prem in keep(json.loads(f.read_text()), 'hl'):
            if r == 0 and prem == 0:                       # delisted assets keep printing exact zeros
                INACTIVE[coin].add(hr(t))
                continue
            if t < HL_HOURLY_FROM:                         # 8-hour settlements until 2023-06-08
                for k in range(8):
                    out[coin][hr(t) - k * HOUR] = r / 8
            else:
                out[coin][hr(t)] = r
    return out


def load_bn():
    out = defaultdict(dict)                                # sym -> {settle_hour: (rate, interval_h)}
    for f in (OUT / 'binance' / 'funding').glob('*.json'):
        v = json.loads(f.read_text())
        if not v:
            continue
        sym = f.name.rsplit('_', 1)[0]
        for t, ih, r in keep(v['rows'], 'binance'):
            out[sym][hr(t)] = (r, ih)
    return out


def load_by():
    out = defaultdict(dict)
    for f in (OUT / 'bybit').glob('*.json'):
        v = json.loads(f.read_text())
        if not v:
            continue
        sym = f.name.rsplit('_', 2)[0]
        for t, r in keep(v, 'bybit'):
            out[sym][hr(t)] = (r, None)
    return out


def load_okx():
    out = defaultdict(dict)
    for f in (OUT / 'okx').glob('*.json'):
        v = json.loads(f.read_text())
        if not v:
            continue
        fam = f.name.rsplit('_', 1)[0]
        for t, r in keep(v['rows'], 'okx'):
            out[fam][hr(t)] = (r, None)
    return out


def with_intervals(series):
    """{settle: (rate, ih)} -> sorted [(settle, rate, interval_h)]; inferred from gaps when absent."""
    ts = sorted(series)
    out = []
    for i, t in enumerate(ts):
        r, ih = series[t]
        if not ih:
            if i == 0:
                continue
            ih = (t - ts[i - 1]) // HOUR
            if ih not in (1, 2, 4, 8):                     # a gap in our sample, not an interval
                continue
        out.append((t, r, int(ih)))
    return out


def cex_hourly(series):
    """hour-end -> CEX rate per hour, for every hour covered by a settlement interval."""
    out = {}
    for t, r, ih in with_intervals(series):
        for k in range(ih):
            out[t - k * HOUR] = r / ih
    return out


# ---------------------------------------------------------------- mapping
def cex_maps(hl_coins, bn, by, okx):
    m = {}
    for c in hl_coins:
        b = F.base_of(c)
        bsyms = (f'1000{b}USDT', f'{b}USDT') if c.startswith('k') else (f'{c}USDT',)
        m[(c, 'binance')] = next((s for s in bsyms if s in bn), None)
        m[(c, 'bybit')] = next((s for s in F.bybit_sym_candidates(c) if s in by), None)
        m[(c, 'okx')] = f'{b}-USDT' if f'{b}-USDT' in okx else None
    return m


def main():
    hl = load_hl()
    bn, by, okx = load_bn(), load_by(), load_okx()
    first = F.hl_coins()
    venues = {'binance': bn, 'bybit': by, 'okx': okx}
    cmap = cex_maps(first, bn, by, okx)
    wins = F.windows()
    R = {}

    # ---------------- A. coverage
    print('# A. coverage')
    print('HL coins with funding before cutoff:', len(first),
          '| first print', day(min(first.values())))
    for c in CORE:
        ts = sorted(hl[c])
        gaps = Counter((b - a) // HOUR for a, b in zip(ts, ts[1:]))
        by_year = defaultdict(Counter)
        for a, b in zip(ts, ts[1:]):
            by_year[year(b)][(b - a) // HOUR] += 1
        print(f'HL {c}: {day(ts[0])} .. {day(ts[-1])}, {len(ts)} prints, gap hours {dict(gaps.most_common(6))}')
        print('   gaps by year', {y: dict(v.most_common(4)) for y, v in sorted(by_year.items())})
    for v, data in venues.items():
        for c in CORE:
            s = cmap[(c, v)]
            if not s:
                print(f'{v} {c}: not found')
                continue
            iv = with_intervals(data[s])
            mix = defaultdict(Counter)
            for t, _, ih in iv:
                mix[year(t)][ih] += 1
            ts = sorted(data[s])
            big = sum(1 for a, b in zip(ts, ts[1:]) if b - a > 8 * HOUR)
            print(f'{v} {c} ({s}): {day(ts[0])} .. {day(ts[-1])}, {len(ts)} prints, '
                  f'gaps>8h {big}, interval mix by year {dict((y, dict(m)) for y, m in sorted(mix.items()))}')
    # overlapping coins per year (from the window sample)
    print('\noverlap counts per year (coins with >=1 sampled window holding both HL and that CEX):')
    cov = defaultdict(lambda: defaultdict(set))
    allcex_mix = defaultdict(Counter)
    for c in first:
        hlh = hl.get(c, {})
        for w in wins:
            span = range(w + HOUR, w + F.WIN_H * HOUR + 1, HOUR)
            if not any(h in hlh for h in span[::24]):
                continue
            cov['hl'][year(w)].add(c)
            anyc = False
            for v, data in venues.items():
                s = cmap[(c, v)]
                if s and any(w <= t < w + F.WIN_H * HOUR for t in data[s]):
                    cov[v][year(w)].add(c)
                    anyc = True
            if anyc:
                cov['any'][year(w)].add(c)
    for v in ('hl', 'binance', 'bybit', 'okx', 'any'):
        print(f'  {v:8s}', {y: len(s) for y, s in sorted(cov[v].items())})
    for v, data in venues.items():
        for c in first:
            s = cmap[(c, v)]
            if s and c not in CORE:
                for t, _, ih in with_intervals(data[s]):
                    allcex_mix[v][ih] += 1
    print('  interval mix, all sampled non-core settlements:', {v: dict(m) for v, m in allcex_mix.items()})
    R['coverage'] = {v: {y: len(s) for y, s in cov[v].items()} for v in cov}

    # ---------------- C. spread distribution
    print('\n# C. spread HL - CEX, annualised (fraction/yr); hourly; quantiles p10 p25 p50 p75 p90')
    print('#    share: fraction of hours with |spread| > RT cost amortised over 1d / 7d / 30d')
    R['spread'] = {}
    hl_hourly_ann = {}
    for v, data in venues.items():
        pooled = defaultdict(list)                         # (group, year) -> [spread]
        percoin = defaultdict(list)                        # (coin, year) -> [spread], core only
        flips = defaultdict(list)                          # (coin, year) -> monthly sign-change counts
        for c in first:
            s = cmap[(c, v)]
            if not s or c not in hl:
                continue
            group = 'core' if c in CORE else 'others'
            ch = cex_hourly(data[s])
            hlh = hl[c]
            if group == 'others':
                allowed = set()
                for w in wins:
                    allowed.update(range(w + HOUR, w + F.WIN_H * HOUR + 1, HOUR))
            for h, r in hlh.items():
                if h not in ch or (group == 'others' and h not in allowed):
                    continue
                sp = (r - ch[h]) * 8760
                pooled[(group, year(h))].append(sp)
                if group == 'core':
                    percoin[(c, year(h))].append(sp)
            if group == 'core':
                # interval-level spread: HL accrued over each CEX interval minus CEX rate; sign-change COUNT per month
                months = defaultdict(list)
                for t, r, ih in with_intervals(data[s]):
                    hs = [hlh.get(t - k * HOUR) for k in range(ih)]
                    if any(x is None for x in hs):
                        continue
                    months[ymonth(t)].append(sum(hs) - r)
                for mth, xs in months.items():
                    n = sum(1 for a, b in zip(xs, xs[1:]) if (a > 0) != (b > 0) and a != 0 and b != 0)
                    flips[(c, int(mth[:4]))].append((n, len(xs)))
        R['spread'][v] = {}
        for key in sorted(pooled):
            xs = pooled[key]
            group = key[0]
            rt = rt_cost_bp('hl', v, group)
            shares = [sum(1 for x in xs if abs(x) > amort_ann(rt, d)) / len(xs) for d in HOLDS]
            q = quant(xs)
            R['spread'][v][f'{key[0]}_{key[1]}'] = {'n': len(xs), 'q': q, 'share': shares, 'rt_bp': rt}
            print(f'{v:8s} {key[0]:6s} {key[1]} n={len(xs):7d} q=' + ' '.join(f'{x * 100:7.2f}%' for x in q)
                  + f' | RT {rt:.0f}bp share>1d/7d/30d ' + ' '.join(f'{x:.3f}' for x in shares))
        for key in sorted(percoin):
            xs = percoin[key]
            q = quant(xs)
            print(f'{v:8s}   {key[0]:4s} {key[1]} n={len(xs):6d} q=' + ' '.join(f'{x * 100:7.2f}%' for x in q))
        print(f'{v} monthly sign-change COUNTS of the interval-level spread (median, max per month; intervals/month):')
        for key in sorted(flips):
            ns = sorted(n for n, _ in flips[key])
            iv = sorted(m for _, m in flips[key])
            print(f'   {key[0]} {key[1]}: months={len(ns)} median={ns[len(ns) // 2]} max={ns[-1]} '
                  f'(median intervals/month {iv[len(iv) // 2]})')
        R['spread'][v]['flips'] = {f'{k[0]}_{k[1]}': flips[k] for k in flips}

    # ---------------- C2. Binance perp-spot basis and funding levels, core
    print('\n# C2. Binance perp vs spot (core): basis bp at hour close; funding annualised (carry of short perp/long spot)')
    rt_ps = 2 * (TAKER['bn_spot'] + TAKER['binance']) + 4 * HALF_SPREAD['core']
    print(f'   RT cost spot+perp {rt_ps:.0f} bp -> amortised 1d/7d/30d =',
          ' '.join(f'{amort_ann(rt_ps, d) * 100:.2f}%' for d in HOLDS))
    R['basis'] = {}
    for c in CORE:
        sp, pp = {}, {}
        for f in (OUT / 'binance' / 'klines_spot').glob(f'{c}USDT_*.json'):
            v = json.loads(f.read_text())
            if v:
                sp.update((hr(t), x) for t, x in keep(v['rows'], 'klines'))
        for f in (OUT / 'binance' / 'klines_um').glob(f'{c}USDT_*.json'):
            v = json.loads(f.read_text())
            if v:
                pp.update((hr(t), x) for t, x in keep(v['rows'], 'klines'))
        by_y = defaultdict(list)
        for t in sp.keys() & pp.keys():
            by_y[year(t)].append((pp[t] / sp[t] - 1) * 1e4)
        fund = defaultdict(list)
        for h, r in cex_hourly(bn[f'{c}USDT']).items():
            fund[year(h)].append(r * 8760)
        for y in sorted(by_y):
            q = quant(by_y[y])
            fq = quant(fund[y])
            xs = fund[y]
            pos = [sum(1 for x in xs if x > amort_ann(rt_ps, d)) / len(xs) for d in HOLDS]
            neg = [sum(1 for x in xs if x < -amort_ann(rt_ps, d)) / len(xs) for d in HOLDS]
            R['basis'][f'{c}_{y}'] = {'basis_q': q, 'fund_q': fq, 'pos': pos, 'neg': neg, 'n': len(by_y[y])}
            print(f'   {c} {y} n={len(by_y[y]):5d} basis bp ' + ' '.join(f'{x:6.2f}' for x in q)
                  + ' | funding ann ' + ' '.join(f'{x * 100:6.2f}%' for x in fq)
                  + ' | share f>+thr 1d/7d/30d ' + ' '.join(f'{x:.3f}' for x in pos)
                  + ' | f<-thr ' + ' '.join(f'{x:.3f}' for x in neg))

    # ---------------- F. tails
    print('\n# F. largest |funding| prints in the loaded sample (per settlement, raw rate; per-hour in brackets)')
    def top(rows, n=5):
        return sorted(rows, key=lambda r: -abs(r[2]))[:n]
    hl_rows = [(c, h, r, r) for c, d in hl.items() for h, r in d.items()]
    for c, h, r, _ in top(hl_rows, 8):
        print(f'   HL   {c:10s} {day(h)} {r * 100:8.4f}%/h')
    caps = [(c, h, r) for c, h, r, _ in hl_rows if abs(r) >= 0.04 - 1e-9]
    near = [(c, h, r) for c, h, r, _ in hl_rows if abs(r) >= 0.01]
    print(f'   HL prints at the 4%/h cap: {len(caps)}; >= 1%/h: {len(near)} of {len(hl_rows)} loaded HL prints')
    for c, h, r in sorted(caps)[:20]:
        print(f'      cap: {c} {day(h)} {r * 100:.2f}%/h')
    for v, data in venues.items():
        rows = []
        for s, ser in data.items():
            for t, r, ih in with_intervals(ser):
                rows.append((s, t, r, r / ih))
        for s, t, r, ph in top(rows, 6):
            print(f'   {v:8s} {s:14s} {day(t)} {r * 100:8.4f}% per settlement ({ph * 100:.4f}%/h)')
        core_syms = {cmap[(c, v)] for c in CORE}
        for s, t, r, ph in top([x for x in rows if x[0] in core_syms], 3):
            print(f'   {v:8s} core {s:14s} {day(t)} {r * 100:8.4f}% per settlement ({ph * 100:.4f}%/h)')
    for c, h, r, _ in top([x for x in hl_rows if x[0] in CORE], 3):
        print(f'   HL   core {c:6s} {day(h)} {r * 100:8.4f}%/h')
    # ---------------- G. sizes for a full pull (upper bounds: every HL coin from its first print to the cutoff)
    print('\n# G. full-pull sizes, 2023-05 .. 2026-06, all HL coins found (upper bound: no delisting end)')
    hl_hours = sum((CUT - t) // HOUR for t in first.values())
    print(f'   HL fundingHistory: {len(first)} coins, {hl_hours:,} coin-hours -> ~{hl_hours // 500:,} requests '
          f'(500 rows/page, weight ~45 -> ~{hl_hours // 500 * 45 / 1100 / 60:.1f} h at 1,100 weight/min)')
    for v, sub in (('binance', OUT / 'binance' / 'funding'), ('okx', OUT / 'okx')):
        sizes = [json.loads(f.read_text()) for f in sub.glob('*.json')]
        sizes = [x['bytes'] for x in sizes if x]
        avg = sum(sizes) / len(sizes)
        n_sym = len({c for c in first if cmap[(c, v)]})
        n_months = sum(min(38, max(1, (CUT - t) // (30 * 86400000))) for c, t in first.items() if cmap[(c, v)])
        print(f'   {v}: avg monthly zip {avg:,.0f} B over {len(sizes)} files; {n_sym} overlapping coins, '
              f'~{n_months:,} coin-months -> ~{n_months * avg / 1e6:.1f} MB, {n_months:,} GETs')
    n_by = sum(1 for c in first if cmap[(c, 'bybit')])
    by_req = sum((CUT - max(t, F.ms(2023, 1))) // (8 * HOUR) // 200 + 1 for c, t in first.items() if cmap[(c, 'bybit')])
    print(f'   bybit: {n_by} overlapping coins -> ~{by_req:,} requests at 8h (x2 for 4h, x8 for 1h symbols)')
    kl = [json.loads(f.read_text())['bytes'] for f in (OUT / 'binance').glob('klines_*/*.json')
          if json.loads(f.read_text())]
    print(f'   binance 1h klines: avg {sum(kl) / len(kl):,.0f} B per symbol-month ({len(kl)} files)')

    print('\nHL rows with rate == premium == 0 (delisted, excluded):',
          sum(len(v) for v in INACTIVE.values()), 'hours over', len(INACTIVE), 'coins')
    const = Counter()
    for v, data in venues.items():
        for c in first:
            s = cmap[(c, v)]
            if not s or c in CORE:
                continue
            for w in wins:
                rs = [data[s][t][0] for t in data[s] if w <= t < w + F.WIN_H * HOUR]
                hl_live = any(h in hl.get(c, {}) for h in range(w + HOUR, w + F.WIN_H * HOUR + 1, 24 * HOUR))
                if len(rs) > 20 and len(set(rs)) == 1 and hl_live:
                    const[v] += 1
    print('paired non-core windows where the CEX rate is constant for the whole window (possibly inactive):', dict(const))
    print('\nholdout rows dropped at load (counts only):', dict(DROPPED))
    (OUT / 'analysis.json').write_text(json.dumps(R, default=str))


if __name__ == '__main__':
    main()
