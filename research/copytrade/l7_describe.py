"""Registration 005 §3 — behaviour-only numbers: event counts through each filter (primary and the
reported variants), entry timing, missing entry prints, cluster counts, causality checks on the
event fields, costs, hedge betas (prices BEFORE each entry), the holdout pull size from local
discovery files, and power from UNCONDITIONAL price data (Bitget 1H closes at all hours of the discovery year, read from
the local cache only — never at or conditioned on event times, never signed by an event).

No exit price, no return after any entry. Log: data/scratch/l7.log.

    .venv/bin/python l7_describe.py
"""
import datetime as dt
import json
import math
import statistics

from ct import drift
from ct.leaders import con
from ct.net import DATA
from l7_run import LIQ, BG_SYMBOL, MAJORS, bg_1h_cached, build, counts, hedge_betas

LOG = DATA / 'scratch/l7.log'
H, D = drift.HOUR, drift.DAY
DISC_START = 1_753_660_800_000            # 2025-07-28 00:00 UTC
CUT = drift.HOLDOUT_START                 # 2026-07-01 00:00 UTC
HOUR_BLOCK = 200 * H
_out = []


def p(*a):
    s = ' '.join(str(x) for x in a)
    print(s, flush=True)
    _out.append(s)


def iso(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).strftime('%Y-%m-%d %H:%M')


def pct(xs, qs=(0.1, 0.25, 0.5, 0.75, 0.9, 0.99)):
    xs = sorted(xs)
    if not xs:
        return {}
    return {q: xs[min(len(xs) - 1, int(q * len(xs)))] for q in qs}


def causality(events):
    """Each event's run must be closed by the entry: no liquidation print in the coin within
    (t_end, t_end + G], and none within [t_start - G, t_start) (the run is maximal)."""
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE ce(k INTEGER, coin VARCHAR, t0 BIGINT, t1 BIGINT)')
    c.executemany('INSERT INTO ce VALUES (?, ?, ?, ?)', [(i, e['coin'], e['t_start'], e['t_end']) for i, e in enumerate(events)])
    G = drift.QUIET
    after, before, latency = c.execute(f"""
        SELECT COUNT(DISTINCT CASE WHEN l.t > ce.t1 AND l.t <= ce.t1 + {G} THEN ce.k END),
               COUNT(DISTINCT CASE WHEN l.t >= ce.t0 - {G} AND l.t < ce.t0 THEN ce.k END),
               COUNT(DISTINCT CASE WHEN l.t > ce.t1 + {G} AND l.t <= ce.t1 + {G + drift.LATENCY} THEN ce.k END)
        FROM ce JOIN read_parquet('{LIQ}') l ON l.coin = ce.coin
         AND l.t >= ce.t0 - {G} AND l.t <= ce.t1 + {G + drift.LATENCY}
         AND (l.t < ce.t0 OR l.t > ce.t1)""").fetchone()
    return after, before, latency



def bg_1h(symbol):
    """[(ts, close)] inside the discovery year from the local cache (l7_run.bg_1h_cached)."""
    return sorted((t, c) for t, c in bg_1h_cached(symbol, DISC_START).items() if DISC_START <= t < CUT)


def log_returns(series, step_h):
    """Non-overlapping close-to-close log returns over step_h hours, on the UTC grid."""
    px = dict(series)
    t0 = series[0][0] - series[0][0] % (step_h * H)
    out = {}
    t = t0
    while t + step_h * H <= series[-1][0]:
        a, b = px.get(t), px.get(t + step_h * H)
        if a and b:
            out[t] = math.log(b / a)
        t += step_h * H
    return out


def kurt(xs):
    m = statistics.fmean(xs)
    s2 = statistics.fmean((x - m) ** 2 for x in xs)
    return statistics.fmean((x - m) ** 4 for x in xs) / s2 ** 2


GRID = 5 * drift.MINUTE


def tape_grid(coins=('BTC', 'ETH', 'SOL')):
    """{coin: {5-min bucket index: last price in the bucket}} over every discovery day file,
    deterministic within a millisecond (file row order). Prices at all times, not conditioned on
    any event; only day files before 2026-07-01 exist locally and only those are listed."""
    root = DATA / 'pruned/by_dex/hyperliquid/fills/perp/all'
    days = sorted(d for d in root.iterdir() if d.name.startswith('date=') and d.name[5:] < '2026-07-01')
    out = {c: {} for c in coins}
    c = con()
    for n, d in enumerate(days, 1):
        f = d / 'fills.parquet'
        rows = c.execute(f"""SELECT coin, epoch_ms(timestamp) // {GRID},
                                    arg_max(CAST(price AS DOUBLE), (epoch_ms(timestamp), file_row_number))
                             FROM read_parquet('{f}', file_row_number = true)
                             WHERE coin IN {tuple(coins)!r} GROUP BY 1, 2""").fetchall()
        for coin, b, px in rows:
            out[coin][b] = px
        if n % 100 == 0:
            print(f'    tape grid: day {n}/{len(days)}', flush=True)
    return out


def rv(grid, k0, n=24):
    """Realised variance from the n five-minute returns ending at bucket k0 - 1 (prices known
    before bucket k0 starts). None if a bucket is missing."""
    px = [grid.get(k) for k in range(k0 - n - 1, k0)]
    if any(x is None for x in px):
        return None
    return sum(math.log(b / a) ** 2 for a, b in zip(px, px[1:]))


UNIV = (('all', lambda c: True), ('majors', lambda c: c in MAJORS), ('alts', lambda c: c not in MAJORS))


def filter_table(name, events, positions, skipped):
    p(f'[{name}]')
    for u, f in UNIV:
        n0 = sum(1 for e in events if f(e['coin']))
        sk = {k: sum(1 for i in v if f(events[i]['coin'])) for k, v in skipped.items()}
        p(f'  {u:<7} events {n0:>5} | warm-up {sk["warmup_fallback"]:>4} | holdout edge {sk["holdout_edge"]:>2}'
          f' | stress {sk["stress"]:>4} | no print 60 s {sk["no_entry_print"]:>2} | overlap {sk["overlap"]:>4}'
          f' | positions {sum(1 for x in positions if f(x["coin"])):>5}')


def main():
    events, plans, prints = build()
    positions, skipped = plans['primary']
    p(f'# l7_describe — registration 005 §3, behaviour only — {dt.datetime.now(dt.UTC).isoformat()}')
    p(f'events (cascades.json primary): {len(events):,}; t_end range {iso(min(e["t_end"] for e in events))} → '
      f'{iso(max(e["t_end"] for e in events))}')

    p('\n## causality of the event fields')
    after, before, lat = causality(events)
    p(f'events with a liquidation print in the coin within (t_end, t_end + 60 s]: {after} (must be 0)')
    p(f'events with a print within [t_start - 60 s, t_start): {before} (must be 0 — the run is maximal)')
    p(f'events where a NEW run starts during the 2 s latency (t_end + 60 s, + 62 s]: {lat}')
    late = sum(1 for e in events if e['t_end'] > drift.entry_time(e) - drift.QUIET)
    p(f'entry time before t_end + 60 s: {late} (must be 0)')
    fb = [e for e in events if e['fallback_median']]
    p(f'qualified on the cross-coin fallback median: {len(fb)} events, majors {sum(1 for e in fb if e["coin"] in MAJORS)} '
      f'(majors dates {sorted({iso(e["t_start"])[:10] for e in fb if e["coin"] in MAJORS})})')

    p('\n## counts through the filters (order: warm-up, holdout edge, stress, no print, overlap)')
    for name, (pos_, sk_) in plans.items():
        filter_table(name, events, pos_, sk_)
        p('  ', json.dumps(counts(events, pos_, sk_)['positions_by_side']))
    maj = [x for x in positions if x['major']]
    p('PRIMARY majors positions by coin:', {c: sum(1 for x in maj if x['coin'] == c) for c in sorted(MAJORS)})
    p('PRIMARY positions by side (L = longs liquidated -> buy):',
      {u: {s: sum(1 for x in xs if x['side'] == s) for s in 'LS'} for u, xs in (('all', positions), ('majors', maj))})
    mid = (DISC_START + CUT) // 2
    p(f'PRIMARY calendar halves (split {iso(mid)}):',
      {u: [sum(1 for x in xs if x['entry'] < mid), sum(1 for x in xs if x['entry'] >= mid)] for u, xs in (('all', positions), ('majors', maj))})
    srt = sorted(maj, key=lambda x: (x['entry'], x['coin']))
    if srt:
        h = len(srt) // 2
        p(f'PRIMARY count halves of majors (the criterion): {h} / {len(srt) - h}, split at {iso(srt[h]["entry"])}; '
          f'sides per half: {[{s: sum(1 for x in part if x["side"] == s) for s in "LS"} for part in (srt[:h], srt[h:])]}')
    st_maj = [events[i] for i in skipped['stress'] if events[i]['coin'] in MAJORS]
    p('PRIMARY stress-skipped days (all coins):', sorted({iso(events[i]['t_end'])[:10] for i in skipped['stress']}))
    p('PRIMARY stress-skipped majors:', len(st_maj), sorted({iso(e['t_end'])[:10] for e in st_maj}))
    inc = plans['inclusive_stress'][1]['stress']
    readm = [events[i] for i in set(plans['inclusive_stress'][1]['stress']) - set(skipped['stress'])]
    rm = [e for e in readm if e['coin'] in MAJORS]
    mj_notional = sorted(e['notional'] for e in events if e['coin'] in MAJORS and not e['fallback_median'])
    top_dec = mj_notional[int(0.9 * len(mj_notional))]
    p(f'own-coin exclusion re-admits {len(readm)} events ({len(rm)} majors); majors re-admitted median notional '
      f'${statistics.median(e["notional"] for e in rm) / 1e6:.1f} M vs all primary-set majors ${statistics.median(mj_notional) / 1e6:.1f} M; '
      f'{sum(1 for e in rm if e["notional"] >= top_dec)} of them in the top decile of majors cascade notional; '
      f'largest {sorted(round(e["notional"] / 1e6) for e in rm)[-5:]} $M' if rm else 'none re-admitted')
    p(f'inclusive stress series would skip (warm-up dropped): all {len(inc)}, majors {sum(1 for i in inc if events[i]["coin"] in MAJORS)}; '
      f'excluding own coin: all {len(skipped["stress"])}, majors {len(st_maj)}')

    p('\n## entry timing (PRIMARY: events past warm-up, holdout edge and stress)')
    gone = set(skipped['warmup_fallback']) | set(skipped['holdout_edge']) | set(skipped['stress'])
    live = [i for i in range(len(events)) if i not in gone]
    for u, f in UNIV:
        ids = [i for i in live if f(events[i]['coin'])]
        delays = [prints[i][0] - drift.entry_time(events[i]) for i in ids if i in prints]
        miss = {n: sum(1 for i in ids if i not in prints or prints[i][0] - drift.entry_time(events[i]) > n * 1000)
                for n in (1, 2, 5, 10, 30, 60)}
        p(f'{u:<7} n {len(ids):>5} | first print after entry time, ms: {pct(delays)} | max {max(delays) if delays else None}')
        p(f'        no print within N s: {miss}')
    since_end = [x['entry'] - x['t_end'] for x in maj]
    p('majors: entry fill − t_end, s:', {q: round(v / 1000, 3) for q, v in pct(since_end).items()})

    p('\n## clusters among PRIMARY majors entries')
    hours, days = {}, {}
    for x in maj:
        hours[x['hour']] = hours.get(x['hour'], 0) + 1
        days[x['day']] = days.get(x['day'], 0) + 1
    p(f'distinct hour clusters {len(hours)}, distinct day clusters {len(days)}; positions per hour max {max(hours.values())}, '
      f'per day max {max(days.values())}; days with >= 3 positions {sum(1 for v in days.values() if v >= 3)}')
    p('positions per day distribution:', {k: sum(1 for v in days.values() if v == k) for k in sorted(set(days.values()))})
    bycoin = sorted(maj, key=lambda x: (x['coin'], x['entry']))
    same_coin_240 = sum(1 for a, b in zip(bycoin, bycoin[1:]) if a['coin'] == b['coin'] and b['entry'] < a['entry'] + 240 * drift.MINUTE)
    p(f'majors positions whose 240-min window overlaps the next position in the same coin: {same_coin_240}')
    for u, xs in (('all', positions), ('alts', [x for x in positions if not x['major']])):
        p(f'{u}: distinct hour clusters {len({x["hour"] for x in xs})}, day clusters {len({x["day"] for x in xs})}')

    p('\n## costs (round trip, bp)')
    for s_ in drift.SLIPS:
        p(f'slippage {s_ * 1e4:.0f} bp each side: {drift.round_trip_cost(s_) * 1e4:.2f} bp (taker {drift.TAKER * 1e4:.1f} bp x 2)')

    p('\n## hedge betas (ETH/SOL on BTC, trailing 30 days of Bitget 1H closes before each PRIMARY entry)')
    betas = hedge_betas(maj)
    for c in ('ETH', 'SOL'):
        bs = [b for i, b in betas.items() if b is not None and events[i]['coin'] == c]
        p(f'{c}: n {len(bs)}, missing {sum(1 for i, b in betas.items() if b is None and events[i]["coin"] == c)}, '
          f'beta percentiles {{{", ".join(f"{q}: {v:.2f}" for q, v in pct(bs, (0.1, 0.5, 0.9)).items())}}}')

    p('\n## power — unconditional Bitget 1H closes, all hours of 2025-07-28 → 2026-06-30, cache only')
    series = {c: bg_1h(s_) for c, s_ in BG_SYMBOL.items()}
    sig, r120 = {}, {}
    for c, srs in series.items():
        p(f'{c}: {len(srs):,} hourly closes {iso(srs[0][0])} → {iso(srs[-1][0])}')
        for step, name in ((1, '60m'), (2, '120m'), (4, '240m')):
            v = list(log_returns(srs, step).values())
            sd = statistics.pstdev(v)
            mad = statistics.median(abs(x - statistics.median(v)) for x in v) * 1.4826
            if name == '120m':
                sig[c], r120[c] = sd, log_returns(srs, step)
            p(f'   {name}: n {len(v):,} sd {sd * 1e4:.1f} bp, robust sd (1.4826 MAD) {mad * 1e4:.1f} bp, kurtosis {kurt(v):.1f}')
    rho = {}
    cs = sorted(sig)
    for i, a in enumerate(cs):
        for b in cs[i + 1:]:
            ks = sorted(set(r120[a]) & set(r120[b]))
            rho[(a, b)] = statistics.correlation([r120[a][k] for k in ks], [r120[b][k] for k in ks])
    p('120-min return correlations:', {f'{a}-{b}': round(v, 3) for (a, b), v in rho.items()})
    se_disc = {}
    for name in plans:
        mj = [x for x in plans[name][0] if x['major']]
        G = len({x['hour'] for x in mj})
        sbar = math.sqrt(statistics.fmean(sig[x['coin']] ** 2 for x in mj))
        p(f'[{name}] majors n {len(mj)}, hour clusters {G}, day clusters {len({x["day"] for x in mj})}; rms sigma(120m) {sbar * 1e4:.1f} bp; '
          f'textbook (2.33 + 0.84) x sigma / sqrt(G) = {(2.33 + 0.8416) * sbar / math.sqrt(G) * 1e4:.1f} bp')
        for scale in (1.0, 1.5, 2.0):
            s_ = {c: v * scale for c, v in sig.items()}
            se_true, se_h, se_d = (drift.null_se(mj, s_, rho, key=k) for k in (None, 'hour', 'day'))
            se_disc[(name, scale)] = (se_true, se_h, se_d, len(mj))
            mh, md = drift.mde(se_h, se_true, drift.Z_HOUR), drift.mde(se_d, se_true, drift.Z_DAY)
            p(f'   sigma x {scale}: null SE true {se_true * 1e4:.1f} bp | hour {se_h * 1e4:.1f} | day {se_d * 1e4:.1f} || '
              f'MDE hour t 2.33: {mh["p50"] * 1e4:.1f} (50%) / {mh["p80"] * 1e4:.1f} bp (80%) | '
              f'day t 1.96: {md["p50"] * 1e4:.1f} / {md["p80"] * 1e4:.1f} bp | bar {drift.BAR * 1e4:.0f} bp')

    p('\n## pre-cascade realised volatility (120 min before t_start, 5-min HL tape returns; nothing at or after t_start)')
    grid = tape_grid()
    ratio = {}
    for c in sorted(BG_SYMBOL):
        g = grid[c]
        pre = [rv(g, x['t_start'] // GRID) for x in maj if x['coin'] == c]
        pre = [v for v in pre if v is not None]
        lo, hi = min(g), max(g)
        unc = [rv(g, k) for k in range(lo + 25, hi + 1, 24)]
        unc = [v for v in unc if v is not None]
        ratio[c] = math.sqrt(statistics.fmean(pre) / statistics.fmean(unc))
        p(f'{c}: pre-cascade windows {len(pre)}, unconditional 2-h windows {len(unc):,}; realised 2-h vol pre '
          f'{math.sqrt(statistics.fmean(pre)) * 1e4:.1f} bp vs unconditional {math.sqrt(statistics.fmean(unc)) * 1e4:.1f} bp '
          f'-> ratio {ratio[c]:.2f} (median-based {math.sqrt(statistics.median(pre) / statistics.median(unc)):.2f})')
    sig_pre = {c: sig[c] * ratio[c] for c in sig}
    se_pre = tuple(drift.null_se(maj, sig_pre, rho, key=k) for k in (None, 'hour', 'day'))
    mh_pre, md_pre = drift.mde(se_pre[1], se_pre[0], drift.Z_HOUR), drift.mde(se_pre[2], se_pre[0], drift.Z_DAY)
    p(f'pre-cascade-scaled sigma(120m): {{{", ".join(f"{c}: {v * 1e4:.0f} bp" for c, v in sig_pre.items())}}}; null SE true '
      f'{se_pre[0] * 1e4:.1f} bp | hour {se_pre[1] * 1e4:.1f} | day {se_pre[2] * 1e4:.1f} || MDE hour t 2.33: '
      f'{mh_pre["p50"] * 1e4:.1f} (50%) / {mh_pre["p80"] * 1e4:.1f} bp (80%) | day t 1.96: {md_pre["p50"] * 1e4:.1f} / {md_pre["p80"] * 1e4:.1f} bp')

    p('\n## holdout read (D14) — power from the same null SE scaled to the expected holdout n')
    disc_days = (CUT - DISC_START) / D - 4            # the four warm-up days carry no primary event
    pull_day = dt.datetime(2026, 10, 9, tzinfo=dt.UTC)
    hold_days = (pull_day.timestamp() * 1000 + D - CUT) / D
    for scale in (1.0, 1.5):
        se_true, se_h, se_d, n = se_disc[('primary', scale)]
        n_h = n * hold_days / disc_days
        k = math.sqrt(n / n_h)
        th, hh = se_true * k, se_h * k
        lowest_pass = drift.Z_HOUR * se_h                 # smallest discovery mean that can pass criterion 1
        p(f'sigma x {scale}: holdout {hold_days:.0f} days (2026-07-01 → 2026-10-09) → expected majors n {n_h:.0f}; '
          f'null SE {th * 1e4:.1f} bp (hour {hh * 1e4:.1f}); confirm needs mean >= {drift.Z_HOLDOUT * hh * 1e4:.1f} bp')
        for mu, lab in ((drift.BAR, 'the 25 bp bar'), (lowest_pass, f'lowest passing discovery mean ({lowest_pass * 1e4:.1f} bp)'),
                        (2 * drift.BAR, '50 bp'), (0.0, 'no effect (false confirm)')):
            p(f'   power of t >= 1.64 at {lab}: {drift.power_at(mu, hh, th, drift.Z_HOLDOUT):.2f}')
        p(f'   probability a true 25 bp effect shows a NEGATIVE holdout mean: {drift.norm_cdf(-drift.BAR / th):.2f}')

    p('\n## joint power by simulation (all six criteria; actual 212 entry times, sides, halves; Gaussian random walk)')
    rate = len(maj) / disc_days
    n_a = round(rate * hold_days)
    se_t, se_h1 = se_disc[('primary', 1.0)][0], se_disc[('primary', 1.0)][1]
    n_analytic = math.ceil(len(maj) * (se_t * (drift.Z_HOLDOUT * se_h1 / se_t + 0.8416) / drift.BAR) ** 2)
    p(f'rate {rate:.3f} majors positions / day; option A n {n_a}; analytic n for 80% at 25 bp (t criterion only): {n_analytic}')
    n_b = None
    for n in range(max(60, n_analytic - 24), min(len(maj), n_analytic + 40) + 1, 4):
        r = drift.simulate_power(maj, sig, rho, drift.BAR, sims=1, seed=n, holdout_blocks=[('B', n)], hold_sims=2000)
        pc = r['holdout']['B']['p_confirm']
        p(f'   holdout n {n}: P(confirm | 25 bp, unconditional vol) {pc:.3f}')
        if pc >= 0.8:
            n_b = n
            break
    end_b, days_b = drift.holdout_end_for(n_b, rate)
    p(f'OPTION B: n {n_b} -> {days_b} days -> fixed end date {iso(end_b)[:10]} (inclusive)')
    for vlab, sg in (('unconditional', sig), ('pre-cascade', sig_pre)):
        for mu in (0.0018, drift.BAR, 0.0037):
            r = drift.simulate_power(maj, sg, rho, mu, sims=2000, seed=7, holdout_blocks=[('A', n_a), ('B', n_b)], hold_sims=2000)
            pa, pb = r['holdout']['A']['p_confirm'], r['holdout']['B']['p_confirm']
            p(f'[{vlab} vol, true mean {mu * 1e4:.0f} bp] P(pass all six) {r["p_pass"]:.3f}; P(pass and mean >= 25 bp) '
              f'{r["p_pass_and_bar"]:.3f}; P(holdout confirm) A {pa:.3f} / B {pb:.3f}; '
              f'END-TO-END P(pass, >= bar, confirm) A {r["p_pass_and_bar"] * pa:.3f} / B {r["p_pass_and_bar"] * pb:.3f}')
    r0 = drift.simulate_power(maj, sig, rho, 0.0, sims=2000, seed=11, holdout_blocks=[('A', n_a), ('B', n_b)], hold_sims=2000)
    p(f'[no effect] P(pass all six) {r0["p_pass"]:.4f}; P(pass, >= bar) {r0["p_pass_and_bar"]:.4f}; '
      f'P(confirm) A {r0["holdout"]["A"]["p_confirm"]:.3f} / B {r0["holdout"]["B"]["p_confirm"]:.3f}')

    p('\n## holdout pull size — from the local DISCOVERY day files only (no listing, no footer of a holdout day)')
    root = DATA / 'pruned/by_dex/hyperliquid/fills/perp/all'
    sizes = sorted((d.name[5:], (d / 'fills.parquet').stat().st_size) for d in root.iterdir()
                   if d.name.startswith('date=') and d.name[5:] < '2026-07-01')
    tot = sum(b for _, b in sizes)
    last90 = statistics.fmean(b for _, b in sizes[-90:])
    GIB = 1 << 30
    p(f'main-dex pruned files on disk: {len(sizes)} days, {tot / GIB:.1f} GiB, mean {tot / len(sizes) / GIB:.3f} GiB/day, '
      f'last 90 days {last90 / GIB:.3f} GiB/day')
    p(f'P3 transfer / on-disk ratio (plan: ~53 GiB transferred, 43 GiB on disk, both dexes): {53 / 43:.2f}')
    p(f'estimate for {hold_days:.0f} holdout days, same 14 columns: {last90 * hold_days / GIB:.1f} GiB on disk, '
      f'~{last90 * hold_days * 53 / 43 / GIB:.0f} GiB transfer (exact figure needs a LIST + footer read: to be quoted)')
    LOG.write_text('\n'.join(_out) + '\n')
    p(f'\nlog: {LOG}')


if __name__ == '__main__':
    main()
