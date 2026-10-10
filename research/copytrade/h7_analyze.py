"""H7 (measurement, not a trial) — HLP as the packaged forced-flow benchmark.

Reads only: the h7 extracts (h7_extract.py fills), the cached public API
snapshots (h7_extract.py vaults / funding), DefiLlama TVL (h7_extract.py tvl),
and liquidations.parquet for the spiral hours. Prints the tables behind
docs/hlp-benchmark-2026-10-10.md and writes h7/summary.json (gitignored).

    python3 h7_analyze.py

P&L reconstruction, per child vault, per coin, per UTC hour h (marks = last
tape trade of the hour, forward-filled):
    pnl_h = position_{h-1} * (mark_h - mark_{h-1}) + sum_fills_h q * (mark_h - px)
i.e. mark-to-market of the inventory ("inv") plus the edge of each fill
against the hour's closing mark ("flow"). Fees are not in the archive; funding
comes from the API. The residual against the API's own pnlHistory is
reported, not hidden.
"""
import json
import math
import statistics
from datetime import datetime, timezone

import duckdb

from ct.net import DATA, cached
from h7_extract import HLP, OUT, funding

SNAPSHOT = '2026-10-10'  # day the vaultDetails / DefiLlama snapshots were cached
PARENT = '0xdfc24b077bc1425ad1dea75bcb6f8158e10df303'
GROUP = {'parent': 'parent', 'strat_a': 'mm', 'strat_b': 'mm', 'strat_x': 'strat_x',
         'liq_1': 'backstop', 'liq_2': 'backstop', 'liq_3': 'backstop', 'liq_4': 'backstop'}
LEGS = ('mm', 'backstop', 'strat_x', 'parent', 'funding')
MAJORS = ('BTC', 'ETH', 'SOL')  # as in l3/l5/l6
H, DAY = 3_600_000, 86_400_000
START = int(datetime(2025, 7, 28, tzinfo=timezone.utc).timestamp() * 1000)
END = int(datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp() * 1000)
STRESS_WINDOW_S, STRESS_THRESHOLD = 300, 77_000_000.0  # 004 D8
BRIEF_DAYS = ('2025-08-01', '2025-08-18', '2025-08-25', '2025-10-10', '2026-02-05')  # 004 §3, horizons H5
FILLS = f"read_parquet('{OUT}/fills/*.parquet')"
MARKS = f"read_parquet('{OUT}/marks/*.parquet')"
LIQ = DATA / 'derived' / 'hyperliquid' / 'liquidations.parquet'


def missing(what):
    def fail():
        raise RuntimeError(f'not cached: run h7_extract.py {what}')
    return fail


def vault(addr):
    return cached('hl-hlp-vaults', f'{SNAPSHOT}:{addr}', missing('vaults'))


def day_str(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y-%m-%d')


def ms(day):
    return int(datetime.strptime(day, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)


def series(addr, window='allTime'):
    """[(t, account value, cumulative pnl)] as served by vaultDetails."""
    p = dict(vault(addr)['portfolio'])[window]
    return [(int(t), float(a), float(b)) for (t, a), (_, b) in zip(p['accountValueHistory'], p['pnlHistory'])]


def drawdown(rets):
    """(max drawdown, index of the trough, index of the peak before it) of a compounded return list."""
    level, peak, peak_i, worst = 1.0, 1.0, 0, (0.0, 0, 0)
    for i, r in enumerate(rets):
        level *= 1 + r
        if level > peak:
            peak, peak_i = level, i
        if level / peak - 1 < worst[0]:
            worst = (level / peak - 1, i, peak_i)
    return worst


def compound(rets):
    return math.prod(1 + r for r in rets) - 1


def sharpe(rets, per_year):
    return statistics.mean(rets) / statistics.stdev(rets) * math.sqrt(per_year)


# ---------------------------------------------------------------- A: API series

def api_section(out):
    s = series(PARENT)[1:]  # the first point is the creation (account value 0)
    iv = [{'t': b[0], 'day': day_str(b[0]), 'av': b[1], 'dpnl': b[2] - a[2], 'ret': (b[2] - a[2]) / a[1]}
          for a, b in zip(s, s[1:])]
    dd, lo, hi = drawdown([x['ret'] for x in iv])
    spacing = statistics.median((b['t'] - a['t']) / DAY for a, b in zip(iv, iv[1:]))
    print('\n== A. API allTime series (parent) ==')
    print(f'{len(iv)} intervals, median spacing {spacing:.1f} d; max drawdown of the compounded interval series '
          f'{dd:.2%} ({iv[hi]["day"]} -> {iv[lo]["day"]})')
    years = {}
    for y in range(2023, 2027):
        sub = [x for x in iv if x['day'].startswith(str(y))]
        years[y] = {'ret': compound([x['ret'] for x in sub]), 'pnl': sum(x['dpnl'] for x in sub),
                    'mean_av': statistics.mean(x['av'] for x in sub), 'n': len(sub)}
        v = years[y]
        print(f"  {y}: return {v['ret']:+.2%}  pnl {v['pnl'] / 1e6:+.1f} M  mean AV {v['mean_av'] / 1e6:.0f} M  ({v['n']} intervals)")
    print('  intervals from 2025-06 on:')
    for x in iv:
        if x['day'] >= '2025-06':
            print(f"    -> {x['day']}  AV {x['av'] / 1e6:7.1f} M  dPnL {x['dpnl'] / 1e6:+7.2f} M  ret {x['ret']:+.3%}")
    rets = [x['ret'] for x in iv]
    span_years = (iv[-1]['t'] - iv[0]['t']) / DAY / 365
    p = vault(PARENT)
    out['api'] = {'years': years, 'max_dd': dd, 'dd_from': iv[hi]['day'], 'dd_to': iv[lo]['day'],
                  'spacing_days': spacing, 'intervals': iv,
                  'cagr_all': (1 + compound(rets)) ** (1 / span_years) - 1,
                  'sharpe_intervals': sharpe(rets, 365 / spacing),
                  'apr_field': p['apr'], 'leader_fraction': p['leaderFraction'],
                  'leader_commission': p['leaderCommission'], 'max_distributable': p['maxDistributable'],
                  'av_now': s[-1][1]}
    m = series(PARENT, 'month')
    out['api']['last_month'] = {'from': day_str(m[0][0]), 'to': day_str(m[-1][0]), 'pnl': m[-1][2], 'av0': m[0][1]}
    print(f"  CAGR since 2023-05 {out['api']['cagr_all']:+.2%}; interval Sharpe {out['api']['sharpe_intervals']:.2f}; "
          f"last 30 d pnl {m[-1][2] / 1e6:+.2f} M on {m[0][1] / 1e6:.0f} M; apr field {p['apr']:.2%}")
    out['children'] = {}
    for addr, child in HLP.items():
        c, v = series(addr), vault(addr)
        perp = dict(v['portfolio'])['perpAllTime']['pnlHistory']
        out['children'][child] = {'name': v['name'], 'av': c[-1][1], 'pnl_all': c[-1][2], 'created': day_str(c[0][0]),
                                  'pnl_perp_all': float(perp[-1][1]) if perp else 0.0, 'desc': v['description']}
        print(f"  child {child:8s} {v['name']:30s} AV {c[-1][1] / 1e6:7.1f} M  pnl(allTime) {c[-1][2] / 1e6:+7.2f} M  "
              f"since {day_str(c[0][0])}")


# ---------------------------------------------------- B: hourly reconstruction

def reconstruct(con):
    """Table `hourly` (child, grp, h, pnl, inv, flow, majors, exposure); h = hour END in epoch ms."""
    con.execute(f"""
      CREATE OR REPLACE TABLE fh AS
      SELECT child, coin, epoch_ms(date_trunc('hour', timestamp)) + {H} AS h,
             sum(CASE side WHEN 'buy' THEN size ELSE -size END) AS q,
             sum(CASE side WHEN 'buy' THEN size ELSE -size END * price) AS qp,
             last(start_position + CASE side WHEN 'buy' THEN size ELSE -size END ORDER BY timestamp, trade_id) AS pos_end,
             first(start_position ORDER BY timestamp, trade_id) AS pos_first
      FROM {FILLS} GROUP BY ALL""")
    con.execute(f"""
      CREATE OR REPLACE TABLE mh AS
      SELECT coin, epoch_ms(date_trunc('hour', ts)) + {H} AS h, arg_max(px, ts) AS m FROM {MARKS} GROUP BY ALL""")
    con.execute(f"""
      CREATE OR REPLACE TABLE hourly AS
      WITH hours AS (SELECT range AS h FROM range({START + H}, {END + H}, {H})),
      pairs AS (SELECT child, coin, first(pos_first ORDER BY h) AS pos0 FROM fh GROUP BY ALL),
      coins AS (SELECT DISTINCT coin FROM pairs),
      marks AS (
        SELECT c.coin, hours.h, last_value(mh.m IGNORE NULLS) OVER (PARTITION BY c.coin ORDER BY hours.h) AS m
        FROM coins c CROSS JOIN hours LEFT JOIN mh ON mh.coin = c.coin AND mh.h = hours.h),
      grid AS (
        SELECT p.child, p.coin, hours.h, p.pos0, coalesce(fh.q, 0) AS q, coalesce(fh.qp, 0) AS qp,
               coalesce(last_value(fh.pos_end IGNORE NULLS) OVER w, p.pos0) AS pos
        FROM pairs p CROSS JOIN hours LEFT JOIN fh ON fh.child = p.child AND fh.coin = p.coin AND fh.h = hours.h
        WINDOW w AS (PARTITION BY p.child, p.coin ORDER BY hours.h)),
      step AS (
        SELECT g.child, g.coin, g.h, g.q, g.qp, m.m,
               lag(g.pos, 1, g.pos0) OVER w AS prev,
               coalesce(m.m - lag(m.m) OVER w, 0) AS dm
        FROM grid g JOIN marks m ON m.coin = g.coin AND m.h = g.h
        WINDOW w AS (PARTITION BY g.child, g.coin ORDER BY g.h))
      SELECT child, h,
             sum(prev * dm) AS inv,
             sum(CASE WHEN q <> 0 THEN q * m - qp ELSE 0 END) AS flow,
             sum(prev * dm + CASE WHEN q <> 0 THEN q * m - qp ELSE 0 END) AS pnl,
             sum(CASE WHEN coin IN {MAJORS} THEN prev * dm + CASE WHEN q <> 0 THEN q * m - qp ELSE 0 END ELSE 0 END) AS majors,
             sum(abs(prev) * coalesce(m, 0)) AS exposure
      FROM step GROUP BY ALL""")
    con.execute('CREATE OR REPLACE TABLE grp(child VARCHAR, g VARCHAR)')
    con.executemany('INSERT INTO grp VALUES (?, ?)', list(GROUP.items()))
    con.execute('CREATE OR REPLACE TABLE hourly AS SELECT hourly.*, grp.g AS grp FROM hourly JOIN grp USING (child)')
    con.execute(f"COPY hourly TO '{OUT / 'hourly_pnl.parquet'}' (FORMAT parquet)")


def load_funding(con):
    rows = [(child, r['time'] // DAY * DAY, r['delta']['coin'], float(r['delta']['usdc']))
            for child, fr in funding(verbose=False).items() for r in fr]
    con.execute('CREATE OR REPLACE TABLE fund(child VARCHAR, day BIGINT, coin VARCHAR, usdc DOUBLE)')
    con.executemany('INSERT INTO fund VALUES (?, ?, ?, ?)', rows)


def reconcile(con, out):
    """Reconstructed vs API pnl over each API allTime interval inside the archive."""
    print('\n== B. Reconstruction vs the API (parent pnlHistory) ==')
    pts = [(t, p) for t, _, p in series(PARENT) if START <= t <= END]
    res = []
    for (a, pa), (b, pb) in zip(pts, pts[1:]):
        ha, hb = a // H * H, b // H * H
        recon = con.execute('SELECT coalesce(sum(pnl), 0) FROM hourly WHERE h > ? AND h <= ?', [ha, hb]).fetchone()[0]
        f = con.execute('SELECT coalesce(sum(usdc), 0) FROM fund WHERE day >= ? AND day < ?',
                        [ha // DAY * DAY, hb // DAY * DAY]).fetchone()[0]
        res.append({'from': day_str(a), 'to': day_str(b), 'api': pb - pa, 'recon': recon, 'funding': f,
                    'resid': pb - pa - recon - f})
        x = res[-1]
        print(f"  {x['from']} -> {x['to']}  API {x['api'] / 1e6:+7.2f} M  fills+marks {recon / 1e6:+7.2f} M  "
              f"funding {f / 1e6:+6.2f} M  residual {x['resid'] / 1e6:+6.2f} M")
    api_t, rec_t = [x['api'] for x in res], [x['recon'] + x['funding'] for x in res]
    corr = statistics.correlation(api_t, rec_t)
    print(f'  totals: API {sum(api_t) / 1e6:+.2f} M, reconstructed {sum(rec_t) / 1e6:+.2f} M, '
          f'residual {sum(x["resid"] for x in res) / 1e6:+.2f} M; correlation across {len(res)} intervals {corr:.3f}')
    out['reconcile'] = {'intervals': res, 'corr': corr, 'api_total': sum(api_t), 'recon_total': sum(rec_t),
                        'resid_total': sum(x['resid'] for x in res),
                        'resid_median_abs': statistics.median(abs(x['resid']) for x in res)}


def api_decomposition(out):
    """The API's own breakdown: each child's pnlHistory over the archive span, split into the intervals
    holding a spiral event (2025-10-10, 2026-01-31) and the rest. The parent's pnl not explained by the
    children is fee accrual / margin and other transfers booked at the parent."""
    events = (ms('2025-10-10'), ms('2026-01-31'))
    print('\n== B2. API decomposition by child over the archive span ($M; event = interval holding 2025-10-10 or 2026-01-31) ==')
    res = {}
    for addr, child in HLP.items():
        pts = [(t, p) for t, _, p in series(addr) if START - 14 * DAY <= t <= END + 14 * DAY]
        ev = rest = 0.0
        for (a, pa), (b, pb) in zip(pts, pts[1:]):
            if b <= START or a >= END:
                continue
            if any(a < e + DAY and b > e for e in events):
                ev += pb - pa
            else:
                rest += pb - pa
        res[child] = {'event': ev, 'rest': rest}
    kids = {k: v for k, v in res.items() if k != 'parent'}
    for leg in ('mm', 'backstop', 'strat_x'):
        e = sum(v['event'] for k, v in kids.items() if GROUP[k] == leg)
        r = sum(v['rest'] for k, v in kids.items() if GROUP[k] == leg)
        res[leg] = {'event': e, 'rest': r}
        print(f'  {leg:9s} event {e / 1e6:+7.2f}  rest {r / 1e6:+7.2f}  total {(e + r) / 1e6:+7.2f}')
    par = res['parent']
    e_k, r_k = sum(v['event'] for v in kids.values()), sum(v['rest'] for v in kids.values())
    res['parent_only'] = {'event': par['event'] - e_k, 'rest': par['rest'] - r_k}
    print(f"  children  event {e_k / 1e6:+7.2f}  rest {r_k / 1e6:+7.2f}")
    print(f"  parent    event {par['event'] / 1e6:+7.2f}  rest {par['rest'] / 1e6:+7.2f}  (unexplained by children: "
          f"{res['parent_only']['event'] / 1e6:+.2f} / {res['parent_only']['rest'] / 1e6:+.2f})")
    print('  (children are sampled on their own timestamps, so event/rest boundaries differ by up to a few days)')
    iv = [x for x in out['api']['intervals'] if START < x['t'] <= END + 14 * DAY]
    ev_iv = [x for x in iv if any(x['t'] - 14 * DAY < e + DAY and x['t'] > e for e in events)]
    rest_iv = [x for x in iv if x not in ev_iv]
    yrs = (iv[-1]['t'] - iv[0]['t'] + 14 * DAY) / DAY / 365
    a = {'span': [iv[0]['day'], iv[-1]['day']], 'compounded': compound([x['ret'] for x in iv]),
         'event_intervals': compound([x['ret'] for x in ev_iv]), 'rest_compounded': compound([x['ret'] for x in rest_iv]),
         'rest_annualised': (1 + compound([x['ret'] for x in rest_iv])) ** (1 / (yrs * len(rest_iv) / len(iv))) - 1,
         'years': yrs}
    print(f"  parent return over {a['span'][0]} -> {a['span'][1]} ({len(iv)} intervals): compounded {a['compounded']:+.2%}; "
          f"the 2 event intervals {a['event_intervals']:+.2%}; the other {len(rest_iv)} {a['rest_compounded']:+.2%} "
          f"(annualised {a['rest_annualised']:+.2%})")
    out['api_decomposition'], out['api_archive_return'] = res, a


# ------------------------------------------------- C: daily series, decomposition

def daily_section(con, out):
    tvl = cached('defillama-hlp', SNAPSHOT, missing('tvl'))['chainTvls']['Hyperliquid L1']['tvl']
    tvl = sorted((int(r['date']) * 1000 // DAY * DAY, float(r['totalLiquidityUSD'])) for r in tvl)
    con.execute('CREATE OR REPLACE TABLE tvl(day BIGINT, tvl DOUBLE)')
    con.executemany('INSERT INTO tvl VALUES (?, ?)', tvl)
    rows = con.execute(f"""
      WITH d AS (SELECT (h - 1) // {DAY} * {DAY} AS day, grp, sum(pnl) AS pnl FROM hourly GROUP BY ALL
                 UNION ALL SELECT day, 'funding', sum(usdc) FROM fund GROUP BY ALL),
      p AS (PIVOT d ON grp IN {LEGS} USING sum(pnl) GROUP BY day)
      SELECT p.*, (SELECT tvl FROM tvl WHERE tvl.day < p.day ORDER BY tvl.day DESC LIMIT 1) AS tvl
      FROM p WHERE day >= {START} AND day < {END} ORDER BY day""").fetchall()
    days = [{'day': day_str(r[0]), **{k: (v or 0.0) for k, v in zip(LEGS, r[1:6])}, 'tvl': r[6]} for r in rows]
    resid = {}  # each API interval's residual spread evenly over its days
    for x in out['reconcile']['intervals']:
        span = [day_str(t) for t in range(ms(x['from']), ms(x['to']), DAY)]
        for dd_ in span:
            resid[dd_] = x['resid'] / len(span)
    for d in days:
        d['total'] = sum(d[k] for k in LEGS)
        d['ret'] = d['total'] / d['tvl']
        d['resid'] = resid.get(d['day'], 0.0)
        d['ret_adj'] = (d['total'] + d['resid']) / d['tvl']
    ra = [d['ret_adj'] for d in days]
    dda, loa, hia = drawdown(ra)
    exa = [d['ret_adj'] for d in days if d['day'] not in out['spiral_day_set']]
    r = [d['ret'] for d in days]
    dd, lo, hi = drawdown(r)
    comp = compound(r)
    ann = (1 + comp) ** (365 / len(r)) - 1
    tot = sorted((d['total'] for d in days), reverse=True)
    year_total = sum(tot)
    share = {k: sum(tot[:k]) / year_total for k in (1, 3, 5, 10)}
    ex = [d['ret'] for d in days if d['day'] not in out['spiral_day_set']]
    ex5 = [d['ret'] for d in days if d['day'] not in BRIEF_DAYS + ('2026-01-31',)]
    worst, best = min(days, key=lambda d: d['ret']), max(days, key=lambda d: d['ret'])
    print('\n== C. Daily series (reconstructed pnl incl. funding / DefiLlama TVL of the prior day) ==')
    print(f"  {len(r)} days {days[0]['day']} -> {days[-1]['day']}: compounded {comp:+.2%}, annualised {ann:+.2%}, "
          f"daily vol {statistics.stdev(r):.3%}, ann. vol {statistics.stdev(r) * math.sqrt(365):.2%}, "
          f"Sharpe {sharpe(r, 365):.2f}")
    print(f"  max drawdown {dd:.2%} ({days[hi]['day']} -> {days[lo]['day']}); worst day {worst['ret']:+.3%} "
          f"({worst['day']}), best day {best['ret']:+.3%} ({best['day']})")
    print("  share of the year's pnl in the top 1/3/5/10 days: " + ', '.join(f'{v:.0%}' for v in share.values()))
    print(f"  without the {len(out['spiral_day_set'])} days that had a spiral hour: compounded {compound(ex):+.2%}, "
          f'Sharpe {sharpe(ex, 365):.2f}; without the brief\'s five days and 2026-01-31: compounded {compound(ex5):+.2%}, '
          f'Sharpe {sharpe(ex5, 365):.2f}')
    worst_a = min(days, key=lambda d: d['ret_adj'])
    print(f"  residual-adjusted (API residual spread evenly over each interval's days; covers "
          f"{sum(1 for d in days if d['day'] in resid)} of {len(days)} days): compounded {compound(ra):+.2%}, "
          f"ann. vol {statistics.stdev(ra) * math.sqrt(365):.2%}, Sharpe {sharpe(ra, 365):.2f}, max drawdown {dda:.2%} "
          f"({days[hia]['day']} -> {days[loa]['day']}), worst day {worst_a['ret_adj']:+.3%} ({worst_a['day']}); "
          f"without the spiral days: compounded {compound(exa):+.2%}, Sharpe {sharpe(exa, 365):.2f}")
    neg = [x for x in ra if x < 0]
    print(f'  downside: {len(neg)} negative days of {len(ra)}, downside deviation (ann.) '
          f'{math.sqrt(sum(x * x for x in neg) / len(ra)) * math.sqrt(365):.2%}, Sortino '
          f'{statistics.mean(ra) * 365 / (math.sqrt(sum(x * x for x in neg) / len(ra)) * math.sqrt(365)):.2f}')
    print('  worst / best days ($M):')
    srt = sorted(days, key=lambda d: d['total'])
    for d in srt[:6] + srt[-8:]:
        print(f"    {d['day']}  " + '  '.join(f'{k} {d[k] / 1e6:+7.2f}' for k in LEGS)
              + f"  total {d['total'] / 1e6:+7.2f}  ret {d['ret']:+.3%}")
    months = {}
    for d in days:
        months.setdefault(d['day'][:7], []).append(d)
    print('  monthly ($M; return compounded daily):')
    mon = []
    for k, ds in months.items():
        m = {'month': k, **{leg: sum(d[leg] for d in ds) for leg in LEGS + ('total',)},
             'tvl': statistics.mean(d['tvl'] for d in ds), 'ret': compound([d['ret'] for d in ds])}
        mon.append(m)
        print(f'    {k}  ' + '  '.join(f'{leg} {m[leg] / 1e6:+7.2f}' for leg in LEGS)
              + f"  total {m['total'] / 1e6:+7.2f}  TVL {m['tvl'] / 1e6:4.0f}  ret {m['ret']:+.2%}")
    year = {leg: sum(d[leg] for d in days) for leg in LEGS + ('total',)}
    print('  year by leg ($M): ' + ', '.join(f'{k} {v / 1e6:+.2f}' for k, v in year.items()))
    out['daily'] = {'n': len(r), 'from': days[0]['day'], 'to': days[-1]['day'], 'compounded': comp,
                    'annualised': ann, 'vol_daily': statistics.stdev(r), 'sharpe': sharpe(r, 365), 'max_dd': dd,
                    'dd_from': days[hi]['day'], 'dd_to': days[lo]['day'], 'worst': worst, 'best': best,
                    'top_share': share, 'ex_spiral_compounded': compound(ex), 'ex_spiral_sharpe': sharpe(ex, 365),
                    'ex_brief_compounded': compound(ex5), 'ex_brief_sharpe': sharpe(ex5, 365),
                    'adj': {'compounded': compound(ra), 'vol_ann': statistics.stdev(ra) * math.sqrt(365),
                            'sharpe': sharpe(ra, 365), 'max_dd': dda, 'dd_from': days[hia]['day'],
                            'dd_to': days[loa]['day'], 'worst': [worst_a['day'], worst_a['ret_adj']],
                            'ex_spiral_compounded': compound(exa), 'ex_spiral_sharpe': sharpe(exa, 365)},
                    'year_by_leg': year, 'monthly': mon, 'mean_tvl': statistics.mean(d['tvl'] for d in days),
                    'days': days}


# ------------------------------------------------------------- D: per-fill edge

def markouts(con, out):
    """Per-fill markouts of HLP's own fills, bucketed by minute; comparable to 003/004's per-fill returns
    but gross of fees (HLP's fee tier is not public; 003's T1 paid 1.5 bp maker + 4.5 bp taker + 5 bp)."""
    print('\n== D. Per-fill markouts of HLP fills (gross; mark = last trade of the minute +15 / +60) ==')
    con.execute(f"""
      CREATE OR REPLACE TABLE mo AS
      WITH b AS (
        SELECT CASE WHEN child LIKE 'liq%' THEN 'backstop' ELSE 'mm' END AS grp, coin,
               date_trunc('minute', timestamp) AS ts,
               coalesce(cp_is_liquidation, false) AS vs_forced, direction LIKE 'Liquidated%' AS takeover,
               crossed AS taker,
               sum(CASE side WHEN 'buy' THEN size ELSE -size END) AS q,
               sum(CASE side WHEN 'buy' THEN size ELSE -size END * price) AS qp,
               sum(size * price) AS gross, count(*) AS n
        FROM {FILLS} WHERE child <> 'parent' AND direction <> 'Net Child Vaults'
          AND coalesce(cp_direction, '') <> 'Auto-Deleveraging'
        GROUP BY ALL),
      m AS (SELECT coin, ts, px FROM {MARKS})
      SELECT b.*, strftime(b.ts, '%Y-%m-%d') IN {tuple(out['spiral_day_set'])} AS spiral, b.coin IN {MAJORS} AS major,
             b.q * m15.px - b.qp AS mo15, b.q * m60.px - b.qp AS mo60
      FROM b
      ASOF LEFT JOIN m m15 ON b.coin = m15.coin AND b.ts + INTERVAL 15 MINUTE >= m15.ts
      ASOF LEFT JOIN m m60 ON b.coin = m60.coin AND b.ts + INTERVAL 60 MINUTE >= m60.ts""")
    rows = con.execute("""
      SELECT grp, takeover, vs_forced, major, taker, sum(n), sum(gross),
             sum(mo15) / sum(gross) * 1e4, sum(mo60) / sum(gross) * 1e4, sum(mo15), sum(mo60),
             sum(mo15) FILTER (WHERE NOT spiral) / sum(gross) FILTER (WHERE NOT spiral) * 1e4,
             sum(mo15) FILTER (WHERE spiral), sum(n) FILTER (WHERE NOT spiral), sum(gross) FILTER (WHERE NOT spiral),
             sum(mo60) FILTER (WHERE NOT spiral) / sum(gross) FILTER (WHERE NOT spiral) * 1e4
      FROM mo GROUP BY ALL ORDER BY sum(gross) DESC""").fetchall()
    keys = ('grp', 'takeover', 'vs_forced', 'major', 'taker', 'fills', 'gross', 'bp15', 'bp60', 'usd15', 'usd60',
            'bp15_ex_spiral', 'usd15_spiral', 'fills_ex_spiral', 'gross_ex_spiral', 'bp60_ex_spiral')
    res = [dict(zip(keys, r)) for r in rows]
    for x in res:
        print(f"  {x['grp']:8s} takeover={x['takeover']!s:5s} vs_forced={x['vs_forced']!s:5s} "
              f"major={x['major']!s:5s} taker={x['taker']!s:5s} fills {x['fills']:>11,}  ${x['gross'] / 1e9:7.2f} B  "
              f"15m {x['bp15']:+6.1f} bp (ex-spiral {x['bp15_ex_spiral'] or 0:+6.1f})  60m {x['bp60']:+6.1f} bp  "
              f"${x['usd15'] / 1e6:+7.2f} M @15m (spiral days ${(x['usd15_spiral'] or 0) / 1e6:+6.2f} M); "
              f"ex-spiral {x['fills_ex_spiral'] or 0:,} fills ${(x['gross_ex_spiral'] or 0) / 1e6:.0f} M "
              f"60m {x['bp60_ex_spiral'] or 0:+.1f} bp")
    out['markouts'] = res


# ------------------------------------------------------------- E: spiral hours

def spiral_section(con, out):
    rate = con.execute(f"""
      WITH s AS (SELECT t // 1000 AS sec, sum(notional) AS n FROM '{LIQ}' GROUP BY 1),
      w AS (SELECT sec, sum(n) OVER (ORDER BY sec RANGE BETWEEN {STRESS_WINDOW_S - 1} PRECEDING AND CURRENT ROW) AS r5
            FROM s)
      SELECT (sec * 1000) // {H} * {H} + {H} AS h, max(r5) FROM w WHERE r5 >= {STRESS_THRESHOLD}
      GROUP BY 1 ORDER BY 1""").fetchall()
    print('\n== E. Spiral hours (market-wide trailing-5-min liquidations >= $77 M at any second; hour = its start, UTC) ==')
    rows = []
    for h, peak in rate:
        liq = con.execute(f"SELECT coalesce(sum(notional), 0) FROM '{LIQ}' WHERE t >= ? AND t < ?", [h - H, h]).fetchone()[0]
        g = dict(con.execute('SELECT grp, sum(pnl) FROM hourly WHERE h = ? GROUP BY 1', [h]).fetchall())
        mm_maj, expo = con.execute("SELECT sum(majors) FILTER (WHERE grp = 'mm'), sum(exposure) FROM hourly WHERE h = ?",
                                   [h]).fetchone()
        x = {'hour': datetime.fromtimestamp((h - H) / 1000, timezone.utc).strftime('%Y-%m-%d %H:00'), 'peak5': peak,
             'liq_hour': liq, 'mm': g.get('mm', 0.0), 'backstop': g.get('backstop', 0.0),
             'total': sum(g.values()), 'mm_majors': mm_maj or 0.0, 'exposure': expo or 0.0}
        rows.append(x)
        print(f"  {x['hour']}  peak 5-min ${peak / 1e6:5.0f} M  liq ${liq / 1e6:6.0f} M  HLP {x['total'] / 1e6:+7.2f} M "
              f"(mm {x['mm'] / 1e6:+6.2f}, backstop {x['backstop'] / 1e6:+6.2f}; mm majors {x['mm_majors'] / 1e6:+6.2f})  "
              f"exposure at hour start ${x['exposure'] / 1e6:5.0f} M")
    by_day = {}
    for x in rows:
        by_day.setdefault(x['hour'][:10], []).append(x)
    print('  by day: spiral hours / whole UTC day / the 24 h after the day ($M):')
    days = []
    for day in sorted(by_day):
        d0 = ms(day)
        whole = con.execute('SELECT sum(pnl) FROM hourly WHERE h > ? AND h <= ?', [d0, d0 + DAY]).fetchone()[0]
        after = con.execute('SELECT sum(pnl) FROM hourly WHERE h > ? AND h <= ?', [d0 + DAY, d0 + 2 * DAY]).fetchone()[0]
        legs = dict(con.execute('SELECT grp, sum(pnl) FROM hourly WHERE h > ? AND h <= ? GROUP BY 1', [d0, d0 + DAY]).fetchall())
        sp = sum(x['total'] for x in by_day[day])
        days.append({'day': day, 'hours': len(by_day[day]), 'spiral_hours_pnl': sp, 'day_pnl': whole,
                     'next_day_pnl': after, 'legs': legs})
        print(f"    {day}: {len(by_day[day]):2d} h  {sp / 1e6:+7.2f} / {whole / 1e6:+7.2f} / {after / 1e6:+7.2f}   "
              + ', '.join(f'{k} {v / 1e6:+.2f}' for k, v in sorted(legs.items())))
    print("  the brief's days and their neighbours, whole UTC day ($M; peak trailing-5-min liquidations):")
    brief = []
    for day in BRIEF_DAYS + ('2026-01-31', '2026-02-06'):
        d0 = ms(day)
        peak, liq = con.execute(f'''
          WITH s AS (SELECT t // 1000 AS sec, sum(notional) AS n FROM '{LIQ}' WHERE t >= {d0 - 300_000} AND t < {d0 + DAY}
                     GROUP BY 1),
          w AS (SELECT sec, n, sum(n) OVER (ORDER BY sec RANGE BETWEEN {STRESS_WINDOW_S - 1} PRECEDING AND CURRENT ROW) AS r5
                FROM s)
          SELECT max(r5) FILTER (WHERE sec >= {d0 // 1000}), sum(n) FILTER (WHERE sec >= {d0 // 1000}) FROM w''').fetchone()
        legs = dict(con.execute('SELECT grp, sum(pnl) FROM hourly WHERE h > ? AND h <= ? GROUP BY 1', [d0, d0 + DAY]).fetchall())
        worst_h = con.execute('SELECT h, sum(pnl) p FROM hourly WHERE h > ? AND h <= ? GROUP BY 1 ORDER BY 2 LIMIT 1',
                              [d0, d0 + DAY]).fetchone()
        best_h = con.execute('SELECT h, sum(pnl) p FROM hourly WHERE h > ? AND h <= ? GROUP BY 1 ORDER BY 2 DESC LIMIT 1',
                             [d0, d0 + DAY]).fetchone()
        b = {'day': day, 'peak5': peak, 'liq': liq, 'legs': legs, 'total': sum(legs.values()),
             'worst_hour': [datetime.fromtimestamp((worst_h[0] - H) / 1000, timezone.utc).strftime('%H:00'), worst_h[1]],
             'best_hour': [datetime.fromtimestamp((best_h[0] - H) / 1000, timezone.utc).strftime('%H:00'), best_h[1]]}
        brief.append(b)
        print(f"    {day}: peak ${peak / 1e6:5.0f} M, liq ${liq / 1e6:6.0f} M, HLP {b['total'] / 1e6:+7.2f} M ("
              + ', '.join(f'{k} {v / 1e6:+.2f}' for k, v in sorted(legs.items()))
              + f"); worst hour {b['worst_hour'][0]} {b['worst_hour'][1] / 1e6:+.2f}, best {b['best_hour'][0]} "
              f"{b['best_hour'][1] / 1e6:+.2f}")
    out['spiral_hours'], out['spiral_days'], out['brief_days'] = rows, days, brief
    out['spiral_day_set'] = sorted(by_day)


def adl_section(con, out):
    """How much of liquidations.parquet's 'Liquidated Cross' notional is HLP's own liquidator positions
    being auto-deleveraged (counterparty direction 'Auto-Deleveraging'), and when."""
    rows = con.execute(f"""
      SELECT strftime(timestamp, '%Y-%m-%d') AS day, child, count(*), sum(size * price)
      FROM {FILLS} WHERE cp_direction = 'Auto-Deleveraging' GROUP BY ALL ORDER BY 4 DESC""").fetchall()
    take = con.execute(f"""
      SELECT direction LIKE '%Cross%', count(*), sum(size * price) FROM {FILLS}
      WHERE child LIKE 'liq%' AND direction LIKE 'Liquidated%' AND cp_is_liquidation GROUP BY 1""").fetchall()
    print('\n== F. Backstop takeovers and HLP liquidator positions auto-deleveraged ==')
    for cross, n, notional in take:
        print(f"  takeovers ({'cross' if cross else 'isolated'}): {n:,} fills, ${notional / 1e9:.2f} B")
    for day, child, n, notional in rows[:10]:
        print(f'  ADL of HLP {child} on {day}: {n:,} fills, ${notional / 1e9:.3f} B')
    out['takeovers'] = [{'cross': c, 'fills': n, 'notional': v} for c, n, v in take]
    out['adl'] = [{'day': d, 'child': c, 'fills': n, 'notional': v} for d, c, n, v in rows]


def main():
    out = {}
    con = duckdb.connect()
    con.execute(f"SET temp_directory='{DATA / 'duckdb_tmp'}'")
    api_section(out)
    reconstruct(con)
    load_funding(con)
    reconcile(con, out)
    api_decomposition(out)
    spiral_section(con, out)
    daily_section(con, out)
    adl_section(con, out)
    markouts(con, out)
    (OUT / 'summary.json').write_text(json.dumps(out, indent=1, default=str))


if __name__ == '__main__':
    main()
