"""H1 house-sleeve allocation: HLP, LLP and LIT statistics for the decision memo (ledger 000v).

Allocation arithmetic, not an alpha test. Stdlib only. Usage:

    python3 -I -S research/alpha/scouts/h1_analyze.py research/alpha/data/scout_h1/pull_YYYYMMDD
"""
import datetime as dt
import json
import math
import os
import statistics
import sys

DAY = 86_400


def day(ts):
    return dt.datetime.fromtimestamp(ts, dt.UTC).strftime('%Y-%m-%d')


def load(d, name):
    with open(os.path.join(d, name)) as f:
        return json.load(f)


def max_dd(index):
    peak, worst, frm, to, pk_t = -math.inf, 0.0, None, None, None
    for t, v in index:
        if v > peak:
            peak, pk_t = v, t
        dd = v / peak - 1
        if dd < worst:
            worst, frm, to = dd, pk_t, t
    return worst, frm and day(frm), to and day(to)


def stats(points, label):
    """points: [(ts, value_index)] at any spacing. Returns annualised figures from elapsed time."""
    rets = [(t1, v1 / v0 - 1, (t1 - t0) / DAY) for (t0, v0), (t1, v1) in zip(points, points[1:])]
    years = (points[-1][0] - points[0][0]) / DAY / 365.25
    total = points[-1][1] / points[0][1] - 1
    cagr = (1 + total) ** (1 / years) - 1
    mean_len = statistics.mean(r[2] for r in rets)
    per_year = 365.25 / mean_len
    sd = statistics.pstdev(r[1] for r in rets)
    vol = sd * math.sqrt(per_year)
    down = math.sqrt(sum(min(r[1], 0) ** 2 for r in rets) / len(rets)) * math.sqrt(per_year)
    dd, dd_from, dd_to = max_dd(points)
    ordered = sorted(rets, key=lambda r: r[1])
    log_total = math.log(1 + total)
    top = {k: sum(math.log(1 + r[1]) for r in ordered[-k:]) / log_total if log_total > 0 else None for k in (1, 3, 5)}
    ex_top3 = math.exp(log_total - sum(math.log(1 + r[1]) for r in ordered[-3:])) ** (1 / years) - 1
    return {
        'label': label, 'from': day(points[0][0]), 'to': day(points[-1][0]), 'years': round(years, 2),
        'n_periods': len(rets), 'period_days': round(mean_len, 2),
        'total': total, 'cagr': cagr, 'vol': vol, 'downside_dev': down,
        'sharpe0': cagr / vol if vol else None, 'sharpe_ex4': (cagr - 0.04) / vol if vol else None,
        'max_dd': dd, 'dd_from': dd_from, 'dd_to': dd_to,
        'worst': [(day(r[0]), r[1]) for r in ordered[:3]], 'best': [(day(r[0]), r[1]) for r in ordered[-3:]],
        'share_of_log_return_top': top, 'cagr_ex_top3_periods': ex_top3,
        'neg_period_share': sum(r[1] < 0 for r in rets) / len(rets),
    }


def window(points, start=None, end=None):
    return [p for p in points if (start is None or p[0] >= start) and (end is None or p[0] <= end)]


def ts(s):
    return int(dt.datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=dt.UTC).timestamp())


def worst_rolling(points, days):
    """Worst return over any `days`-day window on a daily index."""
    by_day = {t // DAY: v for t, v in points}  # daily keys: tolerant of the series' one-hour shifts
    worst = (0.0, None)
    for t, v in points:
        v0 = by_day.get(t // DAY - days)
        if v0:
            r = v / v0 - 1
            if r < worst[0]:
                worst = (r, day(t))
    return worst


def hlp_index(vd):
    """HLP has no share price. Chain period returns dPnL / AV(start) on the API grid (flows ignored
    inside a period; the grid is 7-14 days)."""
    p = dict(vd['portfolio'])['allTime']
    rows = [(int(t) // 1000, float(a), float(b)) for (t, a), (_, b) in zip(p['accountValueHistory'], p['pnlHistory'])]
    idx, out = 1.0, []
    for (t0, a0, p0), (t1, a1, p1) in zip(rows, rows[1:]):
        if t1 - t0 < DAY:
            continue
        if not out:
            out.append((t0, 1.0))
        if a0 > 0:
            idx *= 1 + (p1 - p0) / a0
        out.append((t1, idx))
    return out, rows


def align(daily, grid):
    """Value of a daily index at (or just before) each grid timestamp."""
    j, out = 0, []
    for t, _ in grid:
        while j + 1 < len(daily) and daily[j + 1][0] <= t:
            j += 1
        if daily[j][0] <= t and t - daily[j][0] <= 2 * DAY:
            out.append((t, daily[j][1]))
    return out


def corr(a, b):
    ma, mb = statistics.mean(a), statistics.mean(b)
    num = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    return num / math.sqrt(sum((x - ma) ** 2 for x in a) * sum((y - mb) ** 2 for y in b))


def joint(out, hlp, llp, start, tag):
    grid = window(hlp, ts(start))
    llp_g = align(llp, grid)
    common = sorted(set(t for t, _ in llp_g) & set(t for t, _ in grid))
    h = dict(grid)
    l = dict(llp_g)
    rh = [h[b] / h[a] - 1 for a, b in zip(common, common[1:])]
    rl = [l[b] / l[a] - 1 for a, b in zip(common, common[1:])]
    out[f'joint{tag}_n_periods'] = len(rh)
    out[f'joint{tag}_corr'] = corr(rh, rl)
    top = sorted(range(len(rh)), key=lambda i: rh[i])[-4:]
    out[f'joint{tag}_hlp_top4_periods'] = [(day(common[i + 1]), rh[i], rl[i]) for i in top]
    for w_hlp in (1.0, 0.5, 0.0):
        idx, pts = 1.0, [(common[0], 1.0)]
        for i, t in enumerate(common[1:]):
            idx *= 1 + w_hlp * rh[i] + (1 - w_hlp) * rl[i]
            pts.append((t, idx))
        out[f'basket{tag}_hlp{int(w_hlp * 100)}'] = stats(pts, f'{int(w_hlp * 100)}% HLP / {int(100 - w_hlp * 100)}% LLP, grid rebalanced')


def kelly(r, p, loss):
    """Two-state year: +r w.p. 1-p, -loss w.p. p. Maximise E log(1 + f x)."""
    lo, hi = 0.0, 50.0
    g = lambda f: (1 - p) * r / (1 + f * r) - p * loss / (1 - f * loss)
    if g(0) <= 0:
        return 0.0
    hi = min(hi, 0.999 / loss)
    for _ in range(200):
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if g(mid) > 0 else (lo, mid)
    return lo


def main(d):
    out = {}
    acct = load(d, 'llp_account.json')['accounts'][0]
    pool = acct['pool_info']
    llp = [(int(r['timestamp']), float(r['share_price'])) for r in pool['share_prices']]
    out['llp_meta'] = {'tvl_total_asset_value': float(acct['total_asset_value']), 'total_shares': pool['total_shares'],
                       'api_apy_field': pool['annual_percentage_yield'], 'api_sharpe_field': pool['sharpe_ratio'],
                       'n_strategies': len(pool['strategies']),
                       'strategy_collateral': [float(s['collateral']) for s in pool['strategies']]}
    out['llp_full'] = stats(llp, 'LLP daily, full history (beta era included)')
    for lab, s, e in [('LLP 2025 (beta/early public)', '2025-01-01', '2025-12-31'),
                      ('LLP since LIT gate 2026-01-14', '2026-01-14', None),
                      ('LLP last 6 months', '2026-04-14', None),
                      ('LLP last 3 months', '2026-07-14', None),
                      ('LLP 2026-07-01 -> (A1 unread span)', '2026-07-01', None)]:
        out[lab] = stats(window(llp, ts(s), e and ts(e) + DAY), lab)
    out['llp_worst_7d'] = worst_rolling(llp, 7)
    out['llp_worst_30d'] = worst_rolling(llp, 30)
    out['llp_2025_10_window'] = [(day(t), v) for t, v in window(llp, ts('2025-10-07'), ts('2025-10-16'))]

    vd = load(d, 'hlp_vaultDetails.json')
    hlp, raw = hlp_index(vd)
    out['hlp_tvl_now'] = raw[-1][1]
    out['hlp_full'] = stats(window(hlp, ts('2023-07-01')), 'HLP API grid, 2023-07 ->')
    for lab, s in [('HLP since 2024-10-14 (2y)', '2024-10-14'), ('HLP since 2025-01-16 (LLP overlap)', '2025-01-16'),
                   ('HLP since 2026-01-14', '2026-01-14'), ('HLP since 2026-04-14', '2026-04-14')]:
        out[lab] = stats(window(hlp, ts(s)), lab)
    yearly = {}
    for y in range(2023, 2027):
        w = window(hlp, ts(f'{y}-01-01') - 15 * DAY, ts(f'{y}-12-31'))
        w = [p for p in w if p[0] >= ts(f'{y}-01-01') - 15 * DAY]
        if len(w) > 2:
            yearly[y] = w[-1][1] / w[0][1] - 1
    out['hlp_by_year'] = yearly
    yearly = {}
    for y in (2025, 2026):
        w = window(llp, ts(f'{y}-01-01') - DAY, ts(f'{y}-12-31'))
        yearly[y] = w[-1][1] / w[0][1] - 1
    out['llp_by_year'] = yearly

    for tag, start in (('', '2025-01-20'), ('_since_gate', '2026-01-14')):
        joint(out, hlp, llp, start, tag)

    # LIT
    c = load(d, 'hl_LIT_candles_1d.json')
    lit = [(int(x['t']) // 1000, float(x['c'])) for x in c]
    out['lit'] = stats(lit, 'LIT perp close (HL), daily')
    out['lit_last'] = lit[-1][1]
    out['lit_worst_30d'] = worst_rolling(lit, 30)
    f = load(d, 'hl_LIT_funding.json')
    fr = [(int(x['time']) // 1000, float(x['fundingRate'])) for x in f]
    now = fr[-1][0]
    for lab, span in (('all', None), ('90d', 90), ('30d', 30)):
        sel = [r for t, r in fr if span is None or t >= now - span * DAY]
        hours = len(sel)
        out[f'lit_funding_{lab}'] = {'hours': hours, 'annualised_paid_by_longs': sum(sel) / hours * 24 * 365,
                                     'share_hours_positive': sum(r > 0 for r in sel) / hours}

    # Hedge-venue funding (the hedge sits on Binance or Bitget, not on HL)
    cex = load(d, 'cex_LIT_funding.json')
    for venue, rows, tkey in (('binance', cex['binance'], 'fundingTime'), ('bitget', cex['bitget'], 'fundingTime')):
        pts = sorted((int(x[tkey]) // 1000, float(x['fundingRate'])) for x in rows)
        end = pts[-1][0]
        for lab, span in (('all', None), ('90d', 90), ('30d', 30)):
            sel = [(t, r) for t, r in pts if span is None or t >= end - span * DAY]
            days = (sel[-1][0] - sel[0][0]) / DAY
            out[f'lit_funding_{venue}_{lab}'] = {'from': day(sel[0][0]), 'rows': len(sel),
                                                 'annualised_paid_by_longs': sum(r for _, r in sel) / days * 365}

    # LLP P&L split: perp/backstop book vs the spot book that started 2026-08 (/pnl, cumulative fields)
    pnl = sorted(load(d, 'llp_pnl_1d.json')['pnl'], key=lambda r: r['timestamp'])
    nav = lambda r: r['inflow'] - r['outflow'] + r['trade_pnl'] + r['trade_spot_pnl']
    for s_, e_ in (('2026-01-14', None), ('2026-01-14', '2026-07-14'), ('2026-07-14', None),
                   ('2026-08-13', '2026-09-05'), ('2026-09-05', None)):
        w = [r for r in pnl if r['timestamp'] >= ts(s_) and (e_ is None or r['timestamp'] <= ts(e_) + DAY)]
        yrs = (w[-1]['timestamp'] - w[0]['timestamp']) / DAY / 365.25
        avg = statistics.mean(nav(r) for r in w)  # simple average capital (dollar-weighted, not share price)
        perp = w[-1]['trade_pnl'] - w[0]['trade_pnl']
        spot = w[-1]['trade_spot_pnl'] - w[0]['trade_spot_pnl']
        out[f'llp_split_{s_}_{e_ or "now"}'] = {'perp_usd': perp, 'spot_usd': spot, 'avg_nav': avg,
                                                'perp_ann': perp / avg / yrs, 'spot_ann': spot / avg / yrs}
    out['llp_nav_now'] = nav(pnl[-1])

    # LLP leg economics per $1 of USDC in LLP (memo §4): stake 1 LIT per 10 USDC, short hedge off-venue
    price = out['lit_last']
    stake = 0.1 * price
    legs = {}
    for carry in (0.035, 0.06, 0.10):
        for funding in (0.0, 0.07, 0.10):
            for margin in (1.0, 2.0):
                capital = 1 + stake + margin * stake
                ret = carry + 0.06 * stake + funding * stake - 0.003  # 6% LIT staking; ~0.3%/yr trading costs
                legs[f'carry{carry}_fund{funding}_margin{margin}x'] = {'capital': capital, 'return_on_capital': ret / capital}
    out['llp_leg'] = {'lit_price': price, 'stake_per_usdc': stake, 'cases': legs}

    # Kelly arithmetic (A1 §2.3 model) on EXCESS return over cash rf; loss leg is -L (cash would have earned rf)
    out['kelly_excess'] = {f'r{r}_rf{rf}_p{p}_L{L}': kelly(r - rf, p, L + rf) if r > rf else 0.0
                           for r in (0.021, 0.05, 0.065, 0.08, 0.10) for rf in (0.02, 0.03)
                           for p in (0.03, 0.05, 0.10) for L in (0.5, 0.7)}
    out['kelly'] = {f'r{r}_p{p}_L{L}': kelly(r, p, L)
                    for r in (0.03, 0.06, 0.10, 0.15) for p in (0.05, 0.10) for L in (0.5, 0.7)}
    json.dump(out, sys.stdout, indent=1, default=str)


if __name__ == '__main__':
    main(sys.argv[1])
