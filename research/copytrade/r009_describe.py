"""Registration 009 (L4) — §3 behaviour-only measurements and the power simulation (Revision 3: F1).

    .venv/bin/python -I r009_describe.py build      # zips -> data/binance_years/panel (verified zips only)
    .venv/bin/python -I r009_describe.py            # all §3 numbers -> stdout + data/binance_years/describe.json

BEHAVIOUR ONLY: no return is ever paired with a score. Computes features, their distributions, coverage and breadth, liquidity and spread
estimates, ties, signal rank autocorrelation, turnover and cost estimates from target weights and
the trailing week's (pre-S) drift, the ex-ante beta of the target book, and the cross-sectional
dispersion of CONTEMPORANEOUS (trailing, pre-signal) returns. It never calls `factors.outcomes`
and never reads a bar or settlement stamped at or after a signal for that signal. Revision 3
removed the leg counts near delisting cuts (a forward-exposed statistic, 000g). Never reads
data/carry/ (paths via r009_run.checked; carry-guard).
"""
import datetime as dt
import json
import math
import os
import re
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))                    # -I drops the script dir; the code dir is added explicitly

import r009_run  # noqa: E402
from ct import binance_years as by  # noqa: E402
from ct import factors as fx  # noqa: E402

DATA = HERE / 'data/binance_years'
PANEL = DATA / 'panel'
LISTING = HERE / 'data/scout_l4/listings'
OUT = DATA / 'describe.json'
FLOORS = (0, 5e6, 10e6, 20e6, 50e6)
POWER_ICS = (0.0, 0.02, 0.03, 0.05)
POWER_SD = ('sampling', 0.15, 0.20)
SIMS = int(os.environ.get('R009_SIMS', '2000'))
KEY = 'F1'


def q(xs, ps=(0.05, 0.25, 0.5, 0.75, 0.95)):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    return [xs[min(len(xs) - 1, int(p * len(xs)))] for p in ps]


def mean(xs):
    xs = [x for x in xs if x is not None]
    return statistics.fmean(xs) if xs else None


def pr(title, obj):
    print(f'\n## {title}\n' + json.dumps(obj, indent=1, default=lambda x: round(x, 6) if isinstance(x, float) else str(x)))


def main():
    con, rows, uni, merged = r009_run.load_universe()
    signals = fx.signal_weeks('discovery')
    out = {'bridged_metrics_gaps': merged}
    pr('metrics-archive holes bridged (coin kept trading)', merged)

    # --- coverage and the floor (no metrics needed) ---
    def ok_except_floor(r):
        return r['symbol'] not in fx.EXCLUDE and r['listed_ok'] and r['sigma'] is not None and r['adv30'] is not None
    by_S = {}
    for r in rows:
        by_S.setdefault(r['S'], []).append(r)
    cov = {'alive': [], 'listed_30d': [], 'listed_sigma': [], 'universe': [], 'universe_with_F1': []}
    for S in signals:
        rs = [r for r in by_S.get(S, []) if r['symbol'] not in fx.EXCLUDE]
        cov['alive'].append(len(rs))
        cov['listed_30d'].append(sum(1 for r in rs if r['listed_ok']))
        cov['listed_sigma'].append(sum(1 for r in rs if ok_except_floor(r)))
        cov['universe'].append(len(uni.get(S, [])))
        cov['universe_with_F1'].append(sum(1 for r in uni.get(S, []) if r['F1'] is not None))
    out['coverage_per_week'] = {k: {'mean': mean(v), 'min': min(v), 'max': max(v), 'coin_weeks': sum(v)} for k, v in cov.items()}
    out['universe_by_year_mean'] = {y: mean([n for S, n in zip(signals, cov['universe_with_F1']) if fx.year_of(S) == y])
                                    for y in (2021, 2022, 2023)}
    out['universe_symbols'] = len({r['symbol'] for v in uni.values() for r in v if r['F1'] is not None})
    out['breadth_by_floor'] = {f'{f / 1e6:g}M': mean([sum(1 for r in by_S.get(S, []) if ok_except_floor(r) and r['adv30'] >= f
                                                          and r['F1'] is not None) for S in signals]) for f in FLOORS}
    pr('coverage per signal week', out['coverage_per_week'])
    pr('universe with F1 by year; symbols', [out['universe_by_year_mean'], out['universe_symbols']])
    pr('breadth by floor (with F1)', out['breadth_by_floor'])

    # --- liquidity, spreads, tiers ---
    urows = [r for v in uni.values() for r in v if r['F1'] is not None]
    elig = [r for r in rows if ok_except_floor(r)]
    out['adv30_quantiles_by_year'] = {y: q([r['adv30'] for r in elig if fx.year_of(r['S']) == y]) for y in (2022, 2023)}
    xi_now = json.load(open(r009_run.checked(HERE / 'data/scout_l4/exchangeinfo/fapi_exchangeInfo.json')))['symbols']
    tick = {x['symbol']: float(f['tickSize']) for x in xi_now for f in x.get('filters', []) if f.get('filterType') == 'PRICE_FILTER'}
    out['tiers'] = {}
    for lo, t, s in fx.TIERS:
        tr = [r for r in urows if r['tier'] == t]
        if not tr:
            continue
        ht = [1e4 * tick[r['symbol']] / r['c_last'] / 2 for r in tr if r['symbol'] in tick and r['c_last']]
        out['tiers'][t] = {'share': len(tr) / len(urows), 'slip_bp': 1e4 * s,
                           'ar_half_spread_p75_bp': 1e4 * q([r['ar_half_spread'] for r in tr], (0.75,))[0],
                           'half_tick_bp_p50_p90': q(ht, (0.5, 0.9)),
                           'stress_tier_share_moved_down': mean([fx.tier(r['adv_min90'])[0] != t for r in tr])}
    pr('tiers (registered slippage; AR p75 is NOT a bound; half-tick is a floor)', out['tiers'])

    # --- delisting (dates only; no crossing with the legs, Revision 3) ---
    out['scoring'] = {'universe_coin_weeks': sum(len(v) for v in uni.values()),
                      'scored_F1': sum(1 for v in uni.values() for r in v if r['F1'] is not None),
                      'unscored': sum(1 for v in uni.values() for r in v if r['F1'] is None),
                      'unscored_missing_settlement_in_window': sum(1 for v in uni.values() for r in v
                                                                    if r.get('F1_missing_settlement')),
                      'unscored_no_funding_rows': sum(1 for v in uni.values() for r in v if not r.get('fund_n'))}
    pr('scoring (m3, m5): F1 is required to be scored; the market and beta use the whole universe', out['scoring'])
    out['delisting'] = {'coin_weeks_cut_inside_their_week': sum(1 for r in urows if r['cut_end'] <= r['S'] + fx.WEEK + fx.ENTRY_LAG),
                        'symbols_with_relisting_segments_after_bridging': sorted(
                            s for s, v in fx.bridge_trading_gaps(con, json.loads((PANEL / 'life.json').read_text()))[0].items()
                            if len(v['segments']) > 1)}
    pr('delisting (dates only)', out['delisting'])

    # --- F1 feature, ties ---
    ann = 365 / 7
    path = fx.target_path(uni, KEY, True)
    ties_long = ties_short = 0
    max_leg = (0, None)
    for S, w in path:
        rs = [r for r in uni[S] if r[KEY] is not None and r['sigma']]
        n, k = len(rs), len(rs) // fx.QUANTILE
        srt = sorted(r[KEY] for r in rs)
        if k and srt.count(srt[-k]) > srt[-k:].count(srt[-k]):          # the long entry score recurs outside the top k
            ties_long += 1
        if k and srt.count(srt[k - 1]) > srt[:k].count(srt[k - 1]):     # the short entry score recurs outside the bottom k
            ties_short += 1
        nl = sum(1 for x in w.values() if x > 0)
        ns = sum(1 for x in w.values() if x < 0)
        if max(nl, ns) > max_leg[0]:
            max_leg = (max(nl, ns), dt.datetime.fromtimestamp(S / 1000, dt.UTC).strftime('%Y-%m-%d'), n)
    out['F1'] = {
        'fund7_annualised_pct_quantiles_by_year': {y: q([100 * ann * r['fund7'] for r in urows if fx.year_of(r['S']) == y])
                                                   for y in (2021, 2022, 2023)},
        'share_negative': mean([r['fund7'] < 0 for r in urows]),
        'share_exactly_default_0.0021': mean([round(r['fund7'], fx.FUND_DECIMALS) == 0.0021 for r in urows]),
        'weeks_entry_boundary_tied_long': ties_long, 'weeks_entry_boundary_tied_short': ties_short,
        'largest_leg_names_week_universe': max_leg,
        'legs_names_mean': [mean([sum(1 for x in w.values() if x > 0) for _, w in path if w]),
                            mean([sum(1 for x in w.values() if x < 0) for _, w in path if w])],
    }
    pr('F1 feature and ties', out['F1'])

    # --- persistence, turnover, costs (Revision 3: drift included) ---
    ra = fx.rank_autocorr(uni, KEY)
    cost = {}
    for name, kw in (('buffered_drift', dict(buffer=True, drift=True)), ('buffered_target_only', dict(buffer=True, drift=False)),
                     ('quintile_drift', dict(buffer=False, drift=True)),
                     ('buffered_drift_2x', dict(buffer=True, drift=True, slip_mult=2.0))):
        tv = fx.turnover_path(uni, KEY, **kw)[1:]
        cost[name] = {'turnover': mean([t for _, t, _ in tv]), 'cost_bp': 1e4 * mean([c for *_, c in tv]),
                      'cost_pct_yr': 100 * 52 * mean([c for *_, c in tv])}
    tv = fx.turnover_path(fx.with_stress_tiers(uni), KEY, buffer=True, drift=True)[1:]
    cost['buffered_drift_stress_tiers'] = {'cost_bp': 1e4 * mean([c for *_, c in tv])}
    out['persistence'] = {'rank_autocorr_1w_mean': mean([x for _, x in ra]), 'rank_autocorr_q': q([x for _, x in ra]),
                          'costs': cost}
    pr('persistence and costs (F1)', out['persistence'])

    # --- the target book: beta, tiers, trailing funding gap (all ex ante) ---
    bet, dollars, fgap = [], {}, []
    for S, w in path:
        if not w:
            continue
        rs = {r['symbol']: r for r in uni[S]}
        bet.append((S, sum(x * (rs[s]['beta'] if rs[s]['beta'] is not None else 1.0) for s, x in w.items())))
        for s, x in w.items():
            leg = 'long' if x > 0 else 'short'
            dollars.setdefault(leg, {}).setdefault(rs[s]['tier'], 0.0)
            dollars[leg][rs[s]['tier']] += abs(x)
        fgap.append(-sum(x * rs[s]['fund7'] for s, x in w.items()))
    for leg in dollars:
        tot = sum(dollars[leg].values())
        dollars[leg] = {t: v / tot for t, v in sorted(dollars[leg].items())}
    out['book_ex_ante'] = {'net_beta_mean': mean([b for _, b in bet]), 'net_beta_p10_p90': q([b for _, b in bet], (0.1, 0.9)),
                           'net_beta_by_year': {y: mean([b for S, b in bet if fx.year_of(S) == y]) for y in (2021, 2022, 2023)},
                           'dollar_share_by_tier': dollars,
                           'trailing_funding_gap_bp_per_week_mean': 1e4 * mean(fgap)}
    pr('target book, ex ante (beta to the equal-weight universe, $ by tier, trailing funding gap)', out['book_ex_ante'])

    # --- contemporaneous dispersion and market moves ---
    disp = {S: [r['ret7_trailing'] - (r['fund7'] or 0.0) for r in v if r['ret7_trailing'] is not None] for S, v in uni.items()}
    # Revision 4 (m12): the simulated market is PRICE only, as in the run (`factors.market_returns`)
    mkt = fx.market_moves({S: [r['ret7_trailing'] for r in v if r['ret7_trailing'] is not None] for S, v in uni.items()})
    out['dispersion'] = {'xs_sd_median_by_year': {y: statistics.median(statistics.pstdev(d) for S, d in disp.items()
                                                                      if fx.year_of(S) == y and len(d) > 2) for y in (2021, 2022, 2023)},
                         'market_move_weekly_mean': statistics.fmean(mkt), 'market_move_weekly_sd': statistics.pstdev(mkt)}
    pr('dispersion (trailing week, pre-S) and the empirical market moves', out['dispersion'])

    # --- holdout-A breadth from the listing ---
    full_life = by.life_from_listing(str(LISTING))
    xi = json.load(open(HERE / 'data/scout_l4/exchangeinfo/fapi_exchangeInfo.json'))['symbols']
    bad = {s['symbol'] for s in xi if s.get('contractType') == 'TRADIFI_PERPETUAL' or s.get('underlyingType') == 'INDEX'} | fx.EXCLUDE
    dms = lambda s: int(dt.datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=dt.UTC).timestamp() * 1000)

    def alive_listed(S):
        n = 0
        for s, v in full_life.items():
            if not re.fullmatch(r'[A-Z0-9]+USDT', s) or s in bad:
                continue
            for i, (a, b) in enumerate(v['segments']):
                lst = dms(v['first_kline_day']) if i == 0 and v.get('first_kline_day') else dms(a)
                if dms(a) <= S < dms(b) + fx.DAY and S - min(lst, dms(a)) >= fx.LISTED_MIN:
                    n += 1
                    break
        return n
    ha = fx.signal_weeks('holdout_A')
    out['holdout_A_listing'] = {'weeks': len(ha), 'alive_listed_mean_discovery': mean([alive_listed(S) for S in signals]),
                                'alive_listed_mean_holdout_A': mean([alive_listed(S) for S in ha])}
    pr('holdout A breadth (listing only)', out['holdout_A_listing'])

    # --- power (F1; market factor; drift costs) ---
    out['power'] = {'z_crit': fx.Z_CRIT, 'sims': SIMS, 'family': list(fx.FAMILY)}
    if os.environ.get('R009_POWER', '1') == '1':
        inp = fx.sim_inputs(uni, KEY, disp)
        samp = fx.sampling_sd_ic(inp)
        res = []
        for sd in POWER_SD:
            extra = 0.0 if sd == 'sampling' else fx.extra_for_total(inp, sd)
            for ic in POWER_ICS:
                r = fx.simulate_power(inp, ic, extra, sims=SIMS, seed=1, z=fx.Z_CRIT, holdout_weeks=len(ha), mkt=mkt)
                res.append({'sd_ic': sd if sd != 'sampling' else round(samp, 4), 'ic': ic, **{k: r[k] for k in (
                    'p_pass', 'p_crit1', 'p_crit2', 'p_pass_and_pull', 'p_holdout_confirm', 'end_to_end')},
                    'mean_net_bp': 1e4 * r['mean_net'], 'sd_mean_net_bp': 1e4 * r['sd_mean_net'],
                    'mean_cost_bp': 1e4 * r['mean_cost'], 'mean_cost2x_bp': 1e4 * r['mean_cost2x']})
                print(f'  power sd {sd} ic {ic}: {res[-1]}', flush=True)
        no_mkt = fx.simulate_power(inp, 0.0, fx.extra_for_total(inp, 0.15), sims=SIMS, seed=1, z=fx.Z_CRIT)
        # Revision 3a: a beta-only null — IC 0, the empirical market moves shifted to a -2%/week bear,
        # so the book's net short beta alone produces passes; the beta-adjusted NW t pull condition
        # must stop them
        bear = [m - 0.02 for m in mkt]                                  # price-only moves, shifted
        beta_only = {sd: fx.simulate_power(inp, 0.0, 0.0 if sd == 'sampling' else fx.extra_for_total(inp, sd), sims=SIMS, seed=2,
                                           z=fx.Z_CRIT, mkt=bear) for sd in ('sampling', 0.15)}
        out['power_beta_only_null'] = {str(sd): {k: v[k] for k in ('p_pass', 'p_pass_and_pull', 'mean_net')} for sd, v in beta_only.items()}
        pr('beta-only null (IC 0, market moves -2%/wk)', out['power_beta_only_null'])
        cost_bp = res[0]['mean_cost_bp']
        req = {'breakeven_gross_bp': cost_bp}
        for x in res:
            if x['ic'] == 0.0:
                se = x['sd_mean_net_bp']
                req[f"sd_{x['sd_ic']}"] = {'se_bp': se, 'gross_50pct_bp': cost_bp + fx.Z_CRIT * se,
                                           'gross_80pct_bp': cost_bp + (fx.Z_CRIT + 0.8416) * se}
        out['power'].update({'sampling_sd_ic': samp, 'rows': res, 'required_gross': req,
                             'null_without_market_factor_sd015': {k: no_mkt[k] for k in ('p_pass', 'p_crit1', 'p_crit2')}})
        pr('required gross edge (F1)', req)
    # Revision 4 (m6): the F2 rejection record, measured in Revision 3 with `turnover_path(drift=True)` on
    # the F2 features of that revision (code since removed, describe log of 2026-10-11); the gate (<= 15 bp,
    # autocorrelation >= 0.5) is a behaviour measure, not the run measure (00:00 closes vs 01:00 opens,
    # no feasibility filter, no delisting close)
    out['rejected_F2_record'] = {
        'gate': {'buffered_cost_with_drift_bp_max': 15.0, 'rank_autocorr_min': 0.5},
        '7d_value': {'cost_bp': 28.67, 'target_only_bp': 28.54, 'autocorr': -0.197},
        '7d_contracts': {'cost_bp': 29.14, 'target_only_bp': 28.94, 'autocorr': -0.210},
        '28d_value': {'cost_bp': 15.07, 'target_only_bp': 14.59, 'autocorr': 0.524, 'miss_bp': 0.07},
        '28d_contracts': {'cost_bp': 15.44, 'target_only_bp': 14.93, 'autocorr': 0.516, 'miss_bp': 0.44},
        'F1_same_measure': {'cost_bp': 13.36, 'target_only_bp': 12.74, 'autocorr': 0.580}}
    out['dispersion']['market_move_price_only_mean'] = statistics.fmean(mkt)
    out['dispersion']['market_move_price_only_sd'] = statistics.pstdev(mkt)
    out['dispersion']['implied_beta_pnl_bp_per_week'] = 1e4 * out['book_ex_ante']['net_beta_mean'] * statistics.fmean(mkt)
    OUT.write_text(json.dumps(out, indent=1, default=str))
    print(f'\nwritten {OUT}')


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'build':
        print(by.build(str(DATA), str(LISTING)))
    else:
        main()
