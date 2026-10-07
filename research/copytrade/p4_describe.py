"""Feature distributions and a cleaning funnel — behaviour only, no returns.

Input: data/derived/<dex>/features_<S>.parquet (p4_features.py). For each
selection window it prints quantiles of the features among wallets with at
least 5 completed round trips, then how many wallets survive a base set of
candidate filters, and how that count moves when ONE threshold is varied.
This is what P4's thresholds are chosen from (plan §5.2); nothing here is a
return.

    .venv/bin/python p4_describe.py > data/derived/p4_describe.txt
"""
import pyarrow.parquet as pq

from ct import schedule
from ct.net import DATA

BASE = {
    'round_trips_min': 20,          # enough history to rank on
    'account_value_min': 10_000,    # not dust; a leader whose size is meaningful
    'maker_share_max': 0.5,         # mostly takes liquidity — copyable as taker
    'median_hold_h_min': 2.0,       # 12x the 10-minute poll
    'share_under_1h_max': 0.5,      # not mostly scalps
    'fills_per_day_max': 500,       # not a bot
    'mapped_share_min': 0.8,        # main dex: trades what the venue lists
}
VARY = {
    'round_trips_min': [10, 40],
    'account_value_min': [1_000, 50_000],
    'maker_share_max': [0.3, 0.7],
    'median_hold_h_min': [0.2, 12.0],   # 12x poll for 1-min and 60-min polls
    'share_under_1h_max': [0.3, 0.8],
    'fills_per_day_max': [200, 2_000],
    'mapped_share_min': [0.5, 0.95],
}
QS = (0.1, 0.25, 0.5, 0.75, 0.9)


def passes(r, t, dex):
    checks = [
        (r['round_trips'] or 0) >= t['round_trips_min'],
        (r['account_value'] or 0) >= t['account_value_min'],
        r['maker_share'] is not None and r['maker_share'] <= t['maker_share_max'],
        r['median_hold_h'] is not None and r['median_hold_h'] >= t['median_hold_h_min'],
        r['share_trips_under_1h'] is not None and r['share_trips_under_1h'] <= t['share_under_1h_max'],
        r['fills_per_active_day'] <= t['fills_per_day_max'],
    ]
    if dex == 'hyperliquid':
        checks.append((r['mapped_share'] or 0) >= t['mapped_share_min'])
    return all(checks)


def q(values):
    v = sorted(x for x in values if x is not None)
    return '  '.join(f'{v[int(p * (len(v) - 1))]:>9.3g}' for p in QS) if v else '-'


def main():
    for dex in schedule.ARCHIVE_START:
        for sel, _, _ in schedule.windows(dex):
            rows = pq.read_table(DATA / f'derived/{dex}/features_{sel}.parquet').to_pylist()
            active = [r for r in rows if (r['round_trips'] or 0) >= 5]
            print(f'\n== {dex} {sel}: {len(rows):,} wallets traded in the lookback, {len(active):,} with >= 5 round trips')
            print(f'   {"feature (>=5 trips)":<24}' + ''.join(f'{"p" + str(int(p * 100)):>11}' for p in QS))
            for k in ('round_trips', 'fills_per_active_day', 'median_hold_h', 'share_trips_under_1h',
                      'maker_share', 'twap_share', 'account_value', 'median_leverage', 'n_coins',
                      'mapped_share', 'equity_age_h'):
                print(f'   {k:<24}  {q(r[k] for r in active)}')
            base_n = sum(passes(r, BASE, dex) for r in rows)
            print(f'   funnel, base filters: {base_n:,} wallets pass')
            for k, alts in VARY.items():
                if k == 'mapped_share_min' and dex != 'hyperliquid':
                    continue
                counts = [sum(passes(r, {**BASE, k: a}, dex) for r in rows) for a in alts]
                print(f'     {k:<22} {alts[0]:>8} -> {counts[0]:>6,}   base {BASE[k]:>8} -> {base_n:>6,}   '
                      f'{alts[1]:>8} -> {counts[1]:>6,}')


if __name__ == '__main__':
    main()
