"""Populations for registration 001 at each selection date — behaviour only.

Per window: the eligible set (T1), the slow set (T2/T3) and, within the slow
set, wallets with >= 4 completed trips in BOTH BTC regimes (the cross-regime
score needs them). Uses features (p4_features.py), the vault list, and trip
OPEN TIMES only — `trip_open_times` never computes a cash flow, so no score
or PnL exists after this script.

    .venv/bin/python p5_population.py
Writes data/derived/hyperliquid/population_<S>.json.
"""
import json

import pyarrow.parquet as pq

from ct import schedule
from ct.leaders import load_fills
from ct.net import CACHE, DATA
from ct.regimes import Regimes

DEX = 'hyperliquid'
EPS = 1e-12
COMMON = {'account_value_min': 10_000, 'equity_age_h_max': 7 * 24, 'maker_share_max': 0.5,
          'fills_per_day_max': 500, 'mapped_share_min': 0.8}
ELIGIBLE = {**COMMON, 'round_trips_min': 20, 'median_hold_h_min': 2.0}
SLOW = {**COMMON, 'round_trips_min': 10, 'median_hold_h_min': 24.0}
MIN_TRIPS_PER_REGIME = 4


def passes(r, t):
    return ((r['round_trips'] or 0) >= t['round_trips_min']
            and (r['account_value'] or 0) >= t['account_value_min']
            and r['equity_age_h'] is not None and -r['equity_age_h'] <= t['equity_age_h_max']
            and r['maker_share'] is not None and r['maker_share'] <= t['maker_share_max']
            and r['median_hold_h'] is not None and r['median_hold_h'] >= t['median_hold_h_min']
            and r['fills_per_active_day'] <= t['fills_per_day_max']
            and (r['mapped_share'] or 0) >= t['mapped_share_min'])


def vault_addresses():
    path = next((CACHE / 'hl-vaults').glob('*.json'))
    return {v['summary']['vaultAddress'].lower() for v in json.loads(path.read_text())}


def trip_open_times(fills):
    """Open times of completed flat -> flat trips per coin. Behaviour only: no cash."""
    opened, out = {}, []
    for f in fills:
        signed = f['sz'] if f['side'] == 'B' else -f['sz']
        sp = f['startPosition']
        ep = sp + signed
        coin = f['coin']
        flip = sp * ep < 0
        if coin in opened and (abs(ep) <= 1e-9 * max(1.0, abs(sp)) or flip):
            out.append(opened.pop(coin))
        if (abs(sp) < EPS and abs(ep) > EPS) or flip:
            opened[coin] = f['time']
    return out


def main():
    vaults = vault_addresses()
    reg = Regimes()
    for sel, lb_start, hold_end in schedule.windows(DEX):
        rows = pq.read_table(DATA / f'derived/{DEX}/features_{sel}.parquet').to_pylist()
        not_vault = [r for r in rows if r['address'].lower() not in vaults]
        eligible = [r['address'] for r in not_vault if passes(r, ELIGIBLE)]
        slow = [r['address'] for r in not_vault if passes(r, SLOW)]
        fills = load_fills(DEX, slow, lb_start, sel)
        both = []
        for a in slow:
            opens = trip_open_times(fills[a])
            rising = sum(1 for t in opens if reg.is_rising(t) is True)
            falling = sum(1 for t in opens if reg.is_rising(t) is False)
            if rising >= MIN_TRIPS_PER_REGIME and falling >= MIN_TRIPS_PER_REGIME:
                both.append(a)
        out = {'selection': sel.isoformat(), 'lookback_start': lb_start.isoformat(),
               'hold_end': hold_end.isoformat(), 'vaults_removed': len(rows) - len(not_vault),
               'eligible': eligible, 'slow': slow, 'slow_both_regimes': both}
        (DATA / f'derived/{DEX}/population_{sel}.json').write_text(json.dumps(out))
        print(f'{sel}: eligible {len(eligible):,}, slow {len(slow):,}, slow with >= {MIN_TRIPS_PER_REGIME} trips '
              f'in both regimes {len(both):,} (vault addresses removed: {out["vaults_removed"]})')


if __name__ == '__main__':
    main()
