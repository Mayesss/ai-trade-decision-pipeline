"""Populations for registration 001 at each selection date — behaviour only.

Per window: the eligible set (T1) with its hold-time buckets, the slow set
(T2/T3/T4), the day-trader set (T2b) and, within slow and day, wallets with
>= 4 and >= 8 completed trips in BOTH BTC regimes (the cross-regime score
needs them; D7 fixes the floor by a rule on the counts, below). Uses
features (p4_features.py), the vault list, and trip OPEN TIMES only —
`trip_open_times` never computes a cash flow, so no score or PnL exists
after this script.

D7 rule (approved 2026-10-08, before any count was seen): the per-regime
trip floor is 8 unless some window has fewer than 250 scored slow wallets
at 8, in which case it is 4 for all windows. The chosen floor is written to
population_summary.json and read by p5_run.py.

    .venv/bin/python p5_population.py
Writes data/derived/hyperliquid/population_<S>.json and population_summary.json.
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
DAY = {**COMMON, 'round_trips_min': 20, 'median_hold_h_min': 1.0, 'median_hold_h_max': 24.0}
SCALPER = {**COMMON, 'round_trips_min': 20, 'median_hold_h_min': 0.2, 'median_hold_h_max': 1.0,
           'fills_per_day_max': 2000}   # T1 only — scored, never copied (registration §3)
FLOORS = (4, 8)
D7_MIN_SLOW_AT_8 = 250


def passes(r, t):
    return ((r['round_trips'] or 0) >= t['round_trips_min']
            and (r['account_value'] or 0) >= t['account_value_min']
            and r['equity_age_h'] is not None and -r['equity_age_h'] <= t['equity_age_h_max']
            and r['maker_share'] is not None and r['maker_share'] <= t['maker_share_max']
            and r['median_hold_h'] is not None and r['median_hold_h'] >= t['median_hold_h_min']
            and r['median_hold_h'] < t.get('median_hold_h_max', float('inf'))
            and r['fills_per_active_day'] <= t['fills_per_day_max']
            and (r['mapped_share'] or 0) >= t['mapped_share_min'])


def bucket(r):
    h = r['median_hold_h']
    return 'scalper' if h < 1.0 else 'day' if h < 24.0 else 'slow'


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


def both_regimes(addresses, fills, reg):
    """{floor: [addresses with >= floor trips opened in each BTC regime]}."""
    out = {k: [] for k in FLOORS}
    for a in addresses:
        opens = trip_open_times(fills[a])
        rising = sum(1 for t in opens if reg.is_rising(t) is True)
        falling = sum(1 for t in opens if reg.is_rising(t) is False)
        for k in FLOORS:
            if rising >= k and falling >= k:
                out[k].append(a)
    return out


def main():
    vaults = vault_addresses()
    reg = Regimes()
    summary = {'windows': {}, 'd7_rule': f'floor 8 unless any window has < {D7_MIN_SLOW_AT_8} slow wallets at 8'}
    for sel, lb_start, hold_end in schedule.windows(DEX):
        rows = pq.read_table(DATA / f'derived/{DEX}/features_{sel}.parquet').to_pylist()
        not_vault = [r for r in rows if r['address'].lower() not in vaults]
        eligible = [r['address'] for r in not_vault if passes(r, ELIGIBLE)]
        slow = [r['address'] for r in not_vault if passes(r, SLOW)]
        day = [r['address'] for r in not_vault if passes(r, DAY)]
        scalper = [r['address'] for r in not_vault if passes(r, SCALPER)]
        t1 = {r['address']: bucket(r) for r in not_vault
              if passes(r, ELIGIBLE) or passes(r, DAY) or passes(r, SCALPER)}
        fills = load_fills(DEX, sorted(set(slow) | set(day)), lb_start, sel)
        slow_br = both_regimes(slow, fills, reg)
        day_br = both_regimes(day, fills, reg)
        out = {'selection': sel.isoformat(), 'lookback_start': lb_start.isoformat(),
               'hold_end': hold_end.isoformat(), 'vaults_removed': len(rows) - len(not_vault),
               'eligible': eligible, 't1_buckets': t1, 'slow': slow, 'day': day, 'scalper': scalper,
               **{f'slow_both_regimes_{k}': v for k, v in slow_br.items()},
               **{f'day_both_regimes_{k}': v for k, v in day_br.items()}}
        (DATA / f'derived/{DEX}/population_{sel}.json').write_text(json.dumps(out))
        counts = {k: len(v) for k, v in out.items() if isinstance(v, list)}
        counts['t1'] = {b: sum(1 for x in t1.values() if x == b) for b in ('scalper', 'day', 'slow')}
        summary['windows'][sel.isoformat()] = counts
        print(f'{sel}: eligible {len(eligible):,} (T1 by bucket {counts["t1"]}), slow {len(slow):,} '
              f'[both regimes >=4: {len(slow_br[4]):,}, >=8: {len(slow_br[8]):,}], day {len(day):,} '
              f'[>=4: {len(day_br[4]):,}, >=8: {len(day_br[8]):,}], scalper {len(scalper):,} '
              f'(vaults removed: {out["vaults_removed"]})')
    min_slow_8 = min(w['slow_both_regimes_8'] for w in summary['windows'].values())
    summary['regime_trip_floor'] = 8 if min_slow_8 >= D7_MIN_SLOW_AT_8 else 4
    summary['min_slow_at_8'] = min_slow_8
    (DATA / f'derived/{DEX}/population_summary.json').write_text(json.dumps(summary, indent=1))
    print(f'D7: smallest window at floor 8 has {min_slow_8} slow wallets -> floor {summary["regime_trip_floor"]}')


if __name__ == '__main__':
    main()
