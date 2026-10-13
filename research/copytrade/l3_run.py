"""Registration 002 — the run. REFUSES to start unless the registration is committed with
status REGISTERED and unmodified since. One line appended to results.jsonl.

    .venv/bin/python l3_run.py
"""
import datetime as dt
import json
import random
import re
import statistics
import subprocess
import sys
from pathlib import Path

from ct import eventstudy as es, liq, stats
from ct import bitget as bg
from ct.leaders import con
from ct.net import DATA
from ct.regimes import Regimes
from ct.venue import BitgetVenue

HERE = Path(__file__).resolve().parent
REG = HERE / 'registrations/002-liquidation-cascades.md'
RESULTS = HERE / 'results.jsonl'
DEX = 'hyperliquid'
MINUTE, HOUR, DAY = 60_000, 3_600_000, 86_400_000
TRIALS, Z_CRIT = 2, 1.96
GAP_MS = 60_000                                  # G (D1)
POLL = MINUTE
HORIZONS = {'1m': MINUTE, '5m': 5 * MINUTE, '15m': 15 * MINUTE, '60m': HOUR, '240m': 4 * HOUR, '24h': DAY}
PRIMARY = '60m'
TAKER, SLIP = 0.0006, 0.10                       # D5
SLIPS = [0.0, 0.10, 0.25]
BAND = 0.01                                      # D7
MAJORS = {'BTC', 'ETH', 'SOL'}
NOTIONAL = 10_000.0                              # D8 nominal per event


def guard():
    text = REG.read_text()
    m = re.search(r'^\*\*Status: (\w+)', text, re.M)
    if not m or m.group(1) != 'REGISTERED':
        sys.exit(f'{REG.name} is not REGISTERED — refusing to compute any forward return.')
    if subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--', str(REG)], cwd=HERE).returncode:
        sys.exit(f'{REG.name} differs from the committed version — commit (as an amendment) first.')
    return subprocess.run(['git', 'log', '-1', '--format=%H', '--', str(REG)], cwd=HERE,
                          capture_output=True, text=True).stdout.strip()


def entry_minute(t_end, latency_ms):
    """Detection at t_end + latency; first poll at or after it; act at the open of the next minute."""
    detect = t_end + latency_ms
    poll = -(-detect // POLL) * POLL
    return poll + MINUTE


def candle(sym, minute):
    return bg.candle_at(sym, minute)


def event_row(e, venue, reg, latency_ms=GAP_MS):
    """All per-event quantities (registration §4, §6). Returns None without an entry candle."""
    x = entry_minute(e['t_end'], latency_ms)
    c_in = candle(e['symbol'], x)
    if not c_in:
        return None
    row = {'coin': e['coin'], 'symbol': e['symbol'], 'side': e['side'], 't_end': e['t_end'], 'entry': x,
           'notional': e['notional'], 'rel': e['rel'], 'n_fills': e['n'], 'cluster': x // HOUR, 'day': x // DAY,
           'month': dt.datetime.fromtimestamp(x / 1000, dt.UTC).strftime('%Y-%m'),
           'major': e['coin'] in MAJORS, 'fallback_median': e.get('fallback_median', False),
           'hl_gap_bp': (c_in[1] / (e['end_price'] / venue_qty_mult(e)) - 1) * 1e4 if e.get('end_price') else None,
           'ret': {}, 'ret_slip': {}, 'btc': {}}
    for name, h in HORIZONS.items():
        c_out = candle(e['symbol'], x + h)
        row['ret'][name] = es.event_return(e['side'], c_in, c_out, TAKER, SLIP)
        row['ret_slip'][name] = {s: es.event_return(e['side'], c_in, c_out, TAKER, s) for s in SLIPS}
        b_in, b_out = candle('BTCUSDT', x), candle('BTCUSDT', x + h)
        row['btc'][name] = (b_out[1] / b_in[1] - 1) if b_in and b_out else None
    row['beta'] = venue.beta(e['symbol'], x)
    row['month_label'] = reg.month_label(row['month'])
    # minimum-size realism at the nominal notional
    row['size_ok'] = NOTIONAL >= 5.0
    return row


def venue_qty_mult(e):
    return SYMTAB[e['coin']]['qty_mult']


def density_for(e, snapshots):
    """Map density from the latest snapshot strictly before the cascade start (None in the gap)."""
    i = __import__('bisect').bisect_left([s[0] for s in snapshots], e['t_start']) - 1
    if i < 0:
        return None, None
    snap_ms, path = snapshots[i]
    rows = con().execute(f"""SELECT size, notional, liquidation_price FROM read_parquet('{path}')
                             WHERE market = ? AND size != 0""", [e['coin']]).fetchall()
    d, gross = liq.density(rows, e['side'], e['start_price'] , BAND)
    return d, (e['t_start'] - snap_ms) / HOUR


def summarise(rows, placebo_seeds=200):
    """Statistics and pass flags for T1 / T2 from event rows (pure; tested on synthetic rows)."""
    def vals(rs, name=PRIMARY, key='ret'):
        return [r[key][name] for r in rs], [r['cluster'] for r in rs]

    def t_block(rs, name=PRIMARY):
        v, cl = vals(rs, name)
        m, se, t, n, g = es.cluster_t(v, cl)
        vv = [x for x in v if x is not None]
        return {'mean': m, 'se': se, 't': t, 'n': n, 'clusters': g,
                'median': statistics.median(vv) if vv else None,
                't_day': es.cluster_t(v, [r['day'] for r in rs])[2]}
    rows = [r for r in rows if r['ret'][PRIMARY] is not None]
    out = {'trials': TRIALS, 'z_crit': Z_CRIT, 'events': len(rows)}
    main = t_block(rows)
    half = len(rows) // 2
    srt = sorted(rows, key=lambda r: r['entry'])
    majors, rest = [r for r in rows if r['major']], [r for r in rows if not r['major']]
    halves = [t_block(srt[:half])['mean'], t_block(srt[half:])['mean']] if half else [None, None]
    tm, tr = t_block(majors), t_block(rest)
    crit = {'mean_t': main['t'] is not None and main['t'] >= Z_CRIT and main['mean'] > 0,
            'mean_median_same_sign': main['median'] is not None and (main['mean'] > 0) == (main['median'] > 0),
            'both_halves_positive': all(h is not None and h > 0 for h in halves),
            'majors_and_rest_positive': (tm['mean'] or 0) > 0 and (tr['mean'] or 0) > 0}
    # direction-shuffled placebo: the same returns with the sign randomised per event
    hits = 0
    base = [r['ret'][PRIMARY] for r in rows]
    cl = [r['cluster'] for r in rows]
    for s in range(placebo_seeds):
        rng = random.Random(s)
        v = [x * rng.choice((-1, 1)) for x in base]
        _, _, t, _, _ = es.cluster_t(v, cl)
        hits += t is not None and abs(t) >= Z_CRIT
    out['T1'] = {'primary': main, 'criteria': crit, 'pass': all(crit.values()), 'halves': halves,
                 'majors': tm, 'rest': tr, 'placebo_share': hits / placebo_seeds,
                 'reading': None if main['mean'] is None else
                 ('continuation (reported, not a claim)' if main['mean'] < 0 and main['t'] is not None and main['t'] <= -Z_CRIT else None)}
    rep = {'by_horizon': {h: t_block(rows, h) for h in HORIZONS},
           'by_side': {s: t_block([r for r in rows if r['side'] == s]) for s in ('L', 'S')},
           'by_regime': {lab: t_block([r for r in rows if r['month_label'] == lab]) for lab in ('UP', 'DOWN', 'FLAT')},
           'by_slippage': {str(s): es.cluster_t([r['ret_slip'][PRIMARY][s] for r in rows], cl)[0] for s in SLIPS},
           'by_size_quintile': [], 'hedged_mean': None, 'hl_gap_bp_median': None, 'gap_share': None}
    srt_size = sorted(rows, key=lambda r: r['rel'])
    k = len(srt_size) // 5
    if k:
        rep['by_size_quintile'] = [t_block(srt_size[i * k:(i + 1) * k] if i < 4 else srt_size[4 * k:])['mean'] for i in range(5)]
    hedged = [r['ret'][PRIMARY] - (r['beta'] if r['beta'] is not None else 1.0) * r['btc'][PRIMARY]
              for r in rows if r['btc'][PRIMARY] is not None]
    rep['hedged_mean'] = statistics.fmean(hedged) if hedged else None
    gaps = [r['hl_gap_bp'] for r in rows if r.get('hl_gap_bp') is not None]
    rep['hl_gap_bp_median'] = statistics.median(gaps) if gaps else None
    rep['fallback_median_share'] = sum(1 for r in rows if r['fallback_median']) / len(rows) if rows else None
    out['T1']['reported'] = rep
    # T2 — map density terciles within month
    with_d = [r for r in rows if r.get('density') is not None]
    split = es.tercile_split(with_d, key=lambda r: r['density'], value=lambda r: r['ret'][PRIMARY],
                             cluster=lambda r: r['cluster'], within=lambda r: r['month']) if with_d else None
    t2_pass = bool(split and split['z'] is not None and split['z'] >= Z_CRIT
                   and all(m is not None for m in split['means']) and split['means'][2] > split['means'][1] > split['means'][0]
                   and out['T1']['pass'])
    out['T2'] = {'split': split, 'events_with_map': len(with_d), 'pass': t2_pass,
                 'snapshot_age_h_median': statistics.median(r['snap_age_h'] for r in with_d) if with_d else None}
    return out


SYMTAB = None


def main():
    global SYMTAB
    commit = guard()
    SYMTAB = {c: r for c, r in json.loads((DATA / 'p0/2026-10-07/symbols.json').read_text()).items() if r['status'] == 'ok'}
    grid = json.loads((DATA / f'derived/{DEX}/cascades.json').read_text())
    venue = BitgetVenue('2026-10-07', funding_override={})
    reg = Regimes()
    snaps = sorted((int(p.stem.split('_')[1]), str(p)) for p in
                   (DATA / f'archive/by_dex/{DEX}/snapshots/perp').glob('date=*/*.parquet'))
    out = {'registration': REG.name, 'registration_commit': commit, 'run_at': dt.datetime.now(dt.UTC).isoformat(),
           'params': {'gap_ms': GAP_MS, 'poll_ms': POLL, 'taker': TAKER, 'slip': SLIP, 'band': BAND,
                      'primary': PRIMARY, 'trials': TRIALS, 'z_crit': Z_CRIT}}
    rows = []
    for i, e in enumerate(grid['primary'], 1):
        r = event_row(e, venue, reg)
        if r is None:
            continue
        r['density'], r['snap_age_h'] = density_for(e, snaps)
        rows.append(r)
        if i % 250 == 0:
            print(f'  events {i}/{len(grid["primary"])}')
    out.update(summarise(rows))
    # latency sensitivity and the robustness grid (reported)
    out['reported_latency'] = {}
    for name, lat in (('2G', 2 * GAP_MS), ('5min', 5 * MINUTE)):
        rs = [x for x in (event_row(e, venue, reg, lat) for e in grid['primary']) if x]
        out['reported_latency'][name] = es.cluster_t([r['ret'][PRIMARY] for r in rs], [r['cluster'] for r in rs])[:3]
    out['reported_grid'] = {}
    for key, evs in grid.items():
        if key == 'primary':
            continue
        rs = [x for x in (event_row(e, venue, reg) for e in evs) if x]
        m, se, t, n, g = es.cluster_t([r['ret'][PRIMARY] for r in rs], [r['cluster'] for r in rs])
        out['reported_grid'][key] = {'mean': m, 't': t, 'n': n}
    with open(RESULTS, 'a') as f:
        f.write(json.dumps(out, default=str) + '\n')
    print(json.dumps({k: out[k] for k in ('T1', 'T2')}, indent=1, default=str)[:4000])


if __name__ == '__main__':
    main()
