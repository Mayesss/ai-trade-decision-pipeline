"""Registration 006 (H3: during a forced-flow run on Hyperliquid, does an EXECUTABLE cross-venue gap
open beyond costs and close within a minute?) — the run. REFUSES to start unless the registration is
committed with status REGISTERED, unmodified since, the guarded code is clean and matches the §8
SHA-256 table (code, liquidations.parquet, cascades.json, the Binance download manifest and the run's
data manifest), and results.jsonl holds no row of it (005's guard, `l7_run.guard_failures`). During the
run every Binance and HL file is checked against the data manifest as it is read. One line appended.

    .venv/bin/python l8_run.py             # the trial (guarded)
    .venv/bin/python l8_run.py --events    # counts only: triggers, filters, signals (no price after any t)
    .venv/bin/python l8_run.py --manifest  # write data/scratch/l8_data_manifest.json (path + SHA-256 of
                                           # every Binance parquet and HL fills file the run reads)

Primary (registration §4, revision 3): 004's causal trigger in BTC, ETH, SOL, XRP, DOGE, HYPE
(fallback-median events dropped); scan the forced BOOK prints from the trigger print while the run is
alive; signal at the first print where the SWEEP gap (the forced print vs Binance's last trade at or
before it) reaches G_MIN (per coin) — the executable-quote signal is a reported variant; stress switch (own coin excluded) up to the signal - 1 s; market orders at
t + 1 s filled by the side-aware rule on both venues; exit when the unwind gap <= 0 (orders 1 s later)
or 60 s after the later entry fill. All six coins pooled; eight criteria.
"""
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:                     # allows `python -I l8_run.py` (no cwd / script dir on sys.path)
    sys.path.insert(0, str(HERE))

import bisect                                                # noqa: E402

from ct import bn, drift, liq, tape, trigger, xvenue as xv  # noqa: E402
from ct.leaders import con                                   # noqa: E402
from ct.net import DATA                                      # noqa: E402
from l7_run import guard_failures, stress_rates              # noqa: E402  (005's guard and stress series, unchanged)
from l6_run import TRIG_COOLDOWN, TRIG_FLOOR, TRIG_FRAC, TRIG_WINDOW  # noqa: E402  (004's causal trigger, D1)

REG = HERE / 'registrations/006-cross-venue-gap-seconds.md'
RESULTS = HERE / 'results.jsonl'
DEX = 'hyperliquid'
LIQ = DATA / f'derived/{DEX}/liquidations.parquet'
COINS = xv.COINS
OFFSET_MS = xv.OFFSET_MS
OFFSET_STRESS = 150                                # reported: Binance stamped 150 ms early vs HL (signal and fills)
TRIALS, CUMULATIVE = 1, 15
EVENT_SET = 'primary'
DATA_MANIFEST = DATA / 'scratch/l8_data_manifest.json'
PRE = 60 * xv.SECOND                               # Binance / HL history read before a print (for 'last at or before')
POST = max(xv.LATENCIES) * 2 + max(xv.FIXED.values()) + xv.EXIT_WINDOW
# name: (event set, own coin excluded from the stress series?, drop fallback-median events?, scan points:
#        'book' forced book prints / 'all' book + backstop, signal: 'exec' quote proxies / 'sweep' forced print)
VARIANTS = {
    'primary': ('causal', True, True, 'book', 'sweep'),                 # revision 3: the sweep signal is primary
    'exec_signal': ('causal', True, True, 'book', 'exec'),
    'inclusive_stress': ('causal', False, True, 'book', 'sweep'),
    'inclusive_fallback': ('causal', True, False, 'book', 'sweep'),
    'all_forced_prints': ('causal', True, True, 'all', 'sweep'),
    'cascades_not_point_in_time': ('cascades', True, True, 'book', 'sweep'),
}

# every research module l8_run imports, transitively (walked with ast, revision 2)
GUARDED_CODE = ('l8_run.py', 'ct/__init__.py', 'ct/xvenue.py', 'ct/bn.py', 'ct/drift.py', 'ct/eventstudy.py',
                'ct/provider.py', 'ct/passive.py', 'ct/tape.py', 'ct/archive_fills.py', 'ct/liq.py', 'ct/trigger.py',
                'ct/leaders.py', 'ct/net.py', 'ct/bitget.py', 'l7_run.py', 'l6_run.py', 'l5_run.py')
GUARDED_DATA = ('data/derived/hyperliquid/cascades.json', 'data/derived/hyperliquid/liquidations.parquet',
                'data/binance/manifest.json', 'data/scratch/l8_data_manifest.json')


def guard():
    failures, commit = guard_failures(HERE, str(REG.relative_to(HERE)), code=GUARDED_CODE, data=GUARDED_DATA)
    if failures:
        sys.exit('\n'.join(failures))
    return commit


# a print on HL's book: the taker's row of a match (one per match), not a backstop transfer to the liquidator
# ('Liquidated Cross/Isolated ...' rows), not an auto-deleveraging row, and not a forced (liquidation) fill:
# forced rows are signal points, never fill or quote candidates (D8)
BOOK = ("crossed AND NOT is_liquidation AND direction NOT LIKE 'Liquidated%' "
        "AND direction <> 'Auto-Deleveraging'")
_hashes = {}
_manifest = None


def _sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 22), b''):
            h.update(chunk)
    return h.hexdigest()


def verify_file(path):
    """Guarded run only: every data file read must be in the data manifest with the same SHA-256."""
    if _manifest is None:
        return
    rel = str(Path(path).resolve().relative_to(HERE))
    want = _manifest.get(rel)
    if want is None:
        sys.exit(f'{rel} is not in {DATA_MANIFEST.name} — refusing')
    if rel not in _hashes:
        _hashes[rel] = _sha(path)
    if _hashes[rel] != want:
        sys.exit(f'{rel} does not match its SHA-256 in {DATA_MANIFEST.name} — refusing')


def install_verifier():
    global _manifest
    _manifest = json.loads(DATA_MANIFEST.read_text())['files']
    bn.set_verifier(verify_file)


def hl_files(t0, t1):
    files = tape._files(DEX, t0, t1)
    for f in files:
        verify_file(f)
    return files


def hl_quotes(queries, taker, window=xv.QUOTE_WINDOW):
    """queries: [(key, coin, t)] -> {key: (time, price)} of the LAST HL book print whose aggressor side is
    `taker` ('buy' = a print at the ask, 'sell' = at the bid), at or before t, within `window` ms.
    Same-ms prints: the last by (file, row). Prices at or before t only."""
    by_day = {}
    for q in queries:
        by_day.setdefault(q[2] // xv.DAY, []).append(q)
    out = {}
    c = con()
    for day, qs in sorted(by_day.items()):
        lo, hi = min(q[2] for q in qs) - window, max(q[2] for q in qs)
        assert hi < xv.HOLDOUT_START
        files = hl_files(lo, hi)
        if not files:
            continue
        coins = sorted({q[1] for q in qs})
        c.execute('CREATE OR REPLACE TEMP TABLE hqq(k VARCHAR, coin VARCHAR, t BIGINT)')
        c.executemany('INSERT INTO hqq VALUES (?, ?, ?)', [(str(k), co, int(t)) for k, co, t in qs])
        rows = c.execute(f"""
            WITH a AS (SELECT coin, epoch_ms(timestamp) AS tt,
                              arg_max(CAST(price AS DOUBLE), (filename, file_row_number)) AS px
                       FROM read_parquet({files!r}, filename = true, file_row_number = true)
                       WHERE coin IN {tuple(coins) if len(coins) > 1 else "('" + coins[0] + "')"} AND {BOOK}
                         AND side = '{taker}' AND epoch_ms(timestamp) >= {lo} AND epoch_ms(timestamp) <= {hi}
                       GROUP BY 1, 2)
            SELECT hqq.k, a.tt, a.px FROM hqq ASOF JOIN a ON hqq.coin = a.coin AND hqq.t >= a.tt
            WHERE a.tt >= hqq.t - {int(window)}""").fetchall()
        out.update({k: (tt, px) for k, tt, px in rows})
    return out


def load_events():
    """The 002 cascades (reported variant, NOT point-in-time: qualified on the run's total notional,
    known only at t_end). Scan = the cascade's prints in [t_start, t_end]."""
    evs = json.loads((DATA / f'derived/{DEX}/cascades.json').read_text())[EVENT_SET]
    return [dict(e, scan_from=e['t_start'], scan_to=e['t_end'], kind='cascade') for e in evs]


def load_causal_events(coins=COINS):
    """PRIMARY event set (D1): 004's causal trigger (`ct/trigger.triggers`, l6_run's D1 constants):
    the coin's trailing-60-s liquidation notional reaches max($100 k, 0.5 x the coin's trailing
    30-day median daily liquidation notional), 15-min cooldown; the cross-coin fallback median when
    the coin has < 5 days of its own history (flagged fallback_median -> dropped in the primary).
    Each trigger carries: t_start = the first forced print in the trailing window at the trigger (the
    stress window starts there), scan_from = the trigger print, scan_to = the end of the liquidation
    run that contains it (the first 60-s gap without a forced print in the coin). Scanning print by
    print up to scan_to is causal: each print is evidence the run is still alive. Discovery only."""
    c = con()
    daily = {}
    for coin, day, n in c.execute(f"""SELECT coin, (t // {xv.DAY}) * {xv.DAY}, SUM(notional) FROM read_parquet('{LIQ}')
                                      WHERE t < {xv.HOLDOUT_START} GROUP BY 1, 2 ORDER BY 1, 2""").fetchall():
        daily.setdefault(coin, []).append((day, n))
    fallback = liq.trailing_median_fn(sorted((d, n) for v in daily.values() for d, n in v))
    out = []
    for coin in coins:
        fills = c.execute(f"""SELECT t, direction, notional, price FROM read_parquet('{LIQ}')
                              WHERE coin = ? AND t < {xv.HOLDOUT_START} ORDER BY t, trade_id""", [coin]).fetchall()
        own = liq.trailing_median_fn(daily[coin])
        out += causal_events(coin, fills, own, fallback)
    out.sort(key=lambda e: (e['t_trig'], e['coin']))
    return out


def causal_events(coin, fills, own, fallback):
    """Pure: one coin's triggers from its liquidation fills [(t, direction, notional, price)] (time,
    trade_id order). own(t) / fallback(t): trailing 30-day median daily notional (None if < 5 days).
    Threshold max(TRIG_FLOOR, TRIG_FRAC x median); fallback_median = own(t) is None at the trigger."""
    thr = lambda t: (lambda m: max(TRIG_FLOOR, TRIG_FRAC * m) if m else None)(own(t) or fallback(t))
    runs = liq.runs(fills, TRIG_WINDOW)
    starts = [r['t_start'] for r in runs]
    times = [f[0] for f in fills]
    out = []
    for tr in trigger.triggers(fills, thr, TRIG_WINDOW, TRIG_COOLDOWN):
        run = runs[bisect.bisect_right(starts, tr['t']) - 1]
        k0 = bisect.bisect_left(times, tr['t'] - TRIG_WINDOW)
        first = next(fills[k][0] for k in range(k0, len(fills)) if liq.side_of(fills[k][1]) is not None)
        out.append({'coin': coin, 'side': tr['side'], 't_start': first, 't_trig': tr['t'], 'scan_from': tr['t'],
                    'scan_to': run['t_end'], 't_end': run['t_end'], 'window_notional': tr['window_notional'],
                    'fallback_median': own(tr['t']) is None, 'kind': 'trigger'})
    return out


def forced_prints(events, idx):
    """{event index: [(t, price, notional, kind)]} — the event's forced prints of its own side in
    [scan_from, scan_to], ordered (t, trade_id) as l2_events.py built the runs. kind: 'market' (Close
    Long/Short) or 'backstop' (Liquidated Cross/Isolated)."""
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE fe(k INTEGER, coin VARCHAR, side VARCHAR, t0 BIGINT, t1 BIGINT)')
    c.executemany('INSERT INTO fe VALUES (?, ?, ?, ?, ?)',
                  [(i, events[i]['coin'], events[i]['side'], events[i]['scan_from'], events[i]['scan_to']) for i in idx])
    rows = c.execute(f"""
        SELECT fe.k, l.t, l.price, l.notional, l.direction, fe.side FROM fe JOIN read_parquet('{LIQ}') l
          ON l.coin = fe.coin AND l.t >= fe.t0 AND l.t <= fe.t1
        WHERE l.t < {xv.HOLDOUT_START} ORDER BY fe.k, l.t, l.trade_id""").fetchall()
    out = {}
    for k, t, px, n, d, side in rows:
        if liq.side_of(d) != side:
            continue
        out.setdefault(k, []).append((t, px, n, 'market' if d.startswith('Close') else 'backstop'))
    return out


def build_scans(events, offset=OFFSET_MS):
    """Behaviour only. scans {i: [(t, p_hl, p_bn)]}: the SWEEP scan, the forced print and Binance's
    last trade at or before t (HL clock), every forced print of the event's side (book and backstop;
    market_scans() keeps the book prints); prints {i: {'kind', 'notional'}}; bn_ok {i: files cover the
    event}; quotes {(i, t): (HL quote, Binance quote)}: the executable-quote proxies at each distinct
    scan time (HL ask / Binance bid for longs liquidated, HL bid / Binance ask for shorts; last print
    of that aggressor side within 1 s). Prices at or before each forced print's own instant only."""
    idx = [i for i, e in enumerate(events) if e['coin'] in COINS]
    fp = forced_prints(events, idx)
    scans, prints, ok = {}, {}, {}
    for coin in COINS:
        sym = bn.SYMBOL[coin]
        ids = [i for i in idx if events[i]['coin'] == coin]
        q, by_day = [], {}
        for i in ids:
            for j, (t, *_rest) in enumerate(fp.get(i, [])):
                by_day.setdefault(t // xv.DAY, []).append((f'{i}:{j}', t))
        pb = {}
        for day, qq in sorted(by_day.items()):
            pb.update(bn.last_at_or_before(sym, qq, offset, lookback_ms=PRE))
        for i in ids:
            e = events[i]
            rows = fp.get(i, [])
            scans[i] = [(t, px, (pb.get(f'{i}:{j}') or (None, None))[1]) for j, (t, px, _, _) in enumerate(rows)]
            prints[i] = {'kind': [r[3] for r in rows], 'notional': [r[2] for r in rows]}
            ok[i] = (bn.has_days(sym, e['scan_from'] - PRE - offset, e['scan_to'] + POST - offset)
                     and bn.has_days(sym, e['scan_from'] - 10 * xv.MINUTE, e['scan_from'], 'bookDepth'))
    quotes = {}
    for side in ('L', 'S'):
        tk = xv._takers(side)
        pts = sorted({(i, t) for i in idx if events[i]['side'] == side for t, *_ in fp.get(i, [])})
        hq = hl_quotes([(f'{i}:{t}', events[i]['coin'], t) for i, t in pts], tk[0])
        for coin in COINS:
            by_day = {}
            for i, t in pts:
                if events[i]['coin'] == coin:
                    by_day.setdefault(t // xv.DAY, []).append((f'{i}:{t}', t))
            bq = {}
            for day, qq in sorted(by_day.items()):
                bq.update(bn.last_at_or_before(bn.SYMBOL[coin], qq, offset, lookback_ms=xv.QUOTE_WINDOW, taker=tk[1]))
            for i, t in pts:
                if events[i]['coin'] == coin:
                    b = bq.get(f'{i}:{t}')
                    h = hq.get(f'{i}:{t}')
                    quotes[(i, t)] = (h[1] if h else None, b[1] if b else None, h[0] if h else None, b[0] if b else None)
    return scans, prints, ok, quotes


def market_scans(scans, prints):
    """The scans restricted to book prints ('Close Long/Short' forced fills): the backstop rows
    ('Liquidated Cross/Isolated') are transfers to the liquidator, not prints on the book."""
    return {i: [x for x, k in zip(sc, prints[i]['kind']) if k == 'market'] for i, sc in scans.items()}


def market_prints(prints):
    return {i: {key: [v for v, k in zip(vals, pr['kind']) if k == 'market'] for key, vals in pr.items()}
            for i, pr in prints.items()}


def exec_scans(sweep, quotes):
    """The executable-quote scan: one point per distinct scan time, (t, HL quote, Binance quote)."""
    out = {}
    for i, sc in sweep.items():
        seen, rows = set(), []
        for t, *_ in sc:
            if t not in seen:
                seen.add(t)
                rows.append((t, *quotes.get((i, t), (None, None))[:2]))
        out[i] = rows
    return out


def event_set(events, offset=OFFSET_MS):
    scans_all, prints_all, ok, quotes = build_scans(events, offset)
    book = market_scans(scans_all, prints_all)
    return {'events': events, 'ok': ok, 'prints_all': prints_all, 'prints': market_prints(prints_all), 'quotes': quotes,
            'scans': {('book', 'sweep'): book, ('all', 'sweep'): scans_all,
                      ('book', 'exec'): exec_scans(book, quotes), ('all', 'exec'): exec_scans(scans_all, quotes)}}


def build():
    """Both event sets, their scans, and the plan of every variant. Behaviour only."""
    sets = {'causal': event_set(load_causal_events()), 'cascades': event_set(load_events())}
    rate, own = stress_rates()
    plans = {}
    for name, (sname, ex_own, drop, pts, sig) in VARIANTS.items():
        d = sets[sname]
        plans[name] = xv.plan(d['events'], d['scans'][(pts, sig)], rate, own if ex_own else None,
                              bn_ok=d['ok'], drop_fallback=drop)
    return sets, plans, (rate, own)


def counts(events, positions, skipped):
    def by_u(coins):
        maj = sum(1 for c in coins if c in xv.MAJORS)
        return {'all': len(coins), 'majors': maj, 'alts': len(coins) - maj}
    six = [i for i, e in enumerate(events) if e['coin'] in COINS]
    return {'events': by_u([events[i]['coin'] for i in six]),
            'skipped': {k: by_u([events[i]['coin'] for i in v if events[i]['coin'] in COINS]) for k, v in skipped.items()},
            'positions': by_u([p['coin'] for p in positions]),
            'positions_by_side': {s: by_u([p['coin'] for p in positions if p['side'] == s]) for s in ('L', 'S')},
            'hour_clusters': len({p['hour'] for p in positions}), 'day_clusters': len({p['day'] for p in positions})}


# ------------------------------------------------------------------ price windows (guarded run only)

def hl_window(coin, t0, t1):
    """HL tape: one row per BOOK match (the taker's row, not forced), [(t, price, aggressor side)] in
    [t0, t1], execution order (timestamp, file, row)."""
    assert t1 < xv.HOLDOUT_START
    files = hl_files(t0, t1)
    if not files:
        return []
    return con().execute(f"""
        SELECT epoch_ms(timestamp), CAST(price AS DOUBLE), side FROM read_parquet({files!r}, filename = true, file_row_number = true)
        WHERE coin = ? AND {BOOK} AND epoch_ms(timestamp) >= {int(t0)} AND epoch_ms(timestamp) <= {int(t1)}
        ORDER BY epoch_ms(timestamp), filename, file_row_number""", [coin]).fetchall()


def bn_window(coin, t0, t1, offset=OFFSET_MS):
    assert t1 < xv.HOLDOUT_START
    return [(t, px, 'sell' if m else 'buy') for t, px, m in bn.window_trades(bn.SYMBOL[coin], t0, t1, offset)]


def series(coin, t0, t1, offset=OFFSET_MS):
    return xv.Series(hl_window(coin, t0, t1)), xv.Series(bn_window(coin, t0, t1, offset))


def run_positions(positions, offset=OFFSET_MS, variants=True):
    """Every variant of the trade on the same windows: latency 0.25 / 1 / 3 s, side-blind fills,
    slippage 0 and the 4-bp floor. Each value: {'rows': simulated, 'skipped': entries without both
    fills (never opened; counted)}."""
    names = [f'L{lat}' for lat in xv.LATENCIES] + ['side_blind', 'slip0', 'slip4']
    out = {n: {'rows': [], 'skipped': []} for n in names}
    for n, pos in enumerate(positions, 1):
        hl, bns = series(pos['coin'], pos['t_sig'] - PRE, pos['t_sig'] + POST, offset)
        runs = [(f'L{lat}', {'latency': lat}) for lat in (xv.LATENCIES if variants else (xv.L_PRIMARY,))]
        if variants:
            runs += [('side_blind', {'side_aware': False}), ('slip0', {'slip_hl': 0.0, 'slip_bn': 0.0}),
                     ('slip4', {'slip_hl': max(xv.SLIP_FLOOR_REPORTED, xv.SLIP_HL[pos['coin']]),
                                'slip_bn': max(xv.SLIP_FLOOR_REPORTED, xv.SLIP_BN[pos['coin']])})]
        for name, kw in runs:
            r = xv.simulate(pos, hl, bns, **kw)
            out[name]['skipped' if 'skipped' in r else 'rows'].append(r)
        if n % 50 == 0:
            print(f'    positions: {n}/{len(positions)}', flush=True)
    return out


def half_life(events, sweep, quotes, prints, positions, primary_ids):
    """Reported: the gap's path at +0 .. +60 s, as the SWEEP gap (forced print vs Binance; HL's last
    non-forced book print vs Binance's last trade afterwards) and as the EXECUTABLE gap (quote proxies):
    (a) from each trade's signal, (b) per event over all its forced book prints, notional-weighted, one row
    per event. hl_close vs bn_close: which venue closes it. The sweep path's first second is the input
    for H1 (a resting order is filled AT the sweep price)."""
    trig = {'sweep': [], 'exec': []}
    for pos in positions:
        hl, bns = series(pos['coin'], pos['t_sig'] - PRE, pos['t_sig'] + max(xv.PROFILE) + xv.SECOND)
        base = {'coin': pos['coin'], 'major': pos['major'], 'side': pos['side']}
        trig['sweep'].append(dict(base, profile=xv.gap_profile(pos['side'], pos['t_sig'], pos['p_hl_sig'], pos['p_bn_sig'],
                                                              hl, bns, mode='sweep')))
        h, b = xv.quotes(pos['side'], hl, bns, pos['t_sig'])
        if h and b:
            trig['exec'].append(dict(base, profile=xv.gap_profile(pos['side'], pos['t_sig'], h, b, hl, bns, mode='exec')))
    casc = {'sweep': [], 'exec': []}
    for i in primary_ids:
        e = events[i]
        sc = sweep.get(i) or []
        if not sc:
            continue
        hl, bns = series(e['coin'], e['scan_from'] - PRE, sc[-1][0] + max(xv.PROFILE) + xv.SECOND)
        ps, pe = [], []
        for (t, ph, pb), n in zip(sc, prints[i]['notional']):
            if ph and pb:
                ps.append((n, xv.gap_profile(e['side'], t, ph, pb, hl, bns, mode='sweep')))
            q = quotes.get((i, t))
            if q and q[0] and q[1]:
                pe.append((n, xv.gap_profile(e['side'], t, q[0], q[1], hl, bns, mode='exec')))
        base = {'coin': e['coin'], 'major': e['coin'] in xv.MAJORS, 'side': e['side']}
        casc['sweep'].append(dict(base, profile=xv.weighted_profile(ps)))
        casc['exec'].append(dict(base, profile=xv.weighted_profile(pe)))
    return trig, casc


def profile_table(rows):
    out = {}
    for u, sel in (('all', rows), ('majors', [r for r in rows if r['major']]), ('alts', [r for r in rows if not r['major']])):
        tab = {}
        for x in xv.PROFILE:
            vals = [r['profile'].get(x) for r in sel if r['profile'].get(x) and r['profile'][x][0] is not None]
            tab[x] = {'n': len(vals), 'gap_median': xv.median([v[0] for v in vals]),
                      'gap_mean': (sum(v[0] for v in vals) / len(vals)) if vals else None,
                      'hl_close_median': xv.median([v[1] for v in vals]), 'bn_close_median': xv.median([v[2] for v in vals]),
                      'hl_close_mean': (sum(v[1] for v in vals) / len(vals)) if vals else None,
                      'bn_close_mean': (sum(v[2] for v in vals) / len(vals)) if vals else None}
        out[u] = tab
    return out


def minute_check(events, scans, ids):
    """002 §11 consistency: HL's last forced print vs Binance's first trade of the next minute
    (signed); medians by class and side."""
    rows = []
    for coin in COINS:
        q = []
        for i in ids:
            if events[i]['coin'] == coin and scans.get(i):
                t, ph, _ = scans[i][-1]
                q.append((str(i), t - t % xv.MINUTE + xv.MINUTE, ph))
        got = {}
        by = {}
        for k, t, _ in q:
            by.setdefault(t // xv.DAY, []).append((k, t))
        for _, qq in by.items():
            got.update(bn.first_at_or_after(bn.SYMBOL[coin], qq, OFFSET_MS, xv.MINUTE))
        for k, t, ph in q:
            o = got.get(k)
            if o:
                e = events[int(k)]
                rows.append({'major': coin in xv.MAJORS, 'side': e['side'], 'g': xv.minute_gap(e['side'], ph, o[1])})
    out = {}
    for u, sel in (('majors', [r for r in rows if r['major']]), ('alts', [r for r in rows if not r['major']])):
        out[u] = {s: {'n': len([r for r in sel if r['side'] == s]), 'median': xv.median([r['g'] for r in sel if r['side'] == s])}
                  for s in ('L', 'S')}
    return out


def report(runs, placebo_seeds=200):
    """T1 from the primary-latency rows and every run variant: the eight criteria and the decision,
    then what is reported and decides nothing."""
    rows = runs[f'L{xv.L_PRIMARY}']['rows']
    T1 = xv.summarise(rows, placebo_seeds=placebo_seeds)
    l3 = xv.summarise(runs['L3000']['rows'], placebo_seeds=0)['primary_hour'] if 'L3000' in runs else {'mean': None}
    T1['decision'] = xv.decide(T1, l3['mean'])
    T1['l3_mean'] = l3['mean']
    # reported, decides nothing
    T1['majors'] = xv.summarise([r for r in rows if r['major']], placebo_seeds=min(50, placebo_seeds))
    T1['alts'] = xv.summarise([r for r in rows if not r['major']], placebo_seeds=min(50, placebo_seeds))
    T1['by_coin'] = {c: {'hour': xv._block([r for r in rows if r['coin'] == c]),
                         'day': xv._block([r for r in rows if r['coin'] == c], 'day')} for c in COINS}
    T1['fixed_exits'] = {h: {'hour': xv._block(rows, 'hour', h), 'day': xv._block(rows, 'day', h),
                             'majors': xv._block([r for r in rows if r['major']], 'hour', h),
                             'alts': xv._block([r for r in rows if not r['major']], 'hour', h)} for h in xv.FIXED}
    T1['variants_trade'] = {name: {'primary_hour': xv.summarise(v['rows'], placebo_seeds=0)['primary_hour'],
                                   'entry_skipped': len(v['skipped'])} for name, v in runs.items()}
    T1['entry_skipped'] = {'n': len(runs[f'L{xv.L_PRIMARY}']['skipped']),
                           'missing_leg': [r.get('missing') for r in runs[f'L{xv.L_PRIMARY}']['skipped']]}
    T1['exit_fallback'] = sum(r['exit_fallback'] for r in rows)
    T1['exit_missing'] = sum(r['exit_missing'] for r in rows)
    T1['fill_delay_ms_q'] = {v: xv.quantiles([r[f'delay_{v}'] for r in rows], (0.5, 0.9, 0.99)) for v in ('hl', 'bn')}
    T1['exit_fill_delay_ms_q'] = {v: xv.quantiles([r[f'exit_delay_{v}'] for r in rows], (0.5, 0.9, 0.99)) for v in ('hl', 'bn')}
    sel = lambda f: [r for r in rows if f(r)]
    T1['legs'] = {u: {'hl': xv.median([r['legs'][xv.PRIMARY][0] for r in rr if r['legs'][xv.PRIMARY][0] is not None]),
                      'bn': xv.median([r['legs'][xv.PRIMARY][1] for r in rr if r['legs'][xv.PRIMARY][1] is not None])}
                  for u, rr in (('all', rows), ('majors', sel(lambda r: r['major'])), ('alts', sel(lambda r: not r['major'])))}
    T1['exit_reason'] = {w: sum(1 for r in rows if r['exit_reason'] == w) for w in ('gap', 'cap')}
    T1['hold_ms_q'] = xv.quantiles([r['hold_ms'] for r in rows], (0.1, 0.5, 0.9))
    T1['entry_gap_q'] = xv.quantiles([r['entry_gap'] for r in rows], (0.1, 0.25, 0.5, 0.75, 0.9))
    T1['signal_gap_q'] = xv.quantiles([r['g_sig'] for r in rows], (0.1, 0.25, 0.5, 0.75, 0.9))
    return T1


def data_files(sets):
    """Every Binance parquet (aggTrades, bookDepth) and HL fills file the run can read: the UTC days of
    [scan_from - PRE - 10 min, scan_to + POST + profile + 2 min] of every event of both sets (margins
    cover the 1-minute check, the offset variant and bookDepth's 10-minute lookback)."""
    days = {}
    for d in sets.values():
        for e in d['events']:
            if e['coin'] not in COINS:
                continue
            a = e['scan_from'] - PRE - 10 * xv.MINUTE
            b = e['scan_to'] + POST + max(xv.PROFILE) + 2 * xv.MINUTE
            for k in range(a // xv.DAY, b // xv.DAY + 1):
                days.setdefault(k, set()).add(e['coin'])
    out = []
    for k, coins in sorted(days.items()):
        day = dt.datetime.fromtimestamp(k * xv.DAY / 1000, dt.UTC).date()
        if day >= bn.HOLDOUT_DAY:
            continue
        for x in tape._files(DEX, k * xv.DAY, k * xv.DAY):
            out.append(Path(x))
        for coin in sorted(coins):
            for ds in ('aggTrades', 'bookDepth'):
                f = bn.ROOT / f'{ds}_parquet' / bn.SYMBOL[coin] / f'date={day.isoformat()}.parquet'
                if f.exists():
                    out.append(f)
    return sorted(set(out))


def write_manifest():
    sets = {'causal': {'events': load_causal_events()}, 'cascades': {'events': load_events()}}
    files = data_files(sets)
    man = {}
    for n, f in enumerate(files, 1):
        man[str(f.resolve().relative_to(HERE))] = _sha(f)
        if n % 200 == 0:
            print(f'    hashed {n}/{len(files)}', flush=True)
    DATA_MANIFEST.write_text(json.dumps({'built_at': dt.datetime.now(dt.UTC).isoformat(), 'n': len(man),
                                         'bytes': sum(f.stat().st_size for f in files), 'files': man}, indent=0))
    print(f'{DATA_MANIFEST}: {len(man)} files, {sum(f.stat().st_size for f in files) / 1e9:.1f} GB')


def main():
    if '--manifest' in sys.argv:
        write_manifest()
        return
    if '--events' in sys.argv:
        sets, plans, _ = build()
        print(json.dumps({name: counts(sets[VARIANTS[name][0]]['events'], *plans[name]) for name in plans}, indent=1))
        return
    commit = guard()
    install_verifier()
    sets, plans, (rate, own) = build()
    prim = sets['causal']
    out = {'registration': REG.name, 'registration_commit': commit, 'run_at': dt.datetime.now(dt.UTC).isoformat(),
           'params': {'event_set': 'causal trigger (004 D1)', 'trigger': {'floor': TRIG_FLOOR, 'frac': TRIG_FRAC,
                      'window_ms': TRIG_WINDOW, 'cooldown_ms': TRIG_COOLDOWN}, 'coins': list(COINS),
                      'majors': sorted(xv.MAJORS), 'signal': 'sweep (forced book print vs Binance last trade)',
                      'quote_window_ms': xv.QUOTE_WINDOW,
                      'drop_fallback_median': True, 'stress_excludes_own_coin': True,
                      'offset_ms': OFFSET_MS, 'offset_stress_ms': OFFSET_STRESS, 'latency_ms': xv.L_PRIMARY,
                      'latencies': list(xv.LATENCIES), 'cap_ms': xv.CAP, 'fixed': list(xv.FIXED),
                      'fill_window_ms': xv.FILL_WINDOW, 'exit_window_ms': xv.EXIT_WINDOW, 'side_aware': xv.SIDE_AWARE,
                      'taker_hl': xv.TAKER_HL, 'taker_bn': xv.TAKER_BN, 'slip_hl': xv.SLIP_HL, 'slip_bn': xv.SLIP_BN,
                      'g_min': {c: xv.g_min(c) for c in COINS}, 'extra_g': xv.EXTRA_G,
                      'stress_threshold': xv.STRESS_THRESHOLD, 'z_hour': xv.Z_HOUR, 'z_day': xv.Z_DAY,
                      'gate': xv.GATE, 'bar': xv.BAR, 'trials': TRIALS, 'cumulative_trials': CUMULATIVE},
           'counts': {name: counts(sets[VARIANTS[name][0]]['events'], *plans[name]) for name in plans}}
    prim_pos = plans['primary'][0]
    runs = run_positions(prim_pos)
    T1 = report(runs)
    variants = {}
    for name in plans:
        if name != 'primary':
            vr = run_positions(plans[name][0], variants=False)[f'L{xv.L_PRIMARY}']
            variants[name] = dict(xv.summarise(vr['rows'], placebo_seeds=50), entry_skipped=len(vr['skipped']))
    # clock-offset stress: Binance treated as stamped 150 ms early (signal, trigger set and fills change)
    so = event_set(prim['events'], OFFSET_STRESS)
    pos_o, _ = xv.plan(so['events'], so['scans'][('book', 'sweep')], rate, own, bn_ok=so['ok'])
    vr = run_positions(pos_o, OFFSET_STRESS, variants=False)[f'L{xv.L_PRIMARY}']
    variants[f'offset_plus{OFFSET_STRESS}'] = dict(xv.summarise(vr['rows'], placebo_seeds=50), entry_skipped=len(vr['skipped']))
    T1['variants'] = variants
    out['T1'] = T1
    ev = prim['events']
    prim_ids = [i for i, e in enumerate(ev) if not e['fallback_median'] and prim['ok'].get(i)]
    trig, casc = half_life(ev, prim['scans'][('book', 'sweep')], prim['quotes'], prim['prints'], prim_pos, prim_ids)
    out['half_life'] = {m: {'from_signal': profile_table(trig[m]), 'all_forced_prints_per_event': profile_table(casc[m])}
                        for m in ('sweep', 'exec')}
    k = sets['cascades']
    k_ids = [i for i, e in enumerate(k['events']) if e['coin'] in COINS and not e['fallback_median'] and k['ok'].get(i)]
    out['minute_check_002'] = minute_check(k['events'], k['scans'][('book', 'sweep')], k_ids)
    with open(RESULTS, 'a') as f:
        f.write(json.dumps(out, default=str) + '\n')
    print(json.dumps({k_: T1[k_] for k_ in ('primary_hour', 'primary_day', 'criteria', 'pass', 'decision', 'l3_mean',
                                            'placebo_hour_flip', 'placebo_day_flip')}, indent=1, default=str))


if __name__ == '__main__':
    main()
