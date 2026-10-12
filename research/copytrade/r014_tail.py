"""Registration 014 — the REALISTIC hedged tail on out-of-calendar stress weeks (orchestrator decision
000m). Everything here ends before 2021-03-24 00:00 UTC (DVOL start = the start of the discovery
calendar): every request is end-bounded below TAIL_CUTOFF (asserted before the request) and every row
received is checked below it.

    .venv/bin/python -I r014_tail.py underlying data/r014/tail_pre2021/underlying_<YYYYMMDD>
    .venv/bin/python -I r014_tail.py select     data/r014/tail_pre2021/underlying_<YYYYMMDD>
    .venv/bin/python -I r014_tail.py options    data/r014/tail_pre2021/options_<YYYYMMDD>
    .venv/bin/python -I r014_tail.py compute

`select` uses the underlying's weekly excursion ONLY (no option data, no premium): every Friday-08:00
entry 2019-01-04 -> 2021-03-12 (hold ends before 2021-03-24), excursion = max(|close move|, -low, high)
over [E, E + 7 d] from the perpetual's hourly candles; selected iff BTC or ETH >= 25 %, plus the two
weeks of the adversarial bound (2020-03-06, 2021-01-01). Written to data/r014/tail_pre2021/selected.json.

`options` pulls, per coin, every option trade in [E - 4 h, E + 7 d) for each week computed: the
selected weeks and the 2020-03 cluster weeks (entries 2020-02-14 -> 2020-04-03) for the 4-week path.

`compute` runs the REGISTERED mechanics unchanged (`ct.shortvol.straddle_entry`, `settlement`,
`position_pnl`, H24, p75 spreads, fees, funding, liquidation) at n = 0.10, 0.25, 0.45, with two stated
substitutions before DVOL exists:
  - hedge/mark IV proxy: instead of DVOL, the hourly ATM mark IV of the position's own expiry from the
    tape (the latest trade in the preceding hour at the traded strike nearest the index; carried forward
    when an hour has no trade), in vol points;
  - thin tape at entry: if no traded strike of the 7-day expiry lies within 2.5 % of the index, the
    straddle is struck AT the index and marked with the IV of the nearest traded strike of that expiry
    in [E - 4 h, E) (else [E - 24 h, E)); flagged per week.
"""
import datetime as dt
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from ct import shortvol as sv  # noqa: E402
import r014_fetch as F  # noqa: E402   (read-only: transport, atomic writes, checksums)

TAIL_CUTOFF = sv.ms(2021, 3, 24)
ROOT = HERE / 'data' / 'r014' / 'tail_pre2021'
START = sv.ms(2019, 1, 1)
FIRST_ENTRY = sv.ms(2019, 1, 4, 8)
SIZES = (0.10, 0.25, 0.45)
EXCURSION_MIN = 0.25
NAMED = (sv.ms(2020, 3, 6, 8), sv.ms(2021, 1, 1, 8))
CLUSTER_2020_03 = tuple(sv.ms(2020, 2, 14, 8) + k * sv.WEEK for k in range(8))   # 02-14 .. 04-03


def tget(base, method, params, **kw):
    if not int(params['end_timestamp']) < TAIL_CUTOFF:
        raise F.FetchError(f'{method}: end {params["end_timestamp"]} not before 2021-03-24 (000m scope)')
    return F.get_json(base, method, params, **kw)


def below(stamps, tag):
    if any(int(t) >= TAIL_CUTOFF for t in stamps):
        raise F.FetchError(f'{tag}: a row on/after 2021-03-24 — refusing')


def new_dir(p):
    p = Path(p).resolve()
    if not p.is_relative_to(ROOT.resolve()):
        raise F.FetchError(f'tail pulls live under {ROOT}')
    if p.exists() and any(p.iterdir()):
        raise F.FetchError(f'{p} is not empty: each pull goes in a NEW directory')
    p.mkdir(parents=True, exist_ok=True)
    return p


def windows(a, b, step=30 * sv.DAY):
    t = a
    while t <= b:
        yield t, min(t + step - 1, b)
        t += step


# ------------------------------------------------------------------ underlying
def pull_underlying(outdir):
    d = new_dir(outdir)
    rep = {}
    for coin in sv.COINS:
        inst = f'{coin}-PERPETUAL'
        perp, fund, empty_fund = [], [], 0
        for a, b in windows(START, TAIL_CUTOFF - 1):
            r = tget(F.WWW, 'get_tradingview_chart_data', {'instrument_name': inst, 'start_timestamp': a,
                                                           'end_timestamp': b, 'resolution': '60'})
            if r.get('status') == 'ok':
                below(r['ticks'], inst)
                perp += [[tk, r['open'][i], r['high'][i], r['low'][i], r['close'][i]] for i, tk in enumerate(r['ticks'])
                         if a <= tk and tk + sv.HOUR <= b + 1]
            elif r.get('status') != 'no_data':
                raise F.FetchError(f'{inst} {sv.iso(a)}: status {r.get("status")}')
            f = tget(F.WWW, 'get_funding_rate_history', {'instrument_name': inst, 'start_timestamp': a, 'end_timestamp': b})
            below([x['timestamp'] for x in f], f'funding {inst}')
            if not f:
                empty_fund += 1                       # counted, never written as data
            fund += [[x['timestamp'], x['interest_1h']] for x in f if a <= x['timestamp'] <= b]
        perp = sorted({r[0]: r for r in perp}.values())
        fund = sorted({r[0]: r for r in fund}.values())
        if perp[-1][0] != TAIL_CUTOFF - sv.HOUR:
            raise F.FetchError(f'{inst}: tail coverage — last candle {sv.iso(perp[-1][0])}')
        F.write_atomic(d / f'perp_{coin}.json', perp)
        F.write_atomic(d / f'funding_{coin}.json', fund)
        rep[coin] = {'perp_rows': len(perp), 'perp_first': sv.iso(perp[0][0]), 'perp_last': sv.iso(perp[-1][0]),
                     'funding_rows': len(fund), 'funding_first': sv.iso(fund[0][0]) if fund else None,
                     'funding_empty_windows': empty_fund}
        print(coin, rep[coin], flush=True)
    rep.update({'requests': F.STATS['requests'], 'bytes': F.STATS['bytes']})
    F.write_atomic(d / 'pull_report.json', rep)
    print('files', F.write_sums(d), rep['requests'], 'requests', round(rep['bytes'] / 1e6, 1), 'MB')


def load_underlying(d):
    d = Path(d)
    sums = d / 'SHA256SUMS'
    for line in sums.read_text().splitlines():
        h, n = line.split('  ', 1)
        assert F.sha256(d / n) == h, f'{n}: SHA-256 mismatch'
    perp = {c: json.loads((d / f'perp_{c}.json').read_text()) for c in sv.COINS}
    fund = {c: json.loads((d / f'funding_{c}.json').read_text()) for c in sv.COINS}
    for c in sv.COINS:
        assert all(r[0] < TAIL_CUTOFF for r in perp[c] + fund[c])
    return perp, fund


def entries():
    out, t = [], FIRST_ENTRY
    while t + 7 * sv.DAY < TAIL_CUTOFF:
        out.append(t)
        t += sv.WEEK
    return out


def excursion(perp_rows, E):
    """max(|close move|, -low, high) over the week [E, E + 7 d] from hourly candles; None if incomplete."""
    rows = [r for r in perp_rows if E <= r[0] < E + 7 * sv.DAY]
    if len(rows) < 160:
        return None
    o = rows[0][1]
    close = rows[-1][4]
    lo, hi = min(r[3] for r in rows) / o - 1, max(r[2] for r in rows) / o - 1
    return {'close': close / o - 1, 'low': lo, 'high': hi, 'excursion': max(abs(close / o - 1), -lo, hi)}


def select(d):
    perp, _ = load_underlying(d)
    out = []
    for E in entries():
        ex = {c: excursion(perp[c], E) for c in sv.COINS}
        big = any(x and x['excursion'] >= EXCURSION_MIN for x in ex.values())
        if big or E in NAMED:
            out.append({'entry': E, 'iso': sv.iso(E), 'excursion': ex, 'reason': 'named' if E in NAMED and not big else '>=25%'})
    F.write_atomic(ROOT / 'selected.json', {'rule': 'BTC or ETH weekly excursion >= 25 %, plus 2020-03-06 and 2021-01-01; '
                                                     'entries 2019-01-04 .. 2021-03-12', 'underlying_dir': Path(d).name,
                                             'weeks': out})
    for w in out:
        print(w['iso'], w['reason'], {c: (round(x['excursion'] * 100, 1) if x else None) for c, x in w['excursion'].items()})
    print(len(out), 'weeks selected of', len(entries()))


# ------------------------------------------------------------------ options tape
def tape(coin, start, end):
    """Every option trade in [start, end], forward-paged (r014_fetch.fetch_window's logic, TAIL-bounded)."""
    seen, out, t = set(), [], start
    while True:
        r = tget(F.HIST, 'get_last_trades_by_currency_and_time',
                 {'currency': coin, 'kind': 'option', 'start_timestamp': t, 'end_timestamp': end,
                  'count': 1000, 'sorting': 'asc', 'include_old': 'true'}, pause=0.05)
        tr = r.get('trades')
        if tr is None:
            raise F.FetchError('no trades key')
        below([x['timestamp'] for x in tr], f'trades {coin}')
        new = [x for x in tr if x['trade_id'] not in seen]
        for x in new:
            seen.add(x['trade_id'])
            out.append({k: x.get(k) for k in F.KEEP})
        if not r.get('has_more') or not tr:
            break
        nxt = tr[-1]['timestamp']
        if not new and nxt == t:
            raise F.FetchError('>1000 trades on one millisecond')
        t = nxt
    return sorted(out, key=lambda x: (x['timestamp'], x['trade_id']))


def computed_weeks():
    sel = json.loads((ROOT / 'selected.json').read_text())
    return sorted({w['entry'] for w in sel['weeks']} | set(CLUSTER_2020_03))


def pull_options(outdir):
    d = new_dir(outdir)
    t0 = time.time()
    for E in computed_weeks():
        for coin in sv.COINS:
            a, b = E - 24 * sv.HOUR, E + 7 * sv.DAY - 1
            rows = tape(coin, a, b)
            F.write_atomic(d / f'{coin}_{dt.datetime.fromtimestamp(E / 1000, dt.UTC):%Y-%m-%d}.json', rows)
            print(coin, sv.iso(E), len(rows), 'trades', F.STATS['requests'], 'req', f'{time.time() - t0:.0f}s', flush=True)
    F.write_atomic(d / 'pull_report.json', {'requests': F.STATS['requests'], 'bytes': F.STATS['bytes'],
                                            'weeks': [sv.iso(E) for E in computed_weeks()]})
    print('files', F.write_sums(d), F.STATS['requests'], 'requests', round(F.STATS['bytes'] / 1e6, 1), 'MB')


# ------------------------------------------------------------------ the computation
def atm_iv_series(trades, coin, expiry, perp_close_at, hours):
    """{t: ATM mark IV (vol points)} at each hour t: the latest trade in [t - 1 h, t) of `expiry` at the
    traded strike nearest the perp price at t; carried forward when an hour has none."""
    by_hour = {}
    for r in trades:
        p = sv.parse_name(r['instrument_name'])
        if p and p[0] == coin and p[1] == expiry:
            by_hour.setdefault(r['timestamp'] // sv.HOUR * sv.HOUR + sv.HOUR, []).append((r, p))
    out, last, filled = {}, None, 0
    for t in hours:
        S = perp_close_at.get(t)
        cand = by_hour.get(t, [])
        iv = None
        if cand and S:
            K = min({p[2] for _, p in cand}, key=lambda k: (abs(k - S), k))
            r, p = max(((r, p) for r, p in cand if p[2] == K), key=lambda x: x[0]['timestamp'])
            F0 = float(r['index_price'])
            iv = sv.implied_vol(float(r['mark_price']) * F0, F0, K, (expiry - r['timestamp']) / sv.DAY / sv.YEAR_D, p[3])
        if iv is None:
            filled += 1
            iv = last
        out[t] = None if iv is None else iv * 100
        last = iv
    return out, filled


def entry_with_fallback(coin, E, trades, perp_close):
    em = sv.straddle_entry(coin, E, trades, perp_close)
    if 'missing' not in em:
        return em, 'registered'
    X = E + 7 * sv.DAY
    S = sv.entry_index(sv.window_rows(trades, E - 4 * sv.HOUR, E), E, perp_close)[0]
    for lo in (4, 24):
        cand = [(r, sv.parse_name(r['instrument_name'])) for r in sv.window_rows(trades, E - lo * sv.HOUR, E)]
        cand = [(r, p) for r, p in cand if p and p[0] == coin and p[1] == X]
        if cand and S:
            r, p = min(cand, key=lambda x: (abs(x[1][2] - S), -x[0]['timestamp']))
            F0 = float(r['index_price'])
            iv = sv.implied_vol(float(r['mark_price']) * F0, F0, p[2], (X - r['timestamp']) / sv.DAY / sv.YEAR_D, p[3])
            if iv:
                T = 7 / sv.YEAR_D
                return ({'coin': coin, 'entry': E, 'expiry': X, 'K': S, 'S': S, 'S_source': 'fallback', 'iv': iv,
                         'mark_call': sv.b76(S, S, T, iv, True), 'mark_put': sv.b76(S, S, T, iv, False),
                         'mark_age_min': (E - r['timestamp']) / 60_000, 'nearest_traded_strike': p[2]},
                        f'fallback: struck at the index, IV of the nearest traded strike {p[2]:g} ({lo} h window); registered: {em["missing"]}')
    return None, f'unmarkable: {em["missing"]}'


def compute(und_dir, opt_dir):
    perp, fund = load_underlying(und_dir)
    od = Path(opt_dir)
    for line in (od / 'SHA256SUMS').read_text().splitlines():
        h, n = line.split('  ', 1)
        assert F.sha256(od / n) == h, f'{n}: SHA-256 mismatch'
    sel = {w['entry']: w for w in json.loads((ROOT / 'selected.json').read_text())['weeks']}
    perp_at = {c: {r[0] + sv.HOUR: r[4] for r in perp[c]} for c in sv.COINS}
    fund_at = {c: {r[0]: r[1] for r in fund[c]} for c in sv.COINS}
    weeks = {}
    for E in computed_weeks():
        X = E + 7 * sv.DAY
        hours = list(range(E, X + 1, sv.HOUR))
        res = {'iso': sv.iso(E), 'selected': E in sel, 'coins': {}}
        for c in sv.COINS:
            tr = json.loads((od / f'{c}_{dt.datetime.fromtimestamp(E / 1000, dt.UTC):%Y-%m-%d}.json').read_text())
            assert all(r['timestamp'] < TAIL_CUTOFF for r in tr)
            # the settlement window lies in the NEXT week's file ([X - 24 h, ...)) when computed; else here
            nxt = od / f'{c}_{dt.datetime.fromtimestamp(X / 1000, dt.UTC):%Y-%m-%d}.json'
            settle_tr = tr + (json.loads(nxt.read_text()) if nxt.exists() else [])
            em, how = entry_with_fallback(c, E, tr, perp_at[c].get(E))
            if em is None:
                res['coins'][c] = {'missing': how}
                continue
            ivs, filled = atm_iv_series(tr, c, X, perp_at[c], hours)
            if ivs[E] is None:                       # no ATM trade in the entry hour: anchor on the entry IV
                ivs = {t: (v if v is not None else em['iv'] * 100) for t, v in ivs.items()}
            path = [(t, perp_at[c][t], ivs[t]) for t in hours]
            px, src, mins = sv.settlement(settle_tr, X, perp_at[c].get(X))
            out = {'construction': how, 'iv_entry': round(em['iv'], 4), 'K': em['K'], 'S0': em['S'],
                   'settle_source': src, 'settle_minutes': mins, 'iv_hours_carried': filled}
            for n in SIZES:
                o = sv.position_pnl(em, path, px, hedge_every_h=24, funding=fund_at[c], liq_n=n)
                out[str(n)] = {'ret_notional': o['ret_notional'], 'ret_premium': o['ret_premium'],
                               'liquidated': (sv.iso(o['liquidated']['t']) if o['liquidated'] else None)}
            o = sv.position_pnl(em, path, px, hedge_every_h=None, funding=None, liq_n=None)
            out['unhedged_reported'] = o['ret_notional']
            out['premium_notional'] = (em['mark_call'] + em['mark_put']) / em['S']
            res['coins'][c] = out
        for n in SIZES:
            rs = [v[str(n)]['ret_notional'] for v in res['coins'].values() if 'missing' not in v]
            res[f'book_{n}'] = sum(rs) / len(rs) if rs else None
        weeks[E] = res
    cl = [weeks[E][f'book_{0.10}'] for E in CLUSTER_2020_03]
    paths = {}
    for n in SIZES:
        b = [weeks[E][f'book_{n}'] for E in CLUSTER_2020_03]
        best = None
        for i in range(len(b) - 3):
            eq = 1.0
            for r in b[i:i + 4]:
                eq *= 1 + n * r
            if best is None or eq - 1 < best[1]:
                best = (sv.iso(CLUSTER_2020_03[i]), eq - 1, sum(b[i:i + 4]))
        paths[str(n)] = {'worst_4wk_start': best[0], 'equity_change': best[1], 'sum_book_notional': best[2]}
    runs = {}
    for n in SIZES:                                   # every run of 4 consecutive computed weeks
        lst = []
        for E in weeks:
            seq = [E + k * sv.WEEK for k in range(4)]
            if all(x in weeks and weeks[x][f'book_{n}'] is not None for x in seq):
                eq = 1.0
                for x in seq:
                    eq *= 1 + n * weeks[x][f'book_{n}']
                lst.append((sv.iso(E), eq - 1, sum(weeks[x][f'book_{n}'] for x in seq)))
        runs[str(n)] = sorted(lst, key=lambda x: x[1])
    out = {'weeks': {sv.iso(E): w for E, w in weeks.items()}, 'cluster_2020_03_book_n010': cl,
           'cluster_2020_03_worst_month': paths, 'all_4_week_runs_of_computed_weeks': runs}
    F.write_atomic(ROOT / 'tail_results.json', out)
    for E, w in weeks.items():
        print(w['iso'], 'sel' if w['selected'] else 'cluster', {c: (v.get('missing') or (v['construction'][:8], round(v['0.1']['ret_notional'] * 100, 1),
                                                                    v['0.45']['liquidated'])) for c, v in w['coins'].items()},
              'book', {n: round(w[f'book_{n}'] * 100, 2) for n in SIZES})
    print(json.dumps(paths))
    print('4-week runs (n 0.10):', [(a, round(b * 100, 2), round(c * 100, 1)) for a, b, c in runs['0.1']])


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else ''
    if cmd == 'underlying':
        pull_underlying(sys.argv[2])
    elif cmd == 'select':
        select(sys.argv[2])
    elif cmd == 'options':
        pull_options(sys.argv[2])
    elif cmd == 'compute':
        compute(sys.argv[2], sys.argv[3])
    else:
        sys.exit(__doc__)
