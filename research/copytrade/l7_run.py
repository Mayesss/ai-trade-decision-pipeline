"""Registration 005 (H4, post-cascade drift) — the run. REFUSES to start unless the registration
is committed with status REGISTERED and unmodified since. One line appended to results.jsonl.

    .venv/bin/python l7_run.py           # the trial (guarded)
    .venv/bin/python l7_run.py --events  # counts only: events, filters, positions (no outcome)

Primary (D1, D5): the 002 primary cascades WITHOUT those qualified on the cross-coin fallback
median, and the stress switch on the market series EXCLUDING the cascade coin's own
liquidations. Reported variants: the inclusive 002 event set; the inclusive stress series;
ETH/SOL hedged with BTC (beta from the trailing 30 days of Bitget 1H closes before the entry).
"""
import datetime as dt
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

from ct import drift, tape
from ct.leaders import con
from ct.net import CACHE, DATA
from l5_run import MAJORS

HERE = Path(__file__).resolve().parent
REG = HERE / 'registrations/005-post-cascade-drift.md'
RESULTS = HERE / 'results.jsonl'
DEX = 'hyperliquid'
DAY, HOUR = drift.DAY, drift.HOUR
LIQ = DATA / f'derived/{DEX}/liquidations.parquet'
TRIALS, CUMULATIVE = 1, 14
EVENT_SET = 'primary'
HEDGE = 'BTC'
BG_SYMBOL = {'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'SOL': 'SOLUSDT'}
HOUR_BLOCK = 200 * HOUR
VARIANTS = {                        # name: (own-coin excluded from the stress series?, drop warm-up?)
    'primary': (True, True),
    'inclusive_events': (True, False),
    'inclusive_stress': (False, True),
}


# the code the outcome depends on: committed, clean, and hashed in the registration's §8 table
GUARDED_CODE = ('ct/drift.py', 'l7_run.py', 'ct/tape.py', 'ct/eventstudy.py', 'ct/provider.py', 'ct/liq.py')
GUARDED_DATA = ('data/derived/hyperliquid/cascades.json',)
HASH_ROW = re.compile(r'^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|', re.M)


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def guard_failures(root, reg_rel, code=GUARDED_CODE, data=GUARDED_DATA, results_rel='results.jsonl'):
    """Every reason the run must not start; ([] , commit) when it may. root: the working directory
    the paths are relative to (inside a git work tree). Checks: status REGISTERED; (a) the
    registration is tracked; (b) unmodified vs HEAD; (c) it has a commit; (d) the guarded code is
    clean in git status; (e) code and data match the SHA-256 table in the registration; (f)
    results.jsonl holds no row of this registration yet."""
    root = Path(root)
    reg = root / reg_rel
    git = lambda *a: subprocess.run(['git', *a], cwd=root, capture_output=True, text=True)
    out = []
    text = reg.read_text() if reg.exists() else ''
    m = re.search(r'^\*\*Status: (\w+)', text, re.M)
    if not m or m.group(1) != 'REGISTERED':
        out.append(f'{reg.name} is not REGISTERED — refusing to compute any forward return.')
    if git('ls-files', '--error-unmatch', '--', reg_rel).returncode:
        out.append(f'(a) {reg.name} is not tracked by git')
    if git('diff', '--quiet', 'HEAD', '--', reg_rel).returncode:
        out.append(f'(b) {reg.name} differs from the committed version — commit (as an amendment) first.')
    commit = git('log', '-1', '--format=%H', '--', reg_rel).stdout.strip()
    if not commit:
        out.append(f'(c) {reg.name} has no commit')
    dirty = git('status', '--porcelain', '--', *code).stdout.strip()
    if dirty:
        out.append(f'(d) guarded code not clean: {dirty.splitlines()}')
    table = dict(HASH_ROW.findall(text))
    for f in (*code, *data):
        want = table.get(f)
        if want is None:
            out.append(f'(e) {f} missing from the registration hash table')
        elif not (root / f).exists() or sha256(root / f) != want:
            out.append(f'(e) {f} does not match its registered SHA-256')
    res = root / results_rel
    if res.exists():
        for line in res.read_text().splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get('registration') == reg.name:
                out.append(f'(f) {results_rel} already holds a row for {reg.name} — the trial runs once')
                break
    return out, commit


def guard():
    failures, commit = guard_failures(HERE, str(REG.relative_to(HERE)))
    if failures:
        sys.exit('\n'.join(failures))
    return commit


def load_events():
    return json.loads((DATA / f'derived/{DEX}/cascades.json').read_text())[EVENT_SET]


def stress_rates():
    """004's market-wide series (one-second buckets, all coins) and each coin's own series."""
    rows = con().execute(f"""SELECT coin, (t // 1000) * 1000 AS s, SUM(notional) FROM read_parquet('{LIQ}')
                             WHERE t < {drift.HOLDOUT_START} GROUP BY 1, 2 ORDER BY 2, 1""").fetchall()
    per_coin, allb = {}, {}
    for coin, s, n in rows:
        per_coin.setdefault(coin, []).append((s, n))
        allb[s] = allb.get(s, 0.0) + n
    return drift.StressRate(sorted(allb.items())), {c: drift.StressRate(v) for c, v in per_coin.items()}


def first_prints(queries, label='first prints'):
    """queries: [(key, coin, t0, t1)] -> {key: (t, price)} of the FIRST tape print in [t0, t1].
    A price at the time — nothing after t1 is read. Batched by t0's UTC day."""
    by_day = {}
    for q in queries:
        if q[3] >= drift.HOLDOUT_START:
            continue
        by_day.setdefault(q[2] // DAY, []).append(q)
    out = {}
    c = con()
    for n_day, (day, qs) in enumerate(sorted(by_day.items()), 1):
        files = tape._files(DEX, min(q[2] for q in qs), max(q[3] for q in qs))
        if not files:
            continue
        c.execute('CREATE OR REPLACE TEMP TABLE eq(k VARCHAR, coin VARCHAR, t0 BIGINT, t1 BIGINT)')
        c.executemany('INSERT INTO eq VALUES (?, ?, ?, ?)', [(str(k), co, int(a), int(b)) for k, co, a, b in qs])
        rows = c.execute(f"""
            SELECT eq.k, arg_min(epoch_ms(f.timestamp), (epoch_ms(f.timestamp), f.filename, f.file_row_number)),
                   arg_min(CAST(f.price AS DOUBLE), (epoch_ms(f.timestamp), f.filename, f.file_row_number))
            FROM read_parquet({files!r}, filename = true, file_row_number = true) f JOIN eq ON f.coin = eq.coin
            WHERE epoch_ms(f.timestamp) >= eq.t0 AND epoch_ms(f.timestamp) <= eq.t1
            GROUP BY eq.k""").fetchall()
        out.update({k: (t, px) for k, t, px in rows})
        if n_day % 50 == 0:
            print(f'    {label}: day {n_day}/{len(by_day)}', flush=True)
    return out


def entry_prints(events, window=drift.ENTRY_WINDOW):
    """{event index: (t, price)}: first print in [entry_time, entry_time + window]."""
    q = [(str(i), e['coin'], drift.entry_time(e), drift.entry_time(e) + window) for i, e in enumerate(events)]
    return {int(k): v for k, v in first_prints(q, 'entry prints').items()}


def bg_1h_cached(symbol, t_from):
    """{open_ts: close} of Bitget 1H candles from the local cache, blocks from t_from's block up to
    the last block lying wholly before the holdout (a block reaching 2026-07-01 is not opened).
    Cache only: a missing block is an error, never a fetch."""
    out = {}
    st = t_from - t_from % HOUR_BLOCK
    while st + HOUR_BLOCK <= drift.HOLDOUT_START:
        path = CACHE / 'bg-1h' / (hashlib.sha256(f'{symbol}:{st}'.encode()).hexdigest()[:24] + '.json')
        if not path.exists():
            raise SystemExit(f'missing cached 1H block {symbol} {st} — not fetching')
        out.update({r[0]: r[4] for r in json.loads(path.read_text()) if r[0] < drift.HOLDOUT_START})
        st += HOUR_BLOCK
    return out


def hedge_betas(positions):
    """{position index: beta of the coin on BTC} for ETH/SOL positions, from 1H closes before the
    entry (trailing 30 days). Prices before the entry only."""
    alt = [p for p in positions if p['major'] and p['coin'] != HEDGE]
    if not alt:
        return {}
    t_from = min(p['entry'] for p in alt) - (drift.BETA_DAYS + 1) * DAY
    closes = {c: bg_1h_cached(BG_SYMBOL[c], t_from) for c in {p['coin'] for p in alt} | {HEDGE}}
    return {p['i']: drift.beta(closes[p['coin']], closes[HEDGE], p['entry']) for p in alt}


def build(events=None):
    """Positions per variant, skipped events, entry prints. Behaviour only."""
    events = events if events is not None else load_events()
    rate, own = stress_rates()
    prints = entry_prints(events)
    plans = {name: drift.plan(events, rate, prints, MAJORS, own_rates=own if ex_own else None, drop_fallback=drop)
             for name, (ex_own, drop) in VARIANTS.items()}
    return events, plans, prints


def counts(events, positions, skipped):
    def by_u(idx_or_pos, is_pos=False):
        coins = [p['coin'] for p in idx_or_pos] if is_pos else [events[i]['coin'] for i in idx_or_pos]
        maj = sum(1 for c in coins if c in MAJORS)
        return {'all': len(coins), 'majors': maj, 'alts': len(coins) - maj}
    return {'events': by_u(range(len(events))), 'skipped': {k: by_u(v) for k, v in skipped.items()},
            'positions': by_u(positions, True),
            'positions_by_side': {s: by_u([p for p in positions if p['side'] == s], True) for s in ('L', 'S')}}


def exit_prices(queries):
    """Guarded runner only: the tape's last trade at or before each query time, batched by day."""
    by_day = {}
    for x in queries:
        assert x[2] < drift.HOLDOUT_START
        by_day.setdefault(x[2] // DAY, []).append(x)
    out = {}
    for n_day, (day, qs) in enumerate(sorted(by_day.items()), 1):
        out.update(tape.prices_at_ordered(DEX, qs))
        if n_day % 50 == 0:
            print(f'    exit lookups: day {n_day}/{len(by_day)}', flush=True)
    return out


def main():
    if '--events' in sys.argv:
        events, plans, _ = build()
        print(json.dumps({name: counts(events, *plans[name]) for name in plans}, indent=1))
        return
    commit = guard()
    events, plans, _ = build()
    out = {'registration': REG.name, 'registration_commit': commit, 'run_at': dt.datetime.now(dt.UTC).isoformat(),
           'params': {'event_set': EVENT_SET, 'drop_fallback_median': True, 'stress_excludes_own_coin': True,
                      'quiet_ms': drift.QUIET, 'latency_ms': drift.LATENCY,
                      'entry_window_ms': drift.ENTRY_WINDOW, 'hold_ms': drift.HOLD, 'primary': drift.PRIMARY,
                      'horizons': list(drift.HORIZONS), 'taker': drift.TAKER, 'slip': drift.SLIP,
                      'stress_threshold': drift.STRESS_THRESHOLD, 'stress_window_ms': drift.STRESS_WINDOW,
                      'z_hour': drift.Z_HOUR, 'z_day': drift.Z_DAY, 'bar': drift.BAR, 'majors': sorted(MAJORS),
                      'trials': TRIALS, 'cumulative_trials_001_005': CUMULATIVE},
           'counts': {name: counts(events, *plans[name]) for name in plans}}
    # exit prices for every position of every variant (same event -> same fill -> same exit times)
    allpos = {p['i']: p for name in plans for p in plans[name][0]}
    prices = exit_prices(drift.exit_queries(list(allpos.values())))
    rows = {name: drift.outcomes(plans[name][0], prices) for name in plans}
    prim = rows['primary']
    majors = [r for r in prim if r['major']]
    out['T1'] = drift.summarise(majors)
    out['T1']['primary_universe'] = 'majors'
    out['T1']['decision'] = drift.decide(out['T1'])
    # reported, decides nothing
    out['T1']['alts'] = drift.summarise([r for r in prim if not r['major']], placebo_seeds=50)
    out['T1']['all_coins'] = drift.summarise(prim, placebo_seeds=50)
    out['T1']['by_coin'] = {c: drift.summarise([r for r in majors if r['coin'] == c], placebo_seeds=0)['primary_hour']
                            for c in sorted(MAJORS)}
    out['T1']['variants_majors'] = {name: drift.summarise([r for r in rows[name] if r['major']], placebo_seeds=50)
                                    for name in plans if name != 'primary'}
    # ETH/SOL hedged with BTC: BTC entry = first BTC print at or after the position's fill (60 s),
    # BTC exit = BTC's last print at or before the position's exit time
    alt_maj = [r for r in majors if r['coin'] != HEDGE]
    betas = hedge_betas(alt_maj)
    btc_in = first_prints([(str(r['i']), HEDGE, r['entry'], r['entry'] + drift.ENTRY_WINDOW) for r in alt_maj], 'BTC entry')
    btc_out = exit_prices([(f"{r['i']}:{h}", HEDGE, t) for r in alt_maj for h, t in r['exit_t'].items()])
    hedged = []
    for r in alt_maj:
        bi = btc_in.get(str(r['i']))
        ret = {h: drift.hedged_return(r['side'], r['entry_px'], prices.get(f"{r['i']}:{h}"), bi[1] if bi else None,
                                      btc_out.get(f"{r['i']}:{h}"), betas.get(r['i'])) for h in drift.HORIZONS}
        gross = {h: drift.hedged_return(r['side'], r['entry_px'], prices.get(f"{r['i']}:{h}"), bi[1] if bi else None,
                                        btc_out.get(f"{r['i']}:{h}"), betas.get(r['i']), 0.0, 0.0) for h in drift.HORIZONS}
        hedged.append(dict({k: v for k, v in r.items() if k != 'ret_slip'}, ret=ret, gross=gross, beta=betas.get(r['i'])))
    out['T1']['eth_sol_hedged_btc'] = drift.summarise(hedged, placebo_seeds=50)
    out['T1']['eth_sol_unhedged'] = drift.summarise(alt_maj, placebo_seeds=50)
    with open(RESULTS, 'a') as f:
        f.write(json.dumps(out, default=str) + '\n')
    print(json.dumps({k: out['T1'][k] for k in ('primary_hour', 'primary_day', 'criteria', 'pass', 'decision',
                                                'placebo_hour_flip', 'placebo_day_flip')}, indent=1, default=str))


if __name__ == '__main__':
    main()
