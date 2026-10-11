"""Registration 009 (L4) — weekly factor mechanics on SYNTHETIC data only.

    .venv/bin/python -m tests.test_factors

Covers: the split and its embargo; quintile weights; costs (entry, flip, drift, delisting close);
funding signed by position; the planted factor is found; noise loses its costs; the placebo is
calibrated; the largest-positive-month rule; the power simulation; point-in-time features (a
poisoned future changes nothing, and three deliberately leaky queries ARE caught); the delisting
cut (dead-contract bars after the cut never reach a return or a feature) and relisting; the
universe filters; the hard end; the runner's guard in a throwaway git repo.
"""
import csv
import datetime as dt
import json
import math
import random
import statistics
import subprocess
import tempfile
from pathlib import Path

import duckdb

from ct import factors as fx

H, D, W = fx.HOUR, fx.DAY, fx.WEEK


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def day_str(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).strftime('%Y-%m-%d')


# ------------------------------------------------------------------ synthetic weekly worlds (pure)

def synth_world(seed, weeks=107, n=100, ic=0.0, disp=0.08, persist=0.8, cost=0.0009, start=None):
    """universe {S: rows} and outcomes {S: {sym: {R, F, delisted}}}: scores AR(1) across weeks
    (persist), returns = disp * (rho z + sqrt(1 - rho^2) e) with rho = 2 sin(pi ic / 6) on the
    score's normal score — so the weekly Spearman IC is ~ic."""
    rng = random.Random(seed)
    start = start or fx.signal_weeks('discovery')[0]
    syms = [f'C{i:03d}USDT' for i in range(n)]
    sc = {s: rng.gauss(0, 1) for s in syms}
    rho = 2 * math.sin(math.pi * ic / 6)
    uni, outc = {}, {}
    nd = statistics.NormalDist()
    for k in range(weeks):
        S = start + k * W
        sc = {s: persist * v + math.sqrt(1 - persist ** 2) * rng.gauss(0, 1) for s, v in sc.items()}
        rows = [{'S': S, 'symbol': s, 'F1': sc[s], 'F2': -sc[s], 'sigma': 0.03 + 0.02 * (i % 3), 'cost': cost,
                 'tier': 'T3', 'cut_end': start + 10 ** 12} for i, s in enumerate(syms)]
        rk = fx.stats.ranks([r['F1'] for r in rows])
        zs = [nd.inv_cdf((x - 0.5) / n) for x in rk]
        mkt = rng.gauss(0, 0.05)
        outc[S] = {r['symbol']: {'R': mkt + disp * (rho * z + math.sqrt(1 - rho * rho) * rng.gauss(0, 1)),
                                 'F': 0.0, 'delisted': False} for r, z in zip(rows, zs)}
        uni[S] = rows
    return uni, outc


def pure_checks():
    print('split and embargo')
    disc, ha, hb = (fx.signal_weeks(s) for s in ('discovery', 'holdout_A', 'holdout_B'))
    check('discovery: 107 signal weeks, 2021-12-13 .. 2023-12-25', len(disc) == 107 and day_str(disc[0]) == '2021-12-13'
          and day_str(disc[-1]) == '2023-12-25', f'{len(disc)} {day_str(disc[0])} {day_str(disc[-1])}')
    check('discovery: last exit (S + 7 d + 1 h) is before the 2024-01-08 hard end',
          disc[-1] + W + fx.ENTRY_LAG < fx.HARD_END_DISCOVERY)
    check('holdout A: 81 signal weeks, 2024-01-08 .. 2025-07-21', len(ha) == 81 and day_str(ha[0]) == '2024-01-08'
          and day_str(ha[-1]) == '2025-07-21', f'{len(ha)} {day_str(ha[0])} {day_str(ha[-1])}')
    check('embargo: a full week between the last discovery exit and the first holdout-A entry',
          ha[0] + fx.ENTRY_LAG - (disc[-1] + W + fx.ENTRY_LAG) == W)
    check('holdout B ends before 2026-07-01', hb[-1] + W + fx.ENTRY_LAG <= fx._ms(2026, 7, 1), f'{len(hb)} weeks')
    check('signals are Mondays 00:00 UTC', all(dt.datetime.fromtimestamp(s / 1000, dt.UTC).weekday() == 0
                                              and s % D == 0 for s in disc + ha + hb))
    check('Revision 3: the family is F1 alone, one-sided z_crit 1.645', fx.FAMILY == ('F1',)
          and abs(fx.Z_CRIT - 1.6449) < 1e-4)

    print('quintile weights')
    rows = [{'symbol': f'S{i}', 'F1': float(i), 'sigma': 0.01 * (1 + i % 2)} for i in range(10)]
    w = fx.quintile_weights(rows, 'F1')
    check('10 coins -> 2 long (top scores), 2 short (bottom)', sorted(k for k, v in w.items() if v > 0) == ['S8', 'S9']
          and sorted(k for k, v in w.items() if v < 0) == ['S0', 'S1'], str(w))
    check('legs sum to +1 / -1', abs(sum(v for v in w.values() if v > 0) - 1) < 1e-12
          and abs(sum(v for v in w.values() if v < 0) + 1) < 1e-12)
    check('equal risk: weight x sigma equal within a leg', abs(w['S8'] * 0.01 - w['S9'] * 0.02) < 1e-12)
    tie = [{'symbol': f'T{i}', 'F1': float(v), 'sigma': 0.01} for i, v in enumerate([0, 1, 2, 3, 4, 5, 6, 7, 7, 7])]
    wt = fx.quintile_weights(tie, 'F1')
    check('ties at the boundary share the slots (3 coins tied for 2 long slots -> 1/3 each), never by name',
          all(abs(wt[f'T{i}'] - 1 / 3) < 1e-12 for i in (7, 8, 9)) and abs(wt['T0'] + 0.5) < 1e-12
          and abs(wt['T1'] + 0.5) < 1e-12, str(wt))
    check('every coin tied -> no book', fx.quintile_weights([dict(r, F1=0.0) for r in tie], 'F1') == {})
    check('fewer than 5 coins -> no book', fx.quintile_weights(rows[:4], 'F1') == {})

    print('buffered rebalance (Revision 1 primary rule)')
    mkr = lambda sc: [{'symbol': k, 'F1': v, 'sigma': 0.02} for k, v in sc.items()]
    w1, l1, s1 = fx.buffered_targets(mkr({f'C{i}': float(i) for i in range(10)}), 'F1', set(), set())
    check('week 1 from cash: enters the quintiles only (k = 2)', l1 == {'C8', 'C9'} and s1 == {'C0', 'C1'})
    sc2 = {'C0': 2.5, 'C1': 0.0, 'C2': 0.5, 'C3': 3.0, 'C4': 4.0, 'C5': 5.0, 'C6': 9.0, 'C7': 9.5, 'C8': 7.0, 'C9': 6.0}
    w2, l2, s2 = fx.buffered_targets(mkr(sc2), 'F1', l1, s1)
    check('held longs now 3rd/4th (inside the top-40% band of 4) stay; new top two enter -> 4 longs',
          l2 == {'C6', 'C7', 'C8', 'C9'}, str(sorted(l2)))
    check('held short C0 now 3rd lowest stays (band of 4); C2 enters; a non-held 3rd-ranked name never enters',
          s2 == {'C1', 'C2', 'C0'} and 'C5' not in l2, str(sorted(s2)))
    check('equal risk inside a buffered leg (equal sigma -> 1/4 each)', all(abs(w2[x] - 0.25) < 1e-12 for x in l2))
    sc3 = dict(sc2, C9=4.5)
    _, l3, _ = fx.buffered_targets(mkr(sc3), 'F1', l2, s2)
    check('a held long that falls out of the band (to 5th of 10) leaves', 'C9' not in l3 and {'C6', 'C7', 'C8'} <= l3, str(sorted(l3)))
    rng = random.Random(4)
    syms = [f'R{i:02d}' for i in range(53)]
    sc = {x: rng.gauss(0, 1) for x in syms}
    pl, ps, ok_band, ok_entry, n_checked = set(), set(), True, True, 0
    for t in range(300):
        sc = {x: 0.7 * v + 0.71 * rng.gauss(0, 1) for x, v in sc.items()}
        live = [x for x in syms if rng.random() > 0.03]              # a few coins drop out each week
        rows = [{'symbol': x, 'F1': sc[x], 'sigma': 0.01 + 0.01 * (i % 4)} for i, x in enumerate(live)]
        _, L, Sh = fx.buffered_targets(rows, 'F1', pl, ps)
        srt = sorted(live, key=lambda x: sc[x])
        n = len(srt)
        band_hi, band_lo = set(srt[-int(0.4 * n):]), set(srt[:int(0.4 * n)])
        q_hi, q_lo = set(srt[-(n // 5):]), set(srt[:n // 5])
        ok_band &= all(x in L for x in pl if x in band_hi) and all(x in Sh for x in ps if x in band_lo)
        ok_entry &= (L - pl) <= q_hi and (Sh - ps) <= q_lo and L <= band_hi and Sh <= band_lo
        n_checked += len(pl) + len(ps)
        pl, ps = L, Sh
    check('300 random weeks: a held name inside its band is never traded out', ok_band, f'{n_checked} holdings checked')
    check('300 random weeks: entries only from the quintile, holdings never outside the band', ok_entry)

    print('costs, drift, delisting close, funding sign')
    S0 = fx.signal_weeks('discovery')[0]
    mk = lambda S, scores: [{'S': S, 'symbol': s, 'F1': v, 'sigma': 0.02, 'cost': 0.001} for s, v in scores.items()]
    base = {'A': 1.0, 'B': 2.0, 'C': 3.0, 'D': 4.0, 'E': 5.0}
    flip = {k: -v for k, v in base.items()}
    flat = lambda syms, R=0.0, F=0.0, dl=(): {s: {'R': R, 'F': F, 'delisted': s in dl} for s in syms}
    uni = {S0: mk(S0, base), S0 + W: mk(S0 + W, flip), S0 + 2 * W: mk(S0 + 2 * W, flip)}
    oc = {S: flat(base) for S in uni}
    b = fx.run_book(uni, 'F1', oc)
    check('entry from cash: traded 2, cost 2 x rate', abs(b[0]['turnover'] - 2) < 1e-12 and abs(b[0]['cost'] - 0.002) < 1e-12)
    check('full flip: traded 4, cost 4 x rate', abs(b[1]['turnover'] - 4) < 1e-12 and abs(b[1]['cost'] - 0.004) < 1e-12)
    check('unchanged book, no move: cost 0', b[2]['cost'] == 0.0)
    oc2 = {S0: {**flat(base), 'E': {'R': 0.10, 'F': 0.0, 'delisted': False}}, S0 + W: flat(base), S0 + 2 * W: flat(base)}
    uni2 = {S: mk(S, base) for S in uni}
    b2 = fx.run_book(uni2, 'F1', oc2)
    check('drift: a long up 10% is trimmed back by 0.1 x its weight', abs(b2[1]['turnover'] - 0.1) < 1e-12, str(b2[1]))
    check('gross = weight x return (long E up 10% -> +0.10)', abs(b2[0]['gross'] - 0.10) < 1e-12)
    oc3 = {S0: {**flat(base), 'E': {'R': 0.05, 'F': 0.0, 'delisted': True}}, S0 + W: flat(base), S0 + 2 * W: flat(base)}
    b3 = fx.run_book(uni2, 'F1', oc3)
    check('delisted inside the week: earns R to the cut, pays the closing trade (|1.05| x rate)',
          abs(b3[0]['gross'] - 0.05) < 1e-12 and abs(b3[0]['cost'] - (0.002 + 1.05 * 0.001)) < 1e-12, str(b3[0]))
    check('... and is re-bought from cash next week (traded 1)', abs(b3[1]['turnover'] - 1.0) < 1e-12, str(b3[1]))
    ocf = {S0: {s: {'R': 0.0, 'F': -0.002, 'delisted': False} for s in base}}
    bf = fx.run_book({S0: mk(S0, base)}, 'F1', ocf)
    check('funding signed: positive funding costs the long leg and pays the short leg',
          abs(bf[0]['gross']) < 1e-15 and abs(bf[0]['long'] - (-0.002 - 0.001)) < 1e-12
          and abs(bf[0]['short'] - (0.002 - 0.001)) < 1e-12, str(bf[0]))
    ocm = {S0: {k: dict(v) for k, v in flat(base).items()}}
    del ocm[S0]['E']
    bm = fx.run_book({S0: mk(S0, base)}, 'F1', ocm)
    check('no entry print -> that weight is cash, not re-spread', bm[0]['n_long'] == 0 and abs(bm[0]['turnover'] - 1) < 1e-12)

    print('largest positive month, mean/median, halves')
    mb = [{'S': S0 + k * W, 'net': v, 'gross': v, 'cost': 0.0} for k, v in enumerate([-0.5, -0.5, -0.5, 0.01, 0.01, 0.02, 0.01, 0.01])]
    big = fx.largest_positive_month(mb)
    check('a large LOSS month is never the one removed', big is not None and big != fx.month_of(S0), str(big))
    check('no positive month -> nothing removed', fx.largest_positive_month([dict(x, net=-abs(x['net'])) for x in mb]) is None)

    print('planted factor found, noise loses its costs')
    uni, outc = synth_world(1, ic=0.10)
    ev = fx.evaluate(fx.weekly_ic(uni, 'F1', outc), fx.run_book(uni, 'F1', outc))
    check('planted IC 0.10: IC estimate within 0.02', abs(ev['ic_mean'] - 0.10) < 0.02, f"{ev['ic_mean']:.4f}")
    check('planted IC 0.10, 9 bp per side: passes all five criteria', ev['pass'], json.dumps(ev['criteria']))
    evm = fx.evaluate(fx.weekly_ic(uni, 'F2', outc), fx.run_book(uni, 'F2', outc))
    check('the same factor with the wrong sign fails', not evm['pass'] and evm['ic_mean'] < 0)
    passes, nets, costs = 0, [], []
    for s in range(20):
        u0, o0 = synth_world(100 + s, ic=0.0)
        e0 = fx.evaluate(fx.weekly_ic(u0, 'F1', o0), fx.run_book(u0, 'F1', o0))
        passes += e0['pass']
        nets.append(e0['net_mean'])
        costs.append(e0['cost_mean'])
    check('noise (IC 0): 0 of 20 worlds pass', passes == 0, str(passes))
    check('noise: mean net ~ -mean cost (loses its costs)', statistics.fmean(nets) < 0
          and abs(statistics.fmean(nets) + statistics.fmean(costs)) < 0.004,
          f'net {statistics.fmean(nets):.5f} cost {statistics.fmean(costs):.5f}')

    print('placebo calibration (signal shuffled within week, costs kept)')
    u0, o0 = synth_world(7, ic=0.0, n=60)
    pl0 = fx.placebo(u0, 'F1', o0, seeds=200)
    check('null world: placebo pass share <= alpha (0.025)', pl0['pass_share'] <= 0.025, json.dumps(pl0))
    check('null world: |IC t| >= z in a nominal share (<= 12%, ~5% expected)', pl0['ic_abs_t_tail'] <= 0.12, json.dumps(pl0))
    check('null world: shuffled IC t has s.d. near 1 (0.7 - 1.3)', 0.7 <= pl0['ic_t_sd'] <= 1.3, f"{pl0['ic_t_sd']:.3f}")
    u1, o1 = synth_world(8, ic=0.10, n=60)
    pl1 = fx.placebo(u1, 'F1', o1, seeds=100)
    e1 = fx.evaluate(fx.weekly_ic(u1, 'F1', o1), fx.run_book(u1, 'F1', o1))
    check('planted world: shuffling destroys it (pass share 0) while the real signal passes',
          pl1['pass_share'] == 0.0 and e1['pass'], json.dumps(pl1))
    check('shuffled books pay their own (higher) turnover: crit2 share 0', pl1['crit2_share'] == 0.0)

    print('decision and holdout read')
    check('decide: pass AND both pull conditions -> pull holdout A',
          fx.decide({'F1': {'pass': True, 'pull_conditions': {'all': True}}}) == ('pull_holdout_A', ['F1']))
    check('decide: pass but a pull condition unmet (beta-adjusted or 2x slippage) -> no pull',
          fx.decide({'F1': {'pass': True, 'pull_conditions': {'all': False}}}) == ('close_conditions_unmet', ['F1']))
    check('decide: no pass -> close', fx.decide({'F1': {'pass': False}}) == ('close', []))
    uh, oh = synth_world(9, weeks=81, ic=0.10, start=fx.signal_weeks('holdout_A')[0])
    bk2 = fx.run_book({S: [dict(r, cost=0.0013) for r in v] for S, v in uh.items()}, 'F1', oh)
    ok, evh = fx.holdout_confirm(fx.weekly_ic(uh, 'F1', oh), fx.run_book(uh, 'F1', oh), 1, book_2x=bk2)
    check('holdout confirm (Revision 4): without the 2x book it cannot confirm',
          not fx.holdout_confirm(fx.weekly_ic(uh, 'F1', oh), fx.run_book(uh, 'F1', oh), 1)[0])
    neg2 = [dict(b, net=-0.01) for b in bk2]
    check('holdout confirm (Revision 4): a 2x book with a negative mean net blocks the confirm',
          not fx.holdout_confirm(fx.weekly_ic(uh, 'F1', oh), fx.run_book(uh, 'F1', oh), 1, book_2x=neg2)[0])
    check('holdout confirm: one factor -> z 1.645; planted 0.10 confirms', ok and abs(evh['z'] - 1.6449) < 1e-3)
    check('holdout confirm: two factors -> z 1.96', abs(fx.holdout_confirm(fx.weekly_ic(uh, 'F1', oh),
                                                                          fx.run_book(uh, 'F1', oh), 2)[1]['z'] - 1.96) < 1e-3)

    print('power simulation')
    u, _ = synth_world(11, ic=0.0)
    disp = {S: (lambda g: [g.gauss(0, 0.08) for _ in range(100)])(random.Random(S)) for S in u}
    inp = fx.sim_inputs(u, 'F1', disp)
    check('sampling s.d. of the IC = mean 1/sqrt(n - 1)', abs(fx.sampling_sd_ic(inp) - 1 / math.sqrt(99)) < 1e-9)
    hi = fx.simulate_power(inp, 0.10, 0.0, sims=40, seed=1, holdout_weeks=81)
    lo = fx.simulate_power(inp, 0.0, 0.0, sims=40, seed=2)
    check('planted IC recovered on average (copula: 2 sin(pi IC / 6))', abs(hi['mean_ic'] - 0.10) < 0.005, f"{hi['mean_ic']:.4f}")
    check('IC 0.10 with 9 bp costs: joint power high, holdout confirms often', hi['p_pass'] >= 0.9
          and hi['p_holdout_confirm'] >= 0.8, json.dumps(hi))
    check('IC 0: joint pass ~0 (costs)', lo['p_pass'] <= 0.05, json.dumps(lo))
    ut, _ = synth_world(12, ic=0.0)
    for rows in ut.values():                                   # 40% of coins tied at one value (the default rate)
        for i, r in enumerate(rows):
            if i % 5 < 2:
                r['F1'] = 0.0
    inp_t = fx.sim_inputs(ut, 'F1', {S: (lambda g: [g.gauss(0, 0.08) for _ in range(100)])(random.Random(S)) for S in ut})
    rng_t = random.Random(9)
    ics_t = [ic for _ in range(30) for _, ic, _ in fx.sim_once(inp_t, 0.0, 0.0, rng_t)[0]]
    check('heavy ties (40% of coins on one value): the simulated null IC is unbiased (|mean| < 0.003)',
          abs(statistics.fmean(ics_t)) < 0.003, f'{statistics.fmean(ics_t):.5f}')
    ex = fx.extra_for_total(inp, 0.15)
    rng = random.Random(3)
    sds = [statistics.stdev([ic for _, ic, _ in fx.sim_once(inp, 0.0, ex, rng)[0]]) for _ in range(20)]
    check('extra week-level noise gives the requested total IC s.d. (0.15 +- 0.015)',
          abs(statistics.fmean(sds) - 0.15) < 0.015, f'{statistics.fmean(sds):.4f}')


# ------------------------------------------------------------------ SQL worlds (point in time)

def sql_world(seed=0, n_sym=12, days=70, dead=None, relist=None, thin=None, young=None):
    """In-memory DuckDB with bars / funding / mh for n_sym symbols over `days` days (hourly random
    walks, funding every 8 h on the hour, OI hourly). dead: (symbol, cut_day_ms, delivery_hour) — the
    contract's last full day; after delivery its bars continue with cnt 0 and a FAKE x3 price, as
    dead contracts do in the archive, and its funding rows continue too. Returns (con, life, start)."""
    rng = random.Random(seed)
    start = fx.signal_weeks('discovery')[2] - 40 * D          # 40 days of warm-up before a Monday
    con = duckdb.connect()
    con.execute('CREATE TABLE bars(symbol VARCHAR, ts BIGINT, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, '
                'qv DOUBLE, cnt BIGINT, tbq DOUBLE)')
    con.execute('CREATE TABLE funding(symbol VARCHAR, ts BIGINT, rate DOUBLE, interval_h INTEGER)')
    con.execute('CREATE TABLE mh(symbol VARCHAR, ts BIGINT, n INTEGER, oiv_sum DOUBLE, oi_sum DOUBLE, top_acct DOUBLE, '
                'top_pos DOUBLE, global_ratio DOUBLE, taker_sum DOUBLE)')
    life = {}
    bars, fund, mh = [], [], []
    syms = [f'X{i:02d}USDT' for i in range(n_sym)]
    for s in syms:
        p, oi = 100.0, 1000.0
        cut = dead[1] if dead and dead[0] == s else None
        first = start - 60 * D if s != young else start + 20 * D
        for h in range(days * 24):
            t = start + h * H
            if t < first:
                continue
            o = p
            p *= math.exp(rng.gauss(0, 0.01))
            oi *= math.exp(rng.gauss(0, 0.005))
            alive = cut is None or t < cut + dead[2] * H
            px_o, px_c = (o, p) if alive else (3 * o, 3 * p)
            qv = (0.0 if not alive else (2e5 if s == thin else 1e6) * (1 + rng.random()))
            bars.append((s, t, px_o, max(px_o, px_c) * 1.001, min(px_o, px_c) * 0.999, px_c, qv,
                         (100 if alive else 0), qv / 2))
            if t % (8 * H) == 0:
                fund.append((s, t + 7, rng.gauss(0.0001, 0.0002), 8))
            mh.append((s, t, 12, 12 * oi * p, 12 * oi, 1.0, 1.0, 1.0, 12.0))
        last_day = (cut if cut is not None else start + (days - 1) * D)
        segs = [[day_str(first), day_str(last_day)]]
        if relist and relist[0] == s:
            segs = [[day_str(first), day_str(relist[1])], [day_str(relist[2]), day_str(last_day)]]
        life[s] = {'segments': segs, 'first_kline_day': day_str(first)}
    with tempfile.TemporaryDirectory() as tmp:
        for name, data in (('bars', bars), ('funding', fund), ('mh', mh)):
            path = Path(tmp) / f'{name}.csv'
            with open(path, 'w', newline='') as fh:
                csv.writer(fh).writerows(data)
            con.execute(f"INSERT INTO {name} SELECT * FROM read_csv('{path}', header=false)")
    fx.life_table(con, life)
    return con, life, start


def _same(a, b):
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        for k in x:
            u, v = x[k], y[k]
            if isinstance(u, float) or isinstance(v, float):
                if u is None or v is None or not math.isclose(u, v, rel_tol=1e-12, abs_tol=1e-15):
                    return False
            elif u != v:
                return False
    return True


def leaks(sql, S):
    """True if the features at S change when everything stamped at or after S is poisoned."""
    con, _, _ = sql_world(seed=5)
    before = fx.features(con, [S], sql)
    fx.poison_after(con, S)
    after = fx.features(con, [S], sql)
    return not _same(before, after)


def sql_checks():
    print('point in time: poisoning the future changes no feature; leaky queries are caught')
    S = fx.signal_weeks('discovery')[2]
    check('registered feature query: no change when every row stamped >= S is poisoned', not leaks(None, S))
    base = fx.feature_sql()
    adv = fx.ADV_DAYS * D
    leaky_bar = base.replace(f'b.ts >= c.S - {adv} AND b.ts <= c.S - {H}\n    GROUP BY',
                             f'b.ts >= c.S - {adv} AND b.ts <= c.S\n    GROUP BY')
    check('leak 1 (liquidity reads the bar opening AT S) is caught', leaky_bar != base and leaks(leaky_bar, S))
    leaky_f = base.replace(f'f.ts >= c.S - {W} AND f.ts < c.S', f'f.ts > c.S - {W} AND f.ts <= c.S + 7')
    check('leak 2 (funding includes the settlement AT S) is caught', leaky_f != base and leaks(leaky_f, S))
    leaky_r = base.replace(f'max(close) FILTER (WHERE ts = S - {H}) AS c_last', 'max(close) AS c_last').replace(
        f'AND b.ts <= c.S - {H} AND (c.S - {H} - b.ts) % {D} = 0', f'AND b.ts <= c.S + 6 * {D} AND (c.S - {H} - b.ts) % {D} = 0')
    check('leak 4 (a "trailing" return built from post-signal closes) is caught', leaky_r != base and leaks(leaky_r, S))

    print('features and universe')
    con, life, start = sql_world(seed=6, thin='X01USDT', young='X02USDT')
    rows = [fx.derive(r) for r in fx.features(con, [S])]
    by = {r['symbol']: r for r in rows}
    check('every alive symbol has a row', len(rows) == 12)
    check('F1 = -(sum of the 21 settlements in [S - 7 d, S))', by['X00USDT']['fund_n'] == 21
          and abs(by['X00USDT']['F1'] + con.execute(f"SELECT sum(rate) FROM funding WHERE symbol = 'X00USDT' AND ts >= {S - W} "
                                                     f"AND ts < {S}").fetchone()[0]) < 1e-10)
    conm, _, _ = sql_world(seed=6, thin='X01USDT', young='X02USDT')
    conm.execute('DROP TABLE mh')
    rows_m = {r['symbol']: r for r in (fx.derive(x) for x in fx.features(conm, [S]))}
    check('F1 and the universe need no metrics: with the metrics table gone, nothing changes (ICP fix)',
          all(rows_m[x]['F1'] == by[x]['F1'] and rows_m[x]['in_universe'] == by[x]['in_universe'] for x in by))
    check('ex-ante beta to the equal-weight universe is computed from pre-S daily returns',
          by['X00USDT']['beta'] is not None and by['X00USDT']['n_beta'] >= 20)
    check('thin coin (adv < floor) is out of the universe', by['X01USDT']['adv30'] < fx.FLOOR_ADV and not by['X01USDT']['in_universe'])
    check('coin listed < 30 days is out', not by['X02USDT']['listed_ok'] and not by['X02USDT']['in_universe'])
    check('a normal coin is in, tier T4 (adv ~ $36 M)', by['X00USDT']['in_universe'] and by['X00USDT']['tier'] == 'T4',
          f"{by['X00USDT']['adv30']:.3g}")
    rows_ex = [fx.derive(dict(r, symbol='USDCUSDT')) for r in fx.features(con, [S]) if r['symbol'] == 'X00USDT']
    check('excluded names (index composites, the stablecoin) are out', not rows_ex[0]['in_universe'])

    print('outcomes: entry/exit, funding sign, delisting cut, relisting, hard end')
    uni = fx.universe_by_week(rows)
    oc = fx.outcomes(con, uni)
    o = oc[S]['X00USDT']
    p0 = con.execute(f"SELECT open FROM bars WHERE symbol = 'X00USDT' AND ts = {S + H}").fetchone()[0]
    p1 = con.execute(f"SELECT open FROM bars WHERE symbol = 'X00USDT' AND ts = {S + W + H}").fetchone()[0]
    check('R = open(S + 7 d + 1 h) / open(S + 1 h) - 1', abs(o['R'] - (p1 / p0 - 1)) < 1e-12 and o['entry_ts'] == S + H
          and o['exit_ts'] == S + W + H)
    fpaid = con.execute(f"""SELECT sum(f.rate * b.open / {p0}) FROM funding f JOIN bars b ON b.symbol = f.symbol
                            AND b.ts = f.ts - f.ts % {H} WHERE f.symbol = 'X00USDT' AND f.ts > {S + H} AND f.ts <= {S + W + H}""").fetchone()[0]
    check('F = -(funding settled in (entry, exit], price-weighted); 21 settlements', o['n_settle'] == 21
          and abs(o['F'] + fpaid) < 1e-15)
    check('the settlement AT the next Monday 00:00 is in this week\'s hold, not in the next signal',
          con.execute(f"SELECT count(*) FROM funding WHERE symbol = 'X00USDT' AND ts BETWEEN {S + W} AND {S + W + 1000}").fetchone()[0] == 1)

    cut_day = S + 3 * D                                    # Thursday; delivery at 12:00
    con2, life2, _ = sql_world(seed=7, dead=('X03USDT', cut_day, 12), relist=('X04USDT', S - 50 * D, S - 20 * D))
    S2 = S + W
    rows2 = [fx.derive(r) for r in fx.features(con2, [S, S2])]
    by2 = {(r['S'], r['symbol']): r for r in rows2}
    check('dead coin is a candidate at S (alive), absent at the next signal', (S, 'X03USDT') in by2 and (S2, 'X03USDT') not in by2)
    oc2 = fx.outcomes(con2, fx.universe_by_week(rows2))
    d = oc2[S]['X03USDT']
    last_close = con2.execute(f"SELECT close FROM bars WHERE symbol = 'X03USDT' AND ts = {cut_day + 11 * H}").fetchone()[0]
    p0d = con2.execute(f"SELECT open FROM bars WHERE symbol = 'X03USDT' AND ts = {S + H}").fetchone()[0]
    check('delisted inside the week: exit = close of the last traded bar (11:00 on the cut day)',
          d['delisted'] and d['exit_ts'] == cut_day + 12 * H and abs(d['R'] - (last_close / p0d - 1)) < 1e-12, str(d))
    check('the fake x3 dead-contract prices after delivery never reach the return', abs(d['R']) < 0.5)
    n_after = con2.execute(f"SELECT count(*) FROM funding WHERE symbol = 'X03USDT' AND ts > {cut_day + 12 * H} AND ts <= {S + W + H}").fetchone()[0]
    check('funding rows after the cut exist in the archive but are not paid', n_after > 0 and d['n_settle'] < 21, f"{d['n_settle']}")
    check('relisted coin: listing time = relisting day, so < 30 days -> out at S',
          (S, 'X04USDT') in by2 and not by2[(S, 'X04USDT')]['listed_ok'])
    con3, _, _ = sql_world(seed=8)
    con3.execute(f"UPDATE bars SET cnt = 0 WHERE symbol = 'X05USDT' AND ts = {S + H}")
    oc3 = fx.outcomes(con3, fx.universe_by_week([fx.derive(r) for r in fx.features(con3, [S])]))
    check('no trade in the entry bar -> no position (cash)', 'X05USDT' not in oc3[S] and 'X06USDT' in oc3[S])
    try:
        fx.outcomes(con3, fx.universe_by_week([fx.derive(r) for r in fx.features(con3, [S])]), hard_end=S + 3 * D)
        hard = False
    except AssertionError:
        hard = True
    check('an exit at or after the hard end is refused', hard)


# ------------------------------------------------------------------ the runner's guard

def guard_checks():
    """r009_run.guard_failures in a throwaway git repo: (a)-(f) and the status line."""
    import r009_run
    code, data = ('ct/factors.py', 'r009_run.py'), ('data/p.parquet',)
    reg_rel = 'registrations/009-x.md'
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root,
                                        capture_output=True, text=True, check=True)
        git('init', '-q')
        for f, body in (('ct/factors.py', 'x = 1\n'), ('r009_run.py', 'y = 2\n'), ('data/p.parquet', 'p\n'),
                        ('results.jsonl', json.dumps({'registration': '005-y.md'}) + '\n')):
            (root / f).parent.mkdir(parents=True, exist_ok=True)
            (root / f).write_text(body)
        (root / '.gitignore').write_text('data/\n')

        def write_reg(status='REGISTERED'):
            rows = '\n'.join(f'| `{f}` | `{r009_run.sha256(root / f)}` |' for f in (*code, *data))
            (root / reg_rel).parent.mkdir(exist_ok=True)
            (root / reg_rel).write_text(f'# 009\n\n**Status: {status}**\n\n| file | sha256 |\n|---|---|\n{rows}\n')
        fails = lambda: r009_run.guard_failures(root, reg_rel, code, data)
        write_reg()
        git('add', '.gitignore', 'ct', 'r009_run.py', 'results.jsonl')
        git('commit', '-q', '-m', 'code')
        f0, _ = fails()
        check('guard (a)+(c): an untracked registration is refused', any(x.startswith('(a)') for x in f0)
              and any(x.startswith('(c)') for x in f0), str(f0))
        git('add', reg_rel)
        f1, _ = fails()
        check('guard (c): staged but never committed -> refused', any(x.startswith('(c)') for x in f1), str(f1))
        git('commit', '-q', '-m', 'register')
        f2, c2 = fails()
        check('guard: committed, clean, hashes match, no result row -> allowed', f2 == [] and len(c2) == 40, str(f2))
        (root / reg_rel).write_text((root / reg_rel).read_text() + 'edit\n')
        f3, _ = fails()
        check('guard (b): registration modified after commit -> refused', any(x.startswith('(b)') for x in f3), str(f3))
        git('checkout', '--', reg_rel)
        (root / 'ct/factors.py').write_text('x = 3\n')
        f4, _ = fails()
        check('guard (d)+(e): guarded code edited -> not clean and hash mismatch',
              any(x.startswith('(d)') for x in f4) and any(x.startswith('(e) ct/factors.py') for x in f4), str(f4))
        git('checkout', '--', 'ct/factors.py')
        (root / 'data/p.parquet').write_text('changed\n')
        f5, _ = fails()
        check('guard (e): the (untracked) data file changed -> hash mismatch only',
              f5 == ['(e) data/p.parquet does not match its registered SHA-256'], str(f5))
        (root / 'data/p.parquet').write_text('p\n')
        with open(root / 'results.jsonl', 'a') as fh:
            fh.write(json.dumps({'registration': '009-x.md'}) + '\n')
        f6, _ = fails()
        check('guard (f): a result row for this registration already exists -> refused', any(x.startswith('(f)') for x in f6), str(f6))
        git('checkout', '--', 'results.jsonl')
        write_reg(status='DRAFT')
        git('commit', '-q', '-am', 'draft')
        f7, _ = fails()
        check('guard: status DRAFT -> refused', any('not REGISTERED' in x for x in f7), str(f7))
    uni, outc = synth_world(21, ic=0.10, n=60)
    for rows in uni.values():
        for r in rows:
            r['adv30'] = 3e8
    ev, ic_rows, book = r009_run.block(uni, 'F1', outc, fx.Z_CRIT, placebo_seeds=5)
    check('runner block on synthetic data: criteria, legs, turnover and placebo assembled',
          ev['pass'] and ev['turnover_mean'] > 0 and ev['placebo']['seeds'] == 5 and len(book) == 107)
    s0 = fx.with_slippage(uni, 0.0)
    s2 = fx.with_slippage(uni, 2.0)
    r0 = next(iter(s0.values()))[0]
    r2 = next(iter(s2.values()))[0]
    check('slippage stress: 0x leaves the taker fee only, 2x doubles the tier slippage (T2: 2 bp -> 4 bp)',
          abs(r0['cost'] - fx.TAKER) < 1e-15 and abs(r2['cost'] - (fx.TAKER + 0.0004)) < 1e-15)
    real, _ = r009_run.guard_failures(r009_run.HERE, str(r009_run.REG.relative_to(r009_run.HERE)))
    check('the real 009 registration is refused today (DRAFT)', any('not REGISTERED' in x for x in real), str(real[:2]))


# ------------------------------------------------------------------ Revision 3 checks

def revision3_checks():
    print('Revision 3: no read of data/carry/ (000g)')
    import r009_run
    from ct import binance_years as by
    try:
        r009_run.checked(r009_run.HERE / 'data/carry/panel/funding.parquet')
        refused = False
    except PermissionError:
        refused = True
    check('r009_run.checked refuses a data/carry/ path', refused)
    try:
        by.connect(str(r009_run.HERE / 'data/carry/panel'))
        refused2 = False
    except PermissionError:
        refused2 = True
    check('binance_years.connect refuses a data/carry/ panel', refused2)
    files = ('ct/factors.py', 'ct/binance_years.py', 'ct/stats.py', 'r009_run.py', 'r009_pull.py', 'r009_pull_holdout.py',
             'r009_describe.py')
    bad = []
    for f in files:
        for n, line in enumerate((r009_run.HERE / f).read_text().splitlines(), 1):
            if 'data/carry' in line and not any(m in line for m in ('carry-guard', 'never reads', 'no 009 code path reads',
                                                                     'never data/carry', 'is refused (000g)', 'WHITELIST',
                                                                     'whitelist', 'refuses any')):
                bad.append(f'{f}:{n}')
    check('static scan: every mention of data/carry in 009 code is a guard', not bad, str(bad))
    check('every runner data path goes through checked (load_universe, results)', 'checked(panel)' in
          (r009_run.HERE / 'r009_run.py').read_text() and 'checked(RESULTS)' in (r009_run.HERE / 'r009_run.py').read_text())

    print('Revision 3: beta-adjusted net on a planted-beta book (M2)')
    S0 = fx.signal_weeks('discovery')[0]
    rng = random.Random(31)
    syms = [f'B{i:02d}USDT' for i in range(50)]
    beta = {s: 0.4 + 1.2 * i / 49 for i, s in enumerate(syms)}          # score = -beta: long low beta, short high
    uni, outc = {}, {}
    for k in range(60):
        S = S0 + k * W
        uni[S] = [{'S': S, 'symbol': s, 'F1': -beta[s], 'sigma': 0.03, 'cost': 0.0008, 'beta': beta[s], 'adv30': 3e7}
                  for s in syms]
        m = -0.03                                                       # a bear market every week
        outc[S] = {s: {'R': beta[s] * m, 'F': 0.0, 'delisted': False} for s in syms}   # pure beta, mean beta 1
    book = fx.run_book(uni, 'F1', outc)
    mkt = fx.market_returns(uni, outc)
    adj = [x for _, x in fx.beta_adjusted(book, mkt)]
    raw = statistics.fmean(b['net'] for b in book)
    check('no alpha, net beta < 0 in a bear: the raw net is strongly positive', raw > 0.01 and
          statistics.fmean(b['beta_book'] for b in book) < -0.5, f'raw {raw:.4f}')
    check('... and the beta-adjusted net is exactly -costs (no premium)',
          abs(statistics.fmean(adj) + statistics.fmean(b['cost'] for b in book)) < 1e-12, f'{statistics.fmean(adj):.6f}')
    pc = fx.pull_conditions(book, mkt, fx.run_book(fx.with_slippage(uni, 2.0), 'F1', outc))
    check('... so the holdout-pull condition on the beta-adjusted mean fails', not pc['beta_adjusted_mean_pos'], str(pc))
    for S in outc:                                                      # plant alpha in the long leg
        for s in syms:
            if beta[s] < 0.6:
                outc[S][s]['R'] += 0.01
    book_a = fx.run_book(uni, 'F1', outc)
    check('planted alpha survives the adjustment (beta-adjusted mean > 0)',
          statistics.fmean(x for _, x in fx.beta_adjusted(book_a, fx.market_returns(uni, outc))) > 0.003)

    print('Revision 3: the simulation tests a beta-loaded null (M2)')
    d = {S: (lambda g: [g.gauss(0, 0.08) for _ in syms])(random.Random(S)) for S in uni}
    inp = fx.sim_inputs(uni, 'F1', d)
    null_bear = fx.simulate_power(inp, 0.0, 0.0, sims=60, seed=3, mkt=[-0.03])
    check('IC 0 with a net short beta in a bear market: the raw criteria can pass ...', null_bear['p_pass'] > 0.5,
          json.dumps({k: null_bear[k] for k in ('p_pass', 'p_pass_and_pull')}))
    check('Revision 3a: a beta-only book passes discovery but fails the beta-adjusted NW t >= 1.645 pull condition '
          'at about the one-sided rate (<= 10% pull)', null_bear['p_pass'] > 0.9 and null_bear['p_pass_and_pull'] <= 0.10,
          json.dumps({k: null_bear[k] for k in ('p_pass', 'p_pass_and_pull')}))
    null_bear2 = fx.simulate_power(inp, 0.0, 0.0, sims=400, seed=5, mkt=[-0.03])
    rate = null_bear2['p_pass_and_pull'] / max(null_bear2['p_pass'], 1e-9)
    check('... over 400 beta-only histories the false-pull share among passes is near alpha (0.5% - 10%)',
          0.005 <= rate <= 0.10 or null_bear2['p_pass_and_pull'] <= 0.05, f'{rate:.3f}')
    adj_t = [x for _, x in fx.beta_adjusted(book, mkt)]
    check('pull condition uses NW t, not the mean: the planted-alpha book passes it, the beta-only book does not',
          fx.beta_adjusted_ok([x for _, x in fx.beta_adjusted(book_a, fx.market_returns(uni, outc))]) and not fx.beta_adjusted_ok(adj_t))

    print('Revision 3: drift-inclusive turnover equals what the book pays')
    unid = {S0: [{'S': S0, 'symbol': s, 'F1': v, 'sigma': 0.02, 'cost': 0.001, 'ret7_trailing': 0.0} for s, v in
                 zip('ABCDE', (1., 2., 3., 4., 5.))],
            S0 + W: [{'S': S0 + W, 'symbol': s, 'F1': v, 'sigma': 0.02, 'cost': 0.001, 'ret7_trailing': (0.10 if s == 'E' else 0.0)}
                     for s, v in zip('ABCDE', (1., 2., 3., 4., 5.))]}
    tp = fx.turnover_path(unid, 'F1', drift=True)
    tn = fx.turnover_path(unid, 'F1', drift=False)
    oc = {S0: {s: {'R': (0.10 if s == 'E' else 0.0), 'F': 0.0, 'delisted': False} for s in 'ABCDE'},
          S0 + W: {s: {'R': 0.0, 'F': 0.0, 'delisted': False} for s in 'ABCDE'}}
    bk = fx.run_book(unid, 'F1', oc)
    check('a held long up 10%: drift turnover 0.1 = run_book turnover; target-only says 0',
          abs(tp[1][1] - 0.1) < 1e-12 and abs(bk[1]['turnover'] - 0.1) < 1e-12 and tn[1][1] == 0.0)

    print('Revision 3: a metrics-archive hole in a trading coin is bridged (ICP); a dead hole is not')
    S = fx.signal_weeks('discovery')[2]
    con, life, _ = sql_world(seed=7, relist=('X04USDT', S - 30 * D, S - 20 * D))
    life2, merged = fx.bridge_trading_gaps(con, life)
    check('active hole (bars traded) -> merged into one segment', len(life2['X04USDT']['segments']) == 1
          and any(m[0] == 'X04USDT' and m[1] == 'merged' for m in merged), str(merged))
    con.execute(f"UPDATE bars SET cnt = 0 WHERE symbol = 'X04USDT' AND ts >= {S - 29 * D} AND ts < {S - 20 * D}")
    life3, merged3 = fx.bridge_trading_gaps(con, life)
    check('dead hole (cnt 0 throughout) -> stays a relisting', len(life3['X04USDT']['segments']) == 2
          and not any(m[0] == 'X04USDT' for m in merged3), str(merged3))
    con.execute(f"UPDATE bars SET cnt = 100 WHERE symbol = 'X04USDT' AND ts >= {S - 29 * D} AND ts < {S - 25 * D}")
    life4, merged4 = fx.bridge_trading_gaps(con, life)
    check('partly dead hole (ICP-like): the earlier segment extends over the traded days only, the coin stays split',
          len(life4['X04USDT']['segments']) == 2 and life4['X04USDT']['segments'][0][1] == day_str(S - 26 * D), str(life4['X04USDT']))

    print('Revision 4: a missing settlement is missing (m5)')
    con5, _, _ = sql_world(seed=9)
    con5.execute(f"DELETE FROM funding WHERE symbol = 'X07USDT' AND ts BETWEEN {S - 3 * D} AND {S - 3 * D + 8 * H}")
    con5.execute(f"DELETE FROM funding WHERE symbol = 'X08USDT' AND ts BETWEEN {S + 3 * D} AND {S + 3 * D + 8 * H}")
    r5 = {r['symbol']: r for r in (fx.derive(x) for x in fx.features(con5, [S]))}
    check('a settlement missing from the feature week -> no F1 score, counted as missing', r5['X07USDT']['F1'] is None
          and r5['X07USDT']['F1_missing_settlement'] and r5['X06USDT']['F1'] is not None)
    o5 = fx.outcomes(con5, fx.universe_by_week(list(r5.values())))
    check('a settlement missing from the hold -> F is None (never 0) and the coin is cash in the book',
          o5[S]['X08USDT']['F'] is None and o5[S]['X08USDT']['funding_missing'] and 'X08USDT' not in fx.usable(o5)[S]
          and o5[S]['X06USDT']['F'] is not None)

    print('Revision 3: stress-week tiers and the exact hard end')
    r = fx.derive({'S': S, 'symbol': 'Q', 'listing_ts': 0, 'cut_end': S + 10 ** 12, 'adv30': 6e8, 'n_bars30': 720,
                   'adv_b2': 9e7, 'n_b2': 720, 'adv_b3': 4e7, 'n_b3': 300, 'fund7': 0.0021, 'fund_n': 21,
                   'fund_first': S - W, 'fund_last': S - H, 'sigma_d': 0.03, 'n_r': 30, 'c_last': 1.0, 'c_week_ago': 1.0,
                   'beta': 1.1, 'n_beta': 30, 'ar_s2': None})
    st = fx.with_stress_tiers({S: [r]})[S][0]
    check('min 30-day volume of the past 90 days ignores a block with < 25 days of bars; stress tier costs more',
          r['adv_min90'] == 9e7 and r['tier'] == 'T2' and abs(st['cost'] - (fx.TAKER + 0.0004)) < 1e-15)
    check('F1 is rounded to 1e-10 (equal funding rates tie exactly)', r['F1'] == -0.0021)
    con3, _, _ = sql_world(seed=8)
    u3 = fx.universe_by_week([fx.derive(x) for x in fx.features(con3, [S])])
    fx.outcomes(con3, u3, hard_end=S + W + H)
    try:
        fx.outcomes(con3, u3, hard_end=S + W + H - 1)
        tight = False
    except AssertionError:
        tight = True
    check('the hard end is the true last exit (S + 7 d + 1 h): one ms earlier is refused', tight)
    check('discovery last exit is 2024-01-01 01:00', fx.LAST_EXIT_DISCOVERY == fx.signal_weeks('discovery')[-1] + W + H)


def holdout_guard_checks():
    """Revision 4 (review M1): the holdout-A guard in a throwaway repo — the valid path, and the four
    bypasses the second review found, each refused; (f) and (i) over `git log --all`."""
    import r009_run
    code, data = ('ct/factors.py', 'r009_run.py'), ('data/p.parquet',)
    reg_rel = 'registrations/009-x.md'
    good_ev = {'ic_mean': 0.03, 'ic_t': 2.5, 'net_mean': 0.004, 'net_t': 2.2, 'net_median': 0.003, 'halves': [0.003, 0.005],
               'without_largest_positive_month': 0.0035,
               'criteria': {'1_ic_mean_pos_nw_t': True, '2_book_net_pos_nw_t': True, '3_mean_median_same_sign': True,
                            '4_both_halves_positive': True, '5_without_largest_positive_month_positive': True},
               'pass': True, 'pull_conditions': {'beta_adjusted_mean': 0.003, 'beta_adjusted_t': 1.9, 'net_2x_mean': 0.002,
                                                 'all': True}}
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root,
                                        capture_output=True, text=True, check=True)
        git('init', '-q')
        for f, body in (('ct/factors.py', 'x = 1\n'), ('r009_run.py', 'y = 2\n'), ('data/p.parquet', 'p\n'), ('results.jsonl', '')):
            (root / f).parent.mkdir(parents=True, exist_ok=True)
            (root / f).write_text(body)
        (root / '.gitignore').write_text('data/\n')

        def write_reg(extra=''):
            rows = '\n'.join(f'| `{f}` | `{r009_run.sha256(root / f)}` |' for f in (*code, *data))
            (root / reg_rel).parent.mkdir(exist_ok=True)
            (root / reg_rel).write_text(f'# 009\n\n**Status: REGISTERED**\n\n| file | sha256 |\n|---|---|\n{rows}\n{extra}')
        hold = lambda: r009_run.guard_failures(root, reg_rel, code, data, mode='holdout_A')[0]
        disc = lambda: r009_run.guard_failures(root, reg_rel, code, data, mode='discovery')[0]
        write_reg()
        git('add', '.')
        git('commit', '-q', '-m', 'registered')
        reg_commit = git('rev-parse', 'HEAD').stdout.strip()
        chash = {f: r009_run.sha256(root / f) for f in code}
        row = lambda ev, decision: json.dumps({'registration': '009-x.md', 'mode': 'discovery', 'registration_commit': reg_commit,
                                               'code_hashes': chash, 'results': {'F1': ev}, 'decision': decision})
        check('holdout guard: nothing recorded -> refused (g1) and (h\')', any(x.startswith('(g1)') for x in hold()))

        # bypass 1: a forged pull label over failing numbers, uncommitted
        forged = dict(good_ev, **{'pass': False}, net_t=1.0, criteria=dict(good_ev['criteria'], **{'2_book_net_pos_nw_t': False}),
                      pull_conditions=dict(good_ev['pull_conditions'], all=False))
        line1 = row(forged, ['pull_holdout_A', ['F1']])
        (root / 'results.jsonl').write_text(line1 + '\n')
        write_reg(f'\nHoldout A: APPROVED 2026-11-01 row {r009_run.line_sha(line1)}\n')
        h1 = hold()
        check('bypass 1 refused: forged pull label over failing numbers, uncommitted -> (g2) and (g4)',
              any(x.startswith('(g2)') for x in h1) and any(x.startswith('(g4)') for x in h1), str(h1))
        (root / 'results.jsonl').write_text('')
        write_reg()

        # bypass 2: a genuine close row plus a hand-appended bare pull row, both committed
        close_ev = dict(good_ev, **{'pass': False}, net_t=1.0, criteria=dict(good_ev['criteria'], **{'2_book_net_pos_nw_t': False}))
        bare = json.dumps({'registration': '009-x.md', 'mode': 'discovery', 'decision': ['pull_holdout_A', ['F1']]})
        (root / 'results.jsonl').write_text(row(close_ev, ['close', []]) + '\n' + bare + '\n')
        git('commit', '-qam', 'close + bare')
        write_reg(f'\nHoldout A: APPROVED 2026-11-01 row {r009_run.line_sha(bare)}\n')
        git('commit', '-qam', 'approve bare')
        h2 = hold()
        check('bypass 2 refused: a close row plus a bare pull row -> (g1) two discovery rows', any(x.startswith('(g1)') for x in h2), str(h2))

    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root,
                                        capture_output=True, text=True, check=True)
        git('init', '-q')
        for f, body in (('ct/factors.py', 'x = 1\n'), ('r009_run.py', 'y = 2\n'), ('data/p.parquet', 'p\n'), ('results.jsonl', '')):
            (root / f).parent.mkdir(parents=True, exist_ok=True)
            (root / f).write_text(body)
        (root / '.gitignore').write_text('data/\n')
        write_reg2 = lambda extra='': (root / reg_rel).parent.mkdir(exist_ok=True) or (root / reg_rel).write_text(
            '# 009\n\n**Status: REGISTERED**\n\n| file | sha256 |\n|---|---|\n'
            + '\n'.join(f'| `{f}` | `{r009_run.sha256(root / f)}` |' for f in (*code, *data)) + '\n' + extra)
        hold = lambda: r009_run.guard_failures(root, reg_rel, code, data, mode='holdout_A')[0]
        disc = lambda: r009_run.guard_failures(root, reg_rel, code, data, mode='discovery')[0]
        decl = lambda: r009_run.guard_failures(root, reg_rel, code, data, mode='decline')[0]
        write_reg2()
        git('add', '.')
        git('commit', '-q', '-m', 'registered')
        reg_commit = git('rev-parse', 'HEAD').stdout.strip()
        chash = {f: r009_run.sha256(root / f) for f in code}
        good = json.dumps({'registration': '009-x.md', 'mode': 'discovery', 'registration_commit': reg_commit,
                           'code_hashes': chash, 'results': {'F1': good_ev}, 'decision': ['pull_holdout_A', ['F1']]})

        # bypass 4: the approval committed BEFORE the row exists
        write_reg2(f'\nHoldout A: APPROVED 2026-11-01 row {r009_run.line_sha(good)}\n')
        git('commit', '-qam', 'early approval')
        started = json.dumps({'registration': '009-x.md', 'mode': 'discovery_started', 'registration_commit': reg_commit})
        (root / 'results.jsonl').write_text(started + '\n' + good + '\n')
        git('commit', '-qam', 'discovery pass')
        h4 = hold()
        check("bypass 4 refused: approval committed before the row -> (h')", any(x.startswith("(h')") for x in h4), str(h4))
        check('discovery guard: the started marker alone already blocks a re-run (m10)', any(x.startswith('(f)') for x in disc()))
        write_reg2(f'\nHoldout A: APPROVED 2026-11-02 row {r009_run.line_sha(good)}\n')
        git('commit', '-qam', 'approval after the row')
        check('valid path: one committed recomputed-pass row + a later approval quoting its sha256 -> allowed', hold() == [], str(hold()))
        check('decline guard: valid discovery pull row -> the owner may decline (a terminal state, D17)', decl() == [], str(decl()))

        # bypass 3: the pull row survives only as a removed line in history
        (root / 'results.jsonl').write_text(started + '\n')
        git('commit', '-qam', 'row removed')
        h3 = hold()
        check('bypass 3 refused: the row exists only in history (removed) -> (g2) not in HEAD', any(x.startswith('(g2)') for x in h3), str(h3))
        (root / 'results.jsonl').write_text(started + '\n' + good + '\n')
        git('commit', '-qam', 'row restored')

        # (i) and --all: a holdout row on ANOTHER branch still blocks
        git('checkout', '-q', '-b', 'side')
        with open(root / 'results.jsonl', 'a') as fh:
            fh.write(json.dumps({'registration': '009-x.md', 'mode': 'holdout_A_started'}) + '\n')
        git('commit', '-qam', 'holdout started on a side branch')
        git('checkout', '-q', '-')
        check('(i) over --all: a holdout row on another branch refuses the holdout read', any(x.startswith('(i)') for x in hold()), str(hold()))

        # code changed since the discovery run
        (root / 'ct/factors.py').write_text('x = 9\n')
        git('commit', '-qam', 'code edit')
        check('(g3) guarded code changed since the discovery commit -> refused', any(x.startswith('(g3)') for x in hold()))

    print('Revision 4: the whitelist')
    for bad in ('data/carry/panel/funding.parquet', 'data/scout_l2/binance/funding/x.zip', 'data/binance/um/aggTrades/x.zip',
                'data/cache/hl-funding/x.json'):
        try:
            r009_run.checked(r009_run.HERE / bad)
            ok = False
        except PermissionError:
            ok = True
        check(f'whitelist refuses {bad}', ok)
    for good_p in ('data/binance_years/panel/bars_1h.parquet', 'data/binance_years_holdoutA/MANIFEST.tsv',
                   'data/scout_l4/listings/daily_metrics.csv', 'results.jsonl'):
        check(f'whitelist allows {good_p}', r009_run.checked(r009_run.HERE / good_p) is not None)


def main():
    pure_checks()
    sql_checks()
    guard_checks()
    revision3_checks()
    holdout_guard_checks()
    print('factor checks passed')


if __name__ == '__main__':
    main()
