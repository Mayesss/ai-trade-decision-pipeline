"""Registration 007 (Revision 1) — vault-deposit mechanics on SYNTHETIC data only.

    .venv/bin/python -I tests/test_vaults.py        (from research/copytrade)

Covers: NAV-per-share reconstruction with mid-hold deposits / withdrawals (error vs the exact unit
ledger); a distribution is an OUTFLOW (partial distributions keep the vault in, no truncation);
behavioural closure; no grid point before S ever reaches an outcome (property test) and the
snapshot entry fallback; unmeasurable imputation and the 5% stop; profit share and exit cost;
eligibility with no look-ahead (features SQL, activity and outside-depositor rules on synthetic
parquet / ledgers); the NAV-identity and profit-share gates; IC / Fisher Z / window-and-leader
clustered Z / paired top-minus-all with the leader-clustered bootstrap; the largest-vault removal;
the score-shuffle placebo; the factor-aware null and power; the cutoff-filtered BTC loader; the
holdout-safe loaders; the manifest overwrite refusal; the guard (incl. HEAD results and the
manifest digest); the runner's import closure; the runner assembly.
"""
import datetime as dt
import json
import math
import random
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from ct import vaults as V  # noqa: E402

D, H = V.DAY, V.HOUR
S = dt.date(2025, 12, 22)
S_MS = V.ms(S)
END_MS = S_MS + V.HOLD_DAYS * D


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


# ------------------------------------------------------------------ a synthetic vault

def synth_vault(rng, vol=0.03, drift=0.0, flow_per_day=0.3, flow_frac=0.25, grid_days=7, grid_hour=21,
                dist_day=None, dist_frac=1.0, start_days=-40, end_days=80, grid_phase=None, equity=1e6):
    """Hourly NAV path through the exact unit ledger. At each hour t, in order: the distribution
    (if due; frac 1 closes the vault), a random flow (deposit / withdrawal up to flow_frac of
    equity), the NAV mark price[t] and per-unit cash paid so far paid[t], the allTime grid sample
    (t, av, pnl) every grid_days at grid_hour UTC (random phase), then the hour's trading return.
    Returns (grid, flows, price, paid, events, dist_ms, exit_price)."""
    uv = V.UnitVault(equity)
    t0 = S_MS + start_days * D
    steps = int((end_days - start_days) * 24)
    hv, hd = vol / math.sqrt(24), drift / 24
    phase = rng.randrange(grid_days) if grid_phase is None else grid_phase
    grid, flows, price, paid, events = [], [], {}, {}, []
    idx, cum = {}, 1.0                    # time-weighted (distribution-reinvested) trading index
    due = S_MS + int(dist_day * D) if dist_day is not None else None
    dist_ms = exit_price = None
    for k in range(steps + 1):
        t = t0 + k * H
        if due is not None and t >= due and dist_ms is None:
            exit_price = uv.price
            amt = uv.distribute(dist_frac)
            flows.append((t, -amt))
            events.append({'time': t, 'delta': {'type': 'vaultDistribution', 'vault': '0xv', 'usdc': str(amt)}})
            dist_ms = t
        if not uv.closed:
            if k and rng.random() < flow_per_day / 24:
                f = uv.e * flow_frac * rng.uniform(-1, 1)
                if uv.e + f > 0:
                    uv.flow(f)
                    flows.append((t, f))
                    events.append({'time': t, 'delta': {'type': 'vaultDeposit' if f > 0 else 'vaultWithdraw', 'vault': '0xv',
                                                        **({'usdc': str(f)} if f > 0 else {'requestedUsd': str(-f), 'user': '0xu'})}})
            price[t] = uv.price
            paid[t] = uv.paid
            idx[t] = cum
        day = (t - t0) // D
        if (t % D) == grid_hour * H and (day + phase) % grid_days == 0:
            grid.append((t, uv.e, uv.pnl))
        if not uv.closed:
            r_ = hd + hv * rng.gauss(0, 1)
            uv.trade(r_)
            cum *= 1 + r_
    paid['idx'] = idx
    return grid, flows, price, paid, events, dist_ms, exit_price


def wealth_return(price, paid, t_in, t_out):
    """Exact depositor return per unit from t_in to t_out: NAV plus cash distributed meanwhile."""
    return (price[t_out] + paid[t_out] - paid[t_in]) / price[t_in] - 1


def rm(e):
    return math.sqrt(statistics.fmean(x * x for x in e))


def nav_checks():
    print('NAV per share (Modified Dietz over the grid, unscaled)')
    rng = random.Random(1)
    g, fl, price, paid, _, _, _ = synth_vault(rng, flow_per_day=0.0)
    res = V.hold_return(g, fl, S_MS)
    exact = price[res['exit_t']] / price[res['entry_t']] - 1
    check('no flows: chained return = exact NAV ratio over the measured span', abs(res['gross'] - exact) < 1e-12)
    check('entry in [S, S + 10 d], exit at or before S + 56 d',
          S_MS <= res['entry_t'] <= S_MS + 10 * D and res['exit_t'] <= END_MS)
    g, fl, price, paid, _, _, _ = synth_vault(rng, flow_per_day=1.0, flow_frac=0.5)
    a, b = V.hold_return(g, fl, S_MS), V.hold_return(g, fl, S_MS, numerator='av')
    check('pnl and av-minus-flows numerators agree with a complete ledger', abs(a['log'] - b['log']) < 1e-9)
    for grid_days, fpd, ff in ((7, 0.3, 0.25), (7, 1.0, 0.5), (14, 0.3, 0.25)):
        err_span, err_mid, truth, est, unmeas = [], [], [], [], 0
        for i in range(300):
            r = random.Random(1000 + i)
            g, fl, price, paid, _, _, _ = synth_vault(r, grid_days=grid_days, flow_per_day=fpd, flow_frac=ff)
            res = V.hold_return(g, fl, S_MS)
            if 'unmeasurable' in res:
                unmeas += 1
                continue
            tr = price[res['exit_t']] / price[res['entry_t']] - 1
            err_span.append(res['gross'] - tr)
            err_mid.append(V.hold_return(g, None, S_MS)['gross'] - tr)
            truth.append(price[END_MS] / price[S_MS] - 1)
            est.append(res['gross'])
        sp = V.stats.spearman(est, truth)
        print(f'    grid {grid_days} d, flows {fpd}/day up to {ff:.0%}: error on the measured span rmse {rm(err_span):.4f} '
              f'bias {statistics.fmean(err_span):+.4f}; flows unknown rmse {rm(err_mid):.4f}; '
              f'Spearman(estimate, true 56-day return) {sp:.3f}; unmeasurable (no grid point in [S, S + 10 d]) {unmeas}/300')
        if grid_days == 7 and fpd == 0.3:
            check('ledger-timed Modified Dietz: rmse < 10% of the true sd; unbiased within 0.2%; timing helps',
                  rm(err_span) < 0.1 * statistics.stdev(truth) and abs(statistics.fmean(err_span)) < 0.002
                  and rm(err_span) < rm(err_mid))
            check('attenuation constant: Spearman(estimate, true 56-day return) ~ ATTENUATION (+-0.03)',
                  abs(sp - V.ATTENUATION) < 0.03, f'{sp:.3f} vs {V.ATTENUATION}')
    g, fl, price, paid, _, _, _ = synth_vault(random.Random(7), vol=0.0, flow_per_day=2.0, flow_frac=0.8)
    check('flows alone (zero trading return) give a zero depositor return', abs(V.hold_return(g, fl, S_MS)['gross']) < 1e-12)
    g, fl, price, paid, _, _, _ = synth_vault(random.Random(8), vol=0.0, drift=0.002, flow_per_day=0.5, flow_frac=0.5)
    res = V.hold_return(g, fl, S_MS, scale=True)
    check('scaled variant: constant drift with flows recovers the exact 56-day return (within 0.2%)',
          abs(res['gross'] - (price[END_MS] / price[S_MS] - 1)) < 0.002)


def distribution_outflow_checks():
    print('a distribution is an OUTFLOW (Revision 1, B1)')
    errs, gap = [], []
    for i in range(150):
        r = random.Random(3000 + i)
        dday = r.uniform(2, 50)
        g, fl, price, paid, ev, dist, _ = synth_vault(r, dist_day=dday, dist_frac=r.uniform(0.2, 0.8))
        res = V.hold_return(g, fl, S_MS)
        if i == 0:
            check('a partial distribution does not close or truncate: the span runs to the hold end',
                  not res['closed'] and res['span_days'] > 40, f'span {res["span_days"]:.1f} d')
        idx = paid['idx']
        twr = idx[res['exit_t']] / idx[res['entry_t']] - 1
        errs.append(res['gross'] - twr)
        gap.append(twr - wealth_return(price, paid, res['entry_t'], res['exit_t']))
    print(f'    150 vaults paying out 20-80% of equity 2-50 days into the hold, still trading: error vs the exact '
          f'time-weighted return (payout reinvested) rmse {rm(errs):.4f}, bias {statistics.fmean(errs):+.4f}; '
          f'definitional gap time-weighted minus cash-held wealth return rmse {rm(gap):.4f} (stated in §4, not an error)')
    check('partial distributions (one payout of 20-80% of equity): rmse < 5%, |bias| < 0.5% vs the time-weighted truth',
          rm(errs) < 0.05 and abs(statistics.fmean(errs)) < 0.005)
    # a distribution 2 days after S, before the first grid point: no pre-S point, full span measured
    g, fl, price, paid, ev, dist, _ = synth_vault(random.Random(5), dist_day=2.0, dist_frac=0.5, grid_phase=0)
    res = V.hold_return(g, fl, S_MS)
    check('distribution before the first in-hold grid point: entry still at or after S, outcome measured',
          'gross' in res and res['entry_t'] >= S_MS and not res['closed'])
    flows = V.ledger_flows(ev, '0xv')
    check('ledger_flows signs a distribution as -usdc', any(f < 0 for t, f in flows if t == dist))


def exhaustion_checks():
    print('in-hold exhaustion and thin base (Revision 2, M-b)')
    g, fl, price, paid, ev, dist, xp = synth_vault(random.Random(11), dist_day=30.3, flow_per_day=0.0)
    res = V.hold_return(g, fl, S_MS, equity_at_s=1e6)
    check('full payout: exit at the payout, exact return from the entry to the payout',
          res['closed'] and res['exit_reason'] == 'exhausted' and res['exit_t'] == dist
          and abs(res['gross'] - (xp / price[res['entry_t']] - 1)) < 1e-9, f'{res.get("exit_reason")}')
    # the same vault restarts with new money 10 days later and trades on (inside the hold)
    restart = dist + 10 * D
    g2 = [(t, av if t < restart else 5e5 * (1 + 0.01 * ((t - restart) // D)), pnl if t < restart else pnl + 5e3 * ((t - restart) // D))
          for t, av, pnl in g]
    fl2 = fl + [(restart, 5e5)]
    r2 = V.hold_return(g2, fl2, S_MS, equity_at_s=1e6)
    check('full payout then a restart inside the hold: the depositor still exits at the payout (same return)',
          r2['exit_t'] == dist and abs(r2['gross'] - res['gross']) < 1e-12)
    # a payout one day into a 7-day interval: the interval return is NOT scaled up by ~7
    t0 = S_MS + 3 * D
    grid = [(t0, 1e6, 0.0), (t0 + 7 * D, 0.0, 2e4)]                  # +2% earned before a full payout on day 1
    flows = [(t0 + 1 * D, -1.02e6)]
    r3 = V.hold_return(grid, flows, S_MS, equity_at_s=1e6)
    check('payout one day into an interval: +2% (base = AV at the start, not AV x 1/7)',
          r3['closed'] and abs(r3['gross'] - 0.02) < 1e-12, f'{r3.get("gross")}')
    # thin base: an interval starting under $1k ends the measurement at its start
    grid = [(S_MS + 3 * D, 1e5, 0.0), (S_MS + 10 * D, 500.0, -9.95e4 + 0.0), (S_MS + 17 * D, 5e5, 4e5)]
    r4 = V.hold_return(grid, [], S_MS, equity_at_s=1e5)
    check('thin base: measurement ends at the start of the interval that begins under $1k (no dust returns)',
          r4['exit_reason'] == 'thin_base' and r4['exit_t'] == S_MS + 10 * D and abs(r4['gross'] - (-0.995)) < 1e-9, str(r4))
    check('thin base at the entry: unmeasurable', V.hold_return([(S_MS + 3 * D, 500.0, 0.0), (S_MS + 10 * D, 600.0, 100.0)], [], S_MS)
          == {'unmeasurable': 'thin_base_at_entry'})
    # exhaustion needs an outflow: a vault that loses everything trading is a loss, not an exit
    r5 = V.hold_return([(S_MS + 3 * D, 1e5, 0.0), (S_MS + 10 * D, 50.0, -99950.0), (S_MS + 17 * D, 50.0, -99950.0)], [], S_MS, equity_at_s=1e5)
    check('equity to ~0 without an outflow: a trading loss (-99.95%), not an exhaustion exit',
          not r5['closed'] and abs(r5['gross'] + 0.9995) < 1e-9, str(r5))
    check('exhaustion threshold max($100, 1% of equity at S)', V.exhaustion_threshold(1e6) == 1e4 and V.exhaustion_threshold(5e3) == 100.0)


def post_hold_checks():
    print('no post-hold data reaches an outcome (Revision 2, M-b)')
    bad = 0
    for i in range(300):
        r = random.Random(7000 + i)
        g, fl, price, paid, ev, dist, _ = synth_vault(r, dist_day=r.choice([None, r.uniform(5, 50), r.uniform(57, 75)]),
                                                     dist_frac=r.choice([1.0, 0.5]), flow_per_day=0.3)
        base = V.hold_return(g, fl, S_MS, equity_at_s=1e6)
        g_x = [(t, av * (r.uniform(0, 3) if t > END_MS else 1), pnl + (r.uniform(-1e6, 1e6) if t > END_MS else 0))
               for t, av, pnl in g] + [(END_MS + 1, 0.0, 0.0)]
        fl_x = fl + [(END_MS + D, -1e9), (END_MS + 1, 5e5)]
        alt = V.hold_return(g_x, fl_x, S_MS, equity_at_s=1e6)
        if base != alt:
            bad += 1
    check('300 vaults: perturbing every grid point and flow after S + 56 d changes no outcome', bad == 0, f'{bad}')


def no_pre_s_checks():
    print('never a point before S (Revision 1, B1)')
    bad = 0
    for i in range(500):
        r = random.Random(5000 + i)
        grid = sorted({(S_MS + int(r.uniform(-30, 70) * D), r.uniform(1e4, 1e6), r.uniform(-1e5, 1e5)) for _ in range(r.randrange(0, 12))})
        snaps = sorted((S_MS + int(r.uniform(-5, 15) * D), r.uniform(1e4, 1e6)) for _ in range(r.randrange(0, 3)))
        pts, _ = V.measure_points(grid, S_MS, snaps=snaps)
        if pts is not None and any(p[0] < S_MS or p[0] > END_MS for p in pts):
            bad += 1
    check('500 random grids and snapshots: no measured point before S or after S + 56 d', bad == 0, f'{bad}')
    pre = [(S_MS - 4 * D, 1e6, 0.0)]
    later = [(S_MS + 12 * D, 1.1e6, 1e5), (S_MS + 19 * D, 1.2e6, 2e5)]
    res = V.hold_return(pre + later, [], S_MS)
    check('no grid point in [S, S + 10 d] and no snapshot: unmeasurable (no fallback to S - 4 d)',
          res == {'unmeasurable': 'no_entry'}, str(res))
    snaps = [(S_MS + 1 * D, 1.0e6)]
    a = V.hold_return(pre + later, [], S_MS, snaps=snaps)
    b = V.hold_return([(S_MS - 4 * D, 9e9, 9e9)] + later, [], S_MS, snaps=snaps)
    check('snapshot entry: first snapshot in [S, S + 10 d]; a poisoned pre-S point changes nothing',
          a['entry_source'] == 'snapshot' and a['entry_t'] == S_MS + D and abs(a['gross'] - b['gross']) < 1e-12)
    check('snapshot entry interval uses AV minus the ledger flows: (1.2 - 1.0) / 1.0 = +20%', abs(a['gross'] - 0.2) < 1e-12)
    check('snapshot entry without a ledger (flows unknown): unmeasurable',
          'unmeasurable' in V.hold_return(pre + later, None, S_MS, snaps=snaps))


def impute_checks():
    print('unmeasurable: imputation and the 5% stop')
    win = {0: [{'vault': f'v{i}', 'score': i, 'net': float(i), 'gross': float(i), 'control': 0.0} for i in range(9)]
           + [{'vault': 'vx', 'score': 99, 'net': None, 'gross': None, 'control': 0.0}]}
    w, n = V.impute_unmeasurable(win)
    row = [r for r in w[0] if r['vault'] == 'vx'][0]
    check('imputed at the within-window median of measured rows, flagged', n == 1 and row['net'] == 4.0 and row.get('imputed'))
    check('gate: 5 of 100 passes, 6 of 100 stops', V.unmeasurable_gate(5, 100) and not V.unmeasurable_gate(6, 100))
    w2, _ = V.impute_unmeasurable(win, mode='min')
    check('worst-case variant imputes the window minimum', [r for r in w2[0] if r['vault'] == 'vx'][0]['net'] == 0.0)
    check('market-adjusted outcome: net - beta x BTC simple return', abs(V.market_adjusted(0.05, 2.0, 100.0, 90.0) - 0.25) < 1e-12)
    hg = [(S_MS + 3 * D, 1, 0), (S_MS + 17 * D, 1, 0), (S_MS + 31 * D, 1, 0), (S_MS + 45 * D, 1, 0), (S_MS + 59 * D, 1, 0)]
    check('common span: HLP\'s first point in [S, S + 14 d] to its last at or before S + 56 d',
          V.common_span(hg, S_MS, 14) == (S_MS + 3 * D, S_MS + 45 * D) and V.common_span(hg[1:], S_MS, 10) is None)


def profit_share_checks():
    print('profit share and exit cost')
    check('10% of a positive profit; losses not shared',
          abs(V.net_of_profit_share(0.20) - 0.18) < 1e-12 and V.net_of_profit_share(-0.10) == -0.10)
    g, fl, price, paid, _, _, _ = synth_vault(random.Random(9), vol=0.0, drift=0.003, flow_per_day=0.0)
    r0, r1 = V.hold_return(g, fl, S_MS), V.hold_return(g, fl, S_MS, exit_cost=0.004)
    check('hold_return: net = 0.9 x gross for a gain; exit cost subtracted from net',
          abs(r0['net'] - 0.9 * r0['gross']) < 1e-12 and abs(r1['net'] - (r0['net'] - 0.004)) < 1e-12)
    ev = [{'time': S_MS - 5 * D, 'delta': {'type': 'vaultWithdraw', 'user': '0xu', 'requestedUsd': '1000', 'closingCost': '2'}},
          {'time': S_MS - 4 * D, 'delta': {'type': 'vaultWithdraw', 'user': '0xu', 'requestedUsd': '1000', 'closingCost': '6'}},
          {'time': S_MS - 3 * D, 'delta': {'type': 'vaultWithdraw', 'user': '0xl', 'requestedUsd': '1000', 'closingCost': '50'}},
          {'time': S_MS + 3 * D, 'delta': {'type': 'vaultWithdraw', 'user': '0xu', 'requestedUsd': '1000', 'closingCost': '90'}}]
    check('exit cost: median closingCost / requested of non-leader lookback withdrawals (post-S and leader ignored)',
          abs(V.exit_cost(ev, '0xl', S_MS) - 0.004) < 1e-12)


def gate_checks():
    print('gates (NAV identity, profit share)')
    g, fl, price, paid, ev, _, _ = synth_vault(random.Random(21), flow_per_day=0.12, flow_frac=0.3)
    for e in ev:
        if e['delta']['type'] == 'vaultWithdraw':
            e['delta']['netWithdrawnUsd'] = str(0.9 * float(e['delta']['requestedUsd']))
    ch = {f: V.identity_checks(g, ev, '0xv', S_MS - 40 * D, S_MS + 80 * D, f) for f in V.WITHDRAW_CANDIDATES}
    ok, field, shares = V.identity_gate(ch)
    check('identity gate passes and picks requestedUsd when that is what leaves the vault',
          ok and field == 'requestedUsd' and shares['requestedUsd']['flow'] == 1.0 and shares['netWithdrawnUsd']['flow'] < 0.9, str(shares))
    bad = [(t, av, pnl * 1.05) for t, av, pnl in g]
    ok2, _, _ = V.identity_gate({f: V.identity_checks(bad, ev, '0xv', S_MS - 40 * D, S_MS + 80 * D, f) for f in V.WITHDRAW_CANDIDATES})
    check('a pnl series that is not AV minus net flows fails the gate', not ok2)
    check('the gate reads only intervals inside [lo, hi)', V.identity_checks(g, ev, '0xv', S_MS, S_MS + 1, 'requestedUsd') == [])
    w = [{'time': S_MS - 2 * D, 'delta': {'type': 'vaultWithdraw', 'user': '0xu', 'requestedUsd': '1200', 'basis': '1000', 'commission': '20'}},
         {'time': S_MS - 2 * D, 'delta': {'type': 'vaultWithdraw', 'user': '0xu', 'requestedUsd': '800', 'basis': '1000', 'commission': '0'}},
         {'time': S_MS - 2 * D, 'delta': {'type': 'vaultWithdraw', 'user': '0xu', 'requestedUsd': '1200', 'basis': '1000', 'commission': '5'}},
         {'time': S_MS - 2 * D, 'delta': {'type': 'vaultWithdraw', 'user': '0xl', 'requestedUsd': '1200', 'basis': '1000', 'commission': '0'}}]
    check('commission check: 10% of profit ok, no profit no fee ok, a wrong fee flagged, the leader skipped',
          V.commission_checks(w, '0xl', S_MS - 91 * D, S_MS) == [True, True, False])


# ------------------------------------------------------------------ eligibility on synthetic parquet

def eligibility_checks():
    print('eligibility (no look-ahead; activity and outside depositors)')
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    lb = S - dt.timedelta(days=V.LOOKBACK_DAYS)
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / 'fills'
        rows = {}

        def add_trip(addr, t_open, hours, maker=False):
            for t, side, sp in ((t_open, 'buy', 0.0), (t_open + hours * H, 'sell', 1.0)):
                day = dt.datetime.fromtimestamp(t / 1000, dt.UTC).date()
                rows.setdefault(day, []).append({'address': addr, 'coin': 'BTC', 'timestamp': t, 'side': side, 'size': 1.0,
                                                 'price': 100.0, 'start_position': sp, 'realized_pnl': 0.0,
                                                 'trade_id': sum(len(v) for v in rows.values()), 'direction': 'x',
                                                 'twap_id': None, 'is_liquidation': False, 'crossed': not maker, 'dex': 'hyperliquid'})
        base = V.ms(lb) + 2 * D
        for k in range(25):
            add_trip('0xa', base + k * 3 * D, 5)
        add_trip('0xa', S_MS - 2 * D, 5)                      # A: active in the last 7 days
        for k in range(19):
            add_trip('0xb', base + k * 3 * D, 5)
        for k in range(6):
            add_trip('0xb', S_MS + 1 * D + k * D, 5)          # B: 19 lookback trips + 6 after S
        add_trip('0xb', S_MS - 2 * H, 5)                      #    + one open at S
        for k in range(25):
            add_trip('0xc', base + k * 3 * D, 5)              # C: only a stale / future snapshot
        add_trip('0xc', S_MS - D, 2)
        for k in range(25):
            add_trip('0xe', base + k * 3 * D, 5, maker=True)  # E: maker-heavy
        for k in range(25):
            add_trip('0xi', base + k * 2 * D, 5)              # I: last fill > 7 d before S, then fills after S
        add_trip('0xi', S_MS + 2 * H, 1)
        schema = pa.schema([('address', pa.string()), ('coin', pa.string()), ('timestamp', pa.timestamp('ms', tz='UTC')),
                            ('side', pa.string()), ('size', pa.float64()), ('price', pa.float64()),
                            ('start_position', pa.float64()), ('realized_pnl', pa.float64()), ('trade_id', pa.uint64()),
                            ('direction', pa.string()), ('twap_id', pa.uint64()), ('is_liquidation', pa.bool_()),
                            ('crossed', pa.bool_()), ('dex', pa.string())])
        for day, rs in rows.items():
            p = root / f'date={day.isoformat()}' / 'fills.parquet'
            p.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rs, schema=schema), p)
        snaps = Path(d) / 'snaps.parquet'
        sr = [('0xa', S_MS - 2 * D, 50_000.0, 10_000.0), ('0xb', S_MS - 2 * D, 50_000.0, 0.0),
              ('0xc', S_MS - 9 * D, 50_000.0, 0.0), ('0xc', S_MS + H, 50_000.0, 0.0),
              ('0xe', S_MS - D, 50_000.0, 0.0), ('0xi', S_MS - D, 50_000.0, 0.0),
              ('0xa', S_MS + D, 50_000.0, -99_999.0), ('0xa', V.CUTOFF_MS + D, 1.0, 1.0)]
        pq.write_table(pa.table({'snapshot_ms': [r[1] for r in sr], 'user': [r[0] for r in sr],
                                 'account_value': [r[2] for r in sr], 'gross_notional': [abs(r[3]) for r in sr],
                                 'net_notional': [r[3] for r in sr]}), snaps)
        c = duckdb.connect()
        addrs = ['0xa', '0xb', '0xc', '0xe', '0xi']
        files = V.day_files(lb, S + dt.timedelta(days=30), root=root)
        check('day_files never lists a file on or after the cutoff',
              all('date=2026-07' not in f for f in V.day_files(dt.date(2026, 6, 25), dt.date(2026, 7, 10), root=root)))
        feats = {r['address']: r for r in V.features_sql(c, files, str(snaps), S_MS, V.ms(lb), addrs)}
        act = {}
        for a, day, n, first, last in V.activity_sql(c, files, addrs, V.READ_END_MS):
            act.setdefault(a, []).append((day, n, first, last))
        lf = {a: V.last_fill_before(act.get(a, []), S_MS) for a in addrs}
        check('trips after S are not counted (0xb: 19)', feats['0xb']['round_trips'] == 19)
        check('the snapshot after S is not used (0xc: equity age > 7 d)', -feats['0xc']['equity_age_h'] > 168)
        check('last fill before S ignores fills after S (0xi)', lf['0xi'] is not None and lf['0xi'] < S_MS - 7 * D)
        ok = {a: V.eligible(feats[a], S_MS, lf[a]) for a in addrs}
        check('0xa eligible', ok['0xa'][0], str(ok['0xa']))
        check('0xb: trips', ok['0xb'] == (False, 'trips'))
        check('0xc: equity age', ok['0xc'] == (False, 'equity age'))
        check('0xe: maker share', ok['0xe'] == (False, 'maker share'))
        check('0xi: inactive (no fill in the 7 days before S; its fill after S does not count)', ok['0xi'] == (False, 'inactive'))
        check('a vaultDistribution before S does NOT exclude (Revision 1)',
              V.eligible(feats['0xa'], S_MS, lf['0xa'])[0] and 'distribution' not in V.eligible.__code__.co_varnames)
        ne = V.net_exposure_sql(c, str(snaps), S_MS, V.ms(lb), addrs)
        check('net exposure: lookback snapshots only (0xa: 0.2; the post-S and holdout rows unread)', abs(ne['0xa'] - 0.2) < 1e-12)
        ev = [{'time': S_MS - 10 * D, 'delta': {'type': 'vaultWithdraw', 'user': '0xL', 'requestedUsd': '1'}},
              {'time': S_MS + 1 * D, 'delta': {'type': 'vaultWithdraw', 'user': '0xu', 'requestedUsd': '1'}},
              {'time': S_MS - 100 * D, 'delta': {'type': 'vaultWithdraw', 'user': '0xu', 'requestedUsd': '1'}}]
        check('outside depositor: leader, post-S and pre-lookback withdrawals do not count',
              not V.outside_depositor(ev, '0xl', S_MS))
        ev.append({'time': S_MS - 3 * D, 'delta': {'type': 'vaultWithdraw', 'user': '0xu', 'requestedUsd': '1'}})
        check('outside depositor: a non-leader withdrawal in the lookback counts', V.outside_depositor(ev, '0xl', S_MS))
        check('require_outside excludes a vault without one',
              V.eligible(feats['0xa'], S_MS, lf['0xa'], False, require_outside=True) == (False, 'no outside depositor'))


# ------------------------------------------------------------------ statistics

def synth_windows(rng, n=(100, 80, 65, 74), ic=0.0, vault_effect=0.0, pool=200, sd=0.3, leaders=None):
    eff = {f'v{i}': rng.gauss(0, 1) for i in range(pool)}
    out = {}
    for w, k in enumerate(n):
        rows = []
        for v in rng.sample(sorted(eff), k):
            sc = rng.gauss(0, 1)
            x = sd * (ic * sc + math.sqrt(1 - ic * ic) * (vault_effect * eff[v] + math.sqrt(1 - vault_effect ** 2) * rng.gauss(0, 1)))
            g = math.expm1(x)
            rows.append({'vault': v, 'cluster': (leaders or {}).get(v, v), 'score': sc, 'gross': g,
                         'net': V.net_of_profit_share(g), 'control': rng.random(), 'c3': V.net_of_profit_share(g)})
        out[w] = rows
    return out


def stats_checks():
    print('IC, Fisher Z, clustered Z, top minus all, bootstrap')
    hlp = {w: 0.0 for w in range(4)}
    passes = absz = 0
    for s in range(200):
        res = V.summarise(synth_windows(random.Random(s)), hlp, n_boot=0, z_boot=0, placebo_seeds=0)
        passes += res['criteria']['1_ic']
        absz += abs(res['z']) >= V.Z_CRIT
    check('null: plain criterion 1 passes in <= 3% of 200 seeds; |Z| >= 2.33 in <= 5%', passes <= 6 and absz <= 10,
          f'{passes}/200, {absz}/200')
    res = V.summarise(synth_windows(random.Random(1), ic=0.4), hlp, n_boot=1000, z_boot=200, placebo_seeds=0)
    check('planted loading 0.4: Fisher Z >= 2.33, window t >= 3.18, D > 0, D / SE >= 1.64 (both versions pass)',
          res['pass_a'] and res['pass_b'], f'Z {res["z"]:.2f} t {res["window_t"]:.2f}')
    check('the random-effects window-and-leader Z is reported, not a gate', res['window_leader_z_reported'] is not None
          and 'window_leader' not in ''.join(res['criteria']))
    het = synth_windows(random.Random(8), ic=0.0)
    for w, rows in het.items():                                   # two windows strongly positive, two weakly negative
        k = 0.8 if w < 2 else -0.15
        for r in rows:
            r['net'] = k * r['score'] + 0.3 * random.Random(f"{w}:{r['vault']}").gauss(0, 1)
    hres = V.summarise(het, hlp, n_boot=0, placebo_seeds=0, min_positive=2)
    check('heterogeneous windows: the pooled Fisher Z passes but the window t-test refuses',
          hres['z'] >= 2.33 and hres['window_t'] < 3.182 and not hres['criteria']['1_ic'], f'Z {hres["z"]:.2f} t {hres["window_t"]:.2f}')
    miss = synth_windows(random.Random(1), ic=0.4)
    lab_m = V.label_top(miss)
    k_m = 0
    for w, rows in miss.items():
        for r in rows:
            if lab_m[(w, r['vault'])] and k_m % 5 == 0:
                r['c3'] = None
            k_m += lab_m[(w, r['vault'])]
    mres = V.summarise(miss, hlp, n_boot=0, placebo_seeds=0)
    check('criterion 3 fails when more than 10% of the top quintile\'s c3 values are missing',
          mres['c3_missing_share'] > 0.10 and mres['criteria']['3_top_beats_hlp_same_interval'] is False, f'{mres["c3_missing_share"]:.2f}')
    vres = V.summarise(synth_windows(random.Random(1), ic=0.4), hlp, n_boot=0, placebo_seeds=0, use_c3=False)
    check('outcome variants: criterion 3 n/a, pass on criteria 1, 2, 4, 5', vres['criteria']['3_top_beats_hlp_same_interval'] is None
          and vres['pass_a'])
    w0 = synth_windows(random.Random(2), pool=10_000)
    lab = V.label_top(w0)
    b0, a0 = V.clustered_bootstrap_se(w0, lab, 1000), V.analytic_clustered_se(w0, lab)
    check('bootstrap SE (1,000 draws) ~ analytic clustered SE for independent clusters (within 10%)', abs(b0 / a0 - 1) < 0.1,
          f'{b0:.4f} vs {a0:.4f}')
    pool = 100
    lead = {f'v{i}': f'L{i // 5}' for i in range(pool)}         # 5 vaults per leader, sharing score and outcome
    rng = random.Random(4)
    leff = {f'L{j}': (rng.gauss(0, 1), rng.gauss(0, 1)) for j in range(pool // 5)}
    w1 = synth_windows(random.Random(3), pool=pool, n=(80, 80, 80, 80), leaders=lead)
    noise = random.Random(17)
    for rows in w1.values():
        for r in rows:
            r['score'] = leff[r['cluster']][0] + 0.1 * noise.gauss(0, 1)
            r['net'] = 0.3 * (0.9 * leff[r['cluster']][1] + 0.1 * noise.gauss(0, 1))
    lab1 = V.label_top(w1)
    by_leader = V.analytic_clustered_se(w1, lab1)
    by_vault = V.analytic_clustered_se({w: [dict(r, cluster=r['vault']) for r in rows] for w, rows in w1.items()}, lab1)
    check('leader clustering widens the SE when a leader\'s vaults share score and outcome', by_leader > 1.3 * by_vault,
          f'{by_leader:.4f} vs {by_vault:.4f}')
    # a window-common shock: z_w spread beyond sampling -> tau² > 0 deflates Z
    shock = synth_windows(random.Random(6), ic=0.0)
    for w, rows in shock.items():
        sign = 1 if w % 2 == 0 else -1
        for r in rows:
            r['net'] = r['net'] + sign * 0.4 * r['control']     # outcome loads on a control-like exposure
            r['score'] = r['score'] + sign * 0.0
    wl = V.window_leader_z(shock, 200)
    check('window-and-leader Z returns tau², sd >= the leader-bootstrap sd', wl['tau2'] is not None and wl['sd'] >= wl['sd_leader'])
    check('top_minus_all is the sum of the vault contributions',
          abs(V.top_minus_all(w0, lab) - sum(V.contributions(w0, lab).values())) < 1e-12)


def largest_vault_checks():
    print('largest positive vault removal')
    win = synth_windows(random.Random(3), ic=-0.3)
    lab = V.label_top(win)
    top0 = [r for r in win[0] if lab[(0, r['vault'])]][0]
    top0['net'] = 50.0
    res = V.summarise(win, {w: 0.0 for w in win}, n_boot=0, z_boot=0, placebo_seeds=0)
    check('a planted crash-cluster vault: criterion 2 passes, 5 fails, the planted vault is removed',
          res['criteria']['2a_top_minus_all_positive'] and not res['criteria']['5_without_largest_positive_vault']
          and res['largest_positive_vault'] == top0['vault'])
    win2 = synth_windows(random.Random(4), ic=0.5)
    lab2 = V.label_top(win2)
    loser = [r for r in win2[1] if lab2[(1, r['vault'])]][0]
    loser['net'] = -1.0
    check('a planted loss is never the vault removed', V.largest_positive_vault(win2, lab2) != loser['vault'])


def placebo_checks():
    print('placebo (scores shuffled within window)')
    win = synth_windows(random.Random(5), ic=0.4)
    p = V.placebo(win, {w: 0.0 for w in win}, seeds=200)
    print(f'    planted data, 200 seeds: criterion 1 {p["criterion1_share"]:.3f}, all (a) {p["all_a_share"]:.3f}, '
          f'all (b) {p["all_b_share"]:.3f}, |Z| >= 2.33 {p["abs_z_share"]:.3f}')
    check('calibrated: criterion-1 share <= 5%, |Z| share <= 6%', p['criterion1_share'] <= 0.05 and p['abs_z_share'] <= 0.06)
    check('deterministic per seed', V.placebo(win, {w: 0.0 for w in win}, seeds=5) == V.placebo(win, {w: 0.0 for w in win}, seeds=5))


def power_checks():
    print('power and the factor-aware null (M1)')
    rng = random.Random(1)
    ids = [[f'v{i}' for i in rng.sample(range(150), k)] for k in (74, 64, 50, 54)]
    allv = sorted({v for w in ids for v in w})                    # sorted: set order of str is hash-seeded
    sd = {v: 0.17 for v in allv}
    beta = {v: rng.gauss(0.3, 1.0) for v in allv}
    lead = {v: v for v in allv}
    p0 = V.simulate_power(ids, sd, 0.0, beta=beta, leader=lead, sims=2000, seed=1)
    p3 = V.simulate_power(ids, sd, 0.3, beta=beta, leader=lead, sims=300, seed=1)
    print(f'    IC 0: c1 {p0["1"]:.3f}, all (a) {p0["all_a"]:.3f}; IC 0.3: c1 {p3["1"]:.3f}, all (a) {p3["all_a"]:.3f}')
    check('IC 0 (2,000 sims): criterion 1 <= 2%; IC 0.3: >= 70%', p0['1'] <= 0.02 and p3['1'] >= 0.7, f'{p0["1"]:.4f}')
    bear = [-0.256, -0.253, 0.027, -0.111]                 # BTC log returns over the four holds (market data)
    kw = dict(beta=beta, leader=lead, sims=2000, seed=2, c_beta=-0.4, market=bear)
    conf_r = V.simulate_power(ids, sd, 0.0, control='random', **kw)
    conf_b = V.simulate_power(ids, sd, 0.0, control='beta', **kw)
    print(f'    bear-year null, corr(score, net exposure) -0.4 (2,000 sims): plain c1 {conf_r["1_plain"]:.4f} with an unrelated '
          f'control, {conf_b["1_plain"]:.4f} with net-exposure terciles, {conf_b["1"]:.4f} adding the window t-test; '
          f'all five (a) {conf_b["all_a"]:.4f}')
    check('factor-aware null: a score tilted short in a bear year inflates plain c1 under an unrelated control (> 5%)',
          conf_r['1_plain'] > 0.05)
    check('net-exposure terciles reduce it; the window t-test does not raise it; all five (a) <= 5%',
          conf_b['1_plain'] < conf_r['1_plain'] / 2 and conf_b['1'] <= conf_b['1_plain'] and conf_b['all_a'] <= 0.05)
    shock = V.simulate_power(ids, sd, 0.0, beta=beta, leader=lead, sims=2000, seed=3, shock_sd=0.26)
    print(f'    window-shock null (window-common IC shift sd 0.26, 2,000 sims): plain c1 {shock["1_plain"]:.4f}, '
          f'with the window t-test {shock["1"]:.4f} (Wilson upper {V.binom_upper(round(shock["1"] * 2000), 2000):.4f})')
    check('window-shock null: the t-test brings criterion 1 to <= 2.5% while plain c1 is inflated',
          shock['1'] <= 0.025 and shock['1_plain'] > shock['1'])
    a = V.calibrate_skill(0.2, 0.42, n=8000)
    r = random.Random(9)
    zs = [r.gauss(0, 1) for _ in range(8000)]
    sc = [math.sqrt(0.42) * z + math.sqrt(0.58) * r.gauss(0, 1) for z in zs]
    ret = [math.expm1(0.3 * (a * z + math.sqrt(1 - a * a) * r.gauss(0, 1))) for z in zs]
    check('calibrate_skill plants the requested Spearman IC (0.2 +- 0.03)', abs(V.stats.spearman(sc, ret) - 0.2) < 0.03)
    m = V.overlap_matrix(V.SELECTIONS, V.LOOKBACK_DAYS)
    check('lookback overlap 35/91 between neighbours; primary holds do not overlap',
          abs(m[0][1] - 35 / 91) < 1e-12 and all(V.overlap_matrix(V.SELECTIONS, V.HOLD_DAYS)[i][j] == 0
                                                 for i in range(4) for j in range(4) if i != j))
    check('holdout windows start after the cutoff, do not overlap, end 2026-10-26; discovery reads end 2026-06-08',
          V.HOLDOUT_SELECTIONS[0] >= V.CUTOFF and (V.HOLDOUT_SELECTIONS[1] - V.HOLDOUT_SELECTIONS[0]).days == V.HOLD_DAYS
          and V.HOLDOUT_END == dt.date(2026, 10, 26) and V.READ_END == dt.date(2026, 6, 8))


# ------------------------------------------------------------------ loaders, BTC, manifest

def loader_checks():
    print('holdout-safe loaders, BTC, manifest')
    page = [{'time': V.CUTOFF_MS - 1, 'hash': 'h', 'delta': {'type': 'vaultDeposit', 'vault': '0xa', 'usdc': '5'}}]
    check('ledger rows before the cutoff are kept as served', V.project_ledger(page, '0xa') == page)
    try:
        V.project_ledger(page + [{'time': V.CUTOFF_MS, 'hash': 'h', 'delta': {}}], '0xa')
        stopped = False
    except SystemExit:
        stopped = True
    check('a holdout-dated ledger row stops the fetch', stopped)
    ev = [{'time': V.READ_END_MS - 1, 'delta': {}}, {'time': V.READ_END_MS, 'delta': {}}, {'time': V.CUTOFF_MS + 1, 'delta': {}}]
    check('bounded_events / bounded_grid cut at the read end', len(V.bounded_events(ev, V.READ_END_MS)) == 1
          and V.bounded_grid([(V.READ_END_MS - 1, 1, 1), (V.READ_END_MS, 1, 1)], V.READ_END_MS) == [(V.READ_END_MS - 1, 1, 1)])
    raw = {'apr': 1.0, 'isClosed': True, 'allowDeposits': False,
           'portfolio': [['day', {'accountValueHistory': [[V.CUTOFF_MS + 5, 1]], 'pnlHistory': [[V.CUTOFF_MS + 5, 1]]}],
                         ['allTime', {'accountValueHistory': [[V.CUTOFF_MS - D, '10'], [V.CUTOFF_MS + D, '99']],
                                      'pnlHistory': [[V.CUTOFF_MS - D, '1'], [V.CUTOFF_MS + D, '9']], 'vlm': [[1, 2]]}]]}
    proj = V.project_details(raw)
    check('vaultDetails projection keeps only allTime av/pnl before the cutoff',
          proj == {'ok': True, 'allTime': {'accountValueHistory': [[V.CUTOFF_MS - D, '10']], 'pnlHistory': [[V.CUTOFF_MS - D, '1']]}})
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / 'hlp.json'
        p.write_text(json.dumps(raw))
        check('HLP loader truncates at load', V.load_hlp_alltime(p) == proj['allTime'])
        import pyarrow as pa
        import pyarrow.parquet as pq
        bp = Path(d) / 'btc.parquet'
        ts = [V.CUTOFF_MS - 3 * H, V.CUTOFF_MS - 2 * H, V.CUTOFF_MS - H, V.CUTOFF_MS, V.CUTOFF_MS + H]
        pq.write_table(pa.table({'ts': ts, 'open': [1.0] * 5, 'high': [1.0] * 5, 'low': [1.0] * 5, 'close': [1.0, 2.0, 3.0, 4.0, 5.0]}), bp)
        b = V.Btc(bp)
        check('BTC loader: the query drops candles closing after the cutoff', b.ts == ts[:3] and max(b.close) == 3.0)
        b2 = V.Btc(bp, end_ms=V.CUTOFF_MS - H)
        check('BTC loader: and after the discovery read end when asked', b2.ts == ts[:2])
        f1 = Path(d) / 'a.txt'
        f1.write_text('x')
        m = Path(d) / 'm.json'
        dig = V.write_manifest(m, [f1], Path(d))
        try:
            V.write_manifest(m, [f1], Path(d))
            refused = False
        except SystemExit:
            refused = True
        check('manifest: written once, a second write is refused', refused and json.loads(m.read_text())['digest'] == dig)
    ev = [{'time': 1, 'delta': {'type': 'vaultCreate', 'vault': '0xa', 'usdc': '100', 'fee': '1'}},
          {'time': 2, 'delta': {'type': 'vaultDeposit', 'vault': '0xa', 'usdc': '50'}},
          {'time': 3, 'delta': {'type': 'vaultWithdraw', 'vault': '0xa', 'user': '0xu', 'requestedUsd': '20', 'netWithdrawnUsd': '18'}},
          {'time': 4, 'delta': {'type': 'vaultDistribution', 'vault': '0xa', 'usdc': '130'}},
          {'time': 5, 'delta': {'type': 'internalTransfer', 'user': '0xx', 'destination': '0xa', 'usdc': '7', 'fee': '0'}},
          {'time': 6, 'delta': {'type': 'liquidation'}}]
    check('ledger flows: create / deposit / transfer in; withdraw (requested) / distribution out; liquidation no flow',
          V.ledger_flows(ev, '0xa') == [(1, 100.0), (2, 50.0), (3, -20.0), (4, -130.0), (5, 7.0)])
    check('withdraw field variant (net of commission)', V.ledger_flows(ev, '0xa', withdraw_fields=('netWithdrawnUsd',))[2] == (3, -18.0))


# ------------------------------------------------------------------ guard, import closure, assembly

def guard_checks():
    print('guard (throwaway git repo)')
    code, data = ('ct/vaults.py', 'r007_run.py'), ('data/vaults/manifest_r1.json',)
    reg_rel = 'registrations/007-x.md'
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        git = lambda *a: subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *a], cwd=root,  # noqa: E731
                                        capture_output=True, text=True, check=True)
        git('init', '-q')
        for f, body in (('ct/vaults.py', 'x = 1\n'), ('r007_run.py', 'y = 2\n'),
                        ('data/vaults/manifest_r1.json', json.dumps({'digest': 'ab' * 32}) + '\n'),
                        ('results.jsonl', json.dumps({'registration': '005-y.md'}) + '\n')):
            (root / f).parent.mkdir(parents=True, exist_ok=True)
            (root / f).write_text(body)
        (root / '.gitignore').write_text('data/\n')

        def write_reg(status='REGISTERED', digest='ab' * 32):
            hashes = {f: V.sha256(root / f) for f in (*code, *data)}
            rows = '\n'.join(f'| `{f}` | `{h}` |' for f, h in hashes.items())
            (root / reg_rel).parent.mkdir(exist_ok=True)
            (root / reg_rel).write_text(f'# 007\n\n**Status: {status} 2026-10-12**\n\nsome other digest `{"ef" * 32}`\n\n'
                                        f'**Data manifest:** `{data[0]}` — 1 files, digest `{digest}`.\n\n'
                                        f'| file | sha256 |\n|---|---|\n{rows}\n')
        fails = lambda: V.guard_failures(root, reg_rel, code, data, manifest_rel=data[0])  # noqa: E731
        write_reg()
        git('add', '.gitignore', 'ct', 'r007_run.py', 'results.jsonl')
        git('commit', '-q', '-m', 'code')
        f0, _ = fails()
        check('(a)+(c) untracked registration refused', any(x.startswith('(a)') for x in f0) and any(x.startswith('(c)') for x in f0))
        git('add', reg_rel)
        check('(c) staged, never committed: refused', any(x.startswith('(c)') for x in fails()[0]))
        git('commit', '-q', '-m', 'register')
        f2, c2 = fails()
        check('committed, clean, hashes and digest match, no result row: allowed', f2 == [] and len(c2) == 40, str(f2))
        (root / reg_rel).write_text((root / reg_rel).read_text() + 'edit\n')
        check('(b) registration edited after commit: refused', any(x.startswith('(b)') for x in fails()[0]))
        git('checkout', '--', reg_rel)
        (root / 'ct/vaults.py').write_text('x = 3\n')
        f4, _ = fails()
        check('(d)+(e) guarded code edited: refused', any(x.startswith('(d)') for x in f4) and any(x.startswith('(e) ct/vaults.py') for x in f4))
        git('checkout', '--', 'ct/vaults.py')
        (root / data[0]).write_text(json.dumps({'digest': 'cd' * 32}) + '\n')
        f5, _ = fails()
        check('(e)+(g) manifest replaced: hash and the Data-manifest digest both refuse',
              any(x.startswith('(e) data/vaults') for x in f5) and any(x.startswith('(g)') for x in f5), str(f5))
        (root / data[0]).write_text(json.dumps({'digest': 'ef' * 32}) + '\n')
        check('(g) a digest quoted elsewhere in the file does not satisfy the Data-manifest line',
              any(x.startswith('(g)') for x in fails()[0]))
        (root / data[0]).write_text(json.dumps({'digest': 'ab' * 32}) + '\n')
        with open(root / 'results.jsonl', 'a') as fh:
            fh.write(json.dumps({'registration': '007-x.md'}) + '\n')
        check('(f) a 007 result row in the working copy: refused', any(x.startswith('(f)') for x in fails()[0]))
        git('commit', '-q', '-am', 'result')
        (root / 'results.jsonl').write_text(json.dumps({'registration': '005-y.md'}) + '\n')
        check('(f) the row deleted locally but present in HEAD: still refused',
              any('committed history' in x for x in fails()[0]), str(fails()[0]))
        git('commit', '-q', '-am', 'remove the row')
        check('(f) the row removed by a later commit: still refused (history scan)',
              any('committed history' in x for x in fails()[0]), str(fails()[0]))
        write_reg(status='DRAFT')
        check('status DRAFT: refused', any('not REGISTERED' in x for x in fails()[0]))


def import_closure_check():
    print('runner import closure (static, nested imports included)')
    import ast

    def deps(rel):
        tree = ast.parse((ROOT / rel).read_text())
        pkg = Path(rel).parent
        out = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.level:                                   # from . import x / from .x import y
                    base = pkg
                    mod = node.module
                    targets = [mod] if mod else [n.name for n in node.names]
                    for t in targets:
                        f = base / (t.replace('.', '/') + '.py')
                        if (ROOT / f).exists():
                            out.add(str(f))
                elif node.module and node.module.split('.')[0] == 'ct':
                    if node.module == 'ct':
                        for n in node.names:
                            out.add(f'ct/{n.name}.py')
                    else:
                        out.add(node.module.replace('.', '/') + '.py')
                    out.add('ct/__init__.py')
            elif isinstance(node, ast.Import):
                for n in node.names:
                    if n.name.split('.')[0] == 'ct':
                        out.add(n.name.replace('.', '/') + '.py')
                        out.add('ct/__init__.py')
        return {d for d in out if (ROOT / d).exists()}
    seen, todo = set(), ['r007_run.py']
    while todo:
        f = todo.pop()
        if f in seen:
            continue
        seen.add(f)
        todo.extend(deps(f) - seen)
    import r007_run
    missing = sorted(m for m in seen if m not in r007_run.GUARDED_CODE)
    check('every research module r007_run imports (transitively, nested too) is hashed by the guard', not missing,
          f'{sorted(seen)} missing {missing}')


def assembly_checks():
    print('runner assembly (synthetic inputs)')
    import r007_run

    def per_window(ic, seed, unmeas=0):
        rng = random.Random(seed)
        out = {}
        for w, k in zip(V.SELECTIONS, (74, 64, 50, 54)):
            rows = []
            for i in range(k):
                sc = rng.gauss(0, 1)
                x = 0.3 * (ic * sc + math.sqrt(1 - ic * ic) * rng.gauss(0, 1))
                g = math.expm1(x)
                o = {'gross': g, 'net': V.net_of_profit_share(g), 'closed': False, 'entry_source': 'grid', 'exit_reason': 'hold_end'}
                if i < unmeas:
                    o = {'unmeasurable': 'no_entry'}
                outs = {k2: o for k2 in ('primary', 'scaled', 'av', 'midpoint', 'other_withdraw_field', 'no_exit_cost', 'adj')}
                outs['gross'] = dict(o, net=o['gross']) if 'gross' in o else o
                outs['c3'] = o.get('net')
                rows.append({'vault': f'v{i}', 'leader': f'L{i // 2}', 'score': sc, 'score_xr': sc + rng.gauss(0, 0.5),
                             'score_sharpe': sc / 5, 'n_trips': rng.randrange(20, 200), 'long_bias': rng.random(),
                             'control': rng.gauss(0, 1), 'equity': rng.choice((5e4, 2e5, 1e6)), 'outside': True, 'out': outs,
                             'hold_t': sc + rng.gauss(0, 1), 'hold_size_weighted': rng.gauss(0, 1)})
            out[w] = rows
        return out
    hlp = {w: 0.0 for w in V.SELECTIONS}
    a = r007_run.assemble(per_window(0.5, 1), hlp, placebo_seeds=20, n_boot=100, z_boot=100)
    check('planted effect: passes, decision pull_holdout', a['pass'] and a['decision'] == 'pull_holdout', str(a['criteria']))
    b = r007_run.assemble(per_window(0.0, 2), hlp, placebo_seeds=20, n_boot=100, z_boot=100)
    check('null: no pass, decision close', not b['pass'] and b['decision'] == 'close')
    c = r007_run.assemble(per_window(0.5, 1, unmeas=2), hlp, placebo_seeds=0, n_boot=50, z_boot=50)
    check('8 of 242 unmeasurable (3.3%): imputed, counted by reason, run continues',
          c['unmeasurable_total'] == {'n': 8, 'of': 242} and c['unmeasurable_by_reason'] == {'no_entry': 8})
    try:
        r007_run.assemble(per_window(0.5, 1, unmeas=4), hlp, placebo_seeds=0, n_boot=50, z_boot=50)
        stopped = False
    except SystemExit:
        stopped = True
    check('16 of 242 unmeasurable (6.6%): the run stops before any criterion', stopped)
    e = r007_run.assemble(per_window(0.5, 1, unmeas=4), hlp, placebo_seeds=0, n_boot=50, z_boot=50, gate=False)
    check('Amendment 1: a REPORTED variant with 16 of 242 unmeasurable does not stop the run; it reports its gate as failed',
          e['unmeasurable_gate_pass'] is False and e['unmeasurable_total'] == {'n': 16, 'of': 242})
    src = (Path(r007_run.__file__)).read_text()
    check('Amendment 1: main() calls the primary assemble with the gate on and the full-population variant with gate=False',
          src.count('assemble(per_window, hlp)') == 1 and src.count('z_boot=0, gate=False)') == 1)
    check('reported blocks present (incl. sqrt(n), trip-count control, >= $100k, winsorised, version b)',
          all(k in a['reported'] for k in ('score_over_sqrt_n', 'control_trip_count', 'equity_ge_100k', 'winsorised_2_5pct',
                                           'criterion2_version_b', 'size_weighting_and_activity', 'top_quintile_capacity',
                                           'market_adjusted', 'worst_case_imputation', 'window_leader_z_tau'))
          and a['reported']['scaled_to_56d']['criteria']['3_top_beats_hlp_same_interval'] is None)
    d = r007_run.assemble(per_window(0.5, 1), {w: 10.0 for w in V.SELECTIONS}, placebo_seeds=0, n_boot=50, z_boot=50)
    check('HLP above the top quintile fails criterion 3 only',
          not d['criteria']['3_top_beats_hlp_same_interval'] and d['criteria']['1_ic'] and d['criteria']['2a_top_minus_all_positive'])
    hw = {0: [], 1: []}
    rng = random.Random(5)
    for w in hw:
        for i in range(60):
            sc = rng.gauss(0, 1)
            net = 0.3 * (0.5 * sc + 0.85 * rng.gauss(0, 1))
            hw[w].append({'vault': f'h{i}', 'cluster': f'h{i}', 'score': sc, 'net': net, 'adj': net, 'c3': net, 'control': rng.random()})
    ok_h, det = V.holdout_confirm(hw, {0: -1.0, 1: -1.0}, n_boot=0)
    flipped = {w: [dict(r, adj=-r['adj']) for r in rows] for w, rows in hw.items()}
    ok_f, det_f = V.holdout_confirm(flipped, {0: -1.0, 1: -1.0}, n_boot=0)
    check('holdout CONFIRM on the market-adjusted outcome: a planted effect confirms; flipping adj fails Z, D/SE and the sign rule',
          ok_h and not ok_f and not det_f['holdout_criteria']['sign'], str(det['holdout_criteria']))
    f, _ = V.guard_failures(ROOT, str(r007_run.REG.relative_to(ROOT)), r007_run.GUARDED_CODE, r007_run.GUARDED_DATA,
                            manifest_rel=r007_run.MANIFEST_REL)
    check('the real guard refuses today (DRAFT, not committed)', any('not REGISTERED' in x for x in f), f'{len(f)} reasons')


def main():
    nav_checks()
    distribution_outflow_checks()
    exhaustion_checks()
    post_hold_checks()
    no_pre_s_checks()
    impute_checks()
    profit_share_checks()
    gate_checks()
    eligibility_checks()
    stats_checks()
    largest_vault_checks()
    placebo_checks()
    power_checks()
    loader_checks()
    guard_checks()
    import_closure_check()
    assembly_checks()
    print('vault checks passed')


if __name__ == '__main__':
    main()
