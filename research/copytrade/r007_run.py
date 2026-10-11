"""Registration 007 (L1, depositing into top-scored Hyperliquid user vaults) — the run (Revision 1).

REFUSES to start unless registrations/007-vault-deposit-skill.md says REGISTERED, is committed and
unmodified, the guarded code is clean and matches the §8 SHA-256 table, the data manifest matches
§8 and its digest is quoted in §8, results.jsonl (working copy AND HEAD) has no 007 row. Then
every pinned file must match the manifest. One line is appended to results.jsonl.

    .venv/bin/python -I r007_run.py

Order: guard -> manifest -> eligible sets (pre-S reads only) -> GATES (NAV identity on flow-free
and flow intervals, which also picks the withdrawal field; profit-share check on lookback
withdrawals) -> lookback scores (streamed fills) -> depositor returns and HLP's -> unmeasurable
gate (<= 5%) -> the five criteria, placebo, reported blocks -> results.jsonl.
Every read of fills, ledgers, grids and snapshots ends before 2026-06-08 (the last hold's end), and
each window's outcome reads only [S, S + 56 d] (Revision 2: in-hold exhaustion, no closure
decided by later activity).
"""
import datetime as dt
import json
import math
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))   # run with -I
from ct import scores, stats  # noqa: E402
from ct import vaults as V  # noqa: E402

HERE = Path(__file__).resolve().parent
REG = HERE / 'registrations/007-vault-deposit-skill.md'
RESULTS = HERE / 'results.jsonl'
MANIFEST_REL = 'data/vaults/manifest_r1.json'
DEX = 'hyperliquid'
TRIALS, CUMULATIVE = 1, 15
POPULATION = 'outside'          # D2, PROPOSED (orchestrator's recommendation); 'full' is the reported variant
VERSION = 'a'                   # D9, criterion 2 version, PROPOSED (a); (b) reported
# the runner's own import closure (checked by tests/test_vaults.py) plus ct/regimes.py and
# ct/bitget.py, which 007 no longer imports (ct/vaults.Btc replaced Regimes) but are pinned anyway
GUARDED_CODE = ('ct/__init__.py', 'ct/vaults.py', 'r007_run.py', 'ct/scores.py', 'ct/stats.py', 'ct/leaders.py',
                'ct/archive_fills.py', 'ct/net.py', 'ct/regimes.py', 'ct/bitget.py')
GUARDED_DATA = (MANIFEST_REL,)
DESCRIBE = V.VAULT_DIR / 'describe'


def guard():
    failures, commit = V.guard_failures(HERE, str(REG.relative_to(HERE)), GUARDED_CODE, GUARDED_DATA,
                                        manifest_rel=MANIFEST_REL)
    if failures:
        sys.exit('\n'.join(failures))
    return commit


def verify_manifest():
    m = json.loads((HERE / MANIFEST_REL).read_text())
    bad = [f for f, h in m['files'].items() if not (HERE / f).exists() or V.sha256(HERE / f) != h]
    if bad:
        sys.exit(f'pinned data changed or missing: {bad[:5]} ({len(bad)} files)')
    return m['digest']


# ------------------------------------------------------------------ inputs (all bounded at the read end)

def load_ledger(a):
    ev = json.loads((V.VAULT_DIR / 'ledger' / f'{a}.json').read_text())['events']
    return V.bounded_events(ev, V.READ_END_MS)


def load_grid(a):
    d = json.loads((V.VAULT_DIR / 'details' / f'{a}.json').read_text())
    return V.bounded_grid(V.grid_from_alltime(d.get('allTime') or {}), V.READ_END_MS)


def load_activity():
    out = {}
    for a, day, n, first, last in json.loads((DESCRIBE / 'activity.json').read_text()):
        if last < V.READ_END_MS:
            out.setdefault(a, []).append((day, n, first, last))
    return out


def snapshots_after(addrs, sel):
    """{vault: [(t, av)]} snapshot rows in [S, S + 10 d] (the entry fallback), < read end."""
    from ct.leaders import con
    c = con()
    c.execute('CREATE OR REPLACE TEMP TABLE sn_(address VARCHAR)')
    if not addrs:
        return {}
    c.executemany('INSERT INTO sn_ VALUES (?)', [(a,) for a in addrs])
    s_ms = V.ms(sel)
    rows = c.execute(f"""SELECT "user", snapshot_ms, account_value FROM read_parquet('{V.SNAPS}')
                         WHERE snapshot_ms < {V.CUTOFF_MS} AND snapshot_ms < {V.READ_END_MS}
                           AND snapshot_ms >= {s_ms} AND snapshot_ms <= {s_ms + V.ENTRY_DAYS * V.DAY}
                           AND "user" IN (SELECT address FROM sn_) ORDER BY 1, 2""").fetchall()
    out = {}
    for a, t, av in rows:
        out.setdefault(a, []).append((t, av))
    return out


def eligible_sets(leaders, act):
    cand = json.loads((DESCRIBE / 'candidates.json').read_text())['by_selection']
    out, feats, nexp = {}, {}, {}
    for sel in V.SELECTIONS:
        s_ms = V.ms(sel)
        f = {r['address']: r for r in json.loads((DESCRIBE / f'features_{sel}.json').read_text())}
        feats[sel] = f
        nexp[sel] = json.loads((DESCRIBE / f'net_exposure_{sel}.json').read_text())
        full, outside = [], []
        for a in cand[sel.isoformat()]:
            ev = [e for e in load_ledger(a) if e['time'] < s_ms]
            has_out = V.outside_depositor(ev, leaders[a], s_ms)
            ok, _ = V.eligible(f[a], s_ms, V.last_fill_before(act.get(a, []), s_ms), has_out)
            if ok:
                full.append(a)
                if has_out:
                    outside.append(a)
        out[sel] = {'full': full, 'outside': outside}
    return out, feats, nexp


def lookback_scores(sel, addrs, btc):
    from ct.leaders import iter_fills
    lb = sel - dt.timedelta(days=V.LOOKBACK_DAYS)
    out = {}
    for a, fills in iter_fills(DEX, addrs, lb, sel):
        trips = scores.round_trips(fills)
        t = scores.plain_score(trips)
        out[a] = {'score': t, 'score_xr': scores.cross_regime_score(trips, btc.is_rising, 4),
                  'score_sharpe': (t / math.sqrt(len(trips))) if t is not None and trips else None,
                  'long_bias': scores.long_bias(fills), 'n_trips': len(trips)}
    return out


def hold_trip_stats(sel, addrs):
    from ct.leaders import iter_fills
    end = min(sel + dt.timedelta(days=V.HOLD_DAYS), V.READ_END)
    out = {}
    for a, fills in iter_fills(DEX, addrs, sel, end):
        trips = scores.round_trips(fills)
        den = sum(t['max_notional'] for t in trips)
        out[a] = {'hold_t': scores.plain_score(trips) if len(trips) >= 5 else None,
                  'hold_size_weighted': (sum(t['cash'] - t['fees'] for t in trips) / den) if den else None}
    return out


# ------------------------------------------------------------------ assembly (pure; tested on synthetic inputs)

def outcomes(grid, events, a, leader, sel, eq_s, beta, snaps, field, span, btc):
    """All outcome variants of one vault-window. span: (common entry t, common exit t) from HLP's
    grid (criterion 3), or None. Every read lies in [S, S + 56 d] (Revision 2, M-b)."""
    s_ms = V.ms(sel)
    flows = V.ledger_flows(events, a, withdraw_fields=(field, 'usdc'))
    other = [f for f in V.WITHDRAW_CANDIDATES if f != field][0]
    cost = V.exit_cost([e for e in events if e['time'] < s_ms], leader, s_ms)
    kw = dict(snaps=snaps, equity_at_s=eq_s)
    out = {'primary': V.hold_return(grid, flows, s_ms, exit_cost=cost, **kw),
           'scaled': V.hold_return(grid, flows, s_ms, exit_cost=cost, scale=True, **kw),
           'av': V.hold_return(grid, flows, s_ms, exit_cost=cost, numerator='av', **kw),
           'midpoint': V.hold_return(grid, None, s_ms, exit_cost=cost, **kw),
           'other_withdraw_field': V.hold_return(grid, V.ledger_flows(events, a, withdraw_fields=(other, 'usdc')), s_ms,
                                                 exit_cost=cost, **kw),
           'no_exit_cost': V.hold_return(grid, flows, s_ms, **kw)}
    p = out['primary']
    out['gross'] = dict(p, net=p['gross']) if 'gross' in p else p
    out['adj'] = dict(p, net=V.market_adjusted(p['net'], beta, btc.close_before(p['entry_t']), btc.close_before(p['exit_t']))) \
        if 'net' in p else p
    out['c3'] = None
    if span:
        he, hx = span
        r = V.hold_return(grid, flows, s_ms, end_ms=min(hx + V.C3_TOL, s_ms + V.HOLD_DAYS * V.DAY), exit_cost=cost,
                          entry_lo=he - V.C3_TOL, entry_hi=he + V.C3_TOL, **kw)
        if 'net' in r and abs(r['entry_t'] - he) <= V.C3_TOL and (r['closed'] or abs(r['exit_t'] - hx) <= V.C3_TOL):
            out['c3'] = r['net']
    out['exit_cost'] = cost
    return out


def rows_for(per_window, variant='primary', score='score', control='control', keep=lambda r: True):
    """Criterion rows per window; unmeasurable outcomes come through as None (imputed later).
    Only the primary outcome carries c3; every other variant reports criterion 3 as n/a."""
    out = {}
    for s, rows in per_window.items():
        rs = []
        for r in rows:
            if r.get(score) is None or r.get(control) is None or not keep(r):
                continue
            o = r['out'].get(variant) or {}
            rs.append({'vault': r['vault'], 'cluster': r['leader'], 'score': r[score], 'net': o.get('net'),
                       'gross': o.get('gross'), 'control': r[control], 'c3': r['out'].get('c3') if variant == 'primary' else None})
        out[s] = rs
    return out


def assemble(per_window, hlp, placebo_seeds=200, n_boot=1000, z_boot=1000, version=VERSION, gate=True):
    """per_window: {S: [{'vault', 'leader', 'score', 'score_xr', 'score_sharpe', 'n_trips', 'long_bias',
    'control', 'equity', 'outside', 'out': {variant: hold_return dict, 'c3': net or None}, 'hold_t',
    'hold_size_weighted'}]}; hlp: {S: HLP return over its in-hold interval}.
    Returns the result block; raises SystemExit if more than 5% of vault-windows are unmeasurable and
    gate is True (the PRIMARY only, Amendment 1: a reported variant reports its gate, never stops the run)."""
    raw = rows_for(per_window)
    n_total = sum(len(r) for r in raw.values())
    prim, n_unmeas = V.impute_unmeasurable(raw)
    gate_ok = V.unmeasurable_gate(n_unmeas, n_total)
    if gate and not gate_ok:
        raise SystemExit(f'unmeasurable gate: {n_unmeas} of {n_total} vault-windows (> 5%) — no criterion computed')
    res = V.summarise(prim, hlp, placebo_seeds=placebo_seeds, n_boot=n_boot, z_boot=z_boot, version=version)
    reasons = {}
    for rows in per_window.values():
        for r in rows:
            u = (r['out'].get('primary') or {}).get('unmeasurable')
            if u and r.get('score') is not None:
                reasons[u] = reasons.get(u, 0) + 1
    res['unmeasurable_by_reason'] = reasons
    res['exit_reasons'] = {}
    for rows in per_window.values():
        for r in rows:
            x = (r['out'].get('primary') or {}).get('exit_reason')
            if x:
                res['exit_reasons'][x] = res['exit_reasons'].get(x, 0) + 1
    res['decision'] = V.decide(res)
    res['n_by_window'] = {str(s): len(r) for s, r in prim.items()}
    res['counts'] = {str(s): {'no_score': sum(1 for r in rows if r.get('score') is None),
                              'unmeasurable': sum(1 for r in rows if 'unmeasurable' in (r['out'].get('primary') or {})),
                              'exhausted': sum(1 for r in rows if (r['out'].get('primary') or {}).get('closed')),
                              'snapshot_entry': sum(1 for r in rows if (r['out'].get('primary') or {}).get('entry_source') == 'snapshot'),
                              'thin_base_exit': sum(1 for r in rows if (r['out'].get('primary') or {}).get('exit_reason') == 'thin_base'),
                              'c3_missing': sum(1 for r in rows if r['out'].get('c3') is None)}
                     for s, rows in per_window.items()}
    res['unmeasurable_total'] = {'n': n_unmeas, 'of': n_total}
    res['unmeasurable_gate_pass'] = gate_ok
    rep = {}
    flat = [r for rows in prim.values() for r in rows]
    rep['top_half_ic'] = {str(s): stats.top_half_ic([(r['score'], r['net'], r['control']) for r in rows]) for s, rows in prim.items()}
    rep['decile_means_pooled'] = stats.decile_means([(r['score'], r['net']) for r in flat])
    rep['plain_ic'] = {str(s): stats.spearman([r['score'] for r in rows], [r['net'] for r in rows]) for s, rows in prim.items()}
    rep['window_leader_z_tau'] = V.window_leader_z(prim, 1000, mode='tau')
    rep['window_leader_z_resample'] = V.window_leader_z(prim, 1000, mode='resample')

    def block(windows, use_c3=False, mode='median', **kw):
        w, _ = V.impute_unmeasurable(windows, mode=mode)
        if not any(w.values()):
            return None
        out = V.summarise(w, hlp, placebo_seeds=0, n_boot=200, z_boot=0, version=version, use_c3=use_c3, **kw)
        return {k: v for k, v in out.items() if k != 'placebo'}
    variants = {
        'cross_regime_score': rows_for(per_window, score='score_xr'),
        'score_over_sqrt_n': rows_for(per_window, score='score_sharpe'),
        'control_long_bias': rows_for(per_window, control='long_bias'),
        'control_trip_count': rows_for(per_window, control='n_trips'),
        'scaled_to_56d': rows_for(per_window, variant='scaled'),
        'numerator_av_minus_flows': rows_for(per_window, variant='av'),
        'flows_unknown_midpoint': rows_for(per_window, variant='midpoint'),
        'other_withdraw_field': rows_for(per_window, variant='other_withdraw_field'),
        'no_exit_cost': rows_for(per_window, variant='no_exit_cost'),
        'gross_of_profit_share': rows_for(per_window, variant='gross'),
        'market_adjusted': rows_for(per_window, variant='adj'),
    }
    for name, w in variants.items():
        rep[name] = block(w)                                         # criterion 3 n/a for outcome variants
    rep['equity_ge_100k'] = block(rows_for(per_window, keep=lambda r: (r.get('equity') or 0) >= 100_000), use_c3=True)
    rep['worst_case_imputation'] = block(raw, use_c3=True, mode='min')
    rep['winsorised_2_5pct'] = block(V.winsorise(V.impute_unmeasurable(raw)[0]), use_c3=True)
    rep['criterion2_version_b'] = {'pass': res['pass_b'], 'd_se': res['top_minus_all_z']}
    sw = {}
    for s, rows in per_window.items():
        def ic_of(x, y):
            pts = [(r[x], (r['out'].get('primary') or {}).get('net') if y == 'net' else r.get(y)) for r in rows]
            pts = [p for p in pts if p[0] is not None and p[1] is not None]
            return (stats.spearman([p[0] for p in pts], [p[1] for p in pts]), len(pts)) if len(pts) > 3 else (None, len(pts))
        sw[str(s)] = {'ic_hold_per_trip_t': ic_of('score', 'hold_t'), 'ic_hold_size_weighted_ron': ic_of('score', 'hold_size_weighted'),
                      'ic_depositor_net': ic_of('score', 'net'), 'ic_n_trips_vs_net': ic_of('n_trips', 'net')}
    rep['size_weighting_and_activity'] = sw
    labels = V.label_top(prim)
    eq = {(s, r['vault']): r.get('equity') or 0 for s, rows in per_window.items() for r in rows}
    rep['top_quintile_capacity'] = {str(s): {str(k): sum(1 for r in rows if labels[(s, r['vault'])] and eq[(s, r['vault'])] >= 10 * k)
                                             for k in (10_000, 50_000, 250_000)} for s, rows in prim.items()}
    res['reported'] = rep
    res['ring_fence'] = 'no reported block becomes a primary, and none can unlock or consume the holdout'
    res['criterion3_variants'] = 'n/a: only the primary outcome carries the common-span c3'
    return res


def btc_betas(grids, btc):
    """Per vault: OLS beta of its grid-interval log NAV change on BTC's log return over the same
    interval, intervals inside [2025-07-28, 2026-06-08) only."""
    out = {}
    for a, g in grids.items():
        xs, ys = [], []
        for (ta, av_a, pa), (tb, _, pb) in zip(g, g[1:]):
            if av_a < V.BASE_FLOOR or tb >= V.READ_END_MS or ta < V.ms(V.ARCHIVE_START):
                continue
            b0, b1 = btc.close_before(ta), btc.close_before(tb)
            r = (pb - pa) / av_a
            if b0 and b1 and r > -1:
                xs.append(math.log(b1 / b0))
                ys.append(math.log1p(r))
        if len(xs) >= 8 and statistics.pvariance(xs) > 0:
            mx, my = statistics.fmean(xs), statistics.fmean(ys)
            out[a] = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)
    return out


def main():
    commit = guard()
    digest = verify_manifest()
    btc = V.Btc(end_ms=V.READ_END_MS)
    leaders = {v['address']: v['leader'] for v in V.load_list()}
    act = load_activity()                                        # the runner reads only its pre-S part
    sets, feats, nexp = eligible_sets(leaders, act)
    use = {s: sets[s][POPULATION] for s in V.SELECTIONS}
    vaults = sorted({a for s in V.SELECTIONS for a in sets[s]['full']})
    grids = {a: load_grid(a) for a in vaults}
    ledgers = {a: load_ledger(a) for a in vaults}
    # gate 1: NAV identity, flow-free and flow intervals, [2025-07-28, 2026-06-08); picks the withdrawal field
    checks = {f: [c for a in vaults for c in V.identity_checks(grids[a], ledgers[a], a, V.ms(V.ARCHIVE_START), V.READ_END_MS, f)]
              for f in V.WITHDRAW_CANDIDATES}
    ok, field, shares = V.identity_gate(checks)
    print('identity gate', ok, field, shares, flush=True)
    if not ok:
        sys.exit(f'NAV identity gate failed: {shares} — amend before any criterion is computed')
    # gate 2: the 10% profit share on lookback withdrawals of eligible vaults
    com = [c for s in V.SELECTIONS for a in sets[s]['full']
           for c in V.commission_checks(ledgers[a], leaders[a], V.ms(s) - V.LOOKBACK_DAYS * V.DAY, V.ms(s))]
    com_share = sum(com) / len(com) if com else None
    print('profit-share gate', com_share, len(com), flush=True)
    if com_share is None or com_share < V.COMMISSION_SHARE:
        sys.exit(f'profit-share gate failed: {com_share} over {len(com)} withdrawals — amend before any criterion')
    hlp_grid = V.bounded_grid(V.grid_from_alltime(V.load_hlp_alltime()), V.READ_END_MS)
    hlp, spans, per_window, per_window_full = {}, {}, {}, {}
    for sel in V.SELECTIONS:
        s_ms = V.ms(sel)
        # HLP over its own first in-hold grid point (<= S + 10 d) and last (<= S + 56 d): the common span.
        # HLP has no ledger here, so its net flow sits at each interval's midpoint (synthetic cost: rmse
        # 2.3% vs 1.9% with ledger timing) — a different flow treatment from the vaults', stated in §4.
        span = V.common_span(hlp_grid, s_ms, V.ENTRY_DAYS)
        spans[sel] = span
        h = V.hold_return(hlp_grid, None, s_ms, end_ms=span[1], entry_lo=span[0], entry_hi=span[0],
                          profit_share=0.0) if span else {}
        hlp[sel] = h.get('gross')
        addrs = sets[sel]['full']
        sc = lookback_scores(sel, addrs, btc)
        hold = hold_trip_stats(sel, addrs)
        snaps = snapshots_after(addrs, sel)
        rows = []
        for a in addrs:
            eqs = feats[sel][a]['account_value']
            out = outcomes(grids[a], ledgers[a], a, leaders[a], sel, eqs, nexp[sel].get(a), snaps.get(a), field, span, btc)
            rows.append({'vault': a, 'leader': leaders[a], **sc.get(a, {'score': None}),
                         'control': nexp[sel].get(a), 'equity': eqs, 'outside': a in sets[sel]['outside'],
                         'out': out, **hold.get(a, {})})
        per_window_full[sel] = rows
        per_window[sel] = [r for r in rows if a_in(r, use[sel])]
        print(f'{sel}: {len(per_window[sel])} vaults ({POPULATION}), {len(rows)} full', flush=True)
    res = assemble(per_window, hlp)
    res['reported']['full_population'] = {k: v for k, v in assemble(per_window_full, hlp, placebo_seeds=0, n_boot=1000,
                                                                     z_boot=0, gate=False).items() if k != 'reported'}
    res['reported']['common_spans'] = {str(s): v for s, v in spans.items()}
    betas = btc_betas({a: grids[a] for a in vaults}, btc)
    labels = V.label_top({s: [{'vault': r['vault'], 'score': r['score']} for r in rows if r.get('score') is not None]
                          for s, rows in per_window.items()})
    top_b = [betas[a] for (s, a), t in labels.items() if t and a in betas]
    all_b = [betas[a] for (s, a) in labels if a in betas]
    res['reported']['btc_beta'] = {'top_quintile_median': statistics.median(top_b) if top_b else None,
                                   'all_median': statistics.median(all_b) if all_b else None, 'top_n': len(top_b)}
    res['reported']['regime_by_window'] = {str(s): [btc.month_label(f'{(s + dt.timedelta(days=d)).year}-{(s + dt.timedelta(days=d)).month:02d}')
                                                    for d in (0, 28, 55)] for s in V.SELECTIONS}
    row = {'registration': REG.name, 'registration_commit': commit, 'run_at': dt.datetime.now(dt.UTC).isoformat(),
           'data_manifest_digest': digest,
           'params': {'selections': [s.isoformat() for s in V.SELECTIONS], 'read_end': V.READ_END.isoformat(),
                      'population': POPULATION, 'criterion2_version': VERSION, 'eligibility': V.ELIGIBILITY,
                      'profit_share': V.PROFIT_SHARE, 'z_crit': V.Z_CRIT, 'trials': TRIALS,
                      'cumulative_trials_discovery_year': CUMULATIVE, 'withdraw_field': field,
                      'identity_gate': shares, 'profit_share_gate': com_share},
           'hlp': {str(s): v for s, v in hlp.items()}, 'T1': res}
    with open(RESULTS, 'a') as f:
        f.write(json.dumps(row, default=str) + '\n')
    print(json.dumps({k: res[k] for k in ('ic_by_window', 'z', 'window_leader_z', 'positive_windows', 'top_minus_all',
                                          'top_minus_all_se', 'top_minus_hlp', 'top_mean', 'top_median', 'criteria',
                                          'pass_a', 'pass_b', 'pass', 'decision', 'placebo')}, indent=1, default=str))


def a_in(r, allowed):
    return r['vault'] in allowed


if __name__ == '__main__':
    main()
