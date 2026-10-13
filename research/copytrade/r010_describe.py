"""Registration 010 — the §3 numbers. BEHAVIOUR ONLY and UNCONDITIONAL (Revision 1, B1): coverage,
costs, the positions the rule opens (counts only — never split by position state, never a share of
qualifying times), unconditional levels of the spread and of the price gap, delisted-coin coverage
per venue (B2), margin distances, and power by simulation. Nothing after an entry is read.

    .venv/bin/python -I r010_describe.py      # -> data/carry/logs/describe.json
"""
import datetime as dt
import json
import math
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from ct import carry  # noqa: E402
import r010_run as run  # noqa: E402

OUT = HERE / 'data/carry/logs/describe.json'
HOUR, DAY, GRID = carry.HOUR, carry.DAY, carry.GRID
SIMS = 2000


def year(t):
    return dt.datetime.fromtimestamp(t / 1000, dt.UTC).year


def sd(xs):
    return statistics.pstdev(xs) if len(xs) > 1 else None


def robust_sd(xs):
    if len(xs) < 3:
        return None
    m = statistics.median(xs)
    return 1.4826 * statistics.median(abs(x - m) for x in xs)


def q(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else None


def levels(panel, start, end, need_gap, pairs=carry.PAIRS):
    """Unconditional levels at every eligible coin-pair-time of the grid (whether or not the coin is
    held): the spread, the gap, and one canonical pair per coin for the co-movement."""
    elig = panel.eligible(need_gap)
    s_by = {'core': [], 'other': []}
    s_year = {}
    gaps = {}
    canon = {}
    times = run.grid(start, end)
    for t in times:
        for coin in panel.coins:
            cand = carry.candidates(coin, panel.funding, t, elig, pairs)
            for excess, k, pair, s, c, h, th in cand:
                s_by[carry.cls(coin)].append(s)
                s_year.setdefault((year(t), carry.cls(coin)), []).append(s)
                if need_gap:
                    gaps.setdefault((coin, pair), []).append(panel.gap(coin, pair[0], pair[1], t))
            if cand:
                canon.setdefault(coin, {})[t] = min(cand, key=lambda x: x[1])[3]
    return s_by, s_year, gaps, canon, times


def rho_hat(canon, times, min_n=5):
    """Average pairwise contemporaneous correlation of the standardised spread levels across coins:
    E[m_t^2] = rho + (1 - rho) / n_t for the cross-sectional mean m_t of n_t standardised series."""
    z = {}
    for c, ser in canon.items():
        v = list(ser.values())
        if len(v) < 30:
            continue
        m, s = statistics.fmean(v), statistics.pstdev(v)
        if s > 0:
            z[c] = {t: (x - m) / s for t, x in ser.items()}
    est = []
    for t in times:
        xs = [zc[t] for zc in z.values() if t in zc]
        n = len(xs)
        if n >= min_n:
            m = sum(xs) / n
            est.append((m * m - 1 / n) / (1 - 1 / n))
    return statistics.fmean(est) if est else None, len(z)


def within_sd(groups):
    """Pooled within-group SD (each group de-meaned over its span: a level statistic)."""
    ss = n = 0
    for xs in groups:
        xs = [x for x in xs if x is not None]
        if len(xs) < 2:
            continue
        m = statistics.fmean(xs)
        ss += sum((x - m) ** 2 for x in xs)
        n += len(xs) - 1
    return math.sqrt(ss / n) if n else None


def delisted_coverage(panel, pos, cut=carry.EXPOSED_END - 30 * DAY):
    """B2: per venue, coins whose funding stops before 2026-06-01 (delisted), how many have name-check
    closes during their life, how many trade in discovery (both legs eligible at least once), and how
    many appear in a discovery position. Timestamps and existence only."""
    out = {}
    pos_v = {}
    for p in pos:
        for v in p['pair']:
            pos_v.setdefault(v, set()).add(p['coin'])
    for v in ('HL', 'BN', 'BY', 'OK'):
        dead = [c for (vv, c), f in panel.funding.items() if vv == v and f.last < cut]
        with_lv = [c for c in dead if v == 'BN' or panel.name_res.get((v, c))]
        live_disc = [c for c in dead if panel.funding[(v, c)].first < carry.DISC_END]
        lv_disc = [c for c in live_disc if v == 'BN' or any(t < carry.DISC_END for t, _ in panel.name_res.get((v, c), []))]
        out[v] = {'delisted_before_2026_06': len(dead), 'with_name_check_closes': len(with_lv),
                  'live_in_discovery': len(live_disc), 'live_in_discovery_with_closes': len(lv_disc),
                  'in_a_primary_discovery_position': len(set(dead) & pos_v.get(v, set()))}
    return out


def power_rows(pos, sig_s, sig_g, rho, mus, sims, sigma_months, z=carry.Z):
    rows = []
    for sm in sigma_months:
        for mu in mus:
            cons = carry.simulate_power(pos, sig_s, sig_g, rho, mu, sims=sims, seed=1, z=z, sigma_month=sm)
            rows.append({'mu_yr': mu, 'sigma_month': sm, 'persistent_noise': cons})
    return rows


def main():
    res = {}
    print('discovery panel ...', flush=True)
    pdisc = run.Panel(carry.DISC_END)
    res['coins'] = {'universe': len(pdisc.universe), 'with_2_venues_in_discovery': len(pdisc.coins),
                    'per_venue_discovery': {v: sum(1 for c in pdisc.coins if (v, c) in pdisc.funding
                                                   and pdisc.funding[(v, c)].first < carry.DISC_END)
                                            for v in ('HL', 'BN', 'BY', 'OK')}}
    res['terms'] = {f"{'-'.join(p)} {k}": dict(zip(('C_design_bp', 'H_h', 'threshold_pct_yr'),
                                                   (1e4 * carry.pair_terms(p, c)[0], carry.pair_terms(p, c)[1] / HOUR,
                                                    100 * carry.pair_terms(p, c)[2])))
                    for p in carry.PAIRS for k, c in (('core', 'BTC'), ('other', 'X'))}
    res['margin_distance_short_1x'] = {f'{v} {k}': carry.liq_distance(v, c) for v in carry.VENUES
                                       for k, c in (('core', 'BTC'), ('other', 'X'))}
    ev, (_, pos, pexp, pos_e) = run.events()
    res.update(ev)
    srt = sorted(pos, key=lambda p: (p['t_in'], p['coin']))
    res['discovery']['half_split_t'] = dt.datetime.fromtimestamp(srt[len(srt) // 2]['t_in'] / 1000, dt.UTC).isoformat()
    res['discovery']['abs_signal_at_entry_pct'] = {k: 100 * q([abs(p['signal']) for p in pos], k) for k in (.1, .5, .9)}
    res['discovery']['cost_bp_on_N'] = {k: 1e4 * q([p['cost'] for p in pos], k) for k in (.1, .5, .9)}
    res['delisted_coverage'] = delisted_coverage(pdisc, pos)
    print('levels ...', flush=True)
    s_by, s_year, gaps, canon, times = levels(pdisc, carry.DISC_START, carry.DISC_END, True)
    res['spread_levels_discovery'] = {k: {'n': len(v), 'sd_pct': 100 * sd(v), 'robust_sd_pct': 100 * robust_sd(v),
                                          'p10': 100 * q(v, .1), 'p50': 100 * q(v, .5), 'p90': 100 * q(v, .9)}
                                      for k, v in s_by.items() if v}
    res['spread_levels_by_year'] = {f'{y} {k}': {'n': len(v), 'sd_pct': 100 * sd(v)} for (y, k), v in sorted(s_year.items())}
    rho, nser = rho_hat(canon, times)
    res['rho_discovery'] = {'rho': rho, 'series': nser}
    gsd = {k: within_sd([v for (c, p), v in gaps.items() if carry.cls(c) == k]) for k in ('core', 'other')}
    res['gap_level_within_sd_bp'] = {k: 1e4 * v for k, v in gsd.items() if v is not None}
    res['gap_level_within_sd_bp_by_pair'] = {
        f'{k} {"-".join(pp)}': 1e4 * (within_sd([v for (c, p), v in gaps.items() if carry.cls(c) == k and p == pp])
                                      or float('nan')) for k in ('core', 'other') for pp in carry.PAIRS}
    print('exposed levels ...', flush=True)
    es_by, _, _, ecanon, etimes = levels(pexp, carry.EXPOSED_START, carry.EXPOSED_END, False)
    res['spread_levels_exposed_2025_26'] = {k: {'n': len(v), 'sd_pct': 100 * sd(v), 'robust_sd_pct': 100 * robust_sd(v),
                                                'p10': 100 * q(v, .1), 'p50': 100 * q(v, .5), 'p90': 100 * q(v, .9)}
                                            for k, v in es_by.items() if v}
    erho, _ = rho_hat(ecanon, etimes)
    print('power ...', flush=True)
    sig_s = {k: sd(v) for k, v in s_by.items() if v}
    sig_g = {k: v for k, v in gsd.items() if v is not None}
    res['power_inputs'] = {'sigma_s_pct_yr': {k: 100 * v for k, v in sig_s.items()},
                           'sigma_g_bp': {k: 1e4 * v for k, v in sig_g.items()}, 'rho': rho, 'sims': SIMS}
    mus = (0.0, 0.02, 0.03, 0.04, 0.05, 0.10)
    res['power_T1'] = power_rows(pos, sig_s, sig_g, rho, (0.0, 0.02, 0.03, 0.05), SIMS, (0.06,))
    core = [p for p in pos if p['cls'] == 'core']
    res['power_T2_core'] = power_rows(core, sig_s, sig_g, rho, (0.0, 0.02, 0.03, 0.05), SIMS, (0.06,))
    esig = {k: sd(v) for k, v in es_by.items() if v}
    res['holdout_power_inputs'] = {'pattern': 'exposed 2025-26 positions (counts plan)', 'n_pattern': len(pos_e),
                                   'sigma_s_pct_yr': {k: 100 * v for k, v in esig.items()}, 'rho': erho,
                                   'sigma_g_bp_from_discovery': {k: 1e4 * v for k, v in sig_g.items()}}
    hold = {}
    spans = {'2026-09-30': 92 * DAY, '2026-12-31': 184 * DAY}
    for date, span in spans.items():
        for trial, pat in (('T1', pos_e), ('T2', [p for p in pos_e if p['cls'] == 'core'])):
            for sm in (0.0, 0.06):
                for mu in (0.0, 0.02, 0.03, 0.05):
                    hold[f'{date} {trial} sm={sm} mu={mu}'] = carry.simulate_holdout(
                        pat, span, esig, sig_g, erho, mu, sims=1000, seed=7, sigma_month=sm) if len(pat) > 3 else None
    res['holdout_power_bound'] = hold
    print('joint power ...', flush=True)
    joint = {}
    designs = {'2026-09-30': (92 * DAY, None), '2026-12-31': (184 * DAY, None),
               '2026-12-31 + extension to 2027-06-30': (184 * DAY, 365 * DAY)}
    for name, (span, ext) in designs.items():
        for mu in (0.0, 0.02, 0.03, 0.05):
            joint[f'{name} mu={mu}'] = carry.simulate_joint(pos, pos_e, span, sig_s, sig_g, rho, mu, sims=SIMS, seed=11,
                                                           sigma_month=0.06, hold_sigma_s=esig, rho_hold=erho,
                                                           extension_span_ms=ext)
    res['joint_power_T1_sigma_month_6pct'] = joint
    OUT.write_text(json.dumps(res, indent=1, default=str))
    print(json.dumps(res, indent=1, default=str))


if __name__ == '__main__':
    main()
