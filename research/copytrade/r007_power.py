"""Registration 007 (L1) — joint power of the criteria, before any outcome (Revision 2).

    .venv/bin/python -I r007_power.py

Writes data/vaults/describe/power_r2.json and data/vaults/describe/power_r2.md (the tables quoted
verbatim in the registration, three decimals) and prints the same.

Behaviour-only inputs (no vault return is read or computed):
  * the Revision-1 eligible sets per window (describe_r1.json), full and outside-depositor, with
    real vault identities and real leaders;
  * lookback median GROSS leverage (idiosyncratic dispersion) and lookback median NET leverage
    (market beta and the criterion-1 control);
  * BTC's unconditional 56-day log-return SD and the realised BTC hold returns from 1H closes
    filtered < 2026-06-08 in the query (market data).
Model (registration §3): idiosyncratic SD = max(5%, k x gross leverage x sd_BTC56), k = 1; one
market draw per window N(0, sd_BTC56) x net exposure; rho 0.42; NAV measurement noise with
Spearman 0.93 in every row; arithmetic mean gross return of every vault = HLP's. Criterion 1 =
Fisher Z >= 2.33 AND window t >= 3.182 AND 3 of 4 positive. Discovery rows: 2,000 simulated data
sets; holdout rows: 5,000. Null rows carry a Wilson 95% upper bound.
"""
import json
import math
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ct import vaults as V  # noqa: E402

OUT = V.VAULT_DIR / 'describe'
DISC_SIMS, HOLD_SIMS = 2000, 5000


def btc_sd56(btc, start, end):
    daily = {}
    for ts, c in zip(btc.ts, btc.close):
        if ts % V.DAY == V.DAY - V.HOUR:
            daily[(ts + V.HOUR) // V.DAY] = c
    days = sorted(d for d in daily if V.ms(start) // V.DAY <= d <= V.ms(end) // V.DAY)
    rets = [math.log(daily[d + 56] / daily[d]) for d in days if d + 56 in daily]
    return statistics.stdev(rets), len(rets)


def f3(x):
    return '—' if x is None else f'{x:.3f}'


def main():
    desc = json.loads((OUT / 'describe_r1.json').read_text())
    feats = {s: {r['address']: r for r in json.loads((OUT / f'features_{s}.json').read_text())} for s in desc}
    nexp = {s: json.loads((OUT / f'net_exposure_{s}.json').read_text()) for s in desc}
    leader = {v['address']: v['leader'] for v in V.load_list()}
    btc = V.Btc(end_ms=V.READ_END_MS)
    sig, n_sig = btc_sd56(btc, V.ARCHIVE_START, V.READ_END)
    realised = [math.log(btc.close_before(V.ms(s) + V.HOLD_DAYS * V.DAY) / btc.close_before(V.ms(s))) for s in V.SELECTIONS]
    gl, nl = {}, {}
    for s in desc:
        for a in desc[s]['eligible_list']:
            if feats[s][a].get('median_leverage') is not None:
                gl.setdefault(a, []).append(feats[s][a]['median_leverage'])
            if nexp[s].get(a) is not None:
                nl.setdefault(a, []).append(nexp[s][a])
    gl = {a: statistics.median(v) for a, v in gl.items()}
    nl = {a: statistics.median(v) for a, v in nl.items()}
    mg = statistics.median(gl.values())
    pops = {'outside': [desc[s.isoformat()]['outside_list'] for s in V.SELECTIONS],
            'full': [desc[s.isoformat()]['eligible_list'] for s in V.SELECTIONS]}
    allv = {a for p in pops.values() for w in p for a in w}
    beta = {a: nl.get(a, 0.0) for a in allv}

    def sds(k):
        return {a: max(0.05, k * gl.get(a, mg) * sig) for a in allv}
    lb4 = V.overlap_matrix(V.SELECTIONS, V.LOOKBACK_DAYS)
    out = {'btc_sd56': sig, 'btc_sd56_windows': n_sig, 'btc_hold_log_returns': realised, 'disc_sims': DISC_SIMS,
           'hold_sims': HOLD_SIMS, 'discovery': [], 'holdout': []}
    md = [f'BTC 56-day log-return SD (discovery, {n_sig} windows): {sig:.3f}. BTC log return over the four holds: '
          + ', '.join(f'{x:+.3f}' for x in realised) + '.', '']

    def disc(label, pop, ic, sims=DISC_SIMS, **kw):
        kw.setdefault('sd', sds(1.0))
        p = V.simulate_power(pops[pop], kw.pop('sd'), ic, beta=beta, leader=leader, sims=sims, seed=7, lb_overlap=lb4,
                             sigma_m=sig, **kw)
        ua = V.binom_upper(round(p['all_a'] * sims), sims)
        ub = V.binom_upper(round(p['all_b'] * sims), sims)
        u1 = V.binom_upper(round(p['1'] * sims), sims)
        row = {'design': label, 'pop': pop, 'ic': ic, **{k: round(v, 4) if isinstance(v, float) else v for k, v in p.items()},
               'upper_all_a': ua, 'upper_all_b': ub, 'upper_1': u1}
        out['discovery'].append(row)
        print(f'  {label:<44} {pop:<7} IC {ic:.1f}: c1 plain {p["1_plain"]:.3f} c1 {p["1"]:.3f} (≤{u1:.3f}) c2a {p["2a"]:.3f} '
              f'c2b {p["2b"]:.3f} c3 {p["3"]:.3f} c4 {p["4"]:.3f} c5 {p["5"]:.3f} | ALL (a) {p["all_a"]:.3f} (≤{ua:.3f}) '
              f'(b) {p["all_b"]:.3f} (≤{ub:.3f})', flush=True)
        return row
    print('Discovery, joint power:')
    base = {}
    for pop in ('outside', 'full'):
        for ic in (0.0, 0.1, 0.2, 0.3):
            base[(pop, ic)] = disc('base', pop, ic)
    print('Nulls:')
    nulls = []
    for c in (-0.2, -0.4):
        nulls.append(disc(f'bear null, corr {c}, net-exposure terciles', 'outside', 0.0, c_beta=c, market=realised))
        nulls.append(disc(f'bear null, corr {c}, control unrelated', 'outside', 0.0, c_beta=c, control='random', market=realised))
    nulls.append(disc('bear null, corr -0.4, market-adjusted outcome', 'outside', 0.0, c_beta=-0.4, market=realised, key='adj'))
    for sh in (0.13, 0.26):
        nulls.append(disc(f'window-shock null, shift sd {sh}', 'outside', 0.0, shock_sd=sh))
    print('Sensitivity (outside, IC 0.2):')
    sens = [disc('k = 0.5', 'outside', 0.2, sd=sds(0.5)), disc('rho 0.25', 'outside', 0.2, rho_score=0.25),
            disc('average vault HLP - 2%', 'outside', 0.2, mean_excess=-0.02),
            disc('market-adjusted outcome (reported block)', 'outside', 0.2, key='adj')]
    # ---------------------------------------------------------------- holdout options
    o = pops['outside']
    designs = {'A (2 windows: 07-06, 08-31; sets 50, 54)': (V.HOLDOUT_A, [o[2], o[3]]),
               'B (4 windows: + 10-26, 12-21; sets 74, 64, 50, 54)': (V.HOLDOUT_B, o),
               'B conservative (sets 50, 54, 50, 54)': (V.HOLDOUT_B, [o[2], o[3], o[2], o[3]])}
    print('Holdout (CONFIRM on the market-adjusted outcome: Z >= 1.64, D/SE >= 1.64, sign rule, top beats HLP):')
    hold = {}
    for name, (sels, wins) in designs.items():
        lbh = V.overlap_matrix(sels, V.LOOKBACK_DAYS)
        for label, kw in ([(f'IC {ic}', {'ic': ic}) for ic in (0.0, 0.1, 0.2, 0.3)]
                          + [('IC 0.2, attenuation 0.88', {'ic': 0.2, 'attenuation': 0.88})]
                          + [(f'rally null, corr +{c}', {'ic': 0.0, 'c_beta': c, 'market': [sig] * len(wins)}) for c in (0.2, 0.4)]
                          + [('rally null, corr +0.4, RAW outcome', {'ic': 0.0, 'c_beta': 0.4, 'market': [sig] * len(wins), 'key': 'net'})]):
            ic = kw.pop('ic')
            pc = V.holdout_power(wins, sds(1.0), ic, beta=beta, leader=leader, sims=HOLD_SIMS, lb_overlap=lbh, sigma_m=sig, **kw)
            up = V.binom_upper(round(pc * HOLD_SIMS), HOLD_SIMS)
            hold[(name, label)] = (pc, up)
            out['holdout'].append({'design': name, 'case': label, 'confirm': round(pc, 4), 'upper': up})
            print(f'  {name:<52} {label:<36} P(confirm) {pc:.3f} (≤{up:.3f})', flush=True)
    # ---------------------------------------------------------------- markdown tables
    md += ['**Discovery, joint power** (2,000 data sets per row; Wilson 95% upper bound in brackets for IC 0):', '',
           '| population | version | IC 0 | IC 0.1 | IC 0.2 | IC 0.3 | c1 at IC 0.2 |', '|---|---|---|---|---|---|---|']
    for pop in ('outside', 'full'):
        for ver, k, u in (('(a) D > 0', 'all_a', 'upper_all_a'), ('(b) D/SE ≥ 1.64', 'all_b', 'upper_all_b')):
            r0 = base[(pop, 0.0)]
            md.append(f'| {pop} | {ver} | {f3(r0[k])} (≤ {f3(r0[u])}) | {f3(base[(pop, 0.1)][k])} | {f3(base[(pop, 0.2)][k])} | '
                      f'{f3(base[(pop, 0.3)][k])} | {f3(base[(pop, 0.2)]["1"])} |')
    md += ['', '**Nulls** (outside set, IC 0, 2,000 data sets):', '',
           '| null | plain c1 | c1 (with window t) | all five (a) | all five (b) |', '|---|---|---|---|---|']
    for r in nulls:
        md.append(f'| {r["design"]} | {f3(r["1_plain"])} | {f3(r["1"])} (≤ {f3(r["upper_1"])}) | {f3(r["all_a"])} (≤ {f3(r["upper_all_a"])}) | '
                  f'{f3(r["all_b"])} (≤ {f3(r["upper_all_b"])}) |')
    md += ['', '**Sensitivity** (outside, IC 0.2): ' + '; '.join(f'{r["design"]} {f3(r["all_a"])} (a) / {f3(r["all_b"])} (b)' for r in sens)
           + '.', '', '**Holdout options, P(CONFIRM)** (5,000 data sets per cell; Wilson upper bound for nulls):', '',
           '| option | IC 0 | IC 0.1 | IC 0.2 | IC 0.3 | IC 0.2 at att. 0.88 | rally null +0.2 | rally null +0.4 | +0.4 on RAW outcome |',
           '|---|---|---|---|---|---|---|---|---|']
    for name in designs:
        g = lambda lab: hold[(name, lab)]  # noqa: E731
        md.append(f'| {name} | {f3(g("IC 0.0")[0])} (≤ {f3(g("IC 0.0")[1])}) | {f3(g("IC 0.1")[0])} | {f3(g("IC 0.2")[0])} | '
                  f'{f3(g("IC 0.3")[0])} | {f3(g("IC 0.2, attenuation 0.88")[0])} | {f3(g("rally null, corr +0.2")[0])} (≤ {f3(g("rally null, corr +0.2")[1])}) | '
                  f'{f3(g("rally null, corr +0.4")[0])} (≤ {f3(g("rally null, corr +0.4")[1])}) | {f3(g("rally null, corr +0.4, RAW outcome")[0])} |')
    md += ['', '**End to end** (discovery outside (a) or (b) × holdout P(confirm), attenuation 0.93):', '',
           '| option | (a) IC 0.2 | (a) IC 0.3 | (b) IC 0.2 | (b) IC 0.3 |', '|---|---|---|---|---|']
    for name in designs:
        md.append(f'| {name} | {f3(base[("outside", 0.2)]["all_a"] * hold[(name, "IC 0.2")][0])} | '
                  f'{f3(base[("outside", 0.3)]["all_a"] * hold[(name, "IC 0.3")][0])} | '
                  f'{f3(base[("outside", 0.2)]["all_b"] * hold[(name, "IC 0.2")][0])} | '
                  f'{f3(base[("outside", 0.3)]["all_b"] * hold[(name, "IC 0.3")][0])} |')
    (OUT / 'power_r2.json').write_text(json.dumps(out, indent=1))
    (OUT / 'power_r2.md').write_text('\n'.join(md) + '\n')
    print('\n' + '\n'.join(md))


if __name__ == '__main__':
    main()
