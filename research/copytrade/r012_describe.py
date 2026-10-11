"""Registration 012 — the §3 numbers. BEHAVIOUR ONLY and UNCONDITIONAL (000f): positions OPENED by the
HL-pair rule (never split by position state, never a share of qualifying times, never per coin), the
Stage-2 entry expectation from the 2025-26 rate, and power by simulation. No funding accrual, price
change, gap change or P&L on real data. Never opens a Stage-2 panel.

    .venv/bin/python -I r012_describe.py      # -> data/carry012/logs/describe.json

Power noise inputs are 010's already-published behaviour levels (data/carry/logs/describe.json):
the 2025-26 spread SD (raw: the pessimistic bound; robust: a sensitivity row), the 2025-26 cross-coin
rho, and the discovery HL-pair gap SDs. Nothing new about the spread is measured here.
"""
import datetime as dt
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from ct import carry, carry012 as c12  # noqa: E402
import r012_run as run  # noqa: E402

OUT = HERE / 'data/carry012/logs/describe.json'
SRC = HERE / 'data/carry/logs/describe.json'
DAY = carry.DAY
SIMS = 2000
MUS = (0.0, 0.02, 0.03, 0.05)


def main():
    run.assert_allowed(OUT)
    run.assert_allowed(SRC)
    d10 = json.loads(SRC.read_text())
    res = {'terms': {f"{'-'.join(p)} {k}": dict(zip(('C_design_bp', 'H_h', 'threshold_pct_yr'),
                                                    (1e4 * carry.pair_terms(p, c)[0], carry.pair_terms(p, c)[1] / carry.HOUR,
                                                     100 * carry.pair_terms(p, c)[2])))
                     for p in c12.PAIRS_012 for k, c in (('core', 'BTC'), ('other', 'X'))}}
    print('counts ...', flush=True)
    cnt, pos1 = run.counts()
    res.update(cnt)
    n1 = len(pos1)
    months1 = len({p['month'] for p in pos1})
    span_days = (c12.STAGE1_END - c12.STAGE1_START) / DAY
    rate = n1 / span_days
    res['stage1_primary']['abs_signal_at_entry_pct'] = {
        k: 100 * sorted(abs(p['signal']) for p in pos1)[min(n1 - 1, int(k * n1))] for k in (.1, .5, .9)} if n1 else None
    res['stage1_primary']['cost_bp_on_N'] = {
        k: 1e4 * sorted(p['cost'] for p in pos1)[min(n1 - 1, int(k * n1))] for k in (.1, .5, .9)} if n1 else None
    res['stage2_expected_entries'] = {
        'rate_per_30d_2025_26': 30 * rate, 'months_in_stage1': months1,
        '2026-07-01 -> 2026-12-31 (184 d, exits inside)': rate * 184,
        'extension 2026-07-01 -> 2027-06-30 (365 d)': rate * 365,
        'note': 'naive rate x span; the edge rule (exit before the end) removes about H/span of it; the simulation '
                'windows below give the edge-corrected count'}
    hp = d10['holdout_power_inputs']
    sig_raw = {k: v / 100 for k, v in hp['sigma_s_pct_yr'].items()}
    sig_rob = {k: d10['spread_levels_exposed_2025_26'][k]['robust_sd_pct'] / 100 for k in ('core', 'other')}
    gp = d10['gap_level_within_sd_bp_by_pair']
    sig_g = {'core': (gp['core HL-BN'] + gp['core HL-BY']) / 2e4, 'other': (gp['other HL-BN'] + gp['other HL-BY']) / 2e4}
    rho = hp['rho']
    res['power_inputs'] = {'sigma_s_raw_pct_yr': {k: 100 * v for k, v in sig_raw.items()},
                           'sigma_s_robust_pct_yr': {k: 100 * v for k, v in sig_rob.items()},
                           'sigma_g_hl_pairs_bp': {k: 1e4 * v for k, v in sig_g.items()}, 'rho_2025_26': rho,
                           'sigma_month': 0.06, 'sims': SIMS, 'stage1_positions': n1, 'stage1_months': months1}
    h1 = [p for p in pos1 if p['t_in'] >= 1_767_225_600_000]          # entered 2026-01-01 or later
    res['stage2_expected_entries']['2026-H1 entry pattern (positions, months)'] = (len(h1), len({p['month'] for p in h1}))
    pats = {'2026-H1 rate (headline): first look = 2026-H1 as counted, extension = its copy shifted by 181 days':
            c12.replicate_half_year(h1, 181 * DAY),
            '2025-26 average rate: month-aligned 6/12-month windows, common to both designs': c12.month_windows(pos1)}
    res['power_windows'] = {k: {'windows': len(w), 'first_look_positions': [len(x['first']) for x in w],
                                'extension_positions': [len(x['full']) for x in w]} for k, w in pats.items()}
    designs = {'one look, t_{G-1}(0.95) (extension NO)': False,
               'owner design: extension, t_{G-1}(0.975) at each look': True}
    power = {}
    for pname, wins in pats.items():
        for noise, sig in (('raw_sd_bound', sig_raw), ('robust_sd', sig_rob)):
            for name, ext in designs.items():
                for mu in MUS:
                    print('power', pname[:12], noise, name[:12], mu, flush=True)
                    power[f'{pname[:14]} | {noise} | {name} | mu={mu}'] = c12.simulate_stages(
                        pos1, wins, sig, sig_g, rho, mu, sims=SIMS, seed=12, sigma_month=0.06, extension=ext)
    res['power'] = power
    res['written'] = dt.datetime.now(dt.UTC).isoformat()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, indent=1, default=str))
    print(json.dumps(res, indent=1, default=str))


if __name__ == '__main__':
    main()
