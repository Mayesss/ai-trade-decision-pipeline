"""Probe 3 (OUT-OF-CALENDAR pre-2021 tape; entry windows only; no outcome). For each computed tail
coin-week where the REGISTERED straddle_entry succeeds: which leg's mark set the IV, and how far the
premium moves if the IV is instead taken from (i) the other leg's latest mark at the same strike, or
(ii) the forward implied by put-call parity on the two latest marks (both legs then inverted on F)."""
import json, statistics, sys
from pathlib import Path
sys.path.insert(0, '.')
from ct import shortvol as sv
D = Path('data/r014/tail_pre2021/options_20261013')
U = json.loads(Path('data/r014/tail_pre2021/underlying_20261013/perp_BTC.json').read_text())
perp = {c: {r[0] + sv.HOUR: r[4] for r in json.loads(Path(f'data/r014/tail_pre2021/underlying_20261013/perp_{c}.json').read_text())} for c in sv.COINS}
out = []
for f in sorted(p for p in D.glob('*_*.json') if p.stem[:3] in ('BTC', 'ETH')):
    coin, day = f.stem.split('_')
    y, m, d = map(int, day.split('-'))
    E = sv.ms(y, m, d, 8); X = E + 7 * sv.DAY
    tr = json.loads(f.read_text())
    em = sv.straddle_entry(coin, E, tr, perp[coin].get(E))
    if 'missing' in em:
        continue
    K = em['K']; T = 7 / 365.0
    win = sv.window_rows(tr, E - 4 * sv.HOUR, E)
    legs = {}
    for r in win:
        p = sv.parse_name(r['instrument_name'])
        if p and p[0] == coin and p[1] == X and p[2] == K:
            if p[3] not in legs or (r['timestamp'], r.get('trade_seq', 0)) > (legs[p[3]]['timestamp'], legs[p[3]].get('trade_seq', 0)):
                legs[p[3]] = r
    used = max(legs.items(), key=lambda kv: (kv[1]['timestamp'], kv[1].get('trade_seq', 0)))[0]
    prem = em['mark_call'] + em['mark_put']
    rec = {'coin': coin, 'day': day, 'used': 'C' if used else 'P', 'iv_used': round(em['iv'] * 100, 1)}
    if True in legs and False in legs:
        ivs = {}
        for cp, r in legs.items():
            F0 = float(r['index_price']); T0 = (X - r['timestamp']) / sv.DAY / 365.0
            ivs[cp] = sv.implied_vol(float(r['mark_price']) * F0, F0, K, T0, cp)
        other = ivs[not used]
        if other:
            p_other = sv.b76(em['S'], K, T, other, True) + sv.b76(em['S'], K, T, other, False)
            rec['iv_other'] = round(other * 100, 1)
            rec['prem_err_vs_other_pct'] = round((prem / p_other - 1) * 100, 1)
            avg = 0.5 * (ivs[True] + ivs[False])
            p_avg = sv.b76(em['S'], K, T, avg, True) + sv.b76(em['S'], K, T, avg, False)
            rec['prem_err_vs_avg_pct'] = round((prem / p_avg - 1) * 100, 1)
    out.append(rec)
for r in out:
    print(r)
e = [r['prem_err_vs_avg_pct'] for r in out if 'prem_err_vs_avg_pct' in r]
print('n with both legs', len(e), 'median |err vs two-leg avg| %', statistics.median(abs(x) for x in e), 'mean err %', round(statistics.fmean(e), 2),
      'share call-set', sum(r['used'] == 'C' for r in out) / len(out))
