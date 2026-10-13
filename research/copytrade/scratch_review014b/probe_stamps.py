"""Out of calendar (2020): is a tradingview candle stamped at its OPEN? Compare each BTC-PERPETUAL 1h close
stamped t with the option trades' index price just before t + 1 h (open-stamp hypothesis) and just before t
(close-stamp hypothesis). Uses the tail pulls only (all < 2021-03-24)."""
import json, bisect, statistics, sys
from pathlib import Path
HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from ct import shortvol as sv
R = HERE / 'data/r014/tail_pre2021'
perp = {r[0]: r[4] for r in json.loads((R / 'underlying_20261013/perp_BTC.json').read_text())}
for wk in ('2020-07-24', '2020-11-20', '2021-02-19'):
    tr = sorted((r['timestamp'], r['index_price']) for r in json.loads((R / f'options_20261013/BTC_{wk}.json').read_text()) if r.get('index_price'))
    ts = [t for t, _ in tr]
    def idx_before(t, win=2 * 60_000):
        i = bisect.bisect_left(ts, t) - 1
        return tr[i][1] if i >= 0 and t - ts[i] <= win else None
    a, b = [], []
    E = sv.ms(*map(int, wk.split('-')), 8)
    for t in range(E, E + 7 * sv.DAY, sv.HOUR):
        c = perp.get(t); x, y = idx_before(t + sv.HOUR), idx_before(t)
        if c and x and y:
            a.append(abs(c / x - 1)); b.append(abs(c / y - 1))
    print(wk, 'hours', len(a), '| median |close - index just before t+1h|', f'{statistics.median(a) * 1e4:.1f} bp', '| ... just before t', f'{statistics.median(b) * 1e4:.1f} bp')
