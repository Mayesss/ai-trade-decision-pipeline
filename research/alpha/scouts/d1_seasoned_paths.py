"""D1 scout: squeeze PATH SHAPE on SEASONED coins (a proxy), to weigh margin depth against stops.

NON-LISTING WINDOWS ONLY. Source: the 009 panel (1h OHLC + funding, 2021-11 -> 2024-01-07), read-only. A coin-week
enters only if its start is >= 30 days after the symbol's first kline day (n3_power rule) and its hold ends >= 7 days
before the end of its alive segment (delisting tails out). BTC and ETH out. No Binance listing window (days 0-29) is
touched; nothing dated on or after 2024-01-08 exists in the panel.

Per coin-week, a SHORT of notional 1 at the open of the start bar, held 168 hourly bars, under several tail rules:
  liq1   1x isolated (margin = notional): liquidated when a 1h HIGH reaches entry * 2/1.05 (MMR 5 %), loss -1
  liq05  0.5x (margin = 2 x notional): liquidated at entry * 3/1.05, loss -2 (per unit NOTIONAL)
  stopX  buy-stop at entry * (1 + X) on the 1h high, filled at max(stop, bar open) (gap at the open), X = .3/.5/.9;
         a stop never fills worse than... nothing: the fill is the bar open if it gapped above the stop
  liq05+f  liq05, plus an exit at the close of the bar of the first funding settlement <= -0.3 % (shorts paying)
Funding: the short receives rate * (price at settlement / entry) at each settlement inside the hold (until exit).
No fees (identical across rules; the comparison is between rules).

Reported: per sample, the mean DIFFERENCE of each rule against liq1 (not the level), SDs, the share of weeks each
rule triggers, and, conditional on a liq1 trigger, where the price is at the 7-day exit (the transient vs persistent
question). Stratum 'young' = coin-weeks starting 30-120 days after the first kline.

Usage: python -I d1_seasoned_paths.py <panel_dir> <out_txt>
"""
import datetime as dt
import json
import math
import statistics
import sys

import duckdb

HOUR, DAY = 3_600_000, 86_400_000
H = 168
MMR = 0.05
X1 = 2 / (1 + MMR)          # 1.905
X05 = 3 / (1 + MMR)         # 2.857
FTRIG = -0.003


def ms(day):
    return int(dt.datetime.fromisoformat(day).replace(tzinfo=dt.UTC).timestamp() * 1000)


def short_path(o, h, c, fund, e, rule):
    """P&L per unit notional of a short entered at e over bars o/h/c (lists of len H). fund: [(bar index, rate)]."""
    fi = 0
    pnl_f = 0.0
    for i in range(H):
        while fi < len(fund) and fund[fi][0] <= i:
            j, rate = fund[fi]
            pnl_f += rate * (o[j] / e)
            fi += 1
            if rule == 'liq05+f' and rate <= FTRIG:
                return 1 - c[i] / e + pnl_f, 'ftrig'
        x = h[i] / e
        if rule == 'liq1' and x >= X1:
            return -1.0, 'trig'
        if rule in ('liq05', 'liq05+f') and x >= X05:
            return -2.0, 'trig'
        if rule.startswith('stop'):
            s = 1 + float(rule[4:])
            if x >= s:
                fill = max(s, o[i] / e)
                return 1 - fill + pnl_f, 'trig'
    return 1 - c[H - 1] / e + pnl_f, None


RULES = ('liq1', 'liq05', 'stop.3', 'stop.5', 'stop.9', 'liq05+f')


def main():
    panel, out = sys.argv[1], sys.argv[2]
    life = json.load(open(f'{panel}/life.json'))
    con = duckdb.connect()
    bars = {}
    for s, ts, o, h, c, qv in con.execute(
            f"SELECT symbol, ts, open, high, close, qv FROM '{panel}/bars_1h.parquet' ORDER BY symbol, ts").fetchall():
        bars.setdefault(s, []).append((ts, o, h, c, qv))
    fund = {}
    for s, ts, r in con.execute(f"SELECT symbol, ts, rate FROM '{panel}/funding.parquet' ORDER BY symbol, ts").fetchall():
        fund.setdefault(s, []).append((ts, r))
    last_ts = max(b[-1][0] for b in bars.values())
    starts_mon, starts_day = [], []
    w = ms('2021-12-06')
    while w + H * HOUR <= last_ts + HOUR:
        starts_day.append(w)
        if (w - ms('2021-12-06')) % (7 * DAY) == 0:
            starts_mon.append(w)
        w += DAY
    idx = {s: {b[0]: i for i, b in enumerate(v)} for s, v in bars.items()}
    cumqv = {}
    for s, v in bars.items():
        acc, cq = 0.0, [0.0]
        for b in v:
            acc += b[4] or 0.0
            cq.append(acc)
        cumqv[s] = cq

    def sample(starts):
        rows = []
        for w in starts:
            cand = []
            for s, v in bars.items():
                L = life.get(s)
                if not L or s in ('BTCUSDT', 'ETHUSDT'):
                    continue
                age = (w - ms(L['first_kline_day'])) / DAY
                if age < 30:
                    continue
                if not any(ms(a) <= w and w + H * HOUR <= ms(b) - 7 * DAY for a, b in L['segments']):
                    continue
                i0 = idx[s].get(w)
                if i0 is None or i0 + H > len(v) or v[i0 + H - 1][0] != w + (H - 1) * HOUR:
                    continue
                j30 = idx[s].get(w - 30 * DAY)
                qv30 = cumqv[s][i0] - cumqv[s][j30] if j30 is not None else cumqv[s][i0]
                cand.append((s, i0, age, qv30))
            if len(cand) < 20:
                continue
            q75 = sorted(x[3] for x in cand)[int(0.75 * len(cand))]
            for s, i0, age, qv30 in cand:
                v = bars[s][i0:i0 + H]
                o, h, c = [b[1] for b in v], [b[2] for b in v], [b[3] for b in v]
                e = o[0]
                if not e or e <= 0:
                    continue
                fl = [((t - w) // HOUR, r) for t, r in fund.get(s, []) if w < t <= w + H * HOUR - HOUR]
                fl = [(int(j), r) for j, r in fl if 0 <= j < H]
                res = {r: short_path(o, h, c, fl, e, r) for r in RULES}
                rows.append({'small': qv30 < q75, 'young': age < 120, 'res': res,
                             'exit_x': c[H - 1] / e, 'max_x': max(h) / e})
        return rows

    lines = []

    def report(name, rows):
        n = len(rows)
        if n == 0:
            return
        lines.append(f'\n## {name}: {n} coin-weeks')
        base = [r['res']['liq1'][0] for r in rows]
        for rule in RULES:
            v = [r['res'][rule][0] for r in rows]
            d = [a - b for a, b in zip(v, base)]
            trig = sum(1 for r in rows if r['res'][rule][1] is not None)
            lines.append(f'{rule:8s} trig {trig:5d} ({trig / n:.4f}) | SD {statistics.pstdev(v):.4f} | p1 '
                         f'{sorted(v)[int(0.01 * n)]:+.3f} | min {min(v):+.3f} | mean diff vs liq1 {sum(d) / n:+.5f}'
                         f' (se {statistics.pstdev(d) / math.sqrt(n):.5f})')
        for thr in (1.3, 1.5, X1, X05):
            k = sum(1 for r in rows if r['max_x'] >= thr)
            lines.append(f'touch high >= {thr:.3f} x entry: {k} ({k / n:.4f})')
        liq = [r for r in rows if r['max_x'] >= X1]
        if liq:
            ex = sorted(r['exit_x'] for r in liq)
            m = len(liq)
            lines.append(f'conditional on a liq1 trigger (n {m}): exit/entry quartiles '
                         f'{ex[m // 4]:.3f} / {ex[m // 2]:.3f} / {ex[(3 * m) // 4]:.3f}; exit >= {X1:.3f}: '
                         f'{sum(x >= X1 for x in ex)}; exit <= 1.5: {sum(x <= 1.5 for x in ex)}; exit <= 1.0: '
                         f'{sum(x <= 1.0 for x in ex)}; touched {X05:.3f}: {sum(r["max_x"] >= X05 for r in liq)}')
            for rule in ('liq05', 'stop.5', 'liq05+f'):
                v = [r['res'][rule][0] for r in liq]
                lines.append(f'   {rule:8s} mean P&L on these {sum(v) / m:+.3f} (liq1: -1.000)')
        mid = [r for r in rows if 1.5 <= r['max_x'] < X1]
        if mid:
            m = len(mid)
            lines.append(f'touched [1.5, {X1:.3f}) without liq1 (n {m}): mean P&L liq1 '
                         f'{sum(r["res"]["liq1"][0] for r in mid) / m:+.3f} | stop.5 '
                         f'{sum(r["res"]["stop.5"][0] for r in mid) / m:+.3f}')

    for nm, starts in (('Monday starts', starts_mon), ('daily starts (overlapping)', starts_day)):
        rows = sample(starts)
        report(f'{nm}, small caps', [r for r in rows if r['small']])
        report(f'{nm}, all (ex BTC/ETH)', rows)
        report(f'{nm}, young (age 30-120 d), all', [r for r in rows if r['young']])
    txt = '\n'.join(lines)
    print(txt)
    open(out, 'w').write(txt + '\n')


if __name__ == '__main__':
    main()
