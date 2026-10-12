"""N1 scout, items 5 and 7: PAST-ONLY deviation and peg quality, indices only.

Reference: FRED official daily closes (SP500 -> xyz:SP500, NASDAQ100 -> xyz:XYZ100).
  (a) Tracking at the close: perp last trade in the 60 s BEFORE 16:00 ET
      (13:00 on half days) vs that day's official close. Contemporaneous.
  (b) Off-hours deviation: perp last trade in the 5 min before a checkpoint
      INSIDE the internal (weekend / holiday) session vs the most recent
      official close BEFORE the checkpoint. Checkpoints: Sat 12:00, Sun 12:00,
      Sun 20:00 UTC, and 12:00 UTC on weekday NYSE holidays.
      Nothing after the checkpoint is read: no open, no later close, no later
      perp price. Unconditional quantiles only.
  (c) Power inputs from the UNDERLYING only: FRED close-to-close dispersion
      (all days; Friday close -> next trading-day close, an upper bound for
      the unavailable Friday-close -> Monday-open move), 2023-01 -> 2026-06.

    .venv/bin/python -I scouts/n1_deviation.py data/scout_n1/fred_<stamp>
"""
import csv
import datetime as dt
import math
import statistics
import sys
from zoneinfo import ZoneInfo

import duckdb

sys.path.insert(0, __import__('os').path.dirname(__file__))
from n1_common import OUT, CUTOFF, NYSE_HALF, NYSE_HOL, setup, fills_view  # noqa: E402

NY = ZoneInfo('America/New_York')
UTC = dt.timezone.utc
MAP = {'xyz:SP500': 'SP500', 'xyz:XYZ100': 'NASDAQ100'}


def ms(t):
    return int(t.timestamp() * 1000)


def load_fred(path):
    out = {}
    with open(path) as fh:
        for r in csv.DictReader(fh):
            d, v = r['observation_date'], list(r.values())[1]
            if d < CUTOFF and v not in ('', '.'):
                out[dt.date.fromisoformat(d)] = float(v)
    return out


def q(xs, ps=(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99)):
    xs = sorted(xs)
    if not xs:
        return {}
    res = {}
    for p in ps:
        k = (len(xs) - 1) * p
        lo, hi = math.floor(k), math.ceil(k)
        res[p] = xs[lo] + (xs[hi] - xs[lo]) * (k - lo)
    return res


def fmt(d):
    return ' '.join(f'p{int(p * 100)}={v:+.0f}' for p, v in d.items())


def main(fred_dir):
    con = duckdb.connect()
    setup(con)
    fills_view(con, where="coin IN ('xyz:SP500', 'xyz:XYZ100') AND crossed")
    con.execute('CREATE OR REPLACE TEMP TABLE px AS SELECT coin, epoch_ms(timestamp) AS ts, CAST(price AS DOUBLE) AS p FROM f ORDER BY coin, ts')
    report = []
    for coin, series in MAP.items():
        closes = load_fred(f'{fred_dir}/{series}.csv')
        first = dt.datetime.fromtimestamp(con.execute('SELECT min(ts) FROM px WHERE coin = ?', [coin]).fetchone()[0] / 1000, UTC)
        # (a) tracking at close
        trk = []
        for d, c in sorted(closes.items()):
            hh = 13 if d.isoformat() in NYSE_HALF else 16
            t_close = dt.datetime(d.year, d.month, d.day, hh, 0, tzinfo=NY).astimezone(UTC)
            if t_close <= first:
                continue
            r = con.execute("""SELECT p FROM px WHERE coin = ? AND ts < ? AND ts >= ? - 60000
                               ORDER BY ts DESC LIMIT 1""", [coin, ms(t_close), ms(t_close)]).fetchone()
            if r:
                trk.append((r[0] / c - 1) * 1e4)
        # (b) weekend / holiday checkpoints
        cps = []
        d = first.date()
        while d < dt.date.fromisoformat(CUTOFF):
            if d.weekday() == 5:
                cps += [('sat12', dt.datetime(d.year, d.month, d.day, 12, tzinfo=UTC))]
            if d.weekday() == 6:
                cps += [('sun12', dt.datetime(d.year, d.month, d.day, 12, tzinfo=UTC)),
                        ('sun20', dt.datetime(d.year, d.month, d.day, 20, tzinfo=UTC))]
            if d.isoformat() in NYSE_HOL and d.weekday() < 5:
                cps += [('hol12', dt.datetime(d.year, d.month, d.day, 12, tzinfo=UTC))]
            d += dt.timedelta(days=1)
        dev = {}
        n_missing = 0
        for tag, t in cps:
            if t <= first or t >= dt.datetime.fromisoformat(CUTOFF + 'T00:00:00+00:00'):
                continue
            prev = [x for x in closes if x < t.date()]
            if not prev:
                continue
            c = closes[max(prev)]
            r = con.execute("""SELECT p FROM px WHERE coin = ? AND ts < ? AND ts >= ? - 300000
                               ORDER BY ts DESC LIMIT 1""", [coin, ms(t), ms(t)]).fetchone()
            if not r:
                n_missing += 1
                continue
            dev.setdefault(tag, []).append((r[0] / c - 1) * 1e4)
        report.append(f'== {coin} vs FRED {series}; first fill {first:%Y-%m-%d}')
        report.append(f'(a) tracking at close, n={len(trk)} days, bp: {fmt(q(trk))}; '
                      f'median |err| {statistics.median(abs(x) for x in trk):.0f} bp')
        for tag in ('sat12', 'sun12', 'sun20', 'hol12'):
            xs = dev.get(tag, [])
            if xs:
                report.append(f'(b) {tag} n={len(xs)} dev vs prev close, bp: {fmt(q(xs))}; '
                              f'|dev| p50 {statistics.median(abs(x) for x in xs):.0f}, p90 {q([abs(x) for x in xs])[0.9]:.0f}')
        report.append(f'    checkpoints with no trade in the prior 5 min: {n_missing}')
        # (c) underlying dispersion (FRED only)
        days = sorted(closes)
        rets = [(days[i], math.log(closes[days[i]] / closes[days[i - 1]])) for i in range(1, len(days))]
        allr = [r * 1e4 for _, r in rets]
        wk = []
        for i in range(1, len(days)):
            if (days[i] - days[i - 1]).days >= 3:  # spans a weekend or holiday
                wk.append(math.log(closes[days[i]] / closes[days[i - 1]]) * 1e4)
        disc = [math.log(closes[days[i]] / closes[days[i - 1]]) * 1e4 for i in range(1, len(days))
                if days[i] >= dt.date(2025, 10, 13) and (days[i] - days[i - 1]).days >= 3]
        report.append(f'(c) {series} 2023-01..2026-06: sd daily close-close {statistics.stdev(allr):.0f} bp (n={len(allr)}); '
                      f'sd across-weekend/holiday close-close {statistics.stdev(wk):.0f} bp (n={len(wk)}); '
                      f'same, 2025-10-13..2026-06-30: {statistics.stdev(disc):.0f} bp (n={len(disc)})')
    text = '\n'.join(report)
    print(text)
    (OUT / 'deviation_indices.txt').write_text(text + '\n')


if __name__ == '__main__':
    main(sys.argv[1])
