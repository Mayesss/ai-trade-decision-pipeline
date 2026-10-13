"""Registration 013 — panel build (Revision 1). COVERAGE ONLY: rows are copied, never combined into a price
change, return, funding sum or P&L. Per-event coverage is limited to PRE-ENTRY facts, [d0, first trade + 25 h)
(ledger 000n; review B M5); nothing about the hold window is counted, stored or printed.

    .venv/bin/python -I r013_build.py            # every calendar: verify raw files, build panels, MANIFEST, counts

Inputs (all under data/r013/, each raw source verified against its SHA256SUMS before use):
  listing/um_daily_1d_days.json, listing/event_keys.json, inputs/fapi_exchangeInfo.json,
  raw_klines1m/, raw_funding/, raw_klines1h/
Outputs, one directory per calendar (`panel_reported`, `panel_discovery`, `panel_confirmation`), read only through
the runner's gated `Panel`:
  events.json        per first-segment listing event: id, symbol, d0, calendar, first_trade (first 1m bar with
                     trades), premarket, spot_first, continuation (ct/listings013.CONTINUATIONS),
                     funding_months_missing (archive month files absent; a listing fact), pre-entry coverage
  bars_1m.parquet    id, t, o, h, l, c, qv, n   (t in [d0, d0+10 d) and before the calendar's DATA_END)
  funding.parquet    id, calc_time, interval_h (-1 if blank), rate   (same window)
  market_1h.parquet  symbol, t, o, c, n   (discovery / confirmation: every universe symbol inside the span)
  symbols.json       universe symbol -> first daily file day (seasoning for the market index)
  coverage.json      calendar totals of the pre-entry counts
and data/r013/schedule.json (ungated; pre-entry facts only: id, first trade, flags: the simulation's input),
data/r013/later_segments.json (names and start dates of later trading segments, 2024-01 .. 2026-06-21),
data/r013/MANIFEST.json. Nothing dated on or after 2026-07-01 exists in any input; every row is checked
< the calendar's DATA_END (a refusal, never an assert).
"""
import array
import csv
import hashlib
import io
import json
import os
import sys
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from ct import listings013 as L  # noqa: E402

DATA = HERE / 'data' / 'r013'
RAW = {'klines1m': DATA / 'raw_klines1m', 'funding': DATA / 'raw_funding', 'klines1h': DATA / 'raw_klines1h'}
PANELS = {c: DATA / f'panel_{c}' for c in L.CALENDARS}
MANIFEST = DATA / 'MANIFEST.json'
INPUT_FILES = ('listing/um_daily_1d_days.json', 'listing/event_keys.json', 'inputs/fapi_exchangeInfo.json',
               'raw_klines1m/SHA256SUMS', 'raw_funding/SHA256SUMS', 'raw_klines1h/SHA256SUMS')


def sha256(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def verify_source(raw):
    """{key: path} of every file in SHA256SUMS, each re-hashed; refuses on any mismatch or missing file."""
    out = {}
    for line in (raw / 'SHA256SUMS').read_text().splitlines():
        h, k = line.split('  ', 1)
        p = raw / k
        if not p.exists() or sha256(p) != h:
            sys.exit(f'refused: {p} is missing or does not match SHA256SUMS')
        out[k] = p
    return out


def ts_ms(s):
    v = int(float(s))
    return v // 1000 if v > 10 ** 14 else v            # 2025+ archive files may stamp microseconds


def csv_rows(path):
    with zipfile.ZipFile(path) as z:
        (name,) = z.namelist()
        text = z.read(name).decode()
    rows = [r for r in csv.reader(io.StringIO(text)) if r]
    if rows and not rows[0][0][:1].isdigit():
        rows = rows[1:]
    return rows


class Columns:
    """Compact column store: string columns as Python lists of shared objects, numbers in typed arrays."""

    def __init__(self, spec):
        self.spec = spec
        self.cols = {c: (array.array(t) if t in 'dq' else []) for c, t in spec}

    def add(self, *vals):
        for (c, _t), v in zip(self.spec, vals):
            self.cols[c].append(v)

    def __len__(self):
        return len(self.cols[self.spec[0][0]])

    def write(self, path, order):
        import pyarrow as pa
        import pyarrow.parquet as pq
        a, b = (self.cols[c] for c in order)
        L.refuse_unless(all((a[i], b[i]) < (a[i + 1], b[i + 1]) for i in range(len(self) - 1)),
                        f'{path.name}: rows must arrive strictly ordered by {order} (no duplicates)')
        data = {c: pa.array(self.cols[c], type={'d': pa.float64(), 'q': pa.int64()}.get(t, pa.string()))
                for c, t in self.spec}
        tmp = path.with_suffix('.part')
        pq.write_table(pa.table(data), tmp, compression='zstd')
        os.replace(tmp, path)

    def max_of(self, c):
        return max(self.cols[c]) if len(self) else None


BAR_SPEC = (('id', 's'), ('t', 'q'), ('o', 'd'), ('h', 'd'), ('l', 'd'), ('c', 'd'), ('qv', 'd'), ('n', 'q'))
FUND_SPEC = (('id', 's'), ('calc_time', 'q'), ('interval_h', 'q'), ('rate', 'd'))
MKT_SPEC = (('symbol', 's'), ('t', 'q'), ('o', 'd'), ('c', 'd'), ('n', 'q'))


def assemble(events_keys, days, exclude, m1_files, fr_files, h1_files):
    """Pure assembly (no I/O beyond reading the given zips): per calendar, the event rows, bars, funding and
    market opens, plus coverage counts. Never computes a price change, return or funding sum."""
    out = {c: {'events': [], 'bars': Columns(BAR_SPEC), 'funding': Columns(FUND_SPEC), 'market': Columns(MKT_SPEC),
               'counts': {}} for c in L.CALENDARS}
    for p in sorted(events_keys, key=lambda p: p['id']):
        cal = p['calendar']
        if cal not in L.CALENDARS:
            continue
        end = L.DATA_END[cal]
        d0ms = L.ms(p['d0'])
        lo, hi = d0ms, min(d0ms + L.PULL_DAYS * L.DAY, end)
        bars = []
        for k, _d, _z in p['m1']:
            for r in csv_rows(m1_files[k]):
                t = ts_ms(r[0])
                if lo <= t < hi:
                    bars.append((p['id'], t, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[7]), int(float(r[8]))))
        bars.sort(key=lambda b: b[1])
        dedup = []
        for b in bars:
            if not dedup or b[1] != dedup[-1][1]:
                dedup.append(b)
        traded = [b for b in dedup if b[7] > 0]
        first_trade = traded[0][1] if traded else None
        fund = []
        for k, _d, _z in p['funding']:
            for r in csv_rows(fr_files[k]):
                t = ts_ms(r[0])
                if lo <= t < hi:
                    fund.append((p['id'], t, int(float(r[1])) if r[1] not in ('', None) else -1, float(r[2])))
        fund.sort(key=lambda f: f[1])
        fund = [f for i, f in enumerate(fund) if i == 0 or f[1] != fund[i - 1][1]]
        pre_end = (first_trade + L.PRE_ENTRY) if first_trade is not None else lo
        pre = [b_ for b_ in dedup if b_[1] < pre_end]
        need_days = L.pull_days(p['d0'])
        have_days = {d for _k, d, _z in p['m1']}
        need_months = L.months_between(L.ms(need_days[0]), L.ms(need_days[-1]))
        have_months = {d[:7] for _k, d, _z in p['funding']}
        # PRE-ENTRY coverage only (000n, review B M5): [d0, first trade + 25 h). Nothing about the hold window
        # (rows, zero-trade rows, funding rows, missing minutes, where trading stops) is counted or stored.
        ev = {'id': p['id'], 'symbol': p['symbol'], 'd0': p['d0'],
              'calendar': L.calendar_of(first_trade) if first_trade else 'no_trade',
              'first_trade': first_trade, 'premarket': L.is_premarket(p['d0'], p['index_on_d0']),
              'spot_first': bool(p['spot_on_prev_day']), 'continuation': L.CONTINUATIONS.get(p['id']),
              'funding_months_missing': sorted(set(need_months) - have_months),
              'cov_pre_entry': {'daily_files_d0_d1': sum(1 for d in need_days[:2] if d in have_days),
                                'rows_1m': len(pre), 'rows_zero_trades': sum(1 for b_ in pre if b_[7] == 0),
                                'first_trade_on_d0': first_trade is not None and L.day_of(first_trade) == p['d0'],
                                'funding_rows': sum(1 for f in fund if f[1] < pre_end)}}
        L.refuse_unless(ev['calendar'] in (cal, 'no_trade'), f"{p['id']}: first trade calendar {ev['calendar']} vs d0 {cal}")
        L.refuse_unless(all(b_[1] < end for b_ in dedup) and all(f[1] < end for f in fund), f"{p['id']}: a row at or after {end}")
        out[cal]['events'].append(ev)
        for b_ in dedup:
            out[cal]['bars'].add(*b_)
        for f in fund:
            out[cal]['funding'].add(*f)
    univ = sorted(s for s in days if L.UNIV.match(s) and s not in exclude)
    spans = {c: (L.CALENDARS[c][0], L.DATA_END[c]) for c in ('discovery', 'confirmation')}
    traded_hours = {}
    for k in sorted(h1_files):                     # (symbol, month) order -> rows arrive ordered by (symbol, t)
        path = h1_files[k]
        sym = k.split('/')[5]
        for r in sorted(csv_rows(path), key=lambda r: ts_ms(r[0])):
            t, n = ts_ms(r[0]), int(float(r[8]))
            if n > 0:
                traded_hours.setdefault(sym, []).append(t)
            for c, (a, b) in spans.items():
                if a <= t < b:
                    out[c]['market'].add(sym, t, float(r[1]), float(r[4]), n)
    # later TRADING segments (relistings, ticker reuse): runs of traded hours after >= 7 days without trades,
    # examined only from an event symbol's d0 + 30 days on (nothing about its own window); names and dates only
    first_day = {s: days[s][0] for s in days}
    later = []
    for sym, hs in sorted(traded_hours.items()):
        nb = L.ms(first_day.get(sym, '2020-01-01')) + 30 * L.DAY
        for st in L.later_trading_segments(hs, nb):
            if L.ms('2024-01-01') <= st < L.CALENDARS['confirmation'][1]:
                later.append({'symbol': sym, 'segment_start': L.day_of(st)})
    for c in out:
        mx = [out[c]['bars'].max_of('t'), out[c]['funding'].max_of('calc_time'), out[c]['market'].max_of('t')]
        L.refuse_unless(all(x is None or x < L.DATA_END[c] for x in mx), f'{c}: a row at or after its data end')
        out[c]['symbols'] = {s: days[s][0] for s in univ}
        E = out[c]['events']
        cov = [e['cov_pre_entry'] for e in E]
        prim = [e for e in E if e['first_trade'] and not e['premarket'] and not e['continuation']]
        out[c]['counts'] = {
            'events': len(E), 'premarket': sum(e['premarket'] for e in E), 'spot_first': sum(e['spot_first'] for e in E),
            'continuations': sum(1 for e in E if e['continuation']),
            'primary_events': len(prim), 'primary_listing_weeks': len({L.iso_week(e['first_trade']) for e in prim}),
            'no_trade_bar': sum(1 for e in E if e['first_trade'] is None),
            'first_trade_not_on_d0': sum(1 for x in cov if not x['first_trade_on_d0']),
            'events_missing_d0_or_d1_file': sum(1 for x in cov if x['daily_files_d0_d1'] < 2),
            'pre_entry_rows_1m': sum(x['rows_1m'] for x in cov),
            'pre_entry_zero_trade_rows': sum(x['rows_zero_trades'] for x in cov),
            'events_without_pre_entry_funding_row': sum(1 for x in cov if x['funding_rows'] == 0),
            'events_with_a_missing_funding_month': sum(1 for e in E if e['funding_months_missing']),
            'market_rows': len(out[c]['market']), 'market_symbols': len(set(out[c]['market'].cols['symbol'])),
        }
    out['_later_segments'] = later
    return out


def main():
    m1 = verify_source(RAW['klines1m'])
    fr = verify_source(RAW['funding'])
    h1 = verify_source(RAW['klines1h'])
    days = json.loads((DATA / 'listing' / 'um_daily_1d_days.json').read_text())
    keys = json.loads((DATA / 'listing' / 'event_keys.json').read_text())
    exclude = L.excluded_symbols(json.loads((DATA / 'inputs' / 'fapi_exchangeInfo.json').read_text()))
    for k in (*m1, *fr, *h1):
        L.refuse_unless(k.endswith('.zip'), f'not a zip: {k}')
    out = assemble(keys, days, exclude, m1, fr, h1)
    later = out.pop('_later_segments')
    manifest = {'inputs': {f: sha256(DATA / f) for f in INPUT_FILES}, 'panels': write_panels(out, DATA)}
    (DATA / 'schedule.json').write_text(json.dumps(schedule(out), sort_keys=True, indent=0))
    (DATA / 'later_segments.json').write_text(json.dumps(later, sort_keys=True, indent=0))
    manifest['schedule'] = sha256(DATA / 'schedule.json')
    manifest['later_segments'] = sha256(DATA / 'later_segments.json')
    for c, d in out.items():
        print(c, json.dumps(d['counts']))
    print('later trading segments starting 2024-01-01 .. 2026-06-21 (names):', [f"{x['symbol']}@{x['segment_start']}" for x in later])
    MANIFEST.write_text(json.dumps(manifest, sort_keys=True, indent=1))
    print('MANIFEST', sha256(MANIFEST))


def schedule(out):
    """PRE-ENTRY facts only, ungated (the simulation's input): per calendar, every event's id, first trade,
    pre-market and continuation flags."""
    return {c: [{k: e[k] for k in ('id', 'first_trade', 'premarket', 'continuation')} for e in d['events']]
            for c, d in out.items()}


def write_panels(out, root):
    """One directory per calendar under `root` (the real build: data/r013). Returns {calendar: {file: sha256}}."""
    hashes_ = {}
    for c, d in out.items():
        pdir = Path(root) / f'panel_{c}'
        pdir.mkdir(parents=True, exist_ok=True)
        (pdir / 'events.json').write_text(json.dumps(d['events'], sort_keys=True, indent=0))
        (pdir / 'symbols.json').write_text(json.dumps(d['symbols'], sort_keys=True))
        d['bars'].write(pdir / 'bars_1m.parquet', ('id', 't'))
        d['funding'].write(pdir / 'funding.parquet', ('id', 'calc_time'))
        if c != 'reported':
            d['market'].write(pdir / 'market_1h.parquet', ('symbol', 't'))
        (pdir / 'coverage.json').write_text(json.dumps(d['counts'], sort_keys=True, indent=1))
        hashes_[c] = {p.name: sha256(p) for p in sorted(pdir.iterdir()) if p.is_file()}
    return hashes_


if __name__ == '__main__':
    main()
