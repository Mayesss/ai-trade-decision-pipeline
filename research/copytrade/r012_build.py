"""Registration 012 — build the panels the runner reads (and whose SHA-256 the registration fixes).
Behaviour data only; nothing here computes a return. 010's builder is imported READ-ONLY for its pure
helpers (`universe`, `write`, `hround`).

    .venv/bin/python -I r012_build.py --stage1      # -> data/carry012/panel_stage1/
    .venv/bin/python -I r012_build.py --stage2      # GATED, after the completed gated fetch -> panel_stage2_<end>/
    .venv/bin/python -I r012_build.py --extension   # GATED -> panel_extension_2027-06-30/

Stage-1 panel (2024-11-01 -> 2026-07-01, exclusive), for every coin of 010's registered universe with an
HL symbol:
  universe.json     010's map for those coins, the OKX leg removed
  funding.parquet   010's registered funding rows (HL, BN, BY) in the window
  volume.parquet    010's registered daily quote volumes in the window
  levels.parquet    010's registered name-check closes in the window
  prices.parquet    lp = ln(close of the bar ENDING at t), lpo = ln(open of the bar STARTING at t), as in
                    010: BN 8h klines and HL 8h candles (010's raw downloads, data/carry/), BY 240-minute
                    klines (012's Stage-1 fetch, data/carry012/stage1/, forward-paged)
  path.parquet      Binance 8h high / low / close (the primary's rebalance and liquidation path)
  hlpath.parquet    HL 8h high / low / close (Revision 2, m6: a REPORTED block liquidates the HL short
                    on HL's own highs)
  Close stamps: a bar is stamped at its close, so the 2026-06-30 16:00 bar is stamped 2026-07-01 00:00
  in path/hlpath; its content is pre-cut, and every Panel read is `t < end`.
Stage-2 / extension panels: the same files from the completed gated pull in data/carry012/<kind>_<date>/,
after `assemble_stage2`'s coverage checks (Revision 2, M2), then one <kind>_built row with the panel's
SHA-256. Files under a Stage-2 directory are readable only once the Stage-2 gate has raised the limit in
this process (Revision 2, M6 e).
"""
import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import r010_build as B10  # noqa: E402   (read-only: universe, write, hround)
import r012_fetch as F  # noqa: E402
import r012_run as run  # noqa: E402

HOUR, GRID, DAY = 3_600_000, 8 * 3_600_000, 86_400_000
MAX_GAP = 3 * DAY
SRC = ROOT / 'data' / 'carry'
COUNT = {'rows_at_or_after_end': 0, 'nonfinite': 0}


def in_stage2_dir(path):
    p = Path(path).resolve()
    return any(part.startswith(('stage2_', 'extension_', 'panel_stage2', 'panel_extension')) for part in p.parts)


def load(path):
    """Whitelisted JSON read; a file under a Stage-2 or extension directory only after the gate."""
    run.assert_allowed(path)
    assert not in_stage2_dir(path) or F.LIMIT > F.CUT, \
        f'012 reads nothing dated on or after 2026-07-01 outside its Stage-2 gate; refused: {path}'
    return json.loads(Path(path).read_text()) if Path(path).exists() else None


def keep(rows, tkey, end):
    out = [r for r in rows if tkey(r) < end]
    COUNT['rows_at_or_after_end'] += len(rows) - len(out)
    return out


def add_prices(prices, path, levels, vol, venue, coin, bars, p_lo, p_end, end, bar_h, with_levels, hlpath=None):
    """Price rows exactly as 010's builder: the close of the bar ending at tc (eligibility) and the open
    of the bar starting at o (the post-settlement fill), both on the 8-hour grid; the Binance path; and
    (reported only) the HL path."""
    for o, op, hi, lo, cl, qv in keep(bars, lambda r: r[0], end):
        tc = o + bar_h * HOUR
        if venue == 'BN' and p_lo <= tc <= p_end:
            path.append((coin, tc, hi, lo, cl))
        if venue == 'HL' and hlpath is not None and p_lo <= tc <= p_end:
            hlpath.append((coin, tc, hi, lo, cl))
        if tc % GRID == 0 and p_lo <= tc < p_end and cl > 0:
            prices.append((venue, coin, tc, math.log(cl), None))
        if o % GRID == 0 and p_lo <= o < p_end and op > 0:
            prices.append((venue, coin, o, None, math.log(op)))
        if with_levels and tc % DAY in (0, 16 * HOUR) and tc < end:
            levels.append((venue, coin, tc, cl))
        if vol is not None:
            k = (venue, coin, o - o % DAY)
            vol[k] = vol.get(k, 0.0) + (qv * cl if venue == 'HL' else qv)


def merge_prices(prices):
    fin = lambda x: x is None or math.isfinite(x)
    pm = {}
    for v, c, t, lp, lpo in prices:
        if not (fin(lp) and fin(lpo)):
            COUNT['nonfinite'] += 1
            continue
        a = pm.setdefault((v, c, t), [None, None])
        a[0] = lp if lp is not None else a[0]
        a[1] = lpo if lpo is not None else a[1]
    return sorted((v, c, t, a[0], a[1]) for (v, c, t), a in pm.items())


def write_panel(panel, uni, fund, prices, path, vol_rows, levels, hlpath):
    panel.mkdir(parents=True, exist_ok=True)
    (panel / 'universe.json').write_text(json.dumps(uni, indent=0, sort_keys=True))
    B10.write(panel, 'funding', ['venue VARCHAR', 'coin VARCHAR', 't BIGINT', 'rate DOUBLE'], fund)
    B10.write(panel, 'prices', ['venue VARCHAR', 'coin VARCHAR', 't BIGINT', 'lp DOUBLE', 'lpo DOUBLE'], prices)
    B10.write(panel, 'path', ['coin VARCHAR', 't BIGINT', 'high DOUBLE', 'low DOUBLE', 'close DOUBLE'], path)
    B10.write(panel, 'volume', ['venue VARCHAR', 'coin VARCHAR', 'day BIGINT', 'qvol DOUBLE'], vol_rows)
    B10.write(panel, 'levels', ['venue VARCHAR', 'coin VARCHAR', 't BIGINT', 'close DOUBLE'], levels)
    B10.write(panel, 'hlpath', ['coin VARCHAR', 't BIGINT', 'high DOUBLE', 'low DOUBLE', 'close DOUBLE'], hlpath)


# ------------------------------------------------------------------ coverage (Revision 2, M2)
def _month_starts(t0, t1):
    out = []
    for ym in F.ym_range(t0, t1 + 40 * DAY):
        out.append(int(F.datetime.strptime(ym + '-01', '%Y-%m-%d').replace(tzinfo=F.timezone.utc).timestamp() * 1000))
    return out


def at_month_boundary(t, starts):
    """A last stamp in the final day of a month, or within 8 h after a month starts (a bar-close stamp):
    the signature of a missing next-month file."""
    return any(m - DAY <= t <= m + GRID for m in starts)


def coverage(fund, prices, path, uni, months, end, lo, kline_months=None):
    """Coverage of a panel: (refusals, counts). Pure.

    REFUSALS (Revision 2, M2) — each is fixable by waiting or re-fetching, so the Stage-2 fetch stops
    before its `_fetched` row and the build refuses:
      binance_end_month_missing  a Binance symbol whose funding reaches the last 8 h of the month before
                                 the end month has no end-month file listed (not yet published: wait);
      series_without_prices      a venue-coin whose funding reaches end - 8 h has no price rows at all
                                 (a lost kline or candle fetch). A Binance symbol whose kline files are
                                 not LISTED is counted instead (bn_no_klines_listed: the venue has none;
                                 it can never be entered);
      prices_without_funding     a venue-coin whose prices reach end - 8 h has no funding rows at all;
      month_boundary_truncation  one of a venue-coin's funding / price series stops at a month boundary
                                 while the other runs on more than 3 days (a missing monthly file).
    COUNTED and printed before the decision, never refused (venue-side events a re-fetch cannot change;
    010's funding-hole rule and the N4 exit fallbacks handle them in the P&L):
      funding_gap / price_gap / path_gap  internal gaps > 3 days (suspensions, relistings, archive holes);
      tail_mismatch_midmonth              one series stops mid-month > 3 days before the other;
      ended_before_end                    both series stop before end - 8 h (a delisting)."""
    problems, cov = [], {'venue_coin_series': 0, 'ended_before_end': 0, 'funding_gap': 0, 'price_gap': 0,
                         'path_gap': 0, 'tail_mismatch_midmonth': 0, 'bn_no_klines_listed': 0}
    fs, ps, pa = {}, {}, {}
    for v, c, t, r in fund:
        fs.setdefault((v, c), []).append(t)
    for v, c, t, lp, lpo in prices:
        if lp is not None:
            ps.setdefault((v, c), []).append(t)
    for c, t, *_ in path:
        pa.setdefault(c, []).append(t)
    reach = end - GRID
    starts = _month_starts(lo, end)

    def gaps(ts, a, b):
        ts = sorted(t for t in set(ts) if a <= t <= b)
        if not ts:
            return b - a
        return max([ts[0] - a] + [y - x for x, y in zip(ts, ts[1:])] + [b - ts[-1]])
    for (v, c), ts in sorted(fs.items()):
        ts = sorted(set(ts))
        cov['venue_coin_series'] += 1
        if max((y - x for x, y in zip(ts, ts[1:])), default=0) > MAX_GAP:
            cov['funding_gap'] += 1
        f_first, f_last = ts[0], ts[-1]
        p = sorted(set(ps.get((v, c), [])))
        if not p:
            if v == 'BN' and kline_months is not None and not kline_months.get(uni.get(c, {}).get('BN'), []):
                cov['bn_no_klines_listed'] += 1
            elif f_last >= reach:
                problems.append(['series_without_prices', v, c])
            continue
        p_last = p[-1]
        if f_last >= reach and p_last >= reach:
            pass
        elif f_last < reach and p_last < reach and abs(f_last - p_last) <= MAX_GAP:
            cov['ended_before_end'] += 1
        elif abs(f_last - p_last) > MAX_GAP or (f_last >= reach) != (p_last >= reach):
            early = min(f_last, p_last)
            if abs(f_last - p_last) > MAX_GAP and at_month_boundary(early, starts):
                problems.append(['month_boundary_truncation', v, c, f_last, p_last])
            elif abs(f_last - p_last) > MAX_GAP:
                cov['tail_mismatch_midmonth'] += 1
            else:
                cov['ended_before_end'] += 1
        a, b = max(f_first, lo + 2 * DAY), min(f_last, reach)
        if b > a and gaps(p, a, b) > MAX_GAP:
            cov['price_gap'] += 1
        if v == 'BN' and b > a and gaps(pa.get(c, []), a, b) > MAX_GAP:
            cov['path_gap'] += 1
    for (v, c), p in sorted(ps.items()):
        if (v, c) not in fs and max(p) >= reach:
            problems.append(['prices_without_funding', v, c])
    if months is not None:
        end_month = F.ym_range(end - DAY, end)[-1]
        ems = F.datetime.strptime(end_month + '-01', '%Y-%m-%d').replace(tzinfo=F.timezone.utc).timestamp() * 1000
        for c, u in uni.items():
            last = max(fs.get(('BN', c), [0]))
            if last >= ems - GRID and end_month not in months.get(u['BN'], []):
                problems.append(['binance_end_month_missing', u['BN'], end_month])
    return problems, cov


# ------------------------------------------------------------------ Stage 1
def stage1():
    import duckdb
    lo, end = F.FETCH_START, F.CUT
    panel = run.STAGE1_PANEL
    run.assert_allowed(panel)
    uni10 = json.loads((SRC / 'panel' / 'universe.json').read_text())
    uni = {c: {v: s for v, s in u.items() if v in ('HL', 'BN', 'BY')} for c, u in uni10.items() if 'HL' in u}
    months = json.loads((SRC / 'binance' / 'months.json').read_text())
    coins = sorted(uni)
    con = duckdb.connect()
    con.execute('CREATE TEMP TABLE cs (coin VARCHAR)')
    con.executemany('INSERT INTO cs VALUES (?)', [(c,) for c in coins])
    q = lambda sql: con.execute(sql).fetchall()
    src = SRC / 'panel'
    fund = q(f"SELECT venue, coin, t, rate FROM '{src}/funding.parquet' WHERE venue IN ('HL','BN','BY') "
             f"AND coin IN (SELECT coin FROM cs) AND t >= {lo} AND t < {end} ORDER BY ALL")
    vol = q(f"SELECT venue, coin, day, qvol FROM '{src}/volume.parquet' WHERE venue IN ('HL','BN','BY') "
            f"AND coin IN (SELECT coin FROM cs) AND day >= {lo} AND day < {end} ORDER BY ALL")
    levels = q(f"SELECT venue, coin, t, close FROM '{src}/levels.parquet' WHERE venue IN ('HL','BN','BY') "
               f"AND coin IN (SELECT coin FROM cs) AND t >= {lo} AND t < {end} ORDER BY ALL")
    prices, path, hlpath = [], [], []
    first_m = F.ym_range(lo - 31 * DAY, lo)[0]
    for coin in coins:
        u = uni[coin]
        for m in months.get(u['BN'], []):
            if m >= first_m:
                k = load(SRC / 'binance' / 'klines' / f"{u['BN']}_8h_{m}.json")
                if k:
                    add_prices(prices, path, None, None, 'BN', coin, k['rows'], lo, end, end, 8, False)
        bars = load(SRC / 'hl' / 'candles' / f"{u['HL']}_8h_{F.F.START}_{F.CUT}.json") or []
        add_prices(prices, path, None, None, 'HL', coin, bars, lo, end, end, 8, False, hlpath)
        if 'BY' in u:
            bars = load(F.OUT / 'stage1' / 'bybit' / 'kline' / f"{u['BY']}_240_{lo}_{end}.json") or []
            add_prices(prices, path, None, None, 'BY', coin, bars, lo, end, end, 4, False)
    pm = merge_prices(prices)
    write_panel(panel, uni, [tuple(r) for r in fund], pm, sorted(set(path)), [tuple(r) for r in vol],
                [tuple(r) for r in levels], sorted(set(hlpath)))
    problems, cov = coverage([tuple(r) for r in fund], pm, sorted(set(path)), uni, None, end, lo)
    kinds = {}
    for p in problems:
        kinds[p[0]] = kinds.get(p[0], 0) + 1
    print('stage1 panel: coins', len(uni), {v: sum(v in u for u in uni.values()) for v in ('HL', 'BN', 'BY')},
          '; rows funding', len(fund), 'prices', len(pm), 'path', len(set(path)), 'hlpath', len(set(hlpath)),
          'volume', len(vol), 'levels', len(levels))
    print('coverage (counts only; Stage 1 is not refused on them):', cov, 'would-refuse kinds', kinds)
    print('dropped at or after 2026-07-01 (count only):', COUNT['rows_at_or_after_end'], '; non-finite:', COUNT['nonfinite'])


# ------------------------------------------------------------------ Stage 2 / extension
def assemble_stage2(base, end):
    """Pure assembly of a Stage-2 or extension panel from the raw pull in `base` (no file written):
    the point-in-time universe (coins with an HL coin), funding, prices, paths, volumes, levels, and the
    coverage report. Called by the fetch (before its `_fetched` row) and by the build."""
    lo = F.STAGE2_FETCH_START
    months = load(base / 'binance' / 'months.json')
    kmonths = load(base / 'binance' / 'kline_months.json')
    hlmap = load(base / 'hl' / 'coins.json')
    by = load(base / 'bybit' / 'symbols.json')
    if months is None or kmonths is None or hlmap is None or by is None:
        return {'problems': [['raw_pull_incomplete', str(base)]], 'coverage': {}}
    tie = F.ym_range(lo, end)[-1]
    uni = B10.universe({s: months[s] for s in hlmap}, by, {}, hlmap, tie_months_upto=tie)
    uni = {c: u for c, u in uni.items() if 'HL' in u}
    fund, prices, path, hlpath, levels, vol = [], [], [], [], [], {}
    for coin, u in sorted(uni.items()):
        sym = u['BN']
        for m in months.get(sym, []):
            f = load(base / 'binance' / 'fundingRate' / f'{sym}_{m}.json')
            if f:
                fund += [('BN', coin, B10.hround(t), r) for t, _, r in keep(f['rows'], lambda r: r[0], end) if t >= lo]
        for m in kmonths.get(sym, []):
            k = load(base / 'binance' / 'klines' / f'{sym}_8h_{m}.json')
            if k:
                add_prices(prices, path, levels, vol, 'BN', coin, k['rows'], lo, end, end, 8, True)
        h = u['HL']
        for t, r, pr in keep(load(base / 'hl' / f'{h}_funding_{lo}_{end}.json') or [], lambda r: r[0], end):
            if r == 0.0 and pr == 0.0:
                continue                                   # delisted HL coins keep printing exact zeros (010)
            if math.isfinite(r) and t >= lo:
                fund.append(('HL', coin, B10.hround(t), r))
        add_prices(prices, path, levels, vol, 'HL', coin, load(base / 'hl' / 'candles' / f'{h}_8h_{lo}_{end}.json') or [],
                   lo, end, end, 8, True, hlpath)
        if 'BY' in u:
            s = u['BY']
            fund += [('BY', coin, B10.hround(t), r) for t, r in
                     keep(load(base / 'bybit' / 'funding' / f'{s}_{lo}_{end}.json') or [], lambda r: r[0], end) if t >= lo]
            add_prices(prices, path, None, None, 'BY', coin,
                       load(base / 'bybit' / 'kline' / f'{s}_240_{lo}_{end}.json') or [], lo, end, end, 4, False)
            for r in keep(load(base / 'bybit' / 'kline' / f'{s}_D_{lo}_{end}.json') or [], lambda r: r[0], end):
                k_ = ('BY', coin, r[0])
                vol[k_] = vol.get(k_, 0.0) + r[5]
                if r[0] + DAY < end:
                    levels.append(('BY', coin, r[0] + DAY, r[4]))
    COUNT['nonfinite'] += sum(1 for r in fund if not math.isfinite(r[3]))
    fund = sorted(set(r for r in fund if math.isfinite(r[3])))      # a NaN rate is a hole, never a zero
    pm = merge_prices(prices)
    path, hlpath = sorted(set(path)), sorted(set(hlpath))
    problems, cov = coverage(fund, pm, path, uni, months, end, lo, kmonths)
    cov['coins'] = len(uni)
    return {'uni': uni, 'fund': fund, 'prices': pm, 'path': path, 'hlpath': hlpath, 'levels': sorted(set(levels)),
            'vol': sorted((v, c, d, q) for (v, c, d), q in vol.items()), 'problems': problems, 'coverage': cov}


def stage2(kind='stage2'):
    end, tag, commit, owner = F.stage2_gate(kind, 'build')
    base = F.OUT / tag
    panel = run.EXTENSION_DIR if kind == 'extension' else run.stage2_dir(tag.split('_', 1)[1])
    run.assert_allowed(base)
    run.assert_allowed(panel)
    a = assemble_stage2(base, end)
    print(kind, 'coverage before the build:', a['coverage'], '; problems', len(a['problems']))
    if a['problems']:
        sys.exit(f'{kind} build refused: coverage problems {a["problems"][:5]} — no panel, no {kind}_built row')
    write_panel(panel, a['uni'], a['fund'], a['prices'], a['path'], a['vol'], a['levels'], a['hlpath'])
    run.append_row({'registration': run.REG.name, 'mode': f'{kind}_built', 'registration_commit': commit, 'owner': owner,
                    'coverage': a['coverage'], 'panel_sha256': run.panel_hashes(panel),
                    'run_at': F.datetime.now(F.timezone.utc).isoformat()})
    print(kind, 'panel built: coins', len(a['uni']), '; dropped at or after the end (count only):',
          COUNT['rows_at_or_after_end'], '; non-finite:', COUNT['nonfinite'])


if __name__ == '__main__':
    if '--stage1' in sys.argv:
        stage1()
    elif '--stage2' in sys.argv:
        stage2('stage2')
    elif '--extension' in sys.argv:
        stage2('extension')
    else:
        sys.exit(__doc__)
