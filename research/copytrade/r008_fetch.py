"""Registration 008 — the guarded holdout fetch (Revision 1). Runs only through r008_run's gate (mode 'fetch'):
REGISTERED, committed, unmodified, code and static data matching §11, one offered owner end, now >= end + 7 d,
results.jsonl clean, no fetched / started / holdout row anywhere (history and reflog included).

    .venv/bin/python -I r008_fetch.py        # resumable until its holdout_fetched row

Network (B-B1). With no `hooks` argument the REAL hooks are used, and only when root is this repository,
the data tree is research/copytrade/data and no guard argument is overridden. A `hooks` argument must
supply all eight hooks (no fallback to a real one). Every real hook refuses unless that check passed.

What it pulls (and nothing else):
1. Hyperliquid fills, requester-pays (s3://hydromancer-reservoir, profile `copytrade`), 001's P3 columns,
   for the holdout days inside a window's lookback (end 2026-09-28: 2026-07-01 .. 2026-08-02, 33 days).
   LIST first; the exact plan from Parquet footers; the 15 GiB cap is CUMULATIVE over every attempt and
   counts footers (measured with `archive.bytes_pulled`, written in a `finally` as a COMMITTED
   holdout_fetch_attempt row; an attempt killed before its end row counts its reservation). P3's G1-G2 run on every pulled day, G3 on a sample; a sentinel README marks the
   holdout days in the shared partition root.
2. Hyperliquid daily snapshots, requester-pays, only for signal dates not already on disk (none for
   2026-09-28), whole objects, inside the same cap.
3. Bitget 1H candles (public), every mapped symbol, 2026-05-28 -> the read bound (end + 2 h), 200-hour
   blocks paged FORWARD, rows at or after the bound dropped before writing. Amendment 1: each block is
   requested as [b, b + 200 h) because Bitget's endTime is exclusive (verified on discovery dates).
4. Hyperliquid funding (public), 2026-06-30 -> the read bound, paged forward; HL `meta` and Bitget's
   contract list (liveness).

Never cached: an error, a null or an empty answer. A symbol Bitget no longer lists, or a coin HL `meta` no
longer lists, is tried once per attempt and on failure recorded as unavailable — never a stop (B-B2).
Coverage refusals for live names are keyed; the same key on 3 attempts each >= 24 h after the previous one
becomes 'unavailable (persistent)', recorded and counted, and stops blocking (pre-specified end state).
BTCUSDT, the fill days and the snapshot dates are never eligible for that end state.
"""
import datetime as dt
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import r008_run as run008  # noqa: E402
from ct import crowd008 as c8  # noqa: E402

HOUR, DAY = c8.HOUR, c8.DAY
HOUR_BLOCK = 200 * HOUR
FILLS_PREFIX = 'by_dex/hyperliquid/fills/perp/all/'
SNAP_PREFIX = 'by_dex/hyperliquid/snapshots/perp/'
COLUMNS = ['address', 'coin', 'timestamp', 'side', 'size', 'price', 'start_position', 'realized_pnl', 'trade_id',
           'direction', 'twap_id', 'is_liquidation', 'crossed', 'dex']          # p3_fills.COLUMNS, unchanged
PRICE_START = c8.ms(dt.date(2026, 5, 28))   # 30 trailing daily returns before the first entry, plus slack
FUNDING_START = c8.ms(dt.date(2026, 6, 30))
HL_PAGE = 500
PERSIST_ATTEMPTS, PERSIST_GAP = 3, DAY
PERSISTABLE = ('closes_error:', 'closes_none:', 'closes_interior:', 'closes_tail:', 'closes_partial:',
               'funding_error:', 'funding_none:', 'funding_tail:')
HOOK_NAMES = ('s3_list', 's3_plan', 's3_get_columns', 's3_get_object', 'bg_candles', 'bg_contracts',
              'hl_funding', 'hl_meta')
G3_SAMPLE_DAYS, G3_ADDRESSES = 3, 3000
MAX_UNAVAILABLE = 8      # review r1 m2: at most 8 of the 168 mapped names (~5 %) may become unavailable
PULLED = {'bytes': 0}                       # every requester-pays byte this process (footers included)
_REAL = {'ok': False}                       # set by fetch() only for the real repo, real tree, default arguments


class FetchError(RuntimeError):
    """A typed failure: nothing is cached; the fetch resumes on re-run."""


def count(n):
    """Hooks report requester-pays bytes here as they transfer them (before they may raise)."""
    PULLED['bytes'] += n


# ------------------------------------------------------------------ real network hooks
def _require_real():
    if not _REAL['ok']:
        raise SystemExit('refusing: a real network hook was reached outside the real repository\'s guarded fetch')


def _archive_call(fn, *a):
    from ct import archive
    before = archive.bytes_pulled
    try:
        return fn(*a)
    finally:
        count(archive.bytes_pulled - before)


def _s3_list(prefix):
    _require_real()
    from ct import archive
    return archive.list_prefix(prefix)


def _s3_plan(key, size):
    """(column bytes to transfer, object size, footer bytes read) — footer bytes are counted."""
    _require_real()
    from ct import archive
    return _archive_call(archive.plan_columns, key, COLUMNS, size)


def _s3_get_columns(key, size, data_dir):
    _require_real()
    from ct import archive
    if Path(data_dir).resolve() != run008.REAL_DATA:
        raise SystemExit('refusing: the archive writes only under research/copytrade/data')
    return _archive_call(archive.download_columns, key, COLUMNS, size)


def _s3_get_object(key, size, data_dir):
    _require_real()
    from ct import archive
    if Path(data_dir).resolve() != run008.REAL_DATA:
        raise SystemExit('refusing: the archive writes only under research/copytrade/data')
    return _archive_call(archive.download, key, size)


def _bg_candles(symbol, start, end):
    _require_real()
    from ct import bitget as bg
    try:
        return bg._candles(symbol, '1H', start, end)
    except Exception as err:  # noqa: BLE001 — every failure is typed, never cached
        raise FetchError(f'bitget {symbol} {start}: {err}') from err


def _bg_contracts():
    _require_real()
    from ct import bitget as bg
    try:
        return sorted(r['symbol'] for r in bg._get(f'/api/v2/mix/market/contracts?productType={bg.PRODUCT}'))
    except Exception as err:  # noqa: BLE001
        raise FetchError(f'bitget contracts: {err}') from err


def _hl_funding(coin, start, end):
    _require_real()
    from ct import hl
    try:
        page = hl._info({'type': 'fundingHistory', 'coin': coin, 'startTime': start, 'endTime': end},
                        items_per_weight=20)
    except Exception as err:  # noqa: BLE001
        raise FetchError(f'hl funding {coin}: {err}') from err
    if not isinstance(page, list):
        raise FetchError(f'hl funding {coin}: not a list: {str(page)[:80]}')
    return page


def _hl_meta():
    _require_real()
    from ct import hl
    try:
        uni = hl.perp_universe()
    except Exception as err:  # noqa: BLE001
        raise FetchError(f'hl meta: {err}') from err
    if not isinstance(uni, list) or not uni:
        raise FetchError('hl meta: empty universe')
    return uni


REAL_HOOKS = {'s3_list': _s3_list, 's3_plan': _s3_plan, 's3_get_columns': _s3_get_columns,
              's3_get_object': _s3_get_object, 'bg_candles': _bg_candles, 'bg_contracts': _bg_contracts,
              'hl_funding': _hl_funding, 'hl_meta': _hl_meta}


# ------------------------------------------------------------------ helpers
def day_of(key):
    return key.split('date=')[1].split('/')[0]


def pruned_path(data_dir, key):
    return Path(data_dir) / 'pruned' / key


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value))
    tmp.replace(path)


def mapped(data_dir):
    t = json.loads((Path(data_dir) / 'p0/2026-10-07/symbols.json').read_text())
    return {c: r['symbol'] for c, r in t.items() if r['status'] == 'ok'}


def snapshot_dates_needed(end):
    """Calendar dates of the signal range (each window [max(S, 2026-07-01), hold end - 3 d))."""
    out = []
    for s, _, he in c8.grid_windows(end):
        d = max(s, c8.HOLDOUT_START)
        while c8.ms(d) < c8.ms(he) - c8.B_HORIZON_H * HOUR:
            out.append(d)
            d += dt.timedelta(days=1)
    return out


def snapshot_files(data_dir, dates):
    base = Path(data_dir) / f'archive/{SNAP_PREFIX}'
    return {d: sorted((base / f'date={d.isoformat()}').glob('*.parquet')) for d in dates}


# ------------------------------------------------------------------ the S3 part
def s3_plan(data_dir, end, hooks):
    """LIST first, then the exact plan. Returns (fills [(key, size, planned)], snaps [(key, size)], planned
    bytes). Raises FetchError when a needed day is not (yet) listed, or listed twice."""
    need_days = [d.isoformat() for d in c8.holdout_fill_days(end)]
    listing = [(k, s) for k, s in hooks['s3_list'](FILLS_PREFIX) if k.endswith('.parquet')]
    by_day = {}
    for k, s in listing:
        by_day.setdefault(day_of(k), []).append((k, s))
    missing = [d for d in need_days if len(by_day.get(d, [])) != 1]
    if missing:
        raise FetchError(f'fill days not listed exactly once (wait for the archive): {missing[:5]} ({len(missing)})')
    todo = [by_day[d][0] for d in need_days if not pruned_path(data_dir, by_day[d][0][0]).exists()]
    snap_need = [d for d, files in snapshot_files(data_dir, snapshot_dates_needed(end)).items() if len(files) != 1]
    snaps = []
    if snap_need:
        sl = [(k, s) for k, s in hooks['s3_list'](SNAP_PREFIX) if k.endswith('.parquet')]
        sby = {}
        for k, s in sl:
            sby.setdefault(day_of(k), []).append((k, s))
        miss = [d.isoformat() for d in snap_need if len(sby.get(d.isoformat(), [])) != 1]
        if miss:
            raise FetchError(f'snapshot dates not listed exactly once (wait): {miss[:5]} ({len(miss)})')
        snaps = [sby[d.isoformat()][0] for d in snap_need]
    fills = []
    for k, s in todo:
        need, _, _ = hooks['s3_plan'](k, s)
        fills.append((k, s, need))
    return fills, snaps, sum(p for _, _, p in fills) + sum(s for _, s in snaps)


def s3_pull(data_dir, fills, snaps, hooks, budget_left):
    """Download object by object; refuse BEFORE an object whose planned size would cross the cap, and stop
    if the measured transfer crosses it. budget_left() -> bytes still allowed (measured)."""
    for k, s, p in fills:
        if p > budget_left():
            raise FetchError('cap: the next fill day would cross the 15 GiB cap — ask the owner again')
        hooks['s3_get_columns'](k, s, data_dir)
        if budget_left() < 0:
            raise FetchError('cap reached during the fill pull')
    for k, s in snaps:
        if s > budget_left():
            raise FetchError('cap: the next snapshot would cross the 15 GiB cap — ask the owner again')
        hooks['s3_get_object'](k, s, data_dir)
        if budget_left() < 0:
            raise FetchError('cap reached during the snapshot pull')


def p3_gates(data_dir, end):
    """P3's G1 (UTC partitions) and G2 (dex column) on every holdout fill day, G3 (file order = execution
    order: startPosition chain breaks, file order vs (timestamp, trade_id) sort) on a fixed sample of days
    and addresses. Mechanics only: no cash flow, no PnL. G1/G2 failures refuse; G3 is reported."""
    import p3_checks as p3
    from ct import archive_fills as af
    days = [d.isoformat() for d in c8.holdout_fill_days(end)]
    g12 = p3.g1_g2('hyperliquid', days)
    sample = sorted({days[0], days[len(days) // 2], days[-1]})[:G3_SAMPLE_DAYS]
    fb = sb = nf = 0
    for day in sample:
        addrs = p3.sample_addresses('hyperliquid', day, G3_ADDRESSES, 11)
        by = af.to_api(af.read_day('hyperliquid', day, addresses=addrs))
        for a in addrs:
            fl = by.get(a, [])
            nf += len(fl)
            fb += p3.chain_breaks(fl)
            sb += p3.chain_breaks(sorted(fl, key=lambda f: (f['time'], f['tid'])))
    return {'g1_g2_pass': bool(g12), 'g3_days': sample, 'g3_fills': nf, 'g3_breaks_file_order': fb,
            'g3_breaks_time_tid_sort': sb}


def sentinel(data_dir, end):
    root = Path(data_dir) / 'pruned' / FILLS_PREFIX
    root.mkdir(parents=True, exist_ok=True)
    days = [d.isoformat() for d in c8.holdout_fill_days(end)]
    (root / 'HOLDOUT_DAYS_008_README.txt').write_text(
        'Registration 008 pulled HL holdout fill days here (requester-pays, guarded):\n'
        + '\n'.join(days) + '\nThey belong to the HL holdout calendar (012 D18; 008 D13). Any script that globs '
        'date=* in this directory must exclude dates >= 2026-07-01 unless it is a registration that names them.\n')


# ------------------------------------------------------------------ prices and funding
def blocks(start, end):
    b = start - start % HOUR_BLOCK
    out = []
    while b < end:
        out.append(b)
        b += HOUR_BLOCK
    return out


def fetch_closes(data_dir, end, hooks, symbols, listed, live):
    """Forward-paged 200-hour blocks; a non-empty answer is written (rows bounded by the read bound); an empty
    answer is recorded and NOT written. Blocks wholly before the first candle are not requested. An error on
    a LIVE symbol becomes a keyed refusal (the attempt continues with the next symbol); on a symbol Bitget no
    longer lists it is recorded as unavailable."""
    root = Path(data_dir) / 'r008' / 'bitget_1h'
    p_end = c8.read_bound(end)
    empty, refusals, unavailable = {}, {}, []
    for sym in symbols:
        first = listed.get(sym) or 0
        for b in blocks(PRICE_START, p_end):
            f = root / sym / f'{b}.json'
            if f.exists() or b + HOUR_BLOCK <= first:
                continue
            try:
                # Amendment 1: Bitget's history-candles endTime is EXCLUSIVE (startTime inclusive; a window wider
                # than `limit` returns the NEWEST rows). [b, b + HOUR_BLOCK) is exactly the block's 200 hours.
                rows = hooks['bg_candles'](sym, b, b + HOUR_BLOCK)
            except FetchError as err:
                if sym not in live:
                    unavailable.append(sym)
                else:
                    refusals[f'closes_error:{sym}' if sym != 'BTCUSDT' else 'btc_error'] = str(err)[:200]
                break
            rows = [r for r in (rows or []) if PRICE_START <= int(r[0]) < p_end and b <= int(r[0]) < b + HOUR_BLOCK]
            if not rows:
                empty.setdefault(sym, []).append(b)
                continue
            _write_json(f, sorted(([int(r[0])] + [float(x) for x in r[1:5]] for r in rows), key=lambda r: r[0]))
    return empty, refusals, sorted(set(unavailable))


def fetch_funding(data_dir, end, hooks, coins, hl_live):
    """Forward pages of <= 500 rows from 2026-06-30 to the read bound; written only when complete and
    non-empty. An error on a coin HL still lists is a keyed refusal; otherwise it is unavailable."""
    root = Path(data_dir) / 'r008' / 'funding'
    f_end = c8.read_bound(end)
    empty, refusals, unavailable = [], {}, []
    for coin in coins:
        f = root / f'{coin}.json'
        if f.exists():
            continue
        out, t = [], FUNDING_START
        try:
            while t < f_end:
                page = hooks['hl_funding'](coin, t, f_end)
                rows = [(int(r['time']), float(r['fundingRate'])) for r in page if int(r['time']) < f_end]
                out += rows
                if len(page) < HL_PAGE or not rows:
                    break
                t = rows[-1][0] + 1
        except FetchError as err:
            if coin in hl_live:
                refusals[f'funding_error:{coin}'] = str(err)[:200]
            else:
                unavailable.append(coin)
            continue
        out = sorted(set(out))
        if not out:
            empty.append(coin)
            continue
        _write_json(f, out)
    return empty, refusals, unavailable


def closes_coverage(data_dir, end, symbols, live, listed):
    """(keyed refusals, counts, files to invalidate) from the files on disk. A possibly partial stored block
    is listed for deletion so the re-run asks again (B-m5: partial interior blocks are refused, not kept)."""
    root = Path(data_dir) / 'r008' / 'bitget_1h'
    p_end = c8.read_bound(end)
    refuse, invalidate = {}, []
    counts = {'symbols': len(symbols), 'with_closes': 0, 'not_live_now': 0, 'missing_hours_live': 0}
    for sym in symbols:
        files = {int(p.stem): p for p in (root / sym).glob('*.json')} if (root / sym).exists() else {}
        first_h = max(PRICE_START, listed.get(sym) or 0)
        first_h += (-first_h) % HOUR
        if not files:
            if sym in live and (listed.get(sym) or 0) < c8.ms(end) - c8.LISTED_MIN_DAYS * DAY:
                refuse['btc_none' if sym == 'BTCUSDT' else f'closes_none:{sym}'] = 'listed on Bitget now, no closes at all'
            continue
        counts['with_closes'] += 1
        if sym not in live:
            counts['not_live_now'] += 1
            continue
        bl = sorted(files)
        interior = [b for b in blocks(bl[0], bl[-1] + 1) if b not in files]
        if interior:
            refuse[f'closes_interior:{sym}'] = f'empty interior blocks {interior[:3]}'
        have = {}
        for b, p in files.items():
            have[b] = {int(r[0]) for r in json.loads(p.read_text())}
        last = max(max(v) for v in have.values() if v)
        if last < c8.ms(end) - HOUR:
            key = 'btc_tail' if sym == 'BTCUSDT' else f'closes_tail:{sym}'
            refuse[key] = f'closes stop at {last} before the end'
            invalidate.append((key, files[bl[-1]]))
        for b in bl:
            lo, hi = max(b, first_h), min(b + HOUR_BLOCK, p_end)
            missing = sum(1 for h in range(lo, hi, HOUR) if h not in have[b])
            if missing:
                counts['missing_hours_live'] += missing
                key = f'btc_partial:{b}' if sym == 'BTCUSDT' else f'closes_partial:{sym}:{b}'
                refuse[key] = f'{missing} hours missing in block {b}'
                invalidate.append((key, files[b]))
    return refuse, counts, invalidate


def funding_coverage(data_dir, end, coins, hl_live):
    root = Path(data_dir) / 'r008' / 'funding'
    refuse, invalidate = {}, []
    counts = {'coins': len(coins), 'with_funding': 0, 'gaps_over_2h': 0}
    for coin in coins:
        f = root / f'{coin}.json'
        if not f.exists():
            if coin in hl_live:
                refuse[f'funding_none:{coin}'] = 'listed on HL now, no funding rows'
            continue
        ts = [t for t, _ in json.loads(f.read_text())]
        counts['with_funding'] += 1
        counts['gaps_over_2h'] += sum(1 for a, b in zip(ts, ts[1:]) if b - a > 2 * HOUR)
        if coin in hl_live and ts[-1] < c8.ms(end) - 2 * HOUR:
            refuse[f'funding_tail:{coin}'] = 'listed on HL now but funding stops before the end'
            invalidate.append((f'funding_tail:{coin}', f))
    return refuse, counts, invalidate


def disk_coverage(data_dir, end, root):
    """Fill days and pinned snapshots (B-M1): every lookback day on disk; every needed snapshot date exactly
    one file; the files a full glob would select are exactly those."""
    from ct.archive_fills import PREFIX
    refuse = {}
    for d in c8.lookback_fill_days(end):
        if not (Path(data_dir) / 'pruned' / PREFIX['hyperliquid'] / f'date={d.isoformat()}' / 'fills.parquet').exists():
            refuse[f'fills_missing:{d}'] = 'not on disk'
    for msg in run008.snapshot_pin_failures(root, data_dir, end, pinned_snapshots(root, data_dir, end)):
        refuse[f'snapshots:{msg[:60]}'] = msg
    return refuse


def pinned_snapshots(root, data_dir, end):
    files = []
    for fl in snapshot_files(data_dir, snapshot_dates_needed(end)).values():
        files += [str(p.resolve().relative_to(Path(root).resolve())) for p in fl]
    return sorted(files)


def attempt_history(root, results_rel='results.jsonl'):
    """Every committed fetch attempt, from the rows in the tree, the history of every branch and the reflog
    (review r1, m2). Edits can only ADD versions of a row, never remove the original: the bytes of an attempt
    are the maximum over its versions, and its time is the committer time of the EARLIEST commit that holds
    its end row on HEAD's history (an edited `at` field is ignored). {attempt: {'reserved', 'bytes',
    'refusals', 'time'}}; an attempt without an end row (a killed process) counts its reservation."""
    att = {}
    for line, r in run008.result_rows(root, results_rel):
        if r.get('mode') != 'holdout_fetch_attempt':
            continue
        a = att.setdefault(int(r.get('attempt', 0)), {'reserved': 0, 'bytes': None, 'refusals': set(), 'time': None})
        if r.get('phase') == 'start':
            a['reserved'] = max(a['reserved'], int(r.get('reserved') or 0))
            continue
        a['bytes'] = max(a['bytes'] or 0, int(r.get('bytes') or 0))
        a['refusals'] |= set(r.get('refusals') or [])
        c = run008.first_commit_containing(root, results_rel, line)
        t = run008.commit_time(root, c) if c else None
        if t is not None and (a['time'] is None or t < a['time']):
            a['time'] = t
    return att


def recorded_fill_hashes(root, results_rel='results.jsonl'):
    """{day: {sha256, ...}} from every committed attempt end row (Amendment 1): a holdout fill day already on disk
    is reused only while its hash equals every hash recorded for it."""
    out = {}
    for _, r in run008.result_rows(root, results_rel):
        if r.get('mode') == 'holdout_fetch_attempt' and r.get('phase') == 'end':
            for day, h in (r.get('fill_hashes') or {}).items():
                out.setdefault(day, set()).add(h)
    return out


def spent_bytes(att):
    return sum(a['bytes'] if a['bytes'] is not None else a['reserved'] for a in att.values())


def apply_persistence(refusals, att, now_s):
    """Split keyed refusals into (still blocking, persistent-unavailable). A PERSISTABLE key becomes
    unavailable when it appears in this attempt and in >= 2 earlier COMMITTED attempts forming a chain of 3
    attempts each >= 24 h after the previous one, by commit time."""
    blocking, persistent = {}, {}
    for key, msg in refusals.items():
        if not key.startswith(PERSISTABLE):
            blocking[key] = msg
            continue
        times = sorted(a['time'] for a in att.values() if key in a['refusals'] and a['time'] is not None)
        chain, last = 1, now_s
        for t in reversed(times):
            if (last - t) * 1000 >= PERSIST_GAP:
                chain += 1
                last = t
        (persistent if chain >= PERSIST_ATTEMPTS else blocking)[key] = msg
    return blocking, persistent


def manifest_files(root, data_dir, end):
    """Every file the read depends on, relative to root: the static data, the lookback fill days, the pinned
    signal snapshots, and 008's own store."""
    from ct.archive_fills import PREFIX
    root, data_dir = Path(root), Path(data_dir)
    files = [data_dir / f for f in ('p0/2026-10-07/symbols.json', 'p0/2026-10-07/report.json',
                                    'derived/bitget_listing.json', 'derived/regimes/btc_1h.parquet',
                                    'derived/hyperliquid/snapshot_accounts.parquet')]
    files += sorted((data_dir / 'cache' / 'hl-vaults').glob('*.json'))
    files += [data_dir / 'pruned' / PREFIX['hyperliquid'] / f'date={d.isoformat()}' / 'fills.parquet'
              for d in c8.lookback_fill_days(end)]
    files += [root / p for p in pinned_snapshots(root, data_dir, end)]
    store = data_dir / 'r008'
    for sub in ('bitget_1h', 'funding'):
        files += sorted((store / sub).rglob('*.json'))
    files += [store / 'report.json']
    return [str(p.resolve().relative_to(root.resolve())) for p in files if p.exists()]


# ------------------------------------------------------------------ the fetch
def fetch(root=HERE, data_dir=None, now_ms=None, hooks=None, now=None, **guard_kw):
    root = Path(root).resolve()
    data_dir = Path(data_dir).resolve() if data_dir else root / 'data'
    over = set(guard_kw) | ({'now_ms'} if now_ms is not None else set()) | ({'now'} if now is not None else set())
    pre = run008.real_tree_failures(root, data_dir, over, hooks)
    if hooks is None:
        if root != run008.HERE or data_dir != run008.REAL_DATA or over:
            pre.append('(B1) the real network hooks run only for the real repository, the real data tree and the '
                       'default guard arguments')
    elif set(hooks) != set(HOOK_NAMES):
        pre.append(f'(B1) mock hooks must supply exactly {list(HOOK_NAMES)}; no fallback to a real hook '
                   f'(missing {sorted(set(HOOK_NAMES) - set(hooks))})')
    if pre:
        raise SystemExit('\n'.join(pre))
    failures, ctx = run008.guard_failures(root, 'fetch', now_ms=now_ms, **guard_kw)
    if failures:
        raise SystemExit('\n'.join(failures))
    _REAL['ok'] = hooks is None
    hooks = dict(REAL_HOOKS) if hooks is None else dict(hooks)
    end, commit = ctx['end'], ctx['commit']
    code = guard_kw.get('code', run008.GUARDED_CODE)
    owner = {'end_date': ctx['end_date']}
    store = data_dir / 'r008'
    if not ctx['resume']:
        run008.append_row(root, {'registration': run008.REG_NAME, 'mode': 'holdout_fetch_started',
                                 'registration_commit': commit, 'owner': owner,
                                 'code_hashes': run008.hashes(root, code), 'env': run008.env_fingerprint(),
                                 'run_at': dt.datetime.now(dt.UTC).isoformat()})
        run008.commit_results(root, '008: holdout_fetch_started (automatic, r008_fetch.py)')
    store.mkdir(parents=True, exist_ok=True)
    att = attempt_history(root)                 # Amendment 1: attempts and bytes carry over across the amendment
    spent = spent_bytes(att)
    recorded = recorded_fill_hashes(root)
    n_att = max(att, default=0) + 1
    now = now or dt.datetime.now(dt.UTC)
    end_row = {'registration': run008.REG_NAME, 'mode': 'holdout_fetch_attempt', 'attempt': n_att, 'phase': 'end',
               'bytes': 0, 'refusals': [], 'unavailable': {}}
    b0 = PULLED['bytes']
    report = None
    run008.bind_data(data_dir)
    try:
        fills, snaps, planned = s3_plan(data_dir, end, hooks)
        footers = PULLED['bytes'] - b0
        print(f'plan: {len(fills)} fill days, {len(snaps)} snapshot days, {planned / c8.GIB:.2f} GiB '
              f'(+{footers / 2**20:.1f} MiB footers); spent before: {spent / c8.GIB:.2f} GiB; cap {c8.CAP_BYTES / c8.GIB:.0f} GiB')
        if spent + footers + planned > c8.CAP_BYTES:
            raise FetchError('refusing: planned transfer exceeds the 15 GiB cap — ask the owner again')
        run008.append_row(root, {'registration': run008.REG_NAME, 'mode': 'holdout_fetch_attempt', 'attempt': n_att,
                                 'phase': 'start', 'reserved': footers + planned, 'fills_to_pull': len(fills),
                                 'snapshots_to_pull': len(snaps), 'run_at': dt.datetime.now(dt.UTC).isoformat()})
        run008.commit_results(root, f'008: fetch attempt {n_att} start (automatic)')
        s3_pull(data_dir, fills, snaps, hooks, lambda: c8.CAP_BYTES - spent - (PULLED['bytes'] - b0))
        if fills or not (store / 'p3_gates.json').exists():
            gates = p3_gates(data_dir, end)
            _write_json(store / 'p3_gates.json', gates)
        gates = json.loads((store / 'p3_gates.json').read_text())
        sentinel(data_dir, end)
        from ct.archive_fills import day_file
        fill_hashes = {d.isoformat(): run008.sha256(day_file('hyperliquid', d.isoformat()))
                       for d in c8.holdout_fill_days(end) if day_file('hyperliquid', d.isoformat()).exists()}
        end_row['fill_hashes'] = fill_hashes
        changed = {d for d, h in fill_hashes.items() if recorded.get(d) and recorded[d] != {h}}
        coins = sorted(mapped(data_dir))
        symbols = sorted(set(mapped(data_dir).values()) | {'BTCUSDT'})
        listed = json.loads((data_dir / 'derived/bitget_listing.json').read_text())
        live = set(hooks['bg_contracts']())
        meta = hooks['hl_meta']()
        hl_live = {u['name'] for u in meta if not u.get('isDelisted')}
        empty_blocks, r0, unav_bg = fetch_closes(data_dir, end, hooks, symbols, listed, live)
        empty_coins, r0f, unav_hl = fetch_funding(data_dir, end, hooks, coins, hl_live)
        r1, c1, inv1 = closes_coverage(data_dir, end, symbols, live, listed)
        r2, c2, inv2 = funding_coverage(data_dir, end, coins, hl_live)
        r3 = disk_coverage(data_dir, end, root)
        if not gates['g1_g2_pass']:
            r3['p3_g1_g2'] = 'P3 G1/G2 failed on a holdout fill day'
        refusals = {**r0, **r0f, **r1, **r2, **r3}
        for d in sorted(changed):               # never persistable: a reused file must equal what was recorded
            refusals[f'fills_changed:{d}'] = 'a holdout fill day on disk differs from the hash a committed attempt recorded'
        end_row['refusals'] = sorted(refusals)
        blocking, persistent = apply_persistence(refusals, att, now.timestamp())
        unavailable = {'bitget_not_listed_now': sorted(set(unav_bg)), 'hl_not_listed_now': sorted(set(unav_hl)),
                       'persistent_after_3_attempts': sorted(persistent)}
        names = set(unav_bg) | set(unav_hl) | {k.split(':')[1] for k in persistent}
        if len(names) > MAX_UNAVAILABLE:
            blocking['unavailable_ceiling'] = (f'{len(names)} names would become unavailable (ceiling {MAX_UNAVAILABLE}, '
                                               '~5 % of 168): STOP for an orchestrator decision; something systematic')
        for key, f in inv1 + inv2:              # a possibly partial answer is never kept while it blocks;
            if key in blocking:                 # once persistent (3 attempts >= 24 h apart) it is kept and counted
                Path(f).unlink(missing_ok=True)
        end_row['unavailable'] = unavailable
        report = {'end': ctx['end_date'], 'closes': c1, 'funding': c2, 'p3_gates': gates,
                  'empty_blocks': {k: len(v) for k, v in sorted(empty_blocks.items())},
                  'empty_funding_coins': empty_coins, 'unavailable': unavailable,
                  'bitget_live_symbols': sorted(live),
                  'hl_meta': [{'name': u['name'], 'isDelisted': bool(u.get('isDelisted'))} for u in meta]}
        _write_json(store / 'report.json', report)
        if blocking:
            raise FetchError('coverage refused (no fetched row; re-run later):\n  '
                             + '\n  '.join(f'{k}: {v}' for k, v in list(blocking.items())[:20]))
    finally:
        _REAL['ok'] = False
        end_row['bytes'] = PULLED['bytes'] - b0
        end_row['run_at'] = dt.datetime.now(dt.UTC).isoformat()
        run008.append_row(root, end_row)        # every attempt is a committed row: the cap and the persistence
        run008.commit_results(root, f'008: fetch attempt {n_att} end (automatic)')   # clock come from these
    files = manifest_files(root, data_dir, end)
    hashed = {f: run008.sha256(root / f) for f in files}
    digest = run008.manifest_digest(hashed)
    _write_json(store / 'manifest.json', {'digest': digest, 'files': hashed,
                                          'snapshots': pinned_snapshots(root, data_dir, end)})
    att = attempt_history(root)
    total = spent_bytes(att)
    row = {'registration': run008.REG_NAME, 'mode': 'holdout_fetched', 'registration_commit': commit, 'owner': owner,
           'code_hashes': run008.hashes(root, code), 'env': run008.env_fingerprint(), 'manifest_digest': digest,
           'manifest_files': len(hashed), 'snapshots_pinned': len(pinned_snapshots(root, data_dir, end)),
           'bytes_requester_pays': total, 'attempts': len(att),
           'unavailable': {k: v for k, v in report['unavailable'].items()},
           'coverage': {'closes': report['closes'], 'funding': report['funding'], 'p3_gates': report['p3_gates']},
           'run_at': dt.datetime.now(dt.UTC).isoformat()}
    run008.append_row(root, row)
    run008.commit_results(root, '008: holdout_fetched (automatic, r008_fetch.py)')
    print(f'fetched: {len(hashed)} files in the manifest, {total / c8.GIB:.2f} GiB requester-pays in total')
    return row


if __name__ == '__main__':
    try:
        fetch()
    except FetchError as err:
        sys.exit(f'fetch stopped: {err}')
