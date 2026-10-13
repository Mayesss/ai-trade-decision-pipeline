"""Review 008-R1 guard probes (synthetic data and throwaway git repos only; every network hook mocked).

    cd research/copytrade && .venv/bin/python -I scratch_review008r1/probe_guard_r1.py [names...]

names: split mockreal delisted inject bytes erase commitfail only shadow persist env  (default: all)
Nothing here touches research/copytrade/data or the real results.jsonl: the "real" repository is a stand-in
created in a temp dir, and R.HERE / R.REAL_DATA are pointed at it for the probes that need it.
"""
import importlib
import json
import random
import shutil
import subprocess
import sys
import tempfile
import time
import datetime as dt
from pathlib import Path

CT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CT))

import tests.test_r008 as T  # noqa: E402
from ct import crowd008 as c8  # noqa: E402
import r008_fetch as F  # noqa: E402
import r008_run as R  # noqa: E402

REAL_HERE, REAL_DATA = R.HERE, R.REAL_DATA


def say(*a):
    print('PROBE', *a, flush=True)


def market():
    return T.Market(T.SYMS + ['BTCUSDT'], F.PRICE_START, c8.ms(T.END) + 5 * T.D, seed=13)


def fetched(tmp, status='REGISTERED'):
    root = T.make_repo(tmp, status=status)
    data = root / 'data'
    fills = T.build_tree(data)
    if status == 'REGISTERED':
        F.fetch(root, data, now_ms=T.NOW, hooks=T.Hooks(data, fills, market()).as_dict(), **T.GK)
    return root, data, fills


def open_gate(tmp):
    root, data, _ = fetched(tmp)
    fl = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"holdout_fetched"' in l][0]
    T.write_reg(root, extra=f'\n008 holdout read: APPROVED 2026-10-12 row {R.line_sha(fl)}\n')
    T.git(root, 'commit', '-qam', 'approval')
    f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
    assert f == [], f
    return root, data


def ref(fn, *a, **k):
    try:
        return ('returned', fn(*a, **k))
    except SystemExit as e:
        return ('SystemExit', str(e)[:160])
    except Exception as e:  # noqa: BLE001
        return (type(e).__name__, str(e)[:160])


def modes(root):
    return [json.loads(l).get('mode') for l in (root / 'results.jsonl').read_text().splitlines() if l.strip()]


# ------------------------------------------------------------------ B-B1 re-run: throwaway gate, "real" data
def probe_split():
    with tempfile.TemporaryDirectory() as tmp:
        real = T.make_repo(Path(tmp) / 'real', status='DRAFT')
        T.build_tree(real / 'data')
        fake, fake_data = open_gate(Path(tmp) / 'fake')
        shutil.copytree(fake_data / 'r008', real / 'data' / 'r008')
        R.HERE, R.REAL_DATA = real.resolve(), (real / 'data').resolve()
        try:
            say('split: run(root=throwaway open gate, data_dir=stand-in real tree) ->',
                ref(R.run, fake, data_dir=real / 'data', now_ms=T.NOW, offline=True, **T.GK)[:2])
            R._GATE['pair'] = None
            say('split: compute(stand-in real tree) outside run ->', ref(R.compute, real / 'data', T.END, [], [])[:2])
            # the documented residual (B-m10): a throwaway data dir whose sub-trees are symlinks to the real one
            link = Path(tmp) / 'linkdata'
            link.mkdir()
            for sub in ('p0', 'derived', 'cache', 'archive', 'pruned', 'r008'):
                (link / sub).symlink_to(real / 'data' / sub)
            r = ref(R.run, fake, data_dir=link, now_ms=T.NOW, offline=True, **T.GK)
            say('split: run(root=throwaway, data_dir=<dir of symlinks into the stand-in real tree>) ->', r[0],
                (r[1]['decision'] if r[0] == 'returned' else r[1]))
        finally:
            R.HERE, R.REAL_DATA = REAL_HERE, REAL_DATA
            R._GATE['pair'] = None


# ------------------------------------------------------------------ new: mock hooks on the real tree, default args
def probe_mockreal():
    """After REGISTERED (and end + 7 d), fetch(hooks=<mocks>) with root = the real repo and NO guard override is
    not refused by the B1 pre-checks: synthetic fills/prices are written into the real tree and committed rows
    land in the real results.jsonl. Stand-in: the real repo's guard defaults are pointed at the stand-in's files."""
    with tempfile.TemporaryDirectory() as tmp:
        real = T.make_repo(Path(tmp) / 'real')
        data = real / 'data'
        fills = T.build_tree(data)
        saved_def = R.guard_failures.__defaults__
        saved_code = R.GUARDED_CODE
        R.HERE, R.REAL_DATA = real.resolve(), data.resolve()
        d = list(saved_def)
        d[0], d[3], d[4] = real.resolve(), T.GK['code'], T.GK['data']      # root, code, data defaults
        R.guard_failures.__defaults__ = tuple(d)
        R.GUARDED_CODE = T.GK['code']
        try:
            pre = R.real_tree_failures(real, data, set())
            say('mockreal: real_tree_failures(real root, real data, no override) =', pre)
            hk = T.Hooks(data, fills, market())
            # no now_ms (it would count as an override); the stand-in's clock gate passes on today's date
            r = ref(F.fetch, real, data, hooks=hk.as_dict())
            say('mockreal: fetch(real root, real data, hooks=<complete mock dict>, no override) ->', r[0],
                (r[1] or {}).get('mode') if r[0] == 'returned' else r[1])
            say('mockreal: rows now in the stand-in real results.jsonl:', modes(real),
                '| git status results.jsonl:', repr(T.git(real, 'status', '--porcelain', '--', 'results.jsonl')))
            say('mockreal: synthetic fill days written into the stand-in real tree:',
                len(list((data / 'pruned/by_dex/hyperliquid/fills/perp/all').glob('date=2026-07-*'))))
        finally:
            R.guard_failures.__defaults__ = saved_def
            R.GUARDED_CODE = saved_code
            R.HERE, R.REAL_DATA = REAL_HERE, REAL_DATA


# ------------------------------------------------------------------ B-B2 re-run
def probe_delisted():
    class H(T.Hooks):
        def bg_contracts(self):
            return [s for s in super().bg_contracts() if s != 'C9USDT']

        def bg_candles(self, sym, start, end):
            if sym == 'C9USDT':
                raise F.FetchError('bitget C9USDT: 40034 Parameter symbol does not exist')
            return super().bg_candles(sym, start, end)
    with tempfile.TemporaryDirectory() as tmp:
        root = T.make_repo(tmp)
        data = root / 'data'
        fills = T.build_tree(data)
        r = ref(F.fetch, root, data, now_ms=T.NOW, hooks=H(data, fills, market()).as_dict(), **T.GK)
        say('delisted: attempt 1 ->', r[0], r[1]['unavailable'] if r[0] == 'returned' else r[1])


# ------------------------------------------------------------------ B-M1 re-run
def probe_inject():
    with tempfile.TemporaryDirectory() as tmp:
        root, data = open_gate(tmp)
        extra = data / 'archive/by_dex/hyperliquid/snapshots/perp/date=2026-07-31' / f'1_{T.ms(2026, 7, 30, 12)}.parquet'
        T.write_snapshot(extra, T.snapshot_rows(T.WALLETS, T.COINS, random.Random(5)))
        f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
        say('inject: unlisted-directory snapshot -> guard', [x[:70] for x in f])
        extra.unlink()
        extra2 = data / 'archive/by_dex/hyperliquid/snapshots/perp/date=2026-08-10' / f'2_{T.ms(2026, 8, 10, 6)}.parquet'
        T.write_snapshot(extra2, T.snapshot_rows(T.WALLETS, T.COINS, random.Random(6)))
        f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
        say('inject: second file in a listed date -> guard', [x[:70] for x in f])


# ------------------------------------------------------------------ B-M2 re-run
def probe_bytes():
    with tempfile.TemporaryDirectory() as tmp:
        root = T.make_repo(tmp)
        data = root / 'data'
        fills = T.build_tree(data)
        size = 450 << 20
        truth = {'b': 0}

        class H(T.Hooks):
            def __init__(s, *a, fail_day=None, **k):
                super().__init__(*a, plan_size=size, **k)
                s.fail_day = fail_day

            def s3_plan(s, key, sz):
                truth['b'] += 4096
                return super().s3_plan(key, sz)

            def s3_get_columns(s, key, sz, data_dir):
                truth['b'] += size
                day = key.split('date=')[1].split('/')[0]
                F.count(size)
                if day == s.fail_day:
                    raise RuntimeError('row count mismatch after transfer')
                T.write_fill_day(Path(data_dir), day, s.fills.get(day, []))
                return size
        r = ref(F.fetch, root, data, now_ms=T.NOW, hooks=H(data, fills, market(), fail_day='2026-08-02').as_dict(), **T.GK)
        log = json.loads((data / 'r008/fetch_log.json').read_text())
        say('bytes: attempt 1', r[0], '| logged', round(sum(a['bytes'] for a in log['attempts']) / 2**30, 3),
            'GiB, true', round(truth['b'] / 2**30, 3))
        r = ref(F.fetch, root, data, now_ms=T.NOW, hooks=H(data, fills, market()).as_dict(), **T.GK)
        say('bytes: attempt 2', r[0], '| row bytes', round(r[1]['bytes_requester_pays'] / 2**30, 3) if r[0] == 'returned' else r[1],
            'GiB, true', round(truth['b'] / 2**30, 3))
        # the cap and the persistence clock live in an unversioned file: edit it, and both move
        log = json.loads((data / 'r008/fetch_log.json').read_text())
        say('bytes: fetch_log.json is under data/ (gitignored):', T.git(root, 'check-ignore', 'data/r008/fetch_log.json').strip())


# ------------------------------------------------------------------ B-m1 re-run, plus deliberate history destruction
def probe_erase():
    with tempfile.TemporaryDirectory() as tmp:
        root, data = open_gate(tmp)
        R.run(root, data, now_ms=T.NOW, offline=True, **T.GK)
        say('erase: after run, modes', modes(root), '| dirty:', repr(T.git(root, 'status', '--porcelain', '--', 'results.jsonl')))
        T.git(root, 'reset', '-q', '--hard', 'HEAD~2')
        f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
        say('erase: after reset --hard HEAD~2 ->', [x[:60] for x in f])
        T.git(root, 'reflog', 'expire', '--expire=now', '--all')
        T.git(root, 'gc', '-q', '--prune=now')
        f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
        say('erase: after reflog expire --all + gc --prune=now ->', f or 'GATE OPEN (second read possible)')


# ------------------------------------------------------------------ new: the runner's own commit fails
def probe_commitfail():
    with tempfile.TemporaryDirectory() as tmp:
        root, data = open_gate(tmp)
        hook = root / '.git' / 'hooks' / 'pre-commit'
        # fails only when the staged results.jsonl contains a holdout row (the SECOND automatic commit)
        hook.write_text('#!/bin/sh\ngit show :results.jsonl | grep -q \'"mode": "holdout"\' && exit 1\nexit 0\n')
        hook.chmod(0o755)
        r = ref(R.run, root, data, now_ms=T.NOW, offline=True, **T.GK)
        say('commitfail: run ->', r[0], r[1] if r[0] != 'returned' else '')
        say('commitfail: committed modes', [json.loads(l)['mode'] for l in T.git(root, 'show', 'HEAD:results.jsonl').splitlines()],
            '| working-tree modes', modes(root))
        wt = [json.loads(l) for l in (root / 'results.jsonl').read_text().splitlines() if '"mode": "holdout"' in l]
        say('commitfail: the uncommitted holdout row carries the decision in plain text:', bool(wt and 'decision' in wt[0]))
        hook.unlink()
        T.git(root, 'checkout', '--', 'results.jsonl')
        f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
        say('commitfail: after discarding it, guard ->', [x[:70] for x in f])


# ------------------------------------------------------------------ new: does `commit --only` sweep staged changes?
def probe_only():
    with tempfile.TemporaryDirectory() as tmp:
        root = T.make_repo(tmp)
        (root / 'notes.txt').write_text('staged, unrelated\n')
        T.git(root, 'add', 'notes.txt')
        (root / 'results.jsonl').write_text('{"registration": "x", "mode": "y"}\n')
        R.commit_results(root, 'auto')
        say('only: files in the automatic commit:', T.git(root, 'show', '--name-only', '--format=', 'HEAD').split(),
            '| still staged:', T.git(root, 'diff', '--cached', '--name-only').split())


# ------------------------------------------------------------------ B-m11 re-run, and what (d) does not see
def probe_shadow():
    with tempfile.TemporaryDirectory() as tmp:
        root = T.make_repo(tmp)
        (root / 'ct').mkdir()
        (root / 'ct' / '__init__.py').write_text('')
        (root / 'ct' / 'stats.py').write_text('NAME = "hashed"\n')
        T.git(root, 'add', '-A')
        T.git(root, 'commit', '-qm', 'ct')
        cases = {'duckdb.py': 'top-level module', 'duckdb/__init__.py': 'top-level PACKAGE',
                 'ct/stats/__init__.py': 'package shadowing the hashed ct/stats.py', 'pyarrow.pyc': 'sourceless .pyc'}
        for rel in cases:
            p = root / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text('NAME = "shadow"\n') if p.suffix == '.py' else p.write_bytes(b'')
        flagged = R.untracked_py(root)
        for rel, what in cases.items():
            say(f'shadow: {what:<45} {rel:<22} flagged by (d):', rel in flagged)
        # import precedence: a package directory beats a module of the same name
        r = subprocess.run([sys.executable, '-I', '-c', f'import sys; sys.path.insert(0, {str(root)!r}); '
                            'import ct.stats as s; print(s.NAME, s.__file__)'], capture_output=True, text=True)
        say('shadow: `import ct.stats` resolves to ->', r.stdout.strip() or r.stderr.strip()[-120:])


# ------------------------------------------------------------------ persistence end state
def probe_persist():
    now = dt.datetime(2026, 10, 20, tzinfo=dt.UTC)
    key = 'closes_error:C3USDT'
    honest = {'attempts': [{'at': (now - dt.timedelta(hours=h)).isoformat(), 'refusals': {key: 'e'}} for h in (5, 3)]
              + [{'at': now.isoformat(), 'refusals': {key: 'e'}}]}
    say('persist: 3 attempts within 5 h ->', F.apply_persistence({key: 'e'}, honest, now))
    forged = {'attempts': [{'at': (now - dt.timedelta(days=d)).isoformat(), 'refusals': {key: 'e'}} for d in (2, 1)]
              + [{'at': now.isoformat(), 'refusals': {key: 'e'}}]}
    say('persist: same attempts with edited "at" fields in fetch_log.json ->', F.apply_persistence({key: 'e'}, forged, now))
    say('persist: BTC keys are never persistable ->', F.apply_persistence({'btc_error': 'e'}, forged, now))
    many = {f'closes_error:C{i}USDT': 'rate limited' for i in range(12)}
    log = {'attempts': [{'at': (now - dt.timedelta(days=d)).isoformat(), 'refusals': dict(many)} for d in (2, 1)]
           + [{'at': now.isoformat(), 'refusals': dict(many)}]}
    b, p = F.apply_persistence(many, log, now)
    say(f'persist: a systematic per-symbol failure on 3 attempts -> {len(p)} of {len(many)} symbols unavailable, '
        f'{len(b)} still blocking (no ceiling on how many names may drop)')


# ------------------------------------------------------------------ environment fingerprint
def probe_env():
    t = time.time()
    a = R.env_fingerprint()
    b = R.env_fingerprint()
    say('env: stable across calls', a == b, f'| {time.time() - t:.1f} s for two calls | python', a['python'],
        '| venv interpreter ->', Path(sys.executable).resolve())
    say('env: what it covers:', {k: (v if isinstance(v, str) else {kk: vv for kk, vv in v.items() if kk != 'binaries_sha256'})
                                 for k, v in a.items()})


PROBES = {'split': probe_split, 'mockreal': probe_mockreal, 'delisted': probe_delisted, 'inject': probe_inject,
          'bytes': probe_bytes, 'erase': probe_erase, 'commitfail': probe_commitfail, 'only': probe_only,
          'shadow': probe_shadow, 'persist': probe_persist, 'env': probe_env}

if __name__ == '__main__':
    for name in (sys.argv[1:] or PROBES):
        try:
            PROBES[name]()
        except Exception as e:  # noqa: BLE001
            say(name, 'probe error:', type(e).__name__, str(e)[:300])
