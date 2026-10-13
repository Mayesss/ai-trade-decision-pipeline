"""Reviewer probes for 008 (synthetic data, throwaway git repos only). Run from research/copytrade:
    .venv/bin/python -I <this file>
"""
import json
import random
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

CT = Path('/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline/research/copytrade')
sys.path.insert(0, str(CT))

import tests.test_r008 as T  # noqa: E402
from ct import crowd008 as c8  # noqa: E402
import r008_fetch as F  # noqa: E402
import r008_run as R  # noqa: E402


def say(*a):
    print('PROBE', *a, flush=True)


def open_gate(tmp):
    """Throwaway repo + synthetic tree, fetched, rows committed, approval committed: the run gate open."""
    root = T.make_repo(tmp)
    data = root / 'data'
    fills = T.build_tree(data)
    mk = T.Market(T.SYMS + ['BTCUSDT'], F.PRICE_START, c8.ms(T.END) + 5 * T.D, seed=13)
    F.fetch(root, data, now_ms=T.NOW, hooks=T.Hooks(data, fills, mk).as_dict(), **T.GK)
    T.git(root, 'add', 'results.jsonl')
    T.git(root, 'commit', '-qm', 'fetch rows')
    fl = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"holdout_fetched"' in l][0]
    T.write_reg(root, extra=f'\n008 holdout read: APPROVED 2026-10-12 row {R.line_sha(fl)}\n')
    T.git(root, 'commit', '-qam', 'approval')
    f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
    assert f == [], f
    return root, data


def probe_snapshot_injection():
    with tempfile.TemporaryDirectory() as tmp:
        root, data = open_gate(tmp)
        body0 = R.compute(data, T.END, [])
        # an extra snapshot file, in a date directory the manifest does not list, whose NAME timestamp
        # falls inside window A's signal range (2026-07-30 12:00 UTC)
        extra = data / 'archive/by_dex/hyperliquid/snapshots/perp/date=2026-07-31' / f'1_{T.ms(2026, 7, 30, 12)}.parquet'
        T.write_snapshot(extra, T.snapshot_rows(T.WALLETS, T.COINS, random.Random(5)))
        # and a second file inside a listed date directory
        extra2 = data / 'archive/by_dex/hyperliquid/snapshots/perp/date=2026-08-10' / f'2_{T.ms(2026, 8, 10, 6)}.parquet'
        T.write_snapshot(extra2, T.snapshot_rows(T.WALLETS, T.COINS, random.Random(6)))
        f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
        body1 = R.compute(data, T.END, [])
        say('snapshot injection: run guard failures after adding 2 unlisted snapshot files =', f)
        say('  signal_days before/after:', body0['counts']['signal_days'], body1['counts']['signal_days'],
            '| t_alpha before/after:', round(body0['confirm']['t_alpha'], 4), round(body1['confirm']['t_alpha'], 4))


def probe_erase_and_rerun():
    with tempfile.TemporaryDirectory() as tmp:
        root, data = open_gate(tmp)
        R.run(root, data, now_ms=T.NOW, offline=False, **T.GK)
        modes = [json.loads(l)['mode'] for l in (root / 'results.jsonl').read_text().splitlines()]
        say('after run (uncommitted):', modes)
        T.git(root, 'checkout', '--', 'results.jsonl')            # discard the uncommitted started + holdout rows
        f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
        say('after `git checkout -- results.jsonl`: run guard failures =', f, '-> a second read is', 'OPEN' if not f else 'blocked')
        R.run(root, data, now_ms=T.NOW, offline=False, **T.GK)
        T.git(root, 'commit', '-qam', 'holdout row committed')
        T.git(root, 'reset', '-q', '--hard', 'HEAD~1')               # history rewrite; commit now only in the reflog
        f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
        say('after committing the holdout row then `git reset --hard HEAD~1`: run guard failures =', f,
            '-> a third read is', 'OPEN' if not f else 'blocked')


def probe_modules_and_opens():
    with tempfile.TemporaryDirectory() as tmp:
        root, data = open_gate(tmp)
        opened = set()

        def hook(ev, args):
            if ev == 'open' and args and isinstance(args[0], (str, Path)):
                p = str(args[0])
                if p.startswith(str(data)):
                    opened.add(p[len(str(data)) + 1:])
        sys.addaudithook(hook)
        before = set(sys.modules)
        R.run(root, data, now_ms=T.NOW, offline=True, **T.GK)
        mods = sorted(m.__file__ for m in list(sys.modules.values())
                      if getattr(m, '__file__', None) and m.__file__.startswith(str(CT)) and '.venv' not in m.__file__)
        rel = sorted(str(Path(m).relative_to(CT)) for m in mods)
        guarded = set(R.GUARDED_CODE)
        say('repo modules loaded (probe + tests included):', rel)
        say('  loaded but NOT guarded:', [m for m in rel if m not in guarded])
        dirs = sorted({'/'.join(p.split('/')[:3]) for p in opened})
        say('python-level opens under data/ during run():', dirs)


def probe_owner_line():
    base = '**Status: REGISTERED**\n'
    cases = {
        'quoted': '> 008 holdout end (owner): 2026-11-23\n008 holdout end (owner): 2026-09-28\n',
        'indented': '    008 holdout end (owner): 2026-11-23\n008 holdout end (owner): 2026-09-28\n',
        'two': '008 holdout end (owner): 2026-11-23\n008 holdout end (owner): 2026-09-28\n',
        'both on one line': '008 holdout end (owner): 2026-09-28 / 2026-11-23\n',
        'crlf': '008 holdout end (owner): 2026-09-28\r\n',
        'trailing dot': '008 holdout end (owner): 2026-09-28.\n',
        'in html comment + real': '<!--\n008 holdout end (owner): 2026-11-23\n-->\n008 holdout end (owner): 2026-09-28\n',
        'bold': '**008 holdout end (owner): 2026-11-23**\n008 holdout end (owner): 2026-09-28\n',
    }
    for k, v in cases.items():
        say('owner line', repr(k), '->', R.owner_end(base + v))


def probe_fetch_bytes():
    """A transfer that raises after pulling bytes (as archive.download_columns does on a row-count or range
    mismatch) is not counted; the next attempt's cap check therefore ignores it."""
    with tempfile.TemporaryDirectory() as tmp:
        root = T.make_repo(tmp)
        data = root / 'data'
        fills = T.build_tree(data)
        mk = T.Market(T.SYMS + ['BTCUSDT'], F.PRICE_START, c8.ms(T.END) + 5 * T.D, seed=13)
        size = 450 << 20                                   # 33 x 450 MiB = 14.5 GiB < 15 GiB cap
        truth = {'bytes': 0}

        class H(T.Hooks):
            def __init__(s, *a, fail_day=None, **k):
                super().__init__(*a, plan_size=size, **k)
                s.fail_day = fail_day

            def s3_get_columns(s, key, sz, data_dir):
                day = key.split('date=')[1].split('/')[0]
                truth['bytes'] += size                     # the ranges were transferred (and billed)
                if day == s.fail_day:
                    raise RuntimeError(f'{key}: read N rows, footer says M')   # archive.download_columns' check
                return super().s3_get_columns(key, sz, data_dir)
        try:
            F.fetch(root, data, now_ms=T.NOW, hooks=H(data, fills, mk, fail_day='2026-08-02').as_dict(), **T.GK)
        except RuntimeError as e:
            say('attempt 1 stopped:', str(e)[:60])
        log = json.loads((data / 'r008/fetch_log.json').read_text())
        say('after attempt 1: logged bytes', sum(a['bytes'] for a in log['attempts']) / 2**30, 'GiB; true',
            truth['bytes'] / 2**30, 'GiB')
        row = F.fetch(root, data, now_ms=T.NOW, hooks=H(data, fills, mk).as_dict(), **T.GK)
        say('after attempt 2: fetched row bytes_requester_pays', round(row['bytes_requester_pays'] / 2**30, 3),
            'GiB; true', round(truth['bytes'] / 2**30, 3), 'GiB; cap 15 GiB')


if __name__ == '__main__':
    which = sys.argv[1:] or ['owner', 'bytes', 'inject', 'erase', 'mods']
    if 'owner' in which:
        probe_owner_line()
    if 'bytes' in which:
        probe_fetch_bytes()
    if 'inject' in which:
        probe_snapshot_injection()
    if 'erase' in which:
        probe_erase_and_rerun()
    if 'mods' in which:
        probe_modules_and_opens()


def probe_root_data_split():
    """The run guard is evaluated on `root`; the data come from `data_dir`. A throwaway repo whose gate is open
    unlocks compute() on ANOTHER tree — here a stand-in for research/copytrade (R.HERE monkeypatched to it),
    whose own registration is DRAFT."""
    with tempfile.TemporaryDirectory() as tmp:
        real = T.make_repo(Path(tmp) / 'real', status='DRAFT')          # stand-in for the real repo (DRAFT)
        T.build_tree(real / 'data')
        f_real, _ = R.guard_failures(real, 'run', now_ms=T.NOW, **T.GK)
        fake, fake_data = open_gate(Path(tmp) / 'fake')                   # any throwaway repo, gate open
        shutil.copytree(fake_data / 'r008', real / 'data' / 'r008')        # (stand-in for the real fetch store)
        saved = R.HERE
        R.HERE = real
        R._GATE['open'] = False
        try:
            try:
                R.compute(real / 'data', T.END, [])
                say('compute(real tree) without run: NOT locked')
            except SystemExit as e:
                say('compute(real tree) without run: locked ->', str(e)[:50])
            row = R.run(fake, data_dir=real / 'data', now_ms=T.NOW, offline=True, **T.GK)
            say('real repo guard refuses:', [x[:4] for x in f_real], '| run(root=throwaway, data_dir=real tree) ->',
                'decision', row['decision'], 'computed on the real tree; rows written to the throwaway repo')
        finally:
            R.HERE = saved


if __name__ == '__main__' and 'split' in sys.argv[1:]:
    probe_root_data_split()
