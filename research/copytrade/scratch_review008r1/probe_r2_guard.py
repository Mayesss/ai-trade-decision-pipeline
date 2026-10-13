"""Revision 2 recheck of the guard / fetch fixes (synthetic data, throwaway repos, every hook mocked).

    cd research/copytrade && .venv/bin/python -I scratch_review008r1/probe_r2_guard.py [names...]

names: mockreal shadow attempts ceiling env survflag attr commitfail
"""
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import probe_guard_r1 as G  # noqa: E402

T, F, R, c8 = G.T, G.F, G.R, G.c8
say, ref, modes, market = G.say, G.ref, G.modes, G.market


def probe_mockreal():
    with tempfile.TemporaryDirectory() as tmp:
        real = T.make_repo(Path(tmp) / 'real')
        data = real / 'data'
        fills = T.build_tree(data)
        saved_def, saved_code = R.guard_failures.__defaults__, R.GUARDED_CODE
        R.HERE, R.REAL_DATA = real.resolve(), data.resolve()
        d = list(saved_def)
        d[0], d[3], d[4] = real.resolve(), T.GK['code'], T.GK['data']
        R.guard_failures.__defaults__ = tuple(d)
        R.GUARDED_CODE = T.GK['code']
        try:
            hk = T.Hooks(data, fills, market()).as_dict()
            say('mockreal: fetch(real root, real data, complete mocks, no override) ->', ref(F.fetch, real, data, hooks=hk)[:2])
            other = T.make_repo(Path(tmp) / 'other')
            say('mockreal: fetch(throwaway root, real data, complete mocks) ->', ref(F.fetch, other, data, hooks=hk)[:2])
            say('mockreal: rows in the stand-in real results.jsonl:', modes(real), '| fill days written:',
                len(list((data / 'pruned/by_dex/hyperliquid/fills/perp/all').glob('date=2026-07-*'))))
        finally:
            R.guard_failures.__defaults__, R.GUARDED_CODE = saved_def, saved_code
            R.HERE, R.REAL_DATA = G.REAL_HERE, G.REAL_DATA


def probe_shadow():
    with tempfile.TemporaryDirectory() as tmp:
        root = T.make_repo(tmp)
        (root / 'ct').mkdir()
        (root / 'ct' / '__init__.py').write_text('')
        (root / 'ct' / 'stats.py').write_text('NAME = "hashed"\n')
        (root / '.gitignore').write_text('data/\nignored_*\n')
        T.git(root, 'add', '-A')
        T.git(root, 'commit', '-qm', 'ct')
        cases = ['duckdb.py', 'duckdb/__init__.py', 'ct/stats/__init__.py', 'pyarrow.pyc',
                 'duckdb.cpython-314-darwin.so', 'ct/_x.abi3.so', 'ignored_mod.py', 'ct/tool/__init__.py']
        for rel in cases:
            p = root / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text('X = 1\n') if p.suffix == '.py' else p.write_bytes(b'')
        T.git(root, 'check-ignore', '-q', 'ignored_mod.py')
        found = R.shadow_modules(root)
        for rel in cases:
            say(f'shadow: {rel:<30} flagged:', rel in found)
        (root / 'ct' / 'stats.py').write_text('NAME = "edited"\n')
        say('shadow: modified tracked ct/stats.py flagged:', 'ct/stats.py' in R.shadow_modules(root))
        f, _ = R.guard_failures(root, 'fetch', now_ms=T.NOW, **T.GK)
        say('shadow: guard (d) ->', [x[:90] for x in f if x.startswith('(d)')])


def probe_attempts():
    """Bytes from committed attempt rows: transfer-then-raise, an edited (lowered) end row, a killed attempt,
    and the persistence clock on commit time vs a back-dated hand commit."""
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
                F.count(size)
                day = key.split('date=')[1].split('/')[0]
                if day == s.fail_day:
                    raise RuntimeError('row count mismatch after transfer')
                T.write_fill_day(Path(data_dir), day, s.fills.get(day, []))
                return size
        r = ref(F.fetch, root, data, now_ms=T.NOW, hooks=H(data, fills, market(), fail_day='2026-08-02').as_dict(), **T.GK)
        att = F.attempt_history(root)
        say('attempts: after a transfer-then-raise:', r[0], '| spent from history', round(F.spent_bytes(att) / 2**30, 3),
            'GiB, true', round(truth['b'] / 2**30, 3), '| results.jsonl clean:',
            T.git(root, 'status', '--porcelain', '--', 'results.jsonl') == '')
        # an edited end row with bytes lowered to 0, committed by hand
        lines = (root / 'results.jsonl').read_text().splitlines()
        i = max(j for j, l in enumerate(lines) if '"phase": "end"' in l)
        row = json.loads(lines[i])
        row['bytes'] = 0
        lines[i] = json.dumps(row, sort_keys=True)
        (root / 'results.jsonl').write_text('\n'.join(lines) + '\n')
        T.git(root, 'commit', '-qam', 'edit bytes down')
        say('attempts: after editing the end row to bytes 0: spent', round(F.spent_bytes(F.attempt_history(root)) / 2**30, 3), 'GiB')
        # a killed attempt: a start row committed, no end row
        R.append_row(root, {'registration': R.REG_NAME, 'mode': 'holdout_fetch_attempt', 'attempt': 9, 'phase': 'start',
                            'reserved': 3 << 30})
        R.commit_results(root, 'killed attempt start')
        say('attempts: + a killed attempt reserving 3 GiB: spent', round(F.spent_bytes(F.attempt_history(root)) / 2**30, 3), 'GiB')
        # persistence clock: three end rows with the same key, committed now -> not persistent
        key = 'closes_error:C3USDT'
        now_s = time.time()
        for n in (20, 21):
            R.append_row(root, {'registration': R.REG_NAME, 'mode': 'holdout_fetch_attempt', 'attempt': n, 'phase': 'end',
                                'bytes': 0, 'refusals': [key], 'at': '2026-01-01T00:00:00+00:00'})
            R.commit_results(root, f'attempt {n}')
        b, p = F.apply_persistence({key: 'e'}, F.attempt_history(root), now_s)
        say('persist: two earlier attempts committed seconds ago, back-dated "at" fields -> persistent:', bool(p))
        # a hand commit with a back-dated committer date (deliberate forgery)
        for n, days in ((30, 2), (31, 1)):
            R.append_row(root, {'registration': R.REG_NAME, 'mode': 'holdout_fetch_attempt', 'attempt': n, 'phase': 'end',
                                'bytes': 0, 'refusals': [key]})
            env = {**os.environ, 'GIT_COMMITTER_DATE': f'@{int(now_s - days * 86400 - 60)} +0000'}
            subprocess.run(['git', 'commit', '--only', '-qm', f'forged {n}', '--', 'results.jsonl'], cwd=root, env=env, check=True)
        b, p = F.apply_persistence({key: 'e'}, F.attempt_history(root), now_s)
        say('persist: two forged rows committed with GIT_COMMITTER_DATE back-dated 1 and 2 days -> persistent:', bool(p))


def probe_ceiling():
    with tempfile.TemporaryDirectory() as tmp:
        root = T.make_repo(tmp)
        data = root / 'data'
        fills = T.build_tree(data)
        gone = [f'C{i}USDT' for i in range(9)]
        r = ref(F.fetch, root, data, now_ms=T.NOW, hooks=T.Hooks(data, fills, market(), not_listed=gone).as_dict(), **T.GK)
        say('ceiling: 9 symbols Bitget no longer lists ->', r[0], (r[1] if r[0] != 'returned' else r[1]['unavailable'])
            if r[0] != 'returned' else 'fetched row written')
        r2 = ref(F.fetch, root, data, now_ms=T.NOW, hooks=T.Hooks(data, fills, market(), not_listed=gone).as_dict(), **T.GK)
        say('ceiling: a second attempt (permanent removals) ->', r2[0], str(r2[1])[:120])
    with tempfile.TemporaryDirectory() as tmp:
        root = T.make_repo(tmp)
        data = root / 'data'
        fills = T.build_tree(data)
        r = ref(F.fetch, root, data, now_ms=T.NOW,
                hooks=T.Hooks(data, fills, market(), not_listed=[f'C{i}USDT' for i in range(8)]).as_dict(), **T.GK)
        say('ceiling: 8 symbols gone ->', r[0], r[1]['unavailable']['bitget_not_listed_now'] if r[0] == 'returned' else r[1])


def probe_env():
    t = time.time()
    e = R.env_fingerprint()
    say('env:', {k: (v if isinstance(v, str) else {kk: vv for kk, vv in v.items() if kk != 'files_sha256'}) for k, v in e.items()},
        f'| {time.time() - t:.1f} s | stable:', e == R.env_fingerprint())


def probe_survflag():
    s = {'2026-08-03': {'cohort_gross_share_in_dropped': 0.02, 'crowd_gross_share_in_dropped': 0.03,
                        'dropped_would_rank': {'long': 1, 'short': 0, 'middle': 3}, 'dropped_coins_ranked': 1}}
    say('survflag: 1 long vs 0 short (1 coin) ->', c8.survivorship_flag(s))
    s['2026-08-03'].update(dropped_would_rank={'long': 12, 'short': 4, 'middle': 9}, dropped_coins_ranked=6)
    say('survflag: 6 coins ranked, 12 long vs 4 short ->', c8.survivorship_flag(s))
    s['2026-08-03'].update(crowd_gross_share_in_dropped=0.11, dropped_coins_ranked=1)
    say('survflag: crowd share 11 % ->', c8.survivorship_flag(s))


def probe_attr_and_commitfail():
    with tempfile.TemporaryDirectory() as tmp:
        root, data = G.open_gate(tmp)
        (root / '.gitattributes').write_text('results.jsonl -diff\n')
        hook = root / '.git' / 'hooks' / 'pre-commit'
        hook.write_text('#!/bin/sh\ngit show :results.jsonl | grep -q \'"mode": "holdout"\' && exit 1\nexit 0\n')
        hook.chmod(0o755)
        r = ref(R.run, root, data, now_ms=T.NOW, offline=True, **T.GK)
        say('commitfail: run ->', r[0], r[1][:200] if r[0] != 'returned' else '')
        f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
        say('commitfail: guard while uncommitted ->', [x[:60] for x in f])
        hook.unlink()
        T.git(root, 'commit', '--only', '-qm', 'by hand', '--', 'results.jsonl')
        T.git(root, 'reset', '-q', '--hard', 'HEAD~2')
        f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
        say('attr: with an uncommitted `results.jsonl -diff`, after reset --hard HEAD~2 ->', [x[:60] for x in f] or 'GATE OPEN')


PROBES = {'mockreal': probe_mockreal, 'shadow': probe_shadow, 'attempts': probe_attempts, 'ceiling': probe_ceiling,
          'env': probe_env, 'survflag': probe_survflag, 'attr': probe_attr_and_commitfail}

if __name__ == '__main__':
    for name in (sys.argv[1:] or PROBES):
        try:
            PROBES[name]()
        except Exception as e:  # noqa: BLE001
            say(name, 'probe error:', type(e).__name__, str(e)[:300])
