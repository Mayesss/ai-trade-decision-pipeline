"""Amendment 1 recheck (synthetic data, throwaway repos, every hook mocked).

    cd research/copytrade && .venv/bin/python -I scratch_review008r1/probe_amend1.py [names...]

names: abuse carry fills boundary
"""
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import probe_guard_r1 as G  # noqa: E402

T, F, R, c8 = G.T, G.F, G.R, G.c8
say, ref, market = G.say, G.ref, G.market
HOLE_H = c8.ms(T.END) - 20 * T.D + 5 * T.H          # an interior hour: attempt 1 refuses closes_partial


def started_under_prior(tmp, hooks_kw=None, patch_day_file=False):
    """Throwaway REGISTERED repo; attempt 1 refuses on coverage (a hole in C1USDT) under the prior commit."""
    root = T.make_repo(tmp)
    data = root / 'data'
    fills = T.build_tree(data)
    prior = T.git(root, 'rev-parse', 'HEAD').strip()
    hk = T.Hooks(data, fills, market(), hole={('C1USDT', HOLE_H)}, **(hooks_kw or {}))
    fetch = F.fetch
    if patch_day_file:                                   # attempt 1 as it ran: the REGISTERED (pre-amendment) fetch code
        import importlib.util, subprocess as sp
        src = sp.run(['git', 'show', '2bd855a:research/copytrade/r008_fetch.py'], cwd=G.CT, capture_output=True,
                     text=True, check=True).stdout
        pre = Path(tmp) / 'r008_fetch_pre.py'
        pre.write_text(src)
        spec = importlib.util.spec_from_file_location('r008_fetch_pre', pre)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        fetch = mod.fetch
        T.F_PRE = mod
    r = ref(fetch, root, data, now_ms=T.NOW, hooks=hk.as_dict(), **T.GK)
    assert r[0] == 'FetchError', r
    return root, data, fills, prior


def amend(root, prior, code='x = 2\n', extra_text='', line=None, msg='amendment 1'):
    (root / 'code' / 'a.py').write_text(code)
    line = line or f'008 amendment 1: APPROVED 2026-10-13 prior-commit {prior}'
    T.write_reg(root, extra=f'\n{extra_text}{line}\n')
    T.git(root, 'commit', '-qam', msg)


def fguard(root):
    f, _ = R.guard_failures(root, 'fetch', now_ms=T.NOW, **T.GK)
    return [x[:100] for x in f]


def probe_abuse():
    with tempfile.TemporaryDirectory() as tmp:
        root, data, fills, prior = started_under_prior(tmp)
        (root / 'code' / 'a.py').write_text('x = 2\n')
        T.write_reg(root)
        T.git(root, 'commit', '-qam', 'code change, no amendment line')
        say('abuse: code change WITHOUT an amendment line -> fetch guard', fguard(root))
        T.git(root, 'reset', '-q', '--hard', 'HEAD~1')
        amend(root, prior)
        say('abuse: valid Amendment 1 -> fetch guard', fguard(root) or 'OPEN')
        # (a) after the amendment commit, a FURTHER code change + hash, amendment line kept, no new line
        (root / 'code' / 'a.py').write_text('x = 3  # the decision rule, changed after the amendment\n')
        T.write_reg(root, extra=f'\n008 amendment 1: APPROVED 2026-10-13 prior-commit {prior}\n')
        T.git(root, 'commit', '-qam', 'post-amendment code swap')
        say('abuse: a later commit swaps guarded code again (line kept) -> fetch guard', fguard(root) or 'OPEN')
        # (b) and changes criteria prose / §12 text
        T.write_reg(root, extra=f'\nC1 now needs t >= 1.0 (changed)\n008 amendment 1: APPROVED 2026-10-13 prior-commit {prior}\n')
        T.git(root, 'commit', '-qam', 'post-amendment criteria edit')
        say('abuse: a later commit edits criteria text (line kept) -> fetch guard', fguard(root) or 'OPEN')
        # complete the fetch under the swapped code: the fetched row carries the LATEST commit, so (r)/(g2) never look back
        row = F.fetch(root, data, now_ms=T.NOW, hooks=T.Hooks(data, fills, market()).as_dict(), **T.GK)
        fl = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"holdout_fetched"' in l][0]
        T.write_reg(root, extra=f'\nC1 now needs t >= 1.0 (changed)\n008 amendment 1: APPROVED 2026-10-13 prior-commit {prior}\n'
                    f'008 holdout read: APPROVED 2026-10-13 row {R.line_sha(fl)}\n')
        T.git(root, 'commit', '-qam', 'approval')
        f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
        say('abuse: fetched row commit = post-swap commit:', row['registration_commit'] == T.git(root, 'rev-parse', 'HEAD~1').strip(),
            '| run guard ->', [x[:80] for x in f] or 'OPEN')
    with tempfile.TemporaryDirectory() as tmp:
        root, data, fills, prior = started_under_prior(tmp)
        for name, line in (('abbreviated sha', f'008 amendment 1: APPROVED 2026-10-13 prior-commit {prior[:12]}'),
                           ('forged 40-hex sha', '008 amendment 1: APPROVED 2026-10-13 prior-commit ' + 'a' * 40),
                           ('amendment 2 line', f'008 amendment 2: APPROVED 2026-10-13 prior-commit {prior}')):
            amend(root, prior, line=line, msg=name)
            say(f'abuse: {name} ->', fguard(root)[:2])
            T.git(root, 'reset', '-q', '--hard', 'HEAD~1')
        amend(root, prior, extra_text=f'008 amendment 1: APPROVED 2026-10-12 prior-commit {prior}\n')
        say('abuse: two amendment lines ->', fguard(root)[:2])
    with tempfile.TemporaryDirectory() as tmp:
        # an amendment added AFTER the fetched row, to swap code before the read
        root, data = G.open_gate(tmp)
        prior = T.git(root, 'log', '-1', '--format=%H', '--', 'registrations/008-inverted-positioning-holdout.md').strip()
        fl = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"holdout_fetched"' in l][0]
        (root / 'code' / 'a.py').write_text('x = 9\n')
        T.write_reg(root, extra=f'\n008 holdout read: APPROVED 2026-10-12 row {R.line_sha(fl)}\n'
                    f'008 amendment 1: APPROVED 2026-10-13 prior-commit {prior}\n')
        T.git(root, 'commit', '-qam', 'late amendment')
        f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
        say('abuse: amendment after the fetched row (code swapped) -> run guard', [x[:70] for x in f])


def probe_carry():
    with tempfile.TemporaryDirectory() as tmp:
        root, data, fills, prior = started_under_prior(tmp)
        att = F.attempt_history(root)
        b1 = F.spent_bytes(att)
        amend(root, prior)
        row = F.fetch(root, data, now_ms=T.NOW, hooks=T.Hooks(data, fills, market()).as_dict(), **T.GK)
        att = F.attempt_history(root)
        sr = [json.loads(l) for l in (root / 'results.jsonl').read_text().splitlines() if '"holdout_fetch_started"' in l]
        say('carry: attempts', sorted(att), '| attempt-1 bytes', round(b1 / 2**30, 3), 'GiB | fetched row bytes',
            round(row['bytes_requester_pays'] / 2**30, 3), 'GiB (= attempt 1 + attempt 2:', round(F.spent_bytes(att) / 2**30, 3), ')')
        say('carry: started row commit == prior:', [r['registration_commit'] == prior for r in sr],
            '| fetched row commit == amendment commit:', row['registration_commit'] == T.git(root, 'rev-parse', 'HEAD~1').strip()
            or row['registration_commit'])
        fl = [l for l in (root / 'results.jsonl').read_text().splitlines() if '"holdout_fetched"' in l][0]
        T.write_reg(root, extra=f'\n008 amendment 1: APPROVED 2026-10-13 prior-commit {prior}\n'
                    f'008 holdout read: APPROVED 2026-10-13 row {R.line_sha(fl)}\n')
        T.git(root, 'commit', '-qam', 'approval')
        f, _ = R.guard_failures(root, 'run', now_ms=T.NOW, **T.GK)
        say('carry: run guard after approval ->', f or 'OPEN')
    with tempfile.TemporaryDirectory() as tmp:
        # the started row's owner end must still equal today's
        root, data, fills, prior = started_under_prior(tmp)
        (root / 'code' / 'a.py').write_text('x = 2\n')
        T.write_reg(root, end='2026-11-23', extra=f'\n008 amendment 1: APPROVED 2026-10-13 prior-commit {prior}\n')
        T.git(root, 'commit', '-qam', 'amendment that also moves the end')
        say('carry: amendment that also changes the owner end -> fetch guard', fguard(root)[:2])


def probe_fills():
    """Attempt 1 recorded no fill hashes (as it ran). A fill day swapped before attempt 2 is not detected, and
    G1/G2 are not re-run on reused days."""
    import p3_checks as p3
    calls = {'g12': 0}
    saved = p3.g1_g2

    def counting(*a, **k):
        calls['g12'] += 1
        return saved(*a, **k)
    with tempfile.TemporaryDirectory() as tmp:
        root, data, fills, prior = started_under_prior(tmp, patch_day_file=True)
        e1 = [json.loads(l) for l in (root / 'results.jsonl').read_text().splitlines() if '"phase": "end"' in l][-1]
        say('fills: attempt 1 end row records fill hashes:', bool(e1.get('fill_hashes')))
        day = '2026-07-15'
        rows = fills[day]
        T.write_fill_day(data, day, rows[: len(rows) // 2])        # half the rows gone
        amend(root, prior)
        p3.g1_g2 = counting
        try:
            r = ref(F.fetch, root, data, now_ms=T.NOW, hooks=T.Hooks(data, fills, market()).as_dict(), **T.GK)
        finally:
            p3.g1_g2 = saved
        say('fills: attempt 2 after swapping', day, '->', r[0], '| G1/G2 re-run on reused days:', calls['g12'] > 0)
        # after attempt 2 the hashes are recorded: a second swap is caught (if the fetch had not completed)
    with tempfile.TemporaryDirectory() as tmp:
        root, data, fills, prior = started_under_prior(tmp)            # attempt 1 WITH hashes (revised code)
        T.write_fill_day(data, '2026-07-15', fills['2026-07-15'][:3])
        amend(root, prior)
        r = ref(F.fetch, root, data, now_ms=T.NOW, hooks=T.Hooks(data, fills, market()).as_dict(), **T.GK)
        say('fills: with hashes recorded, a swapped day ->', r[0], str(r[1])[:110].replace('\n', ' '))


def probe_boundary():
    H, B = T.H, F.HOUR_BLOCK
    mk = market()

    def make(mode):
        def bg(sym, start, end):
            hi = end if mode in ('exclusive', 'newest') else end + H                 # exclusive / inclusive end
            hs = list(range(start, hi, H))
            if mode in ('newest', 'inclusive_newest') and len(hs) > 200:
                hs = hs[-200:]                                                     # wider than limit: newest 200
            return [mk.candle_1h(sym, h) for h in hs if mk.candle_1h(sym, h)]
        return {'bg_candles': bg}
    for mode in ('exclusive', 'inclusive', 'newest', 'inclusive_newest'):
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / 'data'
            syms = ['C1USDT', 'BTCUSDT']
            listed = {s: c8.ms(T.END) - 400 * T.D for s in syms}
            F.fetch_closes(data, T.END, make(mode), syms, listed, set(syms))
            ref_, counts, _ = F.closes_coverage(data, T.END, syms, set(syms), listed)
            say(f'boundary: {mode:<17} coverage refusals {len(ref_)}, missing hours {counts["missing_hours_live"]}')


PROBES = {'abuse': probe_abuse, 'carry': probe_carry, 'fills': probe_fills, 'boundary': probe_boundary}

if __name__ == '__main__':
    for name in (sys.argv[1:] or PROBES):
        try:
            PROBES[name]()
        except Exception as e:  # noqa: BLE001
            import traceback
            traceback.print_exc()
            say(name, 'probe error:', type(e).__name__, str(e)[:300])
