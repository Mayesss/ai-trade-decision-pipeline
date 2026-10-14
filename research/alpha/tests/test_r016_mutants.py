"""Registration 016, Revision 1 — every guard / fetch / panel protection has a test that FAILS when the protection is
removed (review B m4: seven of eleven protections could be deleted with the suite still green).

For each mutant: a fresh sandbox (a new git repository holding a copy of 016's code and tests, with REAL_REPO and
ANCHOR rewritten to the sandbox, synthetic data only — never a clone of the real repository, never real data), one
protection deleted by an exact textual replacement (refused unless the original text occurs exactly once), the
sandbox committed, and the targeted section of tests/test_r016.py run there. The mutant must make that section FAIL
on the named check; the unmutated sandbox must pass it.

    python3 -I -S tests/test_r016_mutants.py          (from research/alpha; ~15 minutes)
"""
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.append(str(HERE / 'tests'))
sys.path.append(str(HERE))
import test_r016 as T  # noqa: E402

PY = sys.executable
# (id, protection, file, original text, replacement, section, the check that must fail)
MUTANTS = [
    ('B-m4.1', 'Panel: real data (or a copy) only behind the gate', 'r016_run.py',
     '        if real and not _gate_ok():', '        if False:', 'gate', 'Panel refuses the real data without the gate'),
    ('B-m4.2', 'fetch: mock hooks / `now` never touch the real repository', 'r016_fetch.py',
     '    if run016.in_real(root) or run016.in_real(store) or run016.in_real(root / run016.RESULTS_REL):', '    if False:',
     'gate', 'mock hooks and `now` with root = the real directory'),
    ('B-m4.3', 'fetch: the real path runs only from the real repository', 'r016_fetch.py',
     '    if run016.HERE != _HERE0 or run016.REPO.resolve() != run016.bound_repo(', '    if False and run016.bound_repo(',
     'binding', 'the fetcher in a clone refuses'),
    ('B-m4.4', 'Panel: the span-(a) month cap', 'r016_run.py',
     "            R.refuse_unless(fm[2] <= A_MONTH_LAST, f'the scout pull holds a month after {A_MONTH_LAST}: {rel}')",
     '            pass', 'panel', 'a pull listing a 2026-07 file is refused'),
    ('B-m4.5', 'Panel: the span-(a) row cut at 2026-07-01', 'r016_run.py',
     'part = D.parse_klines(D.zip_text(self._read_verified(self.a / rel, want)), before=R.day(R.SPAN_C[0]))',
     'part = D.parse_klines(D.zip_text(self._read_verified(self.a / rel, want)))', 'panel',
     'a 2026-06 file holding a 2026-07 row'),
    ('B-m4.6', 'fetch: the 2026-06 / 2026-07 seam', 'r016_fetch.py',
     '        if fm[2] == run016.A_MONTH_LAST and tail and span[0] not in by_sym.get(fm[1], ()):', '        if False:',
     'fetch', 'traded at the end of 2026-06 in the pinned pull'),
    ('B-m4.7', 'fetch: BTCUSDT keys are never persistable', 'r016_fetch.py',
     "        if not key.startswith(PERSISTABLE) or f':{R.BTC}' in key:", '        if not key.startswith(PERSISTABLE):',
     'fetch', 'a BTCUSDT key never'),
    ('B-m4.8', '(t) the date gate', 'r016_run.py',
     '    if (now or dt.datetime.now(dt.UTC)).date() < R.READ_NOT_BEFORE:', '    if False:', 'guard',
     '(t) the date gate refuses before 2029-01-08'),
    ('B-m4.9', '(f) the fetch runs once', 'r016_run.py', '        if fetched:', '        if False:', 'guard',
     '(f) the fetch runs once'),
    ('B-m4.10', 'fetch: the .CHECKSUM verification', 'r016_fetch.py', '    if got != _checksum(hooks, key):',
     '    if False:', 'fetch', 'a checksum mismatch raises'),
    ('B-M1', '`now` is refused on the real repository', 'r016_run.py',
     '    if now is not None and (root.resolve().is_relative_to(REAL_REPO) or REAL_REPO.is_relative_to(root.resolve())):',
     '    if False:', 'gate', '`now` is refused on the real repository'),
    ('B-M4.a', '(r) one REGISTERED content anywhere', 'r016_run.py', '    if len({v for _, _, v in vers}) != 1:',
     '    if False:', 'history', 'a second REGISTERED content anywhere'),
    ('B-M4.b', '(r) guarded code at HEAD equals the REGISTERED commit', 'r016_run.py',
     '        if not a or a != head_blob(root, f):', '        if False:', 'history',
     'a committed revision of code + hash table after REGISTERED'),
    ('B-M3', '(d) skip-worktree / assume-unchanged flags', 'r016_run.py', "        if tag == 'S' or tag.islower():",
     '        if False:', 'history', '(d) skip-worktree on guarded code refuses'),
    ('B-m3.a', '(k) results.jsonl ends with a newline', 'r016_run.py',
     "    elif res.stat().st_size and not res.read_bytes().endswith(b'\\n'):", '    elif False:', 'guard',
     '(k) results.jsonl without a final newline'),
    ('B-m3.b', '(f) an unparseable 016 line refuses', 'r016_run.py',
     "                problems.append('an unparseable line mentions a 016 registration')", '                pass',
     'history', 'an unparseable added line'),
    ('B-M4.c', '(i) no restart after a runner-committed read row', 'r016_run.py',
     '        if by_runner or not rs or sl0 not in heads', '        if not rs or sl0 not in heads', 'restart',
     'a runner-committed read_started row is never restarted'),
    ('B-M2.a', '(v) python -I', 'r016_run.py', '    if not sys.flags.isolated:', '    if False:', 'env',
     'without -I: (v) refuses'),
    ('B-M2.b', '(d) a stdlib-named module at the top level', 'r016_run.py',
     "                if base == root and p.name.split('.')[0] in std:", '                if False:', 'env',
     'a COMMITTED stdlib-named module'),
    ('B-m2', 'Panel: a byte copy of a registered pull listing', 'r016_run.py',
     ' or (digest is not None and digest in registered_real_hashes())', '', 'gate', 'a byte copy of a registered pull'),
    ('B-m5.a', 'fetch: funding coverage', 'r016_fetch.py',
     '        if run016.FUNDING_MONTHS[0] <= mo <= run016.FUNDING_MONTHS[1] and (sym, mo) not in fund:', '        if False:',
     'fetch', 'a traded kline month without a funding file'),
    ('B-m5.b', 'fetch: the 2028-11 / 2028-12 seam', 'r016_fetch.py',
     "            if fm[2] == prev_mo and tail and not any(", "            if False and not any(", 'fetch',
     'the second seam'),
    ('B-m5.c', 'fetch: the exchangeInfo listing cross-check', 'r016_fetch.py', '                and s not in by_sym):',
     '                and False):', 'fetch', 'a live COIN perp of the exchangeInfo'),
    ('B-m5.d', 'fetch: resume re-verifies stored files', 'r016_fetch.py',
     '                    if run016.sha256(path) == _checksum(hooks, key):', '                    if True:', 'fetch',
     'resume: a stored file is re-checked'),
    ('B-m1', 'fetch: the witness (archive holds 2027-12)', 'r016_fetch.py',
     '    if not any(month_of_key(k) == run016.FWD_MONTHS[1] for k in keys):', '    if False:', 'fetch',
     'witness: the archive must list BTCUSDT 2028-12'),
    ('B-R1-M1', '(v) -X options refused', 'r016_run.py',
     "    if sys._xoptions or any(a.startswith('-X') for a in sys.orig_argv[1:]):", '    if False:', 'env',
     'a command-line -X pycache_prefix'),
    ('B-B1', '(w) no detached HEAD', 'r016_run.py', "    if _git(root, 'symbolic-ref', '-q', 'HEAD').returncode:",
     '    if False:', 'binding', 'a detached HEAD in the real repository refuses'),
]


def run_section(alpha, section):
    p = subprocess.run([PY, '-I', '-S', 'tests/test_r016.py', '--only', section], cwd=alpha, capture_output=True,
                       text=True)
    return p.returncode, p.stdout + p.stderr


def main():
    only = sys.argv[sys.argv.index('--only') + 1].split(',') if '--only' in sys.argv else None
    sections = sorted({m[5] for m in MUTANTS if only is None or m[0] in only})
    base = {}
    for sec in sections:
        alpha, top, git = T.make_sandbox()
        rc, out = run_section(alpha, sec)
        base[sec] = rc == 0
        print(f'  {"PASS" if rc == 0 else "FAIL"}  baseline (unmutated sandbox): section {sec} passes'
              + ('' if rc == 0 else '\n' + out[-600:]))
    killed = 0
    for mid, what, f, old, new, sec, must in MUTANTS:
        if only is not None and mid not in only:
            continue
        alpha, top, git = T.make_sandbox()
        src = (alpha / f).read_text()
        if src.count(old) != 1:
            print(f'  FAIL  {mid} {what}: the protection text occurs {src.count(old)} times in {f}')
            raise SystemExit(1)
        (alpha / f).write_text(src.replace(old, new))
        git('commit', '-q', '-am', f'mutant {mid}')
        rc, out = run_section(alpha, sec)
        failed_line = next((l for l in out.splitlines() if l.strip().startswith('FAIL')), '')
        ok = rc != 0 and must in failed_line
        killed += ok
        print(f'  {"PASS" if ok else "FAIL"}  {mid} ({what}) removed -> section {sec} fails on: '
              f'{failed_line.strip()[6:120] if failed_line else "(nothing failed)"}')
    n = sum(1 for m in MUTANTS if only is None or m[0] in only)
    print(f'mutants killed: {killed} of {n}; baselines passing: {sum(base.values())} of {len(base)}')
    if killed != n or not all(base.values()):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
