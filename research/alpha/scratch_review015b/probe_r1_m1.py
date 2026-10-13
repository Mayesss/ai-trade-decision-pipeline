"""Reviewer B, 015 Revision 1 recheck: M1 (trees mixed, mocks with the real repo in any role) and N1 (the gate opens
on a PASSED GUARD, not on a committed started row). Run with the stand-in's r015_run (stand-in = the real repo).

    python3 -I -S probe_r1_m1.py <stand-in research/alpha> <tmp dir>              (M1 + N1 from a script)
    RB_ARGS="<stand-in>|<tmp>" python3 -I -S -c "exec(open('probe_r1_m1.py').read())"   (N1 bypass; -I ignores env, so ARGS fall back)"""
import contextlib, io, json, os, subprocess, sys
ARGS = sys.argv[1:] if len(sys.argv) > 2 else os.environ["RB_ARGS"].split("|")
from pathlib import Path
A, T = Path(ARGS[0]).resolve(), Path(ARGS[1]).resolve()
sys.path.append(str(A))
import r015_run as R
assert R.REPO.resolve() == R.REAL_REPO, (R.REPO, R.REAL_REPO)
real_rows = lambda: sum(1 for l in open(A / 'results.jsonl') if l.strip())
head0 = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=A, capture_output=True, text=True).stdout.strip()

def attempt(label, fn):
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            out = fn()
        print(f'{label}: NOT REFUSED -> {str(out)[:120]}')
    except Exception as e:
        print(f'{label}: refused ({type(e).__name__}: {str(e).splitlines()[0][:110]})')

def other_tree(name, link_data):
    o = T / name
    subprocess.run(['rm', '-rf', str(o)]); o.mkdir(parents=True)
    subprocess.run(['git', 'init', '-q', '-b', 'main'], cwd=o)
    (o / 'registrations').mkdir()
    subprocess.run(['cp', str(A / R.REG_REL), str(o / R.REG_REL)])
    (o / 'results.jsonl').write_text('')
    if link_data:
        (o / 'data').mkdir(); os.symlink(A / 'data' / 'r015', o / 'data' / 'r015')
    subprocess.run(['git', '-c', 'user.name=x', '-c', 'user.email=x@x', 'add', '.'], cwd=o)
    subprocess.run(['git', '-c', 'user.name=x', '-c', 'user.email=x@x', 'commit', '-q', '-m', 'o'], cwd=o)
    return o

attempt('M1a run(other) (no argument accepted)', lambda: R.run(T))
saved = (R.RESULTS, R.DATA, dict(R.HOOKS), R.guard)
R.HOOKS['commit'] = lambda msg, res: None
attempt('M1b run() with a mock commit hook', lambda: R.run())
R.HOOKS.clear(); R.HOOKS.update(saved[2])
R.guard = lambda: 'stub'
attempt('M1c run() with a stubbed guard', lambda: R.run())
attempt('M1d _run(real, stub guard)', lambda: R._run(A, lambda: 'stub'))
R.guard = saved[3]
attempt('M1e _run_in(real, real guard)', lambda: R._run_in(A, R.guard))
attempt('M1f record() into the real ledger without the gate', lambda: R.record({'registration': '015-x', 'mode': 'x'}, 'm'))
print('real ledger rows after M1a-f:', real_rows(), '| HEAD unchanged:', head0 == subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=A, capture_output=True, text=True).stdout.strip())

# N1 needs the real guard to pass in THIS process. Called from a script it does not: the module-origin check refuses
# (__main__ is a foreign file; r015_run's bytecode is not under the fresh prefix). Shown first, then bypassed the
# deliberate way: run this file through `python -I -S -c "exec(...)"` (no __main__ file) and null r015_run.__cached__.
if '__file__' in globals():
    attempt('N1 guard() from a foreign script', lambda: R.guard())
    sys.exit(0)
R.__cached__ = None
# N1a: the gate opens on a passed guard alone -- the real data are readable with no row anywhere.
commit = R.guard()
table = dict(R.HASH_ROW.findall(R.head_text(A, R.REG_REL)))
d, v = R.load_french(A / R.US_ZIP, table[R.US_ZIP])
print(f'N1a guard() then load_french(): {len(d)} rows of the guarded file read; real ledger rows: {real_rows()}')

# N1b: the private _run with the REAL guard, RESULTS/DATA pointed at a throwaway ledger whose data/r015 is a symlink
# to the real data tree: the outcome is computed from the real data and recorded OUTSIDE the real repository.
o = other_tree('n1b', link_data=True)
R.RESULTS, R.DATA = o / 'results.jsonl', o / 'data' / 'r015'
try:
    with contextlib.redirect_stdout(io.StringIO()):
        row, rep = R._run(o, R.guard)
    print(f'N1b _run(throwaway, REAL guard): decision {row["decision"]} from data hash {row["data_hashes"][R.US_ZIP][:12]} '
          f'(registered {table[R.US_ZIP][:12]}); rows in the throwaway ledger: {sum(1 for l in open(o / "results.jsonl"))}; '
          f'real ledger rows: {real_rows()}')
except Exception as e:
    print('N1b refused:', str(e).splitlines()[0][:150])
finally:
    R.RESULTS, R.DATA = saved[0], saved[1]
print('real --check afterwards:', subprocess.run([sys.executable, '-I', '-S', str(A / 'r015_run.py'), '--check'],
                                                 capture_output=True, text=True, cwd=A).stdout.strip()[:100])
