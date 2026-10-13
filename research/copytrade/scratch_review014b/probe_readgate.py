"""The read path has no gate. An audit hook ABORTS at the first open() of any file under the REAL
data/r014/panel_* (so nothing is read): reaching that open proves no guard stood in front of it.
Nothing is computed; results.jsonl is redirected to a temp file."""
import sys, tempfile
from pathlib import Path
HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
REAL = str((HERE / 'data' / 'r014').resolve())

class Reached(Exception):
    pass

ARMED = [True]
def hook(ev, args):
    if ARMED[0] and ev == 'open' and args and isinstance(args[0], (str, bytes)) and __import__('os').path.realpath(str(args[0])).startswith(REAL + '/panel_'):
        raise Reached(__import__('os').path.realpath(str(args[0]))[len(REAL) + 1:] + ' (opened as ' + str(args[0])[-40:] + ')')
sys.addaudithook(hook)
import r014_run as R

def attempt(label, f):
    try:
        f(); print(label, '-> returned (unexpected)')
    except Reached as e:
        print(label, '-> REACHED a real panel file with no guard:', e)
    except SystemExit as e:
        print(label, '-> refused (SystemExit):', str(e)[:80])
    except PermissionError as e:
        print(label, '-> refused (PermissionError):', str(e)[:100])
    except Exception as e:
        print(label, '-> stopped before any panel read by', type(e).__name__, str(e)[:60])

def _sym():
    d = Path(tempfile.mkdtemp()) / 'panel_x'
    d.symlink_to(HERE / 'data' / 'r014' / 'panel_holdout')
    return d
attempt("Panel('discovery') from any importer", lambda: R.Panel('discovery'))
attempt("Panel('holdout', allow_holdout=True)", lambda: R.Panel('holdout', allow_holdout=True))
attempt("Panel('discovery', panel_dir=<real holdout dir renamed via symlink>)", lambda: R.Panel('discovery', panel_dir=_sym()))
# the e2e test's recipe with ONE omission (PANELS not redirected): stubbed guard, temp HERE/REG/RESULTS
tmp = Path(tempfile.mkdtemp())
R.HERE, R.REG, R.RESULTS = tmp, tmp / 'reg.md', tmp / 'results.jsonl'
R.guard = lambda mode: 'c0ffee'
attempt('e2e recipe minus the PANELS line: run_discovery()', R.run_discovery)
attempt('e2e recipe minus the PANELS line: run_holdout() with no discovery row', R.run_holdout)
import json
from ct import shortvol as sv
good = [0.012 + 0.02 * ((i * 7919) % 101 - 50) / 50 for i in range(143)]
res, dec = sv.evaluate_discovery({'T1': good, 'T2': good})
R.RESULTS.write_text(json.dumps({'registration': '014-short-vol-deribit.md', 'mode': 'discovery', 'results': res, 'decision': [dec[0], list(dec[1])],
    'weekly': {k: [[i, x] for i, x in enumerate(good)] for k in sv.FAMILY}}, default=str) + '\n')
R.REG = HERE / 'registrations' / '014-short-vol-deribit.md'   # name only: result_rows filters on REG.name
attempt('e2e recipe minus the PANELS line: run_holdout() with a synthetic discovery row in the temp results', R.run_holdout)
ARMED[0] = False
print('014 rows in the temp results.jsonl after the attempts (no started row => the panel open came first):', [__import__('json').loads(l).get('mode') for l in (tmp / 'results.jsonl').read_text().splitlines()])
