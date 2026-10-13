"""Re-run the real-file checks AFTER the stale '(a) untracked' assertion (which fails now that 02fbd1d committed
the DRAFT). Reads only: registration text, hashes, results.jsonl 013 rows, parquet column names, events.json keys."""
import json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))
import r013_run as R
text = R.REG.read_text()
fails = R.guard_failures(HERE, 'registrations/013-new-perp-listings.md', mode='discovery', toplevel=R.REPO, anchor=R.ANCHOR, env_check=True)[0]
print('real guard failures today:', fails)
table = dict(R.HASH_ROW.findall(text))
files = (*R.GUARDED_CODE, *R.GUARDED_DATA)
print('missing from table', [f for f in files if f not in table])
print('wrong hash', [f for f in files if f in table and (not (HERE / f).exists() or R.sha256(HERE / f) != table[f])])
print('extra table rows not guarded', sorted(set(table) - set(files)))
print('013 rows anywhere', len(R.result_rows(HERE, 'results.jsonl')))
ev_keys = set()
for cal in R.PANEL_FILES:
    for e in json.loads((R.DATA_REAL / f'panel_{cal}' / 'events.json').read_text()):
        ev_keys |= set(e) | set(e.get('cov_pre_entry', {}))
print('events.json keys', sorted(ev_keys))
