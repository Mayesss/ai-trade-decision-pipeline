"""Reviewer B, 015, M1: run(root) -- the REAL guard passes on the runner's own repository, then the decision is
computed from whatever tree `root` names and recorded as THE 015 result in the runner's real results.jsonl.

    python3 -I probe_m1_run_root.py <REAL-standin research/alpha> <OTHER research/alpha>

Both trees are synthetic (make_synth_clone.py). REAL-standin plays the real repository (null data, effect 0);
OTHER holds different data (effect x2) and its own registration table. No monkeypatching: default hooks, real guard,
real RESULTS / DATA. Only the documented public parameter `root` is passed."""
import io, contextlib, json, subprocess, sys
from pathlib import Path
real, other = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()
sys.path.insert(0, str(real))
import r015_run as R
assert R.HERE == real
print('real_tree_failures() before:', R.real_tree_failures())
with contextlib.redirect_stdout(io.StringIO()) as out:
    row, rep = R.run(other)
print('decision recorded in the REAL-standin ledger:', row['decision'], ' t =', round(row['existence']['t'], 2))
head = R.head_lines(real)
print('REAL-standin HEAD:results.jsonl modes:', [json.loads(l)['mode'] for l in head])
print('row data_hashes == REAL-standin registration table?',
      all(row['data_hashes'][f] == R.reg_table(real)[f] for f in R.GUARDED_DATA))
print('row US file hash == the hash in the REAL-standin table?', row['data_hashes'][R.US_ZIP] == R.reg_table(real)[R.US_ZIP],
      '| actually read from', other / R.US_ZIP, R.sha256(other / R.US_ZIP)[:12], 'vs real', R.reg_table(real)[R.US_ZIP][:12])
