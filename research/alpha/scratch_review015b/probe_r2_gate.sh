#!/bin/bash
# Reviewer B, 015 Revision 2: the data gate is issued only in _run() after the started row is in HEAD of the real ledger.
set -u; export R015_REV=e2a01b8; source "$(dirname "$0")/r1_lib.sh"
mk gt; A=$S/gt/research/alpha; cd "$A"
$PY -c "
import sys, os, subprocess; sys.path.append('$A'); import r015_run as R
R.__cached__ = os.path.join(sys.pycache_prefix, 'forged.pyc')      # pass the new __cached__ check with a forged record
t = dict(R.HASH_ROW.findall(R.head_text(R.HERE, R.REG_REL)))
print('guard() ->', R.guard()[:12], '| _GATE', R._GATE)
try:
    R.load_french(R.HERE / R.US_ZIP, t[R.US_ZIP]); print('N1a: OPENED after guard() alone')
except R.Refused as e: print('N1a guard() then load_french():', str(e)[:90])
os.makedirs('$S/gt_other/data', exist_ok=True)
o = '$S/gt_other'; subprocess.run(['git','init','-q',o]); open(o+'/results.jsonl','w').close(); os.makedirs(o+'/registrations', exist_ok=True); open(o+'/'+R.REG_REL,'w').write(open(R.HERE/R.REG_REL).read())
subprocess.run(['git','-C',o,'add','.']); subprocess.run(['git','-C',o,'-c','user.name=x','-c','user.email=x@x','commit','-q','-m','o'])
os.path.lexists(o+'/data/r015') or os.symlink('$A/data/r015', o+'/data/r015')
R.RESULTS, R.DATA = R.Path(o)/'results.jsonl', R.Path(o)/'data'/'r015'
try:
    R._run(R.Path(o), R.guard); print('N1b: completed')
except Exception as e: print('N1b _run(throwaway ledger, real guard):', str(e).splitlines()[0][:90])
print('   throwaway ledger rows:', [l[:40] for l in open(o+'/results.jsonl')])
R._GATE = R._GUARD_PASSED; R.RESULTS = R.REAL_RESULTS
d, v = R.load_french(R.HERE / R.US_ZIP, t[R.US_ZIP]); print('in-process forgery R._GATE = R._GUARD_PASSED: opened,', len(d), 'rows (stated residual)')
print('real ledger rows:', sum(1 for l in open(R.REAL_RESULTS) if l.strip()))
" 2>&1 | grep -v '^Traceback'
