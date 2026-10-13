"""A partial mock dict falls back to the REAL network hooks (sentinels stand in for them here)."""
import sys, tempfile
from pathlib import Path
CT = Path('/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline/research/copytrade')
sys.path.insert(0, str(CT))
import tests.test_r008 as T
import r008_fetch as F
from ct import crowd008 as c8
called = []
def sentinel(name):
    def f(*a, **k):
        called.append(name)
        raise SystemExit(f'REAL hook {name} would have been called (requester-pays / network)')
    return f
F.HOOKS = {k: sentinel(k) for k in F.HOOKS}
with tempfile.TemporaryDirectory() as tmp:
    root = T.make_repo(tmp)
    data = root / 'data'
    fills = T.build_tree(data)
    mk = T.Market(T.SYMS + ['BTCUSDT'], F.PRICE_START, c8.ms(T.END) + 5 * T.D, seed=13)
    hk = T.Hooks(data, fills, mk).as_dict()
    del hk['s3_plan']                       # a probe/test that forgets one hook
    try:
        F.fetch(root, data, now_ms=T.NOW, hooks=hk, **T.GK)
    except SystemExit as e:
        print('PROBE', e)
print('PROBE real hooks reached:', called)
