"""Read-only: the real guard in both modes, and an import audit (which repo modules a guard pass loads)."""
import sys
from pathlib import Path
HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))
import r014_run as R
for mode in ('discovery', 'holdout'):
    f, c = R.guard_failures(HERE, 'registrations/014-short-vol-deribit.md', mode=mode)
    print(mode, 'commit', c or None)
    for x in f:
        print('   ', x[:160])
mods = sorted(m for m, v in sys.modules.items() if getattr(v, '__file__', None) and str(HERE) in str(v.__file__))
print('repo modules loaded:', mods)
print('stdlib loaded AFTER sys.path insert candidates:', [m for m in ('random', 'statistics') if m in sys.modules])
