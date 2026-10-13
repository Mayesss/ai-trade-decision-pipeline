#!/bin/bash
# Reviewer B, 016, the date gate (t): what moves it. Synthetic stand-in clone only ($1 = its research/alpha).
# (1) TZ, (2) an env var, (3) git commit dates, (4) PYTHONPATH without -I, (5) a .pth in the interpreter's
# site-packages UNDER -I (a throwaway venv here; the procedure's interpreter is research/copytrade/.venv, git-ignored).
set -u
A=$1; S=$2; D=$(cd "$(dirname "$0")" && pwd)
cd "$A"
echo "== baseline: python3 -I r016_run.py --check"; python3 -I r016_run.py --check
echo "== (1) TZ=Pacific/Kiritimati (UTC+14) and TZ=Etc/GMT-14:"; TZ=Pacific/Kiritimati python3 -I r016_run.py --check | head -1; TZ=Etc/GMT-14 python3 -I r016_run.py --check | head -1
echo "== (2) environment variables read by the guarded code (grep os.environ / getenv):"; grep -n "os.environ\|getenv" r016_run.py r016_fetch.py ct16/*.py
echo "== (3) git dates: a commit dated 2028 at HEAD (GIT_COMMITTER_DATE) does not move (t):"
git -c user.name=rb -c user.email=rb@rb commit -q --allow-empty -m "dated 2028" --date=2028-02-01T00:00:00Z 2>/dev/null; GIT_COMMITTER_DATE=2028-02-01T00:00:00Z git -c user.name=rb -c user.email=rb@rb commit -q --amend --no-edit --allow-empty
git log -1 --format='   HEAD author %aI committer %cI'; python3 -I r016_run.py --check | head -1; git reset -q --hard HEAD~1
echo "== (4) PYTHONPATH=<dir with a forged datetime.py>, WITHOUT -I:"
E=$S/evil_pp; rm -rf "$E"; mkdir -p "$E"
# datetime.py that loads the real module from the stdlib, then fakes now()
cat > "$E/datetime.py" <<'PY'
import importlib.util as _u, sys as _s, sysconfig as _c, os as _o
_spec = _u.spec_from_file_location('datetime', _o.path.join(_c.get_paths()['stdlib'], 'datetime.py'))
_m = _u.module_from_spec(_spec); _s.modules['datetime'] = _m; _spec.loader.exec_module(_m)
exec(open(_o.environ['RB_FAKECLOCK']).read().replace('import datetime as _d', '_d = _m'))
globals().update(_m.__dict__)
PY
RB_FAKECLOCK=$D/fakeclock_module.py.txt PYTHONPATH=$E python3 r016_run.py --check
echo "   control, same PYTHONPATH WITH -I:"; RB_FAKECLOCK=$D/fakeclock_module.py.txt PYTHONPATH=$E python3 -I r016_run.py --check | head -1
echo "== (5) a .pth in the interpreter's site-packages, WITH -I (throwaway venv, same python 3.14.8):"
V=$S/venv_clock; rm -rf "$V"; python3 -m venv "$V"
SITE=$("$V/bin/python" -I -c 'import site; print(site.getsitepackages()[0])')
cp "$D/fakeclock_module.py.txt" "$SITE/_rb016clock.py"; echo 'import _rb016clock' > "$SITE/zz_rb016.pth"
"$V/bin/python" -I -c 'import sys, datetime; print("   isolated", sys.flags.isolated, "no_site", sys.flags.no_site, "now", datetime.datetime.now(datetime.UTC))'
"$V/bin/python" -I r016_run.py --check
echo "   fetch-mode guard, exactly guard()'s arguments, same interpreter:"
"$V/bin/python" -I -c 'import sys; sys.path.insert(0, "."); import r016_run as R; print("  ", R.guard_failures(R.HERE, "fetch", toplevel=R.REPO, anchor=R.ANCHOR, env_check=True)[0])'
echo "   control, -I -S (no site):"; "$V/bin/python" -I -S r016_run.py --check | head -1
