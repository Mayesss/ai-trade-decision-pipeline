#!/bin/bash
# Reviewer B, 016, M2: nothing enforces -I / -S, and the fetcher loads the guard module itself from __pycache__.
# SYNTHETIC stand-in only. $1 = stand-in research/alpha, $2 = forged-clock venv (probe_t_dategate.sh)
set -u
A=$1; V=$2; D=$(cd "$(dirname "$0")" && pwd)
cd "$A"
echo "== (v) checks sys.flags.isolated / no_site?"; grep -n "isolated\|no_site\|ENABLE_USER_SITE" r016_run.py r016_fetch.py || echo "   no: (v) never reads sys.flags.isolated or no_site"
echo "== without -I: the CLI runs, and (v) does not object (the PYTHONPATH case is in probe_t_dategate.out (4))"
python3 r016_run.py --check
echo "== the fetcher CLI: r016_fetch.py imports r016_run BEFORE r016_run sets sys.pycache_prefix"
python3 -I "$D/forge_pyc.py" "$A"
echo "-- python3 -I r016_fetch.py (the registered command; the forged guard stops before any network):"
python3 -I r016_fetch.py 2>&1 | tail -1 | cut -c1-240
echo "-- control: python3 -I r016_run.py --check (a script run as __main__ is compiled from source):"
python3 -I r016_run.py --check | head -1
echo "-- the (v) check does not list r016_run among the modules it verifies:"; grep -n "^LOADED" -A1 r016_run.py
rm -f __pycache__/r016_run.*.pyc
echo "== under -I, site-packages .pth files still run (probe_t_dategate.out (5)); sys.flags in the forged venv:"
"$V/bin/python" -I -c 'import sys; print("   isolated", sys.flags.isolated, "no_site", sys.flags.no_site, "| modules from site-packages:", sorted(m for m, x in sys.modules.items() if "site-packages" in str(getattr(x, "__file__", "") or "")))'
