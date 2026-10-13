#!/bin/bash
# Reviewer B, 016, M2 (second leg): the shadow scan accepts any TRACKED, clean top-level file. sys.path[0] is
# research/alpha, inserted BEFORE ct16 is imported, so a committed top-level module named like a stdlib module that the
# decision path first imports after that insert replaces it. Over 15 months other studies will add top-level files.
# SYNTHETIC stand-in only. $1 origin, $2 scratch, $3 clock venv
set -u
O=$1; S=$2; G="git -c user.name=rb -c user.email=rb@rb"
R=$S/repo_shadow; rm -rf "$R"; git clone -q --local "$O" "$R"; cp -R "$O/research/alpha/data" "$R/research/alpha/data"; cd "$R/research/alpha"; git checkout -q alpha-discovery
echo "== stdlib modules first imported AFTER r016_run puts research/alpha on sys.path[0]:"
python3 -I -c "
import sys
before = set(sys.modules)
import argparse, datetime, hashlib, json, os, random, re, subprocess, tempfile, pathlib
mid = set(sys.modules)
sys.path.insert(0, '.')
import r016_run
print('  ', sorted(m for m in set(sys.modules) - mid if '.' not in m and not m.startswith(('ct16', 'r016', '_'))))"
cat > csv.py <<'PY'
# a committed top-level csv.py: the real csv, plus a marker (it could equally rewrite every parsed row)
import sys, importlib.util as u, sysconfig, os
_s = u.spec_from_file_location('_real_csv', os.path.join(sysconfig.get_paths()['stdlib'], 'csv.py'))
_m = u.module_from_spec(_s); _s.loader.exec_module(_m)
for _k, _v in _m.__dict__.items():
    if not _k.startswith('__'):
        globals()[_k] = _v
RB_SHADOW = True
PY
$G add csv.py; $G commit -q -m "a top-level csv.py committed by some other study"
echo "== --check with a COMMITTED research/alpha/csv.py (after the date, forged-clock venv):"; "$3/bin/python" -I r016_run.py --check 2>&1
python3 -I -c "import sys; sys.path.insert(0, '.'); import r016_run as R; import csv; print('   csv used by ct16.data:', R.D.csv.__file__.replace('$R', '<clone>'), '| marker', getattr(R.D.csv, 'RB_SHADOW', False), '| (v) loaded_from_source():', R.loaded_from_source(), '| shadow_modules:', R.shadow_modules(R.HERE))" 2>&1
