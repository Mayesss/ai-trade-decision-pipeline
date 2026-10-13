#!/bin/bash
# Driver for probe_r1_m1.py (M1 recheck and N1).
set -u; source "$(dirname "$0")/r1_lib.sh"
mk m1
echo '### from a script'; $PY "$P/probe_r1_m1.py" "$S/m1/research/alpha" "$S/tmp_m1"
echo '### via -c (deliberate: no __main__ file, r015_run.__cached__ nulled)'
$PY -c "exec(open('$P/probe_r1_m1.py').read())" "$S/m1/research/alpha" "$S/tmp_m1" 2>&1 | grep -E '^N1|^real --check|Error|Refused'
