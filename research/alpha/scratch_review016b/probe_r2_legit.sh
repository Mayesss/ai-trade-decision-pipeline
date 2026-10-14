#!/bin/bash
# Reviewer B, 016 R2: legitimate gate-open stand-in (Revision 2 code; 015's rows in the ledger): fetch -> approval -> read
# once. Run under sandbox-exec with the network denied.
source "$(dirname "$0")/r2_lib.sh"
mk legit --gate-open; A=$S/legit/research/alpha
HARNESS=r2_fetch_harness.py.txt "$(dirname "$0")/probe_r1_legit.sh" "$A" "$S/legit.objs.pickle"
