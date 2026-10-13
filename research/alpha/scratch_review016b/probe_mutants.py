"""Reviewer B, 016: are the guard/fetch tests vacuous? Each mutant deletes ONE protection in a throwaway clone of the
real repository (code only, no data), updates that file's row in the clone's hash table (so the real-file hash check
does not mask the result), and runs the 016 suite. NO mutant touches a network hook (an earlier version of this probe
removed `_require_real()` and the suite's own 'real hooks refuse' check then sent one GET for the key 'x' to
data.binance.vision: HTTP 404, no data; that mutant is deleted). 'CAUGHT' = the suite fails; 'VACUOUS' = it still passes.
    python3 -I probe_mutants.py <real repo> <scratch>"""
import subprocess, sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
REAL, S = Path(sys.argv[1]), Path(sys.argv[2])
M = {
 'panel_gate_removed': ('r016_run.py', "                if _GATE is None or _GATE[0] != str(HERE) or _GATE[1] != mode:\n                    raise Refused('the real 016 data open only after guard() passed for this mode in this process')\n", "                pass\n"),
 'panel_gate_HERE_term_removed': ('r016_run.py', "_GATE is None or _GATE[0] != str(HERE) or _GATE[1] != mode", "_GATE is None or _GATE[1] != mode"),
 'date_gate_removed': ('r016_run.py', "    if now.date() < R.READ_NOT_BEFORE:\n", "    if False:\n"),
 'fetch_mock_on_real_store_allowed': ('r016_fetch.py', "        if real_store:\n            pre.append('(B1) mock network hooks are never used on the real data tree')\n", ''),
 'fetch_root_check_removed': ('r016_fetch.py', "    if real_store and (root != run016.HERE or over):\n", "    if False:\n"),
 'panel_a_month_cap_removed': ('r016_run.py', "            if fm[2] > A_MONTH_LAST:\n                raise Refused(f'the scout pull holds a month after {A_MONTH_LAST}: {rel}')\n", ''),
 'panel_a_rows_cut_removed': ('r016_run.py', "D.zip_text(self._read_verified(self.a / rel, want)), before=R.day(R.SPAN_C[0]))", "D.zip_text(self._read_verified(self.a / rel, want)))"),
 'seam_check_removed': ('r016_fetch.py', "        if span[0] not in by_sym.get(sym, ()):\n", "        if False:\n"),
 'fetched_once_removed': ('r016_run.py', "        if fetched:\n            out.append('(f) the fetch has completed (a fetched row exists) — it runs once')\n", ''),
 'checksum_check_removed': ('r016_fetch.py', "    if got != want:\n        raise FetchError(f'{key}: checksum mismatch')\n", ''),
 'persistence_btc_exempt_removed': ('r016_fetch.py', "        if not key.startswith(PERSISTABLE) or key.startswith(f'klines_seam:{R.BTC}') or f':{R.BTC}' in key:", "        if not key.startswith(PERSISTABLE):"),
}
def one(name):
    f, old, new = M[name]
    c = S / f'mut_{name}'
    subprocess.run(['rm', '-rf', str(c)])
    subprocess.run(['git', 'clone', '-q', '--local', str(REAL), str(c)], check=True)
    A = c / 'research' / 'alpha'
    p = A / f
    t = p.read_text()
    if old not in t:
        return name, 'PATTERN NOT FOUND'
    import hashlib
    h0 = hashlib.sha256(p.read_bytes()).hexdigest()
    p.write_text(t.replace(old, new, 1))
    reg = A / 'registrations' / '016-crypto-trend-verbatim.md'      # keep the hash table in step: only the
    reg.write_text(reg.read_text().replace(h0, hashlib.sha256(p.read_bytes()).hexdigest()))  # targeted test can fail
    r = subprocess.run([sys.executable, '-I', 'tests/test_r016.py'], cwd=A, capture_output=True, text=True)
    fail = [l.strip() for l in r.stdout.splitlines() if l.strip().startswith('FAIL')]
    out = ('CAUGHT  ' + (fail[0][:110] if fail else (r.stderr.strip().splitlines() or ['?'])[-1][:110])) if r.returncode \
        else 'VACUOUS ' + r.stdout.strip().splitlines()[-1]
    subprocess.run(['rm', '-rf', str(c)])
    return name, out
with ThreadPoolExecutor(7) as ex:
    for name, out in ex.map(one, M):
        print(f'{name:34s} {out}', flush=True)
