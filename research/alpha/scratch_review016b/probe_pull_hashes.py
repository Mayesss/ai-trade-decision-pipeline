"""Reviewer B, 016: HASHES ONLY. Every file listed in the pinned SHA256SUMS exists and matches; nothing else is in the
pull directory. No zip is opened.   python3 -I probe_pull_hashes.py <real research/alpha>"""
import hashlib, sys
from pathlib import Path
d = Path(sys.argv[1]).resolve() / 'data/scout_c1/bn_um_klines_1d_monthly'
want = {}
for line in (d / 'SHA256SUMS').read_text().splitlines():
    h, p = line.split('  ', 1); want[p] = h
on_disk = {str(p.relative_to(d)) for p in d.rglob('*') if p.is_file()} - {'SHA256SUMS'}
bad = [p for p, h in want.items() if not (d / p).is_file() or hashlib.sha256((d / p).read_bytes()).hexdigest() != h]
print('listed', len(want), '| on disk (excl. SHA256SUMS)', len(on_disk), '| listed but missing or mismatched', len(bad),
      bad[:3], '| on disk but unlisted', sorted(on_disk - set(want))[:5], len(on_disk - set(want)))
