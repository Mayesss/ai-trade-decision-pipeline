"""Hydromancer Reservoir (s3://hydromancer-reservoir), requester-pays.

Every byte transferred is billed to the owner's AWS account, so this module
goes through the AWS CLI with the dedicated read-only `copytrade` profile
(policy: ListBucket + GetObject on this bucket only) and tracks the bytes it
pulls. Credentials never enter this repo.

Layout, listed 2026-10-07: one Parquet object per day under
  by_dex/hyperliquid/fills/perp/{all,twap_fills,liquidations,adl,builder_fills}/date=YYYY-MM-DD/
  by_dex/hyperliquid/snapshots/perp/date=YYYY-MM-DD/
"""
import json
import subprocess
from pathlib import Path

from .net import DATA

BUCKET = 'hydromancer-reservoir'
PROFILE = 'copytrade'
_AWS = ['--request-payer', 'requester', '--profile', PROFILE]

bytes_pulled = 0  # GetObject bytes this process; billed as transfer


def _aws(*args):
    res = subprocess.run(['aws', *args, *_AWS], capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f'aws {" ".join(args[:3])}: {res.stderr.strip()}')
    return res.stdout


def day_keys(prefix, day):
    """Object keys under <prefix>/date=<day>/ (one LIST request)."""
    out = _aws('s3api', 'list-objects-v2', '--bucket', BUCKET, '--prefix', f'{prefix}/date={day}/')
    return [o['Key'] for o in json.loads(out or '{}').get('Contents', [])]


def size(key):
    return json.loads(_aws('s3api', 'head-object', '--bucket', BUCKET, '--key', key))['ContentLength']


def _get_range(key, spec, dest):
    global bytes_pulled
    _aws('s3api', 'get-object', '--bucket', BUCKET, '--key', key, '--range', spec, str(dest))
    bytes_pulled += dest.stat().st_size
    return dest.read_bytes()


def footer_file(key, guess=1 << 16):
    """A local sparse stand-in for `key` holding only its Parquet footer.

    pyarrow reads metadata from the footer alone, so a file of the right
    length with zeros everywhere except the tail answers schema, row-group
    and column-size questions for a few KB of transfer instead of ~400 MB.
    """
    total = size(key)
    tmp = DATA / 'footers' / (key.replace('/', '__') + '.tail')
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tail = _get_range(key, f'bytes=-{guess}', tmp)
    footer_len = int.from_bytes(tail[-8:-4], 'little')
    if tail[-4:] != b'PAR1':
        raise RuntimeError(f'{key}: not a Parquet file')
    if footer_len + 8 > len(tail):
        tail = _get_range(key, f'bytes=-{footer_len + 8}', tmp)
    sparse = tmp.with_suffix('.parquet')
    with open(sparse, 'wb') as f:
        f.truncate(total)
        f.seek(total - len(tail))
        f.write(tail)
    tmp.unlink()
    return Path(sparse), total
