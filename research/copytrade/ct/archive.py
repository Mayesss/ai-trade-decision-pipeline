"""Hydromancer Reservoir (s3://hydromancer-reservoir), requester-pays.

Every byte transferred is billed to the owner's AWS account, so this module
goes through the AWS CLI with the dedicated read-only `copytrade` profile
(policy: ListBucket + GetObject on this bucket only) and counts the bytes it
pulls. Credentials never enter this repo.

Layout, listed 2026-10-07: one Parquet object per day under
  by_dex/<dex>/fills/perp/{all,twap_fills,liquidations,adl,builder_fills}/date=YYYY-MM-DD/
  by_dex/<dex>/snapshots/perp/date=YYYY-MM-DD/

pyarrow is imported lazily (only `download_columns` needs it), so the
snapshot downloader still runs on the standard library.
"""
import json
import subprocess
import threading
from pathlib import Path

from .net import DATA

BUCKET = 'hydromancer-reservoir'
PROFILE = 'copytrade'
_AWS = ['--request-payer', 'requester', '--profile', PROFILE]

bytes_pulled = 0  # GetObject bytes this process; billed as transfer
_lock = threading.Lock()


def _count(n):
    global bytes_pulled
    with _lock:
        bytes_pulled += n
    return n


def _aws(*args):
    res = subprocess.run(['aws', *args, *_AWS], capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f'aws {" ".join(args[:3])}: {res.stderr.strip()}')
    return res.stdout


def day_keys(prefix, day):
    """Object keys under <prefix>/date=<day>/ (one LIST request)."""
    out = _aws('s3api', 'list-objects-v2', '--bucket', BUCKET, '--prefix', f'{prefix}/date={day}/')
    return [o['Key'] for o in json.loads(out or '{}').get('Contents', [])]


def list_prefix(prefix):
    """[(key, bytes)] for every object under prefix. LIST calls only (paginated)."""
    out = _aws('s3api', 'list-objects-v2', '--bucket', BUCKET, '--prefix', prefix,
               '--query', 'Contents[].[Key, Size]', '--output', 'json')
    return [(k, int(s)) for k, s in (json.loads(out) or [])]


def local_path(key):
    return DATA / 'archive' / key


def pruned_path(key):
    return DATA / 'pruned' / key


def download(key, expected_size):
    """Fetch one whole object to data/archive/<key>; skip if already complete."""
    dest = local_path(key)
    if dest.exists() and dest.stat().st_size == expected_size:
        return 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix('.part')
    _aws('s3api', 'get-object', '--bucket', BUCKET, '--key', key, str(tmp))
    got = _count(tmp.stat().st_size)
    if got != expected_size:
        raise RuntimeError(f'{key}: got {got} bytes, expected {expected_size}')
    tmp.replace(dest)
    return got


def size(key):
    return json.loads(_aws('s3api', 'head-object', '--bucket', BUCKET, '--key', key))['ContentLength']


def _get_range(key, spec, tmp):
    _aws('s3api', 'get-object', '--bucket', BUCKET, '--key', key, '--range', spec, str(tmp))
    data = tmp.read_bytes()
    tmp.unlink()
    _count(len(data))
    return data


def _scratch(key):
    path = DATA / 'scratch' / key.replace('/', '__')
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def footer_file(key, total=None, guess=1 << 16):
    """A local sparse stand-in for `key` holding only its Parquet footer.

    pyarrow reads metadata from the footer alone, so a file of the right
    length with zeros everywhere except the tail answers schema, row-group
    and column-size questions for a few KB of transfer instead of ~400 MB.
    Returns (path, object size, bytes transferred).
    """
    total = total or size(key)
    base = _scratch(key)
    tail = _get_range(key, f'bytes=-{guess}', base.with_suffix('.tail'))
    pulled = len(tail)
    if tail[-4:] != b'PAR1':
        raise RuntimeError(f'{key}: not a Parquet file')
    footer_len = int.from_bytes(tail[-8:-4], 'little')
    if footer_len + 8 > len(tail):
        tail = _get_range(key, f'bytes=-{footer_len + 8}', base.with_suffix('.tail'))
        pulled += len(tail)
    sparse = base.with_suffix('.parquet')
    with open(sparse, 'wb') as f:
        f.truncate(total)
        f.seek(total - len(tail))
        f.write(tail)
    return sparse, total, pulled


def chunk_ranges(metadata, columns):
    """Byte ranges [start, end) of the wanted columns' chunks, contiguous ones merged.

    A chunk starts at its dictionary page when it has one (that page precedes
    the data pages) and spans total_compressed_size bytes.
    """
    spans = []
    for g in range(metadata.num_row_groups):
        rg = metadata.row_group(g)
        for c in range(rg.num_columns):
            cc = rg.column(c)
            if cc.path_in_schema not in columns:
                continue
            start = cc.data_page_offset
            if cc.has_dictionary_page and cc.dictionary_page_offset:
                start = min(start, cc.dictionary_page_offset)
            spans.append((start, start + cc.total_compressed_size))
    merged = []
    for s, e in sorted(spans):
        if merged and s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    return merged


def plan_columns(key, columns, total=None):
    """(bytes the wanted columns would transfer, object size, footer bytes) — footer only."""
    import pyarrow.parquet as pq
    sparse, total, pulled = footer_file(key, total)
    try:
        md = pq.ParquetFile(sparse).metadata
        need = sum(e - s for s, e in chunk_ranges(md, set(columns)))
    finally:
        sparse.unlink()
    return need, total, pulled


def download_columns(key, columns, total=None):
    """Fetch only `columns` of one Parquet object into data/pruned/<key>.

    The footer, then each wanted column chunk by byte range, is written into a
    sparse local file at its original offset; pyarrow reads only the requested
    columns, so the zero-filled gaps are never touched. The result is rewritten
    as a compact file holding just those columns, and its row count is checked
    against the footer. Returns bytes transferred (0 if already done).
    """
    import pyarrow.parquet as pq
    dest = pruned_path(key)
    if dest.exists():
        return 0
    sparse, total, pulled = footer_file(key, total)
    try:
        md = pq.ParquetFile(sparse).metadata
        missing = set(columns) - set(md.schema.names)
        if missing:
            raise RuntimeError(f'{key}: columns not in file: {sorted(missing)}')
        with open(sparse, 'r+b') as f:
            for s, e in chunk_ranges(md, set(columns)):
                data = _get_range(key, f'bytes={s}-{e - 1}', sparse.with_suffix(f'.r{s}'))
                if len(data) != e - s:
                    raise RuntimeError(f'{key}: range {s}-{e} returned {len(data)} bytes')
                f.seek(s)
                f.write(data)
                pulled += len(data)
        table = pq.read_table(sparse, columns=list(columns))
        if table.num_rows != md.num_rows:
            raise RuntimeError(f'{key}: read {table.num_rows} rows, footer says {md.num_rows}')
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix('.part')
        pq.write_table(table, tmp, compression='zstd')
        tmp.replace(dest)
    finally:
        sparse.unlink(missing_ok=True)
    return pulled
