"""Registration 016 — archive parsers. They take BYTES or TEXT, never a path: every file is opened by the runner's
gated Panel (or by the fetcher under its own gate), so no function here can read the real data tree.

Binance USDⓈ-M 1d klines CSV (with or without a header row):
  open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote, ignore
Binance USDⓈ-M fundingRate CSV: calc_time, funding_interval_hours, last_funding_rate.
"""
import csv
import hashlib
import io
import re
import zipfile

from ct16 import rule as R

SUMS_LINE = re.compile(r'^([0-9a-f]{64})  (\S.*)$')
MINUTE_MS = 60_000


def sha256_bytes(b):
    return hashlib.sha256(b).hexdigest()


def zip_text(blob):
    """The single CSV member of a Binance archive zip, as text. Refuses anything else."""
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        names = [n for n in z.namelist() if not n.endswith('/')]
        R.refuse_unless(len(names) == 1 and names[0].endswith('.csv'), f'unexpected zip members {names[:3]}')
        return z.read(names[0]).decode('utf-8')


def parse_klines(text, not_before=None, before=None):
    """{day: (close, quote_volume, count)} from a 1d klines CSV; rows outside [not_before, before) are dropped.
    A malformed data row is a refusal (never silently skipped); a header row is recognised by its first field."""
    out = {}
    for r in csv.reader(io.StringIO(text)):
        if not r or not r[0].strip():
            continue
        if not r[0].strip().isdigit():
            R.refuse_unless(r[0].strip() == 'open_time', f'unexpected klines header {r[:3]}')
            continue
        R.refuse_unless(len(r) >= 9, f'short klines row {r[:3]}')
        d = R.ms_to_day(r[0])
        if (not_before is not None and d < not_before) or (before is not None and d >= before):
            continue
        close, qv, cnt = float(r[4]), float(r[7]), int(float(r[8]))
        R.refuse_unless(close == close and qv == qv and qv >= 0 and cnt >= 0, f'bad klines values on day {d}')
        R.refuse_unless(d not in out, f'duplicate klines day {d}')
        out[d] = (close, qv, cnt)
    return out


def funding_day(calc_time_ms):
    """A settlement at time tau belongs to the day whose holding period (d 00:00, d+1 00:00] contains it (D6):
    tau rounded to the minute, minus 1 ms."""
    t = int(calc_time_ms)
    t = (t + MINUTE_MS // 2) // MINUTE_MS * MINUTE_MS
    return R.ms_to_day(t - 1)


def parse_funding(text, not_before=None, before=None):
    """{day: summed funding rate} from a fundingRate CSV (rows outside [not_before, before) dropped)."""
    out = {}
    seen = set()
    for r in csv.reader(io.StringIO(text)):
        if not r or not r[0].strip():
            continue
        if not r[0].strip().isdigit():
            R.refuse_unless(r[0].strip() == 'calc_time', f'unexpected funding header {r[:3]}')
            continue
        R.refuse_unless(len(r) >= 3, f'short funding row {r[:3]}')
        t = int(r[0])
        R.refuse_unless(t not in seen, f'duplicate funding time {t}')
        seen.add(t)
        d = funding_day(t)
        if (not_before is not None and d < not_before) or (before is not None and d >= before):
            continue
        rate = float(r[-1])
        R.refuse_unless(rate == rate and abs(rate) < 1.0, f'bad funding rate at {t}')
        out[d] = out.get(d, 0.0) + rate
    return out


def parse_sums(text):
    """{relative path: sha256} from a SHA256SUMS file ("<hex>  <path>"); a malformed or duplicate line refuses."""
    out = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        m = SUMS_LINE.match(line)
        R.refuse_unless(m is not None, f'malformed SHA256SUMS line {line[:80]!r}')
        R.refuse_unless(m.group(2) not in out, f'duplicate SHA256SUMS path {m.group(2)}')
        out[m.group(2)] = m.group(1)
    return out


KLINE_NAME = re.compile(r'^klines/([A-Z0-9]+)/1d/\1-1d-(\d{4})-(\d{2})\.zip$')
FUNDING_NAME = re.compile(r'^fundingRate/([A-Z0-9]+)/\1-fundingRate-(\d{4})-(\d{2})\.zip$')


def file_month(rel):
    """(kind, symbol, 'YYYY-MM') of an archive path relative to its pull root, or None."""
    m = KLINE_NAME.match(rel)
    if m:
        return 'klines', m.group(1), f'{m.group(2)}-{m.group(3)}'
    m = FUNDING_NAME.match(rel)
    if m:
        return 'funding', m.group(1), f'{m.group(2)}-{m.group(3)}'
    return None
