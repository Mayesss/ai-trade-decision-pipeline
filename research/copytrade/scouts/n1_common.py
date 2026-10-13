"""N1 scout (xyz off-hours): shared session calendar, coin classes, guarded reads.

Behaviour only. Every fill read goes through `fills_view`, which globs only
partitions dated < 2026-07-01 and filters timestamps < 2026-07-01 00:00 UTC.

Sessions (all DST-aware via DuckDB ICU `timezone()`):

US single stocks / US-listed ETFs / IPOPs (America/New_York; xyz external
pricing is 24/5, Sun 20:00 ET -> Fri 20:00 ET, per docs as of 2026-01-15):
  REG  09:30-16:00 ET on an NYSE trading day (13:00 close on half days)
  XH   the rest of the 24/5 external week (pre, post, overnight) = "US overnight weekdays"
  WKD  Fri 20:00 ET -> Sun 20:00 ET   (xyz internal oracle)
  HOL  session-date (ET + 4 h) is an NYSE weekday holiday (internal oracle)
US equity indices XYZ100, SP500 (external 23/5 from CME index futures):
  REG  cash 09:30-16:00 ET;  XH other futures hours;  BRK 17:00-18:00 ET Mon-Thu;
  WKD  Fri 17:00 ET -> Sun 18:00 ET;  HOL session-date (ET + 6 h) an NYSE holiday
Commodities (CME Globex, Sun 18:00 -> Fri 17:00 ET, daily 17-18 break):
  REG  Globex open;  BRK daily break;  WKD Fri 17:00 -> Sun 18:00 ET;  HOL CME full closures
FX (Sun 17:00 -> Fri 17:00 ET):  REG / WKD (Fri 17:00 -> Sun 17:00 ET) / HOL (Dec 25, Jan 1)
Korea (Asia/Seoul):  REG 09:00-15:30 KST KRX day;  WKD Fri 20:00 -> Mon 08:45 KST;  HOL KRX holiday;  XH rest
Japan (Asia/Tokyo):  REG 09:00-11:30, 12:30-15:30 JST;  WKD Sat 06:00 -> Mon 08:20 JST;  HOL JPX holiday;  XH rest
Hong Kong (Asia/Hong_Kong):  REG 09:30-12:00, 13:00-16:00 HKT;  WKD Sat-Sun;  HOL;  XH rest
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FILLS_GLOB = str(ROOT / 'data/pruned/by_dex/xyz/fills/perp/all/date=*/fills.parquet')
OUT = ROOT / 'data/scout_n1/derived'
CUTOFF = '2026-07-01'

INDEX = {'XYZ100', 'SP500'}
COMMODITY = {'CL', 'BRENTOIL', 'GOLD', 'SILVER', 'COPPER', 'NATGAS', 'PLATINUM', 'PALLADIUM'}
FX = {'EUR', 'JPY', 'GBP'}
KOREA = {'SKHX', 'SMSN', 'HYUNDAI', 'KR200'}
JAPAN = {'JP225', 'KIOXIA', 'SOFTBANK'}
HK = {'ZHIPU', 'MINIMAX'}
IPOP = {'SPCX', 'QNT', 'CBRS'}  # pre-IPO perps during (part of) discovery: no underlying


def coin_class(sym):
    for name, s in (('index', INDEX), ('commodity', COMMODITY), ('fx', FX), ('korea', KOREA),
                    ('japan', JAPAN), ('hk', HK), ('ipop', IPOP)):
        if sym in s:
            return name
    return 'us_stock'


TZ = {'us_stock': 'America/New_York', 'ipop': 'America/New_York', 'index': 'America/New_York',
      'commodity': 'America/New_York', 'fx': 'America/New_York', 'korea': 'Asia/Seoul',
      'japan': 'Asia/Tokyo', 'hk': 'Asia/Hong_Kong'}

NYSE_HOL = ['2025-11-27', '2025-12-25', '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03',
            '2026-05-25', '2026-06-19']
NYSE_HALF = ['2025-11-28', '2025-12-24']
CME_HOL = ['2025-11-27', '2025-12-25', '2026-01-01', '2026-04-03']
FX_HOL = ['2025-12-25', '2026-01-01']
KRX_HOL = ['2025-10-03', '2025-10-06', '2025-10-07', '2025-10-08', '2025-10-09', '2025-12-25',
           '2025-12-31', '2026-01-01', '2026-02-16', '2026-02-17', '2026-02-18', '2026-03-02',
           '2026-05-01', '2026-05-05', '2026-05-25', '2026-06-03']
JPX_HOL = ['2025-10-13', '2025-11-03', '2025-11-24', '2025-12-31', '2026-01-01', '2026-01-02',
           '2026-01-12', '2026-02-11', '2026-02-23', '2026-03-20', '2026-04-29', '2026-05-04',
           '2026-05-05', '2026-05-06']
HK_HOL = ['2026-06-19']


def setup(con):
    con.execute("SET TimeZone='UTC'")
    con.execute(f"SET temp_directory='{ROOT / 'data/duckdb_tmp'}'")
    rows = []
    for cal, ds in (('nyse', NYSE_HOL), ('half', NYSE_HALF), ('cme', CME_HOL), ('fx', FX_HOL),
                    ('krx', KRX_HOL), ('jpx', JPX_HOL), ('hk', HK_HOL)):
        rows += [(cal, d) for d in ds]
    con.execute('CREATE OR REPLACE TEMP TABLE hol(cal VARCHAR, d DATE)')
    con.executemany('INSERT INTO hol VALUES (?, ?)', rows)


def fills_view(con, name='f', where=''):
    """Guarded view over xyz fills, < 2026-07-01 by partition AND by timestamp."""
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {name} AS
        SELECT * FROM read_parquet('{FILLS_GLOB}', hive_partitioning = true, union_by_name = true)
        WHERE date < DATE '{CUTOFF}' AND timestamp < TIMESTAMPTZ '{CUTOFF} 00:00:00+00'
        {('AND ' + where) if where else ''}
    """)


def session_sql(ts='timestamp', cls='cls'):
    """SQL CASE giving the session label for timestamp column `ts` and class column `cls`."""
    def H(cal, d):
        return f"EXISTS (SELECT 1 FROM hol WHERE hol.cal = '{cal}' AND hol.d = {d})"
    ny = f"timezone('America/New_York', {ts})"
    tod = lambda L: f"(hour({L}) * 60 + minute({L}))"  # noqa: E731
    dow = lambda L: f"dayofweek({L})"                    # noqa: E731  0 = Sun
    us_sd = f"CAST({ny} + INTERVAL 4 HOUR AS DATE)"
    ix_sd = f"CAST({ny} + INTERVAL 6 HOUR AS DATE)"
    fx_sd = f"CAST({ny} + INTERVAL 7 HOUR AS DATE)"
    close = f"(CASE WHEN {H('half', f'CAST({ny} AS DATE)')} THEN 780 ELSE 960 END)"
    us = f"""CASE WHEN dayofweek({us_sd}) IN (0, 6) THEN 'WKD'
                 WHEN {H('nyse', us_sd)} THEN 'HOL'
                 WHEN {tod(ny)} >= 570 AND {tod(ny)} < {close} THEN 'REG'
                 ELSE 'XH' END"""
    wk_ix = f"(({dow(ny)} = 5 AND {tod(ny)} >= 1020) OR {dow(ny)} = 6 OR ({dow(ny)} = 0 AND {tod(ny)} < 1080))"
    ix = f"""CASE WHEN {wk_ix} THEN 'WKD'
                 WHEN {H('nyse', ix_sd)} THEN 'HOL'
                 WHEN {tod(ny)} >= 1020 AND {tod(ny)} < 1080 THEN 'BRK'
                 WHEN {tod(ny)} >= 570 AND {tod(ny)} < {close} THEN 'REG'
                 ELSE 'XH' END"""
    cm = f"""CASE WHEN {wk_ix} THEN 'WKD'
                 WHEN {H('cme', ix_sd)} THEN 'HOL'
                 WHEN {tod(ny)} >= 1020 AND {tod(ny)} < 1080 THEN 'BRK'
                 ELSE 'REG' END"""
    fx = f"""CASE WHEN (({dow(ny)} = 5 AND {tod(ny)} >= 1020) OR {dow(ny)} = 6 OR ({dow(ny)} = 0 AND {tod(ny)} < 1020)) THEN 'WKD'
                 WHEN {H('fx', fx_sd)} THEN 'HOL'
                 ELSE 'REG' END"""
    kr = f"timezone('Asia/Seoul', {ts})"
    ko = f"""CASE WHEN {dow(kr)} IN (0, 6) OR ({dow(kr)} = 5 AND {tod(kr)} >= 1200) OR ({dow(kr)} = 1 AND {tod(kr)} < 525) THEN 'WKD'
                 WHEN {H('krx', f'CAST({kr} AS DATE)')} THEN 'HOL'
                 WHEN {tod(kr)} >= 540 AND {tod(kr)} < 930 THEN 'REG'
                 ELSE 'XH' END"""
    jp = f"timezone('Asia/Tokyo', {ts})"
    ja = f"""CASE WHEN ({dow(jp)} = 6 AND {tod(jp)} >= 360) OR {dow(jp)} = 0 OR ({dow(jp)} = 1 AND {tod(jp)} < 500) THEN 'WKD'
                 WHEN {H('jpx', f'CAST({jp} AS DATE)')} THEN 'HOL'
                 WHEN ({tod(jp)} >= 540 AND {tod(jp)} < 690) OR ({tod(jp)} >= 750 AND {tod(jp)} < 930) THEN 'REG'
                 ELSE 'XH' END"""
    hkt = f"timezone('Asia/Hong_Kong', {ts})"
    hk = f"""CASE WHEN {dow(hkt)} IN (0, 6) THEN 'WKD'
                 WHEN {H('hk', f'CAST({hkt} AS DATE)')} THEN 'HOL'
                 WHEN ({tod(hkt)} >= 570 AND {tod(hkt)} < 720) OR ({tod(hkt)} >= 780 AND {tod(hkt)} < 960) THEN 'REG'
                 ELSE 'XH' END"""
    return f"""CASE {cls}
        WHEN 'index' THEN ({ix}) WHEN 'commodity' THEN ({cm}) WHEN 'fx' THEN ({fx})
        WHEN 'korea' THEN ({ko}) WHEN 'japan' THEN ({ja}) WHEN 'hk' THEN ({hk})
        ELSE ({us}) END"""


def event_key_sql(ts='timestamp', cls='cls'):
    """An id for the closed-market episode a timestamp sits in (weekend id = the
    Monday-side session date), used only to COUNT distinct off-hours events."""
    ny = f"timezone('America/New_York', {ts})"
    return f"""CASE WHEN {cls} IN ('korea') THEN CAST(timezone('Asia/Seoul', {ts}) + INTERVAL 15 HOUR AS DATE)
                    WHEN {cls} IN ('japan') THEN CAST(timezone('Asia/Tokyo', {ts}) + INTERVAL 16 HOUR AS DATE)
                    WHEN {cls} IN ('hk') THEN CAST(timezone('Asia/Hong_Kong', {ts}) + INTERVAL 8 HOUR AS DATE)
                    ELSE CAST({ny} + INTERVAL 8 HOUR AS DATE) END"""


def register_classes(con, coins):
    con.execute('CREATE OR REPLACE TEMP TABLE cc(coin VARCHAR, sym VARCHAR, cls VARCHAR)')
    con.executemany('INSERT INTO cc VALUES (?, ?, ?)',
                    [(c, c.split(':', 1)[1], coin_class(c.split(':', 1)[1])) for c in coins])
