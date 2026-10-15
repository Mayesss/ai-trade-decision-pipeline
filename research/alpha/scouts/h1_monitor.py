"""H1 house sleeve: read-only weekly check of HLP's exit triggers and LLP's reopen condition.

Public keyless endpoints only; no account, no keys, no orders. Prints one line per check and
exits 1 if a hard HLP exit trigger fires. Usage:

    python3 -I -S research/alpha/scouts/h1_monitor.py

Thresholds are docs/house-sleeve-decision-2026-10-14.md §6 (HLP exits) and §7 (LLP reopen);
change them only by editing the memo first.
"""
import datetime as dt
import json
import sys
import time
import urllib.request

HL = 'https://api.hyperliquid.xyz/info'
LIGHTER = 'https://mainnet.zklighter.elliot.ai/api/v1'
HLP = '0xdfc24b077bc1425ad1dea75bcb6f8158e10df303'
LLP_INDEX = 281474976710654
DAY = 86_400

HLP_DD_EXIT = -0.10        # §6 rule 4: chained dPnL/AV index below its high
HLP_DAY_REVIEW = -0.02     # §6 review: a one-day loss of 2% of TVL (month window, ~daily points)
TVL_RUN_REVIEW = -0.50     # §6 review: TVL change over ~30 days
LLP_REOPEN_CARRY = 0.12    # §7: perp/backstop carry, annualised, per trailing quarter


def fetch(url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {'user-agent': 'research-h1', **({'content-type': 'application/json'} if data else {})}
    with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers), timeout=60) as r:
        time.sleep(1.0)
        payload = json.load(r)
    if not payload:
        sys.exit(f'empty response from {url}: cannot check, treat as a failed run')
    return payload


def line(tag, name, value, limit, fmt='{:+.2%}'):
    print(f'{tag:6s} {name:46s} {fmt.format(value)}  (limit {fmt.format(limit)})')


def hlp_checks():
    vd = fetch(HL, {'type': 'vaultDetails', 'vaultAddress': HLP})
    hard = False
    rows = [(float(a), float(b)) for (_, a), (_, b) in
            zip(dict(vd['portfolio'])['allTime']['accountValueHistory'], dict(vd['portfolio'])['allTime']['pnlHistory'])]
    idx, peak = 1.0, 1.0
    for (a0, p0), (_, p1) in zip(rows, rows[1:]):
        if a0 > 0:
            idx *= 1 + (p1 - p0) / a0
        peak = max(peak, idx)
    dd = idx / peak - 1
    hard |= dd < HLP_DD_EXIT
    line('EXIT' if dd < HLP_DD_EXIT else 'ok', 'HLP index drawdown from high (14-day grid)', dd, HLP_DD_EXIT)

    m = dict(vd['portfolio'])['month']
    av = [float(a) for _, a in m['accountValueHistory']]
    pn = [float(p) for _, p in m['pnlHistory']]
    worst = min((pn[i] - pn[i - 1]) / av[i - 1] for i in range(1, len(av)) if av[i - 1] > 0)
    line('REVIEW' if worst < HLP_DAY_REVIEW else 'ok', 'HLP worst ~daily P&L / TVL, last 30 days', worst, HLP_DAY_REVIEW)
    run = av[-1] / av[0] - 1
    line('REVIEW' if run < TVL_RUN_REVIEW else 'ok', 'HLP TVL change, ~30 days', run, TVL_RUN_REVIEW)
    open_ = vd['allowDeposits'] and not vd['isClosed']
    hard |= not open_
    print(f'{"ok" if open_ else "EXIT":6s} HLP open (deposits allowed, not closed)        {open_}')
    print(f'info   HLP TVL ${av[-1] / 1e6:.1f}M; 30-day P&L / TVL {(pn[-1] - pn[0]) / av[0]:+.2%}')
    return hard


def llp_reopen():
    now = int(time.time())
    pnl = fetch(f'{LIGHTER}/pnl?by=index&value={LLP_INDEX}&resolution=1d&start_timestamp={now - 100 * DAY}'
                f'&end_timestamp={now}&count_back=0&ignore_transfers=false')['pnl']
    pnl = sorted(pnl, key=lambda r: r['timestamp'])
    w = [r for r in pnl if r['timestamp'] >= now - 91 * DAY]
    nav = [r['inflow'] - r['outflow'] + r['trade_pnl'] + r['trade_spot_pnl'] for r in w]
    yrs = (w[-1]['timestamp'] - w[0]['timestamp']) / DAY / 365.25
    avg = sum(nav) / len(nav)
    perp = (w[-1]['trade_pnl'] - w[0]['trade_pnl']) / avg / yrs
    spot = (w[-1]['trade_spot_pnl'] - w[0]['trade_spot_pnl']) / avg / yrs
    line('REOPEN' if perp >= LLP_REOPEN_CARRY else 'closed', 'LLP perp/backstop carry, trailing quarter', perp,
         LLP_REOPEN_CARRY)
    print(f'info   LLP spot-book carry same quarter {spot:+.2%}; NAV ${nav[-1] / 1e6:.1f}M '
          '(reopen needs two consecutive quarters, memo §7)')


def main():
    print('as of', dt.datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M UTC'))
    hard = hlp_checks()
    llp_reopen()
    print('\nManual (no API): socialised losses, bridge/validator/escrow incidents, HLP mandate changes (memo §6).')
    sys.exit(1 if hard else 0)


if __name__ == '__main__':
    main()
