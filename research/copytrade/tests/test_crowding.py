"""Registration 001 §4b: crowding features — SYNTHETIC opens only.

    .venv/bin/python -m tests.test_crowding
"""
import random

import duckdb

from ct import crowding

MINUTE, HOUR, DAY = 60_000, 3_600_000, 86_400_000


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def build(con, rows):
    con.execute('CREATE OR REPLACE TEMP TABLE opens(address VARCHAR, coin VARCHAR, dir VARCHAR, t BIGINT)')
    con.executemany('INSERT INTO opens VALUES (?, ?, ?, ?)', rows)


def main():
    print('crowding: shadower excess and repeat share (synthetic opens)')
    rng = random.Random(11)
    t0 = 1_767_225_600_000
    span = 30 * DAY
    coins = ['A', 'B', 'C']
    # background: 300 random wallets opening uniformly at random, both directions
    rows = []
    for i in range(300):
        for _ in range(40):
            rows.append((f'r{i}', rng.choice(coins), rng.choice('LS'), t0 + int(rng.random() * span)))
    # the leader: 12 opens, and 5 copiers that follow each within 5 minutes
    leader = []
    for k in range(12):
        t = t0 + DAY + int(rng.random() * (span - 2 * DAY))
        c, d = rng.choice(coins), rng.choice('LS')
        leader.append(('leader', c, d, t))
        rows.append(('leader', c, d, t))
        for j in range(5):
            rows.append((f'copier{j}', c, d, t + int(rng.random() * 5 * MINUTE)))
    # a second leader nobody copies
    lonely = [('lonely', rng.choice(coins), rng.choice('LS'), t0 + DAY + int(rng.random() * (span - 2 * DAY)))
              for _ in range(12)]
    rows += lonely
    con = duckdb.connect()
    build(con, rows)
    res = crowding.shadowers(con, leader + lonely)
    base_rate = 300 * 40 / (span / HOUR) / 6   # per (coin, dir, hour)
    e, l = res['leader'], res['lonely']
    check('copied leader: excess ~ 5 (base rate removed)', 4.0 <= e['excess_mean'] <= 6.0,
          f"excess {e['excess_mean']:.2f}, base ~{base_rate:.2f}/h")
    check('copied leader: repeat share high', e['repeat_share'] >= 0.5,
          f"{e['repeat_share']:.2f} over {e['shadowers']} shadowers")
    check('uncopied leader: excess ~ 0', abs(l['excess_mean']) <= 1.0, f"excess {l['excess_mean']:.2f}")
    check('uncopied leader: repeat share ~ 0', l['repeat_share'] <= 0.1, f"{l['repeat_share']:.2f}")
    check('n_opens counted', e['n_opens'] == 12 and l['n_opens'] == 12)
    # opens from fills: a flip counts as an open in the new direction; a reduce does not
    symtab = {'A': {'status': 'ok', 'symbol': 'AUSDT'}}
    fills = [{'coin': 'A', 'time': 1, 'side': 'B', 'sz': 1.0, 'startPosition': 0.0},   # open long
             {'coin': 'A', 'time': 2, 'side': 'A', 'sz': 0.5, 'startPosition': 1.0},   # reduce
             {'coin': 'A', 'time': 3, 'side': 'A', 'sz': 2.0, 'startPosition': 0.5},   # flip to short
             {'coin': 'A', 'time': 4, 'side': 'B', 'sz': 1.5, 'startPosition': -1.5}]  # close
    opens = crowding.leader_opens_from_fills('x', fills, symtab)
    check('leader opens from fills: open + flip only', [(d, t) for _, _, d, t in opens] == [('L', 1), ('S', 3)],
          str(opens))
    print('crowding checks passed')


if __name__ == '__main__':
    main()
