"""Registration 001 §10 (revision 4): the T3 crowd is a named set — SYNTHETIC snapshot.

Shows on a hand-built snapshot that (1) the revision-3 "everyone else" crowd
gives a signal whose ranking is identical to the cohort's own tilt, and
(2) a bottom-quintile crowd recovers a planted top-vs-bottom disagreement.

    .venv/bin/python -m tests.test_crowd_sets
"""
import os
import random
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from ct import crowd, stats


def check(name, ok, detail=''):
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')
    if not ok:
        raise SystemExit(1)


def write_snapshot(rows, path):
    users, markets, sizes, notionals = zip(*rows)
    pq.write_table(pa.table({'user': list(users), 'market': list(markets), 'size': list(sizes),
                             'notional': list(notionals)}), path)


def main():
    print('T3 crowd as a named set (synthetic snapshot)')
    rng = random.Random(3)
    coins = [f'C{i}' for i in range(30)]
    top = [f'top{i}' for i in range(20)]
    bottom = [f'bot{i}' for i in range(20)]
    others = [f'o{i}' for i in range(400)]
    # planted: the top group leans long on even coins and short on odd ones;
    # the bottom group leans the opposite way; everyone else is random.
    lean = {c: (1 if i % 2 == 0 else -1) for i, c in enumerate(coins)}
    rows = []
    for u in top:
        for c in rng.sample(coins, 6):
            s = lean[c] * (1 if rng.random() < 0.8 else -1) * rng.uniform(1, 5)
            rows.append((u, c, s, abs(s) * 100))
    for u in bottom:
        for c in rng.sample(coins, 6):
            s = -lean[c] * (1 if rng.random() < 0.8 else -1) * rng.uniform(1, 5)
            rows.append((u, c, s, abs(s) * 100))
    for u in others:
        for c in rng.sample(coins, 3):
            s = rng.choice([-1, 1]) * rng.uniform(1, 50)
            rows.append((u, c, s, abs(s) * 100))
    # perp identity: make every coin's signed notional sum to zero with one
    # balancing counterparty per coin (the real snapshot has it to 1e-15)
    net = {}
    for u, c, s, n in rows:
        net[c] = net.get(c, 0.0) + (1 if s > 0 else -1) * n
    for c, v in net.items():
        if abs(v) > 0:
            rows.append((f'mm_{c}', c, -1.0 if v > 0 else 1.0, abs(v)))
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, 'snap.parquet')
        write_snapshot(rows, path)
        everyone = crowd.tilts(path, top, crowd=None)
        named = crowd.tilts(path, top, crowd=bottom)
        # cohort tilt alone, for the identity
        import duckdb
        con = duckdb.connect()
        con.execute('CREATE TEMP TABLE cohort(address VARCHAR)')
        con.executemany('INSERT INTO cohort VALUES (?)', [(a,) for a in top])
        own = dict(con.execute(f"""
            SELECT market, SUM(sign(size) * abs(notional)) / (SELECT SUM(abs(notional)) FROM read_parquet('{path}')
                   WHERE "user" IN (SELECT address FROM cohort))
            FROM read_parquet('{path}') WHERE "user" IN (SELECT address FROM cohort) GROUP BY market""").fetchall())
        common = sorted(set(everyone) & set(own))
        rho = stats.spearman([everyone[c] for c in common], [own[c] for c in common])
        check('"everyone else" crowd: signal ranking == cohort own tilt (Spearman 1)',
              rho is not None and abs(rho - 1) < 1e-9, f'rho = {rho:.6f} over {len(common)} coins')
        ratio = [everyone[c] / own[c] for c in common if abs(own[c]) > 1e-9]
        check('... and it is the cohort tilt times one per-day constant',
              max(ratio) - min(ratio) < 1e-9, f'constant = {ratio[0]:.4f}')
        agree = sum((named[c] > 0) == (lean[c] > 0) for c in named)
        check('bottom-quintile crowd: planted disagreement recovered',
              agree >= 0.85 * len(named), f'{agree}/{len(named)} coins with the planted sign')
        rho2 = stats.spearman([named[c] for c in common], [own[c] for c in common])
        check('named crowd adds information beyond the cohort tilt (rank corr < 1)',
              rho2 is not None and rho2 < 0.999, f'rho = {rho2:.3f}')
        b = crowd.breadth([(0, named), (1, {c: named[c] for c in list(named)[:4]})])
        check('breadth reported', b['days'] == 2 and b['days_under_5_coins'] == 1, str(b))
    print('crowd-set checks passed')


if __name__ == '__main__':
    main()
