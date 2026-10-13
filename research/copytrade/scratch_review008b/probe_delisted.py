"""A mapped symbol that Bitget delists after 2026-10-07 (absent from contracts; history-candles answers with
an error code): the fetch stops on it at every attempt and can never write its fetched row."""
import sys, tempfile
from pathlib import Path
CT = Path('/Users/mayess.sammoud/Documents/persogit/ai-trade-decision-pipeline/research/copytrade')
sys.path.insert(0, str(CT))
import tests.test_r008 as T
import r008_fetch as F
from ct import crowd008 as c8


class H(T.Hooks):
    def bg_contracts(self):
        return [s for s in super().bg_contracts() if s != 'C9USDT']      # delisted since 10-07

    def bg_candles(self, sym, start, end):
        if sym == 'C9USDT':
            raise F.FetchError('bitget C9USDT: 40034 Parameter symbol does not exist')
        return super().bg_candles(sym, start, end)


with tempfile.TemporaryDirectory() as tmp:
    root = T.make_repo(tmp)
    data = root / 'data'
    fills = T.build_tree(data)
    mk = T.Market(T.SYMS + ['BTCUSDT'], F.PRICE_START, c8.ms(T.END) + 5 * T.D, seed=13)
    for attempt in range(3):
        try:
            F.fetch(root, data, now_ms=T.NOW, hooks=H(data, fills, mk).as_dict(), **T.GK)
            print('PROBE attempt', attempt, 'completed')
        except F.FetchError as e:
            print('PROBE attempt', attempt, 'stopped:', str(e)[:70])
