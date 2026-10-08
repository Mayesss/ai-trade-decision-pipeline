"""Target builders: what the follower should hold, as signed weights.

A weight is follower notional / follower capital for one venue symbol. The
follower (replay.follow) turns weights into venue quantities at the execution
price, so a builder never sees venue units.

Every builder reads leaders through LeaderBook.exposures: a leader's notional
per symbol over their equity, divided by their typical leverage — about 1 in
gross when the leader runs at their usual risk. That normalization is what
makes a 20x leader and a 2x leader comparable (plan §5.5).

Products (plan §1):
- Copy([leader])       a single-leader copy (the P0b engine)
- Copy(leaders)        product A: K leaders, equal capital share, netted per coin
- Consensus(leaders)   product B: trade only where the selected leaders agree
PILOT parameters throughout; P4 freezes them.
"""
EPS = 1e-12


def hl_px(meta, venue_px):
    """Leader-side (Hyperliquid) price per HL unit, from the venue price.

    'direct': same instrument, units differ by qty_mult (kPEPE vs PEPEUSDT).
    'inverse': the venue quotes the reciprocal (HL xyz:JPY in USD vs Capital
    USDJPY in JPY); inverse_k absorbs any unit scaling and is measured when the
    mapping is built, like the direct price check.
    """
    if meta.get('quote', 'direct') == 'direct':
        return venue_px * meta['qty_mult']
    return meta['inverse_k'] / venue_px


class LeaderBook:
    """One leader's positions over time, replayed from events (replay.leader_events).

    lag_ms: the follower sees the leader as of `lag_ms` ago — positions AND
    equity. 0 is a normal copy; registration 001 uses 1 h and 24 h (and a
    decay curve) to test whether an edge survives delay.
    """

    def __init__(self, events, equity_at, typical_leverage, lag_ms=0):
        self.events = events
        self.equity_at = equity_at
        self.typical_leverage = typical_leverage
        self.lag_ms = lag_ms
        self.pos = {}
        self._i = 0
        self.meta = {ev['symbol']: {'qty_mult': ev['qty_mult'], 'min_usdt': ev['min_usdt'],
                                    'size_step': ev['size_step'], 'min_qty': ev['min_qty'],
                                    'quote': ev.get('quote', 'direct'), 'inverse_k': ev.get('inverse_k')}
                     for ev in events}

    def change_times(self):
        """When the follower can first see each change: completion + lag."""
        return [ev['t_last'] + self.lag_ms for ev in self.events]

    def advance(self, t):
        """Apply every position change completed by t - lag (what a poll at t sees)."""
        while self._i < len(self.events) and self.events[self._i]['t_last'] <= t - self.lag_ms:
            ev = self.events[self._i]
            self.pos[ev['symbol']] = ev['pos_after']
            self._i += 1

    def symbols(self):
        return {s for s, q in self.pos.items() if abs(q) > EPS}

    def exposures(self, t, price):
        """{symbol: leader notional / equity / typical leverage}, signed for the venue.

        A long in an inverse-quoted HL instrument is a short in the venue's.
        No known equity (no snapshot yet) or no typical leverage: no target —
        the follower cannot size the copy, so it holds nothing for this leader.
        """
        equity = self.equity_at(t - self.lag_ms)
        out = {}
        if not equity or not self.typical_leverage:
            return out
        for s in self.symbols():
            px = price(s)
            if px is None:
                continue
            m = self.meta[s]
            sign = -1.0 if m['quote'] == 'inverse' else 1.0
            out[s] = sign * self.pos[s] * hl_px(m, px) / equity / self.typical_leverage
        return out


class _Builder:
    def __init__(self, leaders, target_leverage):
        self.leaders = leaders
        self.target_leverage = target_leverage
        self.meta = {}
        for leader in leaders:
            self.meta.update(leader.meta)

    def change_times(self):
        return sorted(t for leader in self.leaders for t in leader.change_times())

    def advance(self, t):
        for leader in self.leaders:
            leader.advance(t)

    def symbols(self):
        return set().union(*(leader.symbols() for leader in self.leaders))


class Copy(_Builder):
    """Copy K leaders with an equal capital share each (K = 1: single leader).

    weight = target_leverage x mean over leaders of their exposure. Linear, so
    opposite positions of two leaders net out before any order is sent.
    """

    def __init__(self, leaders, target_leverage=1.0):
        super().__init__(leaders, target_leverage)

    def weights(self, t, price):
        total = {}
        for leader in self.leaders:
            for s, e in leader.exposures(t, price).items():
                total[s] = total.get(s, 0.0) + e
        k = len(self.leaders)
        return {s: self.target_leverage * v / k for s, v in total.items()}


class Consensus(_Builder):
    """Trade only where the selected leaders agree.

    Each leader votes +1 / -1 on every symbol where their exposure is at least
    min_exposure (dust ignored); leader sizing is deliberately ignored, so one
    large wallet cannot carry the book. Net agreement s = votes / K. Symbols
    with |s| < threshold stay flat; the rest are weighted by s and scaled so
    gross never exceeds target_leverage.
    """

    def __init__(self, leaders, target_leverage=1.0, threshold=0.3, min_exposure=0.05):
        super().__init__(leaders, target_leverage)
        self.threshold = threshold
        self.min_exposure = min_exposure

    def weights(self, t, price):
        votes = {}
        for leader in self.leaders:
            for s, e in leader.exposures(t, price).items():
                if abs(e) >= self.min_exposure:
                    votes[s] = votes.get(s, 0) + (1 if e > 0 else -1)
        k = len(self.leaders)
        raw = {s: v / k for s, v in votes.items() if abs(v / k) >= self.threshold}
        gross = sum(abs(v) for v in raw.values())
        return {s: self.target_leverage * v / max(gross, 1.0) for s, v in raw.items()}
