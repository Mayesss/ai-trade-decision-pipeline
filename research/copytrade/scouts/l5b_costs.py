"""L5b scout: cost and hurdle arithmetic for buying favourites on Kalshi.

Pure arithmetic on the published fee formula and on published returns
(Buergi, Deng & Whelan, "Makers and Takers", Jan 2026). No market data is read.

Run: python3 -I research/copytrade/scouts/l5b_costs.py
"""

import math

TAKER_K = 0.07     # Kalshi taker fee: ceil(0.07 * C * P * (1-P)) dollars per order
MAKER_K = 0.0175   # maker fee where charged (named series only): 25% of taker
RF = 0.04          # risk-free, annual; Kalshi APY is US-only, so idle collateral earns 0


def fee_table() -> None:
    print("## Fee per contract (large order, no rounding), by price P")
    print("P(c) | taker c | taker % of price | maker c | maker % of price | taker % of max profit")
    for p_c in (2, 5, 10, 90, 95, 98):
        p = p_c / 100
        t = TAKER_K * p * (1 - p)
        m = MAKER_K * p * (1 - p)
        print(f"{p_c:>3} | {t*100:.3f} | {t/p*100:.2f}% | {m*100:.3f} | {m/p*100:.2f}% | {t/(1-p)*100:.2f}%")
    # rounding trap: a 1-contract taker order pays a whole cent
    for p_c in (90, 95, 98):
        p = p_c / 100
        one = math.ceil(TAKER_K * 1 * p * (1 - p) * 100) / 100
        print(f"1-contract taker order at {p_c}c pays {one*100:.0f}c = {one/p*100:.2f}% of stake, "
              f"{one/(1-p)*100:.0f}% of max profit")


def hurdle() -> None:
    print("\n## Opportunity-cost hurdle per trade (simple interest at RF), % of stake")
    for days in (1, 5, 10, 30, 91, 182):
        h = RF * days / 365
        print(f"hold {days:>3} d: {h*100:.3f}% of stake = {h*95:.3f}c on a 95c favourite")
    print("\n## Published per-trade returns on the 90-99c bucket, annualised if held 91 days")
    for label, r in (("taker 91-99c (net of taker fee)", 0.005),
                     ("maker 91-99c (no maker fee then)", 0.018),
                     ("maker >=50c (paper text)", 0.026),
                     ("maker 91-99c, halved for 2025 decay", 0.009)):
        ann = (1 + r) ** 4 - 1
        print(f"{label:<40} per trade {r*100:.1f}% -> {ann*100:.1f}%/yr vs RF {RF*100:.0f}% "
              f"-> excess {(ann-RF)*100:+.1f}%/yr")


def diversification() -> None:
    print("\n## Independent trades needed so that the mean edge is 2 SE from zero")
    for p, edge in ((0.95, 0.005), (0.95, 0.009), (0.95, 0.018), (0.90, 0.018)):
        q = p * (1 + edge)  # implied win rate given the edge
        sd = math.sqrt(q * (1 - q)) / p
        n = (2 * sd / edge) ** 2
        print(f"P={p:.2f} edge {edge*100:.1f}%: per-trade SD {sd*100:.1f}% -> N ~ {n:,.0f}")


def turnover() -> None:
    print("\n## Annual return on book = edge x turns x utilisation (no interest on collateral)")
    for edge in (0.0, 0.005, 0.009):
        for days, util in ((5, 0.2), (5, 0.5), (10, 0.5), (91, 1.0)):
            r = edge * 365 / days * util
            print(f"edge {edge*100:.1f}% hold {days:>2} d util {util:.0%}: {r*100:5.1f}%/yr")


if __name__ == "__main__":
    fee_table()
    hurdle()
    diversification()
    turnover()
