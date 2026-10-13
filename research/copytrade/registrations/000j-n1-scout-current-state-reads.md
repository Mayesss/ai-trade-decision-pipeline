# 000j — N1 scout: current-state docs and metadata, and one outcome-adjacent headline

Ledger entry, not a hypothesis. Written 2026-10-12 by the orchestrator, from
the N1 `xyz` scout's report (`research/copytrade/scouts/n1-xyz.md`).

## What happened

1. **Today's trade.xyz docs and Hyperliquid metadata** (fetched 2026-10-12)
   include post-cutoff listings and changelog entries, and example prices of
   unknown date (SKHX 886,000 KRW, SMSN 181,000 KRW, HYUNDAI 499,000 KRW,
   USD/KRW 1,444.10), probably from after 2026-07-01.
2. **A search result's title and summary** (Blockworks) claimed that weekend
   `xyz` prices carry price discovery. The article was not read.
3. **A date artefact, not an exposure.** A first listing printed a maximum
   date of 2026-07-01: a Berlin-time conversion of timestamps up to
   2026-06-30 23:59 UTC. The query was bounded below 2026-07-01 00:00 UTC;
   every later query ran in UTC.
4. The holdout snapshot partitions were never opened or listed.

## What was seen

Mechanics (oracle rules, fee modes, leverage, OI caps, price bounds and
their change dates), listing names, four example prices, one headline. No
`xyz` price series after 2026-06-30 was fetched or computed.

## What must not follow from it

- No N1 parameter may be set from the example prices or the headline. The
  headline is a public prior about the *sign* of a weekend effect; any N1
  rule states its sign from mechanism and says the prior exists.
- Mechanics with change dates (e.g. the oracle time constant 8 h → 1 h on
  2025-11-21 → 30 min on 2026-04-30; the price-bound resets from
  2026-03-13) may shape a split and the rule's mechanics, as documented
  facts, not as outcomes.

## Mitigation

An N1 registration states the docs' fetch date and treats the 2026-07 →
mechanics as known and the 2026-07 → prices as unseen.
