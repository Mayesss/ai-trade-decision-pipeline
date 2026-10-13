# 000u — 016 review B recheck: one real listing request for span-(c) archive file names

Ledger entry, not a hypothesis. Written 2026-10-13 by the orchestrator, from
016 review B's Revision 1 recheck (`research/alpha/registrations/reviews/016-review-b.md`,
appendix; probes `research/alpha/scratch_review016b/probe_r1_*`).

## What happened

One probe ran without the network sandbox against a synthetic stand-in
whose date gate the reviewer had opened. The stand-in's guard passed, and
016's fetcher, as its first step, sent **one real request to Binance's public
archive listing BTCUSDT's monthly kline file names**. The response carried
file names for months in span (c) (2026-07 →) and the server date. **No
file was downloaded, no price or funding value was received, and nothing
was written.** Every later probe ran with the network denied.

## What was seen

The existence (names) of BTCUSDT monthly archive files for months on and
after 2026-07, which is public and implied by the calendar. No values.

## What must not follow from it

Nothing: file existence carries no outcome. 016 states this entry in its
disclosures; span (c) stays unread as data.

## Mitigation

Reviewer probes against fetchers run with the network denied by default;
016's tests already disable sockets. 016 Revision 2 also refuses any `-X`
option (review B recheck, R1-M1).
