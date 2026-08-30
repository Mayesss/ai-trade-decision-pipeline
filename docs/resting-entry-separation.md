# Resting entry orders — separating the tool from the strategy

Status: IMPLEMENTED 2026-08-30, except the Bitget stop-entry PLACEMENT (see
"Venue wiring"). Deviations from this draft, all deliberate:

- **No rollout flags and no A/B.** `RESTING_ENTRY_MODE` is the constant `'both'`,
  not an env read — the tools are the model's permanent surface, not a knob. The
  phased rollout below was collapsed into one change.
- **The doctrine flag was not built, it was deleted.** Rather than gating the
  prescriptive prose behind `SWING_RESTING_ENTRY_DOCTRINE`, the prose is gone.
  What replaced it states the selection asymmetry of the two instruments and
  ends with "Nothing in this prompt tells you which play that should be."
- **`strategy` was added** (`SWING_STRATEGIES` in decisionSchema): the model
  names its own play — breakout, pullback, level_bounce, range_fade, … — and
  code never branches on it. This is the instrumentation this doc asked for.
- **The actionability wall gates were removed too** (`lib/swing/signals.ts`).
  Not in the original draft; it belongs to the same principle. See below.

## Problem

`SWING_PULLBACK_LIMIT_ENABLED` gates two unrelated things with one boolean:

- **the tool** — the `entry_limit_price` field and `sanitizeEntryLimit`
  (lib/swing/decisionRules.ts:695)
- **the strategy** — the whole "when the SETUP is valid but the WAVE POSITION is bad…"
  doctrine block (lib/swing/prompt.ts:1349)

The post-mortem verdict that turned it off was *"resting limits filled at bare retests
lost money"*. That is a finding about **where** the model was told to rest them — which
came from the session-offense playbook — not about resting orders as a mechanism. The
response removed the hand along with the instruction, and the two cannot be re-enabled
independently.

Second, the hand is missing half its fingers. `sanitizeEntryLimit` hardcodes the
pullback side:

```ts
const distAtr = (dir * (price - raw)) / atr;   // positive = pullback side
if (distAtr <= 0) { notes.push('limit_wrong_side_entry_dropped'); ... }
```

A BUY resting *above* price — an ordinary stop-entry, the breakout tool — is rejected as
"wrong side". So of the four resting-entry quadrants, two are built-and-disabled and two
have never existed:

| Resting entry | Today |
| --- | --- |
| BUY below price (limit) | built, flag OFF |
| SELL above price (limit) | built, flag OFF |
| BUY above price (stop) | **no primitive** |
| SELL below price (stop) | **no primitive** |

The breakout entries in production are not orders. They are `HOLD` → wake band → market
fill on a later tick, and a wake band fires on a *cross* (`p >= above` / `p <= below`,
lib/swing/wakeWatch.ts:69). Every surviving entry route therefore triggers only where
price moves **through** a level. That is why the model looks like it only ever plays
breakouts: it is the only shape the remaining machinery can express.

## What already generalizes

The expensive plumbing is side-agnostic and needs no change:

- one-tick TTL sweep + fill-race handling (`sweepPendingEntries` /
  `pendingEntryFilledMidTick`, pages/api/analyze.ts:1006-1056)
- bracket anchoring at the resting price, not live price (analyze.ts:3047)
- `cancelled_pending_entry` feedback into the next prompt (prompt.ts:845)
- AI thread continuity across an unfilled rest (`endSwingAiThread` /
  `markSwingAiThreadInPosition`)
- dashboard `pendingEntry` flag

Only the sanitizer, the flag, the prompt prose, and the venue order-type wiring are
pullback-specific.

## Axis 1 — the tool

Two explicit fields rather than one signed price. A single `entry_rest_price` would be
self-describing from geometry, but then a sign error silently becomes the *opposite*
trade shape; today wrong-side is caught and dropped, and that check is worth keeping.
Making the model name the tool preserves it — geometry then validates the claim.

```ts
entry_limit_price: { type: ['number', 'null'], minimum: 0 },  // rests AGAINST the trade
entry_stop_price:  { type: ['number', 'null'], minimum: 0 },  // rests WITH the trade
```

Mutually exclusive; both null = market. Both non-null → drop entry
(`resting_entry_ambiguous_entry_dropped`).

`sanitizeEntryLimit` becomes `sanitizeRestingEntry`, with the direction lock replaced by
a kind-parameterized distance. One sign convention covers all four quadrants:

```ts
export type RestingEntryKind = 'limit' | 'stop';

const dir  = action === 'BUY' ? 1 : -1;
const away = kind === 'limit' ? -1 : 1;      // limit rests against dir, stop with it
const distAtr = (dir * away * (raw - price)) / atr;   // must be > 0
// clamp target, same convention:
const clamped = price + dir * away * maxAtr * atr;
```

(BUY limit: `raw < price` → positive. BUY stop: `raw > price` → positive. SELL mirrors.
The clamp reduces to today's `price - dir * MAX * atr` for the limit case.)

Return shape gains the kind so downstream knows which venue order to place:

```ts
{ price: number | null; kind: RestingEntryKind | null; dropEntry: boolean; notes: string[] }
```

## Axis 2 — the risk envelope

Applies to any resting order regardless of thesis. Per-kind windows so they tune
independently — a stop-entry needs more clearance than a limit, since it is triggered by
the noise it sits in rather than filled by it:

```ts
export const RESTING_ENTRY_WINDOWS: Record<RestingEntryKind, { minAtr: number; maxAtr: number }> = {
    limit: { minAtr: ENTRY_LIMIT_MIN_ATR, maxAtr: ENTRY_LIMIT_MAX_ATR },  // 0.1 / 1.5 today
    stop:  { minAtr: ENTRY_STOP_MIN_ATR,  maxAtr: ENTRY_STOP_MAX_ATR  },  // new; start 0.25 / 1.5
};
```

Below min → drop (inside bar noise). Above max → clamp (fill odds inside the one-tick
TTL are negligible either way). No ATR → drop. All unchanged in spirit from today, just
no longer welded to one side.

The post-mortem's real lesson belongs here, not in the doctrine prose: **never rest into
a known venue event.** Currently that guardrail lives inside the session-offense block
(prompt.ts:1113) and disappears with it. It should be a code-side envelope check on
every resting order, not advice attached to one strategy.

## Axis 3 — the doctrine

Independent flag. A tool can exist without the prompt prescribing when to reach for it —
that separation is the entire point.

```ts
// off | limit | stop | both — which resting-entry tools exist at all.
export const RESTING_ENTRY_MODE = (() => {
    const raw = String(process.env.SWING_RESTING_ENTRY ?? '').trim().toLowerCase();
    return raw === 'limit' || raw === 'stop' || raw === 'both' ? raw : 'off';
})();

// Prescriptive strategy prose. Orthogonal to the above.
export const RESTING_ENTRY_DOCTRINE = flagOn(process.env.SWING_RESTING_ENTRY_DOCTRINE);
```

`SWING_RESTING_ENTRY` unset → `off` → a resting price of either kind drops the entry,
byte-identical to production today.

The prompt splits into three blocks where there is one:

1. **Mechanics** (whenever any kind is allowed) — what the fields do, the ATR windows,
   the one-tick TTL, that the bracket anchors at the resting price, that an invalid
   value drops the entry rather than falling back to market. No opinion about when.
2. **The tradeoff** (same condition) — one neutral paragraph stating the actual
   asymmetry: a limit is adversely selected (you are filled by whoever is willing to
   trade through your level), a stop is favourably selected but pays the spread and
   fills on sweeps. This is a fact about the instruments, not a strategy.
3. **Doctrine** (only under `RESTING_ENTRY_DOCTRINE`) — today's pullback prose, plus a
   symmetric breakout-stop prose. Off by default, so the model picks the shape from the
   data.

`ENTRY_LIMIT_MIN_ATR` etc. keep their names; the "pullback" wording in comments and in
`cancelled_pending_entry`'s prompt text becomes kind-neutral ("your previous resting
entry").

## Venue wiring

**Capital** — the working-order body already has the field, hardcoded (capital.ts:3128):

```ts
type: "LIMIT",   // → kind === 'stop' ? "STOP" : "LIMIT"
```

`/api/v1/workingorders` documents both types with `level` as the trigger. *Verify against
the live API before relying on it.* List and cancel
(`listCapitalPendingEntryOrders` / `cancelCapitalPendingEntryOrders`) read the same
endpoint and need no change — a STOP working order shows up in the same collection.

**Bitget** — this is the real work, and the safety-critical part. `place-order` cannot
express a stop entry; it needs `/api/v2/mix/order/place-plan-order` (`planType:
normal_plan`, `triggerPrice`), which lives in a **separate order book**. That means
`fetchPendingEntryOrders` and `cancelPendingEntryOrders` (lib/trading.ts:440-505) must
sweep *both* books — plain pending orders and pending plan orders
(`orders-plan-pending` / `cancel-plan-order`).

If the TTL sweep misses the plan book, a resting stop survives into the next tick and a
fresh entry stacks on top of it. That is exactly the DE40 double-fill of 2026-07-13 that
the fail-closed sweep exists to prevent. **The sweep must be extended before the
placement path, not after.**

## Instrumentation

To answer the question this whole change exists to answer — does the model pick fades or
breaks when it has both — record the chosen kind per decision: `kind` into the history
row alongside `entry_limit`, and into `tick_log.metrics_json`. Without it the A/B is
unmeasurable.

## The pre-AI gates (added after the draft)

`evaluateActionability` skips the AI call entirely (pages/api/analyze.ts ~2368).
Two of its rejections were wall-based: `into_context_wall` and
`bounce_into_context_wall`. The measurement behind them was real — 104 such
calls dropped, 0 opens lost — but it was taken while a market entry was the only
way in. The AI never acted on those setups because it *had no tool that fit*;
the gate then encoded that absence as a permanent fact about the market.

A wall is also the best-defined bounce location on the chart. Both rejections are
now `actionable: true` with the branch preserved in the reason
(`confirmed_primary_structure_into_context_wall`, `bounce_long_into_context_wall`,
`bounce_short_into_context_wall`), so the trail stays queryable and the
ai-bouncer — which runs after this and can still decline the expensive call —
receives the context. The matching prompt rule ("Do NOT open into a near opposite
level…") became location information rather than a prohibition.

Untouched, deliberately: `micro_entry_ok_false` and `boxed_or_unconfirmed` still
skip, and the signal-strength budget gate still stands. Those are not wall
rejections and were not part of this argument.

Expect flat AI-call volume to rise. `swing.tick_log` already records the
actionability branch on every skip, so the before/after is a SQL query.

## What shipped

| Piece | Where |
| --- | --- |
| `RestingEntryKind`, `RESTING_ENTRY_MODE`, per-kind windows, venue support | lib/swing/decisionConfig.ts |
| `sanitizeRestingEntry` (four quadrants, one sign convention) | lib/swing/decisionRules.ts |
| `entry_stop_price` + `strategy` on both schema variants | lib/swing/decisionSchema.ts |
| Neutral mechanics prose; prescriptive doctrine deleted | lib/swing/prompt.ts |
| Wall gates → reported, not rejected | lib/swing/signals.ts |
| Capital `type: LIMIT \| STOP` working order | lib/capital.ts |
| Bitget two-book TTL sweep + `place-plan-order` | lib/trading.ts |
| Sanitizer unit tests (quadrants, exclusivity, envelope, venue support) | test/unit/swing/decisionRules.restingEntry.test.ts |
| Wall case now reaches the model | test/contract/analyze/capital-flat-wall.contract.test.ts |

## The feedback loop

`strategy` is only bookkeeping unless it comes back. `market.recent_actions` now
carries `rested_at` / `rested_as` (which tool) and `strategy` alongside the
measured outcome, so across rows the model sees which of its own plays and entry
mechanics worked *on this instrument* — and the prose says explicitly that this
is data to read, not a rule to obey.

One trigger-anchoring bug fell out of adding stop entries and is fixed:
`sanitizeEntryTrigger` validated the failed-break level against LIVE price,
which is right for a market entry taken after a break (the level is behind you)
and wrong for a resting stop (the level is still ahead). It now validates
against the bracket anchor, so break-thesis stop entries keep their failed-break
protection instead of silently losing it.

## Remaining

1. ~~Validate Bitget `place-plan-order`.~~ **DONE 2026-08-30** — 20/20 on demo
   via `npm run validate:bitget:stop`
   ([scripts/validate-bitget-stop-entry.ts](../scripts/validate-bitget-stop-entry.ts)),
   including the opt-in trigger phase. `RESTING_ENTRY_VENUE_SUPPORT.bitget` is
   now `['limit', 'stop']`; both venues carry both tools and nothing about this
   work is conditional any more.

   **The run paid for itself.** `place-plan-order` ACCEPTS
   `presetStopLossPrice` / `presetStopSurplusPrice` — the `place-order` names,
   which is what the implementation used — and then silently discards them. No
   error, order rests normally, `orderId` returned. Only the plan book's own
   `stopLossTriggerPrice` / `stopSurplusTriggerPrice` (plus `*TriggerType`)
   stick. Shipping the docs-derived body would have opened **every triggered
   crypto stop entry naked**: leveraged, no stop-loss, nothing in any response
   to say so. It would have surfaced as a liquidation.

   Two lessons kept in the script: phase B reads *only* the plan-book names, so
   a fallback can never mask the bug again; and phase F re-anchors its trigger
   on each attempt rather than parking one above market and hoping price rises
   (two runs lost that bet and read as failures when the code was fine).
2. **Move the venue-event guardrail into the envelope.** "Never rest into a
   known venue event" still lives inside the session-offense prose
   (prompt.ts ~1113) and disappears when that flag is off. It is the one piece
   of the old doctrine that is a genuine risk rule rather than a strategy.
3. **Watch flat AI-call volume and the new actionability branches.** The wall
   branches are recorded in `swing.tick_log`; if the model simply HOLDs them all
   at higher cost, that is now a measurement rather than an assumption.
