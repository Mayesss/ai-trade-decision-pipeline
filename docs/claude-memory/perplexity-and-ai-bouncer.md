---
name: perplexity-and-ai-bouncer
description: "Perplexity fresh-sentiment block + ai-bouncer triage gate DEPLOYED; SWING_PERPLEXITY_ENABLED confirmed ON in prod (FRESH SENTIMENT block seen in decision 17574, 2026-08-30); digest thin for low-coverage assets like BGB"
metadata: 
  node_type: memory
  type: project
  originSessionId: 757712f6-2644-48bf-bec2-3a6b5d533703
  modified: 2026-08-30T11:02:27.898Z
---

Implemented 2026-08-29 (plan approved by user), both features dark behind default-OFF env flags:

- **Perplexity fresh sentiment** (`lib/swing/perplexity.ts`): `perplexity/sonar` via gateway `/chat/completions`, fetched in the post-gates `Promise.all` in analyze.ts, KV-cached 45min (`swing:perplexity:v1:*`), rendered as FRESH SENTIMENT block in the USER turn after LESSONS. Cadence-matched lookback: detailed last `SWING_PERPLEXITY_FRESH_HOURS` (default 6), ≤2 one-liners for older ≤24h. Flags: `SWING_PERPLEXITY_ENABLED/_MODEL/_TTL_SECONDS/_FRESH_HOURS`.
- **ai-bouncer** (`lib/swing/aiBouncer.ts`): `spacexai/grok-4.1-fast-reasoning` triage between flat_dedupe and the supersede sweep in analyze.ts. NEVER runs on open-position, wake, or swept-entry calls (user hard requirement). Skip stage `ai_bouncer` → teal timeline dot (`.timeline-dot-bouncer`) + "AI bouncer" badge. Verdict persisted on skip AND proceed (snapshot `aiBouncer`, tick_log `metrics.aiBouncer`) — omitted (not null) when off, so dark deploys leave rows byte-identical. Flags: `SWING_AI_BOUNCER_ENABLED/_MODEL`.
- User naming decision: "ai-bouncer", explicitly NOT "nano gate" (nano is a timeframe) and distinct from the hard "gates".

**Why:** grounded fresh sentiment only when the expensive call actually happens; bouncer may only skip work, never override hard gates (user principle).

**2026-08-30 follow-up (uncommitted at time of writing):** sonar digest v2 — fixed
aspect first line (`mood: … | catalyst: yes/no | flows: …`), citation markers banned,
KV key bumped `swing:perplexity:v1`→`v2`. Plus new `lib/swing/fearGreed.ts`:
alternative.me crypto Fear & Greed index → `market.fear_greed` {value,label,
updated_utc,daily_oldest_first (last 10 daily values, today last)}, crypto ticks
only, default-ON (kill switch
SWING_FEAR_GREED_ENABLED; pinned 'false' in test/harness/setup-env.ts because the
harness errors on unhandled hosts — tests enable explicitly with the fearGreedIndex
world). One system-prompt sentence added to the crypto asset note (all crypto
snapshots re-baselined).

**How to apply:** rollout still pending — commit, deploy, then `vercel env add SWING_PERPLEXITY_ENABLED production` = true; bouncer flag later after observing skip cohorts. Full suite green 2026-08-29 (309 tests, 3 new snapshots: flat-perplexity, flat-bouncer-skip + prior). Known risk noted in module headers: chat/completions compat for perplexity/spacexai ids through the gateway is assumed, unverified in prod; both fail open. Grok via gateway has NO live X/Twitter access (no web-search tag) — that idea was dropped for [[ai-gateway-migration]]-style Perplexity grounding instead.
