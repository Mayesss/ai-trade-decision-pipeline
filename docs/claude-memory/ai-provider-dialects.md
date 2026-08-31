---
name: ai-provider-dialects
description: "lib/claudeAi.ts is NOT a pre-gateway leftover — it's the Anthropic Messages dialect client pointed at the same Vercel gateway; SWING_AI_PROVIDER picks it, and it ran 141 real prod decisions in July 2026"
metadata: 
  node_type: memory
  type: project
  originSessionId: 98cc81c3-03d1-49db-b887-3c55de1744d2
  modified: 2026-08-30T12:54:40.738Z
---

Both swing AI clients call the SAME Vercel AI Gateway; they differ only in which
API dialect they speak:
- `lib/ai.ts` → `AI_BASE_URL` = https://ai-gateway.vercel.sh/v1, OpenAI Responses
  (`POST /v1/responses`), raw fetch.
- `lib/claudeAi.ts` → `AI_GATEWAY_ANTHROPIC_BASE_URL` = https://ai-gateway.vercel.sh,
  Anthropic Messages (`POST /v1/messages`), official SDK with baseURL+auth swapped.

`resolveSwingAiProvider()` (SWING_AI_PROVIDER env, else inferred from
DEFAULT_AI_MODEL) picks one. DEFAULT_AI_MODEL = openai/gpt-5.6-sol,
FALLBACK_AI_MODEL = anthropic/claude-opus-4.8.

**Why it can't collapse into one client:** Anthropic Messages carries thinking
blocks that must be echoed back VERBATIM on the next turn of a thread, and
explicit `cache_control` breakpoints — neither maps onto the OpenAI Responses
shape. So the dialect split is real, not legacy.

**Evidence it is live code, not dead:** swing.decisions shows 141 decisions on
`claude-opus-4-8` (2026-07-19/20) alongside 548 on gpt-5.4 — it was genuinely
A/B'd. Any change touching the call path must cover BOTH dialects (see
test/contract/aiGateway.contract.test.ts).

Related: [[ai-gateway-migration]] (the migration unified WHERE calls go and the
credential, not how many dialects we speak), [[system-prompt-growth]].
