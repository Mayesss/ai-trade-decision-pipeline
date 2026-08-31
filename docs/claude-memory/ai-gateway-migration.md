---
name: ai-gateway-migration
description: "AI Gateway migration coded+committed 2026-08-28 (commit 72895d6, unpushed); pending BYOK/credits dashboard setup before deploy"
metadata: 
  node_type: memory
  type: project
  originSessionId: 52254b3d-c385-40d4-8a31-d21bf2204bda
  modified: 2026-08-28T20:44:20.420Z
---

All AI calls route through the Vercel AI Gateway as of commit 72895d6 (2026-08-28, NOT pushed yet). Key facts:
- lib/ai.ts (OpenAI Responses API, store/previous_response_id thread chaining) → `https://ai-gateway.vercel.sh/v1/responses`, same wire format; gateway explicitly supports store+previous_response_id.
- lib/claudeAi.ts → official Anthropic SDK with `baseURL: https://ai-gateway.vercel.sh`; client cache keyed on the auth token (OIDC rotates).
- Model slugs (verified via /v1/models with OIDC token): `openai/gpt-5.6-sol`, `anthropic/claude-opus-4.8` (dots not hyphens) in lib/constants.ts.
- Auth: `resolveAiGatewayKey()` in lib/aiModel.ts — AI_GATEWAY_API_KEY else VERCEL_OIDC_TOKEN (auto in Vercel prod; locally `vercel env pull`). aiError classifies 402 as 'billing'.
- OPENAI_API_KEY / ANTHROPIC_API_KEY env vars are no longer read by code (still present in Vercel; removable after BYOK verified).

**Why:**  Smoke test hit `403 Free tier users do not have access to this model` — BYOK requires the paid tier (purchased AI Gateway credits).

**How to apply:** Before pushing/deploying commit 72895d6, the user must in the Vercel dashboard (team mayess-projects → AI Gateway): (1) buy a small credit top-up, (2) BYOK → OpenAI → Add key (their prepaid OpenAI key) → Enabled → Test Key, optionally same for Anthropic. Then verify locally: `node scripts/with-db-env.mjs node --import tsx scripts/gateway-smoke.ts` (OpenAI path incl. thread-memory check) and `node scripts/with-db-env.mjs node --import tsx scripts/claude-smoke.ts`. Deploying before that stops live trading decisions with a billing-kind AI-health latch. Later phase (user plan): once the OpenAI prepaid credit is used up, remove the BYOK key → billing continues through Vercel credits automatically. Note: removing BYOK makes stored OpenAI response chains (created under their key) inaccessible — code degrades gracefully (stateless retry + re-anchor). Related: [[vercel-neon-access]], [[forex-scalp-cleanup-plan]].

UPDATE 2026-08-28 (later): user bought gateway credits + added OpenAI BYOK (no Anthropic BYOK — key has no credits; Claude bills to Vercel credits). CRITICAL FINDING: the gateway's /v1/responses endpoint is STATELESS — it accepts store/previous_response_id but never replays conversations (gen_* ids, GET by id 404s). Fixed in 13cf1a9: the OpenAI path now chains via client-side transcripts (plain {role,content} turns in swing.ai_threads.transcript, same contract as Claude's appendTurns). SwingThreadContext no longer has previousResponseId. Both paths smoke-verified through the gateway incl. transcript memory and Claude prompt-cache passthrough. Ready to push; OPENAI_API_KEY/ANTHROPIC_API_KEY env vars in Vercel are unused by code and can be deleted after a healthy deploy.

FINAL 2026-08-28: gateway build (13cf1a9) pushed + deployed (Ready); OPENAI_API_KEY and ANTHROPIC_API_KEY deleted from Vercel env (all environments). Remaining user plan item: when the prepaid OpenAI credit is used up, remove the OpenAI BYOK key in the AI Gateway dashboard (no code change). Linting/test setup deliberately deferred to a future session (suggest eslint-config-next + typescript-eslint strict; classic airbnb config is unmaintained).

OUTAGE 2026-08-28 22:00 (investigated, root cause found, fix NOT yet applied): the assumption "gateway auth runs on VERCEL_OIDC_TOKEN in prod" was FALSE — Vercel does NOT inject VERCEL_OIDC_TOKEN into function runtime for this project (OIDC federation not enabled in project settings), and production has no AI_GATEWAY_API_KEY. Every real AI call since the gateway deploy (16:41) fails in resolveAiGatewayKey with the 'config'-kind AiCallError "Missing AI Gateway auth". It looked healthy 16:41→22:00 only because every tick was gate-skipped (no AI call attempted); 22:00 was the first 4H-close cycle with AI-worthy ticks. RESOLVED same evening (~22:45): user set AI_GATEWAY_API_KEY in Production and redeployed. Verified end-to-end: /api/debug-env-values shows the key present at runtime (VERCEL_OIDC_TOKEN still ❌ — never rely on OIDC for this project), a forced /api/evaluate?symbol=BTCUSDT&limit=5 made a real gateway AI call from the prod function (200, evaluation returned), and the KV ai-health latch cleared (degraded=false). /api/evaluate with small limit + admin header is the go-to probe for forcing a prod AI call outside 4H-close cycles. Prior context: OpenAI direct-API credits ran out 2026-08-27 18:00 (429 no-credits errors until the migration deploy), so the old OPENAI_API_KEY account is dry — the BYOK OpenAI key in the gateway is the prepaid one, distinct issue.
