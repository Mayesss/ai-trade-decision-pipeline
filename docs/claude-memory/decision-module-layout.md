---
name: decision-module-layout
description: "lib/ai.ts (3.1K lines) was split 2026-08-30 into lib/openAi.ts + five lib/swing/* modules; the dependency DAG is decisionConfig ← signals ← {prompt, decisionRules}"
metadata: 
  node_type: memory
  type: project
  originSessionId: 98cc81c3-03d1-49db-b887-3c55de1744d2
  modified: 2026-08-30T14:34:45.115Z
---

`lib/ai.ts` no longer exists. It was a 3,104-line grab-bag (transport + prompt +
post-processing + schemas + config) and was split in two commits:

1. `lib/openAi.ts` — OpenAI Responses transport (`callAIThread`), sibling of
   `lib/claudeAi.ts`; `lib/aiProvider.ts` switches. See [[ai-provider-dialects]].
2. Five domain modules under `lib/swing/`:
   - `decisionConfig.ts` (~360 ln) — ALL domain types + env-tunable thresholds +
     feature flags. LEAF: imports nothing from the decision stack.
   - `signals.ts` (~220 ln) — pure derivation: computeMomentumSignals,
     evaluateActionability, computeSignalStrength, resolveReentryCooldown,
     readIndicator.
   - `prompt.ts` (~1,490 ln) — computeSwingState + buildPrompt (the STATE/MARKET
     payloads and the system/user turns).
   - `decisionRules.ts` (~690 ln) — postprocessDecision + the 5 sanitizers (the
     enforcement half of what the prompt claims is "enforced in code").
   - `decisionSchema.ts` (~120 ln) — the two JSON schemas.

**Dependency DAG (no cycles):** decisionConfig ← signals ← {prompt, decisionRules};
decisionSchema is standalone.

**Dead code removed 2026-08-30** (3rd commit): `buildPrompt` (a self-described
backward-compatible wrapper superseded by computeSwingState) and
`lib/swing/evaluationStore.ts` entirely (persistEvaluation/getLastEvaluation were
redundant wrappers — callers use setEvaluation/getEvaluation from lib/utils
DIRECTLY, e.g. pages/api/evaluate.ts and pages/api/dashboard/evaluation.ts).

**Why the ATR/minute constants live in decisionConfig, not next to their users:**
the prompt DESCRIBES them to the model and the sanitizers ENFORCE them — one
definition stops the prose drifting from the rule.

**How it was verified as a pure move:** every one of the 62 symbol blocks from
HEAD:lib/ai.ts was matched verbatim in the new modules (57 byte-identical, 5 with
only an added `export` because they now cross a module boundary), and the full
suite passed with NO snapshot re-baselining.

**Tests mirror the source tree:** `test/unit/swing/<module>.<aspect>.test.ts`
(e.g. decisionRules.exchangeTpSl, prompt.situationalDoctrine, signals.actionability).
Renamed with git mv so history follows.

**AGENTS.md was stale and is now updated** — it had described `lib/ai.ts` with a
`buildPrompt`/`callAI` pair that no longer existed. Its Repo Map, Prompt-Change
Workflow and Debug Pattern now name the real modules. Keep it in sync: it is the
first thing an agent reads.
