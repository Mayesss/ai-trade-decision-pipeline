---
name: forex-scalp-cleanup-plan
description: "Forex/scalp retirement COMPLETE 2026-08-28 including Neon DB drops (5.3GB -> 98MB); recovery branch pre-scalp-drop-2026-08-28"
metadata: 
  node_type: memory
  type: project
  originSessionId: 52254b3d-c385-40d4-8a31-d21bf2204bda
  modified: 2026-08-28T16:40:28.020Z
---

The forex/scalp retirement is COMPLETE including Phase 5 DB drops (2026-08-28): the user ran docs/drop-scalp-tables.sql in the Neon console — 26 scalp_* tables (~4.95 GB) dropped, DB now ~98 MB, swing schema intact (10 tables, 17,100 decisions). Recovery snapshot: Neon branch `pre-scalp-drop-2026-08-28` (br-winter-voice-ag37c56z) — suggest deleting it after a few weeks. Note: the auto-mode permission classifier blocks Claude from executing DROP statements against the prod DB even when asked; hand destructive SQL to the user (Neon console) instead.

Remaining/traps:
- Env var NAMES kept on purpose (`SCALP_PG_CONNECTION_STRING`, `SCALP_CAPITAL_LEVERAGE_*`, `SCALP_ORCHESTRATOR_BASE_URL`, `SCALP_SYMBOL_PIP_SIZE_MAP`, `FOREX_FACTORY_CALENDAR_URL`) — they are live in Vercel and power swing; renaming them is an explicit possible "aftermath" step the user deferred.
- Shared kernel extracted from scalp: `lib/db/` (Neon client, exports now `pgClient`/`isPgConfigured`), `lib/market/` (AssetCategory/market-hours used by lib/capital.ts leverage + tradeability), `lib/cronChaining.ts` (wake-watch).
- Characterization tests live in `lib/marketKernel.test.ts` and `lib/symbolRegistry.test.ts`; `npm run test:ts` now runs lib/*.test.ts + lib/swing/*.test.ts (259 tests).
- Next agreed work after this: Vercel AI Gateway migration (BYOK with OpenAI key until used up, then Vercel billing) touching lib/ai.ts, lib/claudeAi.ts, lib/aiProvider.ts, lib/aiModel.ts, lib/swing/aiHealth.ts; then ESLint setup (classic eslint-config-airbnb is unmaintained — suggest eslint-config-next + typescript-eslint strict, optionally eslint-config-airbnb-extended).
