---
name: system-prompt-growth
description: "Measured growth of the swing decision SYSTEM prompt: ~4.2K chars (June 2026) → ~13K (mid-July) → ~25.5K (Aug plateau) → 28.7K (Aug 30); wake/cooldown doctrine ≈ 40% of it"
metadata: 
  node_type: memory
  type: project
  originSessionId: 98cc81c3-03d1-49db-b887-3c55de1744d2
  modified: 2026-08-30T12:23:20.231Z
---

Measured from swing.decisions prompt_json (2026-08-30, daily avg of length(system)):
June ~4.2K chars → Jul 13 ~8.7K → Jul 20 ~16K → Jul 30 ~23K → Aug plateau ~25.5K →
Aug 30 28.7K (~7K tokens). Latest jump (+2.7K) = sustained-band EXECUTION CONTRACT +
ACT-OR-FOLD + reclaim-wake doctrine ([[reclaim-wake-roadmap]]).

Composition of the 28.7K (BGB row 17574): wake/cooldown lifecycle doctrine (flat
cooldown 3.9K + wake-band trigger 3.8K + reclaim 1.6K + in-position bands 1.2K +
position-wake 1.0K) ≈ 11.5K ≈ 40%. Rest: lessons policy 1.4K, event reaction 1.2K,
in-position discipline 1.1K, margin recycle 1.0K, geometry 1.0K, etc.

**Why:** every feature session appended always-on doctrine paragraphs (kept
unconditional partly to keep the cached system prefix byte-stable). Conditional
triggers (reclaim, failed-break, event reaction) are described on EVERY tick though
each fires rarely.

**RESOLVED 2026-08-30** in two passes (first committed by user, second in tree):
1. Flat vs in-position system-prompt variants; TASKS+JSON contract moved from the
   user turn into the system prompt (chained threads replayed it once per STORED
   turn); code-enforced clamps collected into one "Output hygiene" hard-constraint
   row; duplicate wake prose removed.
2. SITUATIONAL DOCTRINE section: every "how to read market.X" block is gated on
   its payload being present and rendered in ONE section at the TAIL (prefix
   caching — per-tick variation must not sit mid-prompt).

Result: flat 27.9K→16.0K chars, in-position 28.3K→13.3K, user turn 7.4K→5.0K.
A routine scan renders no situational section at all.

**Cache fact (matters for any future tailoring):** hits come from CROSS-SYMBOL
prefix sharing inside a cron sweep (many symbols, same asset class → byte-identical
system prompt), NOT from one symbol over time (4H apart, always cold). So per-tick
variation belongs in the tail; asset-class/state variation is fine anywhere.
Pre-gateway data showed 57% hit rate; post-gateway is unmeasured (cutover 2026-08-29).

3. Transcript compaction (2026-08-30, in tree): the LIVE user turn stays complete;
   only the ARCHIVED copy in swing.ai_threads.transcript is slimmed (4,713→2,774
   chars, -41%; vs the 7,362-char original stored turn that is -62%). Kept: bias/
   trend/structure/momentum.rsi/extension/atr_pct/location/levels/position + any
   trigger block. Dropped: geometry, candles, orderbook, volume_profile,
   recent_actions, calendars, news/sentiment/lessons (all re-fetched each tick).
   Seam: `userForTranscript` opt on callAIThread/callClaudeSwingDecision — each
   client sends `user` but archives the abbreviated turn at appendTurns[0].
   IMPORTANT: decisions.prompt_json still stores the FULL prompt (dashboard viewer
   + postmortem dossier depend on it) — only the conversation is compacted.
   The in-position prompt tells the model its earlier turns are abbreviated by
   design, so absence there is never read as a market change.
