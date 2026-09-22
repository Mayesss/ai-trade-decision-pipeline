# Neon Egress Guardrails

> Egress is not usually the expensive axis on this project — compute-hours are.
> See [neon-compute-cost.md](neon-compute-cost.md) for the 2026-09 budget
> warning and the scale-to-zero rules.

## Account Guardrail

Set a Neon organization spending limit and data-transfer alert from the Neon dashboard. This is intentionally not controlled by application code.

Recommended alert thresholds:

- 50 GB monthly data transfer: early warning.
- 90 GB monthly data transfer: free-tier limit warning.
- 120 GB monthly data transfer: incident threshold.

## Application Guardrails

- Swing reads project away bulky columns (e.g. `prompt_json`) and keep KV in front of the `swing.*` tables as a cache (see `lib/swing/pg.ts`).
- The scalp-era candle-history guardrails were removed with the scalp subsystem (2026-08); see git history for `SCALP_CANDLE_HISTORY_*` if ever needed again.
