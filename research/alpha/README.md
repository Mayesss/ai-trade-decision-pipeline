# research/alpha — round 4 of the edge research (any market)

Programme document: `docs/alpha-discovery-2026-10-13.md`. Brief:
`docs/alpha-discovery-orchestrator-prompt-2026-10-13.md`.

- `scouts/` — behaviour-only scouts (`<id>-<topic>.md` plus `<id>_*.py`).
- `registrations/` — numbering continues from **015**; ledger exposures
  continue from **000p**. `reviews/` holds the independent reviews.
- `results.jsonl` — one row per guarded run, written by the runner.
- `data/` — gitignored; each pull in its own directory with `SHA256SUMS`.

One trial ledger spans this root and `research/copytrade/`: family counts
carry over, and every registration states them. `research/copytrade/ct/` is
reused read-only; a module whose hash appears in either `results.jsonl` is
never modified.
