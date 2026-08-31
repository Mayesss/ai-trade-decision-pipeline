# Claude Code memory — machine transfer

Exported 2026-08-30 for a machine change. These are the 11 memory files plus
`MEMORY.md` (the index Claude loads each session), copied verbatim from this
machine's Claude memory directory. Byte sizes were verified against the
originals at export.

## Where they came from

```
C:\Users\<user>\.claude\projects\c--Users-MayessSammoud-Documents-git-ai-trade-decision-pipeline\memory\
```

## Restoring on the new machine

The directory name is derived from the repo's absolute path, so it only matches
if the repo sits at the same location. Check the path Claude reports for its
memory directory on the new machine first, then copy these files into it —
`MEMORY.md` included, since it is the index that makes the rest discoverable.

If the repo lands at a different path, the folder name changes accordingly
(the pattern is the absolute path with separators replaced by `-`). Copying into
the wrong folder silently yields no memory rather than an error.

## Contents

| file | what it holds |
| --- | --- |
| `MEMORY.md` | the index — one line per memory, loaded every session |
| `no-software-installs.md` | corporate-machine constraint (no winget/binaries/PATH) |
| `vercel-neon-access.md` | which CLIs are authenticated locally |
| `ai-gateway-migration.md` | gateway setup, incl. that `/v1/responses` is stateless |
| `ai-provider-dialects.md` | both provider dialects are live — cover both |
| `decision-module-layout.md` | where the decision code lives after the split |
| `forex-scalp-cleanup-plan.md` | the scalp removal and DB shrink |
| `test-and-lint-roadmap.md` | harness, contract nets, ESLint config decisions |
| `perplexity-and-ai-bouncer.md` | both features' prod status |
| `wake-retest-refusal-loop.md` | the 3.3% conversion finding |
| `reclaim-wake-roadmap.md` | reclaim-wake design and deploy |
| `system-prompt-growth.md` | prompt size history and the caching constraint |

## Note on staleness

Several of these predate the 2026-08-30 restructure and describe mechanisms that
changed that day — resting entries, `wakeAutoEntry`, the actionability wall
gates, `cooldown_wake_sustain_minutes`. `wake-retest-refusal-loop.md` in
particular records the 3.3% finding that motivated a mechanism which has since
been deleted; the finding is still true, the mechanism is gone.

They are exported as-is rather than edited, because the transfer should be a
faithful copy. See `docs/resting-entry-separation.md` and
`docs/flat-decision-structure.md` for what the system actually does now.
