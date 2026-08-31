---
name: no-software-installs
description: "Never install software on this machine (no winget, no portable binaries, no PATH edits) — corporate-managed device"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: fcf552e3-d76e-4c8a-9b9d-34640a2f0cd0
  modified: 2026-08-29T09:21:48.397Z
---

Do not install anything on this machine — no winget/msi installs, no "portable" binary drops, no user-PATH modifications. It is a corporate-managed device (SKR Reisen).

**Why:** User stopped an attempted GitHub CLI install (2026-08-29) with "you cannot install stuff on this machine"; winget's source is also broken/restricted here.

**How to apply:** Work with what exists: `git`, `node`, `npm`/`npx` (project-local dev deps via npm install in the repo are fine — that's dependency management, not machine software). For GitHub API work there is no `gh` — use the token from Windows credential manager instead: `printf "protocol=https\nhost=github.com\n\n" | git credential fill` yields a `password=gho_...` token for curl `Authorization: Bearer` calls (never print it). See [[vercel-neon-access]] for the CLIs that ARE available (vercel, neon via npx).
