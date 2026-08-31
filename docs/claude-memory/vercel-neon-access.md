---
name: vercel-neon-access
description: Neon + Vercel CLI access set up 2026-08-28; project/org IDs and skill locations
metadata: 
  node_type: memory
  type: project
  originSessionId: 52254b3d-c385-40d4-8a31-d21bf2204bda
  modified: 2026-08-28T16:29:22.893Z
---

Both CLIs are authenticated on this machine (user completed browser/device flows on 2026-08-28):
- Neon: org `org-icy-mouse-29976601` ("Vercel: Mayess' projects"), single project `holy-resonance-21485949` (name `neon-aero-house`, eu-central-1, pg17), default branch `br-super-hill-agtd7n6l` (main). CLI: `npx neon@latest ...`; pass `--org-id`/`--project-id` to avoid interactive pickers.
- Vercel: account `mayesssammoud-8592`, team `mayess-projects`, project `ai-trade-decision-pipeline` linked via `.vercel/` (gitignored). `npx vercel env ls production` works; `vercel login` uses a device flow (run in background, give user the `vercel.com/oauth/device?user_code=...` URL).
- Agent skills committed in-repo: `.claude/skills/` (neon, neon-postgres) and `.agents/skills/` + symlinks (env-vars, vercel-cli, deployments-cicd, ai-gateway from vercel/vercel-plugin).
- Vercel prod env already contains `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`(assumed), `NEON_API_KEY`, `NEON__DATABASE_URL` family, `ADMIN_ACCESS_SECRET` — relevant for the [[forex-scalp-cleanup-plan]] phase-5 drops and the AI Gateway migration.
- The scalp-free build deployed to production 2026-08-28 (~22s build, Ready); `/api/dashboard/summary-warm-fallback` returned ok with a live cycleId, confirming the swing pipeline runs on the new build.
