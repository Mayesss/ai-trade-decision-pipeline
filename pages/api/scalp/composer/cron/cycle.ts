export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  invokeScalpComposerCronEndpointDetached,
  type ScalpComposerCronInvokeResult,
} from "../../../../../lib/scalp/composer/cronChaining";
import {
  clampScalpComposerHardCap,
  resolveScalpComposerResearchHardCaps,
} from "../../../../../lib/scalp/composer/costControls";
import {
  parseBool,
  parseIntBounded,
  setNoStoreHeaders,
} from "../../../../../lib/scalp/composer/http";
import { runScalpComposerFullAutoCycle } from "../../../../../lib/scalp/composer/pipeline";
import { runScalpRegimeResearchJob } from "../../../../../lib/scalp/regimes";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (req.method !== "GET") {
    return res
      .status(405)
      .json({ error: "Method Not Allowed", message: "Use GET" });
  }
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  const hardCaps = resolveScalpComposerResearchHardCaps();
  const batchSizeHardCap = Math.max(100, hardCaps.maxBatchSizeWorker);
  const batchSize = clampScalpComposerHardCap(
    parseIntBounded(req.query.batchSize, 100, 1, 600),
    batchSizeHardCap,
  );
  const autoContinue = parseBool(req.query.autoContinue, true);
  const selfHop = parseIntBounded(req.query.selfHop, 0, 0, 20);
  const selfMaxHops = Math.min(
    parseIntBounded(req.query.selfMaxHops, 6, 0, 50),
    hardCaps.maxSelfHops,
  );
  const dryRun = parseBool(req.query.dryRun, false);
  const legacyV2 = parseBool(req.query.legacyV2, false);

  if (!legacyV2) {
    const maxCandidatesPerCall = parseIntBounded(
      req.query.maxCandidatesPerCall,
      Math.max(0, Math.min(500, batchSize)),
      0,
      500,
    );
    const job = await runScalpRegimeResearchJob({
      maxCandidatesPerCall,
      candidateFetchLimit: Math.max(maxCandidatesPerCall * 4, 50),
      forceValidity: parseBool(req.query.forceValidity, false),
    });
    return res.status(200).json({
      ok: job.ok,
      out: {
        discover: { ok: true, skipped: true, reason: "v4_only_mode" },
        evaluate: job,
        worker: { ok: true, skipped: true, reason: "v4_only_mode" },
        promote: { ok: true, skipped: true, reason: "v4_only_mode" },
      },
      version: "v4",
      legacyRoute: "/api/scalp/composer/cron/cycle",
      message:
        "v2 cycle is disabled by default; pass legacyV2=true to run the old v2/v3 cycle.",
      chaining: {
        autoContinue: false,
        selfHop,
        selfMaxHops,
        batchSize,
        batchSizeHardCap,
        selfRecall: null,
        dryRun,
      },
    });
  }

  const out = await runScalpComposerFullAutoCycle({
    researchBatchSize: batchSize,
  });
  const research = out.evaluate;
  let selfRecall: ScalpComposerCronInvokeResult | null = null;
  if (
    research.ok &&
    !research.busy &&
    autoContinue &&
    research.pendingAfter > 0 &&
    selfHop < selfMaxHops
  ) {
    selfRecall = await invokeScalpComposerCronEndpointDetached(
      req,
      "/api/scalp/composer/cron/cycle",
      {
        batchSize,
        autoContinue: 1,
        selfHop: selfHop + 1,
        selfMaxHops,
        dryRun: dryRun ? 1 : 0,
        triggeredBy: "cycle-research-self",
      },
      700,
    );
  }

  return res.status(200).json({
    ok:
      out.discover.ok &&
      out.evaluate.ok &&
      out.worker.ok &&
      out.promote.ok,
    out,
    chaining: {
      autoContinue,
      selfHop,
      selfMaxHops,
      batchSize,
      batchSizeHardCap,
      selfRecall,
    },
  });
}
