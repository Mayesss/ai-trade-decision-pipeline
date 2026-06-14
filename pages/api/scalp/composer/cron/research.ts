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
import { runScalpComposerResearchJob } from "../../../../../lib/scalp/composer/pipeline";

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
  const debug = parseBool(req.query.debug, false);
  const autoSuccessor = parseBool(req.query.autoSuccessor, true);
  const autoContinue = parseBool(req.query.autoContinue, true);
  const selfHop = parseIntBounded(req.query.selfHop, 0, 0, 20);
  const selfMaxHops = Math.min(
    parseIntBounded(req.query.selfMaxHops, 6, 0, 50),
    hardCaps.maxSelfHops,
  );

  const job = await runScalpComposerResearchJob({ batchSize, debugTiming: debug });

  let downstream: ScalpComposerCronInvokeResult | null = null;
  let selfRecall: ScalpComposerCronInvokeResult | null = null;
  if (
    job.ok &&
    !job.busy &&
    autoContinue &&
    job.pendingAfter > 0 &&
    selfHop < selfMaxHops
  ) {
    selfRecall = await invokeScalpComposerCronEndpointDetached(
      req,
      "/api/scalp/composer/cron/research",
      {
        batchSize,
        autoSuccessor: autoSuccessor ? 1 : 0,
        autoContinue: 1,
        selfHop: selfHop + 1,
        selfMaxHops,
        triggeredBy: "research-v2-self",
      },
      700,
    );
  }
  if (job.ok && !job.busy && autoSuccessor && job.pendingAfter <= 0) {
    downstream = await invokeScalpComposerCronEndpointDetached(
      req,
      "/api/scalp/composer/cron/promote",
      {
        triggeredBy: "research-v2",
      },
      850,
    );
  }

  return res.status(200).json({
    ok: job.ok,
    busy: job.busy,
    job,
    chaining: {
      autoSuccessor,
      autoContinue,
      selfHop,
      selfMaxHops,
      debug,
      batchSize,
      batchSizeHardCap,
      downstream,
      selfRecall,
    },
  });
}
