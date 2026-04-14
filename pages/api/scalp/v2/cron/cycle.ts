export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  invokeScalpV2CronEndpointDetached,
  type ScalpV2CronInvokeResult,
} from "../../../../../lib/scalp-v2/cronChaining";
import {
  clampScalpV2HardCap,
  resolveScalpV2ResearchHardCaps,
} from "../../../../../lib/scalp-v2/costControls";
import {
  parseBool,
  parseIntBounded,
  setNoStoreHeaders,
} from "../../../../../lib/scalp-v2/http";
import { runScalpV2FullAutoCycle } from "../../../../../lib/scalp-v2/pipeline";

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

  const hardCaps = resolveScalpV2ResearchHardCaps();
  const batchSizeHardCap = Math.max(100, hardCaps.maxBatchSizeWorker);
  const batchSize = clampScalpV2HardCap(
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

  const out = await runScalpV2FullAutoCycle({
    researchBatchSize: batchSize,
  });
  const research = out.evaluate;
  let selfRecall: ScalpV2CronInvokeResult | null = null;
  if (
    research.ok &&
    !research.busy &&
    autoContinue &&
    research.pendingAfter > 0 &&
    selfHop < selfMaxHops
  ) {
    selfRecall = await invokeScalpV2CronEndpointDetached(
      req,
      "/api/scalp/v2/cron/cycle",
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
