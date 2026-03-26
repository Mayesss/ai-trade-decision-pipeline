export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import {
  invokeCronEndpointDetached,
  type CronInvokeResult,
} from "../../../../lib/scalp/cronChaining";
import { runPromotionPipelineJob } from "../../../../lib/scalp/pipelineJobs";
import {
  listScalpEntrySessionProfiles,
  parseScalpEntrySessionProfileStrict,
} from "../../../../lib/scalp/sessions";

function firstQueryValue(
  value: string | string[] | undefined,
): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0)
    return String(value[0] || "").trim() || undefined;
  return undefined;
}

function parseBool(
  value: string | string[] | undefined,
  fallback: boolean,
): boolean {
  const first = firstQueryValue(value);
  if (!first) return fallback;
  const normalized = first.toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function parseIntBounded(
  value: string | string[] | undefined,
  fallback: number,
  min: number,
  max: number,
): number {
  const first = firstQueryValue(value);
  const n = Number(first);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

function setNoStoreHeaders(res: NextApiResponse): void {
  res.setHeader(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate",
  );
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
}

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

  const batchSize = parseIntBounded(req.query.batchSize, 200, 1, 1_500);
  const autoSuccessor = parseBool(req.query.autoSuccessor, true);
  const autoContinue = parseBool(req.query.autoContinue, true);
  const selfHop = parseIntBounded(req.query.selfHop, 0, 0, 50);
  const selfMaxHops = parseIntBounded(req.query.selfMaxHops, 6, 0, 50);
  const requestedSessionRaw = firstQueryValue(req.query.session);
  const session = requestedSessionRaw
    ? parseScalpEntrySessionProfileStrict(requestedSessionRaw)
    : null;
  if (requestedSessionRaw && !session) {
    return res.status(400).json({
      error: "invalid_session",
      message: `Use session=${listScalpEntrySessionProfiles().join("|")}.`,
    });
  }
  const downstreamSessions = session
    ? [session]
    : listScalpEntrySessionProfiles();

  const result = await runPromotionPipelineJob({
    batchSize,
    entrySessionProfile: session || undefined,
  });

  let downstreamLoadCandles: CronInvokeResult | null = null;
  let downstreamWorker: CronInvokeResult | null = null;
  let downstreamWorkers: CronInvokeResult[] = [];
  let selfRecall: CronInvokeResult | null = null;
  if (
    result.ok &&
    !result.busy &&
    autoSuccessor &&
    result.downstreamRequested
  ) {
    downstreamLoadCandles = await invokeCronEndpointDetached(
      req,
      "/api/scalp/cron/v2/load-candles",
      {
        autoContinue: 1,
        autoSuccessor: 1,
        selfHop: 0,
        selfMaxHops,
        triggeredBy: "promotion",
        session: session || undefined,
      },
      850,
    );
    downstreamWorkers = await Promise.all(
      downstreamSessions.map((entrySessionProfile) =>
        invokeCronEndpointDetached(
          req,
          "/api/scalp/cron/worker",
          {
            autoContinue: 1,
            autoSuccessor: 1,
            selfHop: 0,
            selfMaxHops,
            triggeredBy: "promotion",
            session: entrySessionProfile,
          },
          850,
        ),
      ),
    );
    downstreamWorker = downstreamWorkers[0] || null;
  }

  if (
    result.ok &&
    !result.busy &&
    autoContinue &&
    result.pendingAfter > 0 &&
    selfHop < selfMaxHops
  ) {
    selfRecall = await invokeCronEndpointDetached(
      req,
      "/api/scalp/cron/promotion",
      {
        batchSize,
        autoContinue: 1,
        autoSuccessor: autoSuccessor ? 1 : 0,
        selfHop: selfHop + 1,
        selfMaxHops,
        session: session || undefined,
      },
      700,
    );
  }

  return res.status(200).json({
    ok: result.ok,
    busy: result.busy,
    job: result,
    chaining: {
      autoSuccessor,
      autoContinue,
      selfHop,
      selfMaxHops,
      session,
      downstreamLoadCandles,
      downstreamWorker,
      downstreamWorkers,
      selfRecall,
    },
  });
}
