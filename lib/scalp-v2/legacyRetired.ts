import type { NextApiResponse } from "next";

export function setRetiredNoStoreHeaders(res: NextApiResponse): void {
  res.setHeader(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate",
  );
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
}

export function respondScalpLegacyRetired(
  res: NextApiResponse,
  endpoint: string,
): void {
  setRetiredNoStoreHeaders(res);
  res.status(410).json({
    error: "scalp_legacy_retired",
    message:
      "Legacy scalp v1 runtime/ops endpoints are retired after full v2 cutover. Use /api/scalp/v2/* endpoints.",
    endpoint,
    migrationPath: "/api/scalp/v2/*",
  });
}
