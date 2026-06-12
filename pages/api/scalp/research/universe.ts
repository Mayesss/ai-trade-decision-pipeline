export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { respondScalpLegacyRetired } from "../../../../lib/scalp/composer/legacyRetired";

export default async function handler(
  _req: NextApiRequest,
  res: NextApiResponse,
) {
  return respondScalpLegacyRetired(res, "/api/scalp/research/universe");
}
