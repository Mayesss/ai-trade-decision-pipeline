// api/analyze.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';

import { fetchMarketBundle, computeAnalytics, fetchPositionInfo } from '../../lib/analytics';
import { calculateMultiTFIndicators } from '../../lib/indicators';
import { fetchNewsSentiment } from '../../lib/news';

import { buildPrompt, callAI } from '../../lib/ai';
import { getGates } from '../../lib/gates';

import { executeDecision, getTradeProductType } from '../../lib/trading';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    if (req.method !== 'POST') {
      return res.status(405).json({ error: 'Method Not Allowed', message: 'Use POST' });
    }

    const body = req.body ?? {};
    const symbol = (body.symbol as string) || 'ETHUSDT';
    const timeFrame = body.timeFrame || '15m';
    const dryRun = body.dryRun !== false; // default true
    const sideSizeUSDT = Number(body.notional || 10);

    // 1) Product & position
    const productType = getTradeProductType();
    const positionInfo = await fetchPositionInfo(symbol);
    const positionForPrompt =
      positionInfo.status === 'open'
        ? `${positionInfo.holdSide}, entryPrice: ${positionInfo.entryPrice}, currentPnl=${positionInfo.currentPnl}`
        : 'none';

    // 2) News sentiment
    const news = await fetchNewsSentiment(symbol);

    // 3) Market data & analytics
    const bundle = await fetchMarketBundle(symbol, timeFrame);
    const analytics = computeAnalytics(bundle);

    // 4) Technical indicators
    const indicators = await calculateMultiTFIndicators(symbol);

    // 5) Gates (adaptive) — one call keeps analyze.ts lightweight
    const gatesOut = getGates({
      symbol,
      bundle,
      analytics,
      indicators,
      notionalUSDT: sideSizeUSDT,
      positionOpen: positionInfo.status === 'open',
      // histories?: you can attach rolling arrays here if you keep them (optional)
    });

    // 5b) Optional short-circuit to save tokens if nothing to do
    if (gatesOut.preDecision && positionInfo.status !== 'open') {
      return res.status(200).json({
        symbol,
        timeFrame,
        dryRun,
        decision: gatesOut.preDecision,
        execRes: { placed: false, orderId: null, clientOid: null, reason: 'gates_short_circuit' },
        gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
      });
    }

    // 6) Build prompt with allowed_actions & gates
    const { system, user, allowed_actions } = buildPrompt(
      symbol,
      timeFrame,
      bundle,
      analytics,
      positionForPrompt,
      news,
      indicators,
      {
        allowed_actions: gatesOut.allowed_actions,
        gates: gatesOut.gates,
        metrics: gatesOut.metrics, // helpful in logs
      }
    );

    // 7) Query AI (enforced to allowed_actions)
    const decision = await callAI(system, user, { allowed_actions });

    // 8) Execute (dry run unless explicitly disabled)
    const execRes = await executeDecision(symbol, sideSizeUSDT, decision, productType, dryRun);

    // 9) Respond
    return res.status(200).json({
      symbol,
      timeFrame,
      dryRun,
      decision,
      execRes,
      gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
    });
  } catch (err: any) {
    console.error('Error in /api/analyze:', err);
    return res.status(500).json({ error: err.message || String(err) });
  }
}