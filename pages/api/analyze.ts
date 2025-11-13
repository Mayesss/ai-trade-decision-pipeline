// api/analyze.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';

import { fetchMarketBundle, computeAnalytics, fetchPositionInfo } from '../../lib/analytics';
import { calculateMultiTFIndicators } from '../../lib/indicators';
import { fetchNewsSentiment } from '../../lib/news';

import { buildPrompt, callAI } from '../../lib/ai';
import { getGates } from '../../lib/gates';

import { executeDecision, getTradeProductType } from '../../lib/trading';

// ---------- helpers (local, lightweight) ----------
function parsePnlPct(p: string | undefined): number {
  if (!p) return 0;
  const m = String(p).match(/-?\d+(\.\d+)?/);
  return m ? Number(m[0]) : 0;
}

// flow is "against" if CVD sign opposes the position side
function flowAgainst(side: 'long' | 'short' | undefined, cvd: number): boolean {
  if (!side) return false;
  return side === 'long' ? cvd < 0 : cvd > 0;
}

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

    // 5) Gates (adaptive)
    const gatesOut = getGates({
      symbol,
      bundle,
      analytics,
      indicators,
      notionalUSDT: sideSizeUSDT,
      positionOpen: positionInfo.status === 'open',
      // histories?: add rolling arrays here if you persist them
    });

    // 5b) Optional short-circuit when only HOLD is allowed and no open position
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

    // 5c) Build explicit CLOSE conditions (so AI can’t close immediately without a reason)
    let close_conditions:
      | {
          pnl_gt_pos?: boolean;
          pnl_lt_neg?: boolean;
          opposite_regime?: boolean;
          cvd_flip?: boolean;
          // time_stop?: boolean; // optional if you track bars-in-trade
        }
      | undefined;

    if (positionInfo.status === 'open') {
      const pnlPct = parsePnlPct(positionInfo.currentPnl);
      const side = positionInfo.holdSide; // 'long' | 'short'

      const regimeUp = indicators.macro.includes('trend=up');
      const regimeDown = indicators.macro.includes('trend=down');
      const opposite_regime =
        (side === 'long' && regimeDown) || (side === 'short' && regimeUp);

      const cvdVal = Number(analytics.cvd || 0);
      const cvd_flip = flowAgainst(side, cvdVal);

      // thresholds: adjust if too tight/loose
      close_conditions = {
        pnl_gt_pos: pnlPct >= 1.0,   // take profit ≥ +1%
        pnl_lt_neg: pnlPct <= -1.0,  // stop loss ≤ -1%
        opposite_regime,             // macro regime flipped vs side
        cvd_flip,                    // recent flow turned against side
        // time_stop: false
      };
    }

    // 6) Build prompt with allowed_actions, gates, and close_conditions
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
        // includeMetrics: false,        // keep prompt lean as you prefer
        // metrics: gatesOut.metrics,    // omit if not needed in prompt
        close_conditions,               // <-- important
      }
    );

    // 7) Query AI (post-parse enforcement checks allowed_actions & close_conditions)
    const decision = await callAI(system, user, { allowed_actions, close_conditions });

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
