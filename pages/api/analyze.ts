// api/analyze.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';
import { fetchMarketBundle, computeAnalytics, fetchPositionInfo } from '../../lib/analytics';
import { calculateMultiTFIndicators } from '../../lib/indicators';
import { fetchNewsSentiment } from '../../lib/news';
import { buildPrompt, callAI } from '../../lib/ai';
import { executeDecision, getTradeProductType } from '../../lib/trading';

// ------------------------------
// /api/analyze  →  AI trade decision endpoint
// ------------------------------

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

        // 1️⃣ Product & position
        const productType = getTradeProductType();
        const positionInfo = await fetchPositionInfo(symbol);
        const positionForPrompt =
            positionInfo.status === 'open'
                ? `${positionInfo.holdSide}, entryPrice: ${positionInfo.entryPrice}, currentPnl=${positionInfo.currentPnl}`
                : 'none';

        // 2️⃣ News sentiment
        const news = await fetchNewsSentiment(symbol);

        // 3️⃣ Market data & analytics
        const bundle = await fetchMarketBundle(symbol, timeFrame);
        const analytics = computeAnalytics(bundle);

        // 4️⃣ Technical indicators
        const indicators = await calculateMultiTFIndicators(symbol);

        //
        // 5️⃣ Build AI prompt
        const { system, user } = buildPrompt(symbol, timeFrame, bundle, analytics, positionForPrompt, news, indicators);

        // 7️⃣ Query AI
        const decision = await callAI(system, user);

        // 8️⃣ Execute (dry run unless explicitly disabled)
        const execRes = await executeDecision(symbol, sideSizeUSDT, decision, productType, dryRun);

        // ✅ Respond
        return res.status(200).json({
            symbol,
            timeFrame,
            dryRun,
            decision,
            execRes,
        });
    } catch (err: any) {
        console.error('Error in /api/analyze:', err);
        return res.status(500).json({ error: err.message || String(err) });
    }
}
