// Capital.com DEMO validation — pullback working orders + bracket-merge PUT.
//
// Validates against https://demo-api-capital.backend-capital.com (never live):
//   A. session + working-order listing works on the demo environment
//   B. pullback limit entry via the REAL executeCapitalDecision → resting
//      working order with bracket, visible via listCapitalPendingEntryOrders,
//      cancelled by cancelCapitalPendingEntryOrders (one-tick TTL sweep)
//   C. bracket-merge fix on updateCapitalPositionLevels: an SL-only amend must
//      NOT clear the standing TP (Capital's PUT replaces the whole bracket —
//      the 2026-07-09 COPPER ping-pong bug), and vice versa
//
// Run:  npx tsx scripts/validate-capital-orders.ts
// Env:  uses CAPITAL_DEMO_API_KEY / CAPITAL_DEMO_IDENTIFIER / CAPITAL_DEMO_PASSWORD
//       when present, else the live credentials (Capital demo accounts share the
//       login; the demo base URL is what isolates the environment).
//       Optional: CAPITAL_DEMO_SYMBOL (default GOLD).
import nextEnv from '@next/env';

let failures = 0;
function check(name: string, ok: boolean, detail?: unknown) {
    if (!ok) failures++;
    console.log(`[${ok ? 'PASS' : 'FAIL'}] ${name}${detail !== undefined ? ` :: ${JSON.stringify(detail)?.slice(0, 260)}` : ''}`);
}
function info(msg: string, detail?: unknown) {
    console.log(`[info] ${msg}${detail !== undefined ? ` :: ${JSON.stringify(detail)?.slice(0, 260)}` : ''}`);
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main() {
    nextEnv.loadEnvConfig(process.cwd());
    // Demo routing — MUST be set before lib/capital is imported (module-level
    // const), hence the dynamic import below.
    process.env.CAPITAL_API_BASE = 'https://demo-api-capital.backend-capital.com';
    if (process.env.CAPITAL_DEMO_API_KEY) {
        process.env.CAPITAL_API_KEY = process.env.CAPITAL_DEMO_API_KEY;
        process.env.CAPITAL_IDENTIFIER = process.env.CAPITAL_DEMO_IDENTIFIER || process.env.CAPITAL_IDENTIFIER || '';
        process.env.CAPITAL_PASSWORD = process.env.CAPITAL_DEMO_PASSWORD || process.env.CAPITAL_PASSWORD || '';
        info('using CAPITAL_DEMO_* credentials');
    } else {
        info('no CAPITAL_DEMO_* credentials — using live credentials against the demo base URL');
    }

    const capital = await import('../lib/capital');
    const symbol = String(process.env.CAPITAL_DEMO_SYMBOL || 'GOLD').toUpperCase();
    info(`symbol=${symbol} base=${process.env.CAPITAL_API_BASE}`);

    // ---- Phase A: session + market data + empty order book ----
    const live = await capital.fetchCapitalLivePrice(symbol);
    const price = Number((live as any)?.mid ?? (live as any)?.last ?? (live as any)?.bid);
    if (!(price > 0)) throw new Error(`no demo price for ${symbol}: ${JSON.stringify(live)}`);
    info(`demo price = ${price}`);
    const initialOrders = await capital.listCapitalPendingEntryOrders(symbol);
    check('A1 working-order listing works', Array.isArray(initialOrders), { count: initialOrders.length });

    try {
        // ---- Phase B: pullback working order via the real decision path ----
        const limit = price * 0.985;
        const resB = await capital.executeCapitalDecision(
            symbol,
            20,
            { action: 'BUY', summary: '', reason: '', entry_limit_price: limit } as any,
            false,
            limit * 0.985, // catastrophe stop anchored at the limit
            true,
            limit * 1.02, // TP anchored at the limit
        );
        check('B1 pullback order placed with pendingEntry flag', (resB as any)?.placed === true && (resB as any)?.pendingEntry === true, resB);
        await sleep(2500);
        const pending = await capital.listCapitalPendingEntryOrders(symbol);
        check('B2 resting working order visible', pending.length === 1 && pending[0].level != null, pending);
        const sweep = await capital.cancelCapitalPendingEntryOrders(symbol);
        check('B3 TTL sweep cancels it', sweep.found === 1 && sweep.cancelled === 1 && sweep.errors.length === 0, sweep);
        await sleep(1500);
        const after = await capital.listCapitalPendingEntryOrders(symbol);
        check('B4 nothing resting after sweep', after.length === 0, after);

        // ---- Phase C: bracket-merge on position amend ----
        const resC = await capital.executeCapitalDecision(
            symbol,
            20,
            { action: 'BUY', summary: '', reason: '' } as any,
            false,
            price * 0.97,
            true,
            price * 1.03,
        );
        check('C1 market position opened with bracket', (resC as any)?.placed === true, resC);
        await sleep(3000);
        const pos1 = await capital.fetchCapitalPositionInfo(symbol);
        check(
            'C2 position row exposes both bracket legs',
            pos1.status === 'open' && pos1.takeProfitPrice != null && pos1.stopLossPrice != null,
            pos1,
        );
        if (pos1.status === 'open') {
            const newSl = price * 0.975;
            const amendSl = await capital.updateCapitalPositionLevels({ symbol, stopLevel: newSl });
            check('C3 SL-only amend applied', amendSl.updated === true, amendSl);
            await sleep(2500);
            const pos2 = await capital.fetchCapitalPositionInfo(symbol);
            check(
                'C4 TP SURVIVES an SL-only amend (merge fix)',
                pos2.status === 'open' && pos2.takeProfitPrice != null && pos2.stopLossPrice != null,
                pos2,
            );
            const newTp = price * 1.025;
            const amendTp = await capital.updateCapitalPositionLevels({ symbol, profitLevel: newTp });
            check('C5 TP-only amend applied', amendTp.updated === true, amendTp);
            await sleep(2500);
            const pos3 = await capital.fetchCapitalPositionInfo(symbol);
            check(
                'C6 SL survives a TP-only amend',
                pos3.status === 'open' && pos3.stopLossPrice != null && pos3.takeProfitPrice != null,
                pos3,
            );
        }
    } finally {
        // ---- Cleanup: never leave demo orders or positions behind ----
        try {
            await capital.cancelCapitalPendingEntryOrders(symbol);
            const open = await capital.fetchCapitalPositionInfo(symbol);
            if (open.status === 'open') {
                await capital.executeCapitalDecision(symbol, 20, { action: 'CLOSE', summary: '', reason: '' } as any, false, null, true);
                info('cleanup: closed demo position');
            } else {
                info('cleanup: flat');
            }
        } catch (err) {
            info('cleanup failed — check demo account manually', err instanceof Error ? err.message : String(err));
        }
    }

    console.log(failures === 0 ? '\nALL CHECKS PASSED' : `\n${failures} CHECK(S) FAILED`);
    process.exit(failures === 0 ? 0 : 1);
}

main().catch((err) => {
    console.error('\nvalidation aborted:', err instanceof Error ? err.message : err);
    process.exit(2);
});
