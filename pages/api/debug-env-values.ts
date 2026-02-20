// api/debug-env-values.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';
import { requireAdminAccess } from '../../lib/admin';
import { BITGET_ACCOUNT_TYPE } from '../../lib/constants';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (!requireAdminAccess(req, res)) return;

    try {
        console.log('Environment check:');
        console.log({
            BITGET_API_KEY: process.env.BITGET_API_KEY ? process.env.BITGET_API_KEY.slice(0, 6) + '...' : '❌ missing',
            BITGET_API_SECRET: process.env.BITGET_API_SECRET
                ? process.env.BITGET_API_SECRET.slice(0, 6) + '...'
                : '❌ missing',
            BITGET_API_PASSPHRASE: process.env.BITGET_API_PASSPHRASE
                ? process.env.BITGET_API_PASSPHRASE.slice(0, 2) + '...'
                : '❌ missing',
            OPENAI_API_KEY: process.env.OPENAI_API_KEY ? process.env.OPENAI_API_KEY.slice(0, 6) + '...' : '❌ missing',
            COINDESK_API_KEY: process.env.COINDESK_API_KEY
                ? process.env.COINDESK_API_KEY.slice(0, 6) + '...'
                : '❌ missing',
            MARKETAUX_API_KEY: process.env.MARKETAUX_API_KEY
                ? process.env.MARKETAUX_API_KEY.slice(0, 6) + '...'
                : '❌ missing',
            FOREX_FACTORY_CALENDAR_URL:
                process.env.FOREX_FACTORY_CALENDAR_URL || process.env.FOREX_EVENT_CALENDAR_URL || '(default set)',
            CAPITAL_API_KEY: process.env.CAPITAL_API_KEY
                ? process.env.CAPITAL_API_KEY.slice(0, 6) + '...'
                : '❌ missing',
            CAPITAL_IDENTIFIER: process.env.CAPITAL_IDENTIFIER
                ? process.env.CAPITAL_IDENTIFIER.slice(0, 2) + '...'
                : '❌ missing',
            CAPITAL_PASSWORD: process.env.CAPITAL_PASSWORD ? '✅ set' : '❌ missing',
            BITGET_ACCOUNT_TYPE: BITGET_ACCOUNT_TYPE || '(default: usdt-futures)',
            FOREX_EVENT_REFRESH_MINUTES: process.env.FOREX_EVENT_REFRESH_MINUTES || '(default: 15)',
            FOREX_EVENT_STALE_MINUTES: process.env.FOREX_EVENT_STALE_MINUTES || '(default: 45)',
            FOREX_EVENT_BLOCK_IMPACTS: process.env.FOREX_EVENT_BLOCK_IMPACTS || '(default: HIGH)',
        });

        return res.status(200).json({
            BITGET_API_KEY: process.env.BITGET_API_KEY ? '✅ set' : '❌ missing',
            BITGET_API_SECRET: process.env.BITGET_API_SECRET ? '✅ set' : '❌ missing',
            BITGET_API_PASSPHRASE: process.env.BITGET_API_PASSPHRASE ? '✅ set' : '❌ missing',
            OPENAI_API_KEY: process.env.OPENAI_API_KEY ? '✅ set' : '❌ missing',
            COINDESK_API_KEY: process.env.COINDESK_API_KEY ? '✅ set' : '❌ missing',
            MARKETAUX_API_KEY: process.env.MARKETAUX_API_KEY ? '✅ set' : '❌ missing',
            FOREX_FACTORY_CALENDAR_URL:
                process.env.FOREX_FACTORY_CALENDAR_URL || process.env.FOREX_EVENT_CALENDAR_URL || '(default set)',
            CAPITAL_API_KEY: process.env.CAPITAL_API_KEY ? '✅ set' : '❌ missing',
            CAPITAL_IDENTIFIER: process.env.CAPITAL_IDENTIFIER ? '✅ set' : '❌ missing',
            CAPITAL_PASSWORD: process.env.CAPITAL_PASSWORD ? '✅ set' : '❌ missing',
            BITGET_ACCOUNT_TYPE: BITGET_ACCOUNT_TYPE || '(default: usdt-futures)',
            FOREX_EVENT_REFRESH_MINUTES: process.env.FOREX_EVENT_REFRESH_MINUTES || '(default: 15)',
            FOREX_EVENT_STALE_MINUTES: process.env.FOREX_EVENT_STALE_MINUTES || '(default: 45)',
            FOREX_EVENT_BLOCK_IMPACTS: process.env.FOREX_EVENT_BLOCK_IMPACTS || '(default: HIGH)',
        });
    } catch (err: any) {
        console.error('Error in /debug-env-values:', err);
        return res.status(500).json({ error: err.message || String(err) });
    }
}
