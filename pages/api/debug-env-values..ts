// api/debug-env-values.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';
import { BITGET_ACCOUNT_TYPE } from '../../lib/constants';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
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
            BITGET_ACCOUNT_TYPE: BITGET_ACCOUNT_TYPE || '(default: usdt-futures)',
        });

        return res.status(200).json({
            BITGET_API_KEY: process.env.BITGET_API_KEY ? '✅ set' : '❌ missing',
            BITGET_API_SECRET: process.env.BITGET_API_SECRET ? '✅ set' : '❌ missing',
            BITGET_API_PASSPHRASE: process.env.BITGET_API_PASSPHRASE ? '✅ set' : '❌ missing',
            OPENAI_API_KEY: process.env.OPENAI_API_KEY ? '✅ set' : '❌ missing',
            COINDESK_API_KEY: process.env.COINDESK_API_KEY ? '✅ set' : '❌ missing',
            BITGET_ACCOUNT_TYPE: BITGET_ACCOUNT_TYPE || '(default: usdt-futures)',
        });
    } catch (err: any) {
        console.error('Error in /debug-env-values:', err);
        return res.status(500).json({ error: err.message || String(err) });
    }
}
