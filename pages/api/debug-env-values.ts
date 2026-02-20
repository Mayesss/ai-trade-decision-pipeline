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
            FOREX_EVENT_BLOCK_NEW_IMPACTS: process.env.FOREX_EVENT_BLOCK_NEW_IMPACTS || '(default: HIGH,MEDIUM)',
            FOREX_EVENT_FORCE_CLOSE_IMPACTS: process.env.FOREX_EVENT_FORCE_CLOSE_IMPACTS || '(default: HIGH)',
            FOREX_EVENT_TIGHTEN_ONLY_IMPACTS: process.env.FOREX_EVENT_TIGHTEN_ONLY_IMPACTS || '(default: MEDIUM)',
            FOREX_PACKET_STALE_MINUTES: process.env.FOREX_PACKET_STALE_MINUTES || '(default: 120)',
            FOREX_TIME_STOP_NO_FOLLOW_BARS: process.env.FOREX_TIME_STOP_NO_FOLLOW_BARS || '(default: 18)',
            FOREX_TIME_STOP_MIN_FOLLOW_R: process.env.FOREX_TIME_STOP_MIN_FOLLOW_R || '(default: 0.3)',
            FOREX_TIME_STOP_MAX_HOLD_HOURS: process.env.FOREX_TIME_STOP_MAX_HOLD_HOURS || '(default: 10)',
            FOREX_RISK_PER_TRADE_PCT: process.env.FOREX_RISK_PER_TRADE_PCT || '(default: 0.5)',
            FOREX_RISK_REFERENCE_EQUITY_USD: process.env.FOREX_RISK_REFERENCE_EQUITY_USD ? '✅ set' : '❌ missing',
            FOREX_RISK_MAX_PORTFOLIO_OPEN_PCT: process.env.FOREX_RISK_MAX_PORTFOLIO_OPEN_PCT || '(default: 2.0)',
            FOREX_RISK_MAX_CURRENCY_OPEN_PCT: process.env.FOREX_RISK_MAX_CURRENCY_OPEN_PCT || '(default: 1.0)',
            FOREX_MAX_LEVERAGE_PER_PAIR: process.env.FOREX_MAX_LEVERAGE_PER_PAIR || '(default: 3)',
            FOREX_REENTRY_LOCK_MINUTES: process.env.FOREX_REENTRY_LOCK_MINUTES || '(default: 5)',
            FOREX_SELECTOR_TOP_PERCENT: process.env.FOREX_SELECTOR_TOP_PERCENT || '(default: 40)',
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
            FOREX_EVENT_BLOCK_NEW_IMPACTS: process.env.FOREX_EVENT_BLOCK_NEW_IMPACTS || '(default: HIGH,MEDIUM)',
            FOREX_EVENT_FORCE_CLOSE_IMPACTS: process.env.FOREX_EVENT_FORCE_CLOSE_IMPACTS || '(default: HIGH)',
            FOREX_EVENT_TIGHTEN_ONLY_IMPACTS: process.env.FOREX_EVENT_TIGHTEN_ONLY_IMPACTS || '(default: MEDIUM)',
            FOREX_PACKET_STALE_MINUTES: process.env.FOREX_PACKET_STALE_MINUTES || '(default: 120)',
            FOREX_TIME_STOP_NO_FOLLOW_BARS: process.env.FOREX_TIME_STOP_NO_FOLLOW_BARS || '(default: 18)',
            FOREX_TIME_STOP_MIN_FOLLOW_R: process.env.FOREX_TIME_STOP_MIN_FOLLOW_R || '(default: 0.3)',
            FOREX_TIME_STOP_MAX_HOLD_HOURS: process.env.FOREX_TIME_STOP_MAX_HOLD_HOURS || '(default: 10)',
            FOREX_RISK_PER_TRADE_PCT: process.env.FOREX_RISK_PER_TRADE_PCT || '(default: 0.5)',
            FOREX_RISK_REFERENCE_EQUITY_USD: process.env.FOREX_RISK_REFERENCE_EQUITY_USD ? '✅ set' : '❌ missing',
            FOREX_RISK_MAX_PORTFOLIO_OPEN_PCT: process.env.FOREX_RISK_MAX_PORTFOLIO_OPEN_PCT || '(default: 2.0)',
            FOREX_RISK_MAX_CURRENCY_OPEN_PCT: process.env.FOREX_RISK_MAX_CURRENCY_OPEN_PCT || '(default: 1.0)',
            FOREX_MAX_LEVERAGE_PER_PAIR: process.env.FOREX_MAX_LEVERAGE_PER_PAIR || '(default: 3)',
            FOREX_REENTRY_LOCK_MINUTES: process.env.FOREX_REENTRY_LOCK_MINUTES || '(default: 5)',
            FOREX_SELECTOR_TOP_PERCENT: process.env.FOREX_SELECTOR_TOP_PERCENT || '(default: 40)',
        });
    } catch (err: any) {
        console.error('Error in /debug-env-values:', err);
        return res.status(500).json({ error: err.message || String(err) });
    }
}
