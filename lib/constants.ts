export const BITGET_ACCOUNT_TYPE = 'usdt-futures';
export const AI_MODEL = 'gpt-5.4';
export const AI_BASE_URL = 'https://api.openai.com/v1';
export const COINDESK_API_BASE = 'https://data-api.coindesk.com';
export const COINDESK_NEWS_LIST_PATH = '/news/v1/article/list';
export const MARKETAUX_API_BASE = 'https://api.marketaux.com/v1';
export const TRADE_WINDOW_MINUTES = 60;
export const DEFAULT_TAKER_FEE_RATE = Number(process.env.TAKER_FEE_RATE ?? '0.0006');
export const DEFAULT_NOTIONAL_USDT = 100;
// Pre-prompt cost control: when flat, only spend an AI call if the code-computed
// signal_strength is at least this rank. LOW setups can't open a quality position
// anyway, so we skip the call and HOLD. Entries are evaluated hourly (data is
// collected every tick); the AI only fires when a setup is actually actionable.
// In-position ticks always call the AI regardless (exits/trims can be needed at
// any strength).
//
// signal_strength is CODE-ONLY — it is no longer shown to the model (it's an
// uncalibrated composite that anchored the model's analysis; see lib/ai.ts). Its
// sole remaining roles are this budget gate and postprocessDecision's thresholds.
// Tunable via env (default MEDIUM) so the cost/coverage tradeoff can be adjusted
// without a deploy — e.g. set HIGH to cut flat-tick spend further (7-day data:
// flat MEDIUM ticks opened 0 positions, so HIGH would have saved those calls).
function resolveFlatMinSignalStrength(): 'MEDIUM' | 'HIGH' {
    return String(process.env.SWING_FLAT_MIN_SIGNAL_STRENGTH ?? '').trim().toUpperCase() === 'HIGH'
        ? 'HIGH'
        : 'MEDIUM';
}
export const SWING_FLAT_MIN_SIGNAL_STRENGTH: 'MEDIUM' | 'HIGH' = resolveFlatMinSignalStrength();

export const MICRO_TIMEFRAME = '1H';
export const PRIMARY_TIMEFRAME = '4H';
export const MACRO_TIMEFRAME = '1D';
export const CONTEXT_TIMEFRAME = '1W';
