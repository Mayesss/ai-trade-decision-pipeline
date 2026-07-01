export const BITGET_ACCOUNT_TYPE = 'usdt-futures';
export const AI_MODEL = 'gpt-5.4';
export const AI_BASE_URL = 'https://api.openai.com/v1';
export const COINDESK_API_BASE = 'https://data-api.coindesk.com';
export const COINDESK_NEWS_LIST_PATH = '/news/v1/article/list';
export const MARKETAUX_API_BASE = 'https://api.marketaux.com/v1';
export const TRADE_WINDOW_MINUTES = 60;
export const DEFAULT_TAKER_FEE_RATE = Number(process.env.TAKER_FEE_RATE ?? '0.0006');
export const DEFAULT_NOTIONAL_USDT = 100;
// NOTE: the old flat signal_strength≥MEDIUM budget gate was replaced by the
// actionability gate (evaluateActionability in lib/ai.ts) — a confirmed-structure
// OR bounce-with-room predicate derived from the decision history. signal_strength
// is now used only by postprocessDecision's exception thresholds.

export const MICRO_TIMEFRAME = '1H';
export const PRIMARY_TIMEFRAME = '4H';
export const MACRO_TIMEFRAME = '1D';
export const CONTEXT_TIMEFRAME = '1W';
