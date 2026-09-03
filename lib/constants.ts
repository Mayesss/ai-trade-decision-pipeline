export const BITGET_ACCOUNT_TYPE = 'usdt-futures';
// All AI traffic routes through the Vercel AI Gateway (BYOK provider keys are
// configured in the gateway dashboard, not here). Model ids use the gateway's
// `provider/model` slug format — version numbers with dots, not hyphens.
// Switched off openai/gpt-5.6-sol on 2026-09-03: same measured capability at
// materially lower cost (Artificial Analysis index 60 at max effort vs sol's
// 56 at medium; $1.40/$4.40 per 1M in/out vs $2/$10, cached input $0.14 vs
// $0.20). Every non-Anthropic id speaks the OpenAI dialect through the
// gateway — see dialectForAiModel in lib/aiModel.ts.
export const DEFAULT_AI_MODEL = 'zai/glm-5.3';
export const FALLBACK_AI_MODEL = 'anthropic/claude-opus-4.8';
export const AI_BASE_URL = 'https://ai-gateway.vercel.sh/v1';
// Anthropic-compatible endpoint of the same gateway (the SDK appends /v1).
export const AI_GATEWAY_ANTHROPIC_BASE_URL = 'https://ai-gateway.vercel.sh';
export const COINDESK_API_BASE = 'https://data-api.coindesk.com';
export const COINDESK_NEWS_LIST_PATH = '/news/v1/article/list';
export const MARKETAUX_API_BASE = 'https://api.marketaux.com/v1';
export const TRADE_WINDOW_MINUTES = 60;
export const DEFAULT_TAKER_FEE_RATE = Number(process.env.TAKER_FEE_RATE ?? '0.0006');
export const DEFAULT_NOTIONAL_USDT = 100;
// NOTE: the old flat signal_strength≥MEDIUM budget gate was replaced by the
// actionability gate (evaluateActionability in lib/swing/signals.ts) — a confirmed-structure
// OR bounce-with-room predicate derived from the decision history. signal_strength
// is now used only by postprocessDecision's exception thresholds.

export const NANO_TIMEFRAME = '15m';
export const MICRO_TIMEFRAME = '1H';
export const PRIMARY_TIMEFRAME = '4H';
export const MACRO_TIMEFRAME = '1D';
export const CONTEXT_TIMEFRAME = '1W';
