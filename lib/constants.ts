export const BITGET_ACCOUNT_TYPE = 'usdt-futures';
// All AI traffic routes through the Vercel AI Gateway (BYOK provider keys are
// configured in the gateway dashboard, not here). Model ids use the gateway's
// `provider/model` slug format — version numbers with dots, not hyphens.
// 2026-09-03: switched openai/gpt-5.6-sol → zai/glm-5.3 for cost (AA index
// 60 vs 56; $1.40/$4.40 per 1M in/out vs $2/$10). 2026-09-11: switched BACK
// to gpt-5.6-sol — the model swap landed in the same commit as the prompt/
// gate overhaul, so the losing week that followed could not be attributed
// (docs/week-one-review-2026-09-10.md §4, §12). One variable at a time: sol
// is the model the pre-overhaul baseline ran on, and it stays fixed until
// the measurement window closes. Every non-Anthropic id speaks the OpenAI
// dialect through the gateway — see dialectForAiModel in lib/aiModel.ts.
export const DEFAULT_AI_MODEL = 'openai/gpt-5.6-sol';
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

// Short UI labels for the sizing gates that can refuse an entry and rewrite the
// action to HOLD (analyze.ts dropEntry). Keyed by decision.entry_dropped so the
// dashboard can render "REVERSE ✕ margin" instead of a bare, misleading HOLD.
export const DROPPED_ENTRY_LABEL: Record<string, string> = {
    insufficient_available_margin: 'margin',
    risk_budget_below_min_size: 'min size',
    resting_entry_unplaceable: 'unplaceable',
};
