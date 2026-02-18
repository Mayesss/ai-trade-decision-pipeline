export type AnalysisPlatform = 'bitget' | 'capital';
export type NewsSource = 'coindesk' | 'marketaux';

export function resolveAnalysisPlatform(value?: string | null): AnalysisPlatform {
    const raw = String(value ?? 'bitget')
        .trim()
        .toLowerCase();
    return raw === 'capital' ? 'capital' : 'bitget';
}

export function resolveNewsSource(platform: AnalysisPlatform, value?: string | null): NewsSource {
    const raw = String(value ?? '')
        .trim()
        .toLowerCase();
    if (raw === 'marketaux') return 'marketaux';
    if (raw === 'coindesk') return 'coindesk';
    return platform === 'capital' ? 'marketaux' : 'coindesk';
}

export function resolveInstrumentId(symbol: string, platform: AnalysisPlatform): string {
    return `${platform}:${symbol.toUpperCase()}`;
}
