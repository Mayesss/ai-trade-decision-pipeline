import React, { useEffect, useState } from 'react';

type Evaluation = {
  overall_rating?: number;
  overview?: string;
  what_went_well?: string[];
  issues?: string[];
  improvements?: string[];
  confidence?: string;
  aspects?: Record<
    string,
    {
      rating?: number;
      comment?: string;
    }
  >;
};

type EvaluationEntry = {
  symbol: string;
  evaluation: Evaluation;
  pnl24h?: number | null;
  lastDecision?: {
    action?: string;
    summary?: string;
    reason?: string;
    signal_strength?: string;
    [key: string]: any;
  } | null;
  lastMetrics?: Record<string, any> | null;
};

type EvaluationsResponse = {
  symbols: string[];
  data: EvaluationEntry[];
};

export default function Home() {
  const [symbols, setSymbols] = useState<string[]>([]);
  const [active, setActive] = useState(0);
  const [tabData, setTabData] = useState<Record<string, EvaluationEntry>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadEvaluations = async () => {
    try {
      setLoading(true);
      const res = await fetch('/api/evaluations');
      if (!res.ok) {
        throw new Error(`Request failed with status ${res.status}`);
      }
      const json: EvaluationsResponse = await res.json();
      setSymbols(json.symbols || []);
      const mapped: Record<string, EvaluationEntry> = {};
      for (const entry of json.data || []) {
        mapped[entry.symbol] = entry;
      }
      setTabData(mapped);
      setActive(0);
      setError(null);
    } catch (err: any) {
      setError(err?.message || 'Failed to load evaluations');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadEvaluations();
  }, []);

  const current = symbols[active] ? tabData[symbols[active]] : null;

  return (
    <div className="page">
      <div className="card">
        <h1 className="title">AI Trade Dashboard</h1>
        {error && <div className="error">Could not load evaluations: {error}</div>}
        {!error && (
          <div className="tabs">
            {symbols.map((sym, i) => (
              <button
                key={sym}
                className={`tab ${i === active ? 'tab--active' : ''}`}
                onClick={() => setActive(i)}
              >
                {sym}
              </button>
            ))}
          </div>
        )}
        {loading ? (
          <div className="empty">Loading...</div>
        ) : !symbols.length ? (
          <div className="empty">No evaluations found.</div>
        ) : current ? (
          <div className="stack">
            <div className="panel">
              <div className="panel__title">24h PnL (from last decision)</div>
              <div className="panel__text">
                <span
                  className={`pnl ${
                    typeof current.pnl24h === 'number'
                      ? current.pnl24h >= 0
                        ? 'pnl--up'
                        : 'pnl--down'
                      : ''
                  }`}
                >
                  {typeof current.pnl24h === 'number' ? `${current.pnl24h.toFixed(2)}%` : '—'}
                </span>
              </div>
            </div>
            <div className="panel">
              <div className="panel__title">Latest Evaluation</div>
              <div className="panel__meta">
                Rating: <span className="accent">{current.evaluation.overall_rating ?? '—'}</span>
              </div>
              <div className="panel__text">
                {current.evaluation.overview || 'No overview provided.'}
              </div>
            </div>
            {current.evaluation.aspects && (
              <div className="panel">
                <div className="panel__title">Aspect Ratings</div>
                <div className="grid">
                  {Object.entries(current.evaluation.aspects).map(([key, val]) => (
                    <div key={key} className="aspect">
                      <div className="aspect__label">{key.replace(/_/g, ' ')}</div>
                      <div className="aspect__rating">
                        <span className="accent">{val?.rating ?? '—'}</span>
                        <span className="aspect__comment">{val?.comment || 'No comment'}</span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
            {(current.evaluation.what_went_well?.length ||
              current.evaluation.issues?.length ||
              current.evaluation.improvements?.length) && (
              <div className="panel">
                <div className="panel__title">Details</div>
                {current.evaluation.what_went_well?.length ? (
                  <div className="list">
                    <div className="list__title">What went well</div>
                    <ul>
                      {current.evaluation.what_went_well.map((item, idx) => (
                        <li key={idx}>{item}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
                {current.evaluation.issues?.length ? (
                  <div className="list">
                    <div className="list__title">Issues</div>
                    <ul>
                      {current.evaluation.issues.map((item, idx) => (
                        <li key={idx}>{item}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
                {current.evaluation.improvements?.length ? (
                  <div className="list">
                    <div className="list__title">Improvements</div>
                    <ul>
                      {current.evaluation.improvements.map((item, idx) => (
                        <li key={idx}>{item}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
                {current.evaluation.confidence && (
                  <div className="panel__meta">Confidence: {current.evaluation.confidence}</div>
                )}
              </div>
            )}
            {(current.lastDecision || current.lastMetrics) && (
              <div className="panel">
                <div className="panel__title">Latest Decision & Metrics</div>
                {current.lastDecision && (
                  <div className="panel__text">
                    Action:{' '}
                    <span className="accent">{(current.lastDecision.action || '').toString() || '—'}</span>
                    {current.lastDecision.signal_strength
                      ? ` · Strength: ${current.lastDecision.signal_strength}`
                      : ''}
                    {current.lastDecision.summary ? ` · ${current.lastDecision.summary}` : ''}
                  </div>
                )}
                {current.lastMetrics && (
                  <div className="metrics">
                    {Object.entries(current.lastMetrics).map(([key, val]) => (
                      <div key={key} className="metric">
                        <div className="metric__label">{key}</div>
                        <div className="metric__value">{String(val)}</div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        ) : (
          <div className="empty">Loading...</div>
        )}
      </div>

      <style jsx>{`
        :global(body) {
          margin: 0;
          font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
          background: radial-gradient(circle at 20% 20%, #eef2ff, transparent 35%),
            radial-gradient(circle at 80% 0%, #fef3c7, transparent 30%), #f6f7fb;
          color: #111827;
        }
        .page {
          min-height: 100vh;
          display: flex;
          align-items: center;
          justify-content: center;
          padding: 32px 16px;
        }
        .card {
          width: 100%;
          max-width: 720px;
          background: rgba(255, 255, 255, 0.9);
          backdrop-filter: blur(10px);
          border-radius: 18px;
          padding: 28px;
          box-shadow: 0 20px 50px rgba(15, 23, 42, 0.1);
          border: 1px solid #e5e7eb;
        }
        .title {
          margin: 0 0 18px;
          text-align: center;
          font-size: 28px;
          font-weight: 700;
          letter-spacing: -0.02em;
        }
        .tabs {
          display: flex;
          gap: 10px;
          justify-content: center;
          margin-bottom: 18px;
          flex-wrap: wrap;
        }
        .tab {
          border: 1px solid #e5e7eb;
          background: #f3f4f6;
          color: #374151;
          padding: 8px 16px;
          border-radius: 999px;
          font-size: 14px;
          font-weight: 600;
          cursor: pointer;
          transition: all 0.2s ease;
        }
        .tab:hover {
          background: #e0f2fe;
          border-color: #bfdbfe;
          color: #1d4ed8;
        }
        .tab--active {
          background: linear-gradient(135deg, #2563eb, #1d4ed8);
          color: #fff;
          border-color: transparent;
          box-shadow: 0 10px 20px rgba(37, 99, 235, 0.25);
        }
        .stack {
          display: flex;
          flex-direction: column;
          gap: 14px;
        }
        .row {
          display: flex;
          justify-content: space-between;
          align-items: center;
          font-weight: 600;
          color: #374151;
        }
        .label {
          color: #4b5563;
        }
        .pnl {
          font-size: 18px;
          font-weight: 700;
        }
        .pnl--up {
          color: #16a34a;
        }
        .pnl--down {
          color: #dc2626;
        }
        .panel {
          background: #f9fafb;
          border: 1px solid #e5e7eb;
          border-radius: 12px;
          padding: 14px;
        }
        .grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
          gap: 12px;
        }
        .aspect {
          padding: 10px;
          border: 1px solid #e5e7eb;
          border-radius: 10px;
          background: #fff;
        }
        .aspect__label {
          font-weight: 600;
          color: #1f2937;
          margin-bottom: 4px;
          text-transform: capitalize;
        }
        .aspect__rating {
          display: flex;
          flex-direction: column;
          gap: 4px;
          color: #4b5563;
        }
        .aspect__comment {
          font-size: 13px;
          color: #6b7280;
        }
        .panel__title {
          font-weight: 700;
          color: #1f2937;
          margin-bottom: 4px;
        }
        .metrics {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
          gap: 8px;
          margin-top: 10px;
        }
        .metric {
          border: 1px solid #e5e7eb;
          border-radius: 8px;
          padding: 8px;
          background: #fff;
        }
        .metric__label {
          font-size: 12px;
          color: #6b7280;
          margin-bottom: 2px;
        }
        .metric__value {
          font-weight: 700;
          color: #111827;
          word-break: break-word;
        }
        .panel__meta {
          font-size: 12px;
          color: #6b7280;
          margin-bottom: 6px;
        }
        .panel__text {
          font-size: 14px;
          color: #374151;
          line-height: 1.5;
        }
        .strong {
          font-weight: 700;
        }
        .accent {
          color: #2563eb;
          font-weight: 700;
        }
        .list {
          margin-top: 10px;
        }
        .list__title {
          font-weight: 700;
          margin-bottom: 4px;
          color: #111827;
        }
        ul {
          margin: 0;
          padding-left: 18px;
          color: #374151;
        }
        .empty {
          text-align: center;
          color: #9ca3af;
          padding: 40px 0;
          font-weight: 600;
        }
        .error {
          background: #fef2f2;
          color: #991b1b;
          border: 1px solid #fecdd3;
          padding: 10px 12px;
          border-radius: 10px;
          margin-bottom: 12px;
          font-weight: 600;
          text-align: center;
        }
        @media (max-width: 640px) {
          .card {
            padding: 22px;
          }
          .title {
            font-size: 24px;
          }
        }
      `}</style>
    </div>
  );
}
