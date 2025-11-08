# 🤖 Bitget AI Decision Worker

A Cloudflare Worker that connects to the **Bitget** futures API, analyzes market data with an **AI model (OpenAI-compatible)**, and produces trade decisions (`BUY`, `SELL`, `HOLD`, `CLOSE`) — optionally executing simulated or real trades.

---

## ⚙️ Overview

This Worker:
- Fetches Bitget market data (ticker, candles, trades, order book, funding, OI)
- Computes analytics (CVD, VWAP, RSI, EMA trends, liquidity map)
- Summarizes crypto sentiment from **CoinDesk**
- Prompts an AI model (e.g., `gpt-4o-mini`) to decide an action
- Optionally places a **dry-run or live** market order on Bitget
- Saves every decision in Cloudflare KV storage for history

---

## 🧩 Prerequisites

- [Node.js ≥ 18](https://nodejs.org/)
- [Cloudflare Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/install-and-update/)
- A [Cloudflare account](https://dash.cloudflare.com/)
- A **Bitget API key** (with trading enabled)
- An **OpenAI API key** (or proxy-compatible AI endpoint)
- *(optional)* A CoinDesk API key for sentiment data

---

## 🚀 Quick Setup

### 1️⃣ Install Wrangler

```bash
npm install -g wrangler
Check:

bash
Code kopieren
wrangler --version
2️⃣ Log in to Cloudflare
bash
Code kopieren
wrangler login
3️⃣ Project Structure
cpp
Code kopieren
bitget-ai-decision-worker/
│
├── wrangler.toml
├── src/
│   └── worker.ts
├── README.md
└── package.json  (optional)
4️⃣ Configure wrangler.toml
Example:

toml
Code kopieren
name = "bitget-ai-decision-worker"
main = "src/worker.ts"
compatibility_date = "2025-11-02"
compatibility_flags = ["nodejs_compat"]

[vars]
BITGET_ACCOUNT_TYPE = "usdt-futures"
AI_MODEL = "gpt-4o-mini"
AI_BASE_URL = "https://api.openai.com/v1"
MARGIN_MODE = "crossed"      # or "isolated"
DEFAULT_LEVERAGE = "1"       # 1x leverage by default

[kv_namespaces]
binding = "DECISIONS"
id = "<your_kv_namespace_id>"
5️⃣ Create the KV namespace
bash
Code kopieren
wrangler kv:namespace create "DECISIONS"
Copy the generated ID and paste it into the [kv_namespaces] section of your wrangler.toml.

6️⃣ Set secrets (secure environment variables)
Store credentials safely with Wrangler (they won’t appear in your code):

bash
Code kopieren
wrangler secret put BITGET_API_KEY
wrangler secret put BITGET_API_SECRET
wrangler secret put BITGET_API_PASSPHRASE
wrangler secret put OPENAI_API_KEY
wrangler secret put COINDESK_API_KEY   # optional
7️⃣ Test locally
Start the dev server:

bash
Code kopieren
wrangler dev
Visit http://localhost:8787 — you should see:

arduino
Code kopieren
AI Trade Decision Worker running ✅
Health check:

bash
Code kopieren
curl http://localhost:8787/health
Trigger an analysis (dry-run):

bash
Code kopieren
curl -X POST http://localhost:8787/analyze \
     -H "Content-Type: application/json" \
     -d '{"symbol":"BTCUSDT","dryRun":true}'
8️⃣ Deploy to Cloudflare
When it works locally:

bash
Code kopieren
wrangler deploy
You’ll get a live URL like:

arduino
Code kopieren
https://bitget-ai-decision-worker.<yourname>.workers.dev

🧠 Endpoints
| Method | Path                           | Description                             |
| ------ | ------------------------------ | --------------------------------------- |
| `GET`  | `/`                            | Basic status page                       |
| `GET`  | `/health`                      | Health check                            |
| `POST` | `/analyze`                     | Fetch data, run AI, and decide          |
| `POST` | `/reset`                       | Clear KV storage (optionally by symbol) |
| `GET`  | `/lastDecision?symbol=BTCUSDT` | Get last stored decision                |
| `GET`  | `/history`                     | (stub) future paging support            |
| `GET`  | `/history`                     | (stub) future paging support            |
| `GET`  | `/bitget-auth-test`            | Check bitget auth                       |
| `GET`  | `/debug-env`                   | Check env variables                     |

## TODOS

- herausfinden was das beste KI-modell zum entschiedigen ist
- Data feed, text oder json?
