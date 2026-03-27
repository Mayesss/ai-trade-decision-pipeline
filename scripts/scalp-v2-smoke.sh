#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:3000}"
ADMIN_SECRET="${ADMIN_ACCESS_SECRET:-}"
DISCOVER_MAX_CANDIDATES="${DISCOVER_MAX_CANDIDATES:-120}"
LOAD_BATCH_SIZE="${LOAD_BATCH_SIZE:-10}"
EVALUATE_BATCH_SIZE="${EVALUATE_BATCH_SIZE:-200}"
STATE_LIMIT="${STATE_LIMIT:-300}"
PARITY_DAYS="${PARITY_DAYS:-30}"
CURL_MAX_TIME="${CURL_MAX_TIME:-120}"
RUN_LOAD_MAINTENANCE="${RUN_LOAD_MAINTENANCE:-0}"
SKIP_CONTROL=0

usage() {
  cat <<'EOF'
Scalp v2 smoke test runner (safe mode by default).

Usage:
  scripts/scalp-v2-smoke.sh [options]

Options:
  --base-url <url>         API base URL (default: http://localhost:3000)
  --admin-secret <secret>  ADMIN_ACCESS_SECRET value (optional if server has no admin secret)
  --with-load-maintenance  Include /api/scalp/v2/cron/load-candles maintenance check
  --skip-control           Do not POST safe runtime config before testing
  -h, --help               Show this help

Environment overrides:
  BASE_URL
  ADMIN_ACCESS_SECRET
  DISCOVER_MAX_CANDIDATES
  LOAD_BATCH_SIZE
  EVALUATE_BATCH_SIZE
  STATE_LIMIT
  PARITY_DAYS
  CURL_MAX_TIME
  RUN_LOAD_MAINTENANCE

Examples:
  scripts/scalp-v2-smoke.sh
  scripts/scalp-v2-smoke.sh --base-url https://my-app.vercel.app --admin-secret "$ADMIN_ACCESS_SECRET"
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url)
      BASE_URL="${2:-}"
      shift 2
      ;;
    --admin-secret)
      ADMIN_SECRET="${2:-}"
      shift 2
      ;;
    --with-load-maintenance)
      RUN_LOAD_MAINTENANCE=1
      shift
      ;;
    --skip-control)
      SKIP_CONTROL=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

for cmd in curl jq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd" >&2
    exit 1
  fi
done

BASE_URL="${BASE_URL%/}"
if [[ -z "$BASE_URL" ]]; then
  echo "Invalid base URL." >&2
  exit 1
fi

step() {
  local label="$1"
  echo
  echo "==> $label"
}

request_json() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local url="${BASE_URL}${path}"
  local tmp_file
  tmp_file="$(mktemp)"
  local status

  if [[ -n "$body" ]]; then
    if [[ -n "$ADMIN_SECRET" ]]; then
      status="$(
        curl -sS -o "$tmp_file" -w "%{http_code}" \
          --max-time "$CURL_MAX_TIME" \
          -X "$method" "$url" \
          -H "x-admin-access-secret: $ADMIN_SECRET" \
          -H "content-type: application/json" \
          --data "$body"
      )"
    else
      status="$(
        curl -sS -o "$tmp_file" -w "%{http_code}" \
          --max-time "$CURL_MAX_TIME" \
          -X "$method" "$url" \
          -H "content-type: application/json" \
          --data "$body"
      )"
    fi
  else
    if [[ -n "$ADMIN_SECRET" ]]; then
      status="$(
        curl -sS -o "$tmp_file" -w "%{http_code}" \
          --max-time "$CURL_MAX_TIME" \
          -X "$method" "$url" \
          -H "x-admin-access-secret: $ADMIN_SECRET"
      )"
    else
      status="$(
        curl -sS -o "$tmp_file" -w "%{http_code}" \
          --max-time "$CURL_MAX_TIME" \
          -X "$method" "$url"
      )"
    fi
  fi

  local out
  out="$(cat "$tmp_file")"
  rm -f "$tmp_file"

  if [[ ! "$status" =~ ^2 ]]; then
    echo "Request failed: ${method} ${path} (HTTP ${status})" >&2
    if [[ "$status" == "401" && -z "$ADMIN_SECRET" ]]; then
      echo "Tip: pass --admin-secret or export ADMIN_ACCESS_SECRET." >&2
    fi
    if echo "$out" | jq . >/dev/null 2>&1; then
      echo "$out" | jq . >&2
    else
      echo "$out" >&2
    fi
    exit 1
  fi

  if ! echo "$out" | jq . >/dev/null 2>&1; then
    echo "Non-JSON response from ${method} ${path}" >&2
    echo "$out" >&2
    exit 1
  fi

  echo "$out"
}

assert_ok() {
  local label="$1"
  local json="$2"
  local ok
  ok="$(echo "$json" | jq -r '.ok // empty')"
  if [[ "$ok" != "true" ]]; then
    echo "Expected ok=true for ${label}, got:" >&2
    echo "$json" | jq . >&2
    exit 1
  fi
}

run_cron_job() {
  local label="$1"
  local path="$2"
  local json
  json="$(request_json "GET" "$path")"
  assert_ok "$label" "$json"
  echo "$json" | jq -c '{
    ok,
    busy: (.busy // null),
    jobKind: (.job.jobKind // null),
    processed: (.job.processed // null),
    succeeded: (.job.succeeded // null),
    failed: (.job.failed // null),
    pendingAfter: (.job.pendingAfter // null),
    details: (.job.details // null)
  }'
}

step "Target: ${BASE_URL}"

if [[ "$SKIP_CONTROL" -eq 0 ]]; then
  step "Set safe runtime (enabled=true, liveEnabled=false, dryRunDefault=true)"
  control_json="$(
    request_json "POST" "/api/scalp/v2/control" \
      '{"enabled":true,"liveEnabled":false,"dryRunDefault":true}'
  )"
  assert_ok "control_post" "$control_json"
  echo "$control_json" | jq -c '{
    ok,
    enabled: .runtime.enabled,
    liveEnabled: .runtime.liveEnabled,
    dryRunDefault: .runtime.dryRunDefault
  }'
else
  step "Skip control update (--skip-control provided)"
fi

step "Read runtime config"
runtime_json="$(request_json "GET" "/api/scalp/v2/control")"
assert_ok "control_get" "$runtime_json"
echo "$runtime_json" | jq -c '{
  ok,
  enabled: .runtime.enabled,
  liveEnabled: .runtime.liveEnabled,
  dryRunDefault: .runtime.dryRunDefault,
  defaultStrategyId: .runtime.defaultStrategyId,
  defaultTuneId: .runtime.defaultTuneId
}'

step "Discover"
run_cron_job "discover" \
  "/api/scalp/v2/cron/discover?dryRun=true&includeLiveQuotes=true&maxCandidates=${DISCOVER_MAX_CANDIDATES}&autoSuccessor=false&autoContinue=false"

if [[ "$RUN_LOAD_MAINTENANCE" == "1" ]]; then
  step "Load candles maintenance"
  run_cron_job "load_candles" \
    "/api/scalp/v2/cron/load-candles?batchSize=${LOAD_BATCH_SIZE}&autoSuccessor=false&autoContinue=false"
fi

step "Evaluate"
run_cron_job "evaluate" "/api/scalp/v2/cron/evaluate?batchSize=${EVALUATE_BATCH_SIZE}"

step "Promote"
run_cron_job "promote" "/api/scalp/v2/cron/promote"

step "Execute (dry run)"
run_cron_job "execute" "/api/scalp/v2/cron/execute?dryRun=true"

step "Reconcile"
run_cron_job "reconcile" "/api/scalp/v2/cron/reconcile"

step "Full cycle (dry run)"
cycle_json="$(request_json "GET" "/api/scalp/v2/cron/cycle?dryRun=true")"
assert_ok "cycle" "$cycle_json"
echo "$cycle_json" | jq -c '{
  ok,
  discover: .out.discover.ok,
  evaluate: .out.evaluate.ok,
  promote: .out.promote.ok,
  execute: .out.execute.ok,
  reconcile: .out.reconcile.ok
}'

step "Dashboard summary snapshot"
summary_json="$(
  request_json "GET" \
    "/api/scalp/v2/dashboard/summary?eventLimit=50&ledgerLimit=50&deploymentLimit=200&jobLimit=20"
)"
assert_ok "dashboard_summary" "$summary_json"
echo "$summary_json" | jq -c '{
  ok,
  mode,
  candidates: (.summary.candidates // null),
  deployments: (.summary.deployments // null),
  enabledDeployments: (.summary.enabledDeployments // null),
  jobs: (.jobs | length),
  events: (.events | length),
  ledger: (.ledger | length)
}'

step "Ops state snapshot"
state_json="$(request_json "GET" "/api/scalp/v2/ops/state?limit=${STATE_LIMIT}&parityDays=${PARITY_DAYS}")"
assert_ok "ops_state" "$state_json"
echo "$state_json" | jq -c '{
  ok,
  mode,
  counts,
  parity
}'

echo
echo "Scalp v2 smoke test passed."
