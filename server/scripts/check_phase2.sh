#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-https://il-corridor-monitor.onrender.com}"
CORRIDOR_ID="${CORRIDOR_ID:-1}"

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required"
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required"
  exit 1
fi

TMP_JSON="$(mktemp)"
TMP_BIN="$(mktemp)"
trap 'rm -f "$TMP_JSON" "$TMP_BIN"' EXIT

check_get() {
  local path="$1"
  local code
  code="$(curl -sS -o "$TMP_JSON" -w "%{http_code}" "${BASE_URL}${path}")"
  if [[ "$code" != "200" ]]; then
    echo "[Phase II] FAIL GET ${path} -> ${code}"
    cat "$TMP_JSON"
    exit 1
  fi
  echo "[Phase II] OK   GET ${path}"
}

check_post_json() {
  local path="$1"
  local payload="$2"
  local code
  code="$(curl -sS -o "$TMP_JSON" -w "%{http_code}" -X POST "${BASE_URL}${path}" -H "Content-Type: application/json" -d "$payload")"
  if [[ "$code" != "200" ]]; then
    echo "[Phase II] FAIL POST ${path} -> ${code}"
    cat "$TMP_JSON"
    exit 1
  fi
  echo "[Phase II] OK   POST ${path}"
}

echo "[Phase II] Base URL: ${BASE_URL}"
echo "[Phase II] Corridor: ${CORRIDOR_ID}"

check_get "/api/health"
check_get "/api/feed-profiles"
check_get "/api/settings"
check_get "/api/corridors"
check_get "/api/checkpoints?corridor_id=${CORRIDOR_ID}"
check_get "/api/runs/latest?corridor_id=${CORRIDOR_ID}"
check_get "/api/runs/timeseries?corridor_id=${CORRIDOR_ID}&hours=24"
check_get "/api/models/latest?corridor_id=${CORRIDOR_ID}"
check_get "/api/analysis/insights?corridor_id=${CORRIDOR_ID}"
check_get "/api/analysis/active-strategy?corridor_id=${CORRIDOR_ID}"
check_get "/api/analysis/strategy-presets?corridor_id=${CORRIDOR_ID}"
check_get "/api/runs/recent-cameras?corridor_id=${CORRIDOR_ID}"
check_get "/api/weather/live?corridor_id=${CORRIDOR_ID}"
check_get "/api/route/traffic-map?corridor_id=${CORRIDOR_ID}"
check_get "/api/weights/current?corridor_id=${CORRIDOR_ID}"

check_post_json "/api/chat/query" "{\"corridor_id\":${CORRIDOR_ID},\"question\":\"phase2 smoke check\"}"
jq -e '.answer != null and .modelUsed != null' "$TMP_JSON" >/dev/null
echo "[Phase II] OK   chat payload shape"

check_post_json "/api/poll-now" "{}"
jq -e '.ok == true' "$TMP_JSON" >/dev/null
echo "[Phase II] OK   poll-now response"

SNAP_URL="$(curl -sS "${BASE_URL}/api/runs/recent-cameras?corridor_id=${CORRIDOR_ID}" | jq -r '.[0].snapshot_url // empty')"
if [[ -n "$SNAP_URL" ]]; then
  ENCODED_URL="$(jq -rn --arg v "$SNAP_URL" '$v|@uri')"
  CODE_PREVIEW="$(curl -sS -o "$TMP_BIN" -w "%{http_code}" "${BASE_URL}/api/camera/preview?url=${ENCODED_URL}")"
  if [[ "$CODE_PREVIEW" != "200" ]]; then
    echo "[Phase II] FAIL camera preview -> ${CODE_PREVIEW}"
    exit 1
  fi
  BYTES="$(wc -c < "$TMP_BIN" | tr -d ' ')"
  if [[ "$BYTES" -le 0 ]]; then
    echo "[Phase II] FAIL camera preview returned empty body"
    exit 1
  fi
  echo "[Phase II] OK   camera preview (${BYTES} bytes)"
else
  echo "[Phase II] SKIP camera preview (no recent camera snapshot URL)"
fi

echo "[Phase II] COMPLETE"
