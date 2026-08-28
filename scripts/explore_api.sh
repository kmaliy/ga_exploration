#!/usr/bin/env bash
# Probe both endpoints and log every response with its status code.
# Usage:
#   export GA_API_KEY=...
#   ./scripts/explore_api.sh | tee artifacts/reports/api_exploration.log
set -euo pipefail

: "${GA_API_KEY:?Set GA_API_KEY in your environment first (see .env.example)}"
BASE_URL="${GA_API_BASE_URL:-https://dish-second-course-gateway-2tximoqc.nw.gateway.dev}"

probe() {
  local title="$1"; shift
  echo "=============================================================="
  echo "## ${title}"
  echo "--------------------------------------------------------------"
  curl -sS -G -w '\n[http_status=%{http_code} time=%{time_total}s]\n' \
    -H "X-API-Key: ${GA_API_KEY}" "$@" || true
  echo
}

# --- daily-visits: shape, envelope, pagination -----------------------------
probe "daily-visits: first page, small limit" \
  --data-urlencode "page=1" --data-urlencode "limit=5" \
  "${BASE_URL}/daily-visits"

probe "daily-visits: date-range filter" \
  --data-urlencode "start_date=2016-08-01" --data-urlencode "end_date=2016-08-05" \
  "${BASE_URL}/daily-visits"

probe "daily-visits: past-the-end page (pagination termination behaviour)" \
  --data-urlencode "page=9999" --data-urlencode "limit=100" \
  "${BASE_URL}/daily-visits"

# --- ga-sessions-data: nested shape, filters -------------------------------
probe "ga-sessions: one day, small limit (inspect nesting)" \
  --data-urlencode "page=1" --data-urlencode "limit=3" \
  --data-urlencode "date=20170801" \
  "${BASE_URL}/ga-sessions-data"

probe "ga-sessions: country + device filters" \
  --data-urlencode "date=20160801" --data-urlencode "country=United States" \
  --data-urlencode "device_category=desktop" --data-urlencode "limit=3" \
  "${BASE_URL}/ga-sessions-data"

# Does the undocumented channel_grouping filter actually filter, or is it only
# echoed back in filters_applied? Compare total_records against the unfiltered
# call for the same day (20170801 has 2556 sessions in total).
probe "ga-sessions: baseline for 20170801 (unfiltered, for comparison)" \
  --data-urlencode "date=20170801" --data-urlencode "limit=1" \
  "${BASE_URL}/ga-sessions-data"

probe "ga-sessions: undocumented channel_grouping filter" \
  --data-urlencode "date=20170801" --data-urlencode "channel_grouping=Organic Search" \
  --data-urlencode "limit=3" \
  "${BASE_URL}/ga-sessions-data"

# --- edge cases ------------------------------------------------------------
probe "edge: date outside documented range" \
  --data-urlencode "date=20200101" --data-urlencode "limit=3" \
  "${BASE_URL}/ga-sessions-data"

probe "edge: malformed date format (YYYY-MM-DD where YYYYMMDD expected)" \
  --data-urlencode "date=2016-08-01" --data-urlencode "limit=3" \
  "${BASE_URL}/ga-sessions-data"

echo "=============================================================="
echo "## edge: missing API key (expect 401/403)"
echo "--------------------------------------------------------------"
curl -sS -G -w '\n[http_status=%{http_code} time=%{time_total}s]\n' \
  --data-urlencode "limit=1" \
  "${BASE_URL}/daily-visits" || true
echo

echo "Done. Findings are documented in docs/api.md."
