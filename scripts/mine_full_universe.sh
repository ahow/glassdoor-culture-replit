#!/usr/bin/env bash
# Full-universe dictionary mining loop (resumable via pipeline_output/mine_state.pkl).
# Reads reviews from the PRODUCTION database (read-only streaming) so the mined
# dictionary covers all sectors, not just the finance analysis set.
set -u
cd "$(dirname "$0")/.."

APP=glassdoor-culture-replit
PROD_URL=$(curl -s -H "Authorization: Bearer $HEROKU_API_KEY" \
  -H "Accept: application/vnd.heroku+json; version=3" \
  https://api.heroku.com/apps/$APP/config-vars \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['DATABASE_URL'])")

if [ -z "$PROD_URL" ]; then
  echo "FATAL: could not fetch production DATABASE_URL" >&2
  sleep 300
  exit 1
fi

echo "=== Full-universe mining loop started $(date -u) ==="
while true; do
  OUT=$(DATABASE_URL="$PROD_URL" python3 pipeline/build_dictionaries.py mine --chunk 25000 2>&1)
  echo "$OUT" | tail -2
  if echo "$OUT" | grep -q "MINING COMPLETE"; then
    echo "=== MINING COMPLETE $(date -u) — waiting for finalize step ==="
    break
  fi
  if echo "$OUT" | grep -qi "error\|Traceback"; then
    echo "$OUT" | tail -20
    echo "--- error; retrying in 60s (state is checkpointed) ---"
    # Credentials may have rotated; re-fetch.
    PROD_URL=$(curl -s -H "Authorization: Bearer $HEROKU_API_KEY" \
      -H "Accept: application/vnd.heroku+json; version=3" \
      https://api.heroku.com/apps/$APP/config-vars \
      | python3 -c "import json,sys; print(json.load(sys.stdin)['DATABASE_URL'])")
    sleep 60
  fi
done
# Keep the workflow alive (idle) so it doesn't restart-loop after completion.
sleep infinity
