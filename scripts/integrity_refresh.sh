#!/bin/zsh
set -euo pipefail

REPO_ROOT="/Users/susmitasingh/Documents/GitHub/Stock_Volatility_App"
LOG_DIR="$REPO_ROOT/logs"

cd "$REPO_ROOT"
mkdir -p "$LOG_DIR"

# Load AV_API_KEY (and any other vars) from the repo .env file
set -a
. backend/app/.env
set +a

# Keep Alpha Vantage safely under 5 req/min.
: "${AV_SLEEP_SECONDS:=12}"
export AV_SLEEP_SECONDS

# Run integrity refresh (compact fetch) and append output to log.
/Users/susmitasingh/miniconda3/bin/conda run -p /Users/susmitasingh/miniconda3 --no-capture-output \
  python -c 'from backend.app.services.refresh_service import run_integrity_refresh; import json; print(json.dumps(run_integrity_refresh(every_days=80), indent=2))' \
  >> "$LOG_DIR/cron_integrity_refresh.log" 2>&1
