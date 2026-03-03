#!/bin/zsh
set -euo pipefail

REPO_ROOT="/Users/susmitasingh/Documents/GitHub/Stock_Volatility_App"
LOG_DIR="$REPO_ROOT/logs"

cd "$REPO_ROOT"
mkdir -p "$LOG_DIR"

# Prefix any stderr output with an ISO-like local timestamp.
# This makes launchd_integrity_refresh.err easier to read.
exec 2> >(awk '{ print strftime("%Y-%m-%d %H:%M:%S%z"), $0; fflush(); }' >&2)

# Load AV_API_KEY (and any other vars) from the repo .env file
set -a
. backend/app/.env
set +a

# Keep Alpha Vantage safely under 5 req/min.
: "${AV_SLEEP_SECONDS:=12}"
export AV_SLEEP_SECONDS

# Run integrity refresh (compact fetch).
# Note: We intentionally do NOT redirect output here.
# launchd will capture stdout/stderr using StandardOutPath/StandardErrorPath.
/Users/susmitasingh/miniconda3/bin/conda run -p /Users/susmitasingh/miniconda3 --no-capture-output \
  python -c 'from backend.app.services.refresh_service import run_integrity_refresh; import json; print(json.dumps(run_integrity_refresh(every_days=10), indent=2))'
