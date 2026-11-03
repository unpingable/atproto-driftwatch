#!/bin/sh
set -eu

# Host-side companion to DRIFTWATCH_OPERATOR_PAUSED. The default is fixed for
# production; the environment override exists only for isolated qualification.
pause_sentinel=${DRIFTWATCH_OPERATOR_PAUSE_SENTINEL:-/opt/driftwatch/deploy/.operator-paused}

if [ -e "$pause_sentinel" ]; then
    echo "driftwatch parquet mirror: suppressed by operator pause"
    exit 0
fi

if [ "${1:-}" = "--check-only" ]; then
    echo "driftwatch parquet mirror: enabled"
    exit 0
fi

exec /usr/local/bin/parquet_mirror_claim_history.py --all
