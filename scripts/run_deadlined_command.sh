#!/bin/sh
set -eu

# Execute one diagnostic in a dedicated process group. The retained receipt
# contains no command line, environment, or output—only lifecycle facts.
deadline=${DIAGNOSTIC_DEADLINE_SECONDS:-30}
kill_after=${DIAGNOSTIC_KILL_AFTER_SECONDS:-5}
receipt=${DIAGNOSTIC_RECEIPT:?DIAGNOSTIC_RECEIPT is required}
shift_count=0
if [ "${1:-}" = "--" ]; then
  shift_count=1
fi
[ "$shift_count" -eq 0 ] || shift
[ "$#" -gt 0 ] || { echo 'no diagnostic command' >&2; exit 64; }

umask 077
mkdir -p "$(dirname "$receipt")"
started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
argv_hash=$(printf '%s\000' "$@" | sha256sum | awk '{print $1}')
partial="${receipt}.partial.$$"

setsid timeout --foreground --signal=TERM --kill-after="$kill_after" "$deadline" "$@" &
leader=$!
set +e
wait "$leader"
rc=$?
set -e

cleanup_required=false
if kill -0 -- "-$leader" 2>/dev/null; then
  cleanup_required=true
  kill -TERM -- "-$leader" 2>/dev/null || true
  sleep 1
  kill -KILL -- "-$leader" 2>/dev/null || true
fi
if kill -0 -- "-$leader" 2>/dev/null; then
  cleanup_verified=false
else
  cleanup_verified=true
fi

finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)
printf '{"schema":"atproto.deadlined-diagnostic.v1","started_at":"%s","finished_at":"%s","deadline_seconds":%s,"exit_code":%s,"argv_sha256":"%s","cleanup_required":%s,"cleanup_verified":%s}\n' \
  "$started" "$finished" "$deadline" "$rc" "$argv_hash" \
  "$cleanup_required" "$cleanup_verified" >"$partial"
mv "$partial" "$receipt"

[ "$cleanup_verified" = true ] || exit 70
exit "$rc"
