#!/usr/bin/env bash
# resolver_pending_sampler.sh
#
# Hourly admissibility sampler for the DID resolver pending pool.
# Emits exactly one JSONL row per run, read-only against labeler.sqlite.
#
# WHY THIS EXISTS:
#   A single snapshot (2026-06-24) showed ~324k live DIDs with resolver_status
#   NULL, ALL never_attempted, ~92% older than 24h, oldest 10.4d -- while
#   resolver throughput looked healthy (~22k attempts/hr, all succeeding,
#   pending count flat). Green throughput was hiding a starved aged tail.
#   A single snapshot cannot tell "actively drained, bounded at 10d" from
#   "tracking only goes back to the 2026-06-14 restart" (an observability
#   floor, not a real cap). This sampler produces the time series that can.
#   See docs/resolver-pending-aged-tail.md.
#
# ADMISSIBILITY: each row carries query_version so a later schema change is
#   traceable. Append-only JSONL, never pretty-printed.
#
# query_version history:
#   resolver_pending_v1 (2026-06-24) -- initial field set.
set -euo pipefail

DB="${DRIFTWATCH_DB:-/mnt/zonestorage/driftwatch/data/labeler.sqlite}"
OUT="${RESOLVER_SAMPLE_OUT:-/mnt/zonestorage/driftwatch/data/resolver_pending_samples.jsonl}"
QV="resolver_pending_v1"

# Single read-only query; emits one JSON object. julianday() parses the stored
# ISO8601+offset timestamps (verified). "pending" == status NULL or 'unresolved'.
row="$(sqlite3 -readonly "$DB" <<SQL
.timeout 60000
SELECT json_object(
  'query_version','${QV}',
  'ts_utc',                  strftime('%Y-%m-%dT%H:%M:%SZ','now'),
  'pending_total',           (SELECT COUNT(*) FROM actor_identity_current WHERE resolver_status IS NULL OR resolver_status='unresolved'),
  'pending_never_attempted', (SELECT COUNT(*) FROM actor_identity_current WHERE (resolver_status IS NULL OR resolver_status='unresolved') AND resolver_last_attempt_at IS NULL),
  'pending_attempted',       (SELECT COUNT(*) FROM actor_identity_current WHERE (resolver_status IS NULL OR resolver_status='unresolved') AND resolver_last_attempt_at IS NOT NULL),
  'oldest_pending_hours',    (SELECT ROUND((julianday('now')-julianday(MIN(first_seen_at)))*24.0,2) FROM actor_identity_current WHERE resolver_status IS NULL OR resolver_status='unresolved'),
  'pending_gt_24h',          (SELECT COUNT(*) FROM actor_identity_current WHERE (resolver_status IS NULL OR resolver_status='unresolved') AND (julianday('now')-julianday(first_seen_at))*24.0 >= 24),
  'pending_gt_72h',          (SELECT COUNT(*) FROM actor_identity_current WHERE (resolver_status IS NULL OR resolver_status='unresolved') AND (julianday('now')-julianday(first_seen_at))*24.0 >= 72),
  'pending_gt_168h',         (SELECT COUNT(*) FROM actor_identity_current WHERE (resolver_status IS NULL OR resolver_status='unresolved') AND (julianday('now')-julianday(first_seen_at))*24.0 >= 168),
  'attempts_last_hour',      (SELECT COUNT(*) FROM actor_identity_current WHERE julianday(resolver_last_attempt_at) > julianday('now','-1 hour')),
  'resolved_last_hour',      (SELECT COUNT(*) FROM actor_identity_current WHERE julianday(resolver_last_success_at) > julianday('now','-1 hour')),
  'errors_last_hour',        (SELECT COUNT(*) FROM actor_identity_current WHERE resolver_status='error' AND julianday(resolver_last_attempt_at) > julianday('now','-1 hour')),
  'new_pending_last_hour',   (SELECT COUNT(*) FROM actor_identity_current WHERE (resolver_status IS NULL OR resolver_status='unresolved') AND julianday(first_seen_at) > julianday('now','-1 hour')),
  'status_ok',               (SELECT COUNT(*) FROM actor_identity_current WHERE resolver_status='ok'),
  'status_error',            (SELECT COUNT(*) FROM actor_identity_current WHERE resolver_status='error'),
  'status_not_found',        (SELECT COUNT(*) FROM actor_identity_current WHERE resolver_status='not_found')
);
SQL
)"

if [ -z "${row}" ]; then
  echo "resolver_pending_sampler: empty result (db locked/unavailable?)" >&2
  exit 1
fi

printf '%s\n' "${row}" >> "${OUT}"
