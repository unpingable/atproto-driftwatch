# Driftwatch data contract

Driftwatch is an observatory, not a system of record. It may lose history. It
must never claim continuity or coverage it cannot substantiate.

**Historical observation data is not required for service recovery unless an
explicitly identified capability depends on it.**

## What it observes, and from where

Driftwatch reads the public Bluesky Jetstream WebSocket feed (post and repost
commits, plus identity and account events). From posts it derives claim
fingerprints and claim history. It also resolves DIDs to their current PDS
through the PLC directory or `did:web` documents. Everything it holds is
derived from those public sources; it is not the authority for any of it.

## Three independent guarantees

**Retention** is how long data is kept while the store exists. Raw event JSON
is cleared after about a day, event and edge rows after one to two weeks, and
claim history after about two weeks, when it is archived to Parquet. The facts
export keeps 30 days and recomputes hourly rollups over the last 72 hours.
These are upper bounds, not promises: retention says nothing about whether the
store survives.

**Observation continuity** is whether Driftwatch watched without a break. Each
store has a generation id, minted when the store is created. Every consumer
start is recorded as resuming from the store's durable cursor, starting from a
cursor an operator supplied, or starting unbounded with no cursor. For an
unbounded start, the first observed event time is the lower bound of
substantiated coverage, the coverage floor. Continuity is claimed only from
that floor forward, and only across restarts that resumed from the store's
own cursor. Queue drops, rollback losses and parse failures are counted and
reported as degraded coverage, never hidden.

**Recovery** is whether the service can run again. It can always start from an
empty store. It needs no previous data to do so.

## What happens on loss

If the primary store is lost, the next start creates a new store with a new
generation, starts from the live tail of Jetstream, and records an unbounded
start. Nothing is replayed. Replay only happens if an operator sets
`DRIFTWATCH_START_CURSOR`. That cursor is used only when the store has no
durable cursor, and the start is recorded as operator-supplied. While a session
that began without its own cursor is running, `cursor_continuity` reports
UNKNOWN and says why. It reports PRESENT again only after a restart that
resumes from the new store's cursor, and even then the reason names the
coverage floor.

The durable cursor commits atomically with completed source processing and
replay receipts. It trails the completed frontier by five seconds for
idempotent replay. A received or queued event never advances it. The source
endpoint is bound to the store; changing it requires explicit replay-custody
reconciliation. An operator cursor cannot override an existing durable cursor.

No prior observation frontier is kept outside the lost store. Every empty
start therefore assumes the earlier boundary is unknown, including a first
installation. The restart time is not used as the start of the missing-history
interval. A new generation and coverage floor do not reconstruct the previous
generation's history or establish what happened during the missing interval.

The facts export records the source store's generation. If the generation
changes, or the source's highest claim rowid falls below the export's
watermark, the export discards its derived tables and rebuilds from the
current source instead of skipping new claims. The health baseline checkpoint
carries the generation that wrote it and is refused on any other store, so a
new store warms up cold. A same-generation baseline also needs the existing
earned-standing provenance; lineage does not relax that requirement. A
lineage-triggered export rebuild still requires the configured workspace
admission and runtime guards. Refusal keeps the last published snapshot;
interruption closes the working connection before retry.

Once an export has a known source generation, a source without readable
lineage is refused before any working-store writes, regardless of its rowids.
The export preserves its previous generation, watermark, working rows and
published snapshot. It does not initialize or repair the source. This also
applies to the standalone exporter reading a store from an older image.
Failures while opening or reading that source close the export's working
connection and every source connection the export owns.

## What to preserve outside the store

Keep the deployment configuration, service hostname and route, selected image
identity, and operator credentials in their existing deployment custody.
In particular, `ADMIN_API_TOKEN` belongs in the deployment environment, not
SQLite. The token is not included in operations status. Restoring observation
data is not required to re-establish the same configured endpoint.

Current sealed-lab operation is detect-only, with no registered labeler and no
network signing path. If signing is introduced, its identity and private-key
custody need a separate contract; a new observation generation must not mint a
replacement service identity.

Interpretation state is either configured or rebuildable. The subscription
endpoint/collections and label-query endpoint come from deployment
configuration; fingerprint rules and versions come from the selected source.
Current DID handles, account state and PDS mappings are rebuilt from new
identity/account events and DID-document resolution. They describe current
state, not recovered historical identity. Until rebuilt, missing mappings are
missing evidence. Old label ledgers and old label definitions cannot be
inferred from a newly empty store.

## State whose loss affects continuity or identity

- The primary store. It holds the durable cursor, the store generation and
  the coverage-start records. Losing it ends the lineage, and continuity
  cannot be carried across. It also holds the emitted-label and
  label-decision ledgers.
- Archived claim history, the Parquet partitions. Losing these shortens
  history but does not affect live continuity. By default they live on the
  same data volume as the store.
- Labeler signing identity, if emission is ever enabled. Losing it changes who
  the labeler is, not what was observed.

Derived state can be rebuilt or relearned: the facts sidecar and its
snapshot, the health baseline checkpoint, and the ops status fact files.

## What it never claims

- Coverage before the coverage floor of the current lineage, or continuity
  between two store generations.
- That `fingerprint_bounds.first_seen_epoch` is when a claim first appeared.
  It is the earliest claim in the current export, timed by the post's
  author-supplied `createdAt`. Compare it with `coverage_start_epoch` in the
  export meta. An empty value means no floor has been substantiated yet.
- That something absent from its data was absent from the network.
- Complete coverage of Jetstream while drops or lag are being reported.

## Known remaining limitations

- Labels emitted before a store loss cannot be expired afterwards, because the
  `labels` table was in the lost store. Driftwatch currently runs detect-only.
- A resume from a durable cursor older than Jetstream's own replay window may
  silently start later than the cursor. Driftwatch does not detect this.
- A store that existed before lineage tracking is marked `adopted`. It has no
  recorded coverage floor until its next cursorless start.
- When the export rebuilds after a generation change, it also discards older
  facts that were still within retention.
- The DuckDB/Parquet snapshot writer does not yet carry the source generation
  or a coverage floor.
