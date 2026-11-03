"""Export driftwatch facts to a SQLite sidecar for labelwatch consumption.

Output: /app/data/facts.sqlite (container path, host path depends on deploy config)

Strategy: update working DB in place, periodically snapshot via VACUUM INTO for
atomic reads by labelwatch.

Layout:
  facts_work.sqlite — in-place working DB (updated every cycle, WAL mode)
  facts.sqlite      — read-only snapshot (VACUUM INTO, DELETE journal, atomic replace)
"""

import asyncio
import logging
import math
import os
import shutil
import sqlite3
import time

LOG = logging.getLogger("labeler.facts_export")

RETENTION_DAYS = 30
OVERLAP_HOURS = 72  # hourly rollup window (recomputed each snapshot)
BATCH_LIMIT = 500_000
DEFAULT_INTERVAL_SEC = 30 * 60  # 30 minutes
DEFAULT_SNAPSHOT_INTERVAL_SEC = 60 * 60  # 1 hour
DEFAULT_DISK_RELEASE_THRESHOLD = 0.85
DEFAULT_DISK_MIN_FREE_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_ROOT_MIN_FREE_BYTES = 15 * 1024 * 1024 * 1024
DEFAULT_SCRATCH_BUDGET_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_AGGREGATE_WAL_BUDGET_BYTES = 7 * 1024 * 1024 * 1024
DEFAULT_CONCURRENT_GROWTH_ALLOWANCE_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_RUNTIME_SECONDS = 2 * 60 * 60


class WorkspaceRefusal(RuntimeError):
    """The explicit workspace contract cannot safely admit this export."""


class SourceLineageUnavailable(RuntimeError):
    """A bound sidecar cannot trust a source whose lineage is unavailable."""


def _mount_for_path(path):
    """Return the longest matching mountinfo entry for an existing path."""
    resolved = os.path.realpath(path)
    best = None
    with open("/proc/self/mountinfo", encoding="utf-8") as stream:
        for raw in stream:
            left, sep, right = raw.rstrip("\n").partition(" - ")
            if not sep:
                continue
            fields = left.split()
            after = right.split()
            if len(fields) < 6 or len(after) < 2:
                continue
            mountpoint = fields[4].replace("\\040", " ")
            if resolved != mountpoint and not resolved.startswith(mountpoint.rstrip("/") + "/"):
                continue
            if best is None or len(mountpoint) > len(best["mountpoint"]):
                best = {
                    "mountpoint": mountpoint,
                    "filesystem": after[0],
                    "source": after[1],
                    "device": os.stat(path).st_dev,
                }
    if best is None:
        raise WorkspaceRefusal(f"no mountinfo entry for {path}")
    return best


def _allocated_bytes(path):
    """Allocated bytes below one exporter-owned path, without following links."""
    try:
        root = os.lstat(path)
    except FileNotFoundError:
        return 0
    if os.path.islink(path):
        raise WorkspaceRefusal(f"workspace path became a symlink: {path}")
    total = root.st_blocks * 512
    if not os.path.isdir(path):
        return total
    for current, dirs, files in os.walk(path, followlinks=False):
        for name in dirs + files:
            item = os.path.join(current, name)
            st = os.lstat(item)
            if os.path.islink(item):
                raise WorkspaceRefusal(f"workspace member is a symlink: {item}")
            total += st.st_blocks * 512
    return total


def _filesystem_space_plan(obligations, reserves, growth_allowance):
    """Combine phase obligations by filesystem without double-counting phases.

    ``obligations`` maps a phase to ``[(path, additional_bytes), ...]``.
    ``reserves`` is ``[(path, minimum_free_bytes), ...]``. Paths sharing a
    device share one reserve (the maximum applicable floor) and their
    simultaneous additions are summed. Mutually exclusive phases are combined
    with ``max`` rather than ``sum``.
    """
    devices = {}

    def record(path):
        mount = _mount_for_path(path)
        entry = devices.setdefault(mount["device"], {
            "device": mount["device"],
            "mountpoint": mount["mountpoint"],
            "filesystem": mount["filesystem"],
            "source": mount["source"],
            "free_bytes": shutil.disk_usage(path).free,
            "reserve_bytes": 0,
            "phases": {},
        })
        entry["free_bytes"] = min(entry["free_bytes"], shutil.disk_usage(path).free)
        return entry

    for path, reserve in reserves:
        entry = record(path)
        entry["reserve_bytes"] = max(entry["reserve_bytes"], reserve)
    for phase, items in obligations.items():
        for path, amount in items:
            entry = record(path)
            entry["phases"][phase] = entry["phases"].get(phase, 0) + amount

    admitted = True
    for entry in devices.values():
        peak_additional = max(entry["phases"].values(), default=0)
        entry["growth_allowance_bytes"] = growth_allowance
        entry["peak_additional_bytes"] = peak_additional
        entry["required_free_bytes"] = (
            entry["reserve_bytes"] + growth_allowance + peak_additional
        )
        entry["admitted"] = entry["free_bytes"] >= entry["required_free_bytes"]
        admitted = admitted and entry["admitted"]
    return {"admitted": admitted, "filesystems": devices}


class WorkspaceGuard:
    """Bound one export's runtime, filesystem floors, and owned allocations."""

    def __init__(self, plan, scratch_dir, work_path, snapshot_path):
        self.plan = plan
        self.scratch_dir = scratch_dir
        self.work_wal = work_path + "-wal"
        self.snapshot_tmp = snapshot_path + ".tmp"
        self.scratch_budget = int(os.environ.get(
            "FACTS_EXPORT_SCRATCH_BUDGET_BYTES", DEFAULT_SCRATCH_BUDGET_BYTES,
        ))
        self.wal_budget = int(os.environ.get(
            "FACTS_EXPORT_AGGREGATE_WAL_BUDGET_BYTES",
            DEFAULT_AGGREGATE_WAL_BUDGET_BYTES,
        ))
        self.output_budget = None
        self.deadline = time.monotonic() + int(os.environ.get(
            "FACTS_EXPORT_MAX_RUNTIME_SECONDS", DEFAULT_MAX_RUNTIME_SECONDS,
        ))
        self.phase = "initial"
        self.abort_reason = None

    def set_phase(self, phase, output_budget=None):
        self.phase = phase
        if output_budget is not None:
            self.output_budget = output_budget
        self.check()

    def check(self):
        if time.monotonic() > self.deadline:
            self.abort_reason = "maximum export runtime exceeded"
            raise WorkspaceRefusal(self.abort_reason)
        for entry in self.plan["filesystems"].values():
            free = shutil.disk_usage(entry["mountpoint"]).free
            floor = entry["reserve_bytes"] + entry["growth_allowance_bytes"]
            if free < floor:
                self.abort_reason = (
                    f"filesystem floor threatened at {entry['mountpoint']}: "
                    f"free={free} floor={floor}"
                )
                raise WorkspaceRefusal(self.abort_reason)
        scratch = _allocated_bytes(self.scratch_dir)
        if scratch > self.scratch_budget:
            self.abort_reason = (
                f"scratch budget exceeded: allocated={scratch} budget={self.scratch_budget}"
            )
            raise WorkspaceRefusal(self.abort_reason)
        wal = _allocated_bytes(self.work_wal)
        if wal > self.wal_budget:
            self.abort_reason = f"aggregate WAL budget exceeded: allocated={wal} budget={self.wal_budget}"
            raise WorkspaceRefusal(self.abort_reason)
        if self.output_budget is not None:
            output = _allocated_bytes(self.snapshot_tmp)
            if output > self.output_budget:
                self.abort_reason = (
                    f"snapshot output budget exceeded: allocated={output} "
                    f"budget={self.output_budget}"
                )
                raise WorkspaceRefusal(self.abort_reason)

    def progress(self):
        try:
            self.check()
            return 0
        except WorkspaceRefusal:
            return 1

    def raise_if_interrupted(self, exc):
        if self.abort_reason:
            raise WorkspaceRefusal(self.abort_reason) from exc
        raise exc


def _close_failed_export(sidecar, facts_path):
    """Rollback and close before reclaiming only this export's output path."""
    try:
        sidecar.set_progress_handler(None, 0)
        sidecar.rollback()
    finally:
        sidecar.close()
    _discard_stale_snapshot(facts_path)


def _default_facts_path():
    from .db import DATA_DIR
    return str(DATA_DIR / "facts.sqlite")


def _default_work_path():
    from .db import DATA_DIR
    return str(DATA_DIR / "facts_work.sqlite")


def _ensure_tables(sidecar):
    sidecar.executescript("""
        CREATE TABLE IF NOT EXISTS uri_fingerprint (
            post_uri       TEXT PRIMARY KEY,
            fingerprint    TEXT NOT NULL,
            created_epoch  INTEGER NOT NULL,
            rowid_src      INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_uri_fp ON uri_fingerprint(fingerprint);

        CREATE TABLE IF NOT EXISTS fingerprint_hourly (
            fingerprint    TEXT    NOT NULL,
            hour_epoch     INTEGER NOT NULL,
            event_count    INTEGER NOT NULL,
            unique_authors INTEGER NOT NULL,
            PRIMARY KEY (fingerprint, hour_epoch)
        );

        CREATE TABLE IF NOT EXISTS fingerprint_bounds (
            fingerprint      TEXT PRIMARY KEY,
            first_seen_epoch INTEGER NOT NULL,
            last_seen_epoch  INTEGER NOT NULL,
            total_claims     INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS actor_identity_facts (
            did                     TEXT PRIMARY KEY,
            handle                  TEXT,
            pds_endpoint            TEXT,
            pds_host                TEXT,
            resolver_status         TEXT,
            resolver_last_success_at TEXT,
            is_active               INTEGER,
            identity_source         TEXT
        );

        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
    """)


def _get_meta_int(sidecar, key, default=0):
    row = sidecar.execute(
        "SELECT value FROM meta WHERE key = ?", (key,)
    ).fetchone()
    if row is None:
        return default
    try:
        return int(row[0])
    except (ValueError, TypeError):
        return default


def _set_meta(sidecar, key, value):
    sidecar.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        (key, str(value)),
    )


def _upsert_uri_fingerprints(source_conn, sidecar, last_rowid, batch_max_rowid):
    """Dedup by post_uri: highest rowid wins via MAX(rowid) subquery."""
    rows = source_conn.execute("""
        SELECT ch.post_uri, ch.claim_fingerprint,
               CAST(strftime('%s', ch.createdAt) AS INTEGER),
               ch.rowid
        FROM claim_history ch
        JOIN (
            SELECT post_uri, MAX(rowid) AS max_rowid
            FROM claim_history
            WHERE rowid > ? AND rowid <= ? AND post_uri IS NOT NULL
            GROUP BY post_uri
        ) m ON ch.post_uri = m.post_uri AND ch.rowid = m.max_rowid
    """, (last_rowid, batch_max_rowid)).fetchall()

    sidecar.executemany(
        "INSERT OR REPLACE INTO uri_fingerprint (post_uri, fingerprint, created_epoch, rowid_src) "
        "VALUES (?, ?, ?, ?)",
        rows,
    )


def _recompute_hourly(source_conn, sidecar, overlap_start, now):
    overlap_start_hour = (overlap_start // 3600) * 3600

    sidecar.execute(
        "DELETE FROM fingerprint_hourly WHERE hour_epoch >= ?",
        (overlap_start_hour,),
    )

    # createdAt is ISO text ('YYYY-MM-DDTHH:MM:SS...'), so it cannot be
    # range-compared with datetime()'s 'YYYY-MM-DD HH:MM:SS' output: every
    # 'T' row on the upper bound's date sorts after it. Filter exactly on
    # integer epochs, and keep a coarse text range on createdAt so the
    # idx_claim_history_created range scan still bounds the read. The coarse
    # range is date-granular (lower: the start date, which sorts before any
    # time on that date in either separator format; upper: the day after the
    # end date), so it is a superset of the exact window.
    source_rows = source_conn.execute("""
        SELECT claim_fingerprint,
               (CAST(strftime('%s', createdAt) AS INTEGER) / 3600) * 3600,
               COUNT(*),
               COUNT(DISTINCT authorDid)
        FROM claim_history
        WHERE createdAt >= date(?, 'unixepoch')
          AND createdAt <  date(?, 'unixepoch', '+1 day')
          AND CAST(strftime('%s', createdAt) AS INTEGER) >= ?
          AND CAST(strftime('%s', createdAt) AS INTEGER) <  ?
        GROUP BY 1, 2
    """, (overlap_start, now, overlap_start, now)).fetchall()

    sidecar.executemany(
        "INSERT OR REPLACE INTO fingerprint_hourly "
        "(fingerprint, hour_epoch, event_count, unique_authors) VALUES (?, ?, ?, ?)",
        source_rows,
    )


def _recompute_bounds(sidecar):
    """Recompute from a sequential table scan with SQLite's GROUP BY sorter.

    The fingerprint-only index is not covering: reading timestamps through it
    performs a table lookup per entry in fingerprint order. On the deployed
    multi-gigabyte sidecar this kept a snapshot unfinished for hours. A table
    scan preserves the aggregate while allowing the sorter to spill to disk.
    """
    sidecar.execute("DELETE FROM fingerprint_bounds")
    sidecar.execute("""
        INSERT INTO fingerprint_bounds (fingerprint, first_seen_epoch, last_seen_epoch, total_claims)
        SELECT fingerprint, MIN(created_epoch), MAX(created_epoch), COUNT(*)
        FROM uri_fingerprint NOT INDEXED
        GROUP BY fingerprint
    """)


def _migrate_identity_facts(sidecar):
    """Ensure actor_identity_facts has the current schema.

    Handles persisted working DBs and snapshots that predate column additions.
    """
    cols = {r[1] for r in sidecar.execute("PRAGMA table_info(actor_identity_facts)").fetchall()}
    if "identity_source" not in cols:
        sidecar.execute("ALTER TABLE actor_identity_facts ADD COLUMN identity_source TEXT")
        sidecar.commit()


def _refresh_identity_facts(source_conn, sidecar):
    """Full replace of actor_identity_facts from actor_identity_current.

    Current-state table, not incremental — ~30k rows, fast full replace.
    Only exports the thin projection labelwatch needs.
    """
    _migrate_identity_facts(sidecar)
    sidecar.execute("DELETE FROM actor_identity_facts")
    rows = source_conn.execute("""
        SELECT did, handle, pds_endpoint, pds_host,
               resolver_status, resolver_last_success_at, is_active,
               identity_source
        FROM actor_identity_current
    """).fetchall()
    sidecar.executemany(
        "INSERT INTO actor_identity_facts "
        "(did, handle, pds_endpoint, pds_host, resolver_status, "
        " resolver_last_success_at, is_active, identity_source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    return len(rows)


def _get_meta_text(sidecar, key):
    row = sidecar.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def _read_source_lineage(source_conn):
    """Return (lineage dict or None, max claim_history rowid, coverage floor epoch or None)."""
    from .db import get_store_lineage, get_coverage_floor_epoch
    lineage = get_store_lineage(source_conn)
    max_rowid = source_conn.execute("SELECT MAX(rowid) FROM claim_history").fetchone()[0] or 0
    floor = get_coverage_floor_epoch(source_conn, lineage["generation_id"]) if lineage else None
    return lineage, max_rowid, floor


def _rebuild_reason(sidecar, generation, max_rowid, last_rowid):
    """Why the sidecar can no longer be extended incrementally, or None.

    The rowid watermark is only meaningful against the store that produced
    it. A different source generation, or a source whose rowids are below
    the watermark, means the store was replaced and new claims would be
    silently skipped.
    """
    previous = _get_meta_text(sidecar, "source_generation")
    if generation and previous and previous != generation:
        return "source store generation changed (%s -> %s)" % (previous, generation)
    if max_rowid < last_rowid:
        return "source max rowid %d is below checkpoint rowid %d" % (max_rowid, last_rowid)
    return None


def _reset_sidecar(sidecar, reason, now):
    """Discard everything derived from a previous source store."""
    LOG.warning("rebuilding facts sidecar: %s", reason)
    for table in ("uri_fingerprint", "fingerprint_hourly", "fingerprint_bounds"):
        sidecar.execute(f"DELETE FROM {table}")
    _set_meta(sidecar, "last_checkpoint_rowid", 0)
    _set_meta(sidecar, "last_rebuild_epoch", now)
    _set_meta(sidecar, "last_rebuild_reason", reason)
    sidecar.commit()


def _prune(sidecar, retention_start):
    sidecar.execute(
        "DELETE FROM uri_fingerprint WHERE created_epoch < ?",
        (retention_start,),
    )
    sidecar.execute(
        "DELETE FROM fingerprint_hourly WHERE hour_epoch < ?",
        (retention_start,),
    )


def _snapshot_space_plan(total_bytes, free_bytes, work_size):
    """Return a conservative admission plan for one compact snapshot.

    The completed snapshot must leave enough free space for the ingestion
    pressure brake to remain releasable.  This is deliberately stricter than
    merely proving that ``VACUUM INTO`` can start.
    """
    release_threshold = float(os.environ.get(
        "DISK_RELEASE_THRESHOLD", DEFAULT_DISK_RELEASE_THRESHOLD,
    ))
    if not 0.0 < release_threshold < 1.0:
        raise ValueError("DISK_RELEASE_THRESHOLD must be between 0 and 1")

    min_free_bytes = int(os.environ.get(
        "DISK_MIN_FREE_BYTES", DEFAULT_DISK_MIN_FREE_BYTES,
    ))
    if min_free_bytes < 0:
        raise ValueError("DISK_MIN_FREE_BYTES must not be negative")

    explicit_reserve = int(os.environ.get(
        "FACTS_SNAPSHOT_MIN_FREE_AFTER_BYTES", "0",
    ))
    if explicit_reserve < 0:
        raise ValueError("FACTS_SNAPSHOT_MIN_FREE_AFTER_BYTES must not be negative")

    release_reserve = math.ceil(total_bytes * (1.0 - release_threshold))
    reserve_bytes = max(min_free_bytes, explicit_reserve, release_reserve)
    # VACUUM INTO rewrites the same schema and live rows into a compact database
    # with the same page size. Once the WAL is fully checkpointed, the source
    # DB's page geometry is a conservative upper bound for the output. Applying
    # an uplift to the uncompacted source double-counts free/under-filled pages.
    output_budget = work_size
    required_free = output_budget + reserve_bytes
    return {
        "admitted": free_bytes >= required_free,
        "free_bytes": free_bytes,
        "output_budget_bytes": output_budget,
        "reserve_bytes": reserve_bytes,
        "required_free_bytes": required_free,
    }


def _discard_stale_snapshot(snapshot_path):
    """Remove only the fixed, unpublished VACUUM workspace, if present."""
    tmp_snap = snapshot_path + ".tmp"
    if not os.path.exists(tmp_snap):
        return 0
    allocated_bytes = os.stat(tmp_snap).st_blocks * 512
    os.remove(tmp_snap)
    LOG.warning(
        "discarded stale unpublished snapshot workspace: %s (%dMB allocated)",
        tmp_snap,
        allocated_bytes // (1024 * 1024),
    )
    return allocated_bytes


def _snapshot_preflight(work_conn, snapshot_path):
    """Measure whether one snapshot can complete without trapping ingestion."""
    data_dir = os.path.dirname(snapshot_path) or "."
    work_db_path = work_conn.execute("PRAGMA database_list").fetchone()[2]
    page_size = int(work_conn.execute("PRAGMA page_size").fetchone()[0])
    page_count = int(work_conn.execute("PRAGMA page_count").fetchone()[0])
    work_size = page_size * page_count
    if work_db_path and work_size > os.path.getsize(work_db_path):
        raise RuntimeError("checkpointed SQLite geometry exceeds database file")
    usage = shutil.disk_usage(data_dir)
    return _snapshot_space_plan(usage.total, usage.free, work_size)


def _configure_connection_temp(conn, scratch_dir):
    """Force file-backed SQLite temp work into the runner-owned directory."""
    if not scratch_dir:
        return
    if "'" in scratch_dir or "\x00" in scratch_dir:
        raise WorkspaceRefusal("unsafe scratch directory spelling")
    conn.execute("PRAGMA temp_store=FILE")
    conn.execute(f"PRAGMA temp_store_directory='{scratch_dir}'")
    configured = conn.execute("PRAGMA temp_store_directory").fetchone()
    if not configured or os.path.realpath(configured[0]) != os.path.realpath(scratch_dir):
        raise WorkspaceRefusal("SQLite did not accept the explicit temp directory")


def _validate_runtime_scratch(scratch_dir):
    if not scratch_dir:
        raise WorkspaceRefusal("explicit SQLite scratch directory is required")
    if os.path.islink(scratch_dir) or not os.path.isdir(scratch_dir):
        raise WorkspaceRefusal("SQLite scratch directory is absent, not a directory, or symlinked")
    expected = os.environ.get("FACTS_EXPORT_TEMP_MOUNT")
    if not expected:
        raise WorkspaceRefusal("FACTS_EXPORT_TEMP_MOUNT is required")
    expected = os.path.realpath(expected)
    mount = _mount_for_path(scratch_dir)
    if mount["mountpoint"] != expected:
        raise WorkspaceRefusal(
            f"scratch mount mismatch: actual={mount['mountpoint']} expected={expected}"
        )
    expected_fstype = os.environ.get("FACTS_EXPORT_TEMP_FSTYPE")
    if expected_fstype and mount["filesystem"] != expected_fstype:
        raise WorkspaceRefusal(
            f"scratch filesystem mismatch: actual={mount['filesystem']} expected={expected_fstype}"
        )
    if mount["filesystem"] in {"nfs", "nfs4"}:
        raise WorkspaceRefusal("live SQLite scratch must not use NFS")
    if os.path.realpath(os.environ.get("SQLITE_TMPDIR", "")) != os.path.realpath(scratch_dir):
        raise WorkspaceRefusal("SQLITE_TMPDIR does not name the exporter-owned scratch directory")
    probe = os.path.join(scratch_dir, f".write-probe-{os.getpid()}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(probe, flags, 0o600)
        os.close(fd)
        os.remove(probe)
    except OSError as exc:
        try:
            os.remove(probe)
        except OSError:
            pass
        raise WorkspaceRefusal(f"scratch directory is not safely writable: {exc}") from exc
    return mount


def _explicit_workspace_plan(work_conn, snapshot_path, work_path, scratch_dir):
    """Plan mutually exclusive aggregation and publication obligations."""
    snapshot = _snapshot_preflight(work_conn, snapshot_path)
    growth = int(os.environ.get(
        "FACTS_EXPORT_CONCURRENT_GROWTH_ALLOWANCE_BYTES",
        DEFAULT_CONCURRENT_GROWTH_ALLOWANCE_BYTES,
    ))
    root_reserve = int(os.environ.get(
        "FACTS_EXPORT_ROOT_MIN_FREE_BYTES", DEFAULT_ROOT_MIN_FREE_BYTES,
    ))
    scratch_budget = int(os.environ.get(
        "FACTS_EXPORT_SCRATCH_BUDGET_BYTES", DEFAULT_SCRATCH_BUDGET_BYTES,
    ))
    wal_budget = int(os.environ.get(
        "FACTS_EXPORT_AGGREGATE_WAL_BUDGET_BYTES",
        DEFAULT_AGGREGATE_WAL_BUDGET_BYTES,
    ))
    for name, value in (
        ("growth allowance", growth), ("root reserve", root_reserve),
        ("scratch budget", scratch_budget), ("WAL budget", wal_budget),
    ):
        if value < 0:
            raise ValueError(f"{name} must not be negative")
    root_path = os.environ.get("FACTS_EXPORT_ROOT_RESERVE_PATH", "/")
    output_dir = os.path.dirname(snapshot_path) or "."
    plan = _filesystem_space_plan(
        obligations={
            "aggregation": [(scratch_dir, scratch_budget), (work_path, wal_budget)],
            "publication": [(output_dir, snapshot["output_budget_bytes"])],
        },
        reserves=[
            (output_dir, snapshot["reserve_bytes"]),
            (root_path, root_reserve),
        ],
        growth_allowance=growth,
    )
    plan.update({
        "output_budget_bytes": snapshot["output_budget_bytes"],
        "ingestion_reserve_bytes": snapshot["reserve_bytes"],
        "root_reserve_bytes": root_reserve,
        "scratch_budget_bytes": scratch_budget,
        "aggregate_wal_budget_bytes": wal_budget,
        "growth_allowance_bytes": growth,
    })
    return plan


def _checkpoint_snapshot_wal(work_conn):
    """Checkpoint committed WAL and release it before output overlap."""
    busy, log_frames, checkpointed_frames = work_conn.execute(
        "PRAGMA wal_checkpoint(TRUNCATE)"
    ).fetchone()
    if busy:
        LOG.warning(
            "snapshot deferred: WAL checkpoint busy (%d log frames, %d checkpointed)",
            log_frames,
            checkpointed_frames,
        )
        return False
    return True


def _snapshot(work_conn, snapshot_path, guard=None):
    """Create a compacted snapshot via VACUUM INTO + atomic replace.

    Checks disk space before attempting — VACUUM INTO needs roughly
    the compacted output size in free space.
    """
    plan = _snapshot_preflight(work_conn, snapshot_path)
    if not plan["admitted"]:
        LOG.warning(
            "skipping snapshot: %dMB free, need %dMB (%dMB output + %dMB reserve)",
            plan["free_bytes"] // (1024 * 1024),
            plan["required_free_bytes"] // (1024 * 1024),
            plan["output_budget_bytes"] // (1024 * 1024),
            plan["reserve_bytes"] // (1024 * 1024),
        )
        return False

    tmp_snap = snapshot_path + ".tmp"
    if guard is not None:
        guard.set_phase("publication", plan["output_budget_bytes"])
    try:
        work_conn.execute(f"VACUUM INTO '{tmp_snap}'")
    except sqlite3.OperationalError as exc:
        if os.path.exists(tmp_snap):
            os.remove(tmp_snap)
        if guard is not None:
            guard.raise_if_interrupted(exc)
        raise
    except BaseException:
        if os.path.exists(tmp_snap):
            os.remove(tmp_snap)
        raise
    # VACUUM INTO produces DELETE journal mode by default, which is what we want
    os.replace(tmp_snap, snapshot_path)
    return True


def _open_source(source_conn_or_factory):
    """Open a source connection, accepting either a factory callable or a live connection.

    Returns (conn, should_close). If a factory is provided, the caller must close
    the connection when done with it to release the WAL read lock.
    """
    if isinstance(source_conn_or_factory, sqlite3.Connection):
        return source_conn_or_factory, False
    # Assume callable factory
    return source_conn_or_factory(), True


def export_once(source_conn_or_factory, facts_path=None, work_path=None, force_snapshot=False):
    """Run one export cycle: update working DB in place, optionally snapshot.

    Args:
        source_conn_or_factory: Connection to the main labeler.sqlite, or a
            callable that returns one. Using a factory is preferred — each phase
            opens and closes its own connection, releasing the WAL read lock
            between phases to prevent WAL bloat on the source DB.
        facts_path: Path for the read-only snapshot (labelwatch reads this).
        work_path: Path for the in-place working DB.
        force_snapshot: If True, always create a snapshot this cycle.
    """
    if facts_path is None:
        facts_path = _default_facts_path()
    if work_path is None:
        work_path = _default_work_path()

    scratch_dir = os.environ.get("FACTS_EXPORT_TEMP_ATTEMPT_DIR")
    require_explicit = os.environ.get(
        "FACTS_EXPORT_REQUIRE_EXPLICIT_WORKSPACE", "0",
    ).lower() in ("1", "true")
    if require_explicit:
        _validate_runtime_scratch(scratch_dir)

    # There is one exporter owner and one fixed unpublished workspace.  Clear a
    # crash remnant before this cycle creates more WAL or temporary allocation.
    _discard_stale_snapshot(facts_path)

    t0 = time.monotonic()
    now = int(time.time())
    retention_start = now - (RETENTION_DAYS * 86400)
    overlap_start = now - (OVERLAP_HOURS * 3600)
    snap_interval = int(os.environ.get("FACTS_SNAPSHOT_INTERVAL", DEFAULT_SNAPSHOT_INTERVAL_SEC))

    # Seed working DB from existing snapshot on first run
    if not os.path.exists(work_path) and os.path.exists(facts_path):
        shutil.copyfile(facts_path, work_path)
        LOG.info("seeded working DB from existing snapshot")

    # Open working DB directly (no copy-forward!)
    sidecar = sqlite3.connect(work_path)
    # Read the established binding and source before any working-store writes.
    # A standalone exporter can encounter a source made by an older image;
    # missing lineage must never erase a known generation or admit its rowids.
    try:
        has_meta = sidecar.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta'"
        ).fetchone()
        previous_generation = _get_meta_text(sidecar, "source_generation") if has_meta else None
        source_conn, should_close = _open_source(source_conn_or_factory)
        try:
            lineage, source_max_rowid, coverage_floor = _read_source_lineage(source_conn)
        finally:
            if should_close:
                source_conn.close()
        if previous_generation and not lineage:
            raise SourceLineageUnavailable(
                "source lineage is unavailable; refusing to change bound facts"
            )
        if scratch_dir:
            _validate_runtime_scratch(scratch_dir)
            _configure_connection_temp(sidecar, scratch_dir)
        sidecar.execute("PRAGMA journal_mode=WAL")
        sidecar.execute("PRAGMA busy_timeout=30000")
        sidecar.execute("PRAGMA journal_size_limit=67108864")
        _ensure_tables(sidecar)
    except BaseException:
        _close_failed_export(sidecar, facts_path)
        raise
    generation = lineage["generation_id"] if lineage else None

    # Read checkpoint rowid and last export time
    last_rowid = _get_meta_int(sidecar, "last_checkpoint_rowid", 0)
    last_export = _get_meta_int(sidecar, "last_export_epoch", 0)
    rebuild_reason = _rebuild_reason(sidecar, generation, source_max_rowid, last_rowid)
    last_snap = _get_meta_int(sidecar, "last_snapshot_epoch", 0)
    snapshot_due = force_snapshot or bool(rebuild_reason) or (now - last_snap >= snap_interval)
    rows_upserted = 0

    # A snapshot cycle can allocate WAL and SQLite sorter files while updating
    # and pruning, before the aggregate-only phase begins. Arm the same guard
    # before any of those mutations; the later checkpoints/replans refine the
    # geometry but do not retroactively make earlier allocation safe.
    guard = None
    if snapshot_due and scratch_dir:
        if not _checkpoint_snapshot_wal(sidecar):
            sidecar.close()
            return
        try:
            plan = _explicit_workspace_plan(sidecar, facts_path, work_path, scratch_dir)
            if not plan["admitted"]:
                raise WorkspaceRefusal(f"filesystem admission refused: {plan['filesystems']}")
            guard = WorkspaceGuard(plan, scratch_dir, work_path, facts_path)
            guard.set_phase("update")
            sidecar.set_progress_handler(guard.progress, 250_000)
        except BaseException:
            _close_failed_export(sidecar, facts_path)
            raise

    # Rebuilding can allocate substantial WAL, so it follows the same admission
    # and progress guard as every other sidecar mutation.
    try:
        if rebuild_reason:
            _reset_sidecar(sidecar, rebuild_reason, now)
            last_rowid = 0
            force_snapshot = True
        # Bind exported coverage to the current store, never to a prior lineage's
        # rowid watermark or author-supplied claim timestamp.
        _set_meta(sidecar, "source_generation", generation or "")
        _set_meta(sidecar, "source_store_created_at", (lineage or {}).get("created_at") or "")
        _set_meta(sidecar, "source_store_origin", (lineage or {}).get("origin") or "")
        _set_meta(sidecar, "coverage_start_epoch", "" if coverage_floor is None else coverage_floor)
        sidecar.commit()
    except BaseException as exc:
        _close_failed_export(sidecar, facts_path)
        if isinstance(exc, sqlite3.OperationalError) and guard is not None:
            guard.raise_if_interrupted(exc)
        raise

    # Phase 1: Batch upsert — bounded rowid ranges, no need for cross-phase consistency
    t_batch = time.monotonic()
    source_conn, should_close = _open_source(source_conn_or_factory)
    try:
        if guard is not None:
            _configure_connection_temp(source_conn, scratch_dir)
            source_conn.set_progress_handler(guard.progress, 250_000)
        while True:
            rows = source_conn.execute("""
                SELECT rowid, post_uri, claim_fingerprint, createdAt, authorDid
                FROM claim_history
                WHERE rowid > ? AND post_uri IS NOT NULL
                ORDER BY rowid LIMIT ?
            """, (last_rowid, BATCH_LIMIT)).fetchall()

            if not rows:
                break

            batch_max_rowid = rows[-1][0]
            rows_upserted += len(rows)

            _upsert_uri_fingerprints(source_conn, sidecar, last_rowid, batch_max_rowid)

            last_rowid = batch_max_rowid
            _set_meta(sidecar, "last_checkpoint_rowid", last_rowid)
            sidecar.commit()

            if len(rows) < BATCH_LIMIT:
                break
    finally:
        if guard is not None:
            source_conn.set_progress_handler(None, 0)
        if should_close:
            source_conn.close()
    elapsed_batch = time.monotonic() - t_batch

    # Phase 2: Prune (sidecar-only, no source connection needed)
    t_prune = time.monotonic()
    _prune(sidecar, retention_start)
    elapsed_prune = time.monotonic() - t_prune

    # Phase 3: Identity facts — short read from source, then release
    t_identity = time.monotonic()
    source_conn, should_close = _open_source(source_conn_or_factory)
    try:
        if guard is not None:
            _configure_connection_temp(source_conn, scratch_dir)
            source_conn.set_progress_handler(guard.progress, 250_000)
        identity_count = _refresh_identity_facts(source_conn, sidecar)
    finally:
        if guard is not None:
            source_conn.set_progress_handler(None, 0)
        if should_close:
            source_conn.close()
    elapsed_identity = time.monotonic() - t_identity

    # Update meta
    _set_meta(sidecar, "last_export_epoch", now)
    sidecar.commit()

    elapsed_update = time.monotonic() - t0

    # Phase 4: Snapshot — recompute expensive aggregates then VACUUM INTO
    did_snapshot = False
    elapsed_hourly = 0.0
    elapsed_bounds = 0.0
    hourly_start = overlap_start
    if snapshot_due:
        # Clear only the unpublished fixed workspace left by an interrupted
        # VACUUM, then decide before undertaking the expensive aggregates.
        if not _checkpoint_snapshot_wal(sidecar):
            sidecar.close()
            return
        if scratch_dir:
            try:
                plan = _explicit_workspace_plan(sidecar, facts_path, work_path, scratch_dir)
                if not plan["admitted"]:
                    raise WorkspaceRefusal(f"filesystem admission refused: {plan['filesystems']}")
                guard.plan = plan
                guard.set_phase("aggregation")
            except BaseException:
                _close_failed_export(sidecar, facts_path)
                raise
            LOG.info(
                "explicit workspace admitted: scratch=%s filesystems=%s",
                scratch_dir,
                {
                    str(device): {
                        "mountpoint": item["mountpoint"],
                        "filesystem": item["filesystem"],
                        "free_bytes": item["free_bytes"],
                        "required_free_bytes": item["required_free_bytes"],
                        "reserve_bytes": item["reserve_bytes"],
                        "growth_allowance_bytes": item["growth_allowance_bytes"],
                        "peak_additional_bytes": item["peak_additional_bytes"],
                    }
                    for device, item in plan["filesystems"].items()
                },
            )
        else:
            plan = _snapshot_preflight(sidecar, facts_path)
        if not plan["admitted"]:
            LOG.warning(
                "snapshot cycle deferred before aggregates: %dMB free, "
                "need %dMB (%dMB output + %dMB reserve)",
                plan["free_bytes"] // (1024 * 1024),
                plan["required_free_bytes"] // (1024 * 1024),
                plan["output_budget_bytes"] // (1024 * 1024),
                plan["reserve_bytes"] // (1024 * 1024),
            )
            sidecar.close()
            elapsed = time.monotonic() - t0
            work_mb = os.path.getsize(work_path) / (1024 * 1024)
            LOG.info(
                "facts export complete: %.1fs (batch=%.1fs, prune=%.1fs, "
                "identity=%d in %.1fs, %d new rows, work=%.0fMB, snapshot deferred)",
                elapsed, elapsed_batch, elapsed_prune, identity_count,
                elapsed_identity, rows_upserted, work_mb,
            )
            return

        # Hourly rollup: short read from source for OVERLAP_HOURS window
        t_hourly = time.monotonic()
        source_conn, should_close = _open_source(source_conn_or_factory)
        try:
            if scratch_dir:
                _configure_connection_temp(source_conn, scratch_dir)
                source_conn.set_progress_handler(guard.progress, 250_000)
            _recompute_hourly(source_conn, sidecar, hourly_start, now)
        except sqlite3.OperationalError as exc:
            if guard is not None:
                reason = guard.abort_reason
                _close_failed_export(sidecar, facts_path)
                if reason:
                    raise WorkspaceRefusal(reason) from exc
                raise
            raise
        finally:
            if scratch_dir:
                source_conn.set_progress_handler(None, 0)
            if should_close:
                source_conn.close()
        elapsed_hourly = time.monotonic() - t_hourly

        t_bounds = time.monotonic()
        try:
            _recompute_bounds(sidecar)
            sidecar.commit()
        except sqlite3.OperationalError as exc:
            if guard is not None:
                reason = guard.abort_reason
                _close_failed_export(sidecar, facts_path)
                if reason:
                    raise WorkspaceRefusal(reason) from exc
                raise
            sidecar.rollback()
            raise
        elapsed_bounds = time.monotonic() - t_bounds

        # Aggregate writes can leave gigabytes in the reusable WAL after
        # commit. Fold those pages into the main DB and release the WAL before
        # the compact output overlaps it.
        if not _checkpoint_snapshot_wal(sidecar):
            sidecar.set_progress_handler(None, 0)
            sidecar.close()
            return

        if guard is not None:
            plan = _explicit_workspace_plan(sidecar, facts_path, work_path, scratch_dir)
            if not plan["admitted"]:
                sidecar.set_progress_handler(None, 0)
                sidecar.close()
                raise WorkspaceRefusal(
                    f"filesystem admission refused after aggregates: {plan['filesystems']}"
                )
            guard.plan = plan
            guard.set_phase("publication", plan["output_budget_bytes"])

        # Snapshot: VACUUM INTO with disk space preflight
        t_snap = time.monotonic()
        try:
            snap_ok = _snapshot(sidecar, facts_path, guard=guard)
        except BaseException:
            sidecar.set_progress_handler(None, 0)
            sidecar.rollback()
            sidecar.close()
            _discard_stale_snapshot(facts_path)
            raise
        if snap_ok:
            _set_meta(sidecar, "last_snapshot_epoch", now)
            sidecar.commit()
            snap_elapsed = time.monotonic() - t_snap
            did_snapshot = True
            snap_mb = os.path.getsize(facts_path) / (1024 * 1024)
            LOG.info("snapshot created: %.0fMB in %.1fs (hourly=%.1fs, bounds=%.1fs, identity=%d in %.1fs)",
                     snap_mb, snap_elapsed, elapsed_hourly, elapsed_bounds, identity_count, elapsed_identity)
        else:
            snap_elapsed = time.monotonic() - t_snap
            LOG.warning("snapshot skipped (disk space): hourly=%.1fs, bounds=%.1fs",
                        elapsed_hourly, elapsed_bounds)

    sidecar.set_progress_handler(None, 0)
    sidecar.close()

    elapsed = time.monotonic() - t0
    work_mb = os.path.getsize(work_path) / (1024 * 1024)
    LOG.info("facts export complete: %.1fs (batch=%.1fs, prune=%.1fs, identity=%d in %.1fs, %d new rows, work=%.0fMB%s)",
             elapsed, elapsed_batch, elapsed_prune, identity_count, elapsed_identity,
             rows_upserted, work_mb,
             ", +snapshot[hourly=%.1fs bounds=%.1fs]" % (elapsed_hourly, elapsed_bounds)
             if did_snapshot else "")


async def run_periodic(facts_path=None, work_path=None):
    """Run export on a periodic loop (async-friendly)."""
    interval = int(os.environ.get("FACTS_EXPORT_INTERVAL", DEFAULT_INTERVAL_SEC))
    if facts_path is None:
        facts_path = _default_facts_path()
    if work_path is None:
        work_path = _default_work_path()

    LOG.info("facts export periodic started, interval=%ds, path=%s", interval, facts_path)

    loop = asyncio.get_event_loop()
    first_run = True
    while True:
        try:
            _fp = facts_path
            _wp = work_path
            _force = first_run  # always snapshot on first run
            def _run():
                from .db import get_conn
                # Pass factory, not connection — each phase opens/closes its own
                # connection to release WAL read locks between phases.
                export_once(get_conn, _fp, _wp, force_snapshot=_force)
            await loop.run_in_executor(None, _run)
            first_run = False
        except Exception:
            LOG.exception("facts export failed")
        await asyncio.sleep(interval)


def main(argv=None):
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    # One-shot export for manual/cron runs
    from .db import get_conn
    args = sys.argv[1:] if argv is None else argv
    facts_path = args[0] if args else _default_facts_path()
    work_path = _default_work_path()
    export_once(get_conn, facts_path, work_path, force_snapshot=True)
    LOG.info("one-shot export done")
    return 0


if __name__ == "__main__":
    main()
