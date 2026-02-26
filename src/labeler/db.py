import json
import os
import pathlib
import datetime
import sqlite3
from typing import Optional, Union
import uuid
from . import timeutil

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_conn():
    """Return a DB connection according to DB_BACKEND env var.

    Supported backends: 'sqlite' (default), 'duckdb'.
    """
    backend = os.getenv("DB_BACKEND", "sqlite").lower()
    if backend == "sqlite":
        db_path = DATA_DIR / "labeler.sqlite"
        conn = sqlite3.connect(str(db_path))
        # return rows as tuples (keep compatibility with existing code)
        return conn
    elif backend == "duckdb":
        try:
            import duckdb
        except Exception:
            raise
        db_path = DATA_DIR / "labeler.db"
        return duckdb.connect(database=str(db_path), read_only=False)
    else:
        raise ValueError(f"Unsupported DB_BACKEND: {backend}")


def init_db():
    conn = get_conn()
    # set pragmatic duckdb options when running on duckdb
    if os.getenv("DB_BACKEND", "sqlite").lower() == "duckdb":
        try:
            conn.execute("PRAGMA threads=2")
        except Exception:
            pass

    # SQLite performance: WAL + normal sync
    if os.getenv("DB_BACKEND", "sqlite").lower() == "sqlite":
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        except Exception:
            pass

    # events table: append-only store of raw events (store JSON as TEXT for compatibility)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            event_uri TEXT PRIMARY KEY,
            ctime TIMESTAMP,
            author TEXT,
            raw TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edges (
            src_did TEXT,
            dst_did TEXT,
            type TEXT,
            ctime TIMESTAMP
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS labels (
            subject_uri TEXT,
            labeler_did TEXT,
            label TEXT,
            ctime TIMESTAMP,
            expired_at TIMESTAMP
        )
        """
    )
    # ensure older DBs get the column if possible (duckdb: ALTER TABLE ADD COLUMN if not exists not supported everywhere)
    try:
        conn.execute("ALTER TABLE labels ADD COLUMN expired_at TIMESTAMP")
    except Exception:
        # ignore if column exists or ALTER not supported
        pass

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cursors (
            consumer TEXT PRIMARY KEY,
            cursor TEXT,
            updated_at TIMESTAMP
        )
        """
    )

    # store historical versions of events when they are edited
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS event_versions (
            event_uri TEXT,
            version_ts TIMESTAMP,
            raw TEXT
        )
        """
    )

    # claim history table for tracking claims by (authorDid, claim_fingerprint)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS claim_history (
            authorDid TEXT,
            claim_fingerprint TEXT,
            createdAt TIMESTAMP,
            confidence REAL,
            provenance TEXT,
            evidence_hash TEXT,
            post_uri TEXT,
            post_cid TEXT,
            fingerprint_version TEXT
        )
        """
    )
    # ensure older DBs get new columns if possible
    try:
        conn.execute("ALTER TABLE claim_history ADD COLUMN fingerprint_version TEXT")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE claim_history ADD COLUMN evidence_class TEXT DEFAULT 'none'")
    except Exception:
        pass

    # Legacy recheck_requests table (kept for DB compat; no longer used)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recheck_requests (
            root_uri TEXT PRIMARY KEY,
            scheduled_at TIMESTAMP
        )
        """
    )

    # Fingerprint-keyed recheck queue (debounced: one entry per claim fingerprint)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recheck_queue (
            claim_fingerprint TEXT PRIMARY KEY,
            scheduled_at TIMESTAMP
        )
        """
    )

    # DB-enforced queue cap on recheck_queue
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS recheck_queue_fp_cap
        AFTER INSERT ON recheck_queue
        BEGIN
          DELETE FROM recheck_queue
          WHERE rowid IN (
            SELECT rowid FROM recheck_queue
            ORDER BY scheduled_at ASC
            LIMIT (
              SELECT CASE WHEN COUNT(*)>10000 THEN COUNT(*)-10000 ELSE 0 END
              FROM recheck_queue
            )
          );
        END;
    """)

    # claim-group recheck requests (authorDid + fingerprint)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS claim_recheck_requests (
            authorDid TEXT,
            claim_fingerprint TEXT,
            scheduled_at TIMESTAMP,
            PRIMARY KEY (authorDid, claim_fingerprint)
        )
        """
    )

    # label decision ledger (append-only receipts)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS label_decisions (
            decision_id TEXT PRIMARY KEY,
            created_at TIMESTAMP,
            subject_uri TEXT,
            root_uri TEXT,
            label TEXT,
            rule_id TEXT,
            fingerprint_version TEXT,
            inputs_json TEXT,
            evidence_hashes_json TEXT,
            decision_trace TEXT,
            config_hash TEXT,
            status TEXT
        )
        """
    )

    # quarantined/suppressed emits (audit trail)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quarantine_emits (
            emit_id TEXT PRIMARY KEY,
            created_at TIMESTAMP,
            emit_mode TEXT,
            emit_status TEXT,
            emit_reason TEXT,
            payload_json TEXT
        )
        """
    )

    # Indexes for performance at scale
    # Thread lookup via json_extract (used by longitudinal recheck)
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_reply_root ON events(json_extract(raw, '$.replyRootUri'))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_reply_parent ON events(json_extract(raw, '$.replyParentUri'))")
    except Exception:
        pass  # json_extract indexes not supported on all SQLite versions
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_claim_history_fp ON claim_history(claim_fingerprint, createdAt)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_edges_ctime ON edges(ctime)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_label_decisions_created ON label_decisions(created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_recheck_scheduled ON recheck_requests(scheduled_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_recheck_queue_scheduled ON recheck_queue(scheduled_at)")
    except Exception:
        pass

    conn.commit()
    conn.close()


def insert_event(event_uri: str, ctime: Union[str, int, float, datetime.datetime], author: str, raw: dict):
    """Insert or update an event.

    - If the event is new, insert it and enqueue a recheck for its thread root.
    - If the event exists but `raw` differs, record a version, update the event, and enqueue a recheck.

    Returns a tuple (inserted: bool, updated: bool)
    """
    conn = get_conn()
    raw_json = json.dumps(raw)
    ctime_dt = timeutil.to_utc_datetime(ctime)

    # check if exists
    cur = conn.execute("SELECT raw, ctime FROM events WHERE event_uri = ?", (event_uri,)).fetchall()
    if not cur:
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            (event_uri, ctime_dt.isoformat(), author, raw_json),
        )
        conn.commit()
        # add claim history entry and enqueue fingerprint for recheck (complexity-gated)
        try:
            from .claims import add_claim_history, evidence_hash_from_raw, passes_complexity_gate, classify_evidence
            text = raw.get("text")
            ext_links = raw.get("externalLinks") or []
            embeds = raw.get("embeds") or []
            facets = raw.get("facets") or []
            if text and passes_complexity_gate(text, ext_links, embeds, facets):
                evidence_hash = evidence_hash_from_raw(raw)
                ev_class = classify_evidence(ext_links, embeds, facets)
                fp = add_claim_history(author, text, ctime_dt.isoformat(), event_uri, raw.get("cid"), None, None, evidence_hash, ev_class)
                _add_recheck(conn, fp)
        except Exception:
            pass
        conn.close()
        return (True, False)

    # existing: compare raw
    existing_raw = cur[0][0]
    if existing_raw != raw_json:
        now = timeutil.now_utc().isoformat()
        # store previous version
        conn.execute(
            "INSERT INTO event_versions VALUES (?, ?, ?)",
            (event_uri, now, existing_raw),
        )
        # update events table
        conn.execute(
            "UPDATE events SET raw = ?, ctime = ?, author = ? WHERE event_uri = ?",
            (raw_json, ctime_dt.isoformat(), author, event_uri),
        )
        conn.commit()
        # on update, append new claim history version and enqueue fingerprint (complexity-gated)
        try:
            from .claims import add_claim_history, evidence_hash_from_raw, passes_complexity_gate, classify_evidence
            text = raw.get("text")
            ext_links = raw.get("externalLinks") or []
            embeds = raw.get("embeds") or []
            facets = raw.get("facets") or []
            if text and passes_complexity_gate(text, ext_links, embeds, facets):
                evidence_hash = evidence_hash_from_raw(raw)
                ev_class = classify_evidence(ext_links, embeds, facets)
                fp = add_claim_history(author, text, ctime_dt.isoformat(), event_uri, raw.get("cid"), None, None, evidence_hash, ev_class)
                _add_recheck(conn, fp)
        except Exception:
            pass
        conn.close()
        return (False, True)

    # identical payload -> no-op
    conn.close()
    return (False, False)


RECHECK_QUEUE_MAX = int(os.getenv("RECHECK_QUEUE_MAX", "10000"))


def _add_recheck(conn, claim_fingerprint: str):
    """Enqueue a claim fingerprint for recheck.

    Uses INSERT OR IGNORE so duplicate fingerprints are naturally debounced —
    the queue holds at most one entry per fingerprint at any time.
    """
    now = timeutil.now_utc().isoformat()
    # Best-effort: enqueue in Redis-backed queue if available
    try:
        from .recheck_queue import get_queue
        q = get_queue(conn)
        q.enqueue(claim_fingerprint)
        return
    except Exception:
        pass

    # Fallback: DB-backed INSERT OR IGNORE (debounce via UNIQUE PK)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO recheck_queue (claim_fingerprint, scheduled_at) VALUES (?, ?)",
            (claim_fingerprint, now),
        )
    except Exception:
        cur = conn.execute("SELECT 1 FROM recheck_queue WHERE claim_fingerprint = ?", (claim_fingerprint,)).fetchall()
        if not cur:
            conn.execute("INSERT INTO recheck_queue (claim_fingerprint, scheduled_at) VALUES (?, ?)", (claim_fingerprint, now))

    # Cap enforcement is handled by the recheck_queue_fp_cap trigger
    conn.commit()


def insert_edges(edges):
    # edges: list of tuples (src, dst, type, ctime)
    if not edges:
        return
    conn = get_conn()
    conn.executemany(
        "INSERT INTO edges VALUES (?, ?, ?, ?)", edges
    )
    conn.commit()
    conn.close()


def upsert_cursor(consumer: str, cursor: Optional[str]):
    conn = get_conn()
    now = timeutil.now_utc().isoformat()
    cur = conn.execute("SELECT 1 FROM cursors WHERE consumer = ?", (consumer,)).fetchall()
    if cur:
        conn.execute(
            "UPDATE cursors SET cursor = ?, updated_at = ? WHERE consumer = ?",
            (cursor or "", now, consumer),
        )
    else:
        conn.execute(
            "INSERT INTO cursors VALUES (?, ?, ?)",
            (consumer, cursor or "", now),
        )
    conn.commit()
    conn.close()


def get_cursor(consumer: str) -> Optional[str]:
    conn = get_conn()
    cur = conn.execute("SELECT cursor FROM cursors WHERE consumer = ?", (consumer,)).fetchall()
    conn.close()
    if not cur:
        return None
    return cur[0][0]


def insert_label(subject_uri: str, labeler_did: str, label: dict, ctime: Optional[str] = None, endpoint: Optional[str] = None) -> bool:
    """Insert a label if the exact label payload is not already present for the subject.

    Returns True if a new row was inserted, False if it already existed.
    Optionally records a mapping labeler_did -> endpoint in Redis for per-DID cooldowns.
    """
    from . import metrics

    conn = get_conn()
    ctime = timeutil.to_utc_iso(ctime)
    label_json = json.dumps(label)

    # check existence first (simple and clear)
    cur = conn.execute("SELECT 1 FROM labels WHERE subject_uri = ? AND labeler_did = ? AND label = ?", (subject_uri, labeler_did, label_json)).fetchall()
    if cur:
        metrics.LABELS_SKIPPED.inc()
        conn.close()
        return False

    conn.execute(
        "INSERT INTO labels VALUES (?, ?, ?, ?, ?)",
        (subject_uri, labeler_did, label_json, ctime, None),
    )
    conn.commit()
    conn.close()
    metrics.LABELS_INSERTED.inc()

    # Best-effort: write decision ledger entry for this label
    try:
        label_name = None
        if isinstance(label, dict):
            label_name = label.get("label") or label.get("val")
        insert_label_decision(
            subject_uri=subject_uri,
            root_uri=label.get("root_uri") if isinstance(label, dict) else None,
            label_name=label_name or "unknown",
            rule_id=(label.get("rule_id") if isinstance(label, dict) else None) or "external_labeler",
            fingerprint_version=(label.get("fingerprint_version") if isinstance(label, dict) else None),
            inputs=label.get("inputs") if isinstance(label, dict) else None,
            evidence_hashes=label.get("evidence_hashes") if isinstance(label, dict) else None,
            decision_trace=label.get("decision_trace") if isinstance(label, dict) else None,
            config_hash=label.get("config_hash") if isinstance(label, dict) else None,
            status="committed",
        )
    except Exception:
        pass

    # Best-effort: record mapping labeler_did -> endpoint for adaptive per-DID cooldowns
    try:
        ep = endpoint or os.getenv("LABELER_ENDPOINT")
        if ep:
            try:
                import asyncio as _asyncio
                from . import cooldown
                ep_norm = cooldown.normalize_endpoint(ep)
                loop = _asyncio.get_running_loop()
                loop.create_task(cooldown.add_labeler_endpoint_mapping(ep_norm, labeler_did))
            except RuntimeError:
                # no running loop in this context; ignore
                pass
            except Exception:
                # swallow any errors to avoid breaking insert
                pass
    except Exception:
        pass

    return True


def get_unlabeled_subjects(window_hours: int = 24, limit: int = 100) -> list:
    """Return a list of event URIs that do not yet have labels and are within the time window."""
    cutoff = (timeutil.now_utc() - datetime.timedelta(hours=window_hours)).isoformat()
    conn = get_conn()
    rows = conn.execute(
        "SELECT event_uri FROM events WHERE ctime >= ? AND event_uri NOT IN (SELECT subject_uri FROM labels) ORDER BY ctime DESC LIMIT ?",
        (cutoff, limit),
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_labels_for_subject(subject_uri: str, include_expired: bool = False) -> list:
    conn = get_conn()
    if include_expired:
        rows = conn.execute("SELECT labeler_did, label, ctime, expired_at FROM labels WHERE subject_uri = ? ORDER BY ctime DESC", (subject_uri,)).fetchall()
    else:
        rows = conn.execute("SELECT labeler_did, label, ctime, expired_at FROM labels WHERE subject_uri = ? AND (expired_at IS NULL) ORDER BY ctime DESC", (subject_uri,)).fetchall()
    conn.close()
    return [{"labeler_did": r[0], "label": json.loads(r[1]), "ctime": r[2], "expired_at": r[3]} for r in rows]


def expire_label(subject_uri: str, labeler_did: str, label: dict, expired_at: str = None) -> int:
    """Mark a label as expired. Returns number of rows updated."""
    conn = get_conn()
    expired_at = timeutil.to_utc_iso(expired_at)
    label_json = json.dumps(label)
    cur = conn.execute(
        "UPDATE labels SET expired_at = ? WHERE subject_uri = ? AND labeler_did = ? AND label = ? AND expired_at IS NULL",
        (expired_at, subject_uri, labeler_did, label_json),
    )
    conn.commit()
    # duckdb returns cursor with rowcount via 'rowcount' attribute sometimes - be permissive
    try:
        count = cur.rowcount
    except Exception:
        # fallback: fetch count via a select
        rows = conn.execute("SELECT COUNT(*) FROM labels WHERE subject_uri = ? AND labeler_did = ? AND label = ? AND expired_at = ?", (subject_uri, labeler_did, label_json, expired_at)).fetchall()
        count = rows[0][0] if rows else 0
    conn.close()
    try:
        label_name = label.get("label") if isinstance(label, dict) else None
        if label_name:
            expire_label_decisions(subject_uri, label_name)
    except Exception:
        pass
    return count


def insert_label_decision(
    subject_uri: str,
    root_uri: Optional[str],
    label_name: str,
    rule_id: str,
    fingerprint_version: Optional[str],
    inputs: Optional[dict],
    evidence_hashes: Optional[list],
    decision_trace: Optional[str],
    config_hash: Optional[str],
    status: str = "committed",
) -> str:
    from .claims import FP_VERSION, fingerprint_config_hash
    decision_id = str(uuid.uuid4())
    created_at = timeutil.now_utc().isoformat()
    conn = get_conn()
    conn.execute(
        "INSERT INTO label_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            decision_id,
            created_at,
            subject_uri,
            root_uri,
            label_name,
            rule_id,
            fingerprint_version or FP_VERSION,
            json.dumps(inputs or {}),
            json.dumps(evidence_hashes or []),
            decision_trace or "",
            config_hash or fingerprint_config_hash(),
            status,
        ),
    )
    conn.commit()
    conn.close()
    return decision_id


def expire_label_decisions(subject_uri: str, label_name: str) -> int:
    conn = get_conn()
    cur = conn.execute(
        "UPDATE label_decisions SET status = ? WHERE subject_uri = ? AND label = ? AND status = ?",
        ("expired", subject_uri, label_name, "committed"),
    )
    conn.commit()
    try:
        count = cur.rowcount
    except Exception:
        rows = conn.execute(
            "SELECT COUNT(*) FROM label_decisions WHERE subject_uri = ? AND label = ? AND status = ?",
            (subject_uri, label_name, "expired"),
        ).fetchall()
        count = rows[0][0] if rows else 0
    conn.close()
    return count


def insert_quarantine_emit(emit_mode: str, emit_status: str, emit_reason: str, payload: dict) -> str:
    emit_id = str(uuid.uuid4())
    created_at = timeutil.now_utc().isoformat()
    conn = get_conn()
    conn.execute(
        "INSERT INTO quarantine_emits VALUES (?, ?, ?, ?, ?, ?)",
        (emit_id, created_at, emit_mode, emit_status, emit_reason or "", json.dumps(payload, sort_keys=True)),
    )
    conn.commit()
    conn.close()
    return emit_id


def enqueue_claim_recheck(authorDid: str, claim_fingerprint: str) -> None:
    now = timeutil.now_utc().isoformat()
    conn = get_conn()
    cur = conn.execute(
        "SELECT 1 FROM claim_recheck_requests WHERE authorDid = ? AND claim_fingerprint = ?",
        (authorDid, claim_fingerprint),
    ).fetchall()
    if cur:
        conn.execute(
            "UPDATE claim_recheck_requests SET scheduled_at = ? WHERE authorDid = ? AND claim_fingerprint = ?",
            (now, authorDid, claim_fingerprint),
        )
    else:
        conn.execute(
            "INSERT INTO claim_recheck_requests VALUES (?, ?, ?)",
            (authorDid, claim_fingerprint, now),
        )
    conn.commit()
    conn.close()


def dequeue_claim_rechecks(limit: int = 100) -> list:
    conn = get_conn()
    rows = conn.execute(
        "SELECT authorDid, claim_fingerprint FROM claim_recheck_requests ORDER BY scheduled_at ASC LIMIT ?",
        (limit,),
    ).fetchall()
    items = [(r[0], r[1]) for r in rows]
    for authorDid, fp in items:
        conn.execute(
            "DELETE FROM claim_recheck_requests WHERE authorDid = ? AND claim_fingerprint = ?",
            (authorDid, fp),
        )
    conn.commit()
    conn.close()
    return items
