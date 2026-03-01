import asyncio
import json
import logging
import os

import uvicorn
from fastapi import FastAPI, HTTPException, Response, Depends, Header

from .db import init_db, get_conn
from .consumer import ATProtoConsumer

app = FastAPI(title="Bluesky Labeler MVP")

_consumer_task = None
_label_ingest_task = None


@app.on_event("startup")
async def startup_event():
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    init_db()
    # Optionally start the consumer in background when env var is set
    if os.getenv("FIREHOSE_AUTO_START") == "1":
        loop = asyncio.get_event_loop()
        consumer = ATProtoConsumer()
        global _consumer_task
        _consumer_task = loop.create_task(consumer.run())

    # Optionally start periodic label ingestion when env var is set
    if os.getenv("FIREHOSE_LABEL_INGEST") == "1":
        from .ingest import run_periodic
        loop = asyncio.get_event_loop()
        global _label_ingest_task
        _label_ingest_task = loop.create_task(run_periodic())

    # Optionally start longitudinal recheck loop when env var is set
    if os.getenv("ENABLE_LONGITUDINAL_RECHECK") == "1":
        from .longitudinal import run_periodic as _lr
        global _label_recheck_task
        loop = asyncio.get_event_loop()
        _label_recheck_task = loop.create_task(_lr())

    # Optionally start facts sidecar export for labelwatch
    if os.environ.get("ENABLE_FACTS_EXPORT", "").lower() in ("1", "true"):
        from .facts_export import run_periodic as _fe
        loop = asyncio.get_event_loop()
        loop.create_task(_fe())


@app.on_event("shutdown")
async def shutdown_event():
    global _consumer_task
    if _consumer_task:
        _consumer_task.cancel()
    global _label_ingest_task
    if _label_ingest_task:
        _label_ingest_task.cancel()


@app.get("/health")
async def health():
    return {"status": "ok"}


BUILD_SHA = os.getenv("GIT_SHA", "unknown")


@app.get("/health/extended")
async def health_extended():
    from .emit_mode import get_emit_mode
    try:
        from . import metrics as metrics_module
        quarantine_trips = metrics_module.RECHECK_QUARANTINE_TRIPPED._value.get()
    except Exception:
        quarantine_trips = None
    conn = get_conn()
    queue_rows = conn.execute("SELECT COUNT(*) FROM recheck_queue").fetchall()
    queue_depth = queue_rows[0][0] if queue_rows else 0
    cursor_rows = conn.execute("SELECT consumer, cursor, updated_at FROM cursors ORDER BY updated_at DESC LIMIT 1").fetchall()
    conn.close()
    cursor_info = None
    if cursor_rows:
        cursor_info = {"consumer": cursor_rows[0][0], "cursor": cursor_rows[0][1], "updated_at": cursor_rows[0][2]}
    # Platform health watermark
    try:
        from . import platform_health
        health_snap = platform_health.get_health_snapshot()
    except Exception:
        health_snap = {}
    # Platform health detection envelope (if state transition pending)
    platform_detection = None
    try:
        from . import platform_health as ph_mod
        from .detection import envelope_to_dict
        det = ph_mod.get_detection()
        if det is not None:
            platform_detection = envelope_to_dict(det)
    except Exception:
        pass

    result = {
        "status": "ok",
        "build_sha": BUILD_SHA,
        "emit_mode": get_emit_mode(),
        "queue_depth": queue_depth,
        "last_cursor": cursor_info,
        "quarantine_trips": quarantine_trips,
        "platform_health": health_snap.get("health_state", "unavailable"),
        "coverage_pct": health_snap.get("coverage_pct"),
        "events_per_sec": health_snap.get("current_eps"),
        "baseline_eps": health_snap.get("baseline_eps"),
        "stream_lag_s": health_snap.get("stream_lag_s"),
        "reconnect_count": health_snap.get("reconnect_count"),
        "gate_reasons": health_snap.get("gate_reasons", []),
    }
    if platform_detection is not None:
        result["platform_detection"] = platform_detection
    return result


async def admin_auth(authorization: str = Header(None), x_admin_token: str = Header(None, alias="X-Admin-Token")):
    """Require ADMIN_API_TOKEN if set; accept either Bearer token in Authorization header
    or the `X-Admin-Token` header. If `ADMIN_API_TOKEN` is not set, authentication is a no-op.
    """
    log = logging.getLogger("labeler.admin")
    token = os.getenv("ADMIN_API_TOKEN")
    if not token:
        # no admin token configured → open endpoint (warning for operators)
        log.debug("admin auth: no ADMIN_API_TOKEN configured; allowing open access")
        return True

    # Check X-Admin-Token first
    if x_admin_token and x_admin_token == token:
        return True

    if not authorization:
        log.warning("admin auth failed: missing token")
        raise HTTPException(status_code=401, detail="missing admin token")

    scheme, _, cred = authorization.partition(" ")
    if scheme.lower() != "bearer" or cred != token:
        log.warning("admin auth failed: invalid token provided (scheme=%s)", scheme)
        raise HTTPException(status_code=401, detail="invalid admin token")

    return True


@app.get("/metrics")
async def metrics_endpoint():
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from prometheus_client.core import CollectorRegistry
    # Use the default registry
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


@app.get("/admin/mappings")
async def admin_mappings(auth=Depends(admin_auth)):
    """Return mappings of normalized endpoints to labeler DIDs (requires Redis)."""
    try:
        from . import cooldown
        mappings = await cooldown.get_all_mappings()
        return {"mappings": mappings}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/admin/cooldowns")
async def admin_cooldowns(auth=Depends(admin_auth)):
    """Return active cooldowns (endpoint -> ttl seconds)."""
    try:
        from . import cooldown
        cooldowns = await cooldown.get_all_active_cooldowns()
        return {"cooldowns": cooldowns}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/exposure/{did}")
async def exposure(did: str):
    # naive exposure: count edges where dst_did == did
    conn = get_conn()
    rows = conn.execute("SELECT count(*) FROM edges WHERE dst_did = ?", (did,)).fetchall()
    count = rows[0][0] if rows else 0
    return {"did": did, "incoming_edges": count}


@app.get("/strain/top")
async def strain_top(limit: int = 10):
    # placeholder: return top authors by event count (proxy metric)
    conn = get_conn()
    rows = conn.execute("SELECT author, COUNT(*) as cnt FROM events GROUP BY author ORDER BY cnt DESC LIMIT ?", (limit,)).fetchall()
    return [{"author": r[0], "count": r[1]} for r in rows]


@app.get("/labels/{subject_uri}")
async def labels_for_subject(subject_uri: str):
    from .db import get_labels_for_subject
    labels = get_labels_for_subject(subject_uri)
    if not labels:
        raise HTTPException(status_code=404, detail="no labels found for subject")
    return {"subject_uri": subject_uri, "labels": labels}


@app.get("/recent-decisions")
async def recent_decisions(limit: int = 50, rule_id: str = None, auth=Depends(admin_auth)):
    limit = max(1, min(int(limit), 500))
    conn = get_conn()
    if rule_id:
        rows = conn.execute(
            "SELECT decision_id, created_at, subject_uri, root_uri, label, rule_id, fingerprint_version, inputs_json, evidence_hashes_json, decision_trace, config_hash, status FROM label_decisions WHERE rule_id = ? ORDER BY created_at DESC LIMIT ?",
            (rule_id, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT decision_id, created_at, subject_uri, root_uri, label, rule_id, fingerprint_version, inputs_json, evidence_hashes_json, decision_trace, config_hash, status FROM label_decisions ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    conn.close()

    out = []
    for r in rows:
        inputs = {}
        evidence = []
        try:
            inputs = json.loads(r[7] or "{}")
        except Exception:
            inputs = {}
        try:
            evidence = json.loads(r[8] or "[]")
        except Exception:
            evidence = []
        out.append(
            {
                "decision_id": r[0],
                "created_at": r[1],
                "subject_uri": r[2],
                "root_uri": r[3],
                "label": r[4],
                "rule_id": r[5],
                "fingerprint_version": r[6],
                "inputs": inputs,
                "evidence_hashes": evidence,
                "decision_trace": r[9],
                "config_hash": r[10],
                "status": r[11],
            }
        )
    return {"decisions": out}


@app.get("/quarantine/recent")
async def quarantine_recent(limit: int = 50, auth=Depends(admin_auth)):
    limit = max(1, min(int(limit), 500))
    conn = get_conn()
    rows = conn.execute(
        "SELECT emit_id, created_at, emit_mode, emit_status, emit_reason, payload_json FROM quarantine_emits ORDER BY created_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    out = []
    for r in rows:
        payload = {}
        try:
            payload = json.loads(r[5] or "{}")
        except Exception:
            payload = {}
        out.append(
            {
                "emit_id": r[0],
                "created_at": r[1],
                "emit_mode": r[2],
                "emit_status": r[3],
                "emit_reason": r[4],
                "payload": payload,
            }
        )
    return {"quarantined": out}


if __name__ == "__main__":
    # Allow `python -m labeler.main` for quick local runs
    uvicorn.run("labeler.main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), log_level="info")
