import os
import json
import logging
from typing import List

from .db import get_conn

LOG = logging.getLogger("labeler.longitudinal")

DRIFT_LABELER_DID = os.getenv("DRIFT_LABELER_DID", "did:labeler:drift")

# Import drift modules lazily to avoid import cycles in tests
from .drift.models import Post
from .drift.rules import apply_all_rules
from . import timeutil
from .emit_mode import get_emit_mode, get_emit_limits
from .budgets import parse_rule_budgets, budget_exceeded_in_run, budget_exceeded_in_window


def _load_posts_for_root(conn, root_uri: str) -> List[Post]:
    # Use json_extract to filter at the SQL level instead of loading all events.
    # Falls back to full scan if json_extract is not available (old SQLite).
    try:
        rows = conn.execute(
            """SELECT event_uri, raw, ctime FROM events
               WHERE event_uri = ?
                  OR json_extract(raw, '$.replyRootUri') = ?
                  OR json_extract(raw, '$.replyParentUri') = ?""",
            (root_uri, root_uri, root_uri),
        ).fetchall()
    except Exception:
        # Fallback for SQLite without json_extract
        rows = conn.execute("SELECT event_uri, raw, ctime FROM events").fetchall()

    posts = []
    for r in rows:
        event_uri, raw_json, ctime = r
        try:
            data = json.loads(raw_json)
        except Exception:
            continue

        # Double-check membership (needed for fallback path)
        if not (
            event_uri == root_uri
            or data.get("replyRootUri") == root_uri
            or data.get("replyParentUri") == root_uri
        ):
            continue

        p = Post(
            uri=data.get("uri") or event_uri,
            cid=data.get("cid"),
            text=data.get("text", ""),
            createdAt=timeutil.to_utc_iso(data.get("createdAt") or ctime),
            authorDid=data.get("authorDid", ""),
            replyParentUri=data.get("replyParentUri"),
            replyRootUri=data.get("replyRootUri"),
            facets=data.get("facets", []),
            embeds=data.get("embeds", []),
            externalLinks=data.get("externalLinks", []),
        )
        posts.append(p)

    posts = sorted(posts, key=lambda x: x.createdAt)
    return posts


def _load_posts_for_fingerprint(conn, claim_fingerprint: str) -> List[Post]:
    """Load all posts that share a claim fingerprint (across all authors)."""
    rows = conn.execute(
        "SELECT post_uri, createdAt FROM claim_history WHERE claim_fingerprint = ? ORDER BY createdAt ASC",
        (claim_fingerprint,),
    ).fetchall()
    posts = []
    seen = set()
    for post_uri, _created in rows:
        if post_uri in seen:
            continue
        seen.add(post_uri)
        try:
            ev_rows = conn.execute("SELECT raw, ctime FROM events WHERE event_uri = ?", (post_uri,)).fetchall()
        except Exception:
            ev_rows = []
        if not ev_rows:
            continue
        raw_json, ctime = ev_rows[0]
        try:
            data = json.loads(raw_json)
        except Exception:
            continue
        p = Post(
            uri=data.get("uri") or post_uri,
            cid=data.get("cid"),
            text=data.get("text", ""),
            createdAt=timeutil.to_utc_iso(data.get("createdAt") or ctime),
            authorDid=data.get("authorDid", ""),
            replyParentUri=data.get("replyParentUri"),
            replyRootUri=data.get("replyRootUri"),
            facets=data.get("facets", []),
            embeds=data.get("embeds", []),
            externalLinks=data.get("externalLinks", []),
        )
        posts.append(p)
    return posts


def _load_posts_for_claim_group(conn, authorDid: str, claim_fingerprint: str) -> List[Post]:
    rows = conn.execute(
        "SELECT post_uri, createdAt FROM claim_history WHERE authorDid = ? AND claim_fingerprint = ? ORDER BY createdAt ASC",
        (authorDid, claim_fingerprint),
    ).fetchall()
    posts = []
    for post_uri, _created in rows:
        try:
            ev_rows = conn.execute("SELECT raw, ctime FROM events WHERE event_uri = ?", (post_uri,)).fetchall()
        except Exception:
            ev_rows = []
        if not ev_rows:
            continue
        raw_json, ctime = ev_rows[0]
        try:
            data = json.loads(raw_json)
        except Exception:
            continue
        p = Post(
            uri=data.get("uri") or post_uri,
            cid=data.get("cid"),
            text=data.get("text", ""),
            createdAt=timeutil.to_utc_iso(data.get("createdAt") or ctime),
            authorDid=data.get("authorDid", ""),
            replyParentUri=data.get("replyParentUri"),
            replyRootUri=data.get("replyRootUri"),
            facets=data.get("facets", []),
            embeds=data.get("embeds", []),
            externalLinks=data.get("externalLinks", []),
        )
        posts.append(p)
    return posts


def _decision_inputs_for_post(text: str) -> dict:
    from .drift.extract import extract_claim_signals
    import re

    cs = extract_claim_signals(text or "")
    prep_matches = re.findall(r"\b(in|by|from|at|near|inside|over|on|under|around|within)\s+([A-Za-z0-9_\\-]{2,})", text or "", re.I)
    prep_tokens = [f"{p.lower()}:{o.lower()}" for p, o in prep_matches]
    return {
        "spans": cs.spans,
        "dates": cs.dates,
        "quantities": cs.quantities,
        "entities": cs.entities,
        "modal": cs.modal,
        "prep_tokens": prep_tokens,
    }


def recheck_once(limit: int = 100) -> int:
    """Process up to `limit` recheck requests and re-evaluate claim fingerprints.

    Returns the number of fingerprints processed.
    """
    conn = get_conn()
    # try queue-backed dequeue first (Redis preferred)
    try:
        from .recheck_queue import get_queue
        q = get_queue(conn)
        fingerprints = q.dequeue(limit)
    except Exception:
        rows = conn.execute("SELECT claim_fingerprint FROM recheck_queue ORDER BY scheduled_at ASC LIMIT ?", (limit,)).fetchall()
        fingerprints = [r[0] for r in rows]

    if not fingerprints:
        conn.close()
        return 0

    processed = 0
    emit_mode = get_emit_mode()
    emit_cap = get_emit_limits()
    emit_buffer = []
    run_rule_counts = {}
    budgets = parse_rule_budgets()
    claim_recheck_enabled = os.getenv("ENABLE_CLAIM_RECHECK", "0") == "1"
    claim_recheck_limit = int(os.getenv("CLAIM_RECHECK_MAX_PER_RUN", "25"))
    from . import metrics as metrics_module
    for fp in fingerprints:
        try:
            posts = _load_posts_for_fingerprint(conn, fp)
            # collect labels produced by rules per subject
            labels_by_subject = {}
            for p in posts:
                labs = apply_all_rules(p, posts)
                labels_by_subject.setdefault(p.uri, []).extend(labs)
                # if repeat-no-new-evidence fires, enqueue claim-group recheck
                if claim_recheck_enabled:
                    if any(l.rule_id == "repeat_claim_no_new_evidence" for l in labs):
                        try:
                            from .db import enqueue_claim_recheck
                            enqueue_claim_recheck(p.authorDid, fp)
                        except Exception:
                            pass

            # For each subject, compare current active labels to labels_by_subject
            for subj, labs in labels_by_subject.items():
                # names of labels that should be active according to rules
                desired_names = set(l.label for l in labs)

                # fetch current active labels
                from .db import get_labels_for_subject, expire_label, insert_label

                current = get_labels_for_subject(subj, include_expired=False)
                current_names = set([c["label"]["label"] for c in current])

                # expire labels that are active but no longer supported
                to_expire = [c for c in current if c["label"]["label"] not in desired_names]
                expired_count = 0
                for c in to_expire:
                    cnt = expire_label(subj, c["labeler_did"], c["label"])
                    if cnt:
                        expired_count += cnt

                if expired_count:
                    metrics_module.RECHECK_LABELS_EXPIRED.inc(expired_count)

                # insert/update labels produced by rules
                for l in labs:
                    label_obj = {
                        "label": l.label,
                        "score": l.score,
                        "reasons": l.reasons,
                        "evidence": l.evidence,
                        "time": timeutil.now_utc().isoformat(),
                        "labeler": DRIFT_LABELER_DID,
                        "rule_id": l.rule_id or "unknown",
                        "scheduler": "fingerprint",
                    }
                    inserted = insert_label(subj, DRIFT_LABELER_DID, label_obj)
                    if inserted:
                        metrics_module.RECHECK_LABELS_INSERTED.inc()
                        emit_buffer.append(
                            {
                                "subject_uri": subj,
                                "label": l.label,
                                "score": round(float(l.score), 3),
                                "reasons": l.reasons,
                                "evidence": l.evidence,
                                "rule_id": l.rule_id or "unknown",
                            }
                        )
                        rid = l.rule_id or "unknown"
                        run_rule_counts[rid] = run_rule_counts.get(rid, 0) + 1
                        try:
                            from .claims import FP_VERSION, fingerprint_config_hash, evidence_hash_from_signals
                            from .db import insert_label_decision
                            inputs = _decision_inputs_for_post(p.text)
                            evidence_hashes = []
                            try:
                                evidence_hashes.append(
                                    evidence_hash_from_signals(
                                        p.text,
                                        p.externalLinks,
                                        p.embeds,
                                        p.facets,
                                    )
                                )
                            except Exception:
                                pass
                            decision_trace = json.dumps({"reasons": l.reasons, "evidence": l.evidence, "scheduler": "fingerprint"}, sort_keys=True)
                            insert_label_decision(
                                subject_uri=subj,
                                root_uri=p.replyRootUri or p.uri,
                                label_name=l.label,
                                rule_id=l.rule_id or "unknown",
                                fingerprint_version=FP_VERSION,
                                inputs=inputs,
                                evidence_hashes=evidence_hashes,
                                decision_trace=decision_trace,
                                config_hash=fingerprint_config_hash(),
                                status="committed",
                            )
                        except Exception:
                            pass
                        if emit_cap > 0 and len(emit_buffer) >= emit_cap:
                            emit_mode = "quarantine"
                            metrics_module.RECHECK_QUARANTINE_TRIPPED.inc()

        except Exception:
            LOG.exception("recheck failed for fingerprint %s", fp)
        finally:
            # remove the processed fingerprint from the queue
            try:
                conn.execute("DELETE FROM recheck_queue WHERE claim_fingerprint = ?", (fp,))
                conn.commit()
            except Exception:
                pass
            processed += 1

    # metrics and close
    metrics_module.RECHECK_ITERATIONS.inc()
    try:
        import time
        metrics_module.RECHECK_LAST_RUN_TS.set(time.time())
    except Exception:
        pass

    # claim-group rechecks (opt-in, bounded)
    if claim_recheck_enabled and claim_recheck_limit > 0:
        try:
            from .db import dequeue_claim_rechecks
            claim_items = dequeue_claim_rechecks(limit=claim_recheck_limit)
        except Exception:
            claim_items = []
        for authorDid, fp in claim_items:
            try:
                posts = _load_posts_for_claim_group(conn, authorDid, fp)
                posts = sorted(posts, key=lambda x: x.createdAt)
                for p in posts:
                    labs = apply_all_rules(p, posts)
                    for l in labs:
                        label_obj = {
                            "label": l.label,
                            "score": l.score,
                            "reasons": l.reasons,
                            "evidence": l.evidence,
                            "time": timeutil.now_utc().isoformat(),
                            "labeler": DRIFT_LABELER_DID,
                            "rule_id": l.rule_id or "unknown",
                            "scheduler": "claim_group",
                        }
                        inserted = insert_label(p.uri, DRIFT_LABELER_DID, label_obj)
                        if inserted:
                            metrics_module.RECHECK_LABELS_INSERTED.inc()
                            emit_buffer.append(
                                {
                                    "subject_uri": p.uri,
                                    "label": l.label,
                                    "score": round(float(l.score), 3),
                                    "reasons": l.reasons,
                                    "evidence": l.evidence,
                                    "rule_id": l.rule_id or "unknown",
                                }
                            )
                            rid = l.rule_id or "unknown"
                            run_rule_counts[rid] = run_rule_counts.get(rid, 0) + 1
                            try:
                                from .claims import FP_VERSION, fingerprint_config_hash, evidence_hash_from_signals
                                from .db import insert_label_decision
                                inputs = _decision_inputs_for_post(p.text)
                                evidence_hashes = []
                                try:
                                    evidence_hashes.append(
                                        evidence_hash_from_signals(
                                            p.text,
                                            p.externalLinks,
                                            p.embeds,
                                            p.facets,
                                        )
                                    )
                                except Exception:
                                    pass
                                decision_trace = json.dumps({"reasons": l.reasons, "evidence": l.evidence, "scheduler": "claim_group"}, sort_keys=True)
                                insert_label_decision(
                                    subject_uri=p.uri,
                                    root_uri=p.replyRootUri or p.uri,
                                    label_name=l.label,
                                    rule_id=l.rule_id or "unknown",
                                    fingerprint_version=FP_VERSION,
                                    inputs=inputs,
                                    evidence_hashes=evidence_hashes,
                                    decision_trace=decision_trace,
                                    config_hash=fingerprint_config_hash(),
                                    status="committed",
                                )
                            except Exception:
                                pass
            except Exception:
                LOG.exception("claim-group recheck failed for %s/%s", authorDid, fp)

    if emit_buffer:
        try:
            reason = ""
            exceeded, reason = budget_exceeded_in_run(run_rule_counts, budgets)
            if exceeded:
                emit_mode = "quarantine"
                metrics_module.RECHECK_QUARANTINE_TRIPPED.inc()
            if emit_mode != "quarantine":
                exceeded, reason = budget_exceeded_in_window(conn, budgets)
                if exceeded:
                    emit_mode = "quarantine"
                    metrics_module.RECHECK_QUARANTINE_TRIPPED.inc()
            if reason:
                for rec in emit_buffer:
                    rec["emit_reason"] = reason
            from .emitter import record_emit_decision
            record_emit_decision(emit_buffer, emit_mode)
        except Exception:
            pass

    conn.close()
    return processed


async def run_periodic(stop_event=None, interval: int = None):
    import asyncio
    interval = interval or int(os.getenv("RECHECK_INTERVAL", "60"))
    batch = int(os.getenv("RECHECK_BATCH_SIZE", "10"))
    stop_event = stop_event or asyncio.Event()
    LOG.info("starting recheck loop interval=%s batch=%s", interval, batch)
    loop = asyncio.get_event_loop()
    while not stop_event.is_set():
        try:
            # Run in executor to avoid blocking the event loop (and killing websocket keepalives)
            await loop.run_in_executor(None, recheck_once, batch)
        except Exception:
            LOG.exception("error during recheck loop")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
    LOG.info("recheck loop stopping")
