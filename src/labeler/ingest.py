import os
import asyncio
import logging
import time

from .db import get_unlabeled_subjects, insert_label
from .labeler import query_labels_for_subject
from . import timeutil

LOG = logging.getLogger("labeler.ingest")

INGEST_INTERVAL = int(os.getenv("FIREHOSE_LABEL_INGEST_INTERVAL", "300"))
INGEST_WINDOW_HOURS = int(os.getenv("FIREHOSE_LABEL_INGEST_WINDOW_HOURS", "24"))
INGEST_BATCH = int(os.getenv("FIREHOSE_LABEL_INGEST_BATCH", "200"))


async def ingest_once(window_hours: int = None, limit: int = None):
    window_hours = window_hours or INGEST_WINDOW_HOURS
    limit = limit or INGEST_BATCH

    subjects = get_unlabeled_subjects(window_hours=window_hours, limit=limit)
    if not subjects:
        LOG.debug("no unlabeled subjects found")
        return 0

    from . import metrics as metrics_module

    inserted = 0
    processed_labels = 0
    for subj in subjects:
        try:
            labels = await query_labels_for_subject(subj)
        except Exception:
            LOG.exception("label query failed for %s", subj)
            continue

        for lab in labels or []:
            processed_labels += 1
            # lab is assumed to be a dict-like label object; labeler DID might live under 'labeler' or 'by'
            labeler_did = lab.get("labeler") or lab.get("labeler_did") or lab.get("by") or "unknown"
            ctime = timeutil.to_utc_iso(lab.get("time") or lab.get("ctime"))
            if insert_label(subj, labeler_did, lab, ctime):
                inserted += 1

    LOG.info("ingest_once: processed %s subjects, inserted %s labels, processed_labels=%s", len(subjects), inserted, processed_labels)
    metrics_module.INGEST_ITERATIONS.inc()
    metrics_module.INGEST_LABELS_PROCESSED.inc(processed_labels)
    metrics_module.INGEST_LAST_RUN_TS.set(time.time())
    return inserted


async def run_periodic(stop_event: asyncio.Event = None, interval: int = None):
    interval = interval or INGEST_INTERVAL
    stop_event = stop_event or asyncio.Event()
    LOG.info("starting label ingestion loop interval=%s", interval)
    while not stop_event.is_set():
        try:
            await ingest_once()
        except Exception:
            LOG.exception("error during label ingest iteration")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            # normal loop wake-up
            pass
    LOG.info("label ingestion loop stopping")
