try:
    from prometheus_client import Counter, Histogram, Gauge
except Exception:
    # fallback noop implementations for environments without prometheus client
    class _Noop:
        def __init__(self, *args, **kwargs):
            pass

    class Counter(_Noop):
        def inc(self, *args, **kwargs):
            return None

    class Histogram(_Noop):
        def observe(self, *args, **kwargs):
            return None

    class Gauge(_Noop):
        def set(self, *args, **kwargs):
            return None

# Label query metrics
LABEL_QUERY_TOTAL = Counter("label_query_total", "Total label query attempts")
LABEL_QUERY_SUCCESS = Counter("label_query_success_total", "Successful label queries")
LABEL_QUERY_FAILURE = Counter("label_query_failure_total", "Failed label queries")
LABEL_QUERY_RETRIES = Counter("label_query_retries_total", "Retries performed for label queries")
LABEL_QUERY_DURATION = Histogram("label_query_duration_seconds", "Duration of label query requests seconds")
LABEL_QUERY_429 = Counter("label_query_429_total", "Number of label queries that returned 429 Rate Limit")
LABEL_QUERY_DISTRIBUTED_RATE_LIMITED = Counter("label_query_distributed_rate_limited_total", "Times distributed limiter reported no token immediately")
LABEL_QUERY_COOLDOWN_SKIPPED = Counter("label_query_cooldown_skipped_total", "Times a label query was skipped due to cooldown")
LABEL_QUERY_COOLDOWN_SET = Counter("label_query_cooldown_set_total", "Times a cooldown was set for an endpoint")

# Ingestion / DB metrics
LABELS_INSERTED = Counter("labels_inserted_total", "Labels inserted into DB")
LABELS_SKIPPED = Counter("labels_skipped_total", "Labels skipped due to duplicates or missing data")

# Worker metrics
INGEST_ITERATIONS = Counter("ingest_iterations_total", "Number of ingest loop iterations completed")
INGEST_LABELS_PROCESSED = Counter("ingest_labels_processed_total", "Number of label objects processed by ingest")
INGEST_LAST_RUN_TS = Gauge("ingest_last_run_timestamp", "Timestamp of last ingest run (unix)")

# Recheck / longitudinal metrics
RECHECK_ITERATIONS = Counter("recheck_iterations_total", "Number of recheck loop iterations completed")
RECHECK_LABELS_EXPIRED = Counter("recheck_labels_expired_total", "Number of labels expired during rechecks")
RECHECK_LABELS_INSERTED = Counter("recheck_labels_inserted_total", "Number of new labels inserted during rechecks")
RECHECK_LAST_RUN_TS = Gauge("recheck_last_run_timestamp", "Timestamp of last recheck run (unix)")
RECHECK_QUEUE_DEPTH = Gauge("recheck_queue_depth", "Approximate number of pending recheck requests")
RECHECK_QUARANTINE_TRIPPED = Counter("recheck_quarantine_tripped_total", "Times emit was quarantined due to budgets or caps")
