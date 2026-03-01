"""Detection envelope and evidence spine for the Driftwatch sensor array.

Defines the canonical output shape for all detectors:
- DetectionEnvelope — the universal detection record
- SubjectRef — what the detection is about
- EvidenceStub — opaque commitment to supporting data

Evidence spine (M2):
- canonicalize() — recursive normalization for deterministic hashing
- stable_json() — canonical JSON serialization
- receipt_hash() — SHA256[:16] of stable_json
- compute_window_fingerprint() — commit to data window + config
- normalize_sql() — whitespace-collapse for query commitment

Prime invariant (INV-001): No detector participates in action coupling
unless it emits structured explanation + evidence pointers sufficient to
re-derive and verify.
"""

import hashlib
import json
import math
from dataclasses import dataclass, fields
from typing import Optional

# --- Constants ---

ENVELOPE_SCHEMA_VERSION = 1
MAX_EXPLAIN_TOP_K = 10
MAX_EXPLAIN_STR_LEN = 200

VALID_SEVERITIES = ("info", "low", "med", "high", "critical")
VALID_SUBJECT_TYPES = ("fingerprint", "did", "uri", "labeler_did", "global")
VALID_EVIDENCE_KINDS = ("query_commitment", "event_range", "hashset_root", "note")


# --- Data classes ---

@dataclass(frozen=True)
class SubjectRef:
    """What the detection is about.

    Use SubjectRef("global", "") for global subjects. Never None.
    """
    type: str   # "fingerprint" | "did" | "uri" | "labeler_did" | "global"
    value: str  # fingerprint hash, DID, URI, or "" for global


@dataclass(frozen=True)
class EvidenceStub:
    """Opaque commitment to supporting data.

    ref policy:
    - query_commitment, event_range, hashset_root → hashed refs (opaque)
    - note → human-readable but hard capped at MAX_EXPLAIN_STR_LEN chars
    """
    kind: str   # "query_commitment" | "event_range" | "hashset_root" | "note"
    ref: str    # hash or human-readable pointer


@dataclass(frozen=True)
class DetectionEnvelope:
    """Universal detection record emitted by all sensors/detectors.

    Every field is mandatory. Use "" for fields not yet populated (e.g.,
    window_fingerprint before M2, receipt_hash before computation).
    """
    envelope_schema_version: int  # = ENVELOPE_SCHEMA_VERSION
    detector_id: str              # e.g. "burst_score", "regime_shift"
    detector_version: str         # e.g. "v1"
    ts_start: str                 # ISO timestamp
    ts_end: str                   # ISO timestamp
    window: str                   # "1m" | "10m" | "1h" | "24h"
    subject: SubjectRef           # never None
    type: str                     # "burst" | "regime_shift" | etc.
    score: float
    severity: str                 # "info" | "low" | "med" | "high" | "critical"
    explain: dict                 # bounded, canonicalizable
    evidence: tuple               # tuple of EvidenceStub, sorted by (kind, ref)
    window_fingerprint: str       # "" until M2 wiring
    config_hash: str              # detector config commitment
    receipt_hash: str             # "" until computed; filled by seal_envelope()
    det_id: str                   # H(detector_id, version, window_fp, subject, type)


# --- Canonicalization and hashing (M2 evidence spine) ---

def canonicalize(obj):
    """Recursively normalize obj for deterministic hashing.

    - Sort dict keys
    - Convert tuples/sets to sorted lists (sort by stable_json of each element)
    - Quantize floats: round(x, 6), reject NaN/Inf
    - Strings unchanged
    - None preserved
    - Ints unchanged
    - Booleans unchanged
    """
    if obj is None:
        return None
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, int):
        return obj
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            raise ValueError(f"canonicalize: NaN/Inf not allowed: {obj}")
        return round(obj, 6)
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return {k: canonicalize(v) for k, v in sorted(obj.items())}
    if isinstance(obj, (list, tuple, set, frozenset)):
        items = [canonicalize(x) for x in obj]
        # Sort for sets/frozensets; preserve order for lists/tuples
        if isinstance(obj, (set, frozenset)):
            items.sort(key=lambda x: stable_json(x))
        return items
    raise TypeError(f"canonicalize: unsupported type {type(obj).__name__}")


def stable_json(obj) -> str:
    """Canonical JSON of canonicalize(obj). Compact separators, ASCII."""
    return json.dumps(
        canonicalize(obj),
        sort_keys=True,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
    )


def receipt_hash(obj) -> str:
    """SHA256[:16] of stable_json(obj)."""
    return hashlib.sha256(stable_json(obj).encode("utf-8")).hexdigest()[:16]


def normalize_sql(sql: str) -> str:
    """Collapse whitespace for stable query hashing. No deps."""
    return " ".join(sql.split())


# --- Envelope helpers ---

def severity_rank(severity: str) -> int:
    """Canonical severity ordering for sorting. info=0 .. critical=4."""
    return {"info": 0, "low": 1, "med": 2, "high": 3, "critical": 4}.get(severity, -1)


def compute_det_id(
    detector_id: str,
    detector_version: str,
    window_fingerprint: str,
    subject: SubjectRef,
    detection_type: str,
) -> str:
    """Stable detection identity for dedup/cooldown.

    M1: window_fingerprint="" → det_id computed with "m1_unfingerprinted" placeholder.
    M2: real window_fingerprint. det_id semantics change once — documented, not silent.
    """
    wf = window_fingerprint if window_fingerprint else "m1_unfingerprinted"
    obj = {
        "detector_id": detector_id,
        "detector_version": detector_version,
        "window_fingerprint": wf,
        "subject_type": subject.type,
        "subject_value": subject.value,
        "type": detection_type,
    }
    return receipt_hash(obj)


def compute_window_fingerprint(
    ts_start: str,
    ts_end: str,
    window: str,
    schema_version: int,
    config_hash: str,
    watermark_snapshot: dict,
) -> str:
    """Commit to what data window + config this detection covers."""
    obj = {
        "ts_start": ts_start,
        "ts_end": ts_end,
        "window": window,
        "schema_version": schema_version,
        "config_hash": config_hash,
        "watermark_coverage": watermark_snapshot.get("coverage_pct", 0),
        "watermark_state": watermark_snapshot.get("health_state", "unknown"),
    }
    return receipt_hash(obj)


def make_query_commitment(sql: str, params: list, result_hash: str) -> EvidenceStub:
    """Create a query_commitment evidence stub."""
    ref = receipt_hash({
        "sql_norm": normalize_sql(sql),
        "params": params,
        "result_hash": result_hash,
    })
    return EvidenceStub(kind="query_commitment", ref=ref)


def make_event_range(cursor_start: str, cursor_end: str, events_in: int) -> EvidenceStub:
    """Create an event_range evidence stub."""
    ref = receipt_hash({
        "cursor_start": cursor_start,
        "cursor_end": cursor_end,
        "events_in": events_in,
    })
    return EvidenceStub(kind="event_range", ref=ref)


def make_hashset_root(subjects: list, total_count: int) -> EvidenceStub:
    """Create a hashset_root evidence stub for top-k explanations."""
    ref = receipt_hash({
        "subjects": sorted(subjects),
        "n": total_count,
    })
    return EvidenceStub(kind="hashset_root", ref=ref)


def make_note(text: str) -> EvidenceStub:
    """Create a note evidence stub, capped at MAX_EXPLAIN_STR_LEN."""
    return EvidenceStub(kind="note", ref=text[:MAX_EXPLAIN_STR_LEN])


# --- Envelope construction ---

def _cap_explain_value(v):
    """Enforce explain block bounds on a single value."""
    if isinstance(v, str):
        return v[:MAX_EXPLAIN_STR_LEN]
    if isinstance(v, list):
        capped = v[:MAX_EXPLAIN_TOP_K]
        return [_cap_explain_value(item) for item in capped]
    if isinstance(v, dict):
        return {k: _cap_explain_value(val) for k, val in v.items()}
    return v


def _was_truncated(original_explain: dict) -> bool:
    """Check if capping would change any top-level values."""
    for v in original_explain.values():
        if isinstance(v, list) and len(v) > MAX_EXPLAIN_TOP_K:
            return True
        if isinstance(v, str) and len(v) > MAX_EXPLAIN_STR_LEN:
            return True
    return False


def build_envelope(
    detector_id: str,
    detector_version: str,
    ts_start: str,
    ts_end: str,
    window: str,
    subject: SubjectRef,
    detection_type: str,
    score: float,
    severity: str,
    explain: dict,
    evidence: tuple = (),
    window_fingerprint: str = "",
    config_hash: str = "",
) -> DetectionEnvelope:
    """Build a DetectionEnvelope with proper capping, sorting, and hashing.

    Caps explain block, sorts evidence, computes det_id and receipt_hash.
    """
    # Cap explain block
    original_explain = explain
    explain = {k: _cap_explain_value(v) for k, v in explain.items()}
    if _was_truncated(original_explain):
        explain["truncated"] = True

    # Sort evidence by (kind, ref) for stability
    sorted_evidence = tuple(sorted(evidence, key=lambda e: (e.kind, e.ref)))

    # Compute det_id
    det_id = compute_det_id(
        detector_id, detector_version, window_fingerprint, subject, detection_type
    )

    # Build envelope without receipt_hash first
    envelope = DetectionEnvelope(
        envelope_schema_version=ENVELOPE_SCHEMA_VERSION,
        detector_id=detector_id,
        detector_version=detector_version,
        ts_start=ts_start,
        ts_end=ts_end,
        window=window,
        subject=subject,
        type=detection_type,
        score=round(score, 6),
        severity=severity,
        explain=explain,
        evidence=sorted_evidence,
        window_fingerprint=window_fingerprint,
        config_hash=config_hash,
        receipt_hash="",
        det_id=det_id,
    )

    # Compute receipt_hash over the envelope dict (with receipt_hash="")
    rh = receipt_hash(envelope_to_dict(envelope))

    # Return new envelope with receipt_hash filled in
    return DetectionEnvelope(
        envelope_schema_version=envelope.envelope_schema_version,
        detector_id=envelope.detector_id,
        detector_version=envelope.detector_version,
        ts_start=envelope.ts_start,
        ts_end=envelope.ts_end,
        window=envelope.window,
        subject=envelope.subject,
        type=envelope.type,
        score=envelope.score,
        severity=envelope.severity,
        explain=envelope.explain,
        evidence=envelope.evidence,
        window_fingerprint=envelope.window_fingerprint,
        config_hash=envelope.config_hash,
        receipt_hash=rh,
        det_id=envelope.det_id,
    )


def envelope_to_dict(e: DetectionEnvelope) -> dict:
    """Stable dict for serialization/receipting.

    Evidence sorted by (kind, ref). SubjectRef flattened.
    """
    return {
        "envelope_schema_version": e.envelope_schema_version,
        "detector_id": e.detector_id,
        "detector_version": e.detector_version,
        "ts_start": e.ts_start,
        "ts_end": e.ts_end,
        "window": e.window,
        "subject": {"type": e.subject.type, "value": e.subject.value},
        "type": e.type,
        "score": e.score,
        "severity": e.severity,
        "explain": e.explain,
        "evidence": [{"kind": ev.kind, "ref": ev.ref} for ev in e.evidence],
        "window_fingerprint": e.window_fingerprint,
        "config_hash": e.config_hash,
        "receipt_hash": e.receipt_hash,
        "det_id": e.det_id,
    }


def validate_envelope(e: DetectionEnvelope, strict: bool = False) -> list:
    """Validate envelope against rules. Returns list of violations.

    strict=True (tests/dev): raises on first violation.
    strict=False (prod): returns warnings, does not crash.
    """
    import logging
    LOG = logging.getLogger("labeler.detection")
    violations = []

    def _v(msg):
        violations.append(msg)
        if strict:
            raise ValueError(f"envelope validation: {msg}")

    # Schema version
    if e.envelope_schema_version != ENVELOPE_SCHEMA_VERSION:
        _v(f"schema_version mismatch: got {e.envelope_schema_version}, want {ENVELOPE_SCHEMA_VERSION}")

    # Subject never None
    if e.subject is None:
        _v("subject is None")
    elif e.subject.type not in VALID_SUBJECT_TYPES:
        _v(f"invalid subject.type: {e.subject.type!r}")

    # Severity valid
    if e.severity not in VALID_SEVERITIES:
        _v(f"invalid severity: {e.severity!r}")

    # Evidence non-empty when window_fingerprint is set (M2+)
    if e.window_fingerprint and not e.evidence:
        _v("evidence empty but window_fingerprint is set (M2+ requires evidence)")

    # Evidence kinds valid
    for ev in e.evidence:
        if ev.kind not in VALID_EVIDENCE_KINDS:
            _v(f"invalid evidence kind: {ev.kind!r}")

    # Explain block bounds
    for k, v in e.explain.items():
        if isinstance(v, str) and len(v) > MAX_EXPLAIN_STR_LEN:
            _v(f"explain[{k!r}] string exceeds {MAX_EXPLAIN_STR_LEN} chars: {len(v)}")
        if isinstance(v, list) and len(v) > MAX_EXPLAIN_TOP_K:
            _v(f"explain[{k!r}] list exceeds {MAX_EXPLAIN_TOP_K} items: {len(v)}")

    # Log violations in non-strict mode
    if violations and not strict:
        LOG.warning("envelope validation: %d violations: %s", len(violations), violations)

    return violations


def sort_detections(detections: list) -> list:
    """Sort detections by (severity desc, score desc, detector_id, subject)."""
    return sorted(
        detections,
        key=lambda e: (
            -severity_rank(e.severity),
            -e.score,
            e.detector_id,
            e.subject.type,
            e.subject.value,
        ),
    )
