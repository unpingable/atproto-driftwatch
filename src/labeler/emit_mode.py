import os


def get_emit_mode() -> str:
    mode = os.getenv("LABELER_EMIT_MODE", "detect-only").lower()
    if mode not in ("detect-only", "emit", "quarantine"):
        mode = "detect-only"
    confirm = os.getenv("LABELER_EMIT_CONFIRM", "").lower() in ("1", "true", "yes")
    if mode == "emit" and not confirm:
        mode = "detect-only"
    return mode


def get_emit_limits() -> int:
    try:
        return int(os.getenv("LABELER_EMIT_MAX_PER_RECHECK", "0"))
    except Exception:
        return 0


def get_emit_audit_path(mode: str) -> str:
    mode_norm = (mode or "detect-only").lower()
    audit_dir = os.getenv("LABELER_EMIT_AUDIT_DIR", "").strip()
    if not audit_dir:
        audit_dir = "out"
    if mode_norm == "emit":
        name = "live_emits.jsonl"
    elif mode_norm == "quarantine":
        name = "quarantine_emits.jsonl"
    else:
        name = "detect_only_emits.jsonl"
    return os.path.join(audit_dir, name)
