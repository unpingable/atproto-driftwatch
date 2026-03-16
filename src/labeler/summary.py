"""Human-readable summary of a driftwatch cluster report.

Turns the JSON blob from cluster_report() into something a person can
scan without already living inside the project.

Not a web UI. Not a dashboard. Just a readable dial.
"""
from __future__ import annotations

from typing import Any


def _baseline_status(report: dict) -> str:
    """Four-state baseline status: cold / restored / warming / steady."""
    state = report.get("platform_health", "unknown")
    restored = report.get("baseline_restored", False)
    if state == "ok":
        return "steady"
    if state == "warming_up" and restored:
        return "restored"
    if state == "warming_up":
        return "cold"
    if state == "degraded":
        return "degraded"
    return state


def _is_steady_state(report: dict) -> bool:
    """Whether baselines are mature enough to trust signals."""
    return _baseline_status(report) == "steady"


def _steady_state_banner(report: dict) -> str:
    """Top-level steady-state banner. Must be impossible to miss."""
    status = _baseline_status(report)
    if status == "steady":
        return ""  # no banner needed
    if status == "cold":
        return (
            ">>> COLD START — baselines accumulating from scratch, "
            "burst scores are not trustworthy <<<\n"
        )
    if status == "restored":
        return (
            ">>> BASELINES RESTORED — re-stabilizing, "
            "scores are approximate <<<\n"
        )
    if status == "degraded":
        return (
            ">>> PLATFORM DEGRADED — signals may not be reliable <<<\n"
        )
    return f">>> PLATFORM STATE: {status} <<<\n"


def _platform_state_line(report: dict) -> str:
    """One-line platform state assessment."""
    state = report.get("platform_health", "unknown")
    coverage = report.get("coverage_pct", 0)
    lag = report.get("stream_lag_s", 0)
    gate = report.get("gate_reasons", [])

    status = _baseline_status(report)
    if status == "steady" and coverage >= 0.95:
        desc = "stable"
    elif status == "steady":
        desc = "stable (reduced coverage)"
    elif status == "degraded":
        desc = "degraded"
    elif status == "restored":
        desc = "restored (re-stabilizing)"
    elif status == "cold":
        desc = "cold start (baselines accumulating)"
    else:
        desc = state

    parts = [f"Platform: {desc}"]
    parts.append(f"  coverage={coverage:.1%}, lag={lag:.0f}s")
    if gate:
        parts.append(f"  gates: {', '.join(gate)}")
    return "\n".join(parts)


def _baseline_maturity(report: dict) -> str:
    """Assess baseline maturity from report data."""
    total = report.get("total_claims_in_window", 0)
    fps = report.get("distinct_fps_in_window", 0)
    state = report.get("platform_health", "unknown")

    if state == "warming_up":
        return "Baseline maturity: weak (still warming up)"
    if total < 10000:
        return "Baseline maturity: weak (insufficient data)"
    if total < 100000:
        return "Baseline maturity: warming (accumulating)"
    return "Baseline maturity: good"


def _sensor_status(report: dict) -> str:
    """Summarize sensor array status."""
    detections = report.get("detections", [])
    suppressed = report.get("detections_suppressed", 0)

    by_detector: dict[str, list[dict]] = {}
    for d in detections:
        did = d.get("detector_id", "unknown")
        by_detector.setdefault(did, []).append(d)

    lines = ["Sensors:"]
    if not detections and not suppressed:
        lines.append("  (no detections)")
        return "\n".join(lines)

    for detector_id, dets in sorted(by_detector.items()):
        severities = [d.get("severity", "info") for d in dets]
        high = severities.count("high")
        med = severities.count("med")
        low = severities.count("low")
        info = severities.count("info")

        parts = []
        if high:
            parts.append(f"{high} high")
        if med:
            parts.append(f"{med} med")
        if low:
            parts.append(f"{low} low")
        if info:
            parts.append(f"{info} info")
        lines.append(f"  {detector_id}: {', '.join(parts)}")

    if suppressed:
        lines.append(f"  ({suppressed} suppressed by cooldown)")

    return "\n".join(lines)


def _notable_clusters(report: dict) -> str:
    """Highlight the most notable clusters."""
    clusters = report.get("clusters", [])
    automation = report.get("likely_automation", [])
    steady = _is_steady_state(report)

    if not clusters and not automation:
        return "Notable clusters: none"

    header = "Notable clusters:"
    if not steady:
        header = "Notable clusters (scores unreliable — baselines warming):"
    lines = [header]

    # Show top 5 by burst score
    shown = 0
    for c in clusters[:5]:
        fp = c.get("fingerprint", "?")
        if len(fp) > 40:
            fp = fp[:37] + "..."
        bs = c.get("burst_score", 0)
        posts = c.get("total_posts", 0)
        authors = c.get("latest_authors", 0)
        kind = c.get("fp_kind", "unknown")
        hl = c.get("half_life", {})
        hl_bins = hl.get("half_life_bins") if hl else None

        parts = [f"burst={bs:+.1f}"]
        parts.append(f"{posts} posts")
        parts.append(f"{authors} authors")
        parts.append(f"kind={kind}")
        if hl_bins is not None:
            parts.append(f"half-life={hl_bins}h")
        elif hl and hl.get("peak_posts"):
            parts.append("not yet decayed")

        marker = ""
        if bs >= 5.0:
            marker = " [!]"
        elif bs >= 3.0:
            marker = " [*]"

        lines.append(f"  {fp}{marker}")
        lines.append(f"    {', '.join(parts)}")
        shown += 1

    if len(clusters) > 5:
        lines.append(f"  ... and {len(clusters) - 5} more")

    if automation:
        lines.append(f"  ({len(automation)} single-author clusters flagged as likely automation)")

    return "\n".join(lines)


def _regime_shifts(report: dict) -> str:
    """Summarize regime shifts."""
    shifts = report.get("regime_shifts", [])
    active = [s for s in shifts if s.get("is_shift")]

    if not active:
        return "Regime shifts: none detected"

    lines = [f"Regime shifts: {len(active)} detected"]
    for s in active[-3:]:  # show last 3
        div = s.get("divergence", 0)
        bin_ts = s.get("bin", "?")
        lines.append(f"  {bin_ts}: JSD={div:.3f}")

    return "\n".join(lines)


def _cluster_kind_breakdown(report: dict) -> str:
    """Fingerprint kind distribution."""
    kinds = report.get("cluster_kinds", {})
    if not kinds:
        return ""

    total_fps = sum(v.get("fingerprints", 0) for v in kinds.values())
    total_posts = sum(v.get("posts", 0) for v in kinds.values())

    lines = [f"Fingerprint kinds ({total_fps} distinct, {total_posts} posts):"]
    for kind, counts in sorted(kinds.items(), key=lambda x: -x[1].get("posts", 0)):
        fps = counts.get("fingerprints", 0)
        posts = counts.get("posts", 0)
        pct = posts / total_posts * 100 if total_posts else 0
        lines.append(f"  {kind}: {fps} fps, {posts} posts ({pct:.0f}%)")

    return "\n".join(lines)


def _interpretation(report: dict) -> str:
    """One-sentence human interpretation + caveat."""
    clusters = report.get("clusters", [])
    shifts = [s for s in report.get("regime_shifts", []) if s.get("is_shift")]
    detections = report.get("detections", [])
    state = report.get("platform_health", "unknown")

    high_bursts = [c for c in clusters if c.get("burst_score", 0) >= 3.0]
    high_detections = [d for d in detections if d.get("severity") in ("high", "med")]

    status = _baseline_status(report)
    if status == "cold":
        interpretation = (
            "Cold start — no prior baseline. All scores are relative to "
            "very short history. Wait for steady state before interpreting."
        )
    elif status == "restored":
        interpretation = (
            "Baseline restored from checkpoint — scores are approximate. "
            "Will reach full confidence shortly."
        )
    elif status == "degraded":
        interpretation = (
            "Platform is degraded — signals may not be reliable."
        )
    elif high_bursts and shifts:
        interpretation = (
            f"{len(high_bursts)} notable burst(s) and {len(shifts)} regime shift(s) — "
            "attention may be concentrating around specific claims."
        )
    elif high_bursts:
        interpretation = (
            f"{len(high_bursts)} notable burst(s) above baseline — "
            "worth checking whether these are organic or coordinated."
        )
    elif shifts:
        interpretation = (
            f"{len(shifts)} regime shift(s) in label distribution — "
            "the mix of what's being labeled is changing."
        )
    elif high_detections:
        interpretation = (
            f"{len(high_detections)} sensor detection(s) at med/high severity."
        )
    else:
        interpretation = "No notable signals — normal variance."

    caveat = (
        "Caveat: burst scores are relative to short baselines. "
        "High scores indicate unusual activity, not necessarily coordinated behavior."
    )

    return f"What this means:\n  {interpretation}\n\n{caveat}"


def format_summary(report: dict) -> str:
    """Format a cluster report dict into a human-readable summary.

    This is the main entry point. Takes the output of cluster_report()
    and returns a multi-line string suitable for terminal display.
    """
    window = report.get("window_hours", 24)
    generated = report.get("generated_at", "?")
    total = report.get("total_claims_in_window", 0)
    fps = report.get("distinct_fps_in_window", 0)

    banner = _steady_state_banner(report)

    sections = [
        f"Driftwatch Summary ({window}h window, {generated})",
    ]
    if banner:
        sections.append(banner)
    sections.extend([
        f"Claims: {total:,} total, {fps:,} distinct fingerprints",
        "",
        _platform_state_line(report),
        _baseline_maturity(report),
        "",
        _sensor_status(report),
        "",
        _notable_clusters(report),
        "",
        _regime_shifts(report),
        "",
        _cluster_kind_breakdown(report),
        "",
        _interpretation(report),
    ])

    return "\n".join(sections)
