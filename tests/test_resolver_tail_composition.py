"""Tests for the read-only resolver aged-tail composition report (Slice 1).

Covers the pure classifiers (fixture-level, no DB) and an end-to-end compose over a
fixture DB. The read-only guarantee is asserted two ways: compose mutates nothing,
and the read-only connection rejects writes.
"""
from datetime import datetime, timedelta, timezone

import pytest

from labeler.db import get_conn, init_db
from labeler.resolver_tail_composition import (
    bucket_age,
    classify_source,
    classify_attempt,
    classify_error_kind,
    quarantine_flags,
    compose_tail,
    open_readonly,
)

NOW = datetime(2026, 6, 29, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    import labeler.db as db_mod
    monkeypatch.setattr(db_mod, "DATA_DIR", tmp_path)
    init_db()


# --------------------------- pure classifier units ---------------------------

@pytest.mark.parametrize("hours,expected", [
    (0, "<24h"), (23.9, "<24h"),
    (24, "24-72h"), (71.9, "24-72h"),
    (72, "72-168h"), (167.9, "72-168h"),
    (168, "168-240h"), (239.9, "168-240h"),
    (240, "240-336h"), (335.9, "240-336h"),
    (336, ">336h"), (1000, ">336h"),
    (None, "unparseable"),
])
def test_bucket_age_boundaries(hours, expected):
    assert bucket_age(hours) == expected


@pytest.mark.parametrize("src,expected", [
    ("live", "live"),
    ("labelwatch_seed", "labelwatch_seed"),
    ("both", "both"),
    ("retry", "unknown"),       # retry/requeue is UNAVAILABLE -> unknown
    ("", "unknown"),
    (None, "unknown"),
])
def test_classify_source(src, expected):
    assert classify_source(src) == expected


def test_classify_attempt():
    assert classify_attempt(None) == "0"
    assert classify_attempt("") == "0"
    assert classify_attempt("2026-06-20T00:00:00+00:00") == ">=1"


@pytest.mark.parametrize("status,err,last,expected", [
    ("unresolved", None, None, "never_attempted"),
    (None, None, None, "never_attempted"),
    ("error", "unsupported_method:web", "2026-06-20T00:00:00+00:00", "malformed_did"),
    ("not_found", "http_404", "2026-06-20T00:00:00+00:00", "did_doc_missing"),
    ("not_found", None, "2026-06-20T00:00:00+00:00", "did_doc_missing"),
    ("error", "http_429", "2026-06-20T00:00:00+00:00", "rate_limited"),
    ("error", "http_400", "2026-06-20T00:00:00+00:00", "permanentish"),
    ("error", "http_503", "2026-06-20T00:00:00+00:00", "transient_network"),
    ("error", "TimeoutError", "2026-06-20T00:00:00+00:00", "resolver_timeout"),
    ("error", "URLError", "2026-06-20T00:00:00+00:00", "transient_network"),
    ("error", "ConnectionResetError", "2026-06-20T00:00:00+00:00", "transient_network"),
    ("error", "SomeWeirdError", "2026-06-20T00:00:00+00:00", "unknown"),
    ("error", "", "2026-06-20T00:00:00+00:00", "unknown"),
])
def test_classify_error_kind(status, err, last, expected):
    assert classify_error_kind(status, err, last) == expected


def test_quarantine_flags_unavailable_axes():
    # attempted + aged -> proxy fires; >=3 and repeated_timeout are not faked.
    flags = quarantine_flags(400.0, "2026-06-10T00:00:00+00:00", "permanentish")
    assert flags["age_gt_336h_and_attempted"] is True
    assert flags["permanentish_error"] is True
    # never-attempted aged row is NOT a quarantine candidate (it's drainable sediment)
    flags2 = quarantine_flags(400.0, None, "never_attempted")
    assert flags2["age_gt_336h_and_attempted"] is False
    assert "repeated_timeout" not in flags  # UNAVAILABLE, surfaced in report.schema_gaps


# --------------------------- end-to-end compose ---------------------------

def _insert(conn, did, first_seen_at, *, identity_source="live",
            resolver_status=None, last_attempt_at=None, resolver_error=None):
    conn.execute(
        "INSERT INTO actor_identity_current "
        "(did, first_seen_at, identity_source, resolver_status, "
        " resolver_last_attempt_at, resolver_error) VALUES (?,?,?,?,?,?)",
        (did, first_seen_at, identity_source, resolver_status, last_attempt_at, resolver_error),
    )


def _iso(hours_ago):
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def test_compose_tail_age_source_attempt_buckets():
    conn = get_conn()
    # pending, never-attempted, various ages + sources
    _insert(conn, "did:a", _iso(1), identity_source="live")                 # <24h
    _insert(conn, "did:b", _iso(50), identity_source="labelwatch_seed")     # 24-72h
    _insert(conn, "did:c", _iso(100), identity_source="both")               # 72-168h
    _insert(conn, "did:d", _iso(200), identity_source="live")               # 168-240h
    _insert(conn, "did:e", _iso(300), identity_source="weird")              # 240-336h, unknown src
    _insert(conn, "did:f", _iso(400), identity_source="live",               # >336h, attempted
            resolver_status="unresolved", last_attempt_at=_iso(1),
            resolver_error="TimeoutError")
    # non-pending rows must be excluded from the pending scope
    _insert(conn, "did:ok", _iso(500), resolver_status="ok")
    _insert(conn, "did:err", _iso(500), resolver_status="error",
            last_attempt_at=_iso(1), resolver_error="http_400")
    conn.commit()

    rep = compose_tail(conn, now=NOW)

    assert rep["totals"]["pending_total"] == 6  # ok + error excluded
    assert rep["by_age_bucket"]["<24h"] == 1
    assert rep["by_age_bucket"]["24-72h"] == 1
    assert rep["by_age_bucket"]["72-168h"] == 1
    assert rep["by_age_bucket"]["168-240h"] == 1
    assert rep["by_age_bucket"]["240-336h"] == 1
    assert rep["by_age_bucket"][">336h"] == 1

    assert rep["by_source_population"]["live"] == 3
    assert rep["by_source_population"]["labelwatch_seed"] == 1
    assert rep["by_source_population"]["both"] == 1
    assert rep["by_source_population"]["unknown"] == 1   # "weird" -> unknown

    assert rep["by_attempt_count"]["0"] == 5
    assert rep["by_attempt_count"][">=1 (exact count unavailable)"] == 1

    # 5 never_attempted + 1 timeout
    assert rep["by_last_error_kind"]["never_attempted"] == 5
    assert rep["by_last_error_kind"]["resolver_timeout"] == 1

    # derived totals
    assert rep["totals"]["pending_gt_72h"] == 4
    assert rep["totals"]["pending_gt_168h"] == 3
    assert rep["totals"]["oldest_pending_hours"] == pytest.approx(400.0, abs=0.1)

    # adjacent terminal pool counts (context, not pending)
    assert rep["adjacent_terminal_pool"]["error"] == 1


def test_compose_quarantine_counts_and_unavailables():
    conn = get_conn()
    # aged + attempted + permanentish -> two quarantine signals
    _insert(conn, "did:p", _iso(400), resolver_status="unresolved",
            last_attempt_at=_iso(2), resolver_error="http_400")
    # aged but never-attempted -> NOT a quarantine candidate (drainable)
    _insert(conn, "did:q", _iso(400), resolver_status="unresolved")
    conn.commit()

    rep = compose_tail(conn, now=NOW)
    q = rep["quarantine_candidate_counts"]
    assert q["age_gt_336h_and_attempted"] == 1
    assert q["permanentish_error"] == 1
    # structurally-unavailable axes are reported as the literal string, never a count
    assert q["repeated_timeout"] == "unavailable"
    assert q["age_gt_336h_and_attempts_gte_3"] == "unavailable"
    assert any("repeated_timeout" in g for g in rep["schema_gaps"])
    assert any("attempt_count" in g for g in rep["schema_gaps"])


def test_compose_is_read_only(tmp_path):
    conn = get_conn()
    _insert(conn, "did:a", _iso(400), resolver_status="unresolved")
    conn.commit()
    before = conn.execute("SELECT resolver_status, resolver_last_attempt_at, "
                          "resolver_error FROM actor_identity_current").fetchall()
    compose_tail(conn, now=NOW)
    after = conn.execute("SELECT resolver_status, resolver_last_attempt_at, "
                         "resolver_error FROM actor_identity_current").fetchall()
    assert before == after  # compose mutated nothing


def test_open_readonly_rejects_writes(tmp_path):
    import labeler.db as db_mod
    # ensure the fixture DB exists on disk at the expected path
    get_conn().commit()
    db_path = str(db_mod.DATA_DIR / "labeler.sqlite")
    ro = open_readonly(db_path)
    try:
        with pytest.raises(Exception):
            ro.execute("INSERT INTO actor_identity_current (did) VALUES ('x')")
            ro.commit()
    finally:
        ro.close()
