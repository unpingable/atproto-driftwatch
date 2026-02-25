"""Tests for the Driftwatch longitudinal metrics layer."""

import json
import pytest
from labeler.db import init_db, get_conn
from labeler.driftmetrics import (
    _bin_timestamp,
    fingerprint_timeseries,
    compute_half_life,
    compute_burst_scores,
    label_rate_vectors,
    detect_regime_shifts,
    _jsd,
    _entropy,
    cluster_report,
)


@pytest.fixture(autouse=True)
def fresh_db(tmp_path, monkeypatch):
    """Each test gets a clean SQLite DB."""
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    import labeler.db as db_mod
    db_mod.DATA_DIR = tmp_path
    init_db()
    yield


def _insert_claim(conn, author, fp, created_at, confidence=None, evidence_hash="", post_uri=None, evidence_class="none"):
    conn.execute(
        "INSERT INTO claim_history (authorDid, claim_fingerprint, createdAt, confidence, provenance, evidence_hash, post_uri, post_cid, fingerprint_version, evidence_class) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (author, fp, created_at, confidence, "", evidence_hash, post_uri or f"at://post/{created_at}", "", "v1", evidence_class),
    )


def _insert_decision(conn, decision_id, created_at, label, rule_id="rule_a", status="committed"):
    conn.execute(
        "INSERT INTO label_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (decision_id, created_at, "uri:1", "root:1", label, rule_id, "v1", "{}", "[]", "", "", status),
    )


# ---------------------------------------------------------------------------
# _bin_timestamp
# ---------------------------------------------------------------------------

class TestBinTimestamp:
    def test_hourly_bin(self):
        assert _bin_timestamp("2026-02-25T13:45:00+00:00", 1) == "2026-02-25T13:00:00+00:00"

    def test_4h_bin(self):
        assert _bin_timestamp("2026-02-25T13:45:00+00:00", 4) == "2026-02-25T12:00:00+00:00"

    def test_midnight(self):
        assert _bin_timestamp("2026-02-25T00:30:00+00:00", 1) == "2026-02-25T00:00:00+00:00"

    def test_z_suffix(self):
        assert _bin_timestamp("2026-02-25T13:45:00Z", 1) == "2026-02-25T13:00:00+00:00"


# ---------------------------------------------------------------------------
# fingerprint_timeseries
# ---------------------------------------------------------------------------

class TestFingerprintTimeseries:
    def test_basic_aggregation(self):
        conn = get_conn()
        _insert_claim(conn, "did:alice", "fp_a", "2026-02-25T10:00:00+00:00", 0.5, "ev1")
        _insert_claim(conn, "did:bob", "fp_a", "2026-02-25T10:30:00+00:00", 0.7, "ev2")
        _insert_claim(conn, "did:alice", "fp_a", "2026-02-25T11:15:00+00:00", 0.6, "ev1")
        conn.commit()

        ts = fingerprint_timeseries(conn=conn, fingerprint="fp_a", bin_hours=1)
        conn.close()

        assert len(ts) == 2
        # 10:00 bin
        assert ts[0]["posts"] == 2
        assert ts[0]["authors"] == 2
        assert ts[0]["evidence_classes"] == {"none": 2}
        # 11:00 bin
        assert ts[1]["posts"] == 1
        assert ts[1]["authors"] == 1

    def test_since_filter(self):
        conn = get_conn()
        _insert_claim(conn, "did:alice", "fp_a", "2026-02-25T08:00:00+00:00")
        _insert_claim(conn, "did:alice", "fp_a", "2026-02-25T12:00:00+00:00")
        conn.commit()

        ts = fingerprint_timeseries(conn=conn, since="2026-02-25T10:00:00+00:00")
        conn.close()

        assert len(ts) == 1
        assert ts[0]["bin"] == "2026-02-25T12:00:00+00:00"

    def test_all_fingerprints(self):
        conn = get_conn()
        _insert_claim(conn, "did:alice", "fp_a", "2026-02-25T10:00:00+00:00")
        _insert_claim(conn, "did:bob", "fp_b", "2026-02-25T10:00:00+00:00")
        conn.commit()

        ts = fingerprint_timeseries(conn=conn, bin_hours=1)
        conn.close()

        fps = {t["fingerprint"] for t in ts}
        assert fps == {"fp_a", "fp_b"}

    def test_empty(self):
        conn = get_conn()
        ts = fingerprint_timeseries(conn=conn)
        conn.close()
        assert ts == []


# ---------------------------------------------------------------------------
# compute_half_life
# ---------------------------------------------------------------------------

class TestHalfLife:
    def test_clear_decay(self):
        ts = [
            {"fingerprint": "fp_a", "bin": f"2026-02-25T{h:02d}:00:00+00:00", "posts": posts, "authors": 1, "avg_confidence": None, "evidence_classes": {"none": 1}}
            for h, posts in [(10, 2), (11, 10), (12, 8), (13, 4), (14, 2)]
        ]
        hl = compute_half_life(ts)
        assert hl is not None
        assert hl["peak_posts"] == 10
        assert hl["half_life_bins"] == 2  # peak at idx 1 (10), drops to 4 at idx 3

    def test_no_decay(self):
        ts = [
            {"fingerprint": "fp_a", "bin": f"2026-02-25T{h:02d}:00:00+00:00", "posts": posts, "authors": 1, "avg_confidence": None, "evidence_classes": {"none": 1}}
            for h, posts in [(10, 2), (11, 5), (12, 8), (13, 10)]
        ]
        hl = compute_half_life(ts)
        assert hl is not None
        assert hl["half_life_bins"] is None
        assert hl["decayed_to_bin"] is None

    def test_empty(self):
        assert compute_half_life([]) is None

    def test_single_post_peak(self):
        """Peak of 1 post — threshold < 1, returns None."""
        ts = [
            {"fingerprint": "fp_a", "bin": "2026-02-25T10:00:00+00:00", "posts": 1, "authors": 1, "avg_confidence": None, "evidence_classes": {"none": 1}},
        ]
        assert compute_half_life(ts) is None


# ---------------------------------------------------------------------------
# compute_burst_scores
# ---------------------------------------------------------------------------

class TestBurstScores:
    def test_spike_detection(self):
        """Steady baseline then a spike — burst_score should be high."""
        ts = []
        for h in range(10, 20):
            ts.append({
                "fingerprint": "fp_a",
                "bin": f"2026-02-25T{h:02d}:00:00+00:00",
                "posts": 2,
                "authors": 1,
                "avg_confidence": None,
                "evidence_classes": {"none": 1},
            })
        # spike in the last bin
        ts.append({
            "fingerprint": "fp_a",
            "bin": "2026-02-25T20:00:00+00:00",
            "posts": 20,
            "authors": 15,
            "avg_confidence": None,
            "evidence_classes": {"none": 3, "link": 2},
        })

        bursts = compute_burst_scores(ts)
        assert len(bursts) == 1
        assert bursts[0]["burst_score"] > 2.0
        assert bursts[0]["synchrony"] > 0.5

    def test_needs_min_2_bins(self):
        ts = [{"fingerprint": "fp_a", "bin": "2026-02-25T10:00:00+00:00", "posts": 5, "authors": 3, "avg_confidence": None, "evidence_classes": {"none": 1}}]
        bursts = compute_burst_scores(ts)
        assert len(bursts) == 0

    def test_multiple_fingerprints_sorted(self):
        """Multiple fingerprints — results sorted by burst_score desc."""
        ts = []
        for h in range(10, 13):
            ts.append({"fingerprint": "fp_low", "bin": f"2026-02-25T{h:02d}:00:00+00:00", "posts": 2, "authors": 1, "avg_confidence": None, "evidence_classes": {"none": 1}})
            ts.append({"fingerprint": "fp_high", "bin": f"2026-02-25T{h:02d}:00:00+00:00", "posts": 2, "authors": 1, "avg_confidence": None, "evidence_classes": {"none": 1}})
        # spike only fp_high
        ts.append({"fingerprint": "fp_high", "bin": "2026-02-25T13:00:00+00:00", "posts": 50, "authors": 30, "avg_confidence": None, "evidence_classes": {"none": 5, "link": 3, "embed": 2}})
        ts.append({"fingerprint": "fp_low", "bin": "2026-02-25T13:00:00+00:00", "posts": 3, "authors": 2, "avg_confidence": None, "evidence_classes": {"none": 1}})

        bursts = compute_burst_scores(ts)
        assert bursts[0]["fingerprint"] == "fp_high"
        assert bursts[0]["burst_score"] > bursts[1]["burst_score"]


# ---------------------------------------------------------------------------
# _entropy / _jsd
# ---------------------------------------------------------------------------

class TestInfoTheory:
    def test_entropy_uniform(self):
        assert abs(_entropy([10, 10]) - 1.0) < 0.001

    def test_entropy_singleton(self):
        assert _entropy([10]) == 0.0

    def test_entropy_empty(self):
        assert _entropy([]) == 0.0

    def test_jsd_identical(self):
        p = {"a": 5, "b": 5}
        assert _jsd(p, p) < 0.001

    def test_jsd_disjoint(self):
        p = {"a": 10}
        q = {"b": 10}
        assert _jsd(p, q) > 0.9  # max JSD for disjoint is 1.0

    def test_jsd_empty(self):
        assert _jsd({}, {}) == 0.0


# ---------------------------------------------------------------------------
# label_rate_vectors + detect_regime_shifts
# ---------------------------------------------------------------------------

class TestRegimeShifts:
    def test_label_rate_vectors(self):
        conn = get_conn()
        _insert_decision(conn, "d1", "2026-02-25T10:00:00+00:00", "infobait")
        _insert_decision(conn, "d2", "2026-02-25T10:30:00+00:00", "infogloss")
        _insert_decision(conn, "d3", "2026-02-25T11:00:00+00:00", "infobait")
        conn.commit()

        vecs = label_rate_vectors(conn=conn, bin_hours=1)
        conn.close()

        assert len(vecs) == 2
        assert vecs[0]["label_counts"]["infobait"] == 1
        assert vecs[0]["label_counts"]["infogloss"] == 1
        assert vecs[1]["label_counts"]["infobait"] == 1

    def test_detect_shift(self):
        """Sudden label distribution change should flag a shift."""
        vectors = []
        # 10 bins of mostly "infobait"
        for h in range(10):
            vectors.append({"bin": f"2026-02-25T{h:02d}:00:00+00:00", "label_counts": {"infobait": 10, "infogloss": 1}, "total": 11})
        # sudden shift to infoshield
        vectors.append({"bin": "2026-02-25T10:00:00+00:00", "label_counts": {"infoshield": 10, "infobait": 1}, "total": 11})

        shifts = detect_regime_shifts(vectors, baseline_bins=10, threshold=0.2)
        # the last entry should be flagged
        flagged = [s for s in shifts if s["is_shift"]]
        assert len(flagged) >= 1
        assert flagged[-1]["bin"] == "2026-02-25T10:00:00+00:00"

    def test_no_shift_stable(self):
        vectors = [
            {"bin": f"2026-02-25T{h:02d}:00:00+00:00", "label_counts": {"infobait": 5}, "total": 5}
            for h in range(5)
        ]
        shifts = detect_regime_shifts(vectors, threshold=0.3)
        flagged = [s for s in shifts if s["is_shift"]]
        assert len(flagged) == 0

    def test_too_few_vectors(self):
        assert detect_regime_shifts([{"bin": "x", "label_counts": {}, "total": 0}]) == []


# ---------------------------------------------------------------------------
# cluster_report (integration)
# ---------------------------------------------------------------------------

class TestClusterReport:
    def test_empty_db_returns_structure(self):
        conn = get_conn()
        report = cluster_report(conn=conn, hours=24)
        conn.close()

        assert "generated_at" in report
        assert "clusters" in report
        assert "regime_shifts" in report
        assert report["clusters"] == []

    def test_with_data(self):
        conn = get_conn()
        # Populate claim_history across several hours
        for h in range(10, 15):
            for i in range(h - 9):  # ramp up
                _insert_claim(conn, f"did:user{i}", "fp_hot", f"2026-02-25T{h:02d}:{i*5:02d}:00+00:00", 0.5, f"ev{i}")
        # A second quiet fingerprint
        _insert_claim(conn, "did:alice", "fp_quiet", "2026-02-25T12:00:00+00:00", 0.3, "ev_q")
        _insert_claim(conn, "did:alice", "fp_quiet", "2026-02-25T13:00:00+00:00", 0.3, "ev_q")
        conn.commit()

        report = cluster_report(conn=conn, hours=48, bin_hours=1)
        conn.close()

        assert len(report["clusters"]) >= 1
        # fp_hot should rank higher than fp_quiet
        fps = [c["fingerprint"] for c in report["clusters"]]
        assert "fp_hot" in fps
