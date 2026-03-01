"""Tests for the detection envelope and evidence spine (M1+M2)."""

import json
import os
import pytest
from labeler.detection import (
    ENVELOPE_SCHEMA_VERSION,
    MAX_EXPLAIN_TOP_K,
    MAX_EXPLAIN_STR_LEN,
    SubjectRef,
    EvidenceStub,
    DetectionEnvelope,
    canonicalize,
    stable_json,
    receipt_hash,
    normalize_sql,
    severity_rank,
    compute_det_id,
    compute_window_fingerprint,
    build_envelope,
    envelope_to_dict,
    validate_envelope,
    sort_detections,
    make_query_commitment,
    make_event_range,
    make_hashset_root,
    make_note,
)


# ---------------------------------------------------------------------------
# SubjectRef / EvidenceStub basics
# ---------------------------------------------------------------------------

class TestSubjectRef:
    def test_frozen(self):
        s = SubjectRef("fingerprint", "abc123")
        with pytest.raises(AttributeError):
            s.type = "did"

    def test_global_subject(self):
        s = SubjectRef("global", "")
        assert s.type == "global"
        assert s.value == ""


class TestEvidenceStub:
    def test_frozen(self):
        e = EvidenceStub("note", "some note")
        with pytest.raises(AttributeError):
            e.kind = "other"

    def test_note_kinds(self):
        e = make_note("hello world")
        assert e.kind == "note"
        assert e.ref == "hello world"

    def test_note_truncation(self):
        long_text = "x" * 500
        e = make_note(long_text)
        assert len(e.ref) == MAX_EXPLAIN_STR_LEN


# ---------------------------------------------------------------------------
# Canonicalization
# ---------------------------------------------------------------------------

class TestCanonicalize:
    def test_dict_key_sorting(self):
        obj = {"b": 1, "a": 2}
        assert canonicalize(obj) == {"a": 2, "b": 1}

    def test_nested_dict_sorting(self):
        obj = {"z": {"b": 1, "a": 2}, "a": 0}
        result = canonicalize(obj)
        assert list(result.keys()) == ["a", "z"]
        assert list(result["z"].keys()) == ["a", "b"]

    def test_float_quantization(self):
        assert canonicalize(0.1234567890) == 0.123457

    def test_nan_rejected(self):
        import math
        with pytest.raises(ValueError, match="NaN"):
            canonicalize(float("nan"))

    def test_inf_rejected(self):
        with pytest.raises(ValueError, match="Inf"):
            canonicalize(float("inf"))

    def test_set_sorted(self):
        result = canonicalize({3, 1, 2})
        assert result == [1, 2, 3]

    def test_none_preserved(self):
        assert canonicalize(None) is None

    def test_bool_preserved(self):
        assert canonicalize(True) is True
        assert canonicalize(False) is False

    def test_int_unchanged(self):
        assert canonicalize(42) == 42

    def test_string_unchanged(self):
        assert canonicalize("hello") == "hello"

    def test_list_order_preserved(self):
        assert canonicalize([3, 1, 2]) == [3, 1, 2]

    def test_tuple_becomes_list(self):
        assert canonicalize((1, 2, 3)) == [1, 2, 3]

    def test_unsupported_type(self):
        with pytest.raises(TypeError, match="unsupported type"):
            canonicalize(object())


# ---------------------------------------------------------------------------
# Stable JSON + Receipt Hash
# ---------------------------------------------------------------------------

class TestStableJson:
    def test_compact_separators(self):
        result = stable_json({"a": 1})
        assert result == '{"a":1}'

    def test_deterministic(self):
        obj = {"b": 1.0, "a": [3, 2, 1]}
        assert stable_json(obj) == stable_json(obj)

    def test_key_order_doesnt_matter(self):
        a = stable_json({"b": 1, "a": 2})
        b = stable_json({"a": 2, "b": 1})
        assert a == b


class TestReceiptHash:
    def test_deterministic(self):
        obj = {"detector": "burst", "score": 5.0}
        h1 = receipt_hash(obj)
        h2 = receipt_hash(obj)
        assert h1 == h2

    def test_length(self):
        h = receipt_hash({"a": 1})
        assert len(h) == 16

    def test_hex_chars(self):
        h = receipt_hash({"a": 1})
        assert all(c in "0123456789abcdef" for c in h)

    def test_different_inputs(self):
        h1 = receipt_hash({"a": 1})
        h2 = receipt_hash({"a": 2})
        assert h1 != h2

    def test_float_quantization_stability(self):
        """Same value with tiny floating point difference → same hash."""
        h1 = receipt_hash({"score": 0.1 + 0.2})
        h2 = receipt_hash({"score": 0.3})
        assert h1 == h2


# ---------------------------------------------------------------------------
# SQL normalizer
# ---------------------------------------------------------------------------

class TestNormalizeSql:
    def test_collapses_whitespace(self):
        sql = "SELECT  a, b\n  FROM  table\n  WHERE  x = 1"
        assert normalize_sql(sql) == "SELECT a, b FROM table WHERE x = 1"

    def test_idempotent(self):
        sql = "SELECT a FROM b"
        assert normalize_sql(normalize_sql(sql)) == normalize_sql(sql)


# ---------------------------------------------------------------------------
# Severity
# ---------------------------------------------------------------------------

class TestSeverityRank:
    def test_ordering(self):
        assert severity_rank("info") == 0
        assert severity_rank("low") == 1
        assert severity_rank("med") == 2
        assert severity_rank("high") == 3
        assert severity_rank("critical") == 4

    def test_unknown(self):
        assert severity_rank("bogus") == -1


# ---------------------------------------------------------------------------
# Window fingerprint
# ---------------------------------------------------------------------------

class TestWindowFingerprint:
    def test_deterministic(self):
        args = {
            "ts_start": "2026-02-28T00:00:00+00:00",
            "ts_end": "2026-02-28T01:00:00+00:00",
            "window": "1h",
            "schema_version": 1,
            "config_hash": "abc123",
            "watermark_snapshot": {"coverage_pct": 0.95, "health_state": "ok"},
        }
        wf1 = compute_window_fingerprint(**args)
        wf2 = compute_window_fingerprint(**args)
        assert wf1 == wf2
        assert len(wf1) == 16

    def test_changes_with_config(self):
        base = {
            "ts_start": "2026-02-28T00:00:00+00:00",
            "ts_end": "2026-02-28T01:00:00+00:00",
            "window": "1h",
            "schema_version": 1,
            "watermark_snapshot": {},
        }
        wf1 = compute_window_fingerprint(config_hash="v1", **base)
        wf2 = compute_window_fingerprint(config_hash="v2", **base)
        assert wf1 != wf2


# ---------------------------------------------------------------------------
# det_id
# ---------------------------------------------------------------------------

class TestDetId:
    def test_deterministic(self):
        args = ("burst_score", "v1", "", SubjectRef("fingerprint", "abc"), "burst")
        d1 = compute_det_id(*args)
        d2 = compute_det_id(*args)
        assert d1 == d2

    def test_m1_placeholder(self):
        """Empty window_fingerprint uses m1_unfingerprinted placeholder."""
        d1 = compute_det_id("burst", "v1", "", SubjectRef("global", ""), "burst")
        d2 = compute_det_id("burst", "v1", "real_wfp", SubjectRef("global", ""), "burst")
        assert d1 != d2  # different because placeholder vs real


# ---------------------------------------------------------------------------
# Envelope construction
# ---------------------------------------------------------------------------

class TestBuildEnvelope:
    def _make_simple(self, **overrides):
        defaults = {
            "detector_id": "test_sensor",
            "detector_version": "v1",
            "ts_start": "2026-02-28T00:00:00+00:00",
            "ts_end": "2026-02-28T01:00:00+00:00",
            "window": "1h",
            "subject": SubjectRef("global", ""),
            "detection_type": "test",
            "score": 0.5,
            "severity": "info",
            "explain": {"reason": "testing"},
            "evidence": (),
        }
        defaults.update(overrides)
        return build_envelope(**defaults)

    def test_basic_construction(self):
        e = self._make_simple()
        assert e.envelope_schema_version == ENVELOPE_SCHEMA_VERSION
        assert e.detector_id == "test_sensor"
        assert e.receipt_hash != ""
        assert e.det_id != ""
        assert len(e.receipt_hash) == 16
        assert len(e.det_id) == 16

    def test_explain_capping(self):
        big_list = list(range(50))
        e = self._make_simple(explain={"items": big_list})
        assert len(e.explain["items"]) == MAX_EXPLAIN_TOP_K
        assert e.explain.get("truncated") is True

    def test_explain_string_capping(self):
        long_str = "x" * 500
        e = self._make_simple(explain={"msg": long_str})
        assert len(e.explain["msg"]) == MAX_EXPLAIN_STR_LEN
        assert e.explain.get("truncated") is True

    def test_receipt_hash_deterministic(self):
        e1 = self._make_simple()
        e2 = self._make_simple()
        assert e1.receipt_hash == e2.receipt_hash

    def test_evidence_sorted(self):
        ev = (
            EvidenceStub("note", "zzz"),
            EvidenceStub("hashset_root", "aaa"),
            EvidenceStub("note", "aaa"),
        )
        e = self._make_simple(evidence=ev)
        kinds = [s.kind for s in e.evidence]
        refs = [s.ref for s in e.evidence]
        assert kinds == ["hashset_root", "note", "note"]
        assert refs == ["aaa", "aaa", "zzz"]

    def test_score_rounded(self):
        e = self._make_simple(score=0.123456789)
        assert e.score == 0.123457


# ---------------------------------------------------------------------------
# envelope_to_dict
# ---------------------------------------------------------------------------

class TestEnvelopeToDict:
    def test_serializable(self):
        e = build_envelope(
            detector_id="test",
            detector_version="v1",
            ts_start="2026-02-28T00:00:00+00:00",
            ts_end="2026-02-28T01:00:00+00:00",
            window="1h",
            subject=SubjectRef("fingerprint", "abc"),
            detection_type="burst",
            score=2.5,
            severity="low",
            explain={"reason": "spike"},
            evidence=(EvidenceStub("note", "test"),),
        )
        d = envelope_to_dict(e)
        # Must be JSON-serializable
        s = json.dumps(d)
        assert '"detector_id":"test"' in s.replace(" ", "")

    def test_subject_flattened(self):
        e = build_envelope(
            detector_id="test",
            detector_version="v1",
            ts_start="t0",
            ts_end="t1",
            window="1h",
            subject=SubjectRef("did", "did:plc:xyz"),
            detection_type="test",
            score=0.0,
            severity="info",
            explain={},
        )
        d = envelope_to_dict(e)
        assert d["subject"] == {"type": "did", "value": "did:plc:xyz"}


# ---------------------------------------------------------------------------
# validate_envelope
# ---------------------------------------------------------------------------

class TestValidateEnvelope:
    def _make_valid(self):
        return build_envelope(
            detector_id="test",
            detector_version="v1",
            ts_start="t0",
            ts_end="t1",
            window="1h",
            subject=SubjectRef("global", ""),
            detection_type="test",
            score=0.0,
            severity="info",
            explain={"reason": "test"},
        )

    def test_valid_passes_strict(self):
        e = self._make_valid()
        violations = validate_envelope(e, strict=True)
        assert violations == []

    def test_bad_severity_strict(self):
        e = DetectionEnvelope(
            envelope_schema_version=ENVELOPE_SCHEMA_VERSION,
            detector_id="test",
            detector_version="v1",
            ts_start="t0",
            ts_end="t1",
            window="1h",
            subject=SubjectRef("global", ""),
            type="test",
            score=0.0,
            severity="bogus",
            explain={},
            evidence=(),
            window_fingerprint="",
            config_hash="",
            receipt_hash="",
            det_id="",
        )
        with pytest.raises(ValueError, match="invalid severity"):
            validate_envelope(e, strict=True)

    def test_bad_subject_type_strict(self):
        e = DetectionEnvelope(
            envelope_schema_version=ENVELOPE_SCHEMA_VERSION,
            detector_id="test",
            detector_version="v1",
            ts_start="t0",
            ts_end="t1",
            window="1h",
            subject=SubjectRef("bogus_type", ""),
            type="test",
            score=0.0,
            severity="info",
            explain={},
            evidence=(),
            window_fingerprint="",
            config_hash="",
            receipt_hash="",
            det_id="",
        )
        with pytest.raises(ValueError, match="invalid subject.type"):
            validate_envelope(e, strict=True)

    def test_evidence_required_with_wfp_strict(self):
        e = DetectionEnvelope(
            envelope_schema_version=ENVELOPE_SCHEMA_VERSION,
            detector_id="test",
            detector_version="v1",
            ts_start="t0",
            ts_end="t1",
            window="1h",
            subject=SubjectRef("global", ""),
            type="test",
            score=0.0,
            severity="info",
            explain={},
            evidence=(),
            window_fingerprint="some_wfp",
            config_hash="",
            receipt_hash="",
            det_id="",
        )
        with pytest.raises(ValueError, match="evidence empty"):
            validate_envelope(e, strict=True)

    def test_non_strict_returns_violations(self):
        e = DetectionEnvelope(
            envelope_schema_version=ENVELOPE_SCHEMA_VERSION,
            detector_id="test",
            detector_version="v1",
            ts_start="t0",
            ts_end="t1",
            window="1h",
            subject=SubjectRef("global", ""),
            type="test",
            score=0.0,
            severity="bogus",
            explain={},
            evidence=(),
            window_fingerprint="",
            config_hash="",
            receipt_hash="",
            det_id="",
        )
        violations = validate_envelope(e, strict=False)
        assert len(violations) >= 1
        assert any("severity" in v for v in violations)


# ---------------------------------------------------------------------------
# sort_detections
# ---------------------------------------------------------------------------

class TestSortDetections:
    def test_severity_first(self):
        high = build_envelope(
            detector_id="a", detector_version="v1",
            ts_start="t0", ts_end="t1", window="1h",
            subject=SubjectRef("global", ""),
            detection_type="test", score=1.0, severity="high",
            explain={},
        )
        low = build_envelope(
            detector_id="b", detector_version="v1",
            ts_start="t0", ts_end="t1", window="1h",
            subject=SubjectRef("global", ""),
            detection_type="test", score=5.0, severity="low",
            explain={},
        )
        result = sort_detections([low, high])
        assert result[0].severity == "high"


# ---------------------------------------------------------------------------
# Evidence stub factories
# ---------------------------------------------------------------------------

class TestEvidenceFactories:
    def test_query_commitment(self):
        ev = make_query_commitment("SELECT 1", [42], "result_abc")
        assert ev.kind == "query_commitment"
        assert len(ev.ref) == 16

    def test_event_range(self):
        ev = make_event_range("c_start", "c_end", 500)
        assert ev.kind == "event_range"

    def test_hashset_root(self):
        ev = make_hashset_root(["fp_a", "fp_b"], 100)
        assert ev.kind == "hashset_root"

    def test_hashset_root_deterministic_order(self):
        ev1 = make_hashset_root(["fp_b", "fp_a"], 100)
        ev2 = make_hashset_root(["fp_a", "fp_b"], 100)
        assert ev1.ref == ev2.ref  # sorted internally


# ---------------------------------------------------------------------------
# EFF-002 ban: no COUNT(DISTINCT author) in sensors
# ---------------------------------------------------------------------------

class TestEfficiencyBan:
    def test_no_distinct_author_in_sensors(self):
        """Grep sensor source for COUNT(DISTINCT in executable code (not comments/docstrings)."""
        import ast
        import glob
        sensor_dir = os.path.join(
            os.path.dirname(__file__), "..", "src", "labeler", "sensors"
        )
        sensor_dir = os.path.normpath(sensor_dir)
        violations = []
        for path in glob.glob(os.path.join(sensor_dir, "*.py")):
            with open(path) as f:
                source = f.read()
            # Parse AST to find string literals containing COUNT(DISTINCT
            # that are NOT in docstring position (first expr of module/class/func)
            try:
                tree = ast.parse(source)
            except SyntaxError:
                continue
            # Collect docstring line ranges to exclude
            docstring_lines = set()
            for node in ast.walk(tree):
                if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                    if (node.body and isinstance(node.body[0], ast.Expr)
                            and isinstance(node.body[0].value, ast.Constant)):
                        ds = node.body[0]
                        for ln in range(ds.lineno, ds.end_lineno + 1):
                            docstring_lines.add(ln)
            # Check non-docstring, non-comment lines
            for lineno, line in enumerate(source.splitlines(), 1):
                if lineno in docstring_lines:
                    continue
                stripped = line.lstrip()
                if stripped.startswith("#"):
                    continue
                if "COUNT(DISTINCT" in line.upper():
                    violations.append(f"{os.path.basename(path)}:{lineno}")
        assert violations == [], f"EFF-002 violation: COUNT(DISTINCT found in {violations}"
