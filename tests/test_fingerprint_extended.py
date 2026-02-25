import json
import importlib
from collections import Counter, defaultdict
from pathlib import Path
import os


def _load_items(path):
    p = Path(path)
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def _fps_from_items(items, claims_mod):
    fps_by_group = defaultdict(list)
    for it in items:
        t = it["text"]
        g = it.get("group")
        fps_by_group[g].append(claims_mod.fingerprint_text(t))
    return fps_by_group


def test_group_collision_rate_default():
    # Default behavior should collapse many paraphrases for the same group
    items = _load_items("fixtures/fingerprint_extended.jsonl")
    import labeler.claims as claims
    importlib.reload(claims)
    fps_by_group = _fps_from_items(items, claims)

    # compute per-group majority collision and overall coverage
    totals = 0
    majority_sum = 0
    for g, fps in fps_by_group.items():
        totals += len(fps)
        most_common = Counter(fps).most_common(1)[0][1]
        majority_sum += most_common
    # require that at least 50% of inputs collapse to their group-majority fingerprint (practical threshold)
    assert majority_sum / totals >= 0.5, f"collision rate too low: {majority_sum}/{totals}"


def test_near_miss_separation():
    # ensure magnitude near-miss (20 vs 200) are distinct
    import labeler.claims as claims
    importlib.reload(claims)
    fa = claims.fingerprint_text("200 people were affected.")
    fb = claims.fingerprint_text("20 people were affected.")
    assert fa != fb

    # ensure different claim is separated (affected vs evacuated)
    f1 = claims.fingerprint_text("200 people were affected.")
    f2 = claims.fingerprint_text("200 people were evacuated.")
    assert f1 != f2


def test_number_mode_redact_increases_collision(monkeypatch):
    # redact mode should collapse different numbers to a shared token, increasing collisions
    monkeypatch.setenv("FINGERPRINT_NUMBER_MODE", "redact")
    # reload module to pick up env
    import importlib
    import labeler.claims as claims
    importlib.reload(claims)

    items = _load_items("fixtures/fingerprint_extended.jsonl")
    fps_by_group = _fps_from_items(items, claims)
    # check that the affected1234 group now mostly collapses due to redacting numbers
    g = "affected1234"
    most_common = Counter(fps_by_group[g]).most_common(1)[0][1]
    # expect at least 2/3 of group to collapse when numbers are redacted
    import math
    assert most_common >= math.ceil(len(fps_by_group[g]) * 0.66), f"redact collapse too low: {most_common}/{len(fps_by_group[g])}"


def test_fp_debug_outputs_source_for_fixture():
    import labeler.claims as claims
    importlib.reload(claims)
    items = _load_items("fixtures/fingerprint_extended.jsonl")
    # pick an item and ensure fingerprint_debug exposes the source and config keys
    d = claims.fingerprint_debug(items[0]["text"])
    assert "source" in d and "fingerprint" in d and "fingerprint_version" in d and "config" in d
