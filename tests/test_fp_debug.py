import json
from labeler.claims import fingerprint_debug


def test_fingerprint_debug_contains_config_and_source():
    text = "Confirmed: 1,234 people were affected, according to source X."
    d = fingerprint_debug(text)
    assert "fingerprint" in d
    assert "source" in d
    assert d["fingerprint_version"]
    assert "config" in d
    assert d["config"]["number_mode"] in ("exact", "bucket", "redact")
    # ensure source contains a normalized number token under default bucket mode
    assert "1k" in d["source"] or "1200" in d["source"] or "<NUM>" in d["source"]
