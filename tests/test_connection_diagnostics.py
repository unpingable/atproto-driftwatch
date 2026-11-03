from labeler.connection_diagnostics import (
    connection_failure_record,
    sanitize_close_reason,
)


class Frame:
    def __init__(self, code, reason):
        self.code = code
        self.reason = reason


class Closed(Exception):
    def __init__(self):
        self.code = 1013
        self.reason = "retry https://example.invalid/path did:plc:sensitive"
        self.rcvd = Frame(1013, self.reason)
        self.sent = Frame(1013, "ack")
        self.rcvd_then_sent = True


def test_reason_is_bounded_and_identity_safe():
    value = sanitize_close_reason("see at://did:plc:abc/post/1 " + "x" * 300)
    assert "did:plc" not in value
    assert "at://" not in value
    assert len(value) <= 160


def test_close_record_preserves_protocol_facts_not_sensitive_text():
    record = connection_failure_record(Closed(), queue_depth=5000)
    assert record["initiator"] == "peer"
    assert record["deadline_expired"] is False
    assert record["close_received"]["code"] == 1013
    assert record["queue_depth"] == 5000
    rendered = repr(record)
    assert "example.invalid" not in rendered
    assert "did:plc" not in rendered
