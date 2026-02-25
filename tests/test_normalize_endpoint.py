from labeler.cooldown import normalize_endpoint


def test_normalize_various():
    assert normalize_endpoint("https://labeler.example/xrpc") == "labeler.example"
    assert normalize_endpoint("https://labeler.example:8080/xrpc") == "labeler.example:8080"
    assert normalize_endpoint("labeler.example") == "labeler.example"
    assert normalize_endpoint("127.0.0.1:8080/path") == "127.0.0.1:8080"
    assert normalize_endpoint("") == ""
    assert normalize_endpoint(None) is None
