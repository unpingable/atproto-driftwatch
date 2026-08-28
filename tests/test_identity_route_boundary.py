"""The identity-bearing HTTP routes sit behind the administrative boundary.

`docs/architecture/PUBLIC_SURFACES.md` classifies per-DID surfaces as forbidden
as *public* surfaces, and the 2026-07-17 codex audit (finding #4, "INTERSECTS
RATIFIED DOCTRINE") recorded that `/exposure/{did}` and `/strain/top` were
exposed anyway. These tests pin the repair so the divergence cannot silently
return.

What is asserted here is route *semantics*, not deployment reachability: the
production container binds to loopback, but a bind is defence in depth, not a
substitute for the route carrying its own boundary.

Note the deliberate limit of the mechanism under test. `admin_auth` is a no-op
when `ADMIN_API_TOKEN` is unset — that is pre-existing, shared with
`/recent-decisions` and `/admin/*`, and is asserted rather than quietly fixed,
because changing it would alter the behaviour of already-protected routes.
Setting the token is a deployment prerequisite, not something this file can
establish.
"""
import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from labeler.main import app

# The routes that return identity-bearing material and must not be open.
# `/labels/{subject_uri}` is a single path segment, so a real `at://` URI never
# matches it (see test_at_uri_subject_does_not_route below). The reachable
# shape is a slash-free subject, which is what is exercised here.
IDENTITY_ROUTES = (
    "/exposure/did:plc:example",
    "/strain/top",
    "/labels/subject-without-slashes",
)

# Routes that are deliberately open: liveness and operational telemetry that
# carry no per-account material. Guarded so a future edit that starts leaking
# identity through a health route is caught here too.
OPEN_OPERATIONAL_ROUTES = (
    "/health",
)


@pytest.fixture
def token(monkeypatch):
    monkeypatch.setenv("ADMIN_API_TOKEN", "s3cr3t")
    return "s3cr3t"


@pytest.mark.parametrize("path", IDENTITY_ROUTES)
def test_identity_route_refuses_unauthenticated_request(token, path):
    """No token -> 401, before any query runs."""
    client = TestClient(app)
    r = client.get(path)
    assert r.status_code == 401, (
        f"{path} answered {r.status_code} without an admin token; "
        "identity-bearing routes must sit behind the administrative boundary"
    )


@pytest.mark.parametrize("path", IDENTITY_ROUTES)
def test_identity_route_refuses_wrong_token(token, path):
    client = TestClient(app)
    r = client.get(path, headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401


@pytest.mark.parametrize("path", IDENTITY_ROUTES)
def test_identity_route_leaks_nothing_in_the_refusal(token, path):
    """A refusal must not answer the question it refused."""
    client = TestClient(app)
    r = client.get(path)
    body = r.text
    assert "did:plc:" not in body
    assert "at://" not in body
    assert "incoming_edges" not in body
    assert "author" not in body


@pytest.mark.parametrize("path", IDENTITY_ROUTES)
def test_identity_route_admits_authenticated_request(token, path):
    """The repair is an auth boundary, not a removal: with the token the route
    still works. 404 is a legitimate answer for an absent subject; 401 is not.
    """
    client = TestClient(app)
    for headers in ({"Authorization": f"Bearer {token}"}, {"X-Admin-Token": token}):
        r = client.get(path, headers=headers)
        assert r.status_code != 401, f"{path} rejected a valid admin token"
        assert r.status_code in (200, 404, 503), (
            f"{path} answered {r.status_code} for an authenticated caller"
        )


@pytest.mark.parametrize("path", OPEN_OPERATIONAL_ROUTES)
def test_operational_routes_stay_open(token, path):
    """The repair is bounded: liveness must not require a token."""
    client = TestClient(app)
    r = client.get(path)
    assert r.status_code == 200


def test_identity_routes_match_the_already_protected_route(token):
    """`/labels/{uri}` and `/recent-decisions` return the same data class
    (subject URIs). They must agree about protection.
    """
    client = TestClient(app)
    protected = client.get("/recent-decisions")
    subject = client.get(IDENTITY_ROUTES[2])
    assert protected.status_code == subject.status_code == 401


def test_at_uri_subject_does_not_route(token):
    """Pre-existing routing quirk, pinned so the boundary claim stays precise.

    `/labels/{subject_uri}` captures one path segment, so an `at://` URI —
    which contains slashes — misses the route entirely and 404s at routing,
    before the auth dependency runs. That is not a leak (nothing is answered),
    but it does mean this route cannot serve the subject shape its name
    implies. Recorded, not repaired: fixing the path shape is an API change,
    which is out of scope for a boundary-hardening campaign.
    """
    client = TestClient(app)
    r = client.get("/labels/at://did:plc:example/app.bsky.feed.post/abc")
    assert r.status_code == 404
    assert "did:plc:" not in r.text


def test_admin_auth_is_a_noop_without_a_configured_token(monkeypatch):
    """Documented limit, asserted rather than assumed.

    This is the pre-existing semantics of the shared `admin_auth` dependency —
    `/admin/*` and `/recent-decisions` have always behaved this way. It is
    pinned here so the deployment prerequisite (set ADMIN_API_TOKEN) stays
    visible, and so that a future change to `admin_auth` has to come past this
    test deliberately.
    """
    monkeypatch.delenv("ADMIN_API_TOKEN", raising=False)
    client = TestClient(app)
    r = client.get("/strain/top")
    assert r.status_code != 401
