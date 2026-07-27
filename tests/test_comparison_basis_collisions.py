"""Collision specimens for the claim-drift comparison basis.

These are CHARACTERIZATION tests. They assert what the current implementation
does, not what it should do. Each one exhibits a pair of observations that
collide under the coarse view driftwatch actually uses, while differing in the
target property the emitted label names.

They exist so the defect is executable rather than argued. If a future change
enriches the view or binds the comparison basis, these tests are expected to
fail — that failure is the signal, and the right response is to update the
specimens along with `docs/architecture/COMPARISON-BASIS-BINDING.md`.

Nothing here imports or modifies production behavior beyond calling it.
"""

import json

from labeler.claims import (
    compare_claim_states,
    compute_claim_state_from_post,
    fingerprint_text,
)
from labeler.drift.diff import assertiveness_score
from labeler.drift.extract import extract_claim_signals


# The rule's own predicate, restated here so the specimens are self-contained.
# Mirrors src/labeler/drift/rules.py:144 with the default ASSERTIVENESS_DELTA.
ASSERTIVENESS_DELTA_DEFAULT = 0.2


def _post(text):
    return {"text": text, "externalLinks": [], "embeds": [], "facets": []}


def _rule_would_fire(prior_text, current_text, threshold=ASSERTIVENESS_DELTA_DEFAULT):
    deltas = compare_claim_states(
        compute_claim_state_from_post(_post(prior_text)),
        compute_claim_state_from_post(_post(current_text)),
    )
    fires = deltas["confidence_delta"] >= threshold and not deltas["evidence_changed"]
    return fires, deltas


BARE = "200 people were affected in Springfield."
HEDGED = "Reportedly 200 people were affected in Springfield."
HEDGED_DOUBLE = (
    "Reportedly 200 people were affected in Springfield, according to officials."
)
CERTAIN = "Confirmed: 200 people were affected in Springfield."


def test_specimen_1_same_claim_identity():
    """All four variants are the same claim by driftwatch's own identity.

    Without this, the specimens below would be comparing different claims and
    the collision would be uninteresting.
    """
    fingerprints = {
        fingerprint_text(t) for t in (BARE, HEDGED, HEDGED_DOUBLE, CERTAIN)
    }
    assert len(fingerprints) == 1


def test_specimen_2_view_collides_on_hedged_versus_certain():
    """The coarse view cannot separate attribution from certainty.

    `assertiveness_score` counts MODAL_RE hits, and MODAL_RE
    (src/labeler/drift/extract.py:9) contains both certainty markers
    ("confirmed", "definitely") and attribution markers ("reportedly",
    "according to"). One hedging token and one certainty token score
    identically.
    """
    hedged = assertiveness_score(extract_claim_signals(HEDGED))
    certain = assertiveness_score(extract_claim_signals(CERTAIN))

    assert hedged == certain == 1 / 3
    # ...while the target property separates them: one attributes, one asserts.
    assert extract_claim_signals(HEDGED).modal == ["Reportedly"]
    assert extract_claim_signals(CERTAIN).modal == ["Confirmed"]


def test_specimen_3_adding_attribution_fires_assertiveness_increase():
    """False positive: hedging a bare claim reads as an assertiveness increase.

    The author moved from a bare assertion to an attributed one — assertiveness
    went DOWN — and the rule emits `assertiveness_increase_possible`.
    """
    fires, deltas = _rule_would_fire(BARE, HEDGED_DOUBLE)

    assert deltas["confidence_delta"] == 2 / 3
    assert deltas["evidence_changed"] is False
    assert fires is True


def test_specimen_4_removing_attribution_does_not_fire():
    """False negative, and the disambiguating field is computed then dropped.

    "Reportedly X" -> "Confirmed: X" is the textbook case the rule is named
    for. The delta is exactly zero, so no label. `compare_claim_states` does
    compute `attribution_removed=True` — the rule at
    src/labeler/drift/rules.py:144 reads only `confidence_delta` and
    `evidence_changed`, so that field never reaches the decision.
    """
    fires, deltas = _rule_would_fire(HEDGED, CERTAIN)

    assert deltas["confidence_delta"] == 0.0
    assert deltas["attribution_removed"] is True
    assert fires is False


def test_specimen_5_prior_endpoint_is_reconstructed_from_mutable_storage():
    """An edited post's prior state is reconstructed as its post-edit state.

    `claim_history` records `post_cid`, which identifies the exact record
    version that was witnessed. The comparison path fetches prior content with
    `SELECT raw FROM events WHERE event_uri = ?`
    (src/labeler/drift/rules.py:129), keyed on the mutable URI and ignoring the
    CID. `events.raw` is overwritten in place on edit
    (src/labeler/db.py:384), so the row returned for an older history entry is
    the current text, not the observed text.

    The pre-edit content is not lost — `event_versions` holds it — but no
    analysis path reads that table.
    """
    from labeler.db import get_conn, insert_event

    author = "did:spec:alice"
    uri = "uri:spec:comparison-basis:1"
    declared = "2026-01-01T10:00:00"

    def event(cid, text):
        return {
            "uri": uri,
            "cid": cid,
            "text": text,
            "createdAt": declared,
            "authorDid": author,
            "externalLinks": [],
            "embeds": [],
            "facets": [],
        }

    v1 = event("cid-v1", BARE)
    insert_event(uri, declared, author, v1)
    v2 = event("cid-v2", CERTAIN)
    insert_event(uri, declared, author, v2)

    conn = get_conn()
    history = conn.execute(
        "SELECT post_cid, createdAt FROM claim_history WHERE post_uri = ? ORDER BY rowid",
        (uri,),
    ).fetchall()
    versions = conn.execute("SELECT COUNT(*) FROM event_versions").fetchone()[0]
    raw_now = json.loads(
        conn.execute("SELECT raw FROM events WHERE event_uri = ?", (uri,)).fetchone()[0]
    )
    conn.close()

    # Two observations, distinguishable only by CID.
    assert [h[0] for h in history] == ["cid-v1", "cid-v2"]
    # The declared timestamp — the rule's ordering key — is identical for both.
    assert history[0][1] == history[1][1]
    # The pre-edit content was retained, in a table nothing reads.
    assert versions == 1
    # Fetching the v1 observation by URI returns the v2 text.
    assert raw_now["text"] == CERTAIN
    assert compute_claim_state_from_post(raw_now)["confidence"] == 1 / 3


def test_specimen_6_edit_is_unreachable_as_a_prior():
    """The rule's prior-selection predicate excludes same-instant edits.

    `rule_assertiveness_increase` selects a prior with
    `h["createdAt"] < post.createdAt` (src/labeler/drift/rules.py:123). An edit
    does not change the record's declared `createdAt`, so both history rows
    carry the same value and neither can be the other's prior.
    """
    from labeler.db import get_conn, insert_event

    author = "did:spec:bob"
    uri = "uri:spec:comparison-basis:2"
    declared = "2026-02-02T12:00:00"

    for cid, text in (("cid-a", HEDGED), ("cid-b", CERTAIN)):
        insert_event(
            uri,
            declared,
            author,
            {
                "uri": uri,
                "cid": cid,
                "text": text,
                "createdAt": declared,
                "authorDid": author,
                "externalLinks": [],
                "embeds": [],
                "facets": [],
            },
        )

    conn = get_conn()
    rows = conn.execute(
        "SELECT createdAt FROM claim_history WHERE post_uri = ?", (uri,)
    ).fetchall()
    conn.close()

    stamps = {r[0] for r in rows}
    assert len(rows) == 2
    assert len(stamps) == 1

    current = stamps.pop()
    # No history row is strictly earlier than the current observation, so the
    # rule returns before ever computing a delta.
    assert not [r for r in rows if r[0] < current]
