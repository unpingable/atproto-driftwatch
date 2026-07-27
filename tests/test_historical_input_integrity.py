"""Regression specimens for historical-input integrity in the drift rules.

Every drift rule that reasons about a prior claim receives that claim's
*current* representation, not the representation that was witnessed. The
resolution path is `claim_history` row -> `post_uri` -> `SELECT raw FROM
events`, and `events.raw` is overwritten in place on edit (src/labeler/db.py:386).
`claim_history.post_cid` names the version that was actually observed and is
not used by any of these paths.

The consequence is not merely coarse: a later edit by the observed party can
retroactively create or destroy a label on a *different* post that never
changed.

These are CHARACTERIZATION tests. They assert what the implementation does
today. If immutable version binding is later added, they are expected to fail,
and that failure is the signal — update them together with
`docs/architecture/HISTORICAL-INPUT-INTEGRITY.md`.

Companion to `tests/test_comparison_basis_collisions.py`, which covers the
scoring-view collisions rather than the historical-input path.
"""

import json

from labeler.claims import fingerprint_text
from labeler.db import get_conn, insert_event
from labeler.drift.rules import (
    apply_all_rules,
    rule_provenance_laundering,
    rule_repeat_claim_no_new_evidence,
)
from labeler.longitudinal import (
    _load_posts_for_claim_group,
    _load_posts_for_fingerprint,
)

# These three share one fingerprint, so they land in one claim cluster.
BARE = "200 people were affected in Springfield."
ATTRIBUTED = "Reportedly 200 people were affected in Springfield."


def _event(uri, cid, text, created, author, links=None):
    return {
        "uri": uri,
        "cid": cid,
        "text": text,
        "createdAt": created,
        "authorDid": author,
        "externalLinks": links or [],
        "embeds": [],
        "facets": [],
    }


def _write(uri, cid, text, created, author, links=None):
    """Ingest a record version. A second call with the same URI is an edit."""
    ev = _event(uri, cid, text, created, author, links)
    insert_event(uri, created, author, ev)
    return ev


def _labels(rule_output):
    return sorted((label.label, label.score) for label in rule_output)


def _current(posts, uri):
    return [p for p in posts if p.uri == uri][0]


def test_cluster_precondition_single_fingerprint():
    """Attribution changes do not change the fingerprint.

    Without this the specimens below would be comparing different claims.
    """
    assert fingerprint_text(BARE) == fingerprint_text(ATTRIBUTED)


def test_specimen_1_edit_retroactively_erases_a_laundering_label():
    """False negative: the prior post is edited, the label on a later post dies.

    P1 (attributed) then P2 (bare) by the same author is the shape
    `rule_provenance_laundering` exists to catch, and it fires. When P1 is
    later edited to drop its attribution, the label on P2 disappears — though
    P2 never changed and `claim_history` still holds the row for P1's original
    version.
    """
    author = "did:spec:erasure"
    fp = fingerprint_text(BARE)

    _write("uri:hii:P1", "cid-p1-v1", ATTRIBUTED, "2026-03-01T10:00:00", author)
    _write("uri:hii:P2", "cid-p2", BARE, "2026-03-02T10:00:00", author)

    conn = get_conn()
    posts = _load_posts_for_fingerprint(conn, fp)
    before = _labels(apply_all_rules(_current(posts, "uri:hii:P2"), posts))
    conn.close()

    assert ("provenance_laundering_possible", 0.9) in before

    # The observed party edits their own earlier post. Same URI, new CID.
    _write("uri:hii:P1", "cid-p1-v2", BARE, "2026-03-01T10:00:00", author)

    conn = get_conn()
    posts = _load_posts_for_fingerprint(conn, fp)
    after = _labels(apply_all_rules(_current(posts, "uri:hii:P2"), posts))
    history = conn.execute(
        "SELECT post_uri, post_cid FROM claim_history ORDER BY rowid"
    ).fetchall()
    conn.close()

    assert ("provenance_laundering_possible", 0.9) not in after
    # The witnessed version is still named in claim_history; nothing reads it.
    assert ("uri:hii:P1", "cid-p1-v1") in history


def test_specimen_2_edit_retroactively_manufactures_a_laundering_label():
    """False positive: an edit creates a 0.9-score label on an unchanged post.

    Q1 and Q2 are both bare — there is nothing to launder, and no label. Adding
    attribution to Q1 afterwards makes Q2 look like an attribution-stripped
    repetition of it.
    """
    author = "did:spec:manufacture"
    fp = fingerprint_text(BARE)

    _write("uri:hii:Q1", "cid-q1-v1", BARE, "2026-04-01T10:00:00", author)
    _write("uri:hii:Q2", "cid-q2", BARE, "2026-04-02T10:00:00", author)

    conn = get_conn()
    posts = _load_posts_for_fingerprint(conn, fp)
    before = _labels(apply_all_rules(_current(posts, "uri:hii:Q2"), posts))
    conn.close()

    assert ("provenance_laundering_possible", 0.9) not in before

    # Q1 is edited to add a source. Q2 is untouched.
    _write("uri:hii:Q1", "cid-q1-v2", ATTRIBUTED, "2026-04-01T10:00:00", author)

    conn = get_conn()
    posts = _load_posts_for_fingerprint(conn, fp)
    after = _labels(apply_all_rules(_current(posts, "uri:hii:Q2"), posts))
    conn.close()

    assert ("provenance_laundering_possible", 0.9) in after


def test_specimen_3_claim_history_fallback_reproduces_it_without_a_thread():
    """The rule's own fallback lookup has the defect independently.

    `rule_provenance_laundering` falls back to `claim_history` when no
    thread-local prior matches (src/labeler/drift/rules.py:39-58), resolving
    prior text with `SELECT raw FROM events WHERE event_uri = ?`
    (rules.py:49). Passing a thread containing only the current post isolates
    that path.
    """
    author = "did:spec:fallback"

    _write("uri:hii:F1", "cid-f1-v1", BARE, "2026-04-01T10:00:00", author)
    _write("uri:hii:F2", "cid-f2", BARE, "2026-04-02T10:00:00", author)
    _write("uri:hii:F1", "cid-f1-v2", ATTRIBUTED, "2026-04-01T10:00:00", author)

    conn = get_conn()
    posts = _load_posts_for_fingerprint(conn, fp := fingerprint_text(BARE))
    conn.close()
    current = _current(posts, "uri:hii:F2")

    # Thread of one: the thread-local branch cannot fire, only the fallback.
    assert fp
    assert ("provenance_laundering_possible", 0.9) in _labels(
        rule_provenance_laundering(current, [current])
    )


def test_specimen_4_loader_post_carries_the_wrong_version_identity():
    """The loader labels a historical row with the current version's CID.

    `_load_posts_for_fingerprint` selects only `(post_uri, createdAt)` from
    `claim_history` (src/labeler/longitudinal.py:71), drops `post_cid`, then
    reads `events.raw` and sets `Post.cid` from that current record. The
    resulting Post asserts a version identity that contradicts the history row
    it was derived from. It also dedupes by URI, so two witnessed versions
    collapse into one Post.
    """
    author = "did:spec:identity"
    fp = fingerprint_text(BARE)

    _write("uri:hii:V1", "cid-v1-old", ATTRIBUTED, "2026-06-01T10:00:00", author)
    _write("uri:hii:V1", "cid-v1-new", BARE, "2026-06-01T10:00:00", author)

    conn = get_conn()
    history = conn.execute(
        "SELECT post_cid FROM claim_history WHERE post_uri = ? ORDER BY rowid",
        ("uri:hii:V1",),
    ).fetchall()
    posts = _load_posts_for_fingerprint(conn, fp)
    conn.close()

    # Two witnessed versions in history...
    assert [h[0] for h in history] == ["cid-v1-old", "cid-v1-new"]
    # ...one Post out of the loader, carrying the newer identity and text.
    loaded = [p for p in posts if p.uri == "uri:hii:V1"]
    assert len(loaded) == 1
    assert loaded[0].cid == "cid-v1-new"
    assert loaded[0].text == BARE


def test_specimen_5_the_two_loaders_disagree_on_the_same_data():
    """`_load_posts_for_claim_group` emits one Post per history row, undeduped.

    Same underlying rows as specimen 4. `_load_posts_for_fingerprint` dedupes
    by URI (longitudinal.py:76-80); `_load_posts_for_claim_group` does not
    (longitudinal.py:114-138). The latter therefore yields duplicate Posts that
    are identical in every field, including CID — two copies of the current
    version standing in for two different witnessed versions.
    """
    author = "did:spec:divergence"
    fp = fingerprint_text(BARE)

    _write("uri:hii:D1", "cid-d1-old", ATTRIBUTED, "2026-06-01T10:00:00", author)
    _write("uri:hii:D1", "cid-d1-new", BARE, "2026-06-01T10:00:00", author)

    conn = get_conn()
    by_fingerprint = _load_posts_for_fingerprint(conn, fp)
    by_claim_group = _load_posts_for_claim_group(conn, author, fp)
    conn.close()

    assert len(by_fingerprint) == 1
    assert len(by_claim_group) == 2
    assert by_claim_group[0].cid == by_claim_group[1].cid == "cid-d1-new"
    assert by_claim_group[0].text == by_claim_group[1].text == BARE


def test_specimen_6_repeat_claim_rule_is_substitution_sensitive_too():
    """Adding a link to the prior post retroactively erases the label.

    `rule_repeat_claim_no_new_evidence` (rules.py:63) compares
    `prior.externalLinks or prior.embeds` against the current post's. Those
    fields come from the same current-representation Post objects.
    """
    author = "did:spec:repeat"
    fp = fingerprint_text(BARE)

    _write("uri:hii:R1", "cid-r1-v1", BARE, "2026-05-01T10:00:00", author)
    _write("uri:hii:R2", "cid-r2", BARE, "2026-05-02T10:00:00", author)

    conn = get_conn()
    posts = _load_posts_for_fingerprint(conn, fp)
    before = _labels(
        rule_repeat_claim_no_new_evidence(_current(posts, "uri:hii:R2"), posts)
    )
    conn.close()

    assert ("repeat_claim_no_new_evidence", 0.6) in before

    _write(
        "uri:hii:R1",
        "cid-r1-v2",
        BARE,
        "2026-05-01T10:00:00",
        author,
        links=["https://example.org/report"],
    )

    conn = get_conn()
    posts = _load_posts_for_fingerprint(conn, fp)
    after = _labels(
        rule_repeat_claim_no_new_evidence(_current(posts, "uri:hii:R2"), posts)
    )
    conn.close()

    assert ("repeat_claim_no_new_evidence", 0.6) not in after


def test_specimen_7_the_answer_is_retained_in_event_versions_and_unread():
    """`event_versions` holds the pre-edit record that every specimen needed.

    It is written at src/labeler/db.py:382 and pruned by retention. A grep over
    `src/` finds no other reader, so the repair does not need new capture — it
    needs a lookup path.
    """
    author = "did:spec:unread"

    _write("uri:hii:E1", "cid-e1-v1", ATTRIBUTED, "2026-07-01T10:00:00", author)
    _write("uri:hii:E1", "cid-e1-v2", BARE, "2026-07-01T10:00:00", author)

    conn = get_conn()
    versions = conn.execute(
        "SELECT raw FROM event_versions WHERE event_uri = ?", ("uri:hii:E1",)
    ).fetchall()
    current = json.loads(
        conn.execute(
            "SELECT raw FROM events WHERE event_uri = ?", ("uri:hii:E1",)
        ).fetchone()[0]
    )
    conn.close()

    assert len(versions) == 1
    archived = json.loads(versions[0][0])
    # The witnessed text and its CID are both recoverable from the JSON blob,
    # though there is no CID column to look them up by.
    assert archived["text"] == ATTRIBUTED
    assert archived["cid"] == "cid-e1-v1"
    assert current["text"] == BARE
