"""Regression tests for honesty across total and partial state loss.

Driftwatch may lose history. These tests check that after a loss it does not
claim continuity or coverage it cannot substantiate: a recreated store gets a
new lineage, a cursorless start is recorded as unbounded, the facts sidecar is
rebuilt rather than silently skipping a replaced source, and a health
baseline from an earlier store is not restored.

Synthetic and local only: a Jetstream stub on 127.0.0.1, stores in tmp_path.
"""

import asyncio
import datetime
import json
import sqlite3
import time
import urllib.parse

import pytest
import websockets

import labeler.db as db
from labeler import consumer, facts_export, ops_runtime, ops_status, platform_health, resolver

CURSOR = "driftwatch.observation.cursor_continuity"


# --- helpers -----------------------------------------------------------------

def _wipe_store(data_dir):
    """Total loss of the primary store (the WAL sidecars go with it)."""
    for name in ("labeler.sqlite", "labeler.sqlite-wal", "labeler.sqlite-shm"):
        p = data_dir / name
        if p.exists():
            p.unlink()


def _insert_claims(uris_and_times, fp="fp-a", author="did:plc:a"):
    conn = db.get_conn()
    conn.executemany(
        "INSERT INTO claim_history (authorDid, claim_fingerprint, createdAt, post_uri) VALUES (?, ?, ?, ?)",
        [(author, fp, ts, uri) for uri, ts in uris_and_times],
    )
    conn.commit()
    conn.close()


def _export(tmp_path):
    facts = tmp_path / "facts.sqlite"
    work = tmp_path / "facts_work.sqlite"
    facts_export.export_once(db.get_conn, str(facts), str(work), force_snapshot=True)
    return sqlite3.connect(str(facts))


def _meta(conn):
    return dict(conn.execute("SELECT key, value FROM meta").fetchall())


def _cursor_obs(tmp_path):
    status = ops_status.build_status(tmp_path / "labeler.sqlite", data_dir=tmp_path)
    return {c["id"]: c["observation"] for c in status["concerns"]}[CURSOR]


def _event(time_us, n):
    return {
        "did": "did:plc:synth0001", "time_us": time_us, "kind": "commit",
        "commit": {"rev": "x", "operation": "create", "collection": "app.bsky.feed.post",
                   "rkey": f"r{time_us}", "cid": f"bafy{n}",
                   "record": {"$type": "app.bsky.feed.post",
                              "text": f"According to the county report the bridge closes Monday {n}",
                              "createdAt": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())}},
    }


async def _run_consumer_once(n_events, base_us, port=0):
    """Run the real consumer against a local stub that sends n_events, then stop."""
    paths = []
    done = asyncio.Event()

    async def handler(ws, path=None):
        paths.append(path if path is not None else ws.path)
        for i in range(n_events):
            await ws.send(json.dumps(_event(base_us + i, i)))
        await done.wait()  # returning closes the connection cleanly

    server = await websockets.serve(handler, "127.0.0.1", port)
    port = server.sockets[0].getsockname()[1]
    cons = consumer.ATProtoConsumer(ws_url=f"ws://127.0.0.1:{port}/subscribe")
    task = asyncio.ensure_future(cons.run())
    deadline = time.monotonic() + 20
    while cons._event_count < n_events and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    cons.stop()
    done.set()
    await asyncio.sleep(0.2)
    server.close()
    await asyncio.wait_for(server.wait_closed(), timeout=10)
    await asyncio.wait_for(task, timeout=30)
    assert cons._event_count >= n_events
    return paths, port


@pytest.fixture
def local_consumer(tmp_path, monkeypatch):
    """Point every consumer side effect at tmp_path; no network."""
    monkeypatch.setattr(resolver, "resolve_batch", lambda: {"resolved": 0, "ok": 0, "not_found": 0, "error": 0})
    monkeypatch.setattr(ops_runtime, "DATA_DIR", tmp_path)
    monkeypatch.setattr(platform_health, "CHECKPOINT_PATH", tmp_path / "baseline_checkpoint.json")
    monkeypatch.delenv(consumer.START_CURSOR_ENV, raising=False)
    platform_health._reset()

    # Completed-prefix custody binds the source endpoint. Reuse the same
    # loopback endpoint across restarts instead of substituting another source.
    port = 0

    def run(n_events, base_us):
        nonlocal port
        platform_health._reset()
        paths, port = asyncio.run(_run_consumer_once(n_events, base_us, port))
        return paths

    yield run
    platform_health._reset()


def _starts():
    conn = db.get_conn()
    rows = conn.execute(
        "SELECT generation_id, start_kind, resume_cursor, first_time_us FROM coverage_starts ORDER BY id"
    ).fetchall()
    conn.close()
    return rows


# --- 1. hourly rollup includes the current UTC day -----------------------------

def test_hourly_rollup_includes_current_utc_day(tmp_path):
    now = datetime.datetime.now(datetime.timezone.utc)
    recent = now - datetime.timedelta(minutes=5)
    older = now - datetime.timedelta(hours=30)
    _insert_claims([
        ("at://a/post/1", recent.isoformat()),                              # +00:00 form (insert_event)
        ("at://a/post/2", recent.strftime("%Y-%m-%dT%H:%M:%S.000Z")),       # Z form
        ("at://a/post/3", older.isoformat()),
    ])
    facts = _export(tmp_path)
    hourly = dict(facts.execute("SELECT hour_epoch, event_count FROM fingerprint_hourly").fetchall())
    recent_hour = (int(recent.timestamp()) // 3600) * 3600
    older_hour = (int(older.timestamp()) // 3600) * 3600
    assert hourly.get(recent_hour) == 2
    assert hourly.get(older_hour) == 1


def test_hourly_rollup_still_range_scans_created_index():
    statements = []
    conn = db.get_conn()
    conn.set_trace_callback(statements.append)
    sidecar = sqlite3.connect(":memory:")
    facts_export._ensure_tables(sidecar)
    now = int(time.time())
    facts_export._recompute_hourly(conn, sidecar, now - 72 * 3600, now)
    conn.set_trace_callback(None)
    select = next(s for s in statements if "FROM claim_history" in s)
    plan = " ".join(r[3] for r in conn.execute("EXPLAIN QUERY PLAN " + select).fetchall())
    conn.close()
    assert "USING INDEX idx_claim_history_created" in plan


# --- 2. store lineage and cursorless starts ------------------------------------

def test_store_lineage_minted_once_and_marks_adopted_stores(tmp_path):
    first = db.get_store_lineage()
    assert first["origin"] == "created" and first["generation_id"]
    db.init_db()
    assert db.get_store_lineage() == first

    # A populated store that predates lineage tracking is adopted, not "created".
    conn = db.get_conn()
    conn.execute("DROP TABLE store_lineage")
    conn.commit()
    conn.close()
    db.init_db()
    adopted = db.get_store_lineage()
    assert adopted["origin"] == "adopted"
    assert adopted["generation_id"] != first["generation_id"]

    _wipe_store(tmp_path)
    db.init_db()
    fresh = db.get_store_lineage()
    assert fresh["origin"] == "created"
    assert fresh["generation_id"] not in (first["generation_id"], adopted["generation_id"])


def test_total_loss_records_unbounded_start_and_withholds_continuity(tmp_path, local_consumer):
    base1 = int(time.time() * 1_000_000) - 5_000_000
    paths = local_consumer(5, base1)
    assert "cursor=" not in paths[0]
    gen1 = db.get_store_lineage()["generation_id"]
    assert _starts() == [(gen1, "unbounded", None, base1)]
    # The cursor saved on shutdown is recent, but the session started without
    # one: that is not continuity.
    obs = _cursor_obs(tmp_path)
    assert obs["local_state"] == "UNKNOWN"
    assert "without a durable cursor" in obs["reason"]
    assert obs["facts"]["store_generation"] == gen1

    # A baseline from this store must not survive into the replacement.
    (tmp_path / "baseline_checkpoint.json").write_text(json.dumps({
        "version": platform_health.PlatformHealth.CHECKPOINT_VERSION, "baseline_eps": 50.0,
        "current_eps": 50.0, "stream_lag_s": 1.0, "windows_seen": 30, "state": "ok",
        "store_generation": gen1, "checkpoint_at": time.time(),
        "baseline_standing": platform_health.BASELINE_ESTABLISHED,
        "admissible_windows": platform_health.BASELINE_ESTABLISH_WINDOWS,
    }))

    # Total loss: the store is gone. Restart must be a fresh, honest start.
    _wipe_store(tmp_path)
    base2 = int(time.time() * 1_000_000)
    paths = local_consumer(5, base2)
    assert "cursor=" not in paths[0], "no replay without an explicit cursor"
    assert platform_health.get_health_snapshot()["baseline_restored"] is False
    gen2 = db.get_store_lineage()["generation_id"]
    assert gen2 != gen1
    assert _starts() == [(gen2, "unbounded", None, base2)]
    obs = _cursor_obs(tmp_path)
    assert obs["local_state"] == "UNKNOWN"
    assert obs["facts"]["coverage_start"]["start_kind"] == "unbounded"
    assert obs["facts"]["coverage_floor_at"] == obs["facts"]["coverage_start"]["first_observed_at"]

    # A later restart resumes from the durable cursor; continuity from the
    # floor onward is substantiated, and the floor stays visible.
    paths = local_consumer(3, base2 + 1_000)
    query = urllib.parse.parse_qs(urllib.parse.urlparse(paths[0]).query)
    # The selected runtime keeps a five-second replay overlap behind the
    # completed source frontier. Recovery must preserve that contract.
    assert query["cursor"] == [str(base2 + 4 - consumer.REPLAY_REWIND_US)]
    assert [r[1] for r in _starts()] == ["unbounded", "cursor_resume"]
    obs = _cursor_obs(tmp_path)
    assert obs["local_state"] == "PRESENT"
    assert obs["facts"]["coverage_floor_at"] in obs["reason"]


def test_operator_cursor_is_explicit_and_not_continuity(tmp_path, local_consumer, monkeypatch):
    monkeypatch.setenv(consumer.START_CURSOR_ENV, "1700000000000000")
    base = int(time.time() * 1_000_000)
    paths = local_consumer(2, base)
    assert "cursor=1700000000000000" in paths[0]
    assert [r[1:3] for r in _starts()] == [("operator_cursor", "1700000000000000")]
    assert _cursor_obs(tmp_path)["local_state"] == "UNKNOWN"


# --- 3. facts sidecar is rebuilt when the source store is replaced -------------

@pytest.mark.parametrize("old_rows,new_rows", [(5, 2), (2, 5)])
def test_replaced_source_store_rebuilds_sidecar(tmp_path, old_rows, new_rows):
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    _insert_claims([(f"at://old/post/{i}", ts) for i in range(old_rows)], fp="fp-old")
    facts = _export(tmp_path)
    gen_old = _meta(facts)["source_generation"]
    assert facts.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0] == old_rows
    facts.close()

    # Partial loss: source store replaced, sidecar (facts_work.sqlite) kept.
    _wipe_store(tmp_path)
    db.init_db()
    _insert_claims([(f"at://new/post/{i}", ts) for i in range(new_rows)], fp="fp-new")
    facts = _export(tmp_path)
    meta = _meta(facts)
    uris = {r[0] for r in facts.execute("SELECT post_uri FROM uri_fingerprint")}
    assert uris == {f"at://new/post/{i}" for i in range(new_rows)}
    assert {r[0] for r in facts.execute("SELECT fingerprint FROM fingerprint_bounds")} == {"fp-new"}
    assert meta["source_generation"] == db.get_store_lineage()["generation_id"] != gen_old
    assert "generation changed" in meta["last_rebuild_reason"]
    assert int(meta["last_checkpoint_rowid"]) == new_rows


def test_sidecar_rebuilds_when_source_rowids_regress_without_lineage(tmp_path):
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    _insert_claims([(f"at://old/post/{i}", ts) for i in range(4)])
    _export(tmp_path).close()
    # Same lineage record, but claim_history was emptied and refilled.
    conn = db.get_conn()
    conn.execute("DELETE FROM claim_history")
    conn.commit()
    conn.close()
    _insert_claims([("at://new/post/0", ts)])
    facts = _export(tmp_path)
    assert {r[0] for r in facts.execute("SELECT post_uri FROM uri_fingerprint")} == {"at://new/post/0"}
    assert "below checkpoint rowid" in _meta(facts)["last_rebuild_reason"]


# --- 4. export meta carries the coverage floor and generation ------------------

def test_export_meta_carries_coverage_floor_and_generation(tmp_path):
    facts = _export(tmp_path)
    meta = _meta(facts)
    lineage = db.get_store_lineage()
    assert meta["source_generation"] == lineage["generation_id"]
    assert meta["source_store_created_at"] == lineage["created_at"]
    assert meta["coverage_start_epoch"] == ""  # nothing observed yet: no floor claimed
    facts.close()

    floor_us = 1_760_000_000_123_456
    sid = db.record_coverage_start(consumer.CONSUMER_NAME, db.COVERAGE_UNBOUNDED)
    db.record_coverage_floor(sid, floor_us)
    db.record_coverage_floor(sid, floor_us + 10)  # first write wins
    # A later resume does not move the floor.
    sid2 = db.record_coverage_start(consumer.CONSUMER_NAME, db.COVERAGE_CURSOR_RESUME, "x")
    db.record_coverage_floor(sid2, floor_us + 99_000_000)
    assert int(_meta(_export(tmp_path))["coverage_start_epoch"]) == floor_us // 1_000_000


# --- 5. baseline checkpoint is bound to the store generation -------------------

def test_baseline_checkpoint_not_restored_onto_new_store(tmp_path):
    path = tmp_path / "baseline_checkpoint.json"
    old = platform_health.PlatformHealth()
    old.set_store_generation("gen-old")
    for _ in range(platform_health.BASELINE_ESTABLISH_WINDOWS):
        old.record_window(events_in=2400, window_secs=60.0, backlog=0)
    assert old.get_health_snapshot()["baseline_standing"] == platform_health.BASELINE_ESTABLISHED
    assert old.force_checkpoint(path)
    data = json.loads(path.read_text())
    assert data["store_generation"] == "gen-old"

    new = platform_health.PlatformHealth()
    new.set_store_generation("gen-new")
    assert new.restore(data) is False
    assert new.get_health_snapshot()["baseline_restored"] is False

    same = platform_health.PlatformHealth()
    same.set_store_generation("gen-old")
    assert same.restore(data) is True

    # A checkpoint written before lineage existed is not trusted either.
    legacy = dict(data)
    del legacy["store_generation"]
    assert new.restore(legacy) is False


def test_interrupted_lineage_rebuild_closes_work_store_and_keeps_publication(tmp_path, monkeypatch):
    """A failed reset must release the sidecar connection and publication."""
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    _insert_claims([("at://old/post/0", ts)])
    _export(tmp_path).close()
    publication = (tmp_path / "facts.sqlite").read_bytes()
    _wipe_store(tmp_path)
    db.init_db()
    _insert_claims([("at://new/post/0", ts)])

    reset = facts_export._reset_sidecar
    connections = []

    def interrupt(conn, *args):
        connections.append(conn)
        conn.set_progress_handler(lambda: 1, 1)
        return reset(conn, *args)

    monkeypatch.setattr(facts_export, "_reset_sidecar", interrupt)
    with pytest.raises(sqlite3.OperationalError, match="interrupted"):
        facts_export.export_once(db.get_conn, str(tmp_path / "facts.sqlite"),
                                 str(tmp_path / "facts_work.sqlite"), force_snapshot=True)
    assert (tmp_path / "facts.sqlite").read_bytes() == publication
    assert len(connections) == 1
    try:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connections[0].execute("SELECT 1")
    finally:
        connections[0].close()


def test_total_loss_rebuilds_identity_meaning_and_preserves_admin_configuration(tmp_path, monkeypatch):
    """New identity facts are rebuilt from current source data after store loss."""
    import io
    import os
    from labeler import main

    token = "synthetic-operator-token-not-a-credential"
    monkeypatch.setenv("ADMIN_API_TOKEN", token)
    did = "did:plc:recoveryfixture"

    def apply_identity(handle, timestamp):
        cons = consumer.ATProtoConsumer(ws_url="ws://127.0.0.1:1/fixture-source")
        try:
            written, unresolved = cons._process_batch([{"_source": {
                "did": did, "kind": "identity", "time_us": timestamp,
                "identity": {"did": did, "handle": handle},
            }}])
            assert unresolved == 0
            assert cons._source_committed_total == 1
        finally:
            if cons._writer_conn is not None:
                cons._writer_conn.close()
            cons._writer_executor.shutdown()

    before = int(time.time() * 1_000_000)
    apply_identity("old.example", before)
    resolver._write_resolution(resolver.ResolvedDID(
        did, "https://old-pds.example", "old-pds.example", "old.example", "ok"))
    old_generation = db.get_store_lineage()["generation_id"]
    _wipe_store(tmp_path)
    db.init_db()
    assert db.get_store_lineage()["generation_id"] != old_generation
    assert db.get_cursor(consumer.CONSUMER_NAME) is None
    conn = db.get_conn()
    assert conn.execute("SELECT 1 FROM actor_identity_current").fetchone() is None
    conn.close()

    apply_identity("new.example", before + 10_000_000)
    doc = {"id": did, "alsoKnownAs": ["at://new.example"], "service": [{
        "id": "#atproto_pds", "type": "AtprotoPersonalDataServer",
        "serviceEndpoint": "https://new-pds.example",
    }]}
    monkeypatch.setattr(resolver.urllib.request, "urlopen",
                        lambda *_a, **_k: io.BytesIO(json.dumps(doc).encode()))
    resolver._write_resolution(resolver.resolve_did(did))
    facts = _export(tmp_path)
    assert facts.execute(
        "SELECT did, handle, pds_host, resolver_status FROM actor_identity_facts"
    ).fetchall() == [(did, "new.example", "new-pds.example", "ok")]
    facts.close()
    assert os.environ["ADMIN_API_TOKEN"] == token
    assert asyncio.run(main.admin_auth(authorization=None, x_admin_token=token)) is True
    with pytest.raises(main.HTTPException) as caught:
        asyncio.run(main.admin_auth(authorization=None, x_admin_token="different-token"))
    assert caught.value.status_code == 401
    assert token not in json.dumps(ops_status.build_status(tmp_path / "labeler.sqlite", data_dir=tmp_path))


def test_lineage_rebuild_still_requires_workspace_admission(tmp_path, monkeypatch):
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    _insert_claims([("at://old/post/0", ts)])
    facts = _export(tmp_path)
    old_generation = _meta(facts)["source_generation"]
    facts.close()
    publication = (tmp_path / "facts.sqlite").read_bytes()
    _wipe_store(tmp_path)
    db.init_db()
    _insert_claims([("at://new/post/0", ts)])
    scratch = tmp_path / "export-scratch"
    scratch.mkdir()
    monkeypatch.setenv("FACTS_EXPORT_TEMP_ATTEMPT_DIR", str(scratch))
    monkeypatch.setenv("FACTS_EXPORT_TEMP_MOUNT", str(tmp_path))
    monkeypatch.setenv("SQLITE_TMPDIR", str(scratch))
    monkeypatch.setattr(facts_export, "_mount_for_path", lambda _p: {
        "mountpoint": str(tmp_path), "filesystem": "ext4", "device": 1,
        "source": "/dev/fixture",
    })
    monkeypatch.setattr(facts_export, "_explicit_workspace_plan", lambda *_a: {
        "admitted": False, "filesystems": {},
    })
    # The previous snapshot is recent and this isn't a forced snapshot cycle.
    # A replacement source must still reach admission before deleting old work.
    with pytest.raises(facts_export.WorkspaceRefusal, match="admission refused"):
        facts_export.export_once(db.get_conn, str(tmp_path / "facts.sqlite"),
                                 str(tmp_path / "facts_work.sqlite"))
    assert (tmp_path / "facts.sqlite").read_bytes() == publication
    conn = sqlite3.connect(tmp_path / "facts_work.sqlite")
    assert _meta(conn)["source_generation"] == old_generation
    assert conn.execute("SELECT post_uri FROM uri_fingerprint").fetchall() == [("at://old/post/0",)]
    conn.close()


@pytest.mark.parametrize("replacement_rows", [0, 3, 5, 7])
def test_bound_export_refuses_lineage_less_replacement_without_mutation(tmp_path, replacement_rows):
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    _insert_claims([(f"at://old/post/{i}", ts) for i in range(5)], fp="fp-old")
    facts = _export(tmp_path)
    generation = _meta(facts)["source_generation"]
    facts.close()
    publication = (tmp_path / "facts.sqlite").read_bytes()
    working = (tmp_path / "facts_work.sqlite").read_bytes()
    _wipe_store(tmp_path)
    db.init_db()
    _insert_claims([(f"at://new/post/{i}", ts) for i in range(replacement_rows)], fp="fp-new")
    conn = db.get_conn()
    conn.execute("DROP TABLE store_lineage")  # Replica of a source made by an older image.
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="source lineage"):
        facts_export.export_once(db.get_conn, str(tmp_path / "facts.sqlite"),
                                 str(tmp_path / "facts_work.sqlite"), force_snapshot=True)
    assert db.get_store_lineage() is None, "export must not initialize the source"
    assert (tmp_path / "facts.sqlite").read_bytes() == publication
    assert (tmp_path / "facts_work.sqlite").read_bytes() == working
    conn = sqlite3.connect(tmp_path / "facts_work.sqlite")
    assert _meta(conn)["source_generation"] == generation
    assert _meta(conn)["last_checkpoint_rowid"] == "5"
    assert {r[0] for r in conn.execute("SELECT post_uri FROM uri_fingerprint")} == {
        f"at://old/post/{i}" for i in range(5)
    }
    conn.close()


@pytest.mark.parametrize("failure_phase", ["source_open", "lineage_read"])
def test_pre_admission_source_error_closes_owned_connections(tmp_path, monkeypatch, failure_phase):
    work = str(tmp_path / "facts_work.sqlite")
    original_connect = sqlite3.connect
    held = []

    def connect(*args, **kwargs):
        conn = original_connect(*args, **kwargs)
        held.append(conn)
        return conn

    def unavailable(*_args):
        raise sqlite3.OperationalError("synthetic source unavailable")

    monkeypatch.setattr(facts_export.sqlite3, "connect", connect)
    source = unavailable if failure_phase == "source_open" else db.get_conn
    if failure_phase == "lineage_read":
        monkeypatch.setattr(facts_export, "_read_source_lineage", unavailable)
    try:
        with pytest.raises(sqlite3.OperationalError, match="synthetic source unavailable"):
            facts_export.export_once(source, str(tmp_path / "facts.sqlite"), work, force_snapshot=True)
        assert held
        for conn in held:
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                conn.execute("SELECT 1")
    finally:
        for conn in held:
            conn.close()
