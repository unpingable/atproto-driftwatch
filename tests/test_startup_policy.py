import asyncio

import pytest

from labeler.startup_policy import StartupConfigurationError, resolve_startup_policy


MUTATION_FLAGS = (
    "FIREHOSE_AUTO_START",
    "FIREHOSE_LABEL_INGEST",
    "ENABLE_LONGITUDINAL_RECHECK",
    "ENABLE_CLAIM_RECHECK",
    "ENABLE_FACTS_EXPORT",
    "ENABLE_MAINTENANCE",
    "ENABLE_RETENTION",
)


def test_missing_controls_default_to_normal_but_disabled():
    policy = resolve_startup_policy({})
    assert policy.operator_paused is False
    assert policy.suppressed == ()
    assert policy.effective == {name: False for name in MUTATION_FLAGS}


def test_paused_mode_records_and_suppresses_conflicting_overlays():
    policy = resolve_startup_policy({
        "DRIFTWATCH_OPERATOR_PAUSED": "1",
        "FIREHOSE_AUTO_START": "1",
        "ENABLE_RETENTION": "true",
        "ENABLE_FACTS_EXPORT": "1",
    })
    assert policy.operator_paused is True
    assert policy.configured["ENABLE_RETENTION"] is True
    assert set(policy.suppressed) == {
        "ENABLE_FACTS_EXPORT", "ENABLE_RETENTION", "FIREHOSE_AUTO_START"
    }
    assert not any(policy.effective.values())


def test_normal_explicit_enable_is_preserved():
    policy = resolve_startup_policy({
        "DRIFTWATCH_OPERATOR_PAUSED": "0",
        "FIREHOSE_AUTO_START": "1",
        "ENABLE_RETENTION": "true",
    })
    assert policy.effective["FIREHOSE_AUTO_START"] is True
    assert policy.effective["ENABLE_RETENTION"] is True


def test_unknown_value_refuses_instead_of_guessing():
    with pytest.raises(StartupConfigurationError):
        resolve_startup_policy({"ENABLE_RETENTION": "perhaps"})


def test_real_startup_wiring_suppresses_inherited_retention(monkeypatch):
    pytest.importorskip("uvicorn")
    from labeler import main
    for name in MUTATION_FLAGS:
        monkeypatch.setenv(name, "1")
    monkeypatch.setenv("DRIFTWATCH_OPERATOR_PAUSED", "1")
    initialized = []
    monkeypatch.setattr(main, "init_db", lambda: initialized.append(True))

    class RefusingLoop:
        def create_task(self, _coro):
            raise AssertionError("paused startup created a background task")

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: RefusingLoop())
    asyncio.run(main.startup_event())

    assert initialized == [True]
    record = main.app.state.startup_policy.public_record()
    assert record["operator_paused"] is True
    assert record["configured"]["ENABLE_RETENTION"] is True
    assert record["effective"]["ENABLE_RETENTION"] is False
    assert any(route.path == "/health" for route in main.app.routes)


def test_invalid_configuration_refuses_before_database_initialization(monkeypatch):
    pytest.importorskip("uvicorn")
    from labeler import main
    monkeypatch.setenv("ENABLE_RETENTION", "invalid")
    initialized = []
    monkeypatch.setattr(main, "init_db", lambda: initialized.append(True))
    with pytest.raises(StartupConfigurationError):
        asyncio.run(main.startup_event())
    assert initialized == []


def test_real_startup_wiring_starts_explicit_normal_consumer(monkeypatch):
    pytest.importorskip("uvicorn")
    from labeler import main

    for name in MUTATION_FLAGS:
        monkeypatch.setenv(name, "0")
    monkeypatch.setenv("FIREHOSE_AUTO_START", "1")
    monkeypatch.setenv("DRIFTWATCH_OPERATOR_PAUSED", "0")
    monkeypatch.setattr(main, "init_db", lambda: None)
    created = []

    class FakeConsumer:
        async def run(self):
            return None

    class FakeTask:
        def cancel(self):
            return None

    class RecordingLoop:
        def create_task(self, coro):
            created.append(coro.cr_code.co_name)
            coro.close()
            return FakeTask()

    monkeypatch.setattr(main, "ATProtoConsumer", FakeConsumer)
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: RecordingLoop())
    asyncio.run(main.startup_event())
    assert created == ["run"]
    assert main.app.state.startup_policy.effective["FIREHOSE_AUTO_START"] is True
    main._background_tasks.clear()
    main._consumer_task = None
    main._consumer_instance = None
