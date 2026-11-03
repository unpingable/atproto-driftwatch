import asyncio

import pytest

from labeler import consumer


class _Socket:
    def __init__(self, values):
        self.values = list(values)

    async def recv(self):
        value = self.values.pop(0)
        if isinstance(value, BaseException):
            raise value
        if isinstance(value, tuple):
            delay, value = value
            await asyncio.sleep(delay)
        return value


def test_receive_idle_deadline_applies_while_waiting(monkeypatch):
    monkeypatch.setattr(consumer, "JETSTREAM_RECEIVE_IDLE_TIMEOUT_S", 0.01)

    async def exercise():
        with pytest.raises(TimeoutError, match="source receive idle"):
            await consumer._receive_source_message(_Socket([(1, "late")]))

    asyncio.run(exercise())


def test_receive_idle_deadline_does_not_cover_downstream_backpressure(monkeypatch):
    monkeypatch.setattr(consumer, "JETSTREAM_RECEIVE_IDLE_TIMEOUT_S", 0.01)

    async def exercise():
        assert await consumer._receive_source_message(_Socket(["event"])) == "event"
        # This represents a saturated application queue / slow SQLite writer.
        # It occurs after recv() and therefore isn't mislabeled source-idle.
        await asyncio.sleep(0.03)
        assert await consumer._receive_source_message(_Socket(["next"])) == "next"

    asyncio.run(exercise())
