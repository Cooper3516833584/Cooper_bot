from __future__ import annotations

import asyncio

import pytest

import client


class _CollectingLog:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def info(self, _msg: str) -> None:
        return

    def warning(self, msg: str) -> None:
        self.warnings.append(str(msg))

    def error(self, _msg: str) -> None:
        return

    def exception(self, _msg: str) -> None:
        return


class _FakeBridge:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.stop_calls = 0
        self.error = error

    def stop(self) -> None:
        self.stop_calls += 1
        if self.error is not None:
            raise self.error


class _FakeAIService:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.aclose_calls = 0
        self.error = error

    async def aclose(self) -> None:
        self.aclose_calls += 1
        if self.error is not None:
            raise self.error


@pytest.fixture(autouse=True)
def _reset_post_sync_task():
    original = client._POST_SYNC_TASK
    client._POST_SYNC_TASK = None
    try:
        yield
    finally:
        client._POST_SYNC_TASK = original


async def test_cleanup_stops_bridge_and_closes_ai_service() -> None:
    bridge = _FakeBridge()
    aisvc = _FakeAIService()

    await client._cleanup_runtime(_CollectingLog(), bridge, aisvc)

    assert bridge.stop_calls == 1
    assert aisvc.aclose_calls == 1


async def test_cleanup_tolerates_missing_bridge() -> None:
    aisvc = _FakeAIService()

    await client._cleanup_runtime(_CollectingLog(), None, aisvc)

    assert aisvc.aclose_calls == 1


async def test_cleanup_continues_after_bridge_failure() -> None:
    log = _CollectingLog()
    bridge = _FakeBridge(error=RuntimeError("port busy"))
    aisvc = _FakeAIService()

    await client._cleanup_runtime(log, bridge, aisvc)

    assert bridge.stop_calls == 1
    # 桥清理失败不能中断 AIService 的关闭。
    assert aisvc.aclose_calls == 1
    assert any("搜索桥" in msg for msg in log.warnings)


async def test_cleanup_swallows_ai_service_failure() -> None:
    log = _CollectingLog()

    await client._cleanup_runtime(log, None, _FakeAIService(error=RuntimeError("kimi runner busy")))

    assert any("AI 服务" in msg for msg in log.warnings)


async def test_cleanup_cancels_pending_post_sync_task() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def _long_running() -> None:
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    client._POST_SYNC_TASK = asyncio.create_task(_long_running())
    await started.wait()

    await client._cleanup_runtime(_CollectingLog(), None, _FakeAIService())

    assert cancelled.is_set()
    assert client._POST_SYNC_TASK.cancelled() or client._POST_SYNC_TASK.done()


async def test_cleanup_ignores_already_finished_post_sync_task() -> None:
    async def _instant() -> None:
        return None

    client._POST_SYNC_TASK = asyncio.create_task(_instant())
    await client._POST_SYNC_TASK

    bridge = _FakeBridge()
    await client._cleanup_runtime(_CollectingLog(), bridge, _FakeAIService())

    assert bridge.stop_calls == 1


async def _settle(task: asyncio.Task, rounds: int = 5) -> None:
    for _ in range(rounds):
        await asyncio.sleep(0)


async def test_post_sync_done_callback_ignores_cancellation(monkeypatch) -> None:
    log = _CollectingLog()
    monkeypatch.setattr(client, "log", log)

    async def _long_running() -> None:
        await asyncio.sleep(30)

    task = asyncio.create_task(_long_running())
    await _settle(task)
    task.cancel()
    await _settle(task)
    assert task.cancelled()

    client._log_post_sync_task_result(task)

    # 退出时的正常取消不应产生任何日志噪声。
    assert log.warnings == []


async def test_post_sync_done_callback_logs_real_failures(monkeypatch) -> None:
    log = _CollectingLog()
    monkeypatch.setattr(client, "log", log)

    async def _boom() -> None:
        raise RuntimeError("sync failed")

    task = asyncio.create_task(_boom())
    await _settle(task)
    assert task.done() and not task.cancelled()

    client._log_post_sync_task_result(task)

    assert any("sync failed" in msg for msg in log.warnings)
