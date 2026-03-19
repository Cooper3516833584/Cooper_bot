from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import commands
from filesvc import FileService
from handinsvc import HandinService


class _DummyLogger:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return

    def error(self, _msg: str) -> None:
        return

    def exception(self, _msg: str) -> None:
        return


class _DummyLogService:
    def __init__(self) -> None:
        self.log = _DummyLogger()
        self.in_logs: list[str] = []
        self.out_logs: list[str] = []

    def log_in(self, _ctx, text: str) -> None:
        self.in_logs.append(str(text))

    def log_out(self, _ctx, text: str) -> None:
        self.out_logs.append(str(text))


class _ReplyRecorder:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def __call__(self, _api, ctx, text: str, _logsvc, force_private_user_id=None) -> None:
        self.messages.append(
            {
                "scene": getattr(ctx, "scene", ""),
                "user_id": getattr(ctx, "user_id", 0),
                "group_id": getattr(ctx, "group_id", None),
                "text": str(text),
                "force_private_user_id": force_private_user_id,
            }
        )


class _FakeAIService:
    def __init__(self) -> None:
        self.bot_nick = "Cooepr_bot"
        self.chat_ready = True
        self.semantic_ready = False
        self.fallback_error_reply = "fallback"
        self.chat_with_context = AsyncMock(return_value="smoke-ai-reply")
        self.chat = AsyncMock(return_value="smoke-ai-reply")
        self.semantic_find_paths = AsyncMock(return_value=[])


def _ctx(
    *,
    scene: str = "group",
    level: int = 1,
    user_id: int = 10001,
    group_id: int | None = 20001,
):
    return SimpleNamespace(
        scene=scene,
        user_id=int(user_id),
        nickname="tester",
        card="tester",
        group_id=group_id,
        group_name="group",
        level=int(level),
    )


@pytest.fixture
def smoke_dispatch_harness(monkeypatch):
    recorder = _ReplyRecorder()

    async def _noop_group_context(*_args, **_kwargs):
        return None

    async def _noop_pre_state(*_args, **_kwargs):
        return False

    async def _immediate_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(commands, "reply", recorder)
    monkeypatch.setattr(commands, "_ensure_group_context_and_schedule_digest", _noop_group_context)
    monkeypatch.setattr(commands, "_handle_pre_dispatch_state", _noop_pre_state)
    monkeypatch.setattr(commands.asyncio, "to_thread", _immediate_to_thread)
    return recorder


@pytest.mark.asyncio
async def test_core_routes_smoke_find_aichat_handin_admin(
    monkeypatch,
    test_config: dict,
    smoke_dispatch_harness,
) -> None:
    logger = _DummyLogger()
    filesvc = FileService(logger)
    filesvc.ensure_dirs()
    handin = HandinService(logger)
    aisvc = _FakeAIService()
    state = commands.BotState()
    logsvc = _DummyLogService()

    target = Path(test_config["public_dir"]) / "smoke_route_hit.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("smoke find", encoding="utf-8")

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=_ctx(scene="group", level=1),
        evt={"post_type": "message", "message_type": "group"},
        text="/find smoke_route_hit",
        filesvc=filesvc,
        logsvc=logsvc,
        state=state,
        handin=handin,
        perm=Mock(),
        aisvc=None,
    )
    assert target in state.last_find[commands.conv_key(_ctx(scene="group", level=1))]

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=_ctx(scene="private_friend", level=1, user_id=10001, group_id=None),
        evt={"post_type": "message", "message_type": "private", "sub_type": "friend"},
        text="Chello smoke",
        filesvc=filesvc,
        logsvc=logsvc,
        state=state,
        handin=handin,
        perm=Mock(),
        aisvc=aisvc,
    )
    aisvc.chat_with_context.assert_awaited()

    monkeypatch.setattr(commands, "parse_mmdd_hhmm", lambda _s, _now: 1700000000.0)
    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=_ctx(scene="group", level=2, user_id=10001, group_id=20001),
        evt={"post_type": "message", "message_type": "group"},
        text="/handin smoke_hw 1.1 10:00",
        filesvc=filesvc,
        logsvc=logsvc,
        state=state,
        handin=handin,
        perm=Mock(),
        aisvc=None,
    )
    assert any(t.name == "smoke_hw" for t in handin.list_tasks_by_group(20001))

    perm_admin = Mock()
    perm_admin.get_level.return_value = 2
    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=_ctx(scene="group", level=3, user_id=900001, group_id=20001),
        evt={"post_type": "message", "message_type": "group"},
        text="/level 10002 2",
        filesvc=filesvc,
        logsvc=logsvc,
        state=state,
        handin=handin,
        perm=perm_admin,
        aisvc=None,
    )
    perm_admin.set_level.assert_called_once_with(10002, 2)

    assert smoke_dispatch_harness.messages
