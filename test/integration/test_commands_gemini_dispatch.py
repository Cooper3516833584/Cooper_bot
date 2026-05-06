from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import commands


class _DummyLogger:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
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
        self.gemini_chat_ready = True
        self.notice_ready = True
        self.semantic_ready = False
        self.fallback_error_reply = "fallback"
        self.remember_user_message = Mock()
        self.remember_assistant_message = Mock()
        self.chat_with_context = AsyncMock(return_value="fake-ai-reply")
        self.chat = AsyncMock(return_value="fake-ai-reply")
        self.gemini_chat_with_context = AsyncMock(return_value="gemini-ai-reply")
        self.gemini_chat = AsyncMock(return_value="gemini-ai-reply")
        self.extract_notice_url_head = AsyncMock(return_value="")
        self.classify_notice = AsyncMock(return_value=False)
        self.reason_notice = AsyncMock(return_value="")
        self.sanitize_reasoner_output = lambda text: str(text)
        self.is_notice_silent = lambda _text: False
        self.semantic_find_paths = AsyncMock(return_value=[])


def _make_ctx(
    *,
    scene: str = "private_friend",
    level: int = 1,
    user_id: int = 10001,
    group_id: int | None = None,
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


def _make_filesvc_stub():
    return SimpleNamespace(
        roots=[
            SimpleNamespace(name="public"),
            SimpleNamespace(name="friend"),
            SimpleNamespace(name="admin"),
        ],
        find=Mock(return_value=[]),
        list_dir=Mock(return_value=(True, "ok")),
    )


@pytest.fixture
def dispatch_harness(monkeypatch):
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
    if hasattr(commands, "_AI_REPEAT_GUARD"):
        commands._AI_REPEAT_GUARD.clear()
    return recorder


@pytest.mark.asyncio
async def test_command_aichat_gemini_dispatch(dispatch_harness) -> None:
    ctx = _make_ctx()
    filesvc = _make_filesvc_stub()
    aisvc = _FakeAIService()

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "private", "sub_type": "friend"},
        text="g帮我联网总结一下",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    aisvc.gemini_chat_with_context.assert_awaited_once()
    assert aisvc.gemini_chat_with_context.await_args.args[0] == f"private:{ctx.user_id}"
    model_input = aisvc.gemini_chat_with_context.await_args.args[1]
    assert "发言人QQ:" not in model_input
    assert "帮我联网总结一下" in model_input
    aisvc.chat_with_context.assert_not_awaited()
    assert any("gemini-ai-reply" in one["text"] for one in dispatch_harness.messages)


@pytest.mark.asyncio
async def test_command_help_mentions_gemini_usage(dispatch_harness) -> None:
    ctx = _make_ctx()
    filesvc = _make_filesvc_stub()
    aisvc = _FakeAIService()

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "private", "sub_type": "friend"},
        text="/help",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    assert any("群聊（Gemini联网）：@Cooepr_bot g内容" in one["text"] for one in dispatch_harness.messages)
    assert any("私聊（Gemini联网）：g内容" in one["text"] for one in dispatch_harness.messages)
