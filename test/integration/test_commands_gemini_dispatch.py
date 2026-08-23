from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import cooper_bot.commands.commands as commands


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
        self.bot_nick = "Cooper_bot"
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
        self.restricted_gemini_chat_with_context = AsyncMock(return_value="restricted-gemini-ai-reply")
        self.restricted_gemini_chat = AsyncMock(return_value="restricted-gemini-ai-reply")
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
async def test_level_one_aichat_gemini_uses_restricted_web_search(dispatch_harness) -> None:
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

    aisvc.restricted_gemini_chat_with_context.assert_awaited_once()
    assert aisvc.restricted_gemini_chat_with_context.await_args.args[0] == f"private:{ctx.user_id}"
    model_input = aisvc.restricted_gemini_chat_with_context.await_args.args[1]
    assert aisvc.restricted_gemini_chat_with_context.await_args.args[2] == "gemini"
    assert "发言人QQ:" not in model_input
    assert "帮我联网总结一下" in model_input
    aisvc.gemini_chat_with_context.assert_not_awaited()
    aisvc.chat_with_context.assert_not_awaited()
    assert any("restricted-gemini-ai-reply" in one["text"] for one in dispatch_harness.messages)


@pytest.mark.asyncio
async def test_level_one_aichat_claude_uses_restricted_web_search(dispatch_harness) -> None:
    ctx = _make_ctx()
    filesvc = _make_filesvc_stub()
    aisvc = _FakeAIService()

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "private", "sub_type": "friend"},
        text="c帮我联网总结一下",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    aisvc.restricted_gemini_chat_with_context.assert_awaited_once()
    assert aisvc.restricted_gemini_chat_with_context.await_args.args[0] == f"private:{ctx.user_id}"
    assert "帮我联网总结一下" in aisvc.restricted_gemini_chat_with_context.await_args.args[1]
    assert aisvc.restricted_gemini_chat_with_context.await_args.args[2] == "claude"
    aisvc.gemini_chat_with_context.assert_not_awaited()
    aisvc.chat_with_context.assert_not_awaited()
    assert any("restricted-gemini-ai-reply" in one["text"] for one in dispatch_harness.messages)


@pytest.mark.asyncio
async def test_level_three_aichat_gemini_keeps_full_antigravity(dispatch_harness) -> None:
    ctx = _make_ctx(level=3)
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
    assert aisvc.gemini_chat_with_context.await_args.args[2] == "gemini"
    assert aisvc.gemini_chat_with_context.await_args.kwargs["auto_approve_tools"] is True
    assert any("gemini-ai-reply" in one["text"] for one in dispatch_harness.messages)


@pytest.mark.asyncio
async def test_ai_fallback_private_when_group_send_unconfirmed() -> None:
    if hasattr(commands, "_RECENT_REPLY_KEYS"):
        commands._RECENT_REPLY_KEYS.clear()
    ctx = _make_ctx(scene="group", level=3, group_id=20001, user_id=10001)
    aisvc = _FakeAIService()
    aisvc.gemini_chat_with_context = AsyncMock(side_effect=RuntimeError("agy failed"))
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value=None),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )

    handled = await commands._handle_ai_chat_trigger(
        api=api,
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        t="",
        logsvc=_DummyLogService(),
        aisvc=aisvc,
        forced_ai_input="c帮我联网总结一下",
    )

    assert handled is True
    api.send_group_msg.assert_awaited_once()
    assert api.send_private_msg.await_count == 2
    assert api.send_private_msg.await_args_list[0].args[0] == ctx.user_id
    assert api.send_private_msg.await_args_list[0].args[1] == aisvc.fallback_error_reply
    admin_notice_args = api.send_private_msg.await_args_list[1].args
    assert admin_notice_args[0] == next(iter(commands.ADMIN_USERS))
    assert "机器人报错提醒" in admin_notice_args[1]
    assert "聊天：群聊 group_id=20001 group_name=group" in admin_notice_args[1]
    assert "环节：aichat/claude" in admin_notice_args[1]
    assert "错误：RuntimeError: agy failed" in admin_notice_args[1]


@pytest.mark.asyncio
async def test_antigravity_busy_reply_uses_clear_message(dispatch_harness) -> None:
    ctx = _make_ctx(level=3)
    filesvc = _make_filesvc_stub()
    aisvc = _FakeAIService()
    aisvc.gemini_chat_with_context = AsyncMock(
        side_effect=RuntimeError("antigravity cli service busy: No capacity available for model claude-opus-4-6-thinking")
    )

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "private", "sub_type": "friend"},
        text="c今天有什么新闻",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    assert any("Claude Opus 4.6 当前服务繁忙" in one["text"] for one in dispatch_harness.messages)
    assert not any(one["text"] == aisvc.fallback_error_reply for one in dispatch_harness.messages)


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

    assert any("群聊（联网搜索 Gemini）：@Cooper_bot g内容" in one["text"] for one in dispatch_harness.messages)
    assert any("群聊（联网搜索 Claude）：@Cooper_bot c内容" in one["text"] for one in dispatch_harness.messages)
    assert any("私聊（联网搜索 Gemini）：g内容" in one["text"] for one in dispatch_harness.messages)
    assert any("私聊（联网搜索 Claude）：c内容" in one["text"] for one in dispatch_harness.messages)
