from __future__ import annotations

import asyncio
from pathlib import Path
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import commands


class _DummyLogger:
    def __init__(self) -> None:
        self.records: list[tuple[str, str]] = []

    def info(self, msg: str) -> None:
        self.records.append(("info", str(msg)))

    def warning(self, msg: str) -> None:
        self.records.append(("warning", str(msg)))

    def exception(self, msg: str) -> None:
        self.records.append(("exception", str(msg)))


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


def _make_ctx(
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


def _make_filesvc_stub(find_impl: Mock | None = None, list_impl: Mock | None = None):
    find_method = find_impl or Mock(return_value=[])
    list_method = list_impl or Mock(return_value=(True, "目录内容："))
    return SimpleNamespace(
        roots=[
            SimpleNamespace(name="public"),
            SimpleNamespace(name="friend"),
            SimpleNamespace(name="admin"),
        ],
        find=find_method,
        list_dir=list_method,
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


class _FakeAIService:
    def __init__(self) -> None:
        self.bot_nick = "Cooepr_bot"
        self.chat_ready = True
        self.notice_ready = True
        self.semantic_ready = False
        self.fallback_error_reply = "fallback"
        self.remember_user_message = Mock()
        self.remember_assistant_message = Mock()
        self.chat_with_context = AsyncMock(return_value="fake-ai-reply")
        self.chat = AsyncMock(return_value="fake-ai-reply")
        self.extract_notice_url_head = AsyncMock(return_value="")
        self.classify_notice = AsyncMock(return_value=False)
        self.reason_notice = AsyncMock(return_value="")
        self.sanitize_reasoner_output = lambda text: str(text)
        self.is_notice_silent = lambda _text: False
        self.semantic_find_paths = AsyncMock(return_value=[])


class _FakeHandinTask:
    def __init__(
        self,
        *,
        task_id: str,
        group_id: int,
        creator_id: int,
        name: str,
        deadline_ts: float,
        closed: bool = False,
        cancelled: bool = False,
    ) -> None:
        self.task_id = task_id
        self.group_id = int(group_id)
        self.creator_id = int(creator_id)
        self.name = str(name)
        self.deadline_ts = float(deadline_ts)
        self.closed = bool(closed)
        self.cancelled = bool(cancelled)

    def is_active(self, now: float | None = None) -> bool:
        now_ts = time.time() if now is None else float(now)
        return (not self.closed) and (not self.cancelled) and now_ts < self.deadline_ts


@pytest.mark.asyncio
async def test_command_find_dispatch(tmp_data_dirs: dict, dispatch_harness) -> None:
    target = Path(tmp_data_dirs["public_dir"]) / "find_dispatch_hit.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("dispatch", encoding="utf-8")

    find_mock = Mock(return_value=[target])
    filesvc = _make_filesvc_stub(find_impl=find_mock)
    ctx = _make_ctx(scene="group", level=1)
    state = commands.BotState()

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="/find dispatch",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=state,
        handin=Mock(),
        perm=Mock(),
        aisvc=None,
    )

    assert find_mock.call_count == 1
    assert find_mock.call_args.args[1] == "dispatch"
    assert find_mock.call_args.kwargs["in_dir"] is None
    assert target in state.last_find[commands.conv_key(ctx)]
    assert any("find_dispatch_hit.txt" in one["text"] for one in dispatch_harness.messages)


@pytest.mark.asyncio
async def test_command_ls_dispatch_uses_list_service(dispatch_harness) -> None:
    list_mock = Mock(return_value=(True, "目录内容：\n📁 textbook_and_material/"))
    filesvc = _make_filesvc_stub(list_impl=list_mock)
    ctx = _make_ctx(scene="group", level=1)

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="/ls public",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=None,
    )

    list_mock.assert_called_once_with(ctx, "public")
    assert any("目录内容" in one["text"] for one in dispatch_harness.messages)


@pytest.mark.asyncio
async def test_command_get_dispatch(monkeypatch, tmp_data_dirs: dict, dispatch_harness) -> None:
    src = Path(tmp_data_dirs["public_dir"]) / "get_dispatch_hit.txt"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("get", encoding="utf-8")

    ctx = _make_ctx(scene="group", level=1)
    state = commands.BotState()
    state.last_find[commands.conv_key(ctx)] = [src]
    state.last_find_label[commands.conv_key(ctx)] = "find label"
    filesvc = _make_filesvc_stub()

    monkeypatch.setattr(commands, "_stage_for_napcat", lambda *_args, **_kwargs: ("/bot_data/get.txt", "get.txt", ""))
    send_mock = AsyncMock(return_value=(True, ""))
    monkeypatch.setattr(commands, "_send_file", send_mock)

    async def _noop_warn(*_args, **_kwargs):
        return None

    monkeypatch.setattr(commands, "_warn_large_if_needed", _noop_warn)

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="/get 1",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=state,
        handin=Mock(),
        perm=Mock(),
        aisvc=None,
    )

    assert send_mock.await_count == 1
    assert send_mock.await_args.args[3] == "get.txt"
    assert dispatch_harness.messages


@pytest.mark.asyncio
async def test_command_aichat_dispatch(dispatch_harness) -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None, level=1)
    filesvc = _make_filesvc_stub()
    aisvc = _FakeAIService()

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "private", "sub_type": "friend"},
        text="C你好",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    aisvc.chat_with_context.assert_awaited_once()
    assert aisvc.chat_with_context.await_args.args[0] == f"private:{ctx.user_id}"
    model_input = aisvc.chat_with_context.await_args.args[1]
    assert "发言人QQ:" not in model_input
    assert "你好" in model_input
    assert any("fake-ai-reply" in one["text"] for one in dispatch_harness.messages)


@pytest.mark.asyncio
async def test_command_aichat_private_prefix_allows_newline_payload(dispatch_harness) -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None, level=1)
    filesvc = _make_filesvc_stub()
    aisvc = _FakeAIService()

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "private", "sub_type": "friend"},
        text="C\nhello",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    aisvc.chat_with_context.assert_awaited_once()
    assert aisvc.chat_with_context.await_args.args[0] == f"private:{ctx.user_id}"
    model_input = aisvc.chat_with_context.await_args.args[1]
    assert "发言人QQ:" not in model_input
    assert "hello" in model_input
    assert any("fake-ai-reply" in one["text"] for one in dispatch_harness.messages)


@pytest.mark.asyncio
async def test_dispatch_slash_command_still_reaches_explicit_command(dispatch_harness, monkeypatch) -> None:
    filesvc = _make_filesvc_stub()
    ctx = _make_ctx(scene="private_friend", group_id=None, level=3, user_id=900001)
    explicit_mock = AsyncMock(return_value=None)
    ai_trigger_mock = AsyncMock(return_value=False)
    plain_mock = AsyncMock(return_value=False)

    monkeypatch.setattr(commands, "_handle_ai_chat_trigger", ai_trigger_mock)
    monkeypatch.setattr(commands, "_handle_plain_text_input", plain_mock)
    monkeypatch.setattr(commands, "_handle_explicit_command", explicit_mock)

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "private", "sub_type": "friend"},
        text="/ping",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=None,
    )

    ai_trigger_mock.assert_awaited_once()
    plain_mock.assert_awaited_once()
    explicit_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_private_c_still_reaches_ai_trigger(dispatch_harness, monkeypatch) -> None:
    filesvc = _make_filesvc_stub()
    ctx = _make_ctx(scene="private_friend", group_id=None, level=3, user_id=900001)
    ai_trigger_mock = AsyncMock(return_value=True)
    plain_mock = AsyncMock(return_value=False)
    explicit_mock = AsyncMock(return_value=None)

    monkeypatch.setattr(commands, "_handle_ai_chat_trigger", ai_trigger_mock)
    monkeypatch.setattr(commands, "_handle_plain_text_input", plain_mock)
    monkeypatch.setattr(commands, "_handle_explicit_command", explicit_mock)

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "private", "sub_type": "friend"},
        text="C你好",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=_FakeAIService(),
    )

    ai_trigger_mock.assert_awaited_once()
    plain_mock.assert_not_awaited()
    explicit_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_dispatch_plain_text_still_reaches_plain_text_input(dispatch_harness, monkeypatch) -> None:
    filesvc = _make_filesvc_stub()
    ctx = _make_ctx(scene="private_friend", group_id=None, level=3, user_id=900001)
    ai_trigger_mock = AsyncMock(return_value=False)
    plain_mock = AsyncMock(return_value=True)
    explicit_mock = AsyncMock(return_value=None)

    monkeypatch.setattr(commands, "_handle_ai_chat_trigger", ai_trigger_mock)
    monkeypatch.setattr(commands, "_handle_plain_text_input", plain_mock)
    monkeypatch.setattr(commands, "_handle_explicit_command", explicit_mock)

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "private", "sub_type": "friend"},
        text="普通文本输入",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=None,
    )

    ai_trigger_mock.assert_awaited_once()
    plain_mock.assert_awaited_once()
    explicit_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_command_private_vs_group_route(dispatch_harness) -> None:
    filesvc = _make_filesvc_stub()
    aisvc = _FakeAIService()

    private_ctx = _make_ctx(scene="private_friend", group_id=None, level=1, user_id=10001)
    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=private_ctx,
        evt={"post_type": "message", "message_type": "private", "sub_type": "friend"},
        text="C提问",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    group_ctx = _make_ctx(scene="group", group_id=20001, level=1, user_id=10002)
    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=group_ctx,
        evt={
            "self_id": "42",
            "message": [
                {"type": "at", "data": {"qq": "42"}},
                {"type": "text", "data": {"text": " 提问"}},
            ],
        },
        text="提问",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    session_keys = {call.args[0] for call in aisvc.chat_with_context.await_args_list}
    assert f"private:{private_ctx.user_id}" in session_keys
    assert f"group:{group_ctx.group_id}" in session_keys


@pytest.mark.asyncio
async def test_group_aichat_strips_cq_at_and_uses_sender_qq(dispatch_harness) -> None:
    filesvc = _make_filesvc_stub()
    aisvc = _FakeAIService()
    group_ctx = _make_ctx(scene="group", group_id=20001, level=1, user_id=10002)

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=group_ctx,
        evt={
            "self_id": "42",
            "post_type": "message",
            "message_type": "group",
            "message": [
                {"type": "at", "data": {"qq": "42"}},
                {"type": "text", "data": {"text": " 你能看到我吗"}},
            ],
            "raw_message": "[CQ:at,qq=42] 你能看到我吗",
        },
        text="[CQ:at,qq=42] 你能看到我吗",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    aisvc.chat_with_context.assert_awaited_once()
    assert aisvc.chat_with_context.await_args.args[0] == f"group:{group_ctx.group_id}"
    model_input = aisvc.chat_with_context.await_args.args[1]
    assert "发言人QQ:10002" in model_input
    assert "发言人昵称:tester" in model_input
    assert "你能看到我吗" in model_input
    assert "qq=42" not in model_input
    assert any("fake-ai-reply" in one["text"] for one in dispatch_harness.messages)


@pytest.mark.asyncio
async def test_aichat_repeat_guard_retries_with_stateless_chat(dispatch_harness) -> None:
    filesvc = _make_filesvc_stub()
    ctx = _make_ctx(scene="private_friend", group_id=None, level=1, user_id=10001)

    aisvc = _FakeAIService()
    aisvc.chat_with_context = AsyncMock(side_effect=["same-output", "same-output"])
    aisvc.chat = AsyncMock(return_value="retry-output")

    state = commands.BotState()
    evt = {"post_type": "message", "message_type": "private", "sub_type": "friend"}
    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt=evt,
        text="C第一句",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=state,
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )
    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt=evt,
        text="C第二句",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=state,
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    assert aisvc.chat_with_context.await_count == 2
    aisvc.chat.assert_awaited_once()
    assert any(one["text"] == "retry-output" for one in dispatch_harness.messages)


@pytest.mark.asyncio
async def test_non_aichat_message_is_remembered_for_context(dispatch_harness) -> None:
    filesvc = _make_filesvc_stub()
    aisvc = _FakeAIService()
    ctx = _make_ctx(scene="group", group_id=20001, level=1, user_id=10002)

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="/find calculus",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    aisvc.remember_user_message.assert_called_once_with(
        f"group:{ctx.group_id}",
        "发言人QQ:10002\n发言人昵称:tester\n群号:20001\n/find calculus",
    )


@pytest.mark.asyncio
async def test_non_aichat_auto_reply_is_remembered_for_context(monkeypatch) -> None:
    async def _noop_group_context(*_args, **_kwargs):
        return None

    async def _noop_pre_state(*_args, **_kwargs):
        return False

    monkeypatch.setattr(commands, "_ensure_group_context_and_schedule_digest", _noop_group_context)
    monkeypatch.setattr(commands, "_handle_pre_dispatch_state", _noop_pre_state)

    filesvc = _make_filesvc_stub()
    aisvc = _FakeAIService()
    ctx = _make_ctx(scene="group", group_id=20001, level=1, user_id=10002)

    await commands.dispatch(
        api=SimpleNamespace(send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0})),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="/ping",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    aisvc.remember_assistant_message.assert_called_with(f"group:{ctx.group_id}", "pong")


@pytest.mark.asyncio
async def test_aichat_reply_does_not_duplicate_assistant_memory(monkeypatch) -> None:
    async def _noop_group_context(*_args, **_kwargs):
        return None

    async def _noop_pre_state(*_args, **_kwargs):
        return False

    monkeypatch.setattr(commands, "_ensure_group_context_and_schedule_digest", _noop_group_context)
    monkeypatch.setattr(commands, "_handle_pre_dispatch_state", _noop_pre_state)

    filesvc = _make_filesvc_stub()
    aisvc = _FakeAIService()
    ctx = _make_ctx(scene="private_friend", group_id=None, level=1, user_id=10001)

    await commands.dispatch(
        api=SimpleNamespace(send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0})),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "private", "sub_type": "friend"},
        text="C你好",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    aisvc.remember_assistant_message.assert_not_called()


@pytest.mark.asyncio
async def test_handin_submit_choice_sends_delayed_reminder(monkeypatch) -> None:
    monkeypatch.setattr(commands, "_HANDIN_SUBMIT_REMINDER_SECONDS", 0.01)
    commands._RECENT_REPLY_KEYS.clear()

    state = commands.BotState()
    ctx = _make_ctx(scene="private_friend", group_id=None, user_id=10031)
    logsvc = _DummyLogService()
    api = SimpleNamespace(send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}))
    state.pending_handin_files[ctx.user_id] = [{"path": "pending.pdf", "name": "pending.pdf", "ts": time.time()}]

    commands._set_pending_handin_submit_choice(api, ctx, logsvc, state, ["task-1"])
    await asyncio.sleep(0.05)

    api.send_private_msg.assert_awaited_once_with(ctx.user_id, commands._HANDIN_SUBMIT_REMINDER_TEXT)
    assert commands._HANDIN_SUBMIT_REMINDER_TEXT in logsvc.out_logs


@pytest.mark.asyncio
async def test_handin_submit_choice_reminder_skips_after_state_clears(monkeypatch) -> None:
    monkeypatch.setattr(commands, "_HANDIN_SUBMIT_REMINDER_SECONDS", 0.01)
    commands._RECENT_REPLY_KEYS.clear()

    state = commands.BotState()
    ctx = _make_ctx(scene="private_friend", group_id=None, user_id=10032)
    logsvc = _DummyLogService()
    api = SimpleNamespace(send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}))
    state.pending_handin_files[ctx.user_id] = [{"path": "pending.pdf", "name": "pending.pdf", "ts": time.time()}]

    commands._set_pending_handin_submit_choice(api, ctx, logsvc, state, ["task-1"])
    state.pending_handin_choose.pop(ctx.user_id, None)
    await asyncio.sleep(0.05)

    api.send_private_msg.assert_not_awaited()


@pytest.mark.asyncio
async def test_link_digest_reply_is_remembered_in_aichat_context(monkeypatch) -> None:
    async def _noop_pre_state(*_args, **_kwargs):
        return False

    monkeypatch.setattr(commands, "_handle_pre_dispatch_state", _noop_pre_state)

    filesvc = _make_filesvc_stub()
    aisvc = _FakeAIService()
    aisvc.extract_notice_url_head = AsyncMock(return_value="preview text")
    aisvc.classify_notice = AsyncMock(return_value=True)
    aisvc.reason_notice = AsyncMock(return_value="digest-reply")
    aisvc.sanitize_reasoner_output = lambda text: str(text)
    aisvc.is_notice_silent = lambda _text: False
    ctx = _make_ctx(scene="group", group_id=20001, level=1, user_id=10002)

    await commands.dispatch(
        api=SimpleNamespace(send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0})),
        ctx=ctx,
        evt={
            "post_type": "message",
            "message_type": "group",
            "message": [{"type": "text", "data": {"text": "https://example.com/notice"}}],
            "raw_message": "https://example.com/notice",
        },
        text="https://example.com/notice",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
    )

    await asyncio.sleep(0.05)

    assert any(
        call.args
        == (
            f"group:{ctx.group_id}",
            "发言人QQ:10002\n发言人昵称:tester\n群号:20001\n群链接：https://example.com/notice",
        )
        for call in aisvc.remember_user_message.call_args_list
    )
    aisvc.remember_assistant_message.assert_called_once_with(f"group:{ctx.group_id}", "digest-reply")


@pytest.mark.asyncio
async def test_command_admin_only_route(dispatch_harness) -> None:
    filesvc = _make_filesvc_stub()
    handin = Mock()

    non_admin_ctx = _make_ctx(scene="group", level=1)
    perm_denied = Mock()
    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=non_admin_ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="/level 10002 2",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=handin,
        perm=perm_denied,
        aisvc=None,
    )
    perm_denied.set_level.assert_not_called()
    assert any("/level" in one["text"] for one in dispatch_harness.messages)

    admin_ctx = _make_ctx(scene="group", level=3, user_id=900001)
    perm_allowed = Mock()
    perm_allowed.get_level.return_value = 2
    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=admin_ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="/level 10002 2",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=handin,
        perm=perm_allowed,
        aisvc=None,
    )
    perm_allowed.set_level.assert_called_once_with(10002, 2)


@pytest.mark.asyncio
async def test_command_handin_dispatch(monkeypatch, dispatch_harness) -> None:
    filesvc = _make_filesvc_stub()
    handin = Mock()
    handin.create_task.return_value = (True, "task-created")
    monkeypatch.setattr(commands, "parse_mmdd_hhmm", lambda _s, _now: 1700000000.0)

    ctx = _make_ctx(scene="group", level=2, group_id=20001, user_id=10001)
    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="/handin hw1 1.1 10:00",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=handin,
        perm=Mock(),
        aisvc=None,
    )

    handin.create_task.assert_called_once()
    args = handin.create_task.call_args.args
    assert args[0] == ctx.group_id
    assert args[1] == ctx.user_id
    assert args[2] == "hw1"
    assert args[3] == []
    assert args[4] == 1700000000.0
    assert any("task-created" in one["text"] for one in dispatch_harness.messages)


@pytest.mark.asyncio
async def test_command_autoat_dispatch_sends_single_group_message(dispatch_harness) -> None:
    filesvc = _make_filesvc_stub()
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        get_group_member_list=AsyncMock(
            return_value={
                "status": "ok",
                "retcode": 0,
                "data": [
                    {"user_id": 10001},
                    {"user_id": 10002},
                    {"user_id": 10003},
                ],
            }
        ),
    )

    ctx = _make_ctx(scene="group", level=2, group_id=20001, user_id=10001)
    logsvc = _DummyLogService()
    await commands.dispatch(
        api=api,
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="/autoat",
        filesvc=filesvc,
        logsvc=logsvc,
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=None,
    )

    api.get_group_member_list.assert_awaited_once_with(20001)
    api.send_group_msg.assert_awaited_once_with(
        20001,
        "[CQ:at,qq=10001] [CQ:at,qq=10002] [CQ:at,qq=10003]",
    )
    assert dispatch_harness.messages == []
    assert logsvc.out_logs == ["[CQ:at,qq=10001] [CQ:at,qq=10002] [CQ:at,qq=10003]"]


@pytest.mark.asyncio
async def test_command_autoat_requires_level_two(dispatch_harness) -> None:
    filesvc = _make_filesvc_stub()
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        get_group_member_list=AsyncMock(),
    )

    ctx = _make_ctx(scene="group", level=1, group_id=20001, user_id=10001)
    await commands.dispatch(
        api=api,
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="/autoat",
        filesvc=filesvc,
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=None,
    )

    api.get_group_member_list.assert_not_awaited()
    api.send_group_msg.assert_not_awaited()
    assert any("/autoat" in one["text"] for one in dispatch_harness.messages)

