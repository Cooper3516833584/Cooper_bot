from __future__ import annotations

from types import SimpleNamespace

import pytest

import cooper_bot.commands.commands as commands


class _DummyLogger:
    def warning(self, _msg: str) -> None:
        return None


class _DummyLogService:
    def __init__(self) -> None:
        self.log = _DummyLogger()
        self.in_logs: list[str] = []

    def log_in(self, _ctx, text: str) -> None:
        self.in_logs.append(str(text))

    def log_out(self, _ctx, _text: str) -> None:
        return None


def _make_ctx() -> SimpleNamespace:
    return SimpleNamespace(
        scene="group",
        user_id=10001,
        nickname="tester",
        card="tester",
        group_id=20001,
        group_name="group",
        level=1,
    )


def _make_filesvc() -> SimpleNamespace:
    return SimpleNamespace(roots=[SimpleNamespace(name="public"), SimpleNamespace(name="friend")])


def _make_private_ctx() -> SimpleNamespace:
    return SimpleNamespace(
        scene="private_friend",
        user_id=10001,
        nickname="tester",
        card="tester",
        group_id=None,
        group_name=None,
        level=1,
    )


@pytest.mark.asyncio
async def test_count_flow_collect_countlist_and_end(monkeypatch) -> None:
    replies: list[str] = []

    async def _fake_reply(_api, _ctx, text: str, _logsvc, force_private_user_id=None) -> None:
        _ = force_private_user_id
        replies.append(str(text))

    monkeypatch.setattr(commands, "reply", _fake_reply)

    state = commands.BotState()
    logsvc = _DummyLogService()
    ctx = _make_ctx()
    filesvc = _make_filesvc()
    handin = SimpleNamespace(
        _tasks={},
        _get_roster=lambda: [("U1", "Alice"), ("U2", "Bob"), ("U3", "Carol")],
    )

    await commands._handle_explicit_command(
        api=SimpleNamespace(),
        ctx=ctx,
        t="/count",
        filesvc=filesvc,
        logsvc=logsvc,
        state=state,
        handin=handin,
        perm=None,
        aisvc=None,
    )

    key = commands.conv_key(ctx)
    assert key in state.pending_count_session

    handled = await commands._handle_pre_dispatch_state(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="Alice Bob",
        logsvc=logsvc,
        state=state,
        handin=handin,
        filesvc=filesvc,
    )
    assert handled is True
    assert state.pending_count_session[key]["names"] == ["Alice", "Bob"]

    await commands._handle_explicit_command(
        api=SimpleNamespace(),
        ctx=ctx,
        t="/countlist",
        filesvc=filesvc,
        logsvc=logsvc,
        state=state,
        handin=handin,
        perm=None,
        aisvc=None,
    )
    assert "已提交名单" in replies[-1]
    assert "Alice" in replies[-1]
    assert "Bob" in replies[-1]
    assert "Carol" in replies[-1]

    handled_end = await commands._handle_pre_dispatch_state(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="end",
        logsvc=logsvc,
        state=state,
        handin=handin,
        filesvc=filesvc,
    )
    assert handled_end is True
    assert key not in state.pending_count_session

    await commands._handle_explicit_command(
        api=SimpleNamespace(),
        ctx=ctx,
        t="/countlist",
        filesvc=filesvc,
        logsvc=logsvc,
        state=state,
        handin=handin,
        perm=None,
        aisvc=None,
    )
    assert "没有进行中的 /count 统计" in replies[-1]


def test_parse_count_names_supports_numbered_multiline() -> None:
    names = commands._parse_count_names("1、Alice\n2、Bob\n3)Carol\n4.Dave\n5.   Eve  6.Frank")
    assert names == ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"]


@pytest.mark.asyncio
async def test_help_includes_count_commands(monkeypatch) -> None:
    replies: list[str] = []

    async def _fake_reply(_api, _ctx, text: str, _logsvc, force_private_user_id=None) -> None:
        _ = force_private_user_id
        replies.append(str(text))

    monkeypatch.setattr(commands, "reply", _fake_reply)

    await commands._handle_explicit_command(
        api=SimpleNamespace(),
        ctx=_make_ctx(),
        t="/help",
        filesvc=_make_filesvc(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=SimpleNamespace(_tasks={}),
        perm=None,
        aisvc=None,
    )

    assert replies
    assert "/count  开始临时收集名单" in replies[-1]
    assert "/countlist  查看已提交名单和未交名单" in replies[-1]
    assert "/countremove 序号" in replies[-1]
    assert "/autoat  单条消息依次 @ 当前群全部成员（仅群聊）" not in replies[-1]


@pytest.mark.asyncio
async def test_help_includes_autoat_for_level_two(monkeypatch) -> None:
    replies: list[str] = []

    async def _fake_reply(_api, _ctx, text: str, _logsvc, force_private_user_id=None) -> None:
        _ = force_private_user_id
        replies.append(str(text))

    monkeypatch.setattr(commands, "reply", _fake_reply)

    ctx = _make_ctx()
    ctx.level = 2
    await commands._handle_explicit_command(
        api=SimpleNamespace(),
        ctx=ctx,
        t="/help",
        filesvc=_make_filesvc(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=SimpleNamespace(_tasks={}),
        perm=None,
        aisvc=None,
    )

    assert replies
    assert "/autoat  单条消息依次 @ 当前群全部成员（仅群聊）" in replies[-1]


@pytest.mark.asyncio
async def test_count_end_accepts_slash_and_punctuation(monkeypatch) -> None:
    replies: list[str] = []

    async def _fake_reply(_api, _ctx, text: str, _logsvc, force_private_user_id=None) -> None:
        _ = force_private_user_id
        replies.append(str(text))

    monkeypatch.setattr(commands, "reply", _fake_reply)

    state = commands.BotState()
    ctx = _make_ctx()
    key = commands.conv_key(ctx)
    state.pending_count_session[key] = {"names": ["Alice"], "ts": 1.0}

    handled = await commands._handle_pre_dispatch_state(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="/END。",
        logsvc=_DummyLogService(),
        state=state,
        handin=SimpleNamespace(_tasks={}),
        filesvc=_make_filesvc(),
    )

    assert handled is True
    assert key not in state.pending_count_session
    assert replies
    assert "已结束" in replies[-1]


@pytest.mark.asyncio
async def test_count_mode_collects_plain_text_even_if_like_ai_content(monkeypatch) -> None:
    replies: list[str] = []

    async def _fake_reply(_api, _ctx, text: str, _logsvc, force_private_user_id=None) -> None:
        _ = force_private_user_id
        replies.append(str(text))

    monkeypatch.setattr(commands, "reply", _fake_reply)

    state = commands.BotState()
    ctx = _make_ctx()
    key = commands.conv_key(ctx)
    state.pending_count_session[key] = {"names": ["Alice"], "ts": 1.0}

    handled = await commands._handle_pre_dispatch_state(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="朱稷",
        logsvc=_DummyLogService(),
        state=state,
        handin=SimpleNamespace(_tasks={}),
        filesvc=_make_filesvc(),
    )

    assert handled is True
    assert state.pending_count_session[key]["names"] == ["Alice", "朱稷"]
    assert replies
    assert "已记录" in replies[-1]


@pytest.mark.asyncio
async def test_count_mode_blocks_other_commands(monkeypatch) -> None:
    replies: list[str] = []

    async def _fake_reply(_api, _ctx, text: str, _logsvc, force_private_user_id=None) -> None:
        _ = force_private_user_id
        replies.append(str(text))

    monkeypatch.setattr(commands, "reply", _fake_reply)

    state = commands.BotState()
    ctx = _make_ctx()
    key = commands.conv_key(ctx)
    state.pending_count_session[key] = {"names": ["Alice"], "ts": 1.0}

    handled = await commands._handle_pre_dispatch_state(
        api=SimpleNamespace(),
        ctx=ctx,
        evt={"post_type": "message", "message_type": "group"},
        text="/ping",
        logsvc=_DummyLogService(),
        state=state,
        handin=SimpleNamespace(_tasks={}),
        filesvc=_make_filesvc(),
    )

    assert handled is True
    assert state.pending_count_session[key]["names"] == ["Alice"]
    assert replies
    assert "/count 统计模式" in replies[-1]


@pytest.mark.asyncio
async def test_countlist_is_bound_to_same_context(monkeypatch) -> None:
    replies: list[str] = []

    async def _fake_reply(_api, _ctx, text: str, _logsvc, force_private_user_id=None) -> None:
        _ = force_private_user_id
        replies.append(str(text))

    monkeypatch.setattr(commands, "reply", _fake_reply)

    state = commands.BotState()
    group_ctx = _make_ctx()
    private_ctx = _make_private_ctx()
    state.pending_count_session[commands.conv_key(group_ctx)] = {"names": ["Alice"], "ts": 1.0}

    await commands._handle_explicit_command(
        api=SimpleNamespace(),
        ctx=private_ctx,
        t="/countlist",
        filesvc=_make_filesvc(),
        logsvc=_DummyLogService(),
        state=state,
        handin=SimpleNamespace(_tasks={}, _get_roster=lambda: [("U1", "Alice")]),
        perm=None,
        aisvc=None,
    )

    assert replies
    assert "当前会话没有进行中的 /count 统计" in replies[-1]


@pytest.mark.asyncio
async def test_countremove_removes_by_submitted_index(monkeypatch) -> None:
    replies: list[str] = []

    async def _fake_reply(_api, _ctx, text: str, _logsvc, force_private_user_id=None) -> None:
        _ = force_private_user_id
        replies.append(str(text))

    monkeypatch.setattr(commands, "reply", _fake_reply)

    state = commands.BotState()
    ctx = _make_ctx()
    key = commands.conv_key(ctx)
    state.pending_count_session[key] = {"names": ["Alice", "Bob", "Outsider"], "ts": 1.0}

    await commands._handle_explicit_command(
        api=SimpleNamespace(),
        ctx=ctx,
        t="/countremove 3",
        filesvc=_make_filesvc(),
        logsvc=_DummyLogService(),
        state=state,
        handin=SimpleNamespace(_tasks={}),
        perm=None,
        aisvc=None,
    )

    assert state.pending_count_session[key]["names"] == ["Alice", "Bob"]
    assert replies
    assert "已移除：Outsider" in replies[-1]
