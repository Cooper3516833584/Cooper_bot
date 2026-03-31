from __future__ import annotations

from types import SimpleNamespace

import pytest

import commands


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


def test_parse_count_names_supports_multiple_separators() -> None:
    names = commands._parse_count_names("1.Alice，Bob / Carol； 2)Dave")
    assert names == ["Alice", "Bob", "Carol", "Dave"]


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
