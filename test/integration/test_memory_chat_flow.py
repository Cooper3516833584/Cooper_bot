from __future__ import annotations

from types import SimpleNamespace

import pytest

import cooper_bot.commands.commands as commands
from cooper_bot.commands.memory_commands import handle_memory_command
from cooper_bot.modules.memory.models import CapturedInput, MemoryIdentity
from cooper_bot.modules.memory.policy import scope_for
from cooper_bot.modules.memory.service import MemoryService


GROUP_ID = 30303


class _FakeAI:
    def __init__(self, memory: MemoryService) -> None:
        self.memory = memory
        self.bot_nick = "Cooper_bot"
        self.chat_ready = True
        self.computer_ready = False
        self.fallback_error_reply = "fallback"
        self.run_count = 0

    def remember_user_message(self, *_args, **_kwargs) -> None:
        return

    async def chat_with_context(self, *_args, **_kwargs) -> str:
        self.run_count += 1
        return "must-not-run"

    async def chat(self, *_args, **_kwargs) -> str:
        self.run_count += 1
        return "must-not-run"


class _LogService:
    def __init__(self) -> None:
        self.log = SimpleNamespace(warning=lambda _message: None)

    def log_in(self, *_args) -> None:
        return

    def log_out(self, *_args) -> None:
        return


def _ctx():
    return SimpleNamespace(scene="group", user_id=20202, group_id=GROUP_ID, nickname="member", card="member", group_name="group", level=1)


def _event(message_id: str, text: str) -> dict:
    return {
        "post_type": "message",
        "message_type": "group",
        "self_id": 10101,
        "message_id": message_id,
        "message": [{"type": "text", "data": {"text": text}}],
        "raw_message": text,
    }


async def _dispatch(ai: _FakeAI, text: str, message_id: str) -> None:
    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=_ctx(),
        evt=_event(message_id, text),
        text=text,
        filesvc=SimpleNamespace(),
        logsvc=_LogService(),
        state=commands.BotState(),
        handin=SimpleNamespace(),
        perm=SimpleNamespace(get_level=lambda _actor: 1),
        aisvc=ai,
    )


@pytest.mark.asyncio
async def test_group_all_captures_passive_without_running_kimi_and_directed_does_not(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_GROUP_ALLOWLIST", {GROUP_ID})

    async def _noop(*_args, **_kwargs):
        return None

    async def _not_handled(*_args, **_kwargs):
        return False

    replies: list[str] = []

    async def _reply(_api, _ctx, text, _logsvc, force_private_user_id=None) -> bool:
        replies.append(str(text))
        return True

    monkeypatch.setattr(commands, "_ensure_group_context_and_schedule_digest", _noop)
    monkeypatch.setattr(commands, "_handle_pre_dispatch_state", _not_handled)
    monkeypatch.setattr(commands, "_lookup_fixed_answers", lambda _text: [])
    monkeypatch.setattr(commands, "_lookup_keyword_answers", lambda _text: [])
    monkeypatch.setattr(commands, "reply", _reply)

    admin = MemoryIdentity(10101, 90909, "group", GROUP_ID, "public", True)
    member = MemoryIdentity(10101, 20202, "group", GROUP_ID, "public")
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    ai = _FakeAI(service)
    await service.set_group_enabled(admin, True, "all")

    await _dispatch(ai, "普通群聊内容", "passive-1")
    await _dispatch(ai, "普通群聊内容", "passive-1")
    rows = await service.history(member)
    assert [(row["source_event_id"], row["source_kind"]) for row in rows] == [("passive-1", "passive_chat")]
    assert ai.run_count == 0

    await service.set_group_enabled(admin, True, "directed")
    await _dispatch(ai, "directed 下不采集", "passive-2")
    await _dispatch(ai, "/memory status", "memory-command")
    rows = await service.history(member)
    assert {row["source_event_id"] for row in rows} == {"passive-1"}
    assert ai.run_count == 0
    assert any("总开关：开启" in message for message in replies)
    await service.aclose()


@pytest.mark.asyncio
async def test_group_chat_is_not_captured_until_group_memory_is_on(tmp_path, monkeypatch) -> None:
    """默认全关：管理员开启当前群会话之前，群聊不落库；开启后才采集。"""

    async def _noop(*_args, **_kwargs):
        return None

    async def _not_handled(*_args, **_kwargs):
        return False

    async def _reply(_api, _ctx, text, _logsvc, force_private_user_id=None) -> bool:
        return True

    monkeypatch.setattr(commands, "_ensure_group_context_and_schedule_digest", _noop)
    monkeypatch.setattr(commands, "_handle_pre_dispatch_state", _not_handled)
    monkeypatch.setattr(commands, "_lookup_fixed_answers", lambda _text: [])
    monkeypatch.setattr(commands, "_lookup_keyword_answers", lambda _text: [])
    monkeypatch.setattr(commands, "reply", _reply)

    admin = MemoryIdentity(10101, 90909, "group", GROUP_ID, "public", True)
    member = MemoryIdentity(10101, 20202, "group", GROUP_ID, "public")
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    ai = _FakeAI(service)

    await _dispatch(ai, "默认不该采集的群聊", "off-passive-1")
    assert await service.history(member) == []
    scope = await service.store.scope(scope_for(member))
    assert scope is not None and not bool(scope["enabled"])

    await service.set_group_enabled(admin, True, "all")
    await _dispatch(ai, "开启后的群聊", "on-passive-1")
    rows = await service.history(member)
    assert [(row["source_event_id"], row["source_kind"]) for row in rows] == [("on-passive-1", "passive_chat")]
    assert ai.run_count == 0
    await service.aclose()


@pytest.mark.asyncio
async def test_private_chat_is_not_captured_until_memory_on(tmp_path) -> None:
    """默认全关：私聊要先发 /memory on 才会采集。"""
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    identity = MemoryIdentity(10101, 20202, "private", None, "public")

    assert await service.turn(identity, CapturedInput("private-off-1", "默认不该采集")) is None
    scope = await service.store.scope(scope_for(identity))
    assert scope is not None and not bool(scope["enabled"])

    reply, _ = await handle_memory_command(service, identity, "/memory on")
    assert "已开启当前作用域的记忆" in reply
    turn = await service.turn(identity, CapturedInput("private-on-1", "开启后采集"))
    assert turn is not None
    async with turn:
        await turn.record_generated("回答")
        await turn.finish_delivery(True)
    assert [row["own_text"] for row in await service.history(identity)] == ["开启后采集", "回答"]
    await service.aclose()
