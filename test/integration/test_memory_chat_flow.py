from __future__ import annotations

from types import SimpleNamespace

import pytest

import cooper_bot.commands.commands as commands
from cooper_bot.modules.memory.models import MemoryIdentity
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
