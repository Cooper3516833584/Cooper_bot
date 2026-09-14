from __future__ import annotations

import asyncio
import sqlite3
from types import SimpleNamespace

import pytest

import cooper_bot.commands.commands as commands
from cooper_bot.modules.memory.models import CapturedInput, MemoryIdentity
from cooper_bot.modules.memory.service import MemoryService
from cooper_bot.modules.memory.store import MemoryStore


class _FakeAI:
    def __init__(self, memory: MemoryService, *, blocked: bool = False) -> None:
        self.memory = memory
        self.bot_nick = "Cooper_bot"
        self.chat_ready = True
        self.computer_ready = True
        self.fallback_error_reply = "fallback"
        self.run_count = 0
        self.run_started = asyncio.Event()
        self.release = asyncio.Event()
        if not blocked:
            self.release.set()

    async def chat_with_context(self, *_args, **_kwargs) -> str:
        self.run_count += 1
        self.run_started.set()
        await self.release.wait()
        return "fake-memory-reply"

    async def chat(self, *_args, **_kwargs) -> str:
        raise AssertionError("stateless fallback must not run")


class _ReplyRecorder:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def __call__(self, _api, _ctx, text, _logsvc, force_private_user_id=None) -> bool:
        self.messages.append(str(text))
        return True


def _identity(*, admin: bool = False) -> MemoryIdentity:
    return MemoryIdentity(10101, 20202, "private", None, "admin" if admin else "public")


def _ctx():
    return SimpleNamespace(scene="private_friend", user_id=20202, group_id=None, nickname="tester", card="tester", level=1)


def _logsvc():
    return SimpleNamespace(log=SimpleNamespace(warning=lambda _message: None))


async def _run_trigger(ai: _FakeAI, *, event_id: str) -> bool:
    return await commands._handle_ai_chat_trigger(
        SimpleNamespace(),
        _ctx(),
        {"self_id": 10101},
        "相同正文",
        _logsvc(),
        ai,
        forced_ai_input="相同正文",
        message_id=event_id,
    )


@pytest.mark.asyncio
async def test_duplicate_pending_event_runs_kimi_once(tmp_path, monkeypatch) -> None:
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    await service.set_enabled(_identity(), True)
    ai = _FakeAI(service, blocked=True)
    replies = _ReplyRecorder()
    monkeypatch.setattr(commands, "reply", replies)
    monkeypatch.setattr(commands, "_ai_chat_allows_kimi_computer", lambda *_args: False)

    original_turn = service.turn
    second_registered = asyncio.Event()
    call_count = 0

    async def _tracked_turn(*args, **kwargs):
        nonlocal call_count
        turn = await original_turn(*args, **kwargs)
        call_count += 1
        if call_count == 2:
            second_registered.set()
        return turn

    service.turn = _tracked_turn
    first = asyncio.create_task(_run_trigger(ai, event_id="event-1"))
    await ai.run_started.wait()
    duplicate = asyncio.create_task(_run_trigger(ai, event_id="event-1"))
    await second_registered.wait()
    ai.release.set()
    assert await asyncio.gather(first, duplicate) == [True, True]
    assert ai.run_count == 1
    assert replies.messages == ["fake-memory-reply"]
    await service.aclose()


@pytest.mark.asyncio
async def test_duplicate_completed_event_does_not_resend(tmp_path, monkeypatch) -> None:
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    await service.set_enabled(_identity(), True)
    ai = _FakeAI(service)
    replies = _ReplyRecorder()
    monkeypatch.setattr(commands, "reply", replies)
    monkeypatch.setattr(commands, "_ai_chat_allows_kimi_computer", lambda *_args: False)

    assert await _run_trigger(ai, event_id="event-1") is True
    assert await _run_trigger(ai, event_id="event-1") is True
    assert ai.run_count == 1
    assert replies.messages == ["fake-memory-reply"]
    await service.aclose()


@pytest.mark.asyncio
async def test_same_text_different_event_ids_are_distinct(tmp_path, monkeypatch) -> None:
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    await service.set_enabled(_identity(), True)
    ai = _FakeAI(service)
    replies = _ReplyRecorder()
    monkeypatch.setattr(commands, "reply", replies)
    monkeypatch.setattr(commands, "_ai_chat_allows_kimi_computer", lambda *_args: False)

    assert await _run_trigger(ai, event_id="event-1") is True
    assert await _run_trigger(ai, event_id="event-2") is True
    assert ai.run_count == 2
    assert len(replies.messages) == 2
    await service.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("admin", [False, True], ids=["public", "admin"])
async def test_queued_turn_becomes_stale_before_kimi(tmp_path, monkeypatch, admin: bool) -> None:
    db_path = tmp_path / "memory.sqlite3"
    identity = _identity(admin=admin)
    service = MemoryService(enabled=True, db_path=db_path)
    await service.set_enabled(identity, True)
    blocker = await service.turn(identity, CapturedInput("blocker", "hold", source_kind="request_metadata" if admin else "direct_chat"))
    assert blocker is not None
    await blocker.__aenter__()

    ai = _FakeAI(service)
    replies = _ReplyRecorder()
    monkeypatch.setattr(commands, "reply", replies)
    monkeypatch.setattr(commands, "_ai_chat_allows_kimi_computer", lambda *_args: admin)
    original_turn = service.turn
    registered = asyncio.Event()

    async def _tracked_turn(*args, **kwargs):
        turn = await original_turn(*args, **kwargs)
        registered.set()
        return turn

    service.turn = _tracked_turn
    queued = asyncio.create_task(_run_trigger(ai, event_id="queued-event"))
    await registered.wait()
    old_conversation_id = blocker.snapshot.conversation_id
    new_conversation_id = await service.rotate_conversation(identity)
    await blocker.__aexit__(None, None, None)
    await service.store.fail_pending_input(blocker.scope_id, blocker.snapshot.current_input_seq)

    assert await queued is True
    assert ai.run_count == 0
    assert replies.messages == []
    await service.aclose()

    with sqlite3.connect(db_path) as conn:
        state, conversation_id = conn.execute(
            "SELECT state,conversation_id FROM memory_events WHERE source_event_id='queued-event'"
        ).fetchone()
        assistant_count = conn.execute(
            "SELECT COUNT(*) FROM memory_events WHERE parent_input_seq=(SELECT seq FROM memory_events WHERE source_event_id='queued-event')"
        ).fetchone()[0]
    assert state == "failed"
    assert conversation_id == old_conversation_id
    assert conversation_id != new_conversation_id
    assert assistant_count == 0


@pytest.mark.asyncio
async def test_restart_marks_pending_and_generated_terminal_without_resend(tmp_path) -> None:
    db_path = tmp_path / "memory.sqlite3"
    store = MemoryStore(db_path)
    await store.start()
    scope = await store.ensure_scope("qq:1:public:private:2", bot_id=1, profile="public", kind="private", target_id=2, owner_user_id=2)
    generated_input, _ = await store.append_input_once(scope["scope_id"], 2, "generated", "question", "", "", "direct_chat")
    await store.insert_assistant_once(scope["scope_id"], generated_input["seq"], "answer")
    await store.append_input_once(scope["scope_id"], 2, "pending", "question", "", "", "direct_chat")
    await store.close()

    service = MemoryService(enabled=True, db_path=db_path)
    await service.start()
    await service.aclose()

    with sqlite3.connect(db_path) as conn:
        states = dict(conn.execute("SELECT source_event_id,state FROM memory_events"))
    assert states["generated"] == "unconfirmed"
    assert states["turn:" + generated_input["turn_id"] + ":assistant"] == "unconfirmed"
    assert states["pending"] == "failed"


@pytest.mark.asyncio
async def test_clear_wins_send_gate_old_answer_not_sent(tmp_path, monkeypatch) -> None:
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    identity = _identity()
    await service.set_enabled(identity, True)
    ai = _FakeAI(service)
    replies = _ReplyRecorder()
    monkeypatch.setattr(commands, "reply", replies)
    monkeypatch.setattr(commands, "_ai_chat_allows_kimi_computer", lambda *_args: False)
    original_turn = service.turn
    before_permit = asyncio.Event()
    release_permit = asyncio.Event()

    async def _tracked_turn(*args, **kwargs):
        turn = await original_turn(*args, **kwargs)
        original_permit = turn.acquire_send_permit

        async def _paused_permit():
            before_permit.set()
            await release_permit.wait()
            return await original_permit()

        turn.acquire_send_permit = _paused_permit
        return turn

    service.turn = _tracked_turn
    request = asyncio.create_task(_run_trigger(ai, event_id="clear-wins"))
    await before_permit.wait()
    await service.clear_subject(identity)
    release_permit.set()

    assert await request is True
    assert ai.run_count == 1
    assert replies.messages == []
    await service.aclose()


@pytest.mark.asyncio
async def test_send_permit_wins_then_clear_allows_inflight_reply(tmp_path, monkeypatch) -> None:
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    identity = _identity()
    await service.set_enabled(identity, True)
    ai = _FakeAI(service)
    reply_started = asyncio.Event()
    release_reply = asyncio.Event()
    messages: list[str] = []

    async def _blocked_reply(_api, _ctx, text, _logsvc, force_private_user_id=None) -> bool:
        messages.append(str(text))
        reply_started.set()
        await release_reply.wait()
        return True

    monkeypatch.setattr(commands, "reply", _blocked_reply)
    monkeypatch.setattr(commands, "_ai_chat_allows_kimi_computer", lambda *_args: False)
    request = asyncio.create_task(_run_trigger(ai, event_id="send-wins"))
    await reply_started.wait()
    await service.clear_subject(identity)
    release_reply.set()

    assert await request is True
    assert messages == ["fake-memory-reply"]
    next_turn = await service.turn(identity, CapturedInput("after-clear", "new input"))
    assert next_turn is not None
    async with next_turn:
        assert next_turn.snapshot.recent_events == ()
    await service.store.fail_pending_input(next_turn.scope_id, next_turn.snapshot.current_input_seq)
    await service.aclose()
