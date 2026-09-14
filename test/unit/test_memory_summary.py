from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time

import pytest

from cooper_bot.modules.memory.models import CapturedInput, MemoryIdentity
from cooper_bot.modules.memory.service import MemoryService
from cooper_bot.modules.memory.store import MemoryStore


def _summary() -> dict:
    return {
        "schema_version": 1,
        "topics": ["测试"],
        "current_goal": "验证摘要",
        "decisions": [],
        "open_items": [],
        "uncertainties": [],
    }


class _Gateway:
    def __init__(self, *, blocked: bool = False) -> None:
        self.calls = 0
        self.started = threading.Event()
        self.release = threading.Event()
        if not blocked:
            self.release.set()

    def chat(self, _messages, **_kwargs) -> str:
        self.calls += 1
        self.started.set()
        if not self.release.wait(3):
            raise TimeoutError("test gateway release timed out")
        return json.dumps(_summary(), ensure_ascii=False)


def _identity() -> MemoryIdentity:
    return MemoryIdentity(10101, 20202, "private", None, "public")


async def _complete_turn(service: MemoryService, event_id: str) -> None:
    turn = await service.turn(_identity(), CapturedInput(event_id, f"question-{event_id}"))
    assert turn is not None
    async with turn:
        await turn.record_generated(f"answer-{event_id}")
        await turn.finish_delivery(True)


async def _wait_for_summary(service: MemoryService, scope_id: str, conversation_id: str, epoch: int) -> dict:
    for _ in range(100):
        value = await service.store.summary(scope_id, conversation_id, epoch)
        if value is not None:
            return value
        await asyncio.sleep(0.01)
    raise AssertionError("summary worker did not write a result")


@pytest.mark.asyncio
async def test_summary_not_scheduled_below_threshold(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_SUMMARY_ENABLED", True)
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_SUMMARY_MIN_EVENTS", 3)
    gateway = _Gateway()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(_identity(), True)
    await _complete_turn(service, "one")
    await asyncio.sleep(0)

    def _job_count() -> int:
        return service.store._c().execute("SELECT COUNT(*) FROM memory_jobs").fetchone()[0]

    assert await service.store._call(_job_count) == 0
    assert gateway.calls == 0
    await service.aclose()


@pytest.mark.asyncio
async def test_summary_stable_cutoff_and_confirmed_assistant_parent(tmp_path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    await store.start()
    scope = await store.ensure_scope("scope", bot_id=1, profile="public", kind="private", target_id=2, owner_user_id=2)
    await store.set_scope_policy("scope", True)
    stable, _ = await store.append_input_once("scope", 2, "stable", "stable-user", "", "", "direct_chat")
    await store.insert_assistant_once("scope", stable["seq"], "stable-assistant")
    await store.finish_turn("scope", stable["seq"], True)
    pending, _ = await store.append_input_once("scope", 2, "pending", "pending-user", "", "", "direct_chat")
    await store.append_input_once("scope", 2, "later-passive", "later", "", "", "passive_chat")

    candidate = await store.summary_candidate("scope", 1)
    assert candidate is not None
    assert candidate["target_input_seq"] == stable["seq"]
    context = await store.summary_job_context({
        **candidate,
        "kind": "summary",
    })
    assert context is not None
    assert [(row["role"], row["own_text"]) for row in context["events"]] == [
        ("user", "stable-user"),
        ("assistant", "stable-assistant"),
    ]
    assert all(row["seq"] != pending["seq"] for row in context["events"])
    await store.close()


@pytest.mark.asyncio
async def test_summary_cas_rejects_old_epoch_and_old_base_version(tmp_path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    await store.start()
    scope = await store.ensure_scope("scope", bot_id=1, profile="public", kind="private", target_id=2, owner_user_id=2)
    await store.set_scope_policy("scope", True)
    row, _ = await store.append_input_once("scope", 2, "stable", "stable", "", "", "direct_chat")
    await store.insert_assistant_once("scope", row["seq"], "answer")
    await store.finish_turn("scope", row["seq"], True)
    candidate = await store.summary_candidate("scope", 1)
    assert candidate is not None
    assert await store.write_summary_cas(candidate, _summary(), row["seq"], row["seq"]) is True
    assert await store.write_summary_cas(candidate, _summary(), row["seq"], row["seq"]) is False

    newer = await store.summary("scope", scope["active_conversation_id"], candidate["epoch"])
    assert newer is not None and newer["version"] == 1
    stale_epoch_job = {**candidate, "base_version": 1, "target_input_seq": row["seq"] + 1}
    await store.rotate_conversation("scope")
    assert await store.write_summary_cas(stale_epoch_job, _summary(), row["seq"], row["seq"]) is False
    await store.close()


@pytest.mark.asyncio
async def test_worker_recovers_expired_lease_and_daily_budget_is_persistent(tmp_path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    await store.start()
    await store.enqueue_job("scope", "conversation", 1, "summary", 10, {}, "lease-test")
    first = await store.claim_job(lease_seconds=120)
    assert first is not None and first["attempts"] == 1

    def _expire() -> None:
        with store._c():
            store._c().execute("UPDATE memory_jobs SET lease_until=? WHERE job_id=?", (time.time() - 1, first["job_id"]))

    await store._call(_expire)
    second = await store.claim_job(lease_seconds=120)
    assert second is not None and second["job_id"] == first["job_id"] and second["attempts"] == 2
    assert await store.reserve_daily_usage("summary", 1) is True
    assert await store.reserve_daily_usage("summary", 1) is False
    await store.close()


@pytest.mark.asyncio
async def test_slow_llm_does_not_block_memory_db_commands_and_summary_enters_prompt(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_SUMMARY_ENABLED", True)
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_SUMMARY_MIN_EVENTS", 2)
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_SUMMARY_DAILY_BUDGET", 10)
    gateway = _Gateway(blocked=True)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    identity = _identity()
    await service.set_enabled(identity, True)
    await _complete_turn(service, "one")
    await _complete_turn(service, "two")
    assert await asyncio.to_thread(gateway.started.wait, 2)

    status = await asyncio.wait_for(service.status(identity), timeout=0.5)
    assert status["scope_enabled"] is True
    scope = await service.store.scope("qq:10101:public:private:20202")
    assert scope is not None
    gateway.release.set()
    summary = await _wait_for_summary(service, scope["scope_id"], scope["active_conversation_id"], scope["epoch"])

    next_turn = await service.turn(identity, CapturedInput("three", "question-three"))
    assert next_turn is not None
    async with next_turn:
        assert next_turn.snapshot.summary is not None
        assert next_turn.snapshot.recent_events == ()
        prompt_context = await service.prompt_context(identity, "测试", summary=next_turn.snapshot.summary)
        assert prompt_context["summary"]["content"]["current_goal"] == "验证摘要"
    await service.store.fail_pending_input(next_turn.scope_id, next_turn.snapshot.current_input_seq)
    await service.aclose()


@pytest.mark.asyncio
async def test_schema_v2_migrates_summary_and_job_columns_without_rebuild(tmp_path) -> None:
    db_path = tmp_path / "memory.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
        CREATE TABLE memory_summaries (
          scope_id TEXT NOT NULL, conversation_id TEXT NOT NULL, epoch INTEGER NOT NULL, version INTEGER NOT NULL,
          through_input_seq INTEGER NOT NULL, summary_json TEXT NOT NULL, updated_at REAL NOT NULL,
          PRIMARY KEY(scope_id, conversation_id));
        CREATE TABLE memory_jobs (
          job_id TEXT PRIMARY KEY, scope_id TEXT NOT NULL, conversation_id TEXT, epoch INTEGER NOT NULL,
          kind TEXT NOT NULL, target_input_seq INTEGER, payload_json TEXT NOT NULL DEFAULT '{}', state TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0, not_before REAL NOT NULL, lease_until REAL, last_error_code TEXT,
          dedupe_key TEXT NOT NULL UNIQUE);
        INSERT INTO memory_summaries VALUES('scope','conversation',1,1,10,'{}',1);
        PRAGMA user_version=2;
        """)
    store = MemoryStore(db_path)
    await store.start()

    def _schema_state():
        summary_columns = {row[1] for row in store._c().execute("PRAGMA table_info(memory_summaries)")}
        job_columns = {row[1] for row in store._c().execute("PRAGMA table_info(memory_jobs)")}
        count = store._c().execute("SELECT COUNT(*) FROM memory_summaries").fetchone()[0]
        version = store._c().execute("PRAGMA user_version").fetchone()[0]
        return summary_columns, job_columns, count, version

    summary_columns, job_columns, count, version = await store._call(_schema_state)
    assert {"source_first_seq", "source_last_seq"} <= summary_columns
    assert "base_version" in job_columns
    assert count == 1
    assert version == 3
    await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["clear", "off"])
async def test_clear_off_reject_late_summary_result(tmp_path, monkeypatch, mutation: str) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_SUMMARY_ENABLED", True)
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_SUMMARY_MIN_EVENTS", 2)
    gateway = _Gateway(blocked=True)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    identity = _identity()
    await service.set_enabled(identity, True)
    await _complete_turn(service, "one")
    await _complete_turn(service, "two")
    assert await asyncio.to_thread(gateway.started.wait, 2)
    scope = await service.store.scope("qq:10101:public:private:20202")
    assert scope is not None

    if mutation == "clear":
        await service.clear_subject(identity)
    else:
        await service.set_member_enabled(identity, False)
    gateway.release.set()
    await asyncio.sleep(0.05)

    assert await service.store.summary(scope["scope_id"], scope["active_conversation_id"], scope["epoch"]) is None
    await service.aclose()
