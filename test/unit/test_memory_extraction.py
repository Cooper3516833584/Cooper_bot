from __future__ import annotations

import asyncio
import json
import threading

import pytest

from cooper_bot.modules.memory.models import CapturedInput, MemoryIdentity
from cooper_bot.modules.memory.policy import scope_for
from cooper_bot.modules.memory.service import MemoryService
from cooper_bot.modules.memory.store import MemoryStore


class _Gateway:
    def __init__(self, builder) -> None:
        self.builder = builder
        self.payloads: list[dict] = []

    def chat(self, messages, **_kwargs) -> str:
        payload = json.loads(messages[1]["content"])
        self.payloads.append(payload)
        return json.dumps(self.builder(payload), ensure_ascii=False)


class _BlockingGateway(_Gateway):
    def __init__(self, builder) -> None:
        super().__init__(builder)
        self.started = threading.Event()
        self.release = threading.Event()

    def chat(self, messages, **kwargs) -> str:
        self.started.set()
        if not self.release.wait(3):
            raise TimeoutError("test gateway release timed out")
        return super().chat(messages, **kwargs)


def _identity(actor: int = 20202) -> MemoryIdentity:
    return MemoryIdentity(10101, actor, "private", None, "public")


async def _complete(service: MemoryService, identity: MemoryIdentity, event_id: str, text: str, *, quoted: str = "", visual: str = "") -> int:
    turn = await service.turn(identity, CapturedInput(event_id, text, quoted, visual))
    assert turn is not None
    async with turn:
        await turn.record_generated("answer")
        await turn.finish_delivery(True)
    return turn.snapshot.current_input_seq


async def _wait_for_cursor(service: MemoryService, identity: MemoryIdentity, target: int) -> None:
    scope_id = scope_for(identity)

    def _cursor() -> int:
        row = service.store._c().execute(
            "SELECT extracted_through_input_seq FROM memory_members WHERE scope_id=? AND actor_user_id=?",
            (scope_id, identity.actor_user_id),
        ).fetchone()
        return int(row[0]) if row else 0

    for _ in range(100):
        if await service.store._call(_cursor) >= target:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("extraction cursor did not advance")


def _enable_auto(monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_AUTO_EXTRACT_ENABLED", True)
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_AUTO_EXTRACT_MIN_EVENTS", 1)
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_AUTO_EXTRACT_DAILY_BUDGET", 20)
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_SUMMARY_ENABLED", False)


@pytest.mark.asyncio
async def test_extracts_verified_first_person_fact_and_model_cannot_choose_identity(tmp_path, monkeypatch) -> None:
    _enable_auto(monkeypatch)

    def _build(payload):
        event = payload["events"][0]
        return {
            "schema_version": 1,
            "facts": [{
                "fact_key": "preference.explanation_style",
                "text": "用户偏好多举具体例子",
                "evidence_input_seq": event["input_seq"],
                "evidence_quote": "多举具体例子",
                "subject_id": "user:999999",
                "scope_id": "other-scope",
                "permission": "admin",
            }],
        }

    gateway = _Gateway(_build)
    identity = _identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)
    seq = await _complete(service, identity, "fact", "以后解释的时候多举具体例子")
    await _wait_for_cursor(service, identity, seq)

    facts = await service.list_facts(identity)
    assert [(row["subject_id"], row["fact_key"], row["text"]) for row in facts] == [
        ("user:20202", "preference.explanation_style", "用户偏好多举具体例子")
    ]
    assert await service.list_facts(_identity(20203)) == []
    assert set(gateway.payloads[0]["events"][0]) == {"input_seq", "own_text", "source_kind", "created_at"}
    await service.aclose()


@pytest.mark.asyncio
async def test_does_not_extract_quote_or_visual_and_empty_valid_result_advances_cursor(tmp_path, monkeypatch) -> None:
    _enable_auto(monkeypatch)

    def _build(payload):
        event = payload["events"][0]
        return {
            "schema_version": 1,
            "facts": [{
                "fact_key": "preference.food",
                "text": "用户喜欢榴莲",
                "evidence_input_seq": event["input_seq"],
                "evidence_quote": "我喜欢榴莲",
            }],
        }

    gateway = _Gateway(_build)
    identity = _identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)
    seq = await _complete(service, identity, "quote", "我不知道", quoted="别人说：我喜欢榴莲", visual="图片看起来喜欢榴莲")
    await _wait_for_cursor(service, identity, seq)

    assert await service.list_facts(identity) == []
    assert "quoted_text" not in gateway.payloads[0]["events"][0]
    assert "visual_text" not in gateway.payloads[0]["events"][0]
    await service.aclose()


@pytest.mark.asyncio
async def test_empty_valid_extraction_advances_cursor(tmp_path, monkeypatch) -> None:
    _enable_auto(monkeypatch)
    gateway = _Gateway(lambda _payload: {"schema_version": 1, "facts": []})
    identity = _identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)
    seq = await _complete(service, identity, "empty", "今天讨论一个临时问题")
    await _wait_for_cursor(service, identity, seq)
    assert await service.list_facts(identity) == []
    assert len(gateway.payloads) == 1
    await service.aclose()


@pytest.mark.asyncio
async def test_explicit_fact_wins_same_key_auto_conflict(tmp_path, monkeypatch) -> None:
    _enable_auto(monkeypatch)

    def _build(payload):
        event = payload["events"][0]
        return {"schema_version": 1, "facts": [{
            "fact_key": "preference.explanation_style",
            "text": "自动候选不应覆盖",
            "evidence_input_seq": event["input_seq"],
            "evidence_quote": "偏好",
        }]}

    gateway = _Gateway(_build)
    identity = _identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)
    await service.remember_explicit(identity, "显式事实优先")
    seq = await _complete(service, identity, "conflict", "我的偏好已经明确")
    await _wait_for_cursor(service, identity, seq)
    facts = await service.list_facts(identity)
    assert [(row["source_kind"], row["text"]) for row in facts] == [("explicit_memory", "显式事实优先")]
    await service.aclose()


@pytest.mark.asyncio
async def test_forget_rejects_late_old_job_but_allows_new_restatement(tmp_path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    await store.start()
    scope = await store.ensure_scope("scope", bot_id=1, profile="public", kind="private", target_id=2, owner_user_id=2)
    await store.set_scope_policy("scope", True)
    old, _ = await store.append_input_once("scope", 2, "old", "我偏好多举例子", "", "", "direct_chat")
    await store.insert_assistant_once("scope", old["seq"], "answer")
    await store.finish_turn("scope", old["seq"], True)
    candidate = await store.extraction_candidate("scope", 2, 1)
    assert candidate is not None
    old_job = {
        **candidate,
        "payload_json": json.dumps({"actor_user_id": 2, "cursor": candidate["cursor"]}),
    }
    fact = {
        "fact_key": "preference.explanation_style",
        "text": "用户偏好多举例子",
        "evidence_input_seq": old["seq"],
        "evidence_quote": "多举例子",
    }
    assert await store.apply_extracted_facts(old_job, 2, candidate["cursor"], candidate["target_input_seq"], [fact]) is True
    active = await store.list_facts("scope", ("user:2",))
    assert len(active) == 1
    assert await store.forget_fact("scope", "user:2", active[0]["fact_id"]) is True
    assert await store.apply_extracted_facts(old_job, 2, candidate["cursor"], candidate["target_input_seq"], [fact]) is False

    new, _ = await store.append_input_once("scope", 2, "new", "我再次明确偏好多举例子", "", "", "direct_chat")
    await store.insert_assistant_once("scope", new["seq"], "answer")
    await store.finish_turn("scope", new["seq"], True)
    new_candidate = await store.extraction_candidate("scope", 2, 1)
    assert new_candidate is not None
    new_job = {
        **new_candidate,
        "payload_json": json.dumps({"actor_user_id": 2, "cursor": new_candidate["cursor"]}),
    }
    new_fact = {**fact, "evidence_input_seq": new["seq"]}
    assert await store.apply_extracted_facts(new_job, 2, new_candidate["cursor"], new_candidate["target_input_seq"], [new_fact]) is True
    assert [row["text"] for row in await store.list_facts("scope", ("user:2",))] == ["用户偏好多举例子"]

    def _marker() -> int:
        return store._c().execute(
            "SELECT blocked_through_input_seq FROM memory_forget_markers WHERE scope_id='scope' AND subject_id='user:2' AND fact_key='preference.explanation_style'"
        ).fetchone()[0]

    assert await store._call(_marker) == old["seq"]
    await store.close()


@pytest.mark.asyncio
async def test_clear_rejects_late_extraction_worker_result(tmp_path, monkeypatch) -> None:
    _enable_auto(monkeypatch)

    def _build(payload):
        event = payload["events"][0]
        return {"schema_version": 1, "facts": [{
            "fact_key": "preference.explanation_style",
            "text": "不应晚到写回",
            "evidence_input_seq": event["input_seq"],
            "evidence_quote": "多举例子",
        }]}

    gateway = _BlockingGateway(_build)
    identity = _identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)
    await _complete(service, identity, "late", "以后请多举例子")
    assert await asyncio.to_thread(gateway.started.wait, 2)
    await service.clear_subject(identity)
    gateway.release.set()
    await asyncio.sleep(0.05)

    assert await service.list_facts(identity) == []
    await service.aclose()
