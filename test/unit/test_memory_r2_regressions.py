from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

import cooper_bot.commands.commands as commands
from cooper_bot.core import config
from cooper_bot.modules.memory.jobs import EXTRACTION_DEFER_SECONDS
from cooper_bot.modules.memory.models import CapturedInput, MemoryIdentity
from cooper_bot.modules.memory.policy import scope_for
from cooper_bot.modules.memory.service import MemoryService
from cooper_bot.modules.memory.store import MemoryStore

GROUP_ID = 30303
PROJECT_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------- helpers


def _private_identity(actor: int = 20202) -> MemoryIdentity:
    return MemoryIdentity(10101, actor, "private", None, "public")


def _group_identity(actor: int, *, personal_admin: bool = False) -> MemoryIdentity:
    return MemoryIdentity(10101, actor, "group", GROUP_ID, "public", personal_admin)


def _allow_group(monkeypatch) -> None:
    monkeypatch.setattr(config, "AI_MEMORY_GROUP_ALLOWLIST", {GROUP_ID})


def _enable_auto(monkeypatch, *, budget: int = 20) -> None:
    monkeypatch.setattr(config, "AI_MEMORY_AUTO_EXTRACT_ENABLED", True)
    monkeypatch.setattr(config, "AI_MEMORY_AUTO_EXTRACT_MIN_EVENTS", 1)
    monkeypatch.setattr(config, "AI_MEMORY_AUTO_EXTRACT_DAILY_BUDGET", budget)
    monkeypatch.setattr(config, "AI_MEMORY_SUMMARY_ENABLED", False)


class _ExtractGateway:
    """只实现抽取作业需要的 chat()：记录 payload，返回构建器给的 JSON。"""

    def __init__(self, builder) -> None:
        self.builder = builder
        self.payloads: list[dict] = []

    def chat(self, messages, **_kwargs) -> str:
        payload = json.loads(messages[1]["content"])
        self.payloads.append(payload)
        return json.dumps(self.builder(payload), ensure_ascii=False)


class _FailingExtractGateway:
    def __init__(self, error: type[BaseException] = TimeoutError) -> None:
        self.error = error
        self.payloads: list[dict] = []

    def chat(self, messages, **_kwargs) -> str:
        self.payloads.append(json.loads(messages[1]["content"]))
        raise self.error("provider unavailable")


class _EmbedGateway:
    """鸭子类型 embedding 网关：确定性假向量，不联网。"""

    def __init__(self, *, model: str = "embed-model") -> None:
        self.model = model
        self.calls: list[str] = []

    def provider(self, name: str):
        assert name == "embedding"
        return SimpleNamespace(model=self.model, ready=True)

    def embed(self, text: str, *, timeout=None) -> list[float]:
        self.calls.append(str(text))
        return [1.0, 0.0, 0.0]


class _BlockingFirstEmbedGateway(_EmbedGateway):
    """第一次 embed 会阻塞，便于在 batch 中途制造 /memory off。"""

    def __init__(self) -> None:
        super().__init__()
        self.first_started = threading.Event()
        self.release = threading.Event()

    def embed(self, text: str, *, timeout=None) -> list[float]:
        self.calls.append(str(text))
        if len(self.calls) == 1:
            self.first_started.set()
            if not self.release.wait(5):
                raise TimeoutError("test release timed out")
        return [1.0, 0.0, 0.0]


async def _complete(service: MemoryService, identity: MemoryIdentity, event_id: str, text: str) -> int:
    turn = await service.turn(identity, CapturedInput(event_id, text))
    assert turn is not None
    async with turn:
        await turn.record_generated("answer")
        await turn.finish_delivery(True)
    return int(turn.snapshot.current_input_seq)


async def _cursor(service: MemoryService, identity: MemoryIdentity) -> int:
    scope_id = scope_for(identity)

    def _value() -> int:
        row = service.store._c().execute(
            "SELECT extracted_through_input_seq FROM memory_members WHERE scope_id=? AND actor_user_id=?",
            (scope_id, identity.actor_user_id),
        ).fetchone()
        return int(row[0]) if row else 0

    return int(await service.store._call(_value))


async def _wait_for_cursor(service: MemoryService, identity: MemoryIdentity, target: int, *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await _cursor(service, identity) >= target:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("extraction cursor did not advance")


async def _wait_for_job_attempt(service: MemoryService, kind: str, *, attempts: int = 1, timeout: float = 5.0) -> None:
    """等 worker 真正领过这个作业：attempts 增长（普通重试）或 last_error_code='deferred'（被推迟）都算跑过。"""

    def _rows() -> list[tuple[int, str]]:
        return [(int(row[0]), str(row[1] or "")) for row in service.store._c().execute("SELECT attempts,last_error_code FROM memory_jobs WHERE kind=?", (kind,)).fetchall()]

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any(value >= attempts or code == "deferred" for value, code in await service.store._call(_rows)):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{kind} job was never attempted")


async def _embeddings_count(service: MemoryService, scope_id: str) -> int:
    def _count() -> int:
        return int(service.store._c().execute("SELECT COUNT(*) FROM memory_embeddings WHERE scope_id=?", (scope_id,)).fetchone()[0])

    return int(await service.store._call(_count))


async def _wait_for_embeddings(service: MemoryService, scope_id: str, count: int, *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await _embeddings_count(service, scope_id) >= count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("embeddings were not written")


async def _wait_for_idle(service: MemoryService, *, timeout: float = 5.0) -> None:
    """等队列跑空；超时也返回，供"没有作业"这类否定断言使用。"""

    def _pending() -> int:
        return int(service.store._c().execute("SELECT COUNT(*) FROM memory_jobs WHERE state IN ('queued','running')").fetchone()[0])

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not await service.store._call(_pending):
            return
        await asyncio.sleep(0.02)


async def _seed_auto_fact(service: MemoryService, scope_id: str, subject: str, fact_key: str, text: str) -> str:
    """直接落一条 auto_extracted 事实，模拟抽取作业已经写过的结果。"""
    fact_id = uuid.uuid4().hex

    def _insert() -> None:
        conn = service.store._c()
        now = time.time()
        with conn:
            conn.execute(
                "INSERT INTO memory_facts(fact_id,scope_id,subject_id,fact_key,revision,status,text,source_kind,valid_from,created_at,updated_at) VALUES(?,?,?,?,1,'active',?,'auto_extracted',?,?,?)",
                (fact_id, scope_id, subject, fact_key, text, now, now, now),
            )

    await service.store._call(_insert)
    return fact_id


# ------------------------------------------------- T-R2-01 / T-R2-02 turn 安全


@pytest.mark.asyncio
async def test_r2_t01_pre_generate_exception_does_not_leave_pending_input(tmp_path) -> None:
    db_path = tmp_path / "memory.sqlite3"
    service = MemoryService(enabled=True, db_path=db_path)
    identity = _private_identity()
    await service.set_enabled(identity, True)

    turn = await service.turn(identity, CapturedInput("chat-1", "生成前就失败的问题"))
    assert turn is not None
    with pytest.raises(RuntimeError, match="kimi unavailable"):
        async with turn:
            raise RuntimeError("kimi unavailable")

    with sqlite3.connect(db_path) as conn:
        state = conn.execute("SELECT state FROM memory_events WHERE source_event_id='chat-1'").fetchone()[0]
    assert state == "failed"
    await service.aclose()


class _FailingAI:
    def __init__(self, memory: MemoryService) -> None:
        self.memory = memory
        self.bot_nick = "Cooper_bot"
        self.chat_ready = True
        self.computer_ready = True
        self.fallback_error_reply = "fallback"
        self.run_count = 0

    async def chat_with_context(self, *_args, **_kwargs) -> str:
        self.run_count += 1
        raise RuntimeError("kimi unavailable")

    async def chat(self, *_args, **_kwargs) -> str:
        raise AssertionError("stateless fallback must not run")


@pytest.mark.asyncio
async def test_r2_t01_kimi_path_failure_leaves_no_pending_input(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "memory.sqlite3"
    service = MemoryService(enabled=True, db_path=db_path)
    await service.set_enabled(_private_identity(), True)
    ai = _FailingAI(service)
    replies: list[str] = []

    async def _reply(_api, _ctx, text, _logsvc, force_private_user_id=None) -> bool:
        replies.append(str(text))
        return True

    monkeypatch.setattr(commands, "reply", _reply)
    monkeypatch.setattr(commands, "_ai_chat_allows_kimi_computer", lambda *_args: False)

    handled = await commands._handle_ai_chat_trigger(
        SimpleNamespace(),
        SimpleNamespace(scene="private_friend", user_id=20202, group_id=None, nickname="tester", card="tester", level=1),
        {"self_id": 10101},
        "相同正文",
        SimpleNamespace(log=SimpleNamespace(warning=lambda _message: None)),
        ai,
        forced_ai_input="相同正文",
        message_id="kimi-fail-1",
    )

    assert handled is True
    assert ai.run_count == 1
    assert replies == ["fallback"]
    with sqlite3.connect(db_path) as conn:
        state = conn.execute("SELECT state FROM memory_events WHERE source_event_id='kimi-fail-1'").fetchone()[0]
    assert state == "failed"
    await service.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("failing", ["snapshot_rows", "search_facts"])
async def test_r2_t02_enter_failure_releases_scope_lock(tmp_path, monkeypatch, failing: str) -> None:
    db_path = tmp_path / "memory.sqlite3"
    service = MemoryService(enabled=True, db_path=db_path)
    identity = _private_identity()
    await service.set_enabled(identity, True)
    scope_id = scope_for(identity)

    calls = {"count": 0}
    if failing == "snapshot_rows":
        original = service.store.snapshot_rows
    else:
        original = service.search_facts

    async def _boom(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("enter failed")
        return await original(*args, **kwargs)

    if failing == "snapshot_rows":
        monkeypatch.setattr(service.store, "snapshot_rows", _boom)
    else:
        monkeypatch.setattr(service, "search_facts", _boom)

    first = await service.turn(identity, CapturedInput("enter-1", "准备阶段失败"))
    assert first is not None
    with pytest.raises(RuntimeError, match="enter failed"):
        await first.__aenter__()

    # 进入失败必须释放 scope lock，否则同一 scope 的后续消息会永久卡住。
    assert service._locks[scope_id].locked() is False

    second = await service.turn(identity, CapturedInput("enter-2", "后续消息"))
    assert second is not None
    async with asyncio.timeout(5):
        await second.__aenter__()
    await second.__aexit__(None, None, None)
    assert service._locks[scope_id].locked() is False

    with sqlite3.connect(db_path) as conn:
        states = dict(conn.execute("SELECT source_event_id,state FROM memory_events WHERE role='user'"))
    assert states["enter-1"] == "failed"
    assert states["enter-2"] == "pending"
    await service.aclose()


@pytest.mark.asyncio
async def test_r2_t03_failed_input_does_not_block_stable_cutoff(tmp_path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    await store.start()
    await store.ensure_scope("scope", bot_id=1, profile="public", kind="private", target_id=2, owner_user_id=2)
    await store.set_scope_policy("scope", True)

    first, _ = await store.append_input_once("scope", 2, "first", "问题一", "", "", "direct_chat")
    await store.insert_assistant_once("scope", first["seq"], "回答一")
    await store.finish_turn("scope", first["seq"], True)
    broken, _ = await store.append_input_once("scope", 2, "broken", "生成前失败", "", "", "direct_chat")
    assert await store.fail_pending_input("scope", broken["seq"]) is True
    third, _ = await store.append_input_once("scope", 2, "third", "问题三", "", "", "direct_chat")
    await store.insert_assistant_once("scope", third["seq"], "回答三")
    await store.finish_turn("scope", third["seq"], True)

    candidate = await store.summary_candidate("scope", 1)
    assert candidate is not None
    # 失败的输入不再是 pending：它后面的稳定输入仍要进入窗口。
    assert candidate["target_input_seq"] == third["seq"]

    context = await store.summary_job_context({**candidate, "kind": "summary"})
    assert context is not None
    assert [row["seq"] for row in context["events"] if row["role"] == "user"] == [first["seq"], third["seq"]]
    await store.close()


@pytest.mark.asyncio
async def test_r2_t04_exception_after_generate_finishes_turn_as_failed(tmp_path) -> None:
    db_path = tmp_path / "memory.sqlite3"
    service = MemoryService(enabled=True, db_path=db_path)
    identity = _private_identity()
    await service.set_enabled(identity, True)

    turn = await service.turn(identity, CapturedInput("chat-1", "生成后失败"))
    assert turn is not None
    with pytest.raises(RuntimeError, match="reply failed"):
        async with turn:
            await turn.record_generated("已经生成的回答")
            raise RuntimeError("reply failed")

    with sqlite3.connect(db_path) as conn:
        states = {row[0]: row[1] for row in conn.execute("SELECT role,state FROM memory_events")}
    assert states == {"user": "failed", "assistant": "failed"}
    await service.aclose()


# ------------------------------------------------ T-R2-05 / T-R2-06 opt-out 隐私


@pytest.mark.asyncio
async def test_r2_t05_opt_out_member_is_hidden_from_history(tmp_path, monkeypatch) -> None:
    _allow_group(monkeypatch)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    admin, member_a, member_b = _group_identity(90909, personal_admin=True), _group_identity(20202), _group_identity(20203)
    await service.set_group_enabled(admin, True, "all")

    await _complete(service, member_a, "a-1", "A-SECRET")
    await _complete(service, member_b, "b-1", "B-公开内容")
    visible = [str(row["own_text"]) for row in await service.history(member_b) if row["role"] == "user"]
    assert visible == ["A-SECRET", "B-公开内容"]

    await service.set_member_enabled(member_a, False)
    visible = [str(row["own_text"]) for row in await service.history(member_b) if row["role"] == "user"]
    assert "A-SECRET" not in visible
    assert visible == ["B-公开内容"]
    await service.aclose()


@pytest.mark.asyncio
async def test_r2_t06_opt_out_member_facts_are_not_sent_to_embedding(tmp_path, monkeypatch) -> None:
    _allow_group(monkeypatch)
    gateway = _EmbedGateway()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    admin, member_a, member_b = _group_identity(90909, personal_admin=True), _group_identity(20202), _group_identity(20203)
    await service.set_group_enabled(admin, True, "all")
    scope_id = scope_for(member_a)
    assert scope_id == scope_for(member_b)
    await service.store.save_explicit_fact(scope_id, "user:20202", "A-私密事实")
    await service.store.save_explicit_fact(scope_id, "user:20203", "B-普通事实")

    await service.set_member_enabled(member_a, False)
    await service._maybe_schedule_embedding(scope_id)
    await _wait_for_embeddings(service, scope_id, 1)
    await _wait_for_idle(service)

    assert gateway.calls == ["B-普通事实"]
    await service.aclose()


@pytest.mark.asyncio
async def test_r2_t07_mid_batch_opt_out_stops_remaining_sends(tmp_path, monkeypatch) -> None:
    _allow_group(monkeypatch)
    gateway = _BlockingFirstEmbedGateway()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    admin, member_a, member_b = _group_identity(90909, personal_admin=True), _group_identity(20202), _group_identity(20203)
    await service.set_group_enabled(admin, True, "all")
    scope_id = scope_for(member_a)
    # fact_id 决定回填顺序：a1 -> a2 -> b1
    await service.store.save_explicit_fact(scope_id, "user:20202", "a1-内容", fact_id="a1")
    await service.store.save_explicit_fact(scope_id, "user:20202", "a2-内容", fact_id="a2")
    await service.store.save_explicit_fact(scope_id, "user:20203", "b1-内容", fact_id="b1")

    await service._maybe_schedule_embedding(scope_id)
    assert await asyncio.to_thread(gateway.first_started.wait, 5)
    await service.set_member_enabled(member_a, False)
    gateway.release.set()
    # 退出会让在跑的作业变成 cancelled，_wait_for_idle 无法反映在途处理器，因此等 b1 真正发出。
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and "b1-内容" not in gateway.calls:
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.05)

    # a1 已发出无法撤回；a2 必须停下，其他成员照常继续。
    assert gateway.calls == ["a1-内容", "b1-内容"]
    await service.aclose()


@pytest.mark.asyncio
async def test_r2_embed_backfill_picks_up_facts_written_while_job_runs(tmp_path) -> None:
    """回填作业运行期间新写入的事实也必须被重新排队，否则会永久缺向量。"""
    gateway = _BlockingFirstEmbedGateway()
    identity = _private_identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)
    scope_id = scope_for(identity)

    await service.remember_explicit(identity, "第一条事实")
    assert await asyncio.to_thread(gateway.first_started.wait, 5)
    # 作业还停在第一条上：这次触发必须能让作业再次排队。
    await service.remember_explicit(identity, "第二条事实")
    gateway.release.set()

    await _wait_for_embeddings(service, scope_id, 2)
    assert sorted(gateway.calls) == sorted(["第一条事实", "第二条事实"])
    await service.aclose()


# --------------------------------------- T-R2-08 / T-R2-09 派生事实不被误删

@pytest.mark.asyncio
async def test_r2_t08_opt_out_keeps_other_members_auto_facts(tmp_path, monkeypatch) -> None:
    _allow_group(monkeypatch)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    admin, member_a, member_b = _group_identity(90909, personal_admin=True), _group_identity(20202), _group_identity(20203)
    await service.set_group_enabled(admin, True, "all")
    scope_id = scope_for(member_a)
    await _seed_auto_fact(service, scope_id, "user:20202", "profile.school", "A 的学校")
    await _seed_auto_fact(service, scope_id, "user:20203", "profile.school", "B 的学校")

    await service.set_member_enabled(member_a, False)

    def _auto_count() -> int:
        return int(service.store._c().execute("SELECT COUNT(*) FROM memory_facts WHERE scope_id=? AND source_kind='auto_extracted'", (scope_id,)).fetchone()[0])

    # A 退出只停止"使用"，不得删掉同 scope 其他成员的 auto facts。
    assert await service.store._call(_auto_count) == 2
    assert [row["text"] for row in await service.store.list_facts(scope_id, ("user:20203",))] == ["B 的学校"]
    assert await service.store.list_facts(scope_id, ("user:20202",)) == []
    assert [row["text"] for row in await service.search_facts(member_b, "学校")] == ["B 的学校"]
    assert await service.search_facts(member_a, "学校") == []
    await service.aclose()


# ------------------------------------------------ T-R2-10 / T-R2-11 / T-R2-12 cursor


@pytest.mark.asyncio
async def test_r2_t10_budget_exhausted_does_not_advance_cursor(tmp_path, monkeypatch) -> None:
    _enable_auto(monkeypatch, budget=0)
    gateway = _ExtractGateway(lambda _payload: {"schema_version": 1, "facts": []})
    identity = _private_identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)
    await _complete(service, identity, "budget", "今天讨论一个临时问题")
    await _wait_for_job_attempt(service, "extract")

    assert await _cursor(service, identity) == 0
    assert gateway.payloads == []
    assert await service.list_facts(identity) == []

    # 恢复预算后同一批事件仍能抽取，说明 cursor 没有被跳过。
    monkeypatch.setattr(config, "AI_MEMORY_AUTO_EXTRACT_DAILY_BUDGET", 20)
    target = await _complete(service, identity, "budget-retry", "这是后续的正式问题")
    await _wait_for_cursor(service, identity, target)
    assert gateway.payloads
    await service.aclose()


@pytest.mark.asyncio
async def test_r2_t11_provider_failure_does_not_advance_cursor(tmp_path, monkeypatch) -> None:
    _enable_auto(monkeypatch)
    gateway = _FailingExtractGateway(TimeoutError)
    identity = _private_identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)
    await _complete(service, identity, "timeout", "我很喜欢简明回答")
    await _wait_for_job_attempt(service, "extract")

    assert gateway.payloads
    assert await _cursor(service, identity) == 0
    assert await service.list_facts(identity) == []
    await service.aclose()


@pytest.mark.asyncio
async def test_r2_t12_genuine_empty_result_advances_cursor(tmp_path, monkeypatch) -> None:
    _enable_auto(monkeypatch)
    gateway = _ExtractGateway(lambda _payload: {"schema_version": 1, "facts": []})
    identity = _private_identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)
    target = await _complete(service, identity, "empty", "今天讨论一个临时问题")
    await _wait_for_cursor(service, identity, target)

    assert len(gateway.payloads) == 1
    assert await service.list_facts(identity) == []
    await service.aclose()


# ---------------------------------------- T-R2-13 ~ T-R2-16 explicit/auto 优先级


def _style_candidate(payload, *, key: str = "preference.answer_style", prefix: str = "自动候选：") -> dict:
    event = payload["events"][0]
    return {
        "schema_version": 1,
        "facts": [{
            "fact_key": key,
            "text": f"{prefix}{event['own_text']}",
            "evidence_input_seq": event["input_seq"],
            "evidence_quote": event["own_text"],
        }],
    }


@pytest.mark.asyncio
async def test_r2_t13_freeform_explicit_does_not_block_unrelated_auto_key(tmp_path, monkeypatch) -> None:
    _enable_auto(monkeypatch)
    gateway = _ExtractGateway(lambda payload: _style_candidate(payload, key="profile.location", prefix="用户常驻："))
    identity = _private_identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)
    explicit = await service.remember_explicit(identity, "回答尽量简短")
    target = await _complete(service, identity, "location", "我在南京上学")
    await _wait_for_cursor(service, identity, target)

    facts = await service.list_facts(identity)
    assert explicit["fact_key"].startswith("explicit.")
    assert {(row["source_kind"], row["text"]) for row in facts} == {
        ("explicit_memory", "回答尽量简短"),
        ("auto_extracted", "用户常驻：我在南京上学"),
    }
    await service.aclose()


@pytest.mark.asyncio
async def test_r2_t14_explicit_same_key_wins_over_auto(tmp_path, monkeypatch) -> None:
    _enable_auto(monkeypatch)
    gateway = _ExtractGateway(_style_candidate)
    identity = _private_identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)

    first_target = await _complete(service, identity, "style-1", "我希望答案风格简洁")
    await _wait_for_cursor(service, identity, first_target)
    auto = (await service.list_facts(identity))[0]
    assert (auto["source_kind"], auto["fact_key"]) == ("auto_extracted", "preference.answer_style")

    explicit = await service.remember_explicit(identity, "答案风格改用要点式", replace_id=auto["fact_id"])
    assert explicit["fact_key"] == auto["fact_key"]

    second_target = await _complete(service, identity, "style-2", "再补充一条风格说明")
    await _wait_for_cursor(service, identity, second_target)

    active = await service.list_facts(identity)
    assert [(row["source_kind"], row["text"]) for row in active] == [("explicit_memory", "答案风格改用要点式")]

    def _status_counts() -> dict:
        rows = service.store._c().execute(
            "SELECT status,COUNT(*) FROM memory_facts WHERE scope_id=? AND subject_id=? AND fact_key=? GROUP BY status",
            (scope_for(identity), "user:20202", "preference.answer_style"),
        ).fetchall()
        return {str(row[0]): int(row[1]) for row in rows}

    # 同一 key 上最多一个 active revision，auto 已被 explicit supersede。
    assert await service.store._call(_status_counts) == {"superseded": 1, "active": 1}
    await service.aclose()


@pytest.mark.asyncio
async def test_r2_t15_explicit_supersedes_previous_auto(tmp_path, monkeypatch) -> None:
    _enable_auto(monkeypatch)
    gateway = _ExtractGateway(lambda payload: _style_candidate(payload))
    identity = _private_identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)

    target = await _complete(service, identity, "auto-first", "我喜欢表格形式的回答")
    await _wait_for_cursor(service, identity, target)
    auto = (await service.list_facts(identity))[0]
    assert auto["source_kind"] == "auto_extracted"

    await service.remember_explicit(identity, "以后一律用纯文本回答", replace_id=auto["fact_id"])
    facts = await service.list_facts(identity)
    assert [(row["source_kind"], row["text"], row["revision"]) for row in facts] == [("explicit_memory", "以后一律用纯文本回答", 2)]
    await service.aclose()


# ------------------------------------------------ T-R2-17 / T-R2-18 / T-R2-19 缓存


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["clear", "forget", "new", "member_off", "scope_off"])
async def test_r2_t17_privacy_actions_invalidate_query_vector_cache(tmp_path, action: str) -> None:
    gateway = _EmbedGateway()
    identity = _private_identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)
    fact = await service.remember_explicit(identity, "数学作业需要周三交")
    scope_id = scope_for(identity)
    await _wait_for_embeddings(service, scope_id, 1)

    worker = service.worker
    service.worker = None  # 关掉后台回填，避免它的 provider 调用混进断言
    try:
        await service.search_facts(identity, "作业")
        assert (scope_id, "作业") in service._query_vectors
        if action == "clear":
            await service.clear_subject(identity)
        elif action == "forget":
            assert await service.forget_fact(identity, fact["fact_id"]) is True
        elif action == "new":
            await service.rotate_conversation(identity)
        elif action == "member_off":
            await service.set_member_enabled(identity, False)
        else:
            await service.set_enabled(identity, False)
        assert (scope_id, "作业") not in service._query_vectors
    finally:
        service.worker = worker
    await service.aclose()


@pytest.mark.asyncio
async def test_r2_t19_epoch_rotate_does_not_reuse_stale_query_vector(tmp_path) -> None:
    gateway = _EmbedGateway()
    identity = _private_identity()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)
    scope_id = scope_for(identity)
    await service.remember_explicit(identity, "数学作业需要周三交")
    await _wait_for_embeddings(service, scope_id, 1)

    worker = service.worker
    service.worker = None
    try:
        gateway.calls.clear()
        await service.search_facts(identity, "作业")
        await service.search_facts(identity, "作业")
        assert len(gateway.calls) == 1  # 同一轮查询命中缓存

        await service.rotate_conversation(identity)
        assert (scope_id, "作业") not in service._query_vectors
        await service.search_facts(identity, "作业")
        # 旧缓存已被清掉：必须重新向 provider 取查询向量。
        assert len(gateway.calls) == 2
    finally:
        service.worker = worker
    await service.aclose()


# ------------------------------------------------------------- T-R2-20 默认开关


def test_r2_t20_memory_switches_default_and_docs_agree() -> None:
    assert config.AI_MEMORY_ENABLED is True
    assert config.AI_MEMORY_EMBEDDING_ENABLED is True
    document = (PROJECT_ROOT / "docs" / "memory.md").read_text(encoding="utf-8")
    assert "AI_MEMORY_ENABLED=true" in document
    assert "AI_MEMORY_EMBEDDING_ENABLED=true" in document


@pytest.mark.skipif(not sys.executable, reason="needs an interpreter for the isolated env check")
def test_r2_t20_env_can_disable_both_switches_in_isolated_process() -> None:
    script = (
        "import json;"
        "from cooper_bot.core import config;"
        "print(json.dumps([config.AI_MEMORY_ENABLED, config.AI_MEMORY_EMBEDDING_ENABLED]))"
    )
    env = {**os.environ, "AI_MEMORY_ENABLED": "false", "AI_MEMORY_EMBEDDING_ENABLED": "false"}
    result = subprocess.run([sys.executable, "-c", script], cwd=str(PROJECT_ROOT), env=env, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == [False, False]


# =====================================================================
# 第三轮（R3）简化修复的回归测试
# =====================================================================


async def _wait_for_job_deferred(service: MemoryService, kind: str, *, timeout: float = 5.0) -> None:
    def _codes() -> list[str]:
        return [str(row[0] or "") for row in service.store._c().execute("SELECT last_error_code FROM memory_jobs WHERE kind=?", (kind,)).fetchall()]

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if "deferred" in await service.store._call(_codes):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{kind} job was never deferred")


@pytest.mark.asyncio
async def test_r3_embedding_not_blocked_by_opt_out_fact(tmp_path, monkeypatch) -> None:
    """队头是已退出成员的事实时，后面的正常事实仍要拿到向量。"""
    _allow_group(monkeypatch)
    monkeypatch.setattr(config, "AI_MEMORY_EMBEDDING_BATCH_SIZE", 1)
    gateway = _EmbedGateway()
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    admin, member_a, member_b = _group_identity(90909, personal_admin=True), _group_identity(20202), _group_identity(20203)
    await service.set_group_enabled(admin, True, "all")
    scope_id = scope_for(member_a)
    # a1 的 fact_id 排在 b1 前面：只按 "缺向量" 取批会先取到 a1，被跳过之后 b1 永远轮不到
    await service.store.save_explicit_fact(scope_id, "user:20202", "A-私密事实", fact_id="a1")
    await service.store.save_explicit_fact(scope_id, "user:20203", "B-普通事实", fact_id="b1")
    await service.set_member_enabled(member_a, False)

    await service._maybe_schedule_embedding(scope_id)
    await _wait_for_embeddings(service, scope_id, 1)
    await _wait_for_idle(service)

    assert "A-私密事实" not in gateway.calls
    assert gateway.calls == ["B-普通事实"]
    await service.aclose()


@pytest.mark.asyncio
async def test_r3_deferred_extraction_stays_retryable(tmp_path, monkeypatch) -> None:
    """预算耗尽时抽取被推迟：cursor 不动，作业保持可再执行而不是 failed。"""
    _enable_auto(monkeypatch, budget=0)
    gateway = _ExtractGateway(lambda _payload: {"schema_version": 1, "facts": []})
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    identity = _private_identity()
    await service.set_enabled(identity, True)
    await _complete(service, identity, "defer", "今天讨论一个临时问题")
    await _wait_for_job_deferred(service, "extract")

    def _job() -> dict:
        return dict(service.store._c().execute("SELECT * FROM memory_jobs WHERE kind='extract'").fetchone())

    job = await service.store._call(_job)
    assert job["state"] == "queued"
    assert int(job["attempts"]) == 0
    assert float(job["not_before"]) >= time.time() + EXTRACTION_DEFER_SECONDS - 5
    assert gateway.payloads == []
    assert await _cursor(service, identity) == 0
    await service.aclose()


@pytest.mark.asyncio
async def test_r3_prompt_puts_explicit_first_and_states_conflict_rule(tmp_path) -> None:
    """memory prompt 里 explicit 事实在前，并带固定的冲突口径。"""
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    identity = _private_identity()
    await service.set_enabled(identity, True)
    scope_id = scope_for(identity)
    await service.remember_explicit(identity, "以后回答简洁")
    # auto 事实写入更晚（updated_at 更新），所以库里顺序正好相反
    await _seed_auto_fact(service, scope_id, "user:20202", "preference.answer_style", "喜欢详细回答")

    stored = await service.list_facts(identity)
    assert [row["source_kind"] for row in stored] == ["auto_extracted", "explicit_memory"]

    context = await service.prompt_context(identity, "")
    assert [fact["source_kind"] for fact in context["facts"]] == ["explicit_memory", "auto_extracted"]
    assert context["facts"][0]["text"] == "以后回答简洁"
    assert context["conflict_rule"] == "如果显式记忆与自动提取的记忆冲突，以用户显式要求记住的内容为准。"
    await service.aclose()
