"""高流量群聊记忆配置的回归测试（默认关闭、权限等级、200 条批次、重启恢复）。"""

from __future__ import annotations

import asyncio
import json
import time

import pytest

import cooper_bot.commands.commands as commands
from cooper_bot.commands.memory_commands import handle_memory_command
from cooper_bot.core import config
from cooper_bot.modules.memory.models import CapturedInput, MemoryIdentity
from cooper_bot.modules.memory.policy import scope_for
from cooper_bot.modules.memory.service import MemoryService
from cooper_bot.modules.memory.store import MemoryStore

GROUP_ID = 30303
ADMIN = 90909


def _private(actor: int = 20202, *, operator: bool = False) -> MemoryIdentity:
    return MemoryIdentity(10101, actor, "private", None, "public", False, operator)


def _group(actor: int, *, operator: bool = False) -> MemoryIdentity:
    return MemoryIdentity(10101, actor, "group", GROUP_ID, "public", False, operator)


class _Gateway:
    """只实现事实抽取需要的 chat()。"""

    def __init__(self) -> None:
        self.payloads: list[dict] = []

    def chat(self, messages, **_kwargs) -> str:
        payload = json.loads(messages[1]["content"])
        self.payloads.append(payload)
        return json.dumps({"schema_version": 1, "facts": []}, ensure_ascii=False)


async def _seed_group_scope(db_path, *, events: int = 0, actor: int = 20202) -> str:
    """直接建库建 scope（可选塞入若干条已完成的普通群消息），模拟上一个进程留下的数据。"""
    store = MemoryStore(db_path)
    await store.start()
    scope_id = scope_for(_group(actor))
    await store.ensure_scope(scope_id, bot_id=10101, profile="public", kind="group", target_id=GROUP_ID, owner_user_id=None)
    await store.set_scope_policy(scope_id, True, "all")
    for index in range(events):
        await store.append_input_once(scope_id, actor, f"seed-{actor}-{index}", f"群消息-{index}", "", "", "passive_chat")
    await store.close()
    return scope_id


async def _wait_for_cursor(service: MemoryService, scope_id: str, actor: int, target: int, *, timeout: float = 8.0) -> None:
    def _value() -> int:
        row = service.store._c().execute(
            "SELECT extracted_through_input_seq FROM memory_members WHERE scope_id=? AND actor_user_id=?", (scope_id, actor)
        ).fetchone()
        return int(row[0]) if row else 0

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await service.store._call(_value) >= target:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("extraction cursor did not advance")


async def _apply_candidate(store: MemoryStore, candidate: dict, actor: int) -> bool:
    job = {**candidate, "payload_json": json.dumps({"actor_user_id": actor, "cursor": candidate["cursor"]})}
    return await store.apply_extracted_facts(job, actor, candidate["cursor"], candidate["target_input_seq"], [])


# ------------------------------------------------- Test 2：权限等级

@pytest.mark.asyncio
async def test_private_memory_on_requires_level_two(tmp_path) -> None:
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")

    level1 = _private(20202)
    reply, _ = await handle_memory_command(service, level1, "/memory on")
    assert "需要权限等级 2 或以上" in reply
    scope = await service.store.scope(scope_for(level1))
    assert scope is None or not bool(scope["enabled"])

    level2 = _private(20203, operator=True)
    reply, _ = await handle_memory_command(service, level2, "/memory on")
    assert "已开启当前作用域的记忆" in reply
    scope = await service.store.scope(scope_for(level2))
    assert scope is not None and bool(scope["enabled"])
    await service.aclose()


# ------------------------------------------------- Test 3：remember 不能绕过 on

@pytest.mark.asyncio
async def test_remember_does_not_open_a_closed_session(tmp_path) -> None:
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    identity = _private(20203, operator=True)

    reply, _ = await handle_memory_command(service, identity, "/memory remember 不该自动开启")
    assert "请先使用 /memory on" in reply
    scope = await service.store.scope(scope_for(identity))
    assert scope is None or not bool(scope["enabled"])
    assert await service.list_facts(identity) == []
    await service.aclose()


# ------------------------------------------------- Test 4：200 条计数跨重启不清零

@pytest.mark.asyncio
async def test_member_counter_survives_restart(tmp_path) -> None:
    db_path = tmp_path / "memory.sqlite3"
    store = MemoryStore(db_path)
    await store.start()
    scope_id = scope_for(_group(20202))
    await store.ensure_scope(scope_id, bot_id=10101, profile="public", kind="group", target_id=GROUP_ID, owner_user_id=None)
    await store.set_scope_policy(scope_id, True, "all")
    for index in range(173):
        await store.append_input_once(scope_id, 20202, f"before-restart-{index}", f"消息-{index}", "", "", "passive_chat")
    assert await store.extraction_candidate(scope_id, 20202, 200) is None  # 173 < 200
    await store.close()

    # 新进程：再补 27 条就够一批，计数来自 SQLite 而不是内存
    restored = MemoryStore(db_path)
    await restored.start()
    for index in range(27):
        await restored.append_input_once(scope_id, 20202, f"after-restart-{index}", f"新消息-{index}", "", "", "passive_chat")
    candidate = await restored.extraction_candidate(scope_id, 20202, 200)
    assert candidate is not None
    assert candidate["cursor"] == 0
    assert candidate["target_input_seq"] == 200
    await restored.close()


# ------------------------------------------------- Test 5：够一批但没 enqueue 就崩溃

@pytest.mark.asyncio
async def test_restart_reconciles_full_batch_without_new_message(tmp_path) -> None:
    db_path = tmp_path / "memory.sqlite3"
    scope_id = await _seed_group_scope(db_path, events=200)  # 第 200 条已入库，但没有 job

    gateway = _Gateway()
    service = MemoryService(enabled=True, db_path=db_path, gateway=gateway)
    await service.start()  # 启动补排：不需要等第 201 条消息
    await _wait_for_cursor(service, scope_id, 20202, 200)

    assert gateway.payloads and len(gateway.payloads[0]["events"]) == 200

    def _jobs() -> int:
        return int(service.store._c().execute("SELECT COUNT(*) FROM memory_jobs WHERE kind='extract'").fetchone()[0])

    assert await service.store._call(_jobs) >= 1
    await service.aclose()


# ------------------------------------------------- Test 6：running 作业重启立即恢复

@pytest.mark.asyncio
async def test_running_job_is_requeued_on_startup(tmp_path) -> None:
    db_path = tmp_path / "memory.sqlite3"
    store = MemoryStore(db_path)
    await store.start()
    await store.enqueue_job("scope-any", "", 0, "summary", 1, {}, "summary:restart")
    claimed = await store.claim_job(lease_seconds=120)
    assert claimed is not None and claimed["state"] == "running" and int(claimed["attempts"]) == 1
    await store.close()

    service = MemoryService(enabled=True, db_path=db_path)
    await service.start()

    def _row() -> dict:
        return dict(service.store._c().execute("SELECT * FROM memory_jobs WHERE dedupe_key='summary:restart'").fetchone())

    row = await service.store._call(_row)
    assert row["state"] == "queued"
    assert row["lease_until"] is None
    assert int(row["attempts"]) == 0  # 重启恢复不消耗普通重试次数
    assert float(row["not_before"]) <= time.time()
    await service.aclose()


# ------------------------------------------------- Test 8：一次最多 200 条

@pytest.mark.asyncio
async def test_extraction_batches_are_capped_at_two_hundred(tmp_path) -> None:
    db_path = tmp_path / "memory.sqlite3"
    scope_id = await _seed_group_scope(db_path, events=450)

    store = MemoryStore(db_path)
    await store.start()
    first = await store.extraction_candidate(scope_id, 20202, 200)
    assert first is not None and first["target_input_seq"] == 200
    assert await _apply_candidate(store, first, 20202) is True

    second = await store.extraction_candidate(scope_id, 20202, 200)
    assert second is not None and second["cursor"] == 200 and second["target_input_seq"] == 400
    assert await _apply_candidate(store, second, 20202) is True

    assert await store.extraction_candidate(scope_id, 20202, 200) is None  # 只剩 50 条，等下批
    await store.close()


@pytest.mark.asyncio
async def test_extraction_payload_limits_each_event_text(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(config, "AI_MEMORY_AUTO_EXTRACT_MIN_EVENTS", 1)
    gateway = _Gateway()
    identity = _private(20202, operator=True)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3", gateway=gateway)
    await service.set_enabled(identity, True)

    turn = await service.turn(identity, CapturedInput("long-1", "长" * 1000))
    assert turn is not None
    async with turn:
        await turn.record_generated("回答")
        await turn.finish_delivery(True)

    for _ in range(300):
        if gateway.payloads:
            break
        await asyncio.sleep(0.01)
    assert gateway.payloads
    assert len(gateway.payloads[0]["events"][0]["own_text"]) == 200
    await service.aclose()


# ------------------------------------------------- Test 7：普通图片不为记忆解析

def test_passive_images_are_not_resolved_for_memory() -> None:
    # 默认不把普通（未触发 Bot）消息的图片放进旧上下文等后续补解析
    assert config.VISION_CAPTURE_CONTEXT_IMAGES is False
    # 只有已经解析好的 ready 描述才会进 memory；未解析的 slot 不产生 visual_text
    assert commands._memory_visual_text([{"status": "unresolved", "description": "不应保存"}]) == ""
    assert commands._memory_visual_text([{"status": "ready", "description": "图片里是一只猫"}]) == "图片里是一只猫"


# ------------------------------------------------- failed extraction job 可恢复


class _FlakyGateway:
    """先抛异常、之后恢复：模拟 provider 连续普通异常后重试耗尽。"""

    def __init__(self) -> None:
        self.fail = True
        self.payloads: list[dict] = []

    def chat(self, messages, **_kwargs) -> str:
        payload = json.loads(messages[1]["content"])
        if self.fail:
            raise TimeoutError("provider down")
        self.payloads.append(payload)
        return json.dumps({"schema_version": 1, "facts": []}, ensure_ascii=False)


async def _job(store: MemoryStore, dedupe_key: str) -> dict | None:
    def _row():
        row = store._c().execute("SELECT * FROM memory_jobs WHERE dedupe_key=?", (dedupe_key,)).fetchone()
        return dict(row) if row else None

    return await store._call(_row)


async def _job_count(store: MemoryStore, dedupe_key: str) -> int:
    def _count() -> int:
        return int(store._c().execute("SELECT COUNT(*) FROM memory_jobs WHERE dedupe_key=?", (dedupe_key,)).fetchone()[0])

    return int(await store._call(_count))


@pytest.mark.asyncio
async def test_failed_extraction_job_can_be_enqueued_again(tmp_path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    await store.start()
    key = "extract:scope:20202:0:1000:1200"
    await store.enqueue_job("scope", "conv", 0, "extract", 1200, {"actor_user_id": 20202, "cursor": 1000}, key, revive_failed=True)

    claimed = await store.claim_job()
    assert claimed is not None
    await store.finish_job(claimed["job_id"], False, "TimeoutError", max_attempts=1)
    assert (await _job(store, key))["state"] == "failed"

    # 同一批再次被发现：failed 的旧作业被重新激活，不必等 cursor 变化
    await store.enqueue_job("scope", "conv", 0, "extract", 1200, {"actor_user_id": 20202, "cursor": 1000}, key, revive_failed=True)
    row = await _job(store, key)
    assert row["state"] == "queued"
    assert int(row["attempts"]) == 0
    assert row["lease_until"] is None
    assert row["last_error_code"] is None
    assert float(row["not_before"]) <= time.time()
    assert await _job_count(store, key) == 1
    await store.close()


@pytest.mark.asyncio
async def test_enqueue_does_not_duplicate_queued_or_running_jobs(tmp_path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    await store.start()
    key = "extract:scope:20202:0:1000:1200"
    await store.enqueue_job("scope", "conv", 0, "extract", 1200, {}, key, revive_failed=True)
    await store.enqueue_job("scope", "conv", 0, "extract", 1200, {}, key, revive_failed=True)
    row = await _job(store, key)
    assert row["state"] == "queued" and int(row["attempts"]) == 0
    assert await _job_count(store, key) == 1

    claimed = await store.claim_job(lease_seconds=120)
    assert claimed is not None and claimed["dedupe_key"] == key and claimed["state"] == "running"
    await store.enqueue_job("scope", "conv", 0, "extract", 1200, {}, key, revive_failed=True)
    row = await _job(store, key)
    assert row["state"] == "running"
    assert int(row["attempts"]) == 1
    assert row["lease_until"] is not None
    assert await _job_count(store, key) == 1
    await store.close()


@pytest.mark.asyncio
async def test_succeeded_job_is_never_revived(tmp_path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    await store.start()
    key = "extract:scope:20202:0:1000:1200"
    await store.enqueue_job("scope", "conv", 0, "extract", 1200, {}, key, revive_failed=True)
    claimed = await store.claim_job()
    assert claimed is not None
    await store.finish_job(claimed["job_id"], True)
    assert (await _job(store, key))["state"] == "succeeded"

    await store.enqueue_job("scope", "conv", 0, "extract", 1200, {}, key, revive_failed=True)
    assert (await _job(store, key))["state"] == "succeeded"
    assert await _job_count(store, key) == 1

    # 默认不开复活：其它 kind（例如 summary）的 failed 保持原样
    other = "summary:scope:conv:0:1"
    await store.enqueue_job("scope", "conv", 0, "summary", 1, {}, other)
    claimed = await store.claim_job()
    assert claimed is not None and claimed["dedupe_key"] == other
    await store.finish_job(claimed["job_id"], False, "ValueError", max_attempts=1)
    await store.enqueue_job("scope", "conv", 0, "summary", 1, {}, other)
    assert (await _job(store, other))["state"] == "failed"
    await store.close()


@pytest.mark.asyncio
async def test_service_revives_a_failed_batch_when_it_is_rediscovered(tmp_path) -> None:
    """provider 恢复后，只要系统再次发现这批 candidate（新消息或重启），failed 作业就会重新排上。"""
    db_path = tmp_path / "memory.sqlite3"
    scope_id = await _seed_group_scope(db_path, events=200)
    gateway = _FlakyGateway()
    service = MemoryService(enabled=True, db_path=db_path, gateway=gateway)
    await service.start()  # 启动补排会建 job，但因为 provider 异常而失败

    def _force_failed() -> None:
        with service.store._c():
            service.store._c().execute("UPDATE memory_jobs SET state='failed',attempts=3,last_error_code='TimeoutError' WHERE kind='extract'")

    await service.store._call(_force_failed)

    def _state() -> str:
        row = service.store._c().execute("SELECT state FROM memory_jobs WHERE kind='extract'").fetchone()
        return str(row[0]) if row else ""

    assert await service.store._call(_state) == "failed"

    gateway.fail = False  # provider 恢复；此后只有“重新发现这批”才会让它再跑起来
    await service._maybe_schedule_extraction(scope_id, 20202)
    await _wait_for_cursor(service, scope_id, 20202, 200)
    assert gateway.payloads
    await service.aclose()


# ------------------------------------------------- 所有 /memory 指令限 2 级

@pytest.mark.asyncio
async def test_all_memory_commands_need_level_two(tmp_path) -> None:
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    level1 = _private(20202)
    gated = (
        "/memory",
        "/memory help",
        "/memory status",
        "/memory on",
        "/memory off",
        "/memory remember 内容",
        "/memory list",
        "/memory search 内容",
        "/memory forget abcdef",
        "/memory history",
        "/memory new",
        "/memory clear",
        "/memory group on all",
        "/memory group off",
        "/memory group clear",
        "/memory unknown",
    )
    for command in gated:
        reply, _ = await handle_memory_command(service, level1, command)
        assert "需要权限等级 2 或以上" in reply, command

    # 等级 1 连 off 都用不了：既不会被创建也不会被开启
    scope_id = scope_for(level1)
    scope = await service.store.scope(scope_id)
    assert scope is None or not bool(scope["enabled"])

    # 等级 2 可以正常用
    operator = _private(20203, operator=True)
    reply, _ = await handle_memory_command(service, operator, "/memory status")
    assert "总开关：开启" in reply
    reply, _ = await handle_memory_command(service, operator, "/memory on")
    assert "已开启当前作用域的记忆" in reply
    await service.aclose()


@pytest.mark.asyncio
async def test_memory_on_off_toggles_the_current_chat(tmp_path) -> None:
    """on / off 就是当前会话（私聊或群聊）的记忆开关。"""
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    private = _private(20202, operator=True)
    group = _group(20203, operator=True)

    reply, _ = await handle_memory_command(service, private, "/memory on")
    assert "已开启当前作用域的记忆" in reply
    assert bool((await service.store.scope(scope_for(private)))["enabled"])
    reply, _ = await handle_memory_command(service, private, "/memory off")
    assert "已关闭当前作用域的记忆" in reply
    assert not bool((await service.store.scope(scope_for(private)))["enabled"])

    reply, _ = await handle_memory_command(service, group, "/memory on")
    assert "已开启当前作用域的记忆" in reply
    scope = await service.store.scope(scope_for(group))
    assert scope is not None and bool(scope["enabled"]) and scope["capture_mode"] == "all"
    reply, _ = await handle_memory_command(service, group, "/memory off")
    assert "已关闭当前作用域的记忆" in reply
    assert not bool((await service.store.scope(scope_for(group)))["enabled"])
    await service.aclose()
