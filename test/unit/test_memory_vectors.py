from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest

from cooper_bot.modules.memory import vectors
from cooper_bot.modules.memory.models import MemoryIdentity
from cooper_bot.modules.memory.policy import scope_for
from cooper_bot.modules.memory.service import MemoryService

EMBED_MODEL = "embed-model"
DIMENSION = 3
# 每个维度对应一组关键词：测试用确定性的"假向量空间"，不联网。
AXES = (("作业", "deadline", "截止"), ("课程", "class", "课表"), ("偏好", "preference", "喜欢"))


def _fake_vector(text: str, *, dimension: int = DIMENSION) -> list[float]:
    value = str(text or "").casefold()
    vector = [0.0] * dimension
    for index, keywords in enumerate(AXES[:dimension]):
        if any(keyword in value for keyword in keywords):
            vector[index] = 1.0
    if not any(vector):
        vector[dimension - 1] = 0.01
    return vector


class _Provider:
    def __init__(self, model: str, ready: bool) -> None:
        self.model, self.ready = model, ready


class _EmbedGateway:
    """鸭子类型网关：只提供 provider() + embed()，模拟 embedding 配置。"""

    def __init__(self, *, model: str = EMBED_MODEL, ready: bool = True, dimension: int = DIMENSION, returns_none: bool = False, raises: bool = False) -> None:
        self.model, self.ready = model, ready
        self.dimension, self.returns_none, self.raises = dimension, returns_none, raises
        self.calls: list[str] = []

    def provider(self, name: str) -> _Provider:
        assert name == "embedding"
        return _Provider(self.model, self.ready)

    def embed(self, text: str, *, timeout=None):
        self.calls.append(str(text))
        if self.raises:
            raise RuntimeError("embedding provider failed")
        if self.returns_none:
            return None
        return _fake_vector(text, dimension=self.dimension)


def _identity(actor: int = 20202) -> MemoryIdentity:
    return MemoryIdentity(10101, actor, "private", None, "public")


def _log() -> SimpleNamespace:
    return SimpleNamespace(warning=lambda _message: None)


async def _service(tmp_path, gateway=None, name: str = "memory.sqlite3") -> MemoryService:
    return MemoryService(_log(), enabled=True, db_path=tmp_path / name, gateway=gateway)


async def _embeddings_count(service: MemoryService, scope_id: str, fingerprint: str | None = None) -> int:
    """向量行数（必须经 store._call，SQLite 连接只在 DB 线程里用）。"""

    def _count() -> int:
        conn = service.store._c()
        if fingerprint is None:
            return int(conn.execute("SELECT COUNT(*) FROM memory_embeddings WHERE scope_id=?", (scope_id,)).fetchone()[0])
        return int(conn.execute("SELECT COUNT(*) FROM memory_embeddings WHERE scope_id=? AND fingerprint=?", (scope_id, fingerprint)).fetchone()[0])

    return int(await service.store._call(_count))


async def _wait_for_embeddings(service: MemoryService, scope_id: str, count: int, fingerprint: str | None = None) -> None:
    for _ in range(300):
        if await _embeddings_count(service, scope_id, fingerprint) >= count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("embeddings were not written")


async def _wait_for_idle(service: MemoryService) -> None:
    """等 worker 把队列跑空；没有作业时立即返回，用于否定断言。"""

    def _pending() -> int:
        return int(service.store._c().execute("SELECT COUNT(*) FROM memory_jobs WHERE state IN ('queued','running')").fetchone()[0])

    for _ in range(60):
        if not await service.store._call(_pending):
            return
        await asyncio.sleep(0.02)


async def _seed(tmp_path, gateway, texts: list[str]) -> tuple[MemoryService, MemoryIdentity, str]:
    service = await _service(tmp_path, gateway)
    identity = _identity()
    await service.set_enabled(identity, True)
    for text in texts:
        await service.remember_explicit(identity, text)
    return service, identity, scope_for(identity)


@pytest.mark.asyncio
async def test_explicit_fact_is_embedded_by_worker(tmp_path) -> None:
    gateway = _EmbedGateway()
    service, identity, scope_id = await _seed(tmp_path, gateway, ["数学作业需要周三交"])
    fact = (await service.list_facts(identity))[0]
    await _wait_for_embeddings(service, scope_id, 1)

    def _row() -> dict:
        return dict(service.store._c().execute("SELECT * FROM memory_embeddings WHERE scope_id=?", (scope_id,)).fetchone())

    row = await service.store._call(_row)
    assert row["fact_id"] == fact["fact_id"]
    assert row["fingerprint"] == vectors.fingerprint_for(EMBED_MODEL)
    assert int(row["fact_revision"]) == int(fact["revision"])
    assert int(row["dimension"]) == DIMENSION
    assert len(vectors.decode(row["vector"])) == DIMENSION
    assert gateway.calls == ["数学作业需要周三交"]
    await service.aclose()


@pytest.mark.asyncio
async def test_vector_recall_finds_fact_without_lexical_overlap(tmp_path, monkeypatch) -> None:
    gateway = _EmbedGateway()
    service, identity, scope_id = await _seed(tmp_path, gateway, ["数学作业需要周三交", "喜欢简短回答"])
    await _wait_for_embeddings(service, scope_id, 2)

    # 关闭向量后 "deadline" 与两条中文事实没有任何词法重叠。
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_EMBEDDING_ENABLED", False)
    assert await service.search_facts(identity, "deadline") == []

    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_EMBEDDING_ENABLED", True)
    hits = await service.search_facts(identity, "deadline")
    assert [row["text"] for row in hits] == ["数学作业需要周三交"]
    await service.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["returns_none", "raises", "not_ready"])
async def test_provider_failure_degrades_to_lexical(tmp_path, mode: str) -> None:
    gateway = _EmbedGateway(
        returns_none=mode == "returns_none",
        raises=mode == "raises",
        ready=mode != "not_ready",
    )
    service, identity, scope_id = await _seed(tmp_path, gateway, ["数学作业需要周三交"])
    await _wait_for_idle(service)

    assert await _embeddings_count(service, scope_id) == 0
    assert [row["text"] for row in await service.search_facts(identity, "作业")] == ["数学作业需要周三交"]
    status = await service.status(identity)
    # provider 配置了就绪，但调用失败时不应写入任何向量；未配置则直接视为未就绪。
    assert status["embedding_ready"] is (mode != "not_ready")
    assert status["embedded_facts"] == 0 and status["total_facts"] == 1
    await service.aclose()


@pytest.mark.asyncio
async def test_disabled_switch_never_calls_provider_and_keeps_lexical_order(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_EMBEDDING_ENABLED", False)
    gateway = _EmbedGateway()
    service, identity, scope_id = await _seed(tmp_path, gateway, ["作业与课程安排", "作业"])
    await _wait_for_idle(service)

    assert gateway.calls == []
    assert await _embeddings_count(service, scope_id) == 0
    # 词法同分时按 updated_at 倒序：后写入的 "作业" 在前。
    assert [row["text"] for row in await service.search_facts(identity, "作业")] == ["作业", "作业与课程安排"]
    await service.aclose()


@pytest.mark.asyncio
async def test_daily_budget_exhaustion_skips_provider(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_EMBEDDING_DAILY_BUDGET", 0)
    gateway = _EmbedGateway()
    service, identity, scope_id = await _seed(tmp_path, gateway, ["数学作业需要周三交"])
    await _wait_for_idle(service)

    assert gateway.calls == []
    assert await _embeddings_count(service, scope_id) == 0
    assert [row["text"] for row in await service.search_facts(identity, "作业")] == ["数学作业需要周三交"]
    await service.aclose()


@pytest.mark.asyncio
async def test_backfill_continues_in_batches(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_EMBEDDING_BATCH_SIZE", 1)
    gateway = _EmbedGateway()
    service, _identity_value, scope_id = await _seed(tmp_path, gateway, ["作业一", "作业二", "作业三"])
    await _wait_for_embeddings(service, scope_id, 3)

    assert len(gateway.calls) == 3
    assert await _embeddings_count(service, scope_id) == 3
    await service.aclose()


@pytest.mark.asyncio
async def test_new_fact_is_embedded_after_an_earlier_pass_completed(tmp_path) -> None:
    gateway = _EmbedGateway()
    service, identity, scope_id = await _seed(tmp_path, gateway, ["数学作业需要周三交"])
    await _wait_for_embeddings(service, scope_id, 1)

    await service.remember_explicit(identity, "课表在周四更新")
    await _wait_for_embeddings(service, scope_id, 2)
    assert await _embeddings_count(service, scope_id) == 2
    await service.aclose()


@pytest.mark.asyncio
async def test_model_change_backfills_and_drops_old_fingerprint(tmp_path) -> None:
    old = _EmbedGateway()
    service, identity, scope_id = await _seed(tmp_path, old, ["数学作业需要周三交"])
    await _wait_for_embeddings(service, scope_id, 1)
    assert await _embeddings_count(service, scope_id, vectors.fingerprint_for(EMBED_MODEL)) == 1
    await service.aclose()

    new = _EmbedGateway(model="embed-model-v2")
    restored = await _service(tmp_path, new)
    await restored.start()
    await _wait_for_embeddings(restored, scope_id, 1, vectors.fingerprint_for("embed-model-v2"))
    assert await _embeddings_count(restored, scope_id, vectors.fingerprint_for("embed-model-v2")) == 1
    assert await _embeddings_count(restored, scope_id, vectors.fingerprint_for(EMBED_MODEL)) == 0
    assert [row["text"] for row in await restored.search_facts(identity, "deadline")] == ["数学作业需要周三交"]
    await restored.aclose()


@pytest.mark.asyncio
async def test_superseded_fact_vector_is_pruned(tmp_path) -> None:
    gateway = _EmbedGateway()
    service, identity, scope_id = await _seed(tmp_path, gateway, ["数学作业需要周三交"])
    await _wait_for_embeddings(service, scope_id, 1)
    first = (await service.list_facts(identity))[0]

    await service.remember_explicit(identity, "数学作业改到周四交", replace_id=first["fact_id"])
    await _wait_for_idle(service)

    def _rows() -> list[tuple]:
        return [tuple(row) for row in service.store._c().execute("SELECT fact_id,fact_revision FROM memory_embeddings WHERE scope_id=?", (scope_id,)).fetchall()]

    rows = await service.store._call(_rows)
    assert len(rows) == 1
    assert rows[0][0] != first["fact_id"]
    assert await service.store.prune_stale_embeddings(scope_id) == 0
    await service.aclose()


@pytest.mark.asyncio
async def test_dimension_mismatch_row_is_ignored(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_EMBEDDING_ENABLED", False)
    gateway = _EmbedGateway()
    service, identity, scope_id = await _seed(tmp_path, gateway, ["数学作业需要周三交", "喜欢简短回答"])
    facts = {row["text"]: row for row in await service.list_facts(identity)}
    good, broken = facts["数学作业需要周三交"], facts["喜欢简短回答"]

    def _forge() -> None:
        conn = service.store._c()
        fingerprint = vectors.fingerprint_for(EMBED_MODEL)
        with conn:
            conn.execute(
                "INSERT INTO memory_embeddings(fact_id,fingerprint,scope_id,fact_revision,dimension,vector,updated_at) VALUES(?,?,?,?,?,?,?)",
                (good["fact_id"], fingerprint, scope_id, int(good["revision"]), DIMENSION, vectors.encode(_fake_vector("作业")), 1.0),
            )
            conn.execute(
                "INSERT INTO memory_embeddings(fact_id,fingerprint,scope_id,fact_revision,dimension,vector,updated_at) VALUES(?,?,?,?,?,?,?)",
                (broken["fact_id"], fingerprint, scope_id, int(broken["revision"]), DIMENSION + 2, vectors.encode(_fake_vector("deadline")), 1.0),
            )

    await service.store._call(_forge)
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_EMBEDDING_ENABLED", True)

    hits = await service.search_facts(identity, "deadline")
    assert [row["text"] for row in hits] == ["数学作业需要周三交"]
    await service.aclose()


@pytest.mark.asyncio
async def test_query_embedding_is_cached_across_calls(tmp_path) -> None:
    gateway = _EmbedGateway()
    service, identity, scope_id = await _seed(tmp_path, gateway, ["数学作业需要周三交"])
    await _wait_for_embeddings(service, scope_id, 1)
    gateway.calls.clear()

    await service.search_facts(identity, "作业")
    await service.search_facts(identity, "作业")
    assert len(gateway.calls) == 1

    await service.search_facts(identity, "课程")
    assert len(gateway.calls) == 2
    await service.aclose()


@pytest.mark.asyncio
async def test_prompt_context_uses_fused_order(tmp_path, monkeypatch) -> None:
    gateway = _EmbedGateway()
    service, identity, scope_id = await _seed(tmp_path, gateway, ["作业", "作业与课程安排"])
    await _wait_for_embeddings(service, scope_id, 2)

    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_EMBEDDING_ENABLED", False)
    assert [row["text"] for row in await service.search_facts(identity, "作业")] == ["作业与课程安排", "作业"]

    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_EMBEDDING_ENABLED", True)
    context = await service.prompt_context(identity, "作业")
    assert [fact["text"] for fact in context["facts"]] == ["作业", "作业与课程安排"]
    await service.aclose()


@pytest.mark.asyncio
async def test_worker_does_not_block_when_provider_is_slow(tmp_path) -> None:
    """embed 走 to_thread：慢 provider 不能卡住记忆 DB 的其他调用。"""
    service = await _service(tmp_path, _EmbedGateway())
    identity = _identity()
    await service.set_enabled(identity, True)
    started = asyncio.Event()
    release = threading.Event()

    class _SlowGateway(_EmbedGateway):
        def embed(self, text: str, *, timeout=None):
            self.calls.append(str(text))
            started.set()
            release.wait(5)
            return _fake_vector(text)

    service.gateway = _SlowGateway()
    await service.remember_explicit(identity, "数学作业需要周三交")
    for _ in range(200):
        if started.is_set():
            break
        await asyncio.sleep(0.01)
    assert started.is_set()

    # provider 仍被阻塞时，记忆 DB 的读写必须照常返回。
    assert await service.store.scope(scope_for(identity)) is not None
    status = await asyncio.wait_for(service.status(identity), timeout=2)
    assert status["scope_enabled"] is True
    release.set()
    await service.aclose()


def test_vector_helpers_are_dimension_safe() -> None:
    assert vectors.cosine([1.0, 0.0], [1.0, 0.0, 0.0]) == -1.0
    assert vectors.cosine([0.0, 0.0], [1.0, 0.0]) == -1.0
    assert vectors.normalize([0.0, 0.0]) is None
    assert round(vectors.cosine([3.0, 0.0], [5.0, 0.0]), 6) == 1.0
    assert vectors.fingerprint_for("BAAI/bge-m3") != vectors.fingerprint_for("other-model")
    assert len(vectors.decode(vectors.encode([1.5, -2.5, 3.0]))) == 3
