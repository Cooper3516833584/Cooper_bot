from __future__ import annotations

import time

import pytest

from cooper_bot.modules.memory.models import CapturedInput, MemoryIdentity
from cooper_bot.modules.memory.policy import scope_for
from cooper_bot.modules.memory.service import MemoryService

GROUP_ID = 30303


def _private_identity(actor: int = 20202) -> MemoryIdentity:
    return MemoryIdentity(10101, actor, "private", None, "public")


def _group_identity(actor: int, *, personal_admin: bool = False) -> MemoryIdentity:
    return MemoryIdentity(10101, actor, "group", GROUP_ID, "public", personal_admin)


async def _seed_derived_rows(service: MemoryService, scope_id: str, conversation_id: str, *, fact_id: str | None, job_key: str) -> None:
    """Insert summary/embedding rows a worker could have produced before the deletion."""

    def _seed() -> None:
        conn = service.store._c()
        with conn:
            conn.execute(
                "INSERT INTO memory_summaries(scope_id,conversation_id,epoch,version,through_input_seq,summary_json,updated_at) VALUES(?,?,?,?,?,?,?)",
                (scope_id, conversation_id, 0, 1, 1, '{"topics":["旧摘要"]}', time.time()),
            )
            if fact_id:
                conn.execute(
                    "INSERT INTO memory_embeddings(fact_id,fingerprint,scope_id,fact_revision,dimension,vector,updated_at) VALUES(?,?,?,?,?,?,?)",
                    (fact_id, "fp-1", scope_id, 1, 2, b"\x00\x00", time.time()),
                )

    await service.store._call(_seed)
    await service.store.enqueue_job(scope_id, conversation_id, 0, "summary", 1, {}, job_key)


@pytest.mark.asyncio
async def test_forget_removes_derived_rows_without_leaving_residue(tmp_path) -> None:
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    identity = _private_identity()
    await service.set_enabled(identity, True)
    fact = await service.remember_explicit(identity, "长期偏好：回答要简短")

    turn = await service.turn(identity, CapturedInput("chat-1", "第一个问题"))
    assert turn is not None
    async with turn:
        await turn.record_generated("引用长期偏好的旧回复")
        await turn.finish_delivery(True)
    scope_before = await service.store.scope(turn.scope_id)
    assert scope_before is not None
    await _seed_derived_rows(service, turn.scope_id, turn.snapshot.conversation_id, fact_id=fact["fact_id"], job_key="summary:forget")

    assert await service.forget_fact(identity, fact["fact_id"]) is True

    def _state() -> dict:
        conn = service.store._c()
        pattern = "%长期偏好：回答要简短%"
        return {
            "epoch": int(conn.execute("SELECT epoch FROM memory_scopes WHERE scope_id=?", (turn.scope_id,)).fetchone()[0]),
            "facts": conn.execute("SELECT COUNT(*) FROM memory_facts WHERE scope_id=?", (turn.scope_id,)).fetchone()[0],
            "assistant_events": conn.execute("SELECT COUNT(*) FROM memory_events WHERE scope_id=? AND role='assistant'", (turn.scope_id,)).fetchone()[0],
            "user_events": conn.execute("SELECT COUNT(*) FROM memory_events WHERE scope_id=? AND role='user'", (turn.scope_id,)).fetchone()[0],
            "fact_text_hits": conn.execute(
                "SELECT COUNT(*) FROM memory_events WHERE scope_id=? AND (own_text LIKE ? OR quoted_text LIKE ? OR visual_text LIKE ?)",
                (turn.scope_id, pattern, pattern, pattern),
            ).fetchone()[0],
            "summaries": conn.execute("SELECT COUNT(*) FROM memory_summaries WHERE scope_id=?", (turn.scope_id,)).fetchone()[0],
            "embeddings": conn.execute("SELECT COUNT(*) FROM memory_embeddings WHERE scope_id=?", (turn.scope_id,)).fetchone()[0],
            "job": conn.execute("SELECT state FROM memory_jobs WHERE dedupe_key='summary:forget'").fetchone()[0],
            "markers": [tuple(row) for row in conn.execute("SELECT subject_id,fact_key,blocked_through_input_seq FROM memory_forget_markers WHERE scope_id=?", (turn.scope_id,)).fetchall()],
        }

    state = await service.store._call(_state)
    assert state["epoch"] == int(scope_before["epoch"]) + 1
    assert state["facts"] == 0
    # Mixed bot replies are purged so a forgotten fact cannot come back through them,
    # while the user's own raw conversation stays until /memory clear or new.
    assert state["assistant_events"] == 0
    assert state["user_events"] == 1
    assert state["fact_text_hits"] == 0
    assert state["summaries"] == 0
    assert state["embeddings"] == 0
    assert state["job"] == "cancelled"
    # Explicit facts carry no input sequence evidence, so the watermark stays at 0.
    assert state["markers"] == [(f"user:{identity.actor_user_id}", fact["fact_key"], 0)]
    assert await service.search_facts(identity, "简短") == []
    await service.aclose()


@pytest.mark.asyncio
async def test_forget_then_explicit_restatement_is_visible_again(tmp_path) -> None:
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    identity = _private_identity()
    await service.set_enabled(identity, True)
    first = await service.remember_explicit(identity, "偏好：回答要简短")
    assert await service.forget_fact(identity, first["fact_id"]) is True
    assert await service.list_facts(identity) == []

    second = await service.remember_explicit(identity, "偏好：回答要简短")
    assert second["fact_id"] != first["fact_id"]
    assert [row["text"] for row in await service.list_facts(identity)] == ["偏好：回答要简短"]
    await service.aclose()


@pytest.mark.asyncio
async def test_clear_subject_purges_actor_rows_and_stales_inflight_turn(tmp_path) -> None:
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    identity = _private_identity()
    await service.set_enabled(identity, True)
    fact = await service.remember_explicit(identity, "待清除的显式事实")
    finished = await service.turn(identity, CapturedInput("done-1", "已完成的问题"))
    assert finished is not None
    async with finished:
        await finished.record_generated("已完成的回复")
        await finished.finish_delivery(True)
    await _seed_derived_rows(service, finished.scope_id, finished.snapshot.conversation_id, fact_id=fact["fact_id"], job_key="summary:clear")
    inflight = await service.turn(identity, CapturedInput("inflight-1", "清除时仍在途的问题"))
    assert inflight is not None
    scope_before = await service.store.scope(inflight.scope_id)
    assert scope_before is not None

    await service.clear_subject(identity)

    def _state() -> dict:
        conn = service.store._c()
        return {
            "epoch": int(conn.execute("SELECT epoch FROM memory_scopes WHERE scope_id=?", (inflight.scope_id,)).fetchone()[0]),
            "facts": conn.execute("SELECT COUNT(*) FROM memory_facts WHERE scope_id=?", (inflight.scope_id,)).fetchone()[0],
            "events": conn.execute("SELECT COUNT(*) FROM memory_events WHERE scope_id=?", (inflight.scope_id,)).fetchone()[0],
            "summaries": conn.execute("SELECT COUNT(*) FROM memory_summaries WHERE scope_id=?", (inflight.scope_id,)).fetchone()[0],
            "markers": conn.execute("SELECT COUNT(*) FROM memory_forget_markers WHERE scope_id=?", (inflight.scope_id,)).fetchone()[0],
            "job": conn.execute("SELECT state FROM memory_jobs WHERE dedupe_key='summary:clear'").fetchone()[0],
        }

    state = await service.store._call(_state)
    assert state["epoch"] == int(scope_before["epoch"]) + 1
    assert state["facts"] == 0 and state["events"] == 0 and state["summaries"] == 0 and state["markers"] == 0
    assert state["job"] == "cancelled"

    async with inflight:
        assert inflight.is_stale_or_disabled is True
        assert inflight.snapshot.recent_events == ()
        assert await inflight.acquire_send_permit() is False
    assert await service.search_facts(identity, "") == []
    await service.aclose()


@pytest.mark.asyncio
async def test_group_clear_rotates_conversation_and_wipes_scope_rows(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_GROUP_ALLOWLIST", {GROUP_ID})
    admin = _group_identity(90909, personal_admin=True)
    member = _group_identity(20202)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    await service.set_group_enabled(admin, True, "all")
    group_fact = await service.remember_explicit(admin, "群约定：周三交作业", subject_id=f"group:{GROUP_ID}")
    turn = await service.turn(member, CapturedInput("group-1", "群成员问题"))
    assert turn is not None
    async with turn:
        await turn.record_generated("群成员回复")
        await turn.finish_delivery(True)
    scope_id = scope_for(member)
    assert scope_id == turn.scope_id
    await _seed_derived_rows(service, scope_id, turn.snapshot.conversation_id, fact_id=group_fact["fact_id"], job_key="summary:group-clear")
    scope_before = await service.store.scope(scope_id)
    assert scope_before is not None

    with pytest.raises(ValueError, match="trusted administrator"):
        await service.clear_group(member)

    await service.clear_group(admin)

    def _state() -> dict:
        conn = service.store._c()
        return {
            "scope": dict(conn.execute("SELECT * FROM memory_scopes WHERE scope_id=?", (scope_id,)).fetchone()),
            "events": conn.execute("SELECT COUNT(*) FROM memory_events WHERE scope_id=?", (scope_id,)).fetchone()[0],
            "facts": conn.execute("SELECT COUNT(*) FROM memory_facts WHERE scope_id=?", (scope_id,)).fetchone()[0],
            "summaries": conn.execute("SELECT COUNT(*) FROM memory_summaries WHERE scope_id=?", (scope_id,)).fetchone()[0],
            "embeddings": conn.execute("SELECT COUNT(*) FROM memory_embeddings WHERE scope_id=?", (scope_id,)).fetchone()[0],
            "jobs": conn.execute("SELECT COUNT(*) FROM memory_jobs WHERE scope_id=?", (scope_id,)).fetchone()[0],
            "markers": conn.execute("SELECT COUNT(*) FROM memory_forget_markers WHERE scope_id=?", (scope_id,)).fetchone()[0],
        }

    state = await service.store._call(_state)
    assert state["scope"]["active_conversation_id"] != scope_before["active_conversation_id"]
    assert int(state["scope"]["epoch"]) == int(scope_before["epoch"]) + 1
    assert state["events"] == 0
    assert state["facts"] == 0
    assert state["summaries"] == 0
    assert state["embeddings"] == 0
    assert state["jobs"] == 0
    assert state["markers"] == 0
    assert await service.history(member) == []
    await service.aclose()
