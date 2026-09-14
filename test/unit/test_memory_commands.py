from __future__ import annotations

import time
import re

import pytest

from cooper_bot.commands.memory_commands import handle_memory_command
from cooper_bot.modules.memory.models import CapturedInput, MemoryIdentity
from cooper_bot.modules.memory.policy import scope_for
from cooper_bot.modules.memory.service import MemoryService


GROUP_ID = 30303


def _group_identity(actor: int, *, group_id: int = GROUP_ID, admin_profile: bool = False, personal_admin: bool = False) -> MemoryIdentity:
    return MemoryIdentity(10101, actor, "group", group_id, "admin" if admin_profile else "public", personal_admin)


@pytest.mark.asyncio
async def test_regular_group_member_on_does_not_enable_scope(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_GROUP_ALLOWLIST", {GROUP_ID})
    identity = _group_identity(20202)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")

    reply, _ = await handle_memory_command(service, identity, "/memory on")
    scope = await service.store.scope(scope_for(identity))
    assert "未由管理员开启" in reply
    assert scope is not None and not bool(scope["enabled"])
    await service.aclose()


@pytest.mark.asyncio
async def test_regular_group_member_remember_cannot_enable_disabled_scope(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_GROUP_ALLOWLIST", {GROUP_ID})
    identity = _group_identity(20202)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")

    reply, _ = await handle_memory_command(service, identity, "/memory remember 不应写入")
    scope = await service.store.scope(scope_for(identity))
    assert "未由管理员开启" in reply
    assert scope is not None and not bool(scope["enabled"])
    assert await service.list_facts(identity) == []
    await service.aclose()


@pytest.mark.asyncio
async def test_admin_group_memory_still_requires_deployment_allowlist(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_GROUP_ALLOWLIST", set())
    identity = _group_identity(90909, admin_profile=True, personal_admin=True)
    db_path = tmp_path / "memory.sqlite3"
    service = MemoryService(enabled=True, db_path=db_path)

    with pytest.raises(ValueError, match="not available"):
        await service.remember_explicit(identity, "管理员内容")
    assert not db_path.exists()


@pytest.mark.asyncio
async def test_member_off_invalidates_mixed_data_and_filters_old_events(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_GROUP_ALLOWLIST", {GROUP_ID})
    admin = _group_identity(90909, personal_admin=True)
    member = _group_identity(20202)
    other = _group_identity(20203)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    await service.set_group_enabled(admin, True, "all")

    old_turn = await service.turn(member, CapturedInput("old-event", "成员隐私内容"))
    assert old_turn is not None
    async with old_turn:
        await old_turn.record_generated("引用了成员隐私的混合回复")
        await old_turn.finish_delivery(True)
    scope_before = await service.store.scope(old_turn.scope_id)
    assert scope_before is not None
    await service.store.enqueue_job(old_turn.scope_id, old_turn.snapshot.conversation_id, old_turn.snapshot.epoch, "summary", old_turn.snapshot.current_input_seq, {}, "summary:test")

    def _seed_summary() -> None:
        conn = service.store._c()
        with conn:
            conn.execute(
                "INSERT INTO memory_summaries(scope_id,conversation_id,epoch,version,through_input_seq,summary_json,updated_at) VALUES(?,?,?,?,?,?,?)",
                (old_turn.scope_id, old_turn.snapshot.conversation_id, old_turn.snapshot.epoch, 1, old_turn.snapshot.current_input_seq, "{}", time.time()),
            )

    await service.store._call(_seed_summary)
    await service.set_member_enabled(member, False)
    scope_after = await service.store.scope(old_turn.scope_id)
    assert scope_after is not None
    assert int(scope_after["epoch"]) == int(scope_before["epoch"]) + 1

    next_turn = await service.turn(other, CapturedInput("next-event", "其他成员问题"))
    assert next_turn is not None
    async with next_turn:
        assert all(row["actor_user_id"] != member.actor_user_id for row in next_turn.snapshot.recent_events)
        assert all("混合回复" not in row["own_text"] for row in next_turn.snapshot.recent_events)

    def _derived_state() -> tuple[int, int, str]:
        conn = service.store._c()
        assistant_count = conn.execute("SELECT COUNT(*) FROM memory_events WHERE scope_id=? AND role='assistant'", (old_turn.scope_id,)).fetchone()[0]
        summary_count = conn.execute("SELECT COUNT(*) FROM memory_summaries WHERE scope_id=?", (old_turn.scope_id,)).fetchone()[0]
        job_state = conn.execute("SELECT state FROM memory_jobs WHERE dedupe_key='summary:test'").fetchone()[0]
        return assistant_count, summary_count, job_state

    assert await service.store._call(_derived_state) == (0, 0, "cancelled")
    await service.aclose()


@pytest.mark.asyncio
async def test_regular_group_member_cannot_rotate_public_group_conversation(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_GROUP_ALLOWLIST", {GROUP_ID})
    admin = _group_identity(90909, personal_admin=True)
    member = _group_identity(20202)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    await service.set_group_enabled(admin, True)
    before = await service.store.scope(scope_for(member))

    reply, _ = await handle_memory_command(service, member, "/memory new")
    after = await service.store.scope(scope_for(member))
    assert "仅可信个人管理员" in reply
    assert before is not None and after is not None
    assert after["active_conversation_id"] == before["active_conversation_id"]
    assert after["epoch"] == before["epoch"]
    await service.aclose()


@pytest.mark.asyncio
async def test_group_policy_and_group_remember_require_admin_path(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_GROUP_ALLOWLIST", {GROUP_ID})
    admin = _group_identity(90909, personal_admin=True)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")

    with pytest.raises(ValueError, match="trusted administrator"):
        await service.set_enabled(admin, True)
    reply, _ = await handle_memory_command(service, admin, "/memory group remember 群约定")
    scope = await service.store.scope(scope_for(admin))
    assert "未由管理员开启" in reply
    assert scope is not None and not bool(scope["enabled"])

    reply, _ = await handle_memory_command(service, admin, "/memory group on all")
    assert "已开启当前群记忆" in reply
    reply, _ = await handle_memory_command(service, admin, "/memory group remember 群约定")
    assert "已保存群约定" in reply
    await service.aclose()


@pytest.mark.asyncio
async def test_admin_off_clear_new_request_volatile_history_reset(tmp_path) -> None:
    identity = MemoryIdentity(10101, 90909, "private", None, "admin", True)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    await service.remember_explicit(identity, "管理员显式事实")
    resets: list[str] = []

    reply, _ = await handle_memory_command(service, identity, "/memory new", clear_admin_history=lambda: resets.append("new"))
    assert "已开始新对话" in reply
    reply, _ = await handle_memory_command(service, identity, "/memory off", clear_admin_history=lambda: resets.append("off"))
    assert "已停止采集" in reply
    prompt, _ = await handle_memory_command(service, identity, "/memory clear", clear_admin_history=lambda: resets.append("early"))
    token = re.search(r"--confirm (\S+)", prompt).group(1)
    reply, _ = await handle_memory_command(
        service, identity, f"/memory clear --confirm {token}", clear_admin_history=lambda: resets.append("clear")
    )
    assert "已清除" in reply
    assert resets == ["new", "off", "clear"]
    await service.aclose()


@pytest.mark.asyncio
async def test_status_reports_master_scope_member_mode_and_features(tmp_path, monkeypatch) -> None:
    disabled_path = tmp_path / "disabled.sqlite3"
    disabled = MemoryService(enabled=False, db_path=disabled_path)
    reply, _ = await handle_memory_command(disabled, MemoryIdentity(1, 2, "private", None, "public"), "/memory status")
    assert "总开关：关闭" in reply
    assert not disabled_path.exists()

    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_GROUP_ALLOWLIST", {GROUP_ID})
    identity = _group_identity(20202, personal_admin=True)
    service = MemoryService(enabled=True, db_path=tmp_path / "enabled.sqlite3")
    await service.set_group_enabled(identity, True, "all")
    await service.remember_explicit(identity, "个人事实")
    reply, _ = await handle_memory_command(service, identity, "/memory status")
    assert "scope：开启" in reply
    assert "模式：all" in reply
    assert "本人：启用" in reply
    assert "可见事实：1" in reply
    await service.aclose()


@pytest.mark.asyncio
async def test_fact_prefix_is_scope_bound_and_must_be_unique(tmp_path) -> None:
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    first = MemoryIdentity(10101, 20202, "private", None, "public")
    other = MemoryIdentity(10101, 20203, "private", None, "public")
    await service.set_enabled(first, True)
    await service.set_enabled(other, True)
    await service.store.save_explicit_fact(scope_for(first), "user:20202", "甲", fact_id="abcdef001111")
    await service.store.save_explicit_fact(scope_for(first), "user:20202", "乙", fact_id="abcdef002222")
    await service.store.save_explicit_fact(scope_for(other), "user:20203", "他域", fact_id="abcdef001111-other")

    with pytest.raises(ValueError, match="ambiguous"):
        await service.forget_fact(first, "abcdef00")
    assert await service.forget_fact(first, "abcdef001") is True
    assert [row["text"] for row in await service.list_facts(first)] == ["乙"]
    assert [row["text"] for row in await service.list_facts(other)] == ["他域"]
    await service.aclose()


@pytest.mark.asyncio
async def test_clear_token_is_bound_to_exact_scope_epoch_and_is_one_time(tmp_path, monkeypatch) -> None:
    other_group = GROUP_ID + 1
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_GROUP_ALLOWLIST", {GROUP_ID, other_group})
    first = _group_identity(90909, personal_admin=True)
    other = _group_identity(90909, group_id=other_group, personal_admin=True)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    await service.set_group_enabled(first, True)
    await service.set_group_enabled(other, True)

    prompt, _ = await handle_memory_command(service, first, "/memory clear")
    token = re.search(r"--confirm (\S+)", prompt).group(1)
    reply, _ = await handle_memory_command(service, other, f"/memory clear --confirm {token}")
    assert "确认码无效" in reply
    reply, _ = await handle_memory_command(service, first, f"/memory clear --confirm {token}")
    assert "确认码无效" in reply

    prompt, _ = await handle_memory_command(service, first, "/memory clear")
    token = re.search(r"--confirm (\S+)", prompt).group(1)
    await service.rotate_conversation(first)
    reply, _ = await handle_memory_command(service, first, f"/memory clear --confirm {token}")
    assert "确认码无效" in reply
    await service.aclose()


@pytest.mark.asyncio
async def test_group_clear_requires_admin_and_only_clears_target_group(tmp_path, monkeypatch) -> None:
    other_group = GROUP_ID + 1
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_GROUP_ALLOWLIST", {GROUP_ID, other_group})
    admin = _group_identity(90909, personal_admin=True)
    regular = _group_identity(20202)
    other = _group_identity(90909, group_id=other_group, personal_admin=True)
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    await service.set_group_enabled(admin, True)
    await service.set_group_enabled(other, True)
    await service.remember_explicit(admin, "目标群约定", subject_id=f"group:{GROUP_ID}")
    await service.remember_explicit(other, "其他群约定", subject_id=f"group:{other_group}")

    reply, _ = await handle_memory_command(service, regular, "/memory group clear")
    assert "仅可信个人管理员" in reply
    prompt, _ = await handle_memory_command(service, admin, "/memory group clear")
    token = re.search(r"--confirm (\S+)", prompt).group(1)
    reply, _ = await handle_memory_command(service, admin, f"/memory group clear --confirm {token}")
    assert "已清除当前公共群" in reply
    assert await service.search_facts(admin, "目标群") == []
    assert [row["text"] for row in await service.search_facts(other, "其他群")] == ["其他群约定"]
    target_status = await service.status(admin)
    assert target_status["scope_enabled"] is True
    await service.aclose()


@pytest.mark.asyncio
async def test_history_pagination_has_author_time_and_no_duplicates(tmp_path) -> None:
    identity = MemoryIdentity(10101, 20202, "private", None, "public")
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    await service.set_enabled(identity, True)
    for index in range(3):
        turn = await service.turn(identity, CapturedInput(f"event-{index}", f"关键词-{index}"))
        assert turn is not None
        async with turn:
            await turn.record_generated(f"回答-{index}")
            await turn.finish_delivery(True)

    latest = await service.history(identity, query="关键词", limit=2)
    older = await service.history(identity, query="关键词", before=latest[0]["seq"], limit=2)
    assert {row["seq"] for row in latest}.isdisjoint({row["seq"] for row in older})
    reply, _ = await handle_memory_command(service, identity, "/memory history 关键词")
    assert "QQ:20202" in reply
    assert "user/direct_chat" in reply
    assert "20" in reply
    await service.aclose()
