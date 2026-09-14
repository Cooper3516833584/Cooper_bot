from __future__ import annotations

import sqlite3

import pytest

from cooper_bot.modules.memory.store import MemoryStore


@pytest.mark.asyncio
async def test_confirmed_turn_is_persistent_and_snapshot_uses_parent_input(tmp_path):
    store=MemoryStore(tmp_path / "memory.sqlite3")
    await store.start()
    scope=await store.ensure_scope("qq:1:public:private:2",bot_id=1,profile="public",kind="private",target_id=2,owner_user_id=2)
    await store.set_scope_policy(scope["scope_id"],True)
    a, inserted=await store.append_input_once(scope["scope_id"],2,"a","first","","","direct_chat")
    assert inserted is True
    await store.insert_assistant_once(scope["scope_id"],a["seq"],"answer")
    await store.finish_turn(scope["scope_id"],a["seq"],True)
    b, inserted=await store.append_input_once(scope["scope_id"],2,"b","second","","","direct_chat")
    assert inserted is True
    rows=await store.snapshot_rows(scope["scope_id"],a["conversation_id"],b["seq"],20)
    assert [x["own_text"] for x in rows] == ["first","answer"]
    await store.close()


@pytest.mark.asyncio
async def test_fact_is_scope_and_subject_bound(tmp_path):
    store=MemoryStore(tmp_path / "memory.sqlite3");await store.start()
    await store.ensure_scope("s",bot_id=1,profile="public",kind="private",target_id=2,owner_user_id=2)
    fact=await store.save_explicit_fact("s","user:2","偏好中文说明")
    assert len(await store.list_facts("s",("user:2",))) == 1
    assert not await store.list_facts("s",("user:3",))
    assert await store.forget_fact("s","user:2",fact["fact_id"])
    assert not await store.list_facts("s",("user:2",))
    await store.close()


@pytest.mark.asyncio
async def test_online_backup_restores_consistent_memory_database(tmp_path):
    source=MemoryStore(tmp_path / "source.sqlite3");await source.start()
    await source.ensure_scope("s",bot_id=1,profile="public",kind="private",target_id=2,owner_user_id=2)
    await source.save_explicit_fact("s","user:2","备份事实")
    backup_path=tmp_path / "backup.sqlite3"
    await source.backup(backup_path)
    await source.close()

    restored=MemoryStore(backup_path);await restored.start()
    assert [row["text"] for row in await restored.list_facts("s",("user:2",))] == ["备份事实"]
    await restored.close()


@pytest.mark.asyncio
async def test_future_schema_fails_closed(tmp_path):
    path=tmp_path / "future.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA user_version=999")
    store=MemoryStore(path)
    with pytest.raises(RuntimeError,match="newer version"):
        await store.start()
    await store.close()
