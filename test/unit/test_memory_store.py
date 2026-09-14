from __future__ import annotations

import pytest

from cooper_bot.modules.memory.store import MemoryStore


@pytest.mark.asyncio
async def test_confirmed_turn_is_persistent_and_snapshot_uses_parent_input(tmp_path):
    store=MemoryStore(tmp_path / "memory.sqlite3")
    await store.start()
    scope=await store.ensure_scope("qq:1:public:private:2",bot_id=1,profile="public",kind="private",target_id=2,owner_user_id=2)
    await store.set_scope_policy(scope["scope_id"],True)
    a=await store.append_input_once(scope["scope_id"],2,"a","first","","","direct_chat")
    await store.insert_assistant_once(scope["scope_id"],a["seq"],"answer")
    await store.finish_turn(scope["scope_id"],a["seq"],True)
    b=await store.append_input_once(scope["scope_id"],2,"b","second","","","direct_chat")
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
