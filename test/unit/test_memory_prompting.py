from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import cooper_bot.commands.commands as commands
from cooper_bot.modules.memory.models import CapturedInput, MemoryIdentity
from cooper_bot.modules.memory.prompting import fit_prompt_budget, materialize_memory_events
from cooper_bot.modules.memory.service import MemoryService
from cooper_bot.modules.memory.store import MemoryStore


def test_group_persistent_history_keeps_actor_id_and_context_fields() -> None:
    rows = ({
        "role": "user",
        "actor_user_id": 20202,
        "own_text": "正文",
        "quoted_text": "被引用内容",
        "visual_text": "一张课程表",
    },)
    history = materialize_memory_events(rows, "group")
    assert history == [{
        "role": "user",
        "content": "[发言人QQ:20202]\n正文\n[引用内容]\n被引用内容\n[视觉描述]\n一张课程表",
    }]


def test_current_quote_and_ready_visual_sources_ignore_unresolved_slots() -> None:
    aisvc = SimpleNamespace(find_chat_message_by_msg_id=lambda _session, _msg_id: {"content": "被引用的安全文本"})
    evt = {"message": [{"type": "reply", "data": {"id": "77"}}]}
    slots = [
        {"status": "ready", "description": "图片里是一只猫"},
        {"status": "unresolved", "description": "不应保存"},
    ]
    assert commands._memory_quoted_text(aisvc, "private:20202", evt) == "被引用的安全文本"
    assert commands._memory_visual_text(slots) == "图片里是一只猫"


@pytest.mark.asyncio
async def test_quote_visual_roundtrip_without_resource_paths(tmp_path) -> None:
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    identity = MemoryIdentity(10101, 20202, "private", None, "public")
    await service.set_enabled(identity, True)

    safe_turn = await service.turn(identity, CapturedInput("safe", "问题", "引用文本", "图片显示一只猫"))
    assert safe_turn is not None
    assert safe_turn.input_row["quoted_text"] == "引用文本"
    assert safe_turn.input_row["visual_text"] == "图片显示一只猫"
    unsafe_turn = await service.turn(
        identity,
        CapturedInput("unsafe", "问题", "C:\\Users\\name\\secret.txt", "图片 https://example.test/a.png"),
    )
    assert unsafe_turn is not None
    assert unsafe_turn.input_row["quoted_text"] == ""
    assert unsafe_turn.input_row["visual_text"] == ""
    await service.aclose()


def test_prompt_budget_drops_optional_context_not_current_request() -> None:
    payload = {
        "purpose": "qq_chat",
        "system_instructions": "fixed-system",
        "latest_user_request": "CURRENT-MUST-STAY",
        "conversation_history": [
            {"role": "user", "content": "old-1" * 30},
            {"role": "assistant", "content": "old-2" * 30},
        ],
        "memory_context": {
            "facts": [{"text": "fact" * 30}],
            "recalled_history": [{"text": "recall" * 30}],
        },
    }
    required_only = {key: payload[key] for key in ("purpose", "system_instructions", "latest_user_request")}
    budget = len(json.dumps(required_only, ensure_ascii=False, separators=(",", ":"))) + 30
    fitted = fit_prompt_budget(payload, budget)
    assert fitted["latest_user_request"] == "CURRENT-MUST-STAY"
    assert fitted["system_instructions"] == "fixed-system"
    assert fitted["conversation_history"] == []
    assert "memory_context" not in fitted


@pytest.mark.asyncio
async def test_retention_prunes_terminal_blocks_only(tmp_path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    await store.start()
    scope = await store.ensure_scope("scope", bot_id=1, profile="public", kind="private", target_id=2, owner_user_id=2)
    terminal = []
    for index in range(3):
        row, _ = await store.append_input_once("scope", 2, f"done-{index}", f"question-{index}", "", "", "direct_chat")
        await store.insert_assistant_once("scope", row["seq"], f"answer-{index}")
        await store.finish_turn("scope", row["seq"], True)
        terminal.append(row)
    pending, _ = await store.append_input_once("scope", 2, "pending", "pending", "", "", "direct_chat")
    generated, _ = await store.append_input_once("scope", 2, "generated", "generated", "", "", "direct_chat")
    await store.insert_assistant_once("scope", generated["seq"], "in-flight")

    assert await store.prune_terminal_events(3650, 2) == 1

    def _states():
        return dict(store._c().execute("SELECT source_event_id,state FROM memory_events"))

    states = await store._call(_states)
    assert "done-0" not in states
    assert states["done-1"] == "completed"
    assert states["done-2"] == "completed"
    assert states["pending"] == "pending"
    assert states["generated"] == "pending"
    assert states["turn:" + generated["turn_id"] + ":assistant"] == "generated"
    await store.close()


@pytest.mark.asyncio
async def test_master_false_creates_no_memory_db(tmp_path) -> None:
    db_path = tmp_path / "missing" / "memory.sqlite3"
    service = MemoryService(enabled=False, db_path=db_path)
    await service.start()
    assert not db_path.exists()
    assert not db_path.parent.exists()


@pytest.mark.asyncio
async def test_memory_db_path_rejects_public_material_directory(tmp_path, monkeypatch) -> None:
    public_path = tmp_path / "storage" / "documents" / "public" / "memory.sqlite3"
    private_root = tmp_path / "runtime" / "databases"
    warnings: list[str] = []
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_ENABLED", True)
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_DB_PATH", public_path)
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.DATABASES_DIR", private_root)
    service = MemoryService(SimpleNamespace(warning=warnings.append))

    assert service.enabled is False
    await service.start()
    assert not public_path.exists()
    assert warnings and "outside the private database root" in warnings[0]


@pytest.mark.asyncio
async def test_master_off_restart_preserves_database_for_rollback(tmp_path) -> None:
    db_path = tmp_path / "memory.sqlite3"
    identity = MemoryIdentity(10101, 20202, "private", None, "public")
    enabled = MemoryService(enabled=True, db_path=db_path)
    await enabled.set_enabled(identity, True)
    await enabled.remember_explicit(identity, "回滚后仍保留")
    await enabled.aclose()
    original_size = db_path.stat().st_size

    disabled = MemoryService(enabled=False, db_path=db_path)
    await disabled.start()
    assert db_path.stat().st_size == original_size

    restored = MemoryService(enabled=True, db_path=db_path)
    assert [row["text"] for row in await restored.list_facts(identity)] == ["回滚后仍保留"]
    await restored.aclose()
