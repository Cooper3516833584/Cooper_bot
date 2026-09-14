from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace

import pytest

from cooper_bot.modules.ai.aisvc import AIService
from cooper_bot.modules.memory.models import CapturedInput, MemoryIdentity
from cooper_bot.modules.memory.service import MemoryService


class _Log:
    def warning(self, _msg: str) -> None:
        return


class _Runner:
    def __init__(self) -> None:
        self.settings = SimpleNamespace(
            timeout_seconds=120.0,
            admin_timeout_seconds=480.0,
            admin_enabled=False,
        )
        self.requests = []

    async def run(self, request):
        self.requests.append(request)
        return SimpleNamespace(text="kimi-reply", tool_call_observed=False, tool_names=())


def _patch_settings_validation(monkeypatch, *, public: bool = True, admin: bool = True) -> None:
    monkeypatch.setattr(
        "cooper_bot.modules.ai.aisvc.validate_kimi_settings",
        lambda _settings: SimpleNamespace(public_profile_valid=public, admin_profile_valid=admin),
    )


@pytest.mark.asyncio
async def test_kimi_chat_rejects_when_requested_profile_is_not_ready(monkeypatch) -> None:
    """service 层必须自己校验 readiness，不能依赖调用方先检查。"""
    svc = AIService(_Log())
    svc.system_prompt = "system"
    runner = _Runner()
    svc._kimi_runner = runner
    _patch_settings_validation(monkeypatch)

    # public 未就绪：chat_ready 为 False
    with pytest.raises(RuntimeError) as public_error:
        await svc.kimi_chat_with_context("private:stateless", "hi")
    assert "public profile is not ready" in str(public_error.value)

    # admin 未就绪：computer_ready 为 False，即使调用方误传 allow_computer=True 也必须拒绝
    with pytest.raises(RuntimeError) as admin_error:
        await svc.kimi_chat_with_context(
            "private:stateless", "hi", allow_computer=True, actor_user_id=900001
        )
    assert "admin profile is not ready" in str(admin_error.value)
    assert runner.requests == []


@pytest.mark.asyncio
async def test_kimi_admin_chat_proceeds_when_computer_ready(monkeypatch) -> None:
    svc = AIService(_Log())
    svc.system_prompt = "system"
    runner = _Runner()
    runner.settings.admin_enabled = True
    svc._kimi_runner = runner
    svc._computer_verified = True
    _patch_settings_validation(monkeypatch)

    out = await svc.kimi_chat_with_context(
        "private:stateless", "hi", allow_computer=True, actor_user_id=900001
    )

    assert out == "kimi-reply"
    assert runner.requests[0].profile == "admin"


@pytest.mark.asyncio
async def test_kimi_admin_history_does_not_read_public_group_history(monkeypatch) -> None:
    svc = AIService(_Log())
    svc.system_prompt = "system"
    runner = _Runner()
    runner.settings.admin_enabled = True
    svc._kimi_runner = runner
    svc._public_runtime_safe = True
    # admin 路径现在要求 computer_ready，补齐 readiness 才能走到历史隔离逻辑。
    svc._computer_verified = True
    _patch_settings_validation(monkeypatch)
    svc._save_chat_turn("group:20001", "public-message", "public-reply")

    out = await svc.kimi_chat_with_context("group:20001", "admin-message", allow_computer=True, actor_user_id=900001)

    assert out == "kimi-reply"
    prompt = json.loads(runner.requests[0].prompt)
    assert prompt["conversation_history"] == []
    assert runner.requests[0].profile == "admin"
    assert "admin:900001:group:20001" in svc._chat_sessions
    assert "public-message" not in json.dumps(svc._chat_sessions["admin:900001:group:20001"], ensure_ascii=False)


@pytest.mark.asyncio
async def test_admin_memory_uses_and_saves_volatile_history_with_explicit_facts(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "memory.sqlite3"
    svc = AIService(_Log())
    svc.system_prompt = "system"
    runner = _Runner()
    runner.settings.admin_enabled = True
    svc._kimi_runner = runner
    svc._computer_verified = True
    svc.memory = MemoryService(enabled=True, db_path=db_path)
    _patch_settings_validation(monkeypatch)
    identity = MemoryIdentity(10101, 900001, "private", None, "admin", True)
    await svc.memory.remember_explicit(identity, "偏好简短回答")

    first = await svc.memory.turn(identity, CapturedInput("admin-1", "first", source_kind="request_metadata"))
    assert first is not None
    async with first:
        first_reply = await svc.kimi_chat_with_context(
            "private:900001", "我的回答偏好是什么", allow_computer=True, actor_user_id=900001, memory_turn=first
        )
        await first.record_generated(first_reply)
        await first.finish_delivery(True)

    second = await svc.memory.turn(identity, CapturedInput("admin-2", "second", source_kind="request_metadata"))
    assert second is not None
    async with second:
        second_reply = await svc.kimi_chat_with_context(
            "private:900001", "继续按这个偏好回答", allow_computer=True, actor_user_id=900001, memory_turn=second
        )
        await second.record_generated(second_reply)
        await second.finish_delivery(True)

    first_prompt = json.loads(runner.requests[0].prompt)
    second_prompt = json.loads(runner.requests[1].prompt)
    assert first_prompt["conversation_history"] == []
    assert first_prompt["memory_context"]["facts"][0]["text"] == "偏好简短回答"
    assert [item["content"] for item in second_prompt["conversation_history"]] == ["我的回答偏好是什么", "kimi-reply"]
    assert len(svc._load_active_chat_history("admin:900001:private:900001")) == 4
    await svc.memory.aclose()

    with sqlite3.connect(db_path) as conn:
        stored_text = conn.execute("SELECT own_text FROM memory_events").fetchall()
    assert stored_text and all(row[0] == "" for row in stored_text)


def test_clear_admin_chat_history_only_resets_matching_actor_and_session() -> None:
    svc = AIService(_Log())
    svc._save_chat_turn("admin:900001:group:20001", "one", "reply")
    svc._save_chat_turn("admin:900002:group:20001", "two", "reply")
    svc._save_chat_turn("admin:900001:group:20002", "three", "reply")

    svc.clear_admin_chat_history("group:20001", 900001)

    assert "admin:900001:group:20001" not in svc._chat_sessions
    assert "admin:900002:group:20001" in svc._chat_sessions
    assert "admin:900001:group:20002" in svc._chat_sessions


@pytest.mark.asyncio
async def test_public_group_memory_prompt_keeps_each_actor_id(monkeypatch) -> None:
    svc = AIService(_Log())
    svc.system_prompt = "system"
    runner = _Runner()
    svc._kimi_runner = runner
    svc._public_runtime_safe = True
    _patch_settings_validation(monkeypatch)

    async def _no_facts(_identity, _content, summary=None):
        return None

    svc.memory = SimpleNamespace(prompt_context=_no_facts)
    memory_turn = SimpleNamespace(
        identity=MemoryIdentity(10101, 20203, "group", 30303, "public"),
        snapshot=SimpleNamespace(recent_events=(
            {"role": "user", "actor_user_id": 20201, "own_text": "甲说的话", "quoted_text": "", "visual_text": ""},
            {"role": "assistant", "actor_user_id": 20201, "own_text": "上一轮回复", "quoted_text": "", "visual_text": ""},
            {"role": "user", "actor_user_id": 20202, "own_text": "乙说的话", "quoted_text": "", "visual_text": ""},
        )),
    )

    await svc.kimi_chat_with_context("group:30303", "现在是谁在问？", memory_turn=memory_turn)

    history = json.loads(runner.requests[0].prompt)["conversation_history"]
    assert "[发言人QQ:20201]" in history[0]["content"]
    assert history[1]["content"] == "上一轮回复"
    assert "[发言人QQ:20202]" in history[2]["content"]


@pytest.mark.asyncio
async def test_public_memory_prompt_excludes_cross_scope_and_opted_out_content(tmp_path, monkeypatch) -> None:
    group_a, group_b = 30303, 30304
    monkeypatch.setattr("cooper_bot.modules.memory.service.config.AI_MEMORY_GROUP_ALLOWLIST", {group_a, group_b})
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    admin_a = MemoryIdentity(10101, 90909, "group", group_a, "public", True)
    admin_b = MemoryIdentity(10101, 90909, "group", group_b, "public", True)
    opted_out = MemoryIdentity(10101, 20201, "group", group_a, "public")
    cross_scope = MemoryIdentity(10101, 20202, "group", group_b, "public")
    current_actor = MemoryIdentity(10101, 20203, "group", group_a, "public")
    await service.set_group_enabled(admin_a, True, "all")
    await service.set_group_enabled(admin_b, True, "all")

    for identity, event_id, text in (
        (opted_out, "old-a", "OPT_OUT_SECRET"),
        (cross_scope, "old-b", "CROSS_SCOPE_SECRET"),
    ):
        turn = await service.turn(identity, CapturedInput(event_id, text))
        assert turn is not None
        async with turn:
            await turn.record_generated("old answer")
            await turn.finish_delivery(True)
    await service.set_member_enabled(opted_out, False)

    svc = AIService(_Log())
    svc.system_prompt = "system"
    runner = _Runner()
    svc._kimi_runner = runner
    svc._public_runtime_safe = True
    svc.memory = service
    _patch_settings_validation(monkeypatch)
    current = await service.turn(current_actor, CapturedInput("current", "CURRENT_ONLY"))
    assert current is not None
    async with current:
        await svc.kimi_chat_with_context("group:30303", "CURRENT_ONLY", memory_turn=current)
    await service.store.fail_pending_input(current.scope_id, current.snapshot.current_input_seq)

    prompt = json.loads(runner.requests[0].prompt)
    serialized = json.dumps(prompt, ensure_ascii=False)
    assert "OPT_OUT_SECRET" not in serialized
    assert "CROSS_SCOPE_SECRET" not in serialized
    assert prompt["latest_user_request"] == "CURRENT_ONLY"
    assert all("CURRENT_ONLY" not in item["content"] for item in prompt["conversation_history"])
    assert serialized.count("CURRENT_ONLY") == 1
    await service.aclose()


@pytest.mark.asyncio
async def test_forgotten_fact_never_enters_fake_kimi_prompt(tmp_path, monkeypatch) -> None:
    identity = MemoryIdentity(10101, 20202, "private", None, "public")
    service = MemoryService(enabled=True, db_path=tmp_path / "memory.sqlite3")
    await service.set_enabled(identity, True)
    fact = await service.remember_explicit(identity, "FORGOTTEN_SECRET preference")
    assert await service.forget_fact(identity, fact["fact_id"]) is True

    svc = AIService(_Log())
    svc.system_prompt = "system"
    runner = _Runner()
    svc._kimi_runner = runner
    svc._public_runtime_safe = True
    svc.memory = service
    _patch_settings_validation(monkeypatch)
    turn = await service.turn(identity, CapturedInput("after-forget", "What preference?"))
    assert turn is not None
    async with turn:
        await svc.kimi_chat_with_context("private:20202", "What preference?", memory_turn=turn)
    await service.store.fail_pending_input(turn.scope_id, turn.snapshot.current_input_seq)

    assert "FORGOTTEN_SECRET" not in runner.requests[0].prompt
    await service.aclose()


@pytest.mark.asyncio
async def test_kimi_context_includes_vision_and_saves_only_base_text(monkeypatch) -> None:
    svc = AIService(_Log())
    svc.system_prompt = "system"
    runner = _Runner()
    svc._kimi_runner = runner
    svc._public_runtime_safe = True
    monkeypatch.setattr("cooper_bot.modules.ai.aisvc.validate_kimi_settings", lambda _settings: SimpleNamespace(public_profile_valid=True))

    await svc.kimi_chat_with_context(
        "private:10001",
        "多少钱？",
        msg_id="1",
        vision_slots=[
            {
                "slot_id": "1:1",
                "index": 1,
                "segment_type": "image",
                "status": "ready",
                "description": "类型：产品照片；画面：RTX 5090",
            }
        ],
    )

    prompt = json.loads(runner.requests[0].prompt)
    assert "[视觉内容1] 类型：产品照片；画面：RTX 5090" in prompt["latest_user_request"]
    history = svc._load_active_chat_history("private:10001")
    assert history[0]["content"] == "多少钱？"
    assert "[视觉内容1]" not in history[0]["content"]


@pytest.mark.asyncio
async def test_calendar_web_query_requires_observed_websearch(monkeypatch) -> None:
    svc = AIService(_Log())
    runner = _Runner()
    runner.settings = SimpleNamespace(timeout_seconds=120.0, admin_timeout_seconds=480.0)
    svc._kimi_runner = runner
    svc._public_runtime_safe = True
    monkeypatch.setattr("cooper_bot.modules.ai.aisvc.validate_kimi_settings", lambda _settings: SimpleNamespace(public_profile_valid=True))

    with pytest.raises(RuntimeError, match="not observed"):
        await svc.calendar_web_query("查日历")
    assert svc.calendar_web_ready is False

    async def _search_result(_request):
        return SimpleNamespace(text="{}", tool_call_observed=True, tool_names=("WebSearch",))

    runner.run = _search_result
    assert await svc.calendar_web_query("查日历") == "{}"
    assert svc.calendar_web_ready is True


@pytest.mark.asyncio
async def test_capability_probe_requires_protocol_tools_and_matching_cache(monkeypatch, tmp_path) -> None:
    svc = AIService(_Log())

    class _ProbeRunner:
        def __init__(self) -> None:
            self.settings = SimpleNamespace(
                timeout_seconds=30.0,
                admin_timeout_seconds=30.0,
                admin_enabled=True,
                public=SimpleNamespace(workdir=tmp_path),
            )

        def reset_public_security_state(self):
            return

        async def run(self, request):
            if request.purpose == "public_websearch_probe":
                return SimpleNamespace(text="Paris", tool_call_observed=True, tool_names=("WebSearch",), protocol_observed=True)
            if request.purpose == "computer_probe":
                return SimpleNamespace(text="KIMI_COMPUTER_PROBE", tool_call_observed=True, tool_names=("Bash",), protocol_observed=True)
            return SimpleNamespace(text="tool unavailable", tool_call_observed=False, tool_names=(), protocol_observed=True)

    svc._kimi_runner = _ProbeRunner()
    svc._kimi_capability_cache_path = tmp_path / "kimi_capabilities.json"
    monkeypatch.setattr(
        "cooper_bot.modules.ai.aisvc.validate_kimi_settings",
        lambda _settings: SimpleNamespace(public_profile_valid=True, admin_profile_valid=True, errors=()),
    )

    async def _fingerprint():
        return ({"version": "future-version-string", "public_config_sha256": "a"}, True)

    monkeypatch.setattr(svc, "_kimi_capability_fingerprint", _fingerprint)
    report = await svc.probe_kimi_capabilities()

    assert report.public_forbidden_tools_blocked is True
    assert report.public_websearch_ready is True
    assert report.admin_bash_ready is True
    assert svc.chat_ready is True
    assert svc.calendar_web_ready is True

    restored = AIService(_Log())
    restored._kimi_runner = _ProbeRunner()
    restored._kimi_capability_cache_path = svc._kimi_capability_cache_path
    monkeypatch.setattr(restored, "_kimi_capability_fingerprint", _fingerprint)
    cached = await restored.load_kimi_capability_cache()
    assert cached.version == "future-version-string"
    assert restored.chat_ready is True

    async def _changed_fingerprint():
        return ({"version": "new-version", "public_config_sha256": "a"}, True)

    monkeypatch.setattr(restored, "_kimi_capability_fingerprint", _changed_fingerprint)
    invalidated = await restored.load_kimi_capability_cache()
    assert invalidated.public_forbidden_tools_blocked is False
    assert restored.chat_ready is False
