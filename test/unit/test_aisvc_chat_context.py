from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import aisvc
import config
from aisvc import AIService


class _DummyLog:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return


def _new_service() -> AIService:
    svc = AIService(log=_DummyLog())
    svc.deepseek_base_url = "https://example.local/v1"
    svc.deepseek_api_key = "fake-chat-key"
    svc.system_prompt = "system-prompt"
    return svc


def _install_fake_chat_backend(monkeypatch, svc: AIService) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    seq = {"n": 0}

    def _fake_post_json(_url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        seq["n"] += 1
        payloads.append(payload)
        return {"choices": [{"message": {"content": f"reply-{seq['n']}"}}]}

    monkeypatch.setattr(svc, "_post_json", _fake_post_json)
    monkeypatch.setattr(svc, "_select_chat_system_prompt", lambda _session_key: "system-prompt")
    return payloads


def test_default_system_prompt_includes_level_one_command_guidance() -> None:
    prompt = config.AI_SYSTEM_PROMPT

    assert "机器人业务指令提示（1 级用户）" in prompt
    assert "/help 或 /h" in prompt
    assert "/find 搜索内容" in prompt
    assert "/get 序号" in prompt
    assert "不要声称已经替用户执行指令" in prompt


def test_custom_chat_prompt_override_remains_independent(monkeypatch) -> None:
    svc = _new_service()
    monkeypatch.setattr(
        svc,
        "_load_private_chat_prompt_config",
        lambda: {"default": {}, "users": {"10001": {"system_prompt": "private-special"}}},
    )
    monkeypatch.setattr(
        svc,
        "_load_group_chat_prompt_config",
        lambda: {"default": "", "groups": {"20001": "group-special"}},
    )

    assert svc._select_chat_system_prompt("private:10001") == "private-special"
    assert svc._select_chat_system_prompt("group:20001") == "group-special"
    assert svc._select_chat_system_prompt("private:99999") == "system-prompt"


def test_chat_context_keeps_history_within_30_minutes(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    payloads = _install_fake_chat_backend(monkeypatch, svc)

    first = svc._chat_with_context_sync("group:20001", "hello")
    assert first == "reply-1"

    controlled_time.advance(10 * 60)
    second = svc._chat_with_context_sync("group:20001", "follow-up")
    assert second == "reply-2"

    assert len(payloads) == 2
    assert payloads[0]["model"] == "deepseek-v4-pro"
    assert payloads[0]["thinking"] == {"type": "enabled"}
    assert payloads[0]["reasoning_effort"] == "high"
    first_system = payloads[0]["messages"][0]["content"]
    assert first_system.startswith("system-prompt")
    assert "不能执行 QQ 机器人的自动业务功能" in first_system
    second_messages = payloads[1]["messages"]
    assert [m["role"] for m in second_messages] == ["system", "user", "assistant", "user"]
    assert second_messages[1]["content"] == "hello"
    assert second_messages[2]["content"] == "reply-1"
    assert second_messages[3]["content"] == "follow-up"


def test_chat_context_expires_after_30_minutes(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    payloads = _install_fake_chat_backend(monkeypatch, svc)

    svc._chat_with_context_sync("private:10001", "first")
    controlled_time.advance((30 * 60) + 1)
    svc._chat_with_context_sync("private:10001", "second")

    assert len(payloads) == 2
    second_messages = payloads[1]["messages"]
    assert [m["role"] for m in second_messages] == ["system", "user"]
    assert second_messages[-1]["content"] == "second"


def test_chat_context_trims_to_latest_100_messages(controlled_time) -> None:
    svc = _new_service()
    session = "group:30001"

    for i in range(70):
        svc._save_chat_turn(session, f"q{i}", f"a{i}")
        controlled_time.advance(1)

    history = svc._load_active_chat_history(session)
    assert len(history) == 100
    assert history[0] == {"role": "user", "content": "q20"}
    assert history[1] == {"role": "assistant", "content": "a20"}
    assert history[-1] == {"role": "assistant", "content": "a69"}


def test_chat_context_keeps_non_aichat_user_messages(controlled_time) -> None:
    svc = _new_service()
    session = "group:30002"

    svc.remember_user_message(session, "normal-1")
    controlled_time.advance(1)
    svc.remember_user_message(session, "normal-2")

    history = svc._load_active_chat_history(session)
    assert history == [
        {"role": "user", "content": "normal-1"},
        {"role": "user", "content": "normal-2"},
    ]


def test_chat_context_isolated_between_group_and_private(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    payloads = _install_fake_chat_backend(monkeypatch, svc)

    svc._chat_with_context_sync("group:20001", "group first")
    controlled_time.advance(10)
    svc._chat_with_context_sync("private:10001", "private first")
    controlled_time.advance(10)
    svc._chat_with_context_sync("group:20001", "group second")

    group_second_messages = payloads[2]["messages"]
    all_contents = [str(m.get("content") or "") for m in group_second_messages]
    assert "group first" in all_contents
    assert "reply-1" in all_contents
    assert "private first" not in all_contents


def test_chat_context_invalid_history_resets_but_session_still_works(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    payloads = _install_fake_chat_backend(monkeypatch, svc)

    svc._chat_sessions["group:broken"] = {
        "last_active_ts": controlled_time.time(),
        "messages": [{"role": "system", "content": "bad-structure"}],
    }

    out = svc._chat_with_context_sync("group:broken", "fresh question")
    assert out == "reply-1"

    assert [m["role"] for m in payloads[0]["messages"]] == ["system", "user"]
    history = svc._load_active_chat_history("group:broken")
    assert history == [
        {"role": "user", "content": "fresh question"},
        {"role": "assistant", "content": "reply-1"},
    ]


def test_reason_notice_uses_v4_flash_thinking_mode(monkeypatch) -> None:
    svc = _new_service()
    client_init: list[dict[str, str]] = []
    completion_calls: list[dict[str, Any]] = []

    class _FakeCompletions:
        def create(self, **kwargs):
            completion_calls.append(kwargs)
            return SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content="final answer"))]
            )

    class _FakeOpenAI:
        def __init__(self, api_key: str, base_url: str):
            client_init.append({"api_key": api_key, "base_url": base_url})
            self.chat = SimpleNamespace(completions=_FakeCompletions())

    monkeypatch.setattr(aisvc, "OpenAI", _FakeOpenAI)

    out = svc._reason_notice_sync_v2("source", "snippet")

    assert out == "final answer"
    assert client_init == [{"api_key": "fake-chat-key", "base_url": "https://example.local/v1"}]
    assert len(completion_calls) == 1
    assert completion_calls[0]["model"] == "deepseek-v4-flash"
    assert completion_calls[0]["reasoning_effort"] == "high"
    assert completion_calls[0]["extra_body"] == {"thinking": {"type": "enabled"}}
