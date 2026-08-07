from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

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


def test_chat_context_trims_to_latest_max_messages(controlled_time) -> None:
    svc = _new_service()
    session = "group:30001"
    max_messages = svc._CHAT_CONTEXT_MAX_MESSAGES
    all_messages: list[dict[str, str]] = []

    for i in range(max_messages + 10):
        svc._save_chat_turn(session, f"q{i}", f"a{i}")
        all_messages.append({"role": "user", "content": f"q{i}"})
        all_messages.append({"role": "assistant", "content": f"a{i}"})
        controlled_time.advance(1)

    history = svc._load_active_chat_history(session)
    assert len(history) == max_messages
    assert history == all_messages[-max_messages:]


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


def test_parse_web_search_marker() -> None:
    svc = _new_service()
    assert svc._parse_web_search_marker("[WEB_SEARCH]今天北京天气") == "今天北京天气"
    assert svc._parse_web_search_marker("  [WEB_SEARCH]  今天北京天气  ") == "今天北京天气"
    assert svc._parse_web_search_marker("先解释一下\n[WEB_SEARCH]查一下股票行情") == "查一下股票行情"
    assert svc._parse_web_search_marker("[WEB_SEARCH]") is None
    assert svc._parse_web_search_marker("你好呀") is None
    assert svc._parse_web_search_marker("") is None


def test_extract_responses_search_sources() -> None:
    resp = {
        "output": [
            {"type": "web_search_call", "status": "completed"},
            {
                "type": "function_call",
                "name": "web_search",
                "arguments": json.dumps(
                    {"search_results": [{"title": "标题A", "url": "https://a.example", "content": "内容A"}]},
                    ensure_ascii=False,
                ),
            },
        ]
    }
    out = AIService._extract_responses_search_sources(resp)
    assert "标题A" in out
    assert "https://a.example" in out
    assert "内容A" in out

    resp2 = {
        "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "搜索摘要"}]}]
    }
    out2 = AIService._extract_responses_search_sources(resp2)
    assert "搜索摘要" in out2

    assert AIService._extract_responses_search_sources({}) == ""
    assert AIService._extract_responses_search_sources({"output": []}) == ""


def test_append_web_search_judge_respects_switch() -> None:
    svc = _new_service()
    svc.web_search_enabled = True
    out = svc._append_web_search_judge("base-prompt")
    assert out.startswith("base-prompt")
    assert "联网需求判定" in out

    svc.web_search_enabled = False
    assert svc._append_web_search_judge("base-prompt") == "base-prompt"


def test_chat_sync_no_marker_single_call(monkeypatch) -> None:
    svc = _new_service()
    payloads = _install_fake_chat_backend(monkeypatch, svc)

    out = svc._chat_sync("hello")
    assert out == "reply-1"
    assert len(payloads) == 1
    assert "联网需求判定" in str(payloads[0]["messages"][0]["content"])


def test_chat_with_context_web_search_flow(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    calls: list[dict[str, Any]] = []

    def _fake_post_json(url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        calls.append({"url": url, "payload": payload})
        if url.endswith("/responses"):
            return {
                "output": [
                    {"type": "web_search_call", "status": "completed"},
                    {
                        "type": "function_call",
                        "name": "web_search",
                        "arguments": json.dumps(
                            {"search_results": [{"title": "天气报道", "url": "https://example.com/w", "content": "今日晴"}]},
                            ensure_ascii=False,
                        ),
                    },
                ]
            }
        chat_count = len([c for c in calls if c["url"].endswith("/chat/completions")])
        if chat_count == 1:
            return {"choices": [{"message": {"content": "[WEB_SEARCH]北京今天天气"}}]}
        return {"choices": [{"message": {"content": "整合后的天气回答"}}]}

    monkeypatch.setattr(svc, "_post_json", _fake_post_json)

    out = svc._chat_with_context_sync("private:10001", "今天北京天气怎么样？")
    assert out == "整合后的天气回答"

    responses_calls = [c for c in calls if c["url"].endswith("/responses")]
    assert len(responses_calls) == 1
    assert responses_calls[0]["payload"]["model"] == "deepseek-v4-flash"
    assert responses_calls[0]["payload"]["tools"] == [{"type": "web_search"}]
    assert responses_calls[0]["payload"]["tool_choice"] == {"type": "web_search"}

    chat_calls = [c for c in calls if c["url"].endswith("/chat/completions")]
    assert len(chat_calls) == 2
    # 判定调用：system prompt 含联网判定指令，且输出带标记
    assert "联网需求判定" in str(chat_calls[0]["payload"]["messages"][0]["content"])
    # 整合调用：把原始问题与搜索素材传给 v4-pro
    assert "今天北京天气怎么样？" in str(chat_calls[1]["payload"]["messages"][1]["content"])
    assert "今日晴" in str(chat_calls[1]["payload"]["messages"][1]["content"])

    history = svc._load_active_chat_history("private:10001")
    assert history[-1] == {"role": "assistant", "content": "整合后的天气回答"}


def test_web_search_compose_does_not_reapply_search_judge(monkeypatch) -> None:
    svc = _new_service()
    payloads: list[dict[str, Any]] = []

    def _fake_post_json(_url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        payloads.append(payload)
        return {"choices": [{"message": {"content": "整合回答"}}]}

    monkeypatch.setattr(svc, "_post_json", _fake_post_json)
    system = svc._append_web_search_judge("基础提示")

    out = svc._web_search_compose_final_sync(system, "图片中的商品多少钱？", "搜索素材")

    assert out == "整合回答"
    compose_system = payloads[0]["messages"][0]["content"]
    assert "基础提示" in compose_system
    assert svc._WEB_SEARCH_JUDGE_PROMPT not in compose_system


def test_chat_with_context_web_search_disabled(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    svc.web_search_enabled = False
    payloads: list[dict[str, Any]] = []

    def _fake_post_json(_url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        payloads.append(payload)
        return {"choices": [{"message": {"content": "[WEB_SEARCH]不应触发的查询"}}]}

    monkeypatch.setattr(svc, "_post_json", _fake_post_json)

    out = svc._chat_with_context_sync("private:10002", "查一下新闻")
    assert out == "[WEB_SEARCH]不应触发的查询"
    assert len(payloads) == 1


def test_chat_with_context_web_search_no_sources_falls_back(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    calls: list[dict[str, Any]] = []

    def _fake_post_json(url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        calls.append({"url": url, "payload": payload})
        if url.endswith("/responses"):
            return {"output": [{"type": "web_search_call", "status": "completed"}]}
        n = len([c for c in calls if c["url"].endswith("/chat/completions")])
        if n == 1:
            return {"choices": [{"message": {"content": "[WEB_SEARCH]某查询"}}]}
        return {"choices": [{"message": {"content": "无素材回退回答"}}]}

    monkeypatch.setattr(svc, "_post_json", _fake_post_json)

    out = svc._chat_with_context_sync("private:10003", "查新闻")
    assert out == "无素材回退回答"
    assert len(calls) == 3  # 判定 + responses 搜索(无素材) + 回退


def test_strip_web_search_marker() -> None:
    svc = _new_service()
    assert svc._strip_web_search_marker("[WEB_SEARCH]北京天气") == ""
    assert svc._strip_web_search_marker("说明\n[WEB_SEARCH]查询词\n剩余") == "说明\n剩余"
    assert svc._strip_web_search_marker("普通文本") == "普通文本"


def test_chat_with_context_web_search_failure_falls_back_to_plain(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    calls: list[dict[str, Any]] = []

    def _fake_post_json(url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        calls.append({"url": url, "payload": payload})
        n = len([c for c in calls if c["url"].endswith("/chat/completions")])
        if n == 1:
            return {"choices": [{"message": {"content": "[WEB_SEARCH]北京今天天气"}}]}
        return {"choices": [{"message": {"content": "普通回答"}}]}

    def _boom(_query: str) -> str:
        raise RuntimeError("v4-flash search failed")

    monkeypatch.setattr(svc, "_post_json", _fake_post_json)
    monkeypatch.setattr(svc, "_web_search_fetch_sources_sync", _boom)

    out = svc._chat_with_context_sync("private:10001", "北京今天天气？")
    assert out == "普通回答"

    chat_calls = [c for c in calls if c["url"].endswith("/chat/completions")]
    assert len(chat_calls) == 2
    # 回退调用不带联网判定指令，避免再次输出标记
    assert "联网需求判定" not in str(chat_calls[1]["payload"]["messages"][0]["content"])

    history = svc._load_active_chat_history("private:10001")
    assert history[0] == {"role": "user", "content": "北京今天天气？"}


def test_chat_sync_web_search_failure_falls_back_to_plain(monkeypatch) -> None:
    svc = _new_service()
    calls: list[dict[str, Any]] = []

    def _fake_post_json(url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        calls.append({"url": url, "payload": payload})
        n = len([c for c in calls if c["url"].endswith("/chat/completions")])
        if n == 1:
            return {"choices": [{"message": {"content": "[WEB_SEARCH]北京天气"}}]}
        return {"choices": [{"message": {"content": "stateless 普通回答"}}]}

    def _boom(_query: str) -> str:
        raise RuntimeError("search failed")

    monkeypatch.setattr(svc, "_post_json", _fake_post_json)
    monkeypatch.setattr(svc, "_web_search_fetch_sources_sync", _boom)

    out = svc._chat_sync("北京天气？")
    assert out == "stateless 普通回答"
    chat_calls = [c for c in calls if c["url"].endswith("/chat/completions")]
    assert len(chat_calls) == 2


def test_web_search_fallback_strips_stray_marker(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    calls: list[dict[str, Any]] = []

    def _fake_post_json(url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        calls.append({"url": url, "payload": payload})
        n = len([c for c in calls if c["url"].endswith("/chat/completions")])
        if n == 1:
            return {"choices": [{"message": {"content": "[WEB_SEARCH]某查询"}}]}
        return {"choices": [{"message": {"content": "[WEB_SEARCH]残留标记\n实际上普通内容"}}]}

    def _boom(_query: str) -> str:
        raise RuntimeError("search failed")

    monkeypatch.setattr(svc, "_post_json", _fake_post_json)
    monkeypatch.setattr(svc, "_web_search_fetch_sources_sync", _boom)

    out = svc._chat_with_context_sync("private:10002", "查一下")
    assert "[WEB_SEARCH]" not in out
    assert "实际上普通内容" in out
