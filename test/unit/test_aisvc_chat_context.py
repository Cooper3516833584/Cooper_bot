from __future__ import annotations

from typing import Any

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


def test_chat_context_keeps_history_within_30_minutes(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    payloads = _install_fake_chat_backend(monkeypatch, svc)

    first = svc._chat_with_context_sync("group:20001", "hello")
    assert first == "reply-1"

    controlled_time.advance(10 * 60)
    second = svc._chat_with_context_sync("group:20001", "follow-up")
    assert second == "reply-2"

    assert len(payloads) == 2
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


def test_chat_context_trims_to_latest_15_turns(controlled_time) -> None:
    svc = _new_service()
    session = "group:30001"

    for i in range(20):
        svc._save_chat_turn(session, f"q{i}", f"a{i}")
        controlled_time.advance(1)

    history = svc._load_active_chat_history(session)
    assert len(history) == 30
    assert history[0] == {"role": "user", "content": "q5"}
    assert history[1] == {"role": "assistant", "content": "a5"}
    assert history[-1] == {"role": "assistant", "content": "a19"}


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
        "messages": [{"role": "assistant", "content": "bad-structure"}],
    }

    out = svc._chat_with_context_sync("group:broken", "fresh question")
    assert out == "reply-1"

    assert [m["role"] for m in payloads[0]["messages"]] == ["system", "user"]
    history = svc._load_active_chat_history("group:broken")
    assert history == [
        {"role": "user", "content": "fresh question"},
        {"role": "assistant", "content": "reply-1"},
    ]
