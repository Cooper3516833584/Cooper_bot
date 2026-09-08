from __future__ import annotations

from typing import Any

import pytest

from cooper_bot.modules.ai.aisvc import AIService


class _DummyLog:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return


def _new_service() -> AIService:
    svc = AIService(log=_DummyLog())
    svc.deepseek_base_url = "https://example.local/v1"
    svc.deepseek_api_key = "fake-deepseek-key"
    svc.system_prompt = "system-prompt"
    return svc


def test_deepseek_task_text_keeps_legacy_stateless_payload(monkeypatch) -> None:
    svc = _new_service()
    captured: list[dict[str, Any]] = []

    def _fake_post_json(_url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        assert timeout == 90.0
        captured.append(payload)
        return {"choices": [{"message": {"content": "task-reply"}}]}

    monkeypatch.setattr(svc, "_post_json", _fake_post_json)

    assert svc.deepseek_task_ready is True
    assert svc._deepseek_task_text_sync("生成日历文案") == "task-reply"
    assert captured[0]["model"] == svc.chat_model
    assert captured[0]["temperature"] == svc._CHAT_TEMPERATURE
    assert captured[0]["thinking"] == {"type": "enabled"}
    assert captured[0]["reasoning_effort"] == svc._REASONING_EFFORT_HIGH


def test_chat_sync_remains_a_temporary_deepseek_task_bridge(monkeypatch) -> None:
    svc = _new_service()
    monkeypatch.setattr(svc, "_deepseek_task_text_sync", lambda text: f"legacy:{text}")

    assert svc._chat_sync("hello") == "legacy:hello"


@pytest.mark.asyncio
async def test_deepseek_task_text_async_wrapper_uses_task_path(monkeypatch) -> None:
    svc = _new_service()
    monkeypatch.setattr(svc, "_deepseek_task_text_sync", lambda text: f"task:{text}")

    assert await svc.deepseek_task_text("hello") == "task:hello"
