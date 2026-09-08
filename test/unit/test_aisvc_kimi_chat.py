from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from cooper_bot.modules.ai.aisvc import AIService


class _Log:
    def warning(self, _msg: str) -> None:
        return


class _Runner:
    def __init__(self) -> None:
        self.settings = SimpleNamespace(timeout_seconds=120.0, admin_timeout_seconds=480.0)
        self.requests = []

    async def run(self, request):
        self.requests.append(request)
        return SimpleNamespace(text="kimi-reply")


@pytest.mark.asyncio
async def test_kimi_admin_history_does_not_read_public_group_history() -> None:
    svc = AIService(_Log())
    svc.system_prompt = "system"
    runner = _Runner()
    svc._kimi_runner = runner
    svc._save_chat_turn("group:20001", "public-message", "public-reply")

    out = await svc.kimi_chat_with_context("group:20001", "admin-message", allow_computer=True, actor_user_id=900001)

    assert out == "kimi-reply"
    prompt = json.loads(runner.requests[0].prompt)
    assert prompt["conversation_history"] == []
    assert runner.requests[0].profile == "admin"
    assert "admin:900001:group:20001" in svc._chat_sessions
    assert "public-message" not in json.dumps(svc._chat_sessions["admin:900001:group:20001"], ensure_ascii=False)
