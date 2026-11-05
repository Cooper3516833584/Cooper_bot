import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import cooper_bot.commands.commands as commands
from test.unit.test_kimi_cli import _runner, _request
from cooper_bot.modules.ai.aisvc import AIService
from cooper_bot.modules.ai.kimi_cli import KimiCliRunner, KimiEmptyReplyError, KimiSecurityViolation, KimiTimeoutError


@pytest.mark.asyncio
async def test_forbidden_tool_error_still_latches_public_profile(tmp_path):
    runner = _runner(tmp_path, "import json\nprint(json.dumps({'type': 'tool_call', 'name': 'Bash', 'isError': True}))\n")
    with pytest.raises(KimiSecurityViolation):
        await runner.run(_request())
    assert runner.public_security_healthy is False


@pytest.mark.parametrize("inline", [False, True])
@pytest.mark.parametrize("final", [False, True])
def test_tool_preamble_is_not_a_final_answer(inline, final):
    rows = [{"role": "assistant", "content": "Let me check."}]
    call = {"id": "call-1", "function": {"name": "WebSearch", "arguments": "{}"}}
    if inline:
        rows[0]["tool_calls"] = [call]
    else:
        rows.append({"role": "assistant", "content": "", "tool_calls": [call]})
    rows.append({"role": "tool", "tool_call_id": "call-1", "content": "search result"})
    if final:
        rows.append({"role": "assistant", "content": "Final answer."})
    raw = "\n".join(json.dumps(row) for row in rows).encode()
    if final:
        assert KimiCliRunner._parse_jsonl(raw, "test")[0] == "Final answer."
    else:
        with pytest.raises(KimiEmptyReplyError):
            KimiCliRunner._parse_jsonl(raw, "test")


@pytest.mark.asyncio
@pytest.mark.parametrize("security_failure", [False, True])
async def test_search_probe_failure_does_not_erase_completed_security_checks(tmp_path, monkeypatch, security_failure):
    svc = AIService(Mock())
    settings = SimpleNamespace(public=SimpleNamespace(workdir=tmp_path), admin_enabled=False, timeout_seconds=1)
    async def run(request):
        if request.purpose == "public_websearch_probe":
            raise (KimiSecurityViolation if security_failure else KimiTimeoutError)("probe")
        return SimpleNamespace(protocol_observed=True, tool_names=())
    svc._kimi_runner = SimpleNamespace(settings=settings, reset_public_security_state=Mock(), run=run)
    svc._kimi_capability_cache_path = tmp_path / "cache.json"
    monkeypatch.setattr(svc, "_kimi_capability_fingerprint", AsyncMock(return_value=({"version": "test"}, True)))
    monkeypatch.setattr("cooper_bot.modules.ai.aisvc.validate_kimi_settings", lambda _: SimpleNamespace(public_profile_valid=True, admin_profile_valid=False, errors=()))
    report = await svc.probe_kimi_capabilities()
    assert svc.chat_ready is (not security_failure)
    assert report.public_websearch_ready is False
    assert ("kimi_public_security_probe_failed" in report.errors) is security_failure
    assert "kimi_websearch_probe_failed" in report.errors


@pytest.mark.asyncio
@pytest.mark.parametrize("memory_enabled", [False, True])
async def test_post_delivery_failure_does_not_send_a_second_reply(monkeypatch, memory_enabled):
    monkeypatch.setattr(commands, "_ai_chat_allows_kimi_computer", lambda *_: True)
    monkeypatch.setattr(commands, "notify_admin_error", AsyncMock())
    reply = AsyncMock(return_value=True)
    monkeypatch.setattr(commands, "reply", reply)
    turn = None
    if memory_enabled:
        turn = AsyncMock()
        turn.__aenter__.return_value = turn
        turn.__aexit__.return_value = False
        turn.is_duplicate = turn.is_stale_or_disabled = False
        turn.acquire_send_permit.return_value = True
        turn.finish_delivery.side_effect = RuntimeError("memory write failed")
    svc = SimpleNamespace(
        computer_ready=True, chat=AsyncMock(), chat_with_context=AsyncMock(return_value="answer"),
        commit_chat_turn=Mock(side_effect=RuntimeError("history write failed")),
        memory=SimpleNamespace(turn=AsyncMock(return_value=turn)) if memory_enabled else None,
        fallback_error_reply="fallback",
    )
    ctx = SimpleNamespace(scene="group", group_id=20001, user_id=10001, nickname="admin", card="")
    await commands._handle_ai_chat_trigger(SimpleNamespace(), ctx, {"self_id": 90001}, "ask", SimpleNamespace(log=Mock()), svc, forced_ai_input="ask")
    reply.assert_awaited_once()
    assert reply.await_args.args[2] == "answer"
    commands.notify_admin_error.assert_awaited_once()
