from __future__ import annotations

import sys
from pathlib import Path

import pytest

from cooper_bot.modules.ai.kimi_cli import (
    KimiCliRunner,
    KimiEmptyReplyError,
    KimiInputTooLargeError,
    KimiProfile,
    KimiProtocolError,
    KimiRunRequest,
    KimiSettings,
    KimiTimeoutError,
)


def _runner(tmp_path: Path, script_body: str, *, argv_budget: int = 24000) -> KimiCliRunner:
    script = tmp_path / "fake_kimi.py"
    script.write_text(script_body, encoding="utf-8")
    home = tmp_path / "home"
    workdir = tmp_path / "workdir"
    skills = home / "empty_skills"
    for path in (home, workdir, skills):
        path.mkdir(parents=True, exist_ok=True)
    agent = tmp_path / "agent.md"
    agent.write_text("---\nsubagents: []\n---\n", encoding="utf-8")
    profile = KimiProfile("public", home, workdir, agent, skills, ("WebSearch",))
    settings = KimiSettings(True, sys.executable, "", profile, profile, 2.0, 2.0, 1, False, False)
    return KimiCliRunner(
        settings,
        executable_resolver=lambda _path: sys.executable,
        argv_prefix=(sys.executable, str(script)),
        argv_budget=argv_budget,
    )


def _request(prompt: str = "hello", timeout_seconds: float = 2.0) -> KimiRunRequest:
    return KimiRunRequest(prompt, "public", timeout_seconds, "req-1", "qq_chat")


@pytest.mark.asyncio
async def test_runner_keeps_prompt_as_one_argv_value_and_filters_tool_events(tmp_path) -> None:
    runner = _runner(
        tmp_path,
        "import json, sys\n"
        "print(json.dumps({'type': 'tool_call', 'name': 'WebSearch'}))\n"
        "print(json.dumps({'type': 'assistant', 'content': sys.argv[-1]}))\n",
    )
    prompt = 'line1\n" && % --not-a-flag 😀'

    result = await runner.run(_request(prompt))

    assert result.text == prompt
    assert result.tool_call_observed is True
    assert result.tool_names == ("WebSearch",)


@pytest.mark.asyncio
async def test_runner_rejects_invalid_jsonl_without_raw_fallback(tmp_path) -> None:
    runner = _runner(tmp_path, "print('not-json')\n")

    with pytest.raises(KimiProtocolError, match="request_id=req-1"):
        await runner.run(_request())


@pytest.mark.asyncio
async def test_runner_requires_a_final_assistant_message(tmp_path) -> None:
    runner = _runner(tmp_path, "import json\nprint(json.dumps({'type': 'tool_result', 'name': 'WebSearch'}))\n")

    with pytest.raises(KimiEmptyReplyError, match="request_id=req-1"):
        await runner.run(_request())


@pytest.mark.asyncio
async def test_runner_timeout_releases_its_slot(tmp_path) -> None:
    runner = _runner(tmp_path, "import time\ntime.sleep(5)\n")

    with pytest.raises(KimiTimeoutError, match="request_id=req-1"):
        await runner.run(_request(timeout_seconds=0.1))

    await runner.aclose()


@pytest.mark.asyncio
async def test_runner_rejects_oversized_windows_argument_before_spawn(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, "raise SystemExit('must not start')\n", argv_budget=1024)
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.os.name", "nt")

    with pytest.raises(KimiInputTooLargeError, match="request_id=req-1"):
        await runner.run(_request("😀" * 1000))
