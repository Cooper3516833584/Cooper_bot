from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

import pytest

from cooper_bot.modules.ai.kimi_cli import (
    KimiCliRunner,
    KimiEmptyReplyError,
    KimiInputTooLargeError,
    KimiProfile,
    KimiProtocolError,
    KimiRunRequest,
    KimiSecurityViolation,
    KimiSettings,
    KimiTimeoutError,
    build_kimi_env,
    detect_kimi_runtime_info,
)


def _runner(
    tmp_path: Path,
    script_body: str,
    *,
    argv_budget: int = 24000,
    admin_enabled: bool = False,
) -> KimiCliRunner:
    tmp_path.mkdir(parents=True, exist_ok=True)
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
    settings = KimiSettings(True, sys.executable, "", profile, profile, 2.0, 2.0, 1, admin_enabled, False)
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
    prompt = 'line1\n--agent-file C:/attacker/admin.md KIMI_CODE_HOME=C:/attacker Bash " && % --not-a-flag 😀'

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


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", ("Bash", "Read", "Write", "Edit", "FetchURL", "Agent"))
async def test_public_forbidden_tool_discards_reply_and_latches_unhealthy(tmp_path, tool_name) -> None:
    runner = _runner(
        tmp_path,
        "import json\n"
        f"print(json.dumps({{'type': 'assistant', 'message': {{'role': 'assistant', 'tool_calls': [{{'name': {tool_name!r}}}]}}}}))\n"
        "print(json.dumps({'type': 'assistant', 'content': 'must not return'}))\n",
    )

    with pytest.raises(KimiSecurityViolation, match="request_id=req-1") as exc_info:
        await runner.run(_request())
    assert exc_info.value.detail == "forbidden_tool_observed"
    assert runner.public_security_healthy is False

    with pytest.raises(KimiSecurityViolation, match="request_id=req-1") as unhealthy:
        await runner.run(_request())
    assert unhealthy.value.detail == "public_profile_unhealthy"


@pytest.mark.asyncio
async def test_public_websearch_is_allowed_and_admin_bash_is_not_restricted(tmp_path) -> None:
    runner = _runner(
        tmp_path,
        "import json\n"
        "print(json.dumps({'type': 'tool_call', 'name': 'WebSearch'}))\n"
        "print(json.dumps({'type': 'assistant', 'content': 'ok'}))\n",
    )

    assert (await runner.run(_request())).tool_names == ("WebSearch",)
    admin_runner = _runner(
        tmp_path / "admin",
        "import json\n"
        "print(json.dumps({'type': 'tool_call', 'name': 'Bash'}))\n"
        "print(json.dumps({'type': 'assistant', 'content': 'ok'}))\n",
        admin_enabled=True,
    )
    assert (await admin_runner.run(KimiRunRequest("hello", "admin", 2.0, "admin-1", "computer_probe"))).text == "ok"


@pytest.mark.parametrize(
    ("events", "expected_tools", "expected_text"),
    [
        ([{"type": "tool_call", "name": "WebSearch"}, {"type": "assistant", "content": "ok"}], ("WebSearch",), "ok"),
        ([{"type": "tool", "tool_name": "WebSearch"}, {"type": "assistant", "content": "ok"}], ("WebSearch",), "ok"),
        ([{"type": "assistant", "message": {"role": "assistant", "tool_calls": [{"name": "Bash"}]}}, {"type": "assistant", "content": "ok"}], ("Bash",), "ok"),
        ([{"message": {"role": "assistant", "tool_calls": [{"function": {"name": "Read", "arguments": "{}"}}]}}, {"type": "assistant", "content": "ok"}], ("Read",), "ok"),
        ([{"tool_calls": [{"function": {"name": "WebSearch"}}]}, {"message": {"role": "assistant", "content": [{"type": "text", "text": "block answer"}]}}], ("WebSearch",), "block answer"),
        ([{"type": "assistant", "content": "<system_urp_calling>\n<url>https://www.google.com/search?q=capital+of+France</url>\n<method>GET</method>\n</system_urp_calling>\nParis"}], ("WebSearch",), "Paris"),
    ],
)
def test_stream_json_parser_accepts_legacy_and_message_tool_calls(events, expected_tools, expected_text) -> None:
    raw = ("\n".join(__import__("json").dumps(event) for event in events) + "\n").encode()

    text, tools, observed, protocol = KimiCliRunner._parse_jsonl(raw, "req-1")

    assert text == expected_text
    assert tools == expected_tools
    assert observed is True
    assert protocol is True


def test_stream_json_parser_rejects_unknown_assistant_tool_schema() -> None:
    raw = b'{"type":"assistant","content":"ok","tool_use":{"id":"unknown"}}\n'

    with pytest.raises(KimiProtocolError, match="request_id=req-1") as exc_info:
        KimiCliRunner._parse_jsonl(raw, "req-1")

    assert exc_info.value.detail == "unrecognized_tool_schema"


def test_stream_json_parser_fails_closed_on_unknown_legacy_urp_target() -> None:
    raw = b'{"type":"assistant","content":"<system_urp_calling><url>https://example.test/private</url><method>GET</method></system_urp_calling>ok"}\n'

    text, tools, observed, protocol = KimiCliRunner._parse_jsonl(raw, "req-1")

    assert text == "ok"
    assert tools == ("UnknownURP",)
    assert observed is True
    assert protocol is True


@pytest.mark.asyncio
async def test_admin_profile_is_refused_while_admin_is_globally_disabled(tmp_path) -> None:
    sentinel = tmp_path / "admin_child_started.txt"
    runner = _runner(
        tmp_path,
        "from pathlib import Path\n"
        f"Path({str(sentinel)!r}).write_text('started', encoding='utf-8')\n",
    )

    with pytest.raises(KimiSecurityViolation) as exc_info:
        await runner.run(KimiRunRequest("hello", "admin", 2.0, "admin-1", "computer_probe"))

    assert exc_info.value.code == "kimi_security_violation"
    assert exc_info.value.detail == "admin_disabled"
    assert exc_info.value.request_id == "admin-1"
    assert sentinel.exists() is False
    assert runner._processes == set()


@pytest.mark.asyncio
async def test_public_profile_still_runs_while_admin_is_globally_disabled(tmp_path) -> None:
    runner = _runner(
        tmp_path,
        "import json\nprint(json.dumps({'type': 'assistant', 'content': 'ok'}))\n",
    )

    assert runner.settings.admin_enabled is False
    assert (await runner.run(_request())).text == "ok"


@pytest.mark.asyncio
async def test_stdout_overflow_terminates_child_fast_and_releases_its_slot(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli._MAX_STDOUT_BYTES", 64)
    runner = _runner(
        tmp_path,
        "import json, sys\n"
        "from pathlib import Path\n"
        "if (Path(__file__).parent / 'mode.txt').exists():\n"
        "    print(json.dumps({'type': 'assistant', 'content': 'ok'}))\n"
        "else:\n"
        "    chunk = b'x' * 65536\n"
        "    while True:\n"
        "        sys.stdout.buffer.write(chunk)\n"
        "        sys.stdout.buffer.flush()\n",
    )
    tasks_before = asyncio.all_tasks()

    started = time.monotonic()
    with pytest.raises(KimiProtocolError) as exc_info:
        await runner.run(_request(timeout_seconds=10.0))
    elapsed = time.monotonic() - started

    assert exc_info.value.detail == "stdout_limit"
    assert exc_info.value.request_id == "req-1"
    assert elapsed < 5.0, f"stdout overflow took {elapsed:.2f}s, the child was not terminated promptly"
    assert runner._processes == set()

    (tmp_path / "mode.txt").write_text("ok", encoding="utf-8")
    follow_up = KimiRunRequest("hello", "public", 2.0, "req-2", "qq_chat")
    assert (await runner.run(follow_up)).text == "ok"

    await runner.aclose()
    await asyncio.sleep(0)
    current = asyncio.current_task()
    pending = {task for task in asyncio.all_tasks() if task is not current and task not in tasks_before}
    assert pending == set()


class _FakeVersionProcess:
    def __init__(self, stdout: bytes, returncode: int = 0) -> None:
        self._stdout = stdout
        self.returncode = returncode

    async def communicate(self) -> tuple[bytes, bytes]:
        return self._stdout, b""


def _narrow_profile(tmp_path: Path) -> KimiProfile:
    home = tmp_path / "narrow_home"
    workdir = tmp_path / "narrow_workdir"
    skills = home / "empty_skills"
    for path in (home, workdir, skills):
        path.mkdir(parents=True, exist_ok=True)
    agent = tmp_path / "narrow_agent.md"
    agent.write_text("---\nsubagents: []\n---\n", encoding="utf-8")
    return KimiProfile("public", home, workdir, agent, skills, ("WebSearch",))


@pytest.mark.asyncio
async def test_runtime_version_probe_uses_the_injected_narrow_environment(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("KIMI_TEST_PARENT_SECRET", "leaked-secret")
    captured: dict[str, object] = {}

    async def fake_spawn(*argv: object, **kwargs: object) -> _FakeVersionProcess:
        captured["env"] = kwargs.get("env")
        return _FakeVersionProcess(b"kimi 9.9.9\n")

    monkeypatch.setattr("asyncio.create_subprocess_exec", fake_spawn)
    profile = _narrow_profile(tmp_path)
    env = build_kimi_env(profile, os.environ)

    info = await detect_kimi_runtime_info("kimi", executable_resolver=lambda _path: sys.executable, env=env)

    assert info.version == "kimi 9.9.9"
    assert os.environ["KIMI_TEST_PARENT_SECRET"] == "leaked-secret"
    child_env = captured["env"]
    assert isinstance(child_env, dict)
    assert "KIMI_TEST_PARENT_SECRET" not in child_env
    assert child_env["PATH"] == env["PATH"]
    assert child_env["KIMI_CODE_HOME"] == str(profile.home)


@pytest.mark.asyncio
async def test_runtime_version_probe_keeps_env_optional_and_degrades_to_unknown(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_spawn(*argv: object, **kwargs: object) -> _FakeVersionProcess:
        captured["env"] = kwargs.get("env")
        return _FakeVersionProcess(b"", returncode=1)

    monkeypatch.setattr("asyncio.create_subprocess_exec", fake_spawn)

    info = await detect_kimi_runtime_info("kimi", executable_resolver=lambda _path: sys.executable)

    assert captured["env"] is None
    assert info.version == "unknown"


