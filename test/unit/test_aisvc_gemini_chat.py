from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from cooper_bot.modules.ai.aisvc import AIService


class _DummyLog:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return


def _new_service(work_root) -> AIService:
    svc = AIService(log=_DummyLog())
    svc.system_prompt = "system-prompt"
    svc.gemini_cli_path = "gemini"
    svc.gemini_model = "Gemini Test Model"
    svc.claude_model = "Claude Opus 4.6 (Thinking)"
    svc.gemini_policy_path = work_root / "gemini-policy.toml"
    svc.gemini_policy_path.write_text('[[rule]]\ntoolName = "read_file"\ndecision = "deny"\npriority = 999\n', encoding="utf-8")
    svc.gemini_workdir = work_root / "gemini-workdir"
    svc.gemini_restricted_workdir = work_root / "gemini-restricted-workdir"
    return svc


def test_gemini_chat_with_context_uses_history_and_prompts(monkeypatch, tmp_project_root) -> None:
    svc = _new_service(tmp_project_root)
    prompts: list[str] = []
    models: list[str] = []
    restricted_flags: list[bool] = []
    seq = {"n": 0}

    def _fake_run(
        prompt: str,
        model_name: str | None = None,
        restricted: bool = False,
        auto_approve_tools: bool = False,
    ) -> str:
        seq["n"] += 1
        prompts.append(prompt)
        models.append(str(model_name or ""))
        restricted_flags.append(bool(restricted))
        return f"reply-{seq['n']}"

    monkeypatch.setattr(svc, "_resolve_gemini_cli_executable", lambda: "gemini")
    monkeypatch.setattr(svc, "_run_gemini_cli_sync", _fake_run)
    monkeypatch.setattr(
        svc,
        "_load_group_chat_prompt_config",
        lambda: {"default": "", "groups": {"20001": "group-special"}},
    )

    first = svc._gemini_chat_with_context_sync("group:20001", "hello")
    assert first == "reply-1"

    second = svc._gemini_chat_with_context_sync("group:20001", "follow-up")
    assert second == "reply-2"

    third = svc._gemini_chat_with_context_sync("group:20001", "claude follow-up", "claude")
    assert third == "reply-3"

    fourth = svc._gemini_chat_with_context_sync("group:20001", "restricted follow-up", "gemini", True)
    assert fourth == "reply-4"

    assert len(prompts) == 4
    assert models == [
        "Gemini Test Model",
        "Gemini Test Model",
        "Claude Opus 4.6 (Thinking)",
        "Gemini Test Model",
    ]
    assert restricted_flags == [False, False, False, True]
    assert "google_web_search" in prompts[0]
    assert "System instructions:\ngroup-special" in prompts[0]
    assert "Latest user request:\nhello" in prompts[0]
    assert "Conversation history" in prompts[1]
    assert "User:\nhello" in prompts[1]
    assert "Assistant:\nreply-1" in prompts[1]
    assert "Latest user request:\nfollow-up" in prompts[1]
    assert "Assistant:\nreply-2" in prompts[2]
    assert "Latest user request:\nclaude follow-up" in prompts[2]
    assert "Security policy for this QQ bot request" in prompts[3]
    assert "Assistant:\nreply-3" in prompts[3]
    assert "Latest user request:\nrestricted follow-up" in prompts[3]

    history = svc._load_active_chat_history("group:20001")
    assert [m["content"] for m in history] == [
        "hello",
        "reply-1",
        "follow-up",
        "reply-2",
        "claude follow-up",
        "reply-3",
        "restricted follow-up",
        "reply-4",
    ]


def test_restricted_gemini_prompt_blocks_local_tools(tmp_project_root) -> None:
    svc = _new_service(tmp_project_root)

    prompt = svc._build_restricted_gemini_cli_prompt("system", [], "查一下今天新闻")

    assert "google_web_search only" in prompt
    assert "run_shell_command" in prompt
    assert "read_file" in prompt
    assert "Do not inspect, modify, execute" in prompt


def test_restricted_calendar_chat_is_stateless_and_uses_its_own_timeout(monkeypatch, tmp_project_root) -> None:
    svc = _new_service(tmp_project_root)
    captured: dict[str, object] = {}

    def _fake_run(prompt: str, model_name: str | None = None, restricted: bool = False, timeout_seconds=None) -> str:
        captured["prompt"] = prompt
        captured["model"] = model_name
        captured["restricted"] = restricted
        captured["timeout"] = timeout_seconds
        return '{"date":"2026-06-26","events":[]}'

    monkeypatch.setattr(svc, "_resolve_gemini_cli_executable", lambda: "agy")
    monkeypatch.setattr(svc, "_run_gemini_cli_sync", _fake_run)

    out = svc._restricted_gemini_calendar_chat_sync("只返回 JSON", "claude", 45)

    assert out == '{"date":"2026-06-26","events":[]}'
    assert captured["model"] == "Claude Opus 4.6 (Thinking)"
    assert captured["restricted"] is True
    assert captured["timeout"] == 45
    assert "Security policy for this QQ bot request" in str(captured["prompt"])
    assert "Latest user request:\n只返回 JSON" in str(captured["prompt"])


def test_run_gemini_cli_sync_parses_json_response(monkeypatch, tmp_project_root) -> None:
    svc = _new_service(tmp_project_root)
    captured: dict[str, object] = {}

    def _fake_subprocess_run(cmd, cwd, stdout, stderr, timeout, check, env=None, creationflags=0):
        captured["cmd"] = list(cmd)
        captured["cwd"] = cwd
        captured["stdout"] = stdout
        captured["stderr"] = stderr
        captured["timeout"] = timeout
        captured["check"] = check
        captured["env"] = env
        captured["creationflags"] = creationflags
        return SimpleNamespace(returncode=0, stdout=b'{"response":"OK"}', stderr=b"warning")

    monkeypatch.setattr(svc, "_resolve_gemini_cli_executable", lambda: "C:/tools/gemini.cmd")
    monkeypatch.setattr("cooper_bot.modules.ai.aisvc.subprocess.run", _fake_subprocess_run)

    out = svc._run_gemini_cli_sync("Reply exactly OK")

    assert out == "OK"
    cmd = [str(x) for x in captured["cmd"]]
    cli_idx = next(i for i, c in enumerate(cmd) if Path(c).as_posix() == "C:/tools/gemini.cmd")
    assert cmd[cli_idx + 1:cli_idx + 3] == ["-p", "Reply exactly OK"]
    assert cmd[cli_idx + 3:cli_idx + 5] == ["--model", "Gemini Test Model"]
    assert captured["cwd"] == str(svc.gemini_workdir)
    assert captured["creationflags"] == 0


def test_run_gemini_cli_sync_reads_agy_transcript_when_stdout_empty(monkeypatch, tmp_project_root) -> None:
    svc = _new_service(tmp_project_root)
    captured: dict[str, object] = {}
    conv_id = "7dac489a-31d4-41f8-b9d4-73541c44697a"

    def _fake_subprocess_run(cmd, cwd, stdout, stderr, timeout, check, env=None, creationflags=0):
        captured["cmd"] = list(cmd)
        captured["creationflags"] = creationflags
        log_path = Path(cmd[cmd.index("--log-file") + 1])
        app_dir = tmp_project_root / "agy-app"
        transcript = app_dir / "brain" / conv_id / ".system_generated" / "logs" / "transcript.jsonl"
        transcript.parent.mkdir(parents=True, exist_ok=True)
        transcript.write_text(
            json.dumps({"source": "MODEL", "content": "OK from transcript"}, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        log_path.write_text(
            f"CLI app data directory: {app_dir}\nI0608 server.go:753] Created conversation {conv_id}\n",
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(svc, "_resolve_gemini_cli_executable", lambda: "agy")
    monkeypatch.setattr("cooper_bot.modules.ai.aisvc.subprocess.run", _fake_subprocess_run)

    out = svc._run_gemini_cli_sync("Reply exactly OK", auto_approve_tools=True)

    assert out == "OK from transcript"
    cmd = [str(x) for x in captured["cmd"]]
    assert "--log-file" in cmd
    assert "--dangerously-skip-permissions" in cmd
    assert "--model" in cmd
    assert captured["creationflags"] == getattr(__import__("subprocess"), "CREATE_NO_WINDOW", 0)


def test_run_gemini_cli_sync_restricted_uses_agy_sandbox(monkeypatch, tmp_project_root) -> None:
    svc = _new_service(tmp_project_root)
    captured: dict[str, object] = {}

    def _fake_subprocess_run(cmd, cwd, stdout, stderr, timeout, check, env=None, creationflags=0):
        captured["cmd"] = list(cmd)
        captured["cwd"] = cwd
        captured["creationflags"] = creationflags
        return SimpleNamespace(returncode=0, stdout=b"OK", stderr=b"")

    monkeypatch.setattr(svc, "_resolve_gemini_cli_executable", lambda: "agy")
    monkeypatch.setattr("cooper_bot.modules.ai.aisvc.subprocess.run", _fake_subprocess_run)

    out = svc._run_gemini_cli_sync(
        "Reply exactly OK",
        "Claude Opus 4.6 (Thinking)",
        restricted=True,
        auto_approve_tools=True,
    )

    assert out == "OK"
    cmd = [str(x) for x in captured["cmd"]]
    assert "--sandbox" in cmd
    assert "--dangerously-skip-permissions" not in cmd
    assert cmd[cmd.index("--model") + 1] == "Claude Opus 4.6 (Thinking)"
    assert captured["cwd"] == str(svc.gemini_restricted_workdir)
    assert captured["creationflags"] == getattr(__import__("subprocess"), "CREATE_NO_WINDOW", 0)


def test_run_gemini_cli_sync_reports_agy_log_error_when_empty(monkeypatch, tmp_project_root) -> None:
    svc = _new_service(tmp_project_root)
    conv_id = "e52ea308-be06-4058-a879-86c4ac7f2692"

    def _fake_subprocess_run(cmd, cwd, stdout, stderr, timeout, check, env=None, creationflags=0):
        log_path = Path(cmd[cmd.index("--log-file") + 1])
        log_path.write_text(
            "\n".join(
                [
                    f"I0608 server.go:753] Created conversation {conv_id}",
                    "E0608 log.go:398] agent executor error: UNAVAILABLE (code 503): No capacity available for model claude-opus-4-6-thinking on the server",
                ]
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(svc, "_resolve_gemini_cli_executable", lambda: "agy")
    monkeypatch.setattr("cooper_bot.modules.ai.aisvc.subprocess.run", _fake_subprocess_run)

    with pytest.raises(RuntimeError, match="No capacity available"):
        svc._run_gemini_cli_sync("Reply exactly OK", "Claude Opus 4.6 (Thinking)")


def test_run_gemini_cli_sync_rejects_agy_busy_transcript(monkeypatch, tmp_project_root) -> None:
    svc = _new_service(tmp_project_root)
    conv_id = "7dac489a-31d4-41f8-b9d4-73541c44697a"

    def _fake_subprocess_run(cmd, cwd, stdout, stderr, timeout, check, env=None, creationflags=0):
        log_path = Path(cmd[cmd.index("--log-file") + 1])
        app_dir = tmp_project_root / "agy-app"
        transcript = app_dir / "brain" / conv_id / ".system_generated" / "logs" / "transcript.jsonl"
        transcript.parent.mkdir(parents=True, exist_ok=True)
        transcript.write_text(
            json.dumps({"source": "MODEL", "content": "Our servers are experiencing high traffic right now, please try again in a minute."}) + "\n",
            encoding="utf-8",
        )
        log_path.write_text(
            f"CLI app data directory: {app_dir}\nI0608 server.go:753] Created conversation {conv_id}\n",
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(svc, "_resolve_gemini_cli_executable", lambda: "agy")
    monkeypatch.setattr("cooper_bot.modules.ai.aisvc.subprocess.run", _fake_subprocess_run)

    with pytest.raises(RuntimeError, match="service busy"):
        svc._run_gemini_cli_sync("Reply exactly OK")


def test_run_gemini_cli_sync_preserves_agy_cursor_line_breaks(monkeypatch, tmp_project_root) -> None:
    svc = _new_service(tmp_project_root)
    raw = "可用命令：\x1b[1E- /help\x1b[1E- /ping\x1b[2E结束"

    def _fake_subprocess_run(cmd, cwd, stdout, stderr, timeout, check, env=None, creationflags=0):
        return SimpleNamespace(returncode=0, stdout=raw.encode("utf-8"), stderr=b"")

    monkeypatch.setattr(svc, "_resolve_gemini_cli_executable", lambda: "agy")
    monkeypatch.setattr("cooper_bot.modules.ai.aisvc.subprocess.run", _fake_subprocess_run)

    out = svc._run_gemini_cli_sync("列出命令")

    assert out == "可用命令：\n- /help\n- /ping\n\n结束"


def test_build_gemini_cli_base_command_prefers_node_bundle_for_cmd(monkeypatch, tmp_project_root) -> None:
    svc = _new_service(tmp_project_root)
    cli_dir = tmp_project_root / "gemini-cli"
    cli_dir.mkdir(parents=True, exist_ok=True)
    cli_cmd = cli_dir / "gemini.cmd"
    node_exe = cli_dir / "node.exe"
    js_path = cli_dir / "node_modules" / "@google" / "gemini-cli" / "bundle" / "gemini.js"
    cli_cmd.write_text("@echo off\n", encoding="utf-8")
    node_exe.write_text("", encoding="utf-8")
    js_path.parent.mkdir(parents=True, exist_ok=True)
    js_path.write_text("", encoding="utf-8")

    monkeypatch.setattr(svc, "_resolve_gemini_cli_executable", lambda: str(cli_cmd))

    assert svc._build_gemini_cli_base_command() == [str(node_exe), str(js_path)]
