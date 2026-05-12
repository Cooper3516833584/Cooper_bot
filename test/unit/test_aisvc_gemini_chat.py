from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from aisvc import AIService


class _DummyLog:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return


def _new_service(work_root) -> AIService:
    svc = AIService(log=_DummyLog())
    svc.system_prompt = "system-prompt"
    svc.gemini_cli_path = "gemini"
    svc.gemini_policy_path = work_root / "gemini-policy.toml"
    svc.gemini_policy_path.write_text('[[rule]]\ntoolName = "read_file"\ndecision = "deny"\npriority = 999\n', encoding="utf-8")
    svc.gemini_workdir = work_root / "gemini-workdir"
    return svc


def test_gemini_chat_with_context_ignores_history_and_prompts(monkeypatch, tmp_project_root) -> None:
    svc = _new_service(tmp_project_root)
    prompts: list[str] = []
    seq = {"n": 0}

    def _fake_run(prompt: str) -> str:
        seq["n"] += 1
        prompts.append(prompt)
        return f"reply-{seq['n']}"

    monkeypatch.setattr(svc, "_resolve_gemini_cli_executable", lambda: "gemini")
    monkeypatch.setattr(svc, "_run_gemini_cli_sync", _fake_run)
    monkeypatch.setattr(svc, "_load_active_chat_history", lambda _session_key: (_ for _ in ()).throw(AssertionError("no history")))
    monkeypatch.setattr(svc, "_select_chat_system_prompt", lambda _session_key: (_ for _ in ()).throw(AssertionError("no prompt")))
    monkeypatch.setattr(svc, "_save_chat_turn", lambda *_args: (_ for _ in ()).throw(AssertionError("no context save")))

    first = svc._gemini_chat_with_context_sync("group:20001", "hello")
    assert first == "reply-1"

    second = svc._gemini_chat_with_context_sync("group:20001", "follow-up")
    assert second == "reply-2"

    assert len(prompts) == 2
    assert "google_web_search" in prompts[0]
    assert prompts[0].endswith("hello")
    assert "system-prompt" not in prompts[0]
    assert "Assistant:" not in prompts[1]
    assert prompts[1].endswith("follow-up")


def test_run_gemini_cli_sync_parses_json_response(monkeypatch, tmp_project_root) -> None:
    svc = _new_service(tmp_project_root)
    captured: dict[str, object] = {}

    def _fake_subprocess_run(cmd, cwd, stdout, stderr, text, encoding, errors, timeout, check):
        captured["cmd"] = list(cmd)
        captured["cwd"] = cwd
        captured["stdout"] = stdout
        captured["stderr"] = stderr
        captured["text"] = text
        captured["encoding"] = encoding
        captured["errors"] = errors
        captured["timeout"] = timeout
        captured["check"] = check
        return SimpleNamespace(returncode=0, stdout='{"response":"OK"}', stderr="warning")

    monkeypatch.setattr(svc, "_resolve_gemini_cli_executable", lambda: "C:/tools/gemini.cmd")
    monkeypatch.setattr("aisvc.subprocess.run", _fake_subprocess_run)

    out = svc._run_gemini_cli_sync("Reply exactly OK")

    assert out == "OK"
    cmd = [str(x) for x in captured["cmd"]]
    assert Path(cmd[0]).as_posix() == "C:/tools/gemini.cmd"
    assert cmd[1:5] == ["-p", "Reply exactly OK", "-o", "json"]
    assert cmd[5:7] == ["--approval-mode", "default"]
    assert "--policy" in cmd
    assert captured["cwd"] == str(svc.gemini_workdir)
    assert captured["encoding"] == "utf-8"


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
