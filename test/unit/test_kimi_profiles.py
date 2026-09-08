from __future__ import annotations

from pathlib import Path

from cooper_bot.modules.ai.kimi_cli import (
    ADMIN_TOOLS,
    PUBLIC_TOOLS,
    KimiProfile,
    KimiSettings,
    build_kimi_env,
    validate_kimi_settings,
)


def _write_agent(path: Path, tools: tuple[str, ...]) -> None:
    path.write_text("---\n" + "\n".join(f"- {tool}" for tool in tools) + "\nsubagents: []\n---\n", encoding="utf-8")


def _settings(tmp_path: Path, *, public_workdir: Path | None = None) -> KimiSettings:
    public_home = tmp_path / "public_home"
    admin_home = tmp_path / "admin_home"
    work_public = public_workdir or (tmp_path / "outside_project")
    work_admin = tmp_path / "admin_workdir"
    for path in (public_home, admin_home, work_public, work_admin, public_home / "empty_skills", admin_home / "empty_skills"):
        path.mkdir(parents=True, exist_ok=True)
    public_agent = tmp_path / "public.md"
    admin_agent = tmp_path / "admin.md"
    _write_agent(public_agent, PUBLIC_TOOLS)
    _write_agent(admin_agent, ADMIN_TOOLS)
    return KimiSettings(
        enabled=True,
        cli_path="kimi-test",
        expected_version="0.34.0",
        model="",
        public=KimiProfile("public", public_home, work_public, public_agent, public_home / "empty_skills", PUBLIC_TOOLS),
        admin=KimiProfile("admin", admin_home, work_admin, admin_agent, admin_home / "empty_skills", ADMIN_TOOLS),
        timeout_seconds=120.0,
        admin_timeout_seconds=480.0,
        max_concurrency=3,
        admin_enabled=False,
        allow_group_computer=False,
    )


def test_static_profile_validation_keeps_runtime_capabilities_unverified(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.PROJECT_ROOT", tmp_path / "project")

    readiness = validate_kimi_settings(settings, executable_resolver=lambda _path: "C:/tools/kimi.exe")

    assert readiness.configured is True
    assert readiness.public_profile_valid is True
    assert readiness.admin_profile_valid is True
    assert readiness.calendar_web_ready is False
    assert readiness.computer_ready is False


def test_public_workdir_inside_project_is_rejected(tmp_path, monkeypatch) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    settings = _settings(tmp_path, public_workdir=project_root / "runtime")
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.PROJECT_ROOT", project_root)

    readiness = validate_kimi_settings(settings, executable_resolver=lambda _path: "C:/tools/kimi.exe")

    assert readiness.configured is False
    assert "kimi_public_workdir_inside_project" in readiness.errors


def test_public_agent_without_subagent_disable_is_rejected(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    settings.public.agent_path.write_text("tools:\n  - WebSearch\n", encoding="utf-8")
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.PROJECT_ROOT", tmp_path / "project")

    readiness = validate_kimi_settings(settings, executable_resolver=lambda _path: "C:/tools/kimi.exe")

    assert readiness.public_profile_valid is False
    assert "public_agent_subagents_not_disabled" in readiness.errors


def test_child_environment_keeps_only_explicit_system_values(tmp_path) -> None:
    settings = _settings(tmp_path)

    env = build_kimi_env(
        settings.public,
        {
            "PATH": "C:/trusted/bin",
            "SystemRoot": "C:/Windows",
            "TOKEN": "qq-token",
            "MAIL_PASSWORD": "mail-password",
            "KIMI_CODE_LEGACY_FLAG": "legacy-engine",
        },
    )

    assert env == {
        "PATH": "C:/trusted/bin",
        "SystemRoot": "C:/Windows",
        "KIMI_CODE_HOME": str(settings.public.home),
    }
