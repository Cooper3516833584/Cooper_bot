from __future__ import annotations

from pathlib import Path
import asyncio
import json
from dataclasses import replace

import pytest

from cooper_bot.modules.ai.kimi_cli import (
    ADMIN_TOOLS,
    FEISHU_APIS,
    FEISHU_ENABLED_TOOLS,
    FEISHU_PUBLIC_TOOLS,
    PUBLIC_TOOLS,
    KimiProfile,
    KimiSettings,
    build_kimi_env,
    detect_kimi_runtime_info,
    probe_feishu_mcp_tools,
    validate_kimi_settings,
)


def _write_agent(path: Path, tools: tuple[str, ...]) -> None:
    path.write_text("---\ntools:\n" + "\n".join('  - "*"' if tool == "*" else f"  - {tool}" for tool in tools) + "\nsubagents: []\n---\n", encoding="utf-8")


def _settings(tmp_path: Path, *, public_workdir: Path | None = None) -> KimiSettings:
    public_home = tmp_path / "public_home"
    admin_home = tmp_path / "admin_home"
    work_public = public_workdir or (tmp_path / "outside_project")
    work_admin = tmp_path / "admin_workdir"
    for path in (public_home, admin_home, work_public, work_admin, public_home / "empty_skills", admin_home / "empty_skills"):
        path.mkdir(parents=True, exist_ok=True)
    (public_home / "config.toml").write_text(
        'merge_all_available_skills = false\n'
        'builtin_product_skills = false\n\n'
        '[tools]\n'
        'enabled = ["WebSearch"]\n',
        encoding="utf-8",
    )
    public_agent = tmp_path / "public.md"
    admin_agent = tmp_path / "admin.md"
    _write_agent(public_agent, PUBLIC_TOOLS)
    _write_agent(admin_agent, ADMIN_TOOLS)
    return KimiSettings(
        enabled=True,
        cli_path="kimi-test",
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


def test_admin_full_tools_does_not_require_empty_skills_or_subagent_disable(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    settings.admin.agent_path.write_text('---\ntools:\n  - "*"\n---\n', encoding="utf-8")
    settings.admin.skills_dir.rmdir()
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.PROJECT_ROOT", tmp_path / "project")

    readiness = validate_kimi_settings(settings, executable_resolver=lambda _path: "C:/tools/kimi.exe")

    assert readiness.admin_profile_valid is True
    assert readiness.public_profile_valid is True


def test_admin_full_tools_rejects_the_old_restricted_agent(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _write_agent(settings.admin.agent_path, ("Read", "Bash"))
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.PROJECT_ROOT", tmp_path / "project")

    readiness = validate_kimi_settings(settings, executable_resolver=lambda _path: "C:/tools/kimi.exe")

    assert readiness.admin_profile_valid is False
    assert readiness.public_profile_valid is True


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
            "KIMI_CODE_EXPERIMENTAL_FLAG": "1",
        },
    )

    assert env == {
        "PATH": "C:/trusted/bin",
        "SystemRoot": "C:/Windows",
        "KIMI_CODE_HOME": str(settings.public.home),
    }
    assert "KIMI_CODE_EXPERIMENTAL_FLAG" not in env


def test_feishu_credentials_are_opt_in_and_public_profile_only(tmp_path):
    settings = _settings(tmp_path)
    fake = {"APP_ID": "cli_TEST1234567890", "APP_SECRET": "TEST_ONLY_SECRET"}

    public_env = build_kimi_env(settings.public, {"PATH": "trusted"}, feishu_credentials=fake)
    admin_env = build_kimi_env(settings.admin, {"PATH": "trusted"}, feishu_credentials=fake)

    assert public_env["APP_ID"] == fake["APP_ID"]
    assert public_env["APP_SECRET"] == fake["APP_SECRET"]
    assert "APP_ID" not in admin_env
    assert "APP_SECRET" not in admin_env


@pytest.mark.parametrize(
    ("config_text", "expected_error"),
    [
        ('[tools]\nenabled = []\n', "kimi_public_tools_policy_unsafe"),
        ('[tools]\nenabled = ["WebSearch", "Bash"]\n', "kimi_public_tools_policy_unsafe"),
        ('merge_all_available_skills = false\nbuiltin_product_skills = false\n', "kimi_public_tools_policy_missing"),
    ],
)
def test_public_policy_rejects_unsafe_tool_allowlists(tmp_path, monkeypatch, config_text, expected_error) -> None:
    settings = _settings(tmp_path)
    settings.public.home.joinpath("config.toml").write_text(config_text, encoding="utf-8")
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.PROJECT_ROOT", tmp_path / "project")

    readiness = validate_kimi_settings(settings, executable_resolver=lambda _path: "C:/tools/kimi.exe")

    assert readiness.public_profile_valid is False
    assert expected_error in readiness.errors


@pytest.mark.parametrize(
    ("extra", "expected"),
    [
        ('disabled = ["WebSearch"]\n', "kimi_public_tools_disabled_unsafe"),
        ('disabled = "Bash"\n', "kimi_public_tools_disabled_invalid"),
        ('[permission]\nrules = [{ tool = "WebSearch", decision = "deny" }]\n', "kimi_public_permission_rule_blocks_tool"),
        ('[permission]\nrules = [{ tool = "WebSearch", decision = "ask" }]\n', "kimi_public_permission_rule_blocks_tool"),
    ],
)
def test_public_policy_rejects_disabled_or_noninteractive_permission_rules(tmp_path, monkeypatch, extra, expected):
    settings = _settings(tmp_path)
    path = settings.public.home / "config.toml"
    path.write_text(path.read_text(encoding="utf-8") + "\n" + extra, encoding="utf-8")
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.PROJECT_ROOT", tmp_path / "project")

    readiness = validate_kimi_settings(settings, executable_resolver=lambda _path: "C:/tools/kimi.exe")

    assert readiness.public_profile_valid is False
    assert expected in readiness.errors


def test_public_agent_rejects_disallowed_tools(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    settings.public.agent_path.write_text(
        "---\ntools:\n  - WebSearch\ndisallowedTools:\n  - WebSearch\nsubagents: []\n---\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.PROJECT_ROOT", tmp_path / "project")

    readiness = validate_kimi_settings(settings, executable_resolver=lambda _path: "C:/tools/kimi.exe")

    assert readiness.public_profile_valid is False
    assert "public_agent_disallowed_tools" in readiness.errors


def test_public_policy_rejects_mcp_and_nonempty_skills(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    settings.public.home.joinpath("mcp.json").write_text('{"mcpServers":{"unsafe":{}}}', encoding="utf-8")
    settings.public.skills_dir.joinpath("skill.md").write_text("unsafe", encoding="utf-8")
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.PROJECT_ROOT", tmp_path / "project")

    readiness = validate_kimi_settings(settings, executable_resolver=lambda _path: "C:/tools/kimi.exe")

    assert readiness.public_profile_valid is False
    assert "kimi_public_mcp_enabled" in readiness.errors
    assert "kimi_public_skills_not_empty" in readiness.errors


def _enable_feishu(settings: KimiSettings) -> KimiSettings:
    package_dir = settings.public.home / "node_modules" / "@larksuiteoapi" / "lark-mcp"
    (package_dir / "dist").mkdir(parents=True, exist_ok=True)
    (package_dir / "package.json").write_text('{"version":"0.5.1"}', encoding="utf-8")
    cli_path = package_dir / "dist" / "cli.js"
    _write_agent(settings.public.agent_path, FEISHU_PUBLIC_TOOLS)
    (settings.public.home / "config.toml").write_text(
        'merge_all_available_skills = false\nbuiltin_product_skills = false\n\n'
        '[tools]\nenabled = ' + json.dumps(list(FEISHU_PUBLIC_TOOLS)) + '\n',
        encoding="utf-8",
    )
    (settings.public.home / "mcp.json").write_text(json.dumps({
        "mcpServers": {"feishu": {
            "command": str(settings.public.home / "node.exe"),
            "args": [str(cli_path), "mcp", "--token-mode", "tenant_access_token",
                     "--tool-name-case", "snake", "--language", "zh", "--tools", ",".join(FEISHU_APIS)],
            "enabledTools": list(FEISHU_ENABLED_TOOLS),
        }}
    }), encoding="utf-8")
    return replace(settings, public=replace(settings.public, feishu_enabled=True, tools=FEISHU_PUBLIC_TOOLS), feishu_enabled=True)


def test_enabled_feishu_requires_exact_static_readonly_config(tmp_path, monkeypatch):
    settings = _enable_feishu(_settings(tmp_path))
    credentials = tmp_path / "credentials.json"
    credentials.write_text('{"app_id":"cli_TEST1234567890","app_secret":"TEST_ONLY_SECRET"}', encoding="utf-8")
    credentials.chmod(0o600)
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.FEISHU_CREDENTIALS_PATH", credentials)
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.PROJECT_ROOT", tmp_path / "project")

    readiness = validate_kimi_settings(settings, executable_resolver=lambda _path: "C:/tools/kimi.exe")

    assert readiness.public_profile_valid is True


def test_default_and_feishu_examples_match_only_their_feature_modes(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[2]
    default_config = (root / "config/examples/ai/kimi/public_home_security.example.toml").read_text(encoding="utf-8")
    default_agent = (root / "config/examples/ai/prompts/kimi/public.md.example").read_text(encoding="utf-8")
    feishu_config = (root / "config/examples/ai/kimi/public_home_security_feishu.example.toml").read_text(encoding="utf-8")
    feishu_agent = (root / "config/examples/ai/prompts/kimi/public_feishu.md.example").read_text(encoding="utf-8")
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.PROJECT_ROOT", tmp_path / "project")

    disabled = _settings(tmp_path / "disabled")
    (disabled.public.home / "config.toml").write_text(default_config, encoding="utf-8")
    disabled.public.agent_path.write_text(default_agent, encoding="utf-8")
    assert validate_kimi_settings(disabled, executable_resolver=lambda _path: "C:/tools/kimi.exe").public_profile_valid

    enabled = _enable_feishu(_settings(tmp_path / "enabled"))
    (enabled.public.home / "config.toml").write_text(feishu_config, encoding="utf-8")
    enabled.public.agent_path.write_text(feishu_agent, encoding="utf-8")
    credentials = tmp_path / "enabled" / "credentials.json"
    credentials.write_text('{"app_id":"cli_TEST1234567890","app_secret":"TEST_ONLY_SECRET"}', encoding="utf-8")
    credentials.chmod(0o600)
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.FEISHU_CREDENTIALS_PATH", credentials)
    assert validate_kimi_settings(enabled, executable_resolver=lambda _path: "C:/tools/kimi.exe").public_profile_valid

    disabled_mismatch = replace(disabled, public=replace(disabled.public, tools=FEISHU_PUBLIC_TOOLS, feishu_enabled=True), feishu_enabled=True)
    assert not validate_kimi_settings(disabled_mismatch, executable_resolver=lambda _path: "C:/tools/kimi.exe").public_profile_valid
    enabled_mismatch = replace(enabled, public=replace(enabled.public, tools=PUBLIC_TOOLS, feishu_enabled=False), feishu_enabled=False)
    assert not validate_kimi_settings(enabled_mismatch, executable_resolver=lambda _path: "C:/tools/kimi.exe").public_profile_valid


@pytest.mark.asyncio
async def test_fake_mcp_initialize_and_tools_list_probe_fails_closed(tmp_path, monkeypatch):
    settings = _enable_feishu(_settings(tmp_path))
    credentials = tmp_path / "credentials.json"
    credentials.write_text('{"app_id":"cli_TEST1234567890","app_secret":"TEST_ONLY_SECRET"}', encoding="utf-8")
    credentials.chmod(0o600)
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.FEISHU_CREDENTIALS_PATH", credentials)
    import cooper_bot.modules.ai.kimi_cli as kimi_cli

    class FakeWriter:
        def __init__(self):
            self.payloads = []

        def write(self, payload):
            self.payloads.append(payload)

        async def drain(self):
            return None

    class FakeProcess:
        def __init__(self, tools):
            self.stdin = FakeWriter()
            self.stdout = asyncio.StreamReader()
            self.stdout.feed_data((json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "2024-11-05"}}) + "\n").encode())
            self.stdout.feed_data((json.dumps({"jsonrpc": "2.0", "id": 2, "result": {"tools": tools}}) + "\n").encode())
            self.stdout.feed_eof()
            self.returncode = None

        def terminate(self):
            self.returncode = 0

        async def wait(self):
            return 0

        def kill(self):
            self.returncode = -9

    for names, expected in ((list(FEISHU_ENABLED_TOOLS), True), (list(FEISHU_ENABLED_TOOLS) + ["appTable_update"], False)):
        process = FakeProcess([{"name": name} for name in names])
        captured = {}

        async def fake_spawn(*args, **kwargs):
            captured["args"] = args
            captured["env"] = kwargs["env"]
            return process

        monkeypatch.setattr(kimi_cli.asyncio, "create_subprocess_exec", fake_spawn)
        assert await probe_feishu_mcp_tools(settings.public, {"PATH": "trusted"}) is expected
        requests = [json.loads(payload) for payload in process.stdin.payloads]
        assert requests[0]["method"] == "initialize"
        assert requests[-1]["method"] == "tools/list"
        assert captured["env"]["APP_SECRET"] == "TEST_ONLY_SECRET"
        assert "TEST_ONLY_SECRET" not in captured["args"]


@pytest.mark.parametrize("mutation", ["missing_tool", "extra_tool", "preset", "oauth", "credential", "agent_extra", "global_extra", "config_credential", "agent_credential"])
def test_enabled_feishu_rejects_mcp_permission_drift(tmp_path, monkeypatch, mutation):
    settings = _enable_feishu(_settings(tmp_path))
    credentials = tmp_path / "credentials.json"
    credentials.write_text('{"app_id":"cli_TEST1234567890","app_secret":"TEST_ONLY_SECRET"}', encoding="utf-8")
    credentials.chmod(0o600)
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.FEISHU_CREDENTIALS_PATH", credentials)
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.PROJECT_ROOT", tmp_path / "project")
    mcp_path = settings.public.home / "mcp.json"
    mcp = json.loads(mcp_path.read_text(encoding="utf-8"))
    if mutation == "missing_tool":
        mcp["mcpServers"]["feishu"]["enabledTools"].pop()
    elif mutation == "extra_tool":
        mcp["mcpServers"]["feishu"]["enabledTools"].append("bitable_v1_appTable_update")
    elif mutation == "preset":
        mcp["mcpServers"]["feishu"]["args"][-1] = "preset.default"
    elif mutation == "oauth":
        mcp["mcpServers"]["feishu"]["args"][2] = "user_access_token"
    elif mutation == "credential":
        mcp["mcpServers"]["feishu"]["env"] = {"APP_SECRET": "TEST_ONLY_SECRET"}
    if mutation in {"missing_tool", "extra_tool", "preset", "oauth", "credential"}:
        mcp_path.write_text(json.dumps(mcp), encoding="utf-8")
    elif mutation == "agent_extra":
        settings.public.agent_path.write_text(settings.public.agent_path.read_text(encoding="utf-8").replace(
            "subagents: []", "  - Bash\nsubagents: []"), encoding="utf-8")
    elif mutation == "global_extra":
        config_path = settings.public.home / "config.toml"
        config_path.write_text(config_path.read_text(encoding="utf-8").replace(
            '"WebSearch",', '"Bash", "WebSearch",'), encoding="utf-8")
    elif mutation == "config_credential":
        config_path = settings.public.home / "config.toml"
        config_path.write_text(config_path.read_text(encoding="utf-8") + '\n# cli_TEST1234567890\n', encoding="utf-8")
    else:
        settings.public.agent_path.write_text(settings.public.agent_path.read_text(encoding="utf-8") + '\n# app_secret = TEST_ONLY_SECRET\n', encoding="utf-8")

    readiness = validate_kimi_settings(settings, executable_resolver=lambda _path: "C:/tools/kimi.exe")

    assert readiness.public_profile_valid is False
    if mutation in {"missing_tool", "extra_tool", "preset", "oauth", "credential"}:
        assert "kimi_public_mcp_config_unsafe" in readiness.errors
    elif mutation == "agent_extra":
        assert "public_agent_tools_unsafe" in readiness.errors
    elif mutation == "global_extra":
        assert "kimi_public_tools_policy_unsafe" in readiness.errors
    elif mutation == "config_credential":
        assert "kimi_public_credentials_in_config" in readiness.errors
    else:
        assert "kimi_public_credentials_in_agent" in readiness.errors


@pytest.mark.asyncio
async def test_runtime_version_is_diagnostic_and_not_a_readiness_gate(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr("cooper_bot.modules.ai.kimi_cli.config.PROJECT_ROOT", tmp_path / "project")

    readiness = validate_kimi_settings(settings, executable_resolver=lambda _path: "C:/tools/kimi.exe")
    info = await detect_kimi_runtime_info("missing", executable_resolver=lambda _path: None)

    assert readiness.configured is True
    assert info.version == "unknown"
