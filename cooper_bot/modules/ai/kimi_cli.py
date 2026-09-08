from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Optional

from cooper_bot.core import config


PUBLIC_TOOLS = ("WebSearch",)
ADMIN_TOOLS = (
    "Read",
    "Write",
    "Edit",
    "Grep",
    "Glob",
    "ReadMediaFile",
    "Bash",
    "WebSearch",
    "FetchURL",
)
_PROCESS_ENV_KEYS = (
    "SystemRoot",
    "WINDIR",
    "ComSpec",
    "PATHEXT",
    "PATH",
    "TEMP",
    "TMP",
    "LANG",
    "LC_ALL",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "SSL_CERT_FILE",
    "REQUESTS_CA_BUNDLE",
)


@dataclass(frozen=True)
class KimiProfile:
    name: str
    home: Path
    workdir: Path
    agent_path: Path
    skills_dir: Path
    tools: tuple[str, ...]


@dataclass(frozen=True)
class KimiSettings:
    enabled: bool
    cli_path: str
    expected_version: str
    model: str
    public: KimiProfile
    admin: KimiProfile
    timeout_seconds: float
    admin_timeout_seconds: float
    max_concurrency: int
    admin_enabled: bool
    allow_group_computer: bool


@dataclass(frozen=True)
class KimiReadiness:
    configured: bool
    public_profile_valid: bool
    admin_profile_valid: bool
    calendar_web_ready: bool
    computer_ready: bool
    errors: tuple[str, ...]


def build_kimi_env(profile: KimiProfile, source_env: Optional[Mapping[str, str]] = None) -> dict[str, str]:
    """Build a narrow child environment without inheriting bot credentials."""
    source = source_env or {}
    env = {
        key: str(source[key])
        for key in _PROCESS_ENV_KEYS
        if str(source.get(key) or "").strip()
    }
    env["KIMI_CODE_HOME"] = str(profile.home)
    return env


def load_kimi_settings() -> KimiSettings:
    public_home = Path(config.AI_KIMI_PUBLIC_HOME)
    admin_home = Path(config.AI_KIMI_ADMIN_HOME)
    return KimiSettings(
        enabled=bool(config.AI_KIMI_ENABLED),
        cli_path=str(config.AI_KIMI_CLI_PATH or "").strip(),
        expected_version=str(config.AI_KIMI_EXPECTED_VERSION or "").strip(),
        model=str(config.AI_KIMI_MODEL or "").strip(),
        public=KimiProfile(
            name="public",
            home=public_home,
            workdir=Path(config.AI_KIMI_PUBLIC_WORKDIR),
            agent_path=Path(config.AI_KIMI_PUBLIC_AGENT_PATH),
            skills_dir=public_home / "empty_skills",
            tools=PUBLIC_TOOLS,
        ),
        admin=KimiProfile(
            name="admin",
            home=admin_home,
            workdir=Path(config.AI_KIMI_ADMIN_WORKDIR),
            agent_path=Path(config.AI_KIMI_ADMIN_AGENT_PATH),
            skills_dir=admin_home / "empty_skills",
            tools=ADMIN_TOOLS,
        ),
        timeout_seconds=float(config.AI_KIMI_TIMEOUT_SECONDS),
        admin_timeout_seconds=float(config.AI_KIMI_ADMIN_TIMEOUT_SECONDS),
        max_concurrency=int(config.AI_KIMI_MAX_CONCURRENCY),
        admin_enabled=bool(config.AI_KIMI_ADMIN_ENABLED),
        allow_group_computer=bool(config.AI_KIMI_ALLOW_GROUP_COMPUTER),
    )


def _path_is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False
    except OSError:
        return False


def _validate_agent(profile: KimiProfile) -> Optional[str]:
    try:
        text = profile.agent_path.read_text(encoding="utf-8")
    except Exception:
        return f"{profile.name}_agent_missing"
    if "subagents: []" not in text:
        return f"{profile.name}_agent_subagents_not_disabled"
    for tool in profile.tools:
        if f"- {tool}" not in text:
            return f"{profile.name}_agent_missing_{tool.lower()}"
    return None


def validate_kimi_settings(
    settings: KimiSettings,
    *,
    executable_resolver: Callable[[str], Optional[str]] = shutil.which,
) -> KimiReadiness:
    errors: list[str] = []
    executable = executable_resolver(settings.cli_path) if settings.cli_path else None
    if not settings.enabled:
        errors.append("kimi_disabled")
    if not executable:
        errors.append("kimi_cli_missing")
    if not settings.expected_version:
        errors.append("kimi_expected_version_missing")
    if settings.public.home == settings.admin.home:
        errors.append("kimi_profile_homes_not_isolated")
    if _path_is_within(settings.public.workdir, Path(config.PROJECT_ROOT)):
        errors.append("kimi_public_workdir_inside_project")
    if not settings.public.workdir.is_dir():
        errors.append("kimi_public_workdir_missing")
    if not settings.admin.workdir.is_dir():
        errors.append("kimi_admin_workdir_missing")
    if not settings.public.home.is_dir():
        errors.append("kimi_public_home_missing")
    if not settings.admin.home.is_dir():
        errors.append("kimi_admin_home_missing")
    if not settings.public.skills_dir.is_dir():
        errors.append("kimi_public_skills_dir_missing")
    if not settings.admin.skills_dir.is_dir():
        errors.append("kimi_admin_skills_dir_missing")

    public_error = _validate_agent(settings.public)
    if public_error:
        errors.append(public_error)
    admin_error = _validate_agent(settings.admin)
    if admin_error:
        errors.append(admin_error)

    configured = not any(
        item
        for item in errors
        if item
        in {
            "kimi_disabled",
            "kimi_cli_missing",
            "kimi_expected_version_missing",
            "kimi_profile_homes_not_isolated",
            "kimi_public_workdir_inside_project",
        }
    )
    public_profile_valid = configured and not any(item.startswith("kimi_public_") or item.startswith("public_") for item in errors)
    admin_profile_valid = configured and not any(item.startswith("kimi_admin_") or item.startswith("admin_") for item in errors)

    # These require an isolated runtime probe; static configuration never claims
    # that a search provider or computer-tool boundary has actually worked.
    return KimiReadiness(
        configured=configured,
        public_profile_valid=public_profile_valid,
        admin_profile_valid=admin_profile_valid,
        calendar_web_ready=False,
        computer_ready=False,
        errors=tuple(errors),
    )
