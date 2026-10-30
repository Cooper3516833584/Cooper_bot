from __future__ import annotations

import asyncio
import fnmatch
import json
import os
import re
import shutil
import tomllib
from urllib.parse import urlparse
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Optional

from cooper_bot.core import config
from cooper_bot.modules.ai.feishu_secrets import FeishuCredentialError, load_feishu_credentials


FEISHU_MCP_VERSION = "0.5.1"
FEISHU_APIS = (
    "bitable.v1.appTable.list",
    "bitable.v1.appTableField.list",
    "bitable.v1.appTableRecord.search",
    "docx.v1.document.rawContent",
)
FEISHU_TOOLS = (
    "mcp__feishu__bitable_v1_appTable_list",
    "mcp__feishu__bitable_v1_appTableField_list",
    "mcp__feishu__bitable_v1_appTableRecord_search",
    "mcp__feishu__docx_v1_document_rawContent",
)
FEISHU_ENABLED_TOOLS = tuple(tool.removeprefix("mcp__feishu__") for tool in FEISHU_TOOLS)
PUBLIC_TOOLS = ("WebSearch",)
FEISHU_PUBLIC_TOOLS = ("WebSearch", *FEISHU_TOOLS)
ADMIN_TOOLS = ("*",)
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
_MAX_STDOUT_BYTES = 8 * 1024 * 1024
_MAX_STDERR_BYTES = 64 * 1024
_MAX_FEISHU_RESULT_BYTES = 1024 * 1024
_DEFAULT_QUEUE_TIMEOUT_SECONDS = 8.0
_DEFAULT_MAX_QUEUE = 12
_WINDOWS_ARGV_BUDGET = 24000
_LEGACY_URP_CALL_RE = re.compile(
    r"<system_urp_calling>\s*<url>(?P<url>[^<]+)</url>\s*<method>GET</method>\s*</system_urp_calling>\s*",
    re.IGNORECASE,
)


class KimiCliError(RuntimeError):
    code = "kimi_error"

    def __init__(self, request_id: str, detail: str = "") -> None:
        self.request_id = str(request_id or "")
        self.detail = str(detail or "")
        super().__init__(f"{self.code}: request_id={self.request_id}")


class KimiBusyError(KimiCliError):
    code = "kimi_busy"


class KimiTimeoutError(KimiCliError):
    code = "kimi_timeout"


class KimiProtocolError(KimiCliError):
    code = "kimi_protocol_error"


class KimiEmptyReplyError(KimiCliError):
    code = "kimi_empty_reply"


class KimiInputTooLargeError(KimiCliError):
    code = "kimi_input_too_large"


class KimiSecurityViolation(KimiCliError):
    code = "kimi_security_violation"


@dataclass(frozen=True)
class KimiProfile:
    name: str
    home: Path
    workdir: Path
    agent_path: Path
    skills_dir: Path
    tools: tuple[str, ...]
    feishu_enabled: bool = False


@dataclass(frozen=True)
class KimiSettings:
    enabled: bool
    cli_path: str
    model: str
    public: KimiProfile
    admin: KimiProfile
    timeout_seconds: float
    admin_timeout_seconds: float
    max_concurrency: int
    admin_enabled: bool
    allow_group_computer: bool
    feishu_enabled: bool = False


@dataclass(frozen=True)
class KimiReadiness:
    configured: bool
    public_profile_valid: bool
    admin_profile_valid: bool
    calendar_web_ready: bool
    computer_ready: bool
    errors: tuple[str, ...]


@dataclass(frozen=True)
class PublicPolicyCheck:
    valid: bool
    errors: tuple[str, ...]


@dataclass(frozen=True)
class KimiRuntimeInfo:
    executable: str
    version: str


@dataclass(frozen=True)
class KimiCapabilityReport:
    cli_available: bool
    version: str
    protocol_ok: bool
    public_policy_static_ok: bool
    public_forbidden_tools_blocked: bool
    public_websearch_ready: bool
    admin_bash_ready: bool
    errors: tuple[str, ...]


@dataclass(frozen=True)
class KimiRunRequest:
    prompt: str
    profile: str
    timeout_seconds: float
    request_id: str
    purpose: str


@dataclass(frozen=True)
class KimiRunResult:
    text: str
    request_id: str
    exit_code: int
    tool_names: tuple[str, ...]
    tool_call_observed: bool
    protocol_observed: bool


def build_kimi_env(
    profile: KimiProfile,
    source_env: Optional[Mapping[str, str]] = None,
    *,
    feishu_credentials: Optional[Mapping[str, str]] = None,
) -> dict[str, str]:
    """Build a narrow child environment without inheriting bot credentials."""
    source = source_env or {}
    env = {
        key: str(source[key])
        for key in _PROCESS_ENV_KEYS
        if str(source.get(key) or "").strip()
    }
    env["KIMI_CODE_HOME"] = str(profile.home)
    if profile.name == "public" and feishu_credentials:
        env.update({key: str(feishu_credentials[key]) for key in ("APP_ID", "APP_SECRET")})
    return env


def load_kimi_settings() -> KimiSettings:
    public_home = Path(config.AI_KIMI_PUBLIC_HOME)
    admin_home = Path(config.AI_KIMI_ADMIN_HOME)
    return KimiSettings(
        enabled=bool(config.AI_KIMI_ENABLED),
        cli_path=str(config.AI_KIMI_CLI_PATH or "").strip(),
        model=str(config.AI_KIMI_MODEL or "").strip(),
        public=KimiProfile(
            name="public",
            home=public_home,
            workdir=Path(config.AI_KIMI_PUBLIC_WORKDIR),
            agent_path=Path(config.AI_KIMI_PUBLIC_AGENT_PATH),
            skills_dir=public_home / "empty_skills",
            tools=FEISHU_PUBLIC_TOOLS if config.AI_FEISHU_ENABLED else PUBLIC_TOOLS,
            feishu_enabled=bool(config.AI_FEISHU_ENABLED),
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
        feishu_enabled=bool(config.AI_FEISHU_ENABLED),
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
    if profile.name == "public" and "subagents: []" not in text:
        return f"{profile.name}_agent_subagents_not_disabled"
    if profile.name == "public":
        frontmatter = re.search(r"(?s)^---\s*\n(.*?)\n---(?:\s|$)", text)
        if frontmatter:
            metadata = frontmatter.group(1).splitlines()
            for index, line in enumerate(metadata):
                if re.match(r"^disallowedTools\s*:", line):
                    value = line.split(":", 1)[1].strip()
                    if value not in {"", "[]"}:
                        return "public_agent_disallowed_tools"
                    for following in metadata[index + 1:]:
                        if following and not following[0].isspace():
                            break
                        if following.strip().startswith("-"):
                            return "public_agent_disallowed_tools"
        lines = text.splitlines()
        try:
            start = lines.index("tools:") + 1
        except ValueError:
            return "public_agent_tools_missing"
        actual: list[str] = []
        for line in lines[start:]:
            if line and not line[0].isspace():
                break
            stripped = line.strip()
            if stripped.startswith("- "):
                actual.append(stripped[2:].strip().strip("\"'"))
        if actual != list(profile.tools):
            return "public_agent_tools_unsafe"
    for tool in profile.tools:
        marker = '- "*"' if tool == "*" else f"- {tool}"
        if marker not in text:
            return f"{profile.name}_agent_missing_{tool.lower()}"
    return None


def _pattern_may_match(pattern: str, tool: str) -> bool:
    """Conservative matcher for Kimi MCP tool globs; false means definitely unrelated."""
    if pattern == tool:
        return True
    if not pattern.startswith("mcp__"):
        return False
    if any(char in pattern for char in "{}!+()\\"):
        return "feishu" in pattern.lower() or "*" in pattern or "?" in pattern
    try:
        return fnmatch.fnmatchcase(tool, pattern)
    except (ValueError, re.error):
        return True


def _disabled_pattern_matches(tool: str, patterns: list[str]) -> bool:
    for pattern in patterns:
        if pattern == tool:
            return True
        if tool.startswith("mcp__") and _pattern_may_match(pattern, tool):
            return True
    return False


def _permission_rules_block_tools(tools: tuple[str, ...], rules: list[object]) -> bool:
    for tool in tools:
        for rule in rules:
            if not isinstance(rule, dict):
                return True
            pattern = rule.get("tool") or rule.get("name") or rule.get("pattern")
            decision = rule.get("decision") or rule.get("action")
            if not isinstance(pattern, str) or not isinstance(decision, str):
                return True
            matches = (
                pattern == tool
                or pattern.startswith(tool + "(")
                or (tool.startswith("mcp__") and _pattern_may_match(pattern, tool))
            )
            if not matches:
                continue
            if decision.lower() in {"deny", "ask"}:
                return True
            if decision.lower() == "allow":
                break
            return True
    return False


def validate_public_home_policy(profile: KimiProfile) -> PublicPolicyCheck:
    """Verify the public home has a strict, global WebSearch-only boundary."""
    errors: list[str] = []
    config_path = profile.home / "config.toml"
    try:
        with config_path.open("rb") as fh:
            raw = tomllib.load(fh)
    except FileNotFoundError:
        errors.append("kimi_public_config_missing")
        raw = {}
    except (OSError, tomllib.TOMLDecodeError):
        errors.append("kimi_public_config_invalid")
        raw = {}
    tools = raw.get("tools") if isinstance(raw, dict) else None
    enabled = tools.get("enabled") if isinstance(tools, dict) else None
    if enabled is None:
        errors.append("kimi_public_tools_policy_missing")
    elif not isinstance(enabled, list) or set(enabled) != set(profile.tools) or len(enabled) != len(profile.tools):
        errors.append("kimi_public_tools_policy_unsafe")
    disabled = tools.get("disabled", []) if isinstance(tools, dict) else []
    if not isinstance(disabled, list) or any(not isinstance(item, str) for item in disabled):
        errors.append("kimi_public_tools_disabled_invalid")
    elif any(_disabled_pattern_matches(tool, disabled) for tool in profile.tools):
        errors.append("kimi_public_tools_disabled_unsafe")
    permission = raw.get("permission") if isinstance(raw, dict) else None
    rules = permission.get("rules", []) if isinstance(permission, dict) else []
    if not isinstance(rules, list):
        errors.append("kimi_public_permission_rules_invalid")
    elif _permission_rules_block_tools(profile.tools, rules):
        errors.append("kimi_public_permission_rule_blocks_tool")
    try:
        public_config_text = config_path.read_text(encoding="utf-8")
    except OSError:
        public_config_text = ""
    if re.search(r"(?i)(app[_-]?secret|app[_-]?id|cli_[A-Za-z0-9]{12,})", public_config_text):
        errors.append("kimi_public_credentials_in_config")
    if isinstance(raw, dict) and raw.get("extra_agent_dirs"):
        errors.append("kimi_public_extra_agent_dirs_enabled")
    if isinstance(raw, dict) and raw.get("extra_skill_dirs"):
        errors.append("kimi_public_extra_skill_dirs_enabled")
    if isinstance(raw, dict) and raw.get("merge_all_available_skills") is not False:
        errors.append("kimi_public_merge_all_skills_enabled")
    if isinstance(raw, dict) and raw.get("builtin_product_skills") is not False:
        errors.append("kimi_public_builtin_product_skills_enabled")
    mcp_path = profile.home / "mcp.json"
    if mcp_path.exists():
        try:
            mcp_raw = json.loads(mcp_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            errors.append("kimi_public_mcp_config_invalid")
        else:
            servers = mcp_raw.get("mcpServers") if isinstance(mcp_raw, dict) else None
            if servers:
                if not getattr(profile, "feishu_enabled", False):
                    errors.append("kimi_public_mcp_enabled")
                elif not _valid_feishu_mcp_config(mcp_raw):
                    errors.append("kimi_public_mcp_config_unsafe")
            elif getattr(profile, "feishu_enabled", False):
                errors.append("kimi_public_mcp_config_unsafe")
    elif getattr(profile, "feishu_enabled", False):
        errors.append("kimi_public_mcp_config_missing")
    if profile.skills_dir.is_dir():
        try:
            if any(profile.skills_dir.iterdir()):
                errors.append("kimi_public_skills_not_empty")
        except OSError:
            errors.append("kimi_public_skills_not_empty")
    if (profile.workdir / ".kimi-code").exists():
        errors.append("kimi_public_project_config_present")
    try:
        agent_text = profile.agent_path.read_text(encoding="utf-8")
    except OSError:
        agent_text = ""
    if re.search(r"(?i)(app[_-]?secret|app[_-]?id|cli_[A-Za-z0-9]{12,})", agent_text):
        errors.append("kimi_public_credentials_in_agent")
    return PublicPolicyCheck(not errors, tuple(errors))


def _valid_feishu_mcp_config(raw: object) -> bool:
    """Accept only the fixed, local stdio configuration for lark-mcp 0.5.1."""
    if not isinstance(raw, dict) or set(raw) != {"mcpServers"}:
        return False
    servers = raw.get("mcpServers")
    if not isinstance(servers, dict) or set(servers) != {"feishu"}:
        return False
    server = servers["feishu"]
    if not isinstance(server, dict) or set(server) != {"command", "args", "enabledTools"}:
        return False
    command, args = server.get("command"), server.get("args")
    if not isinstance(command, str) or not Path(command).is_absolute():
        return False
    if Path(command).name.lower() not in {"node", "node.exe"}:
        return False
    if not isinstance(args, list) or len(args) != 10 or not all(isinstance(item, str) for item in args):
        return False
    if not Path(args[0]).is_absolute():
        return False
    if tuple(Path(args[0]).parts[-4:]) != ("@larksuiteoapi", "lark-mcp", "dist", "cli.js"):
        return False
    try:
        package = json.loads((Path(args[0]).parent.parent / "package.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    if not isinstance(package, dict) or package.get("version") != FEISHU_MCP_VERSION:
        return False
    expected = [
        "mcp", "--token-mode", "tenant_access_token", "--tool-name-case", "snake",
        "--language", "zh", "--tools", ",".join(FEISHU_APIS),
    ]
    # args[0] is the pinned package entry point; args[1:] are fixed.
    if args[1:] != expected:
        return False
    if server.get("enabledTools") != list(FEISHU_ENABLED_TOOLS):
        return False
    serialized = json.dumps(raw, ensure_ascii=True)
    if re.search(r"(?i)(APP_SECRET|APP_ID|cli_[A-Za-z0-9]{12,}|--oauth|user_access_token|https?://)", serialized):
        return False
    return True


async def probe_feishu_mcp_tools(profile: KimiProfile, source_env: Optional[Mapping[str, str]] = None) -> bool:
    """Perform a credential-safe stdio initialize/tools-list probe of configured MCP."""
    try:
        mcp_raw = json.loads((profile.home / "mcp.json").read_text(encoding="utf-8"))
        if not _valid_feishu_mcp_config(mcp_raw):
            return False
        server = mcp_raw["mcpServers"]["feishu"]
        credentials = load_feishu_credentials(config.FEISHU_CREDENTIALS_PATH)
        env = build_kimi_env(profile, source_env, feishu_credentials=credentials)
        process = await asyncio.create_subprocess_exec(
            server["command"], *server["args"],
            cwd=str(profile.workdir),
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
    except (OSError, ValueError, FeishuCredentialError):
        return False

    async def send(payload: dict) -> None:
        assert process.stdin is not None
        process.stdin.write((json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8"))
        await process.stdin.drain()

    async def response_for(request_id: int) -> Optional[dict]:
        assert process.stdout is not None
        while True:
            line = await asyncio.wait_for(process.stdout.readline(), timeout=8.0)
            if not line or len(line) > 1024 * 1024:
                return None
            try:
                payload = json.loads(line)
            except (UnicodeError, json.JSONDecodeError):
                return None
            if isinstance(payload, dict) and payload.get("id") == request_id:
                return payload

    try:
        await send({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "Cooper_bot-readonly-probe", "version": "1"},
            },
        })
        initialized = await response_for(1)
        if not isinstance(initialized, dict) or "error" in initialized or not isinstance(initialized.get("result"), dict):
            return False
        await send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        await send({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
        listed = await response_for(2)
        result = listed.get("result") if isinstance(listed, dict) else None
        tools = result.get("tools") if isinstance(result, dict) else None
        names = [item.get("name") for item in tools if isinstance(item, dict)] if isinstance(tools, list) else None
        return (
            isinstance(names, list)
            and len(names) == len(FEISHU_ENABLED_TOOLS)
            and all(isinstance(name, str) for name in names)
            and set(names) == set(FEISHU_ENABLED_TOOLS)
        )
    except (OSError, ValueError, TimeoutError, asyncio.TimeoutError, AssertionError):
        return False
    finally:
        try:
            if process.returncode is None:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=2.0)
        except (OSError, ProcessLookupError, TimeoutError, asyncio.TimeoutError):
            try:
                process.kill()
                await process.wait()
            except (OSError, ProcessLookupError):
                pass


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
    if settings.feishu_enabled:
        try:
            load_feishu_credentials(config.FEISHU_CREDENTIALS_PATH)
        except FeishuCredentialError as exc:
            errors.append(f"kimi_public_{exc.code}")

    public_error = _validate_agent(settings.public)
    if public_error:
        errors.append(public_error)
    admin_error = _validate_agent(settings.admin)
    if admin_error:
        errors.append(admin_error)
    public_policy = validate_public_home_policy(settings.public)
    errors.extend(public_policy.errors)

    configured = not any(
        item
        for item in errors
        if item
        in {
            "kimi_disabled",
            "kimi_cli_missing",
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


async def _stop_process(process: asyncio.subprocess.Process) -> None:
    """Best-effort child teardown: terminate, then kill after a short grace period."""
    if process.returncode is not None:
        return
    try:
        process.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=2.0)
    except TimeoutError:
        try:
            process.kill()
        except ProcessLookupError:
            return
        await process.wait()


async def detect_kimi_runtime_info(
    cli_path: str,
    *,
    executable_resolver: Callable[[str], Optional[str]] = shutil.which,
    timeout_seconds: float = 5.0,
    env: Optional[Mapping[str, str]] = None,
) -> KimiRuntimeInfo:
    """Return diagnostic version data only; it never influences readiness."""
    executable = executable_resolver(str(cli_path or "")) if cli_path else None
    if not executable:
        return KimiRuntimeInfo("", "unknown")
    process: Optional[asyncio.subprocess.Process] = None
    try:
        process = await asyncio.create_subprocess_exec(
            str(executable),
            "--version",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            env=dict(env) if env is not None else None,
        )
        stdout, _ = await asyncio.wait_for(process.communicate(), timeout=max(0.1, float(timeout_seconds)))
        if process.returncode == 0:
            return KimiRuntimeInfo(str(executable), stdout[:1024].decode("utf-8", errors="replace").strip() or "unknown")
    except Exception:
        pass
    finally:
        # 超时后 wait_for 会取消 communicate()，子进程可能仍然存活，必须显式回收。
        if process is not None:
            await _stop_process(process)
    return KimiRuntimeInfo(str(executable), "unknown")


def _windows_argv_units(argv: tuple[str, ...]) -> int:
    # A conservative CreateProcessW approximation: quote every argument and
    # count UTF-16 code units, including separators and the terminating NUL.
    quoted = []
    for item in argv:
        text = str(item)
        escaped = text.replace("\\", "\\\\").replace('"', '\\"')
        quoted.append(f'"{escaped}"')
    return len((" ".join(quoted) + "\0").encode("utf-16-le")) // 2


class KimiCliRunner:
    """Bounded Kimi CLI runner; profile selection never comes from QQ input."""

    def __init__(
        self,
        settings: KimiSettings,
        *,
        executable_resolver: Callable[[str], Optional[str]] = shutil.which,
        argv_prefix: Optional[tuple[str, ...]] = None,
        max_queue: int = _DEFAULT_MAX_QUEUE,
        queue_timeout_seconds: float = _DEFAULT_QUEUE_TIMEOUT_SECONDS,
        argv_budget: int = _WINDOWS_ARGV_BUDGET,
    ) -> None:
        self.settings = settings
        self._executable_resolver = executable_resolver
        self._argv_prefix = tuple(argv_prefix or ())
        self._global_slots = asyncio.Semaphore(max(1, int(settings.max_concurrency)))
        self._admin_slot = asyncio.Lock()
        self._queue_lock = asyncio.Lock()
        self._queued = 0
        self._max_queue = max(1, int(max_queue))
        self._queue_timeout_seconds = max(0.1, float(queue_timeout_seconds))
        self._argv_budget = max(1024, int(argv_budget))
        self._closing = False
        self._processes: set[asyncio.subprocess.Process] = set()
        self._public_security_healthy = True

    @property
    def public_security_healthy(self) -> bool:
        return self._public_security_healthy

    def reset_public_security_state(self) -> None:
        """Permit an explicit trusted re-probe after a public security fault."""
        self._public_security_healthy = True

    def _profile_for(self, name: str, request_id: str = "") -> KimiProfile:
        if name == "public":
            return self.settings.public
        if name == "admin":
            if not self.settings.admin_enabled:
                raise KimiSecurityViolation(request_id, "admin_disabled")
            return self.settings.admin
        raise KimiProtocolError("", "invalid_profile")

    def _build_argv(self, request: KimiRunRequest, profile: KimiProfile) -> tuple[str, ...]:
        executable = self._executable_resolver(self.settings.cli_path)
        if not executable:
            raise KimiProtocolError(request.request_id, "cli_missing")
        prefix = self._argv_prefix or (str(executable),)
        argv = [
            *prefix,
            "--agent-file",
            str(profile.agent_path),
            "--output-format",
            "stream-json",
        ]
        # Kimi 0.34 print mode already uses auto; --prompt rejects --auto/--yolo.
        if request.profile == "public":
            argv.extend(["--skills-dir", str(profile.skills_dir)])
        if self.settings.model:
            argv.extend(["--model", self.settings.model])
        argv.extend(["--prompt", str(request.prompt or "")])
        result = tuple(argv)
        if os.name == "nt" and _windows_argv_units(result) > self._argv_budget:
            # 只裁剪 aichat 的旧历史，不截断当前请求，也不改变固定 agent/工具边界。
            try:
                payload = json.loads(request.prompt) if request.purpose == "qq_chat" else None
            except (TypeError, ValueError):
                payload = None
            history = payload.get("conversation_history") if isinstance(payload, dict) else None
            if not isinstance(history, list):
                raise KimiInputTooLargeError(request.request_id)
            low, high = 1, len(history)
            fitting = None
            while low <= high:
                omitted = (low + high) // 2
                payload["conversation_history"] = history[omitted:]
                payload["omitted_older_messages"] = omitted
                argv[-1] = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
                result = tuple(argv)
                if _windows_argv_units(result) <= self._argv_budget:
                    fitting = result
                    high = omitted - 1
                else:
                    low = omitted + 1
            if fitting is None:
                raise KimiInputTooLargeError(request.request_id)
            result = fitting
        return result

    async def _acquire_slot(self, request: KimiRunRequest) -> tuple[bool, bool]:
        async with self._queue_lock:
            if self._closing or self._queued >= self._max_queue:
                raise KimiBusyError(request.request_id)
            self._queued += 1
        admin_acquired = False
        global_acquired = False
        try:
            if request.profile == "admin":
                await asyncio.wait_for(self._admin_slot.acquire(), timeout=self._queue_timeout_seconds)
                admin_acquired = True
            await asyncio.wait_for(self._global_slots.acquire(), timeout=self._queue_timeout_seconds)
            global_acquired = True
            return admin_acquired, global_acquired
        except TimeoutError as exc:
            raise KimiBusyError(request.request_id) from exc
        finally:
            async with self._queue_lock:
                self._queued = max(0, self._queued - 1)
            if not global_acquired and admin_acquired:
                self._admin_slot.release()

    @staticmethod
    async def _read_stream(stream: asyncio.StreamReader, maximum: int, *, keep_tail: bool = False) -> bytes:
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = await stream.read(65536)
            if not chunk:
                break
            size += len(chunk)
            if keep_tail:
                chunks.append(chunk)
                while sum(len(item) for item in chunks) > maximum:
                    chunks.pop(0)
                continue
            if size > maximum:
                raise KimiProtocolError("", "stdout_limit")
            chunks.append(chunk)
        return b"".join(chunks)

    @staticmethod
    def _extract_legacy_urp_tool_names(content: str) -> list[str]:
        """Adapt Kimi 0.34's structured WebSearch marker in assistant content."""
        text = str(content or "")
        if "<system_urp_calling>" not in text:
            return []
        matches = list(_LEGACY_URP_CALL_RE.finditer(text))
        if not matches:
            return ["UnknownURP"]
        names: list[str] = []
        for match in matches:
            parsed = urlparse(match.group("url").strip())
            if parsed.scheme == "https" and parsed.netloc.lower() in {"google.com", "www.google.com"} and parsed.path == "/search":
                names.append("WebSearch")
            else:
                names.append("UnknownURP")
        return names

    @staticmethod
    def _strip_legacy_urp_blocks(content: str) -> str:
        return _LEGACY_URP_CALL_RE.sub("", str(content or "")).strip()

    @staticmethod
    def _extract_tool_names(event: dict) -> list[str]:
        names: list[str] = []
        for candidate in (event, event.get("message")):
            if not isinstance(candidate, dict):
                continue
            direct = candidate.get("name") or candidate.get("tool_name")
            if isinstance(direct, str) and direct.strip():
                names.append(direct.strip())
            role = str(candidate.get("role") or event.get("type") or "")
            content = candidate.get("content")
            if role == "assistant" and isinstance(content, str):
                names.extend(KimiCliRunner._extract_legacy_urp_tool_names(content))
            calls = candidate.get("tool_calls")
            if not isinstance(calls, list):
                continue
            for call in calls:
                if not isinstance(call, dict):
                    continue
                name = call.get("name") or call.get("tool_name")
                function = call.get("function")
                if not name and isinstance(function, dict):
                    name = function.get("name")
                if isinstance(name, str) and name.strip():
                    names.append(name.strip())
        return names

    @staticmethod
    def _extract_assistant_text(event: dict) -> Optional[str]:
        message = event.get("message") if isinstance(event.get("message"), dict) else event
        if str(message.get("role") or event.get("type") or "") != "assistant":
            return None
        content = message.get("content")
        if isinstance(content, str):
            return KimiCliRunner._strip_legacy_urp_blocks(content)
        if isinstance(content, list):
            parts = [
                str(block.get("text") or "").strip()
                for block in content
                if isinstance(block, dict) and str(block.get("type") or "") in {"text", "output_text"}
            ]
            return "\n".join(part for part in parts if part).strip()
        return ""

    @staticmethod
    def _has_unrecognized_assistant_tool_schema(event: dict, names: list[str]) -> bool:
        if names:
            return False
        message = event.get("message") if isinstance(event.get("message"), dict) else event
        if str(message.get("role") or event.get("type") or "") != "assistant":
            return False
        return any("tool" in str(key).lower() for key in message)

    @staticmethod
    def _has_tool_error(event: dict) -> bool:
        candidates = (event, event.get("message"), event.get("result"))
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            if candidate.get("isError") is True or candidate.get("is_error") is True:
                return True
            result = candidate.get("result")
            if isinstance(result, dict) and (result.get("isError") is True or result.get("is_error") is True):
                return True
        return False

    @staticmethod
    def _tool_calls_with_ids(event: dict) -> list[tuple[Optional[str], str]]:
        message = event.get("message") if isinstance(event.get("message"), dict) else event
        calls = message.get("tool_calls")
        result: list[tuple[Optional[str], str]] = []
        if isinstance(calls, list):
            for call in calls:
                if not isinstance(call, dict):
                    continue
                function = call.get("function") if isinstance(call.get("function"), dict) else {}
                name = call.get("name") or call.get("tool_name") or function.get("name")
                if isinstance(name, str):
                    call_id = call.get("id") or call.get("tool_call_id")
                    result.append((str(call_id) if call_id else None, name))
        if str(message.get("type") or event.get("type") or "") == "tool_call":
            names = KimiCliRunner._extract_tool_names(event)
            result.extend((None, name) for name in names)
        return result

    @staticmethod
    def _validate_feishu_result(tool: str, event: dict, request_id: str) -> None:
        content = event.get("content")
        if isinstance(content, str):
            if len(content.encode("utf-8")) > _MAX_FEISHU_RESULT_BYTES:
                raise KimiProtocolError(request_id, "feishu_result_unverifiable")
            try:
                payload = json.loads(content)
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise KimiProtocolError(request_id, "feishu_result_unverifiable") from exc
        else:
            payload = content

        def error_flag(value: object) -> bool:
            return isinstance(value, dict) and (value.get("isError") is True or value.get("is_error") is True)

        if error_flag(event) or error_flag(payload):
            raise KimiProtocolError(request_id, "feishu_tool_failed")
        # MCP wrappers carry text blocks; decode only this single documented envelope.
        if isinstance(payload, dict) and isinstance(payload.get("content"), list):
            blocks = payload["content"]
            text_blocks = [block.get("text") for block in blocks if isinstance(block, dict) and block.get("type") == "text"]
            if len(text_blocks) != 1 or not isinstance(text_blocks[0], str):
                raise KimiProtocolError(request_id, "feishu_result_unverifiable")
            try:
                payload = json.loads(text_blocks[0])
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise KimiProtocolError(request_id, "feishu_result_unverifiable") from exc
        if not isinstance(payload, dict):
            raise KimiProtocolError(request_id, "feishu_result_unverifiable")
        nested = [payload.get(key) for key in ("data", "result") if isinstance(payload.get(key), dict)]
        if error_flag(payload) or any(error_flag(value) for value in nested):
            raise KimiProtocolError(request_id, "feishu_tool_failed")
        for response in (payload, *nested):
            code = response.get("code")
            if isinstance(code, (int, float)) and not isinstance(code, bool) and code != 0 and ("msg" in response or "message" in response):
                raise KimiProtocolError(request_id, "feishu_tool_failed")
        if tool.endswith("docx_v1_document_rawContent"):
            valid = "content" in payload and isinstance(payload.get("content"), (str, list, dict))
        else:
            valid = isinstance(payload.get("items"), list)
        if not valid:
            raise KimiProtocolError(request_id, "feishu_result_unverifiable")

    @classmethod
    def _parse_jsonl(cls, raw: bytes, request_id: str, *, validate_feishu_results: bool = True) -> tuple[str, tuple[str, ...], bool, bool]:
        try:
            lines = raw.decode("utf-8").splitlines()
        except UnicodeDecodeError as exc:
            raise KimiProtocolError(request_id, "stdout_encoding") from exc
        final_text = ""
        tools: list[str] = []
        tool_seen = False
        protocol_seen = False
        pending_feishu: dict[str, str] = {}
        completed_feishu: set[str] = set()
        invalid_feishu_call = False
        for line in lines:
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise KimiProtocolError(request_id, "invalid_jsonl") from exc
            if not isinstance(event, dict):
                raise KimiProtocolError(request_id, "invalid_event")
            event_type = str(event.get("type") or "")
            if event_type in {"error", "failed"}:
                raise KimiProtocolError(request_id, "terminal_error")
            if cls._has_tool_error(event):
                raise KimiProtocolError(request_id, "tool_error")
            names = cls._extract_tool_names(event)
            if cls._has_unrecognized_assistant_tool_schema(event, names):
                raise KimiProtocolError(request_id, "unrecognized_tool_schema")
            if names:
                tool_seen = True
                tools.extend(names)
                protocol_seen = True
            calls = cls._tool_calls_with_ids(event)
            if validate_feishu_results:
                for call_id, name in calls:
                    if name in FEISHU_TOOLS:
                        if not call_id:
                            invalid_feishu_call = True
                        elif call_id in pending_feishu:
                            invalid_feishu_call = True
                        else:
                            pending_feishu[call_id] = name
                message = event.get("message") if isinstance(event.get("message"), dict) else event
                if str(message.get("role") or "") == "tool":
                    call_id = message.get("tool_call_id")
                    if call_id is not None and str(call_id) in completed_feishu:
                        raise KimiProtocolError(request_id, "feishu_result_unverifiable")
                    tool = pending_feishu.pop(str(call_id), None) if call_id is not None else None
                    if tool:
                        cls._validate_feishu_result(tool, message, request_id)
                        completed_feishu.add(str(call_id))
            text = cls._extract_assistant_text(event)
            if text is not None:
                protocol_seen = True
                if text:
                    final_text = text
            elif event_type in {"tool_call", "tool", "tool_result", "tool_message", "usage", "done"}:
                protocol_seen = True
        if not protocol_seen:
            raise KimiProtocolError(request_id, "unrecognized_protocol")
        if validate_feishu_results and (pending_feishu or invalid_feishu_call):
            raise KimiProtocolError(request_id, "feishu_result_unverifiable")
        if not final_text:
            raise KimiEmptyReplyError(request_id)
        return final_text, tuple(dict.fromkeys(tools)), tool_seen, protocol_seen

    async def _stop_process(self, process: asyncio.subprocess.Process) -> None:
        # 实现统一在模块级函数，version probe 与正式运行共用同一套回收逻辑。
        await _stop_process(process)

    @staticmethod
    async def _drain_stream(stream: asyncio.StreamReader) -> None:
        """Read a terminated child's pipe to EOF so its transport can close."""
        try:
            while await asyncio.wait_for(stream.read(65536), timeout=2.0):
                continue
        except (TimeoutError, OSError):
            pass

    async def _reap_readers(
        self,
        process: asyncio.subprocess.Process,
        stdout_task: asyncio.Task[bytes],
        stderr_task: asyncio.Task[bytes],
    ) -> None:
        """Cancel aborted readers and release the terminated child's pipes."""
        for task in (stdout_task, stderr_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                await self._drain_stream(stream)

    async def run(self, request: KimiRunRequest) -> KimiRunResult:
        if self._closing:
            raise KimiBusyError(request.request_id)
        if request.profile == "public" and not self._public_security_healthy:
            raise KimiSecurityViolation(request.request_id, "public_profile_unhealthy")
        profile = self._profile_for(request.profile, request.request_id)
        argv = self._build_argv(request, profile)
        admin_acquired, global_acquired = await self._acquire_slot(request)
        process: Optional[asyncio.subprocess.Process] = None
        stdout_task: Optional[asyncio.Task[bytes]] = None
        stderr_task: Optional[asyncio.Task[bytes]] = None
        wait_task: Optional[asyncio.Task[int]] = None
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=str(profile.workdir),
                env=build_kimi_env(
                    profile,
                    os.environ,
                    feishu_credentials=(
                        load_feishu_credentials(config.FEISHU_CREDENTIALS_PATH)
                        if request.profile == "public" and self.settings.feishu_enabled
                        else None
                    ),
                ),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            self._processes.add(process)
            assert process.stdout is not None and process.stderr is not None
            stdout_task = asyncio.create_task(self._read_stream(process.stdout, _MAX_STDOUT_BYTES))
            stderr_task = asyncio.create_task(self._read_stream(process.stderr, _MAX_STDERR_BYTES, keep_tail=True))
            wait_task = asyncio.create_task(process.wait())
            # Watch the process exit and both readers together: an early stdout
            # overflow must terminate the child instead of letting it block on a
            # full pipe until the whole request timeout expires.
            done, _pending = await asyncio.wait(
                (wait_task, stdout_task, stderr_task),
                timeout=max(0.1, float(request.timeout_seconds)),
                return_when=asyncio.FIRST_EXCEPTION,
            )
            failure: Optional[BaseException] = None
            for task in (stdout_task, stderr_task):
                if task in done and not task.cancelled():
                    error = task.exception()
                    if error is not None:
                        failure = error
                        break
            if failure is not None:
                await self._stop_process(process)
                await self._reap_readers(process, stdout_task, stderr_task)
                if isinstance(failure, KimiProtocolError) and failure.detail == "stdout_limit":
                    raise KimiProtocolError(request.request_id, "stdout_limit") from failure
                raise failure
            if stdout_task not in done or stderr_task not in done:
                await self._stop_process(process)
                await self._reap_readers(process, stdout_task, stderr_task)
                raise KimiTimeoutError(request.request_id)
            stdout = await stdout_task
            await stderr_task
            if process.returncode != 0:
                raise KimiProtocolError(request.request_id, "process_failed")
            text, tools, tool_seen, protocol_seen = self._parse_jsonl(
                stdout,
                request.request_id,
                validate_feishu_results=request.profile == "public" and profile.feishu_enabled,
            )
            result = KimiRunResult(text, request.request_id, int(process.returncode), tools, tool_seen, protocol_seen)
            if request.profile == "public":
                allowed_tools = set(profile.tools)
                forbidden = set(result.tool_names).difference(allowed_tools)
                if forbidden:
                    self._public_security_healthy = False
                    raise KimiSecurityViolation(request.request_id, "forbidden_tool_observed")
            return result
        except asyncio.CancelledError:
            if process is not None:
                await self._stop_process(process)
            raise
        finally:
            for task in (stdout_task, stderr_task, wait_task):
                if task is not None and not task.done():
                    task.cancel()
            pending_tasks = tuple(task for task in (stdout_task, stderr_task, wait_task) if task is not None)
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
            if process is not None:
                self._processes.discard(process)
            if global_acquired:
                self._global_slots.release()
            if admin_acquired:
                self._admin_slot.release()

    async def aclose(self) -> None:
        self._closing = True
        processes = list(self._processes)
        await asyncio.gather(*(self._stop_process(process) for process in processes), return_exceptions=True)
