from __future__ import annotations

import asyncio
import json
import os
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
_MAX_STDOUT_BYTES = 8 * 1024 * 1024
_MAX_STDERR_BYTES = 64 * 1024
_DEFAULT_QUEUE_TIMEOUT_SECONDS = 8.0
_DEFAULT_MAX_QUEUE = 12
_WINDOWS_ARGV_BUDGET = 24000


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

    def _profile_for(self, name: str) -> KimiProfile:
        if name == "public":
            return self.settings.public
        if name == "admin":
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
            "--skills-dir",
            str(profile.skills_dir),
            "--output-format",
            "stream-json",
        ]
        if self.settings.model:
            argv.extend(["--model", self.settings.model])
        argv.extend(["--prompt", str(request.prompt or "")])
        result = tuple(argv)
        if os.name == "nt" and _windows_argv_units(result) > self._argv_budget:
            raise KimiInputTooLargeError(request.request_id)
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
    def _parse_jsonl(raw: bytes, request_id: str) -> tuple[str, tuple[str, ...], bool]:
        try:
            lines = raw.decode("utf-8").splitlines()
        except UnicodeDecodeError as exc:
            raise KimiProtocolError(request_id, "stdout_encoding") from exc
        final_text = ""
        tools: list[str] = []
        tool_seen = False
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
            if event_type in {"tool_call", "tool", "tool_result"}:
                tool_seen = True
                name = str(event.get("name") or event.get("tool_name") or "").strip()
                if name:
                    tools.append(name)
                continue
            content = event.get("content")
            if event_type == "assistant" and isinstance(content, str):
                final_text = content.strip()
                continue
            message = event.get("message")
            if isinstance(message, dict) and str(message.get("role") or "") == "assistant":
                content = message.get("content")
                if isinstance(content, str):
                    final_text = content.strip()
        if not final_text:
            raise KimiEmptyReplyError(request_id)
        return final_text, tuple(dict.fromkeys(tools)), tool_seen

    async def _stop_process(self, process: asyncio.subprocess.Process) -> None:
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

    async def run(self, request: KimiRunRequest) -> KimiRunResult:
        if self._closing:
            raise KimiBusyError(request.request_id)
        profile = self._profile_for(request.profile)
        argv = self._build_argv(request, profile)
        admin_acquired, global_acquired = await self._acquire_slot(request)
        process: Optional[asyncio.subprocess.Process] = None
        stdout_task: Optional[asyncio.Task[bytes]] = None
        stderr_task: Optional[asyncio.Task[bytes]] = None
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=str(profile.workdir),
                env=build_kimi_env(profile, os.environ),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            self._processes.add(process)
            assert process.stdout is not None and process.stderr is not None
            stdout_task = asyncio.create_task(self._read_stream(process.stdout, _MAX_STDOUT_BYTES))
            stderr_task = asyncio.create_task(self._read_stream(process.stderr, _MAX_STDERR_BYTES, keep_tail=True))
            try:
                await asyncio.wait_for(process.wait(), timeout=max(0.1, float(request.timeout_seconds)))
            except TimeoutError as exc:
                await self._stop_process(process)
                raise KimiTimeoutError(request.request_id) from exc
            stdout = await stdout_task
            await stderr_task
            if process.returncode != 0:
                raise KimiProtocolError(request.request_id, "process_failed")
            text, tools, tool_seen = self._parse_jsonl(stdout, request.request_id)
            return KimiRunResult(text, request.request_id, int(process.returncode), tools, tool_seen)
        except asyncio.CancelledError:
            if process is not None:
                await self._stop_process(process)
            raise
        finally:
            if stdout_task is not None and not stdout_task.done():
                stdout_task.cancel()
            if stderr_task is not None and not stderr_task.done():
                stderr_task.cancel()
            if stdout_task is not None or stderr_task is not None:
                await asyncio.gather(*(task for task in (stdout_task, stderr_task) if task is not None), return_exceptions=True)
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
