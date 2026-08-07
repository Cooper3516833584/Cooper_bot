from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import html
import os
import json
import re
import shutil
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

from vision_skill import VisionSlot

from config import (
    AI_API_KEY_PATH,
    AI_BOT_NICK,
    AI_CHAT_MODEL,
    AI_CLAUDE_MODEL,
    AI_EMBED_MODEL,
    AI_FALLBACK_ERROR_REPLY,
    AI_GEMINI_CLI_PATH,
    AI_GEMINI_MODEL,
    AI_GEMINI_POLICY_PATH,
    AI_GEMINI_RESTRICTED_WORKDIR,
    AI_GEMINI_TIMEOUT_SECONDS,
    AI_GEMINI_WORKDIR,
    AI_INDEX_PATH,
    AI_MATERIAL_DIR,
    AI_METADATA_PATH,
    AI_SEARCH_LIMIT,
    AI_SEARCH_MIN_SIMILARITY,
    AI_SYSTEM_PROMPT,
    AI_VECTORS_PATH,
    AI_WEB_SEARCH_ENABLED,
    AI_WEB_SEARCH_MODEL,
    BASE_DIR,
    ENABLE_OCR,
)

try:
    from pypdf import PdfReader  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    PdfReader = None

try:
    import fitz  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    fitz = None

try:
    from docx import Document  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    Document = None

try:
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    aiohttp = None

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    BeautifulSoup = None

try:
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None

try:
    from rapidocr_onnxruntime import RapidOCR  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    RapidOCR = None


class AIService:
    _SKIP_FILENAMES = {
        "all_files_index.json",
        "file_metadata.json",
        "file_vectors.npy",
        "build_index.py",
        "build_vectors.py",
    }
    _ALLOWED_SUFFIXES = {".pdf", ".doc", ".docx", ".ppt", ".pptx"}
    _EBOOK_SUFFIXES = {".epub", ".mobi"}
    _CHAT_CONTEXT_TTL_SECONDS = 30.0 * 60.0
    _CHAT_CONTEXT_MAX_MESSAGES = 300
    _GEMINI_CHAT_CONTEXT_MAX_MESSAGES = 100
    _CHAT_TEMPERATURE = 0.65
    _AUTO_ORGANIZE_TBD_DIRNAME = "TBD"
    _AUTO_ORGANIZE_EBOOK_SUBJECT = "课外书"
    _AUTO_ORGANIZE_MARKS_FILENAME = "ai_material_scan_marks.json"
    _AUTO_ORGANIZE_STATE_CACHE_FILENAME = "ai_material_state_cache.json"
    _INCREMENTAL_STORE_FILENAME = "ai_semantic_store.sqlite3"
    _INCREMENTAL_STORE_SCHEMA_VERSION = 1
    _AUTO_ORGANIZE_MAX_WARNINGS = 5
    _TBD_CLASSIFY_MAX_CONCURRENCY = 3
    _NEW_FILE_SUMMARY_MAX_CONCURRENCY = 3
    _NEW_FILE_EMBED_MAX_CONCURRENCY = 4
    _MISPLACED_REVIEW_MAX_CANDIDATES = 24
    _ORGANIZE_PROGRESS_EVERY = 5
    _DEEPSEEK_V4_FLASH_MODEL = "deepseek-v4-flash"
    _DEEPSEEK_V4_PRO_MODEL = "deepseek-v4-pro"
    _THINKING_DISABLED = {"type": "disabled"}
    _THINKING_ENABLED = {"type": "enabled"}
    _REASONING_EFFORT_HIGH = "high"
    _NOTICE_SILENT_TOKEN = "[静默]"
    _CHAT_AUTOMATION_BOUNDARY_PROMPT = (
        "# 自动服务边界\n"
        "你只是 AI 聊天回复模型，不能执行 QQ 机器人的自动业务功能。\n"
        "历史消息中可能包含 bot 的自动服务回复，例如文件提交、任务选择、覆盖确认、取消提示、发送文件等；这些只能作为上下文理解，不能当作你当前可以执行的能力。\n"
        "如果用户发送 0、纯数字、Y/N、done 等看起来像业务流程控制的短回复，说明本该由业务功能流程已经结束，需要告知用户。你不得自行推断已经取消、覆盖、提交、归档、删除或发送文件。\n"
        "你应该如实说明：自己不能执行该自动操作，请以机器人业务功能实际返回的消息为准；如果没有业务回复，可以尝试提交重名文件覆盖。"
    )
    _VISION_BOUNDARY_PROMPT = (
        "# 视觉内容说明\n"
        "历史消息中可能包含以 [视觉内容N] 开头的文本。\n"
        "这些文本是外部视觉模型对用户图片、截图或表情包的描述，不是用户亲自输入的原文。\n"
        "回答时可以使用这些描述理解图片，但如果描述中注明识别失败、文字模糊或无法确认，不得假装直接看到了图片，也不得补充描述之外的视觉细节。"
    )
    _BACKEND_HISTORY_LIMITS = {
        "deepseek": 300,
        "gemini": 100,
        "claude": 100,
    }
    _WEB_SEARCH_JUDGE_PROMPT = (
        "# 联网需求判定\n"
        "这是内部机制提示，请勿向用户提起。\n"
        "如果用户的问题依赖实时、时效性或超出你知识范围的最新信息（例如天气、新闻、股价、比赛结果、政策变动、活动安排、最新公告等），\n"
        "你必须仅输出一行，格式为：\n"
        "[WEB_SEARCH]适合搜索引擎的自然语言查询词\n"
        "除这一行外禁止输出任何其他字符，特别是禁止先给出猜测性回答。\n"
        "其余问题请正常作答；涉及你不确定的事实性内容时，明确告知\"我不确定\"，禁止编造。"
    )
    _WEB_SEARCH_FETCH_SYSTEM_PROMPT = (
        "你是一个联网搜索执行器。用户会给你一个搜索查询词。\n"
        "请调用 web_search 工具完成搜索，等待工具返回结果即可，不要自己编造或总结。"
    )
    _WEB_SEARCH_COMPOSE_PROMPT = (
        "# 联网信息整合回答\n"
        "用户的问题需要实时信息，下面是联网搜索返回的资料（可能有噪声、过时或互相矛盾）：\n"
        "请基于这些资料回答用户的问题。必须遵守：\n"
        "1. 只使用资料中明确出现的信息，禁止编造资料中没有的事实、数字、日期、人名或来源。\n"
        "2. 资料不足或互相矛盾时，如实说明\"搜索到的信息有限/存在矛盾\"，不要强行下结论。\n"
        "3. 如能确定来源，用 (来源：标题或域名) 形式注明。\n"
        "4. 用简洁自然的中文回答，适合 QQ 聊天场景；不要提及搜索流程等内部机制。"
    )

    def __init__(self, log):
        self.log = log
        self.api_key_path = Path(AI_API_KEY_PATH)
        self.material_dir = Path(AI_MATERIAL_DIR)
        self.index_path = Path(AI_INDEX_PATH)
        self.metadata_path = Path(AI_METADATA_PATH)
        self.vectors_path = Path(AI_VECTORS_PATH)
        self.notice_prompt_config_path = Path(__file__).resolve().parent / "group_notice_prompts.json"
        self.private_chat_prompt_config_path = Path(__file__).resolve().parent / "private_chat_prompts.json"
        self.group_chat_prompt_config_path = Path(__file__).resolve().parent / "group_chat_prompts.json"
        self.material_scan_marks_path = Path(BASE_DIR) / self._AUTO_ORGANIZE_MARKS_FILENAME
        self.material_state_cache_path = Path(BASE_DIR) / self._AUTO_ORGANIZE_STATE_CACHE_FILENAME
        self.incremental_store_path = Path(BASE_DIR) / self._INCREMENTAL_STORE_FILENAME

        self.bot_nick = str(AI_BOT_NICK or "Cooper_bot")
        self.chat_model = str(AI_CHAT_MODEL or self._DEEPSEEK_V4_PRO_MODEL)
        self.web_search_enabled = bool(AI_WEB_SEARCH_ENABLED)
        self.web_search_model = str(AI_WEB_SEARCH_MODEL or self._DEEPSEEK_V4_FLASH_MODEL)
        self.embed_model = str(AI_EMBED_MODEL or "BAAI/bge-m3")
        self.gemini_cli_path = str(AI_GEMINI_CLI_PATH or "").strip()
        self.gemini_model = str(AI_GEMINI_MODEL or "").strip()
        self.claude_model = str(AI_CLAUDE_MODEL or "Claude Opus 4.6 (Thinking)").strip()
        self.gemini_policy_path = Path(AI_GEMINI_POLICY_PATH)
        self.gemini_workdir = Path(AI_GEMINI_WORKDIR)
        self.gemini_restricted_workdir = Path(AI_GEMINI_RESTRICTED_WORKDIR)
        self.gemini_timeout_seconds = max(10.0, float(AI_GEMINI_TIMEOUT_SECONDS or 120.0))
        self.search_limit = max(1, int(AI_SEARCH_LIMIT))
        self.search_min_similarity = float(AI_SEARCH_MIN_SIMILARITY)
        self.system_prompt = str(AI_SYSTEM_PROMPT or "").strip()
        self.fallback_error_reply = str(AI_FALLBACK_ERROR_REPLY or "").strip() or (
            "哎呀，我的脑子好像卡壳了（API报错/网络波动），请稍后重试，或者@Cooper 检查一下我的后台服务器吧！🔌"
        )

        self.deepseek_base_url = ""
        self.deepseek_api_key = ""
        self.embedding_base_url = ""
        self.embedding_api_key = ""

        self._lock = threading.RLock()
        self._chat_sessions_lock = threading.RLock()
        self._chat_sessions: Dict[str, Dict[str, object]] = {}
        self._semantic_meta: List[dict] = []
        self._semantic_norm_vectors: np.ndarray = np.empty((0, 0), dtype=np.float64)
        self._semantic_entry_by_rel: Dict[str, Tuple[dict, np.ndarray]] = {}
        self._semantic_row_by_rel: Dict[str, int] = {}
        self._semantic_rel_by_row: List[str] = []
        self._semantic_active_count: int = 0
        self._semantic_vector_dim: int = 0
        self._rapid_ocr = None
        self._notice_prompt_cache_mtime: Optional[float] = None
        self._notice_prompt_cache: Dict[str, object] = {"default": {}, "groups": {}}
        self._private_chat_prompt_cache_mtime: Optional[float] = None
        self._private_chat_prompt_cache: Dict[str, object] = {"default": {}, "users": {}}
        self._group_chat_prompt_cache_mtime: Optional[float] = None
        self._group_chat_prompt_cache: Dict[str, object] = {"default": {}, "groups": {}}

    def _build_chat_payload(
        self,
        messages: List[dict],
        temperature: float,
        response_format: Optional[dict] = None,
        enable_thinking: bool = False,
    ) -> dict:
        payload = {
            "model": str(self.chat_model or self._DEEPSEEK_V4_PRO_MODEL),
            "messages": list(messages),
            "temperature": float(temperature),
            "thinking": dict(self._THINKING_ENABLED if enable_thinking else self._THINKING_DISABLED),
        }
        if enable_thinking:
            payload["reasoning_effort"] = self._REASONING_EFFORT_HIGH
        if response_format is not None:
            payload["response_format"] = response_format
        return payload

    def _append_chat_automation_boundary(self, system_prompt: str) -> str:
        prompt = str(system_prompt or "").strip()
        boundary = self._CHAT_AUTOMATION_BOUNDARY_PROMPT.strip()
        vision_boundary = self._VISION_BOUNDARY_PROMPT.strip()
        if not prompt:
            out = boundary
        elif boundary in prompt:
            out = prompt
        else:
            out = f"{prompt}\n\n{boundary}"
        if vision_boundary and vision_boundary not in out:
            out = f"{out}\n\n{vision_boundary}"
        return out

    def _get_deepseek_sdk_base_url(self) -> str:
        return (self.deepseek_base_url or "https://api.deepseek.com").rstrip("/")

    def _create_deepseek_client(self):
        if OpenAI is None:
            raise RuntimeError("openai sdk is not installed")
        return OpenAI(api_key=self.deepseek_api_key, base_url=self._get_deepseek_sdk_base_url())

    def _create_reasoner_completion(self, client, messages: List[dict], temperature: float):
        return client.chat.completions.create(
            model=self._DEEPSEEK_V4_FLASH_MODEL,
            messages=list(messages),
            temperature=float(temperature),
            reasoning_effort=self._REASONING_EFFORT_HIGH,
            extra_body={"thinking": dict(self._THINKING_ENABLED)},
        )

    def _extract_sdk_chat_text(self, resp: object) -> str:
        try:
            return str(resp.choices[0].message.content or "").strip()
        except Exception:
            pass
        if hasattr(resp, "model_dump"):
            try:
                return self._extract_chat_text(resp.model_dump())
            except Exception:
                return ""
        return ""

    @property
    def chat_ready(self) -> bool:
        return bool(self.deepseek_base_url and self.deepseek_api_key and self.system_prompt)

    @property
    def gemini_chat_ready(self) -> bool:
        return bool(self._resolve_gemini_cli_executable() and self.gemini_policy_path.is_file())

    @property
    def semantic_ready(self) -> bool:
        with self._lock:
            active_count = int(self._semantic_active_count)
            vector_dim = int(self._semantic_vector_dim)
            norm_vectors = self._semantic_norm_vectors
            meta_len = len(self._semantic_meta)
            rel_by_row_len = len(self._semantic_rel_by_row)
            row_by_rel_len = len(self._semantic_row_by_rel)
        return bool(
            self.embedding_base_url
            and self.embedding_api_key
            and active_count > 0
            and vector_dim > 0
            and norm_vectors.ndim == 2
            and norm_vectors.shape[0] >= active_count
            and norm_vectors.shape[1] == vector_dim
            and meta_len == active_count
            and rel_by_row_len == active_count
            and row_by_rel_len == active_count
        )

    @property
    def notice_ready(self) -> bool:
        return bool(self.deepseek_api_key and OpenAI is not None)

    async def bootstrap_sync(self) -> None:
        await asyncio.to_thread(self._bootstrap_quick_sync_sync)

    async def bootstrap_post_startup_sync(self) -> None:
        await asyncio.to_thread(self._bootstrap_sync_sync)

    async def rebuild_material_scan_marks_from_current_layout(self) -> Dict[str, int]:
        return await asyncio.to_thread(self._rebuild_material_scan_marks_from_current_layout_sync)

    async def semantic_find_paths(self, demand: str, limit: Optional[int] = None) -> List[Path]:
        return await asyncio.to_thread(self._semantic_find_paths_sync, demand, limit)

    async def chat(self, user_input: str) -> str:
        return await asyncio.to_thread(self._chat_sync, user_input)

    async def chat_with_context(
        self,
        session_key: str,
        user_input: str,
        *,
        msg_id: str = "",
        vision_slots: Optional[list] = None,
    ) -> str:
        return await asyncio.to_thread(
            self._chat_with_context_sync, session_key, user_input, msg_id=msg_id, vision_slots=vision_slots
        )

    async def gemini_chat(self, user_input: str, model_key: Optional[str] = None) -> str:
        return await asyncio.to_thread(self._gemini_chat_sync, user_input, model_key)

    async def gemini_chat_with_context(
        self,
        session_key: str,
        user_input: str,
        model_key: Optional[str] = None,
        *,
        msg_id: str = "",
        vision_slots: Optional[list] = None,
    ) -> str:
        return await asyncio.to_thread(
            self._gemini_chat_with_context_sync,
            session_key,
            user_input,
            model_key,
            msg_id=msg_id,
            vision_slots=vision_slots,
        )

    async def restricted_gemini_chat(self, user_input: str, model_key: Optional[str] = None) -> str:
        return await asyncio.to_thread(self._gemini_chat_sync, user_input, model_key, True)

    async def restricted_gemini_chat_with_context(
        self,
        session_key: str,
        user_input: str,
        model_key: Optional[str] = None,
        *,
        msg_id: str = "",
        vision_slots: Optional[list] = None,
    ) -> str:
        return await asyncio.to_thread(
            self._gemini_chat_with_context_sync,
            session_key,
            user_input,
            model_key,
            True,
            msg_id=msg_id,
            vision_slots=vision_slots,
        )

    async def restricted_gemini_calendar_chat(
        self,
        user_input: str,
        model_key: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> str:
        """Run a stateless, web-search-only Antigravity request with an isolated timeout."""
        return await asyncio.to_thread(
            self._restricted_gemini_calendar_chat_sync,
            user_input,
            model_key,
            timeout_seconds,
        )

    def remember_user_message(
        self,
        session_key: str,
        message_text: str,
        *,
        msg_id: str = "",
        vision_slots: Optional[list] = None,
    ) -> None:
        self._remember_chat_message(session_key, "user", message_text, msg_id=msg_id, vision_slots=vision_slots)

    def remember_assistant_message(self, session_key: str, message_text: str) -> None:
        self._remember_chat_message(session_key, "assistant", message_text)

    def _resolve_gemini_cli_executable(self) -> str:
        raw = str(self.gemini_cli_path or "").strip()
        if not raw:
            return ""
        try:
            direct = Path(raw).expanduser()
        except Exception:
            direct = None
        if direct is not None and direct.is_file():
            return str(direct)
        resolved = shutil.which(raw)
        if resolved:
            return str(resolved)
        if direct is not None and (not direct.is_absolute()):
            try:
                candidate = (Path(BASE_DIR) / direct).resolve()
            except Exception:
                candidate = None
            if candidate is not None and candidate.is_file():
                return str(candidate)
        return ""

    def _build_gemini_cli_base_command(self) -> List[str]:
        cli_exe = self._resolve_gemini_cli_executable()
        if not cli_exe:
            return []

        cli_path = Path(cli_exe)
        suffix = cli_path.suffix.lower()
        if suffix in {".cmd", ".bat", ".ps1"}:
            node_path = cli_path.with_name("node.exe")
            js_path = cli_path.parent / "node_modules" / "@google" / "gemini-cli" / "bundle" / "gemini.js"
            if node_path.is_file() and js_path.is_file():
                return [str(node_path), str(js_path)]
        return [str(cli_path)]

    def _history_limit_for_backend(self, backend: str) -> int:
        key = str(backend or "").strip().lower()
        return max(
            1,
            int(self._BACKEND_HISTORY_LIMITS.get(key, self._CHAT_CONTEXT_MAX_MESSAGES)),
        )

    def _select_history_for_backend(self, history: List[Dict[str, str]], backend: str) -> List[Dict[str, str]]:
        limit = self._history_limit_for_backend(backend)
        if len(history) <= limit:
            return list(history)
        return list(history[-limit:])

    def _trim_gemini_history(self, messages: List[Dict[str, str]]) -> List[Dict[str, str]]:
        return self._select_history_for_backend(list(messages or []), "gemini")

    def _format_gemini_cli_history(self, history: List[Dict[str, str]]) -> str:
        lines: List[str] = []
        for item in self._trim_gemini_history(history):
            normalized = self._normalize_chat_history_item(item)
            if normalized is None:
                continue
            content = str(normalized.get("content") or "").strip()
            if not content:
                continue
            role = str(normalized.get("role") or "")
            label = "User" if role == "user" else "Assistant"
            lines.append(f"{label}:\n{content}")
        return "\n\n".join(lines).strip()

    def _build_gemini_cli_prompt(
        self,
        system_prompt: str,
        history: List[Dict[str, str]],
        user_input: str,
    ) -> str:
        content = str(user_input or "").strip()
        if not content:
            return ""
        sections: List[str] = []
        sys_prompt = str(system_prompt or "").strip()
        if sys_prompt:
            sections.append("System instructions:\n" + sys_prompt)
        history_text = self._format_gemini_cli_history(history)
        if history_text:
            sections.append(
                "Conversation history, oldest to newest. Use it only as context; answer the latest user request:\n"
                + history_text
            )
        sections.append("Use google_web_search before answering the latest user request.")
        sections.append("Latest user request:\n" + content)
        return "\n\n".join(sections).strip()

    def _build_restricted_gemini_cli_prompt(
        self,
        system_prompt: str,
        history: List[Dict[str, str]],
        user_input: str,
    ) -> str:
        content = str(user_input or "").strip()
        if not content:
            return ""
        base = (
            "Security policy for this QQ bot request:\n"
            "- You may answer the user and, when current/public information is needed, use google_web_search only.\n"
            "- Do not use any local-computer capability: no read_file, read_many_files, list_directory, glob, grep_search, write_file, replace, run_shell_command, ask_user, save_memory, activate_skill, or MCP/local tools.\n"
            "- Do not inspect, modify, execute, or summarize files, folders, processes, environment variables, shell output, browser state, or credentials on this computer.\n"
            "- If the request cannot be answered with normal model knowledge plus public web search, say you can only help with联网搜索获取信息.\n\n"
        )
        prompt = self._build_gemini_cli_prompt(system_prompt, history, content)
        return base + prompt

    @staticmethod
    def _normalize_gemini_cli_output(raw: str) -> str:
        raw = str(raw or "")

        # agy clears the screen before printing the final response. Everything
        # before this is usually spinner frames or thought logs.
        if "\x1b[2J" in raw:
            raw = raw.split("\x1b[2J")[-1]

        # Strip OSC (Operating System Command) sequences such as title updates.
        raw = re.sub(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)", "", raw)

        def _line_advance(match: re.Match) -> str:
            params = str(match.group(1) or "")
            first = params.split(";", 1)[0]
            try:
                count = int(first) if first else 1
            except Exception:
                count = 1
            return "\n" * max(1, min(count, 20))

        # agy/conhost may express visual line breaks as cursor-down/next-line
        # control sequences. Preserve those before removing the remaining ANSI.
        raw = re.sub(r"\x1b\[([0-9;]*)(?:B|E|e)", _line_advance, raw)
        raw = raw.replace("\x1bE", "\n").replace("\x1bD", "\n")

        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        raw = ansi_escape.sub("", raw)
        return raw.replace("\r\n", "\n").replace("\r", "\n").strip()

    def _resolve_gemini_cli_model(self, model_key: Optional[str] = None) -> str:
        raw = str(model_key or "").strip()
        key = raw.lower()
        if not key or key == "gemini":
            return str(self.gemini_model or "").strip()
        if key in {"claude", "opus", "opus4.6", "claude-opus"}:
            return str(self.claude_model or "").strip()
        return raw

    @staticmethod
    def _is_antigravity_cli_command(base_cmd: List[str]) -> bool:
        if not base_cmd:
            return False
        try:
            return Path(str(base_cmd[0])).stem.lower() in {"agy", "antigravity"}
        except Exception:
            return False

    @staticmethod
    def _build_antigravity_log_path(workdir: Path) -> Path:
        log_dir = Path(workdir) / "_agy_cli_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        stamp = f"{int(time.time() * 1000)}_{os.getpid()}_{threading.get_ident()}"
        return log_dir / f"agy_{stamp}.log"

    @staticmethod
    def _is_antigravity_cli_busy_text(text: str) -> bool:
        low = str(text or "").lower()
        return (
            "no capacity available" in low
            or "servers are experiencing high traffic" in low
            or "high traffic right now" in low
            or "resource exhausted" in low
            or "unavailable (code 503)" in low
        )

    @staticmethod
    def _extract_antigravity_cli_error(text: str) -> str:
        for line in reversed(str(text or "").splitlines()):
            low = line.lower()
            if (
                "no capacity available" not in low
                and "servers are experiencing high traffic" not in low
                and "high traffic right now" not in low
                and "resource exhausted" not in low
                and "authentication timed out" not in low
                and "you are not logged into antigravity" not in low
                and "agent executor error" not in low
            ):
                continue
            msg = re.sub(r"^[A-Z]\d{4}\s+\S+\s+\d+\s+[^]]+\]\s*", "", line).strip()
            msg = re.sub(r"^agent executor error:\s*", "", msg).strip()
            return msg[:500]
        return ""

    @staticmethod
    def _extract_antigravity_cli_transcript_response(log_path: Optional[Path]) -> Tuple[str, str]:
        if log_path is None or not Path(log_path).is_file():
            return "", ""
        log_text = Path(log_path).read_text(encoding="utf-8", errors="replace")
        conv_ids = re.findall(r"Created conversation ([0-9a-fA-F-]{36})", log_text)
        if not conv_ids:
            return "", log_text
        app_dirs = re.findall(r"CLI app data directory:\s*(.+)", log_text)
        app_dir = Path(app_dirs[-1].strip()) if app_dirs else Path.home() / ".gemini" / "antigravity-cli"
        conv_id = conv_ids[-1]
        logs_dir = app_dir / "brain" / conv_id / ".system_generated" / "logs"
        for name in ("transcript.jsonl", "transcript_full.jsonl"):
            transcript = logs_dir / name
            if not transcript.is_file():
                continue
            out: List[str] = []
            for line in transcript.read_text(encoding="utf-8", errors="replace").splitlines():
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if str(obj.get("source") or "") != "MODEL":
                    continue
                content = str(obj.get("content") or "").strip()
                if content:
                    out.append(content)
            if out:
                return out[-1], log_text
        return "", log_text

    def _run_gemini_cli_sync(
        self,
        prompt: str,
        model_name: Optional[str] = None,
        restricted: bool = False,
        timeout_seconds: Optional[float] = None,
    ) -> str:
        base_cmd = self._build_gemini_cli_base_command()
        if not base_cmd:
            raise RuntimeError("antigravity cli not found")
        policy_path = Path(self.gemini_policy_path)
        if not policy_path.is_file():
            raise RuntimeError("antigravity policy file not found")
        workdir = Path(self.gemini_restricted_workdir if restricted else self.gemini_workdir)
        workdir.mkdir(parents=True, exist_ok=True)

        agy_log_path: Optional[Path] = None
        cmd = [*base_cmd]
        if self._is_antigravity_cli_command(base_cmd):
            agy_log_path = self._build_antigravity_log_path(workdir)
            cmd.extend(["--log-file", str(agy_log_path)])
            if restricted:
                cmd.append("--sandbox")
        elif restricted:
            raise RuntimeError("restricted gemini chat requires antigravity cli")
        cli_label = "antigravity cli"
        cmd.extend(["-p", str(prompt or "")])
        cli_model = str(model_name if model_name is not None else self.gemini_model or "").strip()
        if cli_model:
            cmd.extend(["--model", cli_model])

        # Disable markdown hard-wrapping by pretending the console is very wide
        run_env = os.environ.copy()
        run_env["COLUMNS"] = "9999"
        run_creationflags = 0
        if agy_log_path is not None and os.name == "nt":
            run_creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0) or 0)
        run_timeout = float(self.gemini_timeout_seconds if timeout_seconds is None else timeout_seconds)
        run_timeout = max(10.0, min(run_timeout, 600.0))

        try:
            proc = subprocess.run(
                cmd,
                cwd=str(workdir),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=run_timeout,
                check=False,
                env=run_env,
                creationflags=run_creationflags,
            )
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(f"antigravity cli timeout after {int(run_timeout)}s") from e
        except Exception as e:
            raise RuntimeError(f"antigravity cli launch failed: {e}") from e

        raw_bytes = proc.stdout or b""
        raw = raw_bytes.decode("utf-8", errors="replace")
        raw = self._normalize_gemini_cli_output(raw)
        agy_log_text = ""
        if agy_log_path is not None and not raw:
            transcript_text, agy_log_text = self._extract_antigravity_cli_transcript_response(agy_log_path)
            if transcript_text:
                raw = transcript_text.strip()

        stderr_text = (proc.stderr or b"").decode("utf-8", errors="replace").strip()
        if proc.returncode != 0:
            detail = stderr_text or raw or self._extract_antigravity_cli_error(agy_log_text)
            raise RuntimeError(f"{cli_label} failed: {detail[:300]}")
        if not raw:
            detail = stderr_text or self._extract_antigravity_cli_error(agy_log_text)
            raise RuntimeError(f"{cli_label} empty response: {detail[:300]}")
        if agy_log_path is not None and self._is_antigravity_cli_busy_text(raw):
            raise RuntimeError(f"{cli_label} service busy: {raw[:300]}")

        # agy outputs plain text; try JSON first for backward compat
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                text = str(obj.get("response") or "").strip()
                if text:
                    return text
        except Exception:
            pass

        return raw

    async def extract_notice_file_head(self, path: Path, max_chars: int = 4000, max_pages: int = 6) -> str:
        return await asyncio.to_thread(
            self._extract_notice_file_head_sync,
            Path(path),
            int(max_chars),
            int(max_pages),
        )

    async def extract_notice_url_head(self, url: str, max_chars: int = 4000) -> str:
        return await self._extract_notice_url_head_async(str(url or "").strip(), int(max_chars))

    async def classify_notice(
        self,
        source: str,
        snippet: str,
        group_id: Optional[int] = None,
        kind: str = "notice",
    ) -> bool:
        return await asyncio.to_thread(
            self._classify_notice_sync_v2,
            str(source or ""),
            str(snippet or ""),
            group_id,
            str(kind or "notice"),
        )

    async def reason_notice(
        self,
        source: str,
        snippet: str,
        group_id: Optional[int] = None,
        kind: str = "notice",
    ) -> str:
        return await asyncio.to_thread(
            self._reason_notice_sync_v2,
            str(source or ""),
            str(snippet or ""),
            group_id,
            str(kind or "notice"),
        )

    @classmethod
    def is_notice_silent(cls, text: str) -> bool:
        return str(text or "").strip() == cls._NOTICE_SILENT_TOKEN

    @staticmethod
    def sanitize_reasoner_output(text: str) -> str:
        s = str(text or "")
        s = re.sub(r"<think\b[^>]*>[\s\S]*?</think>", "", s, flags=re.IGNORECASE)
        s = s.strip()
        if s.startswith("```"):
            s = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", s)
            s = re.sub(r"\s*```$", "", s)
        # QQ 里不需要 Markdown 星号样式，统一去掉。
        s = s.replace("*", "")
        # 兜底：去掉“原文未提及/未知/暂无/待通知”这类占位行。
        bad_tokens = ("原文未提及", "未知", "暂无", "待通知")
        kept = []
        for ln in s.splitlines():
            line = ln.strip()
            if not line:
                continue
            if any(tok in line for tok in bad_tokens):
                continue
            kept.append(line)
        s = "\n".join(kept).strip()
        return s.strip()

    @staticmethod
    def _normalize_notice_prompt_lines(value: object) -> List[str]:
        if isinstance(value, str):
            s = value.strip()
            return [s] if s else []
        if isinstance(value, list):
            out: List[str] = []
            for item in value:
                s = str(item or "").strip()
                if s:
                    out.append(s)
            return out
        return []

    @staticmethod
    def _normalize_chat_prompt_text(value: object) -> str:
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            lines: List[str] = []
            for item in value:
                s = str(item or "").strip()
                if s:
                    lines.append(s)
            return "\n".join(lines).strip()
        return ""

    @staticmethod
    def _builtin_private_chat_prompt_config() -> Dict[str, object]:
        return {"default": {}, "users": {}}

    @staticmethod
    def _builtin_group_chat_prompt_config() -> Dict[str, object]:
        return {"default": {}, "groups": {}}

    def _load_private_chat_prompt_config(self) -> Dict[str, object]:
        path = self.private_chat_prompt_config_path
        fallback = self._builtin_private_chat_prompt_config()
        try:
            mtime = path.stat().st_mtime
        except Exception:
            return fallback

        with self._lock:
            if self._private_chat_prompt_cache_mtime == float(mtime):
                return self._private_chat_prompt_cache

            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    raise ValueError("private chat prompt config root must be an object")
                default_cfg = data.get("default") or {}
                users_cfg = data.get("users") or {}
                if not isinstance(default_cfg, dict):
                    default_cfg = {}
                if not isinstance(users_cfg, dict):
                    users_cfg = {}
                normalized = {"default": default_cfg, "users": users_cfg}
                self._private_chat_prompt_cache = normalized
                self._private_chat_prompt_cache_mtime = float(mtime)
                self.log.info(f"AI chat: loaded private prompt config {path.name}")
                return normalized
            except Exception as e:
                self.log.warning(f"AI chat: failed to load private prompt config {path.name}: {e}")
                self._private_chat_prompt_cache = fallback
                self._private_chat_prompt_cache_mtime = float(mtime)
                return fallback

    def _load_group_chat_prompt_config(self) -> Dict[str, object]:
        path = self.group_chat_prompt_config_path
        fallback = self._builtin_group_chat_prompt_config()
        try:
            mtime = path.stat().st_mtime
        except Exception:
            return fallback

        with self._lock:
            if self._group_chat_prompt_cache_mtime == float(mtime):
                return self._group_chat_prompt_cache

            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    raise ValueError("group chat prompt config root must be an object")
                default_cfg = data.get("default")
                if default_cfg is None:
                    default_cfg = {}
                groups_cfg = data.get("groups") or {}
                if not isinstance(groups_cfg, dict):
                    groups_cfg = {}
                normalized = {"default": default_cfg, "groups": groups_cfg}
                self._group_chat_prompt_cache = normalized
                self._group_chat_prompt_cache_mtime = float(mtime)
                self.log.info(f"AI chat: loaded group prompt config {path.name}")
                return normalized
            except Exception as e:
                self.log.warning(f"AI chat: failed to load group prompt config {path.name}: {e}")
                self._group_chat_prompt_cache = fallback
                self._group_chat_prompt_cache_mtime = float(mtime)
                return fallback

    def _select_chat_system_prompt(self, session_key: str) -> str:
        default_prompt = str(self.system_prompt or "").strip()
        key = str(session_key or "").strip()
        if key.startswith("private:"):
            user_id = key.split(":", 1)[1].strip()
            if not user_id:
                return default_prompt

            try:
                cfg = self._load_private_chat_prompt_config()
            except Exception as e:
                self.log.warning(f"AI chat: private prompt read error, fallback to default prompt: user={user_id[:40]} err={e}")
                return default_prompt

            default_raw = cfg.get("default") if isinstance(cfg, dict) else {}
            default_cfg = default_raw if isinstance(default_raw, dict) else {}
            users_cfg = cfg.get("users") if isinstance(cfg, dict) else {}
            if not isinstance(users_cfg, dict):
                users_cfg = {}

            user_raw = users_cfg.get(user_id)
            user_prompt_direct = self._normalize_chat_prompt_text(user_raw)
            if user_prompt_direct:
                return user_prompt_direct

            user_cfg = user_raw or {}
            if not isinstance(user_cfg, dict):
                user_cfg = {}

            for value in (
                user_cfg.get("system_prompt"),
                user_cfg.get("prompt"),
                user_cfg.get("system_prompt_lines"),
                user_cfg.get("prompt_lines"),
            ):
                prompt = self._normalize_chat_prompt_text(value)
                if prompt:
                    return prompt

            default_prompt_direct = self._normalize_chat_prompt_text(default_raw)
            if default_prompt_direct:
                return default_prompt_direct

            for value in (
                default_cfg.get("system_prompt"),
                default_cfg.get("prompt"),
                default_cfg.get("system_prompt_lines"),
                default_cfg.get("prompt_lines"),
            ):
                prompt = self._normalize_chat_prompt_text(value)
                if prompt:
                    return prompt

            return default_prompt

        if key.startswith("group:"):
            group_id = key.split(":", 1)[1].strip()
            if not group_id:
                return default_prompt

            try:
                cfg = self._load_group_chat_prompt_config()
            except Exception as e:
                self.log.warning(f"AI chat: group prompt read error, fallback to default prompt: group={group_id[:40]} err={e}")
                return default_prompt

            default_raw = cfg.get("default") if isinstance(cfg, dict) else {}
            default_cfg = default_raw if isinstance(default_raw, dict) else {}
            groups_cfg = cfg.get("groups") if isinstance(cfg, dict) else {}
            if not isinstance(groups_cfg, dict):
                groups_cfg = {}

            group_raw = groups_cfg.get(group_id)
            group_prompt_direct = self._normalize_chat_prompt_text(group_raw)
            if group_prompt_direct:
                return group_prompt_direct

            group_cfg = group_raw or {}
            if not isinstance(group_cfg, dict):
                group_cfg = {}

            for value in (
                group_cfg.get("system_prompt"),
                group_cfg.get("prompt"),
                group_cfg.get("system_prompt_lines"),
                group_cfg.get("prompt_lines"),
            ):
                prompt = self._normalize_chat_prompt_text(value)
                if prompt:
                    return prompt

            default_prompt_direct = self._normalize_chat_prompt_text(default_raw)
            if default_prompt_direct:
                return default_prompt_direct

            for value in (
                default_cfg.get("system_prompt"),
                default_cfg.get("prompt"),
                default_cfg.get("system_prompt_lines"),
                default_cfg.get("prompt_lines"),
            ):
                prompt = self._normalize_chat_prompt_text(value)
                if prompt:
                    return prompt

            return default_prompt

        return default_prompt

    def _builtin_notice_prompt_config(self) -> Dict[str, object]:
        return {
            "default": {
                "classify_prompt_lines": [
                    "【角色设定】",
                    "你是 QQ 群里的通知过滤助手。",
                    "",
                    "【任务】",
                    "请判断下面内容是否属于“需要群成员采取动作、流程、报名、缴费、开会、填写、提交、关注截止时间”的通知。",
                    "如果只是学习资料、课程介绍、科普文章、普通新闻、经验分享、宣传内容、无明确行动要求的参考材料，请判定为静默。",
                    "如果内容和本群成员关系不大，或无法明确看出需要本群成员行动，也请判定为静默。",
                    "",
                    "【输出要求】",
                    "如果应该静默，只输出：{{silent_token}}",
                    "如果应该回复，只输出：[通知]",
                    "不要输出任何其他字符。",
                    "",
                    "【来源】",
                    "{{source}}",
                    "",
                    "【内容片段】",
                    "{{content}}",
                ],
                "reason_prompt_lines": [
                    "【角色设定】",
                    "你是 QQ 群里的 AI 助手，请基于通知全文生成一份给本群成员看的简洁省流说明。",
                    "",
                    "【要求】",
                    "只提取原文明确出现的信息，不要补充、猜测、外推或编造。",
                    "报名方式、费用、地点、对象、截止时间等字段，只有原文明确写到才允许写入。",
                    "如果某项信息原文没写，就直接省略，不要写“未知”“暂未提及”“待通知”之类占位话术。",
                    "",
                    "【输出格式】",
                    "📢 【省流通知】[核心标题]",
                    "🎯 核心事由：[一句话概括]",
                    "🙋 涉及人员：[按原文填写]",
                    "✅ 需要做什么：",
                    "1. ...",
                    "2. ...",
                    "3. ...",
                    "⏰ 截止时间：[若无则写“无明确截止时间”]",
                    "",
                    "【风格】",
                    "控制在 6 到 10 行。",
                    "可以用 emoji。",
                    "不要 Markdown 星号，不要代码块，不要额外添加模板外字段。",
                    "",
                    "【输入来源】",
                    "{{source}}",
                    "",
                    "【通知全文/片段】",
                    "{{content}}",
                ],
            },
            "groups": {},
        }

    def _load_notice_prompt_config(self) -> Dict[str, object]:
        path = self.notice_prompt_config_path
        fallback = self._builtin_notice_prompt_config()
        try:
            mtime = path.stat().st_mtime
        except Exception:
            return fallback

        with self._lock:
            if self._notice_prompt_cache_mtime == float(mtime):
                return self._notice_prompt_cache

            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    raise ValueError("prompt config root must be an object")
                default_cfg = data.get("default") or {}
                groups_cfg = data.get("groups") or {}
                if not isinstance(default_cfg, dict):
                    default_cfg = {}
                if not isinstance(groups_cfg, dict):
                    groups_cfg = {}
                normalized = {"default": default_cfg, "groups": groups_cfg}
                self._notice_prompt_cache = normalized
                self._notice_prompt_cache_mtime = float(mtime)
                self.log.info(f"群通知解析：已加载群提示词配置 {path.name}")
                return normalized
            except Exception as e:
                self.log.warning(f"群通知解析：读取群提示词配置失败 {path.name}: {e}")
                self._notice_prompt_cache = fallback
                self._notice_prompt_cache_mtime = float(mtime)
                return fallback

    def _select_notice_prompt_lines(
        self,
        group_id: Optional[int],
        kind: str,
        stage: str,
    ) -> List[str]:
        cfg = self._load_notice_prompt_config()
        default_cfg = cfg.get("default") if isinstance(cfg, dict) else {}
        groups_cfg = cfg.get("groups") if isinstance(cfg, dict) else {}
        if not isinstance(default_cfg, dict):
            default_cfg = {}
        if not isinstance(groups_cfg, dict):
            groups_cfg = {}

        group_cfg = groups_cfg.get(str(group_id), {}) if group_id is not None else {}
        if not isinstance(group_cfg, dict):
            group_cfg = {}

        kind_name = str(kind or "").strip().lower()
        kind_field = f"{kind_name}_{stage}_prompt_lines" if kind_name else ""
        generic_field = f"{stage}_prompt_lines"

        candidates = []
        if kind_field:
            candidates.append(group_cfg.get(kind_field))
        candidates.append(group_cfg.get(generic_field))
        if kind_field:
            candidates.append(default_cfg.get(kind_field))
        candidates.append(default_cfg.get(generic_field))

        for item in candidates:
            lines = self._normalize_notice_prompt_lines(item)
            if lines:
                return lines

        builtin = self._builtin_notice_prompt_config().get("default", {})
        if isinstance(builtin, dict):
            for item in (builtin.get(kind_field), builtin.get(generic_field)):
                lines = self._normalize_notice_prompt_lines(item)
                if lines:
                    return lines
        return []

    def _render_notice_prompt(
        self,
        lines: List[str],
        source: str,
        content: str,
    ) -> str:
        template = "\n".join(lines).strip()
        return (
            template
            .replace("{{source}}", str(source or "未知来源"))
            .replace("{{content}}", str(content or ""))
            .replace("{{silent_token}}", self._NOTICE_SILENT_TOKEN)
        )

    @staticmethod
    def _file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
        h = hashlib.sha256()
        with Path(path).open("rb") as f:
            while True:
                buf = f.read(int(chunk_size))
                if not buf:
                    break
                h.update(buf)
        return h.hexdigest().lower()

    @staticmethod
    def _normalize_sha256(value: object) -> str:
        h = str(value or "").strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", h):
            return ""
        return h

    @staticmethod
    def _file_stat_signature(path: Path) -> Tuple[int, int]:
        st = Path(path).stat()
        size = int(st.st_size)
        mtime_ns_raw = getattr(st, "st_mtime_ns", 0)
        try:
            mtime_ns = int(mtime_ns_raw)
        except Exception:
            mtime_ns = int(float(st.st_mtime) * 1_000_000_000)
        if mtime_ns < 0:
            mtime_ns = 0
        if size < 0:
            size = 0
        return size, mtime_ns

    def _load_material_state_cache(self) -> Dict[str, dict]:
        fallback: Dict[str, dict] = {}
        path = self.material_state_cache_path
        if not path.exists():
            return fallback
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(obj, dict):
                return fallback
            raw = obj.get("files")
            if raw is None:
                raw = obj
            if not isinstance(raw, dict):
                return fallback

            out: Dict[str, dict] = {}
            for rel_key, entry in raw.items():
                rel = self._normalize_rel(rel_key)
                if not rel:
                    continue
                if not isinstance(entry, dict):
                    continue
                try:
                    size = int(entry.get("size"))
                    mtime_ns = int(entry.get("mtime_ns"))
                except Exception:
                    continue
                if size < 0 or mtime_ns < 0:
                    continue
                sha = self._normalize_sha256(entry.get("sha256"))
                if not sha:
                    continue
                out[rel] = {"size": size, "mtime_ns": mtime_ns, "sha256": sha}
            return out
        except Exception as e:
            self.log.warning(f"AI 整理：加载状态缓存 {path.name} 失败: {e}")
            return fallback

    def _save_material_state_cache(self, cache_map: Dict[str, dict]) -> None:
        payload: Dict[str, dict] = {"files": {}}
        out = payload["files"]
        for rel_key, entry in (cache_map or {}).items():
            rel = self._normalize_rel(rel_key)
            if not rel or not isinstance(entry, dict):
                continue
            try:
                size = int(entry.get("size"))
                mtime_ns = int(entry.get("mtime_ns"))
            except Exception:
                continue
            if size < 0 or mtime_ns < 0:
                continue
            sha = self._normalize_sha256(entry.get("sha256"))
            if not sha:
                continue
            out[rel] = {"size": size, "mtime_ns": mtime_ns, "sha256": sha}
        self.material_state_cache_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _get_file_hash_by_state_cache(
        self,
        path: Path,
        rel_hint: str,
        cache_map: Dict[str, dict],
    ) -> Tuple[str, bool]:
        rel = self._normalize_rel(rel_hint)
        if not rel:
            try:
                rel = Path(path).relative_to(self.material_dir).as_posix()
            except Exception:
                rel = str(Path(path).name)

        size, mtime_ns = self._file_stat_signature(path)
        old = cache_map.get(rel)
        if isinstance(old, dict):
            old_sha = self._normalize_sha256(old.get("sha256"))
            if old_sha:
                try:
                    old_size = int(old.get("size"))
                    old_mtime_ns = int(old.get("mtime_ns"))
                except Exception:
                    old_size = -1
                    old_mtime_ns = -1
                if old_size == size and old_mtime_ns == mtime_ns:
                    return old_sha, False

        new_sha = self._normalize_sha256(self._file_sha256(path))
        if not new_sha:
            return "", False
        cache_map[rel] = {"size": size, "mtime_ns": mtime_ns, "sha256": new_sha}
        return new_sha, True

    def _set_file_hash_state_cache_entry(
        self,
        path: Path,
        rel_hint: str,
        file_hash: str,
        cache_map: Dict[str, dict],
    ) -> bool:
        rel = self._normalize_rel(rel_hint)
        sha = self._normalize_sha256(file_hash)
        if (not rel) or (not sha):
            return False
        size, mtime_ns = self._file_stat_signature(path)
        new_entry = {"size": size, "mtime_ns": mtime_ns, "sha256": sha}
        old_entry = cache_map.get(rel)
        if old_entry == new_entry:
            return False
        cache_map[rel] = new_entry
        return True

    def _load_material_scan_marks(self) -> Dict[str, object]:
        fallback: Dict[str, object] = {"confirmed_ok_hashes": {}}
        path = self.material_scan_marks_path
        if not path.exists():
            return fallback
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(obj, dict):
                return fallback
            raw = obj.get("confirmed_ok_hashes") or {}
            if not isinstance(raw, dict):
                return fallback
            normalized: Dict[str, dict] = {}
            for k, v in raw.items():
                h = str(k or "").strip().lower()
                if not re.fullmatch(r"[0-9a-f]{64}", h):
                    continue
                if isinstance(v, dict):
                    rel = str(v.get("rel") or "").strip()
                    ts_raw = v.get("ts") or 0
                else:
                    rel = ""
                    ts_raw = 0
                try:
                    ts = int(ts_raw)
                except Exception:
                    ts = 0
                normalized[h] = {"rel": rel, "ts": ts}
            return {"confirmed_ok_hashes": normalized}
        except Exception as e:
            self.log.warning(f"AI 整理：加载标记文件 {path.name} 失败: {e}")
            return fallback

    def _save_material_scan_marks(self, marks: Dict[str, object]) -> None:
        payload = {"confirmed_ok_hashes": {}}
        raw = (marks or {}).get("confirmed_ok_hashes") if isinstance(marks, dict) else {}
        if isinstance(raw, dict):
            out: Dict[str, dict] = {}
            for k, v in raw.items():
                h = str(k or "").strip().lower()
                if not re.fullmatch(r"[0-9a-f]{64}", h):
                    continue
                if isinstance(v, dict):
                    rel = str(v.get("rel") or "").strip()
                    ts_raw = v.get("ts") or 0
                else:
                    rel = ""
                    ts_raw = 0
                try:
                    ts = int(ts_raw)
                except Exception:
                    ts = 0
                out[h] = {"rel": rel, "ts": ts}
            payload["confirmed_ok_hashes"] = out
        self.material_scan_marks_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _rebuild_material_scan_marks_from_current_layout_sync(self) -> Dict[str, int]:
        stats = {"scanned": 0, "marked": 0, "hash_failed": 0, "duplicates": 0}
        if str(self.material_dir.name or "").strip().lower() != "textbook_and_material":
            return stats
        subjects = self._collect_existing_subject_dirs()
        if not subjects:
            return stats
        subject_set = set(subjects)
        files = self._collect_classified_files(subject_set)
        now_ts = int(time.time())
        confirmed: Dict[str, dict] = {}
        for path in files:
            rel = ""
            try:
                rel = path.relative_to(self.material_dir).as_posix()
            except Exception:
                rel = str(path.name)
            try:
                h = self._file_sha256(path)
                stats["scanned"] += 1
            except Exception:
                stats["hash_failed"] += 1
                continue
            if not h:
                continue
            if h in confirmed:
                stats["duplicates"] += 1
                continue
            confirmed[h] = {"rel": rel, "ts": now_ts}
        stats["marked"] = len(confirmed)
        self._save_material_scan_marks({"confirmed_ok_hashes": confirmed})
        return stats

    def _auto_organize_materials_on_boot(self, index_list: List[dict]) -> Tuple[Dict[str, str], Dict[str, dict], set[str]]:
        move_map: Dict[str, str] = {}
        new_file_hints: Dict[str, dict] = {}
        changed_classified_rels: set[str] = set()
        if str(self.material_dir.name or "").strip().lower() != "textbook_and_material":
            return move_map, new_file_hints, changed_classified_rels

        tbd_dir = self.material_dir / self._AUTO_ORGANIZE_TBD_DIRNAME
        tbd_dir.mkdir(parents=True, exist_ok=True)

        subjects = self._collect_existing_subject_dirs()
        if not subjects:
            self.log.info("AI 整理：未找到学科文件夹，跳过启动整理")
            return move_map, new_file_hints, changed_classified_rels
        can_use_ai = bool(self.deepseek_base_url and self.deepseek_api_key)
        if not can_use_ai:
            self.log.info("AI 整理：未配置 DeepSeek，仅执行哈希去重")

        index_by_rel = self._index_item_map_by_rel(index_list)
        subject_set = set(subjects)

        marks = self._load_material_scan_marks()
        raw_confirmed = marks.get("confirmed_ok_hashes") if isinstance(marks, dict) else {}
        confirmed_map: Dict[str, dict] = {}
        if isinstance(raw_confirmed, dict):
            for k, v in raw_confirmed.items():
                h = str(k or "").strip().lower()
                if not re.fullmatch(r"[0-9a-f]{64}", h):
                    continue
                if isinstance(v, dict):
                    rel = str(v.get("rel") or "").strip()
                    ts_raw = v.get("ts") or 0
                else:
                    rel = ""
                    ts_raw = 0
                try:
                    ts = int(ts_raw)
                except Exception:
                    ts = 0
                confirmed_map[h] = {"rel": rel, "ts": ts}
        confirmed_hashes = set(confirmed_map.keys())
        marks_changed = False
        state_cache_map = self._load_material_state_cache()
        state_cache_changed = False

        fix_files: List[Path] = []
        file_hash_by_rel: Dict[str, str] = {}
        existing_rel_by_hash: Dict[str, str] = {}
        classified_hashes_ready = False
        hash_failed = 0
        self.log.info(
            f"AI 整理阶段：准备索引/标记/状态缓存 (索引项={len(index_by_rel)}, 标记={len(confirmed_map)}, 状态缓存={len(state_cache_map)})"
        )

        def _log_progress(stage: str, done: int, total: int, ok: int, skip: int, fail: int, force: bool = False) -> None:
            if total <= 0:
                return
            if (not force) and done < total and (done % int(self._ORGANIZE_PROGRESS_EVERY) != 0):
                return
            self.log.info(
                f"AI 整理进度：{stage} {done}/{total} (成功={ok}, 跳过={skip}, 失败={fail})"
            )

        def _material_rel_exists(rel: str) -> bool:
            rel_norm = str(rel or "").strip()
            if not rel_norm:
                return False
            p = self.material_dir / rel_norm
            return p.exists() and p.is_file()

        def _ensure_classified_hash_cache() -> None:
            nonlocal fix_files, classified_hashes_ready, hash_failed, marks_changed, state_cache_changed, changed_classified_rels
            if classified_hashes_ready:
                return

            fix_files = self._collect_classified_files(subject_set)
            fix_rel_set = set()
            changed_classified_rels = set()
            local_hash_failed = 0
            for path in fix_files:
                rel_hint = ""
                try:
                    rel_hint = path.relative_to(self.material_dir).as_posix()
                except Exception:
                    rel_hint = str(path.name)
                rel_norm = self._normalize_rel(rel_hint)
                if rel_norm:
                    fix_rel_set.add(rel_norm)
                try:
                    h, cache_updated = self._get_file_hash_by_state_cache(path, rel_hint, state_cache_map)
                    if cache_updated:
                        state_cache_changed = True
                        if rel_norm:
                            changed_classified_rels.add(rel_norm)
                    if not h:
                        continue
                    file_hash_by_rel[rel_hint] = h
                    if h not in existing_rel_by_hash:
                        existing_rel_by_hash[h] = rel_hint
                except Exception as e:
                    local_hash_failed += 1
                    if local_hash_failed <= self._AUTO_ORGANIZE_MAX_WARNINGS:
                        self.log.warning(f"AI 整理：计算已归类文件哈希失败 {rel_hint}: {e}")
            hash_failed = local_hash_failed
            if hash_failed > self._AUTO_ORGANIZE_MAX_WARNINGS:
                extra = hash_failed - self._AUTO_ORGANIZE_MAX_WARNINGS
                self.log.warning(f"AI 整理：已归类文件哈希失败过多，省略 {extra} 条")

            stale_hashes = [h for h in list(confirmed_map.keys()) if h not in existing_rel_by_hash]
            if stale_hashes:
                for h in stale_hashes:
                    confirmed_map.pop(h, None)
                    confirmed_hashes.discard(h)
                marks_changed = True
                self.log.info(f"AI 整理：已清理已删除文件的过期标记，数量={len(stale_hashes)}")

            stale_state_rels = []
            for rel_key in list(state_cache_map.keys()):
                rel_norm = self._normalize_rel(rel_key)
                if not rel_norm:
                    stale_state_rels.append(rel_key)
                    continue
                if rel_norm not in fix_rel_set:
                    stale_state_rels.append(rel_key)
            if stale_state_rels:
                for rel_key in stale_state_rels:
                    state_cache_map.pop(rel_key, None)
                state_cache_changed = True
            classified_hashes_ready = True
            self.log.info(
                "AI 整理阶段：已归类文件哈希缓存就绪 "
                f"(文件={len(fix_files)}, 变化={len(changed_classified_rels)}, 哈希失败={hash_failed})"
            )

        self.log.info("AI 整理阶段：收集 TBD 文件")
        tbd_files = self._collect_tbd_files(tbd_dir)
        self.log.info(f"AI 整理阶段：TBD 文件收集完成，总数={len(tbd_files)}")
        tbd_moved = 0
        tbd_deleted = 0
        tbd_kept = 0
        tbd_failed = 0
        tbd_ai_candidates: List[dict] = []
        tbd_seen_hashes: Dict[str, str] = {}
        self.log.info("AI 整理阶段：TBD 去重与预检查")
        tbd_precheck_done = 0
        for path in tbd_files:
            rel_hint = ""
            try:
                rel_hint = path.relative_to(self.material_dir).as_posix()
            except Exception:
                rel_hint = str(path.name)
            tbd_precheck_done += 1

            tbd_hash = ""
            try:
                tbd_hash = self._file_sha256(path)
            except Exception as e:
                tbd_failed += 1
                if tbd_failed <= self._AUTO_ORGANIZE_MAX_WARNINGS:
                    self.log.warning(f"AI 整理：计算 TBD 文件哈希失败 {rel_hint}: {e}")

            if tbd_hash:
                duplicate_rel = existing_rel_by_hash.get(tbd_hash, "")
                if not duplicate_rel:
                    mark_entry = confirmed_map.get(tbd_hash)
                    mark_rel = (
                        str((mark_entry or {}).get("rel") or "").strip()
                        if isinstance(mark_entry, dict)
                        else ""
                    )
                    if _material_rel_exists(mark_rel):
                        duplicate_rel = mark_rel
                        existing_rel_by_hash[tbd_hash] = mark_rel
                if not duplicate_rel:
                    duplicate_rel = tbd_seen_hashes.get(tbd_hash, "")
                if (not duplicate_rel) and (not classified_hashes_ready):
                    _ensure_classified_hash_cache()
                    duplicate_rel = existing_rel_by_hash.get(tbd_hash, "")
                if duplicate_rel:
                    try:
                        path.unlink()
                        tbd_deleted += 1
                        self.log.info(f"AI 整理：删除 TBD 重复文件 {rel_hint}（哈希命中 {duplicate_rel}）")
                        _log_progress("TBD 预检查", tbd_precheck_done, len(tbd_files), tbd_moved + tbd_deleted, tbd_kept, tbd_failed)
                        continue
                    except Exception as e:
                        tbd_failed += 1
                        if tbd_failed <= self._AUTO_ORGANIZE_MAX_WARNINGS:
                            self.log.warning(f"AI 整理：删除 TBD 重复文件失败 {rel_hint}: {e}")
                    _log_progress("TBD 预检查", tbd_precheck_done, len(tbd_files), tbd_moved + tbd_deleted, tbd_kept, tbd_failed)
                    continue
                tbd_seen_hashes[tbd_hash] = rel_hint

            if path.suffix.lower() in self._EBOOK_SUFFIXES:
                try:
                    moved = self._move_material_to_subject(path, self._AUTO_ORGANIZE_EBOOK_SUBJECT)
                    if not moved:
                        tbd_kept += 1
                        continue
                    old_rel, new_rel = moved
                    move_map[old_rel] = new_rel
                    tbd_moved += 1
                    old_rel_norm = self._normalize_rel(old_rel)
                    if old_rel_norm and (old_rel_norm in state_cache_map):
                        state_cache_map.pop(old_rel_norm, None)
                        state_cache_changed = True
                    item = index_by_rel.pop(old_rel, None)
                    if isinstance(item, dict):
                        updated = dict(item)
                        updated["file_path"] = self._to_store_rel(new_rel)
                        updated["subject"] = self._subject_from_rel(new_rel)
                        updated["filename"] = (self.material_dir / new_rel).name
                        index_by_rel[new_rel] = updated
                    if tbd_hash:
                        file_hash_by_rel[new_rel] = tbd_hash
                        if tbd_hash not in existing_rel_by_hash:
                            existing_rel_by_hash[tbd_hash] = new_rel
                        try:
                            if self._set_file_hash_state_cache_entry(
                                self.material_dir / new_rel,
                                new_rel,
                                tbd_hash,
                                state_cache_map,
                            ):
                                state_cache_changed = True
                        except Exception:
                            pass
                    ebook_filename = (self.material_dir / new_rel).name
                    new_file_hints[new_rel] = {
                        "from_tbd": True,
                        "old_rel": old_rel,
                        "classified_target": self._AUTO_ORGANIZE_EBOOK_SUBJECT,
                        "snippet": "",
                        "summary_data": self._fallback_summary(
                            self._AUTO_ORGANIZE_EBOOK_SUBJECT,
                            ebook_filename,
                            path.suffix.lower().lstrip("."),
                        ),
                    }
                    self.log.info(
                        f"AI 整理：已移动 TBD 电子书文件 {old_rel} -> {new_rel}"
                    )
                    _log_progress("TBD 预检查", tbd_precheck_done, len(tbd_files), tbd_moved + tbd_deleted, tbd_kept, tbd_failed)
                    continue
                except Exception as e:
                    tbd_failed += 1
                    if tbd_failed <= self._AUTO_ORGANIZE_MAX_WARNINGS:
                        self.log.warning(f"AI 整理：移动 TBD 电子书文件失败 {rel_hint}: {e}")

            if not can_use_ai:
                tbd_kept += 1
                _log_progress("TBD 预检查", tbd_precheck_done, len(tbd_files), tbd_moved + tbd_deleted, tbd_kept, tbd_failed)
                continue

            tbd_ai_candidates.append({"path": path, "rel_hint": rel_hint, "tbd_hash": tbd_hash})
            _log_progress("TBD 预检查", tbd_precheck_done, len(tbd_files), tbd_moved + tbd_deleted, tbd_kept, tbd_failed)

        tbd_classify_results: Dict[str, dict] = {}
        if can_use_ai and tbd_ai_candidates:
            self.log.info(
                "AI 整理阶段：TBD 分类 "
                f"(候选={len(tbd_ai_candidates)}, 并发={max(1, min(int(self._TBD_CLASSIFY_MAX_CONCURRENCY), len(tbd_ai_candidates)))})"
            )
            tbd_classify_done = 0
            tbd_classify_ok = 0
            tbd_classify_fail = 0
            max_workers = max(1, min(int(self._TBD_CLASSIFY_MAX_CONCURRENCY), len(tbd_ai_candidates)))
            if max_workers <= 1:
                for candidate in tbd_ai_candidates:
                    rel_hint = str(candidate.get("rel_hint") or "").strip()
                    path = candidate.get("path")
                    if (not rel_hint) or (not isinstance(path, Path)):
                        tbd_classify_done += 1
                        tbd_classify_fail += 1
                        _log_progress("TBD 分类", tbd_classify_done, len(tbd_ai_candidates), tbd_classify_ok, 0, tbd_classify_fail)
                        continue
                    try:
                        tbd_classify_results[rel_hint] = self._classify_tbd_target_subject(
                            rel_hint,
                            path,
                            subjects,
                            index_by_rel.get(rel_hint),
                        )
                        tbd_classify_ok += 1
                    except Exception as e:
                        tbd_failed += 1
                        tbd_classify_fail += 1
                        if tbd_failed <= self._AUTO_ORGANIZE_MAX_WARNINGS:
                            self.log.warning(f"AI 整理：TBD 分类失败 {rel_hint}: {e}")
                    finally:
                        tbd_classify_done += 1
                        _log_progress("TBD 分类", tbd_classify_done, len(tbd_ai_candidates), tbd_classify_ok, 0, tbd_classify_fail)
            else:
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_rel: Dict[concurrent.futures.Future, str] = {}
                    for candidate in tbd_ai_candidates:
                        rel_hint = str(candidate.get("rel_hint") or "").strip()
                        path = candidate.get("path")
                        if (not rel_hint) or (not isinstance(path, Path)):
                            tbd_classify_done += 1
                            tbd_classify_fail += 1
                            _log_progress("TBD 分类", tbd_classify_done, len(tbd_ai_candidates), tbd_classify_ok, 0, tbd_classify_fail)
                            continue
                        fut = executor.submit(
                            self._classify_tbd_target_subject,
                            rel_hint,
                            path,
                            subjects,
                            index_by_rel.get(rel_hint),
                        )
                        future_to_rel[fut] = rel_hint
                    for fut in concurrent.futures.as_completed(future_to_rel):
                        rel_hint = future_to_rel[fut]
                        try:
                            tbd_classify_results[rel_hint] = fut.result()
                            tbd_classify_ok += 1
                        except Exception as e:
                            tbd_failed += 1
                            tbd_classify_fail += 1
                            if tbd_failed <= self._AUTO_ORGANIZE_MAX_WARNINGS:
                                self.log.warning(f"AI 整理：TBD 分类失败 {rel_hint}: {e}")
                        finally:
                            tbd_classify_done += 1
                            _log_progress("TBD 分类", tbd_classify_done, len(tbd_ai_candidates), tbd_classify_ok, 0, tbd_classify_fail)

        self.log.info("AI 整理阶段：应用 TBD 分类结果")
        tbd_apply_done = 0
        for candidate in tbd_ai_candidates:
            rel_hint = str(candidate.get("rel_hint") or "").strip()
            path = candidate.get("path")
            if (not rel_hint) or (not isinstance(path, Path)):
                tbd_kept += 1
                tbd_apply_done += 1
                _log_progress("TBD 应用", tbd_apply_done, len(tbd_ai_candidates), tbd_moved, tbd_kept, tbd_failed)
                continue
            classify_result = tbd_classify_results.get(rel_hint)
            if not isinstance(classify_result, dict):
                tbd_kept += 1
                tbd_apply_done += 1
                _log_progress("TBD 应用", tbd_apply_done, len(tbd_ai_candidates), tbd_moved, tbd_kept, tbd_failed)
                continue
            target = str(classify_result.get("target") or "").strip()
            if (not target) or (target not in subject_set):
                tbd_kept += 1
                tbd_apply_done += 1
                _log_progress("TBD 应用", tbd_apply_done, len(tbd_ai_candidates), tbd_moved, tbd_kept, tbd_failed)
                continue
            try:
                moved = self._move_material_to_subject(path, target)
                if not moved:
                    tbd_kept += 1
                    continue
                old_rel, new_rel = moved
                move_map[old_rel] = new_rel
                tbd_moved += 1
                old_rel_norm = self._normalize_rel(old_rel)
                if old_rel_norm and (old_rel_norm in state_cache_map):
                    state_cache_map.pop(old_rel_norm, None)
                    state_cache_changed = True
                item = index_by_rel.pop(old_rel, None)
                if isinstance(item, dict):
                    updated = dict(item)
                    updated["file_path"] = self._to_store_rel(new_rel)
                    updated["subject"] = self._subject_from_rel(new_rel)
                    updated["filename"] = (self.material_dir / new_rel).name
                    index_by_rel[new_rel] = updated
                tbd_hash = str(candidate.get("tbd_hash") or "").strip().lower()
                if tbd_hash:
                    file_hash_by_rel[new_rel] = tbd_hash
                    if tbd_hash not in existing_rel_by_hash:
                        existing_rel_by_hash[tbd_hash] = new_rel
                    try:
                        if self._set_file_hash_state_cache_entry(
                            self.material_dir / new_rel,
                            new_rel,
                            tbd_hash,
                            state_cache_map,
                        ):
                            state_cache_changed = True
                    except Exception:
                        pass
                hint_payload: Dict[str, object] = {
                    "from_tbd": True,
                    "old_rel": old_rel,
                    "classified_target": target,
                    "snippet": str(classify_result.get("snippet") or "").strip(),
                }
                summary_hint = self._normalize_summary_data(
                    classify_result.get("summary_data"),
                    target,
                    path.name,
                )
                if isinstance(summary_hint, dict):
                    hint_payload["summary_data"] = summary_hint
                new_file_hints[new_rel] = hint_payload
                self.log.info(f"AI 整理：已移动 TBD 文件 {old_rel} -> {new_rel}")
            except Exception as e:
                tbd_failed += 1
                if tbd_failed <= self._AUTO_ORGANIZE_MAX_WARNINGS:
                    self.log.warning(f"AI 整理：移动已分类 TBD 文件失败 {rel_hint}: {e}")
            finally:
                tbd_apply_done += 1
                _log_progress("TBD 应用", tbd_apply_done, len(tbd_ai_candidates), tbd_moved, tbd_kept, tbd_failed)
        if tbd_failed > self._AUTO_ORGANIZE_MAX_WARNINGS:
            extra = tbd_failed - self._AUTO_ORGANIZE_MAX_WARNINGS
            self.log.warning(f"AI 整理：TBD 分类失败过多，省略 {extra} 条")

        _ensure_classified_hash_cache()
        self.log.info("AI 整理阶段：增量错放纠偏候选筛选")

        fix_rel_set: set[str] = set()
        for p in fix_files:
            try:
                rel_norm = self._normalize_rel(p.relative_to(self.material_dir).as_posix())
            except Exception:
                rel_norm = ""
            if rel_norm:
                fix_rel_set.add(rel_norm)

        moved_from_tbd_rels: List[str] = []
        for rel in new_file_hints.keys():
            rel_norm = self._normalize_rel(rel)
            if rel_norm and (rel_norm in fix_rel_set):
                moved_from_tbd_rels.append(rel_norm)

        candidate_order: List[str] = []
        seen_candidate = set()
        for rel_norm in moved_from_tbd_rels:
            if rel_norm in seen_candidate:
                continue
            seen_candidate.add(rel_norm)
            candidate_order.append(rel_norm)
        for rel_norm in sorted(changed_classified_rels):
            if (not rel_norm) or (rel_norm in seen_candidate) or (rel_norm not in fix_rel_set):
                continue
            seen_candidate.add(rel_norm)
            candidate_order.append(rel_norm)

        max_candidates = max(1, int(self._MISPLACED_REVIEW_MAX_CANDIDATES))
        truncated = max(0, len(candidate_order) - max_candidates)
        if truncated > 0:
            candidate_order = candidate_order[:max_candidates]
        fix_candidate_rel_set = set(candidate_order)
        self.log.info(
            "AI 整理阶段：增量错放纠偏 "
            f"(来自TBD移动={len(moved_from_tbd_rels)}, 本次变化={len(changed_classified_rels)}, "
            f"选中={len(fix_candidate_rel_set)}, 截断={truncated})"
        )

        skip_rels = set(move_map.values())
        fix_marked_skip = 0
        fix_moved = 0
        fix_kept = 0
        fix_failed = 0
        fix_non_incremental_skip = 0
        fix_review_done = 0
        for path in fix_files:
            rel_hint = ""
            try:
                rel_hint = path.relative_to(self.material_dir).as_posix()
            except Exception:
                rel_hint = str(path.name)
            rel_norm = self._normalize_rel(rel_hint)
            if rel_hint in skip_rels:
                continue
            current_subject = self._subject_from_rel(rel_hint)
            if current_subject not in subject_set:
                continue

            file_hash = file_hash_by_rel.get(rel_hint, "")
            if (not file_hash) and path.exists():
                try:
                    file_hash, cache_updated = self._get_file_hash_by_state_cache(path, rel_hint, state_cache_map)
                    if cache_updated:
                        state_cache_changed = True
                    if file_hash:
                        file_hash_by_rel[rel_hint] = file_hash
                except Exception:
                    file_hash = ""

            is_incremental_candidate = bool(rel_norm and (rel_norm in fix_candidate_rel_set))

            if (path.suffix.lower() in self._EBOOK_SUFFIXES) and (current_subject != self._AUTO_ORGANIZE_EBOOK_SUBJECT):
                try:
                    moved = self._move_material_to_subject(path, self._AUTO_ORGANIZE_EBOOK_SUBJECT)
                    if not moved:
                        fix_kept += 1
                        if is_incremental_candidate:
                            fix_review_done += 1
                            _log_progress(
                                "已归类增量纠偏",
                                fix_review_done,
                                len(fix_candidate_rel_set),
                                fix_moved,
                                fix_marked_skip + fix_kept,
                                fix_failed,
                            )
                        continue
                    old_rel, new_rel = moved
                    move_map[old_rel] = new_rel
                    fix_moved += 1
                    old_rel_norm = self._normalize_rel(old_rel)
                    if old_rel_norm and (old_rel_norm in state_cache_map):
                        state_cache_map.pop(old_rel_norm, None)
                        state_cache_changed = True
                    item = index_by_rel.pop(old_rel, None)
                    if isinstance(item, dict):
                        updated = dict(item)
                        updated["file_path"] = self._to_store_rel(new_rel)
                        updated["subject"] = self._subject_from_rel(new_rel)
                        updated["filename"] = (self.material_dir / new_rel).name
                        index_by_rel[new_rel] = updated
                    if file_hash:
                        file_hash_by_rel[new_rel] = file_hash
                        existing_rel_by_hash[file_hash] = new_rel
                        try:
                            if self._set_file_hash_state_cache_entry(
                                self.material_dir / new_rel,
                                new_rel,
                                file_hash,
                                state_cache_map,
                            ):
                                state_cache_changed = True
                        except Exception:
                            pass
                    self.log.info(f"AI 整理：已移动电子书文件 {old_rel} -> {new_rel}")
                    if is_incremental_candidate:
                        fix_review_done += 1
                        _log_progress(
                            "已归类增量纠偏",
                            fix_review_done,
                            len(fix_candidate_rel_set),
                            fix_moved,
                            fix_marked_skip + fix_kept,
                            fix_failed,
                        )
                    continue
                except Exception as e:
                    fix_failed += 1
                    if fix_failed <= self._AUTO_ORGANIZE_MAX_WARNINGS:
                        self.log.warning(f"AI 整理：移动电子书文件失败 {rel_hint}: {e}")
                    if is_incremental_candidate:
                        fix_review_done += 1
                        _log_progress(
                            "已归类增量纠偏",
                            fix_review_done,
                            len(fix_candidate_rel_set),
                            fix_moved,
                            fix_marked_skip + fix_kept,
                            fix_failed,
                        )
                    continue

            if not is_incremental_candidate:
                fix_non_incremental_skip += 1
                fix_kept += 1
                continue

            if file_hash and (file_hash in confirmed_hashes):
                fix_marked_skip += 1
                fix_review_done += 1
                _log_progress(
                    "已归类增量纠偏",
                    fix_review_done,
                    len(fix_candidate_rel_set),
                    fix_moved,
                    fix_marked_skip + fix_kept,
                    fix_failed,
                )
                continue

            if not can_use_ai:
                fix_kept += 1
                fix_review_done += 1
                _log_progress(
                    "已归类增量纠偏",
                    fix_review_done,
                    len(fix_candidate_rel_set),
                    fix_moved,
                    fix_marked_skip + fix_kept,
                    fix_failed,
                )
                continue

            try:
                target = self._classify_obvious_wrong_subject(
                    rel_hint,
                    path,
                    current_subject,
                    subjects,
                    index_by_rel.get(rel_hint),
                )
                if (not target) or (target == current_subject) or (target not in subject_set):
                    fix_kept += 1
                    if file_hash:
                        old_entry = confirmed_map.get(file_hash)
                        old_rel = str((old_entry or {}).get("rel") or "").strip() if isinstance(old_entry, dict) else ""
                        if old_rel != rel_hint:
                            confirmed_map[file_hash] = {"rel": rel_hint, "ts": int(time.time())}
                            confirmed_hashes.add(file_hash)
                            marks_changed = True
                    fix_review_done += 1
                    _log_progress(
                        "已归类增量纠偏",
                        fix_review_done,
                        len(fix_candidate_rel_set),
                        fix_moved,
                        fix_marked_skip + fix_kept,
                        fix_failed,
                    )
                    continue
                moved = self._move_material_to_subject(path, target)
                if not moved:
                    fix_kept += 1
                    fix_review_done += 1
                    _log_progress(
                        "已归类增量纠偏",
                        fix_review_done,
                        len(fix_candidate_rel_set),
                        fix_moved,
                        fix_marked_skip + fix_kept,
                        fix_failed,
                    )
                    continue
                old_rel, new_rel = moved
                move_map[old_rel] = new_rel
                fix_moved += 1
                old_rel_norm = self._normalize_rel(old_rel)
                if old_rel_norm and (old_rel_norm in state_cache_map):
                    state_cache_map.pop(old_rel_norm, None)
                    state_cache_changed = True
                item = index_by_rel.pop(old_rel, None)
                if isinstance(item, dict):
                    updated = dict(item)
                    updated["file_path"] = self._to_store_rel(new_rel)
                    updated["subject"] = self._subject_from_rel(new_rel)
                    updated["filename"] = (self.material_dir / new_rel).name
                    index_by_rel[new_rel] = updated
                if file_hash:
                    file_hash_by_rel[new_rel] = file_hash
                    existing_rel_by_hash[file_hash] = new_rel
                    try:
                        if self._set_file_hash_state_cache_entry(
                            self.material_dir / new_rel,
                            new_rel,
                            file_hash,
                            state_cache_map,
                        ):
                            state_cache_changed = True
                    except Exception:
                        pass
                self.log.info(f"AI 整理：已纠正明显错放文件 {old_rel} -> {new_rel}")
                fix_review_done += 1
                _log_progress(
                    "已归类增量纠偏",
                    fix_review_done,
                    len(fix_candidate_rel_set),
                    fix_moved,
                    fix_marked_skip + fix_kept,
                    fix_failed,
                )
            except Exception as e:
                fix_failed += 1
                if fix_failed <= self._AUTO_ORGANIZE_MAX_WARNINGS:
                    self.log.warning(f"AI 整理：复查文件失败 {rel_hint}: {e}")
                fix_review_done += 1
                _log_progress(
                    "已归类增量纠偏",
                    fix_review_done,
                    len(fix_candidate_rel_set),
                    fix_moved,
                    fix_marked_skip + fix_kept,
                    fix_failed,
                )
        if fix_failed > self._AUTO_ORGANIZE_MAX_WARNINGS:
            extra = fix_failed - self._AUTO_ORGANIZE_MAX_WARNINGS
            self.log.warning(f"AI 整理：错放复查失败过多，省略 {extra} 条")

        if marks_changed:
            try:
                self._save_material_scan_marks({"confirmed_ok_hashes": confirmed_map})
            except Exception as e:
                self.log.warning(f"AI 整理：保存标记文件 {self.material_scan_marks_path.name} 失败: {e}")

        if state_cache_changed or (not self.material_state_cache_path.exists()):
            try:
                self._save_material_state_cache(state_cache_map)
            except Exception as e:
                self.log.warning(f"AI 整理：保存状态缓存 {self.material_state_cache_path.name} 失败: {e}")

        self.log.info(
            "AI 整理：启动整理完成 "
            f"(TBD 扫描={len(tbd_files)}, 去重删除={tbd_deleted}, 移动={tbd_moved}, 保留={tbd_kept}, 失败={tbd_failed}; "
            f"已归类扫描={len(fix_files)}, 增量候选={len(fix_candidate_rel_set)}, "
            f"非增量跳过={fix_non_incremental_skip}, 已确认跳过={fix_marked_skip}, 移动={fix_moved}, 保留={fix_kept}, 失败={fix_failed})"
        )
        return move_map, new_file_hints, changed_classified_rels

    def _collect_existing_subject_dirs(self) -> List[str]:
        out: List[str] = []
        if not self.material_dir.exists():
            return out
        for p in sorted(self.material_dir.iterdir(), key=lambda x: x.name.lower()):
            if not p.is_dir():
                continue
            nm = str(p.name or "").strip()
            if not nm:
                continue
            if nm.startswith(".") or nm.startswith("_"):
                continue
            if nm.casefold() == self._AUTO_ORGANIZE_TBD_DIRNAME.casefold():
                continue
            out.append(nm)
        return out

    def _collect_tbd_files(self, tbd_dir: Path) -> List[Path]:
        out: List[Path] = []
        if not tbd_dir.exists():
            return out
        for p in tbd_dir.rglob("*"):
            if not p.is_file():
                continue
            if not self._is_auto_organize_suffix(p.suffix.lower()):
                continue
            if self._skip_file(p):
                continue
            out.append(p.resolve())
        out.sort(key=lambda x: str(x).lower())
        return out

    def _collect_classified_files(self, subject_set: set[str]) -> List[Path]:
        out: List[Path] = []
        if not self.material_dir.exists():
            return out
        for p in self.material_dir.rglob("*"):
            if not p.is_file():
                continue
            if not self._is_auto_organize_suffix(p.suffix.lower()):
                continue
            if self._skip_file(p):
                continue
            try:
                rel = p.relative_to(self.material_dir).as_posix()
            except Exception:
                continue
            top = self._subject_from_rel(rel)
            if top.casefold() == self._AUTO_ORGANIZE_TBD_DIRNAME.casefold():
                continue
            if top not in subject_set:
                continue
            out.append(p.resolve())
        out.sort(key=lambda x: str(x).lower())
        return out

    @classmethod
    def _is_auto_organize_suffix(cls, suffix: str) -> bool:
        s = str(suffix or "").strip().lower()
        return (s in cls._ALLOWED_SUFFIXES) or (s in cls._EBOOK_SUFFIXES)

    def _extract_material_snippet(self, path: Path, max_chars: int = 1600) -> str:
        p = Path(path)
        suffix = p.suffix.lower()
        if suffix == ".pdf":
            txt = self._read_pdf_head_fitz(p, max_pages=4, max_chars=max_chars)
            if self._has_enough_text(txt):
                return txt[:max_chars]
            txt2 = self._read_pdf_head(p, max_pages=4, max_chars=max_chars)
            if self._has_enough_text(txt2):
                return txt2[:max_chars]
            txt3 = self._read_pdf_head_ocr(p, max_pages=4, max_chars=max_chars)
            if self._has_enough_text(txt3):
                self.log.info(f"AI 索引：已使用 OCR 提取扫描 PDF 文本 {p.name}")
                return txt3[:max_chars]
            return (txt3 or txt2 or txt)[:max_chars]
        if suffix == ".doc":
            return self._read_doc_head(p, max_chars=max_chars)[:max_chars]
        if suffix == ".docx":
            return self._read_docx_head(p, max_chars=max_chars)[:max_chars]
        return ""

    def _index_item_map_by_rel(self, index_list: List[dict]) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        for item in index_list:
            if not isinstance(item, dict):
                continue
            rel = self._normalize_rel(item.get("file_path"))
            if (not rel) or (rel in out):
                continue
            out[rel] = item
        return out

    def _classify_tbd_target_subject(
        self,
        rel: str,
        path: Path,
        subjects: List[str],
        index_item: Optional[dict],
    ) -> Dict[str, object]:
        item = index_item if isinstance(index_item, dict) else {}
        item_summary = str(item.get("summary") or "").strip()
        item_keywords = item.get("keywords") or []
        if not isinstance(item_keywords, list):
            item_keywords = [str(item_keywords)]
        kw_text = ", ".join(str(x).strip() for x in item_keywords if str(x).strip())

        snippet = ""
        try:
            snippet = self._extract_material_snippet(path, max_chars=1800)
        except Exception:
            snippet = ""
        if len(snippet) > 1800:
            snippet = snippet[:1800]

        prompt = (
            "You are a conservative material organizer.\n"
            "Task: choose whether this file should be moved from TBD into an existing subject folder.\n"
            "Rules:\n"
            "- Move only when highly certain.\n"
            "- Never invent a new folder.\n"
            "- If uncertain, keep the file in TBD.\n"
            "Return strict JSON only with keys: action, target_subject, confidence, reason, keywords, summary.\n"
            "action must be 'move' or 'keep'.\n"
            "If action=move and you are confident, also return concise keywords and summary for indexing.\n"
            f"candidate_subjects={json.dumps(subjects, ensure_ascii=False)}\n"
            f"file_relative_path={rel}\n"
            f"file_name={path.name}\n"
            f"index_summary={item_summary}\n"
            f"index_keywords={kw_text}\n"
            f"content_snippet={snippet}\n"
        )
        obj = self._ask_subject_decision(prompt, timeout=120.0)
        move, target, confidence, obvious_flag, _reason = self._extract_move_decision(obj)
        summary_data = self._normalize_summary_data(obj, self._subject_from_rel(rel), path.name)
        if not move:
            return {"target": "", "snippet": snippet, "summary_data": summary_data}
        if target not in set(subjects):
            return {"target": "", "snippet": snippet, "summary_data": summary_data}
        if not self._is_high_confidence(confidence):
            return {"target": "", "snippet": snippet, "summary_data": summary_data}
        if isinstance(obvious_flag, bool) and (not obvious_flag):
            return {"target": "", "snippet": snippet, "summary_data": summary_data}
        return {"target": target, "snippet": snippet, "summary_data": summary_data}

    def _classify_obvious_wrong_subject(
        self,
        rel: str,
        path: Path,
        current_subject: str,
        subjects: List[str],
        index_item: Optional[dict],
    ) -> str:
        if len(subjects) <= 1:
            return ""

        item = index_item if isinstance(index_item, dict) else {}
        item_summary = str(item.get("summary") or "").strip()
        item_keywords = item.get("keywords") or []
        if not isinstance(item_keywords, list):
            item_keywords = [str(item_keywords)]
        kw_text = ", ".join(str(x).strip() for x in item_keywords if str(x).strip())

        snippet = ""
        if len(item_summary) < 30:
            try:
                snippet = self._extract_material_snippet(path, max_chars=1200)
            except Exception:
                snippet = ""
        if len(snippet) > 1200:
            snippet = snippet[:1200]

        prompt = (
            "You are reviewing subject placement for a learning material file.\n"
            "Goal: only detect obvious misclassification.\n"
            "Be very conservative: if there is any ambiguity, keep it unchanged.\n"
            "Only move when mismatch is obvious (for example, analog circuits placed in politics).\n"
            "Return strict JSON only with keys: action, target_subject, confidence, obvious_error, reason.\n"
            "action must be 'move' or 'keep'.\n"
            f"candidate_subjects={json.dumps(subjects, ensure_ascii=False)}\n"
            f"current_subject={current_subject}\n"
            f"file_relative_path={rel}\n"
            f"file_name={path.name}\n"
            f"index_summary={item_summary}\n"
            f"index_keywords={kw_text}\n"
            f"content_snippet={snippet}\n"
        )
        obj = self._ask_subject_decision(prompt, timeout=120.0)
        move, target, confidence, obvious_flag, _reason = self._extract_move_decision(obj)
        if not move:
            return ""
        if target not in set(subjects):
            return ""
        if target == current_subject:
            return ""
        if obvious_flag is not True:
            return ""
        if not self._is_high_confidence(confidence):
            return ""
        return target

    def _ask_subject_decision(self, prompt: str, timeout: float = 90.0) -> dict:
        payload = self._build_chat_payload(
            [{"role": "user", "content": str(prompt or "")}],
            0.0,
            response_format={"type": "json_object"},
        )
        url = self._join_url(self.deepseek_base_url, "chat/completions")
        data = self._post_json(url, payload, self.deepseek_api_key, timeout=float(timeout))
        text = self._extract_chat_text(data)
        obj = self._parse_json_object(text)
        if obj is None:
            raise RuntimeError("decision json parse failed")
        return obj

    @staticmethod
    def _parse_json_object(text: str) -> Optional[dict]:
        raw = str(text or "").strip()
        if not raw:
            return None
        obj = None
        try:
            obj = json.loads(raw)
        except Exception:
            m = re.search(r"\{[\s\S]*\}", raw)
            if not m:
                return None
            try:
                obj = json.loads(m.group(0))
            except Exception:
                return None
        return obj if isinstance(obj, dict) else None

    @staticmethod
    def _is_high_confidence(value: object) -> bool:
        if isinstance(value, (int, float)):
            try:
                return float(value) >= 0.85
            except Exception:
                return False
        v = str(value or "").strip().lower()
        if not v:
            return False
        if v in {"high", "very_high", "certain", "sure", "definite"}:
            return True
        m = re.match(r"^(\d+(?:\.\d+)?)%?$", v)
        if not m:
            return False
        try:
            score = float(m.group(1))
        except Exception:
            return False
        if v.endswith("%"):
            score = score / 100.0
        return score >= 0.85

    @staticmethod
    def _extract_move_decision(obj: dict) -> Tuple[bool, str, str, Optional[bool], str]:
        action = str(
            obj.get("action")
            or obj.get("decision")
            or obj.get("result")
            or ""
        ).strip().lower()
        target = str(
            obj.get("target_subject")
            or obj.get("target")
            or obj.get("subject")
            or obj.get("new_subject")
            or ""
        ).strip()
        confidence = str(obj.get("confidence") or obj.get("certainty") or "").strip().lower()
        reason = str(obj.get("reason") or obj.get("why") or "").strip()
        obvious_raw = obj.get("obvious_error")
        obvious_flag: Optional[bool] = obvious_raw if isinstance(obvious_raw, bool) else None

        keep_labels = {"keep", "stay", "hold", "no_move", "unchanged", "keep_as_is"}
        move_labels = {"move", "relocate", "correct", "migrate", "change"}

        if action in keep_labels:
            return False, target, confidence, obvious_flag, reason
        if action in move_labels:
            return True, target, confidence, obvious_flag, reason
        if isinstance(obvious_flag, bool):
            return bool(obvious_flag), target, confidence, obvious_flag, reason
        return False, target, confidence, obvious_flag, reason

    def _move_material_to_subject(self, src: Path, target_subject: str) -> Optional[Tuple[str, str]]:
        p = Path(src)
        if (not p.exists()) or (not p.is_file()):
            return None
        target = str(target_subject or "").strip()
        if not target:
            return None
        try:
            old_rel = p.relative_to(self.material_dir).as_posix()
        except Exception:
            return None

        dst_dir = self.material_dir / target
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = self._next_available_path(dst_dir / p.name)
        shutil.move(str(p), str(dst))
        new_rel = dst.relative_to(self.material_dir).as_posix()
        return old_rel, new_rel

    @staticmethod
    def _next_available_path(path: Path) -> Path:
        if not path.exists():
            return path
        stem = path.stem
        suffix = path.suffix
        for i in range(1, 1000):
            cand = path.with_name(f"{stem}__auto{i}{suffix}")
            if not cand.exists():
                return cand
        return path.with_name(f"{stem}__auto{int(time.time())}{suffix}")

    def _remap_index_items_after_move(self, index_list: List[dict], move_map: Dict[str, str]) -> List[dict]:
        if not move_map:
            return index_list
        out: List[dict] = []
        for item in index_list:
            if not isinstance(item, dict):
                continue
            rel = self._normalize_rel(item.get("file_path"))
            new_rel = move_map.get(rel)
            if not new_rel:
                out.append(item)
                continue
            updated = dict(item)
            updated["file_path"] = self._to_store_rel(new_rel)
            updated["subject"] = self._subject_from_rel(new_rel)
            updated["filename"] = (self.material_dir / new_rel).name
            out.append(updated)
        return out

    def _remap_metadata_items_after_move(self, metadata_list: List[dict], move_map: Dict[str, str]) -> List[dict]:
        if not move_map:
            return metadata_list
        out: List[dict] = []
        for item in metadata_list:
            if not isinstance(item, dict):
                continue
            rel = self._normalize_rel(item.get("file_path"))
            new_rel = move_map.get(rel)
            if not new_rel:
                out.append(item)
                continue
            updated = dict(item)
            updated["file_path"] = self._to_store_rel(new_rel)
            updated["subject"] = self._subject_from_rel(new_rel)
            updated["filename"] = (self.material_dir / new_rel).name
            out.append(updated)
        return out

    def _bootstrap_quick_sync_sync(self) -> None:
        self.material_dir.mkdir(parents=True, exist_ok=True)
        self._load_api_config()

        self.log.info("AI 启动阶段：加载缓存索引/元数据/向量")
        index_by_rel, metadata_by_rel, vector_by_rel = self._load_incremental_store_maps()
        self._set_semantic_cache_from_maps(metadata_by_rel, vector_by_rel)

        self.log.info(
            "AI 快速启动："
            f"索引={len(index_by_rel)}, 元数据={len(metadata_by_rel)}, "
            f"向量={len(vector_by_rel)}, "
            f"语义检索就绪={self.semantic_ready}"
        )

    def _bootstrap_sync_sync(self) -> None:
        self.material_dir.mkdir(parents=True, exist_ok=True)
        self._load_api_config()

        self.log.info("AI 启动阶段：加载启动后同步所需缓存索引")
        index_by_rel, metadata_by_rel, vector_by_rel = self._load_incremental_store_maps()
        old_index_rels = set(index_by_rel.keys())

        move_map: Dict[str, str] = {}
        new_file_hints: Dict[str, dict] = {}
        changed_classified_rels: set[str] = set()
        self.log.info("AI 启动阶段：整理资料（TBD + 增量纠偏）")
        try:
            move_map, new_file_hints, changed_classified_rels = self._auto_organize_materials_on_boot(
                [index_by_rel[k] for k in sorted(index_by_rel.keys())]
            )
        except Exception as e:
            self.log.warning(f"AI 整理：启动整理失败，已回退并继续启动: {e}")
            move_map = {}
            new_file_hints = {}
            changed_classified_rels = set()

        index_upserts: Dict[str, dict] = {}
        index_deletes: set[str] = set()
        metadata_upserts: Dict[str, dict] = {}
        metadata_deletes: set[str] = set()
        vector_upserts: Dict[str, np.ndarray] = {}
        vector_deletes: set[str] = set()

        if move_map:
            for old_rel, new_rel in move_map.items():
                old_norm = self._normalize_rel(old_rel)
                new_norm = self._normalize_rel(new_rel)
                if (not old_norm) or (not new_norm):
                    continue

                idx_item = index_by_rel.pop(old_norm, None)
                if old_norm != new_norm:
                    index_deletes.add(old_norm)
                if isinstance(idx_item, dict):
                    updated = dict(idx_item)
                    updated["file_path"] = self._to_store_rel(new_norm)
                    updated["subject"] = self._subject_from_rel(new_norm)
                    updated["filename"] = (self.material_dir / new_norm).name
                    index_by_rel[new_norm] = updated
                    index_upserts[new_norm] = updated

                meta_item = metadata_by_rel.pop(old_norm, None)
                if old_norm != new_norm:
                    metadata_deletes.add(old_norm)
                if isinstance(meta_item, dict):
                    meta_updated = dict(meta_item)
                    meta_updated["file_path"] = self._to_store_rel(new_norm)
                    meta_updated["subject"] = self._subject_from_rel(new_norm)
                    meta_updated["filename"] = (self.material_dir / new_norm).name
                    metadata_by_rel[new_norm] = meta_updated
                    metadata_upserts[new_norm] = meta_updated

                vec_item = vector_by_rel.pop(old_norm, None)
                if old_norm != new_norm:
                    vector_deletes.add(old_norm)
                if isinstance(vec_item, np.ndarray):
                    vec_arr = np.asarray(vec_item, dtype=np.float64).reshape(-1)
                    if vec_arr.size > 0:
                        vector_by_rel[new_norm] = vec_arr
                        vector_upserts[new_norm] = vec_arr

        actual_rels = self._scan_material_files()
        known_rels = set(index_by_rel.keys()) | set(metadata_by_rel.keys()) | set(vector_by_rel.keys())
        stale_rels = known_rels - actual_rels
        for rel in stale_rels:
            if rel in index_by_rel:
                index_by_rel.pop(rel, None)
                index_deletes.add(rel)
            if rel in metadata_by_rel:
                metadata_by_rel.pop(rel, None)
                metadata_deletes.add(rel)
            if rel in vector_by_rel:
                vector_by_rel.pop(rel, None)
                vector_deletes.add(rel)

        existing_index_rels: set[str] = set()
        for rel in sorted(actual_rels):
            item = index_by_rel.get(rel)
            if not isinstance(item, dict):
                continue
            normalized = self._normalize_index_item(item, rel, self.material_dir / rel)
            if normalized != item:
                index_by_rel[rel] = normalized
                index_upserts[rel] = normalized
            existing_index_rels.add(rel)

        new_rels = sorted(actual_rels - existing_index_rels)
        changed_norm_rels = {
            self._normalize_rel(rel)
            for rel in (changed_classified_rels or set())
            if self._normalize_rel(rel)
        }
        modified_rels = sorted((changed_norm_rels & actual_rels) - set(new_rels))
        pipeline_targets = []
        seen_pipeline = set()
        for rel in new_rels + modified_rels:
            if rel in seen_pipeline:
                continue
            seen_pipeline.add(rel)
            pipeline_targets.append(rel)

        pipeline_by_rel: Dict[str, dict] = {}
        pipeline_errors: Dict[str, str] = {}
        if pipeline_targets:
            self.log.info(f"AI 启动阶段：新/变更文件摘要流水线开始，总数={len(pipeline_targets)}")
            max_summary_workers = max(1, min(int(self._NEW_FILE_SUMMARY_MAX_CONCURRENCY), len(pipeline_targets)))
            if max_summary_workers <= 1:
                for rel in pipeline_targets:
                    try:
                        hint = new_file_hints.get(rel)
                        pipeline_by_rel[rel] = self._run_new_file_pipeline(rel, hint=hint, build_vector=False)
                    except Exception as e:
                        pipeline_errors[rel] = str(e)
            else:
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_summary_workers) as executor:
                    future_to_rel: Dict[concurrent.futures.Future, str] = {}
                    for rel in pipeline_targets:
                        hint = new_file_hints.get(rel)
                        fut = executor.submit(self._run_new_file_pipeline, rel, hint, False)
                        future_to_rel[fut] = rel
                    for fut in concurrent.futures.as_completed(future_to_rel):
                        rel = future_to_rel[fut]
                        try:
                            ctx = fut.result()
                            if isinstance(ctx, dict):
                                pipeline_by_rel[rel] = ctx
                            else:
                                pipeline_errors[rel] = "pipeline result is not dict"
                        except Exception as e:
                            pipeline_errors[rel] = str(e)

        new_rel_set = set(new_rels)
        modified_rel_set = set(modified_rels)
        for idx, rel in enumerate(pipeline_targets, 1):
            ctx = pipeline_by_rel.get(rel)
            if not isinstance(ctx, dict):
                err = str(pipeline_errors.get(rel) or "pipeline context missing")
                self.log.warning(f"AI 索引：处理失败 {rel}: {err}")
                continue

            idx_item = ctx.get("index_item")
            if not isinstance(idx_item, dict):
                self.log.warning(f"AI 索引：处理失败 {rel}: pipeline index item missing")
                continue
            index_by_rel[rel] = idx_item
            index_upserts[rel] = idx_item

            raw_meta = ctx.get("metadata_item")
            if isinstance(raw_meta, dict):
                meta_item = dict(raw_meta)
            else:
                meta_item = {
                    "file_path": self._to_store_rel(rel),
                    "filename": str(idx_item.get("filename") or Path(rel).name),
                    "subject": str(idx_item.get("subject") or self._subject_from_rel(rel)),
                }
            metadata_by_rel[rel] = meta_item
            metadata_upserts[rel] = meta_item

            if rel in new_rel_set:
                self.log.info(f"AI 索引：新增[{idx}/{len(pipeline_targets)}] {rel}")
            elif rel in modified_rel_set:
                self.log.info(f"AI 索引：更新[{idx}/{len(pipeline_targets)}] {rel}")
            else:
                self.log.info(f"AI 索引：处理[{idx}/{len(pipeline_targets)}] {rel}")

        if pipeline_targets:
            self.log.info(
                "AI 启动阶段：新/变更文件摘要流水线完成 "
                f"(成功={len(pipeline_by_rel)}, 失败={max(0, len(pipeline_targets) - len(pipeline_by_rel))})"
            )

        vector_candidate_rels: List[str] = []
        seen_vector_candidate = set()
        for rel in pipeline_targets:
            if rel not in pipeline_by_rel:
                continue
            should_embed = False
            if rel in modified_rel_set:
                should_embed = True
            elif (rel in new_rel_set) and (rel not in vector_by_rel):
                should_embed = True
            if (not should_embed) or (rel in seen_vector_candidate):
                continue
            seen_vector_candidate.add(rel)
            vector_candidate_rels.append(rel)

        if vector_candidate_rels:
            self.log.info(f"AI 启动阶段：新/变更文件向量流水线开始，总数={len(vector_candidate_rels)}")
            embed_done = 0
            embed_ok = 0
            embed_fail = 0

            def _log_embed_progress() -> None:
                if len(vector_candidate_rels) <= 0:
                    return
                if embed_done < len(vector_candidate_rels) and (embed_done % int(self._ORGANIZE_PROGRESS_EVERY) != 0):
                    return
                self.log.info(
                    f"AI 启动进度：新/变更文件向量 {embed_done}/{len(vector_candidate_rels)} "
                    f"(成功={embed_ok}, 失败={embed_fail})"
                )

            max_embed_workers = max(1, min(int(self._NEW_FILE_EMBED_MAX_CONCURRENCY), len(vector_candidate_rels)))
            if max_embed_workers <= 1:
                for rel in vector_candidate_rels:
                    ctx = pipeline_by_rel.get(rel)
                    if not isinstance(ctx, dict):
                        embed_done += 1
                        embed_fail += 1
                        _log_embed_progress()
                        continue
                    ctx["vector_attempted"] = True
                    try:
                        vec = self._build_vector_for_embedding_text(str(ctx.get("embedding_text") or ""))
                        ctx["vector"] = vec
                        if isinstance(vec, np.ndarray):
                            embed_ok += 1
                        else:
                            embed_fail += 1
                    except Exception as e:
                        ctx["vector"] = None
                        ctx["vector_error"] = str(e)
                        embed_fail += 1
                    finally:
                        embed_done += 1
                        _log_embed_progress()
            else:
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_embed_workers) as executor:
                    future_to_rel: Dict[concurrent.futures.Future, str] = {}
                    for rel in vector_candidate_rels:
                        ctx = pipeline_by_rel.get(rel)
                        if not isinstance(ctx, dict):
                            embed_done += 1
                            embed_fail += 1
                            _log_embed_progress()
                            continue
                        ctx["vector_attempted"] = True
                        fut = executor.submit(self._build_vector_for_embedding_text, str(ctx.get("embedding_text") or ""))
                        future_to_rel[fut] = rel
                    for fut in concurrent.futures.as_completed(future_to_rel):
                        rel = future_to_rel[fut]
                        ctx = pipeline_by_rel.get(rel)
                        if not isinstance(ctx, dict):
                            continue
                        try:
                            vec = fut.result()
                            ctx["vector"] = vec
                            if isinstance(vec, np.ndarray):
                                embed_ok += 1
                            else:
                                embed_fail += 1
                        except Exception as e:
                            ctx["vector"] = None
                            ctx["vector_error"] = str(e)
                            embed_fail += 1
                        finally:
                            embed_done += 1
                            _log_embed_progress()

            for rel in vector_candidate_rels:
                ctx = pipeline_by_rel.get(rel)
                if not isinstance(ctx, dict):
                    continue
                vec_obj = ctx.get("vector")
                vec = None
                if isinstance(vec_obj, np.ndarray):
                    vec = vec_obj
                if vec is None:
                    idx_item = index_by_rel.get(rel)
                    if isinstance(idx_item, dict):
                        vec = self._build_vector_for_index_item(idx_item)
                if vec is None:
                    continue
                arr = np.asarray(vec, dtype=np.float64).reshape(-1)
                if arr.size <= 0:
                    continue
                vector_by_rel[rel] = arr
                vector_upserts[rel] = arr

            self.log.info(
                "AI 启动阶段：新/变更文件向量流水线完成 "
                f"(成功={embed_ok}, 失败={embed_fail})"
            )

        self.log.info("AI 启动阶段：写入增量索引更新")
        try:
            self._persist_incremental_store_changes(
                index_upserts=index_upserts,
                index_deletes=index_deletes,
                metadata_upserts=metadata_upserts,
                metadata_deletes=metadata_deletes,
                vector_upserts=vector_upserts,
                vector_deletes=vector_deletes,
            )
        except Exception as e:
            self.log.warning(f"AI 索引：增量写入失败，已继续运行: {e}")

        semantic_delete_rels = set(metadata_deletes) | set(vector_deletes) | set(index_deletes)
        if self._semantic_entry_by_rel:
            self._apply_semantic_cache_changes(
                metadata_upserts=metadata_upserts,
                vector_upserts=vector_upserts,
                delete_rels=semantic_delete_rels,
            )
        else:
            self._set_semantic_cache_from_maps(metadata_by_rel, vector_by_rel)

        added = len([rel for rel in index_upserts.keys() if rel not in old_index_rels])
        updated = len(index_upserts) - added
        removed = len(index_deletes)
        self.log.info(
            f"AI 索引：同步完成，现有索引 {len(index_by_rel)} 条，向量 {len(vector_by_rel)} 条，新增 {added}，更新 {updated}，清理 {removed}"
        )

    def _load_api_config(self) -> None:
        lines: List[str] = []
        try:
            lines = [x.strip() for x in self.api_key_path.read_text(encoding="utf-8").splitlines() if x.strip()]
        except Exception as e:
            self.log.warning(f"AI 配置：读取 api_key.txt 失败: {e}")
            return

        if len(lines) < 4:
            self.log.warning("AI 配置：api_key.txt 至少需要 4 行（deepseek base/key + embedding base/key）")
            return

        self.deepseek_base_url = lines[0].rstrip("/")
        self.deepseek_api_key = lines[1]
        self.embedding_base_url = lines[2].rstrip("/")
        self.embedding_api_key = lines[3]
        self.log.info("AI 配置：已加载 DeepSeek 与 Embedding API")

    def _reload_semantic_cache(self) -> None:
        metadata_by_rel: Dict[str, dict] = {}
        vector_by_rel: Dict[str, np.ndarray] = {}
        try:
            _index_by_rel, metadata_by_rel, vector_by_rel = self._load_incremental_store_maps()
        except Exception as e:
            self.log.warning(f"AI 检索：增量缓存重载失败: {e}")
            return

        self._set_semantic_cache_from_maps(metadata_by_rel, vector_by_rel)
        if not self.semantic_ready:
            self.log.warning("AI 检索：向量库为空，/find 引号语义检索不可用")
            return
        self.log.info(
            f"AI 检索：载入 {len(self._semantic_meta)} 条向量，维度 {int(self._semantic_norm_vectors.shape[1])}"
        )

    def _semantic_find_paths_sync(self, demand: str, limit: Optional[int] = None) -> List[Path]:
        q = str(demand or "").strip()
        if (not q) or (not self.semantic_ready):
            return []

        q_vec = self._embed_text(q)
        if q_vec is None:
            return []
        q_arr = np.asarray(q_vec, dtype=np.float64).reshape(-1)
        if q_arr.size <= 0:
            return []

        q_norm = np.linalg.norm(q_arr)
        if q_norm == 0.0:
            return []
        q_arr = q_arr / q_norm

        with self._lock:
            active_count = int(self._semantic_active_count)
            if active_count <= 0:
                return []
            meta = list(self._semantic_meta[:active_count])
            norm_vectors = self._semantic_norm_vectors[:active_count].copy()

        if norm_vectors.ndim != 2 or norm_vectors.shape[0] != len(meta):
            return []
        if norm_vectors.shape[1] != q_arr.size:
            self.log.warning(
                f"AI 检索：查询向量维度不匹配 ({q_arr.size} != {norm_vectors.shape[1]})"
            )
            return []

        sims = np.dot(norm_vectors, q_arr)
        if sims.ndim != 1:
            return []

        top_k = max(1, min(int(limit or self.search_limit), int(norm_vectors.shape[0])))
        min_sim = float(self.search_min_similarity)

        order = np.argsort(-sims)
        out: List[Path] = []
        seen = set()
        for idx in order:
            if len(out) >= top_k:
                break
            score = float(sims[idx])
            if score < min_sim:
                continue
            rel = self._normalize_rel((meta[idx] or {}).get("file_path"))
            if not rel or rel in seen:
                continue
            seen.add(rel)
            p = self.material_dir / rel
            if p.exists() and p.is_file():
                out.append(p.resolve())
        return out

    def _chat_sync(self, user_input: str) -> str:
        if not self.chat_ready:
            raise RuntimeError("chat not ready")

        content = str(user_input or "").strip()
        if not content:
            return "想聊点啥？发我一句话就行。"

        payload = self._build_chat_payload(
            [
                {
                    "role": "system",
                    "content": self._append_web_search_judge(self._append_chat_automation_boundary(self.system_prompt)),
                },
                {"role": "user", "content": content},
            ],
            self._CHAT_TEMPERATURE,
            enable_thinking=True,
        )
        url = self._join_url(self.deepseek_base_url, "chat/completions")
        data = self._post_json(url, payload, self.deepseek_api_key, timeout=90.0)
        text = self._extract_chat_text(data)
        if self.web_search_enabled:
            query = self._parse_web_search_marker(text)
            if query:
                try:
                    material = self._web_search_fetch_sources_sync(query)
                    text = self._web_search_compose_final_sync(
                        self._append_web_search_judge(self._append_chat_automation_boundary(self.system_prompt)),
                        content,
                        material,
                    )
                except Exception as e:
                    self.log.warning(f"AI web search failed, fallback to plain chat: err={e}")
                    text = self._web_search_fallback_chat_sync(
                        [
                            {"role": "system", "content": self._append_chat_automation_boundary(self.system_prompt)},
                            {"role": "user", "content": content},
                        ]
                    )
        if not text:
            raise RuntimeError("empty chat response")
        return text

    def _restricted_gemini_calendar_chat_sync(
        self,
        user_input: str,
        model_key: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> str:
        if not self.gemini_chat_ready:
            raise RuntimeError("gemini chat not ready")
        content = str(user_input or "").strip()
        if not content:
            raise RuntimeError("calendar web request is empty")
        prompt = self._build_restricted_gemini_cli_prompt("", [], content)
        return self._run_gemini_cli_sync(
            prompt,
            self._resolve_gemini_cli_model(model_key),
            restricted=True,
            timeout_seconds=timeout_seconds,
        )

    def _gemini_chat_sync(self, user_input: str, model_key: Optional[str] = None, restricted: bool = False) -> str:
        if not self.gemini_chat_ready:
            raise RuntimeError("gemini chat not ready")

        content = str(user_input or "").strip()
        if not content:
            return "想聊点啥？发我一句话就行。"

        system_prompt = self._append_chat_automation_boundary(self.system_prompt)
        if restricted:
            prompt = self._build_restricted_gemini_cli_prompt(system_prompt, [], content)
            return self._run_gemini_cli_sync(prompt, self._resolve_gemini_cli_model(model_key), restricted=True)
        prompt = self._build_gemini_cli_prompt(system_prompt, [], content)
        return self._run_gemini_cli_sync(prompt, self._resolve_gemini_cli_model(model_key))

    def _chat_with_context_sync(
        self,
        session_key: str,
        user_input: str,
        *,
        msg_id: str = "",
        vision_slots: Optional[list] = None,
    ) -> str:
        if not self.chat_ready:
            raise RuntimeError("chat not ready")

        content = str(user_input or "").strip()
        slots = self._normalize_vision_slots(vision_slots)
        if not content and not slots:
            return self._chat_sync(content)

        key = str(session_key or "").strip()
        if not key:
            return self._chat_sync(content)

        raw_content = content

        history: List[Dict[str, str]] = []
        try:
            history = self._load_active_chat_history(key)
            history = self._select_history_for_backend(history, "deepseek")
            model_history = self._materialize_history_for_model(history)
        except Exception as e:
            self.log.warning(f"AI chat context read failed, fallback to stateless: session={key[:80]} err={e}")
            history = []
            model_history = []

        system_prompt = self._append_web_search_judge(
            self._append_chat_automation_boundary(self._select_chat_system_prompt(key) or self.system_prompt)
        )
        # 当前消息：基础文字 + 视觉 slots 渲染为发给模型的文本
        current_message = {"role": "user", "content": content}
        if slots:
            current_message["_vision"] = slots
        current_rendered = self._render_chat_message_content(current_message)

        payload = self._build_chat_payload(
            [{"role": "system", "content": system_prompt}, *model_history, {"role": "user", "content": current_rendered}],
            self._CHAT_TEMPERATURE,
            enable_thinking=True,
        )
        url = self._join_url(self.deepseek_base_url, "chat/completions")
        data = self._post_json(url, payload, self.deepseek_api_key, timeout=90.0)
        text = self._extract_chat_text(data)
        if self.web_search_enabled:
            query = self._parse_web_search_marker(text)
            if query:
                try:
                    material = self._web_search_fetch_sources_sync(query)
                    text = self._web_search_compose_final_sync(
                        system_prompt,
                        current_rendered,
                        material,
                        history=model_history,
                    )
                except Exception as e:
                    self.log.warning(f"AI web search failed, fallback to plain chat: err={e}")
                    plain_system = self._append_chat_automation_boundary(
                        self._select_chat_system_prompt(key) or self.system_prompt
                    )
                    text = self._web_search_fallback_chat_sync(
                        [
                            {"role": "system", "content": plain_system},
                            *model_history,
                            {"role": "user", "content": current_rendered},
                        ]
                    )
        if not text:
            raise RuntimeError("empty chat response")

        try:
            self._save_chat_turn(key, raw_content, text, msg_id=msg_id, vision_slots=slots)
        except Exception as e:
            self.log.warning(f"AI chat context write failed, keep stateless next turn: session={key[:80]} err={e}")
        return text

    def _append_web_search_judge(self, system_prompt: str) -> str:
        """联网开关开启时在 system prompt 后追加联网判定指令；关闭时原样返回。"""
        prompt = str(system_prompt or "").strip()
        if not self.web_search_enabled:
            return prompt
        judge = self._WEB_SEARCH_JUDGE_PROMPT.strip()
        if not prompt:
            return judge
        if judge in prompt:
            return prompt
        return f"{prompt}\n\n{judge}"

    @staticmethod
    def _parse_web_search_marker(text: str) -> Optional[str]:
        """解析回复文本中的 [WEB_SEARCH] 标记，返回标记后的查询词；无标记返回 None。"""
        raw = str(text or "")
        idx = raw.find("[WEB_SEARCH]")
        if idx < 0:
            return None
        query = raw[idx + len("[WEB_SEARCH]"):].strip()
        return query or None

    def _web_search_fetch_sources_sync(self, query: str) -> str:
        """调用 v4-flash（Responses API + web_search）执行联网搜索，返回搜索素材文本。"""
        if not self.deepseek_api_key:
            raise RuntimeError("deepseek api key not ready")
        q = str(query or "").strip()
        if not q:
            raise RuntimeError("empty web search query")
        payload = {
            "model": str(self.web_search_model or self._DEEPSEEK_V4_FLASH_MODEL),
            "input": [
                {"role": "system", "content": self._WEB_SEARCH_FETCH_SYSTEM_PROMPT},
                {"role": "user", "content": q},
            ],
            "tools": [{"type": "web_search"}],
            "tool_choice": {"type": "web_search"},
        }
        url = self._join_url(self.deepseek_base_url, "responses")
        data = self._post_json(url, payload, self.deepseek_api_key, timeout=60.0)
        material = self._extract_responses_search_sources(data)
        if not material:
            raise RuntimeError("web search returned no usable sources")
        return material

    @staticmethod
    def _extract_responses_search_sources(resp: dict) -> str:
        """从 DeepSeek Responses API 响应中提取搜索素材文本；无素材返回空串。"""
        output = (resp or {}).get("output")
        if not isinstance(output, list):
            return ""
        parts: List[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            itype = item.get("type")
            if itype == "function_call":
                args = str(item.get("arguments") or "")
                try:
                    obj = json.loads(args)
                except Exception:
                    obj = {}
                if not isinstance(obj, dict):
                    continue
                results = obj.get("search_results")
                if not isinstance(results, list):
                    continue
                for r in results:
                    if not isinstance(r, dict):
                        continue
                    block = []
                    title = str(r.get("title") or "").strip()
                    url = str(r.get("url") or "").strip()
                    content = str(r.get("content") or "").strip()
                    if title:
                        block.append(f"标题：{title}")
                    if url:
                        block.append(f"来源：{url}")
                    if content:
                        block.append(content)
                    if block:
                        parts.append("\n".join(block))
            elif itype == "message":
                content_items = item.get("content")
                if not isinstance(content_items, list):
                    continue
                texts = []
                for c in content_items:
                    if isinstance(c, dict) and c.get("type") == "output_text":
                        t = str(c.get("text") or "").strip()
                        if t:
                            texts.append(t)
                if texts:
                    parts.append("【搜索结果摘要】\n" + "\n".join(texts))
        return "\n\n".join(parts).strip()

    def _web_search_compose_final_sync(
        self,
        system_prompt: str,
        user_content: str,
        material: str,
        *,
        history: Optional[List[dict]] = None,
    ) -> str:
        """将原始问题（含当前视觉渲染）与搜索素材交给 v4-pro 整合，返回最终回答文本。"""
        compose_system = f"{str(system_prompt or '').strip()}\n\n{self._WEB_SEARCH_COMPOSE_PROMPT.strip()}"
        prompt = f"用户当前问题：\n{str(user_content or '').strip()}\n\n【联网搜索结果】\n{str(material or '').strip()}"
        messages: List[dict] = [{"role": "system", "content": compose_system}]
        for message in history or []:
            if not isinstance(message, dict):
                continue
            role = str(message.get("role") or "").strip()
            if role in ("user", "assistant"):
                messages.append({"role": role, "content": str(message.get("content") or "")})
        messages.append({"role": "user", "content": prompt})
        payload = self._build_chat_payload(
            messages,
            self._CHAT_TEMPERATURE,
            enable_thinking=True,
        )
        url = self._join_url(self.deepseek_base_url, "chat/completions")
        data = self._post_json(url, payload, self.deepseek_api_key, timeout=90.0)
        text = self._extract_chat_text(data)
        if not text:
            raise RuntimeError("empty web search final response")
        return text

    @staticmethod
    def _strip_web_search_marker(text: str) -> str:
        """从回复中删除 [WEB_SEARCH] 标记行，避免内部标记泄漏给用户。"""
        s = re.sub(r"\[WEB_SEARCH\][^\n]*", "", str(text or ""))
        s = re.sub(r"\n{2,}", "\n", s)
        return s.strip()

    def _web_search_fallback_chat_sync(self, messages: List[dict]) -> str:
        """联网失效时用不含联网判定指令的 prompt 重新请求 v4-pro，按普通模式回答。"""
        payload = self._build_chat_payload(
            list(messages),
            self._CHAT_TEMPERATURE,
            enable_thinking=True,
        )
        url = self._join_url(self.deepseek_base_url, "chat/completions")
        data = self._post_json(url, payload, self.deepseek_api_key, timeout=90.0)
        text = self._extract_chat_text(data)
        query = self._parse_web_search_marker(text)
        if query:
            text = self._strip_web_search_marker(text)
        if not text:
            raise RuntimeError("empty chat response")
        return text

    def _gemini_chat_with_context_sync(
        self,
        session_key: str,
        user_input: str,
        model_key: Optional[str] = None,
        restricted: bool = False,
        *,
        msg_id: str = "",
        vision_slots: Optional[list] = None,
    ) -> str:
        if not self.gemini_chat_ready:
            raise RuntimeError("gemini chat not ready")

        content = str(user_input or "").strip()
        slots = self._normalize_vision_slots(vision_slots)
        if not content and not slots:
            return self._gemini_chat_sync(content, model_key, restricted)

        key = str(session_key or "").strip()
        if not key:
            return self._gemini_chat_sync(content, model_key, restricted)

        try:
            history = self._load_active_chat_history(key)
            backend_key = (
                "claude"
                if str(model_key or "").strip().lower()
                in {"claude", "opus", "opus4.6", "claude-opus"}
                else "gemini"
            )
            history = self._select_history_for_backend(history, backend_key)
            model_history = self._materialize_history_for_model(history)
        except Exception as e:
            self.log.warning(f"AI chat context read failed, fallback to stateless gemini: session={key[:80]} err={e}")
            history = []
            model_history = []

        # 当前消息：基础文字 + 视觉 slots 渲染
        current_message = {"role": "user", "content": content}
        if slots:
            current_message["_vision"] = slots
        current_rendered = self._render_chat_message_content(current_message)

        system_prompt = self._append_chat_automation_boundary(self._select_chat_system_prompt(key) or self.system_prompt)
        if restricted:
            prompt = self._build_restricted_gemini_cli_prompt(system_prompt, model_history, current_rendered)
            text = self._run_gemini_cli_sync(prompt, self._resolve_gemini_cli_model(model_key), restricted=True)
        else:
            prompt = self._build_gemini_cli_prompt(system_prompt, model_history, current_rendered)
            text = self._run_gemini_cli_sync(prompt, self._resolve_gemini_cli_model(model_key))

        try:
            self._save_chat_turn(key, content, text, msg_id=msg_id, vision_slots=slots)
        except Exception as e:
            self.log.warning(f"AI chat context write failed, keep stateless next turn: session={key[:80]} err={e}")
        return text

    def _normalize_chat_history_item(self, item: object) -> Optional[Dict[str, str]]:
        if not isinstance(item, dict):
            return None
        role = str(item.get("role") or "").strip().lower()
        if role not in ("user", "assistant"):
            return None
        content = str(item.get("content") or "")
        out: Dict[str, str] = {"role": role, "content": content}
        if role == "user":
            msg_id = str(item.get("_msg_id") or "").strip()
            if msg_id:
                out["_msg_id"] = msg_id
            vision = self._normalize_vision_slots(item.get("_vision"))
            if vision:
                out["_vision"] = vision
        return out

    @staticmethod
    def _normalize_vision_slots(raw: object) -> List[dict]:
        """严格过滤 _vision 字段类型，只保留合法 slot 字典（兼容 VisionSlot 对象）。"""
        if not isinstance(raw, list):
            return []
        out: List[dict] = []
        for item in raw:
            if isinstance(item, VisionSlot):
                item = item.to_dict()
            if not isinstance(item, dict):
                continue
            slot_id = str(item.get("slot_id") or "").strip()
            if not slot_id:
                continue
            try:
                index = int(item.get("index") or 1)
            except Exception:
                index = 1
            status = str(item.get("status") or "unresolved").strip()
            if status not in {"unresolved", "ready", "retryable_error", "permanent_error"}:
                status = "unresolved"
            slot: dict = {
                "slot_id": slot_id,
                "index": max(1, index),
                "segment_type": str(item.get("segment_type") or "image").strip() or "image",
                "status": status,
                "url": str(item.get("url") or ""),
                "file": str(item.get("file") or ""),
                "file_id": str(item.get("file_id") or ""),
                "path": str(item.get("path") or ""),
                "name": str(item.get("name") or ""),
                "summary": str(item.get("summary") or ""),
                "face_id": str(item.get("face_id") or ""),
                "source_key": str(item.get("source_key") or ""),
                "content_hash": str(item.get("content_hash") or ""),
                "description": str(item.get("description") or ""),
                "retry_after_ts": float(item.get("retry_after_ts") or 0.0),
                "source_kind": str(item.get("source_kind") or "message"),
            }
            out.append(slot)
        return out

    def _validate_and_trim_chat_history(self, messages: List[Dict[str, str]]) -> Optional[List[Dict[str, str]]]:
        if not isinstance(messages, list):
            return None
        out: List[Dict[str, str]] = []
        for item in messages:
            normalized = self._normalize_chat_history_item(item)
            if normalized is None:
                return None
            out.append(normalized)
        max_messages = max(1, int(self._CHAT_CONTEXT_MAX_MESSAGES))
        if len(out) > max_messages:
            out = out[-max_messages:]
        return out

    def _load_active_chat_history(self, session_key: str, now_ts: Optional[float] = None) -> List[Dict[str, str]]:
        key = str(session_key or "").strip()
        if not key:
            return []
        use_ts = float(now_ts if now_ts is not None else time.time())
        with self._chat_sessions_lock:
            return self._load_active_chat_history_locked(key, use_ts)

    def _load_active_chat_history_locked(self, session_key: str, now_ts: float) -> List[Dict[str, str]]:
        entry = self._chat_sessions.get(session_key)
        if entry is None:
            return []
        if not isinstance(entry, dict):
            self._chat_sessions.pop(session_key, None)
            self.log.warning(f"AI chat context invalid session entry, reset: session={session_key[:80]}")
            return []

        try:
            last_active_ts = float(entry.get("last_active_ts") or 0.0)
        except Exception:
            self._chat_sessions.pop(session_key, None)
            self.log.warning(f"AI chat context invalid last_active_ts, reset: session={session_key[:80]}")
            return []
        if (last_active_ts <= 0.0) or ((float(now_ts) - last_active_ts) > float(self._CHAT_CONTEXT_TTL_SECONDS)):
            self._chat_sessions.pop(session_key, None)
            return []

        raw_messages = entry.get("messages")
        if not isinstance(raw_messages, list):
            self._chat_sessions.pop(session_key, None)
            self.log.warning(f"AI chat context invalid messages type, reset: session={session_key[:80]}")
            return []

        out: List[Dict[str, str]] = []
        for item in raw_messages:
            normalized = self._normalize_chat_history_item(item)
            if normalized is None:
                self._chat_sessions.pop(session_key, None)
                self.log.warning(f"AI chat context invalid message item, reset: session={session_key[:80]}")
                return []
            out.append(normalized)

        checked = self._validate_and_trim_chat_history(out)
        if checked is None:
            self._chat_sessions.pop(session_key, None)
            self.log.warning(f"AI chat context invalid message structure, reset: session={session_key[:80]}")
            return []
        if len(checked) != len(out):
            self._chat_sessions[session_key] = {"messages": checked, "last_active_ts": last_active_ts}
        return checked

    def _remember_chat_message(
        self,
        session_key: str,
        role: str,
        content: str,
        *,
        msg_id: str = "",
        vision_slots: Optional[list] = None,
    ) -> None:
        key = str(session_key or "").strip()
        if not key:
            return
        raw: dict = {"role": role, "content": str(content or "")}
        if role == "user":
            mid = str(msg_id or "").strip()
            if mid:
                raw["_msg_id"] = mid
            slots = self._normalize_vision_slots(vision_slots)
            if slots:
                raw["_vision"] = slots
        msg = self._normalize_chat_history_item(raw)
        if msg is None:
            return
        now_ts = float(time.time())
        with self._chat_sessions_lock:
            history = self._load_active_chat_history_locked(key, now_ts)
            history.append(msg)
            checked = self._validate_and_trim_chat_history(history)
            if checked is None:
                checked = [msg]
                self.log.warning(f"AI chat context invalid after append, reset to current message: session={key[:80]}")
            self._chat_sessions[key] = {"messages": checked, "last_active_ts": now_ts}

    def _save_chat_turn(
        self,
        session_key: str,
        user_input: str,
        assistant_output: str,
        *,
        msg_id: str = "",
        vision_slots: Optional[list] = None,
    ) -> None:
        key = str(session_key or "").strip()
        if not key:
            return
        now_ts = float(time.time())
        user_raw: dict = {"role": "user", "content": str(user_input or "")}
        mid = str(msg_id or "").strip()
        if mid:
            user_raw["_msg_id"] = mid
        slots = self._normalize_vision_slots(vision_slots)
        if slots:
            user_raw["_vision"] = slots
        user_msg = self._normalize_chat_history_item(user_raw) or {"role": "user", "content": str(user_input or "")}
        assistant_msg = {"role": "assistant", "content": str(assistant_output or "")}
        with self._chat_sessions_lock:
            history = self._load_active_chat_history_locked(key, now_ts)
            history.append(user_msg)
            history.append(assistant_msg)
            checked = self._validate_and_trim_chat_history(history)
            if checked is None:
                checked = [user_msg, assistant_msg]
                self.log.warning(f"AI chat context invalid after append, reset to current turn: session={key[:80]}")
            self._chat_sessions[key] = {"messages": checked, "last_active_ts": now_ts}

    # ---------- 视觉 slot 接口 ----------

    @staticmethod
    def _render_chat_message_content(message: dict) -> str:
        """把内部 message（含 _vision slots）渲染成发送给模型的文本。"""
        text = str(message.get("content") or "").strip()
        slots = message.get("_vision")
        if not isinstance(slots, list):
            return text
        visual_lines: List[str] = []
        for slot in slots:
            if not isinstance(slot, dict):
                continue
            try:
                index = int(slot.get("index") or 1)
            except Exception:
                index = 1
            status = str(slot.get("status") or "")
            desc = str(slot.get("description") or "").strip()
            if status == "ready":
                if desc:
                    visual_lines.append(f"[视觉内容{index}] {desc}")
            elif status == "retryable_error":
                visual_lines.append(f"[视觉内容{index}] 图片暂时无法识别。")
            elif status == "permanent_error":
                visual_lines.append(f"[视觉内容{index}] 图片识别失败，无法确认具体内容。")
            else:
                visual_lines.append(f"[视觉内容{index}] 图片尚未完成识别。")
        if not visual_lines:
            return text
        parts = [text] if text else []
        parts.append("\n".join(visual_lines))
        return "\n\n".join(parts).strip()

    def _materialize_history_for_model(self, history: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """历史消息渲染为纯文本消息（去掉 _msg_id/_vision 等内部字段）。"""
        out: List[Dict[str, str]] = []
        for message in history:
            if not isinstance(message, dict):
                continue
            role = str(message.get("role") or "").strip()
            content = self._render_chat_message_content(message)
            out.append({"role": role, "content": content})
        return out

    def collect_unresolved_vision_slots(self, session_key: str, backend: str) -> List[dict]:
        """收集后端可见窗口内所有需要补解析的视觉 slot（深拷贝）。

        这是"本次有哪些历史图片需要补解析"的唯一来源。
        """
        try:
            history = self._load_active_chat_history(session_key)
            history = self._select_history_for_backend(history, backend)
        except Exception:
            return []
        now_ts = float(time.time())
        out: List[dict] = []
        for message in history:
            slots = message.get("_vision") if isinstance(message, dict) else None
            if not isinstance(slots, list):
                continue
            for slot in slots:
                if not isinstance(slot, dict):
                    continue
                status = str(slot.get("status") or "")
                if status == "ready":
                    continue
                if status == "permanent_error":
                    continue
                if status == "retryable_error":
                    try:
                        retry_after = float(slot.get("retry_after_ts") or 0.0)
                    except Exception:
                        retry_after = 0.0
                    if now_ts < retry_after:
                        continue
                import copy as _copy

                out.append(_copy.deepcopy(slot))
        return out

    def apply_vision_resolutions(self, session_key: str, resolutions: list) -> int:
        """按 slot_id 精确把解析结果写回历史消息中的 slot。返回更新的 slot 数。"""
        result_map = {str(r.slot_id): r for r in (resolutions or [])}
        if not result_map:
            return 0
        updated = 0
        with self._chat_sessions_lock:
            entry = self._chat_sessions.get(session_key)
            if not isinstance(entry, dict):
                return 0
            messages = entry.get("messages")
            if not isinstance(messages, list):
                return 0
            for message in messages:
                slots = message.get("_vision") if isinstance(message, dict) else None
                if not isinstance(slots, list):
                    continue
                changed = False
                for slot in slots:
                    if not isinstance(slot, dict):
                        continue
                    result = result_map.get(str(slot.get("slot_id") or ""))
                    if result is None:
                        continue
                    self._apply_resolution_to_slot_dict(slot, result)
                    updated += 1
                    changed = True
                if changed:
                    message["_vision"] = self._normalize_vision_slots(slots)
        return updated

    @staticmethod
    def _apply_resolution_to_slot_dict(slot: dict, resolution) -> None:
        """把解析结果应用到历史中的 slot dict；成功后清空临时图片 source。"""
        slot["status"] = str(getattr(resolution, "status", "") or "")
        if getattr(resolution, "description", ""):
            slot["description"] = str(resolution.description)
        if getattr(resolution, "source_key", ""):
            slot["source_key"] = str(resolution.source_key)
        if getattr(resolution, "content_hash", ""):
            slot["content_hash"] = str(resolution.content_hash)
        if getattr(resolution, "retry_after_ts", 0.0):
            slot["retry_after_ts"] = float(resolution.retry_after_ts)
        if slot["status"] == "ready":
            slot["url"] = ""
            slot["file"] = ""
            slot["file_id"] = ""
            slot["path"] = ""

    def find_chat_message_by_msg_id(self, session_key: str, msg_id: str) -> Optional[dict]:
        """在当前会话历史中按消息 id 查找 user message；找不到返回 None。"""
        key = str(session_key or "").strip()
        target = str(msg_id or "").strip()
        if not key or not target:
            return None
        try:
            history = self._load_active_chat_history(key)
        except Exception:
            return None
        for message in history:
            if not isinstance(message, dict):
                continue
            if str(message.get("_msg_id") or "").strip() == target:
                return message
        return None

    def _extract_notice_file_head_sync(self, path: Path, max_chars: int = 4000, max_pages: int = 6) -> str:
        p = Path(path)
        if (not p.exists()) or (not p.is_file()):
            return ""
        suffix = p.suffix.lower()
        if suffix == ".pdf":
            text = self._read_pdf_head_fitz(p, max_pages=max_pages, max_chars=max_chars)
            if self._has_enough_text(text):
                return text
            text2 = self._read_pdf_head(p, max_pages=max_pages, max_chars=max_chars)
            if self._has_enough_text(text2):
                return text2
            # 扫描件兜底：尝试 OCR
            text3 = self._read_pdf_head_ocr(p, max_pages=max_pages, max_chars=max_chars)
            if self._has_enough_text(text3):
                self.log.info(f"群通知解析：已使用 OCR 提取扫描 PDF 文本 {p.name}")
                return text3
            return text3
        if suffix == ".doc":
            return self._read_doc_head(p, max_chars=max_chars)
        if suffix == ".docx":
            return self._read_docx_head(p, max_chars=max_chars)
        if suffix in {".md", ".markdown"}:
            return self._read_md_head(p, max_chars=max_chars)
        return ""

    @staticmethod
    def _has_enough_text(text: str, min_chars: int = 20) -> bool:
        compact = re.sub(r"\s+", "", str(text or ""))
        return len(compact) >= int(min_chars)

    async def _extract_notice_url_head_async(self, url: str, max_chars: int = 4000) -> str:
        if (not url) or (not (url.startswith("http://") or url.startswith("https://"))):
            return ""
        if aiohttp is None or BeautifulSoup is None:
            self.log.warning("群通知解析：缺少 aiohttp 或 beautifulsoup4，回退到 urllib 简易解析")
            return await asyncio.to_thread(self._extract_notice_url_head_urllib_sync, url, int(max_chars))

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        timeout = aiohttp.ClientTimeout(total=25.0)

        html = ""
        content_type = ""
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(url, allow_redirects=True) as resp:
                    if int(resp.status) >= 400:
                        self.log.warning(f"群通知解析：链接抓取返回状态码={int(resp.status)} url={url[:160]}")
                        return ""
                    content_type = str(resp.headers.get("Content-Type") or "").lower()
                    html = await resp.text(errors="ignore")
        except Exception as e:
            self.log.warning(f"群通知解析：网页抓取失败 {url[:120]}: {e}")
            return await asyncio.to_thread(self._extract_notice_url_head_urllib_sync, url, int(max_chars))

        if not html:
            self.log.info(f"群通知解析：链接响应正文为空 url={url[:160]}")
            return ""

        if "text/plain" in content_type and "<html" not in html[:500].lower():
            plain = re.sub(r"\s+", " ", html).strip()
            return plain[:max_chars]

        try:
            soup = BeautifulSoup(html, "html.parser")
            for bad in soup(["script", "style", "noscript", "svg", "canvas", "iframe"]):
                bad.decompose()
            body = soup.body if soup.body is not None else soup
            text = body.get_text(separator="\n", strip=True)
            text = re.sub(r"[ \t]+", " ", text)
            text = re.sub(r"\n{2,}", "\n", text).strip()
            if not text:
                self.log.info(f"群通知解析：网页正文提取为空 url={url[:160]}")
            return text[:max_chars]
        except Exception as e:
            self.log.warning(f"群通知解析：网页正文提取失败 {url[:120]}: {e}")
            return self._extract_notice_url_head_urllib_sync(url, int(max_chars))

    def _extract_notice_url_head_urllib_sync(self, url: str, max_chars: int = 4000) -> str:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=25.0) as resp:
                content_type = str(resp.headers.get("Content-Type") or "").lower()
                raw = resp.read(1024 * 1024)  # 1MB upper bound
        except Exception as e:
            self.log.warning(f"群通知解析：urllib 抓取失败 {url[:120]}: {e}")
            return ""

        txt = raw.decode("utf-8", errors="ignore")
        if not txt:
            return ""

        if "text/plain" in content_type and "<html" not in txt[:500].lower():
            plain = re.sub(r"\s+", " ", txt).strip()
            return plain[:max_chars]

        # Simple HTML cleanup fallback when bs4/aiohttp are unavailable.
        cleaned = re.sub(r"(?is)<(script|style|noscript|svg|canvas|iframe)\b.*?>.*?</\1>", " ", txt)
        cleaned = re.sub(r"(?is)<br\s*/?>", "\n", cleaned)
        cleaned = re.sub(r"(?is)</(p|div|li|h[1-6]|tr|section|article)>", "\n", cleaned)
        cleaned = re.sub(r"(?is)<[^>]+>", " ", cleaned)
        cleaned = html.unescape(cleaned)
        cleaned = re.sub(r"[ \t]+", " ", cleaned)
        cleaned = re.sub(r"\n{2,}", "\n", cleaned).strip()
        return cleaned[:max_chars]

    def _classify_notice_sync(self, source: str, snippet: str) -> bool:
        if not self.deepseek_api_key:
            return False
        if OpenAI is None:
            return False

        content = str(snippet or "").strip()
        if not content:
            return False
        if len(content) > 6000:
            content = content[:6000]

        client = self._create_deepseek_client()
        prompt = (
            "你是 QQ 群消息过滤器。\n"
            "请判断下面内容是否属于“需要同学执行动作/流程/截止日期”的通知。\n"
            "如果是纯学习资料/课件/教材/日历/介绍，请只输出：[静默]\n"
            "如果是需要执行动作的通知，请只输出：[通知]\n"
            "禁止输出任何其他字符。\n\n"
            f"来源：{source or '未知来源'}\n\n"
            f"内容片段：\n{content}"
        )
        try:
            resp = self._create_reasoner_completion(client, [{"role": "user", "content": prompt}], 0.0)
            raw = self._extract_sdk_chat_text(resp)
            out = self.sanitize_reasoner_output(raw)
            if out == "[通知]":
                self.log.info(f"群通知解析：分类结果=通知 source={source[:120]}")
                return True
            if out == self._NOTICE_SILENT_TOKEN:
                self.log.info(f"群通知解析：分类结果=静默 source={source[:120]}")
            elif out:
                self.log.info(f"群通知解析：分类结果=非标准输出但放行 source={source[:120]} output={out[:80]}")
            else:
                self.log.info(f"群通知解析：分类结果=空输出 source={source[:120]}")
            return (out != self._NOTICE_SILENT_TOKEN) and bool(out)
        except Exception as e:
            self.log.warning(f"群通知解析：分类调用失败 source={source[:120]} err={e}")
            return False

    def _reason_notice_sync(self, source: str, snippet: str) -> str:
        if not self.deepseek_api_key:
            raise RuntimeError("deepseek api key not ready")
        if OpenAI is None:
            raise RuntimeError("openai sdk is not installed")

        content = str(snippet or "").strip()
        if not content:
            return self._NOTICE_SILENT_TOKEN
        # 通知类尽量使用完整文本；极端长文档才截断，避免超大请求。
        if len(content) > 120000:
            content = content[:120000]

        client = self._create_deepseek_client()

        prompt = (
            "【角色设定】\n"
            "你是 QQ 群里的 AI 助手 Cooper_bot。\n\n"
            "【任务】\n"
            "根据给定通知全文，生成一份简洁清晰的省流说明。\n"
            "只提取原文中明确出现的信息，不要补充、猜测或外推。\n"
            "报名方式、费用、地点等信息仅在原文明确出现时才写入；未出现就直接省略。\n\n"
            "【输出格式（纯文本，不要*）】\n"
            "📢 【省流通知】[核心标题]\n"
            "🎯 核心事由：[一句话概括]\n"
            "🙋‍♂️ 涉及人员：[全体同学/团员/班委/指定人群]\n"
            "✅ 需要做什么：\n"
            "1. ...\n"
            "2. ...\n"
            "3. ...\n"
            "⏰ 截止时间：[若无则写“无明确截止时间”]\n\n"
            "【风格要求】\n"
            "- 保持简洁，控制在 6~10 行。\n"
            "- 可用 emoji。\n"
            "- 不要 Markdown，不要星号(*)，不要代码块。\n"
            "- 禁止输出“原文未提及/未知/暂无/待通知”等占位语。\n"
            "- 除上述模板外，不要额外追加字段。\n\n"
            "【输入来源】\n"
            f"{source or '未知来源'}\n\n"
            "【通知全文/片段】\n"
            f"{content}"
        )

        resp = self._create_reasoner_completion(client, [{"role": "user", "content": prompt}], 0.2)
        raw = self._extract_sdk_chat_text(resp)
        out = self.sanitize_reasoner_output(raw)
        return out or self._NOTICE_SILENT_TOKEN

    def _classify_notice_sync_v2(
        self,
        source: str,
        snippet: str,
        group_id: Optional[int] = None,
        kind: str = "notice",
    ) -> bool:
        if not self.deepseek_api_key:
            return False
        if OpenAI is None:
            return False

        content = str(snippet or "").strip()
        if not content:
            return False
        if len(content) > 6000:
            content = content[:6000]

        lines = self._select_notice_prompt_lines(group_id, kind, "classify")
        prompt = self._render_notice_prompt(lines, source, content)

        client = self._create_deepseek_client()
        try:
            resp = self._create_reasoner_completion(client, [{"role": "user", "content": prompt}], 0.0)
            raw = self._extract_sdk_chat_text(resp)
            out = self.sanitize_reasoner_output(raw)
            if out == "[通知]":
                self.log.info(f"群通知解析：分类结果=通知 source={source[:120]}")
                return True
            if out == self._NOTICE_SILENT_TOKEN:
                self.log.info(f"群通知解析：分类结果=静默 source={source[:120]}")
            elif out:
                self.log.info(f"群通知解析：分类结果=非标准输出但放行 source={source[:120]} output={out[:80]}")
            else:
                self.log.info(f"群通知解析：分类结果=空输出 source={source[:120]}")
            return (out != self._NOTICE_SILENT_TOKEN) and bool(out)
        except Exception as e:
            self.log.warning(f"群通知解析：分类调用失败 source={source[:120]} err={e}")
            return False

    def _reason_notice_sync_v2(
        self,
        source: str,
        snippet: str,
        group_id: Optional[int] = None,
        kind: str = "notice",
    ) -> str:
        if not self.deepseek_api_key:
            raise RuntimeError("deepseek api key not ready")
        if OpenAI is None:
            raise RuntimeError("openai sdk is not installed")

        content = str(snippet or "").strip()
        if not content:
            return self._NOTICE_SILENT_TOKEN
        if len(content) > 120000:
            content = content[:120000]

        lines = self._select_notice_prompt_lines(group_id, kind, "reason")
        prompt = self._render_notice_prompt(lines, source, content)

        client = self._create_deepseek_client()
        resp = self._create_reasoner_completion(client, [{"role": "user", "content": prompt}], 0.2)
        raw = self._extract_sdk_chat_text(resp)
        out = self.sanitize_reasoner_output(raw)
        return out or self._NOTICE_SILENT_TOKEN

    def _run_new_file_pipeline(
        self,
        rel: str,
        hint: Optional[dict] = None,
        build_vector: bool = True,
    ) -> Dict[str, object]:
        rel_norm = self._normalize_rel(rel)
        if not rel_norm:
            raise ValueError("invalid file relative path")

        abs_path = self.material_dir / rel_norm
        if (not abs_path.exists()) or (not abs_path.is_file()):
            raise FileNotFoundError(f"file not found: {rel_norm}")

        subject = self._subject_from_rel(rel_norm)
        filename = abs_path.name
        ext = abs_path.suffix.lower().lstrip(".")
        if f".{ext}" not in self._ALLOWED_SUFFIXES:
            raise ValueError(f"unsupported file type: {abs_path.suffix}")

        hint_obj = hint if isinstance(hint, dict) else {}
        snippet = str(hint_obj.get("snippet") or "").strip()
        if len(snippet) > 2000:
            snippet = snippet[:2000]

        title_only = ext in ("ppt", "pptx")
        if (not title_only) and (not snippet):
            try:
                snippet = self._extract_material_snippet(abs_path, max_chars=2000)
            except Exception:
                snippet = ""
            if len(snippet) > 2000:
                snippet = snippet[:2000]

        summary_data = self._normalize_summary_data(hint_obj.get("summary_data"), subject, filename)
        if summary_data is None:
            generated = self._generate_summary(
                subject=subject,
                filename=filename,
                file_type=ext,
                text_content=snippet,
                title_only=title_only,
            )
            summary_data = self._normalize_summary_data(generated, subject, filename)
        if summary_data is None:
            summary_data = self._fallback_summary(subject, filename, ext)

        index_item = {
            "file_path": self._to_store_rel(rel_norm),
            "subject": subject,
            "filename": filename,
            "file_type": ext,
            "keywords": summary_data.get("keywords") or [subject],
            "summary": summary_data.get("summary") or f"{subject}资料：{filename}",
        }
        metadata_item = {
            "file_path": self._to_store_rel(rel_norm),
            "filename": filename,
            "subject": subject,
        }
        embedding_text = self._make_embedding_text(index_item)
        vector_arr: Optional[np.ndarray] = None
        if build_vector:
            vec = self._embed_text(embedding_text)
            if vec is not None:
                arr = np.asarray(vec, dtype=np.float64).reshape(-1)
                if arr.size > 0:
                    vector_arr = arr

        return {
            "rel": rel_norm,
            "filename": filename,
            "subject": subject,
            "file_type": ext,
            "snippet": snippet,
            "classification_target": str(hint_obj.get("classified_target") or "").strip(),
            "summary_data": summary_data,
            "index_item": index_item,
            "metadata_item": metadata_item,
            "embedding_text": embedding_text,
            "vector_attempted": bool(build_vector),
            "vector": vector_arr,
        }

    def _build_index_entry(self, rel: str) -> dict:
        ctx = self._run_new_file_pipeline(rel, hint=None, build_vector=False)
        item = ctx.get("index_item")
        if isinstance(item, dict):
            return item
        raise RuntimeError(f"failed to build index item: {rel}")

    def _build_vector_for_index_item(self, item: dict) -> Optional[np.ndarray]:
        combined_text = self._make_embedding_text(item)
        vec = self._embed_text(combined_text)
        if vec is None:
            return None
        arr = np.asarray(vec, dtype=np.float64).reshape(-1)
        return arr if arr.size > 0 else None

    def _build_vector_for_embedding_text(self, embedding_text: str) -> Optional[np.ndarray]:
        vec = self._embed_text(str(embedding_text or ""))
        if vec is None:
            return None
        arr = np.asarray(vec, dtype=np.float64).reshape(-1)
        return arr if arr.size > 0 else None

    def _generate_summary(
        self,
        subject: str,
        filename: str,
        file_type: str,
        text_content: str,
        title_only: bool = False,
    ) -> dict:
        if not self.deepseek_base_url or not self.deepseek_api_key:
            return self._fallback_summary(subject, filename, file_type)

        if title_only:
            text_content = ""

        snippet = (text_content or "").strip()
        if len(snippet) > 2000:
            snippet = snippet[:2000]

        if title_only:
            prompt = (
                "你是高校资料整理助手。请仅根据“学科目录名”和“文件名”生成标签与摘要。\n"
                "不要假设正文内容，不要编造具体知识点细节。\n"
                f"学科目录：{subject}\n"
                f"文件名：{filename}\n"
                f"文件类型：{file_type}\n\n"
                "输出严格 JSON：\n"
                '{"keywords":["词1","词2"],"summary":"一句精简说明"}'
            )
        else:
            if not snippet:
                snippet = "（正文不可提取，仅根据文件名和学科目录推断）"
            prompt = (
                "你是高校资料整理助手。请根据学科、文件名和文本片段生成标签与摘要。\n"
                "标签关注：课程名、知识点、资料类型、年份。\n"
                "摘要控制在 100~150 字，简洁、可检索。\n"
                f"学科目录：{subject}\n"
                f"文件名：{filename}\n"
                f"文件类型：{file_type}\n"
                f"正文片段：{snippet}\n\n"
                "输出严格 JSON：\n"
                '{"keywords":["词1","词2"],"summary":"一句精简说明"}'
            )

        payload = self._build_chat_payload(
            [{"role": "user", "content": prompt}],
            0.1,
            response_format={"type": "json_object"},
        )
        url = self._join_url(self.deepseek_base_url, "chat/completions")
        try:
            data = self._post_json(url, payload, self.deepseek_api_key, timeout=120.0)
            text = self._extract_chat_text(data)
            parsed = self._parse_summary_json(text)
            if parsed:
                return parsed
        except Exception as e:
            self.log.warning(f"AI 摘要：{filename} 生成失败: {e}")
        return self._fallback_summary(subject, filename, file_type)

    def _embed_text(self, text: str) -> Optional[List[float]]:
        if not self.embedding_base_url or not self.embedding_api_key:
            return None
        payload = {"model": self.embed_model, "input": str(text or "")}
        url = self._join_url(self.embedding_base_url, "embeddings")
        try:
            data = self._post_json(url, payload, self.embedding_api_key, timeout=90.0)
            arr = (((data or {}).get("data") or [{}])[0] or {}).get("embedding")
            if isinstance(arr, list) and arr:
                return [float(x) for x in arr]
        except Exception as e:
            self.log.warning(f"AI 向量：embedding 请求失败: {e}")
        return None

    def _make_embedding_text(self, item: dict) -> str:
        subject = str(item.get("subject") or "")
        filename = str(item.get("filename") or "")
        keywords = item.get("keywords") or []
        if not isinstance(keywords, list):
            keywords = [str(keywords)]
        kw_text = ", ".join(str(x).strip() for x in keywords if str(x).strip())
        summary = str(item.get("summary") or "")
        return f"学科：{subject}\n文件名：{filename}\n标签：{kw_text}\n核心内容：{summary}"

    def _scan_material_files(self) -> set[str]:
        rels: set[str] = set()
        if not self.material_dir.exists():
            return rels
        for p in self.material_dir.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() not in self._ALLOWED_SUFFIXES:
                continue
            if self._skip_file(p):
                continue
            try:
                rel = p.relative_to(self.material_dir).as_posix()
            except Exception:
                continue
            top = self._subject_from_rel(rel)
            if top.casefold() == self._AUTO_ORGANIZE_TBD_DIRNAME.casefold():
                continue
            rels.add(rel)
        return rels

    def _skip_file(self, p: Path) -> bool:
        if p.name in self._SKIP_FILENAMES:
            return True
        if p.name.startswith("~$"):
            return True
        try:
            rel = p.relative_to(self.material_dir)
            if any(part.startswith(".") for part in rel.parts):
                return True
        except Exception:
            return True
        return False

    def _metadata_vector_map(
        self,
        metadata_list: List[dict],
        vectors: np.ndarray,
        valid_rels: set[str],
    ) -> Dict[str, np.ndarray]:
        out: Dict[str, np.ndarray] = {}
        if vectors.ndim != 2:
            return out
        rows = int(vectors.shape[0])
        for i, item in enumerate(metadata_list[:rows]):
            rel = self._normalize_rel((item or {}).get("file_path"))
            if (not rel) or (rel not in valid_rels) or (rel in out):
                continue
            out[rel] = vectors[i]
        return out

    def _normalize_index_item(self, item: dict, rel: str, abs_path: Path) -> dict:
        rel = self._normalize_rel(rel)
        subject = str(item.get("subject") or self._subject_from_rel(rel))
        filename = str(item.get("filename") or abs_path.name)
        file_type = str(abs_path.suffix.lower().lstrip("."))
        keywords = item.get("keywords") or []
        if not isinstance(keywords, list):
            keywords = [str(keywords)]
        keywords = [str(x).strip() for x in keywords if str(x).strip()]
        summary = str(item.get("summary") or "").strip()
        return {
            "file_path": self._to_store_rel(rel),
            "subject": subject,
            "filename": filename,
            "file_type": file_type,
            "keywords": keywords or [subject],
            "summary": summary or f"{subject}资料：{filename}",
        }

    @staticmethod
    def _align_metadata_vectors(metadata: List[dict], vectors: np.ndarray) -> Tuple[List[dict], np.ndarray]:
        if vectors.ndim == 1 and vectors.size > 0:
            vectors = vectors.reshape(1, -1)
        if vectors.ndim != 2:
            vectors = np.empty((0, 0), dtype=np.float64)
        vectors = vectors.astype(np.float64, copy=False)
        n = min(len(metadata), int(vectors.shape[0]))
        if n <= 0:
            return [], np.empty((0, 0), dtype=np.float64)
        return metadata[:n], vectors[:n]

    def _open_incremental_store(self) -> sqlite3.Connection:
        self.incremental_store_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.incremental_store_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _ensure_incremental_store_ready(self) -> None:
        conn = self._open_incremental_store()
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS store_meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS index_items ("
                "rel TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_ts INTEGER NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS metadata_items ("
                "rel TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_ts INTEGER NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS vector_items ("
                "rel TEXT PRIMARY KEY, dim INTEGER NOT NULL, vec BLOB NOT NULL, updated_ts INTEGER NOT NULL)"
            )
            conn.execute(
                "INSERT INTO store_meta(k, v) VALUES('schema_version', ?) "
                "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (str(int(self._INCREMENTAL_STORE_SCHEMA_VERSION)),),
            )

            idx_cnt = int((conn.execute("SELECT COUNT(1) FROM index_items").fetchone() or [0])[0])
            meta_cnt = int((conn.execute("SELECT COUNT(1) FROM metadata_items").fetchone() or [0])[0])
            vec_cnt = int((conn.execute("SELECT COUNT(1) FROM vector_items").fetchone() or [0])[0])
            if (idx_cnt + meta_cnt + vec_cnt) > 0:
                conn.commit()
                return

            legacy_index = self._load_json_list(self.index_path)
            legacy_meta = self._load_json_list(self.metadata_path)
            legacy_vec = self._load_vectors(self.vectors_path)
            legacy_meta, legacy_vec = self._align_metadata_vectors(legacy_meta, legacy_vec)

            index_by_rel: Dict[str, dict] = {}
            for item in legacy_index:
                if not isinstance(item, dict):
                    continue
                rel = self._normalize_rel(item.get("file_path"))
                if not rel:
                    continue
                abs_path = self.material_dir / rel
                index_by_rel[rel] = self._normalize_index_item(item, rel, abs_path)

            metadata_by_rel: Dict[str, dict] = {}
            vector_by_rel: Dict[str, np.ndarray] = {}
            rows = int(legacy_vec.shape[0]) if isinstance(legacy_vec, np.ndarray) and legacy_vec.ndim == 2 else 0
            for i, item in enumerate(legacy_meta[:rows]):
                rel = self._normalize_rel((item or {}).get("file_path"))
                if not rel:
                    continue
                metadata_by_rel[rel] = dict(item)
                arr = np.asarray(legacy_vec[i], dtype=np.float64).reshape(-1)
                if arr.size <= 0:
                    continue
                vector_by_rel[rel] = arr

            now_ts = int(time.time())
            if index_by_rel:
                conn.executemany(
                    "INSERT OR REPLACE INTO index_items(rel, payload, updated_ts) VALUES(?, ?, ?)",
                    [
                        (rel, json.dumps(item, ensure_ascii=False, separators=(",", ":")), now_ts)
                        for rel, item in index_by_rel.items()
                    ],
                )
            if metadata_by_rel:
                conn.executemany(
                    "INSERT OR REPLACE INTO metadata_items(rel, payload, updated_ts) VALUES(?, ?, ?)",
                    [
                        (rel, json.dumps(item, ensure_ascii=False, separators=(",", ":")), now_ts)
                        for rel, item in metadata_by_rel.items()
                    ],
                )
            if vector_by_rel:
                conn.executemany(
                    "INSERT OR REPLACE INTO vector_items(rel, dim, vec, updated_ts) VALUES(?, ?, ?, ?)",
                    [
                        (
                            rel,
                            int(arr.size),
                            np.asarray(arr, dtype=np.float64).reshape(-1).tobytes(),
                            now_ts,
                        )
                        for rel, arr in vector_by_rel.items()
                    ],
                )
            conn.commit()
            self.log.info(
                f"AI 索引：已完成增量存储迁移 (索引={len(index_by_rel)}, 元数据={len(metadata_by_rel)}, 向量={len(vector_by_rel)})"
            )
        finally:
            conn.close()

    def _load_incremental_store_maps(self) -> Tuple[Dict[str, dict], Dict[str, dict], Dict[str, np.ndarray]]:
        self._ensure_incremental_store_ready()
        index_by_rel: Dict[str, dict] = {}
        metadata_by_rel: Dict[str, dict] = {}
        vector_by_rel: Dict[str, np.ndarray] = {}

        conn = self._open_incremental_store()
        try:
            for rel, payload in conn.execute("SELECT rel, payload FROM index_items"):
                rel_norm = self._normalize_rel(rel)
                if not rel_norm:
                    continue
                try:
                    obj = json.loads(str(payload or ""))
                except Exception:
                    continue
                if isinstance(obj, dict):
                    index_by_rel[rel_norm] = obj

            for rel, payload in conn.execute("SELECT rel, payload FROM metadata_items"):
                rel_norm = self._normalize_rel(rel)
                if not rel_norm:
                    continue
                try:
                    obj = json.loads(str(payload or ""))
                except Exception:
                    continue
                if isinstance(obj, dict):
                    metadata_by_rel[rel_norm] = obj

            for rel, dim, vec_blob in conn.execute("SELECT rel, dim, vec FROM vector_items"):
                rel_norm = self._normalize_rel(rel)
                if not rel_norm:
                    continue
                try:
                    dim_i = int(dim)
                except Exception:
                    continue
                if dim_i <= 0:
                    continue
                if not isinstance(vec_blob, (bytes, bytearray, memoryview)):
                    continue
                arr = np.frombuffer(bytes(vec_blob), dtype=np.float64)
                if arr.size != dim_i:
                    continue
                vector_by_rel[rel_norm] = arr.copy()
        finally:
            conn.close()
        return index_by_rel, metadata_by_rel, vector_by_rel

    def _persist_incremental_store_changes(
        self,
        *,
        index_upserts: Dict[str, dict],
        index_deletes: set[str],
        metadata_upserts: Dict[str, dict],
        metadata_deletes: set[str],
        vector_upserts: Dict[str, np.ndarray],
        vector_deletes: set[str],
    ) -> None:
        if (
            (not index_upserts)
            and (not index_deletes)
            and (not metadata_upserts)
            and (not metadata_deletes)
            and (not vector_upserts)
            and (not vector_deletes)
        ):
            return

        self._ensure_incremental_store_ready()
        now_ts = int(time.time())
        conn = self._open_incremental_store()
        try:
            with conn:
                if index_deletes:
                    conn.executemany(
                        "DELETE FROM index_items WHERE rel=?",
                        [(self._normalize_rel(rel),) for rel in sorted(index_deletes) if self._normalize_rel(rel)],
                    )
                if metadata_deletes:
                    conn.executemany(
                        "DELETE FROM metadata_items WHERE rel=?",
                        [(self._normalize_rel(rel),) for rel in sorted(metadata_deletes) if self._normalize_rel(rel)],
                    )
                if vector_deletes:
                    conn.executemany(
                        "DELETE FROM vector_items WHERE rel=?",
                        [(self._normalize_rel(rel),) for rel in sorted(vector_deletes) if self._normalize_rel(rel)],
                    )

                if index_upserts:
                    conn.executemany(
                        "INSERT OR REPLACE INTO index_items(rel, payload, updated_ts) VALUES(?, ?, ?)",
                        [
                            (
                                rel_norm,
                                json.dumps(item, ensure_ascii=False, separators=(",", ":")),
                                now_ts,
                            )
                            for rel_norm, item in (
                                (self._normalize_rel(rel), item) for rel, item in index_upserts.items()
                            )
                            if rel_norm and isinstance(item, dict)
                        ],
                    )
                if metadata_upserts:
                    conn.executemany(
                        "INSERT OR REPLACE INTO metadata_items(rel, payload, updated_ts) VALUES(?, ?, ?)",
                        [
                            (
                                rel_norm,
                                json.dumps(item, ensure_ascii=False, separators=(",", ":")),
                                now_ts,
                            )
                            for rel_norm, item in (
                                (self._normalize_rel(rel), item) for rel, item in metadata_upserts.items()
                            )
                            if rel_norm and isinstance(item, dict)
                        ],
                    )
                if vector_upserts:
                    conn.executemany(
                        "INSERT OR REPLACE INTO vector_items(rel, dim, vec, updated_ts) VALUES(?, ?, ?, ?)",
                        [
                            (
                                rel_norm,
                                int(arr.size),
                                arr.tobytes(),
                                now_ts,
                            )
                            for rel_norm, arr in (
                                (
                                    self._normalize_rel(rel),
                                    np.asarray(vec, dtype=np.float64).reshape(-1),
                                )
                                for rel, vec in vector_upserts.items()
                            )
                            if rel_norm and isinstance(arr, np.ndarray) and arr.size > 0
                        ],
                    )
        finally:
            conn.close()

    def _reset_semantic_cache_locked(self) -> None:
        self._semantic_entry_by_rel = {}
        self._semantic_row_by_rel = {}
        self._semantic_rel_by_row = []
        self._semantic_meta = []
        self._semantic_norm_vectors = np.empty((0, 0), dtype=np.float64)
        self._semantic_active_count = 0
        self._semantic_vector_dim = 0

    def _ensure_semantic_capacity_locked(self, required_count: int, vec_dim: int) -> None:
        required = max(0, int(required_count))
        dim = max(0, int(vec_dim))
        if required <= 0 or dim <= 0:
            return

        if self._semantic_vector_dim == 0:
            self._semantic_vector_dim = dim
        elif dim != self._semantic_vector_dim:
            return

        if self._semantic_norm_vectors.ndim != 2 or int(self._semantic_norm_vectors.shape[1]) != self._semantic_vector_dim:
            old_rows = 0
            if (
                self._semantic_norm_vectors.ndim == 2
                and int(self._semantic_norm_vectors.shape[1]) == self._semantic_vector_dim
            ):
                old_rows = min(int(self._semantic_active_count), int(self._semantic_norm_vectors.shape[0]))
            new_capacity = max(required, max(8, old_rows))
            new_mat = np.zeros((new_capacity, self._semantic_vector_dim), dtype=np.float64)
            if old_rows > 0:
                new_mat[:old_rows, :] = self._semantic_norm_vectors[:old_rows, :]
            self._semantic_norm_vectors = new_mat
            return

        old_capacity = int(self._semantic_norm_vectors.shape[0])
        if old_capacity >= required:
            return
        new_capacity = max(required, max(8, old_capacity * 2))
        new_mat = np.zeros((new_capacity, self._semantic_vector_dim), dtype=np.float64)
        active_rows = min(int(self._semantic_active_count), old_capacity)
        if active_rows > 0:
            new_mat[:active_rows, :] = self._semantic_norm_vectors[:active_rows, :]
        self._semantic_norm_vectors = new_mat

    @staticmethod
    def _normalize_semantic_vector(vec: object) -> Optional[np.ndarray]:
        try:
            arr = np.asarray(vec, dtype=np.float64).reshape(-1)
        except Exception:
            return None
        if arr.size <= 0:
            return None
        norm = float(np.linalg.norm(arr))
        if norm <= 0.0:
            return None
        return (arr / norm).astype(np.float64, copy=False)

    def _semantic_insert_or_replace_locked(self, rel: str, meta_item: dict, vec: np.ndarray) -> bool:
        rel_norm = self._normalize_rel(rel)
        if (not rel_norm) or (not isinstance(meta_item, dict)):
            return False

        norm_vec = self._normalize_semantic_vector(vec)
        if norm_vec is None:
            return False
        vec_dim = int(norm_vec.size)

        row = self._semantic_row_by_rel.get(rel_norm)
        if row is not None:
            row_idx = int(row)
            if (
                row_idx < 0
                or row_idx >= int(self._semantic_active_count)
                or row_idx >= len(self._semantic_meta)
                or row_idx >= len(self._semantic_rel_by_row)
            ):
                return False
            if int(self._semantic_vector_dim) <= 0 or vec_dim != int(self._semantic_vector_dim):
                return False
            if (
                self._semantic_norm_vectors.ndim != 2
                or int(self._semantic_norm_vectors.shape[1]) != int(self._semantic_vector_dim)
                or int(self._semantic_norm_vectors.shape[0]) <= row_idx
            ):
                return False
            self._semantic_meta[row_idx] = dict(meta_item)
            self._semantic_norm_vectors[row_idx, :] = norm_vec
            return True

        if int(self._semantic_vector_dim) == 0:
            self._semantic_vector_dim = vec_dim
        elif vec_dim != int(self._semantic_vector_dim):
            return False

        required = int(self._semantic_active_count) + 1
        self._ensure_semantic_capacity_locked(required, int(self._semantic_vector_dim))
        if (
            self._semantic_norm_vectors.ndim != 2
            or int(self._semantic_norm_vectors.shape[1]) != int(self._semantic_vector_dim)
            or int(self._semantic_norm_vectors.shape[0]) < required
        ):
            return False

        row_idx = int(self._semantic_active_count)
        self._semantic_row_by_rel[rel_norm] = row_idx
        self._semantic_rel_by_row.append(rel_norm)
        self._semantic_meta.append(dict(meta_item))
        self._semantic_norm_vectors[row_idx, :] = norm_vec
        self._semantic_active_count = row_idx + 1
        return True

    def _semantic_delete_locked(self, rel: str) -> None:
        rel_norm = self._normalize_rel(rel)
        if not rel_norm:
            return
        row = self._semantic_row_by_rel.pop(rel_norm, None)
        if row is None:
            return

        row_idx = int(row)
        active = int(self._semantic_active_count)
        if active <= 0:
            self._reset_semantic_cache_locked()
            return

        last_row = active - 1
        if (
            row_idx < 0
            or row_idx >= active
            or last_row >= len(self._semantic_rel_by_row)
            or last_row >= len(self._semantic_meta)
        ):
            self._reset_semantic_cache_locked()
            return

        if row_idx != last_row:
            last_rel = self._semantic_rel_by_row[last_row]
            if (
                self._semantic_norm_vectors.ndim != 2
                or int(self._semantic_norm_vectors.shape[0]) <= last_row
                or int(self._semantic_norm_vectors.shape[0]) <= row_idx
                or int(self._semantic_norm_vectors.shape[1]) != int(self._semantic_vector_dim)
            ):
                self._reset_semantic_cache_locked()
                return
            self._semantic_norm_vectors[row_idx, :] = self._semantic_norm_vectors[last_row, :]
            self._semantic_meta[row_idx] = self._semantic_meta[last_row]
            self._semantic_rel_by_row[row_idx] = last_rel
            self._semantic_row_by_rel[last_rel] = row_idx

        self._semantic_rel_by_row.pop()
        self._semantic_meta.pop()
        self._semantic_active_count = last_row
        if int(self._semantic_active_count) <= 0:
            self._reset_semantic_cache_locked()

    def _set_semantic_cache_from_maps(
        self,
        metadata_by_rel: Dict[str, dict],
        vector_by_rel: Dict[str, np.ndarray],
    ) -> None:
        with self._lock:
            self._reset_semantic_cache_locked()
            for rel in sorted((metadata_by_rel or {}).keys()):
                meta_item = (metadata_by_rel or {}).get(rel)
                rel_norm = self._normalize_rel(rel)
                if (not rel_norm) or (not isinstance(meta_item, dict)):
                    continue
                vec = (vector_by_rel or {}).get(rel_norm)
                if vec is None:
                    vec = (vector_by_rel or {}).get(rel)
                if vec is None:
                    continue
                try:
                    raw_arr = np.asarray(vec, dtype=np.float64).reshape(-1)
                except Exception:
                    continue
                if raw_arr.size <= 0:
                    continue
                if self._semantic_insert_or_replace_locked(rel_norm, meta_item, raw_arr):
                    self._semantic_entry_by_rel[rel_norm] = (dict(meta_item), raw_arr)

    def _apply_semantic_cache_changes(
        self,
        *,
        metadata_upserts: Dict[str, dict],
        vector_upserts: Dict[str, np.ndarray],
        delete_rels: set[str],
    ) -> None:
        with self._lock:
            for rel in delete_rels or set():
                rel_norm = self._normalize_rel(rel)
                if not rel_norm:
                    continue
                self._semantic_entry_by_rel.pop(rel_norm, None)
                self._semantic_delete_locked(rel_norm)

            normalized_metadata_upserts: Dict[str, dict] = {}
            for rel, meta_item in (metadata_upserts or {}).items():
                rel_norm = self._normalize_rel(rel)
                if (not rel_norm) or (not isinstance(meta_item, dict)):
                    continue
                normalized_metadata_upserts[rel_norm] = dict(meta_item)

            normalized_vector_upserts: Dict[str, Optional[np.ndarray]] = {}
            for rel, vec in (vector_upserts or {}).items():
                rel_norm = self._normalize_rel(rel)
                if not rel_norm:
                    continue
                try:
                    arr = np.asarray(vec, dtype=np.float64).reshape(-1)
                except Exception:
                    arr = np.empty((0,), dtype=np.float64)
                normalized_vector_upserts[rel_norm] = arr if arr.size > 0 else None

            affected_rels = set(normalized_metadata_upserts.keys()) | set(normalized_vector_upserts.keys())

            for rel_norm in sorted(affected_rels):
                old_entry = self._semantic_entry_by_rel.get(rel_norm)
                old_meta = old_entry[0] if (isinstance(old_entry, tuple) and len(old_entry) == 2) else None
                old_vec = old_entry[1] if (isinstance(old_entry, tuple) and len(old_entry) == 2) else None
                if not isinstance(old_meta, dict):
                    old_meta = None
                if not isinstance(old_vec, np.ndarray):
                    old_vec = None

                new_meta = normalized_metadata_upserts.get(rel_norm, old_meta)
                new_vec = normalized_vector_upserts.get(rel_norm, old_vec)

                if isinstance(new_meta, dict) and isinstance(new_vec, np.ndarray):
                    raw_arr = np.asarray(new_vec, dtype=np.float64).reshape(-1)
                    if raw_arr.size > 0 and self._semantic_insert_or_replace_locked(rel_norm, new_meta, raw_arr):
                        self._semantic_entry_by_rel[rel_norm] = (dict(new_meta), raw_arr)
                        continue

                self._semantic_entry_by_rel.pop(rel_norm, None)
                self._semantic_delete_locked(rel_norm)

    def _rebuild_semantic_cache_locked(self) -> None:
        source_map = dict(self._semantic_entry_by_rel)
        self._reset_semantic_cache_locked()
        for rel in sorted(source_map.keys()):
            rel_norm = self._normalize_rel(rel)
            if not rel_norm:
                continue
            entry = source_map.get(rel)
            if not isinstance(entry, tuple) or len(entry) != 2:
                continue
            meta_item, vec = entry
            if not isinstance(meta_item, dict):
                continue
            try:
                raw_arr = np.asarray(vec, dtype=np.float64).reshape(-1)
            except Exception:
                continue
            if raw_arr.size <= 0:
                continue
            if self._semantic_insert_or_replace_locked(rel_norm, meta_item, raw_arr):
                self._semantic_entry_by_rel[rel_norm] = (dict(meta_item), raw_arr)

    @staticmethod
    def _load_json_list(path: Path) -> List[dict]:
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return [x for x in data if isinstance(x, dict)]
        except Exception:
            pass
        return []

    @staticmethod
    def _save_json(path: Path, data: List[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=4), encoding="utf-8")

    @staticmethod
    def _load_vectors(path: Path) -> np.ndarray:
        if not path.exists():
            return np.empty((0, 0), dtype=np.float64)
        try:
            arr = np.load(path, allow_pickle=False)
            if isinstance(arr, np.ndarray):
                return arr
        except Exception:
            pass
        return np.empty((0, 0), dtype=np.float64)

    def _read_pdf_head(self, path: Path, max_pages: int = 5, max_chars: int = 2000) -> str:
        if PdfReader is None:
            return ""
        text_parts: List[str] = []
        try:
            with path.open("rb") as f:
                reader = PdfReader(f)
                pages = len(reader.pages) if int(max_pages) <= 0 else min(len(reader.pages), int(max_pages))
                for i in range(pages):
                    t = reader.pages[i].extract_text() or ""
                    if t:
                        text_parts.append(t)
        except Exception as e:
            self.log.warning(f"AI 索引：读取 PDF 失败 {path.name}: {e}")
            return ""
        return "\n".join(text_parts)[:max_chars]

    def _read_pdf_head_fitz(self, path: Path, max_pages: int = 6, max_chars: int = 4000) -> str:
        if fitz is None:
            return ""
        chunks: List[str] = []
        try:
            doc = fitz.open(str(path))
            pages = int(doc.page_count) if int(max_pages) <= 0 else min(int(doc.page_count), int(max_pages))
            for i in range(pages):
                t = doc.load_page(i).get_text("text") or ""
                if t:
                    chunks.append(t)
            doc.close()
        except Exception as e:
            self.log.warning(f"群通知解析：读取 PDF 失败 {path.name}: {e}")
            return ""
        return "\n".join(chunks)[:max_chars]

    def _get_rapid_ocr(self):
        if not bool(ENABLE_OCR):
            return None
        if RapidOCR is None:
            return None
        if self._rapid_ocr is False:
            return None
        if self._rapid_ocr is None:
            try:
                self._rapid_ocr = RapidOCR()
            except Exception as e:
                self.log.warning(f"群通知解析：OCR 引擎初始化失败（rapidocr_onnxruntime）: {e}")
                self._rapid_ocr = False
                return None
        return self._rapid_ocr

    def _read_pdf_head_ocr(self, path: Path, max_pages: int = 6, max_chars: int = 4000) -> str:
        if fitz is None:
            return ""
        ocr = self._get_rapid_ocr()
        if ocr is None:
            return ""

        chunks: List[str] = []
        try:
            doc = fitz.open(str(path))
            pages = int(doc.page_count) if int(max_pages) <= 0 else min(int(doc.page_count), int(max_pages))
            for i in range(pages):
                page = doc.load_page(i)
                # 2x 放大能明显提升扫描件 OCR 成功率
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
                arr = np.frombuffer(pix.samples, dtype=np.uint8)
                if arr.size <= 0:
                    continue
                arr = arr.reshape(pix.h, pix.w, pix.n)
                if pix.n >= 4:
                    arr = arr[:, :, :3]

                result, _elapse = ocr(arr)
                if not result:
                    continue
                for item in result:
                    if not isinstance(item, (list, tuple)) or len(item) < 2:
                        continue
                    txt = str(item[1] or "").strip()
                    if txt:
                        chunks.append(txt)
                if len("\n".join(chunks)) >= int(max_chars):
                    break
            doc.close()
        except Exception as e:
            self.log.warning(f"群通知解析：OCR 提取 PDF 失败 {path.name}: {e}")
            return ""
        return "\n".join(chunks)[:max_chars]

    @staticmethod
    def _cleanup_extracted_text(text: str) -> str:
        s = str(text or "")
        s = s.replace("\x00", " ")
        s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _read_md_head(self, path: Path, max_chars: int = 2000) -> str:
        try:
            raw = Path(path).read_bytes()
        except Exception as e:
            self.log.warning(f"AI 索引：读取 MD 失败 {path.name}: {e}")
            return ""
        if not raw:
            return ""
        raw = raw[: 4 * 1024 * 1024]
        txt = self._cleanup_extracted_text(raw.decode("utf-8", errors="replace"))
        return txt[:max_chars]

    def _read_doc_head(self, path: Path, max_chars: int = 2000) -> str:
        antiword_path = shutil.which("antiword")
        if antiword_path:
            try:
                proc = subprocess.run(
                    [antiword_path, str(path)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                    timeout=25,
                )
                raw = bytes(proc.stdout or b"")
                if raw:
                    for enc in ("utf-8", "gb18030", "utf-16-le", "latin1"):
                        txt = self._cleanup_extracted_text(raw.decode(enc, errors="ignore"))
                        if self._has_enough_text(txt):
                            return txt[:max_chars]
            except Exception as e:
                self.log.warning(f"AI 索引：读取 DOC 失败 {path.name}: {e}")

        try:
            raw = path.read_bytes()
        except Exception as e:
            self.log.warning(f"AI 索引：读取 DOC 失败 {path.name}: {e}")
            return ""
        if not raw:
            return ""
        raw = raw[: 4 * 1024 * 1024]

        best = ""
        best_score = -1
        for enc in ("utf-16-le", "utf-8", "gb18030", "latin1"):
            txt = self._cleanup_extracted_text(raw.decode(enc, errors="ignore"))
            if not txt:
                continue
            probe = txt[: max_chars * 2]
            score = len(re.findall(r"[0-9A-Za-z\u4e00-\u9fff]", probe))
            if score > best_score:
                best = txt
                best_score = score
        return best[:max_chars]

    def _read_docx_head(self, path: Path, max_chars: int = 2000) -> str:
        if Document is None:
            return ""
        chunks: List[str] = []
        cur = 0
        try:
            doc = Document(str(path))
            for para in doc.paragraphs:
                t = (para.text or "").strip()
                if not t:
                    continue
                chunks.append(t)
                cur += len(t)
                if cur >= max_chars:
                    break
        except Exception as e:
            self.log.warning(f"AI 索引：读取 DOCX 失败 {path.name}: {e}")
            return ""
        return "\n".join(chunks)[:max_chars]

    @staticmethod
    def _normalize_rel(raw: object) -> str:
        s = str(raw or "").strip().replace("\\", "/")
        if s.startswith("./"):
            s = s[2:]
        if s.startswith(".\\"):
            s = s[2:]
        return s.lstrip("/")

    @staticmethod
    def _to_store_rel(rel: str) -> str:
        rel = rel.replace("/", "\\").lstrip("\\")
        return f".\\{rel}"

    @staticmethod
    def _subject_from_rel(rel: str) -> str:
        parts = [x for x in rel.split("/") if x]
        return parts[0] if parts else "unknown"

    @staticmethod
    def _join_url(base: str, endpoint: str) -> str:
        b = str(base or "").rstrip("/")
        e = str(endpoint or "").lstrip("/")
        return f"{b}/{e}"

    @staticmethod
    def _extract_chat_text(resp: dict) -> str:
        try:
            return str((((resp or {}).get("choices") or [{}])[0] or {}).get("message", {}).get("content") or "").strip()
        except Exception:
            return ""

    @staticmethod
    def _parse_summary_json(text: str) -> Optional[dict]:
        raw = str(text or "").strip()
        if not raw:
            return None
        obj = None
        try:
            obj = json.loads(raw)
        except Exception:
            m = re.search(r"\{[\s\S]*\}", raw)
            if not m:
                return None
            try:
                obj = json.loads(m.group(0))
            except Exception:
                return None
        if not isinstance(obj, dict):
            return None
        keywords = obj.get("keywords") or []
        if not isinstance(keywords, list):
            keywords = [str(keywords)]
        keywords = [str(x).strip() for x in keywords if str(x).strip()]
        summary = str(obj.get("summary") or "").strip()
        if not summary:
            return None
        return {"keywords": keywords[:12], "summary": summary}

    @staticmethod
    def _normalize_summary_data(value: object, subject: str, filename: str) -> Optional[dict]:
        if not isinstance(value, dict):
            return None
        keywords = value.get("keywords") or []
        if not isinstance(keywords, list):
            keywords = [str(keywords)]
        norm_keywords = [str(x).strip() for x in keywords if str(x).strip()]
        summary = str(value.get("summary") or "").strip()
        if not summary:
            return None
        if not norm_keywords:
            norm_keywords = [str(subject or "").strip(), str(Path(filename).stem or "").strip()]
        out_keywords = []
        seen = set()
        for x in norm_keywords:
            k = str(x).strip()
            if (not k) or (k in seen):
                continue
            seen.add(k)
            out_keywords.append(k)
        return {"keywords": out_keywords[:12], "summary": summary}

    @staticmethod
    def _fallback_summary(subject: str, filename: str, file_type: str) -> dict:
        stem = Path(filename).stem
        kws = [subject]
        if stem:
            kws.append(stem)
        if file_type:
            kws.append(file_type.lower())
        # 去重保序
        out = []
        seen = set()
        for x in kws:
            k = str(x).strip()
            if (not k) or (k in seen):
                continue
            seen.add(k)
            out.append(k)
        return {"keywords": out[:10], "summary": f"{subject}资料：{filename}"}

    @staticmethod
    def _post_json(url: str, payload: dict, api_key: str, timeout: float = 60.0) -> dict:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url=url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        if api_key:
            req.add_header("Authorization", f"Bearer {api_key}")

        try:
            with urllib.request.urlopen(req, timeout=float(timeout)) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8", errors="replace")
            except Exception:
                detail = str(e)
            raise RuntimeError(f"http {e.code}: {detail[:300]}")
        except Exception as e:
            raise RuntimeError(str(e))

        txt = raw.decode("utf-8", errors="replace").strip()
        if not txt:
            raise RuntimeError("empty response")
        try:
            obj = json.loads(txt)
        except Exception as e:
            raise RuntimeError(f"json decode failed: {e}")
        if not isinstance(obj, dict):
            raise RuntimeError("invalid response type")
        return obj