
# commands.py
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, TYPE_CHECKING
import asyncio
import html
import re
import time
import shutil
import uuid
import unicodedata
from filesvc import FileService
from logsvc import LogService
from handinsvc import (
    HANDIN_ALLOWED_REQUIRED_SUFFIXES,
    HandinService,
    extract_name_from_filename,
    extract_student_id,
    file_matches_required_suffix,
    normalize_required_suffix,
    parse_mmdd_hhmm,
    pretty_ts,
    required_suffix_display,
)
from command_services import get_handin_task_summary, list_handin_tasks_for_group, run_find_query, run_list_dir_query
from router import get_files
from ziputil import open_fast_zip, write_path as zip_write_path
from daily_calendar import parse_calendar_date
from vision_skill import VisionSlot
from config import (
    ADMIN_USERS,
    DATA_DIR,
    UPLOAD_GROUP_HOST_DIR,
    UPLOAD_PRIVATE_HOST_DIR,
    UPLOAD_GROUP_CONTAINER_DIR,
    UPLOAD_PRIVATE_CONTAINER_DIR,
    SEND_FILENAME_ASCII_SAFE,
    SEND_RETRY_DELAYS,
    AUTO_ZIP_FALLBACK,
    LARGE_FILE_WARN_MB,
    GET_ZIP_THRESHOLD,
    LS_LIMIT,
    FIND_DIR_LIMIT,
    FIND_FILE_LIMIT,
    AI_BOT_NICK,
)
if TYPE_CHECKING:
    from aisvc import AIService
LARGE_FILE_WARN_BYTES = int(LARGE_FILE_WARN_MB) * 1024 * 1024
ANSWER_FILE_PATH = Path(__file__).resolve().parent / "answer.txt"
_ANSWER_CACHE_MTIME: Optional[float] = None
_ANSWER_CACHE: Dict[str, List[str]] = {}
KEYWORD_ANSWER_FILE_PATH = Path(__file__).resolve().parent / "keyword_answer.txt"
_KEYWORD_ANSWER_CACHE_MTIME: Optional[float] = None
_KEYWORD_ANSWER_CACHE: Dict[str, List[str]] = {}
_GROUP_NOTICE_FILE_SUFFIXES = {".pdf", ".doc", ".docx", ".md", ".markdown"}
_URL_RE = re.compile(r"(https?://[^\s<>\"]+)", flags=re.IGNORECASE)
_GROUP_NOTICE_MAX_CANDIDATES = 3
_GROUP_NOTICE_DEDUP_SECONDS = 60.0
_RECENT_REPLY_DEDUP_SECONDS = 2.0
_RECENT_REPLY_KEYS: Dict[str, float] = {}
_AI_REPEAT_GUARD_SECONDS = 5.0 * 60.0
_AI_REPEAT_GUARD: Dict[str, dict] = {}
_STATE_SWEEP_MIN_INTERVAL_SECONDS = 30.0
_STATE_TTL_LAST_FIND_SECONDS = 30.0 * 60.0
_STATE_TTL_PENDING_HANDIN_SECONDS = 6.0 * 60.0 * 60.0
_STATE_TTL_PENDING_COUNT_SECONDS = 6.0 * 60.0 * 60.0
_STATE_TTL_GROUP_NOTICE_SECONDS = 5.0 * 60.0
_STATE_TTL_SIGNIN_SECONDS = 30.0 * 60.0 * 60.0
_HANDIN_SUBMIT_REMINDER_SECONDS = 10.0 * 60.0
_HANDIN_SUBMIT_REMINDER_TEXT = "发完文件需要选择提交任务哇，文件还没提交上去呐（哭唧唧）"
_SIGNIN_VISUAL_TIMESTAMP_TOLERANCE_SECONDS = 3.0 * 60.0
_SIGNIN_DEADLINE_TOLERANCE_SECONDS = 30.0 * 60.0
_SIGNIN_FINALIZE_GRACE_SECONDS = 5.0
_TEXT_COMPANION_EMOJI_SEG_TYPES = {"face", "mface", "market_face"}
_MEDIA_OR_EMOJI_SEG_TYPES = {"image"} | _TEXT_COMPANION_EMOJI_SEG_TYPES
_KEYWORD_ALLOWED_NON_TEXT_SEG_TYPES = {"at", "reply"} | _TEXT_COMPANION_EMOJI_SEG_TYPES
_MEDIA_OR_EMOJI_PLACEHOLDER_RE = re.compile(
    r"^\[\s*(?:\u56fe\u7247|\u8868\u60c5|\u52a8\u753b\u8868\u60c5)\s*\]$",
    flags=re.IGNORECASE,
)
_CQ_SEG_RE = re.compile(r"\[CQ:([a-zA-Z0-9_]+)(?:,[^\]]*)?\]")
_CQ_AT_RE = re.compile(r"\[CQ:at,([^\]]*)\]", flags=re.IGNORECASE)
_CQ_IMAGE_RE = re.compile(r"\[CQ:image,([^\]]+)\]", flags=re.IGNORECASE)
_SIGNIN_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}
_COUNT_NAME_SPLIT_RE = re.compile(r"[\s,，、;；/|]+")
_COUNT_NAME_PREFIX_RE = re.compile(r"^[\d\.\)\(、\-_:：]+")
_COUNT_END_RE = re.compile(r"^/?end[\s。.!！?？]*$", flags=re.IGNORECASE)
_COUNT_END_CN_RE = re.compile(r"^结束[\s。.!！?？]*$")
_FIND_GENERIC_TERMS = {"课本", "教材", "资料", "题库", "试卷"}
_FIND_SUBJECT_SHORT_TERMS = {"数电", "模电", "高数", "大物", "数理方程"}
_EXPLICIT_COMMAND_NAMES = {
    "autoat",
    "chandin",
    "calendartest",
    "count",
    "countlist",
    "countremove",
    "find",
    "get",
    "h",
    "handin",
    "handincheck",
    "handinget",
    "handinstat",
    "help",
    "level",
    "ls",
    "ping",
    "signin",
    "whoami",
}


def _strip_text_companion_cq_segments(text: str) -> str:
    def _replace(match: re.Match) -> str:
        tp = str(match.group(1) or "").lower()
        if tp in _TEXT_COMPANION_EMOJI_SEG_TYPES:
            return ""
        return match.group(0)

    return _CQ_SEG_RE.sub(_replace, str(text or "")).strip()

def _claim_recent_reply(key: str, ttl_seconds: float = _RECENT_REPLY_DEDUP_SECONDS) -> bool:
    now = time.time()
    stale_before = now - max(float(ttl_seconds) * 4.0, 30.0)
    for k, ts in list(_RECENT_REPLY_KEYS.items()):
        if float(ts) < stale_before:
            _RECENT_REPLY_KEYS.pop(k, None)

    prev = _RECENT_REPLY_KEYS.get(key)
    if prev is not None and (now - float(prev) < float(ttl_seconds)):
        return False
    _RECENT_REPLY_KEYS[key] = now
    return True


def _normalize_ai_guard_text(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip())


def _is_likely_ai_stuck_repeat(session_key: Optional[str], user_input: str, assistant_output: str) -> bool:
    key = str(session_key or "").strip() or "__stateless__"
    now = time.time()
    stale_before = now - float(_AI_REPEAT_GUARD_SECONDS)
    for k, item in list(_AI_REPEAT_GUARD.items()):
        if not isinstance(item, dict):
            _AI_REPEAT_GUARD.pop(k, None)
            continue
        try:
            ts = float(item.get("ts") or 0.0)
        except Exception:
            ts = 0.0
        if ts < stale_before:
            _AI_REPEAT_GUARD.pop(k, None)

    user_norm = _normalize_ai_guard_text(user_input)
    out_norm = _normalize_ai_guard_text(assistant_output)
    repeated = False
    prev = _AI_REPEAT_GUARD.get(key)
    if isinstance(prev, dict):
        prev_out = _normalize_ai_guard_text(str(prev.get("out") or ""))
        if prev_out and out_norm and (prev_out == out_norm):
            repeated = True

    _AI_REPEAT_GUARD[key] = {"user": user_norm, "out": out_norm, "ts": now}
    return repeated
def _normalize_answer_q(s: str) -> str:
    # 触发词匹配：忽略首尾空白、大小写，内部连续空白视为一个空格
    return re.sub(r"\s+", " ", (s or "").strip()).casefold()
def _finalize_answer_block(questions: List[str], replies: List[str], table: Dict[str, List[str]]) -> None:
    if not questions or not replies:
        return
    rs = [x for x in replies if (x or "").strip()]
    if not rs:
        return
    for q in questions:
        k = _normalize_answer_q(q)
        if k:
            table[k] = list(rs)
def _parse_answer_txt(content: str) -> Dict[str, List[str]]:
    """解析 answer.txt：
    - q: 触发词（可写多条，作为同义词）
    - a: 单行回复（可写多条，逐条发送）
    - a:| 多行回复（后续缩进行）
    """
    lines = content.splitlines()
    table: Dict[str, List[str]] = {}
    questions: List[str] = []
    replies: List[str] = []
    i = 0
    while i < len(lines):
        raw = lines[i]
        stripped = raw.strip()
        # 空行：结束当前 block
        if not stripped:
            _finalize_answer_block(questions, replies, table)
            questions, replies = [], []
            i += 1
            continue
        # 注释行
        if stripped.startswith("#"):
            i += 1
            continue
        low = stripped.lower()
        if low.startswith("q:"):
            # 若当前 block 已有回复，则新 q 代表新 block
            if questions and replies:
                _finalize_answer_block(questions, replies, table)
                questions, replies = [], []
            q = stripped[2:].strip()
            if q:
                questions.append(q)
            i += 1
            continue
        if low.startswith("a:"):
            body = stripped[2:].lstrip()
            # 多行回复：a:| + 后续缩进行
            if body == "|":
                i += 1
                block_lines: List[str] = []
                while i < len(lines):
                    ln = lines[i]
                    if ln.startswith("  "):
                        block_lines.append(ln[2:])
                        i += 1
                        continue
                    if ln.startswith("\t"):
                        block_lines.append(ln[1:])
                        i += 1
                        continue
                    break
                replies.append("\n".join(block_lines).rstrip("\n"))
                continue
            # 单行回复支持 \n 转义
            replies.append(body.replace("\\n", "\n"))
            i += 1
            continue
        # 兼容：若写成了缩进行，接到上一条回复后面
        if replies and (raw.startswith("  ") or raw.startswith("\t")):
            add = raw[2:] if raw.startswith("  ") else raw[1:]
            replies[-1] = replies[-1] + "\n" + add
            i += 1
            continue
        i += 1
    _finalize_answer_block(questions, replies, table)
    return table
def _reload_answer_cache_if_needed() -> None:
    global _ANSWER_CACHE_MTIME, _ANSWER_CACHE
    try:
        mtime = float(ANSWER_FILE_PATH.stat().st_mtime)
    except Exception:
        _ANSWER_CACHE = {}
        _ANSWER_CACHE_MTIME = None
        return
    if _ANSWER_CACHE_MTIME is not None and abs(_ANSWER_CACHE_MTIME - mtime) < 1e-6:
        return
    try:
        txt = ANSWER_FILE_PATH.read_text(encoding="utf-8")
        _ANSWER_CACHE = _parse_answer_txt(txt)
    except Exception:
        _ANSWER_CACHE = {}
    _ANSWER_CACHE_MTIME = mtime


def _reload_keyword_answer_cache_if_needed() -> None:
    global _KEYWORD_ANSWER_CACHE_MTIME, _KEYWORD_ANSWER_CACHE
    try:
        mtime = float(KEYWORD_ANSWER_FILE_PATH.stat().st_mtime)
    except Exception:
        _KEYWORD_ANSWER_CACHE = {}
        _KEYWORD_ANSWER_CACHE_MTIME = None
        return
    if _KEYWORD_ANSWER_CACHE_MTIME is not None and abs(_KEYWORD_ANSWER_CACHE_MTIME - mtime) < 1e-6:
        return
    try:
        txt = KEYWORD_ANSWER_FILE_PATH.read_text(encoding="utf-8")
        _KEYWORD_ANSWER_CACHE = _parse_answer_txt(txt)
    except Exception:
        _KEYWORD_ANSWER_CACHE = {}
    _KEYWORD_ANSWER_CACHE_MTIME = mtime


def _lookup_fixed_answers(text: str) -> List[str]:
    _reload_answer_cache_if_needed()
    return list(_ANSWER_CACHE.get(_normalize_answer_q(text), []))


def _lookup_keyword_answers(text: str) -> List[str]:
    _reload_keyword_answer_cache_if_needed()
    normalized = _normalize_answer_q(text)
    if not normalized:
        return []

    # 关键词触发：选最长关键词，长度相同按 keyword_answer.txt 中出现顺序。
    best_key = ""
    best_replies: Optional[List[str]] = None
    for key, replies in _KEYWORD_ANSWER_CACHE.items():
        if not key or key not in normalized:
            continue
        if (best_replies is None) or (len(key) > len(best_key)):
            best_key = key
            best_replies = replies
    return list(best_replies or [])


def _is_media_or_emoji_only_message(evt: dict, text: str) -> bool:
    msg = evt.get("message")
    if isinstance(msg, list):
        has_text = False
        has_media_or_emoji = False
        for seg in msg:
            if not isinstance(seg, dict):
                continue
            tp = str(seg.get("type") or "").strip().lower()
            if tp == "text":
                data = seg.get("data") or {}
                t = str(data.get("text") or "").strip()
                if t:
                    has_text = True
            elif tp in _MEDIA_OR_EMOJI_SEG_TYPES:
                has_media_or_emoji = True
        if has_media_or_emoji and (not has_text):
            return True

    s = str(text or "").strip()
    if not s:
        return False
    if _MEDIA_OR_EMOJI_PLACEHOLDER_RE.fullmatch(s):
        return True
    if "[CQ:" in s:
        types = [x.lower() for x in _CQ_SEG_RE.findall(s)]
        if types:
            tail = _CQ_SEG_RE.sub("", s).strip()
            if (not tail) and all(tp in _MEDIA_OR_EMOJI_SEG_TYPES for tp in types):
                return True
    return False


def _is_keyword_text_message(evt: dict, text: str) -> bool:
    def _is_text_with_allowed_cq(raw: str) -> bool:
        s = str(raw or "").strip()
        if not s:
            return False
        if "[CQ:" not in s:
            return True
        types = [x.lower() for x in _CQ_SEG_RE.findall(s)]
        if not types:
            return True
        tail = _CQ_SEG_RE.sub("", s).strip()
        if not tail:
            return False
        return all(tp in _KEYWORD_ALLOWED_NON_TEXT_SEG_TYPES for tp in types)

    msg = evt.get("message")
    if isinstance(msg, list):
        has_text = False
        for seg in msg:
            if not isinstance(seg, dict):
                continue
            tp = str(seg.get("type") or "").strip().lower()
            if tp == "text":
                data = seg.get("data") or {}
                if str(data.get("text") or "").strip():
                    has_text = True
                continue
            if tp in _KEYWORD_ALLOWED_NON_TEXT_SEG_TYPES:
                continue
            return False
        return has_text

    if isinstance(msg, str) and _is_text_with_allowed_cq(msg):
        return True
    return _is_text_with_allowed_cq(text)


def _is_ai_chat_text_message(evt: dict, text: str) -> bool:
    return _is_keyword_text_message(evt, text)


def _fmt_mb(n_bytes: int) -> str:
    try:
        return f"{(float(n_bytes) / (1024 * 1024)):.2f}MB"
    except Exception:
        return ""
def _is_large(n_bytes: Optional[int]) -> bool:
    try:
        return n_bytes is not None and int(n_bytes) >= LARGE_FILE_WARN_BYTES
    except Exception:
        return False
async def _warn_large_if_needed(api, ctx, logsvc: LogService, filename: str, n_bytes: Optional[int], mode: str):
    """大文件提示：mode in {'send','recv','zip'}"""
    if not _is_large(n_bytes):
        return
    size_txt = _fmt_mb(int(n_bytes or 0))
    if mode == "recv":
        await reply(api, ctx, f"📎 收到文件「{filename}」约 {size_txt}，文件较大请耐心等待…", logsvc)
    elif mode == "zip":
        await reply(api, ctx, f"📦 将发送压缩包「{filename}」约 {size_txt}，文件较大请耐心等待…", logsvc)
    else:
        await reply(api, ctx, f"📤 即将发送文件「{filename}」约 {size_txt}，文件较大请耐心等待…", logsvc)
@dataclass
class BotState:
    last_find: Dict[str, List[Path]] = field(default_factory=dict)  # conv_key -> paths (for /get)
    last_find_label: Dict[str, str] = field(default_factory=dict)   # conv_key -> keyword/task-name (for zip naming)
    last_find_ts: Dict[str, float] = field(default_factory=dict)    # conv_key -> last update ts
    # Handin: user_id -> queue of inbox files
    pending_handin_files: Dict[int, List[dict]] = field(default_factory=dict)
    # Handin: user_id -> {"ts": float}（检测到多文件后，等待用户回复 done 再打包）
    pending_handin_wait_done: Dict[int, dict] = field(default_factory=dict)
    # Handin: user_id -> {"ts": float}（已 done，等待用户回复 zip 名称）
    pending_handin_zip_name: Dict[int, dict] = field(default_factory=dict)
    # Handin: user_id -> {"ts": float}（单文件未识别姓名时，等待用户补充姓名或回复 0 跳过）
    pending_handin_name_input: Dict[int, dict] = field(default_factory=dict)
    # Handin: user_id -> {"mode": "submit"|"status"|"cancel", "task_ids":[...], "ts": float, "group_id": Optional[int]}
    pending_handin_choose: Dict[int, dict] = field(default_factory=dict)
    # Handin: user_id -> {"task_id": str, "path": str, "name": str, "ts": float}
    pending_handin_overwrite: Dict[int, dict] = field(default_factory=dict)
    # Handin submit reminder: user_id -> {"task": asyncio.Task, "ts": float}
    pending_handin_submit_reminders: Dict[int, dict] = field(default_factory=dict)
    # Count: conv_key -> {"names": [str, ...], "ts": float}
    pending_count_session: Dict[str, dict] = field(default_factory=dict)
    # Signin: group_id -> in-memory task state.
    signin_tasks: Dict[int, dict] = field(default_factory=dict)
    # Signin: user_id -> {"group_id": int, "task_id": str, ...} after private image passes time check.
    pending_signin_name_input: Dict[int, dict] = field(default_factory=dict)
    # Group notice digest dedup cache: notice_key -> ts
    recent_group_notice_keys: Dict[str, float] = field(default_factory=dict)
    # Opportunistic cleanup guard.
    last_state_sweep_ts: float = 0.0


def _mark_last_find_cache(state: BotState, key: str, hits: List[Path], label: str) -> None:
    state.last_find[key] = hits
    state.last_find_label[key] = label
    state.last_find_ts[key] = time.time()


def _entry_ts(item: object) -> float:
    if not isinstance(item, dict):
        return 0.0
    try:
        return float(item.get("ts") or 0.0)
    except Exception:
        return 0.0


def _files_entry_latest_ts(items: object) -> float:
    if not isinstance(items, list):
        return 0.0
    latest = 0.0
    for it in items:
        ts = _entry_ts(it)
        if ts > latest:
            latest = ts
    return latest


def _cancel_pending_handin_submit_reminder(state: BotState, user_id: int) -> None:
    item = state.pending_handin_submit_reminders.pop(int(user_id), None)
    task = item.get("task") if isinstance(item, dict) else None
    if task is not None and hasattr(task, "done") and hasattr(task, "cancel"):
        try:
            if not task.done():
                task.cancel()
        except Exception:
            pass


def _schedule_pending_handin_submit_reminder(api, ctx, logsvc: LogService, state: BotState, choose_ts: float) -> None:
    try:
        uid = int(ctx.user_id)
    except Exception:
        return

    _cancel_pending_handin_submit_reminder(state, uid)
    if (
        state.pending_handin_wait_done.get(uid)
        or state.pending_handin_zip_name.get(uid)
        or state.pending_handin_name_input.get(uid)
        or state.pending_handin_overwrite.get(uid)
    ):
        return

    try:
        reminder_ts = float(choose_ts)
    except Exception:
        reminder_ts = 0.0
    if reminder_ts <= 0.0:
        return

    slot: dict = {"ts": reminder_ts}

    async def _runner() -> None:
        try:
            await asyncio.sleep(float(_HANDIN_SUBMIT_REMINDER_SECONDS))
            pend = state.pending_handin_choose.get(uid)
            if not isinstance(pend, dict) or pend.get("mode") != "submit":
                return
            try:
                current_ts = float(pend.get("ts") or 0.0)
            except Exception:
                current_ts = 0.0
            if abs(current_ts - reminder_ts) > 0.001:
                return
            if (
                state.pending_handin_wait_done.get(uid)
                or state.pending_handin_zip_name.get(uid)
                or state.pending_handin_name_input.get(uid)
                or state.pending_handin_overwrite.get(uid)
            ):
                return
            if not (state.pending_handin_files.get(uid) or []):
                return
            await reply(api, ctx, _HANDIN_SUBMIT_REMINDER_TEXT, logsvc)
        except asyncio.CancelledError:
            return
        except Exception as e:
            try:
                logsvc.log.warning(f"handin submit reminder failed: user={uid} err={e}")
            except Exception:
                pass
        finally:
            if state.pending_handin_submit_reminders.get(uid) is slot:
                state.pending_handin_submit_reminders.pop(uid, None)

    task = asyncio.create_task(_runner())
    slot["task"] = task
    state.pending_handin_submit_reminders[uid] = slot


def _set_pending_handin_submit_choice(api, ctx, logsvc: LogService, state: BotState, task_ids: List[str]) -> None:
    now_ts = time.time()
    state.pending_handin_choose[ctx.user_id] = {"mode": "submit", "task_ids": list(task_ids), "ts": now_ts}
    _schedule_pending_handin_submit_reminder(api, ctx, logsvc, state, now_ts)


def _clear_pending_handin_user(state: BotState, user_id: int) -> None:
    _cancel_pending_handin_submit_reminder(state, user_id)
    state.pending_handin_files.pop(user_id, None)
    state.pending_handin_wait_done.pop(user_id, None)
    state.pending_handin_zip_name.pop(user_id, None)
    state.pending_handin_name_input.pop(user_id, None)
    state.pending_handin_choose.pop(user_id, None)
    state.pending_handin_overwrite.pop(user_id, None)


def _delete_pending_handin_files(state: BotState, user_id: int, logsvc: Optional[LogService] = None) -> None:
    _cancel_pending_handin_submit_reminder(state, user_id)
    for it in (state.pending_handin_files.get(user_id) or []):
        try:
            Path(str(it.get("path") or "")).unlink(missing_ok=True)
        except Exception as e:
            if logsvc is not None:
                logsvc.log.warning(f"cleanup pending handin file failed: user={user_id} item={it} err={e}")
    state.pending_handin_files[user_id] = []
    state.pending_handin_wait_done.pop(user_id, None)
    state.pending_handin_zip_name.pop(user_id, None)
    state.pending_handin_name_input.pop(user_id, None)
    state.pending_handin_choose.pop(user_id, None)
    state.pending_handin_overwrite.pop(user_id, None)


def _sweep_bot_state_ttl(state: BotState, *, now: Optional[float] = None, force: bool = False) -> None:
    now_ts = float(now if now is not None else time.time())
    try:
        last_sweep = float(state.last_state_sweep_ts or 0.0)
    except Exception:
        last_sweep = 0.0
    if (not force) and ((now_ts - last_sweep) < _STATE_SWEEP_MIN_INTERVAL_SECONDS):
        return
    state.last_state_sweep_ts = now_ts

    stale_find_before = now_ts - _STATE_TTL_LAST_FIND_SECONDS
    for k in list(state.last_find.keys()):
        try:
            ts = float(state.last_find_ts.get(k, now_ts))
        except Exception:
            ts = now_ts
        if ts < stale_find_before:
            state.last_find.pop(k, None)
            state.last_find_label.pop(k, None)
            state.last_find_ts.pop(k, None)
    for k in list(state.last_find_label.keys()):
        if k not in state.last_find:
            state.last_find_label.pop(k, None)
            state.last_find_ts.pop(k, None)
    for k in list(state.last_find_ts.keys()):
        if k not in state.last_find:
            state.last_find_ts.pop(k, None)

    stale_pending_before = now_ts - _STATE_TTL_PENDING_HANDIN_SECONDS
    pending_uids = set(state.pending_handin_files.keys())
    pending_uids.update(state.pending_handin_wait_done.keys())
    pending_uids.update(state.pending_handin_zip_name.keys())
    pending_uids.update(state.pending_handin_name_input.keys())
    pending_uids.update(state.pending_handin_choose.keys())
    pending_uids.update(state.pending_handin_overwrite.keys())
    for uid in list(pending_uids):
        latest = _files_entry_latest_ts(state.pending_handin_files.get(uid))
        for m in (
            state.pending_handin_wait_done,
            state.pending_handin_zip_name,
            state.pending_handin_name_input,
            state.pending_handin_choose,
            state.pending_handin_overwrite,
        ):
            ts = _entry_ts(m.get(uid))
            if ts > latest:
                latest = ts
        if latest <= 0.0:
            latest = now_ts
        if latest < stale_pending_before:
            _clear_pending_handin_user(state, uid)

    stale_count_before = now_ts - _STATE_TTL_PENDING_COUNT_SECONDS
    for ck, item in list(state.pending_count_session.items()):
        ts = _entry_ts(item)
        if ts <= 0.0:
            ts = now_ts
        if ts < stale_count_before:
            state.pending_count_session.pop(ck, None)

    stale_notice_before = now_ts - _STATE_TTL_GROUP_NOTICE_SECONDS
    for k, ts in list(state.recent_group_notice_keys.items()):
        try:
            if float(ts) < stale_notice_before:
                state.recent_group_notice_keys.pop(k, None)
        except Exception:
            state.recent_group_notice_keys.pop(k, None)

    stale_signin_before = now_ts - _STATE_TTL_SIGNIN_SECONDS
    for gid, item in list(state.signin_tasks.items()):
        if not isinstance(item, dict):
            state.signin_tasks.pop(gid, None)
            continue
        try:
            deadline_ts = float(item.get("deadline_ts") or 0.0)
        except Exception:
            deadline_ts = 0.0
        if item.get("closed") or (deadline_ts > 0.0 and deadline_ts < stale_signin_before):
            _cancel_signin_deadline_task(item)
            state.signin_tasks.pop(gid, None)

    for uid, item in list(state.pending_signin_name_input.items()):
        if not isinstance(item, dict):
            state.pending_signin_name_input.pop(uid, None)
            continue
        try:
            gid = int(item.get("group_id") or 0)
        except Exception:
            gid = 0
        task = state.signin_tasks.get(gid)
        if not isinstance(task, dict) or str(task.get("task_id") or "") != str(item.get("task_id") or ""):
            state.pending_signin_name_input.pop(uid, None)


def conv_key(ctx) -> str:
    # 文件检索结果最好按“人”隔离，避免群里互相覆盖
    if ctx.scene == "group" and ctx.group_id is not None:
        return f"g:{ctx.group_id}:{ctx.user_id}"
    return f"p:{ctx.user_id}:{ctx.scene}"


def _compact_admin_notice_text(value: object, max_chars: int = 500) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) > max_chars:
        return text[:max_chars] + "...(truncated)"
    return text


def _format_admin_error_chat(ctx) -> str:
    if ctx is None:
        return "未知聊天"

    scene = str(getattr(ctx, "scene", "") or "").strip() or "unknown"
    user_id = getattr(ctx, "user_id", None)
    nickname = _compact_admin_notice_text(getattr(ctx, "nickname", ""), 80)
    card = _compact_admin_notice_text(getattr(ctx, "card", ""), 80)
    if scene == "group":
        group_id = getattr(ctx, "group_id", None)
        group_name = _compact_admin_notice_text(getattr(ctx, "group_name", ""), 80)
        group_part = f"群聊 group_id={group_id}"
        if group_name:
            group_part += f" group_name={group_name}"
        sender_part = f"发言人 user_id={user_id}"
        if nickname:
            sender_part += f" nickname={nickname}"
        if card and card != nickname:
            sender_part += f" card={card}"
        return f"{group_part}；{sender_part}"

    user_part = f"私聊 scene={scene} user_id={user_id}"
    if nickname:
        user_part += f" nickname={nickname}"
    return user_part


async def notify_admin_error(api, ctx, stage: str, err: object, logsvc: Optional[LogService] = None) -> None:
    send_private = getattr(api, "send_private_msg", None)
    if not callable(send_private):
        return

    admin_ids: List[int] = []
    for uid in (ADMIN_USERS or ()):
        try:
            admin_ids.append(int(uid))
        except Exception:
            continue
    if not admin_ids:
        return

    err_type = type(err).__name__
    err_text = _compact_admin_notice_text(err)
    text = "\n".join(
        [
            "机器人报错提醒",
            f"聊天：{_format_admin_error_chat(ctx)}",
            f"环节：{_compact_admin_notice_text(stage, 120)}",
            f"错误：{err_type}: {err_text}",
        ]
    )
    for admin_uid in sorted(set(admin_ids)):
        try:
            await send_private(admin_uid, text)
        except Exception as send_err:
            try:
                if logsvc is not None:
                    logsvc.log.warning(
                        f"admin error notify failed: admin={admin_uid} "
                        f"stage={_compact_admin_notice_text(stage, 80)} err={send_err}"
                    )
            except Exception:
                pass


def _claim_group_notice_key(state: BotState, key: str, ttl_seconds: float = _GROUP_NOTICE_DEDUP_SECONDS) -> bool:
    now = time.time()
    cache = state.recent_group_notice_keys
    # Opportunistic cleanup.
    stale_before = now - max(float(ttl_seconds) * 3.0, 300.0)
    for k, ts in list(cache.items()):
        if float(ts) < stale_before:
            cache.pop(k, None)

    prev = cache.get(key)
    if prev is not None and (now - float(prev) < float(ttl_seconds)):
        return False
    cache[key] = now
    return True


def _onebot_resp_ok(resp) -> bool:
    if not isinstance(resp, dict):
        return False
    try:
        return resp.get("status") == "ok" and int(resp.get("retcode", 0) or 0) == 0
    except Exception:
        return False


def _onebot_resp_detail(resp) -> str:
    if not isinstance(resp, dict):
        return "no response"
    rc = resp.get("retcode", "")
    msg = (resp.get("wording") or resp.get("message") or "").strip()
    if msg:
        return f"retcode={rc} {msg}"
    return f"retcode={rc}" if rc != "" else "send failed"


def _extract_group_member_user_ids(resp) -> List[int]:
    if not isinstance(resp, dict):
        return []
    data = resp.get("data")
    if not isinstance(data, list):
        return []
    out: List[int] = []
    seen = set()
    for item in data:
        if not isinstance(item, dict):
            continue
        try:
            uid = int(item.get("user_id") or 0)
        except Exception:
            continue
        if uid <= 0 or uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return out


async def reply(
    api,
    ctx,
    text: str,
    logsvc: LogService,
    force_private_user_id: Optional[int] = None,
):
    send_user_id = int(force_private_user_id) if force_private_user_id is not None else ctx.user_id
    send_scene = "private" if force_private_user_id is not None else ctx.scene
    send_group_id = None if force_private_user_id is not None else ctx.group_id

    async def _send_once():
        if send_scene == "group" and send_group_id is not None:
            return await api.send_group_msg(send_group_id, text)
        return await api.send_private_msg(send_user_id, text)

    skip_context_once = False
    try:
        skip_context_once = bool(getattr(ctx, "_skip_reply_context_once", False))
        if skip_context_once:
            setattr(ctx, "_skip_reply_context_once", False)
    except Exception:
        skip_context_once = False

    target = f"g:{send_group_id}" if send_scene == "group" and send_group_id is not None else f"u:{send_user_id}"
    reply_key = f"{send_scene}:{target}:{text.strip()}"
    if not _claim_recent_reply(reply_key):
        logsvc.log.info(f"消息发送去重：已拦截重复回复 target={target}")
        return False
    resp = await _send_once()
    if resp is None:
        logsvc.log.info(
            f"消息发送未确认：scene={send_scene}, group={send_group_id}, user={send_user_id}，为避免重复发送不再重试"
        )
        return False
    if not _onebot_resp_ok(resp):
        # transient network / bridge timeout retry once
        await asyncio.sleep(0.35)
        resp = await _send_once()

    if _onebot_resp_ok(resp):
        logsvc.log_out(ctx, text)
        if not skip_context_once:
            _remember_bot_reply_message(ctx, text, logsvc, send_scene, send_group_id, send_user_id)
        return True
    else:
        logsvc.log.warning(
            f"reply send failed: scene={send_scene}, group={send_group_id}, user={send_user_id}, detail={_onebot_resp_detail(resp)}"
        )
        return False


def _split_args(text: str):
    parts = text.strip().split()
    cmd = parts[0]
    rest = " ".join(parts[1:]).strip() if len(parts) > 1 else ""
    return cmd, rest


def _is_known_explicit_command(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw.startswith(("/", "／")):
        return False
    parts = raw[1:].strip().split(maxsplit=1)
    return bool(parts) and parts[0].lower() in _EXPLICIT_COMMAND_NAMES


def _parse_signin_deadline_hhmm(text: str) -> Optional[Tuple[int, int]]:
    s = unicodedata.normalize("NFKC", str(text or "")).strip()
    m = re.fullmatch(r"(\d{1,2})\s*:\s*(\d{1,2})", s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if not (0 <= hh < 24 and 0 <= mm < 60):
        return None
    return hh, mm


def _signin_deadline_ts(hh: int, mm: int, now_ts: Optional[float] = None) -> float:
    now = float(now_ts if now_ts is not None else time.time())
    lt = time.localtime(now)
    target = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, int(hh), int(mm), 0, lt.tm_wday, lt.tm_yday, lt.tm_isdst))
    if target <= now:
        target += 24.0 * 60.0 * 60.0
    return float(target)


def _format_signin_deadline(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(float(ts)))


def _format_hhmmss_from_ts(ts: Optional[float] = None) -> str:
    return time.strftime("%H:%M:%S", time.localtime(float(ts if ts is not None else time.time())))


def _time_text_to_seconds(text: str) -> Optional[int]:
    m = re.fullmatch(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", str(text or "").strip())
    if not m:
        return None
    hh, mm, ss = int(m.group(1)), int(m.group(2)), int(m.group(3) or 0)
    if not (0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60):
        return None
    return hh * 3600 + mm * 60 + ss


def _clock_delta_seconds(a: str, b: str) -> Optional[float]:
    aa = _time_text_to_seconds(a)
    bb = _time_text_to_seconds(b)
    if aa is None or bb is None:
        return None
    diff = abs(int(aa) - int(bb))
    return float(min(diff, 24 * 3600 - diff))


def _time_delta_to_now_seconds(text: str, now_ts: Optional[float] = None) -> Optional[float]:
    now_text = _format_hhmmss_from_ts(now_ts)
    return _clock_delta_seconds(text, now_text)


def _seconds_to_hhmmss(seconds: float) -> str:
    sec = int(round(float(seconds))) % (24 * 3600)
    return f"{sec // 3600:02d}:{(sec % 3600) // 60:02d}:{sec % 60:02d}"


def _clock_average_time_text(a: str, b: str) -> Optional[str]:
    aa = _time_text_to_seconds(a)
    bb = _time_text_to_seconds(b)
    if aa is None or bb is None:
        return None
    a2 = float(aa)
    b2 = float(bb)
    if abs(a2 - b2) > 12 * 3600:
        if a2 < b2:
            a2 += 24 * 3600
        else:
            b2 += 24 * 3600
    return _seconds_to_hhmmss((a2 + b2) / 2.0)


def _parse_cq_kvs(raw: str) -> Dict[str, str]:
    data: Dict[str, str] = {}
    for kv in str(raw or "").split(","):
        if "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        key = str(k).strip().lower()
        if not key:
            continue
        data[key] = html.unescape(str(v).strip())
    return data


def _normalize_image_src(raw: str) -> str:
    return html.unescape(str(raw or "").strip()).replace("&amp;", "&")


def _extract_signin_image_items(evt: dict) -> List[dict]:
    out: List[dict] = []
    seen = set()

    def _push(item: dict) -> None:
        url = _normalize_image_src(item.get("url") or "")
        file = _normalize_image_src(item.get("file") or "")
        file_id = str(item.get("file_id") or "").strip()
        path = _normalize_image_src(item.get("path") or "")
        key = (url, file, file_id, path)
        if key in seen:
            return
        seen.add(key)
        out.append(
            {
                "url": url,
                "file": file,
                "file_id": file_id,
                "path": path,
                "name": str(item.get("name") or "").strip(),
                "size": str(item.get("size") or "").strip(),
            }
        )

    msg = evt.get("message")
    segments = msg if isinstance(msg, list) else ([msg] if isinstance(msg, dict) else [])
    for seg in segments:
        if not isinstance(seg, dict):
            continue
        if str(seg.get("type") or "").strip().lower() != "image":
            continue
        data = seg.get("data") or {}
        _push(
            {
                "url": data.get("url") or data.get("download_url") or data.get("file_url") or "",
                "file": data.get("file") or "",
                "file_id": data.get("file_id") or data.get("id") or "",
                "path": data.get("path") or data.get("file_path") or "",
                "name": data.get("name") or data.get("file") or "",
                "size": data.get("file_size") or data.get("size") or "",
            }
        )

    raw_values = [evt.get("raw_message")]
    if isinstance(msg, str):
        raw_values.append(msg)
    for raw_value in raw_values:
        raw = str(raw_value or "")
        for m in _CQ_IMAGE_RE.findall(raw):
            kvs = _parse_cq_kvs(m)
            _push(
                {
                    "url": kvs.get("url") or kvs.get("download_url") or kvs.get("file_url") or "",
                    "file": kvs.get("file") or "",
                    "file_id": kvs.get("file_id") or kvs.get("id") or "",
                    "path": kvs.get("path") or kvs.get("file_path") or "",
                    "name": kvs.get("name") or kvs.get("file") or "",
                    "size": kvs.get("file_size") or kvs.get("size") or "",
                }
            )

    return out


def _is_direct_signin_image_src(src: str) -> bool:
    s = _normalize_image_src(src)
    if not s:
        return False
    if s.startswith(("http://", "https://", "file:///")):
        return True
    if s.startswith("/"):
        return True
    return re.match(r"^[A-Za-z]:[\\/]", s) is not None


def _extract_signin_src_from_resp(resp: Optional[dict]) -> str:
    if not resp or resp.get("status") != "ok":
        return ""
    data = resp.get("data")
    if isinstance(data, str):
        return _normalize_image_src(data)
    data = data or {}
    return _normalize_image_src(
        data.get("url")
        or data.get("download_url")
        or data.get("file")
        or data.get("file_path")
        or data.get("path")
        or ""
    )


async def _resolve_signin_image_source(api, item: dict) -> str:
    for key in ("url", "path", "file"):
        src = _normalize_image_src(item.get(key) or "")
        if src and (key == "url" or _is_direct_signin_image_src(src)):
            return src

    call = getattr(api, "call", None)
    tried = set()
    for token in (item.get("file"), item.get("file_id")):
        file_token = str(token or "").strip()
        if not file_token or file_token in tried:
            continue
        tried.add(file_token)
        if callable(call):
            resp = await call("get_image", {"file": file_token}, timeout=60.0)
            src = _extract_signin_src_from_resp(resp)
            if src:
                return src

    get_file = getattr(api, "get_file", None)
    for token in (item.get("file_id"), item.get("file")):
        file_token = str(token or "").strip()
        if not file_token or not callable(get_file):
            continue
        resp = await get_file(file_token, timeout=60.0, retries=1, retry_delay=1.0)
        src = _extract_signin_src_from_resp(resp)
        if src:
            return src
    return ""


def _signin_image_filename(item: dict, user_id: int, src: str = "") -> str:
    candidates = [
        item.get("name") or "",
        item.get("file") or "",
        item.get("path") or "",
        src,
    ]
    name = ""
    for raw in candidates:
        s = _normalize_image_src(raw)
        if not s:
            continue
        s = re.split(r"[?#]", s, maxsplit=1)[0]
        name = Path(s).name or s
        if name:
            break
    if not name:
        name = f"signin_{int(user_id)}.jpg"
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(name).stem).strip("._-") or f"signin_{int(user_id)}"
    ext = Path(name).suffix.lower()
    if ext not in _SIGNIN_IMAGE_SUFFIXES:
        ext = ".jpg"
    return f"{stem}{ext}"


async def _download_signin_image(api, ctx, item: dict, handin: HandinService, logsvc: LogService) -> Tuple[bool, str, Optional[Path]]:
    src = await _resolve_signin_image_source(api, item)
    if not src:
        return False, "图片下载链接获取失败，请重新发送。", None
    expected_size: Optional[int] = None
    try:
        raw_size = str(item.get("size") or "").strip()
        expected_size = int(raw_size) if raw_size else None
    except Exception:
        expected_size = None
    fname = _signin_image_filename(item, ctx.user_id, src)
    ok, msg, p = await asyncio.to_thread(
        handin.download_to_inbox,
        ctx.user_id,
        fname,
        src,
        expected_size,
        180.0,
    )
    if not ok or not p:
        try:
            logsvc.log.warning(f"signin image download failed: user={ctx.user_id} msg={msg}")
        except Exception:
            pass
        return False, "图片下载失败，请重新发送。", None
    return True, "", Path(p)


def _signin_get_roster(handin: HandinService) -> List[Tuple[str, str]]:
    get_roster = getattr(handin, "_get_roster", None)
    if not callable(get_roster):
        return []
    try:
        return list(get_roster() or [])
    except Exception:
        return []


def _signin_get_roster_names(handin: HandinService) -> List[str]:
    get_names = getattr(handin, "_get_roster_names", None)
    if callable(get_names):
        try:
            names = [str(x).strip() for x in list(get_names() or [])]
            return [x for x in names if x]
        except Exception:
            pass
    names: List[str] = []
    seen = set()
    for _, name in _signin_get_roster(handin):
        nm = str(name or "").strip()
        if nm and nm not in seen:
            seen.add(nm)
            names.append(nm)
    names.sort(key=lambda s: len(s), reverse=True)
    return names


def _signin_match_roster_name(ctx, handin: HandinService) -> str:
    text = " ".join([str(getattr(ctx, "card", "") or ""), str(getattr(ctx, "nickname", "") or "")]).strip()
    if not text:
        return ""
    names = _signin_get_roster_names(handin)
    finder = getattr(handin, "find_roster_name_in_filename", None)
    if callable(finder):
        try:
            found = finder(text, roster_names=names)
            if isinstance(found, str) and found.strip():
                return found.strip()
        except Exception:
            pass
    compact = re.sub(r"\s+", "", text)
    for name in names:
        if name and (name in text or name in compact):
            return name
    return ""


def _signin_list_add(values: List[str], value: str) -> None:
    v = str(value or "").strip()
    if v and v not in values:
        values.append(v)


def _signin_event_ts(evt: dict) -> float:
    try:
        ts = float(evt.get("time") or 0.0)
    except Exception:
        ts = 0.0
    return ts if ts > 0.0 else time.time()


def _signin_begin_job(task: dict) -> None:
    active = int(task.get("active_jobs") or 0) + 1
    task["active_jobs"] = active
    event = task.get("idle_event")
    if not isinstance(event, asyncio.Event):
        event = asyncio.Event()
        task["idle_event"] = event
    event.clear()


def _signin_end_job(task: dict) -> None:
    active = max(0, int(task.get("active_jobs") or 0) - 1)
    task["active_jobs"] = active
    if active <= 0:
        event = task.get("idle_event")
        if isinstance(event, asyncio.Event):
            event.set()


def _signin_task_deadline_time_text(task_or_ts) -> str:
    if isinstance(task_or_ts, dict):
        ts = float(task_or_ts.get("deadline_ts") or time.time())
    else:
        ts = float(task_or_ts if task_or_ts is not None else time.time())
    return _format_hhmmss_from_ts(ts)


def _signin_average_image_time_text(res) -> Optional[str]:
    visual_text = str(getattr(res, "visual_time_text", "") or "").strip()
    stamp_text = str(getattr(res, "timestamp_time_text", "") or "").strip()
    if visual_text and stamp_text:
        return _clock_average_time_text(visual_text, stamp_text)
    return None


def _evaluate_signin_ocr_result(res, deadline_ts: Optional[float] = None, now_ts: Optional[float] = None) -> Optional[str]:
    visual_text = str(getattr(res, "visual_time_text", "") or "").strip()
    stamp_text = str(getattr(res, "timestamp_time_text", "") or "").strip()
    if not visual_text:
        return "未识别到有效时间，请重新拍摄。"
    if not stamp_text:
        return "未读取到图片时间戳，请重新拍摄或发送原图。"

    stamp_diff = _clock_delta_seconds(visual_text, stamp_text)
    if stamp_diff is None or stamp_diff > _SIGNIN_VISUAL_TIMESTAMP_TOLERANCE_SECONDS:
        return f"识别时间与图片时间戳相差超过3分钟（识别 {visual_text}，时间戳 {stamp_text}），请重新拍摄。"

    avg_text = _signin_average_image_time_text(res)
    if not avg_text:
        return "未识别到有效时间，请重新拍摄。"
    deadline_text = _signin_task_deadline_time_text(deadline_ts if deadline_ts is not None else now_ts)
    deadline_diff = _clock_delta_seconds(avg_text, deadline_text)
    if deadline_diff is None or deadline_diff > _SIGNIN_DEADLINE_TOLERANCE_SECONDS:
        return f"图片时间与signin截止时间相差超过30分钟（图片平均时间 {avg_text}，截止 {deadline_text}），请重新拍摄。"
    return None


async def _send_signin_creator_notice(api, logsvc: LogService, creator_id: int, text: str) -> None:
    try:
        resp = await api.send_private_msg(int(creator_id), text)
        if resp is not None and not _onebot_resp_ok(resp):
            logsvc.log.warning(f"signin private notice failed: user={creator_id}, detail={_onebot_resp_detail(resp)}")
    except Exception as e:
        try:
            logsvc.log.warning(f"signin private notice failed: user={creator_id}, err={e}")
        except Exception:
            pass


def _cancel_signin_deadline_task(task: dict) -> None:
    scheduled = task.get("deadline_task") if isinstance(task, dict) else None
    if scheduled is None or not hasattr(scheduled, "done") or not hasattr(scheduled, "cancel"):
        return
    try:
        current = asyncio.current_task()
    except Exception:
        current = None
    try:
        if scheduled is not current and not scheduled.done():
            scheduled.cancel()
    except Exception:
        pass


def _signin_submitted_user_lines(users: Dict[str, dict]) -> List[str]:
    lines: List[str] = []
    for i, (uid, item) in enumerate(users.items(), 1):
        nick = str(item.get("nickname") or uid)
        name = str(item.get("name") or "").strip()
        suffix = f"（{name}）" if name else ""
        lines.append(f"{i}. {nick}{suffix}（QQ {uid}）")
    return lines


async def _finish_signin_task(
    api,
    state: BotState,
    group_id: int,
    handin: HandinService,
    logsvc: LogService,
    task_id: Optional[str] = None,
) -> bool:
    gid = int(group_id)
    task = state.signin_tasks.get(gid)
    if not isinstance(task, dict):
        return False
    if task_id is not None and str(task.get("task_id") or "") != str(task_id):
        return False
    if task.get("finalizing"):
        return False
    task["closing"] = True
    task["finalizing"] = True
    try:
        deadline_ts = float(task.get("deadline_ts") or 0.0)
    except Exception:
        deadline_ts = 0.0
    grace_until = deadline_ts + _SIGNIN_FINALIZE_GRACE_SECONDS
    if deadline_ts > 0.0 and time.time() < grace_until:
        await asyncio.sleep(max(0.0, grace_until - time.time()))
    while int(task.get("active_jobs") or 0) > 0:
        event = task.get("idle_event")
        if isinstance(event, asyncio.Event):
            try:
                await asyncio.wait_for(event.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.sleep(0.2)
        if state.signin_tasks.get(gid) is not task:
            return False
    state.signin_tasks.pop(gid, None)
    task["closed"] = True
    _cancel_signin_deadline_task(task)

    creator_id = int(task.get("creator_id") or 0)
    roster = _signin_get_roster(handin)
    roster_names = [str(name or "").strip() for _, name in roster if str(name or "").strip()]
    submitted_names = set(str(x).strip() for x in list(task.get("submitted_names") or []) if str(x).strip())
    users = task.get("submitted_users") if isinstance(task.get("submitted_users"), dict) else {}

    lines = [
        f"signin任务已截止（群 {gid}）",
        f"截止时间：{_format_signin_deadline(float(task.get('deadline_ts') or time.time()))}",
    ]
    if roster_names:
        missing = [name for name in roster_names if name not in submitted_names]
        lines.append(f"已签到：{len(submitted_names)}/{len(roster_names)}")
        if missing:
            lines.append(f"未签到名单（{len(missing)}）：")
            for i, name in enumerate(missing, 1):
                lines.append(f"{i}. {name}")
        else:
            lines.append("未签到名单：无")
        unmatched = [item for item in users.values() if not str(item.get("name") or "").strip()]
        if unmatched:
            lines.append("")
            lines.append("未匹配名册的已签到用户：")
            for item in unmatched:
                nick = str(item.get("nickname") or item.get("qq") or "")
                qq = str(item.get("qq") or "")
                lines.append(f"- {nick}（QQ {qq}）")
    else:
        lines.append("未读取到班级名册，无法核对未签到名单。")
        user_lines = _signin_submitted_user_lines(users)
        lines.append(f"已收到签到（{len(user_lines)}）：")
        lines.extend(user_lines or ["无"])

    if creator_id > 0:
        await _send_signin_creator_notice(api, logsvc, creator_id, "\n".join(lines))
    for uid, item in list(state.pending_signin_name_input.items()):
        if isinstance(item, dict) and int(item.get("group_id") or 0) == gid and str(item.get("task_id") or "") == str(task.get("task_id") or ""):
            state.pending_signin_name_input.pop(uid, None)
    return True


def _schedule_signin_deadline(api, state: BotState, group_id: int, handin: HandinService, logsvc: LogService, task_id: str) -> None:
    task = state.signin_tasks.get(int(group_id))
    if not isinstance(task, dict):
        return
    deadline_ts = float(task.get("deadline_ts") or 0.0)

    async def _runner() -> None:
        try:
            while True:
                delay = deadline_ts - time.time()
                if delay <= 0:
                    break
                await asyncio.sleep(min(float(delay), 3600.0))
            await _finish_signin_task(api, state, int(group_id), handin, logsvc, task_id=task_id)
        except asyncio.CancelledError:
            return
        except Exception as e:
            try:
                logsvc.log.warning(f"signin deadline task failed: group={group_id}, err={e}")
            except Exception:
                pass

    task["deadline_task"] = asyncio.create_task(_runner())


async def _record_signin_failure(api, ctx, logsvc: LogService, task: dict, reason: str) -> None:
    uid = str(ctx.user_id)
    failures = task.setdefault("failures", {})
    if not isinstance(failures, dict):
        failures = {}
        task["failures"] = failures
    reasons = list(failures.get(uid) or [])
    reasons.append(str(reason or "提交失败，请重新拍摄。"))
    failures[uid] = reasons[-2:]

    if len(reasons) < 2:
        await reply(api, ctx, failures[uid][-1], logsvc)
        return

    await reply(api, ctx, "多次失败已联系任务创建人，请重新拍摄后再提交。", logsvc)
    notified = task.setdefault("failure_notified", [])
    if uid in notified:
        return
    notified.append(uid)
    creator_id = int(task.get("creator_id") or 0)
    if creator_id <= 0:
        return
    display = str(getattr(ctx, "card", "") or getattr(ctx, "nickname", "") or ctx.user_id)
    lines = [
        "signin提交多次失败：",
        f"用户：{display}",
        f"QQ：{ctx.user_id}",
        "失败原因：",
    ]
    for i, one in enumerate(failures[uid], 1):
        lines.append(f"{i}. {one}")
    await _send_signin_creator_notice(api, logsvc, creator_id, "\n".join(lines))


def _select_private_signin_task(state: BotState, ctx) -> Tuple[Optional[int], Optional[dict], str]:
    if not str(getattr(ctx, "scene", "") or "").startswith("private"):
        return None, None, ""
    ctx_gid = getattr(ctx, "group_id", None)
    if ctx_gid is not None:
        try:
            gid = int(ctx_gid)
        except Exception:
            gid = 0
        task = state.signin_tasks.get(gid)
        if isinstance(task, dict) and not task.get("closed"):
            return gid, task, ""

    items: List[Tuple[int, dict]] = []
    for gid, task in state.signin_tasks.items():
        if isinstance(task, dict) and not task.get("closed"):
            items.append((int(gid), task))
    if not items:
        return None, None, ""
    if len(items) == 1:
        return items[0][0], items[0][1], ""
    items.sort(key=lambda pair: float(pair[1].get("deadline_ts") or 0.0))
    return None, None, "当前有多个signin任务，无法判断这张图片属于哪一个；请通过对应群的临时会话发送，或联系任务创建人。"


def _record_signin_name(task: dict, user_id: int, nickname: str, name: str, pend: dict) -> None:
    uid = str(user_id)
    clean_name = str(name or "").strip()
    submitted_names = task.setdefault("submitted_names", [])
    if isinstance(submitted_names, list):
        _signin_list_add(submitted_names, clean_name)
    submitted_users = task.setdefault("submitted_users", {})
    if isinstance(submitted_users, dict):
        submitted_users[uid] = {
            "nickname": str(nickname or uid),
            "qq": uid,
            "name": clean_name,
            "time": str(pend.get("time") or ""),
            "image_time": str(pend.get("image_time") or ""),
            "source": str(pend.get("source") or ""),
            "ts": float(pend.get("ts") or time.time()),
        }
    task.setdefault("failures", {}).pop(uid, None)


async def _handle_private_signin_name_input(api, ctx, text: str, logsvc: LogService, state: BotState, handin: HandinService) -> bool:
    if not str(getattr(ctx, "scene", "") or "").startswith("private"):
        return False
    pend = state.pending_signin_name_input.get(int(ctx.user_id))
    if not isinstance(pend, dict):
        return False
    t = unicodedata.normalize("NFKC", str(text or "")).strip()
    if not t:
        return False
    logsvc.log_in(ctx, t)
    if t in ("0", "取消", "/cancel", "／cancel"):
        state.pending_signin_name_input.pop(int(ctx.user_id), None)
        await reply(api, ctx, "已取消本次signin姓名记录，请重新发送图片提交。", logsvc)
        return True
    try:
        gid = int(pend.get("group_id") or 0)
    except Exception:
        gid = 0
    task = state.signin_tasks.get(gid)
    if not isinstance(task, dict) or str(task.get("task_id") or "") != str(pend.get("task_id") or ""):
        state.pending_signin_name_input.pop(int(ctx.user_id), None)
        await reply(api, ctx, "对应的signin任务已结束，请联系任务创建人。", logsvc)
        return True

    roster_name = ""
    finder = getattr(handin, "find_roster_name_in_filename", None)
    if callable(finder):
        try:
            roster_name = str(finder(t, roster_names=_signin_get_roster_names(handin)) or "").strip()
        except Exception:
            roster_name = ""
    name = roster_name or t
    display = str(getattr(ctx, "nickname", "") or ctx.user_id)
    _record_signin_name(task, int(ctx.user_id), display, name, pend)
    state.pending_signin_name_input.pop(int(ctx.user_id), None)
    await reply(api, ctx, f"signin成功，已记录姓名：{name}", logsvc)
    return True


async def _handle_signin_image(api, ctx, evt: dict, logsvc: LogService, state: BotState, handin: HandinService) -> bool:
    if not str(getattr(ctx, "scene", "") or "").startswith("private"):
        return False
    gid, task, select_msg = _select_private_signin_task(state, ctx)
    if select_msg:
        if _extract_signin_image_items(evt):
            await reply(api, ctx, select_msg, logsvc)
            return True
        return False
    if gid is None or not isinstance(task, dict):
        return False
    images = _extract_signin_image_items(evt)
    if not images:
        return False

    logsvc.log_in(ctx, "[signin image]")
    now_ts = time.time()
    deadline_ts = float(task.get("deadline_ts") or 0.0)
    event_ts = _signin_event_ts(evt)
    if event_ts > deadline_ts:
        if not task.get("finalizing"):
            asyncio.create_task(_finish_signin_task(api, state, int(gid), handin, logsvc, task_id=str(task.get("task_id") or "")))
        await reply(api, ctx, "signin任务已截止。", logsvc)
        return True

    _signin_begin_job(task)
    try:
        try:
            from signin_ocr import recognize_led_time_from_path
        except Exception as e:
            logsvc.log.warning(f"signin ocr import failed: err={e}")
            await reply(api, ctx, "signin识别模块暂不可用，请稍后重试。", logsvc)
            return True

        ok, msg, path = await _download_signin_image(api, ctx, images[0], handin, logsvc)
        if not ok or path is None:
            await _record_signin_failure(api, ctx, logsvc, task, msg)
            return True

        try:
            res = await asyncio.to_thread(recognize_led_time_from_path, path)
        except Exception as e:
            logsvc.log.warning(f"signin ocr failed: path={path} err={e}")
            await _record_signin_failure(api, ctx, logsvc, task, "未识别到有效时间，请重新拍摄。")
            return True
        finally:
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                pass

        reason = _evaluate_signin_ocr_result(res, deadline_ts=deadline_ts)
        if reason:
            await _record_signin_failure(api, ctx, logsvc, task, reason)
            return True

        uid = str(ctx.user_id)
        image_time = _signin_average_image_time_text(res) or str(getattr(res, "time_text", "") or "")
        state.pending_signin_name_input[int(ctx.user_id)] = {
            "group_id": int(gid),
            "task_id": str(task.get("task_id") or ""),
            "time": str(getattr(res, "time_text", "") or ""),
            "image_time": image_time,
            "source": str(getattr(res, "source", "") or ""),
            "ts": now_ts,
        }
        source = str(getattr(res, "source", "") or "")
        await reply(api, ctx, f"时间校验通过：{image_time}\n请回复你的姓名完成signin记录。", logsvc)
        return True
    finally:
        _signin_end_job(task)


def _parse_count_names(text: str) -> List[str]:
    raw = unicodedata.normalize("NFKC", str(text or "")).strip()
    if not raw:
        return []
    # 兼容常见输入：
    # - 行首编号：1、张三 / 2.李四 / 3)王五
    # - 行内连续编号：1. 张三 2. 李四
    # - CQ @ 段
    raw = re.sub(r"\d+\s*[、,，.\)）:：\-]\s*", "\n", raw)
    raw = re.sub(r"(?i)\[CQ:at,[^\]]+\]", " ", raw)

    out: List[str] = []
    for one in _COUNT_NAME_SPLIT_RE.split(raw):
        token = _COUNT_NAME_PREFIX_RE.sub("", one.strip())
        token = token.strip().strip("，、,;；。.!！?？")
        if token:
            out.append(token)
    return out


def _is_count_end_input(text: str) -> bool:
    s = unicodedata.normalize("NFKC", str(text or "")).strip()
    if not s:
        return False
    if _COUNT_END_RE.fullmatch(s):
        return True
    return bool(_COUNT_END_CN_RE.fullmatch(s))


def _dedup_names_keep_order(names: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for one in names:
        name = str(one or "").strip()
        if (not name) or (name in seen):
            continue
        seen.add(name)
        out.append(name)
    return out


def _build_count_list_text(submitted_names: List[str], roster: List[Tuple[str, str]]) -> str:
    submitted = _dedup_names_keep_order(submitted_names or [])

    lines: List[str] = []
    lines.append(f"已提交名单（{len(submitted)}）：")
    if submitted:
        for i, name in enumerate(submitted, 1):
            lines.append(f"{i}. {name}")
        lines.append("可用 /countremove 序号 移除已提交名单中的人名。")
    else:
        lines.append("（暂无）")

    roster_names: List[str] = []
    seen_roster = set()
    for _, nm in roster or []:
        name = str(nm or "").strip()
        if (not name) or (name in seen_roster):
            continue
        seen_roster.add(name)
        roster_names.append(name)

    if not roster_names:
        lines.append("")
        lines.append("⚠️ 班级名册不可用，暂时无法计算未交名单。")
        return "\n".join(lines)

    submitted_set = set(submitted)
    missing = [name for name in roster_names if name not in submitted_set]
    outside_roster = [name for name in submitted if name not in seen_roster]

    lines.append("")
    lines.append(f"未交名单（{len(missing)}）：")
    if missing:
        for i, name in enumerate(missing, 1):
            lines.append(f"{i}. {name}")
    else:
        lines.append("✅ 无，已全部提交。")

    if outside_roster:
        lines.append("")
        lines.append("不在班级名册中的已提交姓名：")
        for i, name in enumerate(outside_roster, 1):
            lines.append(f"{i}. {name}")
        lines.append("（可按其在“已提交名单”中的序号使用 /countremove）")
    return "\n".join(lines)


def _parse_find_args(rest: str, filesvc: FileService) -> Tuple[str, Optional[str]]:
    """
    Parse /find arguments.
    Supports:
    - /find <keyword>
    - /find <keyword...> <root/subdir>
    """
    s = (rest or "").strip()
    if not s:
        return "", None

    parts = s.split()
    if len(parts) == 1:
        return parts[0], None

    last = parts[-1].strip().strip("/")
    if not last:
        return s, None

    known_roots = {str(r.name).strip().lower() for r in filesvc.roots}
    known_roots.update({"group", "groups"})

    head = last.split("/", 1)[0].strip().lower()
    looks_like_dir = ("/" in last) or (head in known_roots)
    if looks_like_dir:
        kw = " ".join(parts[:-1]).strip()
        if kw:
            return kw, last
    return s, None


def _path_identity_key(p: Path) -> str:
    try:
        return str(p.resolve()).casefold()
    except (OSError, RuntimeError):
        return str(p).casefold()


def _split_find_scope_parts(in_dir: Optional[str]) -> Optional[List[str]]:
    raw = str(in_dir or "").strip().replace("\\", "/").strip("/")
    if not raw:
        return []
    parts: List[str] = []
    for seg in raw.split("/"):
        piece = seg.strip()
        if not piece or piece == ".":
            continue
        if piece == "..":
            return None
        parts.append(piece)
    return parts


def _semantic_merge_allowed_for_in_dir(in_dir: Optional[str]) -> bool:
    parts = _split_find_scope_parts(in_dir)
    if parts is None:
        return False
    if not parts:
        return True
    lower = [x.lower() for x in parts]
    return len(lower) >= 2 and lower[0] == "public" and lower[1] == "textbook_and_material"


def _semantic_filter_base_for_in_dir(in_dir: Optional[str]) -> Optional[Path]:
    parts = _split_find_scope_parts(in_dir)
    if not parts:
        return None
    lower = [x.lower() for x in parts]
    if len(lower) < 2 or lower[0] != "public" or lower[1] != "textbook_and_material":
        return None
    base = DATA_DIR / "public" / "textbook_and_material"
    for seg in parts[2:]:
        base = base / seg
    return base


def _filter_paths_under_base(paths: List[Path], base: Optional[Path]) -> List[Path]:
    if not base:
        return list(paths)
    try:
        base_res = base.resolve()
    except (OSError, RuntimeError):
        base_res = base
    out: List[Path] = []
    for p in paths:
        try:
            p_res = p.resolve()
        except (OSError, RuntimeError):
            p_res = p
        try:
            p_res.relative_to(base_res)
        except (OSError, RuntimeError, ValueError):
            continue
        out.append(p_res)
    return out


def _merge_find_hits(primary_hits: List[Path], semantic_hits: List[Path]) -> Tuple[List[Path], List[bool]]:
    merged: List[Path] = []
    semantic_flags: List[bool] = []
    seen = set()
    for p in primary_hits:
        k = _path_identity_key(p)
        if k in seen:
            continue
        seen.add(k)
        merged.append(p)
        semantic_flags.append(False)
    semantic_seen = set()
    for p in semantic_hits:
        k = _path_identity_key(p)
        if k in semantic_seen:
            continue
        semantic_seen.add(k)
        merged.append(p)
        semantic_flags.append(True)
    return merged, semantic_flags


def _parse_semantic_find_query(rest: str) -> Optional[str]:
    """Parse `/find "需求"` style semantic query.
    Supports English/Chinese single and double quotes.
    """
    s = (rest or "").strip()
    if not s:
        return None
    quote_pairs = {
        '"': '"',
        "'": "'",
        "\uff02": "\uff02",
        "\uff07": "\uff07",
        "\u201c": "\u201d",
        "\u2018": "\u2019",
    }
    opener = s[0]
    closer = quote_pairs.get(opener)
    if not closer:
        return None
    end = s.find(closer, 1)
    if end <= 1:
        return None
    tail = s[end + 1 :].strip()
    if tail:
        return None
    q = s[1:end].strip()
    return q or None


def _is_brief_or_generic_find_query(query: str) -> bool:
    q = str(query or "").strip()
    if not q:
        return False
    compact = re.sub(r"\s+", "", q).lower()
    if compact in _FIND_GENERIC_TERMS or compact in _FIND_SUBJECT_SHORT_TERMS:
        return True
    parts = [x for x in re.split(r"[\s,，、/|]+", q) if x]
    if len(parts) == 1 and len(compact) <= 2:
        return True
    if len(parts) <= 2:
        normalized_parts = [re.sub(r"\s+", "", p).lower() for p in parts]
        if normalized_parts and all((p in _FIND_GENERIC_TERMS or p in _FIND_SUBJECT_SHORT_TERMS) for p in normalized_parts):
            return True
    return False


def _build_find_guidance_message(query: str = "", no_result: bool = False) -> str:
    compact = re.sub(r"\s+", "", str(query or "")).lower()
    if _is_brief_or_generic_find_query(query):
        if ("数电" in compact) or ("数字电子" in compact):
            return (
                "提示：你也可以把需求说得更具体一些，结果通常会更准。\n"
                "例如：\n"
                "/find 数字电子技术教材\n"
                "/find 数电实验报告模板\n"
                "/find 数电期末复习题及答案"
            )
        if ("高数" in compact) or ("高等数学" in compact):
            return (
                "提示：你也可以把需求说得更具体一些，结果通常会更准。\n"
                "例如：\n"
                "/find 高等数学教材同济版\n"
                "/find 高数期末复习重点总结\n"
                "/find 适合考试复习的高数题库"
            )
        return (
            "提示：你也可以把需求说得更具体一些，结果通常会更准。\n"
            "例如：\n"
            "/find 数字电子技术教材\n"
            "/find 数电实验报告模板\n"
            "/find 适合考试复习的高数题库"
        )
    if no_result:
        return (
            "你也可以换一种更自然、更具体的说法试试，例如：\n"
            "/find 数字电子技术教材\n"
            "/find 期末复习用的数电资料\n"
            "/find 带答案的数理方程课后题"
        )
    return (
        "提示：/find 支持直接描述需求，越具体通常越准确。\n"
        "例如：\n"
        "/find 数字电子技术教材\n"
        "/find 期末复习用的高数资料\n"
        "/find 带答案的数理方程习题"
    )


def _format_ai_chat_at_text(data: dict) -> str:
    if not isinstance(data, dict):
        data = {}
    qq = str(data.get("qq") or data.get("user_id") or "").strip()
    label = str(data.get("name") or data.get("nickname") or data.get("card") or qq).strip()
    if not label:
        return "@"
    if label.startswith("@"):
        return label
    return f"@{label}"


def _normalize_ai_chat_segment_text(text: str) -> str:
    return re.sub(r"[ \t\r\f\v]+", " ", str(text or "")).strip()


def _render_group_ai_chat_message(evt: dict) -> Optional[str]:
    self_id = str(evt.get("self_id") or "").strip()
    msg = evt.get("message")
    if isinstance(msg, list):
        parts: List[str] = []
        for seg in msg:
            if not isinstance(seg, dict):
                continue
            tp = str(seg.get("type") or "").strip().lower()
            data = seg.get("data") or {}
            if not isinstance(data, dict):
                data = {}
            if tp == "text":
                parts.append(str(data.get("text") or ""))
                continue
            if tp == "at":
                qq = str(data.get("qq") or data.get("user_id") or "").strip()
                if self_id and qq == self_id:
                    parts.append(" ")
                else:
                    parts.append(f" {_format_ai_chat_at_text(data)} ")
                continue
            if tp in _TEXT_COMPANION_EMOJI_SEG_TYPES or tp == "reply":
                parts.append(" ")
        rendered = _normalize_ai_chat_segment_text("".join(parts))
        return rendered or None

    raw = str(evt.get("raw_message") or "")
    if not raw:
        return None

    def _replace_at(match: re.Match) -> str:
        data = _parse_cq_kvs(match.group(1))
        qq = str(data.get("qq") or data.get("user_id") or "").strip()
        if self_id and qq == self_id:
            return " "
        return f" {_format_ai_chat_at_text(data)} "

    rendered = _CQ_AT_RE.sub(_replace_at, raw)
    rendered = _strip_text_companion_cq_segments(rendered)
    rendered = _normalize_ai_chat_segment_text(rendered)
    return rendered or None


def _extract_reply_msg_id(evt: dict) -> Optional[str]:
    """提取消息中的引用（reply）段对应的被引用消息 id；无引用返回 None。"""
    msg = evt.get("message")
    if isinstance(msg, list):
        for seg in msg:
            if not isinstance(seg, dict):
                continue
            if str(seg.get("type") or "").strip().lower() != "reply":
                continue
            data = seg.get("data") or {}
            if not isinstance(data, dict):
                data = {}
            rid = str(data.get("id") or data.get("message_id") or "").strip()
            if rid:
                return rid
    raw = str(evt.get("raw_message") or "")
    m = re.search(r"\[CQ:reply,id=(\d+)\]", raw, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def _evt_mentions_me(evt: dict) -> bool:
    self_id = str(evt.get("self_id") or "").strip()
    msg = evt.get("message")
    if isinstance(msg, list):
        for seg in msg:
            if not isinstance(seg, dict):
                continue
            tp = str(seg.get("type") or "").lower()
            if tp != "at":
                continue
            data = seg.get("data") or {}
            qq = str(data.get("qq") or data.get("user_id") or "").strip()
            if self_id and qq == self_id:
                return True
    raw = str(evt.get("raw_message") or "")
    if self_id and raw and (f"qq={self_id}" in raw):
        return True
    return False


def _extract_ai_chat_input(ctx, evt: dict, text: str, bot_nick: str) -> Optional[str]:
    msg = str(text or "").strip()
    scene = str(getattr(ctx, "scene", "") or "")
    if not _is_ai_chat_text_message(evt, msg):
        return None
    msg = _strip_text_companion_cq_segments(msg)
    if scene == "group":
        segment_msg = _render_group_ai_chat_message(evt)
        if segment_msg is not None:
            msg = segment_msg
        nick_aliases = [x for x in {str(bot_nick or "").strip(), "Cooper_bot", "Cooepr_bot"} if x]
        has_nick_mention = False
        for nick in nick_aliases:
            pat = rf"[@\uFF20]\s*{re.escape(nick)}"
            if re.search(pat, msg, flags=re.IGNORECASE):
                has_nick_mention = True
                msg = re.sub(pat, "", msg, flags=re.IGNORECASE).strip()
        # get_text 优先使用 raw_message，群聊@常带 [CQ:at,qq=...]，这里移除避免把 bot QQ 误当作用户 QQ。
        msg = re.sub(r"(?i)\[CQ:at,[^\]]+\]", "", msg).strip()
        if not (_evt_mentions_me(evt) or has_nick_mention):
            return None
        return msg
    if scene.startswith("private"):
        if not msg or msg.startswith(("/", "／")):
            return None
        return msg
    return None


def extract_ai_chat_trigger_text(
    ctx,
    evt: dict,
    raw_text: str,
    has_visual: bool,
    bot_nick: str,
) -> Optional[str]:
    """AI 聊天触发判定（支持纯图片消息）。

    返回触发文本（可为空字符串 = 纯图片触发的有效输入）；不触发返回 None。
    """
    scene = str(getattr(ctx, "scene", "") or "")
    msg = str(raw_text or "").strip()

    if scene == "group":
        segment_msg = _render_group_ai_chat_message(evt)
        if segment_msg is not None:
            msg = segment_msg
        nick_aliases = [x for x in {str(bot_nick or "").strip(), "Cooper_bot", "Cooepr_bot"} if x]
        has_nick_mention = False
        for nick in nick_aliases:
            pat = rf"[@\uFF20]\s*{re.escape(nick)}"
            if re.search(pat, msg, flags=re.IGNORECASE):
                has_nick_mention = True
                msg = re.sub(pat, "", msg, flags=re.IGNORECASE).strip()
        msg = re.sub(r"(?i)\[CQ:at,[^\]]+\]", "", msg).strip()
        if not (_evt_mentions_me(evt) or has_nick_mention):
            return None
        # @机器人 + 纯图片时返回空字符串是有效触发
        return msg
    if scene.startswith("private"):
        if has_visual:
            return msg
        if not msg or msg.startswith(("/", "／")):
            return None
        return msg
    return None


def _split_ai_chat_backend(ai_input: str) -> Tuple[str, str]:
    text = str(ai_input or "").strip()
    if not text:
        return "default", ""
    low = text.lower()
    if low.startswith("antigravity"):
        return "gemini", text[11:].strip()
    if low.startswith("gemini"):
        return "gemini", text[6:].strip()
    if low.startswith("claude"):
        return "claude", text[6:].strip()
    if text[:1] in {"g", "G"}:
        return "gemini", text[1:].strip()
    if text[:1] in {"c", "C"}:
        return "claude", text[1:].strip()
    return "default", text


def _is_antigravity_busy_error(err: object) -> bool:
    low = str(err or "").lower()
    return (
        "no capacity available" in low
        or "servers are experiencing high traffic" in low
        or "high traffic right now" in low
        or "resource exhausted" in low
        or "service busy" in low
        or "unavailable (code 503)" in low
    )


def _antigravity_busy_reply(backend: str) -> str:
    model = "Claude Opus 4.6" if backend == "claude" else "Gemini 3.1 Pro"
    return f"antigravity 的 {model} 当前服务繁忙，上游暂时没有可用容量，请稍后再试。"


def _ai_chat_allows_full_cli(ctx) -> bool:
    try:
        return int(getattr(ctx, "level", 0) or 0) >= 3
    except Exception:
        return False


def _ai_chat_session_key(ctx) -> Optional[str]:
    scene = str(getattr(ctx, "scene", "") or "")
    if scene == "group":
        gid = getattr(ctx, "group_id", None)
        if gid is None:
            return None
        try:
            return f"group:{int(gid)}"
        except Exception:
            return None
    if scene.startswith("private"):
        uid = getattr(ctx, "user_id", None)
        if uid is None:
            return None
        try:
            return f"private:{int(uid)}"
        except Exception:
            return None
    return None


def _compact_ai_sender_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _format_group_ai_user_message(ctx, message_text: str) -> str:
    msg = str(message_text or "").strip()
    if not msg:
        return msg
    scene = str(getattr(ctx, "scene", "") or "").strip().lower()
    if scene != "group":
        return msg
    try:
        uid = int(getattr(ctx, "user_id"))
    except Exception:
        return msg

    nickname = _compact_ai_sender_text(getattr(ctx, "nickname", ""))
    card = _compact_ai_sender_text(getattr(ctx, "card", ""))
    lines = [
        f"发言人QQ:{uid}",
        f"发言人昵称:{nickname or uid}",
    ]
    if card and card != nickname:
        lines.append(f"发言人群名片:{card}")
    gid = getattr(ctx, "group_id", None)
    if gid is not None:
        lines.append(f"群号:{gid}")
    lines.append(msg)
    return "\n".join(lines)


def _augment_ai_input_with_sender(ctx, ai_input: str) -> str:
    msg = str(ai_input or "").strip()
    if not msg:
        return msg
    return _format_group_ai_user_message(ctx, msg)


def _remember_non_ai_chat_message(
    ctx,
    text: str,
    logsvc: LogService,
    aisvc: Optional["AIService"] = None,
    *,
    msg_id: str = "",
    vision_slots: Optional[list] = None,
) -> None:
    if aisvc is None:
        return
    remember_fn = getattr(aisvc, "remember_user_message", None)
    if not callable(remember_fn):
        return
    session_key = _ai_chat_session_key(ctx)
    if not session_key:
        return
    try:
        remember_fn(
            session_key,
            _format_group_ai_user_message(ctx, text),
            msg_id=msg_id,
            vision_slots=vision_slots,
        )
    except Exception as e:
        logsvc.log.warning(f"AI chat context non-aichat write failed: session={session_key[:80]} err={e}")


def _ai_chat_session_key_for_target(scene: str, group_id: Optional[int], user_id: Optional[int]) -> Optional[str]:
    s = str(scene or "").strip().lower()
    if s == "group":
        if group_id is None:
            return None
        try:
            return f"group:{int(group_id)}"
        except Exception:
            return None
    if s.startswith("private"):
        if user_id is None:
            return None
        try:
            return f"private:{int(user_id)}"
        except Exception:
            return None
    return None


def _remember_bot_reply_message(
    ctx,
    text: str,
    logsvc: LogService,
    send_scene: str,
    send_group_id: Optional[int],
    send_user_id: Optional[int],
) -> None:
    aisvc = getattr(ctx, "_ai_chat_context_aisvc", None)
    if aisvc is None:
        return
    remember_fn = getattr(aisvc, "remember_assistant_message", None)
    if not callable(remember_fn):
        return
    session_key = _ai_chat_session_key_for_target(send_scene, send_group_id, send_user_id)
    if not session_key:
        return
    try:
        remember_fn(session_key, text)
    except Exception as e:
        logsvc.log.warning(f"AI chat context bot-reply write failed: session={session_key[:80]} err={e}")


def _remember_notice_digest_context(
    ctx,
    source: str,
    out: str,
    logsvc: LogService,
    aisvc: Optional["AIService"] = None,
) -> bool:
    if aisvc is None:
        return False
    session_key = _ai_chat_session_key(ctx)
    if not session_key:
        return False

    try:
        remember_user_fn = getattr(aisvc, "remember_user_message", None)
        if callable(remember_user_fn):
            remember_user_fn(session_key, _format_group_ai_user_message(ctx, str(source or "")))
    except Exception as e:
        logsvc.log.warning(f"AI chat context notice-source write failed: session={session_key[:80]} err={e}")

    assistant_saved = False
    try:
        remember_assistant_fn = getattr(aisvc, "remember_assistant_message", None)
        if callable(remember_assistant_fn):
            remember_assistant_fn(session_key, str(out or ""))
            assistant_saved = True
    except Exception as e:
        logsvc.log.warning(f"AI chat context notice-reply write failed: session={session_key[:80]} err={e}")
    return assistant_saved


def _is_notice_file_name(name: str) -> bool:
    return Path(str(name or "").strip()).suffix.lower() in _GROUP_NOTICE_FILE_SUFFIXES


def _clean_url_candidate(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    # Remove trailing punctuation that often appears in chat text.
    s = s.rstrip(".,!?;:)]}>\u3002\uff0c\uff1b\uff1a\uff01\uff1f\u3011\u300d\u300f\uff09")
    if not (s.startswith("http://") or s.startswith("https://")):
        return ""
    return s


def _extract_urls_from_evt(evt: dict) -> List[str]:
    urls: List[str] = []
    seen = set()

    def _push(text: str) -> None:
        for m in _URL_RE.findall(str(text or "")):
            u = _clean_url_candidate(m)
            if (not u) or (u in seen):
                continue
            seen.add(u)
            urls.append(u)

    msg = evt.get("message")
    if isinstance(msg, list):
        for seg in msg:
            if not isinstance(seg, dict):
                continue
            tp = str(seg.get("type") or "").lower()
            data = seg.get("data") or {}
            if tp == "text":
                _push(data.get("text") or "")
            elif tp in ("share", "link"):
                u = _clean_url_candidate(str(data.get("url") or ""))
                if u and u not in seen:
                    seen.add(u)
                    urls.append(u)
    else:
        _push(str(evt.get("raw_message") or ""))
        _push(str(msg or ""))

    return urls


async def _resolve_src_by_get_file_for_notice(
    api,
    fid: str,
    big: bool = False,
    group_id: Optional[int] = None,
    busid: Optional[str] = None,
) -> str:
    timeout = 180.0 if big else 60.0

    async def _extract_from_resp(resp: Optional[dict]) -> str:
        if not resp or resp.get("status") != "ok":
            return ""
        data = resp.get("data")
        if isinstance(data, str):
            return data.strip()
        data = data or {}
        return str(
            data.get("url")
            or data.get("download_url")
            or data.get("file")
            or data.get("file_path")
            or data.get("path")
            or ""
        ).strip()

    # 群文件优先尝试 get_group_file_url（NapCat/OneBot 常见接口）。
    if group_id is not None and fid:
        params = {"group_id": int(group_id), "file_id": str(fid)}
        if str(busid or "").strip().isdigit():
            params["busid"] = int(str(busid).strip())
        resp = await api.call("get_group_file_url", params, timeout=timeout)
        src = await _extract_from_resp(resp)
        if src:
            return src

        # 兼容部分实现：不带 busid 再试一次。
        if "busid" in params:
            params2 = {"group_id": int(group_id), "file_id": str(fid)}
            resp2 = await api.call("get_group_file_url", params2, timeout=timeout)
            src2 = await _extract_from_resp(resp2)
            if src2:
                return src2

    # 兜底旧路径。
    resp3 = await api.get_file(
        str(fid),
        timeout=timeout,
        retries=2,
        retry_delay=2.0,
    )
    return await _extract_from_resp(resp3)


async def _extract_notice_text_from_group_file(
    api,
    ctx,
    handin: HandinService,
    aisvc: "AIService",
    f: dict,
    logsvc: Optional[LogService] = None,
    max_chars: int = 4000,
    max_pages: int = 6,
) -> str:
    fname = (f.get("name") or "file").strip()
    file_id = (f.get("file_id") or "").strip()
    url = (f.get("url") or "").strip()
    busid = (f.get("busid") or "").strip()
    size_raw = (f.get("size") or "").strip()
    expected_size: Optional[int] = None
    try:
        expected_size = int(size_raw) if size_raw else None
    except Exception:
        expected_size = None

    big = _is_large(expected_size)
    if logsvc is not None:
        pages_txt = "all" if int(max_pages) <= 0 else str(int(max_pages))
        logsvc.log.info(
            f"群通知解析：开始提取文件 file={fname} max_chars={int(max_chars)} max_pages={pages_txt}"
        )
    src = url
    if (not src) and file_id:
        src = await _resolve_src_by_get_file_for_notice(
            api,
            file_id,
            big=big,
            group_id=ctx.group_id,
            busid=busid,
        )
    if not src:
        if logsvc is not None:
            logsvc.log.warning(f"群通知解析：文件源地址解析失败 file={fname} file_id={file_id}")
        return ""

    dl_timeout = 600.0 if big else 180.0
    ok, _msg, p = await asyncio.to_thread(
        handin.download_to_inbox,
        ctx.user_id,
        fname,
        src,
        expected_size,
        dl_timeout,
    )

    if (not ok) and file_id and src == url:
        src2 = await _resolve_src_by_get_file_for_notice(
            api,
            file_id,
            big=big,
            group_id=ctx.group_id,
            busid=busid,
        )
        if src2 and src2 != src:
            ok, _msg, p = await asyncio.to_thread(
                handin.download_to_inbox,
                ctx.user_id,
                fname,
                src2,
                expected_size,
                dl_timeout,
            )

    if (not ok) or (not p):
        if logsvc is not None:
            logsvc.log.warning(f"群通知解析：文件下载失败 file={fname}: {_msg}")
        return ""

    try:
        out = await aisvc.extract_notice_file_head(Path(p), max_chars=int(max_chars), max_pages=int(max_pages))
        if (not out) and (logsvc is not None):
            logsvc.log.warning(f"群通知解析：文件提取结果为空 file={fname}")
        elif logsvc is not None:
            logsvc.log.info(f"群通知解析：文件提取完成 file={fname} chars={len(out)}")
        return out
    finally:
        try:
            Path(p).unlink(missing_ok=True)
        except Exception:
            pass


async def _run_group_notice_digest(
    api,
    ctx,
    evt: dict,
    text: str,
    logsvc: LogService,
    state: BotState,
    handin: HandinService,
    aisvc: "AIService",
):
    if ctx.scene != "group" or ctx.group_id is None:
        return
    if int(evt.get("self_id") or 0) == int(ctx.user_id):
        return

    files = [x for x in get_files(evt) if _is_notice_file_name(x.get("name") or "")]
    urls = _extract_urls_from_evt(evt)
    # 同一条文件消息里常带一个下载 URL；若同时处理会造成双发。
    if files:
        urls = []
    if (not files) and (not urls):
        msg = evt.get("message")
        if isinstance(msg, list):
            seg_types = [
                str(seg.get("type") or "").lower()
                for seg in msg
                if isinstance(seg, dict)
            ]
            unsupported = sorted({tp for tp in seg_types if tp in ("json", "xml")})
            if unsupported:
                logsvc.log.info(
                    f"群通知解析：未从当前消息中提取到链接，不支持的消息段类型={','.join(unsupported)}"
                )
        return

    # A command like /find URL should not trigger auto-digest unless a file is attached.
    t = (text or "").strip()
    if (not files) and urls and (t.startswith("/") or t.startswith("／")):
        logsvc.log.info("群通知解析：消息看起来像命令，跳过链接自动解析")
        return
    if (not files) and (t.startswith("/") or t.startswith("／")):
        return

    candidates: List[Tuple[str, object]] = []
    for item in files:
        candidates.append(("file", item))
    for item in urls:
        candidates.append(("url", item))
    if not candidates:
        return
    logsvc.log.info(
        f"群通知解析：发现候选内容 files={len(files)} urls={len(urls)} total={len(candidates)}"
    )

    for kind, payload in candidates[:_GROUP_NOTICE_MAX_CANDIDATES]:
        try:
            source = ""
            dedup_key = ""
            preview = ""
            debug_target = ""
            if kind == "file":
                f = payload if isinstance(payload, dict) else {}
                fname = str(f.get("name") or "未命名文件").strip() or "未命名文件"
                fsize = str(f.get("size") or "").strip()
                fid = str(f.get("file_id") or "").strip()
                busid = str(f.get("busid") or "").strip()
                debug_target = fname
                source = f"群文件：{fname}"
                # 跨 message/file 与 notice/group_upload 去重：优先稳定字段（文件名+大小+上传者+群）
                name_norm = re.sub(r"\s+", "", fname).casefold()
                dedup_key = f"g{ctx.group_id}:file:{name_norm}:{fsize}:{ctx.user_id}"
            else:
                u = str(payload or "").strip()
                debug_target = u
                source = f"群链接：{u}"
                dedup_key = f"g{ctx.group_id}:url:{u.casefold()}"

            logsvc.log.info(f"群通知解析：开始处理 kind={kind} target={debug_target[:160]}")
            if dedup_key and (not _claim_group_notice_key(state, dedup_key)):
                logsvc.log.info(f"群通知解析：命中去重，跳过处理 kind={kind} target={debug_target[:160]}")
                continue

            # Step 1: 小窗口预判是否为“需要动作的通知”
            if kind == "file":
                f = payload if isinstance(payload, dict) else {}
                preview = await _extract_notice_text_from_group_file(
                    api,
                    ctx,
                    handin,
                    aisvc,
                    f,
                    logsvc=logsvc,
                    max_chars=6000,
                    max_pages=8,
                )
            else:
                u = str(payload or "").strip()
                preview = await aisvc.extract_notice_url_head(u, max_chars=6000)

            preview = str(preview or "").strip()
            if not preview:
                logsvc.log.info(f"群通知解析：预览内容为空 kind={kind} target={debug_target[:160]}")
                continue
            logsvc.log.info(
                f"群通知解析：预览内容已就绪 kind={kind} chars={len(preview)} target={debug_target[:160]}"
            )

            is_notice = await aisvc.classify_notice(source, preview, group_id=ctx.group_id, kind=kind)
            if not is_notice:
                logsvc.log.info(f"群通知解析：分类结果为不回复 kind={kind} target={debug_target[:160]}")
                continue
            logsvc.log.info(f"群通知解析：分类结果为需要回复 kind={kind} target={debug_target[:160]}")

            # Step 2: 对通知做更完整文本提取，再生成更详细省流
            material = preview
            if kind == "file":
                f = payload if isinstance(payload, dict) else {}
                full_text = await _extract_notice_text_from_group_file(
                    api,
                    ctx,
                    handin,
                    aisvc,
                    f,
                    logsvc=logsvc,
                    max_chars=50000,
                    max_pages=0,  # 0 => all pages
                )
                if full_text:
                    material = full_text
            else:
                u = str(payload or "").strip()
                full_text = await aisvc.extract_notice_url_head(u, max_chars=25000)
                if full_text:
                    material = full_text
            logsvc.log.info(
                f"群通知解析：开始生成省流内容 kind={kind} chars={len(str(material or ''))} target={debug_target[:160]}"
            )

            out = await aisvc.reason_notice(source, material, group_id=ctx.group_id, kind=kind)
            out = aisvc.sanitize_reasoner_output(out)
            if (not out) or aisvc.is_notice_silent(out):
                logsvc.log.info(f"群通知解析：生成结果为静默或空内容 kind={kind} target={debug_target[:160]}")
                continue

            notice_ctx_saved = _remember_notice_digest_context(ctx, source, out, logsvc, aisvc)
            if notice_ctx_saved:
                try:
                    setattr(ctx, "_skip_reply_context_once", True)
                except Exception:
                    pass
            await reply(api, ctx, out, logsvc)
            logsvc.log.info(f"群通知解析：已发送回复 kind={kind} target={debug_target[:160]}")
            return
        except Exception as e:
            logsvc.log.warning(f"group notice digest failed: {e}")
            continue


def _schedule_group_notice_digest(
    api,
    ctx,
    evt: dict,
    text: str,
    logsvc: LogService,
    state: BotState,
    handin: HandinService,
    aisvc: Optional["AIService"],
) -> None:
    if aisvc is None:
        return
    if not getattr(aisvc, "notice_ready", False):
        return
    try:
        setattr(ctx, "_ai_chat_context_aisvc", aisvc)
    except Exception:
        pass
    task = asyncio.create_task(_run_group_notice_digest(api, ctx, evt, text, logsvc, state, handin, aisvc))

    def _done(t: asyncio.Task) -> None:
        try:
            t.result()
        except Exception as e:
            logsvc.log.warning(f"group notice digest task exception: {e}")

    task.add_done_callback(_done)


def _parse_indices(arg: str) -> List[int]:
    """
    支持：
    - 普通数字：1 2 3 / 1,2,3 / 1，2，3
    - 全角数字：１ ２ ３
    - 部分“看起来像数字”的字符：① ② ③ / ¹ ² ³ 等（QQ 有时会发这种）
    """
    if not arg:
        return []
    s = str(arg).strip()
    # 1) 全角数字 -> 半角
    s = s.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    out: List[int] = []
    # 2) 优先提取常规连续数字
    nums = re.findall(r"[0-9]+", s)
    for n in nums:
        try:
            out.append(int(n))
        except Exception:
            pass
    # 3) 如果没提取到，尝试把“数字样字符”转成数值（①、¹ 之类）
    if not out:
        for ch in s:
            try:
                out.append(int(unicodedata.digit(ch)))
                continue
            except Exception:
                pass
            try:
                v = unicodedata.numeric(ch)
                if float(v).is_integer():
                    out.append(int(v))
            except Exception:
                pass
    # 去重但保序
    seen = set()
    uniq: List[int] = []
    for x in out:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq
def _sanitize_ascii_filename(name: str) -> str:
    """把文件名转换成 ASCII 安全形式（保留后缀）。"""
    p = Path(name)
    stem = p.stem
    suf = p.suffix
    stem2 = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._-")
    if not stem2:
        stem2 = "file"
    # 避免过长
    stem2 = stem2[:60]
    return f"{stem2}{suf}"
def _safe_zip_label(raw: str, default: str = "files") -> str:
    safe = re.sub(r'[<>:"/\\|?*]+', "_", (raw or "").strip()).strip(" .")
    safe = re.sub(r"\s+", "_", safe)
    return safe or default
def _sanitize_submitter_name(raw: str) -> str:
    s = (raw or "").strip()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "", s)
    s = s.strip("._-")
    return s[:20]
def _append_submitter_to_filename(filename: str, submitter_name: str) -> str:
    p = Path(filename or "file")
    suf = p.suffix
    stem = p.stem if suf else p.name
    stem = stem.rstrip(" -_")
    new_name = f"{stem}-{submitter_name}{suf}"
    new_name = re.sub(r'[<>:"/\\|?*]+', "_", new_name).strip(" .")
    return new_name or p.name
def _rename_pending_file_with_submitter(item: dict, submitter_name: str) -> Tuple[bool, str]:
    src = Path(str(item.get("path") or ""))
    if (not src.exists()) or (not src.is_file()):
        return False, "临时文件不存在（可能已过期/被清理）。"
    old_display_name = str(item.get("name") or src.name or "file")
    new_name = _append_submitter_to_filename(old_display_name, submitter_name)
    dst = src.with_name(new_name)
    if str(dst) != str(src) and dst.exists():
        stem = dst.stem
        suf = dst.suffix
        for i in range(2, 1000):
            alt = src.with_name(f"{stem}_{i}{suf}")
            if not alt.exists():
                dst = alt
                break
    try:
        if str(dst) != str(src):
            src.replace(dst)
        item["path"] = str(dst)
        item["name"] = dst.name
        return True, dst.name
    except Exception as e:
        return False, f"重命名失败：{e}"
def _cleanup_temp_files(paths: List[Path], logsvc: Optional[LogService] = None) -> None:
    for p in paths:
        try:
            Path(p).unlink(missing_ok=True)
        except Exception as e:
            if logsvc is not None:
                logsvc.log.warning(f"cleanup temp file failed: path={p} err={e}")
def _zip_directory(src_dir: Path, out_zip: Path) -> Tuple[bool, str]:
    try:
        out_zip.parent.mkdir(parents=True, exist_ok=True)
        with open_fast_zip(out_zip) as zf:
            packed = 0
            for p in src_dir.rglob("*"):
                if not p.is_file():
                    continue
                rel = p.relative_to(src_dir).as_posix()
                zip_write_path(zf, p, arcname=f"{src_dir.name}/{rel}")
                packed += 1
            if packed <= 0:
                zf.writestr(f"{src_dir.name}/", "")
        return True, ""
    except Exception as e:
        return False, str(e)
def _zip_pending_files(items: List[dict], out_zip: Path, logsvc: Optional[LogService] = None) -> Tuple[bool, str, int, int]:
    """把待提交队列里的多个文件打成一个 zip。
    返回：(ok, msg, packed_count, missing_count)
    """
    try:
        out_zip.parent.mkdir(parents=True, exist_ok=True)
        packed = 0
        missing = 0
        name_count: Dict[str, int] = {}
        with open_fast_zip(out_zip) as zf:
            for idx, it in enumerate(items, 1):
                p = Path(str(it.get("path") or ""))
                if (not p.exists()) or (not p.is_file()):
                    missing += 1
                    continue
                arc0 = (str(it.get("name") or "").strip() or p.name or f"file_{idx}")
                arc = arc0
                name_count[arc0] = name_count.get(arc0, 0) + 1
                if name_count[arc0] > 1:
                    arc = f"{idx}_{arc0}"
                zip_write_path(zf, p, arcname=arc)
                packed += 1
        if packed <= 0:
            try:
                out_zip.unlink(missing_ok=True)
            except Exception as e:
                if logsvc is not None:
                    logsvc.log.warning(f"remove empty zip failed: path={out_zip} err={e}")
            return False, "打包失败：没有可用文件。", 0, missing
        return True, "", packed, missing
    except Exception as e:
        return False, f"打包失败：{e}", 0, 0
def _suggest_batch_zip_basename(items: List[dict], user_id: int) -> str:
    """根据文件名推断一个默认 zip 基名（不含 .zip）。"""
    nm = ""
    sid = ""
    for it in (items or []):
        raw_name = str(it.get("name") or "").strip()
        if (not nm) and raw_name:
            nm = extract_name_from_filename(raw_name)
        if (not sid) and raw_name:
            sid = extract_student_id(raw_name)
        if nm and sid:
            break
    base = f"{nm}-{sid}" if (nm and sid) else (sid or nm or f"handin_u{user_id}")
    return _safe_zip_label(base, default=f"handin_u{user_id}")[:60].strip("._-") or f"handin_u{user_id}"
def _stage_for_napcat(
    ctx,
    src: Path,
    display_name: Optional[str] = None,
    logsvc: Optional[LogService] = None,
) -> tuple[Optional[str], Optional[str], str]:
    """把要发送的文件复制到 NapCat 专用上传目录，再返回容器内路径。
    返回：(container_path, send_name, msg)
    - container_path: 例如 /data/upload_group_file/xxx （OneBotAPI 会自动转为 file:///）
    - send_name: 展示给 QQ 的文件名（可选择是否 ASCII 化）
    - msg: 失败原因/补充说明
    """
    try:
        mirror_dir: Optional[Path] = None
        if ctx.scene == "group":
            host_dir = UPLOAD_GROUP_HOST_DIR
            cont_dir = UPLOAD_GROUP_CONTAINER_DIR
            # 群里发送失败时会尝试“临时会话私聊”兜底，这里同步一份到私聊目录。
            mirror_dir = UPLOAD_PRIVATE_HOST_DIR
        else:
            host_dir = UPLOAD_PRIVATE_HOST_DIR
            cont_dir = UPLOAD_PRIVATE_CONTAINER_DIR
        host_dir.mkdir(parents=True, exist_ok=True)
        if mirror_dir is not None:
            mirror_dir.mkdir(parents=True, exist_ok=True)
        # 目标文件名（落地到 upload_* 目录里用 ASCII，避免容器侧解析/编码问题）
        safe_base = _sanitize_ascii_filename(src.name)
        suf = Path(safe_base).suffix or src.suffix
        stem = Path(safe_base).stem
        staged_name = f"{stem}_{uuid.uuid4().hex[:10]}{suf}"
        dst = host_dir / staged_name
        # 拷贝到 bind mount 目录（给 NapCat 容器读取）
        # 注意：Windows + Docker Desktop 的共享目录有时会有“同步延迟”，
        # 因此这里只负责把文件落盘；真正发送失败会在 _send_file 里自动重试。
        shutil.copy2(src, dst)
        # 群聊额外镜像到私聊目录（用于群失败后私聊兜底）。
        if mirror_dir is not None:
            try:
                shutil.copy2(src, mirror_dir / staged_name)
            except Exception as e:
                if logsvc is not None:
                    logsvc.log.warning(f"mirror staged file failed: src={src} mirror={mirror_dir / staged_name} err={e}")
        # 基本校验：避免拷贝出空文件（例如源文件被占用/权限问题）
        try:
            if dst.stat().st_size <= 0 and src.stat().st_size > 0:
                return None, None, "staging 失败：复制后文件大小为 0"
        except Exception as e:
            if logsvc is not None:
                logsvc.log.warning(f"validate staged file size failed: src={src} dst={dst} err={e}")
        # 展示名：默认使用原文件名；如上层指定 display_name，则以其为准
        send_name = (display_name or src.name)
        if SEND_FILENAME_ASCII_SAFE:
            send_name = _sanitize_ascii_filename(send_name)
        container_path = f"{cont_dir}/{staged_name}"
        return container_path, send_name, ""
    except Exception as e:
        return None, None, f"staging 失败：{e}"
async def _send_file(api, ctx, container_path: str, name: str):
    """发送文件。
    返回：(sent, detail)
    - sent: True / False / None（None=未确认回包）
    - detail: 失败原因或补充说明（供上层拼提示）
    """
    def _ok(resp: dict) -> bool:
        return bool(resp) and resp.get("status") == "ok" and int(resp.get("retcode", 0) or 0) == 0
    def _detail(resp: dict) -> str:
        if not resp:
            return ""
        rc = resp.get("retcode", "")
        msg = (resp.get("wording") or resp.get("message") or "").strip()
        if msg:
            return f"retcode={rc} {msg}"
        return f"retcode={rc}"
    def _is_rich_fail(s: str) -> bool:
        return "rich media transfer failed" in (s or "").lower()
    def _is_missing_file_fail(s: str) -> bool:
        s2 = (s or "").lower()
        return ("enoent" in s2) or ("no such file or directory" in s2)
    def _is_retryable_fail(s: str) -> bool:
        # ENOENT 在 Windows+Docker 挂载同步延迟时很常见，重试通常可恢复。
        return _is_rich_fail(s) or _is_missing_file_fail(s)
    async def _retry(loop_fn, first_detail: str) -> tuple[Optional[bool], str]:
        """仅在可重试错误时按 SEND_RETRY_DELAYS 重试。"""
        d = first_detail
        if not _is_retryable_fail(d):
            return False, d
        for delay in (SEND_RETRY_DELAYS or []):
            await asyncio.sleep(float(delay))
            resp = await loop_fn()
            if resp is None:
                # 未确认：可能已执行
                return None, ""
            if _ok(resp):
                return True, "（已自动重试后成功）"
            d = _detail(resp)
            if not _is_retryable_fail(d):
                break
        return False, d
    async def _try_group_send(use_name: str) -> tuple[Optional[bool], str]:
        resp = await api.upload_group_file(ctx.group_id, container_path, use_name)
        if resp is None:
            return None, ""
        if _ok(resp):
            return True, ""
        d = _detail(resp)
        return await _retry(lambda: api.upload_group_file(ctx.group_id, container_path, use_name), d)
    async def _try_private_send(use_name: str, group_id: Optional[int] = None, use_path: Optional[str] = None) -> tuple[Optional[bool], str]:
        path = use_path or container_path
        resp = await api.upload_private_file(ctx.user_id, path, use_name, group_id=group_id)
        if resp is None:
            return None, ""
        if _ok(resp):
            return True, ""
        d = _detail(resp)
        return await _retry(lambda: api.upload_private_file(ctx.user_id, path, use_name, group_id=group_id), d)
    # 1) 群聊优先走群文件
    if ctx.scene == "group" and ctx.group_id is not None:
        sent, detail = await _try_group_send(name)
        if sent is True:
            return True, detail
        if sent is None:
            return None, ""
        # 2) 群文件失败：尝试临时会话私聊兜底
        private_path = container_path
        if _is_missing_file_fail(detail) and container_path.startswith(UPLOAD_GROUP_CONTAINER_DIR.rstrip("/") + "/"):
            private_path = UPLOAD_PRIVATE_CONTAINER_DIR.rstrip("/") + "/" + Path(container_path).name
        sentp, detailp = await _try_private_send(name, group_id=ctx.group_id, use_path=private_path)
        if sentp is True:
            return True, "（群文件发送失败，已改为私聊发送）" + (detailp or "")
        if sentp is None:
            return None, "群文件失败，已尝试私聊发送"
        # 两种方式都失败
        extra = ""
        if _is_rich_fail(detail) or _is_rich_fail(detailp):
            extra = "（NapCat/QQ 返回 rich media transfer failed：常见原因是账号风控、群文件权限不足、群文件容量已满，或 Windows↔Docker 挂载同步延迟）"
        return False, f"{detail or '群文件失败'}；私聊也失败：{detailp}{extra}"
    # 私聊：直接发（自动重试）
    sent, detail = await _try_private_send(name)
    if sent is True:
        return True, detail
    if sent is None:
        return None, ""
    return False, detail
def _handin_tasks_list_text(tasks) -> str:
    lines = ["请选择提交任务："]
    for i, t in enumerate(tasks, 1):
        suffix = required_suffix_display(getattr(t, "required_suffix", ""))
        suffix_note = f"，格式 {suffix}" if suffix else ""
        lines.append(f"{i}. {t.name}（群 {t.group_id}，截止 {pretty_ts(t.deadline_ts)}{suffix_note}）")
    lines.append("回复数字选择；回复 0 取消（删除临时文件）。")
    return "\n".join(lines)


def _can_manage_handin_task(ctx, task) -> bool:
    try:
        if int(getattr(ctx, "level", 0) or 0) >= 3:
            return True
        return int(getattr(task, "creator_id", 0) or 0) == int(getattr(ctx, "user_id", 0) or 0)
    except Exception:
        return False


def _filter_manageable_handin_tasks(ctx, tasks):
    return [t for t in tasks if _can_manage_handin_task(ctx, t)]


def _looks_like_handin_suffix_token(token: str) -> bool:
    raw = str(token or "").strip()
    if not raw:
        return False
    if raw.startswith("."):
        return bool(re.fullmatch(r"\.[A-Za-z0-9]{1,8}", raw))
    return bool(re.fullmatch(r"[A-Za-z0-9]{1,8}", raw))


def _handin_suffix_examples() -> str:
    preferred = ["pdf", "docx", "doc", "pptx", "xlsx", "zip", "jpg", "png", "txt"]
    return "、".join(x for x in preferred if x in HANDIN_ALLOWED_REQUIRED_SUFFIXES)


def _parse_handin_create_parts(rest: str) -> Tuple[Optional[str], str, List[str], str]:
    parts = str(rest or "").split()
    usage = (
        "用法：/handin 任务名 [文件后缀] [月.日 时:分 ...] 月.日 时:分\n"
        "示例：/handin 作业1 pdf 1.22 18:30 1.23 20:00 1.24 23:59\n"
        "示例：/handin 作业1 1.22 18:30 1.23 20:00 1.24 23:59\n"
        f"（文件后缀可选，支持 {_handin_suffix_examples()} 等，不区分大小写；提醒时间可不填或填多个；最后一组时间为截止时间；任务名不能有空格）"
    )
    if len(parts) < 3:
        return None, "", [], usage

    time_start = 1
    required_suffix = ""
    if len(parts) >= 4 and ((len(parts) - 2) % 2 == 0):
        suffix = normalize_required_suffix(parts[1])
        if suffix:
            required_suffix = suffix
            time_start = 2
        elif _looks_like_handin_suffix_token(parts[1]):
            return None, "", [], f"文件后缀不支持：{parts[1]}\n目前支持：{_handin_suffix_examples()} 等。"

    if ((len(parts) - time_start) < 2) or ((len(parts) - time_start) % 2 != 0):
        return None, "", [], usage

    time_texts = [f"{parts[i]} {parts[i+1]}" for i in range(time_start, len(parts), 2)]
    return parts[0], required_suffix, time_texts, ""


def _pending_handin_source_names(item: dict) -> List[str]:
    names: List[str] = []
    raw_sources = item.get("source_names") if isinstance(item, dict) else None
    if isinstance(raw_sources, list):
        for name in raw_sources:
            s = str(name or "").strip()
            if s:
                names.append(s)
    if names:
        return names
    if isinstance(item, dict):
        name = str(item.get("name") or "").strip()
        if name:
            names.append(name)
        path_name = Path(str(item.get("path") or "")).name
        if path_name and path_name not in names:
            names.append(path_name)
    return names


def _pending_handin_matches_required_suffix(item: dict, required_suffix: str) -> bool:
    suffix = normalize_required_suffix(required_suffix)
    if not suffix:
        return True
    return any(file_matches_required_suffix(name, suffix) for name in _pending_handin_source_names(item))


def _find_pending_duplicate_by_hash(items: List[dict], file_sha: str) -> str:
    sha = str(file_sha or "").strip().lower()
    if not sha:
        return ""
    for it in items or []:
        if not isinstance(it, dict):
            continue
        old_sha = str(it.get("sha256") or "").strip().lower()
        if not old_sha:
            p = Path(str(it.get("path") or ""))
            if p.exists() and p.is_file():
                old_sha = HandinService.file_sha256(p)
                it["sha256"] = old_sha
        if old_sha and old_sha == sha:
            return str(it.get("name") or Path(str(it.get("path") or "")).name or "已在队列中的文件")
    return ""


def _pending_duplicate_skip_message(download_msg: str, duplicate_name: str, q_len: int, state: BotState, user_id: int) -> str:
    lines = [
        download_msg,
        f"检测到该文件内容与当前待提交文件「{duplicate_name}」完全一致，已自动跳过。",
    ]
    if q_len == 1:
        lines.append("当前仍只有 1 个待提交文件，不进入多文件提交模式。")
    if state.pending_handin_zip_name.get(user_id):
        lines.append(f"当前打包队列共 {q_len} 个文件，请回复压缩包名称（无需加 .zip）。")
    elif state.pending_handin_wait_done.get(user_id):
        lines.append(f"当前打包队列共 {q_len} 个文件；发完后请回复 done。")
    elif state.pending_handin_name_input.get(user_id):
        lines.append("请继续回复提交者姓名（或回复 0 跳过）。")
    else:
        pend = state.pending_handin_choose.get(user_id)
        if isinstance(pend, dict) and pend.get("mode") == "submit":
            lines.append("请继续回复任务序号处理当前待提交文件。")
    return "\n".join(lines)


async def _handle_private_file(api, ctx, evt: dict, logsvc: LogService, state: BotState, handin: HandinService) -> bool:
    """处理私聊发文件：下载到 inbox 并提示选择任务。返回是否已处理（True=已回复）。"""
    files = get_files(evt)
    if not files:
        return False
    f0 = files[0]
    fname = (f0.get("name") or "file").strip()
    url = (f0.get("url") or "").strip()
    file_id = (f0.get("file_id") or "").strip()
    fsize = (f0.get("size") or "").strip()
    # 记录 IN（触发回复才会最终落盘）
    logsvc.log_in(ctx, f"[file] {fname}")
    if ctx.level < 1:
        await reply(api, ctx, "权限不足：你当前是 0 级（游客），不能提交。", logsvc)
        return True
    # 先用事件里的 url 尝试下载；失败则再尝试 get_file(file_id) 拿更“完整”的 url 重新下载
    expected_size = None
    try:
        expected_size = int(fsize) if fsize else None
    except Exception:
        expected_size = None
    # 大文件提示（接收提交）
    await _warn_large_if_needed(api, ctx, logsvc, fname, expected_size, mode="recv")
    # === 先准备下载来源：优先用事件 url；没有就先 get_file 拿 url/本地路径 ===
    src = url
    # 大文件：get_file 更久 + 下载更久
    big = _is_large(expected_size)
    get_file_timeout = 180.0 if big else 60.0
    dl_timeout = 600.0 if big else 180.0  # 允许大文件更久
    async def _resolve_src_by_get_file(fid: str) -> str:
        resp = await api.get_file(fid, timeout=get_file_timeout, retries=2, retry_delay=2.0)
        if not resp or resp.get("status") != "ok":
            return ""
        data = resp.get("data") or {}
        # NapCat / OneBot 实现可能返回 url，也可能返回本地路径字段
        return str(
            data.get("url")
            or data.get("download_url")
            or data.get("file")
            or data.get("file_path")
            or data.get("path")
            or ""
        ).strip()
    if (not src) and file_id:
        # 事件没 url：先 get_file
        src = await _resolve_src_by_get_file(file_id)
    if not src:
        await reply(api, ctx,
                    "获取下载链接失败：事件未提供 url，且 get_file 未返回 url/本地路径（大文件可能需要更久，可稍后重试）。",
                    logsvc)
        return True
    # === 真正下载：放到线程里，避免 100MB+ 阻塞事件循环 ===
    ok, msg, p = await asyncio.to_thread(
        handin.download_to_inbox,
        ctx.user_id,
        fname,
        src,
        expected_size,
        dl_timeout,
    )
    # 如果下载失败且还没用过 get_file 的结果，再补一次（用于：事件 url 是短链/过期）
    if (not ok) and file_id and src == url:
        src2 = await _resolve_src_by_get_file(file_id)
        if src2 and src2 != src:
            ok, msg, p = await asyncio.to_thread(
                handin.download_to_inbox,
                ctx.user_id,
                fname,
                src2,
                expected_size,
                dl_timeout,
            )
    if not ok or not p:
        # 这里的失败通常是 QQ 下载链接无法直连（fname 空/链接过期/网络拦截等）
        await reply(api, ctx, msg, logsvc)
        return True
    # 入队
    q = state.pending_handin_files.get(ctx.user_id) or []
    try:
        file_sha = await asyncio.to_thread(HandinService.file_sha256, Path(p))
        duplicate_name = await asyncio.to_thread(_find_pending_duplicate_by_hash, q, file_sha)
    except Exception as e:
        try:
            Path(p).unlink(missing_ok=True)
        except Exception as e2:
            logsvc.log.warning(f"cleanup hash-failed handin file failed: user={ctx.user_id} err={e2}")
        await reply(api, ctx, f"{msg}\n文件校验失败，本次提交已终止：{e}", logsvc)
        return True
    state.pending_handin_files[ctx.user_id] = q
    if duplicate_name:
        try:
            Path(p).unlink(missing_ok=True)
        except Exception as e:
            logsvc.log.warning(f"cleanup duplicate pending handin file failed: user={ctx.user_id} err={e}")
        await reply(
            api,
            ctx,
            _pending_duplicate_skip_message(msg, duplicate_name, len(q), state, ctx.user_id),
            logsvc,
        )
        return True
    q.append({"path": str(p), "name": fname, "sha256": file_sha, "ts": time.time()})
    state.pending_handin_files[ctx.user_id] = q
    # 已进入“等待 zip 名称”阶段时，新文件继续加入队列并保持等待命名
    if state.pending_handin_zip_name.get(ctx.user_id):
        await reply(
            api,
            ctx,
            f"{msg}\n已加入打包队列，当前共 {len(q)} 个文件。\n请回复压缩包名称（无需加 .zip）。",
            logsvc,
        )
        return True
    # 正在等待“补充姓名”时，如果继续发了第 2 个文件，自动切换为多文件 done 流程
    if state.pending_handin_name_input.get(ctx.user_id):
        if len(q) >= 2:
            state.pending_handin_name_input.pop(ctx.user_id, None)
            tasks = handin.list_active_tasks()
            if not tasks:
                state.pending_handin_choose.pop(ctx.user_id, None)
                await reply(api, ctx, f"{msg}\n当前没有正在进行的提交任务。", logsvc)
                return True
            state.pending_handin_wait_done[ctx.user_id] = {"ts": time.time()}
            state.pending_handin_zip_name.pop(ctx.user_id, None)
            _set_pending_handin_submit_choice(api, ctx, logsvc, state, [t.task_id for t in tasks])
            await reply(
                api,
                ctx,
                f"{msg}\n检测到你在连续发送多个文件：当前共 {len(q)} 个。\n请把文件发完后回复 done，我会先询问压缩包名称，再打包并让你选择归档任务。",
                logsvc,
            )
        else:
            await reply(api, ctx, f"{msg}\n请先回复提交者姓名（或回复 0 跳过）后，再选择归档任务。", logsvc)
        return True
    # 若已有待选择状态，且又收到了新文件：进入“等待 done 再批量打包”模式
    pend = state.pending_handin_choose.get(ctx.user_id)
    if pend and pend.get("mode") == "submit":
        if len(q) >= 2:
            state.pending_handin_wait_done[ctx.user_id] = {"ts": time.time()}
            state.pending_handin_zip_name.pop(ctx.user_id, None)
            await reply(
                api,
                ctx,
                f"{msg}\n检测到你在连续发送多个文件：当前共 {len(q)} 个。\n请把文件发完后回复 done，我会先询问压缩包名称，再打包并让你选择归档任务。",
                logsvc,
            )
        else:
            await reply(api, ctx, f"{msg}\n你还有待分配的提交文件，请先回复数字处理上一份（回复 0 取消上一份）。", logsvc)
        return True
    tasks = handin.list_active_tasks()
    if not tasks:
        await reply(api, ctx, f"{msg}\n当前没有正在进行的提交任务。", logsvc)
        return True
    # 新一轮提交流程，清掉旧的 done 等待状态
    state.pending_handin_wait_done.pop(ctx.user_id, None)
    state.pending_handin_zip_name.pop(ctx.user_id, None)
    state.pending_handin_name_input.pop(ctx.user_id, None)
    # 单文件：优先检测文件名里是否已有名册姓名
    if len(q) == 1:
        roster_name = handin.find_roster_name_in_filename(fname)
        if not roster_name:
            state.pending_handin_name_input[ctx.user_id] = {"ts": time.time()}
            state.pending_handin_choose.pop(ctx.user_id, None)
            lines = [
                msg,
                "检测到你发送了文件提交。",
                "未在文件名中识别到姓名。",
                "请回复提交者姓名（若不需要姓名信息或是小组作业，请回复 0 跳过）。",
            ]
            await reply(api, ctx, "\n".join(lines), logsvc)
            return True
        lines = [msg, f"已识别到姓名：{roster_name}。", _handin_tasks_list_text(tasks)]
        await reply(api, ctx, "\n".join(lines), logsvc)
        _set_pending_handin_submit_choice(api, ctx, logsvc, state, [t.task_id for t in tasks])
        return True
    # 多文件：仍按原有任务选择流程（若继续发送会自动转 done 打包）
    lines = [msg, "检测到你发送了文件提交。", _handin_tasks_list_text(tasks)]
    await reply(api, ctx, "\n".join(lines), logsvc)
    _set_pending_handin_submit_choice(api, ctx, logsvc, state, [t.task_id for t in tasks])
    return True
async def _handle_private_overwrite_yesno(api, ctx, text: str, logsvc: LogService, state: BotState, handin: HandinService) -> bool:
    """处理提交文件同名覆盖确认（Y/N）。返回是否已处理（True=已回复）。"""
    pend = state.pending_handin_overwrite.get(ctx.user_id)
    if not pend:
        return False
    # 记录 IN（触发回复）
    logsvc.log_in(ctx, (text or "").strip())
    ans = (text or "").strip().lower()
    if ans not in ("y", "yes", "n", "no"):
        await reply(api, ctx, "请输入 Y 或 N（不区分大小写）。", logsvc)
        return True
    # 取队首文件（该文件尚未移动）
    q = state.pending_handin_files.get(ctx.user_id) or []
    if not q:
        state.pending_handin_overwrite.pop(ctx.user_id, None)
        state.pending_handin_wait_done.pop(ctx.user_id, None)
        state.pending_handin_zip_name.pop(ctx.user_id, None)
        state.pending_handin_name_input.pop(ctx.user_id, None)
        await reply(api, ctx, "没有待处理的提交文件了。", logsvc)
        return True
    # 找到对应的队首（通常就是 q[0]）
    item_idx = 0
    for i, it in enumerate(q):
        if str(it.get("path")) == str(pend.get("path")):
            item_idx = i
            break
    item = q[item_idx]
    tid = pend.get("task_id")
    task = handin._tasks.get(tid)
    if not task or not task.is_active():
        # 任务不可用，丢弃该文件
        try:
            Path(item.get("path")).unlink(missing_ok=True)
        except Exception as e:
            logsvc.log.warning(f"cleanup stale pending file failed: user={ctx.user_id} item={item} err={e}")
        q.pop(item_idx)
        state.pending_handin_files[ctx.user_id] = q
        state.pending_handin_overwrite.pop(ctx.user_id, None)
        state.pending_handin_name_input.pop(ctx.user_id, None)
        await reply(api, ctx, "任务不存在或已结束，已丢弃该文件。请重新发送文件。", logsvc)
        return True
    if ans in ("n", "no"):
        # 不覆盖：删除临时文件
        try:
            Path(item.get("path")).unlink(missing_ok=True)
        except Exception as e:
            logsvc.log.warning(f"cleanup non-overwrite file failed: user={ctx.user_id} item={item} err={e}")
        q.pop(item_idx)
        state.pending_handin_files[ctx.user_id] = q
        state.pending_handin_overwrite.pop(ctx.user_id, None)
        await reply(api, ctx, "已取消覆盖，请修改文件名后重新发送。", logsvc)
    else:
        ok, msg2, dst, code = await asyncio.to_thread(
            handin.move_inbox_to_task,
            Path(item.get("path")),
            task,
            True,
        )
        if ok:
            q.pop(item_idx)
            state.pending_handin_files[ctx.user_id] = q
            state.pending_handin_overwrite.pop(ctx.user_id, None)
            name = Path(dst).name if dst else (item.get("name") or "")
            nm = extract_name_from_filename(name)
            sid = extract_student_id(name)
            warn = ""
            await reply(api, ctx, msg2 + warn, logsvc)
        elif code == "DUPLICATE":
            try:
                Path(item.get("path")).unlink(missing_ok=True)
            except Exception as e:
                logsvc.log.warning(f"cleanup duplicate overwrite file failed: user={ctx.user_id} item={item} err={e}")
            q.pop(item_idx)
            state.pending_handin_files[ctx.user_id] = q
            state.pending_handin_overwrite.pop(ctx.user_id, None)
            await reply(api, ctx, msg2, logsvc)
        else:
            # 覆盖失败：保留文件，让用户重新选择或取消
            state.pending_handin_overwrite.pop(ctx.user_id, None)
            await reply(api, ctx, f"{msg2}\n你可以重新回复任务序号，或回复 0 取消该文件。", logsvc)
    # 若还有文件继续分配
    if state.pending_handin_files.get(ctx.user_id):
        tasks = handin.list_active_tasks()
        if tasks:
            state.pending_handin_name_input.pop(ctx.user_id, None)
            _set_pending_handin_submit_choice(api, ctx, logsvc, state, [t.task_id for t in tasks])
            await reply(api, ctx, "你还有待分配的提交文件。\n" + _handin_tasks_list_text(tasks), logsvc)
    else:
        state.pending_handin_wait_done.pop(ctx.user_id, None)
        state.pending_handin_zip_name.pop(ctx.user_id, None)
        state.pending_handin_name_input.pop(ctx.user_id, None)
    return True
async def _handle_private_name_input(api, ctx, text: str, logsvc: LogService, state: BotState, handin: HandinService) -> bool:
    """处理“单文件未识别到姓名”时的姓名补充输入。"""
    pend = state.pending_handin_name_input.get(ctx.user_id)
    if not pend:
        return False
    t = (text or "").strip()
    if not t:
        return False
    logsvc.log_in(ctx, t)
    if state.pending_handin_overwrite.get(ctx.user_id):
        await reply(api, ctx, "你有一个待确认的覆盖操作，请先回复 Y/N。", logsvc)
        return True
    q = state.pending_handin_files.get(ctx.user_id) or []
    if not q:
        state.pending_handin_name_input.pop(ctx.user_id, None)
        state.pending_handin_wait_done.pop(ctx.user_id, None)
        state.pending_handin_zip_name.pop(ctx.user_id, None)
        state.pending_handin_choose.pop(ctx.user_id, None)
        await reply(api, ctx, "没有待处理的提交文件了。", logsvc)
        return True
    # 若等待姓名期间又变成多文件，转为 done 打包流程
    if len(q) >= 2:
        state.pending_handin_name_input.pop(ctx.user_id, None)
        tasks = handin.list_active_tasks()
        if not tasks:
            state.pending_handin_choose.pop(ctx.user_id, None)
            await reply(api, ctx, "当前没有正在进行的提交任务。", logsvc)
            return True
        state.pending_handin_wait_done[ctx.user_id] = {"ts": time.time()}
        state.pending_handin_zip_name.pop(ctx.user_id, None)
        _set_pending_handin_submit_choice(api, ctx, logsvc, state, [tt.task_id for tt in tasks])
        await reply(api, ctx, "检测到你在批量发送文件，请发完后回复 done，我会先让你命名 zip，再让你选择归档任务。", logsvc)
        return True
    skip_name = (t == "0")
    rename_note = ""
    if not skip_name:
        submitter_name = _sanitize_submitter_name(t.lstrip("/／").strip())
        if not submitter_name:
            await reply(api, ctx, "姓名格式不合法，请重新发送姓名；若不需要姓名信息或是小组作业，请回复 0 跳过。", logsvc)
            return True
        if re.fullmatch(r"\d+", submitter_name):
            await reply(api, ctx, "请发送姓名文本；若不需要姓名信息或是小组作业，请回复 0 跳过。", logsvc)
            return True
        ok_rename, msg_rename = _rename_pending_file_with_submitter(q[0], submitter_name)
        if not ok_rename:
            await reply(api, ctx, msg_rename, logsvc)
            return True
        rename_note = f"已补充姓名到文件名：{msg_rename}"
    state.pending_handin_files[ctx.user_id] = q
    state.pending_handin_name_input.pop(ctx.user_id, None)
    state.pending_handin_wait_done.pop(ctx.user_id, None)
    state.pending_handin_zip_name.pop(ctx.user_id, None)
    tasks = handin.list_active_tasks()
    if not tasks:
        state.pending_handin_choose.pop(ctx.user_id, None)
        if rename_note:
            await reply(api, ctx, rename_note + "\n当前没有正在进行的提交任务。", logsvc)
        else:
            await reply(api, ctx, "当前没有正在进行的提交任务。", logsvc)
        return True
    _set_pending_handin_submit_choice(api, ctx, logsvc, state, [tt.task_id for tt in tasks])
    lines = []
    if rename_note:
        lines.append(rename_note)
    lines.append(_handin_tasks_list_text(tasks))
    await reply(api, ctx, "\n".join(lines), logsvc)
    return True
async def _handle_private_number_choice(api, ctx, text: str, logsvc: LogService, state: BotState, handin: HandinService, filesvc: FileService) -> bool:
    """处理私聊数字选择。返回是否已处理（True=已回复）。"""
    t = (text or "").strip()
    if not re.fullmatch(r"\d{1,3}", t):
        return False
    pend = state.pending_handin_choose.get(ctx.user_id)
    if not pend:
        return False
    # 记录 IN（触发回复）
    logsvc.log_in(ctx, t)
    choice = int(t)
    mode = pend.get("mode")
    if mode == "submit":
        # 若正在等待覆盖确认，先处理 Y/N
        if state.pending_handin_overwrite.get(ctx.user_id):
            await reply(api, ctx, "你有一个待确认的覆盖操作，请先回复 Y/N。", logsvc)
            return True
        q = state.pending_handin_files.get(ctx.user_id) or []
        if not q:
            state.pending_handin_wait_done.pop(ctx.user_id, None)
            state.pending_handin_zip_name.pop(ctx.user_id, None)
            state.pending_handin_name_input.pop(ctx.user_id, None)
            state.pending_handin_choose.pop(ctx.user_id, None)
            await reply(api, ctx, "没有待分配的文件了。", logsvc)
            return True
        # 多文件收集中：先等 done，再统一打包并选择任务
        if state.pending_handin_wait_done.get(ctx.user_id):
            if choice == 0:
                for it in q:
                    try:
                        Path(str(it.get("path") or "")).unlink(missing_ok=True)
                    except Exception as e:
                        logsvc.log.warning(f"cleanup batch pending file failed: user={ctx.user_id} item={it} err={e}")
                state.pending_handin_files[ctx.user_id] = []
                state.pending_handin_wait_done.pop(ctx.user_id, None)
                state.pending_handin_zip_name.pop(ctx.user_id, None)
                state.pending_handin_name_input.pop(ctx.user_id, None)
                state.pending_handin_choose.pop(ctx.user_id, None)
                await reply(api, ctx, "已取消并删除全部临时文件。", logsvc)
            else:
                await reply(api, ctx, "检测到你在批量发送文件，请先发完后回复 done（随后会先让你命名 zip；回复 0 可取消全部临时文件）。", logsvc)
            return True
        if choice == 0:
            item = q.pop(0)
            state.pending_handin_files[ctx.user_id] = q
            try:
                Path(item["path"]).unlink(missing_ok=True)
            except Exception as e:
                logsvc.log.warning(f"cleanup dropped pending file failed: user={ctx.user_id} item={item} err={e}")
            state.pending_handin_wait_done.pop(ctx.user_id, None)
            state.pending_handin_zip_name.pop(ctx.user_id, None)
            state.pending_handin_name_input.pop(ctx.user_id, None)
            state.pending_handin_choose.pop(ctx.user_id, None)
            await reply(api, ctx, "已取消并删除临时文件。", logsvc)
            return True
        task_ids = pend.get("task_ids") or []
        if choice < 1 or choice > len(task_ids):
            await reply(api, ctx, "序号无效，请重新回复数字。", logsvc)
            return True
        tid = task_ids[choice - 1]
        task = handin._tasks.get(tid)  # internal lookup
        if not task or not task.is_active():
            await reply(api, ctx, "任务不存在或已结束，请重新发送文件。", logsvc)
            state.pending_handin_choose.pop(ctx.user_id, None)
            return True
        # 不先 pop，避免同名覆盖确认时丢失队列
        item = q[0]
        required_suffix = normalize_required_suffix(getattr(task, "required_suffix", ""))
        if required_suffix and not _pending_handin_matches_required_suffix(item, required_suffix):
            suffix_text = required_suffix_display(required_suffix)
            is_batch = bool(item.get("source_names"))
            _delete_pending_handin_files(state, ctx.user_id, logsvc)
            if is_batch:
                msg = f"任务「{task.name}」仅接收 {suffix_text} 文件；本次多文件提交中未找到 {suffix_text} 文件，已取消本轮提交。"
            else:
                msg = f"任务「{task.name}」仅接收 {suffix_text} 文件；本次提交的文件格式不符，已取消本轮提交。"
            await reply(api, ctx, msg + "\n请重新发送符合要求的文件。", logsvc)
            return True
        ok, msg2, dst, code = await asyncio.to_thread(
            handin.move_inbox_to_task,
            Path(item["path"]),
            task,
            False,
        )
        if (not ok) and code == "EXISTS":
            # 等待 Y/N
            state.pending_handin_overwrite[ctx.user_id] = {"task_id": tid, "path": str(item["path"]), "name": item.get("name") or "", "ts": time.time()}
            state.pending_handin_choose.pop(ctx.user_id, None)
            await reply(api, ctx, f"{msg2}\n是否覆盖？(Y/N)", logsvc)
            return True
        if (not ok) and code == "DUPLICATE":
            try:
                Path(item["path"]).unlink(missing_ok=True)
            except Exception as e:
                logsvc.log.warning(f"cleanup duplicate submitted file failed: user={ctx.user_id} item={item} err={e}")
            q.pop(0)
            state.pending_handin_files[ctx.user_id] = q
            await reply(api, ctx, msg2, logsvc)
            if q:
                tasks = handin.list_active_tasks()
                state.pending_handin_name_input.pop(ctx.user_id, None)
                _set_pending_handin_submit_choice(api, ctx, logsvc, state, [t.task_id for t in tasks])
                await reply(api, ctx, f"你还有 {len(q)} 份待分配文件。\n" + _handin_tasks_list_text(tasks), logsvc)
            else:
                state.pending_handin_wait_done.pop(ctx.user_id, None)
                state.pending_handin_zip_name.pop(ctx.user_id, None)
                state.pending_handin_name_input.pop(ctx.user_id, None)
                state.pending_handin_choose.pop(ctx.user_id, None)
            return True
        if not ok:
            # 归档失败：保留文件，让用户重新选择或取消
            await reply(api, ctx, msg2 + "\n请重新回复任务序号，或回复 0 取消该文件。", logsvc)
            return True
        # 成功归档：弹出队首
        q.pop(0)
        state.pending_handin_files[ctx.user_id] = q
        name = Path(dst).name if dst else (item.get("name") or "")
        nm = extract_name_from_filename(name)
        sid = extract_student_id(name)
        warn = ""
        if not nm or not sid:
            warn = "\n（提示：文件名最好包含姓名和学号，例如 张三-U2024xxxxxx.docx）"
        await reply(api, ctx, msg2 + warn, logsvc)
        # 还有文件继续分配
        if q:
            tasks = handin.list_active_tasks()
            state.pending_handin_name_input.pop(ctx.user_id, None)
            _set_pending_handin_submit_choice(api, ctx, logsvc, state, [t.task_id for t in tasks])
            await reply(api, ctx, f"你还有 {len(q)} 份待分配文件。\n" + _handin_tasks_list_text(tasks), logsvc)
        else:
            state.pending_handin_wait_done.pop(ctx.user_id, None)
            state.pending_handin_zip_name.pop(ctx.user_id, None)
            state.pending_handin_name_input.pop(ctx.user_id, None)
            state.pending_handin_choose.pop(ctx.user_id, None)
        return True
    if mode == "status":
        task_ids = pend.get("task_ids") or []
        if choice < 1 or choice > len(task_ids):
            await reply(api, ctx, "序号无效，请重新回复数字。", logsvc)
            return True
        tid = task_ids[choice - 1]
        task = handin._tasks.get(tid)
        if not task:
            await reply(api, ctx, "任务不存在。", logsvc)
            state.pending_handin_choose.pop(ctx.user_id, None)
            return True
        if not _can_manage_handin_task(ctx, task):
            await reply(api, ctx, "权限不足：只能操作你创建的任务（或联系管理员）。", logsvc)
            state.pending_handin_choose.pop(ctx.user_id, None)
            return True
        ok, msgx, missing, stats = handin.compute_missing(task)
        if ok:
            text2 = handin.format_missing_message(task, missing, stats, "📋 未提交名单")
        else:
            text2 = "📋 未提交名单\n" + msgx
        await reply(api, ctx, text2, logsvc)
        state.pending_handin_choose.pop(ctx.user_id, None)
        return True
    if mode == "check":
        task_ids = pend.get("task_ids") or []
        if choice == 0:
            state.pending_handin_choose.pop(ctx.user_id, None)
            await reply(api, ctx, "已取消操作。", logsvc)
            return True
        if choice < 1 or choice > len(task_ids):
            await reply(api, ctx, "序号无效，请重新回复数字。", logsvc)
            return True
        tid = task_ids[choice - 1]
        task = handin._tasks.get(tid)
        if not task:
            await reply(api, ctx, "任务不存在。", logsvc)
            state.pending_handin_choose.pop(ctx.user_id, None)
            return True
        if not _can_manage_handin_task(ctx, task):
            await reply(api, ctx, "权限不足：只能操作你创建的任务（或联系管理员）。", logsvc)
            state.pending_handin_choose.pop(ctx.user_id, None)
            return True
        files = handin.list_submitted_files(task)
        k = conv_key(ctx)
        _mark_last_find_cache(state, k, files, task.name)
        if not files:
            await reply(api, ctx, f"任务「{task.name}」当前还没有提交文件。", logsvc)
        else:
            lines = [f"📦 已提交文件列表（任务：{task.name}，共 {len(files)} 个）："]
            for i, p in enumerate(files, 1):
                lines.append(f"{i}. {p.name}")
            lines.append("用 /get 序号（如/get 1 2 3 4）获取其中一个或多个文件。")
            await reply(api, ctx, "\n".join(lines), logsvc)
        state.pending_handin_choose.pop(ctx.user_id, None)
        return True
    if mode == "getzip":
        task_ids = pend.get("task_ids") or []
        if choice == 0:
            state.pending_handin_choose.pop(ctx.user_id, None)
            await reply(api, ctx, "已取消操作。", logsvc)
            return True
        if choice < 1 or choice > len(task_ids):
            await reply(api, ctx, "序号无效，请重新回复数字。", logsvc)
            return True
        tid = task_ids[choice - 1]
        task = handin._tasks.get(tid)
        if not task:
            await reply(api, ctx, "任务不存在。", logsvc)
            state.pending_handin_choose.pop(ctx.user_id, None)
            return True
        if not _can_manage_handin_task(ctx, task):
            await reply(api, ctx, "权限不足：只能操作你创建的任务（或联系管理员）。", logsvc)
            state.pending_handin_choose.pop(ctx.user_id, None)
            return True
        safe = handin._safe_component(task.name)
        out_zip = (DATA_DIR / "temp" / "handin_exports" / f"{safe}_g{task.group_id}_{int(time.time())}.zip")
        ok, msgz, zpath = handin.zip_submissions(task, out_zip)
        if not ok or not zpath:
            await reply(api, ctx, msgz, logsvc)
            state.pending_handin_choose.pop(ctx.user_id, None)
            return True
        # 大文件提示（打包后的 zip 将要发送）
        try:
            await _warn_large_if_needed(api, ctx, logsvc, f"{task.name}.zip", int(Path(zpath).stat().st_size), mode="zip")
        except Exception as e:
            logsvc.log.warning(f"warn large zip failed: task={task.task_id} zip={zpath} err={e}")
        # 发送 zip：先 staging 到 NapCat 专用上传目录（/data/upload_*），再上传
        cpath, send_name, stage_msg = _stage_for_napcat(ctx, zpath, display_name=f"{task.name}.zip", logsvc=logsvc)
        if not cpath:
            await reply(api, ctx, f"staging 失败：{stage_msg}", logsvc)
            state.pending_handin_choose.pop(ctx.user_id, None)
            return True
        sent, detail = await _send_file(api, ctx, cpath, send_name)
        if sent is True:
            await reply(api, ctx, f"{msgz}\n已发送压缩包。", logsvc)
        elif sent is None:
            await reply(api, ctx, f"{msgz}\n已提交发送。" + ((" " + detail) if detail else "") + "若你已在 QQ 里看到文件卡片，可忽略。", logsvc)
        else:
            await reply(api, ctx, "发送失败：" + (detail or "请确认 docker-compose 挂载、NapCat/QQ 账号权限。"), logsvc)
        # 记录最后一次 /handinget（用于 30 天后清理归档）
        if sent is True or sent is None:
            try:
                task.last_handinget_ts = time.time()
                handin._save()
            except Exception as e:
                logsvc.log.warning(f"update last_handinget_ts failed: task={task.task_id} err={e}")
        state.pending_handin_choose.pop(ctx.user_id, None)
        return True
    return False
async def _handle_cancel_number_choice(api, ctx, text: str, logsvc: LogService, state: BotState, handin: HandinService) -> bool:
    """处理取消任务的数字选择（群聊/私聊均可）。返回是否已处理。"""
    t = (text or "").strip()
    if not re.fullmatch(r"\d{1,3}", t):
        return False
    pend = state.pending_handin_choose.get(ctx.user_id)
    if not pend or pend.get("mode") != "cancel":
        return False
    # 若限定了群，则群里必须匹配该群
    gid = pend.get("group_id", None)
    try:
        gid = int(gid) if gid is not None else None
    except Exception:
        gid = None
    if gid is not None and ctx.scene == "group":
        if ctx.group_id is None or int(ctx.group_id) != gid:
            return False
    # 记录 IN（触发回复）
    logsvc.log_in(ctx, t)
    choice = int(t)
    if choice == 0:
        state.pending_handin_choose.pop(ctx.user_id, None)
        await reply(api, ctx, "已取消操作。", logsvc)
        return True
    task_ids = pend.get("task_ids") or []
    if choice < 1 or choice > len(task_ids):
        await reply(api, ctx, "序号无效，请重新回复数字。", logsvc)
        return True
    tid = task_ids[choice - 1]
    task = handin._tasks.get(tid)  # internal lookup
    if not task or not task.is_active():
        state.pending_handin_choose.pop(ctx.user_id, None)
        await reply(api, ctx, "任务不存在或已结束。", logsvc)
        return True
    # 权限：仅允许创建者或管理员取消
    if not _can_manage_handin_task(ctx, task):
        state.pending_handin_choose.pop(ctx.user_id, None)
        await reply(api, ctx, "权限不足：只能取消你创建的任务（或联系管理员）。", logsvc)
        return True
    ok, msg2 = handin.cancel_task(tid, ctx.user_id)
    state.pending_handin_choose.pop(ctx.user_id, None)
    await reply(api, ctx, msg2, logsvc)
    return True
async def _handle_find_folder_number_choice(
    api,
    ctx,
    text: str,
    logsvc: LogService,
    state: BotState,
    before_handle: Optional[Callable[[], None]] = None,
) -> bool:
    """处理 /find 结果的“直接回复序号查看目录内容（仅下一级）”。"""
    t = (text or "").strip()
    if not re.fullmatch(r"\d{1,3}", t):
        return False
    k = conv_key(ctx)
    hits = state.last_find.get(k) or []
    if not hits:
        return False
    idx = int(t)
    if idx < 1 or idx > len(hits):
        return False
    p = hits[idx - 1]
    if not p.exists():
        if before_handle is not None:
            before_handle()
        await reply(api, ctx, "该条目已不存在，请重新 /find。", logsvc)
        return True
    if p.is_file():
        if before_handle is not None:
            before_handle()
        await reply(api, ctx, f"「{p.name}」是文件，请用 /get {idx} 获取。", logsvc)
        return True
    if not p.is_dir():
        return False
    try:
        entries = list(p.iterdir())
    except Exception as e:
        if before_handle is not None:
            before_handle()
        await reply(api, ctx, f"读取目录失败：{e}", logsvc)
        return True
    entries.sort(key=lambda x: (not x.is_dir(), x.name.lower()))
    has_more = len(entries) > int(LS_LIMIT)
    entries = entries[: int(LS_LIMIT)]
    # 下钻后刷新 /get 的候选列表，支持继续按数字进入下一层目录。
    if before_handle is not None:
        before_handle()
    _mark_last_find_cache(state, k, entries, p.name)
    if not entries:
        await reply(api, ctx, f"📁 {p.name}/ 目录为空。", logsvc)
        return True
    lines = [f"📁 {p.name}/ 下一级目录与文件："]
    for i, child in enumerate(entries, 1):
        if child.is_dir():
            lines.append(f"{i}. 📁 {child.name}/")
            continue
        suffix = ""
        try:
            sz = int(child.stat().st_size)
            if _is_large(sz):
                suffix = f" （{_fmt_mb(sz)}，大文件）"
        except Exception:
            pass
        lines.append(f"{i}. 📄 {child.name}{suffix}")
    if has_more:
        lines.append(f"（当前目录项较多，仅显示前 {LS_LIMIT} 项）")
    lines.append("继续直接回复序号可进入下级目录；选择文件请用 /get 序号。")
    lines.append("也可用 /get 序号（如/get 1 2 3 4）获取当前列表中的文件/文件夹。")
    await reply(api, ctx, "\n".join(lines), logsvc)
    return True


async def _handle_count_name_input(
    api,
    ctx,
    evt: dict,
    text: str,
    logsvc: LogService,
    state: BotState,
    aisvc: Optional["AIService"] = None,
) -> bool:
    _ = evt
    _ = aisvc
    session_key = conv_key(ctx)
    session = state.pending_count_session.get(session_key)
    if not isinstance(session, dict):
        return False

    t = str(text or "").strip()
    if not t:
        return True
    if _is_count_end_input(t):
        logsvc.log_in(ctx, t)
        state.pending_count_session.pop(session_key, None)
        await reply(api, ctx, "本次 /count 统计已结束，临时名单已清空。", logsvc)
        return True
    if t.startswith("/") or t.startswith("／"):
        plain = t[1:].strip()
        if not plain:
            return True
        cmdx, _rest = _split_args(plain)
        cmdx = str(cmdx or "").lower()
        if cmdx in ("count", "countlist", "countremove"):
            return False
        logsvc.log_in(ctx, t)
        await reply(api, ctx, "当前处于 /count 统计模式，请先发送 end 结束后再使用其他功能。", logsvc)
        return True

    logsvc.log_in(ctx, t)
    raw_names = _parse_count_names(t)
    if not raw_names:
        session["ts"] = time.time()
        await reply(api, ctx, "未识别到有效姓名，请继续输入；发送 /countlist 查看，发送 end 结束。", logsvc)
        return True

    current = _dedup_names_keep_order(list(session.get("names") or []))
    current_set = set(current)
    added = 0
    for name in raw_names:
        if name in current_set:
            continue
        current_set.add(name)
        current.append(name)
        added += 1
    session["names"] = current
    session["ts"] = time.time()

    if added <= 0:
        await reply(api, ctx, f"这条消息中的姓名已存在，当前共 {len(current)} 人。发送 /countlist 查看，发送 end 结束。", logsvc)
        return True
    await reply(api, ctx, f"已记录 {added} 人，当前共 {len(current)} 人。发送 /countlist 查看，发送 end 结束。", logsvc)
    return True


async def _ensure_group_context_and_schedule_digest(
    api,
    ctx,
    evt: dict,
    text: str,
    logsvc: LogService,
    state: BotState,
    handin: HandinService,
    aisvc: Optional["AIService"] = None,
):
    # ========== group_name 兜底 ==========
    # 事件里常拿不到 group_name：需要时用 get_group_info 补齐，并缓存到本次 ctx（后续日志会用到“群名_群号”）
    if getattr(ctx, "scene", "") == "group" and getattr(ctx, "group_id", None) is not None:
        try:
            if not getattr(ctx, "group_name", None):
                gname = await api.get_group_name(int(ctx.group_id))
                if gname and str(gname) != str(ctx.group_id):
                    ctx.group_name = str(gname)
        except Exception:
            pass
    if getattr(ctx, "scene", "") == "group":
        _schedule_group_notice_digest(api, ctx, evt, text, logsvc, state, handin, aisvc)
    # ========== Handin: 文件提交 / 数字选择（优先） ==========
    # 私聊文件 / 覆盖确认 / 数字选择（优先）

async def _handle_pre_dispatch_state(
    api,
    ctx,
    evt: dict,
    text: str,
    logsvc: LogService,
    state: BotState,
    handin: HandinService,
    filesvc: FileService,
    aisvc: Optional["AIService"] = None,
):
    handled = await _handle_signin_image(api, ctx, evt, logsvc, state, handin)
    if handled:
        return True
    if ctx.scene.startswith("private"):
        handled = await _handle_private_signin_name_input(api, ctx, text, logsvc, state, handin)
        if handled:
            return True
        handled = await _handle_private_file(api, ctx, evt, logsvc, state, handin)
        if handled:
            return True
        handled = await _handle_private_overwrite_yesno(api, ctx, text, logsvc, state, handin)
        if handled:
            return True
        handled = await _handle_private_done_batch(api, ctx, text, logsvc, state, handin)
        if handled:
            return True
        handled = await _handle_private_zip_name_input(api, ctx, text, logsvc, state, handin)
        if handled:
            return True
        handled = await _handle_private_name_input(api, ctx, text, logsvc, state, handin)
        if handled:
            return True
        handled = await _handle_private_number_choice(api, ctx, text, logsvc, state, handin, filesvc)
        if handled:
            return True
    handled = await _handle_count_name_input(api, ctx, evt, text, logsvc, state, aisvc)
    if handled:
        return True
    handled = await _handle_cancel_number_choice(api, ctx, text, logsvc, state, handin)
    if handled:
        return True
    # ========== 原有文字命令体系 ==========
    return False


async def _handle_ai_chat_trigger(
    api,
    ctx,
    evt: dict,
    t: str,
    logsvc: LogService,
    aisvc: Optional["AIService"] = None,
    forced_ai_input: Optional[str] = None,
    vision_skill=None,
    current_slots: Optional[list] = None,
    message_id: str = "",
):
    current_slots = list(current_slots or [])
    has_visual = bool(current_slots)
    trigger_text = forced_ai_input
    if trigger_text is None:
        trigger_text = extract_ai_chat_trigger_text(
            ctx,
            evt,
            t,
            has_visual=has_visual,
            bot_nick=(aisvc.bot_nick if aisvc else AI_BOT_NICK),
        )
        if trigger_text is None:
            return False
    backend, clean_trigger_text = _split_ai_chat_backend(trigger_text)
    backend_key = "deepseek" if backend == "default" else backend
    session_key = _ai_chat_session_key(ctx)

    # 视觉：历史补解析（按当前后端窗口）+ 当前消息图片统一解析
    current_slots = list(current_slots or [])
    history_slots: List[dict] = []
    if vision_skill is not None and getattr(vision_skill, "ready", False):
        try:
            if session_key and aisvc is not None and callable(getattr(aisvc, "collect_unresolved_vision_slots", None)):
                history_slots = aisvc.collect_unresolved_vision_slots(session_key, backend_key)
        except Exception:
            history_slots = []
        all_targets = history_slots + current_slots
        if all_targets:
            try:
                resolutions = await vision_skill.resolve_slots(api, all_targets)
                if session_key and aisvc is not None and callable(getattr(aisvc, "apply_vision_resolutions", None)):
                    try:
                        aisvc.apply_vision_resolutions(session_key, resolutions)
                    except Exception:
                        pass
                current_slots = vision_skill.apply_resolutions_to_slots(current_slots, resolutions)
            except Exception as e:
                try:
                    logsvc.log.warning(f"vision resolve failed: {type(e).__name__}: {e}")
                except Exception:
                    pass
    ai_input = _augment_ai_input_with_sender(ctx, clean_trigger_text)
    if ai_input is not None:
        if not ai_input and not current_slots:
            await reply(api, ctx, "想聊点啥？群里@我后直接说，私聊直接发送文本就行。", logsvc)
            return True
        if aisvc is None:
            await reply(api, ctx, "AI 聊天暂时不可用（配置未就绪）。", logsvc)
            return True
        restricted_cli = backend in {"gemini", "claude"} and not _ai_chat_allows_full_cli(ctx)
        route_backend = "restricted_antigravity" if restricted_cli else ("deepseek" if backend == "default" else backend)
        use_gemini = backend in {"gemini", "claude"}
        model_key = backend if use_gemini else None
        if use_gemini:
            ready = bool(getattr(aisvc, "gemini_chat_ready", False))
        else:
            ready = bool(getattr(aisvc, "chat_ready", False))
        if not ready:
            if use_gemini:
                msg = "antigravity 联网聊天暂时不可用（antigravity CLI 未就绪）。"
            else:
                msg = "AI 聊天暂时不可用（配置未就绪）。"
            await reply(api, ctx, msg, logsvc)
            return True
        if use_gemini:
            if restricted_cli:
                chat_with_context_fn = getattr(aisvc, "restricted_gemini_chat_with_context", None)
                chat_fn = getattr(aisvc, "restricted_gemini_chat", None)
            else:
                chat_with_context_fn = getattr(aisvc, "gemini_chat_with_context", None)
                chat_fn = getattr(aisvc, "gemini_chat", None)
        else:
            chat_with_context_fn = getattr(aisvc, "chat_with_context", None)
            chat_fn = getattr(aisvc, "chat", None)
        if not callable(chat_fn):
            await reply(api, ctx, "AI 聊天暂时不可用（配置未就绪）。", logsvc)
            return True
        try:
            if session_key and callable(chat_with_context_fn):
                if use_gemini:
                    out = (
                        await chat_with_context_fn(
                            session_key, ai_input, model_key,
                            msg_id=message_id,
                            vision_slots=current_slots,
                        )
                    ).strip()
                else:
                    out = (
                        await chat_with_context_fn(
                            session_key, ai_input,
                            msg_id=message_id,
                            vision_slots=current_slots,
                        )
                    ).strip()
                try:
                    setattr(ctx, "_skip_reply_context_once", True)
                except Exception:
                    pass
            else:
                if use_gemini:
                    out = (await chat_fn(ai_input, model_key)).strip()
                else:
                    out = (await chat_fn(ai_input)).strip()
            if out and _is_likely_ai_stuck_repeat(session_key, ai_input, out):
                retry_prompt = (
                    "你刚才出现了机械复读。请只根据这条新消息给出新的、准确的回复，不要复述上一条答案。\n"
                    + ai_input
                )
                if use_gemini:
                    retry_out = (await chat_fn(retry_prompt, model_key)).strip()
                else:
                    retry_out = (await chat_fn(retry_prompt)).strip()
                if retry_out:
                    out = retry_out
            if not out:
                out = "我这边没收到有效回复，稍后再试一次。"
            await reply(api, ctx, out, logsvc)
        except Exception as e:
            try:
                logsvc.log.warning(
                    f"AI chat failed: backend={route_backend} "
                    f"session={(session_key or '')[:80]} err={e}"
                )
            except Exception:
                pass
            fallback_text = _antigravity_busy_reply(backend) if use_gemini and _is_antigravity_busy_error(e) else aisvc.fallback_error_reply
            fallback_sent = await reply(api, ctx, fallback_text, logsvc)
            if fallback_sent is False and ctx.scene == "group":
                await reply(api, ctx, fallback_text, logsvc, force_private_user_id=ctx.user_id)
            await notify_admin_error(api, ctx, f"aichat/{route_backend}", e, logsvc)
        return True
    return False

async def _handle_plain_text_input(
    api,
    ctx,
    evt: dict,
    t: str,
    logsvc: LogService,
    state: BotState,
    *,
    business_only: bool = False,
    before_handle: Optional[Callable[[], None]] = None,
    has_visual: bool = False,
):
    if not (t.startswith("/") or t.startswith("／")):
        handled = await _handle_find_folder_number_choice(api, ctx, t, logsvc, state, before_handle)
        if handled:
            return True
        answer_text = _strip_text_companion_cq_segments(t)
        fixed_answers = _lookup_fixed_answers(answer_text)
        if fixed_answers:
            if before_handle is not None:
                before_handle()
            for msg in fixed_answers:
                await reply(api, ctx, msg, logsvc)
            return True
        if _is_media_or_emoji_only_message(evt, t):
            # 有视觉上下文时放行给 AI 触发（纯图片消息也可被 AI 理解）
            if not has_visual:
                if before_handle is not None:
                    before_handle()
                return True
        if not _is_keyword_text_message(evt, t):
            if not has_visual:
                if before_handle is not None:
                    before_handle()
                return True
        if getattr(ctx, "scene", "") != "group":
            return not business_only
        keyword_answers = _lookup_keyword_answers(answer_text)
        if keyword_answers:
            if before_handle is not None:
                before_handle()
            for msg in keyword_answers:
                await reply(api, ctx, msg, logsvc)
            return True
        return not business_only
    return False

async def _handle_explicit_command(
    api,
    ctx,
    t: str,
    filesvc: FileService,
    logsvc: LogService,
    state: BotState,
    handin: HandinService,
    perm=None,
    aisvc: Optional["AIService"] = None,
    calendar_service=None,
):
    t = t[1:].strip()  # 去掉 /
    if not t:
        await reply(api, ctx, "未知命令：/（用 /help 查看）", logsvc)
        return
    cmd, rest = _split_args(t)
    cmd = cmd.lower()
    if cmd in ("ping",):
        await reply(api, ctx, "pong", logsvc)
        return
    if cmd in ("whoami",):
        g = ctx.group_id if ctx.group_id is not None else "None"
        await reply(api, ctx, f"scene={ctx.scene}, user={ctx.nickname}-{ctx.user_id}, group={g}, level={ctx.level}", logsvc)
        return
    if cmd == "calendartest":
        if ctx.level < 3:
            await reply(api, ctx, "权限不足：/calendartest 仅管理员可用。", logsvc)
            return
        target_date = parse_calendar_date(rest)
        if target_date is None:
            await reply(api, ctx, "用法：/calendartest YYYY.M.D\n例如：/calendartest 2026.6.26", logsvc)
            return
        if calendar_service is None:
            await reply(api, ctx, "日历服务暂不可用。", logsvc)
            return
        try:
            calendar_cfg = calendar_service.get_group_config(1087250737)
            result = await calendar_service.generate_for_date(
                target_date,
                cfg=calendar_cfg,
                force_refresh=True,
                refresh_holiday_schedule=True,
            )
        except Exception as e:
            try:
                logsvc.log.warning(f"/calendartest failed: date={target_date.isoformat()} err={e}")
            except Exception:
                pass
            await reply(api, ctx, "日历测试生成失败，请稍后重试。", logsvc)
            return
        await reply(api, ctx, result.message if result.special and result.message else "非特殊日期", logsvc)
        return
    if cmd == "level":
        if ctx.level < 3:
            await reply(api, ctx, "权限不足：/level 仅管理员可用。", logsvc)
            return
        if perm is None:
            await reply(api, ctx, "权限服务不可用：当前无法设置等级。", logsvc)
            return
        parts = rest.split()
        if len(parts) == 1 and parts[0].lower() == "list":
            uid_to_level: Dict[int, int] = {}
            for uid, lv in perm.list_users(min_level=1):
                uid = int(uid)
                eff = 3 if uid in ADMIN_USERS else int(lv)
                if eff >= 1:
                    uid_to_level[uid] = eff
            for admin_uid in ADMIN_USERS:
                uid_to_level[int(admin_uid)] = 3
            if not uid_to_level:
                await reply(api, ctx, "当前没有等级 >=1 的用户。", logsvc)
                return
            ordered = sorted(uid_to_level.items(), key=lambda x: (-x[1], x[0]))
            sem = asyncio.Semaphore(8)
            async def _fetch_nick(uid: int) -> str:
                async with sem:
                    try:
                        return await api.get_user_nickname(uid)
                    except Exception:
                        return str(uid)
            names = await asyncio.gather(*[_fetch_nick(uid) for uid, _ in ordered])
            lines = [
                f">=1 级用户共 {len(ordered)} 人",
                "等级 | QQ号 | 昵称",
            ]
            for (uid, lv), name in zip(ordered, names):
                lines.append(f"{lv} | {uid} | {name}")
            # 避免消息过长导致发送失败，按长度切分多条发送
            chunk: List[str] = []
            cur_len = 0
            for line in lines:
                add_len = len(line) + 1
                if chunk and (cur_len + add_len > 3000):
                    await reply(api, ctx, "\n".join(chunk), logsvc)
                    chunk = [line]
                    cur_len = add_len
                else:
                    chunk.append(line)
                    cur_len += add_len
            if chunk:
                await reply(api, ctx, "\n".join(chunk), logsvc)
            return
        if len(parts) != 2:
            await reply(api, ctx, "用法：/level list\n或：/level QQ号 等级\n例如：/level 123456789 2", logsvc)
            return
        uid_raw = parts[0].translate(str.maketrans("０１２３４５６７８９", "0123456789"))
        lv_raw = parts[1].translate(str.maketrans("０１２３４５６７８９", "0123456789"))
        try:
            target_uid = int(uid_raw)
            target_lv = int(lv_raw)
        except Exception:
            await reply(api, ctx, "参数格式不对：QQ号和等级都要是数字。", logsvc)
            return
        if target_uid <= 0:
            await reply(api, ctx, "参数不对：QQ号必须是正整数。", logsvc)
            return
        if target_lv < 0 or target_lv > 3:
            await reply(api, ctx, "参数不对：等级只能是 0~3。", logsvc)
            return
        perm.set_level(target_uid, target_lv)
        stored = perm.get_level(target_uid)
        effective = 3 if target_uid in ADMIN_USERS else stored
        if target_uid in ADMIN_USERS and stored != 3:
            await reply(
                api,
                ctx,
                f"已将 {target_uid} 的存档等级设为 {stored}，但该账号在 ADMIN_USERS 中，实际生效等级仍为 3。",
                logsvc,
            )
            return
        await reply(api, ctx, f"已设置 {target_uid} 的等级为 {stored}（生效等级 {effective}）。", logsvc)
        return
    if cmd == "count":
        key = conv_key(ctx)
        old = state.pending_count_session.get(key)
        old_names = _dedup_names_keep_order(list((old or {}).get("names") or []))
        state.pending_count_session[key] = {"names": [], "ts": time.time()}
        if old_names:
            await reply(
                api,
                ctx,
                "已重新开始 /count 统计，并清空上一次临时名单。\n请分多次发送姓名；发送 /countlist 查看，发送 end 结束并清空。",
                logsvc,
            )
        else:
            await reply(
                api,
                ctx,
                "已进入 /count 统计模式。\n请分多次发送姓名；发送 /countlist 查看，发送 end 结束并清空。",
                logsvc,
            )
        return
    if cmd == "countlist":
        key = conv_key(ctx)
        session = state.pending_count_session.get(key)
        if not isinstance(session, dict):
            await reply(api, ctx, "当前会话没有进行中的 /count 统计，请先在本会话发送 /count。", logsvc)
            return
        session["ts"] = time.time()
        names = _dedup_names_keep_order(list(session.get("names") or []))
        roster: List[Tuple[str, str]] = []
        get_roster = getattr(handin, "_get_roster", None)
        if callable(get_roster):
            try:
                roster = list(get_roster() or [])
            except Exception as e:
                logsvc.log.warning(f"/countlist get roster failed: err={e}")
                roster = []
        await reply(api, ctx, _build_count_list_text(names, roster), logsvc)
        return
    if cmd == "countremove":
        key = conv_key(ctx)
        session = state.pending_count_session.get(key)
        if not isinstance(session, dict):
            await reply(api, ctx, "当前会话没有进行中的 /count 统计，请先在本会话发送 /count。", logsvc)
            return
        arg = (rest or "").strip()
        if not re.fullmatch(r"\d{1,4}", arg):
            await reply(api, ctx, "用法：/countremove 序号（序号来自 /countlist 的“已提交名单”）", logsvc)
            return
        idx = int(arg)
        names = _dedup_names_keep_order(list(session.get("names") or []))
        if idx < 1 or idx > len(names):
            await reply(api, ctx, f"序号无效：{idx}（当前已提交名单共 {len(names)} 人）", logsvc)
            return
        removed = names.pop(idx - 1)
        session["names"] = names
        session["ts"] = time.time()
        await reply(api, ctx, f"已移除：{removed}\n当前已提交 {len(names)} 人。可发送 /countlist 查看。", logsvc)
        return
    if cmd == "signin":
        if ctx.level < 2:
            await reply(api, ctx, "权限不足：/signin 仅对 2 级及以上开放。", logsvc)
            return
        if ctx.scene != "group" or ctx.group_id is None:
            await reply(api, ctx, "/signin 只能在群聊中使用。", logsvc)
            return
        parsed = _parse_signin_deadline_hhmm(rest)
        if parsed is None:
            await reply(api, ctx, "用法：/signin 截止时间\n例如：/signin 18:30（冒号中英文都可以；若该时间已过，将自动设为下一天）", logsvc)
            return
        hh, mm = parsed
        gid = int(ctx.group_id)
        old = state.signin_tasks.get(gid)
        replaced = isinstance(old, dict)
        if replaced:
            _cancel_signin_deadline_task(old)
            for uid, item in list(state.pending_signin_name_input.items()):
                if isinstance(item, dict) and int(item.get("group_id") or 0) == gid:
                    state.pending_signin_name_input.pop(uid, None)
        now_ts = time.time()
        deadline_ts = _signin_deadline_ts(hh, mm, now_ts=now_ts)
        task_id = f"{gid}:{int(now_ts * 1000)}:{int(ctx.user_id)}"
        state.signin_tasks[gid] = {
            "task_id": task_id,
            "group_id": gid,
            "creator_id": int(ctx.user_id),
            "creator_nickname": str(getattr(ctx, "card", "") or getattr(ctx, "nickname", "") or ctx.user_id),
            "created_ts": now_ts,
            "deadline_ts": deadline_ts,
            "submitted_names": [],
            "submitted_users": {},
            "failures": {},
            "failure_notified": [],
        }
        _schedule_signin_deadline(api, state, gid, handin, logsvc, task_id)
        prefix = "已替换原有signin任务。" if replaced else "signin任务已创建。"
        await reply(
            api,
            ctx,
            f"{prefix}\n截止时间：{_format_signin_deadline(deadline_ts)}\n请私聊发送包含带有教室时间牌的图片完成signin。",
            logsvc,
        )
        return
    if cmd in ("help", "h"):
        lines = [
            "命令速览：",
            "基础：",
            "/help 或 /h",
            "/ping",
            "/whoami",
            "/count  开始临时收集名单（模式内仅收集名单；发送 end 结束并清空）",
            "/countlist  查看已提交名单和未交名单",
            "/countremove 序号  移除已提交名单中的人名",
        ]
        if ctx.level >= 2:
            lines.append("/autoat  单条消息依次 @ 当前群全部成员（仅群聊）")
            lines.append("/signin 截止时间  创建signin任务（群内创建，私聊发图提交）")
        if ctx.level >= 3:
            lines.extend([
                "",
                "管理功能：",
                "/level list",
                "/level QQ号 等级",
                "/calendartest YYYY.M.D  测试生成指定日期的重要日提醒",
            ])
        if ctx.level >= 1:
            lines.extend([
                "",
                "资料检索：",
                "/ls [root/子目录]  查看目录",
                "/find 搜索内容 [可选: root/子目录]  支持关键词，也支持直接描述需求",
                "/get 序号（如 /get 1 2 3 4）  获取文件/文件夹（文件夹自动打包）",
                "提示：/find 不用只输关键词，也可以直接说你想找什么。",
                "例如：/find 数字电子技术教材",
                "例如：/find 期末复习用的高数资料",
                "例如：/find 带答案的数理方程习题",
                "提示：可直接回复序号进入下级目录。",
            ])
        lines.extend([
            "",
            "AI聊天（默认 DeepSeek）：",
            "群聊：@Cooper_bot + 内容",
            "群聊（联网搜索 Gemini）：@Cooper_bot g内容（g/G 后面可不加空格）",
            "群聊（联网搜索 Claude）：@Cooper_bot c内容（c/C 后面可不加空格）",
            "私聊：直接发送文本内容",
            "私聊（联网搜索 Gemini）：g内容（g/G 后面可不加空格）",
            "私聊（联网搜索 Claude）：c内容（c/C 后面可不加空格）",
        ])
        if ctx.level >= 2:
            lines.extend([
                "",
                "提交功能：",
                "/handin 任务名 [文件后缀] [提醒时间...] 截止时间（仅群聊，如 pdf/docx）",
                "/handinstat  查看可管理任务并查询未交",
                "/handincheck  查看可管理任务的已交文件（可配合 /get）",
                "/handinget  打包可管理任务的已交文件并发送",
                "/chandin  取消可管理的提交任务（按提示回复序号）",
                "私聊发送文件后按提示操作；多文件发完后回复 done；限定后缀时多文件至少包含一个匹配文件。",
            ])
        msg = "\n".join(lines)
        await reply(api, ctx, msg, logsvc)
        return
    if cmd == "autoat":
        if ctx.level < 2:
            await reply(api, ctx, "权限不足：/autoat 仅对 2 级及以上开放。", logsvc)
            return
        if ctx.scene != "group" or ctx.group_id is None:
            await reply(api, ctx, "/autoat 只能在群聊中使用。", logsvc)
            return
        try:
            list_resp = await api.get_group_member_list(ctx.group_id)
        except Exception as e:
            logsvc.log.warning(f"/autoat get_group_member_list failed: group={ctx.group_id}, err={e}")
            await reply(api, ctx, "获取群成员失败，请稍后重试。", logsvc)
            return
        user_ids = _extract_group_member_user_ids(list_resp)
        if not user_ids:
            detail = _onebot_resp_detail(list_resp)
            logsvc.log.warning(f"/autoat empty member list: group={ctx.group_id}, detail={detail}")
            await reply(api, ctx, "获取群成员失败：没有拿到可用的群成员列表。", logsvc)
            return
        at_message = " ".join(f"[CQ:at,qq={uid}]" for uid in user_ids)
        send_resp = await api.send_group_msg(ctx.group_id, at_message)
        if send_resp is None:
            logsvc.log.info(f"/autoat send unconfirmed: group={ctx.group_id}, count={len(user_ids)}")
            logsvc.log_out(ctx, at_message)
            return
        if _onebot_resp_ok(send_resp):
            logsvc.log_out(ctx, at_message)
            return
        detail = _onebot_resp_detail(send_resp)
        logsvc.log.warning(f"/autoat send failed: group={ctx.group_id}, detail={detail}")
        await reply(api, ctx, "发送失败：请确认 QQ/NapCat 是否限制长消息或频繁 @。", logsvc)
        return
    # Handin commands
    if cmd == "handin":
        if ctx.level < 2:
            await reply(api, ctx, "权限不足：/handin 仅对 2 级及以上开放。", logsvc)
            return
        if ctx.scene != "group" or ctx.group_id is None:
            await reply(api, ctx, "/handin 只能在群聊中使用。", logsvc)
            return
        # 格式：/handin 任务名 [文件后缀] [提醒时间...] 截止时间
        # 时间用两段：月.日 时:分（冒号中英文都兼容）。提醒时间可不填或填多个；最后一组时间为截止时间。
        task_name, required_suffix, time_texts, err = _parse_handin_create_parts(rest)
        if err:
            await reply(api, ctx, err, logsvc)
            return
        now = time.time()
        ts_list = []
        for s in time_texts:
            ts = parse_mmdd_hhmm(s, now)
            if ts is None:
                await reply(api, ctx, f"时间格式不对：{s}\n请用 月.日 时:分，例如 1.22 18:30（冒号中英文都行）。", logsvc)
                return
            ts_list.append(ts)
        deadline_ts = ts_list[-1]
        remind_list = ts_list[:-1]  # 可为空或多个
        ok, msg2 = handin.create_task(ctx.group_id, ctx.user_id, task_name, remind_list, deadline_ts, required_suffix)
        await reply(api, ctx, msg2, logsvc)
        return
    if cmd == "handinstat":
        if ctx.level < 2:
            await reply(api, ctx, "权限不足：/handinstat 仅对 2 级及以上开放。", logsvc)
            return
        # 允许查询已截止任务：用于统计未交/导出等（提交仍只允许进行中）
        if ctx.scene == "group" and ctx.group_id is not None:
            list_res = list_handin_tasks_for_group(
                handin=handin,
                group_id=ctx.group_id,
                include_closed=True,
                active_only=False,
                only_gettable=True,
                sort_mode="active_then_deadline_desc",
            )
            tasks = list_res.data.get("tasks") if (list_res.ok and isinstance(list_res.data, dict)) else []
        else:
            tasks = handin.list_tasks(include_closed=True)
            # 仅保留仍可 /handinget 的任务（归档未被清理）
            tasks = [t for t in tasks if handin.is_task_gettable(t)]
        tasks = _filter_manageable_handin_tasks(ctx, tasks)
        if not tasks:
            await reply(api, ctx, "当前没有提交任务记录。", logsvc)
            return
        now = time.time()
        def _status_tag(t):
            if getattr(t, "cancelled", False):
                return "已取消"
            if now >= float(t.deadline_ts):
                return "已截止"
            if getattr(t, "closed", False):
                return "已结束"
            return "进行中"
        # 进行中优先，其次按截止时间倒序
        if not (ctx.scene == "group" and ctx.group_id is not None):
            tasks.sort(key=lambda t: (0 if t.is_active(now) else 1, -float(t.deadline_ts)))
        text_list = ["提交任务列表："]
        for i, tsk in enumerate(tasks, 1):
            row = get_handin_task_summary(tsk, now_ts=now, pretty_ts_func=pretty_ts, with_status=True, with_group=True)
            if row.ok:
                text_list.append(f"{i}. {row.message}")
            else:
                text_list.append(f"{i}. [{_status_tag(tsk)}] {tsk.name}（群 {tsk.group_id}，截止 {pretty_ts(tsk.deadline_ts)}）")
        text_list.append("回复数字选择任务，我会发送未提交名单（若姓名识别率过低会改发已提交文件列表；已截止任务也可查询）。")
        # 若在群里发，群里提示，列表私聊
        if ctx.scene == "group":
            await reply(api, ctx, "已私聊你提交任务列表，请在私聊里回复数字选择。", logsvc)
            await reply(api, ctx, "\n".join(text_list), logsvc, force_private_user_id=ctx.user_id)
        else:
            await reply(api, ctx, "\n".join(text_list), logsvc)
        state.pending_handin_choose[ctx.user_id] = {"mode": "status", "task_ids": [t.task_id for t in tasks], "ts": time.time()}
        return
    if cmd == "handincheck":
        if ctx.level < 2:
            await reply(api, ctx, "权限不足：/handincheck 仅对 2 级及以上开放。", logsvc)
            return
        tasks = handin.list_tasks(include_closed=True) if ctx.level >= 3 else handin.list_tasks_by_creator(ctx.user_id, include_closed=True)
        # 仅保留仍可 /handinget 的任务（归档未被清理）
        tasks = [t for t in tasks if handin.is_task_gettable(t)]
        if not tasks:
            await reply(api, ctx, "当前没有提交任务记录。", logsvc)
            return
        now = time.time()
        def _status_tag(t):
            if getattr(t, "cancelled", False):
                return "已取消"
            if now >= float(t.deadline_ts):
                return "已截止"
            if getattr(t, "closed", False):
                return "已结束"
            return "进行中"
        tasks.sort(key=lambda t: (0 if t.is_active(now) else 1, -float(t.deadline_ts)))
        text_list = ["全部提交任务列表：" if ctx.level >= 3 else "你创建的提交任务列表："]
        for i, tsk in enumerate(tasks, 1):
            text_list.append(f"{i}. [{_status_tag(tsk)}] {tsk.name}（群 {tsk.group_id}，截止 {pretty_ts(tsk.deadline_ts)}）")
        text_list.append("回复数字选择任务（回复 0 取消），我会列出已提交文件列表（已截止任务也可查看）。")
        if ctx.scene == "group":
            await reply(api, ctx, "已私聊你任务列表，请在私聊里回复数字选择。", logsvc)
            await reply(api, ctx, "\n".join(text_list), logsvc, force_private_user_id=ctx.user_id)
        else:
            await reply(api, ctx, "\n".join(text_list), logsvc)
        state.pending_handin_choose[ctx.user_id] = {"mode": "check", "task_ids": [t.task_id for t in tasks], "ts": time.time()}
        return
    if cmd == "handinget":
        if ctx.level < 2:
            await reply(api, ctx, "权限不足：/handinget 仅对 2 级及以上开放。", logsvc)
            return
        tasks = handin.list_tasks(include_closed=True) if ctx.level >= 3 else handin.list_tasks_by_creator(ctx.user_id, include_closed=True)
        # 仅保留仍可 /handinget 的任务（归档未被清理）
        tasks = [t for t in tasks if handin.is_task_gettable(t)]
        if not tasks:
            await reply(api, ctx, "当前没有提交任务记录。", logsvc)
            return
        now = time.time()
        def _status_tag(t):
            if getattr(t, "cancelled", False):
                return "已取消"
            if now >= float(t.deadline_ts):
                return "已截止"
            if getattr(t, "closed", False):
                return "已结束"
            return "进行中"
        tasks.sort(key=lambda t: (0 if t.is_active(now) else 1, -float(t.deadline_ts)))
        text_list = ["全部提交任务列表：" if ctx.level >= 3 else "你创建的提交任务列表："]
        for i, tsk in enumerate(tasks, 1):
            text_list.append(f"{i}. [{_status_tag(tsk)}] {tsk.name}（群 {tsk.group_id}，截止 {pretty_ts(tsk.deadline_ts)}）")
        text_list.append("回复数字选择任务（回复 0 取消），我会把已提交文件打包为 zip 并发送（已截止任务也可导出）。")
        if ctx.scene == "group":
            await reply(api, ctx, "已私聊你任务列表，请在私聊里回复数字选择。", logsvc)
            await reply(api, ctx, "\n".join(text_list), logsvc, force_private_user_id=ctx.user_id)
        else:
            await reply(api, ctx, "\n".join(text_list), logsvc)
        state.pending_handin_choose[ctx.user_id] = {"mode": "getzip", "task_ids": [t.task_id for t in tasks], "ts": time.time()}
        return
    if cmd == "chandin":
        if ctx.level < 2:
            await reply(api, ctx, "权限不足：/chandin 仅对 2 级及以上开放。", logsvc)
            return
        # 群里默认只列本群可管理任务；私聊则列全部可管理任务
        if ctx.scene == "group" and ctx.group_id is not None:
            list_res = list_handin_tasks_for_group(
                handin=handin,
                group_id=ctx.group_id,
                include_closed=False,
                active_only=True,
                only_gettable=False,
                sort_mode="deadline_asc",
            )
            tasks = list_res.data.get("tasks") if (list_res.ok and isinstance(list_res.data, dict)) else []
            pend_gid = int(ctx.group_id)
        else:
            tasks = handin.list_active_tasks()
            pend_gid = None
        tasks = _filter_manageable_handin_tasks(ctx, tasks)
        if not tasks:
            await reply(api, ctx, "当前没有可取消的提交任务。", logsvc)
            return
        text_list = ["当前可取消的提交任务列表："]
        for i, tsk in enumerate(tasks, 1):
            text_list.append(f"{i}. {tsk.name}（群 {tsk.group_id}，截止 {pretty_ts(tsk.deadline_ts)}）")
        text_list.append("回复数字取消该任务；回复 0 取消操作。")
        text_list.append("（管理员可取消任意提交任务。）" if ctx.level >= 3 else "（仅允许取消你创建的任务。）")
        await reply(api, ctx, "\n".join(text_list), logsvc)
        state.pending_handin_choose[ctx.user_id] = {"mode": "cancel", "task_ids": [t.task_id for t in tasks], "group_id": pend_gid, "ts": time.time()}
        return
        return
    # 文件相关命令：游客(0)直接拒绝
    if cmd in ("ls", "find", "get") and ctx.level < 1:
        await reply(api, ctx, "权限不足：你当前是 0 级（游客），不能访问资料库。", logsvc)
        return
    if cmd == "ls":
        ls_result = run_list_dir_query(filesvc=filesvc, ctx=ctx, path_arg=(rest if rest else None))
        await reply(api, ctx, str(ls_result.message or ""), logsvc)
        return
    if cmd == "find":
        # 支持：
        # /find 关键词
        # /find 多词 关键词 public/子目录
        # /find "需求"（语义检索）
        semantic_q = _parse_semantic_find_query(rest)
        semantic_mode = semantic_q is not None
        if semantic_mode:
            kw = str(semantic_q or "").strip()
            in_dir = None
        else:
            kw, in_dir = _parse_find_args(rest, filesvc)
        try:
            find_result = await asyncio.to_thread(
                run_find_query,
                filesvc=filesvc,
                ctx=ctx,
                keyword=kw,
                in_dir=in_dir,
            )
            find_data = find_result.data if isinstance(find_result.data, dict) else {}
            primary_hits = list(find_data.get("hits") or [])
        except Exception as e:
            logsvc.log.exception(f"/find failed: kw={kw!r} in_dir={in_dir!r} err={e}")
            await reply(api, ctx, "搜索失败，请稍后再试。", logsvc)
            return

        semantic_hits: List[Path] = []
        if _semantic_merge_allowed_for_in_dir(in_dir) and (aisvc is not None) and aisvc.semantic_ready:
            try:
                semantic_hits = await aisvc.semantic_find_paths(kw, limit=10)
            except Exception as e:
                logsvc.log.warning(f"/find semantic supplement failed: kw={kw!r} in_dir={in_dir!r} err={e}")
                semantic_hits = []
            semantic_hits = _filter_paths_under_base(
                semantic_hits,
                _semantic_filter_base_for_in_dir(in_dir),
            )
        hits, semantic_flags = _merge_find_hits(primary_hits, semantic_hits)
        k = conv_key(ctx)
        _mark_last_find_cache(state, k, hits, kw)
        if not hits:
            if semantic_mode:
                await reply(api, ctx, "没找到符合语义的文件，试试换个说法或用普通关键词 /find。", logsvc)
            else:
                await reply(api, ctx, "没找到匹配文件或文件夹。", logsvc)
            await reply(api, ctx, _build_find_guidance_message(query=kw, no_result=True), logsvc)
            return
        exact_lines: List[str] = []
        semantic_lines: List[str] = []
        has_large = False
        for i, p in enumerate(hits, 1):
            if p.is_dir():
                row = f"{i}. 📁 {p.name}/"
                if semantic_flags[i - 1]:
                    semantic_lines.append(row)
                else:
                    exact_lines.append(row)
                continue
            suffix = ""
            try:
                sz = int(p.stat().st_size)
                if _is_large(sz):
                    suffix = f" （{_fmt_mb(sz)}，大文件）"
                    has_large = True
            except Exception as e:
                logsvc.log.warning(f"/find stat failed: path={p} err={e}")
            row = f"{i}. 📄 {p.name}{suffix}"
            if semantic_flags[i - 1]:
                semantic_lines.append(row)
            else:
                exact_lines.append(row)
        lines = ["搜索结果："]
        if exact_lines:
            lines.append("")
            lines.append("【精准匹配】")
            lines.extend(exact_lines)
        if semantic_lines:
            lines.append("")
            lines.append("【智能推荐】")
            lines.extend(semantic_lines)
        lines.append("")
        lines.append("用 /get 序号（如/get 1 2 3 4）获取文件；文件夹会先打包成 zip。")
        lines.append("直接回复序号可进入目录并继续按数字下钻。")
        if has_large:
            lines.append("标记“大文件”的条目发送可能较慢，请耐心等待。")
        await reply(api, ctx, "\n".join(lines), logsvc)
        await reply(api, ctx, _build_find_guidance_message(query=kw), logsvc)
        return
    if cmd == "get":
        k = conv_key(ctx)
        arg = rest.strip()
        if not arg:
            await reply(api, ctx, "用法：/get 序号（如/get 1 2 3 4）", logsvc)
            return
        hits = state.last_find.get(k) or []
        if not hits:
            await reply(api, ctx, "没有可用的搜索结果：先 /find 再 /get", logsvc)
            return
        indices = _parse_indices(arg)
        # fallback：有些客户端会让 rest 里“看起来有 1”，但实际数字落在整条 t 里
        if not indices:
            indices = _parse_indices(t)  # t 是去掉 "/" 后的整条命令，例如 "get 1"
        if not indices:
            await reply(api, ctx, "参数不对：请输入序号，例如 /get 1 3 5", logsvc)
            return
        temp_artifacts: List[Path] = []
        try:
            prepared_items: list[tuple[int, Path, str]] = []
            ok_list = []
            pending_list = []
            bad_list = []
            for idx in indices:
                if idx < 1 or idx > len(hits):
                    bad_list.append(f"{idx}(无效)")
                    continue
                src = hits[idx - 1]
                if not src.exists():
                    bad_list.append(f"{idx}({src.name}:不存在)")
                    continue
                if src.is_dir():
                    out_dir = (DATA_DIR / "temp" / "get_dir_zip")
                    out_dir.mkdir(parents=True, exist_ok=True)
                    safe_stem = Path(_sanitize_ascii_filename(f"{src.name}.zip")).stem[:40].strip("._-") or "folder"
                    zpath = out_dir / f"{safe_stem}_{int(time.time())}_{uuid.uuid4().hex[:6]}.zip"
                    ok_zip, msg_zip = _zip_directory(src, zpath)
                    if not ok_zip:
                        bad_list.append(f"{idx}({src.name}:打包失败:{msg_zip})")
                        continue
                    temp_artifacts.append(zpath)
                    prepared_items.append((idx, zpath, f"{src.name}.zip"))
                elif src.is_file():
                    prepared_items.append((idx, src, src.name))
                else:
                    bad_list.append(f"{idx}({src.name}:不是文件或目录)")
            if not prepared_items:
                msg = "没有可发送的有效条目。"
                if bad_list:
                    msg = "失败： " + ", ".join(bad_list)
                await reply(api, ctx, msg, logsvc)
                return
            # 当有效选择条目 > GET_ZIP_THRESHOLD 时，统一再打一个外层 zip 发送
            if len(prepared_items) > int(GET_ZIP_THRESHOLD):
                label = (state.last_find_label.get(k) or "files").strip() or "files"
                safe_label = _safe_zip_label(label, default="files")
                out_dir = (DATA_DIR / "temp" / "get_zip")
                out_dir.mkdir(parents=True, exist_ok=True)
                outer_zip = out_dir / f"{safe_label}_{int(time.time())}_{uuid.uuid4().hex[:6]}.zip"
                packed = 0
                name_count: dict[str, int] = {}
                try:
                    with open_fast_zip(outer_zip) as zf:
                        for idx2, p, arc0 in prepared_items:
                            if (not p.exists()) or (not p.is_file()):
                                bad_list.append(f"{idx2}({arc0}:不存在)")
                                continue
                            arc = arc0
                            name_count[arc] = name_count.get(arc, 0) + 1
                            if name_count[arc] > 1:
                                arc = f"{idx2}_{arc0}"
                            zip_write_path(zf, p, arcname=arc)
                            packed += 1
                except Exception as e:
                    await reply(api, ctx, f"打包失败：{e}", logsvc)
                    return
                if packed <= 0:
                    msg = "打包失败：没有可写入的文件。"
                    if bad_list:
                        msg += "\n失败： " + ", ".join(bad_list)
                    await reply(api, ctx, msg, logsvc)
                    return
                temp_artifacts.append(outer_zip)
                display_name = f"{label}.zip"
                try:
                    await _warn_large_if_needed(api, ctx, logsvc, display_name, int(outer_zip.stat().st_size), mode="zip")
                except Exception as e:
                    logsvc.log.warning(f"warn large outer zip failed: zip={outer_zip} err={e}")
                cpath, send_name, stage_msg = _stage_for_napcat(ctx, outer_zip, display_name=display_name, logsvc=logsvc)
                if not cpath:
                    await reply(api, ctx, f"staging 失败：{stage_msg}", logsvc)
                    return
                sent, detail = await _send_file(api, ctx, cpath, send_name)
                if sent is True:
                    msg = f"✅ 已打包发送：{display_name}（共 {packed} 个条目）"
                    if bad_list:
                        msg += "\n失败： " + ", ".join(bad_list)
                    await reply(api, ctx, msg, logsvc)
                elif sent is None:
                    msg = (
                        f"📦 已提交发送：{display_name}。"
                        + ((" " + detail) if detail else "")
                        + "若你已在 QQ 里看到文件卡片，可忽略。"
                    )
                    if bad_list:
                        msg += "\n失败： " + ", ".join(bad_list)
                    await reply(api, ctx, msg, logsvc)
                else:
                    msg = "发送失败：" + (detail or "请确认 docker-compose 挂载、NapCat/QQ 账号权限。")
                    if bad_list:
                        msg += "\n失败： " + ", ".join(bad_list)
                    await reply(api, ctx, msg, logsvc)
                return
            for idx, p, shown_name in prepared_items:
                try:
                    await _warn_large_if_needed(api, ctx, logsvc, shown_name, int(p.stat().st_size), mode="send")
                except Exception as e:
                    logsvc.log.warning(f"warn large send file failed: file={p} shown_name={shown_name} err={e}")
                cpath, send_name, stage_msg = _stage_for_napcat(ctx, p, display_name=shown_name, logsvc=logsvc)
                if not cpath:
                    bad_list.append(f"{idx}({shown_name}:{stage_msg or 'staging失败'})")
                    continue
                sent, detail = await _send_file(api, ctx, cpath, send_name)
                if sent is True:
                    ok_list.append(f"{idx}({shown_name})" + (detail or ""))
                elif sent is None:
                    pending_list.append(f"{idx}({shown_name})" + ((":" + detail) if detail else ""))
                else:
                    # 源文件发送失败时，自动打包 zip 再发一次（zip 内保留原文件名）
                    did_zip_fallback = False
                    if AUTO_ZIP_FALLBACK:
                        ext = (p.suffix or "").lower()
                        if ext not in (".zip", ".rar", ".7z"):
                            try:
                                await reply(api, ctx, f"⚠️ 文件「{shown_name}」源文件发送失败，将改为打包 zip 发送（zip 内保留原文件名），请稍等…", logsvc)
                                fb_dir = (DATA_DIR / "temp" / "send_fallback")
                                fb_dir.mkdir(parents=True, exist_ok=True)
                                safe_stem = Path(_sanitize_ascii_filename(p.name)).stem[:40].strip("._-") or "file"
                                zpath = fb_dir / f"{safe_stem}_{int(time.time())}.zip"
                                with open_fast_zip(zpath) as zf:
                                    zip_write_path(zf, p, arcname=p.name)
                                temp_artifacts.append(zpath)
                                try:
                                    await _warn_large_if_needed(api, ctx, logsvc, zpath.name, int(zpath.stat().st_size), mode="zip")
                                except Exception as e:
                                    logsvc.log.warning(f"warn large fallback zip failed: zip={zpath} err={e}")
                                cpath2, _send_name2, stage_msg2 = _stage_for_napcat(ctx, zpath, logsvc=logsvc)
                                if not cpath2:
                                    bad_list.append(f"{idx}({shown_name}:zip staging失败:{stage_msg2})")
                                    did_zip_fallback = True
                                else:
                                    zip_display_name = (_sanitize_ascii_filename(f"{p.stem}.zip") if SEND_FILENAME_ASCII_SAFE else f"{p.stem}.zip")
                                    sentz, detailz = await _send_file(api, ctx, cpath2, zip_display_name)
                                    if sentz is True:
                                        ok_list.append(f"{idx}({shown_name}→zip)" + (detailz or ""))
                                        did_zip_fallback = True
                                    elif sentz is None:
                                        pending_list.append(f"{idx}({shown_name}→zip)" + ((":" + detailz) if detailz else ""))
                                        did_zip_fallback = True
                                    else:
                                        bad_list.append(f"{idx}({shown_name}:zip发送失败:" + (detailz or "失败") + ")")
                                        did_zip_fallback = True
                            except Exception as e:
                                logsvc.log.warning(f"zip fallback failed: file={p} shown_name={shown_name} err={e}")
                                did_zip_fallback = False
                    if not did_zip_fallback:
                        bad_list.append(f"{idx}({shown_name}:" + (detail or "失败") + ")")
            any_zip_fallback = any((('→zip' in x) or (':zip' in x)) for x in (ok_list + pending_list + bad_list))
            msg_lines = []
            if ok_list and not pending_list and not bad_list and (not any_zip_fallback):
                msg_lines.append(f"已发送 {len(ok_list)} 个文件。")
            else:
                if ok_list:
                    msg_lines.append("已发送： " + ", ".join(ok_list))
                    if any_zip_fallback:
                        msg_lines.append("（提示：部分文件源文件发送失败，已自动改为 zip 发送；zip 内保留原文件名）")
                if pending_list:
                    msg_lines.append("已提交（未确认回包）： " + ", ".join(pending_list))
                    msg_lines.append("（若你已在 QQ 里看到文件卡片，可忽略本提示）")
                if bad_list:
                    msg_lines.append("失败： " + ", ".join(bad_list))
                    msg_lines.append("（提示：除挂载外，retcode=1200 + rich media transfer failed 往往是 QQ 账号风控/群文件权限/容量问题）")
            await reply(api, ctx, "\n".join(msg_lines) if msg_lines else "没有发送任何文件。", logsvc)
            return
        finally:
            _cleanup_temp_files(temp_artifacts, logsvc=logsvc)
    # 未知命令
    await reply(api, ctx, f"未知命令：/{cmd}（用 /help 查看）", logsvc)

async def dispatch(
    api,
    ctx,
    evt: dict,
    text: str,
    filesvc: FileService,
    logsvc: LogService,
    state: BotState,
    handin: HandinService,
    perm=None,
    aisvc: Optional["AIService"] = None,
    vision_skill=None,
    calendar_service=None,
):
    try:
        setattr(ctx, "_ai_chat_context_aisvc", aisvc)
    except Exception:
        pass
    _sweep_bot_state_ttl(state)
    await _ensure_group_context_and_schedule_digest(api, ctx, evt, text, logsvc, state, handin, aisvc)
    if await _handle_pre_dispatch_state(api, ctx, evt, text, logsvc, state, handin, filesvc, aisvc):
        return
    raw_text = str(text or "").strip()
    # 视觉 slot 架构：创建当前消息的 VisionSlot（不调视觉 API），
    # 仅在 AI 触发回复时统一补解析（见 _handle_ai_chat_trigger）。
    current_slots: list = []
    message_id = str(evt.get("message_id") or "").strip()
    vision_skill_ready = vision_skill is not None and getattr(vision_skill, "ready", False)
    if vision_skill_ready:
        current_slots = vision_skill.create_slots_from_event(evt, message_id=message_id)
        # 引用（reply）：被引用消息在历史内 → 直接依赖历史 _vision；历史外 → get_msg 建 reference slot
        reply_id = _extract_reply_msg_id(evt)
        if reply_id:
            sess = _ai_chat_session_key(ctx)
            found = None
            if sess and aisvc is not None and callable(getattr(aisvc, "find_chat_message_by_msg_id", None)):
                try:
                    found = aisvc.find_chat_message_by_msg_id(sess, reply_id)
                except Exception:
                    found = None
            if not (found and found.get("_vision")):
                reply_slots: list = []
                if api is not None and callable(getattr(api, "call", None)):
                    try:
                        resp = await api.call("get_msg", {"message_id": int(reply_id)}, timeout=15.0)
                        data = (resp or {}).get("data") or {}
                        ref_msg = data.get("message")
                        if isinstance(ref_msg, list):
                            reply_slots = vision_skill.create_slots_from_event(
                                {"message": ref_msg},
                                message_id=f"ref-{reply_id}",
                                source_kind="reply_reference",
                            )
                    except Exception:
                        reply_slots = []
                current_slots = list(current_slots) + list(reply_slots)
    has_visual = bool(current_slots)
    # AI 触发判断（提前）：当前请求图片不受 capture 开关影响
    ai_triggered = False
    if not raw_text.startswith(("/", "／")):
        ai_triggered = (
            extract_ai_chat_trigger_text(
                ctx,
                evt,
                raw_text,
                has_visual=has_visual,
                bot_nick=(aisvc.bot_nick if aisvc else AI_BOT_NICK),
            )
            is not None
        )
    # VISION_CAPTURE_CONTEXT_IMAGES=false：普通非 AI 消息的视觉内容不保存
    if (not ai_triggered) and vision_skill is not None and (not vision_skill.capture_context_images):
        current_slots = []
        has_visual = False
    enriched_text = raw_text
    if not enriched_text and not has_visual:
        return
    # 记录 IN（只有最终 log_out 才会落盘）
    logsvc.log_in(ctx, raw_text or "[image]")
    non_ai_remembered = False

    def _remember_non_ai_once() -> None:
        nonlocal non_ai_remembered
        if non_ai_remembered:
            return
        if enriched_text or current_slots:
            _remember_non_ai_chat_message(
                ctx,
                enriched_text,
                logsvc,
                aisvc,
                msg_id=message_id,
                vision_slots=current_slots,
            )
        non_ai_remembered = True

    t = raw_text
    if t.startswith(("/", "／")):
        if _is_known_explicit_command(t) or int(getattr(ctx, "level", 0) or 0) < 1:
            _remember_non_ai_once()
            await _handle_explicit_command(api, ctx, t, filesvc, logsvc, state, handin, perm, aisvc, calendar_service)
            return
        if await _handle_ai_chat_trigger(
            api, ctx, evt, t, logsvc, aisvc,
            forced_ai_input=t,
            vision_skill=vision_skill,
            current_slots=current_slots,
            message_id=message_id,
        ):
            return
        _remember_non_ai_once()
        await reply(api, ctx, "未知命令（用 /help 查看）", logsvc)
        return
    if await _handle_plain_text_input(
        api,
        ctx,
        evt,
        t,
        logsvc,
        state,
        business_only=True,
        before_handle=_remember_non_ai_once,
        has_visual=has_visual,
    ):
        return
    if await _handle_ai_chat_trigger(
        api, ctx, evt, t, logsvc, aisvc,
        vision_skill=vision_skill,
        current_slots=current_slots,
        message_id=message_id,
    ):
        return
    _remember_non_ai_once()
    if await _handle_plain_text_input(api, ctx, evt, t, logsvc, state):
        return

async def _handle_private_done_batch(api, ctx, text: str, logsvc: LogService, state: BotState, handin: HandinService) -> bool:
    """处理私聊批量文件的 done 指令：进入“等待 zip 命名”阶段。"""
    t = (text or "").strip()
    if not re.fullmatch(r"(?i)/?done", t):
        return False
    if not state.pending_handin_wait_done.get(ctx.user_id):
        return False
    logsvc.log_in(ctx, t)
    if state.pending_handin_overwrite.get(ctx.user_id):
        await reply(api, ctx, "你有一个待确认的覆盖操作，请先回复 Y/N。", logsvc)
        return True
    q = state.pending_handin_files.get(ctx.user_id) or []
    if not q:
        state.pending_handin_wait_done.pop(ctx.user_id, None)
        state.pending_handin_zip_name.pop(ctx.user_id, None)
        state.pending_handin_name_input.pop(ctx.user_id, None)
        state.pending_handin_choose.pop(ctx.user_id, None)
        await reply(api, ctx, "没有待处理的提交文件了。", logsvc)
        return True
    # 仅 1 个文件时无需打包，直接回到任务选择
    if len(q) == 1:
        state.pending_handin_wait_done.pop(ctx.user_id, None)
        state.pending_handin_zip_name.pop(ctx.user_id, None)
        tasks = handin.list_active_tasks()
        if not tasks:
            state.pending_handin_name_input.pop(ctx.user_id, None)
            state.pending_handin_choose.pop(ctx.user_id, None)
            await reply(api, ctx, "当前没有正在进行的提交任务。", logsvc)
            return True
        one_name = str(q[0].get("name") or Path(str(q[0].get("path") or "")).name)
        roster_name = handin.find_roster_name_in_filename(one_name)
        if not roster_name:
            state.pending_handin_name_input[ctx.user_id] = {"ts": time.time()}
            state.pending_handin_choose.pop(ctx.user_id, None)
            await reply(api, ctx, "当前仅有 1 个文件，无需打包。\n未在文件名中识别到班级名册姓名，请回复提交者姓名（或回复 0 跳过）。", logsvc)
            return True
        state.pending_handin_name_input.pop(ctx.user_id, None)
        _set_pending_handin_submit_choice(api, ctx, logsvc, state, [tt.task_id for tt in tasks])
        await reply(api, ctx, f"当前仅有 1 个文件，无需打包。\n已识别到姓名：{roster_name}。\n" + _handin_tasks_list_text(tasks), logsvc)
        return True
    # 多文件：先询问 zip 名称
    suggested = _suggest_batch_zip_basename(q, ctx.user_id)
    state.pending_handin_wait_done.pop(ctx.user_id, None)
    state.pending_handin_name_input.pop(ctx.user_id, None)
    state.pending_handin_zip_name[ctx.user_id] = {"ts": time.time(), "suggested": suggested}
    await reply(
        api,
        ctx,
        f"请回复压缩包名称（无需 .zip）。\n例如：{suggested}\n请在文件名中包含姓名信息，若不需要姓名信息或者是小组作业请忽略。\n我会用你的回复作为 zip 名，再让你选择归档任务。",
        logsvc,
    )
    return True
async def _handle_private_zip_name_input(api, ctx, text: str, logsvc: LogService, state: BotState, handin: HandinService) -> bool:
    """处理私聊批量文件打包命名输入。"""
    pend = state.pending_handin_zip_name.get(ctx.user_id)
    if not pend:
        return False
    t = (text or "").strip()
    if not t:
        return False
    logsvc.log_in(ctx, t)
    if state.pending_handin_overwrite.get(ctx.user_id):
        await reply(api, ctx, "你有一个待确认的覆盖操作，请先回复 Y/N。", logsvc)
        return True
    if t in ("0", "取消", "/cancel", "／cancel"):
        q_cancel = state.pending_handin_files.get(ctx.user_id) or []
        for it in q_cancel:
            try:
                Path(str(it.get("path") or "")).unlink(missing_ok=True)
            except Exception as e:
                logsvc.log.warning(f"cleanup pending handin file failed: user={ctx.user_id} item={it} err={e}")
        state.pending_handin_files[ctx.user_id] = []
        state.pending_handin_wait_done.pop(ctx.user_id, None)
        state.pending_handin_zip_name.pop(ctx.user_id, None)
        state.pending_handin_name_input.pop(ctx.user_id, None)
        state.pending_handin_choose.pop(ctx.user_id, None)
        await reply(api, ctx, "已取消并删除全部临时文件。", logsvc)
        return True
    q = state.pending_handin_files.get(ctx.user_id) or []
    if not q:
        state.pending_handin_wait_done.pop(ctx.user_id, None)
        state.pending_handin_zip_name.pop(ctx.user_id, None)
        state.pending_handin_name_input.pop(ctx.user_id, None)
        state.pending_handin_choose.pop(ctx.user_id, None)
        await reply(api, ctx, "没有待处理的提交文件了。", logsvc)
        return True
    raw_name = t.lstrip("/／").strip()
    if raw_name.lower().endswith(".zip"):
        raw_name = raw_name[:-4].strip()
    default_name = str(pend.get("suggested") or _suggest_batch_zip_basename(q, ctx.user_id))
    base = _safe_zip_label(raw_name, default=default_name)
    if not base:
        base = default_name
    out_dir = DATA_DIR / "temp" / "handin_batch"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_zip = out_dir / f"{base}.zip"
    if out_zip.exists():
        i = 2
        while i < 1000:
            p2 = out_dir / f"{base}_{i}.zip"
            if not p2.exists():
                out_zip = p2
                break
            i += 1
    ok_zip, msg_zip, packed, missing = _zip_pending_files(q, out_zip, logsvc=logsvc)
    if not ok_zip:
        await reply(api, ctx, msg_zip, logsvc)
        return True
    # 打包成功后删除原临时文件，仅保留 zip
    for it in q:
        try:
            Path(str(it.get("path") or "")).unlink(missing_ok=True)
        except Exception as e:
            logsvc.log.warning(f"remove source after zip failed: user={ctx.user_id} item={it} err={e}")
    source_names = [str(it.get("name") or Path(str(it.get("path") or "")).name) for it in q]
    state.pending_handin_files[ctx.user_id] = [{
        "path": str(out_zip),
        "name": out_zip.name,
        "source_names": source_names,
        "ts": time.time(),
    }]
    state.pending_handin_wait_done.pop(ctx.user_id, None)
    state.pending_handin_zip_name.pop(ctx.user_id, None)
    state.pending_handin_name_input.pop(ctx.user_id, None)
    tasks = handin.list_active_tasks()
    if not tasks:
        state.pending_handin_choose.pop(ctx.user_id, None)
        await reply(api, ctx, f"已将 {packed} 个文件打包为：{out_zip.name}\n当前没有正在进行的提交任务。", logsvc)
        return True
    _set_pending_handin_submit_choice(api, ctx, logsvc, state, [tt.task_id for tt in tasks])
    lines = [f"已将 {packed} 个文件打包为：{out_zip.name}。"]
    if missing > 0:
        lines.append(f"另有 {missing} 个文件未找到，已跳过。")
    lines.append(_handin_tasks_list_text(tasks))
    await reply(api, ctx, "\n".join(lines), logsvc)
    return True
