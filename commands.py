
# commands.py
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING
import asyncio
import re
import time
import shutil
import uuid
import unicodedata
from filesvc import FileService
from logsvc import LogService
from handinsvc import HandinService, parse_mmdd_hhmm, pretty_ts, extract_name_from_filename, extract_student_id
from router import get_files
from ziputil import open_fast_zip, write_path as zip_write_path
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
_GROUP_NOTICE_FILE_SUFFIXES = {".pdf", ".doc", ".docx"}
_URL_RE = re.compile(r"(https?://[^\s<>\"]+)", flags=re.IGNORECASE)
_GROUP_NOTICE_MAX_CANDIDATES = 3
_GROUP_NOTICE_DEDUP_SECONDS = 60.0
_RECENT_REPLY_DEDUP_SECONDS = 2.0
_RECENT_REPLY_KEYS: Dict[str, float] = {}
_STATE_SWEEP_MIN_INTERVAL_SECONDS = 30.0
_STATE_TTL_LAST_FIND_SECONDS = 30.0 * 60.0
_STATE_TTL_PENDING_HANDIN_SECONDS = 6.0 * 60.0 * 60.0
_STATE_TTL_GROUP_NOTICE_SECONDS = 10.0 * 60.0


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
def _lookup_fixed_answers(text: str) -> List[str]:
    _reload_answer_cache_if_needed()
    return list(_ANSWER_CACHE.get(_normalize_answer_q(text), []))
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


def _clear_pending_handin_user(state: BotState, user_id: int) -> None:
    state.pending_handin_files.pop(user_id, None)
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

    stale_notice_before = now_ts - _STATE_TTL_GROUP_NOTICE_SECONDS
    for k, ts in list(state.recent_group_notice_keys.items()):
        try:
            if float(ts) < stale_notice_before:
                state.recent_group_notice_keys.pop(k, None)
        except Exception:
            state.recent_group_notice_keys.pop(k, None)


def conv_key(ctx) -> str:
    # 文件检索结果最好按“人”隔离，避免群里互相覆盖
    if ctx.scene == "group" and ctx.group_id is not None:
        return f"g:{ctx.group_id}:{ctx.user_id}"
    return f"p:{ctx.user_id}:{ctx.scene}"


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

    def _ok(resp) -> bool:
        if not isinstance(resp, dict):
            return False
        try:
            return resp.get("status") == "ok" and int(resp.get("retcode", 0) or 0) == 0
        except Exception:
            return False

    def _detail(resp) -> str:
        if not isinstance(resp, dict):
            return "no response"
        rc = resp.get("retcode", "")
        msg = (resp.get("wording") or resp.get("message") or "").strip()
        if msg:
            return f"retcode={rc} {msg}"
        return f"retcode={rc}" if rc != "" else "send failed"

    target = f"g:{send_group_id}" if send_scene == "group" and send_group_id is not None else f"u:{send_user_id}"
    reply_key = f"{send_scene}:{target}:{text.strip()}"
    if not _claim_recent_reply(reply_key):
        logsvc.log.info(f"消息发送去重：已拦截重复回复 target={target}")
        return

    resp = await _send_once()
    if resp is None:
        logsvc.log.info(
            f"消息发送未确认：scene={send_scene}, group={send_group_id}, user={send_user_id}，为避免重复发送不再重试"
        )
        return
    if not _ok(resp):
        # transient network / bridge timeout retry once
        await asyncio.sleep(0.35)
        resp = await _send_once()

    if _ok(resp):
        logsvc.log_out(ctx, text)
    else:
        logsvc.log.warning(
            f"reply send failed: scene={send_scene}, group={send_group_id}, user={send_user_id}, detail={_detail(resp)}"
        )


def _split_args(text: str):
    parts = text.strip().split()
    cmd = parts[0]
    rest = " ".join(parts[1:]).strip() if len(parts) > 1 else ""
    return cmd, rest


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
    if scene == "group":
        nick_aliases = [x for x in {str(bot_nick or "").strip(), "Cooper_bot", "Cooepr_bot"} if x]
        has_nick_mention = False
        for nick in nick_aliases:
            pat = rf"[@\uFF20]\s*{re.escape(nick)}"
            if re.search(pat, msg, flags=re.IGNORECASE):
                has_nick_mention = True
                msg = re.sub(pat, "", msg, flags=re.IGNORECASE).strip()
        if not (_evt_mentions_me(evt) or has_nick_mention):
            return None
        return msg
    if scene.startswith("private"):
        m = re.match(r"^[cC](.*)$", msg)
        if not m:
            return None
        return (m.group(1) or "").strip()
    return None


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
        lines.append(f"{i}. {t.name}（群 {t.group_id}，截止 {pretty_ts(t.deadline_ts)}）")
    lines.append("回复数字选择；回复 0 取消（删除临时文件）。")
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
    q.append({"path": str(p), "name": fname, "ts": time.time()})
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
            state.pending_handin_choose[ctx.user_id] = {"mode": "submit", "task_ids": [t.task_id for t in tasks], "ts": time.time()}
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
        state.pending_handin_choose[ctx.user_id] = {"mode": "submit", "task_ids": [t.task_id for t in tasks], "ts": time.time()}
        return True
    # 多文件：仍按原有任务选择流程（若继续发送会自动转 done 打包）
    lines = [msg, "检测到你发送了文件提交。", _handin_tasks_list_text(tasks)]
    await reply(api, ctx, "\n".join(lines), logsvc)
    state.pending_handin_choose[ctx.user_id] = {"mode": "submit", "task_ids": [t.task_id for t in tasks], "ts": time.time()}
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
        ok, msg2, dst, code = handin.move_inbox_to_task(Path(item.get("path")), task, overwrite=True)
        if ok:
            q.pop(item_idx)
            state.pending_handin_files[ctx.user_id] = q
            state.pending_handin_overwrite.pop(ctx.user_id, None)
            name = Path(dst).name if dst else (item.get("name") or "")
            nm = extract_name_from_filename(name)
            sid = extract_student_id(name)
            warn = ""
            await reply(api, ctx, msg2 + warn, logsvc)
        else:
            # 覆盖失败：保留文件，让用户重新选择或取消
            state.pending_handin_overwrite.pop(ctx.user_id, None)
            await reply(api, ctx, f"{msg2}\n你可以重新回复任务序号，或回复 0 取消该文件。", logsvc)
    # 若还有文件继续分配
    if state.pending_handin_files.get(ctx.user_id):
        tasks = handin.list_active_tasks()
        if tasks:
            state.pending_handin_name_input.pop(ctx.user_id, None)
            state.pending_handin_choose[ctx.user_id] = {"mode": "submit", "task_ids": [t.task_id for t in tasks], "ts": time.time()}
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
        state.pending_handin_choose[ctx.user_id] = {"mode": "submit", "task_ids": [tt.task_id for tt in tasks], "ts": time.time()}
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
    state.pending_handin_choose[ctx.user_id] = {"mode": "submit", "task_ids": [tt.task_id for tt in tasks], "ts": time.time()}
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
        ok, msg2, dst, code = handin.move_inbox_to_task(Path(item["path"]), task, overwrite=False)
        if (not ok) and code == "EXISTS":
            # 等待 Y/N
            state.pending_handin_overwrite[ctx.user_id] = {"task_id": tid, "path": str(item["path"]), "name": item.get("name") or "", "ts": time.time()}
            state.pending_handin_choose.pop(ctx.user_id, None)
            await reply(api, ctx, f"{msg2}\n是否覆盖？(Y/N)", logsvc)
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
            state.pending_handin_choose[ctx.user_id] = {"mode": "submit", "task_ids": [t.task_id for t in tasks], "ts": time.time()}
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
    if ctx.level < 3 and int(task.creator_id) != int(ctx.user_id):
        state.pending_handin_choose.pop(ctx.user_id, None)
        await reply(api, ctx, "权限不足：只能取消你创建的任务（或联系管理员）。", logsvc)
        return True
    ok, msg2 = handin.cancel_task(tid, ctx.user_id)
    state.pending_handin_choose.pop(ctx.user_id, None)
    await reply(api, ctx, msg2, logsvc)
    return True
async def _handle_find_folder_number_choice(api, ctx, text: str, logsvc: LogService, state: BotState) -> bool:
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
        await reply(api, ctx, "该条目已不存在，请重新 /find。", logsvc)
        return True
    if p.is_file():
        await reply(api, ctx, f"「{p.name}」是文件，请用 /get {idx} 获取。", logsvc)
        return True
    if not p.is_dir():
        return False
    try:
        entries = list(p.iterdir())
    except Exception as e:
        await reply(api, ctx, f"读取目录失败：{e}", logsvc)
        return True
    entries.sort(key=lambda x: (not x.is_dir(), x.name.lower()))
    has_more = len(entries) > int(LS_LIMIT)
    entries = entries[: int(LS_LIMIT)]
    # 下钻后刷新 /get 的候选列表，支持继续按数字进入下一层目录。
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
):
    if ctx.scene.startswith("private"):
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
):
    ai_input = _extract_ai_chat_input(ctx, evt, t, bot_nick=(aisvc.bot_nick if aisvc else AI_BOT_NICK))
    if ai_input is not None:
        if not ai_input:
            await reply(api, ctx, "想聊点啥？群里@我后直接说，私聊消息前加 C。", logsvc)
            return True
        if (aisvc is None) or (not aisvc.chat_ready):
            await reply(api, ctx, "AI 聊天暂时不可用（配置未就绪）。", logsvc)
            return True
        try:
            session_key = _ai_chat_session_key(ctx)
            if session_key:
                out = (await aisvc.chat_with_context(session_key, ai_input)).strip()
            else:
                out = (await aisvc.chat(ai_input)).strip()
            if not out:
                out = "我这边没收到有效回复，稍后再试一次。"
            await reply(api, ctx, out, logsvc)
        except Exception:
            await reply(api, ctx, aisvc.fallback_error_reply, logsvc)
        return True
    return False

async def _handle_plain_text_input(
    api,
    ctx,
    t: str,
    logsvc: LogService,
    state: BotState,
):
    if not (t.startswith("/") or t.startswith("／")):
        handled = await _handle_find_folder_number_choice(api, ctx, t, logsvc, state)
        if handled:
            return True
        fixed_answers = _lookup_fixed_answers(t)
        if fixed_answers:
            for msg in fixed_answers:
                await reply(api, ctx, msg, logsvc)
            return True
        return True
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
):
    t = t[1:]  # 去掉 /
    cmd, rest = _split_args(t)
    cmd = cmd.lower()
    if cmd in ("ping",):
        await reply(api, ctx, "pong", logsvc)
        return
    if cmd in ("whoami",):
        g = ctx.group_id if ctx.group_id is not None else "None"
        await reply(api, ctx, f"scene={ctx.scene}, user={ctx.nickname}-{ctx.user_id}, group={g}, level={ctx.level}", logsvc)
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
    if cmd in ("help", "h"):
        lines = [
            "命令速览：",
            "基础：",
            "/help 或 /h",
            "/ping",
            "/whoami",
        ]
        if ctx.level >= 3:
            lines.extend([
                "",
                "管理功能：",
                "/level list",
                "/level QQ号 等级",
            ])
        if ctx.level >= 1:
            lines.extend([
                "",
                "资料检索：",
                "/ls [root/子目录]  查看目录",
                "/find 关键词 [可选: root/子目录]  关键词搜索",
                '/find "需求"  语义检索（支持中英文单双引号，最多返回10条）',
                "/get 序号（如 /get 1 2 3 4）  获取文件/文件夹（文件夹自动打包）",
                "提示：可直接回复序号进入下级目录。",
            ])
        lines.extend([
            "",
            "AI聊天：",
            "群聊：@Cooepr_bot + 内容",
            "私聊：C + 内容（大小写都可）",
        ])
        if ctx.level >= 2:
            lines.extend([
                "",
                "提交功能：",
                "/handin 任务名 [提醒时间...] 截止时间（仅群聊）",
                "/handinstatus  查看任务并查询未交",
                "/handincheck  查看你创建任务的已交文件（可配合 /get）",
                "/handinget  打包你创建任务的已交文件并发送",
                "/chandin  取消提交任务（按提示回复序号）",
                "私聊发送文件后按提示操作；多文件发完后回复 done。",
            ])
        msg = "\n".join(lines)
        await reply(api, ctx, msg, logsvc)
        return
    # Handin commands
    if cmd == "handin":
        if ctx.level < 2:
            await reply(api, ctx, "权限不足：/handin 仅对 2 级及以上开放。", logsvc)
            return
        if ctx.scene != "group" or ctx.group_id is None:
            await reply(api, ctx, "/handin 只能在群聊中使用。", logsvc)
            return
        # 格式：/handin 任务名 [提醒时间...] 截止时间
        # 时间用两段：月.日 时:分（冒号中英文都兼容）。提醒时间可不填或填多个；最后一组时间为截止时间。
        # 示例：/handin 作业1 1.22 18:30 1.23 20:00 1.24 23:59
        parts = rest.split()
        if len(parts) < 3 or ((len(parts) - 1) % 2 != 0):
            await reply(
                api,
                ctx,
                "用法：/handin 任务名 [月.日 时:分 ...] 月.日 时:分\n"
                "示例：/handin 作业1 1.22 18:30 1.23 20:00 1.24 23:59\n"
                "（提醒时间可不填或填多个；最后一组时间为截止时间；任务名不能有空格；冒号中英文都兼容）",
                logsvc,
            )
            return
        task_name = parts[0]
        now = time.time()
        ts_list = []
        for i in range(1, len(parts), 2):
            s = f"{parts[i]} {parts[i+1]}"
            ts = parse_mmdd_hhmm(s, now)
            if ts is None:
                await reply(api, ctx, f"时间格式不对：{s}\n请用 月.日 时:分，例如 1.22 18:30（冒号中英文都行）。", logsvc)
                return
            ts_list.append(ts)
        deadline_ts = ts_list[-1]
        remind_list = ts_list[:-1]  # 可为空或多个
        ok, msg2 = handin.create_task(ctx.group_id, ctx.user_id, task_name, remind_list, deadline_ts)
        await reply(api, ctx, msg2, logsvc)
        return
    if cmd == "handinstatus":
        if ctx.level < 2:
            await reply(api, ctx, "权限不足：/handinstatus 仅对 2 级及以上开放。", logsvc)
            return
        # 允许查询已截止任务：用于统计未交/导出等（提交仍只允许进行中）
        if ctx.scene == "group" and ctx.group_id is not None:
            tasks = handin.list_tasks_by_group(ctx.group_id, include_closed=True)
        else:
            tasks = handin.list_tasks(include_closed=True)
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
        # 进行中优先，其次按截止时间倒序
        tasks.sort(key=lambda t: (0 if t.is_active(now) else 1, -float(t.deadline_ts)))
        text_list = ["提交任务列表："]
        for i, tsk in enumerate(tasks, 1):
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
        tasks = handin.list_tasks_by_creator(ctx.user_id, include_closed=True)
        # 仅保留仍可 /handinget 的任务（归档未被清理）
        tasks = [t for t in tasks if handin.is_task_gettable(t)]
        if not tasks:
            await reply(api, ctx, "你当前没有提交任务记录。", logsvc)
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
        text_list = ["你创建的提交任务列表："]
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
        tasks = handin.list_tasks_by_creator(ctx.user_id, include_closed=True)
        # 仅保留仍可 /handinget 的任务（归档未被清理）
        tasks = [t for t in tasks if handin.is_task_gettable(t)]
        if not tasks:
            await reply(api, ctx, "你当前没有提交任务记录。", logsvc)
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
        text_list = ["你创建的提交任务列表："]
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
        # 群里默认只列本群任务；私聊则列“你创建的任务”（管理员可列全部）
        if ctx.scene == "group" and ctx.group_id is not None:
            tasks = handin.list_active_tasks_by_group(ctx.group_id)
            pend_gid = int(ctx.group_id)
        else:
            all_tasks = handin.list_active_tasks()
            if ctx.level >= 3:
                tasks = all_tasks
            else:
                tasks = [t for t in all_tasks if int(t.creator_id) == int(ctx.user_id)]
            pend_gid = None
        if not tasks:
            await reply(api, ctx, "当前没有可取消的提交任务。", logsvc)
            return
        text_list = ["当前可取消的提交任务列表："]
        for i, tsk in enumerate(tasks, 1):
            text_list.append(f"{i}. {tsk.name}（群 {tsk.group_id}，截止 {pretty_ts(tsk.deadline_ts)}）")
        text_list.append("回复数字取消该任务；回复 0 取消操作。")
        text_list.append("（提示：仅允许取消你创建的任务。）")
        await reply(api, ctx, "\n".join(text_list), logsvc)
        state.pending_handin_choose[ctx.user_id] = {"mode": "cancel", "task_ids": [t.task_id for t in tasks], "group_id": pend_gid, "ts": time.time()}
        return
        return
    # 文件相关命令：游客(0)直接拒绝
    if cmd in ("ls", "find", "get") and ctx.level < 1:
        await reply(api, ctx, "权限不足：你当前是 0 级（游客），不能访问资料库。", logsvc)
        return
    if cmd == "ls":
        ok, out = filesvc.list_dir(ctx, rest if rest else None)
        await reply(api, ctx, out, logsvc)
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
            if (aisvc is None) or (not aisvc.semantic_ready):
                await reply(api, ctx, "AI 语义检索暂时不可用（向量库或配置未就绪）。", logsvc)
                return
            try:
                hits = await aisvc.semantic_find_paths(kw, limit=10)
            except Exception:
                await reply(api, ctx, aisvc.fallback_error_reply, logsvc)
                return
        else:
            kw, in_dir = _parse_find_args(rest, filesvc)
            try:
                hits = await asyncio.to_thread(filesvc.find, ctx, kw, in_dir=in_dir)
            except Exception as e:
                logsvc.log.exception(f"/find failed: kw={kw!r} in_dir={in_dir!r} err={e}")
                await reply(api, ctx, "搜索失败，请稍后再试。", logsvc)
                return
        k = conv_key(ctx)
        _mark_last_find_cache(state, k, hits, kw)
        if not hits:
            if semantic_mode:
                await reply(api, ctx, "没找到符合语义的文件，试试换个说法或用普通关键词 /find。", logsvc)
            else:
                await reply(api, ctx, "没找到匹配文件或文件夹。", logsvc)
            return
        dir_lines: List[str] = []
        file_lines: List[str] = []
        has_large = False
        for i, p in enumerate(hits, 1):
            if p.is_dir():
                dir_lines.append(f"{i}. 📁 {p.name}/")
                continue
            suffix = ""
            try:
                sz = int(p.stat().st_size)
                if _is_large(sz):
                    suffix = f" （{_fmt_mb(sz)}，大文件）"
                    has_large = True
            except Exception as e:
                logsvc.log.warning(f"/find stat failed: path={p} err={e}")
            file_lines.append(f"{i}. 📄 {p.name}{suffix}")
        lines = ["语义检索结果：" if semantic_mode else "搜索结果："]
        lines.append(f"📁 文件夹命中：")
        if dir_lines:
            lines.extend(dir_lines)
        else:
            lines.append("（无）")
        lines.append(f"📄 文件命中：")
        if file_lines:
            lines.extend(file_lines)
        else:
            lines.append("（无）")
        lines.append("用 /get 序号（如/get 1 2 3 4）获取文件；文件夹会先打包成 zip。")
        lines.append("直接回复序号可进入目录并继续按数字下钻。")
        if has_large:
            lines.append("标记“大文件”的条目发送可能较慢，请耐心等待。")
        lines.append("\n新！增！功！能！\n/find \"需求\"现在可以ai智能查找了！！！")
        await reply(api, ctx, "\n".join(lines), logsvc)
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
):
    _sweep_bot_state_ttl(state)
    await _ensure_group_context_and_schedule_digest(api, ctx, evt, text, logsvc, state, handin, aisvc)
    if await _handle_pre_dispatch_state(api, ctx, evt, text, logsvc, state, handin, filesvc):
        return
    t = (text or "").strip()
    if not t:
        return
    # 记录 IN（只有最终 log_out 才会落盘）
    logsvc.log_in(ctx, t)
    if await _handle_ai_chat_trigger(api, ctx, evt, t, logsvc, aisvc):
        return
    if await _handle_plain_text_input(api, ctx, t, logsvc, state):
        return
    await _handle_explicit_command(api, ctx, t, filesvc, logsvc, state, handin, perm, aisvc)

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
        state.pending_handin_choose[ctx.user_id] = {"mode": "submit", "task_ids": [tt.task_id for tt in tasks], "ts": time.time()}
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
    state.pending_handin_files[ctx.user_id] = [{
        "path": str(out_zip),
        "name": out_zip.name,
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
    state.pending_handin_choose[ctx.user_id] = {"mode": "submit", "task_ids": [tt.task_id for tt in tasks], "ts": time.time()}
    lines = [f"已将 {packed} 个文件打包为：{out_zip.name}。"]
    if missing > 0:
        lines.append(f"另有 {missing} 个文件未找到，已跳过。")
    lines.append(_handin_tasks_list_text(tasks))
    await reply(api, ctx, "\n".join(lines), logsvc)
    return True
