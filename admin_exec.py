from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional, TYPE_CHECKING
import admin_targets

from admin_models import AdminPlan, AdminStep
from command_services import (
    cancel_handin_task_by_identity,
    CommandServiceFormatter,
    format_simple_failure,
    format_simple_success,
    get_handin_task_summary,
    list_handin_tasks_for_group,
    normalize_target_ids,
    run_find_query,
    run_list_dir_query,
)
from handinsvc import parse_mmdd_hhmm, pretty_ts

if TYPE_CHECKING:
    from aisvc import AIService
    from filesvc import FileService
    from handinsvc import HandinService
    from logsvc import LogService


@dataclass
class AdminExecutionContext:
    api: Any
    ctx: Any = None
    evt: dict = field(default_factory=dict)
    text: str = ""
    filesvc: Optional["FileService"] = None
    logsvc: Optional["LogService"] = None
    state: Any = None
    handin: Optional["HandinService"] = None
    perm: Any = None
    aisvc: Optional["AIService"] = None


@dataclass
class ToolResult:
    ok: bool
    summary: str
    detail: str = ""
    data: dict = field(default_factory=dict)


@dataclass
class ExecutionSummary:
    ok: bool
    message: str
    total_steps: int
    completed_steps: int
    failed_tool: Optional[str] = None


ToolHandler = Callable[[AdminExecutionContext, dict], Awaitable[ToolResult]]
MAX_ADMIN_PLAN_STEPS = 5


def _is_ok_response(resp) -> bool:
    if not isinstance(resp, dict):
        return False
    try:
        return resp.get("status") == "ok" and int(resp.get("retcode", 0) or 0) == 0
    except Exception:
        return False


def _format_response_detail(resp) -> str:
    if not isinstance(resp, dict):
        return "无响应"
    retcode = resp.get("retcode", "")
    msg = (resp.get("wording") or resp.get("message") or "").strip()
    if msg:
        return f"retcode={retcode} {msg}"
    if retcode != "":
        return f"retcode={retcode}"
    return "发送失败"


def _validate_positive_int(value: Any, field_name: str) -> tuple[Optional[int], str]:
    try:
        n = int(value)
    except Exception:
        return None, f"{field_name} 必须是正整数。"
    if n <= 0:
        return None, f"{field_name} 必须是正整数。"
    return n, ""


def _validate_message_text(value: Any) -> tuple[Optional[str], str]:
    text = str(value or "").strip()
    if not text:
        return None, "text 不能为空。"
    return text, ""


def _log_exec_warning(exec_ctx: AdminExecutionContext, msg: str) -> None:
    try:
        if exec_ctx.logsvc is not None and hasattr(exec_ctx.logsvc, "log"):
            exec_ctx.logsvc.log.warning(msg)
    except Exception:
        pass


def _pick_first_value(data: dict, *keys: str):
    for key in keys:
        if key in data:
            return data.get(key)
    return None


def _extract_message_arg(data: dict, fallback_text: str = "") -> Optional[str]:
    keys = ["text", "message", "msg", "content", "question", "input", "prompt", "query", "user_input"]
    for k in keys:
        if k in data and data[k] is not None:
            return str(data[k])
    skip_keys = {"chat_type", "chat_id", "group_id", "user_id", "target_id", "as_user_id"}
    for k, v in data.items():
        if k not in skip_keys and isinstance(v, str) and str(v).strip():
            return str(v)
    if str(fallback_text or "").strip():
        return str(fallback_text).strip()
    return None


def _resolve_chat_context(exec_ctx: AdminExecutionContext, args: dict) -> dict:
    data = dict(args if isinstance(args, dict) else {})
    ctx = exec_ctx.ctx

    if ctx is not None:
        scene = str(getattr(ctx, "scene", "") or "")
        default_type = "group" if (scene == "group" or getattr(ctx, "group_id", None)) else "private"
        if not str(data.get("chat_type") or "").strip():
            data["chat_type"] = default_type

        ct = str(data.get("chat_type") or "").strip().lower()
        if ct == "group":
            raw = _pick_first_value(data, "group_id", "chat_id", "target_id")
            if raw is None and getattr(ctx, "group_id", None):
                data["group_id"] = getattr(ctx, "group_id", None)
        elif ct == "private":
            raw = _pick_first_value(data, "user_id", "chat_id", "target_id")
            if raw is None and getattr(ctx, "user_id", None):
                data["user_id"] = getattr(ctx, "user_id", None)

    ct = str(data.get("chat_type") or "").strip().lower()
    if ct == "group":
        raw = _pick_first_value(data, "group_id", "chat_id", "target_id")
        if raw is not None and not isinstance(raw, int) and str(raw).strip() and not str(raw).strip().isdigit():
            rr = admin_targets.resolve_group_target(str(raw).strip(), logsvc=exec_ctx.logsvc)
            if rr.ok:
                data["group_id"] = int(rr.target_id)
                data["chat_id"] = int(rr.target_id)
    elif ct == "private":
        raw = _pick_first_value(data, "user_id", "chat_id", "target_id")
        if raw is not None and not isinstance(raw, int) and str(raw).strip() and not str(raw).strip().isdigit():
            rr = admin_targets.resolve_user_target(str(raw).strip(), logsvc=exec_ctx.logsvc)
            if rr.ok:
                data["user_id"] = int(rr.target_id)
                data["chat_id"] = int(rr.target_id)

    if "group_id" in data and not isinstance(data["group_id"], int):
        raw_str = str(data["group_id"]).strip()
        if raw_str and not raw_str.isdigit():
            rr = admin_targets.resolve_group_target(raw_str, logsvc=exec_ctx.logsvc)
            if rr.ok:
                data["group_id"] = int(rr.target_id)

    return data


def _to_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _render_find_result_text(*, keyword: str, in_dir: Optional[str], hits: list[Any]) -> str:
    scope = str(in_dir or "").strip()
    if not hits:
        if scope:
            return f"在 {scope} 下未找到匹配文件：{keyword}"
        return f"未找到匹配文件：{keyword}"
    lines = ["搜索结果："]
    for i, p in enumerate(hits, 1):
        try:
            is_dir = bool(p.is_dir())
        except Exception:
            is_dir = False
        icon = "📁" if is_dir else "📄"
        suffix = "/" if is_dir else ""
        name = str(getattr(p, "name", "") or str(p))
        lines.append(f"{i}. {icon} {name}{suffix}")
    return "\n".join(lines)


async def handle_send_message(exec_ctx: AdminExecutionContext, args: dict) -> ToolResult:
    data = _resolve_chat_context(exec_ctx, args)
    chat_type = str(data.get("chat_type") or "").strip().lower()
    text_val = _extract_message_arg(data, exec_ctx.text)
    text, err_text = _validate_message_text(text_val)
    if err_text:
        return ToolResult(ok=False, summary="", detail=err_text)

    normalized = normalize_target_ids(
        chat_type=chat_type,
        group_id=data.get("group_id"),
        user_id=data.get("user_id"),
        chat_id=data.get("chat_id"),
    )
    if not normalized.ok:
        err = format_simple_failure(str(normalized.message or "参数不合法。"), error_code=normalized.error_code)
        return ToolResult(ok=False, summary="", detail=err.message)

    target = normalized.data if isinstance(normalized.data, dict) else {}
    target_type = str(target.get("chat_type") or "")
    target_id = int(target.get("target_id") or 0)
    if target_type == "group":
        resp = await exec_ctx.api.send_group_msg(target_id, str(text))
    elif target_type == "private":
        resp = await exec_ctx.api.send_private_msg(target_id, str(text))
    else:
        err = format_simple_failure("chat_type 仅支持 group 或 private。", error_code="INVALID_CHAT_TYPE")
        return ToolResult(ok=False, summary="", detail=err.message)

    if _is_ok_response(resp):
        label = CommandServiceFormatter.format_target_label(target_type, target_id)
        ok_result = format_simple_success(f"向{label} 发送消息")
        return ToolResult(ok=True, summary=ok_result.message)

    err_result = format_simple_failure(_format_response_detail(resp), error_code="SEND_FAILED")
    return ToolResult(ok=False, summary="", detail=err_result.message)


async def handle_send_group_message(exec_ctx: AdminExecutionContext, args: dict) -> ToolResult:
    data = _resolve_chat_context(exec_ctx, args)
    return await handle_send_message(
        exec_ctx,
        {
            "chat_type": "group",
            "group_id": _pick_first_value(data, "group_id", "chat_id", "target_id"),
            "text": _extract_message_arg(data, exec_ctx.text),
        },
    )


async def handle_send_private_message(exec_ctx: AdminExecutionContext, args: dict) -> ToolResult:
    data = _resolve_chat_context(exec_ctx, args)
    return await handle_send_message(
        exec_ctx,
        {
            "chat_type": "private",
            "user_id": _pick_first_value(data, "user_id", "chat_id", "target_id"),
            "text": _extract_message_arg(data, exec_ctx.text),
        },
    )


async def handle_list_directory(exec_ctx: AdminExecutionContext, args: dict) -> ToolResult:
    data = _resolve_chat_context(exec_ctx, args)
    if exec_ctx.filesvc is None:
        return ToolResult(ok=False, summary="", detail="文件服务不可用。")

    path_arg = _pick_first_value(data, "path", "dir", "in_dir", "target")
    try:
        result = await asyncio.to_thread(
            run_list_dir_query,
            filesvc=exec_ctx.filesvc,
            ctx=exec_ctx.ctx,
            path_arg=path_arg,
        )
    except Exception as e:
        return ToolResult(ok=False, summary="", detail=f"列目录失败：{e}")
    if not result.ok:
        return ToolResult(ok=False, summary="", detail=str(result.message or "列目录失败。"))

    out = str(result.message or "").strip() or "目录为空。"
    return ToolResult(
        ok=True,
        summary=out,
        data={
            "text": out,
            "last_text": out,
            "list_result": out,
            "path_arg": (
                (result.data or {}).get("path_arg")
                if isinstance(result.data, dict)
                else None
            ),
        },
    )


async def handle_find_files(exec_ctx: AdminExecutionContext, args: dict) -> ToolResult:
    data = _resolve_chat_context(exec_ctx, args)
    if exec_ctx.filesvc is None:
        return ToolResult(ok=False, summary="", detail="文件服务不可用。")

    keyword = _pick_first_value(data, "keyword", "query", "q", "text")
    in_dir = _pick_first_value(data, "in_dir", "path", "scope", "dir")
    try:
        result = await asyncio.to_thread(
            run_find_query,
            filesvc=exec_ctx.filesvc,
            ctx=exec_ctx.ctx,
            keyword=keyword,
            in_dir=in_dir,
            require_keyword=True,
        )
    except Exception as e:
        return ToolResult(ok=False, summary="", detail=f"搜索失败：{e}")
    if not result.ok:
        return ToolResult(ok=False, summary="", detail=str(result.message or "搜索失败。"))

    payload = result.data if isinstance(result.data, dict) else {}
    hits = payload.get("hits")
    if not isinstance(hits, list):
        hits = []
    kw = str(payload.get("keyword") or "").strip()
    scope = payload.get("in_dir")
    out = _render_find_result_text(keyword=kw, in_dir=(str(scope).strip() if scope is not None else None), hits=hits)
    return ToolResult(
        ok=True,
        summary=out,
        data={
            "text": out,
            "last_text": out,
            "find_result": out,
            "hits": hits,
            "keyword": kw,
            "in_dir": scope,
            "paths": [str(p) for p in hits],
        },
    )


def _format_ambiguous_cancel_message(result) -> str:
    data = result.data if isinstance(result.data, dict) else {}
    matches = data.get("matches")
    if not isinstance(matches, list) or (not matches):
        return str(result.message or "任务不唯一，请改用 task_id。")
    lines = [str(result.message or "存在多个同名任务，请改用 task_id：")]
    for i, one in enumerate(matches, 1):
        if not isinstance(one, dict):
            continue
        tid = str(one.get("task_id") or "").strip()
        name = str(one.get("task_name") or "").strip()
        if tid:
            lines.append(f"{i}. {name or '(未命名任务)'} (task_id={tid})")
    return "\n".join(lines)


async def handle_list_handin_tasks(exec_ctx: AdminExecutionContext, args: dict) -> ToolResult:
    data = _resolve_chat_context(exec_ctx, args)
    if exec_ctx.handin is None:
        return ToolResult(ok=False, summary="", detail="handin 服务不可用。")

    group_id, err_gid = _validate_positive_int(data.get("group_id"), "group_id")
    if err_gid:
        return ToolResult(ok=False, summary="", detail=err_gid)

    include_closed = _to_bool(data.get("include_closed"), default=False)
    active_raw = data.get("active_only")
    active_only = _to_bool(active_raw, default=(not include_closed))
    only_gettable = _to_bool(data.get("only_gettable"), default=False)
    sort_mode = str(data.get("sort_mode") or "").strip() or "active_then_deadline_desc"

    try:
        result = await asyncio.to_thread(
            list_handin_tasks_for_group,
            handin=exec_ctx.handin,
            group_id=int(group_id),
            include_closed=include_closed,
            active_only=active_only,
            only_gettable=only_gettable,
            sort_mode=sort_mode,
        )
    except Exception as e:
        return ToolResult(ok=False, summary="", detail=f"查询 handin 任务失败：{e}")
    if not result.ok:
        return ToolResult(ok=False, summary="", detail=str(result.message or "查询 handin 任务失败。"))

    payload = result.data if isinstance(result.data, dict) else {}
    tasks = payload.get("tasks")
    if not isinstance(tasks, list):
        tasks = []
    gid = int(payload.get("group_id") or int(group_id))
    if not tasks:
        msg = f"群 {gid} 当前没有可用的 handin 任务。"
        return ToolResult(ok=True, summary=msg, data={"text": msg, "last_text": msg, "group_id": gid, "task_count": 0})

    now_ts = time.time()
    lines = [f"群 {gid} handin 任务列表："]
    task_rows: list[dict] = []
    for i, task in enumerate(tasks, 1):
        row = get_handin_task_summary(task, now_ts=now_ts, pretty_ts_func=pretty_ts, with_status=True, with_group=True)
        text = str(row.message or "").strip()
        lines.append(f"{i}. {text}")
        row_data = row.data if isinstance(row.data, dict) else {}
        task_rows.append(
            {
                "task_id": str(row_data.get("task_id") or getattr(task, "task_id", "") or ""),
                "task_name": str(row_data.get("task_name") or getattr(task, "name", "") or ""),
                "status": str(row_data.get("status") or ""),
            }
        )
    out = "\n".join(lines)
    return ToolResult(
        ok=True,
        summary=out,
        data={
            "text": out,
            "last_text": out,
            "group_id": gid,
            "task_count": len(task_rows),
            "tasks": task_rows,
        },
    )


async def handle_cancel_handin_task(exec_ctx: AdminExecutionContext, args: dict) -> ToolResult:
    data = _resolve_chat_context(exec_ctx, args)
    if exec_ctx.handin is None:
        return ToolResult(ok=False, summary="", detail="handin 服务不可用。")

    group_id, err_gid = _validate_positive_int(data.get("group_id"), "group_id")
    if err_gid:
        return ToolResult(ok=False, summary="", detail=err_gid)
    task_id = _pick_first_value(data, "task_id", "id")
    task_name = _pick_first_value(data, "task_name", "name")
    if (not str(task_id or "").strip()) and (not str(task_name or "").strip()):
        return ToolResult(ok=False, summary="", detail="缺少任务标识：请提供 task_id 或 task_name。")

    requester_id, err_uid = _validate_positive_int(getattr(exec_ctx.ctx, "user_id", 0), "by_user_id")
    if err_uid:
        return ToolResult(ok=False, summary="", detail=err_uid)
    try:
        requester_level = int(getattr(exec_ctx.ctx, "level", 0) or 0)
    except Exception:
        requester_level = 0

    try:
        result = await asyncio.to_thread(
            cancel_handin_task_by_identity,
            handin=exec_ctx.handin,
            group_id=int(group_id),
            by_user_id=int(requester_id),
            requester_level=requester_level,
            task_id=task_id,
            task_name=task_name,
        )
    except Exception as e:
        return ToolResult(ok=False, summary="", detail=f"取消 handin 任务失败：{e}")
    if not result.ok:
        if str(result.error_code or "") == "AMBIGUOUS_TASK_NAME":
            return ToolResult(ok=False, summary="", detail=_format_ambiguous_cancel_message(result))
        return ToolResult(ok=False, summary="", detail=str(result.message or "取消 handin 任务失败。"))

    payload = result.data if isinstance(result.data, dict) else {}
    msg = str(result.message or "").strip() or "已取消 handin 任务。"
    return ToolResult(
        ok=True,
        summary=msg,
        data={
            "text": msg,
            "last_text": msg,
            "group_id": int(payload.get("group_id") or int(group_id)),
            "task_id": str(payload.get("task_id") or ""),
            "task_name": str(payload.get("task_name") or ""),
        },
    )


def _normalize_clock_expr(value: str) -> str:
    s = str(value or "").strip()
    return s.replace("：", ":")


def _parse_deadline_text_to_ts(deadline_text: str, now_ts: float) -> tuple[Optional[float], str]:
    text = _normalize_clock_expr(deadline_text)
    if not text:
        return None, "deadline_text 不能为空。"
    ts = parse_mmdd_hhmm(text, now_ts)
    if ts is not None:
        return float(ts), ""

    m_tonight = re.fullmatch(r"今晚\s*(\d{1,2}):(\d{1,2})", text)
    if m_tonight:
        hh = int(m_tonight.group(1))
        mm = int(m_tonight.group(2))
        if hh > 23 or mm > 59:
            return None, f"时间格式不对：{deadline_text}"
        now_lt = time.localtime(now_ts)
        mmdd = f"{now_lt.tm_mon}.{now_lt.tm_mday} {hh}:{mm:02d}"
        ts2 = parse_mmdd_hhmm(mmdd, now_ts)
        if ts2 is None:
            return None, f"时间格式不对：{deadline_text}"
        # parse_mmdd_hhmm 在“同日已过期”时会跳到下一年，这里显式拦截。
        if float(ts2) - float(now_ts) > 48 * 3600:
            return None, f"时间已过：{deadline_text}"
        return float(ts2), ""

    return None, f"时间格式不对：{deadline_text}"


def _parse_reminder_list(
    reminders: list,
    *,
    now_ts: float,
    deadline_ts: float,
) -> tuple[Optional[list[float]], str]:
    if not reminders:
        return [], ""
    out: list[float] = []
    ddl_lt = time.localtime(deadline_ts)
    ddl_mmdd = f"{ddl_lt.tm_mon}.{ddl_lt.tm_mday}"
    for one in reminders:
        if isinstance(one, (int, float)):
            ts_num = float(one)
            if ts_num <= 0:
                return None, "提醒时间必须是正数时间戳。"
            out.append(ts_num)
            continue
        text = _normalize_clock_expr(str(one or ""))
        if not text:
            continue
        ts = parse_mmdd_hhmm(text, now_ts)
        if ts is None:
            m = re.fullmatch(r"(\d{1,2}):(\d{1,2})", text)
            if m:
                hh = int(m.group(1))
                mm = int(m.group(2))
                if hh > 23 or mm > 59:
                    return None, f"提醒时间格式不对：{one}"
                ts = parse_mmdd_hhmm(f"{ddl_mmdd} {hh}:{mm:02d}", now_ts)
        if ts is None:
            return None, f"提醒时间格式不对：{one}"
        out.append(float(ts))

    out = sorted(set(out))
    for ts in out:
        if float(ts) >= float(deadline_ts):
            return None, "提醒时间必须早于截止时间。"
    return out, ""


async def handle_generate_ai_reply(exec_ctx: AdminExecutionContext, args: dict) -> ToolResult:
    data = _resolve_chat_context(exec_ctx, args)
    if exec_ctx.aisvc is None or not bool(getattr(exec_ctx.aisvc, "chat_ready", False)):
        return ToolResult(ok=False, summary="", detail="AI 聊天服务不可用。")
    if not hasattr(exec_ctx.aisvc, "chat_with_context"):
        return ToolResult(ok=False, summary="", detail="AI 服务缺少 chat_with_context 能力。")

    chat_type = str(data.get("chat_type") or "").strip().lower()
    text_val = _extract_message_arg(data, exec_ctx.text)
    message, err_text = _validate_message_text(text_val)
    if err_text:
        return ToolResult(ok=False, summary="", detail=err_text)

    chat_id_raw = data.get("chat_id")
    if chat_type == "group":
        chat_id_raw = data.get("chat_id", data.get("group_id"))
    elif chat_type == "private":
        chat_id_raw = data.get("chat_id", data.get("user_id"))
    chat_id, err_id = _validate_positive_int(chat_id_raw, "chat_id")
    if err_id:
        return ToolResult(ok=False, summary="", detail=err_id)

    speaker_raw = data.get("as_user_id", getattr(exec_ctx.ctx, "user_id", 0))
    speaker_id, err_spk = _validate_positive_int(speaker_raw, "as_user_id")
    if err_spk:
        return ToolResult(ok=False, summary="", detail=err_spk)

    if chat_type == "group":
        session_key = f"group:{chat_id}"
        user_input = f"发言人QQ:{speaker_id}\n群号:{chat_id}\n{message}"
        summary = f"为群 {chat_id} 生成 AI 回复"
    elif chat_type == "private":
        session_key = f"private:{chat_id}"
        user_input = f"发言人QQ:{speaker_id}\n私聊对象:{chat_id}\n{message}"
        summary = f"为用户 {chat_id} 生成 AI 回复"
    else:
        return ToolResult(ok=False, summary="", detail="chat_type 仅支持 group 或 private。")

    try:
        out = str((await exec_ctx.aisvc.chat_with_context(session_key, user_input)) or "").strip()
    except Exception as e:
        return ToolResult(ok=False, summary="", detail=f"AI 生成失败：{e}")
    if not out:
        return ToolResult(ok=False, summary="", detail="AI 未返回有效回复。")
    return ToolResult(
        ok=True,
        summary=summary,
        data={
            "ai_reply": out,
            "last_text": out,
            "text": out,
        },
    )


async def handle_create_handin_task(exec_ctx: AdminExecutionContext, args: dict) -> ToolResult:
    data = _resolve_chat_context(exec_ctx, args)
    if exec_ctx.handin is None or not hasattr(exec_ctx.handin, "create_task"):
        return ToolResult(ok=False, summary="", detail="handin 服务不可用。")

    group_id, err_gid = _validate_positive_int(data.get("group_id"), "group_id")
    if err_gid:
        return ToolResult(ok=False, summary="", detail=err_gid)

    task_name = str(data.get("task_name") or "").strip()
    if not task_name:
        return ToolResult(ok=False, summary="", detail="task_name 不能为空。")

    creator_raw = data.get("creator_id", getattr(exec_ctx.ctx, "user_id", 0))
    creator_id, err_creator = _validate_positive_int(creator_raw, "creator_id")
    if err_creator:
        return ToolResult(ok=False, summary="", detail=err_creator)

    deadline_ts = None
    if data.get("deadline_ts") is not None:
        try:
            deadline_ts = float(data.get("deadline_ts"))
        except Exception:
            return ToolResult(ok=False, summary="", detail="deadline_ts 必须是数字。")
        if deadline_ts <= 0:
            return ToolResult(ok=False, summary="", detail="deadline_ts 必须为正数。")
    else:
        now_ts = time.time()
        deadline_text = str(data.get("deadline_text") or "").strip()
        deadline_ts, err_deadline = _parse_deadline_text_to_ts(deadline_text, now_ts)
        if err_deadline:
            return ToolResult(ok=False, summary="", detail=err_deadline)

    reminder_values = data.get("reminders")
    if reminder_values is None:
        reminder_values = []
    if not isinstance(reminder_values, list):
        return ToolResult(ok=False, summary="", detail="reminders 必须是列表。")
    remind_ts_list, err_remind = _parse_reminder_list(
        reminder_values,
        now_ts=time.time(),
        deadline_ts=float(deadline_ts),
    )
    if err_remind:
        return ToolResult(ok=False, summary="", detail=err_remind)

    try:
        ok, msg = exec_ctx.handin.create_task(
            int(group_id),
            int(creator_id),
            task_name,
            remind_ts_list,
            float(deadline_ts),
        )
    except Exception as e:
        return ToolResult(ok=False, summary="", detail=f"创建任务异常：{e}")
    if not ok:
        return ToolResult(ok=False, summary="", detail=str(msg or "创建任务失败。"))
    return ToolResult(
        ok=True,
        summary=f"在群 {group_id} 创建 handin 任务「{task_name}」",
        detail=str(msg or ""),
        data={
            "group_id": int(group_id),
            "task_name": task_name,
            "deadline_ts": float(deadline_ts),
            "deadline_pretty": pretty_ts(float(deadline_ts)),
        },
    )


TOOLS: dict[str, ToolHandler] = {
    "send_message": handle_send_message,
    "send_group_message": handle_send_group_message,
    "send_private_message": handle_send_private_message,
    "list_directory": handle_list_directory,
    "find_files": handle_find_files,
    "list_handin_tasks": handle_list_handin_tasks,
    "cancel_handin_task": handle_cancel_handin_task,
    "generate_ai_reply": handle_generate_ai_reply,
    "create_handin_task": handle_create_handin_task,
}


def _success_message(done_summaries: list[str]) -> str:
    if not done_summaries:
        return "已完成。"
    if len(done_summaries) == 1:
        return f"已完成：{done_summaries[0]}"
    lines = ["已完成："]
    for i, one in enumerate(done_summaries, 1):
        lines.append(f"{i}. {one}")
    return "\n".join(lines)


def _failure_summary(
    *,
    tool: str,
    reason: str,
    total_steps: int,
    completed_steps: int,
) -> ExecutionSummary:
    return ExecutionSummary(
        ok=False,
        message=f"执行失败：{tool}，原因：{reason}",
        total_steps=int(total_steps),
        completed_steps=int(completed_steps),
        failed_tool=tool,
    )


_VAR_RE = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")


def _render_template_text(text: str, runtime_vars: dict) -> str:
    def _replace(match: re.Match) -> str:
        key = str(match.group(1) or "")
        value = runtime_vars.get(key, "")
        return str(value if value is not None else "")

    return _VAR_RE.sub(_replace, str(text or ""))


def _resolve_step_args(value, runtime_vars: dict):
    if isinstance(value, dict):
        return {k: _resolve_step_args(v, runtime_vars) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_step_args(v, runtime_vars) for v in value]
    if isinstance(value, str):
        return _render_template_text(value, runtime_vars)
    return value


async def execute_plan(exec_ctx: AdminExecutionContext, plan: AdminPlan) -> ExecutionSummary:
    if not isinstance(plan, AdminPlan):
        return ExecutionSummary(
            ok=False,
            message="执行失败：plan，原因：计划格式不合法。",
            total_steps=0,
            completed_steps=0,
            failed_tool="plan",
        )
    steps = plan.steps if isinstance(plan.steps, list) else []
    if not steps:
        return ExecutionSummary(
            ok=False,
            message="执行失败：plan，原因：计划为空。",
            total_steps=0,
            completed_steps=0,
            failed_tool="plan",
        )
    if len(steps) > int(MAX_ADMIN_PLAN_STEPS):
        return ExecutionSummary(
            ok=False,
            message=f"执行失败：plan，原因：步骤数超过上限（最多 {int(MAX_ADMIN_PLAN_STEPS)} 步）。",
            total_steps=len(steps),
            completed_steps=0,
            failed_tool="plan",
        )

    done_summaries: list[str] = []
    runtime_vars: dict[str, Any] = {}
    total = len(steps)
    for idx, step in enumerate(steps, 1):
        tool = str(getattr(step, "tool", "") or "").strip()
        if not tool:
            return _failure_summary(
                tool="(empty)",
                reason="工具名为空。",
                total_steps=total,
                completed_steps=idx - 1,
            )

        handler = TOOLS.get(tool)
        if handler is None:
            return _failure_summary(
                tool=tool,
                reason="工具未注册或不在白名单内。",
                total_steps=total,
                completed_steps=idx - 1,
            )

        args = getattr(step, "args", {})
        if not isinstance(args, dict):
            args = {}
        resolved_args = _resolve_step_args(args, runtime_vars)

        try:
            result = await handler(exec_ctx, resolved_args)
        except Exception as e:
            _log_exec_warning(exec_ctx, f"admin tool execution error: tool={tool} err={e}")
            return _failure_summary(
                tool=tool,
                reason=f"{e}",
                total_steps=total,
                completed_steps=idx - 1,
            )

        if not isinstance(result, ToolResult):
            return _failure_summary(
                tool=tool,
                reason="工具返回结果格式不合法。",
                total_steps=total,
                completed_steps=idx - 1,
            )
        if not result.ok:
            reason = str(result.detail or result.summary or "未知错误。").strip()
            return _failure_summary(
                tool=tool,
                reason=reason,
                total_steps=total,
                completed_steps=idx - 1,
            )
        done_summaries.append(str(result.summary or tool))
        step_text: Optional[str] = None
        if isinstance(result.data, dict):
            for k, v in result.data.items():
                if not isinstance(k, str):
                    continue
                runtime_vars[k] = v
            if "text" in result.data:
                step_text = str(result.data.get("text") or "").strip()
            elif "last_text" in result.data:
                step_text = str(result.data.get("last_text") or "").strip()
        if (not step_text) and isinstance(resolved_args, dict) and ("text" in resolved_args):
            step_text = str(resolved_args.get("text") or "").strip()
        if step_text:
            runtime_vars[f"step_{idx}_text"] = step_text
            runtime_vars["last_text"] = step_text

    return ExecutionSummary(
        ok=True,
        message=_success_message(done_summaries),
        total_steps=total,
        completed_steps=total,
        failed_tool=None,
    )


async def execute_admin_plan(api, plan: AdminPlan) -> tuple[bool, str]:
    summary = await execute_plan(AdminExecutionContext(api=api), plan)
    return bool(summary.ok), str(summary.message)
