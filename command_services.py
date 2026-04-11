from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any


@dataclass
class CommandResult:
    ok: bool
    message: str = ""
    data: Any = None
    error_code: str = ""


def format_simple_success(message: str, data: Any = None) -> CommandResult:
    return CommandResult(ok=True, message=str(message or ""), data=data, error_code="")


def format_simple_failure(message: str, error_code: str = "", data: Any = None) -> CommandResult:
    return CommandResult(ok=False, message=str(message or ""), data=data, error_code=str(error_code or ""))


def normalize_target_ids(
    *,
    chat_type: str,
    group_id: Any = None,
    user_id: Any = None,
    chat_id: Any = None,
) -> CommandResult:
    tp = str(chat_type or "").strip().lower()
    if tp == "group":
        raw = group_id if group_id is not None else chat_id
        try:
            gid = int(raw)
        except Exception:
            return format_simple_failure("group_id 必须是正整数。", error_code="INVALID_GROUP_ID")
        if gid <= 0:
            return format_simple_failure("group_id 必须是正整数。", error_code="INVALID_GROUP_ID")
        return format_simple_success(
            "ok",
            data={
                "chat_type": "group",
                "group_id": gid,
                "target_id": gid,
            },
        )

    if tp == "private":
        raw = user_id if user_id is not None else chat_id
        try:
            uid = int(raw)
        except Exception:
            return format_simple_failure("user_id 必须是正整数。", error_code="INVALID_USER_ID")
        if uid <= 0:
            return format_simple_failure("user_id 必须是正整数。", error_code="INVALID_USER_ID")
        return format_simple_success(
            "ok",
            data={
                "chat_type": "private",
                "user_id": uid,
                "target_id": uid,
            },
        )

    return format_simple_failure("chat_type 仅支持 group 或 private。", error_code="INVALID_CHAT_TYPE")


class CommandServiceFormatter:
    @staticmethod
    def format_target_label(chat_type: str, target_id: int) -> str:
        tp = str(chat_type or "").strip().lower()
        if tp == "group":
            return f"群 {int(target_id)}"
        return f"用户 {int(target_id)}"


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


def _task_deadline_ts(task: Any) -> float:
    try:
        return float(getattr(task, "deadline_ts", 0.0) or 0.0)
    except Exception:
        return 0.0


def _task_is_active(task: Any, now_ts: float) -> bool:
    fn = getattr(task, "is_active", None)
    if callable(fn):
        try:
            return bool(fn(now_ts))
        except Exception:
            try:
                return bool(fn())
            except Exception:
                return False
    closed = bool(getattr(task, "closed", False))
    cancelled = bool(getattr(task, "cancelled", False))
    if closed or cancelled:
        return False
    return now_ts < _task_deadline_ts(task)


def _task_status_tag(task: Any, now_ts: float) -> str:
    if bool(getattr(task, "cancelled", False)):
        return "已取消"
    if now_ts >= _task_deadline_ts(task):
        return "已截止"
    if bool(getattr(task, "closed", False)):
        return "已结束"
    return "进行中"


def get_handin_task_summary(
    task: Any,
    *,
    now_ts: float | None = None,
    pretty_ts_func: Any = None,
    with_status: bool = True,
    with_group: bool = True,
) -> CommandResult:
    if task is None:
        return format_simple_failure("任务不存在。", error_code="TASK_NOT_FOUND")
    now_val = time.time() if now_ts is None else float(now_ts)
    task_name = str(getattr(task, "name", "") or "").strip()
    group_id = int(getattr(task, "group_id", 0) or 0)
    deadline_ts = _task_deadline_ts(task)

    pretty = None
    if callable(pretty_ts_func):
        try:
            pretty = str(pretty_ts_func(deadline_ts))
        except Exception:
            pretty = None
    if not pretty:
        try:
            pretty = time.strftime("%Y-%m-%d %H:%M", time.localtime(deadline_ts))
        except Exception:
            pretty = str(deadline_ts)

    parts = []
    if with_status:
        parts.append(f"[{_task_status_tag(task, now_val)}]")
    parts.append(task_name or "(未命名任务)")
    tail = []
    if with_group:
        tail.append(f"群 {group_id}")
    tail.append(f"截止 {pretty}")
    return format_simple_success(
        " ".join(parts) + f"（{'，'.join(tail)}）",
        data={
            "status": _task_status_tag(task, now_val),
            "task_id": str(getattr(task, "task_id", "") or ""),
            "group_id": group_id,
            "task_name": task_name,
            "deadline_ts": deadline_ts,
            "active": _task_is_active(task, now_val),
        },
    )


def list_handin_tasks_for_group(
    *,
    handin: Any,
    group_id: Any,
    include_closed: bool = True,
    active_only: bool = False,
    only_gettable: bool = False,
    now_ts: float | None = None,
    sort_mode: str = "active_then_deadline_desc",
) -> CommandResult:
    try:
        gid = int(group_id)
    except Exception:
        return format_simple_failure("group_id 必须是正整数。", error_code="INVALID_GROUP_ID")
    if gid <= 0:
        return format_simple_failure("group_id 必须是正整数。", error_code="INVALID_GROUP_ID")
    if handin is None:
        return format_simple_failure("handin 服务不可用。", error_code="HANDIN_UNAVAILABLE")

    tasks = []
    if bool(active_only):
        if not hasattr(handin, "list_active_tasks_by_group"):
            return format_simple_failure("handin 服务缺少 list_active_tasks_by_group。", error_code="HANDIN_METHOD_MISSING")
        tasks = handin.list_active_tasks_by_group(gid)
    else:
        if not hasattr(handin, "list_tasks_by_group"):
            return format_simple_failure("handin 服务缺少 list_tasks_by_group。", error_code="HANDIN_METHOD_MISSING")
        tasks = handin.list_tasks_by_group(gid, include_closed=bool(include_closed))

    items = list(tasks or [])
    if bool(only_gettable) and hasattr(handin, "is_task_gettable"):
        items = [t for t in items if bool(handin.is_task_gettable(t))]

    now_val = time.time() if now_ts is None else float(now_ts)
    mode = str(sort_mode or "").strip().lower()
    if mode == "active_then_deadline_desc":
        items.sort(key=lambda t: (0 if _task_is_active(t, now_val) else 1, -_task_deadline_ts(t)))
    elif mode == "deadline_desc":
        items.sort(key=lambda t: -_task_deadline_ts(t))
    elif mode == "deadline_asc":
        items.sort(key=lambda t: _task_deadline_ts(t))

    return format_simple_success(
        "ok",
        data={
            "group_id": gid,
            "tasks": items,
            "count": len(items),
        },
    )


def cancel_handin_task_by_identity(
    *,
    handin: Any,
    group_id: Any,
    by_user_id: Any,
    requester_level: Any = 0,
    task_id: Any = None,
    task_name: Any = None,
) -> CommandResult:
    if handin is None:
        return format_simple_failure("handin 服务不可用。", error_code="HANDIN_UNAVAILABLE")
    try:
        gid = int(group_id)
    except Exception:
        return format_simple_failure("group_id 必须是正整数。", error_code="INVALID_GROUP_ID")
    if gid <= 0:
        return format_simple_failure("group_id 必须是正整数。", error_code="INVALID_GROUP_ID")
    try:
        requester_id = int(by_user_id)
    except Exception:
        return format_simple_failure("by_user_id 必须是正整数。", error_code="INVALID_USER_ID")
    if requester_id <= 0:
        return format_simple_failure("by_user_id 必须是正整数。", error_code="INVALID_USER_ID")

    tid = str(task_id or "").strip()
    tname = str(task_name or "").strip()
    if (not tid) and (not tname):
        return format_simple_failure("缺少任务标识：请提供 task_id 或 task_name。", error_code="MISSING_TASK_TARGET")
    if not hasattr(handin, "list_tasks_by_group"):
        return format_simple_failure("handin 服务缺少 list_tasks_by_group。", error_code="HANDIN_METHOD_MISSING")

    group_tasks = list(handin.list_tasks_by_group(gid, include_closed=True) or [])
    target = None
    if tid:
        for t in group_tasks:
            if str(getattr(t, "task_id", "") or "").strip() == tid:
                target = t
                break
        if target is None:
            return format_simple_failure(f"未找到群 {gid} 中 task_id={tid} 的任务。", error_code="TASK_NOT_FOUND")
    else:
        exact = [t for t in group_tasks if str(getattr(t, "name", "") or "").strip() == tname]
        active_exact = [t for t in exact if _task_is_active(t, time.time())]
        if len(active_exact) == 1:
            target = active_exact[0]
        elif len(active_exact) > 1:
            return format_simple_failure(
                "存在多个同名进行中任务，请改用 task_id 取消。",
                error_code="AMBIGUOUS_TASK_NAME",
                data={
                    "group_id": gid,
                    "task_name": tname,
                    "matches": [
                        {
                            "task_id": str(getattr(t, "task_id", "") or ""),
                            "task_name": str(getattr(t, "name", "") or ""),
                        }
                        for t in active_exact
                    ],
                },
            )
        elif exact:
            return format_simple_failure(f"任务「{tname}」已结束/已取消。", error_code="TASK_NOT_ACTIVE")
        else:
            return format_simple_failure(f"未找到群 {gid} 的任务「{tname}」。", error_code="TASK_NOT_FOUND")

    if target is None:
        return format_simple_failure("任务不存在。", error_code="TASK_NOT_FOUND")
    if not _task_is_active(target, time.time()):
        return format_simple_failure("任务已结束/已取消。", error_code="TASK_NOT_ACTIVE")

    try:
        level = int(requester_level)
    except Exception:
        level = 0
    creator_id = int(getattr(target, "creator_id", 0) or 0)
    if level < 3 and creator_id != requester_id:
        return format_simple_failure("权限不足：只能取消你创建的任务（或联系管理员）。", error_code="PERMISSION_DENIED")

    if not hasattr(handin, "cancel_task"):
        return format_simple_failure("handin 服务缺少 cancel_task。", error_code="HANDIN_METHOD_MISSING")
    ok, msg = handin.cancel_task(str(getattr(target, "task_id", "") or ""), requester_id)
    if not ok:
        return format_simple_failure(str(msg or "取消任务失败。"), error_code="CANCEL_TASK_FAILED")
    return format_simple_success(
        str(msg or f"已取消任务「{str(getattr(target, 'name', '') or '')}」（群 {gid}）。"),
        data={
            "group_id": gid,
            "task_id": str(getattr(target, "task_id", "") or ""),
            "task_name": str(getattr(target, "name", "") or ""),
        },
    )


def run_list_dir_query(
    *,
    filesvc: Any,
    ctx: Any,
    path_arg: Any = None,
) -> CommandResult:
    if filesvc is None or not hasattr(filesvc, "list_dir"):
        return format_simple_failure("文件服务不可用。", error_code="FILESVC_UNAVAILABLE")
    arg = None
    if path_arg is not None:
        arg_text = str(path_arg).strip()
        arg = arg_text if arg_text else None
    ok, out = filesvc.list_dir(ctx, arg)
    return CommandResult(
        ok=bool(ok),
        message=str(out or ""),
        data={"path_arg": arg},
        error_code="" if ok else "LIST_DIR_FAILED",
    )


def run_find_query(
    *,
    filesvc: Any,
    ctx: Any,
    keyword: Any,
    in_dir: Any = None,
    require_keyword: bool = False,
) -> CommandResult:
    if filesvc is None or not hasattr(filesvc, "find"):
        return format_simple_failure("文件服务不可用。", error_code="FILESVC_UNAVAILABLE")
    kw = str(keyword or "").strip()
    if require_keyword and (not kw):
        return format_simple_failure("请提供要搜索的关键词。", error_code="MISSING_KEYWORD")
    scope = None
    if in_dir is not None:
        scope_text = str(in_dir).strip()
        scope = scope_text if scope_text else None
    hits = filesvc.find(ctx, kw, in_dir=scope)
    if hits is None:
        hits = []
    return format_simple_success(
        "ok",
        data={
            "keyword": kw,
            "in_dir": scope,
            "hits": list(hits),
        },
    )
