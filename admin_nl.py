from __future__ import annotations

import inspect
import re
import time
import unicodedata
from typing import Any, Awaitable, Callable, Optional

import admin_targets
import config

from admin_exec import MAX_ADMIN_PLAN_STEPS, AdminExecutionContext, TOOLS, execute_plan
from admin_models import AdminPlan, AdminStep

_RULE_GROUP_PATTERNS = (
    re.compile(r"^在\s*群\s*(.+?)\s*发\s*[:：]\s*(.+)$"),
    re.compile(r"^在\s*(.+?)\s*群\s*发\s*[:：]\s*(.+)$"),
    re.compile(r"^给\s*群\s*(.+?)\s*发\s*[:：]\s*(.+)$"),
)
_RULE_PRIVATE_PATTERNS = (
    re.compile(r"^给\s*(?:QQ|用户)?\s*(.+?)\s*发\s*[:：]\s*(.+)$"),
)
_RULE_AI_GROUP_PATTERNS = (
    re.compile(r"^在\s*群\s*(.+?)\s*对\s*[“\"](.+?)[”\"]\s*生成\s*AI\s*回复并(?:发出去|发送)$", flags=re.IGNORECASE),
    re.compile(r"^在\s*(.+?)\s*群\s*对\s*[“\"](.+?)[”\"]\s*生成\s*AI\s*回复并(?:发出去|发送)$", flags=re.IGNORECASE),
)
_RULE_AI_PRIVATE_PATTERNS = (
    re.compile(r"^给\s*(?:QQ|用户)?\s*(.+?)\s*这个私聊\s*对\s*[“\"](.+?)[”\"]\s*生成\s*AI\s*回复并(?:发出去|发送)$", flags=re.IGNORECASE),
)
_RULE_HANDIN_PATTERNS = (
    re.compile(
        r"^在\s*群\s*(.+?)\s*创建(?:\s*handin|提交任务)\s*[，,]\s*任务名\s*([^，,]+?)\s*[，,]\s*(.+?)\s*截止(?:\s*[，,]\s*(.+?)\s*提醒)?\s*$",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"^在\s*(.+?)\s*群\s*创建(?:\s*handin|提交任务)\s*[，,]\s*任务名\s*([^，,]+?)\s*[，,]\s*(.+?)\s*截止(?:\s*[，,]\s*(.+?)\s*提醒)?\s*$",
        flags=re.IGNORECASE,
    ),
)
_RULE_HANDIN_LIST_PATTERNS = (
    re.compile(r"^(?:列一下|列出|查看)\s*群\s*(.+?)\s*(?:的)?\s*(?:当前)?(?:有哪些)?\s*(?:handin|提交)\s*任务\s*$", flags=re.IGNORECASE),
    re.compile(r"^(?:列一下|列出|查看)\s*(.+?)\s*群\s*(?:的)?\s*(?:当前)?(?:有哪些)?\s*(?:handin|提交)\s*任务\s*$", flags=re.IGNORECASE),
    re.compile(r"^查看\s*群\s*(.+?)\s*当前有哪些(?:提交)?任务\s*$", flags=re.IGNORECASE),
)
_RULE_HANDIN_CANCEL_BY_ID_PATTERNS = (
    re.compile(r"^取消\s*群\s*(.+?)\s*(?:的)?\s*handin\s*(?:任务)?\s*(?:id|ID)\s*[:：]?\s*([^\s，,]+)\s*$"),
    re.compile(r"^取消\s*(.+?)\s*群\s*(?:的)?\s*handin\s*(?:任务)?\s*(?:id|ID)\s*[:：]?\s*([^\s，,]+)\s*$"),
)
_RULE_HANDIN_CANCEL_BY_NAME_PATTERNS = (
    re.compile(r"^取消\s*群\s*(.+?)(?:\s*的\s*|\s+)(.+?)\s*handin\s*$", flags=re.IGNORECASE),
    re.compile(r"^取消\s*(.+?)\s*群(?:\s*的\s*|\s+)(.+?)\s*handin\s*$", flags=re.IGNORECASE),
    re.compile(r"^取消\s*群\s*(.+?)\s*(?:的)?\s*handin\s*[，,]?\s*任务名\s*[:：]?\s*(.+)\s*$", flags=re.IGNORECASE),
)
_RULE_FIND_IN_DIR_PATTERNS = (
    re.compile(r"^(?:帮我)?在\s*([^\s，,。；;]+)\s*(?:目录)?(?:里)?找\s*(.+)$"),
    re.compile(r"^在\s*([^\s，,。；;]+)\s*里查(?:找)?\s*(.+)$"),
)
_RULE_FIND_PATTERNS = (
    re.compile(r"^(?:帮我)?找\s*(.+)$"),
    re.compile(r"^查资料库里(?:的)?\s*(.+)$"),
    re.compile(r"^查询资料库里(?:的)?\s*(.+)$"),
)
_RULE_LIST_DIR_PATTERNS = (
    re.compile(r"^(?:列一下|列出|查看|看下|浏览)\s*([^\s，,。；;]+)\s*(?:目录|文件夹)?$"),
    re.compile(r"^(?:列一下|列出|查看|看下|浏览)\s*目录\s*[:：]\s*([^\s，,。；;]+)$"),
)
_PLACEHOLDER_DIR_TOKENS = {
    "某个",
    "这个",
    "那个",
    "某个目录",
    "这个目录",
    "那个目录",
    "某个文件夹",
    "这个文件夹",
    "那个文件夹",
    "目录",
    "文件夹",
}
_PLACEHOLDER_TASK_NAMES = {"的", "任务", "handin"}
_CONFIRM_WORDS = {"确认", "confirm", "yes", "y"}
_CANCEL_WORDS = {"取消", "cancel", "no", "n"}
_PENDING_CONFIRM_TTL_SECONDS = 30.0 * 60.0
_ADMIN_NL_LOG_TEXT_LIMIT = 120


def _config_bool(name: str, default: bool) -> bool:
    raw = getattr(config, name, default)
    if isinstance(raw, bool):
        return raw
    s = str(raw or "").strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _clip_log_text(text: str, limit: int = _ADMIN_NL_LOG_TEXT_LIMIT) -> str:
    src = str(text or "").replace("\r", " ").replace("\n", " ").strip()
    if len(src) <= int(limit):
        return src
    return src[: int(limit)] + "..."


def _safe_log(logsvc: Any, level: str, msg: str) -> None:
    try:
        logger = getattr(logsvc, "log", None)
        fn = getattr(logger, level, None)
        if callable(fn):
            fn(str(msg))
    except Exception:
        pass


def _plan_tools_summary(plan: Optional[AdminPlan]) -> str:
    if not isinstance(plan, AdminPlan):
        return ""
    tools = [str(getattr(step, "tool", "") or "").strip() for step in (plan.steps or [])]
    tools = [x for x in tools if x]
    return ",".join(tools[:5])


def _plan_targets_summary(plan: Optional[AdminPlan]) -> str:
    if not isinstance(plan, AdminPlan):
        return ""
    out: list[str] = []
    seen = set()
    for step in (plan.steps or []):
        args = step.args if isinstance(getattr(step, "args", None), dict) else {}
        chat_type = str(args.get("chat_type") or "").strip().lower()
        target = ""
        if "group_id" in args:
            target = f"group:{args.get('group_id')}"
        elif "user_id" in args:
            target = f"user:{args.get('user_id')}"
        elif "chat_id" in args:
            if chat_type in {"group", "private"}:
                target = f"{chat_type}:{args.get('chat_id')}"
            else:
                target = f"chat:{args.get('chat_id')}"
        if not target or target in seen:
            continue
        seen.add(target)
        out.append(target)
        if len(out) >= 5:
            break
    return ",".join(out)


def _log_admin_nl(
    logsvc: Any,
    *,
    stage: str,
    ctx: Any = None,
    text: str = "",
    plan: Optional[AdminPlan] = None,
    reason: str = "",
    outcome: str = "",
) -> None:
    uid = int(getattr(ctx, "user_id", 0) or 0) if ctx is not None else 0
    scene = str(getattr(ctx, "scene", "") or "") if ctx is not None else ""
    parts = [f"admin_nl stage={stage}", f"uid={uid}", f"scene={scene}"]
    text_short = _clip_log_text(text)
    if text_short:
        parts.append(f"text='{text_short}'")
    if isinstance(plan, AdminPlan):
        parts.append(f"source={str(plan.source or '')}")
        parts.append(f"steps={len(plan.steps or [])}")
        parts.append(f"need_confirm={bool(plan.need_confirm)}")
        parts.append(f"confidence={float(getattr(plan, 'confidence', 0.0) or 0.0):.2f}")
        tools = _plan_tools_summary(plan)
        if tools:
            parts.append(f"tools={tools}")
        targets = _plan_targets_summary(plan)
        if targets:
            parts.append(f"targets={targets}")
    reason_short = _clip_log_text(reason)
    if reason_short:
        parts.append(f"reason='{reason_short}'")
    outcome_short = _clip_log_text(outcome)
    if outcome_short:
        parts.append(f"outcome='{outcome_short}'")
    _safe_log(logsvc, "info", " ".join(parts))


def should_handle_admin_nl(ctx, text: str) -> bool:
    if not _config_bool("ENABLE_ADMIN_NL_CONTROL", True):
        return False
    t = (text or "").strip()
    if not t:
        return False
    try:
        user_id = int(getattr(ctx, "user_id", 0) or 0)
    except Exception:
        return False
    admin_users = getattr(config, "ADMIN_USERS", set()) or set()
    if user_id not in admin_users:
        return False
    if str(getattr(ctx, "scene", "") or "") != "private_friend":
        return False
    if t.startswith("/") or t.startswith("／"):
        return False
    if t[:1] in ("C", "c"):
        return False
    return True


def _format_target_resolve_error(target_kind: str, rr: admin_targets.TargetResolveResult) -> str:
    query = str(getattr(rr, "query", "") or "").strip()
    if rr.status == "ambiguous":
        cands = [str(x) for x in (rr.candidates or []) if str(x).strip()]
        if cands:
            return f"{target_kind}目标别名“{query}”存在多个候选：{', '.join(cands)}。请改用ID。"
        return f"{target_kind}目标别名“{query}”存在多个候选。请改用ID。"
    if rr.status == "not_found":
        id_label = "群号" if target_kind == "群" else "QQ号"
        return f"未找到{target_kind}目标别名“{query}”，请改用{id_label}。"
    id_label = "群号" if target_kind == "群" else "QQ号"
    return f"无法解析{target_kind}目标“{query}”，请改用{id_label}。"


def _resolve_group_target_id(raw_target: str, *, logsvc=None) -> tuple[Optional[int], str]:
    rr = admin_targets.resolve_group_target(raw_target, logsvc=logsvc)
    if rr.ok:
        return int(rr.target_id), ""
    return None, _format_target_resolve_error("群", rr)


def _resolve_user_target_id(raw_target: str, *, logsvc=None) -> tuple[Optional[int], str]:
    rr = admin_targets.resolve_user_target(raw_target, logsvc=logsvc)
    if rr.ok:
        return int(rr.target_id), ""
    return None, _format_target_resolve_error("用户", rr)


def _match_plan(
    patterns,
    text: str,
    *,
    tool: str,
    summary: str,
    id_key: str,
    resolver: Callable[..., tuple[Optional[int], str]],
    logsvc=None,
) -> tuple[Optional[AdminPlan], str]:
    for pattern in patterns:
        m = pattern.fullmatch(text)
        if not m:
            continue
        raw_target = str(m.group(1) or "").strip()
        msg = str(m.group(2) or "").strip()
        if (not raw_target) or (not msg):
            return None, ""
        target_id, err_target = resolver(raw_target, logsvc=logsvc)
        if err_target:
            return None, err_target
        return AdminPlan(
            source="rule",
            summary=summary,
            steps=[AdminStep(tool=tool, args={id_key: target_id, "text": msg})],
        ), ""
    return None, ""


def _match_ai_proxy_plan(
    patterns,
    text: str,
    *,
    chat_type: str,
    logsvc=None,
) -> tuple[Optional[AdminPlan], str]:
    for pattern in patterns:
        m = pattern.fullmatch(text)
        if not m:
            continue
        raw_target = str(m.group(1) or "").strip()
        message = str(m.group(2) or "").strip()
        if (not raw_target) or (not message):
            return None, ""
        if chat_type == "group":
            chat_id, err_target = _resolve_group_target_id(raw_target, logsvc=logsvc)
        else:
            chat_id, err_target = _resolve_user_target_id(raw_target, logsvc=logsvc)
        if err_target:
            return None, err_target
        target_key = "group_id" if chat_type == "group" else "user_id"
        return AdminPlan(
            source="rule",
            summary="生成 AI 回复并代发",
            steps=[
                AdminStep(
                    tool="generate_ai_reply",
                    args={
                        "chat_type": chat_type,
                        "chat_id": chat_id,
                        "message": message,
                    },
                ),
                AdminStep(
                    tool="send_message",
                    args={
                        "chat_type": chat_type,
                        target_key: chat_id,
                        "text": "{{last_text}}",
                    },
                ),
            ],
        ), ""
    return None, ""


def _split_reminders(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    parts = re.split(r"[、，,]\s*", text)
    out: list[str] = []
    for one in parts:
        t = str(one or "").strip()
        if t:
            out.append(t)
    return out


def _match_handin_plan(patterns, text: str, *, logsvc=None) -> tuple[Optional[AdminPlan], str]:
    for pattern in patterns:
        m = pattern.fullmatch(text)
        if not m:
            continue
        raw_target = str(m.group(1) or "").strip()
        group_id, err_target = _resolve_group_target_id(raw_target, logsvc=logsvc)
        if err_target:
            return None, err_target
        task_name = str(m.group(2) or "").strip()
        deadline_text = str(m.group(3) or "").strip()
        reminders_text = str(m.group(4) or "").strip()
        if group_id <= 0 or (not task_name) or (not deadline_text):
            return None, ""
        args = {
            "group_id": group_id,
            "task_name": task_name,
            "deadline_text": deadline_text,
        }
        reminders = _split_reminders(reminders_text)
        if reminders:
            args["reminders"] = reminders
        return AdminPlan(
            source="rule",
            summary="创建 handin 任务",
            steps=[AdminStep(tool="create_handin_task", args=args)],
        ), ""
    return None, ""


def _normalize_task_name(raw: str) -> str:
    out = str(raw or "").strip()
    if len(out) >= 2:
        if (out[0], out[-1]) in {("“", "”"), ('"', '"'), ("'", "'")}:
            out = out[1:-1].strip()
    if out.casefold() in {x.casefold() for x in _PLACEHOLDER_TASK_NAMES}:
        return ""
    return out


def _match_handin_list_plan(text: str, *, logsvc=None) -> tuple[Optional[AdminPlan], str]:
    for pattern in _RULE_HANDIN_LIST_PATTERNS:
        m = pattern.fullmatch(text)
        if not m:
            continue
        raw_target = str(m.group(1) or "").strip()
        gid, err_target = _resolve_group_target_id(raw_target, logsvc=logsvc)
        if err_target:
            return None, err_target
        if gid <= 0:
            return None, ""
        return AdminPlan(
            source="rule",
            summary="查看 handin 任务",
            steps=[AdminStep(tool="list_handin_tasks", args={"group_id": gid, "active_only": True})],
        ), ""
    return None, ""


def _match_handin_cancel_plan(text: str, *, logsvc=None) -> tuple[Optional[AdminPlan], str]:
    for pattern in _RULE_HANDIN_CANCEL_BY_ID_PATTERNS:
        m = pattern.fullmatch(text)
        if not m:
            continue
        raw_target = str(m.group(1) or "").strip()
        gid, err_target = _resolve_group_target_id(raw_target, logsvc=logsvc)
        if err_target:
            return None, err_target
        task_id = str(m.group(2) or "").strip()
        if gid <= 0 or (not task_id):
            return None, ""
        return AdminPlan(
            source="rule",
            summary="取消 handin 任务",
            need_confirm=True,
            steps=[AdminStep(tool="cancel_handin_task", args={"group_id": gid, "task_id": task_id})],
        ), ""
    for pattern in _RULE_HANDIN_CANCEL_BY_NAME_PATTERNS:
        m = pattern.fullmatch(text)
        if not m:
            continue
        raw_target = str(m.group(1) or "").strip()
        gid, err_target = _resolve_group_target_id(raw_target, logsvc=logsvc)
        if err_target:
            return None, err_target
        task_name = _normalize_task_name(m.group(2))
        if gid <= 0 or (not task_name):
            return None, ""
        return AdminPlan(
            source="rule",
            summary="取消 handin 任务",
            need_confirm=True,
            steps=[AdminStep(tool="cancel_handin_task", args={"group_id": gid, "task_name": task_name})],
        ), ""
    return None, ""


def _normalize_query_keyword(text: str) -> str:
    return str(text or "").strip().strip("，,。；;：: ")


def _normalize_dir_arg(raw: str) -> Optional[str]:
    text = unicodedata.normalize("NFKC", str(raw or "")).strip()
    if not text:
        return None
    text = text.replace("\\", "/").strip("/")
    if not text:
        return None
    if text in _PLACEHOLDER_DIR_TOKENS:
        return None
    parts = []
    for seg in text.split("/"):
        one = str(seg or "").strip()
        if not one or one in {".", ".."}:
            return None
        if one in _PLACEHOLDER_DIR_TOKENS:
            return None
        parts.append(one)
    if not parts:
        return None
    return "/".join(parts)


def _match_find_plan(text: str) -> Optional[AdminPlan]:
    for pattern in _RULE_FIND_IN_DIR_PATTERNS:
        m = pattern.fullmatch(text)
        if not m:
            continue
        in_dir = _normalize_dir_arg(m.group(1))
        kw = _normalize_query_keyword(m.group(2))
        if (not in_dir) or (not kw):
            return None
        return AdminPlan(
            source="rule",
            summary="查询文件",
            steps=[AdminStep(tool="find_files", args={"keyword": kw, "in_dir": in_dir})],
        )
    for pattern in _RULE_FIND_PATTERNS:
        m = pattern.fullmatch(text)
        if not m:
            continue
        kw = _normalize_query_keyword(m.group(1))
        if not kw:
            return None
        return AdminPlan(
            source="rule",
            summary="查询文件",
            steps=[AdminStep(tool="find_files", args={"keyword": kw})],
        )
    return None


def _match_list_directory_plan(text: str) -> Optional[AdminPlan]:
    for pattern in _RULE_LIST_DIR_PATTERNS:
        m = pattern.fullmatch(text)
        if not m:
            continue
        path_arg = _normalize_dir_arg(m.group(1))
        if not path_arg:
            return None
        return AdminPlan(
            source="rule",
            summary="查看目录",
            steps=[AdminStep(tool="list_directory", args={"path": path_arg})],
        )
    return None


def _build_missing_param_hint(text: str) -> str:
    norm = unicodedata.normalize("NFKC", str(text or "")).strip()
    if re.fullmatch(r"(?:帮我)?找(?:一下)?", norm):
        return "缺少搜索关键词。示例：帮我找高数资料"
    if re.fullmatch(r"(?:查|查找|查询)(?:一下)?(?:资料库(?:里|里的)?)?", norm):
        return "缺少搜索关键词。示例：查资料库里的电路实验报告"
    if re.fullmatch(r"(?:列一下|列出|查看|看下|浏览)\s*(?:目录|文件夹)?", norm):
        return "缺少目录路径。示例：列一下 public/textbook_and_material 目录"
    if ("目录" in norm or "文件夹" in norm) and any(
        token in norm
        for token in ("某个目录", "这个目录", "那个目录", "某个文件夹", "这个文件夹", "那个文件夹")
    ):
        return "缺少目录路径。示例：列一下 public/textbook_and_material 目录"
    if re.fullmatch(r"(?:列一下|列出|查看)\s*handin\s*任务", norm, flags=re.IGNORECASE):
        return "缺少群号。示例：列一下群 123456 的 handin 任务"
    if re.fullmatch(r"取消\s*群\s*.+?\s*(?:的)?\s*handin", norm, flags=re.IGNORECASE):
        return "缺少任务标识。示例：取消群 123456 的作业1 handin"
    if re.fullmatch(r"取消\s*handin(?:任务)?", norm, flags=re.IGNORECASE):
        return "缺少群号和任务标识。示例：取消群 123456 的作业1 handin"
    return ""


def _plan_to_state_dict(plan: AdminPlan) -> dict:
    return {
        "source": str(plan.source or ""),
        "summary": str(plan.summary or ""),
        "need_confirm": bool(plan.need_confirm),
        "confidence": _to_confidence(getattr(plan, "confidence", 0.0), default=0.0),
        "steps": [
            {
                "tool": str(one.tool or ""),
                "args": dict(one.args if isinstance(one.args, dict) else {}),
            }
            for one in (plan.steps or [])
        ],
    }


def _plan_from_state_dict(raw: object) -> Optional[AdminPlan]:
    if not isinstance(raw, dict):
        return None
    steps_raw = raw.get("steps")
    if not isinstance(steps_raw, list) or (not steps_raw):
        return None
    steps: list[AdminStep] = []
    for one in steps_raw:
        if not isinstance(one, dict):
            return None
        tool = str(one.get("tool") or "").strip()
        args = one.get("args")
        if (not tool) or (not isinstance(args, dict)):
            return None
        steps.append(AdminStep(tool=tool, args=dict(args)))
    return AdminPlan(
        source=str(raw.get("source") or "state"),
        summary=str(raw.get("summary") or ""),
        steps=steps,
        need_confirm=bool(raw.get("need_confirm", False)),
        confidence=_to_confidence(raw.get("confidence", 0.0), default=0.0),
    )


def _get_pending_confirm_store(state: Any) -> Optional[dict]:
    if state is None:
        return None
    store = getattr(state, "pending_admin_nl_confirm", None)
    if store is None:
        try:
            setattr(state, "pending_admin_nl_confirm", {})
        except Exception:
            return None
        store = getattr(state, "pending_admin_nl_confirm", None)
    return store if isinstance(store, dict) else None


def _normalize_control_text(text: str) -> str:
    return unicodedata.normalize("NFKC", str(text or "")).strip().casefold()


def _is_high_risk_plan(plan: AdminPlan) -> bool:
    risky_tools = {"set_level", "cancel_handin_task", "cancel_handin"}
    send_like_tools = {"send_message", "send_group_message", "send_private_message"}
    for step in plan.steps or []:
        if str(step.tool or "").strip() in risky_tools:
            return True

    send_targets: list[Optional[str]] = []
    for step in plan.steps or []:
        tool = str(step.tool or "").strip()
        args = step.args if isinstance(step.args, dict) else {}
        if tool not in send_like_tools:
            continue
        target: Optional[str] = None
        if tool == "send_group_message":
            v = args.get("group_id")
            target = f"group:{v}" if v is not None else None
        elif tool == "send_private_message":
            v = args.get("user_id")
            target = f"private:{v}" if v is not None else None
        elif tool == "send_message":
            chat_type = str(args.get("chat_type") or "").strip().lower()
            if chat_type == "group":
                v = args.get("group_id", args.get("chat_id"))
                target = f"group:{v}" if v is not None else None
            elif chat_type == "private":
                v = args.get("user_id", args.get("chat_id"))
                target = f"private:{v}" if v is not None else None
        send_targets.append(target)

    if len(send_targets) <= 1:
        return False
    return True


def _contains_template_expr(value: object) -> bool:
    if isinstance(value, str):
        return ("{{" in value) or ("}}" in value)
    if isinstance(value, list):
        return any(_contains_template_expr(v) for v in value)
    if isinstance(value, dict):
        return any(_contains_template_expr(v) for v in value.values())
    return False


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    s = str(value or "").strip().lower()
    if s in {"1", "true", "yes", "y", "是"}:
        return True
    if s in {"0", "false", "no", "n", "否", ""}:
        return False
    return False


def _to_confidence(value: object, *, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if out < 0.0:
        return 0.0
    if out > 1.0:
        return 1.0
    return out


def _validate_model_plan(raw: dict) -> tuple[Optional[AdminPlan], str]:
    if not isinstance(raw, dict):
        return None, "模型规划结果不是对象。"
    steps_raw = raw.get("steps")
    if not isinstance(steps_raw, list) or (not steps_raw):
        return None, "steps 必须是非空列表。"
    if len(steps_raw) > int(MAX_ADMIN_PLAN_STEPS):
        return None, f"steps 超出上限，最多允许 {int(MAX_ADMIN_PLAN_STEPS)} 步。"

    allowed = set(TOOLS.keys())
    steps: list[AdminStep] = []
    for idx, one in enumerate(steps_raw, 1):
        if not isinstance(one, dict):
            return None, f"第 {idx} 步格式不合法。"
        tool = str(one.get("tool") or "").strip()
        if tool not in allowed:
            return None, f"第 {idx} 步工具不在白名单内：{tool or '(empty)'}。"
        args = one.get("args")
        if not isinstance(args, dict):
            return None, f"第 {idx} 步 args 必须是对象。"
        args2 = dict(args)
        if _contains_template_expr(args2):
            return None, f"第 {idx} 步包含不允许的模板表达式。"
        
        for k, v in list(args2.items()):
            if isinstance(v, str) and str(v).strip().replace(" ", "").startswith("text_from_step:"):
                try:
                    ref_i = int(str(v).split(":")[1].strip())
                    args2["text_from_step"] = ref_i
                    if k != "text_from_step":
                        del args2[k]
                except Exception:
                    pass

        if "text_from_step" in args2:
            if "text" in args2:
                return None, f"第 {idx} 步不能同时包含 text 与 text_from_step。"
            ref = args2.pop("text_from_step")
            try:
                ref_i = int(ref)
            except Exception:
                return None, f"第 {idx} 步 text_from_step 必须是整数。"
            if ref_i < 1 or ref_i >= idx:
                return None, f"第 {idx} 步 text_from_step 只能引用已完成的上一步。"
            args2["text"] = f"{{{{step_{ref_i}_text}}}}"
        steps.append(AdminStep(tool=tool, args=args2))

    summary = str(raw.get("summary") or "").strip() or "模型规划任务"
    need_confirm = _to_bool(raw.get("need_confirm", False))
    confidence = _to_confidence(raw.get("confidence", 0.0), default=0.0)
    return AdminPlan(
        source="model",
        summary=summary,
        steps=steps,
        need_confirm=need_confirm,
        confidence=confidence,
    ), ""


async def _call_model_planner(aisvc: Any, text: str, admin_user_id: int, logsvc) -> Optional[dict]:
    if aisvc is None:
        return None
    fn = getattr(aisvc, "parse_admin_plan", None)
    if fn is None:
        return None
    try:
        ret = fn(
            text=str(text or ""),
            admin_user_id=int(admin_user_id),
            allowed_tools=sorted(TOOLS.keys()),
            max_steps=int(MAX_ADMIN_PLAN_STEPS),
        )
    except TypeError:
        try:
            ret = fn(str(text or ""), int(admin_user_id), sorted(TOOLS.keys()), int(MAX_ADMIN_PLAN_STEPS))
        except TypeError:
            ret = fn(str(text or ""), int(admin_user_id), sorted(TOOLS.keys()))
    except Exception as e:
        logsvc.log.warning(f"admin model planner call failed: {e}")
        return None
    try:
        obj = await ret if inspect.isawaitable(ret) else ret
    except Exception as e:
        logsvc.log.warning(f"admin model planner await failed: {e}")
        return None
    return obj if isinstance(obj, dict) else None


def _build_confirm_preview(plan: AdminPlan) -> str:
    lines = [f"该计划需要确认：{str(plan.summary or '管理员操作')}"]
    conf = float(getattr(plan, "confidence", 0.0) or 0.0)
    if conf > 0.0:
        lines.append(f"置信度：{conf:.2f}")
    for i, step in enumerate(plan.steps or [], 1):
        lines.append(f"{i}. {str(step.tool or '')}")
    lines.append("回复“确认”执行，回复“取消”放弃。")
    return "\n".join(lines)


async def _execute_plan_to_text(
    plan: AdminPlan,
    *,
    api,
    ctx,
    text: str,
    logsvc,
    evt: Optional[dict],
    filesvc: Any,
    state: Any,
    handin: Any,
    perm: Any,
    aisvc: Any,
) -> str:
    exec_ctx = AdminExecutionContext(
        api=api,
        ctx=ctx,
        evt=evt if isinstance(evt, dict) else {},
        text=str(text or ""),
        filesvc=filesvc,
        logsvc=logsvc,
        state=state,
        handin=handin,
        perm=perm,
        aisvc=aisvc,
    )
    summary = await execute_plan(exec_ctx, plan)
    return str(summary.message or "")


def _parse_admin_rule_based_with_error(text: str, *, logsvc=None) -> tuple[Optional[AdminPlan], str]:
    raw = (text or "").strip()
    if not raw:
        return None, ""
    norm = unicodedata.normalize("NFKC", raw).strip()
    if not norm:
        return None, ""

    plan, err = _match_ai_proxy_plan(_RULE_AI_GROUP_PATTERNS, norm, chat_type="group", logsvc=logsvc)
    if plan is not None:
        return plan, ""
    if err:
        return None, err

    plan, err = _match_ai_proxy_plan(_RULE_AI_PRIVATE_PATTERNS, norm, chat_type="private", logsvc=logsvc)
    if plan is not None:
        return plan, ""
    if err:
        return None, err

    plan, err = _match_handin_plan(_RULE_HANDIN_PATTERNS, norm, logsvc=logsvc)
    if plan is not None:
        return plan, ""
    if err:
        return None, err

    plan, err = _match_handin_list_plan(norm, logsvc=logsvc)
    if plan is not None:
        return plan, ""
    if err:
        return None, err

    plan, err = _match_handin_cancel_plan(norm, logsvc=logsvc)
    if plan is not None:
        return plan, ""
    if err:
        return None, err

    plan = _match_find_plan(norm)
    if plan is not None:
        return plan, ""
    plan = _match_list_directory_plan(norm)
    if plan is not None:
        return plan, ""

    plan, err = _match_plan(
        _RULE_GROUP_PATTERNS,
        norm,
        tool="send_group_message",
        summary="向群发送消息",
        id_key="group_id",
        resolver=_resolve_group_target_id,
        logsvc=logsvc,
    )
    if plan is not None:
        return plan, ""
    if err:
        return None, err

    plan, err = _match_plan(
        _RULE_PRIVATE_PATTERNS,
        norm,
        tool="send_private_message",
        summary="向私聊发送消息",
        id_key="user_id",
        resolver=_resolve_user_target_id,
        logsvc=logsvc,
    )
    if plan is not None:
        return plan, ""
    if err:
        return None, err
    return None, ""


def parse_admin_rule_based(text: str, logsvc: Any = None) -> Optional[AdminPlan]:
    plan, _ = _parse_admin_rule_based_with_error(text, logsvc=logsvc)
    return plan


async def _send_admin_feedback(
    api,
    ctx,
    text: str,
    logsvc,
    reply_func: Optional[Callable[..., Awaitable[None]]],
) -> None:
    if reply_func is not None:
        await reply_func(api, ctx, text, logsvc)
        return
    await api.send_private_msg(int(getattr(ctx, "user_id", 0) or 0), text)


async def handle_admin_nl(
    api,
    ctx,
    text: str,
    logsvc,
    evt: Optional[dict] = None,
    filesvc: Any = None,
    state: Any = None,
    handin: Any = None,
    perm: Any = None,
    aisvc: Any = None,
    reply_func: Optional[Callable[..., Awaitable[None]]] = None,
) -> bool:
    if not should_handle_admin_nl(ctx, text):
        return False

    user_id = int(getattr(ctx, "user_id", 0) or 0)
    _log_admin_nl(logsvc, stage="recv", ctx=ctx, text=text)
    pending_store = _get_pending_confirm_store(state)
    if pending_store is not None:
        now_ts = time.time()
        for uid, item in list(pending_store.items()):
            if not isinstance(item, dict):
                pending_store.pop(uid, None)
                continue
            try:
                ts = float(item.get("ts") or 0.0)
            except Exception:
                ts = 0.0
            if ts <= 0.0 or (now_ts - ts) > _PENDING_CONFIRM_TTL_SECONDS:
                pending_store.pop(uid, None)

    pending = pending_store.get(user_id) if isinstance(pending_store, dict) else None
    if isinstance(pending, dict):
        cmd = _normalize_control_text(text)
        if cmd in _CONFIRM_WORDS:
            _log_admin_nl(logsvc, stage="confirm_ack", ctx=ctx, text=text)
            pending_store.pop(user_id, None)
            plan = _plan_from_state_dict(pending.get("plan"))
            if plan is None:
                await _send_admin_feedback(api, ctx, "待确认计划已失效，已清理。", logsvc, reply_func)
                _log_admin_nl(logsvc, stage="confirm_invalid", ctx=ctx, reason="plan_missing")
                return True
            out = ""
            try:
                out = await _execute_plan_to_text(
                    plan,
                    api=api,
                    ctx=ctx,
                    text=text,
                    logsvc=logsvc,
                    evt=evt,
                    filesvc=filesvc,
                    state=state,
                    handin=handin,
                    perm=perm,
                    aisvc=aisvc,
                )
            except Exception as e:
                out = f"执行失败：{e}"
                logsvc.log.warning(f"admin nl confirmed execute failed: {e}")
                _log_admin_nl(logsvc, stage="confirmed_execute_exception", ctx=ctx, plan=plan, reason=str(e))
            if not out:
                out = "执行失败：未知错误。"
            await _send_admin_feedback(api, ctx, out, logsvc, reply_func)
            _log_admin_nl(logsvc, stage="confirmed_executed", ctx=ctx, plan=plan, outcome=out)
            return True
        if cmd in _CANCEL_WORDS:
            pending_store.pop(user_id, None)
            await _send_admin_feedback(api, ctx, "已取消待确认计划。", logsvc, reply_func)
            _log_admin_nl(logsvc, stage="confirm_cancel", ctx=ctx, text=text)
            return True
        await _send_admin_feedback(api, ctx, "你有待确认的管理员计划。回复“确认”执行，回复“取消”放弃。", logsvc, reply_func)
        _log_admin_nl(logsvc, stage="confirm_waiting", ctx=ctx, text=text)
        return True

    plan: Optional[AdminPlan] = None
    parse_err = ""
    planned_by_model = False
    try:
        try:
            plan = parse_admin_rule_based(text, logsvc=logsvc)
        except TypeError:
            plan = parse_admin_rule_based(text)
        if plan is None:
            plan, parse_err = _parse_admin_rule_based_with_error(text, logsvc=logsvc)
    except Exception as e:
        logsvc.log.warning(f"admin nl parse failed: {e}")
        _log_admin_nl(logsvc, stage="parse_exception", ctx=ctx, text=text, reason=str(e))
        return False
    if plan is not None:
        _log_admin_nl(logsvc, stage="plan_rule_hit", ctx=ctx, text=text, plan=plan)
    if plan is None:
        if parse_err:
            await _send_admin_feedback(api, ctx, parse_err, logsvc, reply_func)
            _log_admin_nl(logsvc, stage="parse_reject", ctx=ctx, text=text, reason=parse_err)
            return True
        hint = _build_missing_param_hint(text)
        if hint:
            await _send_admin_feedback(api, ctx, hint, logsvc, reply_func)
            _log_admin_nl(logsvc, stage="missing_param_hint", ctx=ctx, text=text, reason=hint)
            return True
        if not _config_bool("ENABLE_ADMIN_NL_MULTI_STEP", True):
            _log_admin_nl(logsvc, stage="model_planner_disabled", ctx=ctx, text=text)
            return False
        model_obj = await _call_model_planner(aisvc, text, user_id, logsvc)
        if model_obj is None:
            _log_admin_nl(logsvc, stage="model_planner_miss", ctx=ctx, text=text)
            return False
        planned_by_model = True
        plan, err = _validate_model_plan(model_obj)
        if plan is None:
            await _send_admin_feedback(api, ctx, f"无法安全执行：{err}", logsvc, reply_func)
            _log_admin_nl(logsvc, stage="model_plan_reject", ctx=ctx, text=text, reason=err)
            return True
        _log_admin_nl(logsvc, stage="plan_model_hit", ctx=ctx, text=text, plan=plan)

    out = ""
    try:
        need_confirm = bool(plan.need_confirm) or _is_high_risk_plan(plan)
        if need_confirm:
            if not isinstance(pending_store, dict):
                await _send_admin_feedback(api, ctx, "无法安全执行：当前无法保存确认状态。", logsvc, reply_func)
                _log_admin_nl(logsvc, stage="confirm_store_unavailable", ctx=ctx, plan=plan)
                return True
            pending_store[user_id] = {
                "plan": _plan_to_state_dict(plan),
                "ts": time.time(),
                "source": str(plan.source or ("model" if planned_by_model else "rule")),
            }
            await _send_admin_feedback(api, ctx, _build_confirm_preview(plan), logsvc, reply_func)
            _log_admin_nl(logsvc, stage="queued_confirm", ctx=ctx, text=text, plan=plan)
            return True
        out = await _execute_plan_to_text(
            plan,
            api=api,
            ctx=ctx,
            text=text,
            logsvc=logsvc,
            evt=evt,
            filesvc=filesvc,
            state=state,
            handin=handin,
            perm=perm,
            aisvc=aisvc,
        )
    except Exception as e:
        out = f"执行失败：{e}"
        logsvc.log.warning(f"admin nl execute failed: {e}")
        _log_admin_nl(logsvc, stage="execute_exception", ctx=ctx, plan=plan, reason=str(e))

    if not out:
        out = "执行失败：未知错误。"

    try:
        await _send_admin_feedback(api, ctx, out, logsvc, reply_func)
    except Exception as e:
        logsvc.log.warning(f"admin nl feedback failed: {e}")
        _log_admin_nl(logsvc, stage="feedback_exception", ctx=ctx, plan=plan, reason=str(e))
        return True
    _log_admin_nl(logsvc, stage="executed", ctx=ctx, plan=plan, outcome=out)
    return True
