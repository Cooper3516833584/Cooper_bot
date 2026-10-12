from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from cooper_bot.core.config import ADMIN_USERS, KICK_AUDIT_QQ


@dataclass(frozen=True)
class KickCommand:
    action: str
    arguments: str = ""


def personal_level(user_id: int, perm=None) -> int | None:
    uid = int(user_id or 0)
    if uid <= 0:
        return None
    if uid in ADMIN_USERS:
        return 3
    if perm is None or not callable(getattr(perm, "get_level", None)):
        return None
    try:
        level = perm.get_level(uid)
    except Exception:
        return None
    if isinstance(level, bool) or not isinstance(level, int):
        return None
    return level


def parse_kick_command(evt: dict, text: str, ctx=None) -> KickCommand | None:
    if not isinstance(evt, dict) or str(evt.get("post_type") or "").lower() != "message":
        return None
    if str(evt.get("message_type") or "").lower() != "group":
        return None
    if ctx is not None and str(getattr(ctx, "scene", "")) != "group":
        return None
    if evt.get("anonymous"):
        return None
    message = evt.get("message")
    if isinstance(message, list):
        parts: list[str] = []
        for segment in message:
            if not isinstance(segment, dict) or str(segment.get("type") or "").lower() != "text":
                return None
            data = segment.get("data")
            if not isinstance(data, dict):
                return None
            parts.append(str(data.get("text") or ""))
        command_text = "".join(parts)
    elif isinstance(message, str):
        command_text = message
    else:
        command_text = str(text or "")
    normalized = unicodedata.normalize("NFKC", command_text).strip()
    match = re.fullmatch(r"/kick(?:\s+(.*))?", normalized, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    arguments = str(match.group(1) or "").strip()
    action = arguments.split(maxsplit=1)[0].lower() if arguments else ""
    remainder = arguments[len(action):].strip() if action else ""
    return KickCommand(action=action, arguments=remainder)


def _strict_ok(resp) -> bool:
    if not isinstance(resp, dict) or resp.get("status") != "ok":
        return False
    code = resp.get("retcode")
    return not isinstance(code, bool) and (code == 0 or code == "0")


async def _send_command_reply(api, ctx, text: str) -> None:
    if api is None or getattr(ctx, "group_id", None) is None:
        return
    try:
        await api.send_group_msg(
            int(ctx.group_id),
            [{"type": "text", "data": {"text": str(text)}}],
        )
    except Exception:
        return


def _valid_positive_id(value) -> int | None:
    if isinstance(value, bool):
        return None
    raw = str(value or "").strip()
    if not raw.isascii() or not raw.isdigit():
        return None
    result = int(raw)
    return result if result > 0 else None


def _audit_recipient(perm=None) -> int | None:
    target = int(KICK_AUDIT_QQ or min(ADMIN_USERS or {0}))
    if target <= 0:
        return None
    if target in ADMIN_USERS:
        return target
    if personal_level(target, perm) == 3:
        return target
    return None


async def handle_kick_command(
    *, service, api, ctx, evt: dict, text: str, perm=None, aisvc=None, vision_skill=None,
    control_token=None,
) -> bool:
    command = parse_kick_command(evt, text, ctx)
    if command is None:
        return False
    uid = _valid_positive_id(getattr(ctx, "user_id", None))
    level = personal_level(uid or 0, perm)
    if level not in (2, 3):
        await _send_command_reply(api, ctx, "此命令需要个人 Bot 权限 2 或 3 级。")
        return True
    self_id = _valid_positive_id(evt.get("self_id"))
    group_id = _valid_positive_id(evt.get("group_id"))
    if getattr(ctx, "scene", None) != "group" or self_id is None or group_id is None or uid is None:
        await _send_command_reply(api, ctx, "此命令只能在有效的群消息中使用。")
        return True
    if command.action not in {"on", "off", "status"} or command.arguments:
        await _send_command_reply(api, ctx, "用法：/kick on、/kick off、/kick status")
        return True
    if service is None:
        await _send_command_reply(api, ctx, "反广告审核服务暂时不可用。")
        return True

    scope = (self_id, group_id)
    if command.action == "status":
        try:
            status = await service.status_scope(api, self_id, group_id, aisvc, vision_skill)
            enabled = "开启" if status.get("enabled") else "关闭"
            role = str(status.get("bot_role") or "unknown")
            ready = "就绪" if status.get("ready") else "未就绪"
            await _send_command_reply(api, ctx, f"本群自动反广告：{enabled}；审核服务：{ready}；Bot 群身份：{role}。")
        except Exception:
            await _send_command_reply(api, ctx, "状态暂时无法读取。")
        return True

    if command.action == "off":
        token = control_token or service.begin_control(scope, enabled=False)
        try:
            saved = await service.disable_scope(token)
        except Exception:
            saved = False
        message = "我不杀哩😭" if saved else "已在本次运行停止，但保存失败，重启后请确认状态。"
        await _send_command_reply(api, ctx, message)
        return True

    recipient = _audit_recipient(perm)
    if recipient is None:
        await _send_command_reply(api, ctx, "未配置可信的管理员证据接收人，无法开启。")
        return True
    token = control_token or service.begin_control(scope, enabled=True)
    try:
        enabled = await service.enable_scope(
            api=api, token=token, recipient=recipient, aisvc=aisvc, vision_skill=vision_skill
        )
    except Exception:
        enabled = False
    if enabled:
        await _send_command_reply(api, ctx, "开杀😋")
    else:
        await _send_command_reply(api, ctx, "开启失败：请确认 Bot 群管理权限、文本/视觉模型就绪及本地存储可用。")
    return True
