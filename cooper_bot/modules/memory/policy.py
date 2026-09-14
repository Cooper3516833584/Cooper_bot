from __future__ import annotations

import re
from dataclasses import dataclass

from .models import CapturedInput, MemoryIdentity

_SECRET_RE = re.compile(r"(?:api[_ -]?key|password|passwd|secret|token)\s*[:=]|-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----|sk-[A-Za-z0-9_-]{12,}", re.I)
_RESOURCE_RE = re.compile(r"(?:https?://|file://|data:[^\s;,]+;base64,|[A-Za-z]:[\\/]|(?:^|\s)/(?:home|Users|tmp|var)/)", re.I)


@dataclass(frozen=True)
class MemoryPolicy:
    scope_id: str
    enabled: bool
    capture_mode: str
    admin_explicit_only: bool


def safe_text(value: object, limit: int) -> tuple[str, bool, bool]:
    text = str(value or "").strip()
    if _SECRET_RE.search(text):
        return "", False, True
    if len(text) > limit:
        return text[:limit], True, False
    return text, False, False


def safe_context_text(value: object, limit: int) -> tuple[str, bool, bool]:
    text, truncated, secret = safe_text(value, limit)
    if secret or _RESOURCE_RE.search(text):
        return "", False, True
    return text, truncated, False


def valid_identity(identity: MemoryIdentity) -> bool:
    if identity.bot_id <= 0 or identity.actor_user_id <= 0:
        return False
    if identity.scene == "group":
        return bool(identity.group_id and identity.group_id > 0)
    return identity.group_id is None


def scope_for(identity: MemoryIdentity) -> str:
    if not valid_identity(identity):
        raise ValueError("invalid trusted memory identity")
    if identity.profile == "admin":
        if identity.scene == "group":
            return f"qq:{identity.bot_id}:admin:group:{identity.group_id}:actor:{identity.actor_user_id}"
        return f"qq:{identity.bot_id}:admin:private:{identity.actor_user_id}"
    if identity.scene == "group":
        return f"qq:{identity.bot_id}:public:group:{identity.group_id}"
    return f"qq:{identity.bot_id}:public:private:{identity.actor_user_id}"


def may_capture(identity: MemoryIdentity, item: CapturedInput, policy: MemoryPolicy, member_enabled: bool) -> bool:
    if not policy.enabled or not member_enabled:
        return False
    if identity.profile == "admin":
        return item.source_kind in {"explicit_memory", "request_metadata"}
    if item.source_kind == "explicit_memory":
        return True
    if item.source_kind == "direct_chat":
        return True
    return item.source_kind == "passive_chat" and identity.scene == "group" and policy.capture_mode == "all"
