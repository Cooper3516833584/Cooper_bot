"""Current-conversation context and text/mention replies owned by the bot."""
from __future__ import annotations

import html
import json
import re

from .message_context import message_segments, normalize_onebot_event, positive_id
from .quote_context import message_allowed


_CQ_AT_RE = re.compile(r"\[CQ:at,[^\]]*\]", re.I)
_INLINE_AT_RE = re.compile(
    r"(?P<literal>https?://[^\s<>]+|`+[^`]*`+|[A-Za-z0-9_.+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})"
    r"|(?P<cq>\[CQ:at,[^\]]*\])"
    r"|(?<![A-Za-z0-9_@])[@＠](?P<qq>[0-9]{1,20})(?![A-Za-z0-9_@])", re.I,
)


def mentioned_users(event):
    return sorted({uid for segment in message_segments(event.get("message"), event.get("raw_message", ""))
                   if isinstance(segment, dict) and segment.get("type") == "at"
                   and isinstance(segment.get("data"), dict)
                   and (uid := positive_id(segment["data"].get("qq")))
                   and uid != positive_id(event.get("self_id"))})


async def recent_context(api, evt, aisvc, identity, session_key, cutoff):
    rows = []
    history = getattr(aisvc, "message_history", None)
    if history is not None:
        try:
            events = await history.context(
                api, evt, max_messages=aisvc._CHAT_CONTEXT_MAX_MESSAGES,
                ttl_seconds=aisvc._CHAT_CONTEXT_TTL_SECONDS,
                actor_id=identity.actor_user_id if identity and identity.profile == "admin" else None)
            for event in events:
                normalized = normalize_onebot_event(event)
                if normalized and normalized[0] == session_key:
                    item = dict(normalized[1], _history_time=event["time"])
                    if item["role"] == "assistant":
                        if session_key.startswith("private:"):
                            item["_owner_user_id"] = positive_id(session_key.partition(":")[2])
                        else:
                            owner = history.message_owner(event)
                            if owner:
                                item["_owner_user_id"] = owner
                            else:
                                # Group history cannot identify an old reply's requester.
                                item["_owner_unverified"] = True
                    rows.append(item)
        except Exception as exc:
            aisvc.log.warning(f"Chat history recovery unavailable: {type(exc).__name__}")
    loader = getattr(aisvc, "_load_active_chat_history", None)
    if callable(loader):
        rows.extend(item for item in loader(session_key)
                    if not cutoff or not item.get("_ingest_seq") or item["_ingest_seq"] < cutoff)
    if history is not None:
        try:
            rows = await history.filter_context(identity, rows)
        except (OSError, ValueError, TypeError):
            return []
    current_id = str(evt.get("message_id") or "")
    allowed = {}
    permissions = {}
    checked = []
    for item in reversed(rows):
        mid = item.get("_msg_id")
        if not mid or mid == current_id or mid in allowed:
            continue
        if item.get("_owner_unverified"):
            continue
        owner = positive_id(item.get("_owner_user_id")) or (positive_id(item.get("_speaker", {}).get("user_id")) if item.get("role") == "user" else None)
        if owner not in permissions:
            permissions[owner] = await message_allowed(aisvc, item, identity=identity)
        if not permissions[owner]:
            continue
        allowed[mid] = True
        checked.append(item)
    ordered = sorted(reversed(checked), key=lambda item: float(item.get("_history_time") or item.get("_event_time") or 0))
    limit = max(0, int(getattr(aisvc, "_CHAT_CONTEXT_MAX_MESSAGES", 300)))
    return ordered[-limit:] if limit else []


def _render_mentions(body, *, group, speakers, targets, escape_body=False):
    def escape_cq(value):
        return _CQ_AT_RE.sub(lambda m: m[0].replace("[", "&#91;").replace("]", "&#93;"), value)

    def replace(match):
        value = match[0]
        if match.group("literal"):
            return value if escape_body else escape_cq(value)
        is_cq = match.group("cq") is not None
        uid = positive_id(message_segments(value)[0]["data"].get("qq")) if is_cq else positive_id(match.group("qq"))
        if not group or uid is None or uid not in speakers or (uid not in targets and len(targets) >= 10):
            if is_cq:
                return value if escape_body else escape_cq(value)
            return "该用户"
        if uid not in targets:
            targets.append(uid)
        return ""

    # 仅已确认的群成员可转成真正的 @；与结构化目标合并后，同一人只保留一次。
    body = _INLINE_AT_RE.sub(replace, body)
    if escape_body:
        body = html.escape(body, quote=False).replace("[", "&#91;").replace("]", "&#93;")
    return " ".join([*(f"[CQ:at,qq={uid}]" for uid in targets), body.strip()]).strip() if targets else body


def render_reply(text, *, group, speakers):
    try:
        payload = json.loads(text)
    except (ValueError, TypeError):
        payload = None
    if not isinstance(payload, dict) or "qq_reply" not in payload:
        return _render_mentions(text, group=group, speakers=speakers, targets=[]) if isinstance(text, str) else text
    reply = payload["qq_reply"]
    if set(payload) != {"qq_reply"} or not isinstance(reply, dict) or set(reply) not in ({"text"}, {"text", "at"}):
        raise ValueError("invalid QQ reply envelope")
    body, mentions = reply["text"], reply.get("at", [])
    if not isinstance(body, str) or not body.strip() or not isinstance(mentions, list) or len(mentions) > 10:
        raise ValueError("invalid QQ reply content")
    targets = list(dict.fromkeys(positive_id(uid) for uid in mentions))
    if targets and (not group or any(uid is None or uid not in speakers for uid in targets)):
        raise ValueError("unverified QQ mention target")
    return _render_mentions(body, group=group, speakers=speakers, targets=targets, escape_body=True)
