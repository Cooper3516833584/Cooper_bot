"""Pure OneBot message normalization; no I/O or history of its own."""
from __future__ import annotations

import hashlib
import html
import json
import re


MAX_CONTENT_CHARS = 12000
_CQ = re.compile(r"\[CQ:([^,\]\s]+)((?:,[^\]]*)?)\]", re.I)
_MESSAGE_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")


def positive_id(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    raw = str(value).strip()
    return int(raw) if raw.isascii() and raw.isdigit() and len(raw) <= 20 and int(raw) > 0 else None


def message_id(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return ""
    raw = str(value).strip()
    return raw if _MESSAGE_ID.fullmatch(raw) and raw not in {"0", "-0", "-"} else ""


def request_message_id(value):
    raw = message_id(value)
    if re.fullmatch(r"-?[0-9]{1,19}", raw) and 0 < abs(int(raw)) <= 2**63 - 1:
        return int(raw)
    return None


def message_segments(message, raw_message=""):
    if isinstance(message, list) and message:
        return message[:512]
    text = message if isinstance(message, str) and message else raw_message if isinstance(raw_message, str) else ""
    text = text[:MAX_CONTENT_CHARS * 4]
    segments = []
    start = 0
    for match in _CQ.finditer(text):
        if match.start() > start:
            segments.append({"type": "text", "data": {"text": html.unescape(text[start:match.start()])}})
        data = {}
        for pair in match[2].lstrip(",").split(","):
            if "=" in pair:
                key, value = pair.split("=", 1)
                data[key] = html.unescape(value)
        segments.append({"type": match[1].lower(), "data": data})
        start = match.end()
    if start < len(text):
        segments.append({"type": "text", "data": {"text": html.unescape(text[start:])}})
    return segments[:512]


def extract_reply_message_id(evt):
    if not isinstance(evt, dict):
        return None
    for source in (evt.get("message"), evt.get("raw_message")):
        for segment in message_segments(source):
            if not isinstance(segment, dict) or str(segment.get("type", "")).lower() != "reply":
                continue
            data = segment.get("data")
            if isinstance(data, dict):
                found = message_id(data.get("id") or data.get("message_id"))
                if found:
                    return found
    return None


def has_reply_segment(evt):
    if not isinstance(evt, dict):
        return False
    return any(isinstance(segment, dict) and str(segment.get("type", "")).lower() == "reply"
               for source in (evt.get("message"), evt.get("raw_message")) for segment in message_segments(source))


def format_message_content(message, raw_message="", *, self_id=""):
    parts = []
    labels = {"image": "图片", "face": "QQ表情", "mface": "表情", "market_face": "表情", "record": "语音",
              "video": "视频", "file": "文件", "json": "卡片", "xml": "卡片", "forward": "转发消息"}
    total = 0
    for segment in message_segments(message, raw_message):
        if not isinstance(segment, dict) or not isinstance(segment.get("data"), dict):
            continue
        kind = str(segment.get("type", "")).lower()
        data = segment["data"]
        if kind == "reply":
            continue
        if kind == "text":
            value = data.get("text") if isinstance(data.get("text"), str) else ""
        elif kind == "at":
            qq = str(data.get("qq") or data.get("user_id") or "")
            name = str(data.get("name") or data.get("nickname") or "")[:80]
            value = "@全体成员" if qq == "all" else "@" + (name or str(positive_id(qq) or "未知用户"))
        else:
            name = str(data.get("name") or "").replace("\\", "/").rsplit("/", 1)[-1][:160] if kind == "file" else ""
            value = "[" + labels.get(kind, "消息") + (":" + name if name else "") + "]"
        parts.append(value[:MAX_CONTENT_CHARS + 1])
        total += len(value)
        if total > MAX_CONTENT_CHARS:
            break
    out = "".join(parts).strip()
    return out if len(out) <= MAX_CONTENT_CHARS else out[:MAX_CONTENT_CHARS] + "\n[原文过长，已截断]"


def is_self_message(evt):
    if not isinstance(evt, dict):
        return False
    if evt.get("post_type") == "message_sent":
        return True
    sender = evt.get("sender") if isinstance(evt.get("sender"), dict) else {}
    bot = positive_id(evt.get("self_id"))
    authors = (positive_id(evt.get("user_id")), positive_id(sender.get("user_id")))
    return evt.get("post_type") == "message" and bot is not None and bot in authors


def session_key_from_onebot_event(evt):
    if not isinstance(evt, dict) or evt.get("post_type") not in ("message", "message_sent"):
        return None
    kind = evt.get("message_type")
    if kind == "group":
        group = positive_id(evt.get("group_id"))
        return f"group:{group}" if group else None
    if kind != "private":
        return None
    sender = evt.get("sender") if isinstance(evt.get("sender"), dict) else {}
    if is_self_message(evt):
        bot = positive_id(evt.get("self_id"))
        peers = {positive_id(evt.get(key)) for key in ("target_id", "peer_id", "recipient_id")}
        peers.discard(None)
        peers.discard(bot)
        return f"private:{next(iter(peers))}" if bot and len(peers) == 1 else None
    peer = positive_id(evt.get("user_id")) or positive_id(sender.get("user_id"))
    return f"private:{peer}" if peer else None


def normalize_onebot_event(evt):
    key = session_key_from_onebot_event(evt)
    mid = message_id(evt.get("message_id")) if isinstance(evt, dict) else ""
    if not key or not mid:
        return None
    sender = evt.get("sender") if isinstance(evt.get("sender"), dict) else {}
    author = positive_id(evt.get("user_id")) or positive_id(sender.get("user_id"))
    own = is_self_message(evt)
    if evt.get("post_type") == "message" and positive_id(evt.get("user_id")) and positive_id(sender.get("user_id")) and positive_id(evt["user_id"]) != positive_id(sender["user_id"]):
        return None
    if not own and (not author or (positive_id(sender.get("user_id")) and positive_id(sender.get("user_id")) != author)):
        return None
    reply_id = extract_reply_message_id(evt)
    content = format_message_content(evt.get("message"), evt.get("raw_message"))
    if not content and (reply_id or has_reply_segment(evt)):
        content = "[引用消息]"
    if not content:
        return None
    speaker = {"is_bot": True} if own else {"user_id": author, "nickname": str(sender.get("nickname") or "")[:200], "card": str(sender.get("card") or "")[:200], "is_bot": False}
    item = {"role": "assistant" if own else "user", "content": content, "_msg_id": mid, "_speaker": speaker, "_source": "event"}
    if reply_id:
        item["_reply_to"] = reply_id
    return key, item


def normalize_send_ack(scene, target_id, body, message_id_value):
    target = positive_id(target_id)
    mid = message_id(message_id_value)
    content = format_message_content(body)
    if scene not in {"group", "private"} or not target or not mid or not content:
        return None
    return f"{scene}:{target}", {"role": "assistant", "content": content, "_msg_id": mid, "_speaker": {"is_bot": True}, "_source": "send_ack"}


def body_key(message):
    """Full-body equality key for bounded owner metadata, never delivery evidence."""
    if (isinstance(message, str) and len(message) > MAX_CONTENT_CHARS * 4) or (isinstance(message, list) and len(message) > 512):
        return None
    segments = message_segments(message)
    canonical = []
    for segment in segments:
        if not isinstance(segment, dict) or not isinstance(segment.get("data"), dict):
            return None
        kind, data = str(segment.get("type", "")).lower(), segment["data"]
        if kind == "text" and canonical and canonical[-1][0] == "text":
            canonical[-1][1]["text"] += str(data.get("text") or "")
        else:
            canonical.append([kind, dict(data)])
    try:
        raw = json.dumps(canonical, ensure_ascii=False, sort_keys=True).encode("utf-8")
    except (TypeError, ValueError, RecursionError):
        return None
    return hashlib.sha256(raw).hexdigest() if len(raw) <= MAX_CONTENT_CHARS * 4 else None
