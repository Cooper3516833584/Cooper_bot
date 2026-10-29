"""One-round QQ quote resolution with explicit conversation and privacy checks."""
from __future__ import annotations

import copy
from dataclasses import is_dataclass, replace
import re
import time

from .message_context import extract_reply_message_id, has_reply_segment, message_id, normalize_onebot_event, positive_id, request_message_id
from cooper_bot.modules.vision.vision_skill import VisionSkill


async def message_allowed(aisvc, item, *, memory_turn=None, identity=None):
    if item.get("_owner_unverified"):
        return False
    service = getattr(aisvc, "memory", None)
    store = getattr(service, "store", None)
    checker = getattr(store, "member_enabled", None)
    if not callable(checker):
        return True
    scope = getattr(memory_turn, "scope_id", None)
    if identity is not None:
        selector = getattr(service, "_identity_scope", None)
        if callable(selector):
            # Quotes originate in the ordinary QQ conversation, even for an admin task.
            source_identity = replace(identity, profile="public") if is_dataclass(identity) else identity
            try:
                scope = selector(source_identity)
                if scope and not getattr(service, "_started", False):
                    await service.start()
            except Exception:
                return False
    if not isinstance(scope, str) or not scope:
        return True
    speaker = item.get("_speaker") or item.get("speaker") or {}
    owner = positive_id(item.get("_owner_user_id")) or (positive_id(speaker.get("user_id")) if item.get("role") == "user" else None)
    if owner:
        try:
            return bool(await checker(scope, owner))
        except Exception:
            return False
    return True


def _verified_event(data, current_event, session_key, reply_id):
    actual_id = message_id(data.get("message_id"))
    if actual_id and actual_id != reply_id:
        return None
    sender = data.get("sender") if isinstance(data.get("sender"), dict) else {}
    author = positive_id(sender.get("user_id")) or positive_id(data.get("user_id"))
    if not author or (positive_id(data.get("user_id")) and positive_id(data.get("user_id")) != author):
        return None
    bot = positive_id(current_event.get("self_id"))
    if data.get("self_id") is not None and positive_id(data.get("self_id")) != bot:
        return None
    kind, _, target = session_key.partition(":")
    target = positive_id(target)
    if kind not in {"group", "private"} or not target:
        return None
    if data.get("message_type") not in (None, kind):
        return None
    if kind == "group":
        if positive_id(data.get("group_id")) != target:
            return None
    else:
        if positive_id(data.get("group_id")):
            return None
        if author == bot:
            if positive_id(data.get("target_id")) != target:
                return None
        elif author != target:
            return None
        elif data.get("target_id") is not None and positive_id(data.get("target_id")) != bot:
            return None
    return dict(data, post_type="message_sent" if author == bot else "message", message_type=kind,
                self_id=bot, user_id=author, message_id=reply_id)


def _author_valid(item, key):
    if item.get("role") == "assistant":
        return True
    speaker = item.get("_speaker")
    author = positive_id(speaker.get("user_id")) if isinstance(speaker, dict) else None
    return bool(item.get("role") == "user" and author and (not key.startswith("private:") or author == positive_id(key.partition(":")[2])))


async def resolve_quote_for_aichat(*, api, aisvc, session_key, event, vision_skill=None, memory_turn=None, identity=None):
    reply_id = extract_reply_message_id(event)
    if not reply_id:
        return {"available": False, "message_id": "", "reason": "invalid_id"} if has_reply_segment(event) else None
    unavailable = {"available": False, "message_id": reply_id, "reason": "not_found_or_unverified"}
    if not session_key or aisvc is None:
        return unavailable
    finder = getattr(aisvc, "find_chat_message_by_msg_id", None)
    try:
        found = finder(session_key, reply_id) if callable(finder) else None
    except Exception:
        found = None
    item = copy.deepcopy(found) if isinstance(found, dict) else None
    cutoff = event.get("_chat_ingest_seq")
    if item and type(cutoff) is int and item.get("_ingest_seq", 0) >= cutoff:
        return dict(unavailable, reason="not_observed_at_request")
    if item and not await message_allowed(aisvc, item, memory_turn=memory_turn, identity=identity):
        return dict(unavailable, reason="source_not_permitted")
    usable = bool(item and _author_valid(item, session_key) and item.get("content"))
    pure_media = usable and bool(re.fullmatch(r"(?:\[(?:图片|QQ表情|表情|语音|视频|文件|消息|转发消息)[^\]]*\]\s*)+", item["content"]))
    missing_visual = usable and not item.get("_vision") and getattr(vision_skill, "ready", False) and bool(re.search(r"\[(?:图片|QQ表情|表情)\]", item["content"]))
    needs_fetch = not usable or ((pure_media or missing_visual) and not item.get("_vision"))
    reference_event = None
    if needs_fetch:
        numeric_id = request_message_id(reply_id)
        call = getattr(api, "call", None)
        if numeric_id is None or not callable(call):
            return unavailable if not usable else await _quote_result(aisvc, item, reply_id, memory_turn, identity)

        async def temporary_fetch_failure():
            if not usable:
                return unavailable
            quote = await _quote_result(aisvc, item, reply_id, memory_turn, identity)
            if quote.get("available"):
                quote["reason"] = "media_fetch_failed_cached_context"
            return quote

        try:
            response = await call("get_msg", {"message_id": numeric_id}, timeout=8.0)
        except Exception:
            return await temporary_fetch_failure()
        if response is None or (isinstance(response, dict) and (response.get("status") != "ok" or (type(response.get("retcode")) is int and response["retcode"] != 0))):
            return await temporary_fetch_failure()
        if not isinstance(response, dict) or response.get("status") != "ok" or type(response.get("retcode")) is not int or response["retcode"] != 0 or not isinstance(response.get("data"), dict):
            return unavailable
        reference_event = _verified_event(response["data"], event, session_key, reply_id)
        normalized = normalize_onebot_event(reference_event) if reference_event else None
        if normalized is None or normalized[0] != session_key:
            return dict(unavailable, reason="scope_or_author_unverified")
        if usable and (item["role"] != normalized[1]["role"] or (item["role"] == "user" and positive_id(item["_speaker"].get("user_id")) != positive_id(normalized[1]["_speaker"].get("user_id")))):
            return dict(unavailable, reason="scope_or_author_unverified")
        item = normalized[1]
        item["_quote_fetched"] = True
        # Existing owner privacy metadata must not be bypassed through get_msg.
        if isinstance(found, dict):
            for field in ("_owner_user_id", "_owner_unverified"):
                if field in found:
                    item[field] = found[field]
        if not await message_allowed(aisvc, item, memory_turn=memory_turn, identity=identity):
            return dict(unavailable, reason="source_not_permitted")
    if vision_skill is not None and getattr(vision_skill, "ready", False):
        try:
            slots = [VisionSkill._coerce_slot(slot) for slot in item.get("_vision", [])]
            if not slots and reference_event:
                slots = vision_skill.create_slots_from_event(reference_event, message_id=f"ref-{reply_id}", source_kind="reply_reference")
            targets = [slot for slot in slots if slot.status not in {"ready", "permanent_error"} and time.time() >= slot.retry_after_ts]
            if targets:
                resolutions = await vision_skill.resolve_slots(api, targets)
                slots = vision_skill.apply_resolutions_to_slots(slots, resolutions)
            if slots:
                item["_vision"] = [slot.to_dict() for slot in slots]
                attach = getattr(aisvc, "attach_chat_vision", None)
                if found and callable(attach):
                    attach(session_key, reply_id, slots)
        except Exception:
            # Text and verified authorship remain useful when vision is unavailable.
            pass
    return await _quote_result(aisvc, item, reply_id, memory_turn, identity)


async def _quote_result(aisvc, item, reply_id, memory_turn, identity):
    if not await message_allowed(aisvc, item, memory_turn=memory_turn, identity=identity):
        return {"available": False, "message_id": reply_id, "reason": "source_not_permitted"}
    renderer = getattr(aisvc, "_render_chat_message_content", None)
    if callable(renderer):
        content = renderer(item)
    else:
        content = str(item.get("content") or "")
        for slot in item.get("_vision", []):
            if slot.get("status") == "ready" and slot.get("description"):
                content += "\n[视觉内容] " + slot["description"]
    if len(content) > 6000:
        content = content[:6000] + "\n[引用原文过长，已截断]"
    speaker = {"name": str(getattr(aisvc, "bot_nick", "bot")), "is_bot": True} if item["role"] == "assistant" else dict(item["_speaker"])
    result = {"available": True, "message_id": reply_id, "role": item["role"], "speaker": speaker, "content": content}
    if item.get("_quote_fetched"):
        result["_from_get_msg"] = True
    if item["role"] == "user":
        result["_owner_user_id"] = positive_id(speaker.get("user_id"))
    for field in ("_owner_user_id", "_owner_unverified"):
        if field in item:
            result[field] = item[field]
    return result
