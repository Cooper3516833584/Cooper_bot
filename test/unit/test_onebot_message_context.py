import pytest

from cooper_bot.modules.ai.message_context import (
    extract_reply_message_id, format_message_content, is_self_message,
    normalize_onebot_event, normalize_send_ack, request_message_id,
)


def event(*, group=20001, user=10001, mid="m1", post="message", kind="group", message="hello", **kwargs):
    return {"post_type": post, "message_type": kind, "self_id": 90001, "group_id": group,
            "user_id": user, "message_id": mid, "message": message, **kwargs}


@pytest.mark.parametrize("source", [
    [{"type": "reply", "data": {"message_id": 123}}],
    "[CQ:reply,id=123]", "[CQ:reply,seq=10,id=123]", "[CQ:reply,id=123,other=value]",
])
def test_reply_id_is_parsed_by_key(source):
    assert extract_reply_message_id({"message": source}) == "123"


def test_structured_body_wins_and_media_never_exposes_sources():
    message = [{"type": "text", "data": {"text": "text\n"}}, {"type": "image", "data": {"file": "base64://private", "url": "https://private.invalid"}}, {"type": "reply", "data": {"id": 5}}]
    assert format_message_content(message, "duplicate") == "text\n[图片]"
    assert "private" not in format_message_content("[CQ:image,file=base64://private][CQ:json,data=private]")
    assert format_message_content("[CQ:at,qq=all]") == "@全体成员"
    assert len(format_message_content("x" * 13000)) < 12100


def test_group_private_and_bot_peers_remain_isolated():
    assert normalize_onebot_event(event())[0] == "group:20001"
    assert normalize_onebot_event(event(group=20002))[0] == "group:20002"
    assert normalize_onebot_event(event(kind="private"))[0] == "private:10001"
    assert normalize_onebot_event(event(kind="private", user=10002))[0] == "private:10002"
    assert normalize_onebot_event(event(kind="private", user=90001, post="message_sent")) is None
    own = normalize_onebot_event(event(kind="private", user=90001, post="message_sent", target_id=10002))
    assert own[0] == "private:10002" and own[1]["role"] == "assistant"
    assert is_self_message(event(user=90001))


@pytest.mark.parametrize("updates", [{"group_id": True}, {"user_id": "bad"}, {"message_id": {}}, {"post_type": "notice"}, {"message_type": "unknown"}, {"post_type": {}}, {"message_type": []}])
def test_malformed_events_do_not_produce_history(updates):
    item = event();item.update(updates)
    assert normalize_onebot_event(item) is None


def test_ack_and_numeric_request_ids_are_validated_separately():
    assert normalize_send_ack("private", 10001, "text", "opaque-id")[0] == "private:10001"
    assert request_message_id("opaque-id") is None
    assert request_message_id("-123") == -123
    assert request_message_id(True) is None
    assert normalize_send_ack("group", 0, "text", 1) is None


def _service():
    from types import SimpleNamespace
    from cooper_bot.modules.ai.aisvc import AIService
    return AIService(SimpleNamespace(info=lambda *_: None, warning=lambda *_: None))


@pytest.mark.parametrize("ack_first", [True, False])
def test_event_and_ack_upsert_preserve_first_order_and_event_text(ack_first):
    service = _service()
    def ack(): service.observe_send_ack(scene="group", target_id=20001, message="brief", message_id="b1")
    def report(): service.observe_onebot_event(event(post="message_sent", user=90001, mid="b1", message="full event"))
    (ack if ack_first else report)()
    sequence = service.find_chat_message_by_msg_id("group:20001", "b1")["_ingest_seq"]
    (report if ack_first else ack)()
    history = service._load_active_chat_history("group:20001")
    assert len(history) == 1
    assert history[0]["content"] == "full event"
    assert history[0]["_source"] == "event"
    assert history[0]["_ingest_seq"] == sequence


def test_replays_and_same_body_different_ids_are_distinct_and_scoped():
    service = _service()
    for _ in range(3): service.observe_onebot_event(event())
    service.observe_onebot_event(event(mid="m2", user=10002))
    service.observe_onebot_event(event(group=20002))
    assert [x["_msg_id"] for x in service._load_active_chat_history("group:20001")] == ["m1", "m2"]
    assert len(service._load_active_chat_history("group:20002")) == 1
    assert service.find_chat_message_by_msg_id("group:20001", "m2")["_speaker"]["user_id"] == 10002
    for user in (10001, 10002):
        service.observe_onebot_event(event(kind="private", user=user, mid="same", message=str(user)))
    assert service.find_chat_message_by_msg_id("private:10001", "same")["content"] == "10001"
    assert service.find_chat_message_by_msg_id("private:10002", "same")["content"] == "10002"


def test_context_cap_and_session_idle_ttl(monkeypatch):
    import cooper_bot.modules.ai.aisvc as module
    clock = [1000.0]
    monkeypatch.setattr(module.time, "time", lambda: clock[0])
    service = _service()
    for i in range(301):
        service.observe_onebot_event(event(mid=f"m{i}"))
    history = service._load_active_chat_history("group:20001")
    assert len(history) == 300 and history[0]["_msg_id"] == "m1"
    clock[0] += 1700
    service.observe_onebot_event(event(mid="active"))
    clock[0] += 1700
    assert len(service._load_active_chat_history("group:20001")) == 300
    clock[0] += 101
    assert service._load_active_chat_history("group:20001") == []


def test_vision_resolution_is_not_downgraded_by_duplicate_event():
    service = _service()
    slot = {"slot_id": "m1:1", "status": "ready", "description": "resolved"}
    service.observe_onebot_event(event(), vision_slots=[slot])
    service.observe_onebot_event(event(), vision_slots=[dict(slot, status="unresolved", description="")])
    assert service.find_chat_message_by_msg_id("group:20001", "m1")["_vision"][0]["description"] == "resolved"


def test_retryable_vision_can_be_upgraded_by_quote_resolution():
    service = _service()
    service.observe_onebot_event(event(), vision_slots=[{"slot_id": "m1:1", "status": "retryable_error", "retry_after_ts": 1}])
    service.attach_chat_vision("group:20001", "m1", [{"slot_id": "m1:1", "status": "ready", "description": "resolved"}])
    assert service.find_chat_message_by_msg_id("group:20001", "m1")["_vision"][0]["description"] == "resolved"


def test_history_vision_does_not_retain_inline_image_payloads():
    service = _service()
    service.observe_onebot_event(event(message=[{"type": "image", "data": {"file": "base64://private"}}]), vision_slots=[{"slot_id": "m1:1", "file": "base64://private", "url": "data:image/png;base64,private", "status": "ready", "description": "known image"}])
    row = service.find_chat_message_by_msg_id("group:20001", "m1")
    assert row["_vision"][0]["file"] == row["_vision"][0]["url"] == ""
    assert row["_vision"][0]["description"] == "known image"


def test_quote_only_event_has_a_real_observation_boundary():
    service = _service()
    sequence = service.observe_onebot_event(event(message=[{"type": "reply", "data": {"id": "101"}}]))
    assert sequence > 0
    row = service.find_chat_message_by_msg_id("group:20001", "m1")
    assert row["content"] == "[引用消息]" and row["_reply_to"] == "101"


def test_synthetic_notice_preserves_pending_owner_and_observation_order():
    service = _service()
    sequence = service.observe_onebot_event(event())
    service.register_send_owner("group:20001", "pending reply", 10001)
    service.remember_user_message("group:20001", "[群文件上传通知] source")
    history = service._load_active_chat_history("group:20001")
    assert history[-1]["_ingest_seq"] > sequence and "_msg_id" not in history[-1]
    service.observe_onebot_event(event(post="message_sent", user=90001, mid="b1", message="pending reply"))
    assert service.find_chat_message_by_msg_id("group:20001", "b1")["_owner_user_id"] == 10001


def test_owner_metadata_never_fabricates_delivery_and_ambiguity_is_not_guessed():
    service = _service()
    token = service.register_send_owner("group:20001", "reply", 10001)
    assert service._load_active_chat_history("group:20001") == []
    service.observe_send_ack(scene="group", target_id=20001, message="reply", message_id="b1")
    assert service.find_chat_message_by_msg_id("group:20001", "b1")["_owner_user_id"] == 10001
    service.register_send_owner("group:20001", "same", 10001)
    service.register_send_owner("group:20001", "same", 10002)
    service.observe_send_ack(scene="group", target_id=20001, message="same", message_id="b2")
    assert service.find_chat_message_by_msg_id("group:20001", "b2")["_owner_unverified"]
    assert "_owner_user_id" not in service.find_chat_message_by_msg_id("group:20001", "b2")
    service.annotate_chat_owner("group:20001", "b2", 10002)
    assert service.find_chat_message_by_msg_id("group:20001", "b2")["_owner_user_id"] == 10002
    assert not service.find_chat_message_by_msg_id("group:20001", "b2").get("_owner_unverified")


def test_unrelated_notice_does_not_consume_or_inherit_pending_owner():
    service = _service()
    token = service.register_send_owner("group:20001", "user-triggered reply", 10001)
    service.observe_send_ack(scene="group", target_id=20001, message="independent calendar notice", message_id="notice-1")
    notice = service.find_chat_message_by_msg_id("group:20001", "notice-1")
    assert notice["role"] == "assistant" and notice["_msg_id"] == "notice-1"
    assert not notice.get("_owner_unverified") and "_owner_user_id" not in notice
    assert [claim["token"] for claim in service._chat_sessions["group:20001"]["_pending_owners"]] == [token]
    service.observe_send_ack(scene="group", target_id=20001, message="user-triggered reply", message_id="reply-1")
    assert service.find_chat_message_by_msg_id("group:20001", "reply-1")["_owner_user_id"] == 10001
    assert [row["_msg_id"] for row in service._load_active_chat_history("group:20001")] == ["notice-1", "reply-1"]
    assert service._chat_sessions["group:20001"]["_pending_owners"] == []


def test_unrelated_self_event_then_ack_keeps_event_order_and_no_owner_flag():
    service = _service()
    token = service.register_send_owner("group:20001", "user-triggered reply", 10001)
    sequence = service.observe_onebot_event(event(post="message_sent", user=90001, mid="notice-1", message="independent notice"))
    service.observe_send_ack(scene="group", target_id=20001, message="independent notice", message_id="notice-1")
    rows = service._load_active_chat_history("group:20001")
    assert len(rows) == 1 and rows[0]["_source"] == "event" and rows[0]["_ingest_seq"] == sequence
    assert not rows[0].get("_owner_unverified") and "_owner_user_id" not in rows[0]
    assert [claim["token"] for claim in service._chat_sessions["group:20001"]["_pending_owners"]] == [token]


def test_zero_matches_does_not_clear_existing_ambiguous_owner():
    service = _service()
    tokens = [service.register_send_owner("group:20001", "same reply", user) for user in (10001, 10002)]
    service.observe_send_ack(scene="group", target_id=20001, message="same reply", message_id="ambiguous")
    assert service.find_chat_message_by_msg_id("group:20001", "ambiguous")["_owner_unverified"]
    for token in tokens:
        service.cancel_send_owner("group:20001", token)
    service.register_send_owner("group:20001", "unrelated reply", 10003)
    service.observe_onebot_event(event(post="message_sent", user=90001, mid="ambiguous", message="same reply"))
    row = service.find_chat_message_by_msg_id("group:20001", "ambiguous")
    assert row["_owner_unverified"] and "_owner_user_id" not in row


@pytest.mark.asyncio
async def test_public_history_and_quotes_keep_independent_notice_but_filter_opted_out_reply(monkeypatch):
    import json
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from cooper_bot.modules.ai.quote_context import resolve_quote_for_aichat
    from test.unit.test_aisvc_kimi_chat import _Runner, _patch_settings_validation

    service = _service()
    service._kimi_runner = _Runner();service._public_runtime_safe = True
    _patch_settings_validation(monkeypatch)
    service.memory = SimpleNamespace(store=SimpleNamespace(member_enabled=AsyncMock(return_value=False)), prompt_context=AsyncMock(return_value=None))
    turn = SimpleNamespace(scope_id="scope", identity=SimpleNamespace(), snapshot=SimpleNamespace())
    service.register_send_owner("group:20001", "opted-out reply", 10001)
    service.observe_send_ack(scene="group", target_id=20001, message="independent notice", message_id="notice-1")
    service.observe_send_ack(scene="group", target_id=20001, message="opted-out reply", message_id="reply-1")
    await service.kimi_chat_with_context("group:20001", "current request", defer_history=True, memory_turn=turn)
    history = json.loads(service._kimi_runner.requests[-1].prompt)["conversation_history"]
    assert [row["message_id"] for row in history] == ["notice-1"]
    api = SimpleNamespace(call=AsyncMock())
    for mid, expected in (("notice-1", True), ("reply-1", False)):
        quoted = await resolve_quote_for_aichat(api=api, aisvc=service, session_key="group:20001", event=event(message=[{"type": "reply", "data": {"id": mid}}]), memory_turn=turn)
        assert quoted["available"] is expected
    api.call.assert_not_awaited()
