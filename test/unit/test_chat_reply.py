import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from cooper_bot.modules.ai.chat_reply import recent_context, render_reply


def test_plain_text_stays_compatible_and_verified_mention_is_rendered_once():
    assert render_reply("普通回复", group=True, speakers={10}) == "普通回复"
    text = json.dumps({"qq_reply": {"text": "不用谢 [CQ:at,qq=all]", "at": [10, 10]}}, ensure_ascii=False)
    reply = render_reply(text, group=True, speakers={10})
    assert reply == "[CQ:at,qq=10] 不用谢 &#91;CQ:at,qq=all&#93;"


@pytest.mark.parametrize("group", [True, False])
def test_missing_at_defaults_to_no_mentions_and_still_escapes_cq(group):
    text = json.dumps({"qq_reply": {"text": "hello [CQ:at,qq=all]"}})
    assert render_reply(text, group=group, speakers=set()) == "hello &#91;CQ:at,qq=all&#93;"


@pytest.mark.parametrize("payload", [
    {"qq_reply": {"text": "hello", "at": None}},
    {"qq_reply": {"text": "hello", "at": "10"}},
    {"qq_reply": {"text": "hello", "extra": 1}},
    {"qq_reply": {"at": []}},
    {"qq_reply": {"text": "hello"}, "extra": 1},
])
def test_missing_at_compatibility_does_not_accept_other_malformed_envelopes(payload):
    with pytest.raises(ValueError):
        render_reply(json.dumps(payload), group=True, speakers={10})


@pytest.mark.parametrize("mentions,group", [([11], True), (["all"], True), ([True], True), ([10], False)])
def test_unverified_mention_and_private_mentions_are_rejected(mentions, group):
    with pytest.raises(ValueError):
        render_reply(json.dumps({"qq_reply": {"text": "hello", "at": mentions}}), group=group, speakers={10})


@pytest.mark.parametrize("body", [
    "@123456789 hello @123456789",
    "＠123456789 hello @123456789",
    "[CQ:at,qq=123456789] hello [CQ:at,qq=123456789]",
    "@123456789 hello [CQ:at,qq=123456789]",
])
@pytest.mark.parametrize("structured", [False, True])
def test_inline_mentions_are_verified_and_rendered_only_once(body, structured):
    text = json.dumps({"qq_reply": {"text": body, "at": [123456789, 123456789]}}) if structured else body
    assert render_reply(text, group=True, speakers={123456789}) == "[CQ:at,qq=123456789] hello"


def test_inline_mentions_keep_distinct_members_and_target_order():
    text = json.dumps({"qq_reply": {"text": "@987654321 hello @123456789", "at": [123456789]}})
    assert render_reply(text, group=True, speakers={123456789, 987654321}) == "[CQ:at,qq=123456789] [CQ:at,qq=987654321] hello"


@pytest.mark.parametrize("group,speakers", [(True, set()), (False, {123456789})])
@pytest.mark.parametrize("structured", [False, True])
def test_numeric_mentions_without_allowed_group_target_do_not_show_raw_qq(group, speakers, structured):
    text = "@123456789 hello"
    if structured:
        text = json.dumps({"qq_reply": {"text": text}})
    assert render_reply(text, group=group, speakers=speakers) == "该用户 hello"


def test_plain_cq_cannot_bypass_target_checks_or_mention_everyone():
    for body in ("[CQ:at,qq=all] hello", "[CQ:at,qq=987654321] hello"):
        out = render_reply(body, group=True, speakers={123456789})
        assert out == body.replace("[", "&#91;").replace("]", "&#93;")
    assert render_reply("[CQ:at,qq=123456789] hello", group=False, speakers={123456789}) == "&#91;CQ:at,qq=123456789&#93; hello"


@pytest.mark.parametrize("text", [
    "普通回复 @昵称，QQ号 123456789",
    "邮箱 user@123456789.com 和 user+tag@example.com",
    "链接 https://example.com/@123456789 和 https://example.com/?q=@123456789",
    "示例 `@123456789` 和 `[CQ:at,qq=123456789]`",
])
def test_normalizing_mentions_preserves_regular_text_email_urls_and_code(text):
    out = render_reply(text, group=True, speakers={123456789})
    assert out == text.replace("[CQ:at,qq=123456789]", "&#91;CQ:at,qq=123456789&#93;")


def test_inline_mentions_respect_existing_ten_target_limit():
    ids = list(range(123456780, 123456791))
    text = " ".join(f"@{uid}" for uid in ids) + " hello"
    out = render_reply(text, group=True, speakers=set(ids))
    assert out.count("[CQ:at,") == 10
    assert "@123456790" not in out
    assert "该用户" in out


async def test_recent_context_combines_recovery_and_live_without_replaying_commands():
    old = {"post_type": "message", "message_type": "group", "self_id": 90, "group_id": 20,
           "user_id": 10, "sender": {"user_id": 10, "nickname": "学渣"}, "time": 990,
           "message_id": "old", "message": "/dangerous old command"}
    history = SimpleNamespace(context=AsyncMock(return_value=[old]),
                              filter_context=AsyncMock(side_effect=lambda identity, rows: rows))
    live = {"role": "user", "content": "latest", "_msg_id": "live", "_speaker": {"user_id": 11}, "_ingest_seq": 2, "_event_time": 995}
    svc = SimpleNamespace(message_history=history, _CHAT_CONTEXT_MAX_MESSAGES=300, _CHAT_CONTEXT_TTL_SECONDS=1800,
                          _load_active_chat_history=lambda key: [live], memory=None)
    current = dict(old, message_id="current")
    rows = await recent_context(None, current, svc, None, "group:20", 3)
    assert [row["_msg_id"] for row in rows] == ["old", "live"]
    assert rows[0]["content"] == "/dangerous old command"
    assert not hasattr(history, "dispatch")


async def test_reconnect_keeps_newest_messages_across_recovery_and_live_cache():
    live = [{"role": "user", "content": "live", "_msg_id": str(i), "_speaker": {"user_id": 10},
             "_event_time": 100 + i, "_ingest_seq": i} for i in range(1, 300)]
    recovered = [{"post_type": "message", "message_type": "group", "self_id": 90, "group_id": 20,
                  "user_id": 10, "sender": {"user_id": 10}, "time": 100 + i,
                  "message_id": str(i), "message": "recovered"} for i in range(200, 500)]
    history = SimpleNamespace(context=AsyncMock(return_value=recovered),
                              filter_context=AsyncMock(side_effect=lambda identity, rows: rows))
    svc = SimpleNamespace(message_history=history, _CHAT_CONTEXT_MAX_MESSAGES=300, _CHAT_CONTEXT_TTL_SECONDS=1800,
                          _load_active_chat_history=lambda key: live, memory=None)
    rows = await recent_context(None, {"message_id": "500"}, svc, None, "group:20", 500)
    assert [row["_msg_id"] for row in rows] == [str(i) for i in range(200, 500)]
    assert rows[0]["content"] == "live"
    assert sum(row["content"] == "recovered" for row in rows) == 200


async def test_member_opt_out_filters_recovered_and_live_context():
    from cooper_bot.modules.memory.models import MemoryIdentity
    identity = MemoryIdentity(90, 10, "group", 20, "admin", True)
    member_enabled = AsyncMock(return_value=False)
    memory = SimpleNamespace(store=SimpleNamespace(member_enabled=member_enabled), _started=True,
                             _identity_scope=lambda identity: "public-group-scope")
    rows = [{"role": "user", "content": "private", "_msg_id": str(i), "_speaker": {"user_id": 11}}
            for i in range(10)]
    svc = SimpleNamespace(_load_active_chat_history=lambda key: rows, memory=memory, _CHAT_CONTEXT_MAX_MESSAGES=300)
    assert await recent_context(None, {"message_id": "current"}, svc, identity, "group:20", None) == []
    member_enabled.assert_awaited_once_with("public-group-scope", 11)


async def test_corrupt_recovery_metadata_degrades_without_fetching_or_breaking_chat(tmp_path):
    from cooper_bot.modules.history.historysvc import MessageHistoryService
    from cooper_bot.modules.memory.models import MemoryIdentity
    path = tmp_path / "bad-state.json"
    path.write_text("not valid json", encoding="utf-8")
    service = SimpleNamespace(message_history=MessageHistoryService(state_path=path),
                              _CHAT_CONTEXT_MAX_MESSAGES=300, _CHAT_CONTEXT_TTL_SECONDS=1800,
                              _load_active_chat_history=lambda key: [], memory=None, log=Mock())
    api = SimpleNamespace(call=AsyncMock())
    event = {"message_type": "group", "self_id": 90, "group_id": 20, "user_id": 10, "message_id": "current"}
    identity = MemoryIdentity(90, 10, "group", 20, "admin", True)
    assert await recent_context(api, event, service, identity, "group:20", 1) == []
    api.call.assert_not_awaited()
