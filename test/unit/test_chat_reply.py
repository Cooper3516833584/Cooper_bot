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


@pytest.mark.parametrize("mentions,group", [([11], True), (["all"], True), ([True], True), ([10], False)])
def test_unverified_mention_and_private_mentions_are_rejected(mentions, group):
    with pytest.raises(ValueError):
        render_reply(json.dumps({"qq_reply": {"text": "hello", "at": mentions}}), group=group, speakers={10})


async def test_recent_context_combines_recovery_and_live_without_replaying_commands():
    old = {"post_type": "message", "message_type": "group", "self_id": 90, "group_id": 20,
           "user_id": 10, "sender": {"user_id": 10, "nickname": "学渣"}, "time": 990,
           "message_id": "old", "message": "/dangerous old command"}
    history = SimpleNamespace(context=AsyncMock(return_value=[old]),
                              filter_context=AsyncMock(side_effect=lambda identity, rows: rows))
    live = {"role": "user", "content": "latest", "_msg_id": "live", "_speaker": {"user_id": 11}, "_ingest_seq": 2}
    svc = SimpleNamespace(message_history=history, _CHAT_CONTEXT_MAX_MESSAGES=300, _CHAT_CONTEXT_TTL_SECONDS=1800,
                          _load_active_chat_history=lambda key: [live], memory=None)
    current = dict(old, message_id="current")
    rows = await recent_context(None, current, svc, None, "group:20", 3)
    assert [row["_msg_id"] for row in rows] == ["old", "live"]
    assert rows[0]["content"] == "/dangerous old command"
    assert not hasattr(history, "dispatch")


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
