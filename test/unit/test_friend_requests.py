from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import client
from cooper_bot.core.onebot import OneBotAPI


def _event(**changes):
    return {"post_type": "message", "message_type": "private", "sub_type": "friend", **changes}


@pytest.mark.parametrize("confirmed", [True, False])
async def test_request_message_reports_only_confirmed_friendship(monkeypatch, confirmed):
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REQUEST", True)
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REMARK", "test")
    api = SimpleNamespace(
        complete_friend_request=AsyncMock(return_value={"status": "ok", "data": {"is_friend": confirmed}}),
        send_private_msg=AsyncMock(return_value={"status": "ok"}),
    )
    logs = Mock()
    assert await client._handle_friend_request_message(
        api, SimpleNamespace(user_id=12345), _event(), "请求添加你为好友", logs
    )
    api.complete_friend_request.assert_awaited_once_with(12345, "test")
    reply = api.send_private_msg.await_args.args[1]
    assert ("已添加你为好友" in reply) is confirmed
    logs.log_in.assert_called_once()
    logs.log_out.assert_called_once()


@pytest.mark.parametrize("response", [None, {"status": "failed", "data": {"is_friend": True}}])
async def test_failed_or_missing_response_does_not_report_success(monkeypatch, response):
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REQUEST", True)
    api = SimpleNamespace(complete_friend_request=AsyncMock(return_value=response), send_private_msg=AsyncMock())
    assert await client._handle_friend_request_message(api, SimpleNamespace(user_id=12345), _event(), "请求添加你为好友", Mock())
    assert "已添加你为好友" not in api.send_private_msg.await_args.args[1]


async def test_disabled_auto_approval_does_not_call_friend_action(monkeypatch):
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REQUEST", False)
    api = SimpleNamespace(complete_friend_request=AsyncMock(), send_private_msg=AsyncMock())
    assert await client._handle_friend_request_message(api, SimpleNamespace(user_id=12345), _event(), "请求添加你为好友", Mock())
    api.complete_friend_request.assert_not_awaited()
    assert "自动通过已关闭" in api.send_private_msg.await_args.args[1]


async def test_friend_action_exception_is_handled_without_ai_fallback(monkeypatch):
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REQUEST", True)
    api = SimpleNamespace(complete_friend_request=AsyncMock(side_effect=RuntimeError()), send_private_msg=AsyncMock())
    assert await client._handle_friend_request_message(api, SimpleNamespace(user_id=12345), _event(), "请求添加你为好友", Mock())
    assert "自动添加未成功" in api.send_private_msg.await_args.args[1]


@pytest.mark.parametrize("event,text", [
    (_event(message_type="group"), "请求添加你为好友"),
    (_event(sub_type="group"), "请求添加你为好友"),
    (_event(post_type="message_sent"), "请求添加你为好友"),
    (_event(), "有人请求添加你为好友"),
    (_event(), "/find 热统"),
])
async def test_other_messages_keep_existing_routing(event, text):
    api = Mock()
    logs = Mock()
    assert not await client._handle_friend_request_message(api, SimpleNamespace(user_id=12345), event, text, logs)
    assert not api.mock_calls
    assert not logs.mock_calls


async def test_friend_completion_does_not_retry_over_ws_after_http_timeout(monkeypatch):
    api = OneBotAPI(None, Mock(), http_base="http://localhost:3010")
    http = AsyncMock(return_value=None)
    ws = AsyncMock()
    monkeypatch.setattr(api, "_call_http", http)
    monkeypatch.setattr(api, "_call_ws", ws)
    assert await api.complete_friend_request(12345, "test") is None
    http.assert_awaited_once_with("cooper_complete_friend_request", {"user_id": 12345, "remark": "test"}, timeout=10.0)
    ws.assert_not_awaited()
