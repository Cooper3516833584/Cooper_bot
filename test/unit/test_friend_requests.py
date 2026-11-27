import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import client
from cooper_bot.core.onebot import OneBotAPI


def _event(**changes):
    return {"post_type": "message", "message_type": "private", "sub_type": "friend", **changes}


def _friend_notice(**changes):
    return {"post_type": "notice", "notice_type": "friend_add", "user_id": 12345, **changes}


@pytest.mark.parametrize("confirmed", [True, False])
async def test_friend_completion_is_silent_for_both_results(monkeypatch, confirmed):
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REQUEST", True)
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REMARK", "test")
    api = SimpleNamespace(
        complete_friend_request=AsyncMock(return_value={"status": "ok", "data": {"is_friend": confirmed}}),
        send_private_msg=AsyncMock(return_value={"status": "ok"}),
    )
    task = client._schedule_friend_completion(api, SimpleNamespace(user_id=12345), _event(), "请求添加你为好友", set())
    assert task is not None
    await task
    api.complete_friend_request.assert_awaited_once_with(12345, "test", flag="")
    api.send_private_msg.assert_not_awaited()


@pytest.mark.parametrize("response", [None, {"status": "failed", "data": {"is_friend": True}}])
async def test_failed_or_missing_response_does_not_send_reply(monkeypatch, response):
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REQUEST", True)
    api = SimpleNamespace(complete_friend_request=AsyncMock(return_value=response), send_private_msg=AsyncMock())
    await client._schedule_friend_completion(api, SimpleNamespace(user_id=12345), _event(), "请求添加你为好友", set())
    api.send_private_msg.assert_not_awaited()


async def test_disabled_auto_approval_does_not_call_friend_action(monkeypatch):
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REQUEST", False)
    api = SimpleNamespace(complete_friend_request=AsyncMock(), send_private_msg=AsyncMock())
    assert client._schedule_friend_completion(api, None, _friend_notice(), "", set()) is None
    api.complete_friend_request.assert_not_awaited()
    api.send_private_msg.assert_not_awaited()


async def test_friend_action_exception_is_handled_silently(monkeypatch):
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REQUEST", True)
    api = SimpleNamespace(complete_friend_request=AsyncMock(side_effect=RuntimeError()), send_private_msg=AsyncMock())
    await client._schedule_friend_completion(api, SimpleNamespace(user_id=12345), _event(), "请求添加你为好友", set())
    api.send_private_msg.assert_not_awaited()


@pytest.mark.parametrize("event,text", [
    (_event(message_type="group"), "请求添加你为好友"),
    (_event(sub_type="group"), "请求添加你为好友"),
    (_event(post_type="message_sent"), "请求添加你为好友"),
    (_event(), "有人请求添加你为好友"),
    (_event(), "/find 热统"),
    (_friend_notice(notice_type="group_upload"), ""),
    (_friend_notice(user_id="bad"), ""),
    (_friend_notice(self_id="12345"), ""),
])
async def test_other_messages_keep_existing_routing(event, text):
    api = Mock()
    assert client._schedule_friend_completion(api, SimpleNamespace(user_id=12345), event, text, set()) is None
    assert not api.mock_calls


async def test_request_events_use_the_flag_or_match_by_user_when_missing(monkeypatch):
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REQUEST", True)
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REMARK", "test")
    api = SimpleNamespace(
        set_friend_add_request=AsyncMock(),
        complete_friend_request=AsyncMock(return_value={"status": "ok", "data": {"is_friend": True}}),
    )
    event = {"post_type": "request", "request_type": "friend", "user_id": "12345", "comment": "private"}
    pending = set()
    await client._schedule_friend_completion(api, None, {**event, "flag": "345"}, "", pending)
    api.set_friend_add_request.assert_not_awaited()
    api.complete_friend_request.assert_awaited_once_with(12345, "test", flag="345")
    api.complete_friend_request.reset_mock()
    await client._schedule_friend_completion(api, None, event, "", pending)
    api.complete_friend_request.assert_awaited_once_with(12345, "test", flag="")


@pytest.mark.parametrize("response, fallback", [
    ({"status": "failed", "message": "不支持的Api cooper_complete_friend_request"}, True),
    (None, False),
    ({"status": "ok", "data": {"is_friend": False, "reciprocal": "sent"}}, False),
])
async def test_standard_approval_only_when_plugin_action_is_unavailable(monkeypatch, response, fallback):
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REQUEST", True)
    api = SimpleNamespace(
        set_friend_add_request=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        complete_friend_request=AsyncMock(return_value=response),
    )
    event = {"post_type": "request", "request_type": "friend", "user_id": 12345, "flag": "345"}
    await client._schedule_friend_completion(api, None, event, "", set())
    assert api.set_friend_add_request.await_count == int(fallback)


async def test_scheduling_does_not_wait_for_friend_action(monkeypatch):
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REQUEST", True)
    started = asyncio.Event()
    release = asyncio.Event()

    async def complete(*_args, **_kwargs):
        started.set()
        await release.wait()
        return {"status": "ok", "data": {"is_friend": True}}

    api = SimpleNamespace(complete_friend_request=complete, send_private_msg=AsyncMock())
    pending = set()
    task = client._schedule_friend_completion(api, None, _friend_notice(), "", pending)
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert not task.done()
        assert client._schedule_friend_completion(api, None, _friend_notice(), "", pending) is None
        request = {"post_type": "request", "request_type": "friend", "user_id": 12345, "flag": "345"}
        assert client._schedule_friend_completion(api, None, request, "", pending) is None
        api.send_private_msg.assert_not_awaited()
    finally:
        release.set()
        await task
    assert not pending


@pytest.mark.parametrize("notice_type", [None, "friend_add", "cooper_friend_request"])
async def test_ws_friend_message_still_dispatches_while_completion_is_pending(monkeypatch, notice_type):
    monkeypatch.setattr(client, "AUTO_APPROVE_FRIEND_REQUEST", True)
    started, release, dispatched = asyncio.Event(), asyncio.Event(), asyncio.Event()
    event = _event(user_id=12345, raw_message="请求添加你为好友", sender={"user_id": 12345})

    async def complete(*_args, **_kwargs):
        started.set()
        await release.wait()
        return {"status": "ok", "data": {"is_friend": True}}

    async def dispatch(**kwargs):
        await asyncio.wait_for(started.wait(), timeout=1)
        assert kwargs["evt"] == event
        assert kwargs["text"] == "请求添加你为好友"
        assert not release.is_set()
        release.set()
        dispatched.set()

    class WS:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def __aiter__(self):
            if notice_type:
                yield json.dumps(_friend_notice(notice_type=notice_type, flag="345"))
                await asyncio.wait_for(started.wait(), timeout=1)
            yield json.dumps(event)
            await asyncio.wait_for(dispatched.wait(), timeout=2)
            raise asyncio.CancelledError

    async def scheduler(*_args):
        await asyncio.Event().wait()

    api = SimpleNamespace(complete_friend_request=AsyncMock(side_effect=complete), send_private_msg=AsyncMock())
    ai = SimpleNamespace(
        load_kimi_capability_cache=AsyncMock(return_value=SimpleNamespace(public_forbidden_tools_blocked=True)),
        bootstrap_sync=AsyncMock(), bootstrap_post_startup_sync=AsyncMock(),
    )
    monkeypatch.setattr(client.websockets, "connect", lambda *_args, **_kwargs: WS())
    monkeypatch.setattr(client, "OneBotAPI", lambda *_args, **_kwargs: api)
    routed = AsyncMock(side_effect=dispatch)
    monkeypatch.setattr(client, "dispatch", routed)
    monkeypatch.setattr(client, "_POST_SYNC_TASK", None)
    with pytest.raises(asyncio.CancelledError):
        await client._run_bot_loop(
            aisvc=ai, vision_skill=None,
            filesvc=SimpleNamespace(build_find_index=lambda: {}), state=None,
            perm=SimpleNamespace(get_level=lambda _uid: 1),
            handin=SimpleNamespace(scheduler_loop=scheduler),
            calendar_service=SimpleNamespace(scheduler_loop=scheduler),
            email_notify_service=SimpleNamespace(scheduler_loop=scheduler),
        )
    await client._POST_SYNC_TASK
    routed.assert_awaited_once()
    api.complete_friend_request.assert_awaited_once_with(
        12345, client.AUTO_APPROVE_FRIEND_REMARK, flag="345" if notice_type == "cooper_friend_request" else "",
    )
    api.send_private_msg.assert_not_awaited()


async def test_friend_completion_does_not_retry_over_ws_after_http_timeout(monkeypatch):
    api = OneBotAPI(None, Mock(), http_base="http://localhost:3010")
    http = AsyncMock(return_value=None)
    ws = AsyncMock()
    monkeypatch.setattr(api, "_call_http", http)
    monkeypatch.setattr(api, "_call_ws", ws)
    assert await api.complete_friend_request(12345, "test", flag="345") is None
    http.assert_awaited_once_with("cooper_complete_friend_request", {"user_id": 12345, "remark": "test", "flag": "345"}, timeout=16.0)
    ws.assert_not_awaited()
