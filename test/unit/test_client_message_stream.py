import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import client
from cooper_bot.core.onebot import OneBotAPI
from test.unit.test_onebot_message_context import _service, event


def test_self_messages_are_observed_but_never_routable_and_capture_is_optional(monkeypatch):
    service = _service()
    vision = SimpleNamespace(ready=True, capture_context_images=False, create_slots_from_event=Mock())
    assert client._observe_chat_stream(service, vision, event(post="message_sent", user=90001, mid="sent")) is True
    assert client._observe_chat_stream(service, vision, event(user=90001, mid="self")) is True
    assert client._observe_chat_stream(service, vision, event(mid="user")) is False
    assert [row["role"] for row in service._load_active_chat_history("group:20001")] == ["assistant", "assistant", "user"]
    vision.create_slots_from_event.assert_not_called()
    vision.capture_context_images = True
    vision.create_slots_from_event.return_value = []
    client._observe_chat_stream(service, vision, event(mid="image"))
    vision.create_slots_from_event.assert_called_once()


def test_observer_failure_does_not_make_self_events_dispatchable(monkeypatch):
    monkeypatch.setattr(client, "log", SimpleNamespace(warning=Mock()))
    failed = SimpleNamespace(observe_onebot_event=Mock(side_effect=ValueError("private body")))
    assert client._observe_chat_stream(failed, None, event(post="message_sent")) is True
    assert client._observe_chat_stream(failed, None, event()) is False
    assert "private body" not in str(client.log.warning.call_args_list)


@pytest.mark.asyncio
async def test_ws_observes_before_routing_skips_self_and_rebinds_ack_after_reconnect(monkeypatch):
    service = _service()
    service.load_kimi_capability_cache = AsyncMock(return_value=SimpleNamespace(public_forbidden_tools_blocked=True))
    service.bootstrap_sync = AsyncMock()
    service.bootstrap_post_startup_sync = AsyncMock()
    completed = [asyncio.Event(), asyncio.Event()]
    apis = []
    index = [0]
    posts = [[event(mid="first"), event(post="message_sent", mid="bot1", user=90001), event(mid="self", user=90001)], [event(mid="second")]]

    class WS:
        def __init__(self, number): self.number = number
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return False
        async def __aiter__(self):
            for item in posts[self.number]: yield json.dumps(item)
            await asyncio.wait_for(completed[self.number].wait(), 3)
            if self.number == 0: raise ConnectionError("reconnect")
            raise asyncio.CancelledError

    def connect(*_, **__):
        number = index[0];index[0] += 1
        return WS(number)

    def api_factory(ws, log, **kwargs):
        api = OneBotAPI(ws, log, **kwargs)
        api._call_http = AsyncMock(return_value={"status": "ok", "retcode": 0, "data": {"message_id": f"ack{ws.number}"}})
        apis.append(api)
        return api

    async def dispatch(**kwargs):
        evt = kwargs["evt"]
        assert evt["message_id"] in {"first", "second"}
        assert service.find_chat_message_by_msg_id("group:20001", evt["message_id"])
        await kwargs["api"].send_group_msg(20001, "scheduled reply")
        completed[kwargs["api"].ws.number].set()

    original_build = client.build_ctx
    def build(evt, **kwargs):
        assert evt["message_id"] in {"first", "second"}
        assert service.find_chat_message_by_msg_id("group:20001", evt["message_id"])
        return original_build(evt, **kwargs)

    async def scheduler(*_): await asyncio.Event().wait()
    original_sleep = asyncio.sleep
    async def sleep(seconds): await original_sleep(0 if seconds == 2 else seconds)
    monkeypatch.setattr(client.websockets, "connect", connect)
    monkeypatch.setattr(client, "OneBotAPI", api_factory)
    monkeypatch.setattr(client, "build_ctx", build)
    monkeypatch.setattr(client, "dispatch", AsyncMock(side_effect=dispatch))
    monkeypatch.setattr(client, "log", SimpleNamespace(info=Mock(), warning=Mock(), error=Mock(), exception=Mock()))
    monkeypatch.setattr(client.asyncio, "sleep", sleep)
    monkeypatch.setattr(client, "_POST_SYNC_TASK", None)
    moderation = SimpleNamespace(start=AsyncMock(), close=AsyncMock(), observe_event=AsyncMock())
    speech = SimpleNamespace(group_id=20001, observe_event=AsyncMock(), scheduler_loop=scheduler)
    with pytest.raises(asyncio.CancelledError):
        await client._run_bot_loop(
            aisvc=service, vision_skill=None, filesvc=SimpleNamespace(build_find_index=lambda: {}), state=None,
            perm=SimpleNamespace(get_level=lambda _: 1, touch_group_speaker=Mock()),
            handin=SimpleNamespace(scheduler_loop=scheduler), calendar_service=SimpleNamespace(scheduler_loop=scheduler),
            email_notify_service=SimpleNamespace(scheduler_loop=scheduler), moderation_service=moderation,
            speech_service=speech,
        )
    await client._POST_SYNC_TASK
    assert client.dispatch.await_count == 2
    assert moderation.observe_event.await_count == 2
    assert speech.observe_event.await_count == 4
    assert len(apis) == 2
    assert service.find_chat_message_by_msg_id("group:20001", "ack0")
    assert service.find_chat_message_by_msg_id("group:20001", "ack1")


@pytest.mark.asyncio
@pytest.mark.parametrize("scene", ["group", "private"])
@pytest.mark.parametrize("response", [None, {"status": "failed", "retcode": 1}, {"status": "ok", "retcode": 0, "data": {}}, {"status": "ok", "retcode": 0, "data": {"message_id": 501}}])
async def test_send_ack_is_confirmed_only_and_does_not_retry(scene, response):
    service = _service()
    api = OneBotAPI(None, Mock(), http_base="http://localhost", on_send_confirmed=service.observe_send_ack)
    api._call_http = AsyncMock(return_value=response)
    api._call_ws = AsyncMock()
    target = 20001 if scene == "group" else 10001
    assert await getattr(api, f"send_{scene}_msg")(target, "reply") is response
    api._call_http.assert_awaited_once()
    api._call_ws.assert_not_awaited()
    rows = service._load_active_chat_history(f"{scene}:{target}")
    assert len(rows) == (1 if response and response.get("data", {}).get("message_id") else 0)


@pytest.mark.asyncio
async def test_ack_callback_failure_preserves_response_and_unknown_http_can_be_observed_later():
    response = {"status": "ok", "retcode": 0, "data": {"message_id": 501}}
    api = OneBotAPI(None, Mock(), http_base="http://localhost", on_send_confirmed=Mock(side_effect=RuntimeError("private")))
    api._call_http = AsyncMock(return_value=response)
    assert await api.send_private_msg(10001, "reply") is response
    assert api._call_http.await_count == 1
    service = _service()
    api._on_send_confirmed = service.observe_send_ack
    api._call_http.return_value = None
    await api.send_group_msg(20001, "reply")
    assert service._load_active_chat_history("group:20001") == []
    service.observe_onebot_event(event(post="message_sent", user=90001, mid="sent", message="reply"))
    assert len(service._load_active_chat_history("group:20001")) == 1


@pytest.mark.asyncio
async def test_reply_targets_actual_private_recipient_and_late_event_retains_owner(monkeypatch):
    import cooper_bot.commands.commands as commands
    monkeypatch.setattr(commands, "_claim_recent_reply", lambda _: True)
    service = _service()
    api = OneBotAPI(None, Mock(), http_base="http://localhost", on_send_confirmed=service.observe_send_ack)
    api._call_http = AsyncMock(return_value={"status": "ok", "retcode": 0, "data": {"message_id": 501}})
    ctx = SimpleNamespace(scene="group", group_id=20001, user_id=10001, _ai_chat_context_aisvc=service)
    logsvc = SimpleNamespace(log=Mock(), log_out=Mock())
    assert await commands.reply(api, ctx, "private result", logsvc, force_private_user_id=10001)
    assert service._load_active_chat_history("group:20001") == []
    assert service.find_chat_message_by_msg_id("private:10001", "501")["_owner_user_id"] == 10001
    api._call_http.return_value = {"status": "ok", "retcode": 0, "data": {}}
    assert await commands.reply(api, ctx, "late private result", logsvc, force_private_user_id=10001)
    assert len(service._load_active_chat_history("private:10001")) == 1
    service.observe_onebot_event(event(kind="private", post="message_sent", user=90001, target_id=10001, mid="502", message="late private result"))
    assert service.find_chat_message_by_msg_id("private:10001", "502")["_owner_user_id"] == 10001
    assert service._load_active_chat_history("private:90001") == []
