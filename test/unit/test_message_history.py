import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from cooper_bot.modules.history.historysvc import MessageHistoryService
from cooper_bot.modules.ai.message_context import normalize_onebot_event


def event(mid, stamp=990, *, scene="group", target=20, user=10, bot=90, **kw):
    row = {"post_type": "message", "message_type": scene, "self_id": bot, "user_id": user,
           "sender": {"user_id": user, "nickname": f"user-{user}"}, "time": stamp,
           "message_id": mid, "message": "old /command"}
    if scene == "group":
        row["group_id"] = target
    row.update(kw)
    return row


def api(*pages):
    return SimpleNamespace(call=AsyncMock(side_effect=[{"status": "ok", "retcode": 0,
                           "data": {"messages": rows}} for rows in pages]))


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    monkeypatch.setattr("cooper_bot.modules.history.historysvc.time.time", lambda: 1000)


async def test_shared_pagination_filters_bounds_and_keeps_payload_out_of_counting():
    service = MessageHistoryService()
    client = api([event(3, 999), event(4, 1000)], [event(2, 980), event(1, 940)])
    consumer = AsyncMock()
    result = await service.fetch(client, scene="group", target_id=20, lower=950, upper=1000, on_page=consumer)
    assert result.complete and not result.messages
    assert client.call.await_args_list[1].args[1]["message_seq"] == 3
    assert [e["message_id"] for call in consumer.await_args_list for e in call.args[0]] == [3, 2]


@pytest.mark.parametrize("row", [event(1, group_id=21), event(True), event(1, user_id=True),
                                      event(1, sender={"user_id": 11}), event(1, message_type="private")])
async def test_invalid_history_page_is_not_delivered(row):
    consumer = AsyncMock()
    result = await MessageHistoryService().fetch(api([row]), scene="group", target_id=20,
                                                lower=900, upper=1000, on_page=consumer)
    assert not result.complete
    consumer.assert_not_awaited()


async def test_stalled_cursor_and_inclusive_history_floor():
    service = MessageHistoryService()
    result = await service.fetch(api([event(1), event(2)], [event(1), event(2)]),
                                 scene="group", target_id=20, lower=900, upper=1000)
    assert not result.complete
    result = await service.fetch(api([event(1)], [event(1)]), scene="group", target_id=20,
                                 lower=900, upper=1000)
    assert result.complete and result.floor == 990


async def test_context_limits_and_scope_cache_reconnect():
    service = MessageHistoryService()
    client = api([event(i, 990 + i) for i in range(1, 6)], [event(8, 996)])
    current = event(100, 1000)
    rows = await service.context(client, current, max_messages=3, ttl_seconds=20)
    assert [row["message_id"] for row in rows] == [3, 4, 5]
    await service.context(client, event(101, 1000), max_messages=3, ttl_seconds=20)
    assert client.call.await_count == 1
    service.on_connect()
    rows = await service.context(client, current, max_messages=1, ttl_seconds=20)
    assert rows[0]["message_id"] == 8 and client.call.await_count == 2
    assert client.call.await_args.args[0] == "get_group_msg_history"


async def test_restart_restores_only_current_private_peer_and_normalizes_self_messages():
    current = event(100, 1000, scene="private", target=10)
    client = api([event(1, 980, scene="private"), event(2, 990, scene="private", user=90)], [])
    rows = await MessageHistoryService().context(client, current, max_messages=300, ttl_seconds=1800)
    assert client.call.await_args_list[0].args[0] == "get_friend_msg_history"
    assert client.call.await_args_list[0].args[1]["user_id"] == 10
    normalized = [normalize_onebot_event(row) for row in rows]
    assert [row[0] for row in normalized] == ["private:10", "private:10"]
    assert [row[1]["role"] for row in normalized] == ["user", "assistant"]
    wrong_peer = api([event(3, scene="private", user=11)])
    assert await MessageHistoryService().context(wrong_peer, current, max_messages=300, ttl_seconds=1800) == []


@pytest.mark.parametrize("row", [event(1, scene="private", user=90, target_id=11),
                                      event(1, scene="private", self_id=91)])
async def test_private_scope_mismatch_fails_closed(row):
    result = await MessageHistoryService().fetch(api([row]), scene="private", target_id=10,
                                                lower=900, upper=1000, bot_id=90, limit=10)
    assert not result.complete and not result.messages


async def test_future_same_second_and_expired_history_are_not_used():
    current = event(10, 1000)
    client = api([event(1, 970), event(2, 990), event(3, 1000), event(4, 1001)])
    rows = await MessageHistoryService().context(client, current, max_messages=20, ttl_seconds=20)
    assert [row["message_id"] for row in rows] == [2]


async def test_failure_and_timeout_leave_chat_available():
    client = SimpleNamespace(call=AsyncMock(side_effect=TimeoutError))
    service = MessageHistoryService()
    assert await service.context(client, event(10, 1000), max_messages=20, ttl_seconds=20) == []
    service.on_connect()
    client.call = AsyncMock(return_value={"status": "failed", "retcode": 1})
    assert await service.context(client, event(10, 1000), max_messages=20, ttl_seconds=20) == []


@pytest.mark.parametrize("failure", [TimeoutError, OSError])
async def test_history_failure_backs_off_and_retries_after_expiry(monkeypatch, failure):
    clock = [100.0]
    monkeypatch.setattr("cooper_bot.modules.history.historysvc.time", SimpleNamespace(time=lambda: 1000, monotonic=lambda: clock[0]))
    service = MessageHistoryService()
    client = SimpleNamespace(call=AsyncMock(side_effect=failure))
    for _ in range(3):
        assert await service.context(client, event(10, 1000), max_messages=20, ttl_seconds=20) == []
    assert client.call.await_count == 1
    clock[0] += 16
    client.call = api([event(1, 990)], []).call
    assert (await service.context(client, event(11, 1000), max_messages=20, ttl_seconds=20))[0]["message_id"] == 1


async def test_outer_history_deadline_is_cached_and_reconnect_allows_retry(monkeypatch):
    service = MessageHistoryService()
    await service._load_fences()
    monkeypatch.setattr(service, "_CONTEXT_TIMEOUT_SECONDS", 0.1)
    async def stalled(*args, **kwargs):
        await asyncio.Event().wait()
    client = SimpleNamespace(call=AsyncMock(side_effect=stalled))
    for _ in range(3):
        assert await service.context(client, event(10, 1000), max_messages=20, ttl_seconds=20) == []
    assert client.call.await_count == 1
    service.on_connect()
    client.call = api([event(1, 990)], []).call
    assert (await service.context(client, event(11, 1000), max_messages=20, ttl_seconds=20))[0]["message_id"] == 1


async def test_cancelled_history_request_does_not_create_failure_cache():
    service = MessageHistoryService()
    started = asyncio.Event()
    async def stalled(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()
    client = SimpleNamespace(call=AsyncMock(side_effect=stalled))
    task = asyncio.create_task(service.context(client, event(10, 1000), max_messages=20, ttl_seconds=20))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    client.call = api([event(1, 990)], []).call
    assert (await service.context(client, event(11, 1000), max_messages=20, ttl_seconds=20))[0]["message_id"] == 1


async def test_clear_boundary_survives_restart_and_does_not_store_bodies(tmp_path, monkeypatch):
    path = tmp_path / "boundaries.json"
    identity = SimpleNamespace(bot_id=90, scene="group", group_id=20, actor_user_id=10, profile="public")
    await MessageHistoryService(state_path=path).forget_context(identity)
    assert "old /command" not in path.read_text()
    monkeypatch.setattr("cooper_bot.modules.history.historysvc.time.time", lambda: 1010)
    service = MessageHistoryService(state_path=path)
    client = api([event(1, 990), event(2, 1005)])
    rows = await service.context(client, event(10, 1010), max_messages=20, ttl_seconds=1800, actor_id=10)
    assert [row["message_id"] for row in rows] == [2]
    assert await service.filter_context(identity, [{"_event_time": 990}, {"_event_time": 1005}]) == [{"_event_time": 1005}]


async def test_admin_clear_boundary_does_not_clear_other_admin_context(tmp_path, monkeypatch):
    service = MessageHistoryService(state_path=tmp_path / "boundaries.json")
    identity = SimpleNamespace(bot_id=90, scene="group", group_id=20, actor_user_id=10, profile="admin")
    await service.forget_context(identity)
    monkeypatch.setattr("cooper_bot.modules.history.historysvc.time.time", lambda: 1010)
    assert await service.context(api([]), event(10, 1010), max_messages=2, ttl_seconds=30, actor_id=10) == []
    rows = await service.context(api([event(1, 990)], []), event(10, 1010), max_messages=2, ttl_seconds=30, actor_id=11)
    assert rows[0]["message_id"] == 1


async def test_reconnect_during_retrieval_discards_old_connection_snapshot():
    service = MessageHistoryService()
    started, release = asyncio.Event(), asyncio.Event()
    async def fetch(*args, **kwargs):
        started.set()
        await release.wait()
        return SimpleNamespace(messages=[event(1)], complete=True)
    service.fetch = fetch
    task = asyncio.create_task(service.context(None, event(10, 1000), max_messages=20, ttl_seconds=20))
    await started.wait()
    service.on_connect()
    release.set()
    assert await task == []


async def test_simultaneous_context_requests_share_one_retrieval_and_status_resets_it():
    service = MessageHistoryService()
    client = api([event(1, 990)], [], [event(2, 995)], [])
    requests = [service.context(client, event(10, 1000), max_messages=20, ttl_seconds=20) for _ in range(5)]
    results = await asyncio.gather(*requests)
    assert all(rows[0]["message_id"] == 1 for rows in results)
    assert client.call.await_count == 2
    service.observe_status({"post_type": "meta_event", "meta_event_type": "heartbeat", "status": {"online": False}})
    rows = await service.context(client, event(11, 1000), max_messages=20, ttl_seconds=20)
    assert rows[0]["message_id"] == 2 and client.call.await_count == 4


async def test_ack_owner_metadata_restores_bot_reply_without_persisting_content(tmp_path):
    path = tmp_path / "metadata.json"
    service = MessageHistoryService(state_path=path)
    identity = SimpleNamespace(bot_id=90, scene="group", group_id=20, actor_user_id=10, profile="admin")
    await service.record_delivery(identity, "reply-1")
    restored = MessageHistoryService(state_path=path)
    await restored._load_fences()
    assert restored.message_owner(event("reply-1", user=90)) == 10
    assert restored.message_owner(event("reply-1", user=90, target=21)) is None
    assert "old /command" not in path.read_text()
