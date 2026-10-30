from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from cooper_bot.modules.calendar.group_speech import GROUP_ID, TZ, WEEK, GroupSpeechService, report_end


def ok(data):
    return {"status": "ok", "retcode": 0, "data": data}


def event(mid, stamp, uid=10, **extra):
    return {"post_type": "message", "message_type": "group", "group_id": GROUP_ID,
            "user_id": uid, "time": int(stamp), "message_id": mid, **extra}


@pytest.fixture
def service(tmp_path):
    return GroupSpeechService(SimpleNamespace(warning=Mock()), db_path=tmp_path / "speech.sqlite3", admins=[900])


def api(pages, members=None):
    return SimpleNamespace(call=AsyncMock(side_effect=[ok({"messages": p}) for p in pages]),
                           get_group_member_list=AsyncMock(return_value=ok(members or [{"user_id": 10, "card": "甲"}, {"user_id": 20, "nickname": "乙"}])),
                           send_private_msg=AsyncMock(return_value=ok({"message_id": 999})))


def test_week_boundary():
    sunday = datetime(2026, 11, 1, 22, tzinfo=TZ)
    assert report_end(sunday - timedelta(days=5)) == int(sunday.timestamp())
    assert report_end(sunday + timedelta(minutes=5)) == int(sunday.timestamp())
    assert report_end(sunday + timedelta(days=1)) == int(sunday.timestamp()) + WEEK


async def test_content_types_dedup_and_scope(service):
    for i, kind in enumerate(["text", "image", "face", "file", "json", "forward", "reply"]):
        evt = event(i + 1, 100, message=[{"type": kind}])
        await service.observe_event(evt)
        await service.observe_event(dict(evt, post_type="message_sent"))
    await service.observe_event(event(100, 100, group_id=123))
    await service.observe_event(event(101, 100, message_type="private"))
    await service.observe_event(event(102, 100, post_type="notice"))
    assert await service._db(lambda c: c.execute("SELECT COUNT(*) FROM messages").fetchone()[0]) == 7


async def test_paginated_backfill_restarts_without_double_count(tmp_path, service):
    now = datetime(2026, 10, 30, 12, tzinfo=TZ)
    end = report_end(now)
    first = event(1, end - WEEK + 100)
    second = event(2, end - WEEK + 200)
    old = event(3, end - WEEK - 400)
    mock = api([[second], [first, old]])
    await service.observe_event(second)
    await service.check_once(mock, now)
    assert mock.call.await_args_list[1].args[1]["message_seq"] == 2
    restarted = GroupSpeechService(service.log, db_path=service.db_path, admins=[900])
    await restarted.check_once(api([[first, second, old]]), now + timedelta(hours=8), force_sync=True)
    assert await service._db(lambda c: c.execute("SELECT COUNT(*) FROM messages").fetchone()[0]) == 2
    mock.send_private_msg.assert_not_awaited()


async def test_report_zero_members_threshold_and_end_exclusion(service):
    now = datetime(2026, 11, 1, 22, tzinfo=TZ)
    end = int(now.timestamp())
    rows = [event(i + 1, end - 100, 10) for i in range(50)]
    rows += [event(80, end, 20), event(81, end - WEEK - 400, 20)]
    mock = api([rows])
    await service.check_once(mock, now)
    text = mock.send_private_msg.await_args.args[1]
    assert "乙（20）：0 条" in text
    assert "甲（10）" not in text
    assert "本周消息共 50 条" in text
    assert "不足50条 1 人" in text
    await service.check_once(api([rows]), now + timedelta(minutes=1))
    assert mock.send_private_msg.await_count == 1


async def test_failed_history_never_sends_or_advances_coverage(service):
    now = datetime(2026, 11, 1, 22, tzinfo=TZ)
    mock = api([])
    mock.call = AsyncMock(return_value={"status": "failed", "retcode": 200})
    await service.check_once(mock, now)
    mock.send_private_msg.assert_not_awaited()
    assert await service._db(lambda c: c.execute("SELECT value FROM meta WHERE key='coverage'").fetchone()) is None


async def test_stalled_cursor_keeps_report_pending(service):
    now = datetime(2026, 11, 1, 22, tzinfo=TZ)
    evt = event(1, int(now.timestamp()) - 10)
    second = event(2, int(now.timestamp()) - 20)
    mock = api([[evt, second], [evt, second]])
    await service.check_once(mock, now)
    mock.send_private_msg.assert_not_awaited()


async def test_napcat_inclusive_anchor_exhaustion_marks_first_week(service):
    now = datetime(2026, 11, 1, 22, tzinfo=TZ)
    evt = event(1, int(now.timestamp()) - 100)
    mock = api([[evt], [evt]])
    await service.check_once(mock, now)
    assert "本期此前数据不可补回" in mock.send_private_msg.await_args.args[1]


async def test_missed_deadline_and_partial_delivery_resume(service):
    now = datetime(2026, 10, 30, 12, tzinfo=TZ)
    end = report_end(now)
    old = event(1, end - WEEK - 400)
    await service.check_once(api([[old]]), now)
    service.admins = [900, 901]
    mock = api([[old]])
    mock.send_private_msg.side_effect = [ok({"message_id": 1}), None]
    late = datetime(2026, 11, 2, 6, tzinfo=TZ)
    await service.check_once(mock, late, force_sync=True)
    restarted = GroupSpeechService(service.log, db_path=service.db_path, admins=[900, 901])
    retry = api([[old]])
    await restarted.check_once(retry, late, force_sync=True)
    assert retry.send_private_msg.await_count == 1
    assert retry.send_private_msg.await_args.args[0] == 901
    assert await service._db(lambda c: c.execute("SELECT value FROM meta WHERE key='next_end'").fetchone()[0]) == end + WEEK


async def test_long_roster_split_and_cq_escape(service):
    end = report_end(datetime(2026, 11, 1, 22, tzinfo=TZ))
    members = [{"user_id": uid, "card": "[CQ:at,qq=all]" + "甲" * 80} for uid in range(1, 150)]
    mock = api([[]], members=members)
    await service.check_once(mock, datetime.fromtimestamp(end, TZ))
    assert mock.send_private_msg.await_count > 1
    texts = [call.args[1] for call in mock.send_private_msg.await_args_list]
    assert all(len(text) < 2450 and "[CQ:" not in text for text in texts)
    assert sum(text.count("：0 条") for text in texts) == 149
