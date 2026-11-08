import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from cooper_bot.core import config
from cooper_bot.modules.calendar.topic_reminder import TopicReminderService, TZ, render_reports


def stamp(text):
    return int(datetime.fromisoformat(text).replace(tzinfo=TZ).timestamp() * 1000)


def row(mid="r1", *, owner="甲", participants=(), project="2026-11-01", close=None,
        status="进行中", creator="部长甲", created="2026-11-06"):
    return {"record_id": mid, "fields": {
        "负责人": [{"name": owner}], "参与人": [{"name": n} for n in participants],
        "立项日期": stamp(project) if project else None,
        "结项日期": stamp(close) if close else None, "完成状态": status},
        "created_by": {"name": creator}, "created_time": stamp(created)}


def test_monthly_ownership_is_union_and_completion_can_be_from_old_project():
    records = [row(participants=("乙", "甲")),
               row("r2", owner="丙", project="2026-10-01", close="2026-11-03", status="已完成")]
    monthly, _ = render_reports(records, datetime(2026, 11, 15, 22, 50, tzinfo=TZ),
                                ["甲", "乙", "丙", "丁"], ["部长甲"])
    assert "当月尚未立项成员：丙、丁" in monthly
    assert "本月尚未完成选题成员：甲、乙、丁" in monthly
    assert "[CQ:" not in monthly


@pytest.mark.parametrize("day,visible", [(1, False), (14, False), (15, True), (30, True)])
def test_completion_list_starts_on_fifteenth(day, visible):
    monthly, _ = render_reports([], datetime(2026, 11, day, 22, 50, tzinfo=TZ), ["甲"], ["部长甲"])
    assert ("本月尚未完成" in monthly) is visible


def test_only_completed_in_current_month_counts_and_missing_date_is_visible():
    records = [row(owner="甲", close="2026-10-31", status="已完成"),
               row("r2", owner="乙", close="2026-11-02", status="进行中"),
               row("r3", owner="丙", close="2026-11-16", status="已完成"),
               row("r4", owner="丁", status="已完成")]
    monthly, _ = render_reports(records, datetime(2026, 11, 15, 22, 50, tzinfo=TZ),
                                ["甲", "乙", "丙", "丁"], ["部长甲"])
    assert "本月尚未完成选题成员：甲、乙、丙、丁" in monthly
    assert "缺少结项日期" in monthly


def test_month_reset_and_empty_lists():
    records = [row(project="2026-11-30", close="2026-12-01", status="已完成")]
    monthly, _ = render_reports(records, datetime(2026, 12, 15, 22, 50, tzinfo=TZ), ["甲"], ["部长甲"])
    assert "当月尚未立项成员：甲" in monthly
    assert "本月尚未完成选题成员：无" in monthly
    monthly, _ = render_reports([row(close="2026-11-02", status="已完成")],
                                datetime(2026, 11, 15, 22, 50, tzinfo=TZ), ["甲"], ["部长甲"])
    assert "当月尚未立项成员：无" in monthly
    assert "本月尚未完成选题成员：无" in monthly


def test_ministers_use_actual_creator_and_three_calendar_days_in_shanghai():
    now = datetime(2026, 11, 8, 14, 50, tzinfo=timezone.utc)
    records = [row(created="2026-11-05T23:59:59"), row("r2", created="2026-11-06T00:00:00"),
               row("r3", created="2026-11-08T22:50:00", project="2026-10-01", owner="别人"),
               row("r4", created="2026-11-08T22:50:01"),
               row("r5", owner="部长乙", creator="别人", created="2026-11-07")]
    _, minister = render_reports(records, now, ["甲"], ["部长甲", "部长乙"])
    assert "部长甲：2篇\n部长乙：0篇" in minister
    assert "2026年11月06日—2026年11月08日 22:50" in minister


@pytest.fixture
def reminder(tmp_path, monkeypatch):
    cfg = {"enabled": True, "admin_only": True, "group_id": 20001,
           "app_token": "testapp", "table_id": "testtable", "members": ["甲"], "ministers": ["部长甲"]}
    path = tmp_path / "config.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    service = TopicReminderService(Mock(), config_path=path, state_path=tmp_path / "state.json")
    service.fetch_records = AsyncMock(return_value=[row()])
    monkeypatch.setattr(config, "ADMIN_USERS", {900001})
    return service, cfg


def api_with(response=None):
    if response is None:
        response = {"status": "ok", "retcode": 0, "data": {"message_id": 1}}
    return SimpleNamespace(send_private_msg=AsyncMock(return_value=response), send_group_msg=AsyncMock())


async def test_admin_only_two_messages_and_dedup_survives_restart(reminder):
    service, cfg = reminder
    api = api_with()
    now = datetime(2026, 11, 8, 22, 49, tzinfo=TZ)
    await service.check_once(api, now)
    service.fetch_records.assert_not_awaited()
    now += timedelta(minutes=1)
    await service.check_once(api, now)
    assert api.send_private_msg.await_count == 2
    assert all(call.args[0] == 900001 for call in api.send_private_msg.await_args_list)
    api.send_group_msg.assert_not_called()
    restarted = TopicReminderService(Mock(), config_path=service.config_path, state_path=service.state_path)
    restarted.fetch_records = AsyncMock(side_effect=AssertionError("already prepared"))
    await restarted.check_once(api, now)
    assert api.send_private_msg.await_count == 2


async def test_disabled_and_missing_admin_never_fall_back_to_group(reminder, monkeypatch):
    service, cfg = reminder
    api = api_with()
    now = datetime(2026, 11, 8, 22, 50, tzinfo=TZ)
    cfg["enabled"] = False
    service.config_path.write_text(json.dumps(cfg), encoding="utf-8")
    await service.check_once(api, now)
    cfg["enabled"] = True
    service.config_path.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(config, "ADMIN_USERS", set())
    with pytest.raises(ValueError):
        await service.check_once(api, now)
    api.send_private_msg.assert_not_called()
    api.send_group_msg.assert_not_called()
    service.fetch_records.assert_not_awaited()


async def test_group_delivery_requires_explicit_configuration(reminder):
    service, cfg = reminder
    cfg["admin_only"] = False
    service.config_path.write_text(json.dumps(cfg), encoding="utf-8")
    api = api_with()
    api.send_group_msg.return_value = {"status": "ok", "retcode": 0, "data": {"message_id": 2}}
    await service.check_once(api, datetime(2026, 11, 8, 22, 50, tzinfo=TZ))
    assert api.send_group_msg.await_count == 2
    assert all(c.args[0] == 20001 for c in api.send_group_msg.await_args_list)
    api.send_private_msg.assert_not_called()


@pytest.mark.parametrize("result", [None, {"status": "ok", "retcode": 0, "data": {}}])
async def test_uncertain_send_is_never_automatically_duplicated(reminder, result):
    service, _ = reminder
    api = api_with()
    api.send_private_msg.side_effect = [result, {"status": "ok", "retcode": 0, "data": {"message_id": 2}}]
    now = datetime(2026, 11, 8, 22, 50, tzinfo=TZ)
    await service.check_once(api, now)
    await service.check_once(api, now)
    await service.check_once(api, now)
    assert api.send_private_msg.await_count == 2


async def test_rejected_second_message_retries_only_second(reminder):
    service, _ = reminder
    api = api_with()
    ok = {"status": "ok", "retcode": 0, "data": {"message_id": 2}}
    api.send_private_msg.side_effect = [ok, {"status": "failed", "retcode": 100}, ok]
    now = datetime(2026, 11, 8, 22, 50, tzinfo=TZ)
    await service.check_once(api, now)
    await service.check_once(api, now)
    calls = api.send_private_msg.await_args_list
    assert len(calls) == 3 and calls[1] == calls[2] and calls[0] != calls[1]


async def test_bad_data_or_api_failure_does_not_send_zero_count_reports(reminder):
    service, _ = reminder
    api = api_with()
    now = datetime(2026, 11, 8, 22, 50, tzinfo=TZ)
    for bad in (RuntimeError("no access"), [{"record_id": "bad", "fields": {}}]):
        service.fetch_records.side_effect = bad if isinstance(bad, Exception) else None
        service.fetch_records.return_value = bad
        with pytest.raises((ValueError, RuntimeError)):
            await service.check_once(api, now)
    api.send_private_msg.assert_not_called()
    assert not service.state_path.exists()


async def test_next_month_generates_new_snapshot(reminder):
    service, _ = reminder
    api = api_with()
    await service.check_once(api, datetime(2026, 11, 30, 22, 50, tzinfo=TZ))
    await service.check_once(api, datetime(2026, 12, 1, 22, 50, tzinfo=TZ))
    assert service.fetch_records.await_count == 2
    assert api.send_private_msg.await_count == 4
    assert "本月尚未完成" not in api.send_private_msg.await_args_list[2].args[1]


async def test_feishu_pagination_is_complete_deduplicated_and_readonly(reminder, monkeypatch):
    service, cfg = reminder
    service.fetch_records = TopicReminderService.fetch_records.__get__(service)
    monkeypatch.setattr("cooper_bot.modules.calendar.topic_reminder.load_feishu_credentials",
                        lambda _: {"APP_ID": "test", "APP_SECRET": "secret"})
    service._request = AsyncMock(side_effect=[
        {"code": 0, "tenant_access_token": "testtoken"},
        {"code": 0, "data": {"items": [row()], "has_more": True, "page_token": "next/+"}},
        {"code": 0, "data": {"items": [row(), row("r2")], "has_more": False}},
    ])
    assert len(await service.fetch_records(cfg)) == 2
    calls = service._request.await_args_list
    assert "page_token=next%2F%2B" in calls[2].args[1]
    assert all("records/search?" in c.args[1] and c.args[2]["automatic_fields"] is True for c in calls[1:])


async def test_broken_pagination_and_api_error_fail_closed(reminder, monkeypatch):
    service, cfg = reminder
    service.fetch_records = TopicReminderService.fetch_records.__get__(service)
    monkeypatch.setattr("cooper_bot.modules.calendar.topic_reminder.load_feishu_credentials",
                        lambda _: {"APP_ID": "test", "APP_SECRET": "secret"})
    service._request = AsyncMock(side_effect=[{"tenant_access_token": "token"},
                                            {"data": {"items": [], "has_more": True}}])
    with pytest.raises(ValueError, match="pagination"):
        await service.fetch_records(cfg)

    response = SimpleNamespace(raise_for_status=Mock(), json=AsyncMock(return_value={"code": 999, "msg": "secret"}))
    context = AsyncMock()
    context.__aenter__.return_value = response
    with pytest.raises(RuntimeError, match="^feishu topic request failed$"):
        await TopicReminderService._request(SimpleNamespace(post=Mock(return_value=context)), "test", {})


async def test_cancelled_send_stays_claimed(reminder):
    service, _ = reminder
    api = api_with()
    api.send_private_msg.side_effect = asyncio.CancelledError
    now = datetime(2026, 11, 8, 22, 50, tzinfo=TZ)
    with pytest.raises(asyncio.CancelledError):
        await service.check_once(api, now)
    api.send_private_msg.side_effect = None
    await service.check_once(api, now)
    assert api.send_private_msg.await_count == 2
