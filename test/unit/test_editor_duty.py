import json
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from cooper_bot.modules.calendar.editor_duty import (
    EditorDutyService, _TZ, editor_on_duty, greeting_day, greeting_kind,
)


DAY = date(2026, 10, 20)
GROUP_ID = 30303
PUBLISHER_ID = 40404


def ok(data):
    return {"status": "ok", "retcode": 0, "data": data}


def event(text="", *, uid=42, day=DAY, forward=None):
    return {
        "post_type": "message", "message_type": "group", "group_id": GROUP_ID,
        "user_id": uid, "message_id": 123,
        "time": int(datetime(day.year, day.month, day.day, 8, tzinfo=_TZ).timestamp()),
        "message": [{"type": "forward", "data": {"id": forward}}] if forward else [
            {"type": "text", "data": {"text": text}}],
    }


@pytest.fixture
def service(tmp_path):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"enabled": True, "group_id": GROUP_ID, "publisher_id": PUBLISHER_ID, "odd_week_monday": "2026-10-12"}), encoding="utf-8")
    return EditorDutyService(SimpleNamespace(warning=Mock()), config_path=config, state_path=tmp_path / "state.json")


@pytest.fixture
def api():
    return SimpleNamespace(
        call=AsyncMock(return_value=ok({"messages": [event(day=date(2026, 10, 19))]})),
        get_group_member_list=AsyncMock(return_value=ok([
            {"user_id": 42, "card": "QQ周二S-编辑甲"},
            {"user_id": 43, "card": "QQ周二D-编辑乙"},
            {"user_id": 44, "card": "QQ周三S-编辑丙"},
        ])),
        send_group_msg=AsyncMock(return_value=ok({"message_id": 1})),
    )


@pytest.mark.parametrize("card,day,expected", [
    ("QQ周二S-甲", DAY, True), ("qq周二s-甲", DAY, True),
    ("QQ周二D-乙", DAY, False), ("QQ周二SD-甲", DAY, True),
    ("QQ周二d-乙", date(2026, 10, 27), True),
    ("QQ周二S-甲", date(2026, 11, 3), True),
    ("QQ周一S-甲", DAY, False), ("PSD爱好者", DAY, False),
    ("QQ周天S-甲", date(2026, 10, 25), True),
    ("QQ周二D-甲", date(2026, 10, 13), True),
    ("QQ周五d/深度内容–甲", date(2026, 10, 30), True),
    ("甲-活动部/QQ周三S", date(2026, 10, 21), True),
    ("QQ周一D/深度内容/美工部-甲", date(2026, 10, 26), True),
    ("QQ周一d/深度内容 甲", date(2026, 10, 26), True),
    ("QQ周日D/即时内容-甲", date(2026, 11, 1), True),
    ("QQ 周三D编辑甲", date(2026, 10, 28), True),
    ("QQ常务/周六D/微信/美工-甲", date(2026, 10, 31), True),
    ("QQ周六D/小红书/即时内容部-甲", date(2026, 10, 31), True),
    ("活动部/周四d-甲", date(2026, 10, 29), True),
    ("QQ周五S/活动部-甲", date(2026, 10, 23), True),
    ("QQ周日S编辑-甲", date(2026, 10, 25), True),
    ("常务QQ周六S/微信-甲", date(2026, 10, 24), True),
    ("周二Sparky", DAY, False), ("周二Design", DAY, False),
    ("QQ周一D-甲", date(2026, 10, 12), True),
    ("QQ周一S-甲", date(2026, 10, 12), False),
    ("QQ周一S-甲", date(2026, 10, 19), True),
    ("QQ周一D-甲", date(2026, 10, 19), False),
])
def test_roster_week_and_weekday(card, day, expected):
    assert editor_on_duty(card, day, date(2026, 10, 12)) is expected


@pytest.mark.parametrize("slot,with_retired_substitute", [("morning", False), ("evening", True)])
async def test_retired_editor_is_excluded_by_id_even_after_card_change(service, api, slot, with_retired_substitute):
    config = json.loads(service.config_path.read_text(encoding="utf-8"))
    config["excluded_editor_ids"] = [42]
    service.config_path.write_text(json.dumps(config), encoding="utf-8")
    api.get_group_member_list.return_value = ok([
        {"user_id": 42, "card": "QQ周二S-已改名"},
        {"user_id": 45, "card": "QQ周二S-在任编辑"},
    ])
    if with_retired_substitute:
        service._day(DAY)["morning_editor"] = {"user_id": 42, "time": 1}
    service._published = AsyncMock(return_value=False)
    hour, minute, title = (9, 0, "早安") if slot == "morning" else (22, 55, "晚安")
    await service.check_once(api, datetime(2026, 10, 20, hour, minute, tzinfo=_TZ))
    api.send_group_msg.assert_awaited_once_with(GROUP_ID, f"未检测到{title}，请[CQ:at,qq=45]注意值班")


async def test_both_absent_mentions_only_matching_editor_and_deduplicates(service, api):
    service._published = AsyncMock(return_value=False)
    now = datetime(2026, 10, 20, 9, 0, tzinfo=_TZ)
    await service.check_once(api, now)
    api.send_group_msg.assert_awaited_once_with(GROUP_ID, "未检测到早安，请[CQ:at,qq=42]注意值班")
    await service.check_once(api, now)
    restarted = EditorDutyService(service.log, config_path=service.config_path, state_path=service.state_path)
    await restarted.check_once(api, now)
    assert api.send_group_msg.await_count == 1


async def test_forward_content_suppresses_reminder_and_uses_outer_sender(service, api):
    async def call(action, params):
        if action == "get_group_msg_history":
            return ok({"messages": [event(forward="f1", uid=55), event(day=date(2026, 10, 19))]})
        assert params == {"message_id": "f1"}
        return ok({"messages": [{"sender": {"user_id": PUBLISHER_ID}, "content": [
            {"type": "text", "data": {"text": "#早安华中大#\n今天也要加油\n（今日编辑：编辑甲）"}},
        ]}, {"message": [{"type": "image", "data": {"file": "photo"}}]}]})
    api.call.side_effect = call
    service._published = AsyncMock(return_value=None)
    await service.check_once(api, datetime(2026, 10, 20, 9, 0, tzinfo=_TZ))
    api.send_group_msg.assert_not_awaited()
    service._published.assert_not_awaited()
    assert service._day(DAY)["morning"]["user_id"] == 55


async def test_morning_substitute_takes_evening_shift(service, api):
    service._day(DAY)["morning"] = {"user_id": 44, "time": 1}
    service._published = AsyncMock(return_value=False)
    await service.check_once(api, datetime(2026, 10, 20, 22, 55, tzinfo=_TZ))
    api.send_group_msg.assert_awaited_once_with(GROUP_ID, "未检测到晚安，请[CQ:at,qq=44]注意值班")


async def test_publisher_does_not_overwrite_morning_substitute(service, api):
    api.call.return_value = ok({"messages": [{"message": [
        {"type": "text", "data": {"text": "#早安华中大#\n正文\n今日编辑：编辑甲"}},
        {"type": "image", "data": {"file": "photo"}},
    ]}]})
    await service._record(api, event(forward="morning", uid=44), DAY)
    publisher = event(forward="publisher-morning", uid=PUBLISHER_ID)
    publisher["time"] += 60
    await service._record(api, publisher, DAY)
    assert service._day(DAY)["morning_editor"]["user_id"] == 44


async def test_authorized_group_only_fallback_still_reminds(service, api):
    config = json.loads(service.config_path.read_text(encoding="utf-8"))
    config["qzone_unavailable_policy"] = "group_only"
    service.config_path.write_text(json.dumps(config), encoding="utf-8")
    service._published = AsyncMock(return_value=None)
    await service.check_once(api, datetime(2026, 10, 20, 9, 0, tzinfo=_TZ))
    api.send_group_msg.assert_awaited_once()


async def test_group_only_policy_still_skips_when_group_history_is_unknown(service, api):
    config = json.loads(service.config_path.read_text(encoding="utf-8"))
    config["qzone_unavailable_policy"] = "group_only"
    service.config_path.write_text(json.dumps(config), encoding="utf-8")
    service._published = AsyncMock(return_value=None)
    service._history = AsyncMock(return_value=False)
    await service.check_once(api, datetime(2026, 10, 20, 9, 0, tzinfo=_TZ))
    api.send_group_msg.assert_not_awaited()


@pytest.mark.parametrize("published,history_ok", [(True, True), (True, False), (None, True), (False, False)])
async def test_present_or_unknown_never_sends(service, api, published, history_ok):
    service._published = AsyncMock(return_value=published)
    service._history = AsyncMock(return_value=history_ok)
    await service.check_once(api, datetime(2026, 10, 20, 9, 0, tzinfo=_TZ))
    api.send_group_msg.assert_not_awaited()


@pytest.mark.parametrize("uid,text", [
    (42, "大家早安"),
    (PUBLISHER_ID, "大家早安"),
    (PUBLISHER_ID, "#早安华中大#\n正文\n今日编辑：编辑甲"),
    (42, "#早安华中大#\n正文\n今日编辑：编辑甲"),
])
async def test_plain_chat_greeting_is_not_submission(service, api, uid, text):
    await service._record(api, event(text, uid=uid), DAY)
    assert not service._day(DAY).get("morning")
    api.call.assert_not_awaited()


async def test_midnight_evening_submission_does_not_cover_tonights_shift(service, api):
    evt = event(forward="late")
    evt["time"] = int(datetime(2026, 10, 20, 0, 20, tzinfo=_TZ).timestamp())
    api.call.return_value = ok({"messages": [{"message": [
        {"type": "text", "data": {"text": "#晚安华中大#\n正文\n今日编辑：编辑甲"}},
        {"type": "image", "data": {"file": "photo"}},
    ]}]})
    await service._record(api, evt, DAY)
    assert "evening" not in service._day(DAY)
    assert greeting_day(evt["time"], "evening") == date(2026, 10, 19)
    assert greeting_day(evt["time"], "morning") == DAY


@pytest.mark.parametrize("hour,minute,expected", [
    (0, 20, date(2026, 10, 19)),
    (12, 0, date(2026, 10, 19)),
    (15, 59, date(2026, 10, 19)),
    (16, 0, DAY),
    (22, 55, DAY),
])
def test_evening_day_changes_at_16_while_morning_keeps_calendar_date(hour, minute, expected):
    stamp = int(datetime(2026, 10, 20, hour, minute, tzinfo=_TZ).timestamp())
    assert greeting_day(stamp, "evening") == expected
    assert greeting_day(stamp, "morning") == DAY


@pytest.mark.parametrize("hour,minute,detected", [(15, 59, False), (16, 0, True)])
async def test_group_and_qzone_share_the_16_hour_evening_boundary(service, api, monkeypatch, hour, minute, detected):
    text = "#晚安华中大#\n正文\n今日编辑：编辑甲"
    evt = event(forward="evening-boundary")
    evt["time"] = int(datetime(2026, 10, 20, hour, minute, tzinfo=_TZ).timestamp())
    api.call.return_value = ok({"messages": [{"message": [{"type": "text", "data": {"text": text}}]}]})
    await service._record(api, evt, DAY)
    assert bool(service._day(DAY).get("evening")) is detected

    response = Mock()
    payload = {"code": 0, "msglist": [{"created_time": evt["time"], "content": text}]}
    response.read.return_value = json.dumps(payload).encode()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    monkeypatch.setattr("urllib.request.urlopen", Mock(return_value=response))
    assert EditorDutyService._fetch_qzone("uin=o1; p_skey=test-only", DAY, "evening", PUBLISHER_ID) is detected


async def test_other_group_and_private_events_do_not_read_forward(service, api):
    evt = event(day=datetime.now(_TZ).date(), forward="private")
    evt["group_id"] = GROUP_ID + 1
    await service.observe_event(api, evt)
    evt["group_id"] = GROUP_ID
    evt["message_type"] = "private"
    await service.observe_event(api, evt)
    api.call.assert_not_awaited()


async def test_migrated_group_observes_current_submission(service, api):
    today = datetime.now(_TZ).date()
    evt = event(day=today, forward="migrated")
    evt["group_id"] = GROUP_ID
    service._record = AsyncMock(return_value=True)
    await service.observe_event(api, evt)
    service._record.assert_awaited_once_with(api, evt, today)


async def test_history_paginates_even_when_napcat_returns_99_or_fewer(service, api):
    api.call.side_effect = [ok({"messages": [event()]}), ok({"messages": [event(day=date(2026, 10, 19))]})]
    assert await service._history(api, DAY) is True
    assert api.call.await_args_list[1].args[1]["message_seq"] == 123
    assert api.call.await_args_list[1].args[1]["reverseOrder"] is True


async def test_history_loop_and_unreadable_forward_are_unknown(service, api):
    api.call.return_value = ok({"messages": [event()]})
    assert await service._history(api, DAY) is False
    api.call.return_value = None
    kinds, complete = await service._forward_kinds(api, event(forward="broken"))
    assert kinds == set() and complete is False


async def test_cookie_transport_error_is_unknown(service, api):
    api.call.side_effect = RuntimeError("unavailable")
    assert await service._published(api, DAY, "morning") is None


async def test_forwarded_chat_and_nested_unreadable_content(service, api):
    api.call.return_value = ok({"messages": [{"message": [{"type": "text", "data": {"text": "早安"}}]}]})
    assert await service._forward_kinds(api, event(forward="chat")) == (set(), True)
    api.call.return_value = ok({"messages": [{"message": [{"type": "forward", "data": {"id": "nested"}}]}]})
    assert await service._forward_kinds(api, event(forward="nested-parent")) == (set(), False)


@pytest.mark.parametrize("text,expected", [
    ("聊天记录里的普通问候：早安", set()),
    ("#晚安华中大#\n正文\n今日编辑：编辑甲", {"evening"}),
])
async def test_nested_inline_forward_uses_returned_nodes_without_requesting_inner_id(service, api, text, expected):
    api.call.return_value = ok({"messages": [{"message": [{"type": "forward", "data": {
        "id": "inner-id-cannot-be-fetched", "content": [{"message": [
            {"type": "text", "data": {"text": text}},
        ]}],
    }}]}]})
    assert await service._forward_kinds(api, event(forward="outer")) == (expected, True)
    api.call.assert_awaited_once_with("get_forward_msg", {"message_id": "outer"})


async def test_unrelated_nested_forward_does_not_cancel_evening_reminder(service, api):
    morning = event(forward="morning", uid=44)
    nested = event(forward="nested")
    nested["time"] += 3 * 3600
    rows = [event(day=date(2026, 10, 19)), morning, nested]

    async def call(action, params):
        if action == "get_group_msg_history":
            return ok({"messages": rows})
        if params["message_id"] == "morning":
            return ok({"messages": [{"message": [{"type": "text", "data": {
                "text": "#早安华中大#\n正文\n今日编辑：编辑丙",
            }}]}]})
        assert params["message_id"] == "nested"
        return ok({"messages": [{"message": [{"type": "forward", "data": {
            "id": "not-fetchable", "content": [{"message": [{"type": "image", "data": {"file": "photo"}}]}],
        }}]}]})

    api.call.side_effect = call
    service._published = AsyncMock(return_value=False)
    await service.check_once(api, datetime(2026, 10, 20, 22, 55, tzinfo=_TZ))
    api.send_group_msg.assert_awaited_once_with(GROUP_ID, "未检测到晚安，请[CQ:at,qq=44]注意值班")


async def test_nested_forward_depth_is_bounded(service, api):
    nodes = [{"message": [{"type": "text", "data": {"text": "普通聊天"}}]}]
    for _ in range(5):
        nodes = [{"message": [{"type": "forward", "data": {"content": nodes}}]}]
    api.call.return_value = ok({"messages": nodes})
    assert await service._forward_kinds(api, event(forward="too-deep")) == (set(), False)


async def test_unknown_check_retries_after_ten_seconds_and_recovers(service, api):
    service._history = AsyncMock(side_effect=[False, True])
    service._published = AsyncMock(return_value=False)
    now = datetime(2026, 10, 20, 9, 0, tzinfo=_TZ)
    await service.check_once(api, now)
    assert service._day(DAY)["checks"]["morning"] == "unknown"
    assert "群历史" in service.log.warning.call_args.args[0]
    assert "空间未检测到对应栏目" in service.log.warning.call_args.args[0]
    await service.check_once(api, now.replace(second=5))
    assert service._history.await_count == 1
    await service.check_once(api, now.replace(second=10))
    api.send_group_msg.assert_awaited_once()
    assert service._day(DAY)["checks"]["morning"] == "sent"


async def test_unknown_check_stops_after_three_attempts(service, api):
    service._history = AsyncMock(return_value=False)
    service._published = AsyncMock(return_value=None)
    for second in (0, 5, 10, 15, 20, 30, 40):
        await service.check_once(api, datetime(2026, 10, 20, 9, 0, second, tzinfo=_TZ))
    assert service._history.await_count == 3
    assert service.log.warning.call_count == 3
    api.send_group_msg.assert_not_awaited()


async def test_space_only_error_is_reported_separately(service, api):
    service._history = AsyncMock(return_value=True)
    service._published = AsyncMock(return_value=None)
    await service.check_once(api, datetime(2026, 10, 20, 9, 0, tzinfo=_TZ))
    warning = service.log.warning.call_args.args[0]
    assert "空间读取未确认" in warning
    assert "群历史读取或转发解析不完整" not in warning


@pytest.mark.parametrize("text,expected", [
    ("#早安华中大#\n正文\n今日编辑：编辑甲", {"morning"}),
    ("\n#早安华中大#\n正文\n（今日编辑：编辑甲）", {"morning"}),
    ("#午安华中大#\n正文\n今日编辑：编辑乙", {"morning"}),
    ("#晚安华中大#\n正文\n今日编辑:编辑甲", {"evening"}),
    ("#晚安华中大#\n正文里提到早安\n今日编辑：编辑甲", {"evening"}),
    ("大家早安，晚上记得发晚安", set()),
    ("#快讯#\n早安华中大，晚安华中大\n今日编辑：编辑甲", set()),
    ("#周五去哪玩#\n今日编辑：编辑甲\n晚安", set()),
    ("转一条 #早安华中大#\n今日编辑：编辑甲", set()),
    ("#早安华中大#\n正文", set()),
    ("#早安华中大#\n正文\n（今日编辑：）", set()),
    ("#早安华中大#\n今日编辑：\n正文", set()),
    ("#早安华中大好呀#\n正文\n今日编辑：编辑甲", set()),
])
def test_greeting_uses_actual_column_tag_and_editor_signature(text, expected):
    assert greeting_kind(text) == expected


async def test_noon_submission_without_image_sets_actual_editors_evening_shift(service, api):
    api.call.return_value = ok({"messages": [{"sender": {"user_id": 9000}, "message": [
        {"type": "text", "data": {"text": "#午安华中大#\n正文\n今日编辑：编辑丙"}},
    ]}]})
    noon = event(forward="noon", uid=44)
    noon["time"] = int(datetime(2026, 10, 20, 12, 5, tzinfo=_TZ).timestamp())
    await service._record(api, noon, DAY)
    assert service._day(DAY)["morning_editor"]["user_id"] == 44
    api.call.return_value = ok({"messages": [event(day=date(2026, 10, 19))]})
    service._published = AsyncMock(return_value=False)
    await service.check_once(api, datetime(2026, 10, 20, 22, 55, tzinfo=_TZ))
    api.send_group_msg.assert_awaited_once_with(GROUP_ID, "未检测到晚安，请[CQ:at,qq=44]注意值班")


async def test_generic_greeting_with_image_in_forward_is_not_a_submission(service, api):
    api.call.return_value = ok({"messages": [{"message": [
        {"type": "text", "data": {"text": "早安，今晚记得发晚安"}},
        {"type": "image", "data": {"file": "photo"}},
    ]}]})
    await service._record(api, event(forward="casual", uid=44), DAY)
    assert "morning" not in service._day(DAY)
    assert "evening" not in service._day(DAY)
    assert "morning_editor" not in service._day(DAY)


async def test_outside_exact_minutes_and_disabled_do_nothing(service, api):
    for hour, minute in [(8, 59), (9, 1), (22, 54), (22, 56)]:
        await service.check_once(api, datetime(2026, 10, 20, hour, minute, tzinfo=_TZ))
    service.config_path.write_text('{"enabled": false}', encoding="utf-8")
    await service.check_once(api, datetime(2026, 10, 20, 9, 0, tzinfo=_TZ))
    api.call.assert_not_awaited()


async def test_uncertain_send_is_not_retried(service, api):
    service._published = AsyncMock(return_value=False)
    api.send_group_msg.return_value = None
    now = datetime(2026, 10, 20, 9, 0, tzinfo=_TZ)
    await service.check_once(api, now)
    await service.check_once(api, now)
    assert api.send_group_msg.await_count == 1
    assert service._day(DAY)["checks"]["morning"] == "uncertain"


@pytest.mark.parametrize("payload,expected", [
    ({"code": 0, "msglist": [{"created_time": event()["time"], "content": "#早安华中大#\n正文\n今日编辑：编辑甲"}]}, True),
    ({"code": 0, "msglist": [{"created_time": event()["time"], "content": "#午安华中大#\n正文\n今日编辑：编辑甲"}]}, True),
    ({"code": 0, "msglist": [{"created_time": event()["time"], "content": "早安"}]}, False),
    ({"code": 0, "msglist": [{"created_time": event()["time"], "content": "#快讯#\n早安，晚安\n今日编辑：编辑甲"}]}, False),
    ({"code": 0, "msglist": [{"created_time": event()["time"], "content": "其他栏目"}]}, False),
    ({"code": -3000, "message": "login required"}, None),
    ({"code": 0, "total": 0}, False),
    ({"code": 0}, None),
])
def test_qzone_response_and_auth_failures(monkeypatch, payload, expected):
    response = Mock()
    response.read.return_value = ("_Callback(" + json.dumps(payload) + ");").encode()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    opener = Mock(return_value=response)
    monkeypatch.setattr("urllib.request.urlopen", opener)
    assert EditorDutyService._fetch_qzone("uin=o1; p_skey=test-only", DAY, "morning", PUBLISHER_ID) is expected
    assert f"uin={PUBLISHER_ID}" in opener.call_args.args[0].full_url


def test_state_keeps_only_eight_dates_and_no_message_text(service):
    for num in range(1, 15):
        service._day(date(2026, 10, num))["morning"] = {"user_id": 42, "time": num}
    service._save()
    assert len(json.loads(service.state_path.read_text(encoding="utf-8"))["days"]) == 8


@pytest.mark.parametrize("value", [None, 0, -1, "invalid", "１２３", True])
async def test_missing_or_invalid_target_never_calls_api(tmp_path, api, value):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"enabled": True, "group_id": value, "publisher_id": PUBLISHER_ID}), encoding="utf-8")
    service = EditorDutyService(SimpleNamespace(warning=Mock()), config_path=path, state_path=tmp_path / "state.json")
    await service.observe_event(api, event(day=datetime.now(_TZ).date()))
    await service.check_once(api, datetime(2026, 10, 20, 9, 0, tzinfo=_TZ))
    api.call.assert_not_awaited()
    api.get_group_member_list.assert_not_awaited()
    api.send_group_msg.assert_not_awaited()
