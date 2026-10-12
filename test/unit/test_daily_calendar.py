from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import cooper_bot.commands.commands as commands
from cooper_bot.modules.calendar.daily_calendar import CalendarResult, DailyCalendarService, parse_calendar_date


class _Log:
    def __init__(self) -> None:
        self.warnings: list[str] = []
        self.infos: list[str] = []

    def warning(self, text: str) -> None:
        self.warnings.append(str(text))

    def info(self, text: str) -> None:
        self.infos.append(str(text))


class _CalendarAI:
    def __init__(self, web_reply: str = "") -> None:
        self.chat_ready = bool(web_reply)
        self.deepseek_task_ready = False
        self._web_reply = web_reply
        self.calls: list[tuple[str, int]] = []

    async def calendar_web_query(self, prompt: str, *, timeout_seconds: int) -> str:
        self.calls.append((prompt, timeout_seconds))
        return self._web_reply


def _service(tmp_path, ai=None) -> DailyCalendarService:
    config_path = tmp_path / "daily_calendar_config.json"
    config_path.write_text(
        json.dumps(
            {
                "groups": {
                    "1087250737": {
                        "enabled": True,
                        "web_timeout_seconds": 20,
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    return DailyCalendarService(_Log(), ai, config_path=config_path, data_dir=tmp_path / "data")


def test_parse_calendar_date_accepts_documented_format() -> None:
    assert parse_calendar_date("2026.6.26") == date(2026, 6, 26)
    assert parse_calendar_date("２０２６．６．２６") == date(2026, 6, 26)
    assert parse_calendar_date("2026.13.1") is None
    assert parse_calendar_date("2026-6") is None


@pytest.mark.asyncio
async def test_generate_returns_non_special_result_when_no_verified_event(tmp_path) -> None:
    service = _service(tmp_path)

    result = await service.generate_for_date(date(2026, 6, 26), force_refresh=True)

    assert result.special is False
    assert result.message == ""
    assert result.events == []


@pytest.mark.asyncio
async def test_generate_uses_local_lunar_calendar_when_web_is_unavailable(tmp_path) -> None:
    service = _service(tmp_path)

    result = await service.generate_for_date(date(2026, 2, 17), force_refresh=True)

    assert result.special is True
    assert any(event["name"] == "春节" for event in result.events)
    assert "春节" in result.message


@pytest.mark.asyncio
async def test_generate_accepts_official_web_event_and_uses_deepseek_fallback_template(tmp_path) -> None:
    ai = _CalendarAI(
        json.dumps(
            {
                "date": "2026-06-26",
                "events": [
                    {
                        "name": "国家级纪念日示例",
                        "category": "official",
                        "fact": "测试用的已核验官方纪念日。",
                        "importance": 3,
                        "campus_relevance": 1,
                        "selection_reason": "具有全国公共记忆意义。",
                        "date_evidence": "来源明确说明每年6月26日纪念。",
                        "tone": "light",
                        "source_title": "中国政府网示例",
                        "source_url": "https://www.gov.cn/example",
                    }
                ],
            }
        )
    )
    service = _service(tmp_path, ai)

    result = await service.generate_for_date(date(2026, 6, 26), cfg=service.get_group_config(1087250737), force_refresh=True)

    assert result.special is True
    assert "国家级纪念日示例" in result.message
    assert ai.calls and ai.calls[0][1:] == (20,)


@pytest.mark.asyncio
async def test_generate_rejects_untrusted_official_web_event(tmp_path) -> None:
    ai = _CalendarAI(
        json.dumps(
            {
                "date": "2026-06-26",
                "events": [
                    {
                        "name": "不可信示例",
                        "category": "official",
                        "fact": "不应采纳。",
                        "importance": 3,
                        "campus_relevance": 1,
                        "selection_reason": "具有全国公共记忆意义。",
                        "date_evidence": "声称每年6月26日纪念。",
                        "tone": "light",
                        "source_title": "转载站",
                        "source_url": "https://example.com/fake",
                    }
                ],
            }
        )
    )
    service = _service(tmp_path, ai)

    result = await service.generate_for_date(date(2026, 6, 26), cfg=service.get_group_config(1087250737), force_refresh=True)

    assert result.special is False
    assert result.events == []


def test_merge_events_enriches_holiday_and_keeps_solar_festival_and_term(tmp_path) -> None:
    service = _service(tmp_path)

    events = service._merge_events(
        [
            {"name": "端午节", "category": "official", "fact": "当年法定节假日安排", "source": "local"},
            {"name": "父亲节", "category": "solar_festival", "fact": "公历节日", "source": "local"},
            {"name": "夏至", "category": "solar_term", "fact": "二十四节气", "source": "local"},
        ],
        [
            {
                "name": "端午节假期",
                "category": "official",
                "fact": "今日为端午节假期最后一天。",
                "source": "web",
                "source_title": "中国政府网示例",
                "source_url": "https://www.gov.cn/example",
            }
        ],
    )

    assert [event["name"] for event in events] == ["端午节", "父亲节", "夏至"]
    assert events[0]["fact"] == "今日为端午节假期最后一天。"


@pytest.mark.asyncio
async def test_render_message_uses_explicit_deepseek_task_interface(tmp_path) -> None:
    ai = SimpleNamespace(
        deepseek_task_ready=True,
        deepseek_task_text=AsyncMock(return_value="今天值得记住。"),
        chat=AsyncMock(side_effect=AssertionError("calendar must not call QQ chat")),
    )
    service = _service(tmp_path, ai)

    message = await service._render_message(
        date(2026, 6, 26),
        {"weekday": "五", "lunar": "五月十一", "solar_terms": []},
        [{"name": "测试日", "fact": "已核验事实", "category": "official"}],
    )

    ai.deepseek_task_text.assert_awaited_once()
    ai.chat.assert_not_awaited()
    assert "今天值得记住。" in message


@pytest.mark.asyncio
async def test_render_message_keeps_template_when_deepseek_task_is_unavailable(tmp_path) -> None:
    ai = SimpleNamespace(
        deepseek_task_ready=False,
        chat_ready=True,
        chat=AsyncMock(side_effect=AssertionError("calendar must not call QQ chat")),
    )
    service = _service(tmp_path, ai)

    message = await service._render_message(
        date(2026, 6, 26),
        {"weekday": "五", "lunar": "五月十一", "solar_terms": []},
        [{"name": "测试日", "fact": "已核验事实", "category": "official"}],
    )

    ai.chat.assert_not_awaited()
    assert message == service._base_message(
        date(2026, 6, 26),
        {"weekday": "五", "lunar": "五月十一", "solar_terms": []},
        [{"name": "测试日", "fact": "已核验事实", "category": "official"}],
    )


@pytest.mark.asyncio
async def test_generate_july_first_keeps_fixed_local_events_without_web(tmp_path) -> None:
    service = _service(tmp_path)

    result = await service.generate_for_date(date(2026, 7, 1), force_refresh=True)

    names = [str(event.get("name") or "") for event in result.events]
    assert len([name for name in names if "建党" in name or "中国共产党" in name]) == 1
    assert any("香港回归" in name for name in names)
    assert "铭记历史，珍视和平" not in result.message


@pytest.mark.asyncio
async def test_generate_ignores_old_daily_cache_without_version(tmp_path) -> None:
    service = _service(tmp_path)
    target = date(2026, 7, 1)
    service._json_save(
        service._daily_cache_path(target),
        {
            "date": "2026-07-01",
            "snapshot": {"date": "2026-07-01", "weekday": "三"},
            "events": [{"name": "建党节", "category": "solar_festival"}],
            "message": "旧缓存",
            "special": True,
        },
    )

    result = await service.generate_for_date(target)

    assert result.message != "旧缓存"
    assert any("香港回归" in str(event.get("name") or "") for event in result.events)


@pytest.mark.asyncio
async def test_generate_july_first_deduplicates_web_aliases_and_keeps_hong_kong(tmp_path) -> None:
    ai = _CalendarAI(
        json.dumps(
            {
                "date": "2026-07-01",
                "events": [
                    {
                        "name": "中国共产党成立纪念日",
                        "canonical_name": "中国共产党成立纪念日（建党节）",
                        "category": "national_memorial",
                        "fact": "7月1日是中国共产党成立纪念日。",
                        "importance": 3,
                        "campus_relevance": 1,
                        "selection_reason": "具有全国公共记忆意义。",
                        "date_evidence": "每年7月1日纪念。",
                        "tone": "solemn",
                        "source_title": "中国政府网示例",
                        "source_url": "https://www.gov.cn/example",
                    },
                    {
                        "name": "建党节",
                        "canonical_name": "中国共产党成立纪念日（建党节）",
                        "category": "national_memorial",
                        "fact": "同一纪念日的常用简称。",
                        "importance": 3,
                        "campus_relevance": 1,
                        "selection_reason": "具有全国公共记忆意义。",
                        "date_evidence": "每年7月1日纪念。",
                        "tone": "solemn",
                        "source_title": "中国政府网示例",
                        "source_url": "https://www.gov.cn/example",
                    },
                    {
                        "name": "香港特别行政区成立纪念日",
                        "canonical_name": "香港回归纪念日",
                        "category": "national_memorial",
                        "fact": "1997年7月1日香港回归祖国，香港特别行政区成立。",
                        "importance": 3,
                        "campus_relevance": 1,
                        "selection_reason": "具有全国公共记忆意义。",
                        "date_evidence": "1997年7月1日香港回归。",
                        "tone": "solemn",
                        "source_title": "中国政府网示例",
                        "source_url": "https://www.gov.cn/example",
                    },
                ],
            },
            ensure_ascii=False,
        )
    )
    service = _service(tmp_path, ai)

    result = await service.generate_for_date(date(2026, 7, 1), cfg=service.get_group_config(1087250737), force_refresh=True)

    names = [str(event.get("name") or "") for event in result.events]
    assert len([name for name in names if "建党" in name or "中国共产党" in name]) == 1
    assert any(
        "香港回归" in str(event.get("name") or "") or "香港特别行政区" in str(event.get("fact") or "")
        for event in result.events
    )
    assert "铭记历史，珍视和平" not in result.message


def _web_candidate(**overrides):
    event = {
        "name": "公共服务职业纪念日示例",
        "category": "official",
        "fact": "国家正式设立，具有广泛公共服务价值。",
        "source_url": "https://www.gov.cn/example",
        "source_title": "官方日期依据",
        "date_evidence": "每年同一月日纪念。",
        "importance": 3,
        "campus_relevance": 1,
        "selection_reason": "体现公共服务职业价值，不限于本校专业。",
    }
    return {**event, **overrides}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target,name,category,fact,url",
    [
        (date(2026, 8, 15), "日本宣布无条件投降", "historical_anniversary",
         "1945年8月15日，日本宣布无条件投降。",
         "https://www.fmprc.gov.cn/ziliao_674904/historytoday_674971/200308/t20030815_7949130.shtml"),
        (date(2026, 8, 19), "中国医师节", "official",
         "国务院批复自2018年起，每年8月19日为中国医师节。",
         "https://www.nhc.gov.cn/bgt/gwywj2/201711/5aadf3c5065e46bfb79c9cdaa14184b0.shtml"),
        (date(2026, 9, 3), "中国人民抗日战争胜利纪念日", "national_memorial",
         "每年9月3日为中国人民抗日战争胜利纪念日。",
         "https://www.fmprc.gov.cn/web/ziliao_674904/historytoday_674971/200309/t20030903_9284634.shtml"),
    ],
)
async def test_generate_accepts_public_memory_and_profession_without_name_list(tmp_path, target, name, category, fact, url):
    ai = _CalendarAI(json.dumps({"date": target.isoformat(), "events": [
        _web_candidate(name=name, category=category, fact=fact, date_evidence=fact, source_url=url),
    ]}))
    service = _service(tmp_path, ai)

    result = await service.generate_for_date(target)

    assert any(event["name"] == name and event["category"] == category for event in result.events)
    assert fact in result.message
    assert (await service.generate_for_date(target)).message == result.message
    assert len(ai.calls) == 1


@pytest.mark.parametrize("name", ["中国城市无车日", "某行业宣传倡议日"])
def test_official_source_does_not_bypass_editorial_filter(tmp_path, name):
    service = _service(tmp_path)
    target = date(2026, 9, 22)
    candidate = _web_candidate(name=name, importance=1, campus_relevance=2,
                               selection_reason="城市或行业宣传倡议，与一般公众有关。")

    assert service._validate_web_events({"date": target.isoformat(), "events": [candidate]}, target, {}) == []


@pytest.mark.parametrize("fields,accepted", [
    ({"importance": 2, "campus_relevance": 2}, True),
    ({"importance": 2, "campus_relevance": 1}, False),
    ({"importance": 3, "campus_relevance": 0}, True),
    ({"category": "historical_anniversary", "importance": 2, "campus_relevance": 2}, False),
    ({"importance": None}, False),
    ({"importance": "3"}, False),
    ({"importance": True}, False),
    ({"importance": 4}, False),
    ({"campus_relevance": True}, False),
    ({"campus_relevance": 3}, False),
    ({"selection_reason": ""}, False),
    ({"date_evidence": ""}, False),
    ({"source_url": "https://gov.cn.example.com/fake"}, False),
    ({"source_url": "https://example.com/gov.cn"}, False),
])
def test_editorial_selection_requires_valid_evidence_and_scores(tmp_path, fields, accepted):
    service = _service(tmp_path)
    target = date(2026, 8, 19)
    raw = {"date": target.isoformat(), "events": [_web_candidate(**fields)]}

    assert bool(service._validate_web_events(raw, target, {})) is accepted


@pytest.mark.asyncio
async def test_solar_festival_cannot_bypass_review_via_local_calendar(tmp_path, monkeypatch):
    target = date(2026, 4, 1)
    candidate = _web_candidate(name="愚人节", category="solar_festival", importance=1)
    ai = _CalendarAI(json.dumps({"date": target.isoformat(), "events": [candidate]}))
    service = _service(tmp_path, ai)
    snapshot = {"date": target.isoformat(), "solar_festivals": ["愚人节"]}
    monkeypatch.setattr(service, "_build_snapshot", lambda _date: snapshot)

    result = await service.generate_for_date(target)

    assert result.events == []
    assert result.special is False
    candidate.update(importance=2, campus_relevance=2)
    assert service._validate_web_events({"date": target.isoformat(), "events": [candidate]}, target, snapshot)
    candidate["category"] = "traditional"
    assert service._validate_web_events({"date": target.isoformat(), "events": [candidate]}, target, snapshot) == []


def test_candidates_are_filtered_and_ranked_before_final_limit(tmp_path):
    service = _service(tmp_path)
    target = date(2026, 8, 15)
    candidates = [_web_candidate(name=f"校园选题{i}", importance=2, campus_relevance=2) for i in range(4)]
    candidates.append(_web_candidate(name="重大历史节点", category="historical_anniversary"))
    raw = {"date": target.isoformat(), "events": candidates}

    events = service._merge_events([], service._validate_web_events(raw, target, {}))

    assert len(events) == 3
    assert events[0]["name"] == "重大历史节点"


def test_aliases_share_one_slot_and_keep_stronger_assessment(tmp_path):
    service = _service(tmp_path)
    target = date(2026, 8, 15)
    candidates = [
        _web_candidate(name="历史事件别名", canonical_name="同一历史事件", importance=2, campus_relevance=2),
        _web_candidate(name="历史事件规范标题", canonical_name="同一历史事件", category="historical_anniversary"),
        _web_candidate(name="另一重要事项"),
        _web_candidate(name="第三项重要事项"),
    ]
    raw = {"date": target.isoformat(), "events": candidates}

    events = service._merge_events([], service._validate_web_events(raw, target, {}))

    assert [event["name"] for event in events] == ["历史事件规范标题", "另一重要事项", "第三项重要事项"]
    assert events[0]["importance"] == 3


@pytest.mark.asyncio
async def test_failed_web_lookup_is_retried_after_recovery(tmp_path):
    service = _service(tmp_path)
    target = date(2026, 8, 19)

    result = await service.generate_for_date(target)

    assert not result.web_used
    assert not service._daily_cache_path(target).exists()
    service.aisvc = _CalendarAI(json.dumps({"date": target.isoformat(), "events": [_web_candidate()]}))
    result = await service.generate_for_date(target)
    assert result.web_used and result.special


@pytest.mark.asyncio
async def test_previous_editorial_policy_cache_is_invalidated(tmp_path):
    service = _service(tmp_path, _CalendarAI(json.dumps({"date": "2026-09-22", "events": []})))
    target = date(2026, 9, 22)
    service._json_save(service._daily_cache_path(target), {
        "cache_version": 2, "date": target.isoformat(), "snapshot": {},
        "events": [_web_candidate(name="旧策略误选的活动")], "message": "旧缓存", "special": True,
    })

    result = await service.generate_for_date(target)

    assert result.message != "旧缓存"
    assert all(event["name"] != "旧策略误选的活动" for event in result.events)


@pytest.mark.asyncio
async def test_calendartest_returns_exact_non_special_text(monkeypatch) -> None:
    replies: list[str] = []

    async def _reply(_api, _ctx, text, _logsvc, force_private_user_id=None):
        replies.append(str(text))

    target_date = date(2026, 6, 26)
    fake_service = SimpleNamespace(
        get_group_config=Mock(return_value={}),
        generate_for_date=AsyncMock(
            return_value=CalendarResult(target_date, {}, [], "", False, False)
        ),
    )
    ctx = SimpleNamespace(level=3, scene="group", group_id=1087250737, user_id=1, nickname="tester", card="tester")
    logsvc = SimpleNamespace(log=SimpleNamespace(warning=lambda _msg: None))
    monkeypatch.setattr(commands, "reply", _reply)

    await commands._handle_explicit_command(
        SimpleNamespace(),
        ctx,
        "/calendartest 2026.6.26",
        SimpleNamespace(),
        logsvc,
        commands.BotState(),
        SimpleNamespace(),
        None,
        None,
        fake_service,
    )

    assert replies == ["非特殊日期"]
    fake_service.generate_for_date.assert_awaited_once_with(
        target_date,
        cfg={},
        force_refresh=True,
        refresh_holiday_schedule=True,
    )


@pytest.mark.asyncio
async def test_calendartest_requires_admin(monkeypatch) -> None:
    reply = AsyncMock()
    ctx = SimpleNamespace(level=2, scene="group", group_id=1087250737, user_id=1, nickname="tester", card="tester")
    monkeypatch.setattr(commands, "reply", reply)

    await commands._handle_explicit_command(
        SimpleNamespace(),
        ctx,
        "/calendartest 2026.6.26",
        SimpleNamespace(),
        SimpleNamespace(log=SimpleNamespace(warning=lambda _msg: None)),
        commands.BotState(),
        SimpleNamespace(),
        None,
        None,
        None,
    )

    assert "仅管理员可用" in str(reply.await_args.args[2])
