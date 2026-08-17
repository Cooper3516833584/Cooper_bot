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
        self.gemini_chat_ready = bool(web_reply)
        self.chat_ready = False
        self._web_reply = web_reply
        self.calls: list[tuple[str, str, int]] = []

    async def restricted_gemini_calendar_chat(self, prompt: str, *, model_key: str, timeout_seconds: int) -> str:
        self.calls.append((prompt, model_key, timeout_seconds))
        return self._web_reply


def _service(tmp_path, ai=None) -> DailyCalendarService:
    config_path = tmp_path / "daily_calendar_config.json"
    config_path.write_text(
        json.dumps(
            {
                "groups": {
                    "1087250737": {
                        "enabled": True,
                        "primary_web_model": "gemini",
                        "secondary_web_model": "claude",
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
    assert ai.calls and ai.calls[0][1:] == ("gemini", 20)


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
                        "category": "national_memorial",
                        "fact": "7月1日是中国共产党成立纪念日。",
                        "tone": "solemn",
                        "source_title": "中国政府网示例",
                        "source_url": "https://www.gov.cn/example",
                    },
                    {
                        "name": "建党节",
                        "category": "national_memorial",
                        "fact": "同一纪念日的常用简称。",
                        "tone": "solemn",
                        "source_title": "中国政府网示例",
                        "source_url": "https://www.gov.cn/example",
                    },
                    {
                        "name": "香港特别行政区成立纪念日",
                        "category": "national_memorial",
                        "fact": "1997年7月1日香港回归祖国，香港特别行政区成立。",
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
