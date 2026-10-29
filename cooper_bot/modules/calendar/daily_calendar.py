from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from cooper_bot.core.config import DAILY_CALENDAR_CONFIG_PATH, DAILY_CALENDAR_DATA_DIR, TIMEZONE
from cooper_bot.modules.ai.prompt_store import PromptConfigError, render_prompt

try:
    from lunar_python import Solar
except Exception:  # pragma: no cover - 依赖缺失时仍允许联网降级
    Solar = None


_TZ = ZoneInfo(TIMEZONE)
_WEEKDAYS = ("一", "二", "三", "四", "五", "六", "日")
_URL_RE = re.compile(r"https?://[^\s<>\]\[\"']+", flags=re.IGNORECASE)
_CQ_RE = re.compile(r"\[CQ:[^\]]+\]", flags=re.IGNORECASE)
_DATE_RE = re.compile(r"^(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})$")
_RETRYABLE_WEB_ERROR_MARKERS = ("busy", "capacity", "503", "429", "temporarily", "timeout", "unavailable")
_VALID_WEB_CATEGORIES = {"traditional", "solar_term", "solar_festival", "official", "national_memorial", "historical_anniversary"}
_DAILY_CACHE_VERSION = 3
_FIXED_SOLAR_EVENTS: dict[tuple[int, int], list[dict[str, str]]] = {
    (7, 1): [
        {
            "name": "中国共产党成立纪念日（建党节）",
            "category": "national_memorial",
            "fact": "1921年中国共产党成立，7月1日为中国共产党成立纪念日。",
            "tone": "light",
        },
        {
            "name": "香港回归纪念日",
            "category": "national_memorial",
            "fact": "1997年7月1日香港回归祖国，香港特别行政区成立。",
            "tone": "light",
        },
    ],
}


@dataclass
class CalendarResult:
    target_date: date
    snapshot: dict[str, Any]
    events: list[dict[str, Any]]
    message: str
    special: bool
    web_used: bool = False


def parse_calendar_date(text: str) -> Optional[date]:
    """Parse the documented YYYY.M.D format, plus common full-width separators."""
    raw = str(text or "").strip().translate(str.maketrans("０１２３４５６７８９．／－", "0123456789./-"))
    m = _DATE_RE.fullmatch(raw)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


class DailyCalendarService:
    """Single-purpose daily calendar reminder with a cached, web-assisted fact pipeline."""

    def __init__(
        self,
        log,
        aisvc=None,
        *,
        config_path: Path = DAILY_CALENDAR_CONFIG_PATH,
        data_dir: Path = DAILY_CALENDAR_DATA_DIR,
    ) -> None:
        self.log = log
        self.aisvc = aisvc
        self.config_path = Path(config_path)
        self.data_dir = Path(data_dir)
        self.state_path = self.data_dir / "state.json"
        self._group_locks: dict[int, asyncio.Lock] = {}

    @staticmethod
    def _json_load(path: Path, default: Any) -> Any:
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
            return obj
        except Exception:
            return default

    @staticmethod
    def _json_save(path: Path, value: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp.replace(path)

    def _warning(self, text: str) -> None:
        try:
            self.log.warning(text)
        except Exception:
            pass

    def _info(self, text: str) -> None:
        try:
            self.log.info(text)
        except Exception:
            pass

    def _load_config(self) -> dict[str, Any]:
        raw = self._json_load(self.config_path, {})
        return raw if isinstance(raw, dict) else {}

    def get_group_config(self, group_id: int) -> dict[str, Any]:
        groups = self._load_config().get("groups", {})
        if not isinstance(groups, dict):
            return {}
        raw = groups.get(str(int(group_id)), {})
        return dict(raw) if isinstance(raw, dict) else {}

    def _load_state(self) -> dict[str, Any]:
        raw = self._json_load(self.state_path, {})
        if not isinstance(raw, dict):
            raw = {}
        raw.setdefault("groups", {})
        raw.setdefault("holiday_refresh", {})
        return raw

    def _save_state(self, state: dict[str, Any]) -> None:
        self._json_save(self.state_path, state)

    @staticmethod
    def _parse_hhmm(value: Any, default_hour: int, default_minute: int) -> tuple[int, int]:
        text = str(value or "").strip()
        m = re.fullmatch(r"(\d{1,2}):(\d{2})", text)
        if not m:
            return default_hour, default_minute
        hour, minute = int(m.group(1)), int(m.group(2))
        if 0 <= hour < 24 and 0 <= minute < 60:
            return hour, minute
        return default_hour, default_minute

    @staticmethod
    def _safe_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _as_string_list(value: Any) -> list[str]:
        if value is None:
            return []
        values = value if isinstance(value, (list, tuple, set)) else [value]
        out: list[str] = []
        for item in values:
            text = str(item or "").strip()
            if text and text not in out:
                out.append(text)
        return out

    @staticmethod
    def _event_key(name: Any) -> str:
        raw = str(name or "").strip()
        key = re.sub(r"[\s·•，,。.!！?？()（）\[\]【】]", "", raw).casefold()
        return key

    @staticmethod
    def _is_official_source(url: Any) -> bool:
        try:
            host = (urlparse(str(url or "")).hostname or "").casefold()
        except Exception:
            return False
        return host == "gov.cn" or host.endswith(".gov.cn")

    def _holiday_path(self, year: int) -> Path:
        return self.data_dir / f"holiday_schedule_{int(year)}.json"

    def _daily_cache_path(self, target_date: date) -> Path:
        return self.data_dir / f"daily_facts_{target_date.isoformat()}.json"

    def _pending_holiday_path(self, year: int) -> Path:
        return self.data_dir / f"holiday_schedule_pending_{int(year)}.json"

    def _load_holiday_schedule(self, year: int) -> dict[str, Any]:
        raw = self._json_load(self._holiday_path(year), {})
        if not isinstance(raw, dict) or int(raw.get("year") or 0) != int(year):
            return {"year": int(year), "days": {}}
        days = raw.get("days")
        raw["days"] = days if isinstance(days, dict) else {}
        return raw

    def _build_snapshot(self, target_date: date) -> dict[str, Any]:
        schedule = self._load_holiday_schedule(target_date.year)
        schedule_day = schedule.get("days", {}).get(target_date.isoformat())
        if not isinstance(schedule_day, dict):
            schedule_day = {}

        snapshot: dict[str, Any] = {
            "date": target_date.isoformat(),
            "weekday": _WEEKDAYS[target_date.weekday()],
            "lunar": "",
            "solar_terms": [],
            "solar_festivals": [],
            "lunar_festivals": [],
            "holiday_status": str(schedule_day.get("status") or "").strip(),
            "holiday_name": str(schedule_day.get("name") or "").strip(),
            "holiday_source_url": str(schedule.get("source_url") or "").strip(),
            "calendar_provider_ready": Solar is not None,
        }
        if Solar is None:
            return snapshot
        try:
            solar = Solar.fromYmd(target_date.year, target_date.month, target_date.day)
            lunar = solar.getLunar()
            month = str(lunar.getMonthInChinese() or "").strip()
            day = str(lunar.getDayInChinese() or "").strip()
            if month and day:
                snapshot["lunar"] = f"农历{month}月{day}"
            snapshot["solar_terms"] = self._as_string_list(lunar.getJieQi())
            snapshot["solar_festivals"] = self._as_string_list(solar.getFestivals())
            snapshot["lunar_festivals"] = self._as_string_list(lunar.getFestivals())
        except Exception as e:
            self._warning(f"daily calendar local snapshot failed: date={target_date.isoformat()} err={e}")
        return snapshot

    def _local_events(self, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        if snapshot.get("holiday_status") == "holiday":
            name = str(snapshot.get("holiday_name") or "法定休息日").strip()
            events.append({"name": name, "category": "official", "fact": "当年法定节假日安排", "tone": "light", "source": "local"})
        try:
            month, day = (int(part) for part in str(snapshot.get("date") or "").split("-")[1:3])
        except Exception:
            month, day = 0, 0
        for event in _FIXED_SOLAR_EVENTS.get((month, day), []):
            local_event = dict(event)
            local_event["source"] = "local"
            events.append(local_event)
        for name in self._as_string_list(snapshot.get("lunar_festivals")):
            events.append({"name": name, "category": "traditional", "fact": "农历传统节日", "tone": "light", "source": "local"})
        # 公历节日表只提供候选，不能替代校园媒体的选题价值判断。
        for name in self._as_string_list(snapshot.get("solar_terms")):
            events.append({"name": name, "category": "solar_term", "fact": "二十四节气", "tone": "light", "source": "local"})
        return events

    def _calendar_prompt(self, target_date: date, snapshot: dict[str, Any]) -> str:
        safe_snapshot = {
            "date": snapshot.get("date"),
            "weekday": snapshot.get("weekday"),
            "lunar": snapshot.get("lunar"),
            "solar_terms": snapshot.get("solar_terms"),
            "solar_festivals": snapshot.get("solar_festivals"),
            "lunar_festivals": snapshot.get("lunar_festivals"),
            "holiday_status": snapshot.get("holiday_status"),
            "holiday_name": snapshot.get("holiday_name"),
            "confirmed_events": [{"name": event["name"], "category": event["category"]} for event in self._local_events(snapshot)],
        }
        return render_prompt("calendar.events", date=target_date.isoformat(), snapshot=json.dumps(safe_snapshot, ensure_ascii=False))

    def _holiday_prompt(self, year: int) -> str:
        return render_prompt("calendar.holidays", year=int(year))

    @staticmethod
    def _extract_json_object(raw: Any) -> Optional[dict[str, Any]]:
        text = str(raw or "").strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
            text = re.sub(r"\s*```$", "", text).strip()
        candidates = [text]
        match = re.search(r"\{[\s\S]*\}", text)
        if match and match.group(0) != text:
            candidates.append(match.group(0))
        for candidate in candidates:
            try:
                obj = json.loads(candidate)
            except Exception:
                continue
            if isinstance(obj, dict):
                return obj
        return None

    @staticmethod
    def _is_retryable_web_error(exc: Exception) -> bool:
        text = str(exc or "").casefold()
        return any(marker in text for marker in _RETRYABLE_WEB_ERROR_MARKERS)

    async def _call_web_model(self, prompt: str, cfg: dict[str, Any]) -> tuple[Optional[dict[str, Any]], bool]:
        if self.aisvc is None or not bool(getattr(self.aisvc, "chat_ready", False)):
            return None, False
        call = getattr(self.aisvc, "calendar_web_query", None)
        if not callable(call):
            return None, False
        timeout_seconds = max(10, min(self._safe_int(cfg.get("web_timeout_seconds"), 45), 120))
        try:
            raw = await call(prompt, timeout_seconds=timeout_seconds)
            obj = self._extract_json_object(raw)
            if obj is None:
                raise RuntimeError("calendar web response was not a JSON object")
            return obj, True
        except Exception as e:
            self._warning(f"daily calendar web query failed: {str(e)[:180]}")
        return None, False

    def _validate_web_events(self, raw: dict[str, Any], target_date: date, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        if str(raw.get("date") or "").strip() != target_date.isoformat():
            return []
        local_names = {
            category: {self._event_key(name) for name in self._as_string_list(snapshot.get(field))}
            for category, field in (
                ("traditional", "lunar_festivals"),
                ("solar_festival", "solar_festivals"),
                ("solar_term", "solar_terms"),
            )
        }
        out: list[dict[str, Any]] = []
        raw_events = raw.get("events")
        if not isinstance(raw_events, list):
            return out
        for item in raw_events[:12]:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            category = str(item.get("category") or "").strip().lower()
            fact = str(item.get("fact") or "").strip()[:120]
            tone = "solemn" if str(item.get("tone") or "").strip().lower() == "solemn" else "light"
            source_url = str(item.get("source_url") or "").strip()
            source_title = str(item.get("source_title") or "").strip()[:160]
            importance = item.get("importance")
            relevance = item.get("campus_relevance")
            reason = str(item.get("selection_reason") or "").strip()[:240]
            date_evidence = str(item.get("date_evidence") or "").strip()[:240]
            if not name or category not in _VALID_WEB_CATEGORIES:
                continue
            key = self._event_key(name)
            if category in local_names:
                if key not in local_names[category]:
                    continue
            elif not self._is_official_source(source_url) or not fact or not date_evidence:
                continue
            if category not in {"traditional", "solar_term"}:
                if type(importance) is not int or importance not in range(4):
                    continue
                if type(relevance) is not int or relevance not in range(3) or not reason:
                    continue
                if importance < 3 and not (importance == 2 and relevance == 2):
                    continue
                if category == "historical_anniversary" and importance != 3:
                    continue
            else:
                importance, relevance = 2, 1
            if category == "national_memorial":
                tone = "solemn"
            out.append(
                {
                    "name": name,
                    "canonical_name": str(item.get("canonical_name") or name).strip()[:120] or name,
                    "category": category,
                    "fact": fact or "联网核验的重要日子",
                    "tone": tone,
                    "source": "web",
                    "source_title": source_title,
                    "source_url": source_url,
                    "importance": importance,
                    "campus_relevance": relevance,
                    "selection_reason": reason,
                    "date_evidence": date_evidence,
                }
            )
        return out

    def _merge_events(self, local_events: list[dict[str, Any]], web_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        seen: dict[str, dict[str, Any]] = {}
        for event in [*local_events, *web_events]:
            key = self._event_key(event.get("canonical_name") or event.get("name"))
            if str(event.get("category") or "") == "official":
                key = re.sub(r"(?:假期|放假安排|放假|休假)$", "", key)
            if not key:
                continue
            old = seen.get(key)
            if old is not None:
                # 本地已确认“是什么节”，联网结果通常补充“怎么放假”；合并而不是重复占名额。
                if str(old.get("source") or "") == "local" and str(event.get("source") or "") == "web":
                    fact = str(event.get("fact") or "").strip()
                    if fact:
                        old["fact"] = fact
                    old["source"] = "web"
                    old["source_title"] = event.get("source_title", "")
                    old["source_url"] = event.get("source_url", "")
                elif self._safe_int(event.get("importance"), 0) > self._safe_int(old.get("importance"), 0):
                    old.update(event)
                continue
            seen[key] = event
            out.append(event)
        rank = {"national_memorial": 0, "historical_anniversary": 0, "official": 1, "traditional": 2, "solar_festival": 2, "solar_term": 3}
        out.sort(key=lambda x: (
            -self._safe_int(x.get("importance"), 3 if x.get("category") in {"national_memorial", "official"} else 2),
            rank.get(str(x.get("category") or ""), 9),
            -self._safe_int(x.get("campus_relevance"), 1),
        ))
        return out[:3]

    @staticmethod
    def _sanitize_generated_text(text: Any) -> str:
        out = _CQ_RE.sub("", str(text or ""))
        out = _URL_RE.sub("", out)
        out = re.sub(r"\n{3,}", "\n\n", out).strip()
        if len(out) > 180:
            out = out[:180].rstrip("，,。；;、 ") + "。"
        return out

    def _base_message(self, target_date: date, snapshot: dict[str, Any], events: list[dict[str, Any]]) -> str:
        lines = [f"📅 今日重要日｜{target_date.year}年{target_date.month}月{target_date.day}日 星期{snapshot.get('weekday', '')}"]
        calendar_bits = [str(snapshot.get("lunar") or "").strip(), *self._as_string_list(snapshot.get("solar_terms"))]
        if calendar_bits:
            lines.append(" · ".join(x for x in calendar_bits if x))
        for event in events:
            fact = str(event.get("fact") or "").strip()
            lines.append(f"• {event.get('name')}" + (f"：{fact}" if fact else ""))
        return "\n".join(lines).strip()

    async def _render_message(self, target_date: date, snapshot: dict[str, Any], events: list[dict[str, Any]]) -> str:
        base = self._base_message(target_date, snapshot, events)
        if self.aisvc is None or not bool(getattr(self.aisvc, "deepseek_task_ready", False)):
            return base
        task_text = getattr(self.aisvc, "deepseek_task_text", None)
        if not callable(task_text):
            return base
        factual_events = [{"name": x.get("name"), "fact": x.get("fact"), "category": x.get("category"), "tone": x.get("tone")} for x in events]
        try:
            prompt = render_prompt("calendar.render_note", date=target_date.isoformat(), snapshot=json.dumps(snapshot, ensure_ascii=False), events=json.dumps(factual_events, ensure_ascii=False))
        except PromptConfigError:
            return base
        try:
            note = self._sanitize_generated_text(await task_text(prompt))
        except Exception as e:
            self._warning(f"daily calendar DeepSeek render failed: {str(e)[:180]}")
            note = ""
        return base if not note else base + "\n" + note

    def _result_from_cache(self, target_date: date) -> Optional[CalendarResult]:
        raw = self._json_load(self._daily_cache_path(target_date), {})
        if not isinstance(raw, dict) or str(raw.get("date") or "") != target_date.isoformat():
            return None
        if self._safe_int(raw.get("cache_version"), 0) != _DAILY_CACHE_VERSION:
            return None
        snapshot = raw.get("snapshot")
        events = raw.get("events")
        if not isinstance(snapshot, dict) or not isinstance(events, list):
            return None
        return CalendarResult(
            target_date=target_date,
            snapshot=snapshot,
            events=[x for x in events if isinstance(x, dict)],
            message=str(raw.get("message") or ""),
            special=bool(raw.get("special")),
            web_used=bool(raw.get("web_used")),
        )

    def _save_result(self, result: CalendarResult) -> None:
        self._json_save(
            self._daily_cache_path(result.target_date),
            {
                "cache_version": _DAILY_CACHE_VERSION,
                "date": result.target_date.isoformat(),
                "snapshot": result.snapshot,
                "events": result.events,
                "message": result.message,
                "special": result.special,
                "web_used": result.web_used,
                "generated_at": datetime.now(_TZ).isoformat(timespec="seconds"),
            },
        )

    async def generate_for_date(
        self,
        target_date: date,
        *,
        cfg: Optional[dict[str, Any]] = None,
        force_refresh: bool = False,
        refresh_holiday_schedule: bool = False,
    ) -> CalendarResult:
        use_cfg = dict(cfg or {})
        if refresh_holiday_schedule:
            await self._maybe_refresh_holiday_schedule(target_date, use_cfg)
        if not force_refresh:
            cached = self._result_from_cache(target_date)
            if cached is not None:
                return cached
        snapshot = self._build_snapshot(target_date)
        local_events = self._local_events(snapshot)
        try:
            web_prompt = self._calendar_prompt(target_date, snapshot)
        except PromptConfigError:
            web_raw, web_used = None, False
        else:
            web_raw, web_used = await self._call_web_model(web_prompt, use_cfg)
        web_events = self._validate_web_events(web_raw, target_date, snapshot) if web_raw else []
        events = self._merge_events(local_events, web_events)
        special = bool(events)
        message = await self._render_message(target_date, snapshot, events) if special else ""
        result = CalendarResult(target_date, snapshot, events, message, special, web_used)
        # 联网失败的本地兜底不固化为全天结果，恢复后仍可补查遗漏的重要日。
        if web_used and web_raw and web_raw.get("date") == target_date.isoformat() and isinstance(web_raw.get("events"), list):
            self._save_result(result)
        return result

    def _should_refresh_holiday_schedule(self, target_date: date, state: dict[str, Any]) -> bool:
        year_key = str(target_date.year)
        recent = state.get("holiday_refresh", {}).get(year_key, {})
        last = str(recent.get("last_attempt_date") or "") if isinstance(recent, dict) else ""
        try:
            last_date = date.fromisoformat(last)
        except Exception:
            last_date = None
        interval_days = 7 if target_date.month in {12, 1} else 30
        if last_date is not None and (target_date - last_date).days < interval_days:
            return False
        schedule = self._load_holiday_schedule(target_date.year)
        return not schedule.get("days") or last_date is None or (target_date - last_date).days >= interval_days

    def _validate_holiday_schedule(self, raw: dict[str, Any], year: int) -> Optional[dict[str, Any]]:
        if int(raw.get("year") or 0) != int(year):
            return None
        source_url = str(raw.get("source_url") or "").strip()
        if not self._is_official_source(source_url):
            return None
        days: dict[str, dict[str, str]] = {}
        raw_days = raw.get("days")
        if not isinstance(raw_days, list):
            return None
        for item in raw_days:
            if not isinstance(item, dict):
                continue
            raw_date = str(item.get("date") or "").strip()
            status = str(item.get("status") or "").strip().lower()
            name = str(item.get("name") or "").strip()[:80]
            try:
                parsed = date.fromisoformat(raw_date)
            except ValueError:
                continue
            if parsed.year != year or status not in {"holiday", "workday"}:
                continue
            days[parsed.isoformat()] = {"status": status, "name": name}
        if not days:
            return None
        return {
            "year": int(year),
            "source_title": str(raw.get("source_title") or "").strip()[:160],
            "source_url": source_url,
            "days": days,
            "updated_at": datetime.now(_TZ).isoformat(timespec="seconds"),
        }

    async def _maybe_refresh_holiday_schedule(self, target_date: date, cfg: dict[str, Any]) -> None:
        state = self._load_state()
        if not self._should_refresh_holiday_schedule(target_date, state):
            return
        state.setdefault("holiday_refresh", {})[str(target_date.year)] = {"last_attempt_date": target_date.isoformat()}
        self._save_state(state)
        try:
            holiday_prompt = self._holiday_prompt(target_date.year)
        except PromptConfigError:
            return
        raw, _used = await self._call_web_model(holiday_prompt, cfg)
        if raw is None:
            return
        candidate = self._validate_holiday_schedule(raw, target_date.year)
        if candidate is None:
            self._warning(f"daily calendar holiday refresh rejected: year={target_date.year}")
            return
        existing = self._load_holiday_schedule(target_date.year)
        if existing.get("days") and existing.get("days") != candidate.get("days"):
            self._json_save(self._pending_holiday_path(target_date.year), candidate)
            self._warning(f"daily calendar holiday refresh pending review: year={target_date.year}")
            return
        self._json_save(self._holiday_path(target_date.year), candidate)
        self._info(f"daily calendar holiday schedule updated: year={target_date.year} days={len(candidate['days'])}")

    @staticmethod
    def _response_ok(response: Any) -> bool:
        if not isinstance(response, dict):
            return False
        if str(response.get("status") or "").lower() == "ok":
            return True
        try:
            return int(response.get("retcode")) == 0
        except Exception:
            return False

    def _empty_day_message(self, target_date: date, snapshot: dict[str, Any]) -> str:
        bits = [str(snapshot.get("lunar") or "").strip(), *self._as_string_list(snapshot.get("solar_terms"))]
        tail = " · ".join(x for x in bits if x)
        heading = f"📅 今日历｜{target_date.year}年{target_date.month}月{target_date.day}日 星期{snapshot.get('weekday', '')}"
        return heading + (("\n" + tail) if tail else "") + "\n今日暂无需特别提醒的重要日子。"

    async def _run_group_once(self, api, group_id: int, cfg: dict[str, Any], now: datetime) -> None:
        target_date = now.date()
        await self._maybe_refresh_holiday_schedule(target_date, cfg)
        result = await self.generate_for_date(target_date, cfg=cfg)
        if result.special:
            text = result.message
        elif bool(cfg.get("send_when_empty", True)):
            text = self._empty_day_message(target_date, result.snapshot)
        else:
            return
        state = self._load_state()
        groups = state.setdefault("groups", {})
        entry = groups.setdefault(str(group_id), {})
        entry[target_date.isoformat()] = {"status": "prepared", "message": text}
        self._save_state(state)
        if bool(cfg.get("dry_run", False)):
            entry[target_date.isoformat()]["status"] = "dry_run"
            self._save_state(state)
            self._info(f"daily calendar dry run: group={group_id} date={target_date.isoformat()} special={result.special}")
            return
        try:
            entry[target_date.isoformat()]["status"] = "sending"
            self._save_state(state)
            response = await api.send_group_msg(int(group_id), text)
        except Exception as e:
            entry[target_date.isoformat()]["status"] = "failed"
            entry[target_date.isoformat()]["error"] = str(e)[:180]
            self._save_state(state)
            self._warning(f"daily calendar send failed: group={group_id} date={target_date.isoformat()} err={e}")
            return
        if response is None:
            entry[target_date.isoformat()]["status"] = "uncertain"
        elif self._response_ok(response):
            entry[target_date.isoformat()]["status"] = "sent"
        else:
            entry[target_date.isoformat()]["status"] = "failed"
        self._save_state(state)

    def _is_send_due(self, group_id: int, cfg: dict[str, Any], now: datetime, state: dict[str, Any]) -> bool:
        send_hour, send_minute = self._parse_hhmm(cfg.get("send_time"), 9, 0)
        limit_hour, limit_minute = self._parse_hhmm(cfg.get("send_catch_up_until"), 12, 0)
        minute_of_day = now.hour * 60 + now.minute
        if minute_of_day < send_hour * 60 + send_minute or minute_of_day > limit_hour * 60 + limit_minute:
            return False
        item = state.get("groups", {}).get(str(group_id), {}).get(now.date().isoformat(), {})
        if not isinstance(item, dict):
            return True
        return str(item.get("status") or "") not in {"prepared", "sending", "sent", "uncertain", "failed", "dry_run"}

    async def scheduler_loop(self, api) -> None:
        while True:
            try:
                now = datetime.now(_TZ)
                config = self._load_config()
                groups = config.get("groups") if isinstance(config, dict) else {}
                if isinstance(groups, dict):
                    state = self._load_state()
                    for group_id_text, raw_cfg in groups.items():
                        try:
                            group_id = int(group_id_text)
                        except Exception:
                            continue
                        cfg = raw_cfg if isinstance(raw_cfg, dict) else {}
                        if group_id <= 0 or not bool(cfg.get("enabled", False)):
                            continue
                        if not self._is_send_due(group_id, cfg, now, state):
                            continue
                        lock = self._group_locks.setdefault(group_id, asyncio.Lock())
                        if lock.locked():
                            continue
                        async with lock:
                            await self._run_group_once(api, group_id, cfg, now)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._warning(f"daily calendar scheduler error: {e}")
            await asyncio.sleep(30)
