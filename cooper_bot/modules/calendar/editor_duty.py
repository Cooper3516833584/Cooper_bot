from __future__ import annotations

import asyncio
import html
import json
import re
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from http.cookies import SimpleCookie
from pathlib import Path
from zoneinfo import ZoneInfo

from cooper_bot.core.config import CONFIG_DIR, STATE_DIR, TIMEZONE


GROUP_ID = 1087250737
PUBLISHER_ID = 1428941369
_TZ = ZoneInfo(TIMEZONE)
_SLOTS = {"morning": (9, 0, "早安"), "evening": (22, 55, "晚安")}
_CARD = re.compile(r"(?:周|星期)\s*([一二三四五六日天])\s*([SD]+)(?![A-Za-z])", re.I)
_GREETING_TITLE = re.compile(r"^\s*#(早安华中大|午安华中大|晚安华中大)#")
_EDITOR_SIGNATURE = re.compile(r"今日编辑[ \t\u3000]*[：:][ \t\u3000]*[^\s（）()]")


def editor_on_duty(card: str, day: date, odd_monday: date) -> bool:
    match = _CARD.search(card)
    if not match:
        return False
    weekday = "日" if match[1] == "天" else match[1]
    parity = "D" if ((day - odd_monday).days // 7) % 2 == 0 else "S"
    return weekday == "一二三四五六日"[day.weekday()] and parity in match[2].upper()


def _text(message) -> str:
    if isinstance(message, str):
        return html.unescape(re.sub(r"\[CQ:[^\]]*\]", "", message))
    if not isinstance(message, list):
        return ""
    return "\n".join(str(seg.get("data", {}).get("text", "")) for seg in message
                     if isinstance(seg, dict) and seg.get("type") == "text")


def greeting_kind(text: str) -> set[str]:
    # 按实际投稿/说说的栏目标签和署名识别，午安是迟发的早安。
    title = _GREETING_TITLE.match(text)
    if not title or not _EDITOR_SIGNATURE.search(text):
        return set()
    return {"evening" if title[1] == "晚安华中大" else "morning"}


def greeting_day(stamp: int, slot: str) -> date:
    local = datetime.fromtimestamp(stamp, _TZ)
    # 16:00 前收到的晚安归前一晚，不能抵消当天 22:55 的值班提醒。
    return local.date() - timedelta(days=1) if slot == "evening" and local.hour < 16 else local.date()


def _data(response):
    if isinstance(response, dict) and response.get("status") == "ok" and response.get("retcode", 0) == 0:
        return response.get("data")
    return None


class EditorDutyService:
    def __init__(self, log, *, config_path=None, state_path=None):
        self.log = log
        self.config_path = Path(config_path or CONFIG_DIR / "calendar" / "editor_duty_config.json")
        self.state_path = Path(state_path or STATE_DIR / "editor_duty" / "state.json")
        self._state = self._load_json(self.state_path, {"days": {}})
        if not isinstance(self._state.get("days"), dict):
            raise ValueError("invalid editor duty state")
        self._lock = asyncio.Lock()
        self._forwards = {}

    @staticmethod
    def _load_json(path, default):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return default
        if not isinstance(value, dict):
            raise ValueError("invalid editor duty JSON")
        return value

    def _config(self):
        return self._load_json(self.config_path, {})

    def _day(self, day):
        return self._state["days"].setdefault(day.isoformat(), {})

    def _save(self):
        self._state["days"] = dict(sorted(self._state["days"].items())[-8:])
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self._state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp.replace(self.state_path)

    async def _forward_kinds(self, api, evt):
        message = evt.get("message", [])
        ids = []
        if isinstance(message, list):
            ids = [str(seg.get("data", {}).get("id", "")) for seg in message
                   if isinstance(seg, dict) and seg.get("type") == "forward"]
        elif isinstance(message, str):
            ids = re.findall(r"\[CQ:forward,id=([^,\]]+)", message)
        kinds = set()
        complete = True
        for forward_id in ids[:5]:
            if not forward_id:
                complete = False
                continue
            if forward_id not in self._forwards:
                payload = _data(await api.call("get_forward_msg", {"message_id": forward_id}))
                nodes = payload.get("messages") if isinstance(payload, dict) else payload
                if not isinstance(nodes, list) or not nodes:
                    complete = False
                    continue
                result = set()
                node_complete = True
                for node in nodes:
                    if isinstance(node, dict):
                        content = node.get("content", node.get("message", []))
                        result.update(greeting_kind(_text(content)))
                        if isinstance(content, list):
                            if any(isinstance(seg, dict) and seg.get("type") == "forward" for seg in content):
                                node_complete = False
                if len(self._forwards) >= 500:
                    self._forwards.clear()
                if node_complete:
                    self._forwards[forward_id] = result
                else:
                    complete = False
                    continue
            kinds.update(self._forwards[forward_id])
        return kinds, complete and len(ids) <= 5

    async def _record(self, api, evt, day):
        kinds, complete = await self._forward_kinds(api, evt)
        uid = int(evt.get("user_id") or evt.get("sender", {}).get("user_id") or 0)
        stamp = int(evt.get("time") or 0)
        entry = self._day(day)
        changed = False
        for slot in kinds:
            if greeting_day(stamp, slot) != day:
                continue
            previous = entry.get(slot, {})
            if stamp >= previous.get("time", 0) and uid > 0:
                entry[slot] = {"user_id": uid, "time": stamp}
                changed = True
            if slot == "morning" and uid > 0 and uid != PUBLISHER_ID:
                editor = entry.get("morning_editor", {})
                if stamp >= editor.get("time", 0):
                    entry["morning_editor"] = {"user_id": uid, "time": stamp}
                    changed = True
        if changed:
            self._save()
        return complete

    async def observe_event(self, api, evt):
        if (evt.get("post_type") != "message" or evt.get("message_type") != "group"
                or str(evt.get("group_id")) != str(GROUP_ID) or not self._config().get("enabled", False)):
            return
        day = datetime.fromtimestamp(int(evt.get("time") or 0), _TZ).date()
        if day != datetime.now(_TZ).date():
            return
        async with self._lock:
            await self._record(api, evt, day)

    async def _history(self, api, day):
        cursor = 0
        seen = set()
        for _ in range(20):
            params = {"group_id": GROUP_ID, "count": 100, "reverseOrder": True}
            if cursor:
                params["message_seq"] = cursor
            payload = _data(await api.call("get_group_msg_history", params))
            messages = payload.get("messages") if isinstance(payload, dict) else None
            if not isinstance(messages, list):
                return False
            if not messages:
                return True
            older = False
            complete = True
            for evt in sorted(messages, key=lambda row: int(row.get("time") or 0)):
                stamp = int(evt.get("time") or 0)
                if not stamp:
                    return False
                event_day = datetime.fromtimestamp(stamp, _TZ).date()
                if event_day < day:
                    older = True
                elif event_day == day:
                    complete = await self._record(api, evt, day) and complete
            if not complete:
                return False
            if older:
                return True
            earliest = min(messages, key=lambda row: int(row.get("time") or 0))
            cursor = earliest.get("message_id") or earliest.get("message_seq")
            if not cursor or str(cursor) in seen:
                return False
            seen.add(str(cursor))
        return False

    @staticmethod
    def _fetch_qzone(cookie, day, slot):
        jar = SimpleCookie()
        jar.load(cookie)
        key = jar.get("p_skey") or jar.get("skey")
        if key is None or not key.value:
            return None
        token = 5381
        for char in key.value:
            token += (token << 5) + ord(char)
        params = {"uin": PUBLISHER_ID, "ftype": 0, "sort": 0, "pos": 0, "num": 20,
                  "replynum": 0, "g_tk": token & 0x7fffffff, "format": "json"}
        url = "https://user.qzone.qq.com/proxy/domain/taotao.qq.com/cgi-bin/emotion_cgi_msglist_v6?"
        req = urllib.request.Request(url + urllib.parse.urlencode(params), headers={
            "Cookie": cookie, "Referer": f"https://user.qzone.qq.com/{PUBLISHER_ID}",
            "User-Agent": "Mozilla/5.0",
        })
        with urllib.request.urlopen(req, timeout=12) as response:
            raw = response.read(2 * 1024 * 1024).decode("utf-8")
        raw = raw.strip()
        if not raw.startswith("{"):
            start, end = raw.find("("), raw.rfind(")")
            raw = raw[start + 1:end] if start >= 0 and end > start else ""
        payload = json.loads(raw)
        if payload.get("code") != 0:
            return None
        items = payload.get("msglist")
        if items is None and payload.get("total") == 0:
            return False
        if not isinstance(items, list):
            return None
        older = False
        for item in items:
            stamp = int(item.get("created_time") or 0)
            if not stamp:
                return None
            item_day = datetime.fromtimestamp(stamp, _TZ).date()
            older |= item_day < day
            if greeting_day(stamp, slot) == day and slot in greeting_kind(str(item.get("content") or "")):
                return True
        return False if older or len(items) < 20 else None

    async def _published(self, api, day, slot):
        try:
            payload = _data(await api.call("get_cookies", {"domain": "qzone.qq.com"}))
            cookie = payload.get("cookies") if isinstance(payload, dict) else None
            if not cookie:
                return None
            return await asyncio.to_thread(self._fetch_qzone, cookie, day, slot)
        except Exception:
            return None

    async def check_once(self, api, now):
        cfg = self._config()
        if not cfg.get("enabled", False):
            return
        now = now.astimezone(_TZ)
        slots = [slot for slot, (hour, minute, _) in _SLOTS.items() if (now.hour, now.minute) == (hour, minute)]
        if not slots:
            return
        async with self._lock:
            day = now.date()
            entry = self._day(day)
            for slot in slots:
                checks = entry.setdefault("checks", {})
                if slot in checks:
                    continue
                history_ok = await self._history(api, day)
                if entry.get(slot):
                    checks[slot] = "detected_in_group"
                    self._save()
                    continue
                published = await self._published(api, day, slot)
                if published is True:
                    checks[slot] = "detected_in_qzone"
                    self._save()
                    continue
                if not history_ok or (published is None and cfg.get("qzone_unavailable_policy") != "group_only"):
                    checks[slot] = "unknown"
                    self._save()
                    self.log.warning("编辑值班检查未完成：群记录或空间读取失败，本次未提醒")
                    continue
                if published is None:
                    self.log.warning("编辑值班检查：空间暂不可读，本次按群内稿件判断")
                members = _data(await api.get_group_member_list(GROUP_ID))
                if not isinstance(members, list):
                    checks[slot] = "unknown"
                    self._save()
                    self.log.warning("编辑值班检查未完成：群成员读取失败")
                    continue
                substitute = entry.get("morning_editor", entry.get("morning", {})).get("user_id") if slot == "evening" else None
                excluded = {int(uid) for uid in cfg.get("excluded_editor_ids", [])}
                if substitute in excluded:
                    substitute = None
                ids = {int(member["user_id"]) for member in members if member.get("user_id")
                       and int(member["user_id"]) not in excluded
                       and (int(member["user_id"]) == substitute if substitute and substitute != PUBLISHER_ID
                            else editor_on_duty(str(member.get("card") or ""), day,
                                                date.fromisoformat(cfg["odd_week_monday"])))}
                if not ids:
                    checks[slot] = "no_editor"
                    self._save()
                    self.log.warning("编辑值班检查未完成：未找到当天编辑群名片")
                    continue
                checks[slot] = "sending"
                self._save()
                mentions = " ".join(f"[CQ:at,qq={uid}]" for uid in sorted(ids))
                response = await api.send_group_msg(GROUP_ID, f"未检测到{_SLOTS[slot][2]}，请{mentions}注意值班")
                checks[slot] = "sent" if _data(response) is not None else "uncertain"
                self._save()

    async def scheduler_loop(self, api):
        while True:
            try:
                await self.check_once(api, datetime.now(_TZ))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning(f"编辑值班检查异常：{type(exc).__name__}")
            await asyncio.sleep(1)
