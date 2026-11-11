from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import aiohttp

from cooper_bot.core import config
from cooper_bot.modules.ai.feishu_secrets import load_feishu_credentials


TZ = ZoneInfo("Asia/Shanghai")
FIELDS = ["负责人", "参与人", "完成状态", "立项日期", "结项日期"]


def _people(value):
    if value is None:
        return set()
    if not isinstance(value, list) or any(not isinstance(p, dict) or not p.get("name") for p in value):
        raise ValueError("invalid topic people")
    return {p["name"].strip() for p in value}


def _stamp(value):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("invalid topic date")
    return datetime.fromtimestamp(value / 1000, TZ)


def render_reports(records, now, members, ministers):
    now = now.astimezone(TZ)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    recent_start = (now - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    established, completed, undated = set(), set(), set()
    counts = dict.fromkeys(ministers, 0)
    for record in records:
        fields = record["fields"]
        owners = (_people(fields.get("负责人")) | _people(fields.get("参与人"))) & set(members)
        project_date, close_date = _stamp(fields.get("立项日期")), _stamp(fields.get("结项日期"))
        if project_date is not None and month_start <= project_date <= now:
            established.update(owners)
        if fields.get("完成状态") == "已完成":
            if close_date is not None and month_start <= close_date <= now:
                completed.update(owners)
            elif close_date is None and now.day >= 15:
                undated.update(owners)
        if owners and project_date is None:
            undated.update(owners)
        created = _stamp(record.get("created_time"))
        creator = record.get("created_by")
        if created is None or not isinstance(creator, dict) or not creator.get("name"):
            raise ValueError("missing topic creation metadata")
        if recent_start <= created <= now and creator["name"].strip() in counts:
            counts[creator["name"].strip()] += 1

    lines = ["📢 【喻家山下·每日选题进度播报】",
             f"🗓️ 日期：{now.year}年{now.month}月{now.day}日",
             "⚠️ 当月尚未立项成员：" + ("、".join(n for n in members if n not in established) or "无")]
    if now.day >= 15:
        lines.append("⏳ 本月尚未完成选题成员：" + ("、".join(n for n in members if n not in completed) or "无"))
    if undated:
        lines.append("⚠️ 部分选题缺少立项日期或已完成但缺少结项日期，未计入对应统计："
                     + "、".join(n for n in members if n in undated))
    lines.append("💡 请各位成员关注选题进度，合理安排时间，确保按时推进。")
    minister_text = ("📊 【部长选题创建统计】\n"
                     f"🗓️ {recent_start:%Y年%m月%d日}—{now:%Y年%m月%d日 %H:%M}（北京时间）\n"
                     + "\n".join(f"{name}：{count}篇" for name, count in counts.items())
                     + "\n💪 各位部长继续加油，保持选题产出节奏！")
    return ["\n".join(lines), minister_text]


class TopicReminderService:
    def __init__(self, log, *, config_path=None, state_path=None):
        self.log = log
        self.config_path = Path(config_path or config.PRIVATE_CONFIG_DIR / "calendar" / "topic_reminder_config.json")
        self.state_path = Path(state_path or config.STATE_DIR / "topic_reminder" / "state.json")
        self._lock = asyncio.Lock()

    @staticmethod
    def _load(path):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        if not isinstance(value, dict):
            raise ValueError("invalid topic reminder JSON")
        return value

    def _save(self, state):
        state = dict(sorted(state.items())[-31:])
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp.replace(self.state_path)

    @staticmethod
    async def _request(session, path, body, token=None):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        async with session.post("https://open.feishu.cn/open-apis/" + path,
                                json=body, headers=headers, allow_redirects=False) as response:
            response.raise_for_status()
            payload = await response.json()
        if not isinstance(payload, dict) or payload.get("code") != 0:
            raise RuntimeError("feishu topic request failed")
        return payload

    async def fetch_records(self, cfg):
        app, table = cfg.get("app_token", ""), cfg.get("table_id", "")
        if not all(isinstance(v, str) and re.fullmatch(r"[A-Za-z0-9]+", v) for v in (app, table)):
            raise ValueError("invalid topic table configuration")
        credentials = load_feishu_credentials(config.FEISHU_CREDENTIALS_PATH)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            auth = await self._request(session, "auth/v3/tenant_access_token/internal", {
                "app_id": credentials["APP_ID"], "app_secret": credentials["APP_SECRET"]})
            token = auth.get("tenant_access_token")
            if not isinstance(token, str) or not token:
                raise RuntimeError("missing feishu topic access token")
            records, pages = {}, set()
            page_token = None
            while True:
                path = f"bitable/v1/apps/{app}/tables/{table}/records/search?page_size=500"
                if page_token:
                    path += "&page_token=" + quote(page_token, safe="")
                payload = await self._request(session, path, {
                    "field_names": FIELDS, "automatic_fields": True}, token)
                data = payload.get("data")
                if not isinstance(data, dict) or not isinstance(data.get("items"), list):
                    raise ValueError("invalid topic records response")
                for record in data["items"]:
                    if (not isinstance(record, dict) or not isinstance(record.get("record_id"), str)
                            or not isinstance(record.get("fields"), dict)):
                        raise ValueError("invalid topic record")
                    records[record["record_id"]] = record
                if data.get("has_more") is False:
                    return list(records.values())
                page_token = data.get("page_token")
                if data.get("has_more") is not True or not isinstance(page_token, str) or not page_token or page_token in pages:
                    raise ValueError("incomplete topic pagination")
                pages.add(page_token)

    async def generate(self, cfg, now):
        rosters = []
        for key in ("members", "ministers"):
            names = cfg.get(key)
            if not isinstance(names, list) or not names or any(not isinstance(n, str) or not n.strip() for n in names):
                raise ValueError("invalid topic roster")
            rosters.append(list(dict.fromkeys(n.strip() for n in names)))
        async with asyncio.timeout(180):
            records = await self.fetch_records(cfg)
        return render_reports(records, now, *rosters)

    async def check_once(self, api, now):
        now = now.astimezone(TZ)
        async with self._lock:
            cfg = self._load(self.config_path)
            if cfg.get("enabled") is not True or (now.hour, now.minute) < (22, 50):
                return
            if cfg.get("admin_only", True) is True:
                targets = [("private", uid) for uid in sorted(config.ADMIN_USERS) if uid > 0]
            elif cfg.get("admin_only") is False and type(cfg.get("group_id")) is int and cfg["group_id"] > 0:
                targets = [("group", cfg["group_id"])]
            else:
                raise ValueError("invalid topic reminder target")
            if not targets:
                raise ValueError("missing topic reminder admins")
            state = self._load(self.state_path)
            day = state.get(now.date().isoformat())
            if day is None:
                day = {"messages": await self.generate(cfg, now), "deliveries": {}}
                state[now.date().isoformat()] = day
                self._save(state)
            for scene, target in targets:
                for part, message in enumerate(day["messages"]):
                    key = f"{scene}:{target}:{part}"
                    if day["deliveries"].get(key) in {"sending", "sent", "uncertain"}:
                        continue
                    day["deliveries"][key] = "sending"
                    self._save(state)
                    try:
                        if scene == "private":
                            result = await api.send_private_msg(target, message)
                        else:
                            result = await api.send_group_msg(target, message)
                    except Exception:
                        day["deliveries"][key] = "uncertain"
                        self._save(state)
                        raise
                    if (isinstance(result, dict) and result.get("status") == "ok" and result.get("retcode") == 0
                            and isinstance(result.get("data"), dict) and result["data"].get("message_id")):
                        status = "sent"
                    elif (isinstance(result, dict) and result.get("status") == "failed"
                          and type(result.get("retcode")) is int and result["retcode"] != 0):
                        status = "failed"
                    else:
                        status = "uncertain"
                    day["deliveries"][key] = status
                    self._save(state)
                    if status != "sent":
                        self.log.warning("选题提醒：消息发送未确认，请检查发送状态")
                        return

    async def scheduler_loop(self, api):
        self.log.info("选题提醒定时任务已启动：每日北京时间22:50，按本地配置选择接收目标")
        while True:
            delay = 30
            try:
                await self.check_once(api, datetime.now(TZ))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning(f"选题提醒暂未完成：{type(exc).__name__}")
                delay = 300
            await asyncio.sleep(delay)
