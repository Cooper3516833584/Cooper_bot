from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from cooper_bot.core import config


GROUP_ID = 798977395
TZ = ZoneInfo("Asia/Shanghai")
WEEK = 7 * 86400


def report_end(now):
    now = now.astimezone(TZ)
    return int((now.replace(hour=22, minute=0, second=0, microsecond=0)
                + timedelta(days=6 - now.weekday())).timestamp())


def response_data(response):
    if (isinstance(response, dict) and response.get("status") == "ok"
            and type(response.get("retcode")) is int and response["retcode"] == 0):
        return response.get("data")
    return None


class GroupSpeechService:
    """Persist message identities only; all content types count as one message."""

    group_id = GROUP_ID

    def __init__(self, log, *, db_path=None, admins=None):
        self.log = log
        self.db_path = Path(db_path) if db_path is not None else config.DATABASES_DIR / "group_speech.sqlite3"
        self.admins = admins
        self._db_lock = asyncio.Lock()
        self._check_lock = asyncio.Lock()
        self._next_sync = 0

    async def _db(self, operation):
        async with self._db_lock:
            return await asyncio.to_thread(self._db_sync, operation)

    def _db_sync(self, operation):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS messages (
                    message_id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, ts INTEGER NOT NULL);
                CREATE INDEX IF NOT EXISTS messages_ts ON messages(ts, user_id);
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS reports (end_ts INTEGER PRIMARY KEY, chunks TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS delivered (
                    end_ts INTEGER, admin INTEGER, part INTEGER,
                    PRIMARY KEY(end_ts, admin, part));
            """)
            with conn:
                return operation(conn)
        finally:
            conn.close()

    @staticmethod
    def _row(evt):
        try:
            if int(evt.get("group_id") or 0) != GROUP_ID:
                return None
            mid = evt.get("message_id")
            uid, stamp = evt.get("user_id"), evt.get("time")
            if any(isinstance(v, bool) for v in (mid, uid, stamp)):
                return None
            if not isinstance(mid, (str, int)) or not str(mid).strip():
                return None
            uid, stamp = int(uid), int(stamp)
            return (str(mid), uid, stamp) if uid > 0 and stamp > 0 else None
        except (TypeError, ValueError):
            return None

    async def _insert(self, rows):
        if rows:
            await self._db(lambda c: c.executemany(
                "INSERT OR IGNORE INTO messages VALUES(?,?,?)", rows))

    async def observe_event(self, evt):
        if evt.get("post_type") not in ("message", "message_sent") or evt.get("message_type") != "group":
            return
        row = self._row(evt)
        if row:
            await self._insert([row])

    async def _backfill(self, api, lower, upper):
        cursor = None
        seen = set()
        for _ in range(2000):
            params = {"group_id": GROUP_ID, "count": 100, "reverseOrder": True}
            if cursor is not None:
                params["message_seq"] = cursor
            payload = response_data(await api.call("get_group_msg_history", params))
            messages = payload.get("messages") if isinstance(payload, dict) else None
            if not isinstance(messages, list):
                return False
            if not messages:
                return True
            rows = []
            for evt in messages:
                if not isinstance(evt, dict):
                    return False
                row = self._row(evt)
                if row is None:
                    return False
                rows.append(row)
            await self._insert([row for row in rows if lower <= row[2] < upper])
            if min(row[2] for row in rows) < lower:
                return True
            # NapCat includes the anchor itself when no older history is available.
            if len(rows) == 1 and cursor is not None and rows[0][0] == str(cursor):
                def record_floor(c):
                    if c.execute("SELECT 1 FROM meta WHERE key='coverage'").fetchone() is None:
                        c.execute("INSERT OR IGNORE INTO meta VALUES('history_floor',?)", (rows[0][2],))
                await self._db(record_floor)
                return True
            earliest = min(range(len(rows)), key=lambda i: rows[i][2])
            cursor = messages[earliest]["message_id"]
            if str(cursor) in seen:
                return False
            seen.add(str(cursor))
        return False

    async def _chunks(self, api, end):
        stored = await self._db(lambda c: c.execute("SELECT chunks FROM reports WHERE end_ts=?", (end,)).fetchone())
        if stored:
            return json.loads(stored[0])
        members = response_data(await api.get_group_member_list(GROUP_ID))
        if not isinstance(members, list) or not members:
            return None
        counts = dict(await self._db(lambda c: c.execute(
            "SELECT user_id,COUNT(*) FROM messages WHERE ts>=? AND ts<? GROUP BY user_id",
            (end - WEEK, end)).fetchall()))
        roster = {}
        for member in members:
            if not isinstance(member, dict):
                return None
            try:
                uid = int(member["user_id"])
            except (KeyError, TypeError, ValueError):
                return None
            if uid <= 0:
                return None
            name = str(member.get("card") or member.get("nickname") or uid)
            name = " ".join(name.split()).replace("[", "［").replace("]", "］")[:80]
            roster[uid] = name
        low = sorted((counts.get(uid, 0), uid, name) for uid, name in roster.items() if counts.get(uid, 0) < 50)
        start_text = datetime.fromtimestamp(end - WEEK, TZ).strftime("%Y-%m-%d %H:%M")
        end_text = datetime.fromtimestamp(end, TZ).strftime("%Y-%m-%d %H:%M")
        header = (f"群 {GROUP_ID} 每周发言统计\n{start_text} 至 {end_text}（北京时间，不含结束时刻）\n"
                  f"本周消息共 {sum(counts.values())} 条；当前群成员 {len(roster)} 人；不足50条 {len(low)} 人。\n"
                  "不足50条名单（含零发言成员）：\n")
        floor = await self._db(lambda c: c.execute("SELECT value FROM meta WHERE key='history_floor'").fetchone())
        if floor and end - WEEK < floor[0] < end:
            earliest = datetime.fromtimestamp(floor[0], TZ).strftime("%Y-%m-%d %H:%M")
            header = f"注意：首次可读历史始于 {earliest}，本期此前数据不可补回。\n" + header
        chunks = []
        text = header
        for count, uid, name in low:
            line = f"{name}（{uid}）：{count} 条\n"
            if len(text) + len(line) > 2400:
                chunks.append(text)
                text = "不足50条名单（续）：\n"
            text += line
        chunks.append(text if low else text + "无")
        chunks = [f"[{i}/{len(chunks)}] {chunk}" for i, chunk in enumerate(chunks, 1)]
        await self._db(lambda c: c.execute("INSERT OR IGNORE INTO reports VALUES(?,?)", (end, json.dumps(chunks, ensure_ascii=False))))
        return chunks

    async def check_once(self, api, now, *, force_sync=False):
        stamp = int(now.timestamp())
        async with self._check_lock:
            def setup(c):
                c.execute("INSERT OR IGNORE INTO meta VALUES('next_end',?)", (report_end(now),))
                return dict(c.execute("SELECT key,value FROM meta").fetchall())
            meta = await self._db(setup)
            end = meta["next_end"]
            if force_sync or stamp >= self._next_sync or end <= stamp:
                lower = meta.get("coverage", end - WEEK) - 300
                if not await self._backfill(api, lower, stamp):
                    self.log.warning("群发言统计：历史补读未完成，稍后重试")
                    return
                await self._db(lambda c: c.execute("INSERT OR REPLACE INTO meta VALUES('coverage',?)", (stamp,)))
                self._next_sync = stamp + 300
            while end <= stamp:
                admins = sorted({int(uid) for uid in (config.ADMIN_USERS if self.admins is None else self.admins) if int(uid) > 0})
                if not admins:
                    self.log.warning("群发言统计：未配置管理员，报告等待发送")
                    return
                chunks = await self._chunks(api, end)
                if chunks is None:
                    self.log.warning("群发言统计：成员列表读取失败，报告延后")
                    return
                delivered = set(await self._db(lambda c: c.execute("SELECT admin,part FROM delivered WHERE end_ts=?", (end,)).fetchall()))
                for admin in admins:
                    for part, chunk in enumerate(chunks):
                        if (admin, part) in delivered:
                            continue
                        data = response_data(await api.send_private_msg(admin, chunk))
                        if not isinstance(data, dict) or not data.get("message_id"):
                            self.log.warning("群发言统计：报告发送未确认，稍后重试")
                            return
                        await self._db(lambda c: c.execute("INSERT OR IGNORE INTO delivered VALUES(?,?,?)", (end, admin, part)))
                end += WEEK
                await self._db(lambda c: c.execute("UPDATE meta SET value=? WHERE key='next_end'", (end,)))

    async def scheduler_loop(self, api):
        first = True
        while True:
            try:
                await self.check_once(api, datetime.now(TZ), force_sync=first)
                first = False
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning(f"群发言统计异常：{type(exc).__name__}")
            await asyncio.sleep(15)
