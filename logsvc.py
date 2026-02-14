from __future__ import annotations

import asyncio
import re
import time
from pathlib import Path
from typing import Dict, Optional

from config import IDLE_SPLIT_SECONDS
from router import Ctx


# Windows 文件/文件夹名不允许这些字符：<>:"/\|?*
_WIN_ILLEGAL = re.compile(r'[<>:"/\\|?*]')


def _safe_component(s: str, max_len: int = 80) -> str:
    """把任意字符串变成适合做 Windows 路径片段的形式。"""
    s = (s or "").strip()
    if not s:
        return "_"
    s = _WIN_ILLEGAL.sub("_", s)
    s = re.sub(r"\s+", " ", s).strip()
    # 去掉末尾点和空格（Windows 也不允许）
    s = s.rstrip(" .")
    if not s:
        s = "_"
    if len(s) > max_len:
        s = s[:max_len].rstrip(" .") or "_"
    return s


def _ts(now: Optional[float] = None) -> str:
    now = time.time() if now is None else now
    return time.strftime("%Y%m%d-%H%M%S", time.localtime(now))


class LogService:
    """把会话写到磁盘：

    logs/
      group/<群名_群号>/  <昵称>-<群名片>-<QQ>_<时间>.txt
      private/<昵称>-<QQ>/ <scene>_<时间>.txt

    仅保存“触发了 bot 回复”的会话：
    - log_out 被调用过才会落盘
    - 超过 IDLE_SPLIT_SECONDS 没新消息就 flush 成一个文件
    """

    def __init__(self, base_dir: Path, log):
        self.base_dir = Path(base_dir)
        self.group_dir = self.base_dir / "group"
        self.private_dir = self.base_dir / "private"
        self.group_dir.mkdir(parents=True, exist_ok=True)
        self.private_dir.mkdir(parents=True, exist_ok=True)

        self.log = log
        self._sessions: Dict[str, Dict] = {}

    # ---------- public ----------
    def log_in(self, ctx: Ctx, text: str):
        s = self._ensure_session(ctx)
        who = self._who(ctx)
        s["lines"].append(f"[IN ] {self._pretty_time()} {who}: {text}")
        s["last"] = time.time()

    def log_out(self, ctx: Ctx, text: str):
        s = self._ensure_session(ctx)
        s["lines"].append(f"[OUT] {self._pretty_time()} bot: {text}")
        s["last"] = time.time()
        s["has_out"] = True

    async def cleanup_loop(self, interval: float = 5.0):
        while True:
            await asyncio.sleep(interval)
            self.flush_idle()

    def flush_idle(self):
        now = time.time()
        keys = list(self._sessions.keys())
        for k in keys:
            s = self._sessions.get(k)
            if not s:
                continue
            if now - s["last"] >= IDLE_SPLIT_SECONDS:
                self._flush(k)

    # ---------- internal ----------
    def _session_key(self, ctx: Ctx) -> str:
        # 群聊：按群聚合（同一段会话里不同人写进同一个日志文件）
        if ctx.scene == "group":
            return f"g:{ctx.group_id}"
        # 私聊：仍然按“场景 + 人”区分（friend/group/stranger）
        return f"p:{ctx.scene}:{ctx.user_id}"

    def _ensure_session(self, ctx: Ctx) -> Dict:
        key = self._session_key(ctx)
        s = self._sessions.get(key)
        if s:
            # group_name 可能后补
            if ctx.scene == "group" and ctx.group_name and not s.get("group_name"):
                s["group_name"] = ctx.group_name
                # 路径也更新一次（让最终落盘用群名）
                s["path"] = self._make_path(ctx, s.get("start_ts", time.time()))
            return s

        start = time.time()
        path = self._make_path(ctx, start)
        s = {
            "start_ts": start,
            "last": start,
            "lines": [self._header(ctx, start)],
            "path": path,
            "has_out": False,
            "group_name": ctx.group_name,
        }
        self._sessions[key] = s
        return s

    def _make_path(self, ctx: Ctx, start_ts: float) -> Path:
        if ctx.scene == "group":
            gid = str(ctx.group_id or "group")
            gname = _safe_component(ctx.group_name) if ctx.group_name else ""

            if not gname or gname == gid:
                folder_name = gid  # 只保留一个群号
            else:
                folder_name = f"{gname}_{gid}"

            folder = self.group_dir / folder_name
            folder.mkdir(parents=True, exist_ok=True)
            folder.mkdir(parents=True, exist_ok=True)

            fname = f"session_{_ts(start_ts)}.txt"
            return folder / fname

        # private
        nick = _safe_component(ctx.nickname)
        folder = self.private_dir / f"{nick}-{ctx.user_id}"
        folder.mkdir(parents=True, exist_ok=True)
        fname = f"{ctx.scene}_{_ts(start_ts)}.txt"
        return folder / fname

    def _flush(self, key: str):
        s = self._sessions.pop(key, None)
        if not s:
            return

        if not s.get("has_out"):
            # 没有触发 bot 回复：不落盘
            return

        path: Path = s["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            path.write_text("\n".join(s["lines"]) + "\n", encoding="utf-8")
            self.log.info(f"日志已保存: {path}")
        except Exception as e:
            self.log.error(f"写日志失败: {path}, err={e}")

    def _pretty_time(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def _who(self, ctx: Ctx) -> str:
        nick = ctx.nickname or str(ctx.user_id)
        if ctx.scene == "group" and ctx.card and ctx.card != ctx.nickname:
            return f"{nick}-{ctx.card}-{ctx.user_id}"
        return f"{nick}-{ctx.user_id}"

    def _header(self, ctx: Ctx, start_ts: float) -> str:
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_ts))
        if ctx.scene == "group":
            return f"# start={t} scene=group group={ctx.group_name or ctx.group_id}({ctx.group_id}) user={self._who(ctx)}"
        return f"# start={t} scene={ctx.scene} user={self._who(ctx)}"
