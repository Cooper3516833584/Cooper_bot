"""onebot.py

NapCat + OneBot v11
-----------------

在 NapCat 的一些部署模式下（尤其是“事件 WS + HTTP Server”同时开启时），
通过 WS 发送 action 往往“能执行，但不回 echo/回包不稳定”。
这会导致：

1) bot 等待 action 回包直到超时（默认 30s） -> 指令回复慢
2) 实际已经发出文件/消息，但 bot 因超时误判失败 -> 事后又发失败提示
3) 日志不断打印 OneBot call 超时

因此：
- **优先使用 HTTP Server 调用 action**（稳定、快速）
- WS 仅用于接收事件；action 作为兜底可走 WS（可选）
"""

from __future__ import annotations

import asyncio
import json
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, Optional


class OneBotAPI:
    def __init__(self, ws, log, http_base: Optional[str] = None, http_token: Optional[str] = None):
        self.ws = ws
        self.log = log
        self.http_base = http_base.rstrip("/") if http_base else None
        self.http_token = (http_token or "").strip() or None

        # WS action 回包（兜底用）
        self._pending: Dict[str, asyncio.Future] = {}

        # 限流打印 warning，避免刷屏
        self._last_warn: Dict[str, float] = {}

        # 群名缓存：group_id -> (name, ts)
        self._group_name_cache: Dict[int, tuple[str, float]] = {}

    def _warn_throttle(self, key: str, msg: str, interval: float = 10.0):
        now = time.time()
        last = self._last_warn.get(key, 0.0)
        if now - last >= interval:
            self._last_warn[key] = now
            self.log.warning(msg)

    def _is_timeout_like(self, e: Exception) -> bool:
        if isinstance(e, (TimeoutError, socket.timeout)):
            return True
        if isinstance(e, urllib.error.URLError):
            reason = getattr(e, "reason", None)
            if isinstance(reason, (TimeoutError, socket.timeout)):
                return True
            if reason and ("timed out" in str(reason).lower() or "timeout" in str(reason).lower()):
                return True
        s = str(e).lower()
        return ("timed out" in s) or ("timeout" in s)


    def _file_uri(self, path: str) -> str:
        """将本地路径规范化为 URI 形式（兼容 NapCat 的一些版本/场景）。

        - 已带 scheme（如 http://、https://、file://、base64://）则原样返回
        - Linux/容器内绝对路径（/xxx） => file:///xxx
        """
        s = str(path)
        if "://" in s:
            return s
        if s.startswith("/"):
            # file:// + /abs/path => file:///abs/path
            return "file://" + s
        return s

    # ========= WS 回包（仅兜底） =========
    def feed_response(self, data: dict):
        echo = data.get("echo")
        if echo and echo in self._pending:
            fut = self._pending.pop(echo)
            if not fut.done():
                fut.set_result(data)

    async def _call_ws(self, action: str, params: dict, timeout: float) -> Optional[dict]:
        echo = f"{action}_{int(time.time() * 1000)}"
        payload = {"action": action, "params": params, "echo": echo}

        fut = asyncio.get_running_loop().create_future()
        self._pending[echo] = fut

        await self.ws.send(json.dumps(payload, ensure_ascii=False))

        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(echo, None)
            self._warn_throttle(action, f"OneBot call 超时（WS）：{action}")
            return None
        except Exception as e:
            self._pending.pop(echo, None)
            self._warn_throttle(action, f"OneBot call 异常（WS）：{action}: {e}")
            return None

    # ========= HTTP action（首选） =========
    async def _call_http(self, action: str, params: dict, timeout: float) -> Optional[dict]:
        if not self.http_base:
            return None
        url = f"{self.http_base}/{action}"
        body = json.dumps(params, ensure_ascii=False).encode("utf-8")

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # NapCat/OneBot 常见鉴权方式：
        # - query: ?access_token=...
        # - header: Authorization: Bearer ...
        if self.http_token:
            token_q = urllib.parse.quote(self.http_token, safe="")
            sep = "&" if ("?" in url) else "?"
            url = f"{url}{sep}access_token={token_q}"
            headers["Authorization"] = f"Bearer {self.http_token}"

        def _do_request() -> Optional[dict]:
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    raw = resp.read()
                text = raw.decode("utf-8", errors="replace").strip()
                if not text:
                    return None
                return json.loads(text)
            except urllib.error.URLError as e:
                # 连接失败/超时
                raise e

        try:
            return await asyncio.to_thread(_do_request)
        except Exception as e:
            # 某些 NapCat 部署下，上传 action 可能“已执行但 HTTP 长时间不回包”。
            # 这类超时按“未确认”处理，避免误导性 warning 刷屏。
            if action in ("upload_group_file", "upload_private_file") and self._is_timeout_like(e):
                return None
            # 不要刷屏：同一 action 10 秒内只提示一次
            self._warn_throttle(action, f"OneBot call 超时/失败（HTTP）：{action}: {e}")
            return None

    async def call(self, action: str, params: dict, timeout: float = 8.0) -> Optional[dict]:
        """调用 OneBot action。

        默认 timeout 设短一些，避免卡顿。
        上传文件会在对应方法里传更长的 timeout。
        """
        # 1) 首选 HTTP
        resp = await self._call_http(action, params, timeout=timeout)
        if resp is not None:
            return resp
        # 2) 兜底 WS（仍然用较短超时，避免再次卡顿）
        return await self._call_ws(action, params, timeout=min(timeout, 8.0))

    # ========= 业务封装 =========
    async def send_group_msg(self, group_id: int, text: str):
        return await self.call(
            "send_group_msg",
            {"group_id": int(group_id), "message": text},
            timeout=6.0,
        )

    async def send_private_msg(self, user_id: int, text: str):
        return await self.call(
            "send_private_msg",
            {"user_id": int(user_id), "message": text},
            timeout=6.0,
        )

    async def upload_group_file(self, group_id: int, file: str, name: str, folder: Optional[str] = None):
        """上传群文件。file 必须是 NapCat 容器内可访问的本地路径。"""
        params = {"group_id": int(group_id), "file": self._file_uri(file), "name": str(name)}
        if folder:
            params["folder"] = str(folder)
        # 上传可能更久一些，但这里不硬等太久：
        # - 超时则返回 None，由上层标记“未确认”避免误报失败
        return await self.call("upload_group_file", params, timeout=300.0)

    async def upload_private_file(self, user_id: int, file: str, name: str, group_id: Optional[int] = None):
        """私聊发文件。file 必须是 NapCat 容器内可访问的本地路径。

        注：部分实现支持传 group_id 以“临时会话”给群成员发文件（若不支持会忽略/报错，由上层兜底）。
        """
        params = {"user_id": int(user_id), "file": self._file_uri(file), "name": str(name)}
        if group_id is not None:
            params["group_id"] = int(group_id)
        return await self.call("upload_private_file", params, timeout=300.0)



    async def get_file(self, file_id: str, timeout: float = 180.0, retries: int = 2, retry_delay: float = 2.0):
        """获取文件信息（用于拿 url / 本地路径）。大文件可能很慢，需要更长超时+重试。"""
        last = None
        for i in range(int(retries) + 1):
            last = await self.call(
                "get_file",
                {"file_id": str(file_id)},
                timeout=float(timeout),
            )
            if last is not None:
                return last
            if i < int(retries):
                await asyncio.sleep(float(retry_delay) * (i + 1))
        return last

    async def get_group_info(self, group_id: int, no_cache: bool = True):
        return await self.call(
            "get_group_info",
            {"group_id": int(group_id), "no_cache": bool(no_cache)},
            timeout=6.0,
        )

    async def get_group_name(self, group_id: int, ttl_seconds: float = 6 * 3600) -> str:
        """获取群名（带缓存）。
        - 事件里常拿不到 group_name，所以这里在需要时补一次
        - ttl_seconds：缓存有效期，默认 6 小时
        """
        gid = int(group_id)
        now = time.time()
        cached = self._group_name_cache.get(gid)
        if cached:
            name, ts = cached
            if now - ts <= float(ttl_seconds) and name:
                return str(name)

        try:
            resp = await self.get_group_info(gid, no_cache=True)
            if resp and resp.get("status") == "ok":
                data = resp.get("data") or {}
                name = (data.get("group_name") or "").strip()
                if name and name != str(gid):
                    self._group_name_cache[gid] = (str(name), now)
                    return str(name)
        except Exception:
            pass

        return str(gid)
