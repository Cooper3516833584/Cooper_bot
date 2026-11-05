from __future__ import annotations

import asyncio
import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

from cooper_bot.modules.ai.message_context import message_id, positive_id


@dataclass(frozen=True)
class HistoryResult:
    complete: bool
    messages: tuple = ()
    floor: int | None = None


class MessageHistoryService:
    """Fetch history without dispatching messages or persisting their contents."""

    def __init__(self, *, state_path=None):
        self._locks = {}
        self._contexts = OrderedDict()
        self._context_locks = {}
        self._generation = 0
        self._online = None
        self._state_path = Path(state_path) if state_path is not None else None
        self._fences = None
        self._owners = {}
        self._state_lock = asyncio.Lock()

    async def _load_fences(self):
        if self._fences is None:
            async with self._state_lock:
                if self._fences is None:
                    def load():
                        if self._state_path is None or not self._state_path.exists():
                            return {}
                        data = json.loads(self._state_path.read_text(encoding="utf-8"))
                        if not isinstance(data, dict):
                            raise ValueError("invalid context recovery boundaries")
                        fences = data.get("boundaries", data)
                        owners = data.get("owners", {}) if "boundaries" in data else {}
                        if not isinstance(fences, dict) or not isinstance(owners, dict):
                            raise ValueError("invalid context recovery metadata")
                        return {key: float(value) for key, value in fences.items()}, owners
                    loaded = await asyncio.to_thread(load)
                    self._fences, self._owners = loaded if isinstance(loaded, tuple) else (loaded, {})

    async def _save_state(self):
        if self._state_path is None:
            return
        floor = time.time() - 1800
        data = {"boundaries": {key: stamp for key, stamp in self._fences.items() if stamp >= floor},
                "owners": {key: value for key, value in self._owners.items() if value[1] >= floor}}
        def save():
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self._state_path.with_suffix(".tmp")
            temporary.write_text(json.dumps(data), encoding="utf-8")
            temporary.replace(self._state_path)
        await asyncio.to_thread(save)

    async def record_delivery(self, identity, mid):
        if not message_id(mid):
            return
        await self._load_fences()
        target = identity.group_id if identity.scene == "group" else identity.actor_user_id
        key = f"{identity.bot_id}:{identity.scene}:{target}:{mid}"
        async with self._state_lock:
            self._owners = {key: value for key, value in self._owners.items() if value[1] >= time.time() - 1800}
            self._owners[key] = [identity.actor_user_id, time.time()]
            await self._save_state()

    def message_owner(self, event):
        scene = event["message_type"]
        target = event.get("group_id") if scene == "group" else event.get("target_id")
        key = f"{event.get('self_id')}:{scene}:{target}:{event.get('message_id')}"
        value = self._owners.get(key)
        return positive_id(value[0]) if value and value[1] >= time.time() - 1800 else None

    async def forget_context(self, identity):
        await self._load_fences()
        target = identity.group_id if identity.scene == "group" else identity.actor_user_id
        key = f"{identity.bot_id}:{identity.scene}:{target}"
        if identity.profile == "admin":
            key += f":admin:{identity.actor_user_id}"
        async with self._state_lock:
            self._fences[key] = time.time()
            self._contexts.clear()
            await self._save_state()

    async def filter_context(self, identity, rows):
        if identity is None:
            return []
        await self._load_fences()
        target = identity.group_id if identity.scene == "group" else identity.actor_user_id
        scope = f"{identity.bot_id}:{identity.scene}:{target}"
        fence = max(self._fences.get(scope, 0), self._fences.get(f"{scope}:admin:{identity.actor_user_id}", 0)
                    if identity.profile == "admin" else 0)
        return [row for row in rows if not fence or float(row.get("_history_time") or row.get("_event_time") or 0) > fence]

    def on_connect(self):
        self._generation += 1
        self._contexts.clear()

    def observe_status(self, event):
        if event.get("post_type") != "meta_event":
            return
        if event.get("meta_event_type") == "lifecycle":
            self.on_connect()
        if event.get("meta_event_type") == "heartbeat":
            status = event.get("status")
            online = status.get("online") if isinstance(status, dict) else None
            if type(online) is bool:
                if online is False or self._online is False:
                    self.on_connect()
                self._online = online

    @staticmethod
    def _event(raw, scene, target, bot_id=None):
        if not isinstance(raw, dict) or not message_id(raw.get("message_id")):
            return None
        uid, stamp = positive_id(raw.get("user_id")), positive_id(raw.get("time"))
        if not uid or not stamp or raw.get("message_type") not in (None, scene):
            return None
        sender = raw.get("sender") if isinstance(raw.get("sender"), dict) else {}
        if sender.get("user_id") is not None and positive_id(sender["user_id"]) != uid:
            return None
        if bot_id and raw.get("self_id") is not None and positive_id(raw["self_id"]) != bot_id:
            return None
        if scene == "group":
            if positive_id(raw.get("group_id")) != target:
                return None
        else:
            if positive_id(raw.get("group_id")) or not bot_id or uid not in (target, bot_id):
                return None
            peer = positive_id(raw.get("target_id"))
            if peer and peer != (target if uid == bot_id else bot_id):
                return None
        evt = dict(raw, post_type="message_sent" if bot_id and uid == bot_id else "message",
                   message_type=scene, user_id=uid, time=stamp)
        if bot_id:
            evt["self_id"] = bot_id
        if scene == "private" and uid == bot_id:
            # Only the verified current peer is supplied to get_friend_msg_history.
            evt["target_id"] = target
        return evt

    async def fetch(self, api, *, scene, target_id, lower, upper, on_page=None,
                    limit=None, bot_id=None, max_pages=2000):
        target = positive_id(target_id)
        if scene not in ("group", "private") or not target or lower >= upper:
            return HistoryResult(False)
        key = (scene, target)
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            cursor = None
            seen = set()
            collected = []
            for _ in range(max_pages):
                params = {"group_id" if scene == "group" else "user_id": target,
                          "count": min(100, limit) if limit else 100, "reverseOrder": True}
                if cursor is not None:
                    params["message_seq"] = cursor
                response = await api.call("get_group_msg_history" if scene == "group" else "get_friend_msg_history",
                                          params, timeout=8.0)
                if (not isinstance(response, dict) or response.get("status") != "ok"
                        or type(response.get("retcode")) is not int or response["retcode"] != 0):
                    return HistoryResult(False, tuple(collected))
                data = response.get("data")
                raw = data.get("messages") if isinstance(data, dict) else None
                if not isinstance(raw, list):
                    return HistoryResult(False, tuple(collected))
                if not raw:
                    return HistoryResult(True, tuple(collected))
                page = [self._event(row, scene, target, bot_id) for row in raw]
                if any(row is None for row in page):
                    return HistoryResult(False, tuple(collected))
                selected = [row for row in page if lower <= row["time"] < upper]
                if on_page is not None:
                    await on_page(selected)
                if limit:
                    by_id = {str(row["message_id"]): row for row in selected + collected}
                    collected = sorted(by_id.values(), key=lambda row: row["time"])[-limit:]
                oldest = min(page, key=lambda row: row["time"])
                if oldest["time"] < lower:
                    return HistoryResult(True, tuple(collected))
                if len(page) == 1 and cursor is not None and str(page[0]["message_id"]) == str(cursor):
                    return HistoryResult(True, tuple(collected), page[0]["time"])
                next_cursor = oldest["message_id"]
                if str(next_cursor) in seen:
                    return HistoryResult(False, tuple(collected))
                seen.add(str(next_cursor))
                cursor = next_cursor
                if limit and len(collected) >= limit:
                    return HistoryResult(False, tuple(collected))
        return HistoryResult(False, tuple(collected))

    async def context(self, api, evt, *, max_messages, ttl_seconds, actor_id=None):
        scene = evt.get("message_type")
        bot = positive_id(evt.get("self_id"))
        target = positive_id(evt.get("group_id") if scene == "group" else evt.get("user_id"))
        if scene not in ("group", "private") or not bot or not target:
            return []
        key = (bot, scene, target, actor_id)
        lock = self._context_locks.setdefault(key, asyncio.Lock())
        async def retrieve():
            async with lock:
                return await self._context(api, evt, max_messages=max_messages, ttl_seconds=ttl_seconds, actor_id=actor_id)
        try:
            return await asyncio.wait_for(retrieve(), timeout=8.0)
        except TimeoutError:
            return []

    async def _context(self, api, evt, *, max_messages, ttl_seconds, actor_id=None):
        scene = evt.get("message_type")
        bot = positive_id(evt.get("self_id"))
        target = positive_id(evt.get("group_id") if scene == "group" else evt.get("user_id"))
        if scene not in ("group", "private") or not bot or not target or (scene == "private" and target == bot):
            return []
        limit = min(300, max(0, int(max_messages)))
        ttl = min(1800.0, max(0.0, float(ttl_seconds)))
        if not limit or not ttl:
            return []
        await self._load_fences()
        upper = min(positive_id(evt.get("time")) or int(time.time()), int(time.time()))
        scope = f"{bot}:{scene}:{target}"
        lower = max(upper - ttl, self._fences.get(scope, 0),
                    self._fences.get(f"{scope}:admin:{actor_id}", 0) if actor_id else 0)
        if lower >= upper:
            return []
        key = (bot, scene, target, actor_id)
        cached = self._contexts.get(key)
        # A cache fetched for a later request cannot supply an older queued turn.
        if cached and cached[0] > time.monotonic() and cached[1] <= upper:
            self._contexts.move_to_end(key)
            return [row for row in cached[2] if lower <= row["time"] < upper][-limit:]
        generation = self._generation
        try:
            result = await asyncio.wait_for(self.fetch(
                api, scene=scene, target_id=target, lower=lower, upper=upper,
                limit=limit, bot_id=bot, max_pages=20), timeout=8.0)
        except (TimeoutError, OSError):
            return []
        if generation != self._generation:
            return []
        rows = list(result.messages)
        # Retry incomplete/failed retrieval sooner; contents remain memory-only.
        self._contexts[key] = (time.monotonic() + (300 if result.complete or len(rows) == limit else 15), upper, rows)
        self._contexts.move_to_end(key)
        while len(self._contexts) > 128:
            self._contexts.popitem(last=False)
        if len(self._locks) > 256:
            self._locks = {key: lock for key, lock in self._locks.items() if lock.locked()}
        if len(self._context_locks) > 256:
            self._context_locks = {key: lock for key, lock in self._context_locks.items() if lock.locked()}
        return rows
