from __future__ import annotations

import asyncio
import time
import uuid
from collections import defaultdict
from pathlib import Path

from cooper_bot.core import config

from .models import CapturedInput, MemoryIdentity, MemorySnapshot
from .policy import MemoryPolicy, may_capture, safe_text, scope_for, valid_identity
from .retrieval import search
from .store import MemoryStore


class _Turn:
    def __init__(self, service, identity, scope_id, input_row, lock, metadata_only=False, duplicate=False):
        self.service, self.identity, self.scope_id = service, identity, scope_id
        self.input_row, self._lock, self.metadata_only, self.is_duplicate = input_row, lock, metadata_only, duplicate
        self.snapshot = MemorySnapshot(scope_id, input_row.get("conversation_id", ""), int(input_row.get("epoch", 0)), int(input_row.get("seq", 0)), ())
        self._generated = False
    async def __aenter__(self):
        if self._lock is not None: await self._lock.acquire()
        rows = await self.service.store.snapshot_rows(self.scope_id, self.snapshot.conversation_id, self.snapshot.current_input_seq, config.AI_MEMORY_RECENT_EVENTS)
        facts = await self.service.search_facts(self.identity, "", limit=config.AI_MEMORY_TOP_K)
        self.snapshot = MemorySnapshot(self.scope_id, self.snapshot.conversation_id, self.snapshot.epoch, self.snapshot.current_input_seq, tuple(rows), tuple(facts))
        return self
    async def record_generated(self, text):
        if not self.is_duplicate:
            await self.service.store.insert_assistant_once(self.scope_id, self.snapshot.current_input_seq, text, metadata_only=self.metadata_only)
            self._generated = True
    async def ensure_current(self):
        current = await self.service.store.scope(self.scope_id)
        return bool(current and int(current["epoch"]) == self.snapshot.epoch and current["active_conversation_id"] == self.snapshot.conversation_id)
    async def finish_delivery(self, confirmed: bool):
        if self._generated: await self.service.store.finish_turn(self.scope_id, self.snapshot.current_input_seq, bool(confirmed))
    async def abort(self, reason_code="failed"):
        if self._generated: await self.service.store.finish_turn(self.scope_id, self.snapshot.current_input_seq, False, failed=True)
    async def __aexit__(self, typ, value, tb):
        if typ is not None: await self.abort(type(value).__name__ if value else "failed")
        if self._lock is not None and self._lock.locked(): self._lock.release()


class MemoryService:
    def __init__(self, log=None, *, enabled: bool | None=None, db_path: Path | None=None):
        self.log = log
        self.enabled = bool(config.AI_MEMORY_ENABLED if enabled is None else enabled)
        self.store = MemoryStore(Path(db_path or config.AI_MEMORY_DB_PATH), busy_timeout_ms=config.AI_MEMORY_DB_BUSY_TIMEOUT_MS)
        self._locks: dict[str, asyncio.Lock] = {}
        self._started = False

    async def start(self):
        if self._started or not self.enabled: return
        await self.store.start(); self._started = True

    async def aclose(self):
        if not self._started: return
        self._started = False; await self.store.close()

    def _identity_scope(self, identity: MemoryIdentity) -> str | None:
        if not self.enabled or not valid_identity(identity): return None
        if identity.scene == "group" and identity.profile == "public" and identity.group_id not in config.AI_MEMORY_GROUP_ALLOWLIST: return None
        return scope_for(identity)

    async def _ensure(self, identity: MemoryIdentity):
        scope_id = self._identity_scope(identity)
        if not scope_id: return None, None
        await self.start()
        kind = identity.scene; target = identity.group_id if kind == "group" else identity.actor_user_id
        scope = await self.store.ensure_scope(scope_id, bot_id=identity.bot_id, profile=identity.profile, kind=kind, target_id=int(target), owner_user_id=None if kind == "group" and identity.profile == "public" else identity.actor_user_id)
        return scope_id, scope

    async def turn(self, identity: MemoryIdentity, item: CapturedInput):
        scope_id, scope = await self._ensure(identity)
        if not scope_id:
            return None
        member = await self.store.member_enabled(scope_id, identity.actor_user_id)
        policy = MemoryPolicy(scope_id, bool(scope["enabled"]), scope["capture_mode"], identity.profile == "admin")
        metadata_only = identity.profile == "admin"
        if not may_capture(identity, item, policy, member): return None
        own, _, secret = safe_text(item.own_text, config.AI_MEMORY_MAX_STORED_EVENT_CHARS)
        quote, _, _ = safe_text(item.quoted_text, config.AI_MEMORY_MAX_STORED_EVENT_CHARS)
        visual, _, _ = safe_text(item.visual_text, config.AI_MEMORY_MAX_STORED_EVENT_CHARS)
        row = await self.store.append_input_once(scope_id, identity.actor_user_id, item.source_event_id or uuid.uuid4().hex, "" if secret else own, quote, visual, item.source_kind, metadata_only=metadata_only)
        duplicate = row["state"] != "pending"
        return _Turn(self, identity, scope_id, row, self._locks.setdefault(scope_id, asyncio.Lock()), metadata_only, duplicate)

    async def set_enabled(self, identity, enabled: bool, capture_mode="directed"):
        scope_id, _ = await self._ensure(identity)
        if not scope_id: raise ValueError("memory is not available in this scope")
        await self.store.set_scope_policy(scope_id, enabled, capture_mode)
    async def set_member_enabled(self, identity, enabled: bool):
        scope_id, _=await self._ensure(identity)
        if not scope_id: raise ValueError("memory is not available in this scope")
        await self.store.set_member_policy(scope_id, identity.actor_user_id, enabled)
    async def remember_explicit(self, identity, text: str, replace_id: str | None=None, subject_id: str | None=None):
        scope_id, scope=await self._ensure(identity)
        if not scope_id: raise ValueError("memory is not available in this scope")
        clean, _, secret=safe_text(text, config.AI_MEMORY_MAX_FACT_CHARS)
        if secret or not clean or len(text) > config.AI_MEMORY_MAX_FACT_CHARS: raise ValueError("memory text is unsafe or too long")
        await self.store.set_member_policy(scope_id, identity.actor_user_id, True)
        if not scope["enabled"]: await self.store.set_scope_policy(scope_id, True)
        return await self.store.save_explicit_fact(scope_id, subject_id or f"user:{identity.actor_user_id}", clean, replace_id=replace_id)
    async def search_facts(self, identity, query: str, limit=6):
        scope_id, _=await self._ensure(identity)
        if not scope_id:return []
        subjects=(f"user:{identity.actor_user_id}",)+( (f"group:{identity.group_id}",) if identity.scene=="group" and identity.profile=="public" else ())
        return search(await self.store.list_facts(scope_id, subjects), query, limit)
    async def list_facts(self, identity): return await self.search_facts(identity,"",limit=200)
    async def history(self, identity, *, before: int | None=None, limit: int=20):
        scope_id, _=await self._ensure(identity)
        if not scope_id:return []
        if identity.profile == "admin": return []
        return await self.store.history(scope_id,before=before,limit=limit)
    async def forget_fact(self,identity,fact_id):
        scope_id,_=await self._ensure(identity); return bool(scope_id and await self.store.forget_fact(scope_id,f"user:{identity.actor_user_id}",fact_id))
    async def clear_subject(self,identity):
        scope_id,_=await self._ensure(identity)
        if not scope_id:raise ValueError("memory is not available in this scope")
        await self.store.clear_subject(scope_id,f"user:{identity.actor_user_id}")
    async def rotate_conversation(self,identity):
        scope_id,_=await self._ensure(identity)
        if not scope_id:raise ValueError("memory is not available in this scope")
        return await self.store.rotate_conversation(scope_id)
    async def prompt_context(self, identity, query):
        facts=await self.search_facts(identity,query,config.AI_MEMORY_TOP_K)
        return {"schema_version":1,"data_only":True,"facts":[{"id":f['fact_id'],"text":f['text'],"source_kind":f['source_kind'],"updated_at":f['updated_at']} for f in facts]} if facts else None
