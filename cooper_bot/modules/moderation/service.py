from __future__ import annotations

import asyncio
import json
import math
import re
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from cooper_bot.core.config import ADMIN_USERS, KICK_AUDIT_QQ, TIMEZONE
from cooper_bot.modules.moderation.review import ReviewResult, review_snapshot
from cooper_bot.modules.moderation.rules import (
    VOTE_WINDOW_SECONDS,
    calculate_local_votes,
    parse_group_level,
    record_text,
    with_member_level,
)
from cooper_bot.modules.moderation.store import ModerationStore


WORKER_COUNT = 2
QUEUE_CAPACITY = 64
MEMBER_CACHE_SECONDS = 300.0
CASE_DEADLINE_SECONDS = 180.0
AUDIT_WINDOW_SECONDS = 1800.0
RETRACTION_WINDOW_SECONDS = 300.0
MAX_FORWARD_NODES = 20
MAX_FORWARD_REQUEST_BYTES = 256 * 1024
MAX_FORWARD_PART_BYTES = 220 * 1024
_MESSAGE_ID_RE = re.compile(r"^-?\d+$")
_FORWARD_SEGMENTS = {"text", "image", "face", "mface", "market_face", "record", "video", "file", "location", "share", "json", "xml", "dice", "rps"}


def api_outcome(response) -> str:
    """Return ok, failed or unknown without treating ambiguous replies as failures."""
    if response is None or not isinstance(response, dict):
        return "unknown"
    status = response.get("status")
    retcode = response.get("retcode")
    if isinstance(retcode, bool):
        return "unknown"
    if status == "failed":
        return "failed"
    if status != "ok":
        return "unknown"
    if retcode == 0 or retcode == "0":
        return "ok"
    if isinstance(retcode, (int, str)) and str(retcode).isascii() and str(retcode).isdigit():
        return "failed"
    return "unknown"


def _positive_id(value) -> int | None:
    if isinstance(value, bool):
        return None
    raw = str(value or "").strip()
    if not raw.isascii() or not raw.isdigit():
        return None
    parsed = int(raw)
    return parsed if parsed > 0 else None


def _message_id(value) -> str | None:
    if isinstance(value, bool):
        return None
    raw = str(value or "").strip()
    if not _MESSAGE_ID_RE.fullmatch(raw):
        return None
    try:
        parsed = int(raw)
    except ValueError:
        return None
    if parsed < -2147483648 or parsed > 2147483647:
        return None
    return str(parsed)


def _member_data(response, group_id: int, user_id: int) -> dict | None:
    if api_outcome(response) != "ok":
        return None
    data = response.get("data")
    if not isinstance(data, dict):
        return None
    if _positive_id(data.get("group_id")) != int(group_id) or _positive_id(data.get("user_id")) != int(user_id):
        return None
    role = str(data.get("role") or "").strip().lower()
    if role not in {"member", "admin", "owner"}:
        return None
    return {"group_id": int(group_id), "user_id": int(user_id), "role": role, "level": data.get("level")}


def _safe_segment_text(segment: dict) -> str:
    data = segment.get("data")
    return str(data.get("text") or "") if isinstance(data, dict) else ""


def _record_from_event(evt: dict, received_at: float) -> dict | None:
    if str(evt.get("post_type") or "").lower() != "message" or str(evt.get("message_type") or "").lower() != "group":
        return None
    if evt.get("anonymous"):
        return None
    self_id = _positive_id(evt.get("self_id"))
    group_id = _positive_id(evt.get("group_id"))
    sender = evt.get("sender") if isinstance(evt.get("sender"), dict) else {}
    user_id = _positive_id(sender.get("user_id") or evt.get("user_id"))
    message_id = _message_id(evt.get("message_id"))
    if not self_id or not group_id or not user_id or not message_id or user_id == self_id:
        return None

    message = evt.get("message")
    segments: list[dict]
    if isinstance(message, list):
        if any(not isinstance(item, dict) for item in message):
            return None
        segments = message
        plain_text = "".join(_safe_segment_text(item) for item in segments if str(item.get("type") or "").lower() == "text")
    elif isinstance(message, str):
        plain_text = message
        segments = [{"type": "text", "data": {"text": message}}]
    else:
        raw = evt.get("raw_message")
        if not isinstance(raw, str):
            return None
        # CQ-like text is deliberately stored as text, never reinterpreted as media.
        plain_text = raw
        segments = [{"type": "text", "data": {"text": raw}}]
    try:
        json.dumps(segments, ensure_ascii=False)
    except (TypeError, ValueError):
        return None

    event_time = evt.get("time")
    source = "event"
    try:
        if isinstance(event_time, bool):
            raise ValueError
        occurred_at = float(event_time)
        if not math.isfinite(occurred_at) or occurred_at <= 0 or occurred_at > received_at + 5.0:
            raise ValueError
    except (TypeError, ValueError, OverflowError):
        occurred_at = float(received_at)
        source = "received"
    return {
        "self_id": self_id,
        "group_id": group_id,
        "user_id": user_id,
        "message_id": message_id,
        "occurred_at": occurred_at,
        "received_at": float(received_at),
        "time_source": source,
        "nickname": str(sender.get("nickname") or "")[:256],
        "card": str(sender.get("card") or "")[:256],
        "segments": segments,
        "plain_text": plain_text,
    }


class ModerationService:
    def __init__(self, store: ModerationStore | None = None, log=None, *, clock=None):
        self.store = store or ModerationStore()
        self.log = log
        self.clock = clock or time.time
        self.active = False
        self.api = None
        self.aisvc = None
        self.vision_skill = None
        self.perm = None
        self._scopes: dict[tuple[int, int], dict] = {}
        self._volatile_off_scopes: dict[tuple[int, int], int] = {}
        self._member_cache: dict[tuple[int, int, int], tuple[dict, float]] = {}
        self._observe_locks: dict[tuple[int, int], asyncio.Lock] = {}
        self._jobs: dict[tuple[int, int, int], dict] = {}
        self._queue: asyncio.Queue | None = None
        self._workers: list[asyncio.Task] = []
        self._last_queue_warning = 0.0
        self._last_cleanup = 0.0

    async def start(self, api, aisvc, vision_skill, perm=None) -> None:
        if self.active:
            await self.close()
        self.api = api
        self.aisvc = aisvc
        self.vision_skill = vision_skill
        self.perm = perm
        now = float(self.clock())
        await asyncio.to_thread(self.store.mark_running_interrupted, now)
        await asyncio.to_thread(self.store.mark_capture_gap, now)
        loaded = await asyncio.to_thread(self.store.load_group_states)
        for scope, revision in self._volatile_off_scopes.items():
            state = loaded.setdefault(
                scope,
                {"enabled": False, "revision": 0, "enabled_at": None, "last_capture_gap_at": None},
            )
            state.update(enabled=False, revision=max(int(state.get("revision", 0)), int(revision)))
        self._scopes = {
            key: {**value, "generation": 0, "audit_recipient": None}
            for key, value in loaded.items()
        }
        self._jobs.clear()
        self._observe_locks.clear()
        self._queue = asyncio.Queue(maxsize=QUEUE_CAPACITY)
        self.active = True
        self._workers = [asyncio.create_task(self._worker(index)) for index in range(WORKER_COUNT)]

    async def close(self) -> None:
        if not self.active and not self._workers:
            return
        self.active = False
        for state in self._scopes.values():
            state["generation"] = int(state.get("generation", 0)) + 1
        tasks, self._workers = self._workers, []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        now = float(self.clock())
        try:
            await asyncio.to_thread(self.store.mark_running_interrupted, now)
            await asyncio.to_thread(self.store.mark_capture_gap, now)
        except Exception:
            pass
        self._jobs.clear()
        self.api = None

    def _scope_state(self, scope: tuple[int, int]) -> dict:
        return self._scopes.setdefault(
            (int(scope[0]), int(scope[1])),
            {"enabled": False, "revision": 0, "enabled_at": None, "last_capture_gap_at": None, "generation": 0, "audit_recipient": None},
        )

    def begin_control(self, scope: tuple[int, int], *, enabled: bool) -> dict:
        key = (int(scope[0]), int(scope[1]))
        state = self._scope_state(key)
        state["generation"] = int(state.get("generation", 0)) + 1
        next_revision = int(state.get("revision", 0)) + 1
        token = {"scope": key, "generation": state["generation"], "revision": next_revision, "target_enabled": bool(enabled)}
        if not enabled:
            state["enabled"] = False
            state["revision"] = next_revision
            state["audit_recipient"] = None
            self._volatile_off_scopes[key] = next_revision
            self._drop_scope_jobs(key)
        return token

    def _token_current(self, token: dict) -> bool:
        scope = tuple(token.get("scope") or ())
        if len(scope) != 2 or not self.active:
            return False
        state = self._scope_state((int(scope[0]), int(scope[1])))
        return int(state.get("generation", -1)) == int(token.get("generation", -2))

    def _is_current(self, scope: tuple[int, int], revision: int, generation: int) -> bool:
        if not self.active:
            return False
        state = self._scope_state(scope)
        return bool(
            state.get("enabled")
            and int(state.get("revision", -1)) == int(revision)
            and int(state.get("generation", -1)) == int(generation)
        )

    def _drop_scope_jobs(self, scope: tuple[int, int]) -> None:
        for key in list(self._jobs):
            if key[:2] == scope:
                self._jobs.pop(key, None)

    def _gateway_ready(self, aisvc=None, vision_skill=None) -> bool:
        aisvc = self.aisvc if aisvc is None else aisvc
        vision_skill = self.vision_skill if vision_skill is None else vision_skill
        try:
            gateway = getattr(aisvc, "gateway", None)
            return bool(gateway and gateway.provider("deepseek").ready and vision_skill and vision_skill.ready)
        except Exception:
            return False

    async def _query_member(self, api, group_id: int, user_id: int, *, no_cache: bool) -> dict | None:
        method = getattr(api, "get_group_member_info", None)
        if not callable(method):
            return None
        try:
            response = await method(int(group_id), int(user_id), no_cache=bool(no_cache))
        except asyncio.CancelledError:
            raise
        except Exception:
            return None
        return _member_data(response, group_id, user_id)

    async def enable_scope(self, *, api, token: dict, recipient: int, aisvc=None, vision_skill=None) -> bool:
        if not self._token_current(token):
            return False
        self_id, group_id = token["scope"]
        bot = await self._query_member(api, group_id, self_id, no_cache=True)
        if not self._token_current(token) or not bot or bot["role"] not in {"admin", "owner"}:
            return False
        if not self._gateway_ready(aisvc, vision_skill) or not self._token_current(token):
            return False
        now = float(self.clock())
        try:
            saved = await asyncio.to_thread(
                self.store.set_group_enabled, self_id, group_id, True, int(token["revision"]), now
            )
        except Exception:
            return False
        if not saved or not self._token_current(token):
            return False
        state = self._scope_state((self_id, group_id))
        state.update(enabled=True, revision=int(token["revision"]), enabled_at=now, audit_recipient=int(recipient))
        self._volatile_off_scopes.pop((self_id, group_id), None)
        return True

    async def disable_scope(self, token: dict) -> bool:
        scope = tuple(token.get("scope") or ())
        if len(scope) != 2 or bool(token.get("target_enabled")):
            return False
        self_id, group_id = int(scope[0]), int(scope[1])
        revision = int(token["revision"])
        try:
            saved = await asyncio.to_thread(
                self.store.set_group_enabled, self_id, group_id, False, revision, float(self.clock())
            )
        except Exception:
            return False
        if saved and self._volatile_off_scopes.get((self_id, group_id)) == revision:
            self._volatile_off_scopes.pop((self_id, group_id), None)
        if not saved:
            try:
                persisted = await asyncio.to_thread(self.store.get_group_state, self_id, group_id)
                if int(persisted.get("revision", 0)) > revision:
                    return True
            except Exception:
                pass
        return bool(saved)

    async def status_scope(self, api, self_id: int, group_id: int, aisvc=None, vision_skill=None) -> dict:
        key = (int(self_id), int(group_id))
        state = self._scopes.get(key)
        try:
            persisted = await asyncio.to_thread(self.store.get_group_state, *key)
        except Exception:
            persisted = {"enabled": False, "revision": 0}
        if state is None:
            state = self._scope_state(key)
            state.update(persisted)
        bot = await self._query_member(api, int(group_id), int(self_id), no_cache=True)
        return {
            "enabled": bool(state.get("enabled", persisted.get("enabled", False))),
            "bot_role": bot.get("role") if bot else "unknown",
            "ready": self._gateway_ready(aisvc, vision_skill),
        }

    async def observe_event(self, ctx, evt: dict) -> None:
        if not self.active:
            return
        received_at = float(self.clock())
        record = _record_from_event(evt, received_at)
        if record is None:
            return
        scope = (record["self_id"], record["group_id"])
        state = self._scope_state(scope)
        if not state.get("enabled"):
            return
        if record["user_id"] == record["self_id"]:
            return
        sender = evt.get("sender") if isinstance(evt.get("sender"), dict) else {}
        if str(sender.get("role") or "").lower() in {"admin", "owner"}:
            return
        lock = self._observe_locks.setdefault(scope, asyncio.Lock())
        async with lock:
            state = self._scope_state(scope)
            if not state.get("enabled") or not self.active:
                return
            revision = int(state.get("revision", 0))
            generation = int(state.get("generation", 0))
            try:
                inserted, seq = await asyncio.to_thread(self.store.append_message_if_enabled, record, revision)
            except Exception:
                return
            if not inserted or seq is None or not self._is_current(scope, revision, generation):
                return
            start = record["occurred_at"] - VOTE_WINDOW_SECONDS
            try:
                rows = await asyncio.to_thread(
                    self.store.read_sender_window, *scope, record["user_id"], start, record["occurred_at"], seq
                )
            except Exception:
                return
            local = calculate_local_votes(rows, record["occurred_at"])
            if sum(local.votes.values()) < 2:
                return
            key = (record["self_id"], record["group_id"], record["user_id"])
            self._enqueue_candidate(key, seq, record["occurred_at"], revision, generation)

    def _enqueue_candidate(self, key: tuple[int, int, int], seq: int, anchor_at: float, revision: int, generation: int) -> None:
        if self._queue is None:
            return
        existing = self._jobs.get(key)
        if existing is not None:
            if int(seq) > int(existing.get("latest_seq", 0)):
                existing.update(latest_seq=int(seq), latest_anchor_at=float(anchor_at), revision=int(revision), generation=int(generation))
            return
        job = {
            "latest_seq": int(seq), "latest_anchor_at": float(anchor_at), "revision": int(revision),
            "generation": int(generation), "queued_at": float(self.clock()), "queued": True, "running": False,
        }
        try:
            self._queue.put_nowait(key)
        except asyncio.QueueFull:
            now = float(self.clock())
            if now - self._last_queue_warning >= 60.0:
                self._last_queue_warning = now
                if self.log is not None and callable(getattr(self.log, "warning", None)):
                    self.log.warning("反广告候选队列已满，本次候选静默跳过。")
            return
        self._jobs[key] = job

    async def _worker(self, index: int) -> None:
        while self.active:
            if self._queue is None:
                return
            try:
                key = await asyncio.wait_for(self._queue.get(), timeout=60.0)
            except asyncio.TimeoutError:
                if index == 0 and self.active:
                    now = float(self.clock())
                    if now - self._last_cleanup >= 60.0:
                        self._last_cleanup = now
                        try:
                            await asyncio.to_thread(self.store.cleanup_expired, now)
                        except Exception:
                            pass
                continue
            job = self._jobs.get(key)
            if job is None or not job.get("queued"):
                self._queue.task_done()
                continue
            job["queued"] = False
            job["running"] = True
            processed_seq = int(job.get("latest_seq", 0))
            snapshot = dict(job)
            try:
                await self._process_candidate(key, snapshot)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            finally:
                now = float(self.clock())
                if index == 0 and now - self._last_cleanup >= 60.0:
                    self._last_cleanup = now
                    try:
                        await asyncio.to_thread(self.store.cleanup_expired, now)
                    except Exception:
                        pass
                job["running"] = False
                current = self._jobs.get(key)
                if current is job:
                    if int(job.get("latest_seq", 0)) > processed_seq and self.active:
                        job["queued"] = True
                        try:
                            self._queue.put_nowait(key)
                        except asyncio.QueueFull:
                            self._jobs.pop(key, None)
                    else:
                        self._jobs.pop(key, None)
                self._queue.task_done()

    async def _process_candidate(self, key: tuple[int, int, int], job: dict) -> None:
        self_id, group_id, user_id = key
        scope = (self_id, group_id)
        state = self._scope_state(scope)
        revision, generation = int(job["revision"]), int(job["generation"])
        if not self._is_current(scope, revision, generation):
            return
        deadline = float(job["queued_at"]) + CASE_DEADLINE_SECONDS
        if float(self.clock()) >= deadline:
            return
        seq = int(job["latest_seq"])
        anchor_at = float(job["latest_anchor_at"])
        rows = await asyncio.to_thread(
            self.store.read_sender_window, self_id, group_id, user_id,
            anchor_at - VOTE_WINDOW_SECONDS, anchor_at, seq,
        )
        if not rows or not self._is_current(scope, revision, generation):
            return
        anchor = next((row for row in rows if int(row.get("seq") or 0) == seq), None)
        if anchor is None:
            return
        local = calculate_local_votes(rows, anchor_at)
        if sum(local.votes.values()) < 2:
            return
        now = float(self.clock())
        if not await asyncio.to_thread(self.store.cooldown_available, self_id, group_id, user_id, now):
            return

        cache_key = (self_id, group_id, user_id)
        cached = self._member_cache.get(cache_key)
        info = None
        from_fresh_api = False
        if cached and now - cached[1] <= MEMBER_CACHE_SECONDS:
            info = cached[0]
        else:
            self._member_cache.pop(cache_key, None)
            info = await self._query_member(self.api, group_id, user_id, no_cache=True)
            from_fresh_api = True
            if info and info["role"] in {"member", "admin", "owner"}:
                self._member_cache[cache_key] = (info, float(self.clock()))
        if not self._is_current(scope, revision, generation) or info is None or info["role"] != "member":
            return
        votes = with_member_level(local, info.get("level"))
        if votes.total < 3:
            return
        if not from_fresh_api:
            info = await self._query_member(self.api, group_id, user_id, no_cache=True)
            if not self._is_current(scope, revision, generation) or info is None or info["role"] != "member":
                return
            self._member_cache[cache_key] = (info, float(self.clock()))
            votes = with_member_level(local, info.get("level"))
            if votes.total < 3:
                return
        if not self._gateway_ready() or not self._is_current(scope, revision, generation):
            return
        created_at = float(self.clock())
        try:
            case_id = await asyncio.to_thread(
                self.store.reserve_review, self_id, group_id, user_id, revision,
                anchor["message_id"], seq, created_at,
                {"votes": votes.votes, "total": votes.total, "member_level": parse_group_level(info.get("level"))},
            )
        except Exception:
            return
        if not case_id:
            return
        review_at = float(self.clock())
        try:
            through_seq = await asyncio.to_thread(self.store.latest_seq)
            snapshot_messages = await asyncio.to_thread(
                self.store.read_sender_window, self_id, group_id, user_id,
                review_at - AUDIT_WINDOW_SECONDS, review_at, through_seq,
            )
            await asyncio.to_thread(self.store.set_review_snapshot, case_id, review_at, through_seq)
        except Exception:
            await self._finish_case(case_id, "skipped", "snapshot_storage_error")
            return
        if not self._is_current(scope, revision, generation):
            await self._finish_case(case_id, "interrupted", "scope_changed")
            return
        trigger_ids = set(votes.evidence_message_ids)
        outcome = await review_snapshot(
            api=self.api,
            gateway=self.aisvc.gateway,
            vision_skill=self.vision_skill,
            self_id=self_id,
            group_id=group_id,
            user_id=user_id,
            messages=snapshot_messages,
            trigger_message_ids=trigger_ids,
            votes={"votes": votes.votes, "total": votes.total},
            deadline=deadline,
            still_current=lambda: self._is_current(scope, revision, generation),
            clock=self.clock,
        )
        if not self._is_current(scope, revision, generation):
            await self._finish_case(case_id, "interrupted", "scope_changed")
            return
        if outcome.decision != "block":
            await self._finish_case(case_id, outcome.decision, outcome.stop_reason)
            return
        await asyncio.to_thread(
            self.store.update_case_decision, case_id, "running", "confirmed_content_violation", list(outcome.evidence)
        )
        await self._enforce_confirmed(
            case_id=case_id, key=key, revision=revision, generation=generation,
            category=outcome.category, evidence=list(outcome.evidence),
            trigger_message_at=float(anchor["occurred_at"]), deadline=deadline,
        )

    async def _finish_case(self, case_id: str, decision: str, reason: str) -> None:
        try:
            await asyncio.to_thread(self.store.update_case_decision, case_id, decision, reason, None, float(self.clock()))
        except Exception:
            pass

    def _audit_recipient(self) -> int | None:
        target = int(KICK_AUDIT_QQ or min(ADMIN_USERS or {0}))
        if target <= 0:
            return None
        if target in ADMIN_USERS:
            return target
        try:
            level = self.perm.get_level(target) if self.perm is not None else None
            if isinstance(level, int) and not isinstance(level, bool) and level == 3:
                return target
        except Exception:
            pass
        return None

    async def _enforce_confirmed(
        self, *, case_id: str, key: tuple[int, int, int], revision: int, generation: int,
        category: str, evidence: list[dict], trigger_message_at: float, deadline: float,
    ) -> None:
        self_id, group_id, user_id = key
        scope = (self_id, group_id)
        if not self._is_current(scope, revision, generation) or float(self.clock()) >= deadline:
            await self._finish_case(case_id, "skipped", "scope_or_deadline")
            return
        recipient = self._audit_recipient()
        if recipient is None:
            await self._finish_case(case_id, "skipped", "audit_recipient_unavailable")
            return
        bot = await self._query_member(self.api, group_id, self_id, no_cache=True)
        target = await self._query_member(self.api, group_id, user_id, no_cache=True)
        if not self._is_current(scope, revision, generation) or not bot or bot["role"] not in {"admin", "owner"} or not target or target["role"] != "member":
            await self._finish_case(case_id, "skipped", "role_or_scope_changed")
            return
        action_at = float(self.clock())
        try:
            through_seq = await asyncio.to_thread(self.store.latest_seq)
            audit_messages = await asyncio.to_thread(
                self.store.read_sender_window, self_id, group_id, user_id,
                action_at - AUDIT_WINDOW_SECONDS, action_at, through_seq,
            )
            if not audit_messages:
                raise RuntimeError("empty audit snapshot")
            await asyncio.to_thread(
                self.store.freeze_audit_snapshot, case_id, action_at, through_seq, audit_messages, evidence
            )
        except Exception:
            await self._finish_case(case_id, "skipped", "audit_snapshot_error")
            return
        group_state = await asyncio.to_thread(self.store.get_group_state, self_id, group_id)
        try:
            batches = self._build_forward_batches(
                self_id=self_id, group_id=group_id, user_id=user_id, case_id=case_id,
                category=category, recipient=recipient, messages=audit_messages,
                action_at=action_at, enabled_at=group_state.get("enabled_at"),
                gap_at=group_state.get("last_capture_gap_at"),
            )
        except Exception:
            await self._finish_case(case_id, "skipped", "audit_payload_incomplete")
            return

        sent_batches = 0
        for index, batch in enumerate(batches, start=1):
            if not self._is_current(scope, revision, generation) or float(self.clock()) >= deadline:
                await self._finish_case(case_id, "skipped", "scope_or_deadline")
                return
            state, _response = await self._write_once(
                case_id, f"audit:batch:{index}",
                lambda batch=batch: self.api.send_private_forward_msg(recipient, batch),
                {"batch": index, "batches": len(batches)}, scope, revision, generation,
            )
            if state != "ok":
                await self._finish_case(case_id, "skipped", "audit_forward_failed")
                if sent_batches:
                    await self._send_final_result(case_id, recipient, "证据转发未全部确认，已停止后续处置。", scope, revision, generation)
                return
            sent_batches += 1

        notice = self._group_notice(user_id, category)
        state, _response = await self._write_once(
            case_id, "group_notice",
            lambda: self.api.send_group_msg(group_id, [{"type": "text", "data": {"text": notice}}]),
            {}, scope, revision, generation,
        )
        if state != "ok":
            await self._finish_case(case_id, "skipped", "group_notice_failed")
            await self._send_final_result(case_id, recipient, "群内提示未确认，未执行踢出或撤回。", scope, revision, generation)
            return

        if not self._is_current(scope, revision, generation) or float(self.clock()) >= deadline:
            await self._finish_case(case_id, "skipped", "scope_or_deadline")
            return
        bot = await self._query_member(self.api, group_id, self_id, no_cache=True)
        target = await self._query_member(self.api, group_id, user_id, no_cache=True)
        if not self._is_current(scope, revision, generation) or not bot or bot["role"] not in {"admin", "owner"} or not target or target["role"] != "member":
            await self._finish_case(case_id, "skipped", "role_or_scope_changed")
            await self._send_final_result(case_id, recipient, "踢出前权限或成员状态已变化，未继续操作。", scope, revision, generation)
            return

        kick_state, _response = await self._write_once(
            case_id, "kick",
            lambda: self.api.set_group_kick(group_id, user_id, reject_add_request=False),
            {}, scope, revision, generation,
        )
        if kick_state != "ok":
            await self._finish_case(case_id, "skipped", "kick_failed_or_unconfirmed")
            phrase = "踢出结果未确认" if kick_state == "unknown" else "踢出失败"
            await self._send_final_result(case_id, recipient, f"{phrase}；未撤回消息。", scope, revision, generation)
            return

        observe_lock = self._observe_locks.get(scope)
        try:
            if observe_lock is None:
                delete_messages = await asyncio.to_thread(
                    self.store.read_sender_window, self_id, group_id, user_id,
                    trigger_message_at - RETRACTION_WINDOW_SECONDS,
                    trigger_message_at + RETRACTION_WINDOW_SECONDS,
                )
            else:
                async with observe_lock:
                    delete_messages = await asyncio.to_thread(
                        self.store.read_sender_window, self_id, group_id, user_id,
                        trigger_message_at - RETRACTION_WINDOW_SECONDS,
                        trigger_message_at + RETRACTION_WINDOW_SECONDS,
                    )
        except Exception:
            await self._finish_case(case_id, "block", "confirmed_and_kick_succeeded_retraction_window_read_failed")
            await self._send_final_result(
                case_id, recipient, "已确认踢出成功；撤回窗口读取失败，未能执行撤回。",
                scope, revision, generation,
            )
            return

        delete_ids = list(dict.fromkeys(str(row.get("message_id") or "") for row in delete_messages))
        delete_counts = {"ok": 0, "failed": 0, "unknown": 0}
        messages_by_id = {str(row.get("message_id") or ""): row for row in delete_messages}
        for message_id in reversed(delete_ids):
            row = messages_by_id.get(message_id)
            if not row or int(row.get("self_id") or 0) != self_id or int(row.get("group_id") or 0) != group_id or int(row.get("user_id") or 0) != user_id:
                continue
            parsed = _message_id(message_id)
            if parsed is None or not self._is_current(scope, revision, generation):
                break
            result, _ = await self._write_once(
                case_id, f"delete:{parsed}", lambda parsed=parsed: self.api.delete_msg(int(parsed)),
                {}, scope, revision, generation,
            )
            delete_counts[result] += 1
        await self._finish_case(case_id, "block", "confirmed_and_kick_succeeded")
        summary = (
            f"已确认踢出成功；撤回成功 {delete_counts['ok']} 条，失败 {delete_counts['failed']} 条，"
            f"结果未确认 {delete_counts['unknown']} 条。"
        )
        await self._send_final_result(case_id, recipient, summary, scope, revision, generation)

    async def _write_once(self, case_id: str, effect_key: str, call, details: dict, scope, revision, generation):
        if not self._is_current(scope, revision, generation):
            return "unknown", None
        try:
            attempted = await asyncio.to_thread(self.store.mark_effect_attempted, case_id, effect_key, details)
        except Exception:
            return "unknown", None
        if not attempted or not self._is_current(scope, revision, generation):
            if attempted:
                try:
                    await asyncio.to_thread(self.store.mark_effect_result, case_id, effect_key, "interrupted", {})
                except Exception:
                    pass
            return "unknown", None
        try:
            response = await call()
            outcome = api_outcome(response)
        except asyncio.CancelledError:
            raise
        except Exception:
            response = None
            outcome = "unknown"
        response_details = {}
        if isinstance(response, dict):
            response_details = {"retcode": str(response.get("retcode", ""))[:20], "status": str(response.get("status", ""))[:20]}
            data = response.get("data")
            if isinstance(data, dict) and data.get("message_id") is not None:
                response_details["message_id"] = str(data.get("message_id"))[:32]
        try:
            await asyncio.to_thread(self.store.mark_effect_result, case_id, effect_key, outcome, response_details)
        except Exception:
            return "unknown", response
        return outcome, response

    async def _send_final_result(self, case_id: str, recipient: int, text: str, scope, revision: int, generation: int) -> None:
        if not self._is_current(scope, revision, generation):
            return
        await self._write_once(
            case_id, "final_result",
            lambda: self.api.send_private_msg(recipient, text), {}, scope, revision, generation,
        )

    @staticmethod
    def _group_notice(user_id: int, category: str) -> str:
        reasons = {
            "unsolicited_ad": "发布广告推广信息",
            "deceptive_group_redirect": "发布冒充管理通知的转群引导",
            "suspected_fraud": "发布具有明确欺骗性引流特征的信息",
        }
        reason = reasons.get(category, "发布违反本群管理规则的信息")
        return f"成员 QQ：{int(user_id)} 因{reason}，将被移出本群。\n如认为踢出错误请找管理员申诉"

    @staticmethod
    def _message_time(timestamp: float) -> str:
        try:
            return datetime.fromtimestamp(float(timestamp), ZoneInfo(str(TIMEZONE))).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _split_text(text: str, size: int = 16000) -> list[str]:
        if len(text) <= size:
            return [text]
        return [text[index:index + size] for index in range(0, len(text), size)]

    def _original_nodes(self, user_id: int, messages: list[dict]) -> list[list[dict]]:
        message_nodes: list[list[dict]] = []
        for row in messages:
            name = str(row.get("card") or row.get("nickname") or user_id)[:128]
            message_id = str(row["message_id"])
            prefix_base = f"[原记录 {self._message_time(float(row['occurred_at']))}；message_id={message_id}"
            content_segments: list[dict] = []
            for segment in row.get("segments") or []:
                if not isinstance(segment, dict):
                    raise ValueError("invalid original segment")
                kind = str(segment.get("type") or "").lower()
                data = segment.get("data")
                if not isinstance(data, dict):
                    raise ValueError("invalid original segment data")
                if kind == "at":
                    target = str(data.get("qq") or data.get("user_id") or "")
                    if not target:
                        raise ValueError("incomplete original at segment")
                    content_segments.append({"type": "text", "data": {"text": f"（原@对象 QQ:{target}，未触发通知）"}})
                elif kind == "reply":
                    reply_id = str(data.get("id") or data.get("message_id") or "")
                    if not reply_id:
                        raise ValueError("incomplete original reply segment")
                    content_segments.append({"type": "text", "data": {"text": f"（原引用消息 ID:{reply_id}）"}})
                elif kind not in _FORWARD_SEGMENTS:
                    raise ValueError("unsupported original segment")
                elif kind == "text":
                    raw_text = data.get("text")
                    if raw_text is not None and not isinstance(raw_text, str):
                        raise ValueError("invalid original text segment")
                    text = raw_text or ""
                    for part in self._split_text(text, 8000):
                        content_segments.append({"type": "text", "data": {"text": part}})
                else:
                    content_segments.append({"type": kind, "data": data})

            def make_node(part_content: list[dict], part_number: int, total_parts: int) -> dict:
                suffix = f"；part {part_number}/{total_parts}" if total_parts > 1 else ""
                prefix = f"{prefix_base}{suffix}]\n"
                content = [{"type": "text", "data": {"text": prefix}}, *part_content]
                return {"type": "node", "data": {"uin": str(int(user_id)), "name": name, "content": content}}

            parts: list[list[dict]] = []
            current: list[dict] = []
            for segment in content_segments:
                candidate = current + [segment]
                probe = make_node(candidate, 999, 999)
                size = len(json.dumps(probe, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
                if current and size > MAX_FORWARD_PART_BYTES:
                    parts.append(current)
                    current = [segment]
                    probe = make_node(current, 999, 999)
                    size = len(json.dumps(probe, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
                else:
                    current = candidate
                if size > MAX_FORWARD_PART_BYTES:
                    raise ValueError("single original segment exceeds payload limit")
            if current or not parts:
                parts.append(current)
            nodes = [make_node(part, index, len(parts)) for index, part in enumerate(parts, start=1)]
            if any(
                len(json.dumps(node, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) > MAX_FORWARD_PART_BYTES
                for node in nodes
            ):
                raise ValueError("original message part exceeds payload limit")
            message_nodes.append(nodes)
        return message_nodes

    def _build_forward_batches(
        self, *, self_id: int, group_id: int, user_id: int, case_id: str, category: str,
        recipient: int, messages: list[dict], action_at: float, enabled_at, gap_at,
    ) -> list[list[dict]]:
        if not messages:
            raise ValueError("no evidence messages")
        message_nodes = self._original_nodes(user_id, messages)
        start_at = float(action_at) - AUDIT_WINDOW_SECONDS
        flags: list[str] = []
        if enabled_at is not None and float(enabled_at) > start_at:
            flags.append(f"首次开启后记录自 {self._message_time(float(enabled_at))} 起")
        if gap_at is not None and float(gap_at) >= start_at:
            flags.append(f"存在重启/断线/关闭造成的采集缺口（最近时间 {self._message_time(float(gap_at))}）")
        coverage = "；".join(flags) if flags else "已收到并落盘记录覆盖该时间范围"

        def header(batch_no: int, total: int) -> dict:
            text = (
                f"案件 {case_id}；群 {group_id}；目标 QQ {user_id}\n"
                f"取证范围 {self._message_time(start_at)} 至 {self._message_time(action_at)}；"
                f"类别 {category}；状态：自动处置准备。\n"
                f"原始消息 {len(messages)} 条；第 {batch_no}/{total} 批；覆盖说明：{coverage}。\n"
                "危险链接仅作为证据，请勿点击。"
            )
            return {"type": "node", "data": {"uin": str(int(self_id)), "name": "Cooper_bot", "content": [{"type": "text", "data": {"text": text}}]}}

        chunks: list[list[dict]] = []
        current: list[dict] = []
        for nodes in message_nodes:
            for node in nodes:
                candidate = current + [node]
                request = {"user_id": int(recipient), "messages": [header(999, 999), *candidate]}
                size = len(json.dumps(request, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
                if current and (len(candidate) > MAX_FORWARD_NODES or size > MAX_FORWARD_REQUEST_BYTES):
                    chunks.append(current)
                    current = [node]
                    request = {"user_id": int(recipient), "messages": [header(999, 999), node]}
                    size = len(json.dumps(request, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
                else:
                    current = candidate
                if size > MAX_FORWARD_REQUEST_BYTES:
                    raise ValueError("single message node exceeds payload limit")
        if current:
            chunks.append(current)
        total = len(chunks)
        result = []
        for index, part in enumerate(chunks, start=1):
            batch = [header(index, total), *part]
            request = {"user_id": int(recipient), "messages": batch}
            if len(part) > MAX_FORWARD_NODES or len(json.dumps(request, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) > MAX_FORWARD_REQUEST_BYTES:
                raise ValueError("forward payload exceeds limit")
            result.append(batch)
        return result
