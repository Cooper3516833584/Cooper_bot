"""统一视觉描述 Skill（slot 架构）。

架构原则：
- 图片属于聊天消息，图片描述也属于同一条聊天消息。
- 视觉元数据（VisionSlot）随 user message 保存在聊天历史中，不设独立队列。
- 机器人需要 AI 回复时，按后端窗口扫描历史中 unresolved 的 slot 补解析。
- 解析结果通过 slot_id 精确写回原消息，绝不按顺序错位回填。
- 同一图片通过 source_key / content_hash / LRU / singleflight 去重，
  尽量只调用一次视觉 API。
"""

from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import inspect
import io
import json
import re
import tempfile
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from PIL import Image, ImageDraw, UnidentifiedImageError

import config
from config import (
    BASE_DIR,
    DATA_DIR,
    VISION_API_KEY,
    VISION_BASE_URL,
    VISION_CACHE_MAX_ENTRIES,
    VISION_CACHE_TTL_SECONDS,
    VISION_CAPTURE_CONTEXT_IMAGES,
    VISION_DESCRIPTION_MAX_CHARS,
    VISION_ENABLED,
    VISION_MAX_CONCURRENCY,
    VISION_MAX_EDGE,
    VISION_MAX_IMAGE_BYTES,
    VISION_MAX_IMAGES_PER_MESSAGE,
    VISION_MODEL,
    VISION_NEGATIVE_CACHE_TTL_SECONDS,
    VISION_TIMEOUT_SECONDS,
)

try:
    from openai import AsyncOpenAI
except Exception:  # pragma: no cover - optional dependency
    AsyncOpenAI = None

try:
    import aiohttp
except Exception:  # pragma: no cover - optional dependency
    aiohttp = None


# ============ 数据结构 ============


@dataclass
class VisualSegment:
    index: int
    segment_type: str
    url: str = ""
    file: str = ""
    file_id: str = ""
    path: str = ""
    name: str = ""
    summary: str = ""
    face_id: str = ""


@dataclass
class VisionSlot:
    """一条视觉内容的持久状态，随 user message 保存在聊天历史中。"""

    slot_id: str
    index: int
    segment_type: str

    status: str = "unresolved"

    url: str = ""
    file: str = ""
    file_id: str = ""
    path: str = ""

    name: str = ""
    summary: str = ""
    face_id: str = ""

    source_key: str = ""
    content_hash: str = ""

    description: str = ""

    retry_after_ts: float = 0.0

    source_kind: str = "message"  # message | reply_reference（仅内部调试用）

    def to_dict(self) -> dict:
        return {
            "slot_id": self.slot_id,
            "index": self.index,
            "segment_type": self.segment_type,
            "status": self.status,
            "url": self.url,
            "file": self.file,
            "file_id": self.file_id,
            "path": self.path,
            "name": self.name,
            "summary": self.summary,
            "face_id": self.face_id,
            "source_key": self.source_key,
            "content_hash": self.content_hash,
            "description": self.description,
            "retry_after_ts": float(self.retry_after_ts or 0.0),
            "source_kind": self.source_kind,
        }


@dataclass
class VisionResolution:
    """一次视觉解析结果，按 slot_id 精确回填。"""

    slot_id: str
    status: str

    description: str = ""

    source_key: str = ""
    content_hash: str = ""

    retry_after_ts: float = 0.0

    cache_hit: bool = False


# ============ 常量 ============

QQ_FACE_NAMES = {
    "0": "惊讶",
    "1": "撇嘴",
    "2": "色",
    "3": "发呆",
    "4": "得意",
    "5": "流泪",
    "6": "害羞",
    "14": "微笑",
    "32": "疑问",
}

_VISION_PROMPT_VERSION = "v1"

_VISION_SYSTEM_PROMPT = """你是聊天图片描述器。

请分析输入图片，生成供另一个纯文本聊天模型理解的描述。

要求：
1. 判断类型：照片、截图、聊天截图、表情包、漫画、动图帧或其他。
2. 描述主要人物、物体、动作和场景。
3. 提取清晰可见的文字。
4. 如果是表情包，说明情绪、语气和通常表达的聊天含义。
5. 不要猜测无法确认的人物身份。
6. 不要编造模糊或不可见文字。
7. 不要对图片外的上下文作推断。
8. 输出必须是 JSON。
9. 描述应简洁，适合放入聊天历史。

JSON 格式：
{
  "kind": "图片类型",
  "scene": "主要画面",
  "visible_text": "可见文字，没有则为空字符串",
  "emotion": "情绪或语气，没有则为空字符串",
  "intent": "可能表达的聊天含义，没有则为空字符串"
}"""

MAX_IMAGE_PIXELS = 20_000_000
_ANIMATED_SAMPLE_FRAMES = 3
_JPEG_QUALITY = 82

_SENSITIVE_FILENAMES = {
    "api_key.txt",
    "secrets.env",
    ".env",
    "key.txt",
    "token.txt",
}

_VISION_STATUS_READY = "ready"
_VISION_STATUS_UNRESOLVED = "unresolved"
_VISION_STATUS_RETRYABLE = "retryable_error"
_VISION_STATUS_PERMANENT = "permanent_error"

_PERMANENT_DESCRIPTION = "图片识别失败，无法确认具体内容。"
_RETRYABLE_DESCRIPTION = "图片暂时无法识别。"
_UNRESOLVED_DESCRIPTION = "图片尚未完成识别。"


def is_direct_image_source(src: str) -> bool:
    """判断图片源是否为可直接访问的 URL 或绝对本地路径。"""
    s = str(src or "").strip()
    if not s:
        return False
    if s.startswith(("http://", "https://", "file:///")):
        return True
    if s.startswith("/"):
        return True
    return re.match(r"^[A-Za-z]:[\\/]", s) is not None


def build_source_key(slot) -> str:
    """构造视觉内容的 source key，用于第一级去重（同源不重复下载/解析）。"""
    segment_type = str(getattr(slot, "segment_type", "") or "").strip().lower()
    file_id = str(getattr(slot, "file_id", "") or "").strip()
    face_id = str(getattr(slot, "face_id", "") or "").strip()
    url = str(getattr(slot, "url", "") or "").strip()
    file = str(getattr(slot, "file", "") or "").strip()

    if segment_type == "face":
        return f"face:{face_id}" if face_id else ""
    if segment_type in {"mface", "market_face"}:
        if file_id:
            return f"mface:{file_id}"
        if face_id:
            return f"mface:{face_id}"
        if url:
            return f"mface:url:{_sha256(url)}"
        return ""
    # image
    if file_id:
        return f"image:fid:{file_id}"
    if url:
        return f"image:url:{_sha256(url)}"
    if file:
        return f"image:f:{_sha256(file)}"
    return ""


def _truncate_at_punctuation(text: str, max_chars: int) -> str:
    s = str(text or "").strip()
    limit = max(1, int(max_chars))
    if len(s) <= limit:
        return s
    cut = s[:limit]
    m = re.search(r"[。；，,.!?！？;、\s](?=[^。；，,.!?！？;、\s]*$)", cut)
    if m and m.end() > limit // 2:
        return cut[: m.end()].strip()
    return cut.strip()


# ============ Skill 主类 ============


class VisionSkill:
    """统一视觉描述 Skill。

    依赖注入（便于测试）：
    - client: AsyncOpenAI 兼容客户端
    - downloader: async (url: str) -> bytes 下载器
    - clock: () -> float 时间函数
    """

    def __init__(
        self,
        log,
        *,
        client=None,
        downloader=None,
        clock=None,
    ):
        self.log = log
        self.enabled = bool(VISION_ENABLED)
        self.api_key = str(VISION_API_KEY or "").strip()
        self.base_url = str(VISION_BASE_URL or "").strip().rstrip("/")
        self.model = str(VISION_MODEL or "").strip()
        self.timeout_seconds = max(5.0, float(VISION_TIMEOUT_SECONDS or 20.0))
        self.max_images_per_message = max(1, int(VISION_MAX_IMAGES_PER_MESSAGE or 3))
        self.max_image_bytes = max(64 * 1024, int(VISION_MAX_IMAGE_BYTES or 8 * 1024 * 1024))
        self.max_edge = max(64, int(VISION_MAX_EDGE or 1024))
        self.description_max_chars = max(20, int(VISION_DESCRIPTION_MAX_CHARS or 240))
        self.max_concurrency = max(1, int(VISION_MAX_CONCURRENCY or 4))
        self.cache_max_entries = max(0, int(VISION_CACHE_MAX_ENTRIES or 0))
        self.cache_ttl = max(0.0, float(VISION_CACHE_TTL_SECONDS or 21600.0))
        self.negative_cache_ttl = max(0.0, float(VISION_NEGATIVE_CACHE_TTL_SECONDS or 60.0))
        self.capture_context_images = bool(VISION_CAPTURE_CONTEXT_IMAGES)

        self.clock = clock or time.time
        self._client = client
        self._downloader = downloader

        # 识图成本去重缓存：cache_key -> (description, created_ts)，与聊天上下文生命周期无关
        self._cache: OrderedDict[str, tuple[str, float]] = OrderedDict()
        # 负缓存：source_key hash -> 失败时间戳（短 TTL）
        self._negative_cache: OrderedDict[str, float] = OrderedDict()
        # singleflight：cache_key -> 进行中的 Task
        self._inflight_lock = asyncio.Lock()
        self._inflight: dict[str, asyncio.Task] = {}
        # 并发限制（只包视觉 API 调用）
        self._semaphore: Optional[asyncio.Semaphore] = None

    # ---------- ready ----------

    @property
    def ready(self) -> bool:
        return bool(
            self.enabled
            and self.api_key
            and self.base_url
            and self.model
            and AsyncOpenAI is not None
        )

    def _get_client(self):
        if self._client is None:
            self._client = AsyncOpenAI(
                api_key=self.api_key,
                base_url=self.base_url,
                timeout=self.timeout_seconds,
                max_retries=1,
            )
        return self._client

    def _get_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_concurrency)
        return self._semaphore

    # ---------- 段提取 ----------

    def extract_visual_segments(self, evt: dict) -> list[VisualSegment]:
        out: list[VisualSegment] = []
        seen: set[tuple] = set()
        index = 0

        def _push(segment_type: str, data: dict) -> None:
            nonlocal index
            seg = self._build_segment(index, segment_type, data)
            dedup = (seg.segment_type, seg.url, seg.file, seg.file_id, seg.path, seg.face_id)
            if dedup in seen:
                return
            seen.add(dedup)
            out.append(seg)
            index += 1

        msg = evt.get("message")
        if isinstance(msg, list):
            for seg in msg:
                if not isinstance(seg, dict):
                    continue
                tp = str(seg.get("type") or "").strip().lower()
                if tp not in {"image", "mface", "market_face", "face"}:
                    continue
                data = seg.get("data") or {}
                if not isinstance(data, dict):
                    data = {}
                _push(tp, data)
        elif isinstance(msg, dict):
            tp = str(msg.get("type") or "").strip().lower()
            if tp in {"image", "mface", "market_face", "face"}:
                data = msg.get("data") or {}
                _push(tp, data if isinstance(data, dict) else {})

        raw_values: list[str] = []
        raw = evt.get("raw_message")
        if isinstance(raw, str) and raw.strip():
            raw_values.append(raw)
        if isinstance(msg, str) and msg.strip():
            raw_values.append(msg)
        for raw_value in raw_values:
            for tp, payload in _iter_cq_segments(raw_value):
                if tp not in {"image", "mface", "market_face", "face"}:
                    continue
                _push(tp, _parse_cq_kvs(payload))

        return out

    @staticmethod
    def _build_segment(index: int, segment_type: str, data: dict) -> VisualSegment:
        url = _normalize_src(data.get("url") or data.get("download_url") or data.get("file_url") or "")
        file = _normalize_src(data.get("file") or "")
        file_id = str(data.get("file_id") or data.get("id") or "").strip()
        path = _normalize_src(data.get("path") or data.get("file_path") or "")
        summary = str(data.get("summary") or data.get("text") or "").strip()
        face_id = str(data.get("id") or data.get("face_id") or "").strip()
        name = str(data.get("name") or data.get("file") or "").strip()
        return VisualSegment(
            index=index,
            segment_type=str(segment_type or "").lower(),
            url=url,
            file=file,
            file_id=file_id,
            path=path,
            name=name,
            summary=summary,
            face_id=face_id,
        )

    # ---------- slot 创建 ----------

    def _make_slot_id(self, message_id: str, display_index: int) -> str:
        mid = str(message_id or "").strip()
        if mid:
            return f"{mid}:{int(display_index)}"
        return f"local:{int(time.time() * 1000)}:{int(display_index)}:{uuid.uuid4().hex[:8]}"

    def create_slots_from_event(
        self,
        evt: dict,
        *,
        message_id: str = "",
        source_kind: str = "message",
    ) -> list[VisionSlot]:
        """从消息事件提取视觉段并创建 VisionSlot 列表（不调用视觉 API）。"""
        if not message_id:
            message_id = str(evt.get("message_id") or "").strip()
        segments = self.extract_visual_segments(evt)
        slots: list[VisionSlot] = []
        for display_index, segment in enumerate(segments, start=1):
            if display_index > self.max_images_per_message:
                break
            slot = VisionSlot(
                slot_id=self._make_slot_id(message_id, display_index),
                index=display_index,
                segment_type=segment.segment_type,
                url=segment.url,
                file=segment.file,
                file_id=segment.file_id,
                path=segment.path,
                name=segment.name,
                summary=segment.summary,
                face_id=segment.face_id,
                source_kind=source_kind,
            )
            self._initialize_local_slot(slot)
            slots.append(slot)
        return slots

    def _initialize_local_slot(self, slot: VisionSlot) -> None:
        """face / 无图的 mface 本地直接完成，不调用视觉 API。"""
        if slot.segment_type == "face":
            face_id = str(slot.face_id or "").strip()
            name = QQ_FACE_NAMES.get(face_id)
            if name:
                slot.description = f"[QQ表情：{name}]"
            else:
                slot.description = f"[QQ内置表情，ID：{face_id}]" if face_id else "[QQ内置表情]"
            slot.status = _VISION_STATUS_READY
            slot.source_key = build_source_key(slot)
            return
        if slot.segment_type in {"mface", "market_face"} and not self._slot_has_image_source(slot):
            summary = str(slot.summary or "").strip()
            if summary:
                slot.description = f"[商城表情：{summary}]"
            elif slot.face_id:
                slot.description = f"[商城表情，ID：{slot.face_id}，具体画面不可用]"
            else:
                slot.description = "[商城表情，具体画面不可用]"
            slot.status = _VISION_STATUS_READY
            slot.source_key = build_source_key(slot)
            return

    @staticmethod
    def _slot_has_image_source(slot) -> bool:
        return bool(slot.url or slot.file or slot.file_id or slot.path)

    # ---------- 源解析 ----------

    async def resolve_segment_source(self, api, slot) -> str:
        """按优先级解析图片源：url / path / file 直接源 → get_image → get_file。"""
        for key in ("url", "path", "file"):
            src = _normalize_src(str(getattr(slot, key) or ""))
            if src and (key == "url" or is_direct_image_source(src)):
                return src

        call = getattr(api, "call", None)
        tried: set[str] = set()
        for token in (slot.file, slot.file_id):
            t = str(token or "").strip()
            if not t or t in tried:
                continue
            tried.add(t)
            if callable(call):
                try:
                    resp = await call("get_image", {"file": t}, timeout=max(15.0, self.timeout_seconds))
                except Exception:
                    resp = None
                src = _extract_src_from_resp(resp)
                if src:
                    return src

        get_file = getattr(api, "get_file", None)
        for token in (slot.file_id, slot.file):
            t = str(token or "").strip()
            if not t or not callable(get_file):
                continue
            try:
                resp = await get_file(t, timeout=max(30.0, self.timeout_seconds), retries=1, retry_delay=1.0)
            except Exception:
                resp = None
            src = _extract_src_from_resp(resp)
            if src:
                return src
        return ""

    # ---------- 解析入口 ----------

    async def resolve_slots(
        self,
        api,
        slots: list,
    ) -> list[VisionResolution]:
        """统一解析一批 slots（历史补解析 + 当前消息）。

        按 source_key 分组去重，每组只解析一个代表，结果复制给同组 slot。
        """
        normalized = [self._coerce_slot(s) for s in (slots or [])]
        now = float(self.clock())

        resolutions: list[VisionResolution] = []
        work: list[VisionSlot] = []

        for slot in normalized:
            if slot.status == _VISION_STATUS_READY:
                continue
            if slot.status == _VISION_STATUS_PERMANENT:
                continue
            if slot.status == _VISION_STATUS_RETRYABLE and now < float(slot.retry_after_ts or 0.0):
                continue
            if slot.segment_type == "face":
                self._initialize_local_slot(slot)
                resolutions.append(
                    VisionResolution(
                        slot_id=slot.slot_id,
                        status=_VISION_STATUS_READY,
                        description=slot.description,
                        source_key=slot.source_key,
                    )
                )
                continue
            if slot.segment_type in {"mface", "market_face"} and not self._slot_has_image_source(slot):
                self._initialize_local_slot(slot)
                resolutions.append(
                    VisionResolution(
                        slot_id=slot.slot_id,
                        status=_VISION_STATUS_READY,
                        description=slot.description,
                        source_key=slot.source_key,
                    )
                )
                continue
            work.append(slot)

        # 负缓存过滤：source_key 命中短时负缓存 → 直接 retryable
        still_work: list[VisionSlot] = []
        for slot in work:
            sk = build_source_key(slot)
            if sk and self._is_negative_source(sk):
                resolutions.append(
                    VisionResolution(
                        slot_id=slot.slot_id,
                        status=_VISION_STATUS_RETRYABLE,
                        retry_after_ts=now + self.negative_cache_ttl,
                        source_key=sk,
                    )
                )
                continue
            still_work.append(slot)
        work = still_work

        # source_key 分组（第一级去重：同源只解析一个代表）
        groups: dict[str, list[VisionSlot]] = {}
        for slot in work:
            key = build_source_key(slot) or f"slot:{slot.slot_id}"
            groups.setdefault(key, []).append(slot)

        async def _resolve_group(group: list[VisionSlot]) -> list[VisionResolution]:
            rep = group[0]
            rep_res = await self._resolve_one(api, rep)
            out: list[VisionResolution] = []
            for slot in group:
                if slot is rep:
                    out.append(rep_res)
                else:
                    out.append(_copy_resolution(rep_res, slot.slot_id))
            return out

        # 不同 source group 真正并发处理；视觉 API 并发由 semaphore 限制
        group_results = await asyncio.gather(
            *[_resolve_group(group) for group in groups.values()],
            return_exceptions=True,
        )
        for group, result in zip(groups.values(), group_results):
            if isinstance(result, Exception):
                # 防御：group 任务异常不拖垮整批
                for slot in group:
                    resolutions.append(
                        VisionResolution(
                            slot_id=slot.slot_id,
                            status=_VISION_STATUS_RETRYABLE,
                            description=_RETRYABLE_DESCRIPTION,
                            source_key=build_source_key(slot),
                            retry_after_ts=now + self.negative_cache_ttl,
                        )
                    )
                continue
            resolutions.extend(result)

        # 保持输入顺序输出（不依赖 gather 顺序）
        resolution_map = {r.slot_id: r for r in resolutions}
        ordered: list[VisionResolution] = []
        for slot in normalized:
            result = resolution_map.get(slot.slot_id)
            if result is not None:
                ordered.append(result)

        self.log.info(
            f"vision resolve complete: targets={len(normalized)} "
            f"resolved={sum(1 for r in ordered if r.status == _VISION_STATUS_READY)} "
            f"retryable={sum(1 for r in ordered if r.status == _VISION_STATUS_RETRYABLE)} "
            f"permanent={sum(1 for r in ordered if r.status == _VISION_STATUS_PERMANENT)}"
        )
        return ordered

    async def _resolve_one(self, api, slot: VisionSlot) -> VisionResolution:
        source_key = build_source_key(slot)
        try:
            src = await self.resolve_segment_source(api, slot)
            if not src:
                raise _VisionDownloadError("no source")
            data_url, norm_bytes = await self._download_and_normalize(src)
            content_hash = hashlib.sha256(norm_bytes).hexdigest()
            cache_key = self._cache_key_for_content(content_hash)

            cached = self._get_cache(cache_key)
            if cached is not None:
                return VisionResolution(
                    slot_id=slot.slot_id,
                    status=_VISION_STATUS_READY,
                    description=cached,
                    source_key=source_key,
                    content_hash=content_hash,
                    cache_hit=True,
                )

            desc, _hit = await self._describe_with_singleflight(cache_key, data_url)
            if not desc:
                raise RuntimeError("empty vision description")
            return VisionResolution(
                slot_id=slot.slot_id,
                status=_VISION_STATUS_READY,
                description=desc,
                source_key=source_key,
                content_hash=content_hash,
            )
        except _PermanentVisionError:
            return VisionResolution(
                slot_id=slot.slot_id,
                status=_VISION_STATUS_PERMANENT,
                description=_PERMANENT_DESCRIPTION,
                source_key=source_key,
            )
        except Exception:
            self._record_negative_source(source_key)
            return VisionResolution(
                slot_id=slot.slot_id,
                status=_VISION_STATUS_RETRYABLE,
                description=_RETRYABLE_DESCRIPTION,
                source_key=source_key,
                retry_after_ts=float(self.clock()) + self.negative_cache_ttl,
            )

    def apply_resolutions_to_slots(
        self,
        slots: list,
        resolutions: list,
    ) -> list[VisionSlot]:
        """把解析结果应用到（当前消息的）slots，返回更新后的新列表。"""
        result_map = {str(r.slot_id): r for r in (resolutions or [])}
        out: list[VisionSlot] = []
        for slot in slots:
            slot = self._coerce_slot(slot)
            r = result_map.get(slot.slot_id)
            if r is not None:
                _apply_resolution_to_slot(slot, r)
            out.append(slot)
        return out

    @staticmethod
    def _coerce_slot(item) -> VisionSlot:
        if isinstance(item, VisionSlot):
            return item
        if isinstance(item, dict):
            return VisionSlot(
                slot_id=str(item.get("slot_id") or ""),
                index=int(item.get("index") or 1),
                segment_type=str(item.get("segment_type") or "image"),
                status=str(item.get("status") or _VISION_STATUS_UNRESOLVED),
                url=str(item.get("url") or ""),
                file=str(item.get("file") or ""),
                file_id=str(item.get("file_id") or ""),
                path=str(item.get("path") or ""),
                name=str(item.get("name") or ""),
                summary=str(item.get("summary") or ""),
                face_id=str(item.get("face_id") or ""),
                source_key=str(item.get("source_key") or ""),
                content_hash=str(item.get("content_hash") or ""),
                description=str(item.get("description") or ""),
                retry_after_ts=float(item.get("retry_after_ts") or 0.0),
                source_kind=str(item.get("source_kind") or "message"),
            )
        raise TypeError(f"unsupported slot type: {type(item).__name__}")

    # ---------- singleflight + 并发限制 ----------

    async def _describe_with_singleflight(self, cache_key: str, data_url: str) -> tuple[str, bool]:
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached, True

        async def _call_and_cache() -> str:
            sem = self._get_semaphore()
            async with sem:
                raw = await self._call_vision_api(data_url)
            desc = self._format_vision_text(raw)
            if not desc:
                raise RuntimeError("empty vision description")
            self._put_cache(cache_key, desc)
            return desc

        desc = await self._get_or_create_inflight(cache_key, _call_and_cache)
        return desc, False

    async def _get_or_create_inflight(self, cache_key: str, factory) -> str:
        async with self._inflight_lock:
            task = self._inflight.get(cache_key)
            if task is None:
                task = asyncio.create_task(factory())
                self._inflight[cache_key] = task
        try:
            return await task
        finally:
            if task.done():
                async with self._inflight_lock:
                    if self._inflight.get(cache_key) is task:
                        self._inflight.pop(cache_key, None)

    # ---------- 下载与预处理 ----------

    async def _download_and_normalize(self, src: str) -> tuple[str, bytes]:
        if src.startswith("http://") or src.startswith("https://"):
            raw = await self._fetch_bytes(src)
        elif src.startswith("file:///"):
            p = Path(src[len("file:///"):])
            raw = self._read_allowed_local_file(p)
        elif is_direct_image_source(src):
            p = Path(src)
            raw = self._read_allowed_local_file(p)
        else:
            raise _VisionDownloadError("unsupported source")

        if not raw:
            raise _VisionDownloadError("empty image")
        if len(raw) > self.max_image_bytes:
            raise _PermanentVisionError("image too large")

        # Pillow 预处理是同步 CPU 操作，丢到线程池避免阻塞事件循环
        return await asyncio.to_thread(self._normalize_image_bytes, raw)

    async def _fetch_bytes(self, url: str) -> bytes:
        if self._downloader is not None:
            result = self._downloader(url)
            if inspect.isawaitable(result):
                data = await result
            else:
                data = result
            if not data:
                raise _VisionDownloadError("empty download")
            raw = bytes(data)
            if len(raw) > self.max_image_bytes:
                raise _PermanentVisionError("image too large")
            return raw
        if aiohttp is None:
            return await asyncio.to_thread(self._fetch_bytes_urllib_sync, url)
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "image/*"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout_seconds)) as session:
            async with session.get(url, headers=headers, allow_redirects=False) as resp:
                if resp.status >= 400:
                    raise _VisionDownloadError(f"http {resp.status}")
                content_length = resp.content_length
                if content_length is not None and int(content_length) > self.max_image_bytes:
                    raise _PermanentVisionError("image too large")
                chunks: list[bytes] = []
                total = 0
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    total += len(chunk)
                    if total > self.max_image_bytes:
                        raise _PermanentVisionError("image too large")
                    chunks.append(chunk)
                if not chunks:
                    raise _VisionDownloadError("empty body")
                return b"".join(chunks)

    def _fetch_bytes_urllib_sync(self, url: str) -> bytes:
        import urllib.request

        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "image/*"})
        with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
            data = resp.read(self.max_image_bytes + 1)
        if len(data) > self.max_image_bytes:
            raise _PermanentVisionError("image too large")
        return data

    def _read_allowed_local_file(self, path: Path) -> bytes:
        resolved = Path(path).resolve()
        if not self._is_allowed_local_path(resolved):
            raise _PermanentVisionError("local path not allowed")
        try:
            data = resolved.read_bytes()
        except Exception:
            raise _VisionDownloadError("local read failed")
        return data

    def _is_allowed_local_path(self, path: Path) -> bool:
        low = str(path).lower()
        for sensitive in _SENSITIVE_FILENAMES:
            if sensitive in low:
                return False
        for allowed in self._allowed_local_roots():
            try:
                if path.is_relative_to(allowed):
                    return True
            except Exception:
                try:
                    path.relative_to(allowed)
                    return True
                except Exception:
                    continue
        return False

    @staticmethod
    def _allowed_local_roots() -> list[Path]:
        roots: list[Path] = []
        for d in (
            BASE_DIR / "ocr",
            DATA_DIR / "ocr",
            DATA_DIR / "temp",
            BASE_DIR / "data" / "ocr",
            BASE_DIR / "data" / "temp",
        ):
            try:
                roots.append(d.resolve())
            except Exception:
                continue
        try:
            roots.append(Path(tempfile.gettempdir()).resolve())
        except Exception:
            pass
        return roots

    def _normalize_image_bytes(self, raw: bytes) -> tuple[str, bytes]:
        try:
            with Image.open(io.BytesIO(raw)) as im:
                if getattr(im, "n_frames", 1) > 1:
                    data_url, norm_bytes = self._normalize_animated(im)
                else:
                    data_url, norm_bytes = self._normalize_static(im)
        except (UnidentifiedImageError, OSError, ValueError):
            raise _PermanentVisionError("invalid image")
        except Image.DecompressionBombError:
            raise _PermanentVisionError("decompression bomb")
        return data_url, norm_bytes

    def _normalize_static(self, im: Image.Image) -> tuple[str, bytes]:
        width, height = im.size
        if int(width) * int(height) > MAX_IMAGE_PIXELS:
            raise _PermanentVisionError("too many pixels")
        frame = im.convert("RGB")
        frame.thumbnail((self.max_edge, self.max_edge), Image.LANCZOS)
        buf = io.BytesIO()
        frame.save(buf, format="JPEG", quality=_JPEG_QUALITY, optimize=True)
        norm_bytes = buf.getvalue()
        return _to_data_url(norm_bytes, "image/jpeg"), norm_bytes

    def _normalize_animated(self, im: Image.Image) -> tuple[str, bytes]:
        total = max(1, int(getattr(im, "n_frames", 1)))
        frame_indexes = _sample_frame_indexes(total, _ANIMATED_SAMPLE_FRAMES)
        frames: list[Image.Image] = []
        for idx in frame_indexes:
            try:
                im.seek(idx)
            except Exception:
                continue
            frame = im.convert("RGB")
            frame.thumbnail((self.max_edge // _ANIMATED_SAMPLE_FRAMES, self.max_edge), Image.LANCZOS)
            frames.append(frame)
        if not frames:
            raise _PermanentVisionError("no frames")
        frame_w, frame_h = frames[0].size
        sheet = Image.new("RGB", (frame_w * len(frames), frame_h), (255, 255, 255))
        draw = ImageDraw.Draw(sheet)
        for i, frame in enumerate(frames):
            sheet.paste(frame, (i * frame_w, 0))
            draw.text((i * frame_w + 4, 4), f"帧{i + 1}", fill=(255, 0, 0))
        buf = io.BytesIO()
        sheet.save(buf, format="JPEG", quality=_JPEG_QUALITY, optimize=True)
        norm_bytes = buf.getvalue()
        return _to_data_url(norm_bytes, "image/jpeg"), norm_bytes

    # ---------- API 调用 ----------

    async def _call_vision_api(self, data_url: str) -> str:
        client = self._get_client()
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": _VISION_SYSTEM_PROMPT},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            }
        ]
        kwargs: dict = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.0,
            "max_tokens": 220,
        }
        try:
            completion = await client.chat.completions.create(**kwargs)
        except TypeError:
            kwargs.pop("max_tokens", None)
            completion = await client.chat.completions.create(**kwargs)
        try:
            return str(completion.choices[0].message.content or "").strip()
        except Exception:
            return ""

    def _format_vision_text(self, raw: str) -> str:
        obj = _parse_vision_json(raw)
        if obj is None:
            return ""
        parts: list[str] = []
        kind = str(obj.get("kind") or "").strip()
        scene = str(obj.get("scene") or "").strip()
        visible_text = str(obj.get("visible_text") or "").strip()
        emotion = str(obj.get("emotion") or "").strip()
        intent = str(obj.get("intent") or "").strip()

        if kind:
            parts.append(f"类型：{kind}")
        if scene:
            parts.append(f"画面：{scene}")
        if visible_text:
            parts.append(f"文字：{visible_text}")
        if emotion or intent:
            meaning = "，".join(x for x in [emotion, intent] if x)
            parts.append(f"语气/含义：{meaning}")
        result = "；".join(parts)
        return _truncate_at_punctuation(result, self.description_max_chars)

    # ---------- 缓存 ----------

    def _cache_key_for_content(self, content_hash: str) -> str:
        h = hashlib.sha256()
        h.update(str(content_hash or "").encode("utf-8", errors="ignore"))
        h.update(b"|")
        h.update(self.model.encode("utf-8", errors="ignore"))
        h.update(b"|")
        h.update(_VISION_PROMPT_VERSION.encode("utf-8", errors="ignore"))
        return h.hexdigest()

    def _get_cache(self, key: str) -> Optional[str]:
        if self.cache_max_entries <= 0:
            return None
        entry = self._cache.get(key)
        if entry is None:
            return None
        desc, created_ts = entry
        if self.cache_ttl > 0 and (self.clock() - created_ts) > self.cache_ttl:
            self._cache.pop(key, None)
            return None
        self._cache.move_to_end(key)
        return desc

    def _put_cache(self, key: str, desc: str) -> None:
        if self.cache_max_entries <= 0:
            return
        self._cache[key] = (desc, self.clock())
        self._cache.move_to_end(key)
        while len(self._cache) > self.cache_max_entries:
            self._cache.popitem(last=False)

    def _negative_source_key(self, source_key: str) -> str:
        return "s:" + hashlib.sha256(str(source_key or "").encode("utf-8", errors="ignore")).hexdigest()[:16]

    def _record_negative_source(self, source_key: str) -> None:
        key = self._negative_source_key(source_key)
        if not key or self.negative_cache_ttl <= 0:
            return
        self._negative_cache[key] = self.clock()
        self._negative_cache.move_to_end(key)
        while len(self._negative_cache) > max(64, self.cache_max_entries):
            self._negative_cache.popitem(last=False)

    def _is_negative_source(self, source_key: str) -> bool:
        key = self._negative_source_key(source_key)
        if not key:
            return False
        ts = self._negative_cache.get(key)
        if ts is None:
            return False
        if (self.clock() - ts) <= self.negative_cache_ttl:
            return True
        self._negative_cache.pop(key, None)
        return False


# ============ 内部工具 ============


class _VisionError(Exception):
    pass


class _VisionDownloadError(_VisionError):
    """可重试错误：网络/超时/上游临时失败。"""


class _PermanentVisionError(_VisionError):
    """永久错误：非法路径/非图片/像素超限等，不再重试。"""


def _copy_resolution(resolution: VisionResolution, slot_id: str) -> VisionResolution:
    return VisionResolution(
        slot_id=slot_id,
        status=resolution.status,
        description=resolution.description,
        source_key=resolution.source_key,
        content_hash=resolution.content_hash,
        retry_after_ts=resolution.retry_after_ts,
        cache_hit=resolution.cache_hit,
    )


def _apply_resolution_to_slot(slot: VisionSlot, resolution: VisionResolution) -> None:
    """把解析结果应用到 slot；成功后清空临时图片 source。"""
    slot.status = resolution.status
    if resolution.description:
        slot.description = resolution.description
    if resolution.source_key:
        slot.source_key = resolution.source_key
    if resolution.content_hash:
        slot.content_hash = resolution.content_hash
    if resolution.retry_after_ts:
        slot.retry_after_ts = float(resolution.retry_after_ts)
    if resolution.status == _VISION_STATUS_READY:
        # 成功后释放临时图片 source，避免 QQ 临时 URL 长期留在聊天上下文
        slot.url = ""
        slot.file = ""
        slot.file_id = ""
        slot.path = ""


def _normalize_src(raw: str) -> str:
    import html as _html

    return _html.unescape(str(raw or "").strip()).replace("&amp;", "&")


def _parse_cq_kvs(raw: str) -> dict[str, str]:
    import html as _html

    data: dict[str, str] = {}
    for kv in str(raw or "").split(","):
        if "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        key = str(k).strip().lower()
        if not key:
            continue
        data[key] = _html.unescape(str(v).strip())
    return data


_CQ_SEG_RE = re.compile(r"\[CQ:([a-zA-Z0-9_]+)(?:,([^\]]*))?\]")


def _iter_cq_segments(text: str):
    for m in _CQ_SEG_RE.finditer(str(text or "")):
        yield m.group(1).lower(), m.group(2) or ""


def _extract_src_from_resp(resp: Optional[dict]) -> str:
    if not resp or resp.get("status") != "ok":
        return ""
    data = resp.get("data")
    if isinstance(data, str):
        return _normalize_src(data)
    data = data or {}
    return _normalize_src(
        data.get("url")
        or data.get("download_url")
        or data.get("file")
        or data.get("file_path")
        or data.get("path")
        or ""
    )


def _sample_frame_indexes(total: int, count: int) -> list[int]:
    if total <= 1:
        return [0]
    if total <= count:
        return list(range(total))
    if count <= 1:
        return [0]
    return [0, total // 2, total - 1][:count]


def _to_data_url(data: bytes, mime: str) -> str:
    return f"data:{mime};base64,{base64.b64encode(data).decode('ascii')}"


def _parse_vision_json(raw: str) -> Optional[dict]:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _sha256(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8", errors="ignore")).hexdigest()
