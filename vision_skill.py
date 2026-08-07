"""统一视觉描述 Skill。

对 OneBot 消息中的图片/表情段做一次统一识别，生成紧凑文本描述，
供 DeepSeek / Gemini / Claude 等纯文本聊天模型复用。

设计要点：
- 视觉处理发生在 AI 后端选择之前，三个后端共用同一份描述。
- 图片描述内联到对应 user message 写入共享聊天历史。
- 原始图片识别完成后立即释放，不长期落盘。
- 同图通过内存 LRU 缓存去重，避免重复调用视觉 API。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import io
import json
import re
import tempfile
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable, Optional

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
class VisualDescription:
    index: int
    segment_type: str
    description: str
    cache_key: str = ""
    cache_hit: bool = False


@dataclass
class VisionContext:
    descriptions: list[VisualDescription] = field(default_factory=list)
    failed_count: int = 0
    skipped_count: int = 0

    @property
    def has_visual(self) -> bool:
        return bool(self.descriptions or self.failed_count or self.skipped_count)

    @property
    def has_valid_description(self) -> bool:
        return bool(self.descriptions)

    def to_context_block(self) -> str:
        lines: list[str] = []

        for item in sorted(self.descriptions, key=lambda x: x.index):
            lines.append(f"[视觉内容{item.index}] {item.description}")

        for _ in range(self.failed_count):
            lines.append("[视觉内容] 图片识别失败，无法确认具体内容。")

        for _ in range(self.skipped_count):
            lines.append("[视觉内容] 图片数量超过单条消息处理上限，未继续分析。")

        return "\n".join(lines).strip()


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

_DYNAMIC_IMAGE_HINT = "这是动态图片的多个采样帧，请结合动作变化描述表情或含义。"

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


def compose_ai_context_text(text: str, vision_context: VisionContext) -> str:
    """原始文本 + 视觉描述块拼接为 AI 上下文文本。"""
    text_part = str(text or "").strip()
    visual_part = vision_context.to_context_block()

    parts: list[str] = []
    if text_part:
        parts.append(text_part)
    if visual_part:
        parts.append(visual_part)
    return "\n\n".join(parts).strip()


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
        self.max_concurrency = max(1, int(VISION_MAX_CONCURRENCY or 4))  # 预留：当前为全并发，未做限流
        self.cache_max_entries = max(0, int(VISION_CACHE_MAX_ENTRIES or 0))
        self.cache_ttl = max(0.0, float(VISION_CACHE_TTL_SECONDS or 21600.0))
        self.negative_cache_ttl = max(0.0, float(VISION_NEGATIVE_CACHE_TTL_SECONDS or 60.0))
        self.capture_context_images = bool(VISION_CAPTURE_CONTEXT_IMAGES)

        self.clock = clock or time.time
        self._client = client
        self._downloader = downloader

        # 内存成本去重缓存：key -> (description, created_ts)
        self._cache: OrderedDict[str, tuple[str, float]] = OrderedDict()
        # 负缓存：key -> 失败时间戳（短 TTL）
        self._negative_cache: OrderedDict[str, float] = OrderedDict()

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

    # ---------- 对外入口 ----------

    async def describe_event(
        self,
        api,
        evt: dict,
        segments: Optional[list[VisualSegment]] = None,
        *,
        limit: Optional[int] = None,
    ) -> VisionContext:
        if segments is None:
            segments = self.extract_visual_segments(evt)
        if not segments:
            return VisionContext()
        if limit is None:
            # 单消息路径：受 max_images_per_message 限制
            selected = segments[: self.max_images_per_message]
            skipped = len(segments) - len(selected)
        else:
            # pending 路径：调用方已控制数量，全量解析
            selected = segments[: max(0, int(limit))]
            skipped = 0

        # 全并发：有多少图就并发多少识别任务，各自独立失败互不影响
        tasks = [self.describe_segment(api, seg) for seg in selected]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        descriptions: list[VisualDescription] = []
        failed = 0
        for seg, result in zip(selected, results):
            if isinstance(result, VisualDescription):
                descriptions.append(result)
                continue
            failed += 1
            if isinstance(result, Exception):
                self._record_negative(seg)

        try:
            session_hint = f"{(evt or {}).get('group_id') or (evt or {}).get('user_id') or ''}"
        except Exception:
            session_hint = ""
        self.log.info(
            "vision describe complete: "
            f"session={session_hint} segments={len(segments)} "
            f"success={len(descriptions)} failed={failed} "
            f"cache_hit={sum(1 for d in descriptions if d.cache_hit)}"
        )
        return VisionContext(
            descriptions=descriptions,
            failed_count=failed,
            skipped_count=max(0, skipped),
        )

    async def describe_pending(self, api, pending_images: list[dict]) -> VisionContext:
        """解析会话内待识别图片列表（缓存命中自动跳过已解析图）。

        pending_images: [{url, file_id}]，由 AIService.record_pending_vision 累积。
        """
        segments: list[VisualSegment] = []
        for index, img in enumerate(pending_images or []):
            segments.append(
                VisualSegment(
                    index=index,
                    segment_type="image",
                    url=str(img.get("url") or "").strip(),
                    file_id=str(img.get("file_id") or "").strip(),
                )
            )
        return await self.describe_event(api, None, segments=segments, limit=len(segments))

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

        # OneBot 数组消息
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

        # CQ fallback（raw_message）
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

    # ---------- 源解析 ----------

    async def resolve_segment_source(self, api, segment: VisualSegment) -> str:
        """按优先级解析图片源：url / path / file 直接源 → get_image → get_file。"""
        for key in ("url", "path", "file"):
            src = _normalize_src(getattr(segment, key) or "")
            if src and (key == "url" or is_direct_image_source(src)):
                return src

        call = getattr(api, "call", None)
        tried: set[str] = set()
        for token in (segment.file, segment.file_id):
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
        for token in (segment.file_id, segment.file):
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

    # ---------- 单段识别 ----------

    async def describe_segment(self, api, segment: VisualSegment) -> Optional[VisualDescription]:
        # face：QQ 内置表情，本地映射，不调视觉 API
        if segment.segment_type == "face":
            face_id = str(segment.face_id or "").strip()
            name = QQ_FACE_NAMES.get(face_id)
            if name:
                desc = f"[QQ表情：{name}]"
            else:
                desc = f"[QQ内置表情，ID：{face_id}]" if face_id else "[QQ内置表情]"
            return VisualDescription(segment.index, segment.segment_type, desc)

        # mface / market_face：无图但有 summary 时本地描述
        if segment.segment_type in {"mface", "market_face"}:
            if not (segment.url or segment.file or segment.file_id or segment.path):
                summary = str(segment.summary or "").strip()
                if summary:
                    return VisualDescription(segment.index, segment.segment_type, f"[商城表情：{summary}]")
                face_id = str(segment.face_id or "").strip()
                if face_id:
                    return VisualDescription(
                        segment.index, segment.segment_type, f"[商城表情，ID：{face_id}，具体画面不可用]"
                    )
                return VisualDescription(
                    segment.index, segment.segment_type, "[商城表情，具体画面不可用]"
                )

        # image / 有资源的 mface：下载 → 规范化 → 调 API
        if self._is_negative(segment):
            return None

        try:
            src = await self.resolve_segment_source(api, segment)
        except Exception:
            src = ""
        if not src:
            self._record_negative(segment)
            return None

        try:
            data_url, norm_bytes = await self._download_and_normalize(src)
        except _VisionDownloadError:
            self._record_negative(segment)
            return None
        except Exception:
            self._record_negative(segment)
            return None
        if not data_url:
            self._record_negative(segment)
            return None

        cache_key = self._cache_key(norm_bytes)
        cached = self._get_cache(cache_key)
        if cached is not None:
            return VisualDescription(
                segment.index, segment.segment_type, cached, cache_key=cache_key, cache_hit=True
            )

        try:
            raw_text = await self._call_vision_api(data_url)
        except Exception:
            self._record_negative(segment)
            return None
        if not raw_text:
            self._record_negative(segment)
            return None

        desc = self._format_vision_text(raw_text)
        if not desc:
            self._record_negative(segment)
            return None
        self._put_cache(cache_key, desc)
        return VisualDescription(segment.index, segment.segment_type, desc, cache_key=cache_key)

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
            raise _VisionDownloadError("image too large")

        # Pillow 预处理是同步 CPU 操作，丢到线程池避免阻塞事件循环，保证多图真正并行
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
            return bytes(data)
        if aiohttp is None:
            # 回退 urllib（异步包装，避免阻塞事件循环）
            return await asyncio.to_thread(self._fetch_bytes_urllib_sync, url)
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "image/*"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout_seconds)) as session:
            async with session.get(url, headers=headers, allow_redirects=False) as resp:
                if resp.status >= 400:
                    raise _VisionDownloadError(f"http {resp.status}")
                body = await resp.read()
                if not body:
                    raise _VisionDownloadError("empty body")
                return body

    def _fetch_bytes_urllib_sync(self, url: str) -> bytes:
        import urllib.request

        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "image/*"})
        with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
            return resp.read(self.max_image_bytes + 1)

    def _read_allowed_local_file(self, path: Path) -> bytes:
        resolved = Path(path).resolve()
        if not self._is_allowed_local_path(resolved):
            raise _VisionDownloadError("local path not allowed")
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
            raise _VisionDownloadError("invalid image")
        except Image.DecompressionBombError:
            raise _VisionDownloadError("decompression bomb")
        return data_url, norm_bytes

    def _normalize_static(self, im: Image.Image) -> tuple[str, bytes]:
        width, height = im.size
        if int(width) * int(height) > MAX_IMAGE_PIXELS:
            raise _VisionDownloadError("too many pixels")
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
            raise _VisionDownloadError("no frames")
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

    def _cache_key(self, normalized_bytes: bytes) -> str:
        h = hashlib.sha256()
        h.update(normalized_bytes)
        h.update(self.model.encode("utf-8", errors="ignore"))
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

    def _negative_key(self, segment: VisualSegment) -> str:
        if segment.file_id:
            return f"fid:{segment.file_id}"
        if segment.file:
            return "f:" + hashlib.sha256(segment.file.encode("utf-8", errors="ignore")).hexdigest()[:16]
        if segment.url:
            return "u:" + hashlib.sha256(segment.url.encode("utf-8", errors="ignore")).hexdigest()[:16]
        return ""

    def _record_negative(self, segment: VisualSegment) -> None:
        key = self._negative_key(segment)
        if not key or self.negative_cache_ttl <= 0:
            return
        self._negative_cache[key] = self.clock()
        self._negative_cache.move_to_end(key)
        while len(self._negative_cache) > max(64, self.cache_max_entries):
            self._negative_cache.popitem(last=False)

    def _is_negative(self, segment: VisualSegment) -> bool:
        key = self._negative_key(segment)
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


class _VisionDownloadError(Exception):
    pass


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
