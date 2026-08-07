from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional

import pytest

from vision_skill import (
    VisionContext,
    VisionSkill,
    VisualDescription,
    VisualSegment,
    compose_ai_context_text,
    is_direct_image_source,
)


class _DummyLog:
    def __init__(self) -> None:
        self.records: list[str] = []

    def info(self, msg: str) -> None:
        self.records.append(str(msg))

    def warning(self, msg: str) -> None:
        self.records.append(str(msg))


def _fake_png_bytes() -> bytes:
    import base64

    # 1x1 红色 PNG
    return base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
    )


def _distinct_png_downloader() -> "Any":
    import hashlib as _hashlib
    import io as _io

    from PIL import Image

    def _dl(url: str) -> bytes:
        seed = int(_hashlib.md5(str(url).encode("utf-8")).hexdigest()[:4], 16) % 255
        buf = _io.BytesIO()
        Image.new("RGB", (8, 8), (seed, 20, 200 - seed)).save(buf, format="PNG")
        return buf.getvalue()

    return _dl


class _FakeCompletions:
    def __init__(self, content: str) -> None:
        self._content = content
        self.calls: list[dict[str, Any]] = []

    async def create(self, **kwargs) -> Any:
        self.calls.append(kwargs)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=self._content))]
        )


class _FakeClient:
    def __init__(self, content: str) -> None:
        self.chat = SimpleNamespace(completions=_FakeCompletions(content))


class _FakeAPI:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def call(self, action: str, params: dict, timeout: float = 8.0) -> Optional[dict]:
        self.calls.append((action, params))
        return None

    async def get_file(self, file_id: str, timeout: float = 180.0, retries: int = 2, retry_delay: float = 2.0) -> Optional[dict]:
        self.calls.append(("get_file", {"file_id": file_id}))
        return None


def _make_skill(
    log=None,
    *,
    content: str = '{"kind":"表情包","scene":"熊猫头抱臂","visible_text":"就这？","emotion":"不屑","intent":"嘲讽"}',
    downloader=None,
    cache_max_entries: Optional[int] = None,
    cache_ttl: float = 600.0,
) -> VisionSkill:
    svc = VisionSkill(
        log=log or _DummyLog(),
        client=_FakeClient(content),
        downloader=downloader or (lambda url: _fake_png_bytes()),
        clock=lambda: 1000.0,
    )
    if cache_max_entries is not None:
        svc.cache_max_entries = cache_max_entries
    svc.cache_ttl = cache_ttl
    return svc


# ---------- 提取 ----------


def test_extract_image_segments_from_array() -> None:
    svc = _make_skill()
    evt = {
        "message": [
            {"type": "text", "data": {"text": "看图"}},
            {"type": "image", "data": {"file": "f1", "url": "https://a/1.png"}},
            {"type": "image", "data": {"file_id": "f2", "url": "https://a/2.png"}},
        ]
    }
    segs = svc.extract_visual_segments(evt)
    assert [s.segment_type for s in segs] == ["image", "image"]
    assert segs[0].url == "https://a/1.png"
    assert segs[0].file == "f1"
    assert segs[1].file_id == "f2"


def test_extract_image_segments_from_cq() -> None:
    svc = _make_skill()
    evt = {"raw_message": "[CQ:image,file=f1,url=https://a/1.png] 看图"}
    segs = svc.extract_visual_segments(evt)
    assert len(segs) == 1
    assert segs[0].segment_type == "image"
    assert segs[0].file == "f1"
    assert segs[0].url == "https://a/1.png"


def test_extract_mface_with_url() -> None:
    svc = _make_skill()
    evt = {"message": [{"type": "mface", "data": {"url": "https://a/m.png", "summary": "疑惑"}}]}
    segs = svc.extract_visual_segments(evt)
    assert len(segs) == 1
    assert segs[0].segment_type == "mface"
    assert segs[0].url == "https://a/m.png"


def test_extract_face_local_mapping() -> None:
    svc = _make_skill()
    evt = {"message": [{"type": "face", "data": {"id": "14"}}]}
    segs = svc.extract_visual_segments(evt)
    assert len(segs) == 1
    assert segs[0].segment_type == "face"
    assert segs[0].face_id == "14"


def test_extract_dedupe_array_and_raw() -> None:
    svc = _make_skill()
    evt = {
        "message": [{"type": "image", "data": {"file": "f1", "url": "https://a/1.png"}}],
        "raw_message": "[CQ:image,file=f1,url=https://a/1.png]",
    }
    segs = svc.extract_visual_segments(evt)
    assert len(segs) == 1


def test_extract_preserves_order_multi_image() -> None:
    svc = _make_skill()
    evt = {
        "message": [
            {"type": "image", "data": {"url": "https://a/1.png"}},
            {"type": "image", "data": {"url": "https://a/2.png"}},
            {"type": "image", "data": {"url": "https://a/3.png"}},
        ]
    }
    segs = svc.extract_visual_segments(evt)
    assert [s.url for s in segs] == ["https://a/1.png", "https://a/2.png", "https://a/3.png"]
    assert [s.index for s in segs] == [0, 1, 2]


# ---------- face / mface 本地描述 ----------


@pytest.mark.asyncio
async def test_describe_face_uses_local_mapping() -> None:
    svc = _make_skill()
    seg = VisualSegment(index=0, segment_type="face", face_id="14")
    out = await svc.describe_segment(_FakeAPI(), seg)
    assert out is not None
    assert out.description == "[QQ表情：微笑]"

    seg2 = VisualSegment(index=1, segment_type="face", face_id="999")
    out2 = await svc.describe_segment(_FakeAPI(), seg2)
    assert out2 is not None
    assert out2.description == "[QQ内置表情，ID：999]"


@pytest.mark.asyncio
async def test_describe_mface_without_image_uses_summary() -> None:
    svc = _make_skill()
    seg = VisualSegment(index=0, segment_type="mface", summary="疑惑")
    out = await svc.describe_segment(_FakeAPI(), seg)
    assert out is not None
    assert out.description == "[商城表情：疑惑]"

    seg2 = VisualSegment(index=1, segment_type="market_face", face_id="abc123")
    out2 = await svc.describe_segment(_FakeAPI(), seg2)
    assert out2 is not None
    assert out2.description == "[商城表情，ID：abc123，具体画面不可用]"


# ---------- 描述与缓存 ----------


@pytest.mark.asyncio
async def test_describe_image_calls_api_and_formats() -> None:
    log = _DummyLog()
    svc = _make_skill(log=log)
    seg = VisualSegment(index=0, segment_type="image", url="https://a/1.png")
    out = await svc.describe_segment(_FakeAPI(), seg)
    assert out is not None
    assert "类型：表情包" in out.description
    assert "熊猫头抱臂" in out.description
    assert "就这？" in out.description
    assert not out.cache_hit


@pytest.mark.asyncio
async def test_same_image_hits_cache_second_call() -> None:
    client = _FakeClient('{"kind":"表情包","scene":"猫","visible_text":"","emotion":"","intent":""}')
    svc = VisionSkill(_DummyLog(), client=client, downloader=lambda url: _fake_png_bytes(), clock=lambda: 1000.0)
    svc.cache_ttl = 600.0
    seg = VisualSegment(index=0, segment_type="image", url="https://a/1.png")

    first = await svc.describe_segment(_FakeAPI(), seg)
    second = await svc.describe_segment(_FakeAPI(), seg)

    assert first is not None and second is not None
    assert first.description == second.description
    assert first.cache_hit is False
    assert second.cache_hit is True
    assert len(client.chat.completions.calls) == 1


@pytest.mark.asyncio
async def test_cache_ttl_expiry_reinvokes() -> None:
    client = _FakeClient('{"kind":"照片","scene":"猫","visible_text":"","emotion":"","intent":""}')
    now = {"ts": 1000.0}
    svc = VisionSkill(_DummyLog(), client=client, downloader=lambda url: _fake_png_bytes(), clock=lambda: now["ts"])
    svc.cache_ttl = 100.0
    seg = VisualSegment(index=0, segment_type="image", url="https://a/1.png")

    await svc.describe_segment(_FakeAPI(), seg)
    now["ts"] += 200.0
    await svc.describe_segment(_FakeAPI(), seg)
    assert len(client.chat.completions.calls) == 2


@pytest.mark.asyncio
async def test_cache_max_entries_evicts_oldest() -> None:
    client = _FakeClient('{"kind":"照片","scene":"x","visible_text":"","emotion":"","intent":""}')
    svc = VisionSkill(_DummyLog(), client=client, downloader=_distinct_png_downloader(), clock=lambda: 1000.0)
    svc.cache_ttl = 600.0
    svc.cache_max_entries = 2

    segs = [VisualSegment(index=i, segment_type="image", url=f"https://a/{i}.png") for i in range(3)]
    for seg in segs:
        await svc.describe_segment(_FakeAPI(), seg)

    assert len(svc._cache) == 2
    assert len(client.chat.completions.calls) == 3


@pytest.mark.asyncio
async def test_cache_disabled_when_max_entries_zero() -> None:
    client = _FakeClient('{"kind":"照片","scene":"x","visible_text":"","emotion":"","intent":""}')
    svc = VisionSkill(_DummyLog(), client=client, downloader=lambda url: _fake_png_bytes(), clock=lambda: 1000.0)
    svc.cache_max_entries = 0
    seg = VisualSegment(index=0, segment_type="image", url="https://a/1.png")
    await svc.describe_segment(_FakeAPI(), seg)
    await svc.describe_segment(_FakeAPI(), seg)
    assert len(client.chat.completions.calls) == 2


# ---------- 失败与降级 ----------


@pytest.mark.asyncio
async def test_download_failure_returns_none() -> None:
    def _boom(_url: str) -> bytes:
        raise RuntimeError("network down")

    svc = _make_skill(downloader=_boom)
    seg = VisualSegment(index=0, segment_type="image", url="https://a/1.png")
    out = await svc.describe_segment(_FakeAPI(), seg)
    assert out is None


@pytest.mark.asyncio
async def test_invalid_json_response_returns_none() -> None:
    svc = _make_skill(content="not json at all")
    seg = VisualSegment(index=0, segment_type="image", url="https://a/1.png")
    out = await svc.describe_segment(_FakeAPI(), seg)
    assert out is None


@pytest.mark.asyncio
async def test_empty_response_returns_none() -> None:
    svc = _make_skill(content="")
    seg = VisualSegment(index=0, segment_type="image", url="https://a/1.png")
    out = await svc.describe_segment(_FakeAPI(), seg)
    assert out is None


@pytest.mark.asyncio
async def test_describe_event_partial_failure() -> None:
    calls = {"n": 0}
    distinct = _distinct_png_downloader()

    def _downloader(_url: str) -> bytes:
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")
        return distinct(_url)

    svc = _make_skill(downloader=_downloader)
    evt = {
        "message": [
            {"type": "image", "data": {"url": "https://a/1.png"}},
            {"type": "image", "data": {"url": "https://a/2.png"}},
            {"type": "image", "data": {"url": "https://a/3.png"}},
        ]
    }
    ctx = await svc.describe_event(_FakeAPI(), evt)
    assert len(ctx.descriptions) == 2
    assert ctx.failed_count == 1
    block = ctx.to_context_block()
    assert "[视觉内容0]" in block
    assert "[视觉内容2]" in block
    assert "图片识别失败" in block


@pytest.mark.asyncio
async def test_describe_event_respects_max_images() -> None:
    svc = _make_skill()
    svc.max_images_per_message = 2
    evt = {
        "message": [
            {"type": "image", "data": {"url": f"https://a/{i}.png"}} for i in range(4)
        ]
    }
    ctx = await svc.describe_event(_FakeAPI(), evt)
    assert len(ctx.descriptions) == 2
    assert ctx.skipped_count == 2


def test_ready_false_when_disabled() -> None:
    svc = _make_skill()
    svc.enabled = False
    assert svc.ready is False


# ---------- 安全 ----------


def test_is_direct_image_source() -> None:
    assert is_direct_image_source("https://a/b.png") is True
    assert is_direct_image_source("file:///C:/x.png") is True
    assert is_direct_image_source("C:\\Users\\a.png") is True
    assert is_direct_image_source("/tmp/a.png") is True
    assert is_direct_image_source("a.png") is False
    assert is_direct_image_source("") is False


def test_local_path_whitelist_blocks_system_paths(tmp_path) -> None:
    svc = _make_skill()
    assert svc._is_allowed_local_path(Path("C:/Windows/system32/passwd")) is False
    assert svc._is_allowed_local_path(Path("/etc/passwd")) is False
    assert svc._is_allowed_local_path(Path("C:/Users/Cooper/Desktop/a.png")) is False
    # 敏感文件
    assert svc._is_allowed_local_path(Path("C:/x/api_key.txt")) is False
    # 白名单目录内允许
    allowed_root = Path(__file__).resolve().parent.parent.parent / "data" / "ocr"
    if allowed_root.exists():
        assert svc._is_allowed_local_path(allowed_root / "a.jpg") is True


def test_compose_ai_context_text() -> None:
    ctx = VisionContext(
        descriptions=[VisualDescription(index=1, segment_type="image", description="类型：表情包；画面：猫")],
        failed_count=1,
    )
    out = compose_ai_context_text("你看看这个", ctx)
    assert "你看看这个" in out
    assert "[视觉内容1] 类型：表情包；画面：猫" in out
    assert "图片识别失败" in out

    assert compose_ai_context_text("", VisionContext()) == ""
    assert compose_ai_context_text("纯文本", VisionContext()) == "纯文本"


def test_vision_context_to_context_block_ordering() -> None:
    ctx = VisionContext(
        descriptions=[
            VisualDescription(index=2, segment_type="image", description="d2"),
            VisualDescription(index=1, segment_type="image", description="d1"),
        ]
    )
    block = ctx.to_context_block()
    assert block.index("[视觉内容1]") < block.index("[视觉内容2]")


@pytest.mark.asyncio
async def test_describe_event_runs_images_concurrently() -> None:
    import asyncio as _asyncio
    import time as _time

    active = {"n": 0, "peak": 0}

    async def _slow_downloader(_url: str) -> bytes:
        active["n"] += 1
        active["peak"] = max(active["peak"], active["n"])
        await _asyncio.sleep(0.15)
        active["n"] -= 1
        return _distinct_png_downloader()(_url)

    client = _FakeClient('{"kind":"照片","scene":"x","visible_text":"","emotion":"","intent":""}')
    svc = VisionSkill(_DummyLog(), client=client, downloader=_slow_downloader, clock=lambda: 1000.0)
    svc.cache_ttl = 600.0

    evt = {"message": [{"type": "image", "data": {"url": f"https://a/{i}.png"}} for i in range(3)]}
    start = _time.monotonic()
    ctx = await svc.describe_event(_FakeAPI(), evt)
    elapsed = _time.monotonic() - start

    assert len(ctx.descriptions) == 3
    # 并发执行：3 张图总耗时显著小于串行的 0.45s，且出现过并发峰值
    assert elapsed < 0.4
    assert active["peak"] >= 2


@pytest.mark.asyncio
async def test_describe_pending_resolves_all_beyond_single_message_limit() -> None:
    client = _FakeClient('{"kind":"照片","scene":"x","visible_text":"","emotion":"","intent":""}')
    svc = VisionSkill(_DummyLog(), client=client, downloader=_distinct_png_downloader(), clock=lambda: 1000.0)
    svc.cache_ttl = 600.0
    svc.max_images_per_message = 3  # 单消息上限 3

    pending = [{"url": f"https://a/{i}.png", "file_id": f"f{i}"} for i in range(8)]
    ctx = await svc.describe_pending(_FakeAPI(), pending)

    # pending 路径不受单消息上限限制，8 张全解析
    assert len(ctx.descriptions) == 8
    assert ctx.skipped_count == 0
    assert len(client.chat.completions.calls) == 8
