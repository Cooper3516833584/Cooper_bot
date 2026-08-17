from __future__ import annotations

import asyncio
import io
import json
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional

import pytest
from PIL import Image

from cooper_bot.modules.vision.vision_skill import (
    VisionResolution,
    VisionSkill,
    VisionSlot,
    VisualSegment,
    build_source_key,
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

    return base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
    )


def _distinct_png_downloader() -> "Any":
    import hashlib as _hashlib

    def _dl(url: str) -> bytes:
        seed = int(_hashlib.md5(str(url).encode("utf-8")).hexdigest()[:4], 16) % 255
        buf = io.BytesIO()
        Image.new("RGB", (8, 8), (seed, 20, 200 - seed)).save(buf, format="PNG")
        return buf.getvalue()

    return _dl


class _FakeCompletions:
    def __init__(self, content: str) -> None:
        self._content = content
        self.calls: list[dict[str, Any]] = []
        self.active = 0
        self.peak = 0

    async def create(self, **kwargs) -> Any:
        self.calls.append(kwargs)
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(0.02)
            return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=self._content))])
        finally:
            self.active -= 1


class _FakeClient:
    def __init__(self, content: str = '{"kind":"表情包","scene":"一只猫","visible_text":"","emotion":"","intent":""}') -> None:
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
    content: str = '{"kind":"表情包","scene":"一只猫","visible_text":"","emotion":"","intent":""}',
    downloader=None,
    clock=None,
    max_concurrency: int = 4,
) -> VisionSkill:
    svc = VisionSkill(
        log=log or _DummyLog(),
        client=_FakeClient(content),
        downloader=downloader or (lambda url: _fake_png_bytes()),
        clock=clock or (lambda: 1000.0),
    )
    svc.cache_ttl = 600.0
    svc.max_concurrency = max_concurrency
    return svc


def _image_slot(slot_id: str, *, url: str = "", file_id: str = "", segment_type: str = "image", **kw) -> VisionSlot:
    data: dict[str, Any] = dict(slot_id=slot_id, index=1, segment_type=segment_type, url=url, file_id=file_id)
    data.update(kw)
    return VisionSlot(**data)


# ============ 段提取 ============


def test_extract_image_segments_from_array_and_cq() -> None:
    svc = _make_skill()
    evt = {
        "message": [
            {"type": "text", "data": {"text": "看图"}},
            {"type": "image", "data": {"file": "f1", "url": "https://a/1.png"}},
            {"type": "face", "data": {"id": "14"}},
        ],
        "raw_message": "[CQ:image,file=f1,url=https://a/1.png]",
    }
    segs = svc.extract_visual_segments(evt)
    assert [s.segment_type for s in segs] == ["image", "face"]
    assert segs[0].url == "https://a/1.png"


def test_extract_preserves_order_and_dedupes() -> None:
    svc = _make_skill()
    evt = {
        "message": [
            {"type": "image", "data": {"url": "https://a/1.png"}},
            {"type": "image", "data": {"url": "https://a/2.png"}},
            {"type": "image", "data": {"url": "https://a/1.png"}},
        ]
    }
    segs = svc.extract_visual_segments(evt)
    assert [s.url for s in segs] == ["https://a/1.png", "https://a/2.png"]


# ============ create_slots_from_event ============


def test_create_slots_from_event_with_message_id() -> None:
    svc = _make_skill()
    evt = {"message_id": "987654321", "message": [{"type": "image", "data": {"url": "https://a/1.png"}}]}
    slots = svc.create_slots_from_event(evt)
    assert len(slots) == 1
    assert slots[0].slot_id == "987654321:1"
    assert slots[0].index == 1
    assert slots[0].status == "unresolved"


def test_create_slots_face_ready_locally() -> None:
    svc = _make_skill()
    evt = {"message_id": "1", "message": [{"type": "face", "data": {"id": "14"}}]}
    slots = svc.create_slots_from_event(evt)
    assert slots[0].status == "ready"
    assert "微笑" in slots[0].description


def test_create_slots_mface_summary_ready_locally() -> None:
    svc = _make_skill()
    evt = {"message_id": "1", "message": [{"type": "mface", "data": {"summary": "流泪"}}]}
    slots = svc.create_slots_from_event(evt)
    assert slots[0].status == "ready"
    assert "流泪" in slots[0].description


def test_create_slots_respects_max_images_per_message() -> None:
    svc = _make_skill()
    svc.max_images_per_message = 3
    evt = {"message_id": "1", "message": [{"type": "image", "data": {"url": f"https://a/{i}.png"}} for i in range(5)]}
    slots = svc.create_slots_from_event(evt)
    assert len(slots) == 3
    assert slots[0].slot_id == "1:1"
    assert slots[-1].slot_id == "1:3"


# ============ build_source_key ============


def test_build_source_key() -> None:
    assert build_source_key(_image_slot("s", segment_type="face", face_id="14")) == "face:14"
    assert build_source_key(_image_slot("s", segment_type="image", file_id="fid1")) == "image:fid:fid1"
    assert build_source_key(_image_slot("s", segment_type="image", url="https://a/1.png")).startswith("image:url:")
    assert build_source_key(_image_slot("s", segment_type="mface", file_id="mf")) == "mface:mf"


# ============ resolve_slots：成功 ============


@pytest.mark.asyncio
async def test_resolve_image_ready() -> None:
    client = _FakeClient()
    svc = _make_skill(content=client.chat.completions._content)
    svc._client = client
    slot = _image_slot("A:1", url="https://a/1.png")
    resolutions = await svc.resolve_slots(_FakeAPI(), [slot])
    assert len(resolutions) == 1
    assert resolutions[0].slot_id == "A:1"
    assert resolutions[0].status == "ready"
    assert "一只猫" in resolutions[0].description
    assert client.chat.completions.calls[0]["max_tokens"] == 800


@pytest.mark.asyncio
async def test_resolve_face_and_mface_no_api() -> None:
    client = _FakeClient()
    svc = _make_skill()
    svc._client = client
    slots = [
        _image_slot("f:1", segment_type="face", face_id="14"),
        _image_slot("m:1", segment_type="mface", summary="流泪"),
    ]
    resolutions = await svc.resolve_slots(_FakeAPI(), slots)
    assert len(resolutions) == 2
    assert all(r.status == "ready" for r in resolutions)
    assert len(client.chat.completions.calls) == 0


@pytest.mark.asyncio
async def test_resolve_same_source_key_calls_api_once() -> None:
    client = _FakeClient()
    svc = _make_skill()
    svc._client = client
    slots = [_image_slot(f"m{i}:1", url="https://x/doge.png", file_id="doge") for i in range(3)]
    resolutions = await svc.resolve_slots(_FakeAPI(), slots)
    assert len(resolutions) == 3
    assert all(r.status == "ready" for r in resolutions)
    assert len(client.chat.completions.calls) == 1


@pytest.mark.asyncio
async def test_different_urls_same_bytes_api_once() -> None:
    client = _FakeClient()
    svc = _make_skill()
    svc._client = client
    slots = [_image_slot("a:1", url="https://a/1.png"), _image_slot("b:1", url="https://b/1.png")]
    resolutions = await svc.resolve_slots(_FakeAPI(), slots)
    assert len(resolutions) == 2
    assert all(r.status == "ready" for r in resolutions)
    assert len(client.chat.completions.calls) == 1


@pytest.mark.asyncio
async def test_singleflight_cross_session_concurrent() -> None:
    client = _FakeClient()
    svc = _make_skill()
    svc._client = client
    slot_a = _image_slot("ga:1", url="https://x/a.png")
    slot_b = _image_slot("gb:1", url="https://y/b.png")
    await asyncio.gather(svc.resolve_slots(_FakeAPI(), [slot_a]), svc.resolve_slots(_FakeAPI(), [slot_b]))
    assert len(client.chat.completions.calls) == 1


@pytest.mark.asyncio
async def test_concurrency_limited_by_max_concurrency() -> None:
    client = _FakeClient()
    svc = _make_skill()
    svc._client = client
    svc.max_concurrency = 2
    slots = [_image_slot(f"s{i}:1", url=f"https://a/{i}.png") for i in range(6)]
    resolutions = await svc.resolve_slots(_FakeAPI(), slots)
    assert len([r for r in resolutions if r.status == "ready"]) == 6
    assert client.chat.completions.peak <= 2


# ============ 失败映射 ============


@pytest.mark.asyncio
async def test_failure_mapping_no_misattribution() -> None:
    client = _FakeClient()
    calls = {"n": 0}
    distinct = _distinct_png_downloader()

    def _downloader(url: str) -> bytes:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("network down")
        return distinct(url)

    svc = _make_skill()
    svc._client = client
    svc._downloader = _downloader
    slots = [_image_slot("A:1", url="https://x/a.png"), _image_slot("B:1", url="https://x/b.png")]
    resolutions = await svc.resolve_slots(_FakeAPI(), slots)
    by_id = {r.slot_id: r for r in resolutions}
    assert by_id["A:1"].status == "retryable_error"
    assert by_id["A:1"].description != "一只猫"
    assert by_id["B:1"].status == "ready"
    assert "一只猫" in by_id["B:1"].description


@pytest.mark.asyncio
async def test_permanent_error_not_retried() -> None:
    svc = _make_skill(downloader=lambda url: b"not an image at all")
    slot = _image_slot("p:1", url="https://a/1.png")
    resolutions = await svc.resolve_slots(_FakeAPI(), [slot])
    assert resolutions[0].status == "permanent_error"
    # 已是 permanent_error 的 slot 不再处理
    slot.status = "permanent_error"
    resolutions2 = await svc.resolve_slots(_FakeAPI(), [slot])
    assert resolutions2 == []


@pytest.mark.asyncio
async def test_retryable_skip_until_retry_after() -> None:
    client = _FakeClient()
    now = {"ts": 1000.0}
    svc = _make_skill(clock=lambda: now["ts"])
    svc._client = client
    slot = _image_slot("r:1", url="https://a/1.png")
    slot.status = "retryable_error"
    slot.retry_after_ts = now["ts"] + 60.0
    # 未到 retry_after：跳过
    resolutions = await svc.resolve_slots(_FakeAPI(), [slot])
    assert resolutions == []
    assert len(client.chat.completions.calls) == 0
    # 超过 retry_after：重新处理
    now["ts"] += 61.0
    resolutions2 = await svc.resolve_slots(_FakeAPI(), [slot])
    assert len(resolutions2) == 1
    assert resolutions2[0].status == "ready"


# ============ apply_resolutions_to_slots ============


def test_apply_resolutions_to_slots_by_slot_id() -> None:
    svc = _make_skill()
    slots = [_image_slot("A:1", url="https://a/1.png"), _image_slot("B:1", url="https://b/1.png")]
    resolutions = [
        VisionResolution(slot_id="A:1", status="retryable_error", retry_after_ts=1100.0),
        VisionResolution(slot_id="B:1", status="ready", description="一只猫", content_hash="abc"),
    ]
    updated = svc.apply_resolutions_to_slots(slots, resolutions)
    assert updated[0].status == "retryable_error"
    assert updated[0].description == ""
    assert updated[1].status == "ready"
    assert updated[1].description == "一只猫"
    assert updated[1].content_hash == "abc"
    # 成功后释放 source
    assert updated[1].url == ""


def test_slot_to_dict_roundtrip() -> None:
    svc = _make_skill()
    slot = _image_slot("x:1", url="https://a/1.png", file_id="f1")
    d = slot.to_dict()
    assert d["slot_id"] == "x:1"
    assert d["status"] == "unresolved"


@pytest.mark.asyncio
async def test_resolve_concurrent_peak_gt_one() -> None:
    client = _FakeClient()
    svc = _make_skill(downloader=_distinct_png_downloader())
    svc._client = client
    svc.max_concurrency = 2
    # 6 张不同 content 的图片，必须真正并发（peak > 1）且不超过上限
    slots = [_image_slot(f"s{i}:1", url=f"https://a/{i}.png") for i in range(6)]
    resolutions = await svc.resolve_slots(_FakeAPI(), slots)
    assert len([r for r in resolutions if r.status == "ready"]) == 6
    assert 1 < client.chat.completions.peak <= 2


@pytest.mark.asyncio
async def test_image_too_large_is_permanent() -> None:
    svc = _make_skill(downloader=lambda url: b"x" * 2048)
    svc.max_image_bytes = 1024
    slot = _image_slot("big:1", url="https://a/1.png")
    resolutions = await svc.resolve_slots(_FakeAPI(), [slot])
    assert resolutions[0].status == "permanent_error"
    # permanent 后不再重复解析
    slot.status = "permanent_error"
    resolutions2 = await svc.resolve_slots(_FakeAPI(), [slot])
    assert resolutions2 == []


@pytest.mark.asyncio
async def test_retry_after_uses_configured_negative_ttl() -> None:
    client = _FakeClient()
    now = {"ts": 1000.0}
    svc = _make_skill(clock=lambda: now["ts"])
    svc._client = client
    svc.negative_cache_ttl = 10.0

    def _boom(_url: str) -> bytes:
        raise RuntimeError("network down")

    svc._downloader = _boom
    slot = _image_slot("r:1", url="https://a/1.png")
    resolutions = await svc.resolve_slots(_FakeAPI(), [slot])
    assert resolutions[0].status == "retryable_error"
    assert resolutions[0].retry_after_ts == 1010.0  # 使用配置值，而不是写死 60


@pytest.mark.asyncio
async def test_negative_ttl_zero_allows_immediate_retry() -> None:
    client = _FakeClient()
    now = {"ts": 1000.0}
    svc = _make_skill(clock=lambda: now["ts"])
    svc._client = client
    svc.negative_cache_ttl = 0.0

    def _boom(_url: str) -> bytes:
        raise RuntimeError("network down")

    svc._downloader = _boom
    slot = _image_slot("r:1", url="https://a/1.png")
    resolutions = await svc.resolve_slots(_FakeAPI(), [slot])
    assert resolutions[0].status == "retryable_error"
    assert resolutions[0].retry_after_ts <= 1000.0  # 立即重试
