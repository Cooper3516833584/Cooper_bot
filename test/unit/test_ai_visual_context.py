from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import AsyncMock, Mock

import pytest

import cooper_bot.commands.commands as commands
from cooper_bot.modules.ai.aisvc import AIService
from cooper_bot.modules.vision.vision_skill import VisionResolution, VisionSkill, VisionSlot


class _DummyLog:
    def __init__(self) -> None:
        self.records: list[str] = []

    def info(self, _msg: str) -> None:
        return

    def warning(self, msg: str) -> None:
        self.records.append(str(msg))

    def exception(self, msg: str) -> None:
        self.records.append(str(msg))


class _DummyLogService:
    def __init__(self) -> None:
        self.log = _DummyLog()
        self.in_logs: list[str] = []
        self.out_logs: list[str] = []

    def log_in(self, _ctx, text: str) -> None:
        self.in_logs.append(str(text))

    def log_out(self, _ctx, text: str) -> None:
        self.out_logs.append(str(text))


class _ReplyRecorder:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def __call__(self, _api, ctx, text: str, _logsvc, force_private_user_id=None) -> None:
        self.messages.append({"scene": getattr(ctx, "scene", ""), "text": str(text)})


def _make_ctx(*, scene: str = "group", level: int = 1, user_id: int = 10001, group_id: int | None = 20001):
    return SimpleNamespace(
        scene=scene,
        user_id=int(user_id),
        nickname="tester",
        card="tester",
        group_id=group_id,
        group_name="group",
        level=int(level),
    )


def _make_filesvc_stub():
    return SimpleNamespace(
        roots=[SimpleNamespace(name="public")],
        find=Mock(return_value=[]),
        list_dir=Mock(return_value=(True, "目录内容：")),
    )


@pytest.fixture
def dispatch_harness(monkeypatch):
    recorder = _ReplyRecorder()

    async def _noop_group_context(*_args, **_kwargs):
        return None

    async def _noop_pre_state(*_args, **_kwargs):
        return False

    async def _immediate_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(commands, "reply", recorder)
    monkeypatch.setattr(commands, "_ensure_group_context_and_schedule_digest", _noop_group_context)
    monkeypatch.setattr(commands, "_handle_pre_dispatch_state", _noop_pre_state)
    monkeypatch.setattr(commands.asyncio, "to_thread", _immediate_to_thread)
    if hasattr(commands, "_AI_REPEAT_GUARD"):
        commands._AI_REPEAT_GUARD.clear()
    return recorder


class _FakeAIService:
    def __init__(self) -> None:
        self.bot_nick = "Cooper_bot"
        self.chat_ready = True
        self.gemini_chat_ready = True
        self.notice_ready = False
        self.semantic_ready = False
        self.fallback_error_reply = "fallback"
        self.remember_user_message = Mock()
        self.remember_assistant_message = Mock()
        self.chat_with_context = AsyncMock(return_value="fake-ai-reply")
        self.chat = AsyncMock(return_value="fake-ai-reply")
        self.gemini_chat_with_context = AsyncMock(return_value="gemini-ai-reply")
        self.gemini_chat = AsyncMock(return_value="gemini-ai-reply")
        self.restricted_gemini_chat_with_context = AsyncMock(return_value="restricted-gemini-ai-reply")
        self.restricted_gemini_chat = AsyncMock(return_value="restricted-gemini-ai-reply")
        self.semantic_find_paths = AsyncMock(return_value=[])
        # 视觉 slot 接口（dispatch 层测试用）
        self.collect_unresolved_vision_slots = Mock(return_value=[])
        self.apply_vision_resolutions = Mock(return_value=0)
        self.find_chat_message_by_msg_id = Mock(return_value=None)


class _FakeVisionSkill:
    ready = True

    def __init__(self, *, capture_context_images: bool = True) -> None:
        self.capture_context_images = capture_context_images
        self.resolve_calls: list[list] = []
        self.slots_by_evt: list[list] = []

    def create_slots_from_event(self, evt: dict, *, message_id: str = "", source_kind: str = "message") -> list:
        msg = evt.get("message")
        segs = msg if isinstance(msg, list) else []
        slots = []
        for i, s in enumerate(segs, start=1):
            if not isinstance(s, dict) or str(s.get("type") or "").lower() != "image":
                continue
            data = s.get("data") or {}
            slots.append(
                VisionSlot(
                    slot_id=f"{message_id or 'local'}:{i}",
                    index=i,
                    segment_type="image",
                    url=str(data.get("url") or ""),
                    file_id=str(data.get("file_id") or data.get("file") or ""),
                    source_kind=source_kind,
                )
            )
        self.slots_by_evt.append(slots)
        return slots

    async def resolve_slots(self, api, slots: list) -> list:
        self.resolve_calls.append(list(slots))
        return [
            VisionResolution(
                slot_id=str(s.get("slot_id") if isinstance(s, dict) else s.slot_id),
                status="ready",
                description="类型：表情包；画面：猫",
            )
            for s in slots
        ]

    def apply_resolutions_to_slots(self, slots: list, resolutions: list) -> list:
        result_map = {r.slot_id: r for r in resolutions}
        out = []
        for s in slots:
            r = result_map.get(s.slot_id)
            if r is not None:
                s.status = r.status
                s.description = r.description
            out.append(s)
        return out


def _new_aisvc() -> AIService:
    svc = AIService(log=_DummyLog())
    svc.deepseek_base_url = "https://example.local/v1"
    svc.deepseek_api_key = "fake-key"
    svc.system_prompt = "system-prompt"
    return svc


def _image_evt(*, group: bool = True, mention: bool = False, text_part: str = "", message_id: str = "100"):
    segs: list[dict] = []
    if mention:
        segs.append({"type": "at", "data": {"qq": "1622236011"}})
    segs.append({"type": "image", "data": {"file": "f1", "url": "https://a/1.png"}})
    if text_part:
        segs.append({"type": "text", "data": {"text": text_part}})
    return {
        "post_type": "message",
        "message_type": "group" if group else "private",
        "sub_type": "friend",
        "self_id": "1622236011",
        "message_id": message_id,
        "message": segs,
        "raw_message": "",
    }


# ============ aisvc：生命周期与窗口 ============


def _make_history(n: int) -> list[dict[str, Any]]:
    return [{"role": "user" if i % 2 == 0 else "assistant", "content": f"msg-{i}"} for i in range(n)]


def test_select_history_windows() -> None:
    svc = _new_aisvc()
    history = _make_history(350)
    ds = svc._select_history_for_backend(history, "deepseek")
    assert len(ds) == 300
    gm = svc._select_history_for_backend(history, "gemini")
    assert len(gm) == 100
    cl = svc._select_history_for_backend(history, "claude")
    assert len(cl) == 100
    # 第 150 条：deepseek 可见、gemini/claude 不可见
    assert "msg-150" in {m["content"] for m in ds}
    assert "msg-150" not in {m["content"] for m in gm}


def test_normalize_vision_slots_filters_invalid() -> None:
    svc = _new_aisvc()
    out = svc._normalize_vision_slots(
        [
            {"slot_id": "1:1", "index": 1, "segment_type": "image", "status": "unresolved", "url": "https://a"},
            {"slot_id": "2:1", "index": 0, "segment_type": "image", "status": "bogus"},
            "not-a-dict",
            {"index": 1, "segment_type": "image"},  # 无 slot_id
        ]
    )
    # index=0 会被修正为 1，bogus status 会被修正为 unresolved，均保留；无 slot_id 和非 dict 被丢弃
    assert len(out) == 2
    assert out[0]["slot_id"] == "1:1"
    assert out[0]["index"] == 1
    assert out[0]["status"] == "unresolved"
    assert out[1]["slot_id"] == "2:1"
    assert out[1]["index"] == 1
    assert out[1]["status"] == "unresolved"


def test_render_chat_message_content_by_status() -> None:
    svc = _new_aisvc()
    msg = {
        "role": "user",
        "content": "看看这个",
        "_vision": [
            {"slot_id": "1:1", "index": 1, "segment_type": "image", "status": "ready", "description": "类型：表情包；画面：猫"},
            {"slot_id": "1:2", "index": 2, "segment_type": "image", "status": "retryable_error"},
            {"slot_id": "1:3", "index": 3, "segment_type": "image", "status": "permanent_error"},
            {"slot_id": "1:4", "index": 4, "segment_type": "image", "status": "unresolved"},
        ],
    }
    rendered = svc._render_chat_message_content(msg)
    assert "看看这个" in rendered
    assert "[视觉内容1] 类型：表情包；画面：猫" in rendered
    assert "[视觉内容2] 图片暂时无法识别。" in rendered
    assert "[视觉内容3] 图片识别失败，无法确认具体内容。" in rendered
    assert "[视觉内容4] 图片尚未完成识别。" in rendered


def test_materialize_history_removes_internal_fields() -> None:
    svc = _new_aisvc()
    history = [
        {
            "role": "user",
            "content": "看图",
            "_msg_id": "123",
            "_vision": [
                {"slot_id": "123:1", "index": 1, "segment_type": "image", "status": "ready", "description": "类型：截图；画面：报错"}
            ],
        }
    ]
    out = svc._materialize_history_for_model(history)
    assert out == [{"role": "user", "content": "看图\n\n[视觉内容1] 类型：截图；画面：报错"}]
    assert "_vision" not in out[0]
    assert "_msg_id" not in out[0]


def test_collect_unresolved_by_backend_window() -> None:
    svc = _new_aisvc()
    # 构造 150 条历史，第 1 条（index 0）带 unresolved slot：
    # 150 条里 index 0 在 deepseek 300 窗口内，但不在 gemini/claude 最后 100 条窗口内
    history = _make_history(150)
    history[0] = {
        "role": "user",
        "content": "msg-0",
        "_msg_id": "999",
        "_vision": [{"slot_id": "999:1", "index": 1, "segment_type": "image", "status": "unresolved", "url": "https://a"}],
    }
    with svc._chat_sessions_lock:
        svc._chat_sessions["group:1"] = {"last_active_ts": time.time(), "messages": history}

    deepseek = svc.collect_unresolved_vision_slots("group:1", "deepseek")
    assert len(deepseek) == 1  # 300 窗口内可见
    gemini = svc.collect_unresolved_vision_slots("group:1", "gemini")
    assert gemini == []  # 最后 100 条窗口内不可见（第 1 条）
    claude = svc.collect_unresolved_vision_slots("group:1", "claude")
    assert claude == []


def test_apply_vision_resolutions_by_slot_id() -> None:
    svc = _new_aisvc()
    history = [
        {
            "role": "user",
            "content": "a",
            "_msg_id": "1",
            "_vision": [{"slot_id": "1:1", "index": 1, "segment_type": "image", "status": "unresolved", "url": "https://a"}],
        },
        {
            "role": "user",
            "content": "b",
            "_msg_id": "2",
            "_vision": [{"slot_id": "2:1", "index": 1, "segment_type": "image", "status": "unresolved", "url": "https://b"}],
        },
    ]
    with svc._chat_sessions_lock:
        svc._chat_sessions["group:1"] = {"last_active_ts": time.time(), "messages": history}

    updated = svc.apply_vision_resolutions(
        "group:1",
        [
            VisionResolution(slot_id="1:1", status="retryable_error", retry_after_ts=time.time() + 60),
            VisionResolution(slot_id="2:1", status="ready", description="一只猫"),
        ],
    )
    assert updated == 2
    loaded = svc._load_active_chat_history("group:1")
    slot_a = loaded[0]["_vision"][0]
    slot_b = loaded[1]["_vision"][0]
    assert slot_a["status"] == "retryable_error"
    assert slot_a["description"] != "一只猫"
    assert slot_b["status"] == "ready"
    assert slot_b["description"] == "一只猫"
    # 成功后 source 清空
    assert slot_b["url"] == ""


def test_apply_resolutions_ready_clears_source() -> None:
    svc = _new_aisvc()
    history = [
        {
            "role": "user",
            "content": "x",
            "_msg_id": "1",
            "_vision": [{"slot_id": "1:1", "index": 1, "segment_type": "image", "status": "unresolved", "url": "https://a", "file_id": "f1"}],
        }
    ]
    with svc._chat_sessions_lock:
        svc._chat_sessions["group:1"] = {"last_active_ts": time.time(), "messages": history}
    svc.apply_vision_resolutions("group:1", [VisionResolution(slot_id="1:1", status="ready", description="ok")])
    slot = svc._load_active_chat_history("group:1")[0]["_vision"][0]
    assert slot["url"] == ""
    assert slot["file_id"] == ""


def test_over_300_history_removes_vision_metadata() -> None:
    svc = _new_aisvc()
    history = _make_history(300)
    history[0] = {
        "role": "user",
        "content": "old",
        "_msg_id": "1",
        "_vision": [{"slot_id": "1:1", "index": 1, "segment_type": "image", "status": "unresolved", "url": "https://a"}],
    }
    with svc._chat_sessions_lock:
        svc._chat_sessions["group:1"] = {"last_active_ts": time.time(), "messages": history}
    # 加 1 条触发裁剪
    svc.remember_user_message("group:1", "new message")
    loaded = svc._load_active_chat_history("group:1")
    assert len(loaded) <= svc._CHAT_CONTEXT_MAX_MESSAGES
    assert all("_vision" not in m or not m["_vision"] for m in loaded) or "old" not in {m["content"] for m in loaded}


def test_session_ttl_removes_vision_metadata(controlled_time) -> None:
    svc = _new_aisvc()
    svc.remember_user_message(
        "group:1",
        "看图",
        msg_id="123",
        vision_slots=[{"slot_id": "123:1", "index": 1, "segment_type": "image", "status": "unresolved", "url": "https://a"}],
    )
    controlled_time.advance(30 * 60 + 1)
    assert svc._load_active_chat_history("group:1") == []


def test_save_chat_turn_stores_basic_text_and_slots() -> None:
    svc = _new_aisvc()
    svc._save_chat_turn(
        "group:1",
        "看图",
        "回复",
        msg_id="123",
        vision_slots=[VisionSlot(slot_id="123:1", index=1, segment_type="image", url="https://a")],
    )
    history = svc._load_active_chat_history("group:1")
    assert history[0]["content"] == "看图"
    assert "视觉内容" not in history[0]["content"]
    assert history[0]["_msg_id"] == "123"
    assert history[0]["_vision"][0]["status"] == "unresolved"


def test_find_chat_message_by_msg_id() -> None:
    svc = _new_aisvc()
    svc.remember_user_message("group:1", "a", msg_id="111")
    svc.remember_user_message("group:1", "b", msg_id="222")
    found = svc.find_chat_message_by_msg_id("group:1", "111")
    assert found is not None
    assert found["content"] == "a"
    assert svc.find_chat_message_by_msg_id("group:1", "999") is None


# ============ dispatch 集成 ============


@pytest.mark.asyncio
async def test_dispatch_private_pure_image_triggers_ai(dispatch_harness) -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill()
    evt = _image_evt(group=False)

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt=evt,
        text="",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    aisvc.chat_with_context.assert_awaited_once()
    kwargs = aisvc.chat_with_context.await_args.kwargs
    assert kwargs["vision_slots"]
    assert kwargs["vision_slots"][0].status == "ready"
    assert kwargs["vision_slots"][0].description == "类型：表情包；画面：猫"


@pytest.mark.asyncio
async def test_dispatch_group_image_without_mention_saves_unresolved_slot(dispatch_harness) -> None:
    ctx = _make_ctx(scene="group")
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill()

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt=_image_evt(group=True, mention=False),
        text="",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    aisvc.chat_with_context.assert_not_awaited()
    aisvc.remember_user_message.assert_called_once()
    kwargs = aisvc.remember_user_message.call_args.kwargs
    assert kwargs["msg_id"] == "100"
    assert kwargs["vision_slots"]
    assert kwargs["vision_slots"][0].status == "unresolved"
    assert vision.resolve_calls == []  # 未触发 AI，不解析


@pytest.mark.asyncio
async def test_dispatch_capture_false_skips_non_ai_images(dispatch_harness) -> None:
    ctx = _make_ctx(scene="group")
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill(capture_context_images=False)

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt=_image_evt(group=True, mention=False),
        text="",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    # capture=false：普通群图不入上下文
    aisvc.remember_user_message.assert_not_called()


@pytest.mark.asyncio
async def test_dispatch_capture_false_still_resolves_ai_request(dispatch_harness) -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill(capture_context_images=False)

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt=_image_evt(group=False),
        text="看看这个",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    aisvc.chat_with_context.assert_awaited_once()
    kwargs = aisvc.chat_with_context.await_args.kwargs
    assert kwargs["vision_slots"]


@pytest.mark.asyncio
async def test_dispatch_signin_consumed_skips_vision(monkeypatch, dispatch_harness) -> None:
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill()

    async def _consume(*_args, **_kwargs) -> bool:
        return True

    monkeypatch.setattr(commands, "_handle_pre_dispatch_state", _consume)

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=_make_ctx(scene="private_friend", group_id=None),
        evt=_image_evt(group=False),
        text="",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    assert vision.slots_by_evt == []
    aisvc.chat_with_context.assert_not_awaited()


@pytest.mark.asyncio
async def test_dispatch_reply_in_history_uses_existing_vision(dispatch_harness) -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    aisvc = _FakeAIService()
    aisvc.find_chat_message_by_msg_id = Mock(
        return_value={
            "role": "user",
            "content": "图",
            "_msg_id": "111",
            "_vision": [{"slot_id": "111:1", "index": 1, "segment_type": "image", "status": "ready", "description": "类型：表情包；画面：狗"}],
        }
    )
    vision = _FakeVisionSkill()
    evt = {
        "post_type": "message",
        "message_type": "private",
        "sub_type": "friend",
        "self_id": "1622236011",
        "message_id": "200",
        "message": [{"type": "reply", "data": {"id": "111"}}, {"type": "text", "data": {"text": "这张图什么意思"}}],
        "raw_message": "",
    }

    api = SimpleNamespace(call=AsyncMock(return_value=None))

    await commands.dispatch(
        api=api,
        ctx=ctx,
        evt=evt,
        text="这张图什么意思",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    # 历史内找到：不调 get_msg、不额外解析当前图
    api.call.assert_not_awaited()
    aisvc.chat_with_context.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_reply_outside_history_uses_get_msg(dispatch_harness) -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    aisvc = _FakeAIService()  # find 返回 None（历史外）
    vision = _FakeVisionSkill()

    async def _fake_call(action: str, params: dict, timeout: float = 8.0):
        if action == "get_msg":
            return {"data": {"message": [{"type": "image", "data": {"url": "https://ref/2.png", "file_id": "rf2"}}]}}
        return None

    api = SimpleNamespace(call=_fake_call)
    evt = {
        "post_type": "message",
        "message_type": "private",
        "sub_type": "friend",
        "self_id": "1622236011",
        "message_id": "300",
        "message": [{"type": "reply", "data": {"id": "999"}}, {"type": "text", "data": {"text": "看看"}}],
        "raw_message": "",
    }

    await commands.dispatch(
        api=api,
        ctx=ctx,
        evt=evt,
        text="看看",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    aisvc.chat_with_context.assert_awaited_once()
    kwargs = aisvc.chat_with_context.await_args.kwargs
    assert kwargs["vision_slots"]
    assert kwargs["vision_slots"][0].source_kind == "reply_reference"


@pytest.mark.asyncio
async def test_dispatch_slash_fallback_keeps_vision(dispatch_harness) -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill()
    evt = {
        "post_type": "message",
        "message_type": "private",
        "sub_type": "friend",
        "self_id": "1622236011",
        "message_id": "400",
        "message": [
            {"type": "image", "data": {"url": "https://a/1.png"}},
            {"type": "text", "data": {"text": "/fnd 看这个"}},
        ],
        "raw_message": "",
    }

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt=evt,
        text="/fnd 看这个",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    # slash 进入 aichat fallback 时仍带视觉（forced 路径）
    aisvc.chat_with_context.assert_awaited_once()
    kwargs = aisvc.chat_with_context.await_args.kwargs
    assert kwargs["vision_slots"]


@pytest.mark.asyncio
async def test_dispatch_group_mention_image_resolves_history_slots(dispatch_harness) -> None:
    ctx = _make_ctx(scene="group")
    aisvc = _FakeAIService()
    aisvc.collect_unresolved_vision_slots = Mock(
        return_value=[
            {"slot_id": "old:1", "index": 1, "segment_type": "image", "status": "unresolved", "url": "https://old"}
        ]
    )
    vision = _FakeVisionSkill()

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt=_image_evt(group=True, mention=True, text_part="这些图什么意思"),
        text="这些图什么意思",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    aisvc.chat_with_context.assert_awaited_once()
    # 历史补解析 + 当前消息都解析
    assert len(vision.resolve_calls) == 1
    resolved_slot_ids = {
        s.get("slot_id") if isinstance(s, dict) else s.slot_id for s in vision.resolve_calls[0]
    }
    assert "old:1" in resolved_slot_ids
    aisvc.apply_vision_resolutions.assert_called_once()


# ============ 剩余问题 regression tests ============


def test_deepseek_web_search_keeps_current_vision(monkeypatch) -> None:
    svc = _new_aisvc()
    svc.web_search_enabled = True
    payloads: list[dict[str, Any]] = []

    def _fake_post_json(_url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        payloads.append(payload)
        if len(payloads) == 1:
            return {"choices": [{"message": {"content": "[WEB_SEARCH]某显卡最新价格"}}]}
        return {"choices": [{"message": {"content": "整合回答"}}]}

    monkeypatch.setattr(svc, "_post_json", _fake_post_json)
    monkeypatch.setattr(svc, "_web_search_fetch_sources_sync", lambda _q: "搜索素材")
    slots = [
        {"slot_id": "1:1", "index": 1, "segment_type": "image", "status": "ready", "description": "类型：产品照片；画面：RTX 5090"}
    ]

    out = svc._chat_with_context_sync("private:10001", "多少钱？", msg_id="1", vision_slots=slots)
    assert out == "整合回答"
    assert len(payloads) == 2
    # 联网整合阶段 user content 必须包含当前图片描述
    second_user = payloads[1]["messages"][-1]["content"]
    assert "多少钱？" in second_user
    assert "[视觉内容1] 类型：产品照片；画面：RTX 5090" in second_user
    # 历史保存仍为基础文本
    history = svc._load_active_chat_history("private:10001")
    assert history[0]["content"] == "多少钱？"
    assert "[视觉内容1]" not in history[0]["content"]


def test_deepseek_web_search_fallback_keeps_current_vision(monkeypatch) -> None:
    svc = _new_aisvc()
    svc.web_search_enabled = True
    payloads: list[dict[str, Any]] = []

    def _fake_post_json(_url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        payloads.append(payload)
        if len(payloads) == 1:
            return {"choices": [{"message": {"content": "[WEB_SEARCH]查询"}}]}
        return {"choices": [{"message": {"content": "回退回答"}}]}

    def _boom(_q: str) -> str:
        raise RuntimeError("search failed")

    monkeypatch.setattr(svc, "_post_json", _fake_post_json)
    monkeypatch.setattr(svc, "_web_search_fetch_sources_sync", _boom)
    slots = [
        {"slot_id": "1:1", "index": 1, "segment_type": "image", "status": "ready", "description": "类型：产品照片；画面：RTX 5090"}
    ]

    out = svc._chat_with_context_sync("private:10001", "多少钱？", msg_id="1", vision_slots=slots)
    assert out == "回退回答"
    fallback_user = payloads[1]["messages"][-1]["content"]
    assert "[视觉内容1] 类型：产品照片；画面：RTX 5090" in fallback_user


@pytest.mark.asyncio
async def test_capture_false_slash_fallback_keeps_current_image(dispatch_harness) -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill(capture_context_images=False)
    evt = {
        "post_type": "message",
        "message_type": "private",
        "sub_type": "friend",
        "self_id": "1622236011",
        "message_id": "500",
        "message": [
            {"type": "image", "data": {"url": "https://a/1.png"}},
            {"type": "text", "data": {"text": "/fnd 看这个"}},
        ],
        "raw_message": "",
    }

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt=evt,
        text="/fnd 看这个",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    # capture=false 不能影响 slash fallback 的当前图片
    aisvc.chat_with_context.assert_awaited_once()
    kwargs = aisvc.chat_with_context.await_args.kwargs
    assert kwargs["vision_slots"]
    assert kwargs["vision_slots"][0].status == "ready"


def test_group_pure_image_context_keeps_sender_metadata() -> None:
    ctx = _make_ctx(scene="group")
    aisvc = _FakeAIService()
    slot = VisionSlot(slot_id="1:1", index=1, segment_type="image", url="https://a/1.png")
    commands._remember_non_ai_chat_message(
        ctx, "", _DummyLogService(), aisvc, msg_id="1", vision_slots=[slot]
    )
    aisvc.remember_user_message.assert_called_once()
    content = aisvc.remember_user_message.call_args.args[1]
    assert "发言人QQ:10001" in content
    assert "发言人昵称:tester" in content
    assert "群号:20001" in content
    assert aisvc.remember_user_message.call_args.kwargs["vision_slots"]


@pytest.mark.asyncio
async def test_group_mention_pure_image_ai_input_has_sender(dispatch_harness) -> None:
    ctx = _make_ctx(scene="group")
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill()

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt=_image_evt(group=True, mention=True),
        text="",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    aisvc.chat_with_context.assert_awaited_once()
    ai_input = aisvc.chat_with_context.await_args.args[1]
    assert "发言人QQ:10001" in ai_input
    assert "发言人昵称:tester" in ai_input
    assert "群号:20001" in ai_input
