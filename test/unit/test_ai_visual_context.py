from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import AsyncMock, Mock

import pytest

import aisvc
import commands
from aisvc import AIService
from vision_skill import VisionContext, VisualDescription, VisualSegment, compose_ai_context_text


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
        self.messages.append(
            {
                "scene": getattr(ctx, "scene", ""),
                "text": str(text),
                "force_private_user_id": force_private_user_id,
            }
        )


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
        # 视觉 pending 队列（模拟真实 AIService 接口）
        self.record_pending_vision = Mock()
        self.clear_pending_vision = Mock()
        self.apply_vision_descriptions_to_history = Mock(return_value=0)
        self._load_pending_vision = Mock(return_value=[])


class _FakeVisionSkill:
    ready = True

    def __init__(self, ctx: Optional[VisionContext] = None) -> None:
        self._ctx = ctx or VisionContext(
            descriptions=[VisualDescription(1, "image", "类型：表情包；画面：猫")]
        )
        self.calls: list[dict] = []

    def extract_visual_segments(self, evt: dict):
        # 与真实实现一致：只提取 image 段（透传 url/file_id）
        msg = evt.get("message")
        segs = msg if isinstance(msg, list) else []
        out = []
        for i, s in enumerate(segs):
            if not isinstance(s, dict):
                continue
            if str(s.get("type") or "").lower() != "image":
                continue
            data = s.get("data") or {}
            out.append(
                VisualSegment(
                    index=i,
                    segment_type="image",
                    url=str(data.get("url") or ""),
                    file_id=str(data.get("file_id") or data.get("file") or ""),
                )
            )
        return out

    async def describe_event(self, api, evt: dict, segments=None) -> VisionContext:
        self.calls.append({"evt": evt, "segments": segments})
        return self._ctx

    async def describe_pending(self, api, pending_images) -> VisionContext:
        self.calls.append({"pending": pending_images})
        return self._ctx


def _image_evt(*, group: bool = True, mention: bool = False, text_part: str = ""):
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
        "message": segs,
        "raw_message": "",
    }


# ============ 触发判定 ============


def test_trigger_private_pure_image_returns_empty_string() -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    out = commands.extract_ai_chat_trigger_text(ctx, _image_evt(group=False), "", True, "Cooper_bot")
    assert out == ""


def test_trigger_private_g_prefix_with_image() -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    out = commands.extract_ai_chat_trigger_text(ctx, _image_evt(group=False, text_part=""), "g", True, "Cooper_bot")
    assert out == "g"


def test_trigger_private_text_without_visual() -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    out = commands.extract_ai_chat_trigger_text(
        ctx, {"message": [{"type": "text", "data": {"text": "hello"}}], "self_id": "1622236011"}, "hello", False, "Cooper_bot"
    )
    assert out == "hello"


def test_trigger_private_slash_command_no_visual() -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    out = commands.extract_ai_chat_trigger_text(ctx, {"message": []}, "/fnd 高数", False, "Cooper_bot")
    assert out is None


def test_trigger_group_image_without_mention_returns_none() -> None:
    ctx = _make_ctx(scene="group")
    out = commands.extract_ai_chat_trigger_text(ctx, _image_evt(group=True, mention=False), "", True, "Cooper_bot")
    assert out is None


def test_trigger_group_mention_pure_image_returns_empty_string() -> None:
    ctx = _make_ctx(scene="group")
    out = commands.extract_ai_chat_trigger_text(ctx, _image_evt(group=True, mention=True), "", True, "Cooper_bot")
    assert out == ""


def test_trigger_group_mention_image_with_text() -> None:
    ctx = _make_ctx(scene="group")
    out = commands.extract_ai_chat_trigger_text(
        ctx, _image_evt(group=True, mention=True, text_part="看看这个"), "看看这个", True, "Cooper_bot"
    )
    assert "看看这个" in out


# ============ dispatch 集成 ============


@pytest.mark.asyncio
async def test_dispatch_private_pure_image_triggers_ai_with_vision(dispatch_harness) -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill()

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
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

    aisvc.chat_with_context.assert_awaited_once()
    _, ai_input, *_rest = aisvc.chat_with_context.await_args.args
    assert "类型：表情包" in ai_input
    assert "[视觉内容1]" in ai_input


@pytest.mark.asyncio
async def test_dispatch_private_g_image_routes_gemini(dispatch_harness) -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill()

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt=_image_evt(group=False),
        text="g",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    aisvc.restricted_gemini_chat_with_context.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_group_image_without_mention_placeholder_only(dispatch_harness) -> None:
    ctx = _make_ctx(scene="group")
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill()

    # 纯图未 @：不触发 AI、不解析，但以占位符入上下文（供后续回复时解析引用）
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
    saved = aisvc.remember_user_message.call_args.args[1]
    assert "[图片待识别]" in saved
    assert vision.calls == []
    assert not any("fake-ai-reply" in m["text"] for m in dispatch_harness.messages)


@pytest.mark.asyncio
async def test_dispatch_group_image_text_without_mention_saves_text_and_placeholder(dispatch_harness) -> None:
    ctx = _make_ctx(scene="group")
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill()

    # 图+文字未 @：不触发 AI，保存"文字 + 图片占位符"（不解析）
    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt=_image_evt(group=True, mention=False, text_part="看看这个"),
        text="看看这个",
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
    saved = aisvc.remember_user_message.call_args.args[1]
    assert "看看这个" in saved
    assert "[图片待识别]" in saved
    assert vision.calls == []


@pytest.mark.asyncio
async def test_dispatch_signin_consumed_image_skips_vision(monkeypatch, dispatch_harness) -> None:
    # signin 消费后 dispatch 直接 return，不调用 VisionSkill
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

    assert vision.calls == []
    aisvc.chat_with_context.assert_not_awaited()


# ============ 后端历史窗口 ============


def _new_aisvc() -> AIService:
    svc = AIService(log=_DummyLog())
    svc.deepseek_base_url = "https://example.local/v1"
    svc.deepseek_api_key = "fake-key"
    svc.system_prompt = "system-prompt"
    return svc


def _make_history(n: int) -> list[dict[str, str]]:
    return [{"role": "user" if i % 2 == 0 else "assistant", "content": f"msg-{i}"} for i in range(n)]


def test_history_limit_for_backend() -> None:
    svc = _new_aisvc()
    assert svc._history_limit_for_backend("deepseek") == 300
    assert svc._history_limit_for_backend("gemini") == 100
    assert svc._history_limit_for_backend("claude") == 100
    assert svc._history_limit_for_backend("unknown") == svc._CHAT_CONTEXT_MAX_MESSAGES


def test_select_history_windows() -> None:
    svc = _new_aisvc()
    history = _make_history(350)

    ds = svc._select_history_for_backend(history, "deepseek")
    assert len(ds) == 300
    assert ds[0]["content"] == "msg-50"
    assert ds[-1]["content"] == "msg-349"

    gm = svc._select_history_for_backend(history, "gemini")
    assert len(gm) == 100
    assert gm[0]["content"] == "msg-250"

    cl = svc._select_history_for_backend(history, "claude")
    assert len(cl) == 100
    assert cl[0]["content"] == "msg-250"

    # 第 150 条对 gemini 不可见，对 deepseek 可见
    assert "msg-150" in {m["content"] for m in ds}
    assert "msg-150" not in {m["content"] for m in gm}


def test_select_history_keeps_short_history() -> None:
    svc = _new_aisvc()
    history = _make_history(30)
    assert svc._select_history_for_backend(history, "deepseek") == history
    assert svc._select_history_for_backend(history, "gemini") == history


def test_chat_with_context_uses_deepseek_window(monkeypatch, controlled_time) -> None:
    svc = _new_aisvc()
    payloads: list[dict[str, Any]] = []

    def _fake_post_json(_url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        payloads.append(payload)
        return {"choices": [{"message": {"content": "reply"}}]}

    monkeypatch.setattr(svc, "_post_json", _fake_post_json)

    # 预填 350 条历史
    with svc._chat_sessions_lock:
        svc._chat_sessions["private:10001"] = {
            "last_active_ts": controlled_time.time(),
            "messages": _make_history(350),
        }

    out = svc._chat_with_context_sync("private:10001", "hi")
    assert out == "reply"
    sent = payloads[0]["messages"]
    assert len(sent) == 1 + 300 + 1  # system + 300 history + user
    assert sent[1]["content"] == "msg-50"
    assert sent[-1]["content"] == "hi"


def test_gemini_chat_with_context_uses_backend_window(monkeypatch, controlled_time) -> None:
    svc = _new_aisvc()
    captured: dict[str, Any] = {}

    def _fake_run(prompt, model, restricted=False, timeout_seconds=None):
        captured["prompt"] = prompt
        captured["restricted"] = restricted
        return "gemini reply"

    monkeypatch.setattr(svc, "_run_gemini_cli_sync", _fake_run)

    with svc._chat_sessions_lock:
        svc._chat_sessions["private:10001"] = {
            "last_active_ts": controlled_time.time(),
            "messages": _make_history(150),
        }

    # gemini：只取最后 100 条（150 条中 msg-50 ~ msg-149）
    out = svc._gemini_chat_with_context_sync("private:10001", "hi", "gemini", True)
    assert out == "gemini reply"
    assert "msg-49" not in captured["prompt"]
    assert "msg-50" in captured["prompt"]
    assert "msg-149" in captured["prompt"]

    # claude：同样 100 条（独立会话，避免 gemini 调用写入的历史干扰）
    with svc._chat_sessions_lock:
        svc._chat_sessions["private:10002"] = {
            "last_active_ts": controlled_time.time(),
            "messages": _make_history(150),
        }
    captured2: dict[str, Any] = {}
    monkeypatch.setattr(svc, "_run_gemini_cli_sync", lambda prompt, model, restricted=False, timeout_seconds=None: captured2.__setitem__("prompt", prompt) or "claude reply")
    svc._gemini_chat_with_context_sync("private:10002", "hi", "claude", True)
    assert "msg-49" not in captured2["prompt"]
    assert "msg-50" in captured2["prompt"]
    assert "msg-149" in captured2["prompt"]


def test_gemini_call_does_not_delete_deepseek_range(controlled_time) -> None:
    svc = _new_aisvc()
    with svc._chat_sessions_lock:
        svc._chat_sessions["private:10001"] = {
            "last_active_ts": controlled_time.time(),
            "messages": _make_history(250),
        }

    # gemini 调用后，101~300 条共享历史仍在
    svc._select_history_for_backend(_make_history(250), "gemini")
    history = svc._load_active_chat_history("private:10001")
    assert len(history) == 250
    assert "msg-50" in {m["content"] for m in history}


# ============ 内联与格式化 ============


def test_vision_description_inlined_single_user_message() -> None:
    ctx = VisionContext(
        descriptions=[
            VisualDescription(1, "image", "类型：表情包；画面：猫"),
            VisualDescription(2, "image", "类型：截图；画面：报错信息"),
        ]
    )
    out = compose_ai_context_text("你看看", ctx)
    assert out.count("[视觉内容") == 2
    # 内联在同一条文本中，而不是两条 user 消息
    assert "[视觉内容1]" in out
    assert "[视觉内容2]" in out


def test_builtin_vision_boundary_prompt_in_system() -> None:
    svc = _new_aisvc()
    out = svc._append_chat_automation_boundary("base")
    assert "视觉内容" in out
    assert "外部视觉模型" in out
    # 幂等
    out2 = svc._append_chat_automation_boundary(out)
    assert out2 == out


@pytest.mark.asyncio
async def test_dispatch_reply_resolves_pending_images_and_applies_history(dispatch_harness) -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    aisvc = _FakeAIService()
    # 模拟真实 AIService：存在待识别图片
    aisvc._load_pending_vision = Mock(return_value=[{"url": "https://a/1.png", "file_id": "f1"}])
    vision = _FakeVisionSkill()

    await commands.dispatch(
        api=SimpleNamespace(),
        ctx=ctx,
        evt=_image_evt(group=False),
        text="这是什么",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    # 回复时解析全部 pending（describe_pending），并写回历史 + 清空 pending
    assert any("pending" in c for c in vision.calls)
    aisvc.apply_vision_descriptions_to_history.assert_called_once()
    aisvc.clear_pending_vision.assert_called_once()
    aisvc.chat_with_context.assert_awaited_once()
    _, ai_input, *_rest = aisvc.chat_with_context.await_args.args
    assert "类型：表情包" in ai_input


def test_apply_vision_descriptions_replaces_placeholders(controlled_time) -> None:
    from aisvc import AIService

    svc = AIService(log=_DummyLog())
    svc.deepseek_base_url = "https://example.local/v1"
    svc.deepseek_api_key = "fake-key"
    svc.system_prompt = "system-prompt"

    with svc._chat_sessions_lock:
        svc._chat_sessions["group:20001"] = {
            "last_active_ts": controlled_time.time(),
            "messages": [
                {"role": "user", "content": "看图\n\n[图片待识别]"},
                {"role": "assistant", "content": "ok"},
            ],
        }

    unused = svc.apply_vision_descriptions_to_history("group:20001", ["类型：表情包；画面：猫"])
    assert unused == 0
    history = svc._load_active_chat_history("group:20001")
    assert "[图片待识别]" not in history[0]["content"]
    assert "[视觉内容1] 类型：表情包；画面：猫" in history[0]["content"]


def test_pending_vision_ttl_and_unbounded_accumulation(controlled_time) -> None:
    from aisvc import AIService

    svc = AIService(log=_DummyLog())
    svc.record_pending_vision("group:20001", [{"url": "https://a/1.png", "file_id": "f1"}])
    controlled_time.advance(10)
    svc.record_pending_vision("group:20001", [{"url": "https://a/2.png", "file_id": "f2"}])
    assert len(svc._load_pending_vision("group:20001")) == 2

    # TTL 内按实际图片数量累积，不设条数上限
    for i in range(30):
        svc.record_pending_vision("group:20001", [{"url": f"https://a/{i}.png", "file_id": f"f{i}"}])
    assert len(svc._load_pending_vision("group:20001")) == 32

    # TTL 过期后全部清理
    controlled_time.advance(30 * 60 + 1)
    assert svc._load_pending_vision("group:20001") == []
    svc.clear_pending_vision("group:20001")


def test_extract_reply_msg_id() -> None:
    # 数组 reply 段
    evt1 = {"message": [{"type": "reply", "data": {"id": "12345"}}, {"type": "text", "data": {"text": "看看"}}]}
    assert commands._extract_reply_msg_id(evt1) == "12345"
    # CQ reply
    evt2 = {"raw_message": "[CQ:reply,id=67890] 这张图什么意思"}
    assert commands._extract_reply_msg_id(evt2) == "67890"
    # 无引用
    assert commands._extract_reply_msg_id({"message": [{"type": "text", "data": {"text": "hi"}}]}) is None
    assert commands._extract_reply_msg_id({}) is None


def test_msg_images_cache_ttl_and_lookup(controlled_time) -> None:
    from aisvc import AIService

    svc = AIService(log=_DummyLog())
    svc.record_msg_images("111", [{"url": "https://a/1.png", "file_id": "f1"}])
    svc.record_msg_images("222", [{"url": "https://a/2.png", "file_id": "f2"}, {"url": "https://a/3.png", "file_id": "f3"}])

    assert svc._lookup_msg_images("111") == [{"url": "https://a/1.png", "file_id": "f1"}]
    assert len(svc._lookup_msg_images("222")) == 2
    assert svc._lookup_msg_images("999") == []

    controlled_time.advance(30 * 60 + 1)
    assert svc._lookup_msg_images("111") == []
    assert svc._lookup_msg_images("222") == []


@pytest.mark.asyncio
async def test_dispatch_reply_includes_referenced_image_in_pending(dispatch_harness) -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill()
    # 被引用消息的图片（本地缓存命中）；模拟记录后 pending 队列可被回复时读取
    aisvc._lookup_msg_images = Mock(return_value=[{"url": "https://ref/1.png", "file_id": "rf1"}])
    aisvc._load_pending_vision = Mock(return_value=[{"url": "https://ref/1.png", "file_id": "rf1"}])
    evt = {
        "post_type": "message",
        "message_type": "private",
        "sub_type": "friend",
        "self_id": "1622236011",
        "message_id": "777",
        "message": [
            {"type": "reply", "data": {"id": "111"}},
            {"type": "text", "data": {"text": "这张图什么意思"}},
        ],
        "raw_message": "",
    }

    await commands.dispatch(
        api=SimpleNamespace(),
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

    # 被引用图进入 pending，且本次输入含其描述
    aisvc.record_pending_vision.assert_called_once()
    recorded = aisvc.record_pending_vision.call_args.args[1]
    assert any(x["file_id"] == "rf1" for x in recorded)
    aisvc.chat_with_context.assert_awaited_once()
    _, ai_input, *_rest = aisvc.chat_with_context.await_args.args
    assert "类型：表情包" in ai_input


@pytest.mark.asyncio
async def test_dispatch_reply_falls_back_to_get_msg(dispatch_harness) -> None:
    ctx = _make_ctx(scene="private_friend", group_id=None)
    aisvc = _FakeAIService()
    vision = _FakeVisionSkill()
    # 本地缓存无命中，走 get_msg API；模拟记录后 pending 队列可被回复时读取
    aisvc._lookup_msg_images = Mock(return_value=[])
    aisvc._load_pending_vision = Mock(return_value=[{"url": "https://ref/2.png", "file_id": "rf2"}])

    async def _fake_call(action: str, params: dict, timeout: float = 8.0):
        if action == "get_msg":
            return {
                "data": {
                    "message": [
                        {"type": "image", "data": {"url": "https://ref/2.png", "file_id": "rf2"}}
                    ]
                }
            }
        return None

    api = SimpleNamespace(call=_fake_call)
    evt = {
        "post_type": "message",
        "message_type": "private",
        "sub_type": "friend",
        "self_id": "1622236011",
        "message_id": "778",
        "message": [
            {"type": "reply", "data": {"id": "999"}},
            {"type": "text", "data": {"text": "看看这个"}},
        ],
        "raw_message": "",
    }

    await commands.dispatch(
        api=api,
        ctx=ctx,
        evt=evt,
        text="看看这个",
        filesvc=_make_filesvc_stub(),
        logsvc=_DummyLogService(),
        state=commands.BotState(),
        handin=Mock(),
        perm=Mock(),
        aisvc=aisvc,
        vision_skill=vision,
    )

    aisvc.record_pending_vision.assert_called_once()
    recorded = aisvc.record_pending_vision.call_args.args[1]
    assert any(x["file_id"] == "rf2" for x in recorded)
