from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from cooper_bot.commands import commands
from cooper_bot.commands import kick_commands
from cooper_bot.modules.moderation import service as service_module
from cooper_bot.modules.moderation.review import ReviewResult, _strict_json_result, review_snapshot
from cooper_bot.modules.moderation.rules import calculate_local_votes, parse_group_level, with_member_level
from cooper_bot.modules.moderation.service import ModerationService, api_outcome
from cooper_bot.modules.moderation.store import ModerationStore
from cooper_bot.core.onebot import OneBotAPI


def _text_record(message_id: str, occurred_at: float, text: str) -> dict:
    return {
        "self_id": 500,
        "group_id": 700,
        "user_id": 100,
        "message_id": message_id,
        "occurred_at": occurred_at,
        "received_at": occurred_at,
        "time_source": "event",
        "nickname": "member",
        "card": "",
        "segments": [{"type": "text", "data": {"text": text}}],
        "plain_text": text,
    }


def test_group_level_is_strict_and_votes_are_bounded():
    assert [parse_group_level(value) for value in (0, "0", "２", 3, None, True, 1.0, "LV2", -1)] == [
        0, 0, 2, 3, None, None, None, None, None,
    ]
    now = 1000.0
    text_a = "课程资料与作业讨论，内容完整。" * 4
    text_b = "可以添加微信了解详情 https://example.invalid/offer " + ("课程安排说明。" * 4)
    vote = calculate_local_votes(
        [_text_record("1", now - 10, text_a), _text_record("2", now, text_b)], now,
    )
    full = with_member_level(vote, "1")
    assert vote.votes == {"L": 0, "B": 2, "U": 1, "I": 0, "K": 3, "G": 0}
    assert full.total == 7
    assert with_member_level(vote, True).votes["L"] == 0
    assert with_member_level(vote, 4).votes["L"] == 1
    assert with_member_level(vote, 5).votes["L"] == 0


def test_short_burst_vote_uses_closed_20_second_window_and_100_characters():
    anchor = 1000.0
    exactly_100 = [_text_record("a", 980.0, "x" * 50), _text_record("b", anchor, "y" * 50)]
    ninety_nine = [_text_record("a", 980.0, "x" * 49), _text_record("b", anchor, "y" * 50)]
    outside_window = [_text_record("a", 979.999, "x" * 100), _text_record("b", anchor, "y" * 100)]
    duplicate_id = [_text_record("same", 980.0, "x" * 100), _text_record("same", anchor, "y" * 100)]
    assert calculate_local_votes(exactly_100, anchor).votes["B"] == 2
    assert calculate_local_votes(ninety_nine, anchor).votes["B"] == 0
    assert calculate_local_votes(outside_window, anchor).votes["B"] == 0
    assert calculate_local_votes(duplicate_id, anchor).votes["B"] == 0


def test_direct_new_group_phrase_with_image_reaches_review_at_low_member_level():
    row = _text_record("401", 1000.0, "本群即将解散，请扫下方二维码进入新群")
    row["segments"].append({"type": "image", "data": {"file": "ad.jpg"}})
    votes = with_member_level(calculate_local_votes([row], 1000.0), 1)

    assert votes.votes["L"] == 1
    assert votes.votes["I"] == 1
    assert votes.votes["G"] == 3
    assert votes.total == 5
    assert votes.needs_review


def test_group_migration_and_new_group_hint_combine_only_inside_vote_window():
    anchor = 1000.0
    migration = _text_record("402", anchor - 5, "本群即将解散")
    new_group = _text_record("403", anchor, "新群二维码在群公告")
    assert calculate_local_votes([migration, new_group], anchor).votes["G"] == 3

    card_hint = _text_record("407", anchor, "")
    card_hint["segments"] = [{"type": "share", "data": {"title": "新群二维码", "content": "请查看群公告"}}]
    assert calculate_local_votes([migration, card_hint], anchor).votes["G"] == 3

    outside_window = _text_record("404", anchor - 20.001, "本群即将解散")
    assert calculate_local_votes([outside_window, new_group], anchor).votes["G"] == 0


@pytest.mark.parametrize("text", ["本群即将解散", "新群有多少人", "QQ群里今天很热闹"])
def test_group_migration_vote_requires_a_specific_new_group_hint(text):
    assert calculate_local_votes([_text_record("405", 1000.0, text)], 1000.0).votes["G"] == 0


def test_group_invite_does_not_change_b_vote_or_review_threshold():
    anchor = 1000.0
    long_text = "本群即将解散，请进入新群。" + ("补充说明。" * 30)
    row = _text_record("same", anchor, long_text)
    local = calculate_local_votes([row], anchor)
    assert local.text_count >= 100
    assert local.votes["B"] == 0
    assert local.votes["G"] == 3

    duplicate_id = _text_record("same", anchor - 1, long_text)
    duplicate = calculate_local_votes([duplicate_id, row], anchor)
    assert duplicate.votes["B"] == 0

    low_level = with_member_level(local, 1)
    assert low_level.total == 4
    assert low_level.needs_review is True


def test_group_invite_uses_existing_visible_text_normalization():
    row = _text_record("406", 1000.0, "本群即将解散，请进入\u200b新群")
    assert calculate_local_votes([row], 1000.0).votes["G"] == 3


def test_store_deduplicates_by_message_id_and_preserves_case_evidence(tmp_path):
    store = ModerationStore(tmp_path / "moderation.sqlite3")
    assert store.get_group_state(500, 700)["enabled"] is False
    assert store.set_group_enabled(500, 700, True, 1, 1000.0)

    first = _text_record("-12", 999.0, "同一内容")
    assert store.append_message_if_enabled(first, 1)[0] is True
    assert store.append_message_if_enabled(first, 1)[0] is False
    second = _text_record("13", 999.0, "同一内容")
    assert store.append_message_if_enabled(second, 1)[0] is True
    rows = store.read_sender_window(500, 700, 100, 900.0, 1000.0)
    assert [row["message_id"] for row in rows] == ["-12", "13"]

    case_id = store.reserve_review(500, 700, 100, 1, "13", rows[-1]["seq"], 1000.0, {"total": 3})
    assert case_id
    evidence = [{"message_id": "13", "source": "text", "slot_id": None, "quote": "同一内容"}]
    store.update_case_decision(case_id, "running", "confirmed", evidence)
    store.update_case_decision(case_id, "block", "complete", None, 1001.0)
    assert store.get_case(case_id)["evidence"] == evidence

    assert store.set_group_enabled(500, 700, False, 2, 1002.0)
    assert store.get_group_state(500, 700)["last_capture_gap_at"] == 1002.0
    assert store.append_message_if_enabled(_text_record("14", 1003.0, "closed"), 2) == (False, None)


def test_api_outcomes_keep_ambiguous_async_unknown():
    assert api_outcome({"status": "ok", "retcode": 0}) == "ok"
    assert api_outcome({"status": "failed", "retcode": 1}) == "failed"
    assert api_outcome({"status": "async", "retcode": 1}) == "unknown"
    assert api_outcome({"status": "ok", "retcode": True}) == "unknown"
    assert api_outcome(None) == "unknown"


def test_explicit_untrusted_evidence_recipient_is_rejected(monkeypatch):
    monkeypatch.setattr(kick_commands, "ADMIN_USERS", {900})
    monkeypatch.setattr(kick_commands, "KICK_AUDIT_QQ", -1)
    assert kick_commands._audit_recipient(_FakePerm()) is None
    monkeypatch.setattr(kick_commands, "KICK_AUDIT_QQ", 901)
    assert kick_commands._audit_recipient(_FakePerm()) is None
    monkeypatch.setattr(kick_commands, "KICK_AUDIT_QQ", 900)
    assert kick_commands._audit_recipient(_FakePerm()) == 900


@pytest.mark.asyncio
async def test_onebot_ws_echoes_are_unique_and_match_out_of_order_responses():
    class _WS:
        def __init__(self):
            self.sent = []

        async def send(self, payload):
            self.sent.append(json.loads(payload))

    class _Log:
        def warning(self, _message):
            return

    ws = _WS()
    api = OneBotAPI(ws, _Log())
    first = asyncio.create_task(api._call_ws("get_group_member_info", {"user_id": 1}, 1.0))
    second = asyncio.create_task(api._call_ws("get_group_member_info", {"user_id": 2}, 1.0))
    await asyncio.sleep(0)
    assert len(ws.sent) == 2
    assert ws.sent[0]["echo"] != ws.sent[1]["echo"]
    api.feed_response({"echo": ws.sent[1]["echo"], "user_id": 2})
    api.feed_response({"echo": ws.sent[0]["echo"], "user_id": 1})
    assert (await first)["user_id"] == 1
    assert (await second)["user_id"] == 2


@pytest.mark.asyncio
async def test_onebot_write_http_timeout_does_not_retry_over_ws():
    class _WS:
        async def send(self, _payload):
            pytest.fail("write was retried over WebSocket")

    class _Log:
        def warning(self, _message):
            return

    api = OneBotAPI(_WS(), _Log(), http_base="http://localhost:3000")
    calls = []

    async def _http(action, params, timeout):
        calls.append((action, params, timeout))
        return None

    api._call_http = _http
    result = await api.set_group_kick(700, 100, reject_add_request=False)
    assert result is None
    assert len(calls) == 1


class _FakeVision:
    ready = True

    def __init__(self, fail_slot=None):
        self.resolve_calls = 0
        self.fail_slot = fail_slot

    def _build_segment(self, index, segment_type, data):
        return SimpleNamespace(
            url=str(data.get("url") or ""), file=str(data.get("file") or ""),
            file_id=str(data.get("file_id") or ""), path=str(data.get("path") or ""),
            name="", summary="", face_id="",
        )

    async def resolve_slots(self, _api, slots):
        self.resolve_calls += 1
        return [
            SimpleNamespace(
                slot_id=slot.slot_id,
                status="retryable_error" if slot.slot_id == self.fail_slot else "ready",
                description="" if slot.slot_id == self.fail_slot else f"已检查图片 {slot.slot_id}",
            )
            for slot in slots
        ]

    def apply_resolutions_to_slots(self, slots, resolutions):
        by_id = {item.slot_id: item for item in resolutions}
        for slot in slots:
            result = by_id.get(slot.slot_id)
            if result is not None:
                slot.status = result.status
                slot.description = result.description
        return slots


class _FakeGateway:
    def __init__(self, response=None):
        self.response = response
        self.payload = None

    def provider(self, _name):
        return SimpleNamespace(ready=True)

    def chat(self, messages, **_kwargs):
        self.payload = json.loads(messages[1]["content"])
        if self.response is not None:
            return self.response(self.payload)
        return json.dumps({"decision": "allow", "category": "none", "reason": "上下文正常", "evidence": []})


@pytest.mark.asyncio
async def test_review_visits_all_images_without_shared_per_message_cap():
    vision = _FakeVision()
    gateway = _FakeGateway()
    segments = [
        {"type": "image", "data": {"file_id": f"img-{index}", "url": f"https://media.invalid/{index}"}}
        for index in range(21)
    ]
    message = _text_record("50", time.time(), "附带图片")
    message["segments"] = segments
    result = await review_snapshot(
        api=None, gateway=gateway, vision_skill=vision, self_id=500, group_id=700, user_id=100,
        messages=[message], trigger_message_ids={"50"}, votes={"total": 3},
        deadline=time.time() + 60, still_current=lambda: True,
    )
    assert result.decision == "allow"
    assert len(gateway.payload["messages"][0]["images"]) == 21
    assert vision.resolve_calls == 6


def test_invalid_unhashable_model_enum_is_uncertain():
    result = _strict_json_result(
        json.dumps({"decision": ["block"], "category": "none", "reason": "", "evidence": []}),
        [], {}, {}, set(),
    )
    assert result == ReviewResult("uncertain", stop_reason="invalid_model_enum")


class _FakePerm:
    def get_level(self, user_id):
        return 3 if int(user_id) == 900 else 0


class _FakeAPI:
    def __init__(self):
        self.writes = []
        self.bot_role = "admin"

    async def get_group_member_info(self, group_id, user_id, no_cache=True):
        role = self.bot_role if int(user_id) == 500 else "member"
        level = 1 if int(user_id) == 100 else 8
        return {"status": "ok", "retcode": 0, "data": {"group_id": group_id, "user_id": user_id, "role": role, "level": level}}

    async def send_private_forward_msg(self, user_id, messages):
        self.writes.append(("forward", user_id, messages))
        return {"status": "ok", "retcode": 0}

    async def send_group_msg(self, group_id, message):
        self.writes.append(("notice", group_id, message))
        return {"status": "ok", "retcode": 0}

    async def set_group_kick(self, group_id, user_id, reject_add_request=False):
        self.writes.append(("kick", group_id, user_id, reject_add_request))
        return {"status": "ok", "retcode": 0}

    async def delete_msg(self, message_id):
        self.writes.append(("delete", int(message_id)))
        return {"status": "ok", "retcode": 0}

    async def send_private_msg(self, user_id, text):
        self.writes.append(("result", user_id, text))
        return {"status": "ok", "retcode": 0}


@pytest.mark.asyncio
async def test_observation_serializes_same_scope_store_order_without_blocking_off(tmp_path):
    now = time.time()
    store = ModerationStore(tmp_path / "observe.sqlite3")
    service = ModerationService(store=store, clock=lambda: now)
    api = _FakeAPI()
    state = service._scope_state((500, 700))
    state.update(enabled=True, revision=1, generation=0)
    service.active = True
    service.api = api
    service.aisvc = SimpleNamespace(gateway=_FakeGateway())
    service.vision_skill = _FakeVision()
    service._queue = asyncio.Queue(maxsize=64)
    assert store.set_group_enabled(500, 700, True, 1, now)

    original_append = store.append_message_if_enabled

    def _delay_first(record, revision):
        if record["message_id"] == "201":
            time.sleep(0.05)
        return original_append(record, revision)

    store.append_message_if_enabled = _delay_first
    ctx = SimpleNamespace(scene="group")
    first = {
        "post_type": "message", "message_type": "group", "self_id": 500, "group_id": 700,
        "message_id": 201, "time": now - 5, "sender": {"user_id": 100, "role": "member"},
        "message": [{"type": "text", "data": {"text": "招家教，" + "课程安排说明" * 8}}],
    }
    second = {
        "post_type": "message", "message_type": "group", "self_id": 500, "group_id": 700,
        "message_id": 202, "time": now - 1, "sender": {"user_id": 100, "role": "member"},
        "message": [{"type": "text", "data": {"text": "学习资料讨论与计划说明" * 6}}],
    }
    try:
        first_task = asyncio.create_task(service.observe_event(ctx, first))
        second_task = asyncio.create_task(service.observe_event(ctx, second))
        await asyncio.gather(first_task, second_task)
        assert service._queue.qsize() == 1
        assert service._jobs[(500, 700, 100)]["latest_seq"] == store.latest_seq()

        token = service.begin_control((500, 700), enabled=False)
        assert service._scope_state((500, 700))["enabled"] is False
        assert await service.disable_scope(token)
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_off_storage_failure_remains_off_after_reconnect(tmp_path):
    now = time.time()
    store = ModerationStore(tmp_path / "volatile-off.sqlite3")
    service = ModerationService(store=store, clock=lambda: now)
    api = _FakeAPI()
    vision = _FakeVision()
    aisvc = SimpleNamespace(gateway=_FakeGateway())
    try:
        await service.start(api, aisvc, vision, _FakePerm())
        on_token = service.begin_control((500, 700), enabled=True)
        assert await service.enable_scope(api=api, token=on_token, recipient=900)
        original_set_enabled = store.set_group_enabled

        def _fail_off(self_id, group_id, enabled, revision, timestamp):
            if not enabled:
                raise sqlite3.OperationalError("simulated storage failure")
            return original_set_enabled(self_id, group_id, enabled, revision, timestamp)

        store.set_group_enabled = _fail_off
        off_token = service.begin_control((500, 700), enabled=False)
        assert await service.disable_scope(off_token) is False
        assert service._scope_state((500, 700))["enabled"] is False
        await service.close()

        store.set_group_enabled = original_set_enabled
        await service.start(api, aisvc, vision, _FakePerm())
        assert service._scope_state((500, 700))["enabled"] is False
        assert store.get_group_state(500, 700)["enabled"] is True
    finally:
        store.set_group_enabled = original_set_enabled if "original_set_enabled" in locals() else store.set_group_enabled
        await service.close()


@pytest.mark.asyncio
async def test_on_requires_bot_admin_but_off_does_not(tmp_path):
    now = time.time()
    store = ModerationStore(tmp_path / "role-check.sqlite3")
    service = ModerationService(store=store, clock=lambda: now)
    api = _FakeAPI()
    aisvc = SimpleNamespace(gateway=_FakeGateway())
    vision = _FakeVision()
    try:
        await service.start(api, aisvc, vision, _FakePerm())
        api.bot_role = "member"
        first_token = service.begin_control((500, 700), enabled=True)
        assert await service.enable_scope(api=api, token=first_token, recipient=900) is False
        status = await service.status_scope(api, 500, 700, aisvc, vision)
        assert status["enabled"] is False and status["bot_role"] == "member"

        api.bot_role = "admin"
        on_token = service.begin_control((500, 700), enabled=True)
        assert await service.enable_scope(api=api, token=on_token, recipient=900)
        api.bot_role = "member"
        off_token = service.begin_control((500, 700), enabled=False)
        assert await service.disable_scope(off_token)
        assert store.get_group_state(500, 700)["enabled"] is False
    finally:
        await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("decision", ["block", "uncertain", "image_failure", "malformed_at", "malformed_reply"])
async def test_candidate_fake_success_order_and_uncertain_silence(tmp_path, monkeypatch, decision):
    now = time.time()
    trigger_at = now - 600.0 if decision == "block" else now - 2.0
    api = _FakeAPI()
    vision = _FakeVision(fail_slot="kick:500:700:102:21" if decision == "image_failure" else None)

    def _model_response(_payload):
        if decision == "uncertain":
            return json.dumps({"decision": "uncertain", "category": "none", "reason": "授权不明", "evidence": []})
        return json.dumps({
            "decision": "block", "category": "unsolicited_ad", "reason": "明确广告推广",
            "evidence": [{"message_id": "102", "source": "text", "slot_id": None, "quote": "添加微信"}],
        })

    gateway = _FakeGateway(response=_model_response)
    monkeypatch.setattr(service_module, "ADMIN_USERS", {900})
    monkeypatch.setattr(service_module, "KICK_AUDIT_QQ", 0)
    store = ModerationStore(tmp_path / f"{decision}.sqlite3")
    service = ModerationService(store=store, clock=lambda: now)
    try:
        await service.start(api, SimpleNamespace(gateway=gateway), vision, _FakePerm())
        token = service.begin_control((500, 700), enabled=True)
        assert await service.enable_scope(api=api, token=token, recipient=900)

        first = _text_record("101", trigger_at - 8, "课程资料说明与作业安排。" * 6)
        second = _text_record("102", trigger_at, "有偿家教推广，添加微信了解详情 https://example.invalid/offer。" + ("报名说明。" * 5))
        if decision == "block":
            second["segments"].extend([
                {"type": "at", "data": {"qq": "all"}},
                {"type": "reply", "data": {"id": "12345"}},
            ])
        elif decision.startswith("malformed_"):
            second["segments"].append({"type": decision.removeprefix("malformed_"), "data": {}})
        if decision == "image_failure":
            second["segments"].extend(
                {"type": "image", "data": {"file_id": f"img-{index}", "url": f"https://media.invalid/{index}"}}
                for index in range(21)
            )
        assert store.append_message_if_enabled(first, token["revision"])[0]
        inserted, seq = store.append_message_if_enabled(second, token["revision"])
        assert inserted and seq
        if decision == "block":
            for message_id, occurred_at in (
                ("103", trigger_at - 300.0),
                ("104", trigger_at + 300.0),
                ("105", trigger_at - 300.001),
                ("106", trigger_at + 300.001),
            ):
                assert store.append_message_if_enabled(
                    _text_record(message_id, occurred_at, "普通群聊内容"), token["revision"]
                )[0]
        state = service._scope_state((500, 700))
        await service._process_candidate(
            (500, 700, 100),
            {"revision": token["revision"], "generation": state["generation"], "queued_at": now,
             "latest_seq": seq, "latest_anchor_at": trigger_at},
        )
        cases = store.get_case(f"{int(now):x}-{int(seq):x}-{100:x}")
        assert cases is not None
        if decision in {"uncertain", "image_failure"}:
            assert cases["decision"] == "uncertain"
            assert api.writes == []
            if decision == "image_failure":
                assert gateway.payload is None
        elif decision.startswith("malformed_"):
            assert cases["decision"] == "skipped"
            assert api.writes == []
        else:
            assert cases["decision"] == "block"
            assert [item[0] for item in api.writes] == [
                "forward", "notice", "kick", "delete", "delete", "delete", "delete", "result",
            ]
            assert api.writes[2] == ("kick", 700, 100, False)
            assert [item[1] for item in api.writes if item[0] == "delete"] == [104, 102, 101, 103]
            notice_text = api.writes[1][2][0]["data"]["text"]
            assert "如认为踢出错误请找管理员申诉" in notice_text
            forward_node = next(
                node for node in api.writes[0][2][1:]
                if any("message_id=102" in segment.get("data", {}).get("text", "")
                       for segment in node["data"]["content"])
            )
            forward_content = forward_node["data"]["content"]
            forward_text = "".join(
                segment.get("data", {}).get("text", "")
                for segment in forward_content if segment.get("type") == "text"
            )
            assert "原@对象 QQ:all，未触发通知" in forward_text
            assert "原引用消息 ID:12345" in forward_text
            assert not any(segment.get("type") in {"at", "reply"} for segment in forward_content)
    finally:
        await service.close()


def test_forward_batches_split_by_count_and_preserve_long_text():
    service = ModerationService(store=SimpleNamespace())
    messages = [_text_record(str(index), 1_800_000_000 + index, f"原文{index}") for index in range(21)]
    batches = service._build_forward_batches(
        self_id=500, group_id=700, user_id=100, case_id="case", category="unsolicited_ad",
        recipient=900, messages=messages, action_at=1_800_000_100, enabled_at=1_800_000_000, gap_at=None,
    )
    assert [len(batch) - 1 for batch in batches] == [20, 1]
    assert "原始消息 21 条" in batches[0][0]["data"]["content"][0]["data"]["text"]

    long_text = "学" * 360_000
    long_message = _text_record("long", 1_800_000_050, long_text)
    long_batches = service._build_forward_batches(
        self_id=500, group_id=700, user_id=100, case_id="case-long", category="unsolicited_ad",
        recipient=900, messages=[long_message], action_at=1_800_000_100,
        enabled_at=1_800_000_000, gap_at=None,
    )
    recovered = "".join(
        segment["data"]["text"]
        for batch in long_batches
        for node in batch[1:]
        for segment in node["data"]["content"][1:]
    )
    assert recovered == long_text
    for batch in long_batches:
        payload = json.dumps(
            {"user_id": 900, "messages": batch}, ensure_ascii=False, separators=(",", ":")
        )
        assert len(payload.encode("utf-8")) <= 256 * 1024


def test_original_nodes_safely_preserve_mixed_at_reply_image_and_text_order():
    service = ModerationService(store=SimpleNamespace())
    occurred_at = 1_800_000_050.0
    row = _text_record("301", occurred_at, "请看结尾")
    row["segments"] = [
        {"type": "text", "data": {"text": "请看"}},
        {"type": "at", "data": {"qq": "all"}},
        {"type": "reply", "data": {"id": "12345"}},
        {"type": "image", "data": {"url": "https://media.invalid/test.jpg", "file": "test.jpg"}},
        {"type": "text", "data": {"text": "结尾"}},
    ]
    original_nodes = service._original_nodes(100, [row])
    batches = service._build_forward_batches(
        self_id=500, group_id=700, user_id=100, case_id="case-mixed", category="unsolicited_ad",
        recipient=900, messages=[row], action_at=1_800_000_100,
        enabled_at=1_800_000_000, gap_at=None,
    )

    node = original_nodes[0][0]
    content = node["data"]["content"]
    text_segments = [segment["data"]["text"] for segment in content if segment.get("type") == "text"]
    joined_text = "".join(text_segments)
    assert "message_id=301" in text_segments[0]
    assert service._message_time(occurred_at) in text_segments[0]
    assert joined_text.index("请看") < joined_text.index("原@对象 QQ:all")
    assert joined_text.index("原@对象 QQ:all") < joined_text.index("原引用消息 ID:12345")
    assert joined_text.index("原引用消息 ID:12345") < joined_text.index("结尾")
    assert not any(segment.get("type") in {"at", "reply"} for segment in content)
    assert next(segment for segment in content if segment.get("type") == "image") == row["segments"][3]

    batch_node = batches[0][1]
    assert batch_node["data"]["content"] == content
    for batch in batches:
        payload = json.dumps(
            {"user_id": 900, "messages": batch}, ensure_ascii=False, separators=(",", ":")
        )
        assert len(payload.encode("utf-8")) <= 256 * 1024


@pytest.mark.parametrize("kind", ["at", "reply"])
def test_original_nodes_reject_incomplete_at_or_reply_segments(kind):
    service = ModerationService(store=SimpleNamespace())
    row = _text_record("302", 1_800_000_050.0, "缺少字段")
    row["segments"] = [{"type": kind, "data": {}}]
    with pytest.raises(ValueError, match="incomplete original"):
        service._original_nodes(100, [row])


def test_record_from_event_preserves_array_segments_and_does_not_parse_cq_text():
    segments = [
        {"type": "text", "data": {"text": "看这里"}},
        {"type": "image", "data": {"file": "test.jpg"}},
        {"type": "at", "data": {"qq": "all"}},
        {"type": "reply", "data": {"id": "12345"}},
    ]
    event = {
        "post_type": "message", "message_type": "group", "self_id": 500, "group_id": 700,
        "user_id": 100, "message_id": 501, "time": 1000.0, "message": segments,
    }
    record = service_module._record_from_event(event, 1000.0)
    assert record["segments"] == segments
    assert [segment["type"] for segment in record["segments"]] == ["text", "image", "at", "reply"]

    cq_event = {**event, "message_id": 502, "message": "[CQ:image,file=not-a-real-image.jpg]"}
    cq_record = service_module._record_from_event(cq_event, 1000.0)
    assert [segment["type"] for segment in cq_record["segments"]] == ["text"]


@pytest.mark.asyncio
async def test_dispatch_handles_kick_before_regular_side_effects(monkeypatch):
    sent = []

    class _API:
        async def send_group_msg(self, group_id, message):
            sent.append((group_id, message))
            return {"status": "ok", "retcode": 0}

    class _Perm:
        def get_level(self, _user_id):
            return 0

    monkeypatch.setattr(kick_commands, "ADMIN_USERS", set())
    monkeypatch.setattr(commands, "_ensure_group_context_and_schedule_digest", lambda *_a, **_k: pytest.fail("ordinary dispatch ran"))
    ctx = SimpleNamespace(scene="group", user_id=123, nickname="member", card="", group_id=700, group_name="test", level=3)
    event = {"post_type": "message", "message_type": "group", "self_id": 500, "group_id": 700,
             "sender": {"user_id": 123}, "message": [{"type": "text", "data": {"text": "/kick on"}}]}
    await commands.dispatch(
        api=_API(), ctx=ctx, evt=event, text="/kick on", filesvc=None, logsvc=None, state=None,
        handin=None, perm=_Perm(), aisvc=None, vision_skill=None, calendar_service=None, moderation=None,
    )
    assert len(sent) == 1
    assert "个人 Bot 权限 2 或 3 级" in sent[0][1][0]["data"]["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "enabled", "reply"),
    [("on", True, "开杀😋"), ("off", False, "我不杀哩😭")],
)
async def test_kick_control_success_replies_with_requested_text(monkeypatch, action, enabled, reply):
    sent = []

    class _API:
        async def send_group_msg(self, group_id, message):
            sent.append((group_id, message))
            return {"status": "ok", "retcode": 0}

    class _Perm:
        def get_level(self, _user_id):
            return 3

    class _Service:
        def begin_control(self, _scope, *, enabled):
            assert enabled is expected_enabled
            return {"token": "control"}

        async def enable_scope(self, **_kwargs):
            return True

        async def disable_scope(self, _token):
            return True

    monkeypatch.setattr(kick_commands, "ADMIN_USERS", {900})
    monkeypatch.setattr(kick_commands, "KICK_AUDIT_QQ", 900)
    expected_enabled = enabled
    ctx = SimpleNamespace(scene="group", user_id=123, group_id=700)
    event = {
        "post_type": "message", "message_type": "group", "self_id": 500, "group_id": 700,
        "message": [{"type": "text", "data": {"text": f"/kick {action}"}}],
    }

    handled = await kick_commands.handle_kick_command(
        service=_Service(), api=_API(), ctx=ctx, evt=event, text=f"/kick {action}", perm=_Perm(),
    )

    assert handled is True
    assert sent == [(700, [{"type": "text", "data": {"text": reply}}])]
