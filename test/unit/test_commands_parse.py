from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest

import commands


def _fake_filesvc_roots():
    return [
        SimpleNamespace(name="public"),
        SimpleNamespace(name="friend"),
        SimpleNamespace(name="admin"),
    ]


def test_parse_find_args_keyword_only() -> None:
    filesvc = SimpleNamespace(roots=_fake_filesvc_roots())
    kw, in_dir = commands._parse_find_args("calculus", filesvc)
    assert kw == "calculus"
    assert in_dir is None


def test_parse_find_args_with_in_dir() -> None:
    filesvc = SimpleNamespace(roots=_fake_filesvc_roots())
    kw, in_dir = commands._parse_find_args("calculus notes public/math", filesvc)
    assert kw == "calculus notes"
    assert in_dir == "public/math"


def test_parse_find_args_without_valid_dir_suffix() -> None:
    filesvc = SimpleNamespace(roots=_fake_filesvc_roots())
    kw, in_dir = commands._parse_find_args("calculus final review", filesvc)
    assert kw == "calculus final review"
    assert in_dir is None


def test_parse_semantic_find_query() -> None:
    assert commands._parse_semantic_find_query('"考试安排"') == "考试安排"
    assert commands._parse_semantic_find_query("“实验报告模板”") == "实验报告模板"


def test_parse_semantic_find_query_rejects_invalid_tail() -> None:
    assert commands._parse_semantic_find_query('"考试安排" public') is None
    assert commands._parse_semantic_find_query("考试安排") is None


def test_parse_signin_deadline_accepts_ascii_and_chinese_colon() -> None:
    assert commands._parse_signin_deadline_hhmm("07:59") == (7, 59)
    assert commands._parse_signin_deadline_hhmm("07：59") == (7, 59)


def test_parse_signin_deadline_rejects_invalid_time() -> None:
    assert commands._parse_signin_deadline_hhmm("24:00") is None
    assert commands._parse_signin_deadline_hhmm("07:60") is None
    assert commands._parse_signin_deadline_hhmm("0759") is None


def test_signin_clock_delta_wraps_midnight() -> None:
    assert commands._clock_delta_seconds("23:59:30", "00:00:20") == 50.0


def test_signin_eval_checks_visual_timestamp_before_now() -> None:
    res = SimpleNamespace(time_text="07:57:21", visual_time_text="07:57:21", timestamp_time_text="08:01:00")
    assert "时间戳" in (commands._evaluate_signin_ocr_result(res, now_ts=time.time()) or "")


def test_signin_eval_rejects_timestamp_without_visual_time() -> None:
    now_ts = time.mktime((2026, 5, 12, 7, 58, 0, 0, 0, -1))
    res = SimpleNamespace(time_text="07:57:21", visual_time_text="", timestamp_time_text="07:57:21")
    assert "未识别到有效时间" in (commands._evaluate_signin_ocr_result(res, now_ts=now_ts) or "")


def test_signin_eval_uses_average_time_against_deadline() -> None:
    deadline_ts = time.mktime((2026, 5, 12, 8, 20, 0, 0, 0, -1))
    res = SimpleNamespace(time_text="07:57:00", visual_time_text="07:57:00", timestamp_time_text="08:00:00")
    assert commands._signin_average_image_time_text(res) == "07:58:30"
    assert commands._evaluate_signin_ocr_result(res, deadline_ts=deadline_ts) is None


def test_signin_eval_rejects_average_far_from_deadline() -> None:
    deadline_ts = time.mktime((2026, 5, 12, 8, 40, 0, 0, 0, -1))
    res = SimpleNamespace(time_text="07:57:00", visual_time_text="07:57:00", timestamp_time_text="08:00:00")
    assert "截止时间" in (commands._evaluate_signin_ocr_result(res, deadline_ts=deadline_ts) or "")


def test_extract_signin_image_items_accepts_message_string_cq() -> None:
    evt = {"message": "[CQ:image,file=abc.jpg,url=http://example.test/a.jpg]"}
    items = commands._extract_signin_image_items(evt)
    assert len(items) == 1
    assert items[0]["file"] == "abc.jpg"
    assert items[0]["url"] == "http://example.test/a.jpg"


@pytest.mark.asyncio
async def test_private_signin_image_then_name_records_submission(monkeypatch) -> None:
    replies: list[str] = []

    async def _fake_reply(_api, _ctx, text: str, _logsvc, force_private_user_id=None) -> None:
        _ = force_private_user_id
        replies.append(str(text))

    img_path = commands.DATA_DIR / "temp" / f"signin_test_{time.time_ns()}.jpg"
    img_path.parent.mkdir(parents=True, exist_ok=True)
    img_path.write_bytes(b"fake")
    now_text = time.strftime("%H:%M:%S", time.localtime())

    async def _fake_download(*_args, **_kwargs):
        return True, "", img_path

    monkeypatch.setattr(commands, "reply", _fake_reply)
    monkeypatch.setattr(commands, "_download_signin_image", _fake_download)

    import signin_ocr

    monkeypatch.setattr(
        signin_ocr,
        "recognize_led_time_from_path",
        lambda _path: SimpleNamespace(
            time_text=now_text,
            visual_time_text=now_text,
            timestamp_time_text=now_text,
            source="visual",
        ),
    )

    ctx = SimpleNamespace(scene="private_friend", group_id=None, user_id=200, card="", nickname="tester")
    state = commands.BotState()
    state.signin_tasks[100] = {
        "task_id": "t1",
        "creator_id": 300,
        "deadline_ts": time.time() + 60.0,
        "submitted_names": [],
        "submitted_users": {},
        "failures": {},
        "failure_notified": [],
    }
    handin = SimpleNamespace(
        _get_roster_names=lambda: ["Alice"],
        find_roster_name_in_filename=lambda _text, roster_names=None: "Alice",
    )
    logsvc = SimpleNamespace(log=SimpleNamespace(warning=lambda *_args, **_kwargs: None), log_in=lambda *_args: None)
    evt = {"message": "[CQ:image,file=abc.jpg,url=http://example.test/a.jpg]", "time": time.time()}

    handled = await commands._handle_signin_image(SimpleNamespace(), ctx, evt, logsvc, state, handin)

    assert handled is True
    assert 200 in state.pending_signin_name_input
    assert state.signin_tasks[100]["submitted_names"] == []
    assert any("请回复你的姓名" in one for one in replies)

    handled_name = await commands._handle_private_signin_name_input(SimpleNamespace(), ctx, "Alice", logsvc, state, handin)
    assert handled_name is True
    assert state.signin_tasks[100]["submitted_names"] == ["Alice"]
    assert any("signin成功" in one for one in replies)


@pytest.mark.asyncio
async def test_finish_signin_waits_active_jobs(monkeypatch) -> None:
    monkeypatch.setattr(commands, "_SIGNIN_FINALIZE_GRACE_SECONDS", 0.0)
    sent: list[str] = []

    async def _send_private_msg(_user_id: int, text: str):
        sent.append(str(text))
        return {"status": "ok"}

    state = commands.BotState()
    task = {
        "task_id": "t1",
        "creator_id": 300,
        "deadline_ts": time.time() - 1.0,
        "submitted_names": [],
        "submitted_users": {},
        "active_jobs": 1,
        "idle_event": asyncio.Event(),
    }
    state.signin_tasks[100] = task
    handin = SimpleNamespace(_get_roster=lambda: [("001", "Alice")])
    logsvc = SimpleNamespace(log=SimpleNamespace(warning=lambda *_args, **_kwargs: None))

    finish_task = asyncio.create_task(
        commands._finish_signin_task(SimpleNamespace(send_private_msg=_send_private_msg), state, 100, handin, logsvc, task_id="t1")
    )
    await asyncio.sleep(0.05)
    assert 100 in state.signin_tasks
    task["submitted_names"].append("Alice")
    commands._signin_end_job(task)
    assert await finish_task is True
    assert 100 not in state.signin_tasks
    assert sent and "已签到：1/1" in sent[-1]


def test_extract_ai_chat_input_private_keeps_leading_c() -> None:
    ctx = SimpleNamespace(scene="private_friend")
    out = commands._extract_ai_chat_input(ctx, evt={}, text="C你好", bot_nick="Cooper_bot")
    assert out == "C你好"


def test_extract_ai_chat_input_group_requires_mention() -> None:
    ctx = SimpleNamespace(scene="group")
    evt = {
        "self_id": "42",
        "message": [
            {"type": "at", "data": {"qq": "42"}},
            {"type": "text", "data": {"text": " 你好"}},
        ],
    }
    out = commands._extract_ai_chat_input(ctx, evt=evt, text="你好", bot_nick="Cooper_bot")
    assert out == "你好"
