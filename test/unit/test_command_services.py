from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import Mock

import cooper_bot.commands.command_services as cs


def test_format_simple_success_and_failure() -> None:
    ok = cs.format_simple_success("done", data={"k": 1})
    assert ok.ok is True
    assert ok.message == "done"
    assert ok.data == {"k": 1}
    assert ok.error_code == ""

    bad = cs.format_simple_failure("bad", error_code="E_BAD")
    assert bad.ok is False
    assert bad.message == "bad"
    assert bad.error_code == "E_BAD"


def test_normalize_target_ids_group_success() -> None:
    out = cs.normalize_target_ids(chat_type="group", chat_id="123456")
    assert out.ok is True
    assert out.data["chat_type"] == "group"
    assert out.data["group_id"] == 123456
    assert out.data["target_id"] == 123456


def test_normalize_target_ids_private_success() -> None:
    out = cs.normalize_target_ids(chat_type="private", user_id=123456789)
    assert out.ok is True
    assert out.data["chat_type"] == "private"
    assert out.data["user_id"] == 123456789
    assert out.data["target_id"] == 123456789


def test_normalize_target_ids_invalid_input() -> None:
    out1 = cs.normalize_target_ids(chat_type="group", group_id="abc")
    assert out1.ok is False
    assert out1.error_code == "INVALID_GROUP_ID"

    out2 = cs.normalize_target_ids(chat_type="private", user_id=0)
    assert out2.ok is False
    assert out2.error_code == "INVALID_USER_ID"

    out3 = cs.normalize_target_ids(chat_type="channel", chat_id=1)
    assert out3.ok is False
    assert out3.error_code == "INVALID_CHAT_TYPE"


def test_command_service_formatter_target_label() -> None:
    assert cs.CommandServiceFormatter.format_target_label("group", 123) == "群 123"
    assert cs.CommandServiceFormatter.format_target_label("private", 456) == "用户 456"


def test_run_list_dir_query_reuses_filesvc_result() -> None:
    filesvc = SimpleNamespace(list_dir=Mock(return_value=(True, "目录内容：\n- a.txt")))
    ctx = SimpleNamespace(level=1)
    out = cs.run_list_dir_query(filesvc=filesvc, ctx=ctx, path_arg="public")
    assert out.ok is True
    assert out.message == "目录内容：\n- a.txt"
    assert out.data["path_arg"] == "public"
    filesvc.list_dir.assert_called_once_with(ctx, "public")


def test_run_find_query_with_and_without_keyword_requirement() -> None:
    filesvc = SimpleNamespace(find=Mock(return_value=["a.txt", "b.txt"]))
    ctx = SimpleNamespace(level=1)

    no_kw = cs.run_find_query(filesvc=filesvc, ctx=ctx, keyword="", require_keyword=True)
    assert no_kw.ok is False
    assert no_kw.error_code == "MISSING_KEYWORD"
    filesvc.find.assert_not_called()

    out = cs.run_find_query(filesvc=filesvc, ctx=ctx, keyword="高数资料", in_dir="public", require_keyword=True)
    assert out.ok is True
    assert out.data["keyword"] == "高数资料"
    assert out.data["in_dir"] == "public"
    assert out.data["hits"] == ["a.txt", "b.txt"]
    filesvc.find.assert_called_once_with(ctx, "高数资料", in_dir="public")


class _FakeTask:
    def __init__(
        self,
        *,
        task_id: str,
        group_id: int,
        creator_id: int,
        name: str,
        deadline_ts: float,
        closed: bool = False,
        cancelled: bool = False,
    ) -> None:
        self.task_id = task_id
        self.group_id = group_id
        self.creator_id = creator_id
        self.name = name
        self.deadline_ts = float(deadline_ts)
        self.closed = bool(closed)
        self.cancelled = bool(cancelled)

    def is_active(self, now: float | None = None) -> bool:
        now_ts = time.time() if now is None else float(now)
        return (not self.closed) and (not self.cancelled) and now_ts < self.deadline_ts


def test_list_handin_tasks_for_group_sorted_and_filtered() -> None:
    now = time.time()
    t1 = _FakeTask(task_id="1", group_id=123, creator_id=900001, name="作业1", deadline_ts=now + 3600)
    t2 = _FakeTask(task_id="2", group_id=123, creator_id=900001, name="作业2", deadline_ts=now + 7200)
    t3 = _FakeTask(task_id="3", group_id=123, creator_id=900001, name="作业3", deadline_ts=now - 3600, closed=True)
    handin = SimpleNamespace(
        list_tasks_by_group=Mock(return_value=[t1, t2, t3]),
        list_active_tasks_by_group=Mock(return_value=[t1, t2]),
        is_task_gettable=Mock(side_effect=lambda t: t is not t3),
    )
    out = cs.list_handin_tasks_for_group(
        handin=handin,
        group_id=123,
        include_closed=True,
        active_only=False,
        only_gettable=True,
        sort_mode="active_then_deadline_desc",
    )
    assert out.ok is True
    tasks = out.data["tasks"]
    assert [x.task_id for x in tasks] == ["2", "1"]


def test_get_handin_task_summary_and_cancel_by_identity() -> None:
    now = time.time()
    task = _FakeTask(task_id="g123:作业1:1", group_id=123, creator_id=900001, name="作业1", deadline_ts=now + 3600)
    cancelled = {"called": False}

    def _cancel_task(task_id: str, by_user_id: int):
        cancelled["called"] = True
        assert task_id == "g123:作业1:1"
        assert by_user_id == 900001
        return True, "已取消任务「作业1」（群 123）。"

    handin = SimpleNamespace(
        list_tasks_by_group=Mock(return_value=[task]),
        cancel_task=Mock(side_effect=_cancel_task),
    )
    summary = cs.get_handin_task_summary(task, now_ts=now, pretty_ts_func=lambda _ts: "2026-04-12 23:59")
    assert summary.ok is True
    assert "[进行中]" in summary.message
    assert "作业1" in summary.message

    out = cs.cancel_handin_task_by_identity(
        handin=handin,
        group_id=123,
        by_user_id=900001,
        requester_level=3,
        task_name="作业1",
    )
    assert out.ok is True
    assert cancelled["called"] is True


def test_cancel_handin_task_by_identity_ambiguous_name() -> None:
    now = time.time()
    t1 = _FakeTask(task_id="1", group_id=123, creator_id=900001, name="作业1", deadline_ts=now + 1000)
    t2 = _FakeTask(task_id="2", group_id=123, creator_id=900001, name="作业1", deadline_ts=now + 2000)
    handin = SimpleNamespace(list_tasks_by_group=Mock(return_value=[t1, t2]), cancel_task=Mock())
    out = cs.cancel_handin_task_by_identity(
        handin=handin,
        group_id=123,
        by_user_id=900001,
        requester_level=3,
        task_name="作业1",
    )
    assert out.ok is False
    assert out.error_code == "AMBIGUOUS_TASK_NAME"
    handin.cancel_task.assert_not_called()
