from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import admin_exec
import admin_nl
from admin_models import AdminPlan, AdminStep


class _DummyLogger:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, msg: str) -> None:
        self.warnings.append(str(msg))


class _DummyLogService:
    def __init__(self) -> None:
        self.log = _DummyLogger()


def _make_exec_ctx(api) -> admin_exec.AdminExecutionContext:
    return admin_exec.AdminExecutionContext(
        api=api,
        ctx=SimpleNamespace(scene="private_friend", user_id=900001),
        evt={},
        text="",
        filesvc=None,
        logsvc=_DummyLogService(),
        state=None,
        handin=None,
        perm=None,
        aisvc=None,
    )


@pytest.mark.asyncio
async def test_execute_plan_rejects_non_whitelist_tool() -> None:
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    plan = AdminPlan(
        source="rule",
        summary="invalid",
        steps=[AdminStep(tool="dangerous_eval", args={})],
    )

    summary = await admin_exec.execute_plan(_make_exec_ctx(api), plan)

    assert not summary.ok
    assert "白名单" in summary.message
    api.send_group_msg.assert_not_awaited()
    api.send_private_msg.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_plan_invalid_args_returns_clear_error() -> None:
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    plan = AdminPlan(
        source="rule",
        summary="bad args",
        steps=[AdminStep(tool="send_group_message", args={"group_id": "abc", "text": "hi"})],
    )

    summary = await admin_exec.execute_plan(_make_exec_ctx(api), plan)

    assert not summary.ok
    assert "group_id" in summary.message
    api.send_group_msg.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_plan_whitelist_send_message_success() -> None:
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    plan = AdminPlan(
        source="rule",
        summary="send message",
        steps=[AdminStep(tool="send_message", args={"chat_type": "group", "chat_id": 123456, "text": "hi"})],
    )

    summary = await admin_exec.execute_plan(_make_exec_ctx(api), plan)

    assert summary.ok
    api.send_group_msg.assert_awaited_once_with(123456, "hi")
    assert "已完成：向群 123456 发送消息" == summary.message


@pytest.mark.asyncio
async def test_execute_plan_stops_after_step_failure() -> None:
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "failed", "retcode": 100, "message": "blocked"}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    plan = AdminPlan(
        source="rule",
        summary="multi",
        steps=[
            AdminStep(tool="send_private_message", args={"user_id": 123456789, "text": "第一步"}),
            AdminStep(tool="send_group_message", args={"group_id": 123456, "text": "第二步"}),
            AdminStep(tool="send_private_message", args={"user_id": 22334455, "text": "第三步"}),
        ],
    )

    summary = await admin_exec.execute_plan(_make_exec_ctx(api), plan)

    assert not summary.ok
    assert summary.completed_steps == 1
    assert "send_group_message" in summary.message
    api.send_private_msg.assert_awaited_once_with(123456789, "第一步")
    api.send_group_msg.assert_awaited_once_with(123456, "第二步")


@pytest.mark.asyncio
async def test_execute_plan_multi_step_runs_in_order() -> None:
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    plan = AdminPlan(
        source="rule",
        summary="multi ok",
        steps=[
            AdminStep(tool="send_message", args={"chat_type": "group", "chat_id": 123456, "text": "第一条"}),
            AdminStep(tool="send_message", args={"chat_type": "group", "chat_id": 123456, "text": "第二条"}),
        ],
    )

    summary = await admin_exec.execute_plan(_make_exec_ctx(api), plan)

    assert summary.ok
    assert api.send_group_msg.await_count == 2
    assert api.send_group_msg.await_args_list[0].args == (123456, "第一条")
    assert api.send_group_msg.await_args_list[1].args == (123456, "第二条")


@pytest.mark.asyncio
async def test_execute_plan_rejects_too_many_steps() -> None:
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    steps = [
        AdminStep(tool="list_directory", args={"path": f"public_{idx}"})
        for idx in range(admin_exec.MAX_ADMIN_PLAN_STEPS + 1)
    ]
    plan = AdminPlan(source="rule", summary="too many", steps=steps)

    summary = await admin_exec.execute_plan(_make_exec_ctx(api), plan)

    assert not summary.ok
    assert "步骤数超过上限" in summary.message
    api.send_group_msg.assert_not_awaited()
    api.send_private_msg.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_plan_generate_then_send_uses_generated_text() -> None:
    aisvc = SimpleNamespace(chat_ready=True, chat_with_context=AsyncMock(return_value="AI拼接回复"))
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    plan = AdminPlan(
        source="rule",
        summary="ai send",
        steps=[
            AdminStep(
                tool="generate_ai_reply",
                args={"chat_type": "group", "chat_id": 123456, "message": "老师说明天检查作业"},
            ),
            AdminStep(
                tool="send_message",
                args={"chat_type": "group", "chat_id": 123456, "text": "{{last_text}}"},
            ),
        ],
    )
    exec_ctx = _make_exec_ctx(api)
    exec_ctx.aisvc = aisvc

    summary = await admin_exec.execute_plan(exec_ctx, plan)

    assert summary.ok
    aisvc.chat_with_context.assert_awaited_once()
    assert aisvc.chat_with_context.await_args.args[0] == "group:123456"
    api.send_group_msg.assert_awaited_once_with(123456, "AI拼接回复")


@pytest.mark.asyncio
async def test_handle_admin_nl_single_step_success_feedback_summary() -> None:
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    ctx = SimpleNamespace(scene="private_friend", user_id=900001)
    feedback = AsyncMock()

    handled = await admin_nl.handle_admin_nl(
        api=api,
        ctx=ctx,
        text="给QQ123456789发：收到",
        logsvc=_DummyLogService(),
        evt={"post_type": "message"},
        reply_func=feedback,
    )

    assert handled is True
    api.send_private_msg.assert_awaited_once_with(123456789, "收到")
    feedback.assert_awaited_once()
    assert feedback.await_args.args[2] == "已完成：向用户 123456789 发送消息"


@pytest.mark.asyncio
async def test_handle_admin_nl_multi_send_requires_confirm(monkeypatch) -> None:
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    ctx = SimpleNamespace(scene="private_friend", user_id=900001)
    feedback = AsyncMock()
    state = SimpleNamespace(pending_admin_nl_confirm={})
    plan = AdminPlan(
        source="rule",
        summary="multi success",
        steps=[
            AdminStep(tool="send_group_message", args={"group_id": 123456, "text": "第一条"}),
            AdminStep(tool="send_group_message", args={"group_id": 123456, "text": "第二条"}),
        ],
    )

    monkeypatch.setattr(admin_nl, "parse_admin_rule_based", lambda _text: plan)

    handled = await admin_nl.handle_admin_nl(
        api=api,
        ctx=ctx,
        text="随便一句普通文本",
        logsvc=_DummyLogService(),
        evt={"post_type": "message"},
        state=state,
        reply_func=feedback,
    )

    assert handled is True
    api.send_group_msg.assert_not_awaited()
    api.send_private_msg.assert_not_awaited()
    assert 900001 in state.pending_admin_nl_confirm
    feedback.assert_awaited_once()
    message = str(feedback.await_args.args[2])
    assert "该计划需要确认" in message
    assert "1. send_group_message" in message
    assert "2. send_group_message" in message


@pytest.mark.asyncio
async def test_execute_plan_find_files_tool_reuses_query_service(tmp_data_dirs: dict) -> None:
    target = tmp_data_dirs["public_dir"] / "query_hit.txt"
    target.write_text("ok", encoding="utf-8")
    filesvc = SimpleNamespace(find=lambda _ctx, _kw, in_dir=None: [target])
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    exec_ctx = _make_exec_ctx(api)
    exec_ctx.filesvc = filesvc
    plan = AdminPlan(
        source="rule",
        summary="find",
        steps=[AdminStep(tool="find_files", args={"keyword": "query"})],
    )

    summary = await admin_exec.execute_plan(exec_ctx, plan)

    assert summary.ok
    assert "搜索结果" in summary.message
    assert "query_hit.txt" in summary.message


@pytest.mark.asyncio
async def test_execute_plan_list_directory_tool_reuses_query_service() -> None:
    filesvc = SimpleNamespace(list_dir=lambda _ctx, arg: (True, f"目录内容：{arg}"))
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    exec_ctx = _make_exec_ctx(api)
    exec_ctx.filesvc = filesvc
    plan = AdminPlan(
        source="rule",
        summary="ls",
        steps=[AdminStep(tool="list_directory", args={"path": "public"})],
    )

    summary = await admin_exec.execute_plan(exec_ctx, plan)

    assert summary.ok
    assert "目录内容：public" in summary.message


@pytest.mark.asyncio
async def test_execute_plan_find_files_missing_keyword_returns_clear_error() -> None:
    filesvc = SimpleNamespace(find=lambda _ctx, _kw, in_dir=None: [])
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    exec_ctx = _make_exec_ctx(api)
    exec_ctx.filesvc = filesvc
    plan = AdminPlan(
        source="rule",
        summary="find",
        steps=[AdminStep(tool="find_files", args={})],
    )

    summary = await admin_exec.execute_plan(exec_ctx, plan)

    assert not summary.ok
    assert "请提供要搜索的关键词" in summary.message


@pytest.mark.asyncio
async def test_execute_plan_list_handin_tasks_tool_success() -> None:
    now = time.time()
    task = SimpleNamespace(
        task_id="123:作业1:1",
        group_id=123,
        creator_id=900001,
        name="作业1",
        deadline_ts=now + 3600,
        closed=False,
        cancelled=False,
        is_active=lambda ts=None: True,
    )
    handin = SimpleNamespace(
        list_tasks_by_group=lambda group_id, include_closed=True: [task],
        list_active_tasks_by_group=lambda group_id: [task],
        is_task_gettable=lambda _task: True,
    )
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    exec_ctx = _make_exec_ctx(api)
    exec_ctx.handin = handin
    plan = AdminPlan(
        source="rule",
        summary="list handin",
        steps=[AdminStep(tool="list_handin_tasks", args={"group_id": 123})],
    )

    summary = await admin_exec.execute_plan(exec_ctx, plan)

    assert summary.ok
    assert "群 123 handin 任务列表" in summary.message
    assert "作业1" in summary.message


@pytest.mark.asyncio
async def test_execute_plan_cancel_handin_task_tool_requires_unique_target() -> None:
    now = time.time()
    t1 = SimpleNamespace(
        task_id="1",
        group_id=123,
        creator_id=900001,
        name="作业1",
        deadline_ts=now + 3600,
        closed=False,
        cancelled=False,
        is_active=lambda ts=None: True,
    )
    t2 = SimpleNamespace(
        task_id="2",
        group_id=123,
        creator_id=900001,
        name="作业1",
        deadline_ts=now + 7200,
        closed=False,
        cancelled=False,
        is_active=lambda ts=None: True,
    )
    handin = SimpleNamespace(
        list_tasks_by_group=lambda group_id, include_closed=True: [t1, t2],
        cancel_task=AsyncMock(),
    )
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    exec_ctx = _make_exec_ctx(api)
    exec_ctx.handin = handin
    plan = AdminPlan(
        source="rule",
        summary="cancel handin",
        steps=[AdminStep(tool="cancel_handin_task", args={"group_id": 123, "task_name": "作业1"})],
    )

    summary = await admin_exec.execute_plan(exec_ctx, plan)

    assert not summary.ok
    assert "多个同名进行中任务" in summary.message


@pytest.mark.asyncio
async def test_execute_plan_cancel_handin_task_tool_success() -> None:
    now = time.time()
    task = SimpleNamespace(
        task_id="123:作业1:1",
        group_id=123,
        creator_id=900001,
        name="作业1",
        deadline_ts=now + 3600,
        closed=False,
        cancelled=False,
        is_active=lambda ts=None: True,
    )
    cancel_mock = Mock(return_value=(True, "已取消任务「作业1」（群 123）。"))
    handin = SimpleNamespace(
        list_tasks_by_group=lambda group_id, include_closed=True: [task],
        cancel_task=cancel_mock,
    )
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    exec_ctx = _make_exec_ctx(api)
    exec_ctx.handin = handin
    plan = AdminPlan(
        source="rule",
        summary="cancel handin",
        steps=[AdminStep(tool="cancel_handin_task", args={"group_id": 123, "task_id": "123:作业1:1"})],
    )

    summary = await admin_exec.execute_plan(exec_ctx, plan)

    assert summary.ok
    assert "已取消任务「作业1」（群 123）。" in summary.message
