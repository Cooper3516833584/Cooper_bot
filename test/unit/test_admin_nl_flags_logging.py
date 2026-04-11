from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import admin_nl


class _DummyLogger:
    def __init__(self) -> None:
        self.infos: list[str] = []
        self.warnings: list[str] = []

    def info(self, msg: str) -> None:
        self.infos.append(str(msg))

    def warning(self, msg: str) -> None:
        self.warnings.append(str(msg))


class _DummyLogService:
    def __init__(self) -> None:
        self.log = _DummyLogger()


@pytest.mark.asyncio
async def test_admin_nl_control_switch_off_skips_all(monkeypatch) -> None:
    monkeypatch.setattr(admin_nl.config, "ADMIN_USERS", {900001})
    monkeypatch.setattr(admin_nl.config, "ENABLE_ADMIN_NL_CONTROL", False)
    ctx = SimpleNamespace(scene="private_friend", user_id=900001)
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )

    handled = await admin_nl.handle_admin_nl(
        api=api,
        ctx=ctx,
        text="在群123456发：今晚八点开会",
        logsvc=_DummyLogService(),
        evt={"post_type": "message"},
    )

    assert handled is False
    api.send_group_msg.assert_not_awaited()
    api.send_private_msg.assert_not_awaited()


@pytest.mark.asyncio
async def test_admin_nl_multi_step_switch_off_does_not_call_model_planner(monkeypatch) -> None:
    monkeypatch.setattr(admin_nl.config, "ADMIN_USERS", {900001})
    monkeypatch.setattr(admin_nl.config, "ENABLE_ADMIN_NL_CONTROL", True)
    monkeypatch.setattr(admin_nl.config, "ENABLE_ADMIN_NL_MULTI_STEP", False)
    ctx = SimpleNamespace(scene="private_friend", user_id=900001)
    logsvc = _DummyLogService()
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    aisvc = SimpleNamespace(parse_admin_plan=AsyncMock(return_value={"summary": "x", "steps": []}))

    handled = await admin_nl.handle_admin_nl(
        api=api,
        ctx=ctx,
        text="请处理这个管理员任务",
        logsvc=logsvc,
        evt={"post_type": "message"},
        aisvc=aisvc,
    )

    assert handled is False
    aisvc.parse_admin_plan.assert_not_awaited()
    assert any("stage=model_planner_disabled" in one for one in logsvc.log.infos)


@pytest.mark.asyncio
async def test_admin_nl_single_step_still_works_when_multi_step_off(monkeypatch) -> None:
    monkeypatch.setattr(admin_nl.config, "ADMIN_USERS", {900001})
    monkeypatch.setattr(admin_nl.config, "ENABLE_ADMIN_NL_CONTROL", True)
    monkeypatch.setattr(admin_nl.config, "ENABLE_ADMIN_NL_MULTI_STEP", False)
    ctx = SimpleNamespace(scene="private_friend", user_id=900001)
    logsvc = _DummyLogService()
    api = SimpleNamespace(
        send_group_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
        send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}),
    )
    feedback = AsyncMock()

    handled = await admin_nl.handle_admin_nl(
        api=api,
        ctx=ctx,
        text="给QQ123456789发：收到",
        logsvc=logsvc,
        evt={"post_type": "message"},
        reply_func=feedback,
    )

    assert handled is True
    api.send_private_msg.assert_awaited_once_with(123456789, "收到")
    feedback.assert_awaited_once()
    assert any("stage=plan_rule_hit" in one for one in logsvc.log.infos)
    assert any("tools=send_private_message" in one for one in logsvc.log.infos)
    assert any("stage=executed" in one for one in logsvc.log.infos)
