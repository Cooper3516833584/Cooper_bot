from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import admin_targets
import admin_nl
from admin_models import AdminPlan, AdminStep


def _ctx(*, user_id: int, scene: str) -> SimpleNamespace:
    return SimpleNamespace(user_id=int(user_id), scene=str(scene))


def _write_admin_targets(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_should_handle_admin_nl_admin_private_plain_text(monkeypatch) -> None:
    monkeypatch.setattr(admin_nl.config, "ADMIN_USERS", {900001})
    ctx = _ctx(user_id=900001, scene="private_friend")
    assert admin_nl.should_handle_admin_nl(ctx, "帮我发个通知")


@pytest.mark.parametrize(
    "user_id,scene,text",
    [
        (10001, "private_friend", "帮我发个通知"),
        (900001, "group", "帮我发个通知"),
        (900001, "private_friend", "/ping"),
        (900001, "private_friend", "C你好"),
        (900001, "private_friend", ""),
    ],
)
def test_should_handle_admin_nl_rejects_non_target_input(
    monkeypatch,
    user_id: int,
    scene: str,
    text: str,
) -> None:
    monkeypatch.setattr(admin_nl.config, "ADMIN_USERS", {900001})
    ctx = _ctx(user_id=user_id, scene=scene)
    assert not admin_nl.should_handle_admin_nl(ctx, text)


def test_parse_rule_send_group_message() -> None:
    plan = admin_nl.parse_admin_rule_based("在群123456发：今晚八点开会")
    assert isinstance(plan, AdminPlan)
    assert plan.source == "rule"
    assert plan.summary == "向群发送消息"
    assert len(plan.steps) == 1
    assert plan.steps[0].tool == "send_group_message"
    assert plan.steps[0].args["group_id"] == 123456
    assert plan.steps[0].args["text"] == "今晚八点开会"


def test_parse_rule_send_private_message() -> None:
    plan = admin_nl.parse_admin_rule_based("给QQ123456789发：收到")
    assert isinstance(plan, AdminPlan)
    assert plan.source == "rule"
    assert plan.summary == "向私聊发送消息"
    assert len(plan.steps) == 1
    assert plan.steps[0].tool == "send_private_message"
    assert plan.steps[0].args["user_id"] == 123456789
    assert plan.steps[0].args["text"] == "收到"


def test_parse_rule_send_group_message_with_alias(tmp_data_dirs: dict) -> None:
    cfg = Path(tmp_data_dirs["data_dir"]) / "admin_targets.json"
    _write_admin_targets(cfg, {"groups": {"高数群": 123456}, "users": {}})
    admin_targets.clear_target_resolver_cache()

    plan = admin_nl.parse_admin_rule_based("在高数群发：今晚交作业")

    assert isinstance(plan, AdminPlan)
    assert plan.steps[0].tool == "send_group_message"
    assert plan.steps[0].args["group_id"] == 123456
    assert plan.steps[0].args["text"] == "今晚交作业"


def test_parse_rule_send_private_message_with_alias(tmp_data_dirs: dict) -> None:
    cfg = Path(tmp_data_dirs["data_dir"]) / "admin_targets.json"
    _write_admin_targets(cfg, {"groups": {}, "users": {"班长": 234567890}})
    admin_targets.clear_target_resolver_cache()

    plan = admin_nl.parse_admin_rule_based("给班长发：收到")

    assert isinstance(plan, AdminPlan)
    assert plan.steps[0].tool == "send_private_message"
    assert plan.steps[0].args["user_id"] == 234567890
    assert plan.steps[0].args["text"] == "收到"


def test_parse_rule_ai_proxy_plan() -> None:
    plan = admin_nl.parse_admin_rule_based("在群123456对“老师说明天检查作业”生成AI回复并发出去")
    assert isinstance(plan, AdminPlan)
    assert plan.source == "rule"
    assert len(plan.steps) == 2
    assert plan.steps[0].tool == "generate_ai_reply"
    assert plan.steps[0].args["chat_type"] == "group"
    assert plan.steps[0].args["chat_id"] == 123456
    assert plan.steps[1].tool == "send_message"
    assert plan.steps[1].args["chat_type"] == "group"
    assert plan.steps[1].args["group_id"] == 123456
    assert plan.steps[1].args["text"] == "{{last_text}}"


def test_parse_rule_handin_create_plan() -> None:
    plan = admin_nl.parse_admin_rule_based("在群123456创建提交任务，任务名实验一，4.12 23:59截止，18:00提醒")
    assert isinstance(plan, AdminPlan)
    assert plan.source == "rule"
    assert len(plan.steps) == 1
    assert plan.steps[0].tool == "create_handin_task"
    assert plan.steps[0].args["group_id"] == 123456
    assert plan.steps[0].args["task_name"] == "实验一"
    assert plan.steps[0].args["deadline_text"] == "4.12 23:59"
    assert plan.steps[0].args["reminders"] == ["18:00"]


def test_parse_rule_list_handin_tasks_plan() -> None:
    plan = admin_nl.parse_admin_rule_based("列一下群123456的handin任务")
    assert isinstance(plan, AdminPlan)
    assert plan.source == "rule"
    assert plan.summary == "查看 handin 任务"
    assert len(plan.steps) == 1
    assert plan.steps[0].tool == "list_handin_tasks"
    assert plan.steps[0].args["group_id"] == 123456
    assert plan.steps[0].args["active_only"] is True


def test_parse_rule_list_handin_tasks_plan_with_alias(tmp_data_dirs: dict) -> None:
    cfg = Path(tmp_data_dirs["data_dir"]) / "admin_targets.json"
    _write_admin_targets(cfg, {"groups": {"电路实验群": 556677}, "users": {}})
    admin_targets.clear_target_resolver_cache()

    plan = admin_nl.parse_admin_rule_based("列一下电路实验群的handin任务")

    assert isinstance(plan, AdminPlan)
    assert plan.steps[0].tool == "list_handin_tasks"
    assert plan.steps[0].args["group_id"] == 556677
    assert plan.steps[0].args["active_only"] is True


def test_parse_rule_cancel_handin_task_plan_requires_confirm() -> None:
    plan = admin_nl.parse_admin_rule_based("取消群123456的作业1 handin")
    assert isinstance(plan, AdminPlan)
    assert plan.source == "rule"
    assert plan.summary == "取消 handin 任务"
    assert plan.need_confirm is True
    assert len(plan.steps) == 1
    assert plan.steps[0].tool == "cancel_handin_task"
    assert plan.steps[0].args["group_id"] == 123456
    assert plan.steps[0].args["task_name"] == "作业1"


def test_parse_rule_find_files_plan() -> None:
    plan = admin_nl.parse_admin_rule_based("帮我找高数资料")
    assert isinstance(plan, AdminPlan)
    assert plan.source == "rule"
    assert plan.summary == "查询文件"
    assert len(plan.steps) == 1
    assert plan.steps[0].tool == "find_files"
    assert plan.steps[0].args["keyword"] == "高数资料"


def test_parse_rule_list_directory_plan() -> None:
    plan = admin_nl.parse_admin_rule_based("列一下 public/textbook_and_material 目录")
    assert isinstance(plan, AdminPlan)
    assert plan.source == "rule"
    assert plan.summary == "查看目录"
    assert len(plan.steps) == 1
    assert plan.steps[0].tool == "list_directory"
    assert plan.steps[0].args["path"] == "public/textbook_and_material"


def test_missing_param_hint_for_ambiguous_query_text() -> None:
    assert "缺少目录路径" in admin_nl._build_missing_param_hint("列一下某个目录")
    assert "缺少搜索关键词" in admin_nl._build_missing_param_hint("帮我找")
    assert "缺少群号" in admin_nl._build_missing_param_hint("列一下handin任务")
    assert "缺少任务标识" in admin_nl._build_missing_param_hint("取消群123456的handin")


@pytest.mark.parametrize(
    "text",
    [
        "随便聊聊天",
        "在群123456发一下",
        "创建任务吧",
        "给123456789这个私聊对收到请回复生成AI回复并发送",
    ],
)
def test_parse_rule_invalid_or_ambiguous_text_returns_none(text: str) -> None:
    assert admin_nl.parse_admin_rule_based(text) is None


def test_validate_model_plan_supports_confidence_and_step_refs() -> None:
    raw = {
        "summary": "multi step",
        "need_confirm": False,
        "confidence": 0.82,
        "steps": [
            {"tool": "find_files", "args": {"keyword": "高数"}},
            {"tool": "send_private_message", "args": {"user_id": 123456789, "text_from_step": 1}},
        ],
    }

    plan, err = admin_nl._validate_model_plan(raw)

    assert err == ""
    assert isinstance(plan, AdminPlan)
    assert plan.confidence == pytest.approx(0.82, abs=1e-6)
    assert plan.steps[1].args["text"] == "{{step_1_text}}"


def test_validate_model_plan_rejects_too_many_steps() -> None:
    raw = {
        "summary": "too many",
        "need_confirm": False,
        "steps": [
            {"tool": "find_files", "args": {"keyword": f"k{i}"}}
            for i in range(int(admin_nl.MAX_ADMIN_PLAN_STEPS) + 1)
        ],
    }

    plan, err = admin_nl._validate_model_plan(raw)

    assert plan is None
    assert "steps 超出上限" in err


def test_is_high_risk_plan_when_has_multiple_send_actions() -> None:
    plan = AdminPlan(
        source="model",
        summary="multi send",
        steps=[
            AdminStep(tool="send_group_message", args={"group_id": 123456, "text": "第一条"}),
            AdminStep(tool="send_group_message", args={"group_id": 123456, "text": "第二条"}),
        ],
    )

    assert admin_nl._is_high_risk_plan(plan) is True

def test_is_high_risk_plan_when_has_cancel_handin_task() -> None:
    plan = AdminPlan(
        source="model",
        summary="cancel handin",
        steps=[AdminStep(tool="cancel_handin_task", args={"group_id": 123456, "task_name": "作业1"})],
    )
    assert admin_nl._is_high_risk_plan(plan) is True


@pytest.mark.parametrize("tool_name", ["set_level", "cancel_handin"])
def test_is_high_risk_plan_does_not_use_unregistered_legacy_tools(tool_name: str) -> None:
    plan = AdminPlan(
        source="model",
        summary="legacy tool",
        steps=[AdminStep(tool=tool_name, args={"x": 1})],
    )
    assert admin_nl._is_high_risk_plan(plan) is False


def test_high_risk_tool_names_are_subset_of_registered_tools() -> None:
    assert set(admin_nl._HIGH_RISK_TOOL_NAMES).issubset(set(admin_nl.TOOLS.keys()))
