from pathlib import Path

import pytest

from cooper_bot.core import config
from cooper_bot.modules.ai import prompt_store
from cooper_bot.modules.ai.prompt_store import PromptConfigError, PromptStore


def _write(root, relative, text):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_all_example_templates_satisfy_their_runtime_contracts(prompt_examples_root):
    store = PromptStore(prompt_examples_root)
    for name in prompt_store.SPECS:
        assert store.get(name).strip()


def test_render_preserves_json_braces_and_does_not_reinterpret_user_data(tmp_path, monkeypatch):
    _write(tmp_path, "web/compose_input.txt", '{"data":{"nested":true}}\n{{question}}\n{{material}}')
    monkeypatch.setattr(config, "AI_PROMPTS_DIR", tmp_path)
    result = prompt_store.render_prompt("web.compose_input", question="{{material}}", material="quoted {data}")
    assert result == '{"data":{"nested":true}}\n{{material}}\nquoted {data}'
    with pytest.raises(PromptConfigError):
        prompt_store.render_prompt("web.compose_input", question="missing material")
    with pytest.raises(PromptConfigError):
        prompt_store.render_prompt("web.compose_input", question="q", material="m", extra="x")


@pytest.mark.parametrize("text", ["", "  ", "{{question}}", "{{question}} {{material}} {{typo}}", "{{question}} {{material"])
def test_empty_missing_and_misspelled_variables_are_rejected(tmp_path, text):
    _write(tmp_path, "web/compose_input.txt", text)
    with pytest.raises(PromptConfigError):
        PromptStore(tmp_path).get("web.compose_input")


def test_protocol_requirements_are_not_optional(tmp_path):
    _write(tmp_path, "memory/extract.txt", "Return plain text with no structured protocol.")
    with pytest.raises(PromptConfigError):
        PromptStore(tmp_path).get("memory.extract")


def test_snapshot_stays_stable_until_new_store_is_created(tmp_path):
    path = _write(tmp_path, "web/judge.txt", "First [WEB_SEARCH]")
    store = PromptStore(tmp_path)
    path.write_text("Second [WEB_SEARCH]", encoding="utf-8")
    assert store.get("web.judge") == "First [WEB_SEARCH]"
    assert PromptStore(tmp_path).get("web.judge") == "Second [WEB_SEARCH]"


def test_examples_and_backups_are_never_runtime_fallbacks(tmp_path):
    root = tmp_path / "private/ai/prompts"
    _write(tmp_path, "examples/ai/prompts/web/judge.txt.example", "Example [WEB_SEARCH]")
    _write(root, "migration_backup/web/judge.txt", "Backup [WEB_SEARCH]")
    with pytest.raises(PromptConfigError):
        PromptStore(root).get("web.judge")


def test_known_template_cannot_escape_prompt_root(tmp_path, monkeypatch):
    private_text = "private-value-must-not-appear-in-errors"
    _write(tmp_path, "secret.txt", private_text)
    monkeypatch.setitem(prompt_store.SPECS, "web.judge", ("../secret.txt", (), ()))
    with pytest.raises(PromptConfigError) as caught:
        PromptStore(tmp_path / "prompts").get("web.judge")
    assert private_text not in str(caught.value)
    assert str(tmp_path) not in str(caught.value)
    with pytest.raises(PromptConfigError):
        PromptStore(tmp_path).get("../secret.txt")


def test_bad_utf8_and_bad_notice_json_are_rejected(tmp_path):
    path = _write(tmp_path, "web/judge.txt", "placeholder")
    path.write_bytes(b"\xff\xfe")
    _write(tmp_path, "notice/defaults.json", '{"default":[]}')
    for name in ("web.judge", "notice.defaults"):
        with pytest.raises(PromptConfigError):
            PromptStore(tmp_path).get(name)


def test_notice_override_substitutes_once_and_rejects_unknown_variables():
    assert prompt_store.render_text("{{source}}:{{content}}", source="{{content}}", content="text", silent_token="silent") == "{{content}}:text"
    with pytest.raises(PromptConfigError):
        prompt_store.render_text("{{unrecognized}}", source="source", content="text", silent_token="silent")


@pytest.mark.asyncio
async def test_missing_calendar_templates_preserve_local_facts_without_model_calls(tmp_path, monkeypatch):
    from datetime import date
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from cooper_bot.modules.calendar.daily_calendar import DailyCalendarService

    monkeypatch.setattr(config, "AI_PROMPTS_DIR", tmp_path / "empty_prompts")
    ai = SimpleNamespace(deepseek_task_ready=True, deepseek_task_text=AsyncMock(), calendar_web_query=AsyncMock())
    service = DailyCalendarService(SimpleNamespace(), ai, config_path=tmp_path / "calendar.json", data_dir=tmp_path / "state")
    result = await service.generate_for_date(date(2026, 7, 1), force_refresh=True)
    assert result.special is True
    assert result.message
    assert result.web_used is False
    ai.deepseek_task_text.assert_not_awaited()
    ai.calendar_web_query.assert_not_awaited()
