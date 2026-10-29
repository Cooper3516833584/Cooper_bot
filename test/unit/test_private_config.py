from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from cooper_bot.core import config


def _load_isolated(tmp_path, *, settings="", secrets="", env=None):
    module = tmp_path / "cooper_bot/core/config.py"
    module.parent.mkdir(parents=True)
    module.write_bytes(Path(config.__file__).read_bytes())
    private = tmp_path / "config/private"
    private.mkdir(parents=True)
    (private / "settings.env").write_text(settings, encoding="utf-8")
    (private / "secrets.env").write_text(secrets, encoding="utf-8")
    child_env = {
        key: value for key, value in os.environ.items()
        if not key.startswith(("AI_", "VISION_", "MAIL_", "CALENDAR_", "MEMORY_PROBE_"))
        and key not in {"TOKEN", "HTTP_TOKEN", "ADMIN_USERS", "TIMEZONE"}
    }
    child_env.update(env or {})
    code = (
        "import json,runpy,sys; c=runpy.run_path(sys.argv[1]); "
        "print(json.dumps({k:str(c[k]) for k in "
        "['AI_CHAT_MODEL','TOKEN','CALENDAR_TEST_GROUP_ID','AI_SYSTEM_PROMPT',"
        "'AI_GROUP_CHAT_PROMPTS_PATH','EDITOR_DUTY_CONFIG_PATH']}))"
    )
    result = subprocess.run(
        [sys.executable, "-B", "-c", code, str(module)], env=child_env,
        capture_output=True, text=True, encoding="utf-8", check=True,
    )
    return json.loads(result.stdout)


def test_env_priority_and_private_paths(tmp_path):
    loaded = _load_isolated(
        tmp_path, settings="AI_CHAT_MODEL=settings-model\nCALENDAR_TEST_GROUP_ID=30303\nTOKEN=settings-token\n",
        secrets="AI_CHAT_MODEL=legacy-model\nTOKEN=secret-token\n",
        env={"AI_CHAT_MODEL": "system-model"},
    )
    assert loaded["AI_CHAT_MODEL"] == "system-model"
    assert loaded["TOKEN"] == "secret-token"
    assert loaded["CALENDAR_TEST_GROUP_ID"] == "30303"
    assert Path(loaded["AI_GROUP_CHAT_PROMPTS_PATH"]) == tmp_path / "config/private/ai/prompts/chat/group_overrides.json"
    assert Path(loaded["EDITOR_DUTY_CONFIG_PATH"]) == tmp_path / "config/private/calendar/editor_duty_config.json"


def test_settings_and_missing_identity_fallback(tmp_path):
    loaded = _load_isolated(tmp_path, settings="AI_CHAT_MODEL=settings-model\n")
    assert loaded["AI_CHAT_MODEL"] == "settings-model"
    assert loaded["CALENDAR_TEST_GROUP_ID"] == "0"
    assert "/help" in loaded["AI_SYSTEM_PROMPT"]


def test_group_permissions_require_valid_explicit_levels(tmp_path):
    path = tmp_path / "permissions.json"
    assert config._load_group_levels(path) == {}
    path.write_text('{"group_levels":{"30303":2}}', encoding="utf-8")
    assert config._load_group_levels(path) == {30303: 2}
    for raw in [{"30303": True}, {"30303": 4}, {"../30303": 2}, []]:
        path.write_text(json.dumps({"group_levels": raw}), encoding="utf-8")
        with pytest.raises(ValueError):
            config._load_group_levels(path)
