from __future__ import annotations

import json
import os

import pytest

from cooper_bot.modules.ai.feishu_secrets import FeishuCredentialError, load_feishu_credentials


def _write_credentials(path, app_id="cli_TEST1234567890", app_secret="TEST_SECRET_SENTINEL"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"app_id": app_id, "app_secret": app_secret}), encoding="utf-8")
    if os.name != "nt":
        path.chmod(0o600)


def test_loads_test_credentials_without_changing_environment(tmp_path, monkeypatch):
    path = tmp_path / "credentials.json"
    _write_credentials(path)
    monkeypatch.delenv("APP_ID", raising=False)
    monkeypatch.delenv("APP_SECRET", raising=False)

    assert load_feishu_credentials(path) == {
        "APP_ID": "cli_TEST1234567890",
        "APP_SECRET": "TEST_SECRET_SENTINEL",
    }
    assert "APP_ID" not in os.environ
    assert "APP_SECRET" not in os.environ


@pytest.mark.parametrize("contents,code", [
    ("{invalid", "feishu_credentials_invalid"),
    (json.dumps({"app_id": "cli_REPLACE_WITH_YOUR_APP_ID", "app_secret": "TEST"}), "feishu_credentials_invalid"),
    (json.dumps({"app_id": "cli_TEST1234567890", "app_secret": "REPLACE_WITH_YOUR_APP_SECRET"}), "feishu_credentials_placeholder"),
    (json.dumps({"app_id": "cli_TEST1234567890", "app_secret": "TEST", "extra": "x"}), "feishu_credentials_invalid"),
])
def test_rejects_invalid_or_placeholder_credentials_without_echo(tmp_path, contents, code):
    path = tmp_path / "credentials.json"
    path.write_text(contents, encoding="utf-8")
    if os.name != "nt":
        path.chmod(0o600)

    with pytest.raises(FeishuCredentialError) as exc_info:
        load_feishu_credentials(path)
    assert exc_info.value.code == code
    assert "TEST" not in str(exc_info.value)


def test_rejects_missing_credentials_without_path_or_contents(tmp_path):
    path = tmp_path / "absent.json"

    with pytest.raises(FeishuCredentialError) as exc_info:
        load_feishu_credentials(path)

    assert exc_info.value.code == "feishu_credentials_missing"
    assert str(path) not in str(exc_info.value)


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode-bit check")
def test_rejects_world_readable_credentials(tmp_path):
    path = tmp_path / "credentials.json"
    _write_credentials(path)
    path.chmod(0o644)

    with pytest.raises(FeishuCredentialError) as exc_info:
        load_feishu_credentials(path)

    assert exc_info.value.code == "feishu_credentials_permissions_unsafe"
