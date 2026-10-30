"""Read local Feishu app credentials without exposing them to bot-wide config."""
from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path


class FeishuCredentialError(RuntimeError):
    """Safe, non-sensitive credential configuration error."""

    def __init__(self, code: str) -> None:
        self.code = str(code)
        super().__init__(self.code)


def load_feishu_credentials(path: Path) -> dict[str, str]:
    """Load a private JSON credential file; never include file contents in errors."""
    try:
        if path.is_symlink():
            raise FeishuCredentialError("feishu_credentials_invalid")
        info = path.stat()
        if not stat.S_ISREG(info.st_mode):
            raise FeishuCredentialError("feishu_credentials_invalid")
        if os.name != "nt" and stat.S_IMODE(info.st_mode) & 0o077:
            raise FeishuCredentialError("feishu_credentials_permissions_unsafe")
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FeishuCredentialError:
        raise
    except FileNotFoundError:
        raise FeishuCredentialError("feishu_credentials_missing") from None
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise FeishuCredentialError("feishu_credentials_invalid") from None
    if not isinstance(raw, dict) or set(raw) != {"app_id", "app_secret"}:
        raise FeishuCredentialError("feishu_credentials_invalid")
    app_id = raw.get("app_id")
    app_secret = raw.get("app_secret")
    if not isinstance(app_id, str) or not re.fullmatch(r"cli_[A-Za-z0-9]+", app_id):
        raise FeishuCredentialError("feishu_credentials_invalid")
    if not isinstance(app_secret, str) or not app_secret or any(ch in app_secret for ch in "\r\n\x00"):
        raise FeishuCredentialError("feishu_credentials_invalid")
    if "REPLACE_WITH" in app_secret or "REPLACE_WITH" in app_id:
        raise FeishuCredentialError("feishu_credentials_placeholder")
    return {"APP_ID": app_id, "APP_SECRET": app_secret}
