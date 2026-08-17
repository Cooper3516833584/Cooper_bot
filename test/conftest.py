from __future__ import annotations

import importlib
import shutil
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

from test.helpers.fake_ai import FakeAIClient
from test.helpers.fake_events import make_admin_event, make_group_event, make_private_event
from test.helpers.sample_builders import build_find_tree, build_handin_env, build_material_tree
from test.helpers.time_tools import freeze_time, patch_project_time


def _patch_module_attr(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    attr: str,
    value: Any,
    *,
    import_if_missing: bool = False,
) -> None:
    module = sys.modules.get(module_name)
    if module is None and import_if_missing:
        try:
            module = importlib.import_module(module_name)
        except Exception:
            return
    if module is None:
        return
    monkeypatch.setattr(module, attr, value, raising=False)


def _apply_path_patches(monkeypatch: pytest.MonkeyPatch, cfg: dict[str, Any]) -> None:
    config_map = {
        "BASE_DIR": cfg["project_root"],
        "DATA_DIR": cfg["data_dir"],
        "FIND_INDEX_PATH": cfg["find_index_path"],
        "AI_SEMANTIC_STORE_PATH": cfg["ai_semantic_store_path"],
        "AI_MATERIAL_SCAN_MARKS_PATH": cfg["ai_material_scan_marks_path"],
        "AI_MATERIAL_STATE_CACHE_PATH": cfg["ai_material_state_cache_path"],
        "LOG_DIR": cfg["log_dir"],
        "DOC_ROOTS": cfg["doc_roots"],
        "GROUP_DOCS_DIR": cfg["groups_dir"],
        "USER_DOCS_DIR": cfg["users_dir"],
        "PERM_DB_PATH": cfg["perm_db_path"],
        "HANDIN_DB_PATH": cfg["handin_db_path"],
        "HANDIN_INBOX_DIR": cfg["handin_inbox_dir"],
        "HANDIN_ROOT_DIR": cfg["handin_root_dir"],
        "ROSTER_XLSX_PATH": cfg["roster_xlsx_path"],
        "UPLOAD_GROUP_HOST_DIR": cfg["upload_group_dir"],
        "UPLOAD_PRIVATE_HOST_DIR": cfg["upload_private_dir"],
        "AI_API_KEY_PATH": cfg["ai_api_key_path"],
        "AI_MATERIAL_DIR": cfg["ai_material_dir"],
        "AI_INDEX_PATH": cfg["ai_index_path"],
        "AI_METADATA_PATH": cfg["ai_metadata_path"],
        "AI_VECTORS_PATH": cfg["ai_vectors_path"],
        "ADMIN_USERS": cfg["admin_users"],
        "GROUP_LEVEL": {},
        "DEFAULT_LEVEL": 0,
    }
    for attr, value in config_map.items():
        _patch_module_attr(monkeypatch, "cooper_bot.core.config", attr, value, import_if_missing=True)

    module_patch_map: dict[str, dict[str, Any]] = {
        "cooper_bot.modules.files.filesvc": {
            "DOC_ROOTS": cfg["doc_roots"],
            "GROUP_DOCS_DIR": cfg["groups_dir"],
            "USER_DOCS_DIR": cfg["users_dir"],
            "HANDIN_ROOT_DIR": cfg["handin_root_dir"],
            "UPLOAD_GROUP_HOST_DIR": cfg["upload_group_dir"],
            "UPLOAD_PRIVATE_HOST_DIR": cfg["upload_private_dir"],
            "DATA_DIR": cfg["data_dir"],
            "FIND_INDEX_PATH": cfg["find_index_path"],
        },
        "cooper_bot.modules.handin.handinsvc": {
            "DATA_DIR": cfg["data_dir"],
            "GROUP_DOCS_DIR": cfg["groups_dir"],
            "USER_DOCS_DIR": cfg["users_dir"],
            "HANDIN_DB_PATH": cfg["handin_db_path"],
            "HANDIN_INBOX_DIR": cfg["handin_inbox_dir"],
            "HANDIN_ROOT_DIR": cfg["handin_root_dir"],
            "ROSTER_XLSX_PATH": cfg["roster_xlsx_path"],
        },
        "cooper_bot.commands.commands": {
            "DATA_DIR": cfg["data_dir"],
            "UPLOAD_GROUP_HOST_DIR": cfg["upload_group_dir"],
            "UPLOAD_PRIVATE_HOST_DIR": cfg["upload_private_dir"],
            "ADMIN_USERS": cfg["admin_users"],
        },
        "cooper_bot.core.router": {
            "ADMIN_USERS": cfg["admin_users"],
            "GROUP_LEVEL": {},
            "DEFAULT_LEVEL": 0,
        },
        "cooper_bot.modules.ai.aisvc": {
            "BASE_DIR": cfg["project_root"],
            "AI_API_KEY_PATH": cfg["ai_api_key_path"],
            "AI_MATERIAL_DIR": cfg["ai_material_dir"],
            "AI_INDEX_PATH": cfg["ai_index_path"],
            "AI_METADATA_PATH": cfg["ai_metadata_path"],
            "AI_VECTORS_PATH": cfg["ai_vectors_path"],
            "AI_SEMANTIC_STORE_PATH": cfg["ai_semantic_store_path"],
            "AI_MATERIAL_SCAN_MARKS_PATH": cfg["ai_material_scan_marks_path"],
            "AI_MATERIAL_STATE_CACHE_PATH": cfg["ai_material_state_cache_path"],
        },
    }
    for module_name, attr_map in module_patch_map.items():
        for attr, value in attr_map.items():
            _patch_module_attr(monkeypatch, module_name, attr, value)


@pytest.fixture
def tmp_project_root() -> Path:
    base = Path(__file__).resolve().parent / ".tmp_workspaces"
    base.mkdir(parents=True, exist_ok=True)
    root = base / f"case_{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def tmp_data_dirs(tmp_project_root: Path) -> dict[str, Path]:
    data_dir = tmp_project_root / "storage" / "documents"
    log_dir = tmp_project_root / "runtime" / "logs"
    public_dir = data_dir / "public"
    friend_dir = data_dir / "friend"
    admin_dir = data_dir / "admin"
    groups_dir = data_dir / "groups"
    users_dir = data_dir / "users"
    handin_root_dir = tmp_project_root / "storage" / "submissions" / "handin"
    handin_inbox_dir = tmp_project_root / "runtime" / "staging" / "handin"
    upload_group_dir = tmp_project_root / "runtime" / "staging" / "group"
    upload_private_dir = tmp_project_root / "runtime" / "staging" / "private"
    ai_material_dir = public_dir / "textbook_and_material"

    all_dirs = (
        data_dir,
        log_dir,
        public_dir,
        friend_dir,
        admin_dir,
        groups_dir,
        users_dir,
        handin_root_dir,
        handin_inbox_dir,
        upload_group_dir,
        upload_private_dir,
        ai_material_dir,
    )
    for path in all_dirs:
        path.mkdir(parents=True, exist_ok=True)

    return {
        "data_dir": data_dir,
        "log_dir": log_dir,
        "public_dir": public_dir,
        "friend_dir": friend_dir,
        "admin_dir": admin_dir,
        "groups_dir": groups_dir,
        "users_dir": users_dir,
        "handin_root_dir": handin_root_dir,
        "handin_inbox_dir": handin_inbox_dir,
        "upload_group_dir": upload_group_dir,
        "upload_private_dir": upload_private_dir,
        "ai_material_dir": ai_material_dir,
        "find_index_path": tmp_project_root / "runtime" / "databases" / "file_search" / "find_index.json",
    }


@pytest.fixture
def test_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_project_root: Path,
    tmp_data_dirs: dict[str, Path],
) -> dict[str, Any]:
    admin_user_id = 900001
    default_user_id = 10001
    default_group_id = 20001

    cfg: dict[str, Any] = {
        "project_root": tmp_project_root,
        "data_dir": tmp_data_dirs["data_dir"],
        "log_dir": tmp_data_dirs["log_dir"],
        "public_dir": tmp_data_dirs["public_dir"],
        "friend_dir": tmp_data_dirs["friend_dir"],
        "admin_dir": tmp_data_dirs["admin_dir"],
        "groups_dir": tmp_data_dirs["groups_dir"],
        "users_dir": tmp_data_dirs["users_dir"],
        "handin_root_dir": tmp_data_dirs["handin_root_dir"],
        "handin_inbox_dir": tmp_data_dirs["handin_inbox_dir"],
        "upload_group_dir": tmp_data_dirs["upload_group_dir"],
        "upload_private_dir": tmp_data_dirs["upload_private_dir"],
        "ai_material_dir": tmp_data_dirs["ai_material_dir"],
        "find_index_path": tmp_data_dirs["find_index_path"],
        "ai_semantic_store_path": tmp_project_root / "runtime" / "databases" / "ai" / "semantic_store.sqlite3",
        "ai_material_scan_marks_path": tmp_project_root / "runtime" / "state" / "ai" / "material_scan_marks.json",
        "ai_material_state_cache_path": tmp_project_root / "runtime" / "state" / "ai" / "material_state_cache.json",
        "ai_index_path": tmp_data_dirs["ai_material_dir"] / "all_files_index.json",
        "ai_metadata_path": tmp_data_dirs["ai_material_dir"] / "file_metadata.json",
        "ai_vectors_path": tmp_data_dirs["ai_material_dir"] / "file_vectors.npy",
        "ai_api_key_path": tmp_project_root / "api_key.txt",
        "perm_db_path": tmp_project_root / "runtime" / "databases" / "permissions" / "levels.json",
        "handin_db_path": tmp_project_root / "runtime" / "databases" / "handin" / "tasks.json",
        "roster_xlsx_path": tmp_data_dirs["friend_dir"] / "class_roster.xlsx",
        "admin_users": {admin_user_id},
        "admin_user_id": admin_user_id,
        "default_user_id": default_user_id,
        "default_group_id": default_group_id,
    }
    cfg["doc_roots"] = [
        ("public", cfg["public_dir"], 1),
        ("friend", cfg["friend_dir"], 2),
        ("admin", cfg["admin_dir"], 3),
    ]

    cfg["ai_api_key_path"].write_text("sk-test-only\n", encoding="utf-8")
    for path in (
        cfg["find_index_path"],
        cfg["ai_semantic_store_path"],
        cfg["ai_material_scan_marks_path"],
        cfg["ai_material_state_cache_path"],
        cfg["perm_db_path"],
        cfg["handin_db_path"],
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
    _apply_path_patches(monkeypatch, cfg)
    return cfg


@pytest.fixture(autouse=True)
def _auto_use_test_config(test_config: dict[str, Any]) -> None:
    _ = test_config


@pytest.fixture(autouse=True)
def _block_external_services(monkeypatch: pytest.MonkeyPatch) -> None:
    def _blocked(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("External network/service calls are disabled in tests.")

    try:
        websockets = importlib.import_module("websockets")
        monkeypatch.setattr(websockets, "connect", _blocked, raising=False)
    except Exception:
        pass

    for module_name in ("cooper_bot.modules.ai.aisvc", "cooper_bot.modules.handin.handinsvc"):
        module = sys.modules.get(module_name)
        if module is None:
            continue

        urllib_mod = getattr(module, "urllib", None)
        request_mod = getattr(urllib_mod, "request", None)
        if request_mod is not None:
            monkeypatch.setattr(request_mod, "urlopen", _blocked, raising=False)

    _patch_module_attr(monkeypatch, "cooper_bot.modules.ai.aisvc", "OpenAI", None)
    _patch_module_attr(monkeypatch, "cooper_bot.modules.ai.aisvc", "RapidOCR", None)


@pytest.fixture
def fake_ai_client() -> FakeAIClient:
    return FakeAIClient()


@pytest.fixture
def fake_group_event(test_config: dict[str, Any]) -> dict[str, Any]:
    return make_group_event(
        text="/ping",
        user_id=int(test_config["default_user_id"]),
        group_id=int(test_config["default_group_id"]),
    )


@pytest.fixture
def fake_private_event(test_config: dict[str, Any]) -> dict[str, Any]:
    return make_private_event(
        text="/ping",
        user_id=int(test_config["default_user_id"]),
        sub_type="friend",
    )


@pytest.fixture
def fake_admin_event(test_config: dict[str, Any]) -> dict[str, Any]:
    return make_admin_event(
        text="/admin ping",
        user_id=int(test_config["admin_user_id"]),
        group_id=int(test_config["default_group_id"]),
    )


@pytest.fixture
def sample_material_tree(tmp_data_dirs: dict[str, Path]) -> dict[str, Any]:
    return build_material_tree(tmp_data_dirs["data_dir"])


@pytest.fixture
def sample_find_tree(tmp_data_dirs: dict[str, Path]) -> dict[str, Any]:
    return build_find_tree(tmp_data_dirs["data_dir"])


@pytest.fixture
def sample_handin_env(test_config: dict[str, Any]) -> dict[str, Any]:
    return build_handin_env(
        test_config["data_dir"],
        group_id=int(test_config["default_group_id"]),
        creator_id=int(test_config["admin_user_id"]),
        submitter_ids=(int(test_config["default_user_id"]),),
    )


@pytest.fixture
def controlled_time(monkeypatch: pytest.MonkeyPatch):
    controller = freeze_time()
    patch_project_time(monkeypatch, controller)
    return controller
