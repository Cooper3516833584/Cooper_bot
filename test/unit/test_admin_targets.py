from __future__ import annotations

import json
from pathlib import Path

import admin_targets


def _write_targets(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_resolve_target_explicit_id_still_works_without_alias_file(monkeypatch, tmp_data_dirs: dict) -> None:
    monkeypatch.setattr(admin_targets.config, "DATA_DIR", tmp_data_dirs["data_dir"])
    cfg = Path(tmp_data_dirs["data_dir"]) / "admin_targets.json"
    if cfg.exists():
        cfg.unlink()
    admin_targets.clear_target_resolver_cache()

    g = admin_targets.resolve_group_target("群123456")
    u = admin_targets.resolve_user_target("QQ123456789")

    assert g.ok is True
    assert g.target_id == 123456
    assert u.ok is True
    assert u.target_id == 123456789


def test_resolve_target_alias_hit(monkeypatch, tmp_data_dirs: dict) -> None:
    monkeypatch.setattr(admin_targets.config, "DATA_DIR", tmp_data_dirs["data_dir"])
    cfg = Path(tmp_data_dirs["data_dir"]) / "admin_targets.json"
    _write_targets(
        cfg,
        {
            "groups": {"高数群": 123456},
            "users": {"班长": 234567890},
        },
    )
    admin_targets.clear_target_resolver_cache()

    g1 = admin_targets.resolve_group_target("高数群")
    g2 = admin_targets.resolve_group_target("高数")
    u = admin_targets.resolve_user_target("班长")

    assert g1.ok is True and g1.target_id == 123456
    assert g2.ok is True and g2.target_id == 123456
    assert u.ok is True and u.target_id == 234567890


def test_resolve_target_alias_not_found(monkeypatch, tmp_data_dirs: dict) -> None:
    monkeypatch.setattr(admin_targets.config, "DATA_DIR", tmp_data_dirs["data_dir"])
    cfg = Path(tmp_data_dirs["data_dir"]) / "admin_targets.json"
    _write_targets(cfg, {"groups": {"高数群": 123456}, "users": {}})
    admin_targets.clear_target_resolver_cache()

    rr = admin_targets.resolve_group_target("未知群")
    assert rr.ok is False
    assert rr.status == "not_found"


def test_resolve_target_alias_ambiguous(monkeypatch, tmp_data_dirs: dict) -> None:
    monkeypatch.setattr(admin_targets.config, "DATA_DIR", tmp_data_dirs["data_dir"])
    cfg = Path(tmp_data_dirs["data_dir"]) / "admin_targets.json"
    _write_targets(
        cfg,
        {
            "groups": {
                "高数群": 123456,
                "高数 群": 223344,
            },
            "users": {},
        },
    )
    admin_targets.clear_target_resolver_cache()

    rr = admin_targets.resolve_group_target("高数群")
    assert rr.ok is False
    assert rr.status == "ambiguous"
    assert sorted(rr.candidates) == [123456, 223344]


def test_resolve_target_alias_disabled_still_allows_explicit_id(monkeypatch, tmp_data_dirs: dict) -> None:
    monkeypatch.setattr(admin_targets.config, "DATA_DIR", tmp_data_dirs["data_dir"])
    monkeypatch.setattr(admin_targets.config, "ENABLE_ADMIN_TARGET_ALIASES", False)
    cfg = Path(tmp_data_dirs["data_dir"]) / "admin_targets.json"
    _write_targets(cfg, {"groups": {"高数群": 123456}, "users": {"班长": 234567890}})
    admin_targets.clear_target_resolver_cache()

    by_alias = admin_targets.resolve_group_target("高数群")
    by_id = admin_targets.resolve_group_target("群123456")
    user_by_alias = admin_targets.resolve_user_target("班长")
    user_by_id = admin_targets.resolve_user_target("QQ234567890")

    assert by_alias.ok is False and by_alias.status == "not_found"
    assert user_by_alias.ok is False and user_by_alias.status == "not_found"
    assert by_id.ok is True and by_id.target_id == 123456
    assert user_by_id.ok is True and user_by_id.target_id == 234567890
