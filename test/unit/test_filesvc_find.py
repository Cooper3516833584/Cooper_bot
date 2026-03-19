from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from filesvc import FileService


class _DummyLog:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return


def _make_ctx(*, level: int, group_id: int = 20001, user_id: int = 10001):
    return SimpleNamespace(
        scene="group",
        user_id=int(user_id),
        nickname="tester",
        card="tester",
        group_id=int(group_id),
        group_name="test-group",
        level=int(level),
    )


def _build_service() -> FileService:
    service = FileService(log=_DummyLog())
    service.ensure_dirs()
    return service


def test_find_basic_keyword_match(test_config: dict) -> None:
    service = _build_service()
    public_dir = Path(test_config["public_dir"])
    dir_hit = public_dir / "course_keyword_notes"
    file_hit = public_dir / "docs" / "keyword_summary.txt"
    dir_hit.mkdir(parents=True, exist_ok=True)
    file_hit.parent.mkdir(parents=True, exist_ok=True)
    file_hit.write_text("keyword material", encoding="utf-8")

    ctx = _make_ctx(level=1)
    hits = service.find(ctx, "keyword")

    assert dir_hit in hits
    assert file_hit in hits


def test_find_respects_in_dir(test_config: dict) -> None:
    service = _build_service()
    public_dir = Path(test_config["public_dir"])
    allowed = public_dir / "math" / "algebra_keyword.txt"
    blocked = public_dir / "physics" / "algebra_keyword.txt"
    allowed.parent.mkdir(parents=True, exist_ok=True)
    blocked.parent.mkdir(parents=True, exist_ok=True)
    allowed.write_text("in-dir", encoding="utf-8")
    blocked.write_text("out-dir", encoding="utf-8")

    ctx = _make_ctx(level=1)
    hits = service.find(ctx, "algebra", in_dir="public/math")

    assert allowed in hits
    assert blocked not in hits


def test_find_respects_permission_scope(test_config: dict) -> None:
    service = _build_service()
    public_file = Path(test_config["public_dir"]) / "scope_public_doc.txt"
    friend_file = Path(test_config["friend_dir"]) / "scope_friend_doc.txt"
    public_file.parent.mkdir(parents=True, exist_ok=True)
    friend_file.parent.mkdir(parents=True, exist_ok=True)
    public_file.write_text("scope", encoding="utf-8")
    friend_file.write_text("scope", encoding="utf-8")

    level1_hits = service.find(_make_ctx(level=1), "scope")
    level2_hits = service.find(_make_ctx(level=2), "scope")

    assert public_file in level1_hits
    assert friend_file not in level1_hits
    assert public_file in level2_hits
    assert friend_file in level2_hits


def test_find_uses_index_when_available(test_config: dict, monkeypatch) -> None:
    service = _build_service()
    hit = Path(test_config["public_dir"]) / "index_hit.txt"
    hit.parent.mkdir(parents=True, exist_ok=True)
    hit.write_text("index", encoding="utf-8")
    calls = {"index": 0, "scan": 0}

    def _index_only(**_kwargs):
        calls["index"] += 1
        return [hit]

    def _scan_unused(**_kwargs):
        calls["scan"] += 1
        return []

    monkeypatch.setattr(service, "_find_with_index", _index_only)
    monkeypatch.setattr(service, "_find_by_scan", _scan_unused)

    hits = service.find(_make_ctx(level=1), "index")

    assert hits == [hit]
    assert calls["index"] == 1
    assert calls["scan"] == 0


def test_find_falls_back_when_index_broken(test_config: dict, monkeypatch) -> None:
    service = _build_service()
    hit = Path(test_config["public_dir"]) / "fallback_hit.txt"
    hit.parent.mkdir(parents=True, exist_ok=True)
    hit.write_text("fallback", encoding="utf-8")
    calls = {"scan": 0}

    def _broken_index(**_kwargs):
        raise FileNotFoundError("broken index")

    def _scan_fallback(**_kwargs):
        calls["scan"] += 1
        return [hit]

    monkeypatch.setattr(service, "_find_with_index", _broken_index)
    monkeypatch.setattr(service, "_find_by_scan", _scan_fallback)

    hits = service.find(_make_ctx(level=1), "fallback")

    assert hits == [hit]
    assert calls["scan"] == 1


def test_find_result_order_is_stable(test_config: dict) -> None:
    service = _build_service()
    public_dir = Path(test_config["public_dir"])
    for name in ("topic_a.txt", "topic_b.txt", "topic_c.txt"):
        p = public_dir / "stable" / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("stable order", encoding="utf-8")

    ctx = _make_ctx(level=1)
    first = [p.name for p in service.find(ctx, "topic")]
    second = [p.name for p in service.find(ctx, "topic")]

    assert first == second
