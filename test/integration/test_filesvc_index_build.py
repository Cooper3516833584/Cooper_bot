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


def _new_service() -> FileService:
    service = FileService(log=_DummyLog())
    service.ensure_dirs()
    return service


def test_find_uses_index_when_available(test_config: dict, monkeypatch) -> None:
    target = Path(test_config["public_dir"]) / "indexed" / "disk_index_target.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("index target", encoding="utf-8")

    builder = _new_service()
    stats = builder.build_find_index()
    assert int(stats["entries"]) >= 1

    finder = _new_service()

    def _scan_should_not_run(**_kwargs):
        raise AssertionError("scan fallback should not be used when index is healthy")

    monkeypatch.setattr(finder, "_find_by_scan", _scan_should_not_run)

    hits = finder.find(_make_ctx(level=1), "disk_index_target")
    assert target in hits


def test_find_falls_back_when_index_broken(test_config: dict, monkeypatch) -> None:
    target = Path(test_config["public_dir"]) / "broken_index" / "need_scan.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("broken index fallback", encoding="utf-8")

    service = _new_service()
    service.build_find_index()
    service._find_index_path.write_text("{broken-json", encoding="utf-8")
    service._find_index_loaded = False
    service._find_index_entries = []
    calls = {"scan": 0}
    original_scan = service._find_by_scan

    def _scan_with_flag(**kwargs):
        calls["scan"] += 1
        return original_scan(**kwargs)

    monkeypatch.setattr(service, "_find_by_scan", _scan_with_flag)

    hits = service.find(_make_ctx(level=1), "need_scan")
    assert target in hits
    assert calls["scan"] >= 1


def test_find_falls_back_when_index_missing(test_config: dict, monkeypatch) -> None:
    target = Path(test_config["public_dir"]) / "missing_index" / "scan_only.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("missing index fallback", encoding="utf-8")

    service = _new_service()
    service.build_find_index()
    if service._find_index_path.exists():
        service._find_index_path.unlink()
    service._find_index_loaded = False
    service._find_index_entries = []
    calls = {"scan": 0}
    original_scan = service._find_by_scan

    def _scan_with_flag(**kwargs):
        calls["scan"] += 1
        return original_scan(**kwargs)

    monkeypatch.setattr(service, "_find_by_scan", _scan_with_flag)

    hits = service.find(_make_ctx(level=1), "scan_only")
    assert target in hits
    assert calls["scan"] >= 1
