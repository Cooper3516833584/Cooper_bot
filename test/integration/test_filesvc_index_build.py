from __future__ import annotations

import json
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


def test_find_index_skips_deleted_paths(test_config: dict, monkeypatch) -> None:
    target = Path(test_config["public_dir"]) / "indexed" / "deleted_index_target.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("stale index target", encoding="utf-8")

    builder = _new_service()
    builder.build_find_index()
    target.unlink()

    finder = _new_service()

    def _scan_should_not_run(**_kwargs):
        raise AssertionError("scan fallback should not be used when index is healthy")

    monkeypatch.setattr(finder, "_find_by_scan", _scan_should_not_run)

    hits = finder.find(_make_ctx(level=1), "deleted_index_target")
    assert target not in hits


def test_find_index_excludes_blacklisted_files(test_config: dict) -> None:
    public_dir = Path(test_config["public_dir"])
    pdf = public_dir / "clean_index" / "useful_answer.pdf"
    rare_doc = public_dir / "clean_index" / "useful_answer.caj"
    image = public_dir / "clean_index" / "useful_answer.png"
    aux = public_dir / "clean_index" / "useful_answer.aux"
    html = public_dir / "clean_index" / "useful_answer.html"
    source = public_dir / "clean_index" / "useful_answer.cpp"
    numeric_suffix = public_dir / "clean_index" / "useful_answer.1"
    no_suffix = public_dir / "clean_index" / "useful_answer"
    pdf.parent.mkdir(parents=True, exist_ok=True)
    pdf.write_text("pdf placeholder", encoding="utf-8")
    rare_doc.write_text("rare document placeholder", encoding="utf-8")
    image.write_text("image placeholder", encoding="utf-8")
    aux.write_text("aux placeholder", encoding="utf-8")
    html.write_text("html placeholder", encoding="utf-8")
    source.write_text("source placeholder", encoding="utf-8")
    numeric_suffix.write_text("numeric suffix placeholder", encoding="utf-8")
    no_suffix.write_text("no suffix placeholder", encoding="utf-8")

    service = _new_service()
    service.build_find_index()
    payload = json.loads(service._find_index_path.read_text(encoding="utf-8"))
    indexed_names = {str(item.get("name") or "") for item in payload.get("entries") or []}

    assert pdf.name in indexed_names
    assert rare_doc.name in indexed_names
    assert image.name not in indexed_names
    assert aux.name not in indexed_names
    assert html.name not in indexed_names
    assert source.name not in indexed_names
    assert numeric_suffix.name not in indexed_names
    assert no_suffix.name not in indexed_names


def test_specific_file_can_rank_before_generic_directory(test_config: dict) -> None:
    public_dir = Path(test_config["public_dir"])
    generic_dir = public_dir / "ranking_noise_topic"
    target = public_dir / "ranking_noise_topic_answer.pdf"
    generic_dir.mkdir(parents=True, exist_ok=True)
    target.write_text("answer placeholder", encoding="utf-8")

    service = _new_service()
    service.build_find_index()

    hits = service.find(_make_ctx(level=1), "ranking noise topic answer")
    assert hits
    assert hits[0] == target


def test_find_lists_files_before_directories(test_config: dict) -> None:
    public_dir = Path(test_config["public_dir"])
    exact_dir = public_dir / "prioritytopic"
    file_hit = public_dir / "prioritytopic_notes.txt"
    exact_dir.mkdir(parents=True, exist_ok=True)
    file_hit.write_text("file should be above directories", encoding="utf-8")

    service = _new_service()
    service.build_find_index()

    hits = service.find(_make_ctx(level=1), "prioritytopic")
    assert file_hit in hits
    assert exact_dir in hits
    assert hits.index(file_hit) < hits.index(exact_dir)


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
