from __future__ import annotations

import json
from pathlib import Path
import zipfile

from handinsvc import HANDIN_HASH_INDEX_FILENAME, HandinService


class _DummyLog:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def info(self, msg: str) -> None:
        self.messages.append(("info", str(msg)))

    def warning(self, msg: str) -> None:
        self.messages.append(("warning", str(msg)))

    def error(self, msg: str) -> None:
        self.messages.append(("error", str(msg)))

    def exception(self, msg: str) -> None:
        self.messages.append(("exception", str(msg)))


def _new_service() -> HandinService:
    return HandinService(_DummyLog())


def _first_task(service: HandinService) -> object:
    tasks = service.list_tasks_by_group(20001, include_closed=True)
    assert tasks
    return tasks[0]


def test_create_task_success(controlled_time) -> None:
    service = _new_service()
    now_ts = controlled_time.time()

    ok, msg = service.create_task(
        group_id=20001,
        creator_id=900001,
        name="hw_unit_create",
        remind_ts_list=[now_ts + 300],
        deadline_ts=now_ts + 1800,
    )

    assert ok is True
    assert "hw_unit_create" in msg
    tasks = service.list_active_tasks_by_group(20001)
    assert any(t.name == "hw_unit_create" for t in tasks)
    assert service._task_files_dir(20001, "hw_unit_create").exists()


def test_create_task_with_required_suffix_casefold(controlled_time) -> None:
    service = _new_service()
    now_ts = controlled_time.time()

    ok, msg = service.create_task(
        group_id=20001,
        creator_id=900001,
        name="hw_pdf_only",
        remind_ts_list=[],
        deadline_ts=now_ts + 1800,
        required_suffix="PDF",
    )

    assert ok is True
    assert "限定格式：.pdf" in msg
    task = _first_task(service)
    assert task.required_suffix == "pdf"


def test_deadline_judgement_and_recreate_after_expire(controlled_time) -> None:
    service = _new_service()
    now_ts = controlled_time.time()
    ok, _ = service.create_task(
        group_id=20001,
        creator_id=900001,
        name="hw_deadline",
        remind_ts_list=[],
        deadline_ts=now_ts + 30,
    )
    assert ok is True
    task = _first_task(service)

    assert task.is_active(now_ts + 10) is True
    assert task.is_active(now_ts + 31) is False

    controlled_time.advance(31)
    ok2, _ = service.create_task(
        group_id=20001,
        creator_id=900001,
        name="hw_deadline",
        remind_ts_list=[],
        deadline_ts=controlled_time.time() + 60,
    )
    assert ok2 is True


def test_submit_success_and_duplicate_behavior(controlled_time) -> None:
    service = _new_service()
    now_ts = controlled_time.time()
    ok, _ = service.create_task(
        group_id=20001,
        creator_id=900001,
        name="hw_submit",
        remind_ts_list=[],
        deadline_ts=now_ts + 3600,
    )
    assert ok is True
    task = _first_task(service)

    inbox_file = service.inbox_dir / "10001" / "Alice_U20230001_hw_submit.pdf"
    inbox_file.parent.mkdir(parents=True, exist_ok=True)
    inbox_file.write_bytes(b"first-submit")

    moved_ok, _msg, dst, code = service.move_inbox_to_task(inbox_file, task, overwrite=False)
    assert moved_ok is True
    assert code == "OK"
    assert dst is not None and dst.exists()

    duplicate_file = service.inbox_dir / "10001" / "Alice_U20230001_hw_submit.pdf"
    duplicate_file.write_bytes(b"second-submit")
    dup_ok, _dup_msg, dup_dst, dup_code = service.move_inbox_to_task(duplicate_file, task, overwrite=False)
    assert dup_ok is False
    assert dup_code == "EXISTS"
    assert dup_dst is not None and dup_dst.exists()

    overwrite_ok, _over_msg, overwrite_dst, overwrite_code = service.move_inbox_to_task(
        duplicate_file,
        task,
        overwrite=True,
    )
    assert overwrite_ok is True
    assert overwrite_code == "OK"
    assert overwrite_dst is not None
    assert overwrite_dst.read_bytes() == b"second-submit"


def test_submit_rejects_same_content_with_different_filename(controlled_time) -> None:
    service = _new_service()
    now_ts = controlled_time.time()
    ok, _ = service.create_task(
        group_id=20001,
        creator_id=900001,
        name="hw_hash_duplicate",
        remind_ts_list=[],
        deadline_ts=now_ts + 3600,
    )
    assert ok is True
    task = _first_task(service)

    first = service.inbox_dir / "10001" / "Alice_U20230001_hw.pdf"
    first.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(b"same-content")

    moved_ok, _msg, dst, code = service.move_inbox_to_task(first, task, overwrite=False)
    assert moved_ok is True
    assert code == "OK"
    assert dst is not None and dst.exists()

    files_dir = service._task_files_dir(task.group_id, task.name)
    hash_index = files_dir / HANDIN_HASH_INDEX_FILENAME
    assert hash_index.exists()
    index = json.loads(hash_index.read_text(encoding="utf-8"))
    assert index["files"][dst.name]["sha256"] == HandinService.file_sha256(dst)

    duplicate = service.inbox_dir / "10001" / "Bob_U20230002_hw.pdf"
    duplicate.write_bytes(b"same-content")
    dup_ok, dup_msg, dup_dst, dup_code = service.move_inbox_to_task(duplicate, task, overwrite=False)

    assert dup_ok is False
    assert dup_code == "DUPLICATE"
    assert "内容完全相同" in dup_msg
    assert dup_dst == dst
    assert duplicate.exists()
    assert [p.name for p in service.list_submitted_files(task)] == [dst.name]


def test_outside_roster_submission_not_counted(monkeypatch, controlled_time) -> None:
    service = _new_service()
    now_ts = controlled_time.time()
    ok, _ = service.create_task(
        group_id=20001,
        creator_id=900001,
        name="hw_roster_scope",
        remind_ts_list=[],
        deadline_ts=now_ts + 3600,
    )
    assert ok is True
    task = _first_task(service)

    submit_path = service._task_files_dir(task.group_id, task.name) / "Outsider_U20239999_hw.pdf"
    submit_path.parent.mkdir(parents=True, exist_ok=True)
    submit_path.write_text("outsider submission", encoding="utf-8")

    monkeypatch.setattr(service, "_get_roster", lambda: [("U20230001", "Alice")])
    ok_stats, _msg, missing, stats = service.compute_missing(task)

    assert ok_stats is True
    assert stats["handed_in"] == 0
    assert stats["missing"] == 1
    assert missing == [("U20230001", "Alice")]


def test_summary_and_zip_core_path(tmp_project_root: Path, monkeypatch, controlled_time) -> None:
    service = _new_service()
    now_ts = controlled_time.time()
    ok, _ = service.create_task(
        group_id=20001,
        creator_id=900001,
        name="hw_pack",
        remind_ts_list=[],
        deadline_ts=now_ts + 3600,
    )
    assert ok is True
    task = _first_task(service)

    files_dir = service._task_files_dir(task.group_id, task.name)
    f1 = files_dir / "Alice_U20230001_hw_pack.pdf"
    f2 = files_dir / "Bob_U20230002_hw_pack.docx"
    f1.write_bytes(b"a")
    f2.write_bytes(b"b")
    hash_index = files_dir / HANDIN_HASH_INDEX_FILENAME
    hash_index.write_text('{"version":1,"files":{}}', encoding="utf-8")

    monkeypatch.setattr(service, "_get_roster", lambda: [("U20230001", "Alice"), ("U20230002", "Bob")])
    ok_stats, _msg, missing, stats = service.compute_missing(task)
    assert ok_stats is True
    assert missing == []
    text = service.format_missing_message(task, missing, stats, title="summary")
    assert task.name in text

    out_zip = tmp_project_root / "hw_pack.zip"
    zip_ok, _zip_msg, zip_path = service.zip_submissions(task, out_zip)
    assert zip_ok is True
    assert zip_path is not None and zip_path.exists()
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = set(zf.namelist())
    assert "Alice_U20230001_hw_pack.pdf" in names
    assert "Bob_U20230002_hw_pack.docx" in names
    assert HANDIN_HASH_INDEX_FILENAME not in names
