from __future__ import annotations

from pathlib import Path
import zipfile

from handinsvc import HandinService


class _DummyLog:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return

    def error(self, _msg: str) -> None:
        return

    def exception(self, _msg: str) -> None:
        return


def _new_service() -> HandinService:
    return HandinService(_DummyLog())


def test_handin_flow_create_submit_duplicate_summary_and_zip(monkeypatch, tmp_project_root: Path, controlled_time) -> None:
    service = _new_service()
    now_ts = controlled_time.time()
    ok, _ = service.create_task(
        group_id=20001,
        creator_id=900001,
        name="hw_flow",
        remind_ts_list=[],
        deadline_ts=now_ts + 1800,
    )
    assert ok is True
    task = service.list_tasks_by_group(20001)[0]

    inbox_file = service.inbox_dir / "10001" / "Alice_U20230001_hw_flow.pdf"
    inbox_file.parent.mkdir(parents=True, exist_ok=True)
    inbox_file.write_bytes(b"flow-first")

    moved_ok, _msg, moved_dst, moved_code = service.move_inbox_to_task(inbox_file, task, overwrite=False)
    assert moved_ok is True
    assert moved_code == "OK"
    assert moved_dst is not None and moved_dst.exists()

    dup_file = service.inbox_dir / "10001" / "Alice_U20230001_hw_flow.pdf"
    dup_file.write_bytes(b"flow-second")
    dup_ok, _dup_msg, _dup_dst, dup_code = service.move_inbox_to_task(dup_file, task, overwrite=False)
    assert dup_ok is False
    assert dup_code == "EXISTS"

    monkeypatch.setattr(service, "_get_roster", lambda: [("U20230001", "Alice"), ("U20230002", "Bob")])
    ok_stats, _stats_msg, missing, stats = service.compute_missing(task)
    assert ok_stats is True
    assert stats["handed_in"] == 1
    assert stats["missing"] == 1
    assert missing == [("U20230002", "Bob")]

    summary = service.format_missing_message(task, missing, stats, title="flow-summary")
    assert task.name in summary

    out_zip = tmp_project_root / "hw_flow.zip"
    zip_ok, _zip_msg, zip_path = service.zip_submissions(task, out_zip)
    assert zip_ok is True
    assert zip_path is not None and zip_path.exists()
    with zipfile.ZipFile(zip_path, "r") as zf:
        assert "Alice_U20230001_hw_flow.pdf" in set(zf.namelist())


def test_handin_flow_persists_and_reloads_tasks(controlled_time) -> None:
    service_1 = _new_service()
    now_ts = controlled_time.time()
    ok, _ = service_1.create_task(
        group_id=20001,
        creator_id=900001,
        name="hw_persist",
        remind_ts_list=[],
        deadline_ts=now_ts + 900,
    )
    assert ok is True
    task_id = service_1.list_tasks_by_group(20001)[0].task_id

    service_2 = _new_service()
    reloaded = service_2.list_tasks_by_group(20001, include_closed=True)
    assert any(t.task_id == task_id for t in reloaded)


def test_handin_flow_outside_roster_submission_is_not_counted(monkeypatch, controlled_time) -> None:
    service = _new_service()
    now_ts = controlled_time.time()
    ok, _ = service.create_task(
        group_id=20001,
        creator_id=900001,
        name="hw_outside_flow",
        remind_ts_list=[],
        deadline_ts=now_ts + 1800,
    )
    assert ok is True
    task = service.list_tasks_by_group(20001)[0]

    submitted = service._task_files_dir(task.group_id, task.name) / "Outsider_U20239999_hw_outside_flow.pdf"
    submitted.parent.mkdir(parents=True, exist_ok=True)
    submitted.write_bytes(b"outsider")

    monkeypatch.setattr(service, "_get_roster", lambda: [("U20230001", "Alice")])
    ok_stats, _msg, missing, stats = service.compute_missing(task)

    assert ok_stats is True
    assert stats["handed_in"] == 0
    assert stats["missing"] == 1
    assert missing == [("U20230001", "Alice")]
