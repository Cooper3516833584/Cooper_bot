from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import logsvc
from logsvc import LogService


class _DummyLog:
    def __init__(self) -> None:
        self.records: list[tuple[str, str]] = []

    def info(self, msg: str) -> None:
        self.records.append(("info", str(msg)))

    def warning(self, msg: str) -> None:
        self.records.append(("warning", str(msg)))

    def error(self, msg: str) -> None:
        self.records.append(("error", str(msg)))

    def exception(self, msg: str) -> None:
        self.records.append(("exception", str(msg)))


def _ctx(
    *,
    scene: str,
    user_id: int,
    group_id: int | None = None,
    group_name: str | None = None,
    nickname: str = "tester",
    card: str = "",
):
    return SimpleNamespace(
        scene=scene,
        user_id=int(user_id),
        nickname=str(nickname),
        card=str(card),
        group_id=group_id,
        group_name=group_name,
    )


def _immediate_flush(monkeypatch) -> None:
    monkeypatch.setattr(logsvc, "IDLE_SPLIT_SECONDS", 0)


def _list_log_files(base_dir: Path) -> list[Path]:
    return sorted([p for p in base_dir.rglob("*.txt") if p.is_file()])


def test_log_sessions_aggregate_for_same_group(monkeypatch, tmp_project_root: Path) -> None:
    logger = _DummyLog()
    service = LogService(base_dir=tmp_project_root / "logs", log=logger)
    _immediate_flush(monkeypatch)

    g1 = _ctx(scene="group", user_id=10001, group_id=20001, group_name="class-a", nickname="alice")
    g2 = _ctx(scene="group", user_id=10002, group_id=20001, group_name="class-a", nickname="bob")
    service.log_in(g1, "/find matrix")
    service.log_in(g2, "/find lab")
    service.log_out(g1, "results")
    service.flush_idle()

    files = _list_log_files(service.base_dir)
    assert len(files) == 1
    text = files[0].read_text(encoding="utf-8")
    assert "/find matrix" in text
    assert "/find lab" in text
    assert "[OUT]" in text


def test_log_flush_writes_file_after_idle(monkeypatch, tmp_project_root: Path) -> None:
    logger = _DummyLog()
    service = LogService(base_dir=tmp_project_root / "logs", log=logger)
    _immediate_flush(monkeypatch)

    pctx = _ctx(scene="private_friend", user_id=10001, nickname="private-user")
    service.log_in(pctx, "hello")
    service.log_out(pctx, "hi")
    service.flush_idle()

    files = _list_log_files(service.base_dir)
    assert len(files) == 1
    assert "private" in str(files[0].parent).lower()


def test_log_isolates_multi_group_and_private_sessions(monkeypatch, tmp_project_root: Path) -> None:
    logger = _DummyLog()
    service = LogService(base_dir=tmp_project_root / "logs", log=logger)
    _immediate_flush(monkeypatch)

    g1 = _ctx(scene="group", user_id=10001, group_id=20001, group_name="g1")
    g2 = _ctx(scene="group", user_id=10002, group_id=30001, group_name="g2")
    p1 = _ctx(scene="private_friend", user_id=10003, nickname="p1")
    for c in (g1, g2, p1):
        service.log_in(c, "ping")
        service.log_out(c, "pong")
    service.flush_idle()

    files = _list_log_files(service.base_dir)
    assert len(files) == 3
    paths_text = "\n".join(str(p) for p in files)
    assert "20001" in paths_text
    assert "30001" in paths_text
    assert "private" in paths_text.lower()


def test_log_write_error_does_not_crash(monkeypatch, tmp_project_root: Path) -> None:
    logger = _DummyLog()
    service = LogService(base_dir=tmp_project_root / "logs", log=logger)
    _immediate_flush(monkeypatch)

    gctx = _ctx(scene="group", user_id=10001, group_id=20001, group_name="g1")
    service.log_in(gctx, "will-fail")
    service.log_out(gctx, "will-fail-out")

    def _raise_write(self, *_args, **_kwargs):
        raise OSError("disk full (test)")

    monkeypatch.setattr(Path, "write_text", _raise_write)
    service.flush_idle()

    assert any(level == "error" for level, _ in logger.records)
