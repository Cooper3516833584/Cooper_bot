from __future__ import annotations

from pathlib import Path

import numpy as np

from cooper_bot.modules.ai.aisvc import AIService
from cooper_bot.modules.files.filesvc import FileService
from cooper_bot.modules.handin.handinsvc import HandinService
from cooper_bot.modules.logging.logsvc import LogService


class _DummyLog:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return

    def error(self, _msg: str) -> None:
        return

    def exception(self, _msg: str) -> None:
        return


def test_startup_can_instantiate_core_services(tmp_data_dirs: dict) -> None:
    logger = _DummyLog()
    filesvc = FileService(logger)
    filesvc.ensure_dirs()
    handin = HandinService(logger)
    aisvc = AIService(logger)
    log_service = LogService(base_dir=tmp_data_dirs["log_dir"], log=logger)

    assert Path(tmp_data_dirs["public_dir"]).exists()
    assert handin.db_path.exists() or handin.db_path.parent.exists()
    assert aisvc.material_dir.exists()
    assert log_service.base_dir.exists()


def test_startup_quick_bootstrap_and_find_index_build(monkeypatch, test_config: dict) -> None:
    logger = _DummyLog()
    filesvc = FileService(logger)
    filesvc.ensure_dirs()

    target = Path(test_config["public_dir"]) / "smoke_startup" / "intro_notes.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("smoke startup material", encoding="utf-8")

    aisvc = AIService(logger)
    monkeypatch.setattr(aisvc, "_load_api_config", lambda: None)
    aisvc._bootstrap_quick_sync_sync()

    stats = filesvc.build_find_index()
    assert int(stats["entries"]) >= 1


def test_startup_flow_runs_without_real_ai(monkeypatch, test_config: dict) -> None:
    logger = _DummyLog()
    service = AIService(logger)
    service.embedding_base_url = "https://embed.local/v1"
    service.embedding_api_key = "fake-embed-key"

    monkeypatch.setattr(service, "_load_api_config", lambda: None)
    monkeypatch.setattr(service, "_auto_organize_materials_on_boot", lambda _index_list: ({}, {}, set()))

    def _fake_pipeline(rel: str, hint=None, build_vector: bool = True) -> dict:
        _ = (hint, build_vector)
        rel_norm = service._normalize_rel(rel)
        subject = service._subject_from_rel(rel_norm)
        filename = Path(rel_norm).name
        return {
            "index_item": {
                "file_path": service._to_store_rel(rel_norm),
                "subject": subject,
                "filename": filename,
                "file_type": "pdf",
                "keywords": [subject],
                "summary": f"smoke:{filename}",
            },
            "metadata_item": {
                "file_path": service._to_store_rel(rel_norm),
                "filename": filename,
                "subject": subject,
            },
            "embedding_text": f"embed:{rel_norm}",
        }

    monkeypatch.setattr(service, "_run_new_file_pipeline", _fake_pipeline)
    monkeypatch.setattr(
        service,
        "_build_vector_for_embedding_text",
        lambda _text: np.asarray([1.0, 0.0, 0.0], dtype=np.float64),
    )

    p = Path(test_config["ai_material_dir"]) / "math" / "smoke_bootstrap.pdf"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"fake-pdf")

    service._bootstrap_quick_sync_sync()
    service._bootstrap_sync_sync()

    index_by_rel, metadata_by_rel, vector_by_rel = service._load_incremental_store_maps()
    assert "math/smoke_bootstrap.pdf" in index_by_rel
    assert "math/smoke_bootstrap.pdf" in metadata_by_rel
    assert "math/smoke_bootstrap.pdf" in vector_by_rel
