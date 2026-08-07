from __future__ import annotations

from pathlib import Path
from typing import Optional

import aisvc
from aisvc import AIService


class _DummyLog:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def info(self, _msg: str) -> None:
        return

    def warning(self, msg: str) -> None:
        self.warnings.append(str(msg))


def _new_service(log: Optional[_DummyLog] = None) -> AIService:
    return AIService(log=log or _DummyLog())


class _FakePage:
    def __init__(self, text: Optional[str]) -> None:
        self._text = text

    def extract_text(self) -> Optional[str]:
        return self._text


class _FakeReader:
    def __init__(self, _fp) -> None:
        self.pages = [_FakePage("page-1"), _FakePage(""), _FakePage("page-3")]


class _BrokenReader:
    def __init__(self, _fp) -> None:
        raise RuntimeError("boom")


def _write_fake_pdf(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"%PDF-1.4\n%fake\n")


def test_read_pdf_head_extracts_text_by_page_and_respects_limits(monkeypatch, test_config: dict) -> None:
    monkeypatch.setattr(aisvc, "PdfReader", _FakeReader)
    svc = _new_service()
    pdf_path = Path(test_config["ai_material_dir"]) / "math" / "ok.pdf"
    _write_fake_pdf(pdf_path)

    out = svc._read_pdf_head(pdf_path, max_pages=3, max_chars=20)
    assert out == "page-1\npage-3"

    clipped = svc._read_pdf_head(pdf_path, max_pages=3, max_chars=6)
    assert clipped == "page-1"


def test_read_pdf_head_returns_empty_when_reader_missing(monkeypatch, test_config: dict) -> None:
    monkeypatch.setattr(aisvc, "PdfReader", None)
    svc = _new_service()
    pdf_path = Path(test_config["ai_material_dir"]) / "math" / "missing_dep.pdf"
    _write_fake_pdf(pdf_path)

    assert svc._read_pdf_head(pdf_path, max_pages=3, max_chars=20) == ""


def test_read_pdf_head_logs_warning_and_returns_empty_on_reader_failure(monkeypatch, test_config: dict) -> None:
    monkeypatch.setattr(aisvc, "PdfReader", _BrokenReader)
    log = _DummyLog()
    svc = _new_service(log=log)
    pdf_path = Path(test_config["ai_material_dir"]) / "math" / "broken.pdf"
    _write_fake_pdf(pdf_path)

    out = svc._read_pdf_head(pdf_path, max_pages=3, max_chars=20)
    assert out == ""
    assert any("broken.pdf" in msg for msg in log.warnings)


def test_extract_notice_file_head_reads_md(tmp_path) -> None:
    svc = _new_service()
    md_path = tmp_path / "notes.md"
    md_path.write_text(
        "# 期中考试安排\n\n- 时间：下周一 14:00\n- 地点：A101\n\n**请带学生证**",
        encoding="utf-8",
    )

    out = svc._extract_notice_file_head_sync(md_path, max_chars=4000)
    assert "期中考试安排" in out
    assert "A101" in out
    assert "学生证" in out

    clipped = svc._extract_notice_file_head_sync(md_path, max_chars=10)
    assert len(clipped) <= 10


def test_extract_notice_file_head_reads_markdown_extension(tmp_path) -> None:
    svc = _new_service()
    md_path = tmp_path / "README.markdown"
    md_path.write_text("## 使用说明\n\n详情见文档。", encoding="utf-8")
    out = svc._extract_notice_file_head_sync(md_path, max_chars=200)
    assert "使用说明" in out


def test_extract_notice_file_head_md_missing_file_returns_empty(tmp_path) -> None:
    svc = _new_service()
    assert svc._extract_notice_file_head_sync(tmp_path / "nope.md", max_chars=200) == ""
    assert svc._extract_notice_file_head_sync(tmp_path / "nope.xyz", max_chars=200) == ""
