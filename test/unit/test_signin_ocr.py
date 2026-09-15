from __future__ import annotations

from datetime import time
from types import SimpleNamespace

import cv2
import numpy as np

import cooper_bot.modules.vision.signin_ocr as signin_ocr
from cooper_bot.modules.vision.signin_ocr import (
    SigninOcrResult,
    _parse_ocr_time_text,
    _parse_ssocr_time_text,
    _prepare_ssocr_mask,
    _recognize_visual_time,
    _recognize_ssocr_candidates,
    _run_ssocr,
    _ssocr_mask_variants,
    _valid_hhmmss_digits,
    recognize_led_time_from_path,
)


def test_signin_ocr_result_parsed_time() -> None:
    assert SigninOcrResult("12:34:56", 1.0, "test").parsed_time == time(12, 34, 56)
    assert SigninOcrResult("07:08", 1.0, "test").parsed_time == time(7, 8)
    assert SigninOcrResult("24:00:00", 1.0, "test").parsed_time is None
    assert SigninOcrResult("12:60:00", 1.0, "test").parsed_time is None
    assert SigninOcrResult("abc", 1.0, "test").parsed_time is None
    assert SigninOcrResult(None, 1.0, "test").parsed_time is None


def test_valid_hhmmss_digits() -> None:
    assert _valid_hhmmss_digits("000000")
    assert _valid_hhmmss_digits("235959")
    assert not _valid_hhmmss_digits("240000")
    assert not _valid_hhmmss_digits("126000")
    assert not _valid_hhmmss_digits("12345")
    assert not _valid_hhmmss_digits("abcdef")


def test_parse_ocr_time_text() -> None:
    assert _parse_ocr_time_text("12:34:56") == "12:34:56"
    assert _parse_ocr_time_text("12：34：56") == "12:34:56"
    assert _parse_ocr_time_text("123456") == "12:34:56"
    assert _parse_ocr_time_text("93456") == "09:34:56"
    assert _parse_ocr_time_text("24:00:00") is None
    assert _parse_ocr_time_text("not a time") is None


def test_recognize_led_time_from_missing_path_returns_failure(tmp_path) -> None:
    result = recognize_led_time_from_path(tmp_path / "missing.jpg")

    assert result.time_text is None
    assert result.source == "none"
    assert result.reason == "image_read_failed"
    assert result.visual_time_text is None


def test_parse_ssocr_time_text_requires_six_valid_digits() -> None:
    assert _parse_ssocr_time_text("123456\n") == "12:34:56"
    assert _parse_ssocr_time_text("12.34.56\n") == "12:34:56"
    assert _parse_ssocr_time_text("12:34:56") == "12:34:56"
    assert _parse_ssocr_time_text("235959") == "23:59:59"
    assert _parse_ssocr_time_text("240000") is None
    assert _parse_ssocr_time_text("126000") is None
    assert _parse_ssocr_time_text("12345") is None
    assert _parse_ssocr_time_text("1234567") is None
    assert _parse_ssocr_time_text("12?456") is None
    assert _parse_ssocr_time_text("") is None


def test_find_ssocr_executable_uses_path_fallback(monkeypatch) -> None:
    monkeypatch.setattr(signin_ocr.shutil, "which", lambda name: "/bin/ssocr" if name == "ssocr" else None)
    assert signin_ocr._find_ssocr_executable() == "/bin/ssocr"

    monkeypatch.setattr(signin_ocr.shutil, "which", lambda name: "C:/bin/ssocr.exe" if name == "ssocr.exe" else None)
    assert signin_ocr._find_ssocr_executable() == "C:/bin/ssocr.exe"

    monkeypatch.setattr(signin_ocr.shutil, "which", lambda _name: None)
    assert signin_ocr._find_ssocr_executable() is None


def test_run_ssocr_passes_png_to_stdin_without_shell(monkeypatch) -> None:
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout=b"123456\n", stderr=b"")

    monkeypatch.setattr(signin_ocr.subprocess, "run", fake_run)
    mask = np.zeros((12, 48), dtype=np.uint8)
    mask[2:10, 3:45] = 255

    assert _run_ssocr(mask, "/usr/bin/ssocr") == "12:34:56"
    args, kwargs = calls[0]
    assert args[:7] == ["/usr/bin/ssocr", "-d", "6", "-c", "digits", "-f", "white"]
    assert args[-1] == "-"
    assert "shell" not in kwargs
    decoded = cv2.imdecode(np.frombuffer(kwargs["input"], dtype=np.uint8), cv2.IMREAD_GRAYSCALE)
    assert decoded is not None
    assert decoded.shape == mask.shape


def test_run_ssocr_returns_none_for_process_failures(monkeypatch) -> None:
    mask = np.zeros((12, 48), dtype=np.uint8)

    for returncode in (1, 2, 99):
        monkeypatch.setattr(
            signin_ocr.subprocess,
            "run",
            lambda *_args, returncode=returncode, **_kwargs: SimpleNamespace(returncode=returncode, stdout=b"123456", stderr=b""),
        )
        assert _run_ssocr(mask, "ssocr") is None

    def raise_timeout(*_args, **_kwargs):
        raise signin_ocr.subprocess.TimeoutExpired("ssocr", 2.0)

    monkeypatch.setattr(signin_ocr.subprocess, "run", raise_timeout)
    assert _run_ssocr(mask, "ssocr") is None


def test_run_ssocr_rejects_invalid_mask() -> None:
    assert _run_ssocr(np.array([], dtype=np.uint8), "ssocr") is None


def test_prepare_ssocr_mask_trims_scales_and_pads() -> None:
    assert _prepare_ssocr_mask(np.array([], dtype=np.uint8)) is None

    mask = np.zeros((20, 40), dtype=np.uint8)
    mask[5:15, 10:30] = 255
    prepared = _prepare_ssocr_mask(mask)

    assert prepared is not None
    assert prepared.dtype == np.uint8
    assert prepared.ndim == 2
    assert prepared.shape[0] > 120
    assert prepared[0, 0] == 0
    assert cv2.countNonZero(prepared) > 0


def test_ssocr_mask_variants_are_binary_and_limited() -> None:
    img = np.zeros((40, 100, 3), dtype=np.uint8)
    img[8:32, 10:90] = (0, 0, 255)

    variants = _ssocr_mask_variants(img, (5, 5, 90, 30))

    assert 1 <= len(variants) <= 3
    for name, mask in variants:
        assert name.startswith("ssocr_")
        assert mask.ndim == 2
        assert mask.dtype == np.uint8
        assert set(np.unique(mask)).issubset({0, 255})


def test_recognize_ssocr_candidates_limits_boxes_and_uses_ssocr(monkeypatch) -> None:
    calls = []
    variants = [("ssocr_red_80", np.ones((4, 4), dtype=np.uint8))] * 3
    monkeypatch.setattr(signin_ocr, "_find_ssocr_executable", lambda: "/usr/bin/ssocr")
    monkeypatch.setattr(signin_ocr, "_ssocr_mask_variants", lambda _img, _box: variants)
    monkeypatch.setattr(signin_ocr, "_run_ssocr", lambda _mask, _executable: calls.append(1) or "12:34:56")
    boxes = [(float(index), (index * 20, 0, 10, 10)) for index in range(5)]

    candidates = _recognize_ssocr_candidates(np.zeros((20, 120, 3), dtype=np.uint8), boxes)

    assert len(candidates) == 12
    assert len(calls) == 12
    assert all(candidate.time_text == "12:34:56" for candidate in candidates)
    assert all(candidate.reason.startswith("ssocr_") for candidate in candidates)
    assert all(candidate.confidence == 0.92 for candidate in candidates)


def test_recognize_ssocr_candidates_skips_when_ssocr_is_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(signin_ocr, "_find_ssocr_executable", lambda: None)

    assert _recognize_ssocr_candidates(np.zeros((20, 20, 3), dtype=np.uint8), [(1.0, (0, 0, 10, 10))]) == []


def _stub_visual_time_boxes(monkeypatch) -> None:
    monkeypatch.setattr(signin_ocr, "_black_panel_frame_candidate_boxes", lambda *_args: [(1.0, (0, 0, 50, 20))])
    monkeypatch.setattr(signin_ocr, "_candidate_boxes_from_mask", lambda _mask: [(0, 0, 50, 20)])
    monkeypatch.setattr(signin_ocr, "_score_candidate_box", lambda *_args: 1.2)


def test_visual_time_prefers_ssocr_without_rapidocr(monkeypatch) -> None:
    _stub_visual_time_boxes(monkeypatch)
    monkeypatch.setattr(
        signin_ocr,
        "_recognize_ssocr_candidates",
        lambda *_args: [signin_ocr._ParsedCandidate("12:34:56", 0.92, 2.5, "ssocr_red_80")],
    )
    monkeypatch.setattr(
        signin_ocr,
        "_recognize_ocr_candidates",
        lambda *_args: (_ for _ in ()).throw(AssertionError("RapidOCR must be fallback only")),
    )

    result = _recognize_visual_time(np.zeros((40, 100, 3), dtype=np.uint8))

    assert result.time_text == "12:34:56"
    assert result.source == "visual"
    assert result.reason.startswith("ssocr_")


def test_visual_time_falls_back_to_rapidocr(monkeypatch) -> None:
    _stub_visual_time_boxes(monkeypatch)
    monkeypatch.setattr(signin_ocr, "_recognize_ssocr_candidates", lambda *_args: [])
    monkeypatch.setattr(
        signin_ocr,
        "_recognize_ocr_candidates",
        lambda *_args: [signin_ocr._ParsedCandidate("12:34:56", 0.88, 2.0, "rapidocr_crop")],
    )

    result = _recognize_visual_time(np.zeros((40, 100, 3), dtype=np.uint8))

    assert result.time_text == "12:34:56"
    assert result.reason == "rapidocr_crop"


def test_visual_time_returns_existing_failure_when_both_backends_fail(monkeypatch) -> None:
    _stub_visual_time_boxes(monkeypatch)
    monkeypatch.setattr(signin_ocr, "_recognize_ssocr_candidates", lambda *_args: [])
    monkeypatch.setattr(signin_ocr, "_recognize_ocr_candidates", lambda *_args: [])

    result = _recognize_visual_time(np.zeros((40, 100, 3), dtype=np.uint8))

    assert result.time_text is None
    assert result.reason == "no_valid_led_time"
