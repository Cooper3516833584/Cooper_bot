from __future__ import annotations

from datetime import time
from types import SimpleNamespace
import os

import cv2
import numpy as np
import pytest

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
    assert _parse_ssocr_time_text("123456_") is None
    assert _parse_ssocr_time_text("error123456") is None
    assert _parse_ssocr_time_text("") is None


def test_find_ssocr_executable_uses_path_fallback(monkeypatch) -> None:
    monkeypatch.setattr(signin_ocr.shutil, "which", lambda name: "/bin/ssocr" if name == "ssocr" else None)
    assert signin_ocr._find_ssocr_executable() == "/bin/ssocr"

    monkeypatch.setattr(signin_ocr.shutil, "which", lambda name: "C:/bin/ssocr.exe" if name == "ssocr.exe" else None)
    assert signin_ocr._find_ssocr_executable() == "C:/bin/ssocr.exe"

    monkeypatch.setattr(signin_ocr.shutil, "which", lambda _name: None)
    assert signin_ocr._find_ssocr_executable() is None


def _mock_wsl_discovery(monkeypatch, runner):
    monkeypatch.setattr(signin_ocr.sys, "platform", "win32")
    monkeypatch.setattr(signin_ocr.shutil, "which", lambda name: "C:/Windows/wsl.exe" if name == "wsl.exe" else None)
    monkeypatch.setattr(signin_ocr, "_WSL_SSOCR_CACHE", ("", 0.0, None))
    monkeypatch.setattr(signin_ocr.subprocess, "run", runner)


def test_find_ssocr_uses_verified_wsl_and_caches_probe(monkeypatch) -> None:
    calls = []
    def run(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=42, stdout=b"", stderr=b"Seven Segment Optical Character Recognition Version 2.23.1")
    _mock_wsl_discovery(monkeypatch, run)
    expected = ("C:/Windows/wsl.exe", "--exec", "timeout", "2s", "ssocr")
    assert signin_ocr._find_ssocr_executable() == expected
    assert signin_ocr._find_ssocr_executable() == expected
    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args == [*expected, "-V"]
    assert kwargs["stdin"] == signin_ocr.subprocess.DEVNULL
    assert kwargs["timeout"] == 8.0
    assert kwargs["creationflags"] == getattr(signin_ocr.subprocess, "CREATE_NO_WINDOW", 0)
    assert "shell" not in kwargs


@pytest.mark.parametrize("failure", ["timeout", "oserror", "wrong_version", "missing"])
def test_wsl_probe_failures_are_cached_and_retried(monkeypatch, failure) -> None:
    calls = []
    clock = [100.0]
    def run(*args, **kwargs):
        calls.append(1)
        if failure == "timeout":
            raise signin_ocr.subprocess.TimeoutExpired("wsl.exe", 8)
        if failure == "oserror":
            raise OSError("WSL unavailable")
        return SimpleNamespace(returncode=42 if failure == "wrong_version" else 127,
                               stdout=b"", stderr=b"unavailable")
    _mock_wsl_discovery(monkeypatch, run)
    monkeypatch.setattr(signin_ocr.time_module, "monotonic", lambda: clock[0])
    assert signin_ocr._find_ssocr_executable() is None
    assert signin_ocr._find_ssocr_executable() is None
    assert len(calls) == 1
    clock[0] += 61
    assert signin_ocr._find_ssocr_executable() is None
    assert len(calls) == 2


def test_non_windows_never_probes_wsl(monkeypatch) -> None:
    _mock_wsl_discovery(monkeypatch, lambda *_args, **_kwargs: pytest.fail("unexpected WSL probe"))
    monkeypatch.setattr(signin_ocr.sys, "platform", "linux")
    assert signin_ocr._find_ssocr_executable() is None


def test_native_ssocr_still_preferred_over_wsl(monkeypatch) -> None:
    _mock_wsl_discovery(monkeypatch, lambda *_args, **_kwargs: pytest.fail("native should win"))
    monkeypatch.setattr(signin_ocr.shutil, "which", lambda name: "C:/bin/ssocr.exe" if name == "ssocr" else "C:/Windows/wsl.exe")
    assert signin_ocr._find_ssocr_executable() == "C:/bin/ssocr.exe"


def test_wsl_process_receives_binary_stdin_without_shell(monkeypatch) -> None:
    calls = []
    def run(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout=b"091058", stderr=b"")
    monkeypatch.setattr(signin_ocr.subprocess, "run", run)
    command = ("C:/Windows/wsl.exe", "--exec", "timeout", "2s", "ssocr")
    mask = np.zeros((140, 600), np.uint8)
    assert _run_ssocr(mask, command) == "09:10:58"
    args, kwargs = calls[0]
    assert args[:5] == list(command)
    assert args[-1] == "-"
    assert kwargs["input"].startswith(b"\x89PNG")
    assert kwargs["timeout"] == 3.0
    assert "shell" not in kwargs


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
    assert _prepare_ssocr_mask(np.ones((1, 5000), np.uint8)) is None


def _synthetic_display(text="12:34:56", angle=0):
    patterns = {
        "0": "abcedf", "1": "bc", "2": "abged", "3": "abgcd", "4": "fgbc",
        "5": "afgcd", "6": "afgecd", "7": "abc", "8": "abcdefg", "9": "abfgcd",
    }
    segments = {
        "a": (8, 5, 36, 13), "b": (34, 10, 42, 48), "c": (34, 52, 42, 90),
        "d": (8, 87, 36, 95), "e": (2, 52, 10, 90), "f": (2, 10, 10, 48),
        "g": (8, 46, 36, 54),
    }
    image = np.full((350, 650, 3), 220, np.uint8)
    cv2.rectangle(image, (95, 100), (545, 240), (12, 12, 12), -1)
    x = 120
    for char in text:
        if char == ":":
            for y in (145, 190):
                cv2.circle(image, (x + 5, y), 5, (10, 30, 250), -1)
            x += 25
            continue
        for segment in patterns[char]:
            x1, y1, x2, y2 = segments[segment]
            cv2.rectangle(image, (x + x1, 120 + y1), (x + x2, 120 + y2), (10, 30, 250), -1)
        x += 58
    if angle:
        image = cv2.warpAffine(image, cv2.getRotationMatrix2D((325, 175), angle, 1),
                               (650, 350), borderValue=(220, 220, 220))
    return image


def test_display_candidates_find_whole_rotated_panel_and_dedupe() -> None:
    for angle in (-12, 0, 12):
        panels = signin_ocr._display_candidates(_synthetic_display(angle=angle))
        assert len(panels) == 1
        assert np.linalg.norm(panels[0].quad.mean(axis=0) - (320, 170)) < 10
        assert panels[0].crop.shape[1] > 440
        assert 130 < panels[0].crop.shape[0] < 150


def test_display_candidates_ignore_red_clutter_and_blank_images() -> None:
    image = np.full((400, 700, 3), 220, np.uint8)
    cv2.rectangle(image, (100, 100), (500, 220), (0, 0, 240), -1)
    assert signin_ocr._display_candidates(image) == []
    assert signin_ocr._display_candidates(np.zeros_like(image)) == []
    assert signin_ocr._display_candidates(np.full_like(image, 255)) == []


def test_ssocr_mask_variants_are_binary_and_limited() -> None:
    panel = signin_ocr._display_candidates(_synthetic_display())[0]
    variants = _ssocr_mask_variants(panel.crop)
    assert len(variants) == 3
    for name, mask in variants:
        assert name.startswith("ssocr_")
        assert mask.shape[0] == 140
        assert mask.dtype == np.uint8
        assert set(np.unique(mask)) == {0, 255}
        assert not np.any(mask[0]) and not np.any(mask[-1])


def test_red_channel_retains_unsaturated_led_cores() -> None:
    image = _synthetic_display()
    red = image[:, :, 2] == 250
    image[red] = (190, 205, 250)
    # Supply the already located frame: HSV should not hollow out pale cores.
    variants = _ssocr_mask_variants(image[100:240, 95:545])
    assert len(variants) == 3
    assert all(cv2.countNonZero(mask) > 5000 for _, mask in variants)


def test_remove_colon_dots_keeps_narrow_ones_and_unpaired_segments() -> None:
    mask = np.zeros((140, 250), np.uint8)
    mask[10:130, 10:20] = 255
    cv2.circle(mask, (60, 45), 7, 255, -1)
    cv2.circle(mask, (60, 90), 7, 255, -1)
    mask[20:30, 100:130] = 255
    clean = signin_ocr._remove_colon_dots(mask)
    assert np.all(clean[10:130, 10:20] == 255)
    assert not clean[45, 60] and not clean[90, 60]
    assert np.array_equal(clean[:, 100:], mask[:, 100:])
    assert mask[45, 60] == 255  # Does not mutate callers' masks.


def test_recognize_ssocr_candidates_limits_panels(monkeypatch) -> None:
    calls = []
    variants = [("ssocr_red_0.4", np.ones((4, 4), dtype=np.uint8))] * 3
    monkeypatch.setattr(signin_ocr, "_find_ssocr_executable", lambda: "/usr/bin/ssocr")
    monkeypatch.setattr(signin_ocr, "_ssocr_mask_variants", lambda _crop: variants)
    monkeypatch.setattr(signin_ocr, "_run_ssocr", lambda *_args: calls.append(1) or "12:34:56")
    panel = signin_ocr._display_candidates(_synthetic_display())[0]
    candidates = _recognize_ssocr_candidates([panel] * 5)
    assert len(candidates) == len(calls) == 12
    assert {item.candidate_id for item in candidates} == {0, 1, 2, 3}


def test_recognize_ssocr_candidates_skips_when_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(signin_ocr, "_find_ssocr_executable", lambda: None)
    assert _recognize_ssocr_candidates([]) == []


def _reading(text="12:34:56", panel=0):
    return signin_ocr._ParsedCandidate(text, 0.92, 3.0, "ssocr_red_0.4", panel)


def test_consensus_requires_repetition_within_one_panel_and_rejects_conflicts() -> None:
    select = signin_ocr._select_consensus_candidate
    assert select([]) is None
    assert select([_reading()]) is None
    assert select([_reading(panel=0), _reading(panel=1)]) is None
    assert select([_reading(), _reading()]).time_text == "12:34:56"
    assert select([_reading(), _reading(), _reading("12:34:58")]).time_text == "12:34"
    assert select([_reading(), _reading(), _reading("12:34:58", 1), _reading("12:34:58", 1)]) is None


def test_visual_time_prefers_ssocr_without_rapidocr(monkeypatch) -> None:
    monkeypatch.setattr(signin_ocr, "_recognize_ssocr_candidates", lambda *_: [_reading(), _reading()])
    monkeypatch.setattr(signin_ocr, "_recognize_ocr_candidates",
                        lambda *_: (_ for _ in ()).throw(AssertionError("fallback must not run")))
    result = _recognize_visual_time(_synthetic_display())
    assert result.time_text == "12:34:56"
    assert result.source == "visual"
    assert result.reason.endswith("_consensus")


@pytest.mark.parametrize("levels", [(0.6, 0.8, 0.7, 0.9), (0.4, 0.6, 0.8, 0.7, 0.9)])
def test_consensus_recovers_single_low_threshold_prefix_outlier(levels) -> None:
    readings = [signin_ocr._ParsedCandidate(
        "10:58:39" if level == min(levels) else "10:57:39",
        0.92, 3.0, f"ssocr_red_{level:.1f}", 0,
    ) for level in levels]
    for ordered in (readings, list(reversed(readings))):
        result = signin_ocr._select_consensus_candidate(ordered)
        assert result.time_text == "10:57:39"
        assert result.reason.startswith("ssocr_red_0.7" if len(levels) == 4 else "ssocr_red_0.6")
    # A second clock still vetoes this result, even with a single reading.
    assert signin_ocr._select_consensus_candidate(readings + [_reading("10:58:39", 1)]) is None
    assert signin_ocr._select_consensus_candidate(readings + [_reading("10:57:40", 1)]) is None


@pytest.mark.parametrize("texts,levels", [
    (["10:58:39", "10:57:39", "10:57:39"], [.4, .6, .8]),
    (["10:57:39", "10:57:39", "10:57:39", "10:58:39"], [.4, .6, .7, .9]),
    (["10:57:39", "10:58:39", "10:57:39", "10:57:39"], [.4, .6, .7, .9]),
    (["10:58:39", "10:58:39", "10:57:39", "10:57:39", "10:57:39"], [.4, .6, .7, .8, .9]),
    (["10:58:39", "10:57:39", "10:57:39", "10:57:39"], [.4, .6, .6, .6]),
    (["10:58", "10:57:39", "10:57:39", "10:57:39"], [.4, .6, .7, .8]),
])
def test_consensus_rejects_unstable_or_insufficient_threshold_evidence(texts, levels) -> None:
    readings = [signin_ocr._ParsedCandidate(text, .92, 3.0, f"ssocr_red_{level:.1f}", 0)
                for text, level in zip(texts, levels)]
    assert signin_ocr._select_consensus_candidate(readings) is None


def test_threshold_recovery_preserves_seconds_fallback_and_ocr_conflict() -> None:
    readings = [signin_ocr._ParsedCandidate(
        "10:57:49" if level == .4 else "10:57:50", .92, 3.0, f"ssocr_red_{level:.1f}", 0,
    ) for level in (.4, .6, .7, .8)]
    assert signin_ocr._select_consensus_candidate(readings).time_text == "10:57"
    readings = [signin_ocr._ParsedCandidate(text, .88, 3.0, "rapidocr_crop", 0)
                for text in ("10:58:50", "10:57:50", "10:57:50", "10:57:50")]
    assert signin_ocr._select_consensus_candidate(readings) is None


@pytest.mark.parametrize("readings,reason", [
    ([_reading()], "insufficient_led_consensus"),
    ([_reading(), _reading("12:35:58")], "ambiguous_led_time"),
])
def test_visual_time_does_not_override_uncertain_ssocr(monkeypatch, readings, reason) -> None:
    monkeypatch.setattr(signin_ocr, "_recognize_ssocr_candidates", lambda *_: readings)
    monkeypatch.setattr(signin_ocr, "_recognize_ocr_candidates",
                        lambda *_: (_ for _ in ()).throw(AssertionError("must not override uncertainty")))
    result = _recognize_visual_time(_synthetic_display())
    assert result.time_text is None and result.reason == reason


def test_visual_time_falls_back_only_when_no_ssocr_readings(monkeypatch) -> None:
    monkeypatch.setattr(signin_ocr, "_recognize_ssocr_candidates", lambda *_: [])
    monkeypatch.setattr(signin_ocr, "_recognize_ocr_candidates", lambda *_: [_reading(), _reading()])
    assert _recognize_visual_time(_synthetic_display()).time_text == "12:34:56"
    monkeypatch.setattr(signin_ocr, "_recognize_ocr_candidates", lambda *_: [])
    assert _recognize_visual_time(_synthetic_display()).reason == "no_valid_led_time"


def test_recognize_never_uses_filename_as_visual_evidence(tmp_path, monkeypatch) -> None:
    path = tmp_path / "IMG_20260916_123456.jpg"
    cv2.imwrite(str(path), np.zeros((50, 100, 3), np.uint8))
    monkeypatch.setattr(signin_ocr, "_recognize_visual_time",
                        lambda _: SigninOcrResult(None, 0.0, "visual", "no_valid_led_time"))
    result = recognize_led_time_from_path(path)
    assert result.time_text is None
    assert result.timestamp_time_text == "12:34:56"


def _real_ssocr_executable():
    executable = signin_ocr._find_ssocr_executable()
    if os.environ.get("SIGNIN_OCR_TEST_WSL") == "1":
        executable = ("wsl.exe", "-e", "ssocr")
    if not executable:
        pytest.skip("real ssocr unavailable; optionally set SIGNIN_OCR_TEST_WSL=1")
    return executable


@pytest.mark.parametrize("text,angle", [
    ("00:00:00", 0), ("23:59:59", 0), ("12:34:56", -12),
    ("07:08:09", 12), ("11:11:11", 0), ("20:46:38", 0),
])
def test_real_ssocr_synthetic_displays(text, angle) -> None:
    executable = _real_ssocr_executable()
    panels = signin_ocr._display_candidates(_synthetic_display(text, angle))
    readings = signin_ocr._recognize_ssocr_candidates(panels, executable)
    result = signin_ocr._select_consensus_candidate(readings)
    assert result is not None, [(item.time_text, item.reason) for item in readings]
    assert result.time_text == text


@pytest.mark.parametrize("text,expected", [("24:00:00", None), ("12:60:00", None), ("12:34:60", "12:34")])
def test_real_ssocr_rejects_invalid_prefix_but_recovers_minutes(text, expected) -> None:
    panels = signin_ocr._display_candidates(_synthetic_display(text))
    assert panels
    readings = signin_ocr._recognize_ssocr_candidates(panels, _real_ssocr_executable())
    result = signin_ocr._select_consensus_candidate(readings)
    assert (result.time_text if result else None) == expected


def test_real_ssocr_rejects_two_different_displays() -> None:
    image = np.vstack([_synthetic_display("12:34:56"), _synthetic_display("07:08:09")])
    panels = signin_ocr._display_candidates(image)
    assert len(panels) == 2
    readings = signin_ocr._recognize_ssocr_candidates(panels, _real_ssocr_executable())
    assert {item.time_text for item in readings} == {"12:34:56", "07:08:09"}
    assert signin_ocr._select_consensus_candidate(readings) is None


def test_rapidocr_fallback_needs_two_matching_views(monkeypatch) -> None:
    monkeypatch.setattr(signin_ocr, "_get_rapid_ocr",
                        lambda: lambda _: ([(None, "12:34:56", 0.93)], None))
    panels = signin_ocr._display_candidates(_synthetic_display())
    readings = signin_ocr._recognize_ocr_candidates(panels)
    assert len(readings) == 2
    assert signin_ocr._select_consensus_candidate(readings).time_text == "12:34:56"


def test_rapidocr_fallback_rejects_weak_or_embedded_text(monkeypatch) -> None:
    panels = signin_ocr._display_candidates(_synthetic_display())
    for text, confidence in [("12:34:56", 0.6), ("time 12:34:56", 0.99), ("12:34:56", float("nan"))]:
        monkeypatch.setattr(signin_ocr, "_get_rapid_ocr",
                            lambda: lambda _: ([(None, text, confidence)], None))
        assert signin_ocr._recognize_ocr_candidates(panels) == []


def test_iter_images_does_not_recognize_generated_previews(tmp_path) -> None:
    for name in ("original.jpg", "original.signin_debug.png", "signin_ocr_contact.png", "note.txt"):
        (tmp_path / name).touch()
    assert [p.name for p in signin_ocr._iter_images(tmp_path)] == ["original.jpg"]


def test_one_ratio_adapts_to_wide_glyphs_but_keeps_tall_font() -> None:
    for width, expected in ((90, 2), (80, 3), (50, 3), (12, 3)):
        mask = np.zeros((140, 600), np.uint8)
        for x in range(10, 600 - width, 95):
            mask[10:130, x:x + width] = 255
        assert signin_ocr._ssocr_one_ratio(mask) == expected


@pytest.mark.parametrize("text,code,expected", [
    ("10220_", 2, "10:22"), ("1022__", 2, "10:22"), ("102299", 0, "10:22"),
    ("102205", 0, "10:22:05"), ("_02205", 2, None), ("10_205", 2, None),
    ("2460__", 2, None), ("1060__", 2, None), ("10220_", 1, None),
    ("1022", 0, None), ("1022000", 0, None), ("102205", 2, None),
    ("message 10220_", 2, None), ("1022?_", 2, None),
])
def test_partial_reading_requires_six_positions_and_valid_prefix(text, code, expected) -> None:
    assert signin_ocr._parse_ssocr_reading(text, code) == expected


def test_minute_consensus_preserves_precision_and_rejects_minute_rollover() -> None:
    select = signin_ocr._select_consensus_candidate
    result = select([_reading("10:22"), _reading("10:22")])
    assert result.time_text == "10:22" and "minute" in result.reason
    assert select([_reading("10:22"), _reading("10:22:03")]).time_text == "10:22"
    assert select([_reading("10:22"), _reading("10:22:03"), _reading("10:22:03")]).time_text == "10:22:03"
    assert select([_reading("10:22")]) is None
    assert select([_reading("10:22"), _reading("10:23")]) is None
    assert select([_reading("10:22"), _reading("10:22"), _reading("10:23:00")]) is None
    assert select([_reading("10:22"), _reading("10:22", 1)]) is None


def test_blurred_panel_retries_only_after_incomplete_consensus(monkeypatch) -> None:
    mask = np.zeros((140, 500), np.uint8)
    monkeypatch.setattr(signin_ocr, "_find_ssocr_executable", lambda: "ssocr")
    monkeypatch.setattr(signin_ocr, "_ssocr_mask_variants",
                        lambda crop, levels=(.4, .6, .8): [(str(level), mask) for level in levels])
    answers = iter([None, None, "10:21:50", "10:21", "10:21:50"])
    monkeypatch.setattr(signin_ocr, "_run_ssocr", lambda *_: next(answers))
    panels = signin_ocr._display_candidates(_synthetic_display())
    readings = signin_ocr._recognize_ssocr_candidates(panels)
    assert len(readings) == 3
    assert signin_ocr._select_consensus_candidate(readings).time_text == "10:21:50"


def test_real_ssocr_switching_last_digit_returns_minutes() -> None:
    image = _synthetic_display("10:22:02")
    # Overlay the lower-right segment of 3 on 2: an impossible transition glyph.
    cv2.rectangle(image, (494, 172), (502, 210), (10, 30, 250), -1)
    panels = signin_ocr._display_candidates(image)
    readings = signin_ocr._recognize_ssocr_candidates(panels, _real_ssocr_executable())
    result = signin_ocr._select_consensus_candidate(readings)
    assert result is not None
    assert result.time_text == "10:22"


@pytest.mark.parametrize("filename,expected", [
    ("IMG_20260917_105737.jpg", "10:57:39"),
    ("IMG_20260917_105748.jpg", "10:57:50"),
])
def test_real_ssocr_low_threshold_outlier_photos(monkeypatch, filename, expected) -> None:
    path = signin_ocr.Path(__file__).resolve().parents[1] / "ocr" / filename
    if not path.is_file():
        pytest.skip("local OCR photo not present")
    executable = _real_ssocr_executable()
    monkeypatch.setattr(signin_ocr, "_find_ssocr_executable", lambda: executable)
    result = recognize_led_time_from_path(path)
    assert result.time_text == expected
    assert result.source == "visual"
