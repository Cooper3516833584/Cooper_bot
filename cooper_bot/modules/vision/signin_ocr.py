from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from pathlib import Path
import argparse
import re
import shutil
import subprocess
import sys
import threading
import time as time_module
from typing import Iterable, Optional

import cv2
import numpy as np
from PIL import Image, ExifTags


@dataclass(frozen=True)
class SigninOcrResult:
    time_text: Optional[str]
    confidence: float
    source: str
    reason: str = ""
    visual_time_text: Optional[str] = None
    timestamp_time_text: Optional[str] = None
    visual_confidence: float = 0.0

    @property
    def parsed_time(self) -> Optional[time]:
        if not self.time_text:
            return None
        m = re.fullmatch(r"(\d{2}):(\d{2})(?::(\d{2}))?", self.time_text)
        if not m:
            return None
        hh = int(m.group(1))
        mm = int(m.group(2))
        ss = int(m.group(3) or 0)
        if not (0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60):
            return None
        return time(hh, mm, ss)


@dataclass(frozen=True)
class _ParsedCandidate:
    time_text: str
    confidence: float
    score: float
    reason: str = ""
    candidate_id: int = 0


def recognize_led_time_from_path(path: str | Path) -> SigninOcrResult:
    p = Path(path)
    img = cv2.imread(str(p))
    if img is None or getattr(img, "size", 0) == 0:
        return SigninOcrResult(None, 0.0, "none", "image_read_failed")

    visual = _recognize_visual_time(img)
    stamp_text = _read_image_timestamp_time(p)
    if visual.time_text:
        return SigninOcrResult(
            visual.time_text,
            visual.confidence,
            "visual",
            visual.reason,
            visual_time_text=visual.time_text,
            timestamp_time_text=stamp_text,
            visual_confidence=visual.confidence,
        )
    return SigninOcrResult(
        None,
        visual.confidence,
        "visual",
        visual.reason,
        visual_time_text=None,
        timestamp_time_text=stamp_text,
        visual_confidence=visual.confidence,
    )


_WSL_SSOCR_LOCK = threading.Lock()
_WSL_SSOCR_CACHE: tuple[str, float, Optional[tuple[str, ...]]] = ("", 0.0, None)


def _find_ssocr_executable() -> str | tuple[str, ...] | None:
    native = shutil.which("ssocr") or shutil.which("ssocr.exe")
    if native or sys.platform != "win32":
        return native
    wsl = shutil.which("wsl.exe")
    if not wsl:
        return None
    return _find_wsl_ssocr(wsl)


def _find_wsl_ssocr(wsl: str) -> Optional[tuple[str, ...]]:
    global _WSL_SSOCR_CACHE
    with _WSL_SSOCR_LOCK:
        cached_path, expires, command = _WSL_SSOCR_CACHE
        if cached_path == wsl and time_module.monotonic() < expires:
            return command
        # Bound the Linux process too, even if the Windows launcher is terminated.
        command = (wsl, "--exec", "timeout", "2s", "ssocr")
        try:
            completed = subprocess.run(
                [*command, "-V"], stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                timeout=8.0, check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            output = completed.stdout + completed.stderr
            if completed.returncode != 42 or b"Seven Segment Optical Character Recognition" not in output:
                command = None
        except (subprocess.TimeoutExpired, OSError, ValueError):
            command = None
        # Cache failures as well; unavailable WSL must not stall every request.
        _WSL_SSOCR_CACHE = (wsl, time_module.monotonic() + 60.0, command)
        return command


def _parse_ssocr_time_text(text: str) -> Optional[str]:
    raw = str(text or "").strip()
    if not re.fullmatch(r"[0-9:.\s]+", raw):
        return None
    digits = "".join(re.findall(r"[0-9]", raw))
    if len(digits) != 6 or not _valid_hhmmss_digits(digits):
        return None
    return f"{digits[:2]}:{digits[2:4]}:{digits[4:6]}"


def _parse_ssocr_reading(text: str, returncode: int) -> Optional[str]:
    raw = str(text or "").strip()
    if returncode == 0:
        complete = _parse_ssocr_time_text(raw)
        if complete:
            return complete
    # Exit 2 means six positions were segmented but a digit is unknown.
    # Keep the prefix only when both hour/minute pairs are intact and legal.
    if not re.fullmatch(r"[0-9]{4}[0-9_]{2}", raw):
        return None
    if not (returncode == 2 and "_" in raw[4:] or returncode == 0 and raw.isdigit() and int(raw[4:]) >= 60):
        return None
    if int(raw[:2]) >= 24 or int(raw[2:4]) >= 60:
        return None
    return f"{raw[:2]}:{raw[2:4]}"


def _ssocr_one_ratio(mask: np.ndarray) -> int:
    occupied = np.any(mask > 0, axis=0).astype(np.int8)
    edges = np.diff(np.pad(occupied, (1, 1)))
    ratios = []
    for left, right in zip(np.where(edges == 1)[0], np.where(edges == -1)[0]):
        ys = np.where(mask[:, left:right] > 0)[0]
        height = int(np.ptp(ys)) + 1
        if height >= 70:
            ratios.append(height / (right - left))
    # Adapt to wide, blooming displays without labelling every tall glyph as 1.
    if not ratios:
        return 3
    estimate = 1.35 * float(np.median(sorted(ratios)[:3]))
    # ssocr expects an integer here; a fractional string is silently truncated.
    return int(np.ceil(min(3.0, max(2.0, estimate))))


def _run_ssocr(mask: np.ndarray, executable: str | tuple[str, ...]) -> Optional[str]:
    if not isinstance(mask, np.ndarray) or mask.dtype != np.uint8 or mask.ndim != 2 or mask.size == 0:
        return None
    try:
        ok, encoded = cv2.imencode(".png", mask)
    except cv2.error:
        return None
    if not ok:
        return None

    args = [
        *([executable] if isinstance(executable, str) else executable),
        "-d", "6",
        "-c", "digits",
        "-f", "white",
        "-b", "black",
        "-a",
        "-t", "50",
        "-r", str(_ssocr_one_ratio(mask)),
        "-n", "5",
        "-i", "2",
        "-M", "2x70",
        "-C",
        "-",
    ]
    try:
        completed = subprocess.run(
            args,
            input=encoded.tobytes(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=3.0 if isinstance(executable, tuple) else 2.0,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except (subprocess.TimeoutExpired, OSError, ValueError):
        return None
    if completed.returncode not in (0, 2):
        return None
    stdout = completed.stdout
    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8", errors="replace")
    return _parse_ssocr_reading(str(stdout), completed.returncode)


@dataclass(frozen=True)
class _DisplayCandidate:
    score: float
    quad: np.ndarray
    crop: np.ndarray


def _display_candidates(img: np.ndarray) -> list[_DisplayCandidate]:
    """Find complete dark display frames, not arbitrary groups of red pixels."""
    scale = min(1.0, 1800.0 / max(img.shape[:2]))
    small = cv2.resize(img, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)
    gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
    hsv = cv2.cvtColor(small, cv2.COLOR_BGR2HSV)
    red = ((hsv[:, :, 0] < 15) | (hsv[:, :, 0] > 165)) & (hsv[:, :, 1] > 100) & (hsv[:, :, 2] > 120)
    ranked: list[tuple[float, np.ndarray]] = []
    for threshold in (45, 75, 105, 135):
        dark = (gray < threshold).astype(np.uint8) * 255
        dark = cv2.morphologyEx(dark, cv2.MORPH_CLOSE, np.ones((3, 3), np.uint8))
        contours, _ = cv2.findContours(dark, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        for contour in contours:
            rect = cv2.minAreaRect(contour)
            short, long = sorted(rect[1])
            # A foreground obstruction (cable, bracket) can make the detected
            # frame look squat; red-density and fill checks still reject clutter.
            if short < 7 or long < 35 or not 1.8 < long / short < 9:
                continue
            area = long * short
            if area > gray.size * 0.85:
                continue
            fill = cv2.contourArea(contour) / area
            if fill < 0.72:
                continue
            x, y, w, h = cv2.boundingRect(contour)
            density = np.count_nonzero(red[y:y + h, x:x + w]) / area
            if not 0.015 < density < 0.5:
                continue
            ranked.append((2.0 * fill + 10.0 * min(density, 0.18), cv2.boxPoints(rect) / scale))

    selected: list[_DisplayCandidate] = []
    for score, quad in sorted(ranked, key=lambda item: item[0], reverse=True):
        center = quad.mean(axis=0)
        if any(np.linalg.norm(center - old.quad.mean(axis=0)) < 0.2 * np.linalg.norm(quad[0] - quad[2])
               for old in selected):
            continue
        crop = _rectify_display(img, quad)
        if crop.size:
            selected.append(_DisplayCandidate(float(score), quad, crop))
        if len(selected) >= 4:
            break
    return selected


def _rectify_display(img: np.ndarray, quad: np.ndarray) -> np.ndarray:
    points = sorted(quad, key=lambda point: point[0])
    left = sorted(points[:2], key=lambda point: point[1])
    right = sorted(points[2:], key=lambda point: point[1])
    points = np.array([left[0], right[0], right[1], left[1]], np.float32)
    width = max(2, round(max(np.linalg.norm(points[0] - points[1]), np.linalg.norm(points[2] - points[3]))))
    height = max(2, round(max(np.linalg.norm(points[0] - points[3]), np.linalg.norm(points[1] - points[2]))))
    target = np.array([[0, 0], [width - 1, 0], [width - 1, height - 1], [0, height - 1]], np.float32)
    transform = cv2.getPerspectiveTransform(points, target)
    return cv2.warpPerspective(img, transform, (width, height))


def _prepare_ssocr_mask(mask: np.ndarray) -> Optional[np.ndarray]:
    if not isinstance(mask, np.ndarray) or mask.ndim != 2 or mask.size == 0:
        return None
    ys, xs = np.where(mask > 0)
    if len(xs) < 20:
        return None
    trimmed = np.where(mask[ys.min():ys.max() + 1, xs.min():xs.max() + 1] > 0, 255, 0).astype(np.uint8)
    if not 1.5 <= trimmed.shape[1] / trimmed.shape[0] <= 12.0:
        return None
    scale = 120.0 / trimmed.shape[0]
    # A fixed height makes stroke thickness and ssocr's pixel thresholds scale independent.
    normalized = cv2.resize(trimmed, None, fx=scale, fy=scale, interpolation=cv2.INTER_NEAREST)
    return cv2.copyMakeBorder(normalized, 10, 10, 10, 10, cv2.BORDER_CONSTANT, value=0)


def _clean_ssocr_mask(mask: np.ndarray) -> np.ndarray:
    h, w = mask.shape
    slopes: list[float] = []
    count, labels, stats, _ = cv2.connectedComponentsWithStats(mask, 8)
    for index in range(1, count):
        _x, _y, cw, ch, _area = stats[index]
        if ch > h * 0.6 and cw / ch < 0.4:
            ys, xs = np.where(labels == index)
            slope = float(np.cov(xs, ys)[0, 1] / np.var(ys))
            if abs(slope) < 0.4:
                slopes.extend([slope] * 3)
    lines = cv2.HoughLinesP(
        cv2.Canny(mask, 50, 150), 1, np.pi / 180,
        threshold=max(10, round(h * 0.2)), minLineLength=h * 0.35, maxLineGap=h * 0.08,
    )
    if lines is not None:
        for x1, y1, x2, y2 in lines[:, 0, :]:
            if abs(y2 - y1) > h * 0.35 and abs((x2 - x1) / (y2 - y1)) < 0.4:
                slopes.append(float((x2 - x1) / (y2 - y1)))
    shear = -float(np.median(slopes)) if len(slopes) >= 3 else 0.0
    transform = np.array([[1, shear, max(0, -shear * h) + 4], [0, 1, 0]], np.float32)
    mask = cv2.warpAffine(mask, transform, (w + round(abs(shear * h)) + 8, h), flags=cv2.INTER_NEAREST)

    # Deflate only unusually thick light bloom; thin/dim segments must survive.
    radius = float(cv2.distanceTransform(mask, cv2.DIST_L2, 5).max())
    if radius > 12:
        size = max(3, 2 * round((radius - 10.8) / 2) + 1)
        mask = cv2.erode(mask, np.ones((size, size), np.uint8))
    return _remove_colon_dots(mask)


def _remove_colon_dots(mask: np.ndarray) -> np.ndarray:
    # ssocr -C skips decimal points, not the two vertically aligned colon dots.
    mask = mask.copy()
    count, labels, stats, centers = cv2.connectedComponentsWithStats(mask, 8)
    dots = []
    for index in range(1, count):
        _x, _y, w, h, _area = stats[index]
        if 4 <= min(w, h) and max(w, h) < 36 and 0.45 < w / h < 2.2:
            dots.append(index)
    for index in dots:
        if any(index != other and abs(centers[index, 0] - centers[other, 0]) < 15
               and 24 < abs(centers[index, 1] - centers[other, 1]) < 96 for other in dots):
            mask[labels == index] = 0
    return mask


def _ssocr_mask_variants(
    crop: np.ndarray, *, levels: tuple[float, ...] = (0.4, 0.6, 0.8),
) -> list[tuple[str, np.ndarray]]:
    if not isinstance(crop, np.ndarray) or crop.ndim != 3 or crop.size == 0:
        return []
    h, w = crop.shape[:2]
    # Work inside the dark frame. Red-channel intensity retains unsaturated LED
    # cores and dim rolling-shutter segments that HSV/grayscale masks discard.
    red = crop[round(h * 0.12):round(h * 0.88), round(w * 0.04):round(w * 0.96), 2]
    if red.size == 0 or int(red.max()) - int(red.min()) < 30:
        return []
    threshold, _ = cv2.threshold(red, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    peak = float(np.percentile(red, 95))
    variants: list[tuple[str, np.ndarray]] = []
    for alpha in levels:
        level = threshold + max(0.0, peak - threshold) * alpha
        prepared = _prepare_ssocr_mask((red > level).astype(np.uint8) * 255)
        if prepared is not None:
            variants.append((f"ssocr_red_{alpha:.1f}", _clean_ssocr_mask(prepared)))
    return variants


def _recognize_ssocr_candidates(
    panels: list[_DisplayCandidate], executable: str | tuple[str, ...] | None = None,
) -> list[_ParsedCandidate]:
    executable = executable or _find_ssocr_executable()
    if not executable:
        return []
    candidates: list[_ParsedCandidate] = []
    for index, panel in enumerate(panels[:4]):
        panel_readings: list[_ParsedCandidate] = []
        for name, mask in _ssocr_mask_variants(panel.crop):
            parsed = _run_ssocr(mask, executable)
            if parsed:
                panel_readings.append(_ParsedCandidate(parsed, 0.86 if len(parsed) == 5 else 0.92, panel.score, name, index))
        # High thresholds separate blurred strokes. Only retry an undecided panel.
        if _select_consensus_candidate(panel_readings) is None:
            for name, mask in _ssocr_mask_variants(panel.crop, levels=(0.7, 0.9)):
                parsed = _run_ssocr(mask, executable)
                if parsed:
                    panel_readings.append(_ParsedCandidate(parsed, 0.86 if len(parsed) == 5 else 0.92, panel.score, name, index))
        candidates.extend(panel_readings)
    return candidates


def _stable_ssocr_group(group: list[_ParsedCandidate]) -> list[_ParsedCandidate]:
    # Bloom/colon remnants can corrupt a low-threshold reading. Require three
    # distinct higher thresholds to agree before discarding a single outlier.
    # Keep seconds disagreements/partial readings for the minute fallback.
    if (len(group) < 4 or any(len(item.time_text) != 8 for item in group)
            or len({item.time_text[6:] for item in group}) != 1):
        return group
    levels = []
    for item in group:
        match = re.fullmatch(r"ssocr_red_(0\.\d+)", item.reason)
        if not match:
            return group
        levels.append(float(match.group(1)))
    if len(set(levels)) != len(levels):
        return group
    ordered = [item for _, item in sorted(zip(levels, group), key=lambda pair: pair[0])]
    stable = ordered[1:]
    if (len({item.time_text for item in stable}) == 1
            and ordered[0].time_text != stable[0].time_text):
        return stable
    return group


def _select_consensus_candidate(candidates: list[_ParsedCandidate]) -> Optional[_ParsedCandidate]:
    # Do not resolve disagreements by a geometric score or by a filename timestamp.
    if not candidates:
        return None
    groups: dict[int, list[_ParsedCandidate]] = {}
    for candidate in candidates:
        groups.setdefault(candidate.candidate_id, []).append(candidate)
    groups = {index: _stable_ssocr_group(group) for index, group in groups.items()}
    candidates = [item for group in groups.values() for item in group]
    if len({item.time_text[:5] for item in candidates}) != 1:
        return None
    agreed = [group for group in groups.values() if len(group) >= 2]
    if not agreed:
        return None
    full_texts = {item.time_text for item in candidates if len(item.time_text) == 8}
    # Two separate clocks showing different seconds are still ambiguous.
    if len(groups) > 1 and len(full_texts) > 1:
        return None
    if len(full_texts) == 1:
        full_groups = [[item for item in group if len(item.time_text) == 8] for group in agreed]
        full_groups = [group for group in full_groups if len(group) >= 2]
        if full_groups:
            best = max((item for group in full_groups for item in group), key=lambda item: item.score)
            return _ParsedCandidate(best.time_text, best.confidence, best.score, best.reason + "_consensus", best.candidate_id)
    best = max((item for group in agreed for item in group), key=lambda item: item.score)
    return _ParsedCandidate(best.time_text[:5], min(0.86, best.confidence), best.score,
                            best.reason + "_minute_consensus", best.candidate_id)


def _recognize_ocr_candidates(panels: list[_DisplayCandidate]) -> list[_ParsedCandidate]:
    if not panels:
        return []
    engine = _get_rapid_ocr()
    if engine is None:
        return []
    candidates: list[_ParsedCandidate] = []
    for index, panel in enumerate(panels[:4]):
        gray = cv2.cvtColor(panel.crop, cv2.COLOR_BGR2GRAY)
        for name, variant in (
            ("rapidocr_crop", panel.crop),
            ("rapidocr_crop_eq", cv2.cvtColor(cv2.equalizeHist(gray), cv2.COLOR_GRAY2BGR)),
        ):
            scale = min(4.0, 160.0 / variant.shape[0])
            resized = cv2.resize(variant, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
            try:
                result, _ = engine(resized)
            except Exception:
                continue
            parsed_rows: list[tuple[str, float]] = []
            for _points, text, conf_text in result or []:
                # A display must contain one complete time, not a time substring in text.
                if not re.fullmatch(r"[0-9:：.\s]+", str(text)):
                    continue
                parsed = _parse_ocr_time_text(str(text))
                try:
                    confidence = float(conf_text)
                except (TypeError, ValueError):
                    continue
                if parsed and confidence >= 0.85:
                    parsed_rows.append((parsed, confidence))
            if len(parsed_rows) == 1:
                parsed, confidence = parsed_rows[0]
                candidates.append(_ParsedCandidate(parsed, min(0.88, confidence), panel.score, name, index))
    return candidates


def _recognize_visual_time(img: np.ndarray) -> SigninOcrResult:
    panels = _display_candidates(img)
    candidates = _recognize_ssocr_candidates(panels)
    # A contradictory/single ssocr reading is not rescued by a weaker OCR guess.
    if not candidates:
        candidates = _recognize_ocr_candidates(panels)
    best = _select_consensus_candidate(candidates)
    if best:
        return SigninOcrResult(best.time_text, best.confidence, "visual", best.reason)
    reason = "no_valid_led_time"
    if candidates:
        reason = "ambiguous_led_time" if len({item.time_text for item in candidates}) > 1 else "insufficient_led_consensus"
    return SigninOcrResult(None, 0.0, "visual", reason)


_RAPID_OCR = None
_RAPID_OCR_FAILED = False


def _get_rapid_ocr():
    global _RAPID_OCR, _RAPID_OCR_FAILED
    if _RAPID_OCR_FAILED:
        return None
    if _RAPID_OCR is None:
        try:
            from rapidocr_onnxruntime import RapidOCR
        except Exception:
            _RAPID_OCR_FAILED = True
            return None
        try:
            _RAPID_OCR = RapidOCR()
        except Exception:
            _RAPID_OCR_FAILED = True
            return None
    return _RAPID_OCR


def _parse_ocr_time_text(text: str) -> Optional[str]:
    normalized = text.replace("：", ":").replace(".", ":")
    normalized = re.sub(r"\s+", "", normalized)
    if re.search(r"[A-Za-z]", normalized):
        return None

    patterns = (
        r"(?<!\d)(\d{1,2}):(\d{2}):(\d{2})(?!\d)",
        r"(?<!\d)(\d{1,2}):(\d{2})(\d{2})(?!\d)",
        r"(?<!\d)(\d{6})(?!\d)",
        r"(?<!\d)(\d{5})(?!\d)",
    )
    for pattern in patterns:
        m = re.search(pattern, normalized)
        if not m:
            continue
        if len(m.groups()) == 1:
            digits = m.group(1)
            if len(digits) == 5:
                hh, mm, ss = int(digits[0]), int(digits[1:3]), int(digits[3:5])
            else:
                hh, mm, ss = int(digits[0:2]), int(digits[2:4]), int(digits[4:6])
        else:
            hh, mm, ss = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60:
            return f"{hh:02d}:{mm:02d}:{ss:02d}"
    return None


def _valid_hhmmss_digits(s: str) -> bool:
    if not re.fullmatch(r"\d{6}", s):
        return False
    hh = int(s[0:2])
    mm = int(s[2:4])
    ss = int(s[4:6])
    return 0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60


def _read_image_timestamp_time(path: Path) -> Optional[str]:
    dt = _read_exif_datetime(path)
    if not dt:
        m = re.search(r"_(\d{6})(?:\D|$)", path.name)
        if m:
            dt = m.group(1)
    if not dt:
        try:
            dt = time_module.strftime("%H%M%S", time_module.localtime(path.stat().st_mtime))
        except Exception:
            dt = None
    if not dt:
        return None

    m = re.search(r"(\d{2}):?(\d{2}):?(\d{2})$", dt.strip())
    if not m:
        return None
    hh, mm, ss = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    if not (0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60):
        return None
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def _read_exif_datetime(path: Path) -> Optional[str]:
    try:
        img = Image.open(path)
        exif = img.getexif()
    except Exception:
        return None
    if not exif:
        return None
    names = {ExifTags.TAGS.get(k, k): v for k, v in exif.items()}
    val = names.get("DateTimeOriginal") or names.get("DateTime")
    if not val:
        return None
    return str(val)


def _iter_images(path: Path) -> Iterable[Path]:
    if path.is_file():
        yield path
        return
    for p in sorted(path.iterdir()):
        if not p.is_file() or p.name.endswith(".signin_debug.png") or p.name == "signin_ocr_contact.png":
            continue
        if p.suffix.lower() in {".jpg", ".jpeg", ".png", ".bmp", ".webp"}:
            yield p


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", nargs="?", default="ocr")
    args = parser.parse_args()
    for p in _iter_images(Path(args.path)):
        res = recognize_led_time_from_path(p)
        text = res.time_text or "<none>"
        print(f"{p.name}\t{text}\t{res.source}\t{res.confidence:.2f}\t{res.reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
