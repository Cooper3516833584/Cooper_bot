from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from pathlib import Path
import argparse
import math
import re
import shutil
import subprocess
import time as time_module
from typing import Iterable, Optional

import cv2
import numpy as np
from PIL import Image, ExifTags


_RED_THRESHOLDS = (
    (120, 100),
    (80, 80),
    (60, 60),
)

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


def _find_ssocr_executable() -> Optional[str]:
    return shutil.which("ssocr") or shutil.which("ssocr.exe")


def _parse_ssocr_time_text(text: str) -> Optional[str]:
    digits = "".join(re.findall(r"\d", str(text or "").strip()))
    if len(digits) != 6 or not _valid_hhmmss_digits(digits):
        return None
    return f"{digits[:2]}:{digits[2:4]}:{digits[4:6]}"


def _run_ssocr(mask: np.ndarray, executable: str) -> Optional[str]:
    if not isinstance(mask, np.ndarray) or mask.dtype != np.uint8 or mask.ndim != 2 or mask.size == 0:
        return None
    try:
        ok, encoded = cv2.imencode(".png", mask)
    except cv2.error:
        return None
    if not ok:
        return None

    args = [
        executable,
        "-d", "6",
        "-c", "digits",
        "-f", "white",
        "-b", "black",
        "-a",
        "-t", "50",
        "-C",
        "-",
    ]
    try:
        completed = subprocess.run(
            args,
            input=encoded.tobytes(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=2.0,
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError, ValueError):
        return None
    if completed.returncode != 0:
        return None
    stdout = completed.stdout
    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8", errors="replace")
    return _parse_ssocr_time_text(str(stdout))


def _red_mask_hsv(img: np.ndarray, sat_min: int, val_min: int) -> np.ndarray:
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    lower1 = np.array([0, sat_min, val_min], dtype=np.uint8)
    upper1 = np.array([12, 255, 255], dtype=np.uint8)
    lower2 = np.array([165, sat_min, val_min], dtype=np.uint8)
    upper2 = np.array([180, 255, 255], dtype=np.uint8)
    return cv2.bitwise_or(cv2.inRange(hsv, lower1, upper1), cv2.inRange(hsv, lower2, upper2))


def _has_led_red_candidate(img: np.ndarray) -> bool:
    for sat_min, val_min in _RED_THRESHOLDS:
        mask = _red_mask_hsv(img, sat_min, val_min)
        for x, y, w, h in _candidate_boxes_from_mask(mask):
            if w >= 80 and h >= 25:
                return True
    return False


def _recognize_visual_time(img: np.ndarray) -> SigninOcrResult:
    panel_ocr_boxes: list[tuple[float, tuple[int, int, int, int]]] = []
    red_ocr_boxes: list[tuple[float, tuple[int, int, int, int]]] = []
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    panel_mask = _red_mask_hsv(img, 80, 80)

    panel_boxes = _black_panel_frame_candidate_boxes(img, gray, panel_mask)[:3]
    for panel_score, panel_box in panel_boxes:
        panel_ocr_boxes.append((panel_score + 1.2, panel_box))

    for sat_min, val_min in _RED_THRESHOLDS:
        mask = _red_mask_hsv(img, sat_min, val_min)
        for box in _candidate_boxes_from_mask(mask):
            box_score = _score_candidate_box(img, gray, mask, box)
            if box_score >= 1.15:
                red_ocr_boxes.append((box_score, box))

    ssocr_boxes = panel_ocr_boxes[:2] + red_ocr_boxes[:4]
    best = _select_consensus_candidate(_recognize_ssocr_candidates(img, ssocr_boxes))
    if best is not None:
        return SigninOcrResult(best.time_text, best.confidence, "visual", best.reason)

    ocr_boxes = panel_ocr_boxes + red_ocr_boxes
    if ocr_boxes:
        best = _select_consensus_candidate(_recognize_ocr_candidates(img, ocr_boxes))

    if best is not None:
        return SigninOcrResult(best.time_text, best.confidence, "visual", best.reason)
    return SigninOcrResult(None, 0.0, "visual", "no_valid_led_time")


def _select_consensus_candidate(candidates: list[_ParsedCandidate]) -> Optional[_ParsedCandidate]:
    best: Optional[_ParsedCandidate] = None
    by_text: dict[str, list[_ParsedCandidate]] = {}
    for candidate in candidates:
        by_text.setdefault(candidate.time_text, []).append(candidate)

    for time_text, group in by_text.items():
        top = max(group, key=lambda item: item.score)
        adjusted_score = top.score + min(1.60, 0.40 * (len(group) - 1))
        adjusted_confidence = max(top.confidence, _score_to_confidence(adjusted_score))
        adjusted = _ParsedCandidate(time_text, adjusted_confidence, adjusted_score, top.reason)
        if best is None or adjusted.score > best.score:
            best = adjusted
    return best


def _score_candidate_box(
    img: np.ndarray,
    gray: np.ndarray,
    mask: np.ndarray,
    box: tuple[int, int, int, int],
) -> float:
    x, y, w, h = box
    area = max(w * h, 1)
    red_density = float(cv2.countNonZero(mask[y : y + h, x : x + w])) / float(area)
    dark_ratio = float((gray[y : y + h, x : x + w] < 85).mean())
    aspect = float(w) / float(max(h, 1))
    aspect_score = math.exp(-((aspect - 3.6) / 2.5) ** 2)
    size_ratio = max(float(w) / float(img.shape[1]), float(h) / float(img.shape[0]))
    return red_density * 4.0 + dark_ratio * 0.9 + aspect_score * 0.8 - max(0.0, size_ratio - 0.55) * 1.8


def _candidate_boxes_from_mask(mask: np.ndarray) -> list[tuple[int, int, int, int]]:
    boxes: list[tuple[int, int, int, int]] = []
    h_img, w_img = mask.shape[:2]

    for kernel_w, kernel_h in ((55, 25), (95, 35), (150, 45)):
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (kernel_w, kernel_h))
        merged = cv2.dilate(mask, kernel, iterations=1)
        contours, _ = cv2.findContours(merged, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        for cnt in contours:
            x, y, w, h = cv2.boundingRect(cnt)
            if w < 70 or h < 25:
                continue
            aspect = w / max(float(h), 1.0)
            if not (1.2 <= aspect <= 9.0):
                continue
            red_area = int(cv2.countNonZero(mask[y : y + h, x : x + w]))
            if red_area < 250:
                continue
            boxes.append(_expand_box((x, y, w, h), w_img, h_img, 0.08))

    boxes.extend(_row_group_boxes(mask))
    return _dedupe_boxes(boxes)


def _row_group_boxes(mask: np.ndarray) -> list[tuple[int, int, int, int]]:
    count, _labels, stats, centroids = cv2.connectedComponentsWithStats(mask, 8)
    comps = []
    for i in range(1, count):
        x, y, w, h, area = [int(v) for v in stats[i]]
        if area < 50 or h < 15 or w < 5:
            continue
        comps.append((x, y, w, h, area, float(centroids[i][0]), float(centroids[i][1])))

    boxes: list[tuple[int, int, int, int]] = []
    h_img, w_img = mask.shape[:2]
    for comp in comps:
        _x, _y, _w, h, _area, _cx, cy = comp
        group = [c for c in comps if abs(c[6] - cy) <= max(28.0, h * 0.75)]
        if len(group) < 3:
            continue
        x1 = min(c[0] for c in group)
        y1 = min(c[1] for c in group)
        x2 = max(c[0] + c[2] for c in group)
        y2 = max(c[1] + c[3] for c in group)
        if x2 - x1 < 70 or y2 - y1 < 25:
            continue
        boxes.append(_expand_box((x1, y1, x2 - x1, y2 - y1), w_img, h_img, 0.12))
    return boxes


def _black_panel_candidate_boxes(
    img: np.ndarray,
    gray: np.ndarray,
    red_mask: np.ndarray,
) -> list[tuple[float, tuple[int, int, int, int]]]:
    count, _labels, stats, centroids = cv2.connectedComponentsWithStats(red_mask, 8)
    comps = []
    for i in range(1, count):
        x, y, w, h, area = [int(v) for v in stats[i]]
        if area < 20 or h < 8 or w < 2:
            continue
        comps.append((x, y, w, h, area, float(centroids[i][0]), float(centroids[i][1])))

    h_img, w_img = red_mask.shape[:2]
    boxes: list[tuple[float, tuple[int, int, int, int]]] = []
    for comp in comps:
        _x, _y, _w, h, _area, _cx, cy = comp
        row = sorted([c for c in comps if abs(c[6] - cy) <= max(18.0, h * 0.90)], key=lambda c: c[0])
        if len(row) < 3:
            continue
        clusters: list[list[tuple[int, int, int, int, int, float, float]]] = []
        current: list[tuple[int, int, int, int, int, float, float]] = []
        for item in row:
            if current:
                prev = current[-1]
                gap = item[0] - (prev[0] + prev[2])
                max_h = max(c[3] for c in current)
                if gap > max(90, int(max_h * 4.0)):
                    clusters.append(current)
                    current = []
            current.append(item)
        if current:
            clusters.append(current)

        for group in clusters:
            if not (3 <= len(group) <= 16):
                continue
            x1 = min(c[0] for c in group)
            y1 = min(c[1] for c in group)
            x2 = max(c[0] + c[2] for c in group)
            y2 = max(c[1] + c[3] for c in group)
            span = x2 - x1
            height = y2 - y1
            if span < 80 or height < 18:
                continue
            aspect = float(span) / float(max(height, 1))
            if not (2.0 <= aspect <= 8.5):
                continue

            box = _expand_box((x1, y1, span, height), w_img, h_img, 0.35)
            bx, by, bw, bh = box
            if bw < 90 or bh < 25:
                continue
            roi = gray[by : by + bh, bx : bx + bw]
            dark_ratio = float((roi < 120).mean()) if roi.size else 0.0
            red_count = int(cv2.countNonZero(red_mask[by : by + bh, bx : bx + bw]))
            red_density = float(red_count) / float(max(bw * bh, 1))
            if red_count < 120 or dark_ratio < 0.18:
                continue
            rel_size = max(float(bw) / float(w_img), float(bh) / float(h_img))
            score = dark_ratio * 2.0 + red_density * 8.0 + math.exp(-((aspect - 3.4) / 2.8) ** 2)
            score -= max(0.0, rel_size - 0.45) * 3.5
            boxes.append((score, box))

    out: list[tuple[float, tuple[int, int, int, int]]] = []
    ranked = sorted(boxes, key=lambda item: item[0], reverse=True)
    compact = sorted(boxes, key=lambda item: item[1][2] * item[1][3])
    for score, box in ranked[:10] + compact[:14]:
        if any(_box_iou(box, old_box) >= 0.55 for _old_score, old_box in out):
            continue
        out.append((score, box))
        if len(out) >= 18:
            break
    return out


def _black_panel_frame_candidate_boxes(
    img: np.ndarray,
    gray: np.ndarray,
    red_mask: np.ndarray,
) -> list[tuple[float, tuple[int, int, int, int]]]:
    count, _labels, stats, centroids = cv2.connectedComponentsWithStats(red_mask, 8)
    comps = []
    for i in range(1, count):
        x, y, w, h, area = [int(v) for v in stats[i]]
        if area < 20 or h < 8 or w < 2:
            continue
        comps.append((x, y, w, h, area, float(centroids[i][0]), float(centroids[i][1])))

    h_img, w_img = red_mask.shape[:2]
    boxes: list[tuple[float, tuple[int, int, int, int]]] = []
    for comp in comps:
        _x, _y, _w, h, _area, _cx, cy = comp
        row = sorted([c for c in comps if abs(c[6] - cy) <= max(18.0, h * 0.90)], key=lambda c: c[0])
        if len(row) < 3:
            continue

        x1 = min(c[0] for c in row)
        y1 = min(c[1] for c in row)
        x2 = max(c[0] + c[2] for c in row)
        y2 = max(c[1] + c[3] for c in row)
        span = x2 - x1
        height = y2 - y1
        if span < 80 or height < 18:
            continue

        pad_x = int(round(span * 0.80))
        pad_y = int(round(height * 1.35))
        sx1 = max(0, x1 - pad_x)
        sy1 = max(0, y1 - pad_y)
        sx2 = min(w_img, x2 + pad_x)
        sy2 = min(h_img, y2 + pad_y)
        if sx2 <= sx1 or sy2 <= sy1:
            continue

        roi = gray[sy1:sy2, sx1:sx2]
        dark = cv2.inRange(roi, 0, 115)
        kernel_w = max(15, int(round(span * 0.08)))
        kernel_h = max(9, int(round(height * 0.45)))
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (kernel_w, kernel_h))
        dark = cv2.morphologyEx(dark, cv2.MORPH_CLOSE, kernel, iterations=1)
        contours, _ = cv2.findContours(dark, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        red_center_x = (x1 + x2) * 0.5
        red_center_y = (y1 + y2) * 0.5
        for cnt in contours:
            bx, by, bw, bh = cv2.boundingRect(cnt)
            bx += sx1
            by += sy1
            if bw < span * 1.05 or bh < height * 1.05:
                continue
            if not (bx <= red_center_x <= bx + bw and by <= red_center_y <= by + bh):
                continue
            aspect = float(bw) / float(max(bh, 1))
            if not (1.8 <= aspect <= 8.5):
                continue
            rel_size = max(float(bw) / float(w_img), float(bh) / float(h_img))
            if rel_size > 0.60:
                continue

            panel_gray = gray[by : by + bh, bx : bx + bw]
            panel_red = red_mask[by : by + bh, bx : bx + bw]
            dark_ratio = float((panel_gray < 115).mean()) if panel_gray.size else 0.0
            red_count = int(cv2.countNonZero(panel_red))
            red_density = float(red_count) / float(max(bw * bh, 1))
            if dark_ratio < 0.28 or red_count < 120:
                continue

            score = dark_ratio * 2.8 + red_density * 10.0
            score += math.exp(-((aspect - 4.6) / 2.6) ** 2) * 1.2
            score -= max(0.0, rel_size - 0.45) * 3.0
            boxes.append((score + 0.8, _expand_box((bx, by, bw, bh), w_img, h_img, 0.22)))

    out: list[tuple[float, tuple[int, int, int, int]]] = []
    for score, box in sorted(boxes, key=lambda item: item[0], reverse=True):
        if any(_box_iou(box, old_box) >= 0.55 for _old_score, old_box in out):
            continue
        out.append((score, box))
        if len(out) >= 8:
            break
    return out


def _expand_box(box: tuple[int, int, int, int], img_w: int, img_h: int, ratio: float) -> tuple[int, int, int, int]:
    x, y, w, h = box
    px = int(round(w * ratio))
    py = int(round(h * ratio))
    x1 = max(0, x - px)
    y1 = max(0, y - py)
    x2 = min(img_w, x + w + px)
    y2 = min(img_h, y + h + py)
    return x1, y1, max(0, x2 - x1), max(0, y2 - y1)


def _dedupe_boxes(boxes: Iterable[tuple[int, int, int, int]]) -> list[tuple[int, int, int, int]]:
    out: list[tuple[int, int, int, int]] = []
    for box in sorted(boxes, key=lambda b: b[2] * b[3], reverse=True):
        if any(_box_iou(box, old) >= 0.72 for old in out):
            continue
        out.append(box)
    return out[:8]


def _box_iou(a: tuple[int, int, int, int], b: tuple[int, int, int, int]) -> float:
    ax1, ay1, aw, ah = a
    bx1, by1, bw, bh = b
    ax2, ay2 = ax1 + aw, ay1 + ah
    bx2, by2 = bx1 + bw, by1 + bh
    ix1, iy1 = max(ax1, bx1), max(ay1, by1)
    ix2, iy2 = min(ax2, bx2), min(ay2, by2)
    iw, ih = max(0, ix2 - ix1), max(0, iy2 - iy1)
    inter = iw * ih
    union = aw * ah + bw * bh - inter
    return float(inter) / float(union or 1)


def _trim_red_crop(crop: np.ndarray, pad_ratio: float = 0.08) -> np.ndarray:
    ys, xs = np.where(crop > 0)
    if len(xs) < 20:
        return crop
    h, w = crop.shape[:2]
    x1, x2 = int(xs.min()), int(xs.max()) + 1
    y1, y2 = int(ys.min()), int(ys.max()) + 1
    pad_x = max(1, int(round((x2 - x1) * pad_ratio)))
    pad_y = max(1, int(round((y2 - y1) * pad_ratio)))
    x1 = max(0, x1 - pad_x)
    x2 = min(w, x2 + pad_x)
    y1 = max(0, y1 - pad_y)
    y2 = min(h, y2 + pad_y)
    return crop[y1:y2, x1:x2]


def _deskew_red_crop(crop: np.ndarray) -> np.ndarray:
    ys, xs = np.where(crop > 0)
    if len(xs) < 20:
        return crop
    pts = np.column_stack([xs, ys]).astype(np.float32)
    rect = cv2.minAreaRect(pts)
    (cx, cy), (w, h), angle = rect
    if w < h:
        angle += 90.0
    rotated = cv2.warpAffine(
        crop,
        cv2.getRotationMatrix2D((float(cx), float(cy)), float(angle), 1.0),
        (crop.shape[1], crop.shape[0]),
        flags=cv2.INTER_NEAREST,
        borderValue=0,
    )
    ys2, xs2 = np.where(rotated > 0)
    if len(xs2) < 20:
        return crop
    return rotated[ys2.min() : ys2.max() + 1, xs2.min() : xs2.max() + 1]


def _prepare_ssocr_mask(mask: np.ndarray) -> Optional[np.ndarray]:
    if not isinstance(mask, np.ndarray) or mask.ndim != 2 or mask.size == 0:
        return None
    normalized = np.where(mask > 0, 255, 0).astype(np.uint8)
    trimmed = _trim_red_crop(normalized)
    if trimmed.size == 0 or cv2.countNonZero(trimmed) < 20:
        return None
    height, width = trimmed.shape[:2]
    if height <= 0 or width <= 0:
        return None
    if height < 120:
        scale = 120.0 / float(height)
        trimmed = cv2.resize(
            trimmed,
            (max(1, int(round(width * scale))), 120),
            interpolation=cv2.INTER_NEAREST,
        )
    pad = max(4, int(round(trimmed.shape[0] * 0.05)))
    return cv2.copyMakeBorder(trimmed, pad, pad, pad, pad, cv2.BORDER_CONSTANT, value=0)


def _ssocr_mask_variants(img: np.ndarray, box: tuple[int, int, int, int]) -> list[tuple[str, np.ndarray]]:
    if not isinstance(img, np.ndarray) or img.ndim != 3 or img.size == 0:
        return []
    x, y, w, h = box
    if w <= 0 or h <= 0:
        return []
    x1, y1 = max(0, x), max(0, y)
    x2, y2 = min(img.shape[1], x + w), min(img.shape[0], y + h)
    crop = img[y1:y2, x1:x2]
    if crop.size == 0:
        return []

    red_80 = _red_mask_hsv(crop, 80, 80)
    raw_variants = (
        ("ssocr_red_80", red_80),
        ("ssocr_red_80_deskew", _deskew_red_crop(red_80)),
        ("ssocr_red_60", _red_mask_hsv(crop, 60, 60)),
    )
    variants: list[tuple[str, np.ndarray]] = []
    for name, raw_mask in raw_variants:
        prepared = _prepare_ssocr_mask(raw_mask)
        if prepared is None:
            continue
        if any(prepared.shape == old_mask.shape and np.array_equal(prepared, old_mask) for _old_name, old_mask in variants):
            continue
        variants.append((name, prepared))
    return variants


def _score_to_confidence(score: float) -> float:
    return max(0.0, min(0.95, 0.62 + score / 12.0))


def _recognize_ocr_candidates(
    img: np.ndarray,
    scored_boxes: list[tuple[float, tuple[int, int, int, int]]],
) -> list[_ParsedCandidate]:
    boxes: list[tuple[float, tuple[int, int, int, int]]] = []
    for score, box in sorted(scored_boxes, key=lambda item: item[0], reverse=True):
        if any(_box_iou(box, old_box) >= 0.72 for _old_score, old_box in boxes):
            continue
        boxes.append((score, box))
        if len(boxes) >= 18:
            break

    engine = _get_rapid_ocr()
    if engine is None:
        return []

    candidates: list[_ParsedCandidate] = []
    for box_score, box in boxes:
        x, y, w, h = box
        accepted_for_box = False
        for pad_ratio in (0.18, 0.04):
            if accepted_for_box:
                break
            pad = max(8, int(round(max(w, h) * pad_ratio)))
            x1 = max(0, x - pad)
            y1 = max(0, y - pad)
            x2 = min(img.shape[1], x + w + pad)
            y2 = min(img.shape[0], y + h + pad)
            crop = img[y1:y2, x1:x2]
            if crop.size == 0:
                continue
            scale = 4 if max(crop.shape[:2]) < 500 else 2
            gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
            variants = (
                ("rapidocr_crop", crop),
                ("rapidocr_crop_eq", cv2.cvtColor(cv2.equalizeHist(gray), cv2.COLOR_GRAY2BGR)),
            )
            for reason, variant in variants:
                resized = cv2.resize(variant, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
                try:
                    result, _ = engine(resized)
                except Exception:
                    continue
                if not result:
                    continue
                for _points, text, conf_text in result:
                    parsed = _parse_ocr_time_text(str(text))
                    if not parsed:
                        continue
                    try:
                        ocr_conf = float(conf_text)
                    except (TypeError, ValueError):
                        ocr_conf = 0.0
                    if ocr_conf < 0.70:
                        continue
                    score = ocr_conf * 2.0 + box_score * 0.20
                    confidence = max(0.0, min(0.95, 0.50 + ocr_conf * 0.45 + box_score * 0.03))
                    candidates.append(_ParsedCandidate(parsed, confidence, score, reason))
                    accepted_for_box = True
    return candidates


def _recognize_ssocr_candidates(
    img: np.ndarray,
    scored_boxes: list[tuple[float, tuple[int, int, int, int]]],
) -> list[_ParsedCandidate]:
    executable = _find_ssocr_executable()
    if not executable:
        return []

    boxes: list[tuple[float, tuple[int, int, int, int]]] = []
    for score, box in sorted(scored_boxes, key=lambda item: item[0], reverse=True):
        if any(_box_iou(box, old_box) >= 0.72 for _old_score, old_box in boxes):
            continue
        boxes.append((score, box))
        if len(boxes) >= 4:
            break

    variant_bonus = {
        "ssocr_red_80": 0.15,
        "ssocr_red_80_deskew": 0.10,
        "ssocr_red_60": 0.05,
    }
    candidates: list[_ParsedCandidate] = []
    for box_score, box in boxes:
        for variant_name, mask in _ssocr_mask_variants(img, box):
            parsed = _run_ssocr(mask, executable)
            if parsed:
                score = 2.5 + box_score * 0.20 + variant_bonus.get(variant_name, 0.0)
                candidates.append(_ParsedCandidate(parsed, 0.92, score, variant_name))
    return candidates


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
