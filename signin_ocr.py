from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from pathlib import Path
import argparse
import itertools
import math
import re
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

_SEGMENT_PATTERNS = {
    0: set("abcdef"),
    1: set("bc"),
    2: set("abged"),
    3: set("abgcd"),
    4: set("fgbc"),
    5: set("afgcd"),
    6: set("afgecd"),
    7: set("abc"),
    8: set("abcdefg"),
    9: set("abfgcd"),
}

_SEGMENT_ZONES = {
    "a": (0.22, 0.02, 0.78, 0.16),
    "g": (0.22, 0.43, 0.78, 0.57),
    "d": (0.22, 0.84, 0.78, 0.98),
    "f": (0.02, 0.14, 0.25, 0.44),
    "b": (0.75, 0.14, 0.98, 0.44),
    "e": (0.02, 0.56, 0.25, 0.86),
    "c": (0.75, 0.56, 0.98, 0.86),
}


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
    candidates: list[_ParsedCandidate] = []
    ocr_boxes: list[tuple[float, tuple[int, int, int, int]]] = []
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    panel_mask = _red_mask_hsv(img, 80, 80)

    for sat_min, val_min in _RED_THRESHOLDS:
        mask = _red_mask_hsv(img, sat_min, val_min)
        for box in _candidate_boxes_from_mask(mask):
            box_score = _score_candidate_box(img, gray, mask, box)
            candidate = _parse_candidate_mask(mask, box, box_score)
            if candidate is not None:
                candidates.append(candidate)
            if box_score >= 1.15:
                ocr_boxes.append((box_score, box))

    for panel_score, panel_box in _black_panel_candidate_boxes(img, gray, panel_mask):
        candidate = _parse_candidate_mask(panel_mask, panel_box, panel_score)
        if candidate is not None and candidate.score >= -0.5:
            candidates.append(candidate)
        ocr_boxes.append((panel_score + 1.2, panel_box))

    best = _select_consensus_candidate(candidates)
    if best is None or best.score < -3.0:
        candidates.extend(_recognize_ocr_candidates(img, ocr_boxes))
        best = _select_consensus_candidate(candidates)

    if best is not None and best.score >= -3.0:
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


def _parse_candidate_mask(mask: np.ndarray, box: tuple[int, int, int, int], box_score: float = 0.0) -> Optional[_ParsedCandidate]:
    x, y, w, h = box
    crop = mask[y : y + h, x : x + w]
    if cv2.countNonZero(crop) < 80:
        return None

    best: Optional[_ParsedCandidate] = None
    for variant in (crop, _deskew_red_crop(crop)):
        if cv2.countNonZero(variant) < 80:
            continue
        for digit_width_factor in (0.55, 0.62, 0.68, 0.75, 0.82):
            windows = _digit_windows_from_crop(variant, digit_width_factor)
            if len(windows) != 6:
                continue
            picked = _pick_valid_time(variant, windows)
            if picked is None:
                continue
            raw, digit_score = picked
            score = digit_score + box_score * 0.10
            confidence = _score_to_confidence(score)
            candidate = _ParsedCandidate(f"{raw[0:2]}:{raw[2:4]}:{raw[4:6]}", confidence, score)
            if best is None or candidate.score > best.score:
                best = candidate
    return best


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


def _digit_windows_from_crop(crop: np.ndarray, digit_width_factor: float = 0.68) -> list[tuple[int, int, int, int]]:
    height, width = crop.shape[:2]
    if height <= 0 or width <= 0:
        return []

    col = (crop > 0).sum(axis=0)
    min_col = max(1, int(round(height * 0.015)))
    runs: list[tuple[int, int, int, int]] = []
    start: Optional[int] = None
    for idx, value in enumerate(col):
        active = int(value) >= min_col
        if active and start is None:
            start = idx
        if (not active or idx == width - 1) and start is not None:
            end = idx if not active else idx + 1
            area = int(col[start:end].sum())
            max_col = int(col[start:end].max()) if end > start else 0
            if area >= max(8, int(height * 0.20)):
                runs.append((start, end, area, max_col))
            start = None

    merged: list[tuple[int, int, int, int]] = []
    for run in runs:
        if merged and run[0] - merged[-1][1] <= 1:
            old_start, _old_end, old_area, old_max = merged[-1]
            merged[-1] = (old_start, run[1], old_area + run[2], max(old_max, run[3]))
        else:
            merged.append(run)

    expected_w = max(8.0, float(height) * digit_width_factor)
    windows: list[tuple[int, int, int, int]] = []
    for x1, x2, _area, max_col in merged:
        run_w = x2 - x1
        if run_w < expected_w * 0.28 and max_col < height * 0.55:
            continue
        if run_w < expected_w * 1.55:
            parts = 1
        else:
            parts = max(2, min(4, int(round(float(run_w) / expected_w))))
        for idx in range(parts):
            part_x1 = int(round(x1 + idx * run_w / parts))
            part_x2 = int(round(x1 + (idx + 1) * run_w / parts))
            pad = max(1, int(round(height * 0.03)))
            wx1 = max(0, part_x1 - pad)
            wx2 = min(width, part_x2 + pad)
            if wx2 > wx1:
                windows.append((wx1, 0, wx2 - wx1, height))

    return windows


def _classify_digit(win: np.ndarray) -> tuple[int, float]:
    ranks = _rank_digit(win)
    return ranks[0] if ranks else (0, -9.0)


def _rank_digit(win: np.ndarray) -> list[tuple[int, float]]:
    if win.size == 0:
        return [(0, -9.0)]
    active_mask = win > 0
    ys, xs = np.where(active_mask)
    if len(xs) == 0:
        return [(0, -9.0)]
    height, width = active_mask.shape

    fg_w = int(xs.max() - xs.min() + 1)
    fg_h = int(ys.max() - ys.min() + 1)
    if float(fg_w) / float(max(fg_h, 1)) < 0.42:
        return [(1, 0.10), (7, -2.20), (4, -2.50)]

    vals = {}
    for name, (x1, y1, x2, y2) in _SEGMENT_ZONES.items():
        xx1 = int(round(x1 * width))
        xx2 = max(xx1 + 1, int(round(x2 * width)))
        yy1 = int(round(y1 * height))
        yy2 = max(yy1 + 1, int(round(y2 * height)))
        region = active_mask[yy1:min(yy2, height), xx1:min(xx2, width)]
        vals[name] = float(region.mean()) if region.size else 0.0

    max_val = max(vals.values()) if vals else 0.0
    threshold = max(0.18, max_val * 0.55)
    active = {name for name, val in vals.items() if val >= threshold}

    ranks: list[tuple[int, float]] = []
    all_segments = set("abcdefg")
    for digit, pattern in _SEGMENT_PATTERNS.items():
        misses = len(pattern - active)
        extras = len(active - pattern)
        score = -(1.40 * misses + 0.65 * extras)
        score += 0.08 * sum(vals[k] for k in pattern)
        score -= 0.08 * sum(vals[k] for k in all_segments - pattern)
        ranks.append((digit, score))
    return sorted(ranks, key=lambda item: item[1], reverse=True)[:5]


def _pick_valid_time(crop: np.ndarray, windows: list[tuple[int, int, int, int]]) -> Optional[tuple[str, float]]:
    ranks = []
    for wx, wy, ww, wh in windows:
        ranks.append(_rank_digit(crop[wy : wy + wh, wx : wx + ww]))
    if len(ranks) != 6 or any(not rank for rank in ranks):
        return None

    best: Optional[tuple[str, float]] = None
    for combo in itertools.product(*ranks):
        raw = "".join(str(digit) for digit, _score in combo)
        if not _valid_hhmmss_digits(raw):
            continue
        score = float(sum(score for _digit, score in combo))
        if best is None or score > best[1]:
            best = (raw, score)
    return best


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
