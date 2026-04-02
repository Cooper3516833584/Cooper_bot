import argparse
from collections import defaultdict
import csv
import json
import logging
import os
import re
import tempfile
import threading
import unicodedata
from pathlib import Path
from typing import Any

import cv2
import numpy as np
from rapidocr import RapidOCR


_OCR_ENGINE_LOCK = threading.Lock()
_OCR_ENGINE: RapidOCR | None = None

# 閽堝鏁欏缁胯壊榛戞澘鐨勭粡楠?HSV 鑼冨洿锛堣緝鏃ч槇鍊兼洿鏀舵暃锛屽噺灏戦潪榛戞澘璇Е鍙戯級
BOARD_GREEN_LOWER = np.array([38, 35, 25], dtype=np.uint8)
BOARD_GREEN_UPPER = np.array([92, 255, 255], dtype=np.uint8)


def _suppress_rapidocr_noise() -> None:
    """
    RapidOCR 榛樿浼氳緭鍑哄ぇ閲?info/warning锛堝妯″瀷璺緞銆佺┖妫€娴嬫彁绀猴級銆?    杩欓噷缁熶竴鍘嬪埌 ERROR锛屼粎鍦ㄧ湡姝ｅ紓甯告椂杈撳嚭銆?    """
    candidates = ["RapidOCR", "rapidocr"]
    for name in candidates:
        lg = logging.getLogger(name)
        lg.setLevel(logging.ERROR)
        lg.propagate = False

    for logger_name in list(logging.root.manager.loggerDict.keys()):
        if logger_name.startswith("RapidOCR") or logger_name.startswith("rapidocr"):
            lg = logging.getLogger(logger_name)
            lg.setLevel(logging.ERROR)
            lg.propagate = False


# =========================
# 鍩虹宸ュ叿
# =========================

def order_points(pts: np.ndarray) -> np.ndarray:
    """灏嗗洓涓偣鎸?tl, tr, br, bl 鎺掑簭"""
    rect = np.zeros((4, 2), dtype="float32")
    s = pts.sum(axis=1)
    diff = np.diff(pts, axis=1).reshape(-1)

    rect[0] = pts[np.argmin(s)]      # top-left
    rect[2] = pts[np.argmax(s)]      # bottom-right
    rect[1] = pts[np.argmin(diff)]   # top-right
    rect[3] = pts[np.argmax(diff)]   # bottom-left
    return rect


def four_point_transform(image: np.ndarray, pts: np.ndarray) -> np.ndarray:
    """閫忚鍙樻崲"""
    rect = order_points(pts.astype(np.float32))
    (tl, tr, br, bl) = rect

    width_a = np.linalg.norm(br - bl)
    width_b = np.linalg.norm(tr - tl)
    max_width = int(max(width_a, width_b))

    height_a = np.linalg.norm(tr - br)
    height_b = np.linalg.norm(tl - bl)
    max_height = int(max(height_a, height_b))

    max_width = max(max_width, 10)
    max_height = max(max_height, 10)

    dst = np.array(
        [
            [0, 0],
            [max_width - 1, 0],
            [max_width - 1, max_height - 1],
            [0, max_height - 1],
        ],
        dtype="float32",
    )

    m = cv2.getPerspectiveTransform(rect, dst)
    warped = cv2.warpPerspective(image, m, (max_width, max_height))
    return warped


def parse_manual_points(s: str | None) -> np.ndarray | None:
    """
    鎵嬪姩鍥涚偣鏍煎紡:
    "x1,y1;x2,y2;x3,y3;x4,y4"
    """
    if not s:
        return None

    pts = []
    for item in s.split(";"):
        x, y = item.split(",")
        pts.append([float(x), float(y)])
    pts = np.array(pts, dtype=np.float32)

    if pts.shape != (4, 2):
        raise ValueError('manual_points 蹇呴』鏄?4 涓偣锛屼緥濡? "x1,y1;x2,y2;x3,y3;x4,y4"')

    return pts


# =========================
# 1. 榛戞澘鍖哄煙妫€娴?
# =========================

def detect_board_auto(image: np.ndarray) -> np.ndarray:
    """
    鑷姩妫€娴嬬豢鑹查粦鏉垮苟閫忚鐭
    """
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)

    mask = cv2.inRange(hsv, BOARD_GREEN_LOWER, BOARD_GREEN_UPPER)

    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 15))
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel, iterations=2)
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel, iterations=1)

    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    h, w = image.shape[:2]
    img_area = h * w
    candidates = []

    for cnt in contours:
        area = cv2.contourArea(cnt)
        if area < img_area * 0.08:
            continue

        x, y, bw, bh = cv2.boundingRect(cnt)
        aspect = bw / max(bh, 1)
        score = area + 1000 * min(aspect, 3.0)
        candidates.append((score, cnt))

    if not candidates:
        return image

    cnt = max(candidates, key=lambda x: x[0])[1]

    peri = cv2.arcLength(cnt, True)
    approx = cv2.approxPolyDP(cnt, 0.02 * peri, True)

    if len(approx) == 4:
        pts = approx.reshape(4, 2)
    else:
        rect = cv2.minAreaRect(cnt)
        pts = cv2.boxPoints(rect)

    warped = four_point_transform(image, pts)

    # 妯澘浼樺厛
    if warped.shape[0] > warped.shape[1]:
        warped = cv2.rotate(warped, cv2.ROTATE_90_CLOCKWISE)

    # 鐣ヨ杈规
    hh, ww = warped.shape[:2]
    mx = int(ww * 0.02)
    my = int(hh * 0.02)
    warped = warped[my:hh - my, mx:ww - mx]

    return warped


def get_board(image: np.ndarray, manual_points: np.ndarray | None = None) -> np.ndarray:
    if manual_points is not None:
        board = four_point_transform(image, manual_points)
        if board.shape[0] > board.shape[1]:
            board = cv2.rotate(board, cv2.ROTATE_90_CLOCKWISE)
        return board
    return detect_board_auto(image)


def _green_board_mask(image: np.ndarray) -> np.ndarray:
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    mask = cv2.inRange(hsv, BOARD_GREEN_LOWER, BOARD_GREEN_UPPER)
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 15))
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel, iterations=2)
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel, iterations=1)
    return mask


def _has_green_board_contour(
    mask: np.ndarray,
    image_shape: tuple[int, ...],
    min_area_ratio: float = 0.18,
    min_rect_area_ratio: float = 0.20,
    min_extent: float = 0.45,
    min_aspect: float = 1.10,
) -> bool:
    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return False
    h, w = image_shape[:2]
    img_area = max(h * w, 1)
    for cnt in contours:
        area = cv2.contourArea(cnt)
        if area < img_area * float(min_area_ratio):
            continue

        x, y, bw, bh = cv2.boundingRect(cnt)
        rect_area = max(bw * bh, 1)
        rect_ratio = float(rect_area) / float(img_area)
        if rect_ratio < max(float(min_rect_area_ratio), float(min_area_ratio) * 1.15):
            continue

        aspect = float(bw) / float(max(bh, 1))
        if aspect < float(min_aspect):
            continue

        extent = float(area) / float(rect_area)
        if extent < float(min_extent):
            continue
        return True
    return False


def is_green_blackboard(image: np.ndarray, min_area_ratio: float = 0.18, min_green_ratio: float = 0.18) -> bool:
    if image is None or image.size == 0:
        return False
    mask = _green_board_mask(image)
    green_ratio = float(np.count_nonzero(mask)) / float(mask.size or 1)
    if green_ratio < float(min_green_ratio):
        return False
    return _has_green_board_contour(mask, image.shape, min_area_ratio=float(min_area_ratio))


def extract_green_board(image: np.ndarray, min_area_ratio: float = 0.18, min_green_ratio: float = 0.18) -> tuple[bool, np.ndarray]:
    if image is None or image.size == 0:
        return False, image
    ok = is_green_blackboard(image, min_area_ratio=min_area_ratio, min_green_ratio=min_green_ratio)
    if not ok:
        return False, image

    # For near-board photos, skip perspective warp to avoid thinning chalk strokes.
    mask = _green_board_mask(image)
    green_ratio = float(np.count_nonzero(mask)) / float(mask.size or 1)
    if green_ratio >= 0.45:
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if contours:
            h, w = image.shape[:2]
            img_area = max(h * w, 1)
            cnt = max(contours, key=cv2.contourArea)
            x, y, bw, bh = cv2.boundingRect(cnt)
            bbox_ratio = float(bw * bh) / float(img_area)
            aspect = float(bw) / float(max(bh, 1))
            if bbox_ratio >= 0.60 and 0.90 <= aspect <= 3.50:
                return True, image

    return True, get_board(image)


# =========================
# 2. 鍥惧儚澧炲己
# =========================

def resize_for_ocr(img: np.ndarray, target_width: int = 1800) -> np.ndarray:
    h, w = img.shape[:2]
    if w == 0:
        return img
    scale = target_width / w
    interp = cv2.INTER_CUBIC if scale >= 1.0 else cv2.INTER_AREA
    return cv2.resize(img, None, fx=scale, fy=scale, interpolation=interp)


def enhance_variants(board: np.ndarray) -> dict[str, np.ndarray]:
    """
    鐢熸垚澶氫釜鍥惧儚鐗堟湰锛孫CR 鍚庨€夋渶濂界殑涓€鐗?
    """
    orig_h, orig_w = board.shape[:2]
    board = resize_for_ocr(board, target_width=1800)

    lab = cv2.cvtColor(board, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    l = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8)).apply(l)
    lab = cv2.merge([l, a, b])
    board_clahe = cv2.cvtColor(lab, cv2.COLOR_LAB2BGR)

    gray = cv2.cvtColor(board_clahe, cv2.COLOR_BGR2GRAY)

    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (21, 21))
    tophat = cv2.morphologyEx(gray, cv2.MORPH_TOPHAT, kernel)
    norm = cv2.normalize(tophat, None, 0, 255, cv2.NORM_MINMAX)

    binary = cv2.adaptiveThreshold(
        norm,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31,
        -5,
    )

    # 鍘诲皬鍣偣
    num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(binary, connectivity=8)
    cleaned = np.zeros_like(binary)
    for i in range(1, num_labels):
        area = stats[i, cv2.CC_STAT_AREA]
        if 12 <= area <= 50000:
            cleaned[labels == i] = 255

    # 杞垚鐧藉簳榛戝瓧鐗堟湰锛屾湁鏃舵洿鍒╀簬 OCR
    white_bg = 255 - cleaned

    adaptive = cv2.adaptiveThreshold(
        gray,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31,
        -3,
    )

    variants = {
        "board_original": board,
        "board_clahe": board_clahe,
        "board_gray": cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR),
        "board_enhanced": cv2.cvtColor(norm, cv2.COLOR_GRAY2BGR),
        "board_binary": cv2.cvtColor(white_bg, cv2.COLOR_GRAY2BGR),
        "board_adaptive": cv2.cvtColor(adaptive, cv2.COLOR_GRAY2BGR),
        "board_adaptive_inv": cv2.cvtColor(255 - adaptive, cv2.COLOR_GRAY2BGR),
    }

    # Small captures often lose faint digits (e.g. "1.4.1") at OCR scale.
    # Add one zoomed contrast variant only for small-source boards to limit cost.
    if orig_h <= 450 or orig_w <= 1350:
        zoom = cv2.resize(board, None, fx=1.8, fy=1.8, interpolation=cv2.INTER_CUBIC)
        z_lab = cv2.cvtColor(zoom, cv2.COLOR_BGR2LAB)
        z_l, z_a, z_b = cv2.split(z_lab)
        z_l = cv2.createCLAHE(clipLimit=2.3, tileGridSize=(8, 8)).apply(z_l)
        z_lab = cv2.merge([z_l, z_a, z_b])
        variants["board_zoom_clahe"] = cv2.cvtColor(z_lab, cv2.COLOR_LAB2BGR)

    return variants


# =========================
# 3. RapidOCR 杈撳嚭瑙ｆ瀽
# =========================

def quad_to_xyxy(box: Any) -> list[int] | None:
    """
    鎶?box 缁熶竴杞垚 [x1, y1, x2, y2]
    鏀寔:
    - [x1, y1, x2, y2]
    - [[x, y], [x, y], [x, y], [x, y]]
    """
    if box is None:
        return None

    try:
        arr = np.array(box, dtype=float)
    except Exception:
        return None

    if arr.size == 4 and arr.ndim == 1:
        x1, y1, x2, y2 = arr.tolist()
        x1, x2 = min(x1, x2), max(x1, x2)
        y1, y2 = min(y1, y2), max(y1, y2)
        return [int(x1), int(y1), int(x2), int(y2)]

    if arr.ndim == 2 and arr.shape[1] == 2:
        x1 = np.min(arr[:, 0])
        y1 = np.min(arr[:, 1])
        x2 = np.max(arr[:, 0])
        y2 = np.max(arr[:, 1])
        return [int(x1), int(y1), int(x2), int(y2)]

    flat = arr.reshape(-1)
    if flat.size >= 8 and flat.size % 2 == 0:
        pts = flat.reshape(-1, 2)
        x1 = np.min(pts[:, 0])
        y1 = np.min(pts[:, 1])
        x2 = np.max(pts[:, 0])
        y2 = np.max(pts[:, 1])
        return [int(x1), int(y1), int(x2), int(y2)]

    return None


def _unwrap_rapidocr_result(raw: Any) -> Any:
    """
    鍏煎 RapidOCR 鍙兘杩斿洖:
    - result
    - (result, elapse)
    """
    if raw is None:
        return None

    if isinstance(raw, tuple) and len(raw) >= 1:
        return raw[0]

    return raw


def _safe_list(x: Any) -> list:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, tuple):
        return list(x)
    try:
        return list(x)
    except Exception:
        return []


def _build_item(box: Any, text: Any, score: Any) -> dict | None:
    xyxy = quad_to_xyxy(box)
    if xyxy is None:
        return None

    text = "" if text is None else str(text).strip()
    if not text:
        return None

    try:
        score = float(score)
    except Exception:
        score = 1.0

    x1, y1, x2, y2 = xyxy
    return {
        "text": text,
        "score": score,
        "box": [x1, y1, x2, y2],
        "xc": (x1 + x2) / 2,
        "yc": (y1 + y2) / 2,
        "h": max(1, y2 - y1),
    }


def parse_rapidocr_output(raw: Any) -> list[dict]:
    """
    鍏煎涓嶅悓 RapidOCR 杩斿洖鏍煎紡锛屽苟澶勭悊 txts=None 鐨勬儏鍐?
    """
    data = _unwrap_rapidocr_result(raw)
    if data is None:
        return []

    # 鎯呭喌 1: 瀵硅薄灞炴€?
    if hasattr(data, "boxes") or hasattr(data, "txts") or hasattr(data, "scores"):
        boxes = _safe_list(getattr(data, "boxes", None))
        txts = _safe_list(getattr(data, "txts", None))
        scores = _safe_list(getattr(data, "scores", None))

        if not txts:
            return []

        if not scores:
            scores = [1.0] * len(txts)

        items = []
        for i, text in enumerate(txts):
            box = boxes[i] if i < len(boxes) else None
            score = scores[i] if i < len(scores) else 1.0
            item = _build_item(box, text, score)
            if item is not None:
                items.append(item)
        return items

    # 鎯呭喌 2: dict
    if isinstance(data, dict):
        boxes = _safe_list(data.get("boxes"))
        txts = _safe_list(data.get("txts", data.get("texts", data.get("rec_texts"))))
        scores = _safe_list(data.get("scores", data.get("rec_scores")))

        if txts:
            if not scores:
                scores = [1.0] * len(txts)

            items = []
            for i, text in enumerate(txts):
                box = boxes[i] if i < len(boxes) else None
                score = scores[i] if i < len(scores) else 1.0
                item = _build_item(box, text, score)
                if item is not None:
                    items.append(item)
            return items

        if "results" in data:
            return parse_rapidocr_output(data["results"])

        if "data" in data:
            return parse_rapidocr_output(data["data"])

        return []

    # 鎯呭喌 3: list
    if isinstance(data, list):
        items = []
        for elem in data:
            if isinstance(elem, dict):
                box = elem.get("box", elem.get("boxes"))
                text = elem.get("text", elem.get("txt", elem.get("rec_text")))
                score = elem.get("score", elem.get("scores", elem.get("rec_score", 1.0)))
                item = _build_item(box, text, score)
                if item is not None:
                    items.append(item)
                continue

            if isinstance(elem, (list, tuple)):
                if len(elem) >= 3:
                    box, text, score = elem[0], elem[1], elem[2]
                    item = _build_item(box, text, score)
                    if item is not None:
                        items.append(item)
                elif len(elem) == 2:
                    box, text = elem[0], elem[1]
                    item = _build_item(box, text, 1.0)
                    if item is not None:
                        items.append(item)

        return items

    # 鎯呭喌 4: 鍏滃簳灏濊瘯 __dict__
    if hasattr(data, "__dict__"):
        return parse_rapidocr_output(vars(data))

    return []


def run_ocr_on_image(engine: RapidOCR, image: np.ndarray) -> list[dict]:
    """
    灏嗗浘鍍忓啓鎴愪复鏃舵枃浠跺悗鍠傜粰 RapidOCR
    """
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        temp_path = f.name

    try:
        ok = cv2.imwrite(temp_path, image)
        if not ok:
            return []

        raw = engine(temp_path)
        items = parse_rapidocr_output(raw)
        return items
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


# =========================
# 4. OCR 鏂囨湰鍚庡鐞?
# =========================

PAGE_RE = re.compile(r"\bP\s*(\d{2,3})\b", re.IGNORECASE)
QUESTION_RE = re.compile(r"\b\d+(?:\.\d+)+(?:\(\d+\))*\b")
VALID_QUESTION_RE = re.compile(r"^\d{1,2}\.\d{1,2}\.\d{1,3}(?:\(\d+\))*$")

OCR_DIGIT_TRANS = str.maketrans(
    {
        "O": "0",
        "Q": "0",
        "I": "1",
        "L": "1",
        "人": "1",
        "丨": "1",
        "K": "4",
        "长": "4",
        "Z": "2",
        "S": "5",
        "B": "8",
        "G": "6",
    }
)


def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s).upper()

    repl = {
        "?": ".",
        "?": ".",
        "?": ".",
        ",": ".",
        "?": ".",
        "?": ".",
        "?": ".",
        ":": ":",
        "?": ":",
        ";": " ",
        "?": " ",
        "?": "(",
        "?": ")",
        "[": "(",
        "]": ")",
        "{": "(",
        "}": ")",
        "?": "(",
        "?": ")",
        "%": "8",
        "?": "+",
    }
    for k, v in repl.items():
        s = s.replace(k, v)

    # ??????????
    s = s.replace("??", ".")

    s = re.sub(r"\bP\s+(\d+)\b", r"P\1", s)
    s = re.sub(r"(?<=\d)\s*\.\s*(?=\d)", ".", s)
    s = re.sub(r"\s*\(\s*", "(", s)
    s = re.sub(r"\s*\)\s*", ")", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _normalize_digit_token(s: str) -> str:
    s = s.translate(OCR_DIGIT_TRANS)
    return re.sub(r"\D", "", s)


def _normalize_sub_number(s: str) -> str:
    s = _normalize_digit_token(s)
    if not s:
        return ""
    if s in {"10", "01", "11"}:
        return "1"
    if len(s) > 1 and len(set(s)) == 1:
        s = s[0]
    return s.lstrip("0") or "0"


def is_valid_question(q: str | None) -> bool:
    if not q:
        return False
    if not VALID_QUESTION_RE.fullmatch(q):
        return False

    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{1,3})", q)
    if not m:
        return False

    a, b, c = map(int, m.groups())
    if not (1 <= a <= 30 and 1 <= b <= 30 and 1 <= c <= 999):
        return False
    return True


def question_prefix(q: str) -> tuple[str, str] | None:
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.\d{1,3}", q)
    if not m:
        return None
    return m.group(1), m.group(2)


def question_base(q: str) -> str:
    m = re.match(r"^(\d{1,2}\.\d{1,2}\.\d{1,3})", q)
    if not m:
        return q
    return m.group(1)


def question_sort_key(q: str) -> tuple[int, int, int, int]:
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{1,3})", q)
    if not m:
        return (999, 999, 999, 999)
    a, b, c = map(int, m.groups())
    subs = [int(x) for x in re.findall(r"\((\d+)\)", q)]
    first_sub = subs[0] if subs else -1
    return (a, b, c, first_sub)


def question_triplet_str(q: str) -> tuple[str, str, str] | None:
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{1,3})", q)
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3)


def _is_compact_ambiguous_question_token(token: str) -> bool:
    """
    褰㈠ "1.56" 鐨勭揣鍑戝啓娉曞湪 OCR 涓涔夊緢澶э紝甯哥敱 "1.6.1" 鎵洸鑰屾潵銆?    杩欓噷浠呭仛鏍囪锛屼笉鐩存帴涓㈠純锛屽悗缁粨鍚堟敮鎸佸害鍐嶈繃婊ゃ€?    """
    t = normalize_text(token).strip()
    if not t or t.startswith("P"):
        return False
    t = t.replace(" ", "")
    if "(" in t or ")" in t:
        return False

    # 鍙鈥滃崟涓偣 + 鍓?鍚?浣嶆暟瀛椻€濆仛姝т箟鏍囪锛屽 1.56 / 2.26
    if t.count(".") != 1:
        return False
    parts = [x for x in t.split(".") if x]
    if len(parts) != 2:
        return False
    left = _normalize_digit_token(parts[0])
    right = _normalize_digit_token(parts[1])
    if not left or not right:
        return False
    return len(left) == 1 and len(right) == 2


def _is_joined_triplet_ambiguous_token(token: str) -> bool:
    """
    Tokens like "141"/"156" (without delimiters) are easy OCR confusions.
    Mark them as ambiguous and let merge stage decide with context.
    """
    t = normalize_text(token).strip()
    if not t or t.startswith("P"):
        return False
    t = t.replace(" ", "")
    if "." in t or "(" in t or ")" in t:
        return False
    t = re.sub(r"[^0-9OILKSZBG\u4EBA\u4E28\u957F]", "", t)
    digits = _normalize_digit_token(t)
    return len(digits) == 3


def _unique_keep_order(values: list[Any]) -> list[Any]:
    seen = set()
    out = []
    for v in values:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _build_question_from_parts(
    parts: list[str],
    sub_nums: list[str],
    prefix: tuple[str, str] | None,
) -> str | None:
    triplet: list[str] | None = None

    if len(parts) == 3:
        triplet = parts[:3]
    elif len(parts) == 2:
        left, right = parts
        if len(left) == 2:
            triplet = [left[0], left[1], right]
        elif len(left) == 1 and len(right) == 2:
            triplet = [left, right[0], right[1]]
        elif len(left) == 1 and prefix is not None and left == prefix[0]:
            # 褰㈠ "2.2(2)" 鏃讹紝閫氬父鏄己澶卞瓧绗﹀鑷达紝涓嶇洿鎺ヨˉ鎴?2.2.2
            if len(right) == 1 and sub_nums:
                return None
            triplet = [left, prefix[1], right]
    elif len(parts) == 1:
        digits = parts[0]
        if len(digits) == 3:
            if prefix is None:
                # ?????????????????? 446/141/422
                if digits[0] == digits[1] or digits[0] == digits[2] or digits[1] == digits[2]:
                    triplet = [digits[0], digits[1], digits[2]]
            else:
                if digits[0] == prefix[0]:
                    mid = digits[1]
                    if mid != prefix[1]:
                        if digits[0] == digits[1] or digits[0] == digits[2] or digits[1] == digits[2]:
                            mid = prefix[1]
                        else:
                            return None
                    triplet = [digits[0], mid, digits[2]]

    if triplet is None:
        return None

    a, b, c = triplet

    if len(a) > 1:
        if len(set(a)) == 1:
            a = a[0]
        else:
            return None
    if len(b) > 1:
        if len(set(b)) == 1:
            b = b[0]
        else:
            return None
    if len(c) > 3:
        return None

    try:
        a_i = int(a)
        b_i = int(b)
        c_i = int(c)
    except ValueError:
        return None

    out = f"{a_i}.{b_i}.{c_i}"
    if not is_valid_question(out):
        return None

    if sub_nums:
        out += "".join(f"({x})" for x in sub_nums)
        if not is_valid_question(out):
            return None

    return out


def repair_question_token(
    token: str,
    preferred_prefixes: list[tuple[str, str]] | None = None,
) -> str | None:
    preferred_prefixes = preferred_prefixes or []
    q = normalize_text(token)
    if not q:
        return None

    for ch in ("'", "`", "’", "‘"):
        q = q.replace(ch, ".")
    q = q.replace("+", "")
    q = q.replace(" ", "")
    q = re.sub(r"(?<=\d)\.(?=\()", "", q)
    q = re.sub(r"(?<!\()(\d+)\)", r"(\1)", q)
    q = re.sub(r"[^\dOILKSZBG\u4EBA\u4E28\u957F\.\(\)]", "", q)
    q = re.sub(r"\.+", ".", q).strip(".")
    if not q:
        return None

    if "(" in q:
        idx = q.find("(")
        base = q[:idx].strip(".")
        suffix = q[idx:]
    else:
        base = q.strip(".")
        suffix = ""
    if not base:
        return None

    parts = [_normalize_digit_token(x) for x in base.split(".") if x]
    parts = [x for x in parts if x]
    if not parts:
        return None

    raw_sub_nums = re.findall(r"\(([^()]*)\)", suffix)
    if not raw_sub_nums and suffix:
        raw_sub_nums = re.findall(r"\d+", suffix)

    sub_nums = []
    for raw_num in raw_sub_nums:
        num = _normalize_sub_number(raw_num)
        if num:
            sub_nums.append(num)

    tried = set()
    for pref in [None] + preferred_prefixes:
        key = tuple(pref) if pref is not None else None
        if key in tried:
            continue
        tried.add(key)

        candidate = _build_question_from_parts(parts, sub_nums, pref)
        if is_valid_question(candidate):
            return candidate

    return None


def group_items_to_lines(items: list[dict]) -> list[dict]:
    """
    ??y ?????OCR box ??????????
    """
    items = [x for x in items if x["score"] >= 0.20 and x["text"].strip()]
    items = sorted(items, key=lambda x: (x["yc"], x["xc"]))

    lines = []
    for item in items:
        placed = False
        for line in lines:
            tol = max(18, 0.6 * line["avg_h"])
            if abs(item["yc"] - line["yc"]) <= tol:
                line["items"].append(item)
                ys = [it["yc"] for it in line["items"]]
                hs = [it["h"] for it in line["items"]]
                line["yc"] = sum(ys) / len(ys)
                line["avg_h"] = sum(hs) / len(hs)
                placed = True
                break

        if not placed:
            lines.append(
                {
                    "yc": item["yc"],
                    "avg_h": item["h"],
                    "items": [item],
                }
            )

    line_texts = []
    for line in sorted(lines, key=lambda x: x["yc"]):
        line["items"] = sorted(line["items"], key=lambda x: x["xc"])
        tokens = [normalize_text(it["text"]) for it in line["items"] if it["text"].strip()]
        text = " ".join(tokens)
        score = sum([it["score"] for it in line["items"]]) / max(len(line["items"]), 1)
        line_texts.append(
            {
                "text": text,
                "score": score,
                "tokens": tokens,
            }
        )
    return line_texts


def repair_page_token(token: str) -> str:
    """
    ?????OCR ????????token ?????? P206
    ???:
    - 206   -> P206
    - P206  -> P206
    - PZ06  -> P206
    - R208  -> P208
    """
    raw = unicodedata.normalize("NFKC", str(token or "")).upper().strip()
    if not raw:
        return raw

    # Page tokens should stay within a narrow OCR confusion charset.
    # This avoids treating text like "人长1" as page number "P141".
    if not re.fullmatch(r"[PR0-9OILKSZBG]+", raw):
        return normalize_text(raw).strip()

    token = normalize_text(raw).strip()
    if not token:
        return token

    if token.startswith("P"):
        body = _normalize_digit_token(token[1:])
    else:
        body = _normalize_digit_token(token)

    if 2 <= len(body) <= 3:
        return "P" + body
    return token


def _parse_page_num(page: str | None) -> int | None:
    if not page:
        return None
    m = re.fullmatch(r"P(\d{2,3})", page)
    if not m:
        return None
    return int(m.group(1))


def extract_page_from_line(
    line: dict,
    current_page: str | None = None,
    known_prefixes: list[tuple[str, str]] | None = None,
) -> str | None:
    """
    ???????????????
    """
    text = line["text"]
    tokens = line.get("tokens", [])
    known_prefixes = known_prefixes or []

    # ?? Pxxx ???
    m = PAGE_RE.search(text)
    if m:
        return f"P{m.group(1)}"

    if not tokens:
        return None

    first_raw = normalize_text(tokens[0]).strip()
    first_page = repair_page_token(first_raw)
    m = PAGE_RE.fullmatch(first_page)
    if not m:
        return None

    page = f"P{m.group(1)}"

    # ?? P ????????
    if first_raw.startswith("P"):
        return page

    # ? P ????????????????????????
    if "." in first_raw or "(" in first_raw or ")" in first_raw:
        return None
    if len(tokens) < 2:
        return None

    has_strong_question = False
    for tok in tokens[1:]:
        q = repair_question_token(tok, preferred_prefixes=known_prefixes)
        if is_valid_question(q):
            has_strong_question = True
            break
    if not has_strong_question:
        return None

    current_num = _parse_page_num(current_page)
    new_num = _parse_page_num(page)
    if current_num is not None and new_num is not None:
        # ??????????????????
        if abs(new_num - current_num) > 40:
            return None

    return page


def extract_questions_from_line(
    line: dict,
    known_prefixes: list[tuple[str, str]] | None = None,
) -> list[tuple[str, float, bool]]:
    """
    Parse question ids from one OCR text line.
    Returns tuples: (question, score_factor, ambiguous_source).
    """
    text = normalize_text(line["text"])
    tokens = line.get("tokens", [])
    known_prefixes = known_prefixes or []

    qs: list[str] = []
    q_factor: dict[str, float] = {}
    q_ambiguous: dict[str, bool] = {}
    unresolved_tokens: list[str] = []

    def _add_question(candidate: str | None, factor: float, ambiguous: bool) -> None:
        if not is_valid_question(candidate):
            return
        q = str(candidate)
        prev = q_factor.get(q)
        if prev is None:
            qs.append(q)
            q_factor[q] = float(factor)
            q_ambiguous[q] = bool(ambiguous)
            return
        if factor > prev:
            q_factor[q] = float(factor)
            q_ambiguous[q] = bool(ambiguous)
        elif abs(factor - prev) <= 1e-8 and q_ambiguous.get(q, False) and not ambiguous:
            q_ambiguous[q] = False

    for tok in tokens:
        tok_norm = normalize_text(tok)
        if not tok_norm:
            continue
        if tok_norm.startswith("P"):
            continue

        candidate = repair_question_token(tok_norm)
        if is_valid_question(candidate):
            amb = _is_compact_ambiguous_question_token(tok_norm) or _is_joined_triplet_ambiguous_token(tok_norm)
            _add_question(candidate, factor=(0.58 if amb else 1.00), ambiguous=amb)
        else:
            unresolved_tokens.append(tok_norm)

    local_prefixes = [question_prefix(q) for q in qs if question_prefix(q) is not None]
    prefix_candidates = _unique_keep_order(local_prefixes + known_prefixes)

    for tok_norm in unresolved_tokens:
        pref_for_tok = prefix_candidates
        # Avoid cross-line prefix bleed for compact 3-digit tokens like "156":
        # when current line already has a local prefix, prefer local-only repair.
        if _is_joined_triplet_ambiguous_token(tok_norm) and local_prefixes:
            pref_for_tok = _unique_keep_order(local_prefixes)
        candidate = repair_question_token(tok_norm, preferred_prefixes=pref_for_tok)
        if is_valid_question(candidate):
            amb = _is_compact_ambiguous_question_token(tok_norm) or _is_joined_triplet_ambiguous_token(tok_norm)
            _add_question(candidate, factor=(0.56 if amb else 0.92), ambiguous=amb)

    # Whole-line fallback: recover from token split/merge noise.
    line_chunks = re.findall(r"[0-9OILKSZBG\u4EBA\u4E28\u957F\.\(\)'`+\-]{3,}", text)
    for chunk in line_chunks:
        chunk_candidates = [chunk]
        if any(sep in chunk for sep in "-_/|"):
            split_parts = [x for x in re.split(r"[-_/|]+", chunk) if x]
            if len(split_parts) >= 2:
                chunk_candidates.extend(split_parts)
        for chk in _unique_keep_order(chunk_candidates):
            pref_for_chunk = prefix_candidates
            if _is_joined_triplet_ambiguous_token(chk) and local_prefixes:
                pref_for_chunk = _unique_keep_order(local_prefixes)
            candidate = repair_question_token(chk, preferred_prefixes=pref_for_chunk)
            if is_valid_question(candidate):
                amb = _is_compact_ambiguous_question_token(chk) or _is_joined_triplet_ambiguous_token(chk)
                _add_question(candidate, factor=(0.50 if amb else 0.68), ambiguous=amb)

            # Joined fallback, e.g. "141.1.43" -> 1.4.1 + 1.4.3
            joined = chk.strip(".")
            m_join = re.fullmatch(r"(\d{3})\.(\d)\.(\d{2})", joined)
            if m_join:
                q1 = repair_question_token(m_join.group(1), preferred_prefixes=prefix_candidates)
                if is_valid_question(q1):
                    _add_question(q1, factor=0.72, ambiguous=True)

                local_pref = question_prefix(q1) if is_valid_question(q1) else None
                pref_for_q2 = prefix_candidates
                if local_pref is not None:
                    pref_for_q2 = [local_pref] + [x for x in prefix_candidates if x != local_pref]

                q2 = repair_question_token(
                    f"{m_join.group(2)}.{m_join.group(3)}",
                    preferred_prefixes=pref_for_q2,
                )
                if is_valid_question(q2):
                    _add_question(q2, factor=0.72, ambiguous=True)

            # Glued form like "1411.4.3"/"1长11.4.3": split into 141 + 1.4.3.
            m_glued = re.fullmatch(
                r"([0-9OILKSZBG\u4EBA\u4E28\u957F]{3})(\d\.\d\.\d)",
                joined,
            )
            if m_glued:
                q1 = repair_question_token(m_glued.group(1), preferred_prefixes=prefix_candidates)
                if is_valid_question(q1):
                    _add_question(q1, factor=0.70, ambiguous=True)

                local_pref = question_prefix(q1) if is_valid_question(q1) else None
                pref_for_q2 = prefix_candidates
                if local_pref is not None:
                    pref_for_q2 = [local_pref] + [x for x in prefix_candidates if x != local_pref]
                q2 = repair_question_token(m_glued.group(2), preferred_prefixes=pref_for_q2)
                if is_valid_question(q2):
                    _add_question(q2, factor=0.70, ambiguous=True)

    # De-dup while preserving order.
    seen = set()
    out: list[str] = []
    for q in qs:
        if q in seen:
            continue
        seen.add(q)
        out.append(q)

    # If one question has bracket sub-number, avoid treating that sub-number as a new question.
    cleaned: list[str] = []
    for q in out:
        m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{1,3})", q)
        if m is None:
            cleaned.append(q)
            continue

        a, b, c = m.group(1), m.group(2), m.group(3)
        if "(" not in q:
            shadowed = False
            for other in out:
                if other == q or "(" not in other:
                    continue
                m2 = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{1,3})", other)
                if m2 is None:
                    continue
                if m2.group(1) == a and m2.group(2) == b:
                    sub_nums = set(re.findall(r"\((\d+)\)", other))
                    if c in sub_nums:
                        shadowed = True
                        break
            if shadowed:
                continue

        cleaned.append(q)

    # Line-local conflict downweight:
    # if a high single-digit tail (>=5) appears with a nearby lower-prefix low tail (<=3)
    # in the same line, treat the high-tail one as likely OCR drift (e.g. 1.5.6 vs 1.4.3).
    parsed_cleaned: list[tuple[int, str, int, int, int]] = []
    for idx, q in enumerate(cleaned):
        m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{1,3})", q)
        if m is None:
            continue
        try:
            a_i, b_i, c_i = int(m.group(1)), int(m.group(2)), int(m.group(3))
        except ValueError:
            continue
        parsed_cleaned.append((idx, q, a_i, b_i, c_i))

    for idx, q, a_i, b_i, c_i in parsed_cleaned:
        if c_i < 5 or c_i > 9:
            continue
        for idx2, q2, a2, b2, c2 in parsed_cleaned:
            if idx2 == idx:
                continue
            if a2 != a_i:
                continue
            if b2 >= b_i or abs(b_i - b2) > 1:
                continue
            if c2 > 3:
                continue
            if float(q_factor.get(q2, 1.0)) + 1e-6 < float(q_factor.get(q, 1.0)):
                continue
            q_factor[q] = min(float(q_factor.get(q, 1.0)), 0.62)
            q_ambiguous[q] = True
            break

    with_meta: list[tuple[str, float, bool]] = []
    for q in cleaned:
        with_meta.append((q, float(q_factor.get(q, 1.0)), bool(q_ambiguous.get(q, False))))
    return with_meta

def extract_assignments_from_lines(lines: list[dict]) -> list[dict]:
    """
    ???:
    P210 4.4.2 4.4.5
    4.4.6 4.4.8
    4.4.11

    ???????????Pxxx?????????????
    """
    current_page = None
    known_prefixes: list[tuple[str, str]] = []
    extracted = []

    for line in lines:
        page = extract_page_from_line(line, current_page=current_page, known_prefixes=known_prefixes)
        if page is not None:
            current_page = page

        questions = extract_questions_from_line(line, known_prefixes=known_prefixes)

        for q, q_factor, q_ambiguous in questions:
            extracted.append(
                {
                    "page": current_page,
                    "question": q,
                    "line_text": line["text"],
                    "line_score": float(line["score"]) * float(q_factor),
                    "q_ambiguous": bool(q_ambiguous),
                }
            )
            pref = question_prefix(q)
            if pref is not None:
                known_prefixes = [pref] + [x for x in known_prefixes if x != pref]
                known_prefixes = known_prefixes[:8]

    # ???????
    seen = set()
    deduped = []
    for item in extracted:
        key = (item["page"], item["question"])
        if key not in seen:
            seen.add(key)
            deduped.append(item)

    return deduped


def merge_variant_assignments(variant_results: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    overall_page_weights: dict[str, float] = defaultdict(float)
    order_idx = 0

    for vr_idx, vr in enumerate(variant_results):
        variant_name = str(vr.get("variant") or f"variant_{vr_idx}")
        for item in vr.get("assignments", []):
            q = repair_question_token(str(item.get("question", "")))
            if not is_valid_question(q):
                continue

            line_score = float(item.get("line_score", 0.0))
            q_ambiguous = bool(item.get("q_ambiguous", False))

            page_raw = item.get("page")
            page = None
            if page_raw:
                page_candidate = repair_page_token(str(page_raw))
                if PAGE_RE.fullmatch(page_candidate):
                    page = page_candidate
                    overall_page_weights[page] += line_score

            if q not in merged:
                merged[q] = {
                    "first_order": order_idx,
                    "best_score": line_score,
                    "best_line_text": str(item.get("line_text", "")),
                    "page_weights": defaultdict(float),
                    "count": 0,
                    "variants": set(),
                    "ambiguous_hits": 0,
                }
                order_idx += 1

            rec = merged[q]
            rec["count"] += 1
            rec["variants"].add(variant_name)
            if q_ambiguous:
                rec["ambiguous_hits"] += 1
            if line_score > rec["best_score"]:
                rec["best_score"] = line_score
                rec["best_line_text"] = str(item.get("line_text", ""))
            if page is not None:
                rec["page_weights"][page] += line_score

    if not merged:
        return []

    default_page = None
    if overall_page_weights:
        default_page = max(overall_page_weights.items(), key=lambda x: x[1])[0]

    # Merge by question base: keep the stronger full form.
    best_form_by_base: dict[str, tuple[str, tuple[float, int]]] = {}
    for q, rec in merged.items():
        base = question_base(q)
        score_key = (rec["best_score"] + 0.15 * rec["count"], rec["count"])
        prev = best_form_by_base.get(base)
        if prev is None or score_key > prev[1]:
            best_form_by_base[base] = (q, score_key)

    selected_questions = {
        q for q in (v[0] for v in best_form_by_base.values()) if q in merged
    }

    # Dominant first segment (chapter) for outlier filtering.
    first_seg_weights: dict[int, float] = defaultdict(float)
    first_second_weights: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    first_second_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for q in selected_questions:
        m = re.match(r"^(\d{1,2})\.", q)
        if m:
            first_seg_weights[int(m.group(1))] += merged[q]["best_score"] + 0.2 * merged[q]["count"]
        m2 = re.match(r"^(\d{1,2})\.(\d{1,2})\.", q)
        if m2:
            first_second_weights[m2.group(1)][m2.group(2)] += (
                merged[q]["best_score"] + 0.2 * merged[q]["count"]
            )
            first_second_counts[m2.group(1)][m2.group(2)] += 1

    dominant_first_seg = None
    if first_seg_weights:
        dominant_first_seg = max(first_seg_weights.items(), key=lambda x: x[1])[0]

    # Prefix density for suppressing low-confidence singleton noise.
    prefix_density: dict[tuple[str, str], int] = defaultdict(int)
    for q in selected_questions:
        pref = question_prefix(q)
        if pref is not None:
            prefix_density[pref] += 1

    # Prefix-level page support, used to smooth per-question page jitter.
    prefix_page_weights: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: defaultdict(float))
    # Reliable anchors where a question under prefix has a clearly dominant page.
    prefix_page_anchor_count: dict[tuple[tuple[str, str], str], int] = defaultdict(int)
    for q in selected_questions:
        pref = question_prefix(q)
        if pref is None:
            continue
        rec_q = merged[q]
        valid_pages: list[tuple[str, float]] = []
        for p, w in rec_q["page_weights"].items():
            if PAGE_RE.fullmatch(p):
                wv = float(w)
                prefix_page_weights[pref][p] += wv
                valid_pages.append((p, wv))
        if not valid_pages:
            continue
        valid_pages.sort(key=lambda x: x[1], reverse=True)
        top_page, top_w = valid_pages[0]
        second_w = valid_pages[1][1] if len(valid_pages) > 1 else 0.0
        triplet_q = question_triplet_str(q)
        tail_len_q = len(triplet_q[2]) if triplet_q is not None else 1
        # Avoid letting weak long-tail noise drive prefix-page consensus.
        if (
            tail_len_q <= 2
            and rec_q["best_score"] >= 0.80
            and top_w >= second_w + 0.06
        ):
            prefix_page_anchor_count[(pref, top_page)] += 1

    # If both short and long tail forms exist (e.g. 1 and 11), downweight likely OCR adhesion.
    likely_suffix_noise: set[str] = set()
    prefix_to_questions: dict[tuple[str, str], list[str]] = defaultdict(list)
    for q in selected_questions:
        pref = question_prefix(q)
        if pref is not None:
            prefix_to_questions[pref].append(q)
    for qs in prefix_to_questions.values():
        triples = []
        for q in qs:
            triplet = question_triplet_str(q)
            if triplet is not None:
                triples.append((q, triplet[2]))
        for q_long, c_long in triples:
            if len(c_long) < 2:
                continue
            rec_long = merged[q_long]
            for q_short, c_short in triples:
                if q_short == q_long:
                    continue
                if len(c_long) != len(c_short) + 1 or not c_long.startswith(c_short):
                    continue
                rec_short = merged[q_short]
                long_support = len(rec_long["variants"])
                short_support = len(rec_short["variants"])
                if short_support < long_support:
                    continue
                if rec_short["best_score"] + 0.03 < rec_long["best_score"]:
                    continue
                if long_support <= 2 and rec_long["best_score"] < 0.92:
                    likely_suffix_noise.add(q_long)
                    break

    # Tail-length distribution under each prefix, used to suppress long-tail outliers.
    prefix_tail_len_weights: dict[tuple[str, str], dict[int, float]] = defaultdict(dict)
    prefix_tail_support: dict[tuple[tuple[str, str], str], int] = {}
    prefix_tail_numbers: dict[tuple[str, str], set[int]] = defaultdict(set)
    prefix_tail_best_q: dict[tuple[tuple[str, str], str], str] = {}
    for q in selected_questions:
        pref = question_prefix(q)
        triplet = question_triplet_str(q)
        if pref is None or triplet is None:
            continue
        seg_len = len(triplet[2])
        cur = prefix_tail_len_weights[pref].get(seg_len, 0.0)
        support = int(len(merged[q]["variants"]))
        prefix_tail_len_weights[pref][seg_len] = cur + max(1.0, float(support))
        key_tail = (pref, triplet[2])
        prev_support = prefix_tail_support.get(key_tail, 0)
        if support > prev_support:
            prefix_tail_support[key_tail] = support
            prefix_tail_best_q[key_tail] = q
        try:
            prefix_tail_numbers[pref].add(int(triplet[2]))
        except ValueError:
            pass

    out = []
    for q in selected_questions:
        rec = merged[q]
        variant_support = len(rec["variants"])
        q_emit = q

        triplet = question_triplet_str(q)
        if q in likely_suffix_noise:
            continue
        pref = question_prefix(q)
        # Ambiguous singleton suppression:
        # keep low-tail candidates (e.g. 1.4.1), but suppress high-tail noise like 1.5.6.
        if (
            pref is not None
            and triplet is not None
            and prefix_density.get(pref, 0) >= 2
            and rec["count"] == 1
            and rec.get("ambiguous_hits", 0) >= 1
            and rec["best_score"] < 0.82
        ):
            try:
                tail_num = int(triplet[2])
            except ValueError:
                tail_num = -1
            tail_nums = prefix_tail_numbers.get(pref, set())
            has_low_tail = any(n <= 3 for n in tail_nums if n != tail_num)
            if tail_num >= 5 and has_low_tail:
                continue
        if triplet is not None and pref is not None:
            seg_len = len(triplet[2])
            try:
                tail_num_for_soft = int(triplet[2])
            except ValueError:
                tail_num_for_soft = -1
            # Weak outlier section under same chapter (e.g. 4.1.1 amid strong 4.4.* cluster).
            sec_weights = first_second_weights.get(triplet[0], {})
            if len(sec_weights) >= 2:
                dominant_sec, dominant_w = max(sec_weights.items(), key=lambda x: x[1])
                cur_w = float(sec_weights.get(triplet[1], 0.0))
                if (
                    triplet[1] != dominant_sec
                    and rec["count"] <= 1
                    and variant_support <= 1
                    and rec["best_score"] < 0.82
                    and cur_w > 0.0
                    and dominant_w >= 3.5 * cur_w
                ):
                    continue
            # Sparse section outlier: one weak singleton section while at least two other
            # sections under the same chapter have dense support.
            sec_counts = first_second_counts.get(triplet[0], {})
            cur_sec_cnt = int(sec_counts.get(triplet[1], 0))
            dense_other_secs = sum(
                1 for s, c in sec_counts.items() if s != triplet[1] and int(c) >= 2
            )
            if (
                cur_sec_cnt <= 1
                and dense_other_secs >= 2
                and rec["count"] <= 1
                and variant_support <= 1
                and rec["best_score"] < 0.82
            ):
                continue
            seg_w = prefix_tail_len_weights.get(pref, {})
            if seg_w:
                dominant_len = max(seg_w.items(), key=lambda x: x[1])[0]
                # Soft suppression for implausible single-digit high-tail singleton noise
                # (e.g. 1.5.6 from a blurred 1.4.1), while preserving 2-digit tails like 11.
                if (
                    seg_len == 1
                    and tail_num_for_soft >= 5
                    and rec["count"] <= 2
                    and variant_support <= 2
                    and rec["best_score"] < 0.92
                ):
                    tail_nums_this_pref = prefix_tail_numbers.get(pref, set())
                    low_tails_this_pref = [n for n in tail_nums_this_pref if n <= 3]
                    if low_tails_this_pref and len(tail_nums_this_pref) <= 2:
                        first_seg = pref[0]
                        has_other_low_tail_prefix = False
                        for opref, nums in prefix_tail_numbers.items():
                            if opref == pref or opref[0] != first_seg:
                                continue
                            if any(n <= 3 for n in nums):
                                has_other_low_tail_prefix = True
                                break
                        if has_other_low_tail_prefix:
                            continue
                # Weak singleton neighbor-tail noise (e.g. 1.4.2 vs much stronger 1.4.3).
                if (
                    seg_len == 1
                    and rec["count"] <= 1
                    and variant_support <= 1
                    and rec["best_score"] < 0.82
                ):
                    weak_tail = tail_num_for_soft
                    if weak_tail > 1:
                        should_drop_weak_neighbor = False
                        for nb in (weak_tail - 1, weak_tail + 1):
                            nb_q = prefix_tail_best_q.get((pref, str(nb)))
                            if nb_q is None or nb_q not in merged:
                                continue
                            nb_rec = merged[nb_q]
                            nb_support = len(nb_rec["variants"])
                            if (
                                nb_support >= variant_support
                                and nb_rec["best_score"] >= rec["best_score"] + 0.10
                            ):
                                should_drop_weak_neighbor = True
                                break
                        if should_drop_weak_neighbor:
                            continue
                if (
                    dominant_len == 1
                    and seg_len >= 3
                    and len(prefix_to_questions.get(pref, [])) >= 2
                    and variant_support <= 1
                    and rec["best_score"] < 0.90
                ):
                    continue
                # Repeated two-digit tail with low support/score: map to single-digit tail.
                # Example: 1.6.11 -> 1.6.1 when no strong evidence for true 11.
                if (
                    seg_len == 2
                    and triplet[2][0] == triplet[2][1]
                    and variant_support <= 1
                    and rec["best_score"] < 0.78
                ):
                    tail_nums = prefix_tail_numbers.get(pref, set())
                    if any(n <= 9 for n in tail_nums):
                        q_emit = f"{int(triplet[0])}.{int(triplet[1])}.{int(triplet[2][0])}"
                # Symmetric case: weak single tail likely split from strong repeated tail.
                if (
                    seg_len == 1
                    and variant_support <= 1
                ):
                    repeated_tail = f"{triplet[2]}{triplet[2]}"
                    long_q = prefix_tail_best_q.get((pref, repeated_tail))
                    if long_q is not None and long_q in merged:
                        long_rec = merged[long_q]
                        long_support = len(long_rec["variants"])
                        if (
                            long_support >= 2
                            and long_rec["best_score"] >= rec["best_score"] + 0.03
                        ):
                            continue
                # Repeated-tail noise fallback, e.g. 1.6.11 from 1.6.1 adhesion.
                if (
                    dominant_len == 1
                    and seg_len == 2
                    and triplet[2][0] == triplet[2][1]
                    and variant_support <= 1
                ):
                    single_tail = triplet[2][0]
                    single_support = prefix_tail_support.get((pref, single_tail), 0)
                    single_q = prefix_tail_best_q.get((pref, single_tail))
                    if single_q is not None:
                        try:
                            tail_num = int(triplet[2])
                            single_num = int(single_tail)
                        except ValueError:
                            tail_num = -1
                            single_num = -1

                        tail_nums = prefix_tail_numbers.get(pref, set())
                        has_mid = any(single_num < n < tail_num for n in tail_nums)
                        low_nums = [n for n in tail_nums if n <= 9]
                        max_low = max(low_nums) if low_nums else single_num
                        large_jump = (tail_num - max_low) >= 6 if tail_num >= 0 and max_low >= 0 else False
                        if single_support >= variant_support and has_mid and large_jump:
                            continue

        page = default_page
        if rec["page_weights"]:
            page = max(rec["page_weights"].items(), key=lambda x: x[1])[0]

        # Weak single-page evidence: fall back to dominant page.
        if (
            default_page is not None
            and page != default_page
            and len(rec["page_weights"]) == 1
            and rec["best_score"] < 0.82
        ):
            page = default_page

        # Smooth page by same question-prefix consensus (e.g. 4.2.* => P208).
        pref_pages = prefix_page_weights.get(pref, {})
        if pref is not None and pref_pages:
            sorted_pref_pages = sorted(pref_pages.items(), key=lambda x: x[1], reverse=True)
            pref_page, pref_total_w = sorted_pref_pages[0]
            second_total_w = sorted_pref_pages[1][1] if len(sorted_pref_pages) > 1 else 0.0
            pref_anchor = int(prefix_page_anchor_count.get((pref, pref_page), 0))
            if page != pref_page:
                cur_w = float(rec["page_weights"].get(page, 0.0)) if page is not None else 0.0
                pref_w = float(rec["page_weights"].get(pref_page, 0.0))
                # Only smooth when prefix consensus is both dominant and anchored by
                # at least one reliable neighbor question under the same prefix.
                prefix_consensus_strong = (
                    pref_anchor >= 1 and pref_total_w >= second_total_w + 0.10
                )
                if (
                    prefix_consensus_strong
                    and pref_w > 0.0
                    and pref_w + 1e-6 >= 0.95 * max(cur_w, 1e-9)
                ):
                    page = pref_page
                elif prefix_consensus_strong and page is None and pref_w > 0.0:
                    page = pref_page

        # Filter chapter outliers.
        m_first = re.match(r"^(\d{1,2})\.", q)
        if dominant_first_seg is not None and m_first is not None:
            first_seg = int(m_first.group(1))
            if first_seg != dominant_first_seg:
                dom_w = first_seg_weights.get(dominant_first_seg, 0.0)
                cur_w = first_seg_weights.get(first_seg, 0.0)
                if rec["count"] <= 1:
                    continue
                if cur_w > 0 and dom_w >= 2.5 * cur_w and rec["best_score"] < 0.85:
                    continue

        # Filter low-confidence singletons under dense same-prefix clusters.
        pref = question_prefix(q)
        if (
            pref is not None
            and prefix_density.get(pref, 0) >= 5
            and rec["count"] == 1
            and rec["best_score"] < 0.80
        ):
            continue

        out.append(
            {
                "page": page,
                "question": q_emit,
                "line_text": rec["best_line_text"],
                "line_score": rec["best_score"],
                "_order": rec["first_order"],
            }
        )

    # Keep board reading order first; use page/question sort as fallback.
    out.sort(
        key=lambda x: (
            x["_order"],
            _parse_page_num(x["page"]) if x["page"] is not None else 99999,
            *question_sort_key(x["question"]),
        )
    )

    deduped_out = []
    seen_questions = set()
    for item in out:
        q = item.get("question")
        if q in seen_questions:
            continue
        seen_questions.add(q)
        deduped_out.append(item)
    out = deduped_out

    for item in out:
        item.pop("_order", None)
    return out


def choose_best_variant(variant_results: list[dict]) -> dict:
    """
    浼樺厛閫夆€滄彁鍙栭鍙锋暟閲忔洿澶氣€濈殑鐗堟湰锛?
    鏁伴噺鐩稿悓鍒欓€夊钩鍧囩疆淇″害鏇撮珮鐨勭増鏈€?
    """
    best = None
    for vr in variant_results:
        count = len(vr["assignments"])
        avg_score = 0.0
        if vr["assignments"]:
            avg_score = sum(x["line_score"] for x in vr["assignments"]) / len(vr["assignments"])

        score_tuple = (count, avg_score)
        if best is None or score_tuple > best["score_tuple"]:
            vr["score_tuple"] = score_tuple
            best = vr

    return best


def draw_boxes(image: np.ndarray, items: list[dict]) -> np.ndarray:
    vis = image.copy()
    for it in items:
        x1, y1, x2, y2 = it["box"]
        cv2.rectangle(vis, (x1, y1), (x2, y2), (0, 0, 255), 2)
        txt = f'{normalize_text(it["text"])}:{it["score"]:.2f}'
        cv2.putText(
            vis,
            txt[:40],
            (x1, max(y1 - 5, 15)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.55,
            (255, 0, 0),
            1,
            cv2.LINE_AA,
        )
    return vis


# =========================
# 5. 鍗曞浘 / 鎵瑰鐞?
# =========================

def process_one_image(
    image_path: Path,
    engine: RapidOCR,
    out_dir: Path,
    manual_points: np.ndarray | None = None,
) -> dict:
    image = cv2.imread(str(image_path))
    if image is None:
        raise ValueError(f"璇诲浘澶辫触: {image_path}")

    board = get_board(image, manual_points=manual_points)
    variants = enhance_variants(board)

    variant_results = []
    debug_dir = out_dir / "debug" / image_path.stem
    debug_dir.mkdir(parents=True, exist_ok=True)

    cv2.imwrite(str(debug_dir / "01_board.jpg"), board)

    for name, var_img in variants.items():
        items = run_ocr_on_image(engine, var_img)
        lines = group_items_to_lines(items)
        assignments = extract_assignments_from_lines(lines)

        variant_results.append(
            {
                "variant": name,
                "image": var_img,
                "items": items,
                "lines": lines,
                "assignments": assignments,
            }
        )

        cv2.imwrite(str(debug_dir / f"{name}.jpg"), var_img)
        boxed = draw_boxes(var_img, items)
        cv2.imwrite(str(debug_dir / f"{name}_boxes.jpg"), boxed)

        with open(debug_dir / f"{name}_lines.txt", "w", encoding="utf-8") as f:
            for line in lines:
                f.write(f'{line["score"]:.3f}\t{line["text"]}\n')

    best = choose_best_variant(variant_results)
    merged_assignments = merge_variant_assignments(variant_results)
    if not merged_assignments:
        merged_assignments = best["assignments"]

    result = {
        "image": image_path.name,
        "best_variant": f"ensemble/{best['variant']}",
        "assignments": merged_assignments,
    }

    with open(debug_dir / "best_result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


def collect_images(input_path: Path) -> list[Path]:
    if input_path.is_file():
        return [input_path]

    exts = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}
    files = [p for p in input_path.iterdir() if p.is_file() and p.suffix.lower() in exts]
    return sorted(files)


def save_results(results: list[dict], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(out_dir / "homework_result.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    with open(out_dir / "homework_result.csv", "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["image", "page", "question", "best_variant"])
        for r in results:
            for item in r["assignments"]:
                writer.writerow(
                    [
                        r["image"],
                        item["page"] if item["page"] is not None else "",
                        item["question"],
                        r["best_variant"],
                    ]
                )


def build_ocr() -> RapidOCR:
    _suppress_rapidocr_noise()
    try:
        engine = RapidOCR(params={"Global.log_level": "error"})
    except Exception:
        engine = RapidOCR()
    _suppress_rapidocr_noise()
    return engine


def get_shared_ocr() -> RapidOCR:
    global _OCR_ENGINE
    if _OCR_ENGINE is not None:
        return _OCR_ENGINE
    with _OCR_ENGINE_LOCK:
        if _OCR_ENGINE is None:
            _OCR_ENGINE = build_ocr()
    return _OCR_ENGINE


def process_board_image(board: np.ndarray, engine: RapidOCR | None = None) -> dict:
    ocr_engine = engine or get_shared_ocr()
    variants = enhance_variants(board)
    variant_results = []

    for name, var_img in variants.items():
        items = run_ocr_on_image(ocr_engine, var_img)
        lines = group_items_to_lines(items)
        assignments = extract_assignments_from_lines(lines)
        variant_results.append(
            {
                "variant": name,
                "items": items,
                "lines": lines,
                "assignments": assignments,
            }
        )

    if not variant_results:
        return {"best_variant": "none", "assignments": []}

    best = choose_best_variant(variant_results)
    merged_assignments = merge_variant_assignments(variant_results)
    if not merged_assignments:
        merged_assignments = best.get("assignments") or []

    return {
        "best_variant": f"ensemble/{best['variant']}",
        "assignments": merged_assignments,
    }


def recognize_homework_from_array(
    image: np.ndarray,
    engine: RapidOCR | None = None,
    min_area_ratio: float = 0.18,
    min_green_ratio: float = 0.18,
) -> dict:
    if image is None or image.size == 0:
        return {
            "is_green_blackboard": False,
            "best_variant": "none",
            "assignments": [],
        }

    has_board, board = extract_green_board(
        image,
        min_area_ratio=float(min_area_ratio),
        min_green_ratio=float(min_green_ratio),
    )
    if not has_board:
        return {
            "is_green_blackboard": False,
            "best_variant": "none",
            "assignments": [],
        }

    data = process_board_image(board, engine=engine)
    data["is_green_blackboard"] = True
    return data


def recognize_homework_from_path(
    image_path: str | Path,
    engine: RapidOCR | None = None,
    min_area_ratio: float = 0.18,
    min_green_ratio: float = 0.18,
) -> dict:
    image = cv2.imread(str(image_path))
    if image is None:
        raise ValueError(f"读取图片失败: {image_path}")
    out = recognize_homework_from_array(
        image,
        engine=engine,
        min_area_ratio=min_area_ratio,
        min_green_ratio=min_green_ratio,
    )
    out["image"] = Path(str(image_path)).name
    return out


def format_assignment_lines(assignments: list[dict]) -> list[str]:
    lines = []
    for item in assignments:
        page = str(item.get("page") or "P?")
        question = str(item.get("question") or "").strip()
        if not question:
            continue
        lines.append(f"{page}  {question}")
    return lines


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default=".",
        help="单张图片路径，或图片目录；默认为当前目录",
    )
    parser.add_argument("--output", default="output_hw", help="杈撳嚭鐩綍")
    parser.add_argument(
        "--manual_points",
        default="",
        help='鍙€夛紝鎵嬪姩鎸囧畾榛戞澘鍥涚偣: "x1,y1;x2,y2;x3,y3;x4,y4"',
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    out_dir = Path(args.output)
    manual_points = parse_manual_points(args.manual_points) if args.manual_points else None

    engine = build_ocr()

    image_files = collect_images(input_path)
    if not image_files:
        raise ValueError("娌℃湁鎵惧埌鍥剧墖")

    results = []
    for img_path in image_files:
        print(f"[INFO] Processing: {img_path}")
        result = process_one_image(
            img_path,
            engine,
            out_dir,
            manual_points=manual_points,
        )
        results.append(result)

        print(f"[INFO] Best variant: {result['best_variant']}")
        if result["assignments"]:
            for a in result["assignments"]:
                print(f"    {a['page'] or 'P?'}  {a['question']}")
        else:
            print("    No assignments detected.")

    save_results(results, out_dir)
    print(f"\n[OK] Done. Results saved to: {out_dir}")


if __name__ == "__main__":
    main()

