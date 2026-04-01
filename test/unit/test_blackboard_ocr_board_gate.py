from __future__ import annotations

import cv2
import numpy as np

import blackboard_ocr as bo


def test_green_board_contour_accepts_large_wide_rectangle() -> None:
    h, w = 720, 1280
    mask = np.zeros((h, w), dtype=np.uint8)
    cv2.rectangle(mask, (90, 130), (1190, 620), 255, thickness=-1)

    ok = bo._has_green_board_contour(mask, (h, w, 3), min_area_ratio=0.18)
    assert ok is True


def test_green_board_contour_rejects_large_round_blob() -> None:
    h, w = 720, 1280
    mask = np.zeros((h, w), dtype=np.uint8)
    cv2.circle(mask, (640, 360), 260, 255, thickness=-1)

    ok = bo._has_green_board_contour(mask, (h, w, 3), min_area_ratio=0.18)
    assert ok is False


def test_extract_green_board_uses_original_for_near_board_image() -> None:
    h, w = 540, 960
    image = np.full((h, w, 3), (85, 150, 85), dtype=np.uint8)
    cv2.putText(
        image,
        "P39 1.5.2 1.6.1",
        (120, 220),
        cv2.FONT_HERSHEY_SIMPLEX,
        1.1,
        (230, 230, 230),
        2,
        cv2.LINE_AA,
    )

    ok, board = bo.extract_green_board(image, min_area_ratio=0.18, min_green_ratio=0.18)
    assert ok is True
    assert board.shape == image.shape


def test_repair_question_token_accepts_k_as_four() -> None:
    out = bo.repair_question_token("1K1")
    assert out == "1.4.1"
