from __future__ import annotations

import blackboard_ocr as bo


def _item(page: str, question: str, score: float, text: str = "line") -> dict:
    return {
        "page": page,
        "question": question,
        "line_score": score,
        "line_text": text,
    }


def _item_amb(page: str, question: str, score: float, ambiguous: bool, text: str = "line") -> dict:
    d = _item(page, question, score, text=text)
    d["q_ambiguous"] = bool(ambiguous)
    return d


def test_merge_variant_assignments_filters_noise_and_keeps_board_order() -> None:
    variant_results = [
        {
            "variant": "board_original",
            "assignments": [
                _item("P39", "1.5.2", 0.93, "P39 1.5.2 1.6.1 1.6.3"),
                _item("P39", "1.6.1", 0.91, "P39 1.5.2 1.6.1 1.6.3"),
                _item("P39", "1.6.3", 0.92, "P39 1.5.2 1.6.1 1.6.3"),
                _item("P39", "1.4.1", 0.90, "1.4.1 1.4.3"),
                _item("P39", "1.4.3", 0.90, "1.4.1 1.4.3"),
            ],
        },
        {
            "variant": "board_adaptive",
            "assignments": [
                _item("P37", "1.5.2", 0.84, "P37 1.5.2 1.5.611 1.6.11"),
                _item("P37", "1.5.611", 0.86, "P37 1.5.2 1.5.611 1.6.11"),
                _item("P37", "1.6.11", 0.84, "P37 1.5.2 1.5.611 1.6.11"),
                _item("P39", "1.4.1", 0.82, "1.4.1"),
                _item("P39", "1.6.3", 0.83, "1.6.3"),
            ],
        },
        {
            "variant": "board_clahe",
            "assignments": [
                _item("P39", "1.5.2", 0.88, "P39 1.5.2 1.6.3"),
                _item("P39", "1.6.3", 0.87, "P39 1.5.2 1.6.3"),
                _item("P39", "1.4.1", 0.86, "1.4.1"),
            ],
        },
    ]

    merged = bo.merge_variant_assignments(variant_results)
    got = [(item["page"], item["question"]) for item in merged]
    assert got == [
        ("P39", "1.5.2"),
        ("P39", "1.6.1"),
        ("P39", "1.6.3"),
        ("P39", "1.4.1"),
        ("P39", "1.4.3"),
    ]


def test_merge_variant_assignments_preserves_reading_order_for_single_variant() -> None:
    variant_results = [
        {
            "variant": "board_original",
            "assignments": [
                _item("P39", "1.5.2", 0.94),
                _item("P39", "1.4.1", 0.93),
                _item("P39", "1.4.3", 0.92),
            ],
        }
    ]

    merged = bo.merge_variant_assignments(variant_results)
    got = [item["question"] for item in merged]
    assert got == ["1.5.2", "1.4.1", "1.4.3"]


def test_merge_variant_assignments_keeps_sparse_valid_items() -> None:
    variant_results = [
        {
            "variant": "board_original",
            "assignments": [
                _item("P39", "1.5.2", 0.88),
                _item("P39", "1.6.1", 0.86),
                _item("P39", "1.6.3", 0.85),
                _item("P39", "1.4.1", 0.84),
                _item("P39", "1.4.3", 0.84),
            ],
        },
        {
            "variant": "board_adaptive",
            "assignments": [
                _item("P37", "1.5.2", 0.82),
                _item("P37", "1.5.611", 0.86),
                _item("P37", "1.6.11", 0.87),
            ],
        },
    ]

    merged = bo.merge_variant_assignments(variant_results)
    got = [(item["page"], item["question"]) for item in merged]
    assert got == [
        ("P39", "1.5.2"),
        ("P39", "1.6.1"),
        ("P39", "1.6.3"),
        ("P39", "1.4.1"),
        ("P39", "1.4.3"),
    ]


def test_merge_variant_assignments_drops_repeated_tail_noise_like_1611() -> None:
    variant_results = [
        {
            "variant": "board_original",
            "assignments": [
                _item("P39", "1.6.1", 0.79),
                _item("P39", "1.6.3", 0.84),
                _item("P39", "1.5.2", 0.82),
            ],
        },
        {
            "variant": "board_clahe",
            "assignments": [
                _item("P39", "1.6.1", 0.81),
                _item("P39", "1.4.1", 0.83),
                _item("P39", "1.4.3", 0.82),
            ],
        },
        {
            "variant": "board_adaptive",
            "assignments": [
                _item("P37", "1.6.11", 0.89),
            ],
        },
    ]

    merged = bo.merge_variant_assignments(variant_results)
    got = [item["question"] for item in merged]
    assert "1.6.11" not in got
    assert "1.6.1" in got


def test_merge_variant_assignments_keeps_11_without_distribution_evidence() -> None:
    variant_results = [
        {
            "variant": "board_original",
            "assignments": [
                _item("P40", "2.5.1", 0.83),
                _item("P40", "2.5.11", 0.87),
            ],
        },
        {
            "variant": "board_clahe",
            "assignments": [
                _item("P40", "2.5.1", 0.81),
            ],
        },
    ]

    merged = bo.merge_variant_assignments(variant_results)
    got = [item["question"] for item in merged]
    assert "2.5.1" in got
    assert "2.5.11" in got


def test_merge_variant_assignments_drops_single_ambiguous_compact_noise() -> None:
    variant_results = [
        {
            "variant": "board_original",
            "assignments": [
                _item_amb("P37", "1.5.2", 0.90, ambiguous=False),
                _item_amb("P37", "1.6.1", 0.88, ambiguous=False),
                _item_amb("P37", "1.5.6", 0.57, ambiguous=True),
            ],
        },
        {
            "variant": "board_clahe",
            "assignments": [
                _item_amb("P39", "1.5.2", 0.86, ambiguous=False),
                _item_amb("P39", "1.6.1", 0.84, ambiguous=False),
            ],
        },
    ]

    merged = bo.merge_variant_assignments(variant_results)
    got = [item["question"] for item in merged]
    assert "1.5.6" not in got
    assert "1.5.2" in got
    assert "1.6.1" in got


def test_merge_variant_assignments_keeps_single_non_ambiguous_under_same_prefix() -> None:
    variant_results = [
        {
            "variant": "board_original",
            "assignments": [
                _item_amb("P40", "1.5.2", 0.90, ambiguous=False),
                _item_amb("P40", "1.5.6", 0.88, ambiguous=False),
            ],
        }
    ]

    merged = bo.merge_variant_assignments(variant_results)
    got = [item["question"] for item in merged]
    assert "1.5.2" in got
    assert "1.5.6" in got


def test_merge_variant_assignments_keeps_low_tail_ambiguous_candidate() -> None:
    variant_results = [
        {
            "variant": "board_original",
            "assignments": [
                _item_amb("P39", "1.4.3", 0.90, ambiguous=False),
                _item_amb("P39", "1.4.1", 0.60, ambiguous=True),
            ],
        }
    ]

    merged = bo.merge_variant_assignments(variant_results)
    got = [item["question"] for item in merged]
    assert "1.4.1" in got
    assert "1.4.3" in got


def test_joined_triplet_token_is_marked_ambiguous() -> None:
    assert bo._is_joined_triplet_ambiguous_token("156") is True
    assert bo._is_joined_triplet_ambiguous_token("1.5.6") is False


def test_extract_questions_from_line_prefers_local_prefix_for_joined_triplet() -> None:
    line = {
        "text": "156 1.4.3",
        "tokens": ["156", "1.4.3"],
        "score": 0.90,
    }
    qs = bo.extract_questions_from_line(line, known_prefixes=[("1", "5"), ("1", "6")])
    got = [q for q, _, _ in qs]
    assert "1.4.3" in got
    assert "1.5.6" not in got
